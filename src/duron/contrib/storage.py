from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from io import IOBase

    from duron.log import BaseEntry, Entry


try:
    import fcntl

    def _lock_file(f: IOBase, /) -> None:
        if f.writable():
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_file(f: IOBase, /) -> None:
        if f.writable():
            fcntl.flock(f, fcntl.LOCK_UN)

except ModuleNotFoundError:

    def _lock_file(_f: IOBase, /) -> None:
        pass

    def _unlock_file(_f: IOBase, /) -> None:
        pass


try:
    import orjson

    def _json_dumps(obj: object) -> bytes:
        try:
            return orjson.dumps(obj)
        except (TypeError, ValueError) as e:
            msg = f"Object of type {type(obj).__name__} is not JSON serializable"
            raise TypeError(msg) from e

    def _json_loads(s: bytes) -> object:
        try:
            return orjson.loads(s)
        except orjson.JSONDecodeError as e:
            raise json.JSONDecodeError(e.msg, e.doc, e.pos) from None

except ModuleNotFoundError:

    def _json_dumps(obj: object) -> bytes:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )

    def _json_loads(s: bytes) -> object:
        return json.loads(s)


class FileLogStorage:
    """A [log storage][duron.log.LogStorage] that uses a file to store log entries."""

    __slots__ = ("_lease", "_lock", "_log_file")

    def __init__(self, log_file: str | Path) -> None:
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._lease: IOBase | None = None
        self._lock = asyncio.Lock()

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        if not self._log_file.exists():
            return

        with Path(self._log_file).open("rb") as f:  # noqa: ASYNC230
            # Read existing lines from start offset
            while True:
                line_start_offset = f.tell()
                line = f.readline()
                if line:
                    try:
                        entry = _json_loads(line)
                        if isinstance(entry, dict):
                            yield (
                                line_start_offset,
                                cast("BaseEntry", cast("object", entry)),
                            )
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                else:
                    # Reached end of file
                    break

    async def acquire_lease(self) -> bytes:
        async with self._lock:
            self._lease = Path(self._log_file).open("ab")  # noqa: ASYNC230, SIM115
            _lock_file(self._lease)
            return self._lease.fileno().to_bytes(8, "big")

    async def release_lease(self, token: bytes) -> None:
        async with self._lock:
            if self._lease and token == self._lease.fileno().to_bytes(8, "big"):
                _unlock_file(self._lease)
                self._lease.close()
                self._lease = None

    async def append(self, token: bytes, entry: Entry) -> int:
        async with self._lock:
            if not self._lease or token != self._lease.fileno().to_bytes(8, "big"):
                msg = "Invalid lease token"
                raise ValueError(msg)

            f = self._lease
            offset = f.tell()
            _ = f.write(_json_dumps(entry))
            _ = f.write(b"\n")
            f.flush()
            return offset


class MemoryLogStorage:
    """A [log storage][duron.log.LogStorage] that keeps log entries in memory."""

    __slots__ = ("_condition", "_entries", "_leases", "_lock")

    _entries: list[BaseEntry]
    _leases: bytes | None
    _lock: asyncio.Lock
    _condition: asyncio.Condition

    def __init__(self, entries: list[BaseEntry] | None = None) -> None:
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # Yield existing entries
        async with self._lock:
            entries_snapshot = self._entries.copy()

        for index in range(len(entries_snapshot)):
            yield (index, entries_snapshot[index])

    async def acquire_lease(self) -> bytes:
        lease_id = uuid.uuid4().bytes
        async with self._lock:
            self._leases = lease_id
        return lease_id

    async def release_lease(self, token: bytes) -> None:
        async with self._lock:
            if token == self._leases:
                self._leases = None

    async def append(self, token: bytes, entry: Entry) -> int:
        async with self._condition:
            if token != self._leases:
                msg = "Invalid lease token"
                raise ValueError(msg)

            offset = len(self._entries)
            self._entries.append(cast("BaseEntry", cast("object", entry)))
            self._condition.notify_all()
            return offset

    async def entries(self) -> list[BaseEntry]:
        async with self._lock:
            return self._entries.copy()


class SQLiteLogManager:
    """A log manager that stores multiple task logs in a single SQLite database.

    Uses WAL mode and database-backed leases for multiprocess support.
    Last acquirer wins - no expiration tracking.
    """

    __slots__ = ("_db_path", "_lock", "_logs")

    def __init__(self, db_path: str | Path) -> None:
        """Initialize the SQLite log manager.

        Args:
            db_path: Path to the SQLite database file.

        """
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._logs: dict[str, SQLiteLog] = {}
        self._lock = asyncio.Lock()

        # Initialize database schema with WAL mode
        conn = sqlite3.connect(self._db_path)
        try:
            # Enable WAL mode for better concurrent access
            conn.execute("PRAGMA journal_mode=WAL")

            # Create log entries table - stores full entry data except metadata
            # Uses ROWID as offset (implicit, auto-incrementing)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    task_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    data BLOB NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_log_entries_task_id
                ON log_entries (task_id)
            """)

            # Create log metadata table - stores metadata separately
            # Joined with log_entries on entry_rowid
            # Only created when metadata exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS log_metadata (
                    entry_rowid INTEGER PRIMARY KEY,
                    metadata BLOB NOT NULL,
                    FOREIGN KEY (entry_rowid) REFERENCES log_entries(rowid)
                )
            """)

            # Create leases table for multiprocess coordination
            # Last acquirer wins - no expiration
            conn.execute("""
                CREATE TABLE IF NOT EXISTS leases (
                    task_id TEXT PRIMARY KEY,
                    lease_id TEXT NOT NULL
                )
            """)

            conn.commit()
        finally:
            conn.close()

    async def create_log(self, task_id: str) -> SQLiteLog:
        """Create or retrieve a log storage for the given task ID.

        Returns:
            A SQLiteLog instance for the specified task.

        """
        async with self._lock:
            if task_id not in self._logs:
                self._logs[task_id] = SQLiteLog(self._db_path, task_id)
            return self._logs[task_id]


class SQLiteLog:
    """A [log storage][duron.log.LogStorage] for a single task in a SQLite database.

    Implements multiprocess-safe lease mechanism using database-backed leases.
    Last acquirer wins - lease is only validated on append.
    """

    __slots__ = ("_db_path", "_lock", "_task_id")

    def __init__(self, db_path: Path, task_id: str) -> None:
        self._db_path = db_path
        self._task_id = task_id
        self._lock = asyncio.Lock()

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        loop = asyncio.get_running_loop()

        def _read_entries() -> list[tuple[int, BaseEntry]]:
            conn = sqlite3.connect(self._db_path)
            try:
                # Read from log_entries, convert JSONB data to JSON text
                # Data contains full entry except metadata
                cursor = conn.execute(
                    "SELECT rowid, json(data) "
                    "FROM log_entries "
                    "WHERE task_id = ? ORDER BY rowid",
                    (self._task_id,),
                )
                results: list[tuple[int, BaseEntry]] = []
                for rowid, data_json in cursor:
                    try:
                        # Parse entry from JSONB data
                        if data_json:
                            entry = _json_loads(data_json)
                            if isinstance(entry, dict):
                                results.append((
                                    rowid,
                                    cast("BaseEntry", cast("object", entry)),
                                ))
                    except json.JSONDecodeError:  # noqa: PERF203
                        pass
                return results
            finally:
                conn.close()

        entries = await loop.run_in_executor(None, _read_entries)
        for offset, entry in entries:
            yield (offset, entry)

    async def acquire_lease(self) -> bytes:
        """Acquire a lease for this task log.

        Uses database-backed leases for multiprocess coordination.
        Last acquirer wins - replaces any existing lease.

        Returns:
            A lease token that must be provided to append() and release_lease().

        """
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _acquire() -> bytes:
                conn = sqlite3.connect(self._db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    lease_id = uuid.uuid4().hex

                    # Upsert new lease (replaces existing lease for this task)
                    conn.execute(
                        "INSERT INTO leases (task_id, lease_id) "
                        "VALUES (?, ?) "
                        "ON CONFLICT(task_id) DO UPDATE SET "
                        "lease_id = excluded.lease_id",
                        (self._task_id, lease_id),
                    )
                    conn.commit()
                    return lease_id.encode("utf-8")
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()

            return await loop.run_in_executor(None, _acquire)

    async def release_lease(self, token: bytes) -> None:
        """Release a previously acquired lease.

        Args:
            token: The lease token returned by acquire_lease().

        """
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _release() -> None:
                conn = sqlite3.connect(self._db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    lease_id = token.decode("utf-8")
                    conn.execute(
                        "DELETE FROM leases WHERE task_id = ? AND lease_id = ?",
                        (self._task_id, lease_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()

            await loop.run_in_executor(None, _release)

    async def append(self, token: bytes, entry: Entry) -> int:
        """Append an entry to the log.

        Validates the lease before appending. Raises ValueError if invalid.

        Args:
            token: The lease token returned by acquire_lease().
            entry: The log entry to append.

        Returns:
            The offset (ROWID) of the appended entry.

        """
        async with self._lock:
            loop = asyncio.get_running_loop()

            def _append_entry() -> int:
                def _raise_invalid_lease() -> None:
                    msg = "Invalid lease token"
                    raise ValueError(msg)

                conn = sqlite3.connect(self._db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    lease_id = token.decode("utf-8")

                    # Extract fields
                    entry_id = entry["id"]
                    metadata = entry.get("metadata")

                    # Create entry without metadata for log_entries table
                    entry_without_metadata = {
                        k: v for k, v in entry.items() if k != "metadata"
                    }
                    data_json = _json_dumps(entry_without_metadata)

                    # Validate lease and insert into log_entries
                    cursor = conn.execute(
                        "INSERT INTO log_entries (task_id, id, data) "
                        "SELECT ?, ?, jsonb(?) "
                        "WHERE EXISTS ("
                        "    SELECT 1 FROM leases "
                        "    WHERE task_id = ? AND lease_id = ?"
                        ")",
                        (self._task_id, entry_id, data_json, self._task_id, lease_id),
                    )

                    # Check if insert succeeded (rowcount = 1 means valid lease)
                    if cursor.rowcount == 0:
                        conn.rollback()
                        _raise_invalid_lease()

                    rowid = cursor.lastrowid

                    # Insert into log_metadata only if metadata exists
                    if metadata is not None:
                        metadata_json = _json_dumps(metadata)
                        conn.execute(
                            "INSERT INTO log_metadata (entry_rowid, metadata) "
                            "VALUES (?, jsonb(?))",
                            (rowid, metadata_json),
                        )
                except Exception:
                    conn.rollback()
                    raise
                else:
                    conn.commit()
                    return rowid if rowid is not None else 0
                finally:
                    conn.close()

            return await loop.run_in_executor(None, _append_entry)
