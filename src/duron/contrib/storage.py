from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast

from duron.log._entry import CorruptLogError
from duron.log._helper import validate_entry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator
    from io import IOBase

    from duron.log import BaseEntry, Entry


try:
    import fcntl

    _file_lock_supported = True

    def _lock_file(f: IOBase, /) -> None:
        if f.writable():
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_file(f: IOBase, /) -> None:
        if f.writable():
            fcntl.flock(f, fcntl.LOCK_UN)

except ModuleNotFoundError:
    _file_lock_supported = False

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
    """File-backed log storage using a sidecar fencing token and OS file locks.

    Acquiring a lease immediately supersedes the previous token, including tokens
    issued by another `FileLogStorage` instance for the same path. This backend
    requires `fcntl` and therefore fails during initialization on unsupported
    platforms.
    """

    __slots__ = ("_lease_file", "_lock", "_lock_file", "_log_file")

    def __init__(self, log_file: str | Path) -> None:
        if not _file_lock_supported:
            msg = "FileLogStorage requires a platform with fcntl file locking"
            raise RuntimeError(msg)
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = self._log_file.with_suffix(self._log_file.suffix + ".lock")
        self._lease_file = self._log_file.with_suffix(self._log_file.suffix + ".lease")
        self._lock = asyncio.Lock()

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # The file read + yield lives in a synchronous generator so that
        # GeneratorExit-driven cleanup of the `with` block is deterministic
        # (an async generator abandoned mid-iteration may skip cleanup).
        for item in self._read_entries():
            yield item

    def _read_entries(self) -> Generator[tuple[int, BaseEntry], None, None]:
        if not self._log_file.exists():
            return

        with Path(self._log_file).open("rb") as f:
            # Read existing lines from start offset
            while True:
                line_start_offset = f.tell()
                line = f.readline()
                if line:
                    try:
                        entry = _json_loads(line)
                        yield (
                            line_start_offset,
                            validate_entry(entry, line_start_offset),
                        )
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        # A final line without a newline may be an interrupted append;
                        # ignore it so the last complete entry remains recoverable.
                        if not line.endswith(b"\n") and not f.read(1):
                            break
                        raise CorruptLogError(
                            line_start_offset, f"invalid JSON: {exc}"
                        ) from exc
                else:
                    # Reached end of file
                    break

    async def acquire_lease(self) -> bytes:
        async with self._lock:
            token = uuid.uuid4().bytes
            with self._locked_file():
                self._lease_file.write_bytes(token)
            return token

    async def release_lease(self, token: bytes) -> None:
        async with self._lock:
            with self._locked_file():
                if self._read_lease() == token:
                    self._lease_file.unlink(missing_ok=True)

    async def append(self, token: bytes, entry: Entry) -> int:
        async with self._lock:
            with self._locked_file():
                if token != self._read_lease():
                    msg = "Invalid lease token"
                    raise ValueError(msg)

                with self._log_file.open("ab") as log_file:
                    offset = log_file.tell()
                    _ = log_file.write(_json_dumps(entry))
                    _ = log_file.write(b"\n")
                    log_file.flush()
                    return offset

    @contextlib.contextmanager
    def _locked_file(self) -> Generator[None, None, None]:
        with self._lock_file.open("a+b") as lock_file:
            _lock_file(lock_file)
            try:
                yield
            finally:
                _unlock_file(lock_file)

    def _read_lease(self) -> bytes | None:
        try:
            return self._lease_file.read_bytes()
        except FileNotFoundError:
            return None


class MemoryLogStorage:
    """In-memory log storage with last-acquirer-wins fencing leases."""

    __slots__ = ("_entries", "_leases", "_lock")

    _entries: list[BaseEntry]
    _leases: bytes | None
    _lock: asyncio.Lock

    def __init__(self, entries: list[BaseEntry] | None = None) -> None:
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # Yield existing entries
        async with self._lock:
            entries_snapshot = self._entries.copy()

        for index in range(len(entries_snapshot)):
            yield (index, validate_entry(entries_snapshot[index], index))

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
        async with self._lock:
            if token != self._leases:
                msg = "Invalid lease token"
                raise ValueError(msg)

            offset = len(self._entries)
            self._entries.append(cast("BaseEntry", cast("object", entry)))
            return offset

    async def entries(self) -> list[BaseEntry]:
        async with self._lock:
            return self._entries.copy()


class SQLiteLogManager:
    """A log manager that stores multiple task logs in a single SQLite database.

    Uses WAL mode and database-backed leases for multiprocess support.
    Last acquirer wins; leases do not expire automatically.
    """

    __slots__ = ("_db_path", "_lock", "_logs")

    def __init__(self, db_path: str | Path) -> None:
        """Initialize the SQLite log manager.

        Args:
            db_path: Path to the SQLite database file.

        Raises:
            RuntimeError: If the SQLite runtime does not support JSONB.

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
            if sqlite3.sqlite_version_info < (3, 45, 0):
                msg = (
                    "SQLiteLogManager requires SQLite 3.45 or newer for JSONB "
                    f"(found {sqlite3.sqlite_version})"
                )
                raise RuntimeError(msg)

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
    Last acquirer wins and the lease is atomically validated on append.
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
                    "SELECT e.rowid, json(e.data), json(m.metadata) "
                    "FROM log_entries AS e "
                    "LEFT JOIN log_metadata AS m ON m.entry_rowid = e.rowid "
                    "WHERE e.task_id = ? ORDER BY e.rowid",
                    (self._task_id,),
                )
                results: list[tuple[int, BaseEntry]] = []
                for rowid, data_json, metadata_json in cursor:
                    try:
                        entry: object = _json_loads(data_json)
                        if metadata_json is not None:
                            metadata = _json_loads(metadata_json)
                            if not isinstance(entry, dict):
                                raise CorruptLogError(rowid, "entry is not an object")
                            entry_object = cast("dict[str, object]", entry)
                            entry_object["metadata"] = metadata
                            entry = entry_object
                        results.append((rowid, validate_entry(entry, rowid)))
                    except json.JSONDecodeError as exc:  # noqa: PERF203
                        raise CorruptLogError(rowid, f"invalid JSON: {exc}") from exc
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
