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
    from collections.abc import AsyncGenerator, Callable, Generator
    from io import IOBase

    from duron.log import BaseEntry, Entry


try:
    import fcntl

    _file_lock_supported = True

    def _lock_file(f: IOBase, /) -> None:
        if f.writable():
            # Blocking: the lock spans only a short read/write, and the
            # last-acquirer-wins lease contract requires contenders to wait
            # for it rather than fail with BlockingIOError (LOCK_NB) while
            # holding a perfectly valid lease token.
            fcntl.flock(f, fcntl.LOCK_EX)

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


# Entries read per executor round trip by stream(); bounds memory use and lets
# early-exit consumers (header reads, emptiness probes) stop at the head. Large
# enough that a full-log read costs one round trip per chunk rather than one per
# handful of entries — each round trip reopens the backing file or sqlite handle.
_STREAM_CHUNK = 4096


async def _chunked_stream(
    read_chunk: Callable[[int | None], list[tuple[int, BaseEntry]]], offset: int | None
) -> AsyncGenerator[tuple[int, BaseEntry], None]:
    """Yield log entries in bounded chunks read off the event loop.

    The chunking contract — the ``_STREAM_CHUNK`` short-read termination rule,
    the offset-is-last-yielded-value cursor convention, and the ``run_in_executor``
    offload — lives here once so every backend resumes identically.

    Args:
        read_chunk: Reads at most ``_STREAM_CHUNK`` entries strictly after
            ``offset``, returning their ``(offset, entry)`` pairs.
        offset: The last offset the caller consumed, or ``None`` to start.

    Yields:
        Each ``(offset, entry)`` pair in log order.

    """
    loop = asyncio.get_running_loop()
    cursor = offset
    while True:
        chunk = await loop.run_in_executor(None, read_chunk, cursor)
        if not chunk:
            return
        for line_offset, entry in chunk:
            cursor = line_offset
            yield (line_offset, entry)
        if len(chunk) < _STREAM_CHUNK:
            return


def _truncate_torn_tail(f: IOBase) -> int:
    """Truncate any newline-less final line, returning the append offset.

    Returns:
        The byte offset at which the next entry will start.

    """
    _ = f.seek(0, 2)
    end = f.tell()
    if end == 0:
        return 0
    _ = f.seek(end - 1)
    if f.read(1) == b"\n":
        return end
    # Scan backward in chunks for the last newline; everything after it is an
    # uncommitted remnant of an interrupted append.
    pos = end
    chunk = 4096
    new_end = 0
    while pos > 0:
        start = max(0, pos - chunk)
        _ = f.seek(start)
        data = f.read(pos - start)
        idx = data.rfind(b"\n")
        if idx != -1:
            new_end = start + idx + 1
            break
        pos = start
    _ = f.truncate(new_end)
    return new_end


class FileStorage:
    """File-backed log storage using a sidecar fencing token and an OS data-file lock.

    Acquiring a lease immediately supersedes the previous token, including tokens
    issued by another `FileStorage` instance for the same path. This backend
    requires `fcntl` and therefore fails during initialization on unsupported
    platforms.
    """

    __slots__ = ("_lease_file", "_lock", "_log_file")

    def __init__(self, log_file: str | Path) -> None:
        if not _file_lock_supported:
            msg = "FileStorage requires a platform with fcntl file locking"
            raise RuntimeError(msg)
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._lease_file = self._log_file.with_suffix(self._log_file.suffix + ".lease")
        self._lock = asyncio.Lock()

    async def stream(
        self, *, offset: int | None = None
    ) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # Read in bounded chunks in an executor so the file read neither blocks
        # the host loop nor keeps a descriptor open across yields (an abandoned
        # async generator may otherwise skip closing it).
        async for entry in _chunked_stream(self._read_chunk, offset):
            yield entry

    def _read_chunk(self, offset: int | None) -> list[tuple[int, BaseEntry]]:
        if not self._log_file.exists():
            return []

        entries: list[tuple[int, BaseEntry]] = []
        with Path(self._log_file).open("rb") as f:
            if offset is not None:
                # Offsets are byte positions of line starts; seek to the last
                # seen entry and discard it so only entries after it are read.
                f.seek(offset)
                _ = f.readline()
            while len(entries) < _STREAM_CHUNK:
                line_start_offset = f.tell()
                line = f.readline()
                if not line:
                    # Reached end of file
                    break
                try:
                    entry = _json_loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    # A final line without a newline may be an interrupted append;
                    # ignore it so the last complete entry remains recoverable.
                    if not line.endswith(b"\n") and not f.read(1):
                        break
                    raise CorruptLogError(
                        line_start_offset, f"invalid JSON: {exc}"
                    ) from exc
                entries.append((
                    line_start_offset,
                    validate_entry(entry, line_start_offset),
                ))
        return entries

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
            with self._locked_file() as log_file:
                if token != self._read_lease():
                    msg = "Invalid lease token"
                    raise ValueError(msg)

                # A torn final line from an interrupted append is tolerated by
                # readers, but writing after it would fuse the remnant and the
                # new entry into one corrupt interior line that permanently
                # blocks resume. Drop the (uncommitted) remnant first.
                offset = _truncate_torn_tail(log_file)
                _ = log_file.write(_json_dumps(entry) + b"\n")
                log_file.flush()
                return offset

    @contextlib.contextmanager
    def _locked_file(self) -> Generator[IOBase, None, None]:
        with self._log_file.open("a+b") as log_file:
            _lock_file(log_file)
            try:
                yield log_file
            finally:
                _unlock_file(log_file)

    def _read_lease(self) -> bytes | None:
        try:
            return self._lease_file.read_bytes()
        except FileNotFoundError:
            return None


class MemoryStorage:
    """In-memory log storage with last-acquirer-wins fencing leases."""

    __slots__ = ("_entries", "_leases", "_lock")

    _entries: list[BaseEntry]
    _leases: bytes | None
    _lock: asyncio.Lock

    def __init__(self, entries: list[BaseEntry] | None = None) -> None:
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()

    async def stream(
        self, *, offset: int | None = None
    ) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # Offsets are list indices; slice the tail so resuming is O(new).
        start = 0 if offset is None else offset + 1
        async with self._lock:
            snapshot = self._entries[start:]

        for index, raw in enumerate(snapshot, start):
            yield (index, validate_entry(raw, index))

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


def ensure_schema(db_path: Path) -> None:
    """Create the database schema and enable WAL mode, if not already present.

    Raises:
        RuntimeError: If the SQLite runtime does not support JSONB.

    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
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

        """
        self._db_path = Path(db_path)
        ensure_schema(self._db_path)
        self._logs: dict[str, SQLiteLog] = {}
        self._lock = asyncio.Lock()

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
    """A [log storage][duron.log.Storage] for a single task in a SQLite database.

    Implements multiprocess-safe lease mechanism using database-backed leases.
    Last acquirer wins and the lease is atomically validated on append.
    """

    __slots__ = ("_db_path", "_lock", "_task_id")

    def __init__(self, db_path: Path, task_id: str) -> None:
        self._db_path = db_path
        self._task_id = task_id
        self._lock = asyncio.Lock()

    async def stream(
        self, *, offset: int | None = None
    ) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        async for entry in _chunked_stream(self._read_chunk, offset):
            yield entry

    def _read_chunk(self, after: int | None) -> list[tuple[int, BaseEntry]]:
        conn = sqlite3.connect(self._db_path)
        try:
            # Read from log_entries, convert JSONB data to JSON text
            # Data contains full entry except metadata. Resume from `after`
            # via the indexed rowid range rather than rescanning.
            base_sql = (
                "SELECT e.rowid, json(e.data), json(m.metadata) "
                "FROM log_entries AS e "
                "LEFT JOIN log_metadata AS m ON m.entry_rowid = e.rowid "
                "WHERE e.task_id = ?"
            )
            if after is None:
                cursor = conn.execute(
                    base_sql + " ORDER BY e.rowid LIMIT ?",
                    (self._task_id, _STREAM_CHUNK),
                )
            else:
                cursor = conn.execute(
                    base_sql + " AND e.rowid > ? ORDER BY e.rowid LIMIT ?",
                    (self._task_id, after, _STREAM_CHUNK),
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


_SINGLE_RUN_ID = "__duron_run__"


class SQLiteStorage(SQLiteLog):
    """Single-run SQLite-backed storage."""

    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        ensure_schema(path)
        super().__init__(path, _SINGLE_RUN_ID)
