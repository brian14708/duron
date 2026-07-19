from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import threading
import uuid
from bisect import bisect_right
from pathlib import Path
from typing import TYPE_CHECKING, cast

from duron._runtime.storage.backend import HistoryRecord, LeaseToken, validate_run_id
from duron.errors import LeaseLostError, StorageError

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

_SCHEMA_LOCK = threading.Lock()


def _validate_stream_id(stream_id: object) -> None:
    if stream_id is not None and (not isinstance(stream_id, str) or not stream_id):
        msg = "stream_id must be a nonempty string or None"
        raise ValueError(msg)


def _validate_read(after: int | None, limit: int, stream_id: str | None) -> None:
    if type(limit) is not int or limit <= 0:
        msg = "limit must be a positive integer"
        raise ValueError(msg)
    if after is not None and type(after) is not int:
        msg = "after must be an integer or None"
        raise ValueError(msg)
    _validate_stream_id(stream_id)


def _validate_records(records: Sequence[HistoryRecord]) -> tuple[HistoryRecord, ...]:
    snapshot = tuple(records)
    for record in snapshot:
        if not isinstance(cast("object", record), HistoryRecord):
            msg = "records must contain HistoryRecord values"
            raise TypeError(msg)
        if not isinstance(cast("object", record.id), str) or not record.id:
            msg = "history record id must be nonempty"
            raise ValueError(msg)
        if not isinstance(cast("object", record.payload), bytes):
            msg = "history record payload must be bytes"
            raise TypeError(msg)
        _validate_stream_id(record.stream_id)
    if len({record.id for record in snapshot}) != len(snapshot):
        msg = "history record ids must be unique within a batch"
        raise StorageError(msg)
    return snapshot


class MemoryStore:
    """Process-local histories with last-acquirer-wins writer fencing.

    Acquiring a lease immediately fences the previous owner, even when that
    owner is still live. State belongs to this instance and is lost at process
    exit.
    """

    def __init__(self) -> None:
        self._logs: dict[str, list[HistoryRecord]] = {}
        self._ids: dict[str, set[str]] = {}
        self._streams: dict[tuple[str, str], list[int]] = {}
        self._leases: dict[str, LeaseToken] = {}
        self._lock = asyncio.Lock()

    async def acquire_lease(self, run_id: str) -> LeaseToken:
        validate_run_id(run_id)
        async with self._lock:
            lease = LeaseToken(uuid.uuid4().bytes)
            self._leases[run_id] = lease
            return lease

    async def release_lease(self, run_id: str, lease: LeaseToken) -> None:
        validate_run_id(run_id)
        async with self._lock:
            if self._leases.get(run_id) == lease:
                self._leases.pop(run_id, None)

    async def append(
        self, run_id: str, lease: LeaseToken, records: Sequence[HistoryRecord]
    ) -> Sequence[int]:
        validate_run_id(run_id)
        snapshot = _validate_records(records)
        async with self._lock:
            if self._leases.get(run_id) != lease:
                msg = "invalid lease token for run"
                raise LeaseLostError(msg)
            ids = self._ids.setdefault(run_id, set())
            if any(record.id in ids for record in snapshot):
                msg = "history record id already exists in run"
                raise StorageError(msg)
            log = self._logs.setdefault(run_id, [])
            start = len(log)
            log.extend(snapshot)
            ids.update(record.id for record in snapshot)
            for offset, record in enumerate(snapshot, start):
                if record.stream_id is not None:
                    self._streams.setdefault((run_id, record.stream_id), []).append(
                        offset
                    )
            return list(range(start, start + len(snapshot)))

    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, HistoryRecord]]:
        validate_run_id(run_id)
        _validate_read(after, limit, stream_id)
        start = 0 if after is None else max(0, after + 1)
        async with self._lock:
            log = self._logs.get(run_id, ())
            if stream_id is not None:
                offsets = self._streams.get((run_id, stream_id), ())
                position = 0 if after is None else bisect_right(offsets, after)
                return tuple(
                    (offset, log[offset])
                    for offset in offsets[position : position + limit]
                )
            page = log[start : start + limit]
        return tuple(enumerate(page, start))


class SQLiteStore:
    """SQLite histories with last-acquirer-wins writer fencing.

    Acquiring a lease immediately fences the previous owner, even when that
    owner is still live. Connections use a five second busy timeout and
    ``synchronous=FULL``. A cancelled offloaded append may have committed.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._initialized = False
        self._schema_lock = threading.Lock()

    def _initialize(self, conn: sqlite3.Connection) -> None:
        with _SCHEMA_LOCK, self._schema_lock:
            if self._initialized:
                return
            columns = conn.execute("PRAGMA table_info(log_entries)").fetchall()
            if columns and (
                {column[1] for column in columns}
                != {"offset", "run_id", "id", "stream_id", "data"}
                or not any(
                    column[1] == "offset" and column[2] == "INTEGER" and column[5] == 1
                    for column in columns
                )
            ):
                msg = "unsupported SQLite history schema; use a new database"
                raise StorageError(msg)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    offset INTEGER PRIMARY KEY,
                    run_id TEXT NOT NULL, id TEXT NOT NULL,
                    stream_id TEXT, data BLOB NOT NULL,
                    UNIQUE (run_id, id)
                );
                CREATE INDEX IF NOT EXISTS idx_log_entries_run_id
                    ON log_entries (run_id);
                CREATE INDEX IF NOT EXISTS idx_log_entries_stream
                    ON log_entries (run_id, stream_id, offset)
                    WHERE stream_id IS NOT NULL;
                CREATE TABLE IF NOT EXISTS leases (
                    run_id TEXT PRIMARY KEY,
                    token BLOB NOT NULL
                );
            """)
            self._initialized = True

    @contextlib.contextmanager
    def _connection(
        self, *, write: bool = False
    ) -> Generator[sqlite3.Connection, None, None]:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self._path, timeout=5.0)
            try:
                conn.execute("PRAGMA synchronous=FULL")
                self._initialize(conn)
                if write:
                    conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    if write:
                        conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
            finally:
                conn.close()
        except sqlite3.IntegrityError as exc:
            msg = "history record id already exists in run"
            raise StorageError(msg) from exc
        except (sqlite3.Error, OSError) as exc:
            msg = f"SQLite store operation failed: {exc}"
            raise StorageError(msg) from exc

    async def acquire_lease(self, run_id: str) -> LeaseToken:
        run_key = validate_run_id(run_id)

        def acquire() -> LeaseToken:
            lease = LeaseToken(uuid.uuid4().bytes)
            with self._connection(write=True) as conn:
                conn.execute(
                    "INSERT INTO leases (run_id, token) VALUES (?, ?) "
                    "ON CONFLICT(run_id) DO UPDATE SET token = excluded.token",
                    (run_key, lease.value),
                )
            return lease

        return await asyncio.to_thread(acquire)

    async def release_lease(self, run_id: str, lease: LeaseToken) -> None:
        run_key = validate_run_id(run_id)

        def release() -> None:
            with self._connection(write=True) as conn:
                conn.execute(
                    "DELETE FROM leases WHERE run_id = ? AND token = ?",
                    (run_key, lease.value),
                )

        await asyncio.to_thread(release)

    async def append(
        self, run_id: str, lease: LeaseToken, records: Sequence[HistoryRecord]
    ) -> Sequence[int]:
        run_key = validate_run_id(run_id)
        snapshot = _validate_records(records)

        def append() -> list[int]:
            offsets: list[int] = []
            with self._connection(write=True) as conn:
                current = conn.execute(
                    "SELECT token FROM leases WHERE run_id = ?", (run_key,)
                ).fetchone()
                if current is None or current[0] != lease.value:
                    msg = "invalid lease token for run"
                    raise LeaseLostError(msg)
                for record in snapshot:
                    cursor = conn.execute(
                        "INSERT INTO log_entries (run_id, id, stream_id, data) "
                        "VALUES (?, ?, ?, ?)",
                        (run_key, record.id, record.stream_id, record.payload),
                    )
                    assert cursor.lastrowid is not None
                    offsets.append(cursor.lastrowid)
            return offsets

        return await asyncio.to_thread(append)

    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, HistoryRecord]]:
        run_key = validate_run_id(run_id)
        _validate_read(after, limit, stream_id)

        def read() -> list[tuple[int, HistoryRecord]]:
            with self._connection() as conn:
                query = (
                    "SELECT offset, id, data, stream_id FROM log_entries "
                    "WHERE run_id = ? "
                )
                params: list[str | int] = [run_key]
                if stream_id is not None:
                    query += "AND stream_id = ? "
                    params.append(stream_id)
                if after is not None:
                    query += "AND offset > ? "
                    params.append(after)
                params.append(limit)
                rows = conn.execute(
                    query + "ORDER BY offset LIMIT ?", params
                ).fetchall()
                return [
                    (offset, HistoryRecord(record_id, bytes(payload), record_stream))
                    for offset, record_id, payload, record_stream in rows
                ]

        return await asyncio.to_thread(read)
