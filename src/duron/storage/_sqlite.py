from __future__ import annotations

import asyncio
import contextlib
import json
import math
import sqlite3
import threading
import uuid
import weakref
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Self, cast

from duron.errors import ClaimBusyError, LeaseLostError, PostConflictError, StorageError
from duron.storage._protocol import Event, LeaseToken, validate_run_id

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence
    from types import TracebackType

_SCHEMA_LOCK = threading.Lock()


def _validate_ttl(ttl: float) -> None:
    if not math.isfinite(ttl) or not 0 < ttl <= 300:
        msg = "claim ttl must be finite and between 0 and 300 seconds"
        raise ValueError(msg)


def _batch_payload(records: Sequence[Event]) -> bytes:
    return sha256(
        json.dumps(
            [[record.id, record.stream_id, record.payload.hex()] for record in records],
            separators=(",", ":"),
        ).encode()
    ).digest()


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


def _validate_records(records: Sequence[Event]) -> tuple[Event, ...]:
    snapshot = tuple(records)
    for record in snapshot:
        if not isinstance(cast("object", record), Event):
            msg = "records must contain Event values"
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


class SQLiteStore:
    """SQLite histories with exclusive, expiring ownership.

    A retained connection uses a five second busy timeout
    and WAL with ``synchronous=NORMAL``. A power loss may lose the most recent
    committed transactions. A cancelled offloaded write may have committed.
    Use ``async with SQLiteStore(path)`` or ``await store.close()`` to release
    the connection promptly.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._connection_lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._changed = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._finalizer: weakref.finalize[[], SQLiteStore] | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Release the connection after outstanding operations finish.

        The next storage operation opens a new connection. Closing a store does
        not release its run leases.
        """
        await asyncio.to_thread(self._close)

    async def wait_for_change(self) -> None:
        """Wait for a local commit, periodically checking other processes."""
        self._loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout(0.1):
                await self._changed.wait()
        except TimeoutError:
            pass
        self._changed.clear()

    def _close(self) -> None:
        with self._connection_lock:
            if self._finalizer is not None:
                self._finalizer()
                self._finalizer = None
                self._conn = None

    async def create(self, run_id: str, header: Event) -> bool:
        validate_run_id(run_id)
        _validate_records((header,))

        def create() -> bool:
            with self._connection(write=True) as conn:
                if conn.execute(
                    "SELECT 1 FROM log_entries WHERE run_id = ? LIMIT 1", (run_id,)
                ).fetchone():
                    return False
                conn.execute(
                    "INSERT INTO log_entries (run_id, id, stream_id, data) "
                    "VALUES (?, ?, ?, ?)",
                    (run_id, header.id, header.stream_id, header.payload),
                )
                return True

        return await asyncio.to_thread(create)

    @staticmethod
    def _initialize(conn: sqlite3.Connection) -> None:
        with _SCHEMA_LOCK:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version == 20261019:
                conn.execute("PRAGMA journal_mode=WAL")
                return
            if (
                version != 0
                or conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone()
            ):
                msg = "unsupported SQLite schema version; use a new database"
                raise StorageError(msg)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS log_entries (
                    offset INTEGER PRIMARY KEY,
                    run_id TEXT NOT NULL, id TEXT NOT NULL,
                    stream_id TEXT, data BLOB NOT NULL,
                    namespace TEXT GENERATED ALWAYS AS (
                        CASE WHEN json_valid(run_id) THEN
                            CASE WHEN json_extract(run_id, '$[0]') = 'run'
                            THEN json_extract(run_id, '$[1]') END END
                    ) STORED,
                    job_id TEXT GENERATED ALWAYS AS (
                        CASE WHEN json_valid(run_id) THEN
                            CASE WHEN json_extract(run_id, '$[0]') = 'run'
                            THEN json_extract(run_id, '$[2]') END END
                    ) STORED,
                    event_type TEXT GENERATED ALWAYS AS (
                        CASE WHEN json_valid(CAST(data AS TEXT))
                        THEN json_extract(CAST(data AS TEXT), '$.type') END
                    ) STORED,
                    event_source TEXT GENERATED ALWAYS AS (
                        CASE WHEN json_valid(CAST(data AS TEXT))
                        THEN json_extract(CAST(data AS TEXT), '$.source') END
                    ) STORED,
                    event_ts INTEGER GENERATED ALWAYS AS (
                        CASE WHEN json_valid(CAST(data AS TEXT))
                        THEN json_extract(CAST(data AS TEXT), '$.ts') END
                    ) STORED,
                    event_key TEXT GENERATED ALWAYS AS (
                        CASE WHEN json_valid(CAST(data AS TEXT)) THEN CASE
                            json_extract(CAST(data AS TEXT), '$.type')
                            WHEN 'stream.create' THEN
                                json_extract(CAST(data AS TEXT), '$.name')
                            WHEN 'promise.complete' THEN
                                json_extract(CAST(data AS TEXT), '$.promise_id')
                            WHEN 'input.accept' THEN json_array(
                                json_extract(CAST(data AS TEXT), '$.port'),
                                json_extract(CAST(data AS TEXT), '$.retry_id'))
                            END END
                    ) STORED,
                    UNIQUE (run_id, id)
                );
                CREATE INDEX IF NOT EXISTS idx_history_event
                    ON log_entries (run_id, event_type, event_key, offset);
                CREATE INDEX IF NOT EXISTS idx_history_event_type
                    ON log_entries (run_id, event_type, offset);
                CREATE INDEX IF NOT EXISTS idx_history_job
                    ON log_entries (namespace, job_id, offset);
                CREATE INDEX IF NOT EXISTS idx_log_entries_run_id
                    ON log_entries (run_id);
                CREATE INDEX IF NOT EXISTS idx_log_entries_stream
                    ON log_entries (run_id, stream_id, offset)
                    WHERE stream_id IS NOT NULL;
                CREATE TABLE IF NOT EXISTS leases (
                    run_id TEXT PRIMARY KEY,
                    token BLOB NOT NULL,
                    expires REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS claim_requests (
                    run_id TEXT NOT NULL, request_id TEXT NOT NULL,
                    token BLOB NOT NULL,
                    PRIMARY KEY (run_id, request_id)
                );
                CREATE TABLE IF NOT EXISTS append_batches (
                    run_id TEXT NOT NULL, batch_id TEXT NOT NULL,
                    payload BLOB NOT NULL, offsets TEXT NOT NULL,
                    PRIMARY KEY (run_id, batch_id)
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    namespace TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    history_id TEXT NOT NULL UNIQUE,
                    workflow_name TEXT NOT NULL,
                    workflow_version TEXT NOT NULL,
                    contract TEXT NOT NULL CHECK(
                        json_valid(contract) AND json_type(contract) = 'object'
                    ),
                    inputs TEXT NOT NULL CHECK(
                        json_valid(inputs) AND json_type(inputs) = 'object'
                    ),
                    seed TEXT NOT NULL CHECK(length(seed) > 0),
                    state TEXT NOT NULL CHECK(state IN (
                        'queued', 'running', 'suspended', 'blocked',
                        'succeeded', 'failed', 'cancelled')),
                    cancelled INTEGER NOT NULL DEFAULT 0 CHECK(cancelled IN (0, 1)),
                    error TEXT,
                    retry_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (namespace, run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_dispatch
                    ON jobs (namespace, retry_at, history_id)
                    WHERE state NOT IN ('succeeded', 'failed', 'cancelled');
                CREATE INDEX IF NOT EXISTS idx_jobs_workflow
                    ON jobs (namespace, workflow_name, workflow_version, state);
                CREATE TABLE IF NOT EXISTS deliveries (
                    sequence INTEGER PRIMARY KEY,
                    id TEXT NOT NULL UNIQUE,
                    namespace TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK(kind IN ('input', 'close')),
                    port TEXT NOT NULL,
                    payload TEXT NOT NULL CHECK(json_valid(payload)),
                    retry_id TEXT,
                    state TEXT NOT NULL DEFAULT 'pending'
                        CHECK(state IN ('pending', 'accepted', 'rejected')),
                    error_type TEXT,
                    error_message TEXT,
                    created_at REAL NOT NULL,
                    acknowledged_at REAL,
                    FOREIGN KEY (namespace, run_id) REFERENCES jobs(namespace, run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_deliveries_pending
                    ON deliveries (namespace, run_id, sequence) WHERE state = 'pending';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_retry
                    ON deliveries (namespace, run_id, kind, port, retry_id)
                    WHERE retry_id IS NOT NULL;
                PRAGMA user_version=20261019;
                COMMIT;
            """)

    @contextlib.contextmanager
    def _connection(
        self, *, write: bool = False
    ) -> Generator[sqlite3.Connection, None, None]:
        try:
            # Offloaded calls may use different threads. Hold this lock for the
            # whole transaction so a shared connection never interleaves calls.
            with self._connection_lock:
                conn = self._conn
                if conn is None:
                    self._path.parent.mkdir(parents=True, exist_ok=True)
                    conn = sqlite3.connect(
                        self._path, timeout=5.0, check_same_thread=False
                    )
                    try:
                        conn.execute("PRAGMA synchronous=NORMAL")
                        conn.execute("PRAGMA foreign_keys=ON")
                        self._initialize(conn)
                    except BaseException:
                        conn.close()
                        raise
                    self._conn = conn
                    self._finalizer = weakref.finalize(self, conn.close)
                if write:
                    conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    if write:
                        conn.commit()
                        if self._loop is not None and not self._loop.is_closed():
                            self._loop.call_soon_threadsafe(self._changed.set)
                except BaseException:
                    conn.rollback()
                    raise
        except sqlite3.IntegrityError as exc:
            msg = "history record id already exists in run"
            raise StorageError(msg) from exc
        except (sqlite3.Error, OSError) as exc:
            msg = f"SQLite store operation failed: {exc}"
            raise StorageError(msg) from exc

    @staticmethod
    def _now(conn: sqlite3.Connection) -> float:
        return cast(
            "float",
            conn.execute("SELECT (julianday('now') - 2440587.5) * 86400.0").fetchone()[
                0
            ],
        )

    async def claim(
        self, run_id: str, request_id: str, *, ttl: float = 30.0
    ) -> LeaseToken:
        run_key = validate_run_id(run_id)
        validate_run_id(request_id)
        _validate_ttl(ttl)

        def acquire() -> LeaseToken:
            lease = LeaseToken(uuid.uuid4().bytes)
            with self._connection(write=True) as conn:
                now = self._now(conn)
                current = conn.execute(
                    "SELECT token, expires FROM leases WHERE run_id = ?", (run_key,)
                ).fetchone()
                previous = conn.execute(
                    "SELECT token FROM claim_requests "
                    "WHERE run_id = ? AND request_id = ?",
                    (run_key, request_id),
                ).fetchone()
                if previous is not None:
                    if (
                        current is None
                        or current[0] != previous[0]
                        or current[1] <= now
                    ):
                        msg = "claim request no longer owns this history"
                        raise LeaseLostError(msg)
                    return LeaseToken(bytes(previous[0]))
                if current is not None and current[1] > now:
                    msg = "history already has an active owner"
                    raise ClaimBusyError(msg)
                conn.execute(
                    "INSERT INTO leases (run_id, token, expires) VALUES (?, ?, ?) "
                    "ON CONFLICT(run_id) DO UPDATE SET token = excluded.token, "
                    "expires = excluded.expires",
                    (run_key, lease.value, now + ttl),
                )
                conn.execute(
                    "INSERT INTO claim_requests (run_id, request_id, token) "
                    "VALUES (?, ?, ?)",
                    (run_key, request_id, lease.value),
                )
            return lease

        return await asyncio.to_thread(acquire)

    async def renew(self, run_id: str, lease: LeaseToken, *, ttl: float = 30.0) -> None:
        run_key = validate_run_id(run_id)
        _validate_ttl(ttl)

        def renew() -> None:
            with self._connection(write=True) as conn:
                now = self._now(conn)
                updated = conn.execute(
                    "UPDATE leases SET expires = ? "
                    "WHERE run_id = ? AND token = ? AND expires > ?",
                    (now + ttl, run_key, lease.value, now),
                )
                if not updated.rowcount:
                    msg = "invalid or expired history claim"
                    raise LeaseLostError(msg)

        await asyncio.to_thread(renew)

    async def release(self, run_id: str, lease: LeaseToken) -> None:
        run_key = validate_run_id(run_id)

        def release() -> None:
            with self._connection(write=True) as conn:
                conn.execute(
                    "DELETE FROM leases WHERE run_id = ? AND token = ?",
                    (run_key, lease.value),
                )

        await asyncio.to_thread(release)

    async def append(
        self, run_id: str, lease: LeaseToken, records: Sequence[Event], *, batch_id: str
    ) -> Sequence[int]:
        run_key = validate_run_id(run_id)
        validate_run_id(batch_id)
        snapshot = _validate_records(records)
        payload = _batch_payload(snapshot)

        def append() -> list[int]:
            offsets: list[int] = []
            with self._connection(write=True) as conn:
                current = conn.execute(
                    "SELECT token, expires FROM leases WHERE run_id = ?", (run_key,)
                ).fetchone()
                if (
                    current is None
                    or current[0] != lease.value
                    or current[1] <= self._now(conn)
                ):
                    msg = "invalid lease token for run"
                    raise LeaseLostError(msg)
                previous = conn.execute(
                    "SELECT payload, offsets FROM append_batches "
                    "WHERE run_id = ? AND batch_id = ?",
                    (run_key, batch_id),
                ).fetchone()
                if previous is not None:
                    if previous[0] != payload:
                        msg = "append batch ID was reused with different records"
                        raise PostConflictError(msg)
                    return cast("list[int]", json.loads(previous[1]))
                for record in snapshot:
                    cursor = conn.execute(
                        "INSERT INTO log_entries (run_id, id, stream_id, data) "
                        "VALUES (?, ?, ?, ?)",
                        (run_key, record.id, record.stream_id, record.payload),
                    )
                    assert cursor.lastrowid is not None
                    offsets.append(cursor.lastrowid)
                conn.execute(
                    "UPDATE jobs SET state = CASE "
                    "WHEN json_extract(CAST(h.data AS TEXT), "
                    "'$.cancelled') THEN 'cancelled' "
                    "WHEN json_type(CAST(h.data AS TEXT), "
                    "'$.error') IS NOT NULL THEN 'failed' "
                    "ELSE 'succeeded' END, error = NULL, retry_at "
                    "= NULL, updated_at = ? "
                    "FROM log_entries AS h WHERE jobs.history_id = ? "
                    "AND h.run_id = jobs.history_id AND h.event_type = 'run.finish'",
                    (self._now(conn), run_key),
                )
                conn.execute(
                    "INSERT INTO append_batches (run_id, batch_id, payload, offsets) "
                    "VALUES (?, ?, ?, ?)",
                    (run_key, batch_id, payload, json.dumps(offsets)),
                )
            return offsets

        return await asyncio.to_thread(append)

    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, Event]]:
        run_key = validate_run_id(run_id)
        _validate_read(after, limit, stream_id)

        def read() -> list[tuple[int, Event]]:
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
                    (offset, Event(record_id, bytes(payload), record_stream))
                    for offset, record_id, payload, record_stream in rows
                ]

        return await asyncio.to_thread(read)
