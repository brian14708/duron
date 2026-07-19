from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import threading
import time
import uuid
import weakref
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Self, TypeVar, cast
from typing_extensions import override

from duron._tasks import finish_task
from duron.errors import ClaimBusyError, ClaimLostError, PostConflictError, StorageError
from duron.storage._jobs import SQLiteJobs
from duron.storage._protocol import ClaimToken, Event, validate_id

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator, Sequence
    from types import TracebackType

    from duron.storage._protocol import TerminalState

_R = TypeVar("_R")


def _sql_payload(payload: bytes) -> str | bytes:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload


def _event_row(row: tuple[int, str, str | bytes, str | None]) -> tuple[int, Event]:
    offset, id_, payload, stream_id = row
    return offset, Event(
        id_, payload.encode("utf-8") if isinstance(payload, str) else payload, stream_id
    )


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


def _close_connections(
    connection: sqlite3.Connection, ownership: sqlite3.Connection | None
) -> None:
    try:
        connection.close()
    finally:
        if ownership is not None:
            ownership.close()


class SQLiteStore(SQLiteJobs):
    """SQLite histories owned by one store at a time.

    A retained connection uses a five second busy timeout
    and WAL with ``synchronous=NORMAL``. A power loss may lose the most recent
    committed transactions. A cancelled offloaded write may have committed.
    Use ``async with SQLiteStore(path)`` or ``await store.close()`` to release
    the connection promptly.
    """

    def __init__(self, path: str | Path, *, namespace: str = "default") -> None:
        self._path = Path(path) if str(path) == ":memory:" else Path(path).resolve()
        self._namespace = validate_id(namespace, "namespace")
        self._connection_lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._changes: weakref.WeakValueDictionary[str | None, asyncio.Event] = (
            weakref.WeakValueDictionary()
        )
        self._operations = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._claims: dict[str, ClaimToken] = {}
        self._entered = False
        self._finalizer: weakref.finalize[[], SQLiteStore] | None = None

    async def __aenter__(self) -> Self:
        if self._entered:
            msg = "SQLiteStore cannot be entered more than once"
            raise RuntimeError(msg)
        self._entered = True
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.close()
        self._entered = False

    async def close(self) -> None:
        """Release the connection after outstanding operations finish.

        The next storage operation opens a new connection. Closing a store
        releases all run claims and database ownership.
        """
        await self._offload(self._close)

    def watch(self, run_id: str | None = None) -> Callable[[], Awaitable[None]]:
        """Capture a commit notification before reading, optionally for one run.

        Returns:
            A function to await when the read finds nothing ready.

        """
        self._loop = asyncio.get_running_loop()
        changed = self._changes.get(run_id)
        if changed is None:
            changed = asyncio.Event()
            self._changes[run_id] = changed

        async def wait() -> None:
            await changed.wait()

        return wait

    def _notify(self, run_id: str | None) -> None:
        for key in {None, run_id}:
            if changed := self._changes.pop(key, None):
                changed.set()

    def _close(self) -> None:
        with self._connection_lock:
            if self._finalizer is not None:
                self._finalizer()
                self._finalizer = None
                self._conn = None
                self._claims.clear()

    async def create(self, run_id: str, header: Event) -> bool:
        validate_id(run_id, "run_id")
        _validate_records((header,))

        def create(conn: sqlite3.Connection) -> bool:
            if conn.execute(
                "SELECT 1 FROM log_entries WHERE namespace = ? AND run_id = ? LIMIT 1",
                (self._namespace, run_id),
            ).fetchone():
                return False
            conn.execute(
                "INSERT INTO log_entries (namespace, run_id, id, stream_id, data) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    self._namespace,
                    run_id,
                    header.id,
                    header.stream_id,
                    _sql_payload(header.payload),
                ),
            )
            return True

        return await self._run(create, write=True, run_id=run_id)

    @staticmethod
    def _initialize(conn: sqlite3.Connection) -> None:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version == 2026102003:
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
                namespace TEXT NOT NULL, run_id TEXT NOT NULL, id TEXT NOT NULL,
                stream_id TEXT, data TEXT NOT NULL,
                event_type TEXT GENERATED ALWAYS AS (
                    CASE WHEN json_valid(CAST(data AS TEXT))
                    THEN json_extract(CAST(data AS TEXT), '$.type') END
                ) STORED,
                event_key TEXT GENERATED ALWAYS AS (
                    CASE WHEN json_valid(CAST(data AS TEXT)) THEN CASE
                        json_extract(CAST(data AS TEXT), '$.type')
                        WHEN 'stream.create' THEN
                            json_extract(CAST(data AS TEXT), '$.name')
                        WHEN 'promise.complete' THEN
                            json_extract(CAST(data AS TEXT), '$.promise_id')
                        WHEN 'input.accept' THEN
                            json_extract(CAST(data AS TEXT), '$.port')
                        END END
                ) STORED,
                UNIQUE (namespace, run_id, id)
            );
            CREATE INDEX IF NOT EXISTS idx_history_event
                ON log_entries (
                    namespace, run_id, event_type, event_key, offset
                );
            CREATE INDEX IF NOT EXISTS idx_history_event_type
                ON log_entries (namespace, run_id, event_type, offset);
            CREATE INDEX IF NOT EXISTS idx_log_entries_run
                ON log_entries (namespace, run_id, offset);
            CREATE INDEX IF NOT EXISTS idx_log_entries_stream
                ON log_entries (namespace, run_id, stream_id, offset)
                WHERE stream_id IS NOT NULL;
            CREATE TABLE IF NOT EXISTS jobs (
                namespace TEXT NOT NULL,
                run_id TEXT NOT NULL,
                contract TEXT NOT NULL CHECK(
                    json_valid(contract) AND json_type(contract) = 'object'
                ),
                workflow_name TEXT GENERATED ALWAYS AS (
                    json_extract(contract, '$.name')
                ) VIRTUAL NOT NULL,
                inputs TEXT NOT NULL CHECK(
                    json_valid(inputs) AND json_type(inputs) = 'object'
                ),
                seed TEXT NOT NULL CHECK(length(seed) > 0),
                state TEXT NOT NULL CHECK(state IN (
                    'queued', 'running', 'suspended', 'blocked',
                    'succeeded', 'failed', 'cancelled')),
                cancelled INTEGER NOT NULL DEFAULT 0 CHECK(cancelled IN (0, 1)),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                error TEXT,
                error_kind TEXT,
                retry_at REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (namespace, run_id)
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_dispatch
                ON jobs (namespace, retry_at, run_id)
                WHERE state NOT IN ('succeeded', 'failed', 'cancelled');
            CREATE TABLE IF NOT EXISTS inbox (
                sequence INTEGER PRIMARY KEY,
                id TEXT NOT NULL,
                namespace TEXT NOT NULL,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('input', 'close')),
                port TEXT NOT NULL,
                payload TEXT NOT NULL CHECK(json_valid(payload)),
                UNIQUE (namespace, run_id, id),
                FOREIGN KEY (namespace, run_id) REFERENCES jobs(namespace, run_id)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_inbox_run
                ON inbox (namespace, run_id, sequence);
            CREATE TRIGGER IF NOT EXISTS clear_terminal_inbox
                AFTER UPDATE OF state ON jobs
                WHEN NEW.state IN ('succeeded', 'failed', 'cancelled')
            BEGIN
                DELETE FROM inbox
                WHERE namespace = NEW.namespace AND run_id = NEW.run_id;
            END;
            PRAGMA user_version=2026102003;
            COMMIT;
        """)

    @contextlib.contextmanager
    def _connection(
        self, *, write: bool = False, run_id: str | None = None
    ) -> Generator[sqlite3.Connection, None, None]:
        try:  # ruff: ignore[too-many-nested-blocks]
            # Offloaded calls may use different threads. Hold this lock for the
            # whole transaction so a shared connection never interleaves calls.
            with self._connection_lock:
                conn = self._conn
                if conn is None:
                    self._path.parent.mkdir(parents=True, exist_ok=True)
                    ownership = None
                    if str(self._path) != ":memory:":
                        ownership = sqlite3.connect(
                            str(self._path) + ".lock",
                            timeout=0,
                            check_same_thread=False,
                        )
                        try:
                            ownership.execute("BEGIN EXCLUSIVE")
                        except sqlite3.Error as error:
                            ownership.close()
                            msg = "SQLite database is already owned by another store"
                            raise StorageError(msg) from error
                    try:
                        conn = sqlite3.connect(
                            self._path, timeout=5.0, check_same_thread=False
                        )
                        conn.execute("PRAGMA synchronous=NORMAL")
                        conn.execute("PRAGMA foreign_keys=ON")
                        self._initialize(conn)
                    except BaseException:
                        if conn is not None:
                            conn.close()
                        if ownership is not None:
                            ownership.close()
                        raise
                    self._conn = conn
                    self._finalizer = weakref.finalize(
                        self, partial(_close_connections, conn, ownership)
                    )
                if write:
                    conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    if write:
                        conn.commit()
                        if self._loop is not None and not self._loop.is_closed():
                            with contextlib.suppress(RuntimeError):
                                self._loop.call_soon_threadsafe(self._notify, run_id)
                except BaseException:
                    conn.rollback()
                    raise
        except sqlite3.IntegrityError as exc:
            msg = str(exc)
            if "log_entries.namespace, log_entries.run_id, log_entries.id" in msg:
                msg = "history record id already exists in run"
            raise StorageError(msg) from exc
        except (sqlite3.Error, OSError) as exc:
            msg = f"SQLite store operation failed: {exc}"
            raise StorageError(msg) from exc

    @override
    async def _run(
        self,
        operation: Callable[[sqlite3.Connection], _R],
        *,
        write: bool = False,
        run_id: str | None = None,
    ) -> _R:
        def execute() -> _R:
            with self._connection(write=write, run_id=run_id) as connection:
                return operation(connection)

        return await self._offload(execute)

    async def _offload(self, operation: Callable[[], _R]) -> _R:
        # Serialize before entering the thread pool; SQLite has one connection.
        # Keep the slot until an offloaded operation has actually finished.
        async with self._operations:
            pending = asyncio.create_task(asyncio.to_thread(operation))
            try:
                return await asyncio.shield(pending)
            except asyncio.CancelledError:
                await finish_task(pending)
                raise

    async def claim(self, run_id: str) -> ClaimToken:
        run_key = validate_id(run_id, "run_id")

        def acquire(_conn: sqlite3.Connection) -> ClaimToken:
            if run_key in self._claims:
                msg = "history already has an active owner"
                raise ClaimBusyError(msg)
            token = ClaimToken(uuid.uuid4().bytes)
            self._claims[run_key] = token
            return token

        return await self._run(acquire)

    async def release(self, run_id: str, token: ClaimToken) -> None:
        run_key = validate_id(run_id, "run_id")

        def release() -> None:
            with self._connection_lock:
                if self._claims.get(run_key) == token:
                    del self._claims[run_key]

        await self._offload(release)

    def _append(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        token: ClaimToken,
        records: Sequence[Event],
    ) -> list[int]:
        if self._claims.get(run_id) != token:
            msg = "invalid claim token for run"
            raise ClaimLostError(msg)
        existing = conn.execute(
            "SELECT id, offset FROM log_entries WHERE namespace = ? "
            "AND run_id = ? AND id IN (SELECT value FROM json_each(?))",
            (self._namespace, run_id, json.dumps([record.id for record in records])),
        ).fetchall()
        if existing:
            if len(existing) != len(records):
                msg = "append retry contains only some committed event IDs"
                raise PostConflictError(msg)
            offsets_by_id = {row[0]: row[1] for row in existing}
            return [offsets_by_id[record.id] for record in records]
        offsets: list[int] = []
        for record in records:
            cursor = conn.execute(
                "INSERT INTO log_entries "
                "(namespace, run_id, id, stream_id, data) VALUES (?, ?, ?, ?, ?)",
                (
                    self._namespace,
                    run_id,
                    record.id,
                    record.stream_id,
                    _sql_payload(record.payload),
                ),
            )
            assert cursor.lastrowid is not None
            offsets.append(cursor.lastrowid)
        return offsets

    async def append(
        self, run_id: str, token: ClaimToken, records: Sequence[Event]
    ) -> Sequence[int]:
        run_key = validate_id(run_id, "run_id")
        snapshot = _validate_records(records)
        return await self._run(
            lambda conn: self._append(conn, run_key, token, snapshot),
            write=True,
            run_id=run_key,
        )

    async def finish(
        self,
        run_id: str,
        token: ClaimToken,
        records: Sequence[Event],
        state: TerminalState,
    ) -> Sequence[int]:
        run_key = validate_id(run_id, "run_id")
        snapshot = _validate_records(records)
        if state not in {"succeeded", "failed", "cancelled"} or not snapshot:
            msg = "finish requires final events and a terminal job state"
            raise ValueError(msg)

        def finish(conn: sqlite3.Connection) -> list[int]:
            offsets = self._append(conn, run_key, token, snapshot)
            conn.execute(
                "UPDATE jobs SET state = ?, error = NULL, error_kind = NULL, "
                "retry_at = NULL, updated_at = ? WHERE namespace = ? AND run_id = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled')",
                (state, time.time(), self._namespace, run_key),
            )
            return offsets

        return await self._run(finish, write=True, run_id=run_key)

    async def read_event(self, run_id: str, event_id: str) -> tuple[int, Event] | None:
        run_key = validate_id(run_id, "run_id")
        record_key = validate_id(event_id, "event_id")

        def select(conn: sqlite3.Connection) -> tuple[int, Event] | None:
            row = conn.execute(
                "SELECT offset, id, data, stream_id FROM log_entries "
                "WHERE namespace = ? AND run_id = ? AND id = ?",
                (self._namespace, run_key, record_key),
            ).fetchone()
            if row is None:
                return None
            return _event_row(row)

        return await self._run(select)

    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, Event]]:
        run_key = validate_id(run_id, "run_id")
        _validate_read(after, limit, stream_id)

        def read(conn: sqlite3.Connection) -> list[tuple[int, Event]]:
            query = (
                "SELECT offset, id, data, stream_id FROM log_entries "
                "WHERE namespace = ? AND run_id = ? "
            )
            params: list[str | int] = [self._namespace, run_key]
            if stream_id is not None:
                query += "AND stream_id = ? "
                params.append(stream_id)
            if after is not None:
                query += "AND offset > ? "
                params.append(after)
            params.append(limit)
            rows = conn.execute(query + "ORDER BY offset LIMIT ?", params).fetchall()
            return [_event_row(row) for row in rows]

        return await self._run(read)

    async def read_by_type(
        self, run_id: str, event_type: str, event_key: str | None = None
    ) -> Sequence[tuple[int, Event]]:
        """Read indexed history entries without exposing SQLite internals.

        Returns:
            Matching events in offset order.

        """
        query = (
            "SELECT offset,id,data,stream_id FROM log_entries "
            "WHERE namespace=? AND run_id=? AND event_type=?"
        )
        params: list[str | None] = [self._namespace, run_id, event_type]
        if event_key is not None:
            query += " AND event_key=?"
            params.append(event_key)

        def select(conn: sqlite3.Connection) -> list[tuple[int, Event]]:
            rows = conn.execute(query + " ORDER BY offset", params).fetchall()
            return [_event_row(row) for row in rows]

        return await self._run(select)
