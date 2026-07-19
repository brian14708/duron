"""SQLite implementation of the scheduler job and inbox storage contract."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, TypeVar, cast

from duron._execution.models import canonical_json
from duron.errors import (
    FailureKind,
    PostConflictError,
    RunNotFoundError,
    SubmissionConflictError,
)
from duron.storage._protocol import InboxMessage, ScheduledRun

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Sequence

    from duron._history.entries import Entry

_R = TypeVar("_R")


_SELECT_JOB = (
    "SELECT run_id, contract, inputs, state, cancelled, error, error_kind, seed "
)


def _row(row: Sequence[object]) -> ScheduledRun:
    kind = cast("str | None", row[6])
    return ScheduledRun(
        id=cast("str", row[0]),
        contract=cast("str", row[1]).encode(),
        inputs=cast("str", row[2]).encode(),
        state=cast("str", row[3]),
        cancelled=bool(row[4]),
        error=cast("str | None", row[5]),
        error_kind=None if kind is None else FailureKind(kind),
        seed=cast("str", row[7]),
    )


class SQLiteJobs:
    """SQLite :class:`JobStore` operations, mixed into ``SQLiteStore``."""

    async def _run(
        self,
        operation: Callable[[sqlite3.Connection], _R],
        *,
        write: bool = False,
        run_id: str | None = None,
    ) -> _R:
        raise NotImplementedError

    async def job_get(self, namespace: str, run_id: str) -> ScheduledRun:
        def get(conn: sqlite3.Connection) -> ScheduledRun:
            row = conn.execute(
                _SELECT_JOB + "FROM jobs WHERE namespace = ? AND run_id = ?",
                (namespace, run_id),
            ).fetchone()
            if row is None:
                msg = f"run {run_id!r} does not exist"
                raise RunNotFoundError(msg)
            return _row(row)

        return await self._run(get)

    async def job_submit(
        self, namespace: str, run_id: str, contract: bytes, inputs: bytes, seed: str
    ) -> str:
        def submit(conn: sqlite3.Connection) -> str:
            now = time.time()
            conn.execute(
                "INSERT INTO jobs (namespace, run_id, "
                "contract, inputs, seed, state, "
                "retry_at, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'queued', 0, ?, ?) "
                "ON CONFLICT(namespace, run_id) DO NOTHING",
                (namespace, run_id, contract.decode(), inputs.decode(), seed, now, now),
            )
            row = conn.execute(
                "SELECT contract, inputs, seed FROM jobs WHERE "
                "namespace = ? AND run_id = ?",
                (namespace, run_id),
            ).fetchone()
            assert row is not None
            if row[0] != contract.decode() or row[1] != inputs.decode():
                msg = "run ID already identifies a different invocation"
                raise SubmissionConflictError(msg)
            return cast("str", row[2])

        return await self._run(submit, write=True, run_id=run_id)

    async def job_candidates(
        self, namespace: str, after: str
    ) -> Sequence[ScheduledRun]:
        def candidates(conn: sqlite3.Connection) -> list[ScheduledRun]:
            rows = conn.execute(
                _SELECT_JOB + "FROM jobs "
                "WHERE namespace = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled') "
                "AND retry_at <= ? AND run_id > ? ORDER BY run_id LIMIT 64",
                (namespace, time.time(), after),
            ).fetchall()
            return [_row(row) for row in rows]

        return await self._run(candidates)

    async def job_delay(self, namespace: str, active: Sequence[str]) -> float | None:
        """Find the next retry or suspended deadline.

        Returns:
            Seconds until work is due, or None if there is no scheduled work.

        """

        def delay(conn: sqlite3.Connection) -> float | None:
            now = time.time()
            next_at = conn.execute(
                "SELECT min(retry_at) FROM jobs WHERE namespace = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled') "
                "AND run_id NOT IN (SELECT value FROM json_each(?))",
                (namespace, json.dumps(active)),
            ).fetchone()[0]
            return None if next_at is None else max(0, float(next_at) - now)

        return await self._run(delay)

    async def job_state(
        self,
        namespace: str,
        run_id: str,
        state: str,
        error: str | None = None,
        kind: FailureKind | None = None,
    ) -> None:
        def update(conn: sqlite3.Connection) -> None:
            now = time.time()
            conn.execute(
                "UPDATE jobs SET state = ?, error = ?, error_kind = ?, "
                "retry_at = ?, updated_at = ? "
                "WHERE namespace = ? AND run_id = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled') "
                "AND (? != 'blocked' OR cancelled = 0)",
                (
                    state,
                    error,
                    kind,
                    None if state == "blocked" else now,
                    now,
                    namespace,
                    run_id,
                    state,
                ),
            )

        await self._run(update, write=True, run_id=run_id)

    async def job_attempt_failed(
        self,
        namespace: str,
        run_id: str,
        error: str,
        kind: FailureKind,
        *,
        retryable: bool,
        max_attempts: int,
    ) -> None:
        def fail(conn: sqlite3.Connection) -> None:
            now = time.time()
            row = conn.execute(
                "SELECT attempts FROM jobs WHERE namespace = ? AND run_id = ?",
                (namespace, run_id),
            ).fetchone()
            attempts = int(row[0]) if row is not None else 0
            retry = retryable and attempts + 1 < max_attempts
            state = "queued" if retry else "failed"
            conn.execute(
                "UPDATE jobs SET state = ?, error = ?, error_kind = ?, "
                "retry_at = ?, attempts = attempts + 1, updated_at = ? "
                "WHERE namespace = ? AND run_id = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled')",
                (
                    state,
                    error,
                    kind,
                    now + 0.1 if retry else now,
                    now,
                    namespace,
                    run_id,
                ),
            )

        await self._run(fail, write=True, run_id=run_id)

    async def job_cancel(self, namespace: str, run_id: str) -> None:
        def cancel(conn: sqlite3.Connection) -> None:
            cursor = conn.execute(
                "UPDATE jobs SET cancelled = 1, retry_at = 0, "
                "error = CASE WHEN state = 'blocked' THEN NULL ELSE error END, "
                "error_kind = CASE WHEN state = 'blocked' "
                "THEN NULL ELSE error_kind END, "
                "updated_at = ? "
                "WHERE namespace = ? AND run_id = ?",
                (time.time(), namespace, run_id),
            )
            if not cursor.rowcount:
                msg = f"run {run_id!r} does not exist"
                raise RunNotFoundError(msg)

        await self._run(cancel, write=True, run_id=run_id)

    async def job_suspend(
        self, namespace: str, run_id: str, deadline: float | None
    ) -> bool:
        def suspend(conn: sqlite3.Connection) -> bool:
            cursor = conn.execute(
                "UPDATE jobs SET state = 'suspended', retry_at = ?, updated_at = ? "
                "WHERE namespace = ? AND run_id = ? AND state "
                "= 'running' AND cancelled = 0 "
                "AND NOT EXISTS (SELECT 1 FROM inbox AS d "
                "WHERE d.namespace = jobs.namespace AND d.run_id = "
                "jobs.run_id)",
                (deadline, time.time(), namespace, run_id),
            )
            return bool(cursor.rowcount)

        return await self._run(suspend, write=True, run_id=run_id)

    async def job_wake(self, namespace: str) -> None:
        def wake(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE jobs SET state = 'queued', retry_at = 0, "
                "error = NULL, error_kind = NULL, updated_at = ? "
                "WHERE namespace = ? AND state = 'blocked'",
                (time.time(), namespace),
            )

        await self._run(wake, write=True)

    async def inbox_post(
        self,
        namespace: str,
        inbox_id: str,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
    ) -> None:
        def post(conn: sqlite3.Connection) -> None:
            # Acceptance commits before dequeue. Check both locations in the same
            # transaction so retries remain idempotent across worker crashes.
            accepted = conn.execute(
                "SELECT data FROM log_entries "
                "WHERE namespace = ? AND run_id = ? AND id = ?",
                (namespace, run_id, inbox_id),
            ).fetchone()
            if accepted is not None:
                entry = cast("Entry", json.loads(accepted[0]))
                if (
                    entry["type"] != "input.accept"
                    or entry["port"] != port
                    or entry["closed"] != (kind == "close")
                    or canonical_json(entry.get("value")) != payload
                ):
                    msg = "input idempotency key was reused with different content"
                    raise PostConflictError(msg)
                return
            now = time.time()
            conn.execute(
                "INSERT INTO inbox (id, namespace, run_id, kind, port, payload) "
                "SELECT ?, namespace, run_id, ?, ?, ? FROM jobs "
                "WHERE namespace = ? AND run_id = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled') "
                "ON CONFLICT(namespace, run_id, id) DO NOTHING",
                (inbox_id, kind, port, payload.decode(), namespace, run_id),
            )
            row = conn.execute(
                "SELECT kind, port, payload FROM inbox "
                "WHERE namespace = ? AND run_id = ? AND id = ?",
                (namespace, run_id, inbox_id),
            ).fetchone()
            if row is not None and row != (kind, port, payload.decode()):
                msg = "input idempotency key was reused with different content"
                raise PostConflictError(msg)
            conn.execute(
                "UPDATE jobs SET retry_at = 0, updated_at = ? "
                "WHERE namespace = ? AND run_id = ? AND state = 'suspended'",
                (now, namespace, run_id),
            )

        await self._run(post, write=True, run_id=run_id)

    async def inbox_pending(
        self, namespace: str, run_id: str
    ) -> Sequence[InboxMessage]:
        def pending(conn: sqlite3.Connection) -> list[InboxMessage]:
            rows = conn.execute(
                "SELECT id, kind, port, payload FROM inbox "
                "WHERE namespace = ? AND run_id = ? "
                "ORDER BY sequence LIMIT 64",
                (namespace, run_id),
            ).fetchall()
            return [
                InboxMessage(
                    cast("str", row[0]),
                    cast("str", row[1]),
                    cast("str", row[2]),
                    cast("str", row[3]).encode(),
                )
                for row in rows
            ]

        return await self._run(pending)

    async def inbox_delete(self, namespace: str, run_id: str, inbox_id: str) -> None:
        def delete(conn: sqlite3.Connection) -> None:
            conn.execute(
                "DELETE FROM inbox WHERE namespace = ? AND run_id = ? AND id = ?",
                (namespace, run_id, inbox_id),
            )

        await self._run(delete, write=True, run_id=run_id)
