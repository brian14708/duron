"""Typed scheduler interface and structured SQLite implementation."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self, cast
from typing_extensions import ParamSpec, TypedDict, TypeVar, override

from duron._api.ports import Input, NoPorts, Output, PortSet
from duron._api.run import OutputCursor, Run, RunState, _OutputReader, _Run
from duron._api.workflow import Workflow
from duron._execution.models import canonical_json
from duron._history.encoding import encode_entry, random_id
from duron._runtime.history import CURRENT_VERSION, entries, read_header, read_outcome
from duron._runtime.leases import acquire_lease
from duron._runtime.machine import RunMachine
from duron.errors import (
    ClaimBusyError,
    ContractMismatchError,
    DuronError,
    HistoryMismatchError,
    PortClosedError,
    PostConflictError,
    RunNotFoundError,
    StorageError,
    SubmissionConflictError,
)
from duron.storage._protocol import validate_run_id
from duron.storage._sqlite import SQLiteStore

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Collection, Sequence
    from pathlib import Path
    from types import TracebackType

    from duron._api.reference import WorkflowRef
    from duron._hints import JSONValue
    from duron._history.entries import RunFinishEntry

_P = ParamSpec("_P")
_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound=PortSet, default=NoPorts)
_V = TypeVar("_V")


_Result = TypeVar("_Result")


class DeliveryError(TypedDict):
    type: str
    message: str


@dataclass(frozen=True, slots=True)
class Delivery:
    id: str
    kind: str
    port: str
    payload: bytes
    retry_id: str | None


@dataclass(frozen=True, slots=True)
class ScheduledRun:
    id: str
    contract: bytes
    inputs: bytes
    state: RunState
    cancelled: bool
    error: str | None
    seed: str


def _outcome_status(outcome: RunFinishEntry) -> RunState:
    if outcome["cancelled"]:
        return RunState.CANCELLED
    return RunState.FAILED if "error" in outcome else RunState.SUCCEEDED


class Scheduler(ABC):
    """Abstract scheduler contract for typed submission and observation."""

    @abstractmethod
    async def __aenter__(self) -> Self: ...

    @abstractmethod
    async def __aexit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None: ...

    @abstractmethod
    async def submit(
        self,
        workflow: Workflow[_P, _T, _Ports] | WorkflowRef[_P, _T, _Ports],
        run_id: str,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Run[_T, _Ports]: ...

    @abstractmethod
    async def open(
        self,
        workflow: Workflow[_P, _T, _Ports] | WorkflowRef[_P, _T, _Ports],
        run_id: str,
        /,
    ) -> Run[_T, _Ports]: ...


class SQLiteScheduler(Scheduler):
    """Durable workflows and structured scheduler state in an owned SQLite database."""

    def __init__(
        self,
        path: str | Path,
        *,
        workflows: Collection[Workflow[..., Any, Any]] = (),
        namespace: str = "default",
        idle_timeout: float = 60.0,
    ) -> None:
        if not math.isfinite(idle_timeout) or idle_timeout < 0:
            msg = "idle_timeout must be finite and nonnegative"
            raise ValueError(msg)
        validate_run_id(namespace)
        self._path = path
        self._workflows = tuple(workflows)
        self._storage: SQLiteStore | None = None
        self._registry: dict[tuple[str, str], Workflow[..., Any, Any]] = {}
        for workflow in self._workflows:
            key = workflow.name, workflow.version
            previous = self._registry.get(key)
            if previous is not None and previous is not workflow:
                msg = f"duplicate workflow registration {key!r}"
                raise ValueError(msg)
            self._registry[key] = workflow
        self._namespace = namespace
        self._idle_timeout = idle_timeout
        self._dispatcher: asyncio.Task[None] | None = None
        self._active: dict[str, asyncio.Task[None]] = {}
        self._work_changed = asyncio.Event()
        self._running = False

    @property
    def _store(self) -> SQLiteStore:
        if self._storage is None:
            msg = "scheduler context is not active"
            raise RuntimeError(msg)
        return self._storage

    @override
    async def __aenter__(self) -> Self:
        if self._storage is not None:
            msg = "scheduler context is already active"
            raise RuntimeError(msg)
        self._registry = {(flow.name, flow.version): flow for flow in self._workflows}
        self._storage = SQLiteStore(self._path)
        try:
            await self._job_wake()
            self._running = True
            self._dispatcher = self._start_dispatcher()
        except BaseException:
            self._running = False
            try:
                await self._store.close()
            finally:
                self._storage = None
            raise
        return self

    def _start_dispatcher(self, *, recovering: bool = False) -> asyncio.Task[None]:
        task = asyncio.create_task(self._dispatch(recovering=recovering))
        task.add_done_callback(self._dispatcher_done)
        return task

    def _dispatcher_done(self, task: asyncio.Task[None]) -> None:
        error = None if task.cancelled() else task.exception()
        if self._running:
            logging.getLogger(__name__).error(
                "scheduler dispatcher stopped; replacing it", exc_info=error
            )
            self._dispatcher = self._start_dispatcher(recovering=True)

    def _run_done(self, run_id: str, task: asyncio.Task[None]) -> None:
        del self._active[run_id]
        if not task.cancelled() and (error := task.exception()) is not None:
            logging.getLogger(__name__).error(
                "run %r stopped; leaving it available for recovery",
                run_id,
                exc_info=error,
            )

    @override
    async def __aexit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self._running = False
        try:
            if self._dispatcher is not None:
                self._dispatcher.cancel()
                await asyncio.gather(self._dispatcher, return_exceptions=True)
                self._dispatcher = None
            tasks = list(self._active.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            try:
                if self._storage is not None:
                    await self._storage.close()
            finally:
                self._storage = None

    def _check_running(self) -> None:
        if not self._running:
            msg = "scheduler context is not active"
            raise RuntimeError(msg)

    async def _wait(self) -> None:
        self._check_running()
        await self._store.wait_for_change()

    async def _wait_for_work(self) -> None:
        # History commits notify observers, but do not create dispatch or ingress work.
        self._check_running()
        try:
            async with asyncio.timeout(0.1):
                await self._work_changed.wait()
        except TimeoutError:
            pass
        self._work_changed.clear()

    def _key(self, run_id: str) -> str:
        validate_run_id(run_id)
        return json.dumps(["run", self._namespace, run_id], separators=(",", ":"))

    @override
    async def submit(
        self,
        workflow: Workflow[_P, _T, _Ports] | WorkflowRef[_P, _T, _Ports],
        run_id: str,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Run[_T, _Ports]:
        self._check_running()
        ref = workflow.ref if isinstance(workflow, Workflow) else workflow
        key = ref.name, ref.version
        local = self._registry.get(key)
        if local is not None and local.ref._identity != ref._identity:
            msg = "submitted reference does not match registered workflow contract"
            raise ContractMismatchError(msg)
        if isinstance(workflow, Workflow):
            self._registry[key] = workflow
        seed = await self._job_submit(
            self._key(run_id), ref._identity, ref._encode(*args, **kwargs)
        )
        return _Run(self, ref, run_id, seed)

    @override
    async def open(
        self,
        workflow: Workflow[_P, _T, _Ports] | WorkflowRef[_P, _T, _Ports],
        run_id: str,
        /,
    ) -> Run[_T, _Ports]:
        self._check_running()
        ref = workflow.ref if isinstance(workflow, Workflow) else workflow
        job = await self._job_get(self._key(run_id))
        if job.contract != ref._identity:
            msg = "reference does not match the persisted invocation contract"
            raise ContractMismatchError(msg)
        return _Run(self, ref, run_id, job.seed)

    async def _observe(
        self, run_id: str
    ) -> tuple[RunState, RunFinishEntry | None, str | None]:
        self._check_running()
        key = self._key(run_id)
        job = await self._job_get(key)
        header = await read_header(self._store, run_id=key)
        if (
            header is not None
            and canonical_json(header.get("contract")) != job.contract
        ):
            msg = "history contract does not match scheduling envelope"
            raise ContractMismatchError(msg)
        outcome = await read_outcome(self._store, key)
        if outcome is not None:
            return _outcome_status(outcome), outcome, None
        if job.state in {"succeeded", "failed", "cancelled"}:
            return (
                RunState.BLOCKED,
                None,
                "contract: terminal job has no committed outcome",
            )
        return job.state, None, job.error

    async def _cancel_run(self, run_id: str) -> None:
        self._check_running()
        await self._job_cancel(self._key(run_id))
        with contextlib.suppress(ClaimBusyError):
            await self._cancel_queued(await self._job_get(self._key(run_id)))

    async def _cancel_queued(self, job: ScheduledRun) -> None:
        token = await acquire_lease(self._store, job.id)
        try:
            outcome = await read_outcome(self._store, job.id)
            if outcome is not None:
                await self._job_state(job.id, _outcome_status(outcome))
                return
            definition = cast("dict[str, JSONValue]", json.loads(job.contract))
            header = await read_header(self._store, run_id=job.id)
            if header is None:
                values = cast("dict[str, JSONValue]", json.loads(job.inputs))
                args: list[JSONValue] = []
                kwargs: dict[str, JSONValue] = {}
                for parameter in cast(
                    "list[dict[str, JSONValue]]", definition["parameters"]
                ):
                    name = cast("str", parameter["name"])
                    if parameter["kind"] == "KEYWORD_ONLY":
                        kwargs[name] = values[name]
                    else:
                        args.append(values[name])
                header = {
                    "version": CURRENT_VERSION,
                    "nonce": job.seed,
                    "workflow_name": definition["name"],
                    "workflow_version": definition["version"],
                    "args": args,
                    "kwargs": kwargs,
                    "contract": definition,
                }
                await self._store.create(
                    job.id,
                    encode_entry({
                        "type": "run.create",
                        "source": "effect",
                        "id": random_id(),
                        "ts": 0,
                        "result": cast("JSONValue", header),
                    }),
                )
            if canonical_json(header.get("contract")) != job.contract:
                msg = "cancellation history contract does not match submission"
                raise HistoryMismatchError(msg)
            await self._store.append(
                job.id,
                token,
                [
                    encode_entry({
                        "type": "run.finish",
                        "id": random_id(),
                        "source": "effect",
                        "ts": 0,
                        "cancelled": True,
                    })
                ],
                batch_id=random_id(),
            )
            await self._job_state(job.id, RunState.CANCELLED)
        finally:
            await self._store.release(job.id, token)

    def _outputs(
        self,
        run_id: str,
        ref: WorkflowRef[..., Any, Any],
        port: Output[_V],
        after: OutputCursor | None,
        prefetch: int,
        identity: str,
    ) -> _OutputReader[_V]:
        self._check_running()
        return _OutputReader(
            self._store,
            ref.codec,
            port,
            after=after,
            prefetch=prefetch,
            history_id=self._key(run_id),
            identity=identity,
            state=lambda: self._output_state(run_id, port.name),
            wait=self._wait,
        )

    async def _output_state(self, run_id: str, port: str) -> tuple[str | None, bool]:
        self._check_running()
        key = self._key(run_id)
        _ = await self._job_get(key)
        terminal = await read_outcome(self._store, key) is not None
        streams = await entries(self._store, key, "stream.create", f"__output__:{port}")
        return (streams[0][1]["id"] if streams else None, terminal)

    async def _deliver(
        self,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
        retry_id: str | None = None,
    ) -> None:
        self._check_running()
        key = self._key(run_id)
        job = await self._job_get(key)
        contract = cast("dict[str, JSONValue]", json.loads(job.contract))
        ports = cast("list[dict[str, JSONValue]]", contract["ports"])
        expected = "input"
        if kind not in {"input", "close"} or not any(
            item.get("name") == port and item.get("direction") == expected
            for item in ports
        ):
            msg = "delivery does not match a declared workflow port"
            raise ValueError(msg)
        if retry_id is not None:
            validate_run_id(retry_id)
        internal_retry = (
            "user:" + retry_id if retry_id is not None else "delivery:" + random_id()
        )
        if await self._terminal_delivery(key, kind, port, payload, internal_retry):
            return
        delivery_id = await self._delivery_post(
            random_id(), key, kind, port, payload, internal_retry
        )
        while True:
            self._check_running()
            state, error = await self._delivery_receipt(delivery_id)
            if state == "accepted":
                return
            if error is not None:
                errors = {
                    "PostConflictError": PostConflictError,
                    "PortClosedError": PortClosedError,
                    "ValueError": ValueError,
                    "TypeError": TypeError,
                }
                raise errors.get(error["type"], DuronError)(error["message"])
            if await self._terminal_delivery(key, kind, port, payload, internal_retry):
                await self._delivery_acknowledge(delivery_id)
                return
            await self._wait()

    async def _terminal_delivery(
        self, key: str, kind: str, port: str, payload: bytes, retry_id: str
    ) -> bool:
        if kind == "input":
            retry_key = json.dumps(
                [port, retry_id], separators=(",", ":"), ensure_ascii=False
            )
            for _offset, entry in await entries(
                self._store, key, "input.accept", retry_key
            ):
                assert entry["type"] == "input.accept"
                if canonical_json(entry["value"]) != payload:
                    msg = "input retry ID was reused with a different payload"
                    raise PostConflictError(msg)
                return True
        if await read_outcome(self._store, key) is not None:
            if kind == "close":
                return True
            msg = "run is terminal"
            raise PortClosedError(msg)
        return False

    async def _serve(self, execution: RunMachine[Any]) -> None:
        while True:
            for delivery in await self._delivery_pending(execution.run_id):
                try:
                    port = next(
                        (
                            port
                            for port in execution.workflow.ports
                            if port.name == delivery.port
                        ),
                        None,
                    )
                    if delivery.kind in {"input", "close"} and isinstance(port, Input):
                        async with execution.ingress:
                            await execution.accept_input(
                                delivery.port,
                                cast("JSONValue", json.loads(delivery.payload)),
                                retry_id=delivery.retry_id
                                if delivery.kind == "input"
                                else None,
                                closed=delivery.kind == "close",
                            )
                    else:
                        msg = "delivery does not match a declared workflow endpoint"
                        raise ValueError(msg)  # ruff: ignore[raise-within-try]
                except (
                    PortClosedError,
                    PostConflictError,
                    ValueError,
                    TypeError,
                ) as error:
                    await self._delivery_acknowledge(
                        delivery.id,
                        {"type": type(error).__name__, "message": str(error)},
                    )
                else:
                    await self._delivery_acknowledge(delivery.id)
            await self._wait_for_work()

    async def _dispatch(self, *, recovering: bool = False) -> None:
        if recovering:
            await asyncio.sleep(0.1)
        cursor = ""
        while True:
            try:
                candidates = await self._job_candidates(cursor)
                if not candidates:
                    cursor = ""
                for job in candidates:
                    cursor = job.id
                    if job.id in self._active:
                        continue
                    definition = cast("dict[str, JSONValue]", json.loads(job.contract))
                    if job.cancelled:
                        with contextlib.suppress(ClaimBusyError):
                            await self._cancel_queued(job)
                        continue
                    workflow = self._registry.get((
                        cast("str", definition["name"]),
                        cast("str", definition["version"]),
                    ))
                    if workflow is None:
                        await self._job_state(
                            job.id,
                            RunState.BLOCKED,
                            "implementation unavailable",
                            retry=True,
                        )
                        continue
                    if workflow.ref._identity != job.contract:
                        await self._job_state(
                            job.id,
                            RunState.BLOCKED,
                            "contract: registered workflow does not match submission",
                            retry=True,
                        )
                        continue
                    task = asyncio.create_task(self._execute(job, workflow))
                    self._active[job.id] = task
                    task.add_done_callback(functools.partial(self._run_done, job.id))
            except StorageError:
                await asyncio.sleep(0.1)
            await self._wait_for_work()

    async def _execute(
        self, job: ScheduledRun, workflow: Workflow[..., Any, Any]
    ) -> None:
        try:

            async def cancelled() -> bool:
                return (await self._job_get(job.id)).cancelled

            async def suspend(deadline: float | None) -> bool:
                return await self._job_suspend(job.id, deadline)

            async with await RunMachine.load(
                self._store, workflow, job.id, job.inputs, run_seed=job.seed
            ) as machine:
                await self._job_state(job.id, RunState.RUNNING)
                if not await machine.run(
                    cancelled, self._serve, suspend, idle_timeout=self._idle_timeout
                ):
                    return
        except ClaimBusyError:
            await self._job_defer(job.id)
        except HistoryMismatchError as error:
            await self._job_state(
                job.id, RunState.BLOCKED, "contract: " + str(error), retry=True
            )
        except Exception as error:  # ruff: ignore[blind-except]  # Abandon the attempt and retry infrastructure failures.
            await self._job_state(job.id, RunState.QUEUED, str(error), retry=True)
        else:
            outcome = await read_outcome(self._store, job.id)
            assert outcome is not None
            await self._job_state(job.id, _outcome_status(outcome))

    async def _sql(
        self, operation: Callable[[sqlite3.Connection], _Result], *, write: bool = False
    ) -> _Result:
        store = self._store

        def execute() -> _Result:
            with store._connection(write=write) as connection:
                return operation(connection)

        pending = asyncio.create_task(asyncio.to_thread(execute))
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            await pending
            raise

    @staticmethod
    def _job_row(row: Sequence[object]) -> ScheduledRun:
        return ScheduledRun(
            id=cast("str", row[0]),
            contract=cast("str", row[1]).encode(),
            inputs=cast("str", row[2]).encode(),
            state=RunState(cast("str", row[3])),
            cancelled=bool(row[4]),
            error=cast("str | None", row[5]),
            seed=cast("str", row[6]),
        )

    async def _job_get(self, history_id: str) -> ScheduledRun:
        def get(conn: sqlite3.Connection) -> ScheduledRun:
            row = conn.execute(
                "SELECT history_id, contract, inputs, state, "
                "cancelled, error, seed "
                "FROM jobs WHERE namespace = ? AND history_id = ?",
                (self._namespace, history_id),
            ).fetchone()
            if row is None:
                msg = f"run {history_id!r} does not exist"
                raise RunNotFoundError(msg)
            return self._job_row(row)

        return await self._sql(get)

    async def _job_submit(self, history_id: str, contract: bytes, inputs: bytes) -> str:
        definition = cast("dict[str, JSONValue]", json.loads(contract))
        run_id = cast("list[str]", json.loads(history_id))[2]
        seed = random_id()

        def submit(conn: sqlite3.Connection) -> str:
            now = SQLiteStore._now(conn)
            conn.execute(
                "INSERT INTO jobs (namespace, run_id, history_id, workflow_name, "
                "workflow_version, contract, inputs, seed, state, "
                "retry_at, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?) "
                "ON CONFLICT(namespace, run_id) DO NOTHING",
                (
                    self._namespace,
                    run_id,
                    history_id,
                    definition["name"],
                    definition["version"],
                    contract.decode(),
                    inputs.decode(),
                    seed,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT contract, inputs, seed FROM jobs WHERE "
                "namespace = ? AND run_id = ?",
                (self._namespace, run_id),
            ).fetchone()
            assert row is not None
            if row[0] != contract.decode() or row[1] != inputs.decode():
                msg = "run ID already identifies a different invocation"
                raise SubmissionConflictError(msg)
            return cast("str", row[2])

        seed = await self._sql(submit, write=True)
        self._work_changed.set()
        return seed

    async def _job_candidates(self, after: str) -> list[ScheduledRun]:
        def candidates(conn: sqlite3.Connection) -> list[ScheduledRun]:
            rows = conn.execute(
                "SELECT history_id, contract, inputs, state, "
                "cancelled, error, seed "
                "FROM jobs WHERE namespace = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled') "
                "AND retry_at <= ? AND history_id > ? ORDER BY history_id LIMIT 64",
                (self._namespace, SQLiteStore._now(conn), after),
            ).fetchall()
            return [self._job_row(row) for row in rows]

        return await self._sql(candidates)

    async def _job_state(
        self,
        history_id: str,
        state: RunState,
        error: str | None = None,
        *,
        retry: bool = False,
    ) -> None:
        def update(conn: sqlite3.Connection) -> None:
            now = SQLiteStore._now(conn)
            retry_at = now + ((5 if state == "blocked" else 0.1) if retry else 0)
            conn.execute(
                "UPDATE jobs SET state = ?, error = ?, retry_at = ?, updated_at = ? "
                "WHERE namespace = ? AND history_id = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled')",
                (state, error, retry_at, now, self._namespace, history_id),
            )

        await self._sql(update, write=True)

    async def _job_cancel(self, history_id: str) -> None:
        def cancel(conn: sqlite3.Connection) -> None:
            cursor = conn.execute(
                "UPDATE jobs SET cancelled = 1, retry_at = 0, updated_at = ? "
                "WHERE namespace = ? AND history_id = ?",
                (SQLiteStore._now(conn), self._namespace, history_id),
            )
            if not cursor.rowcount:
                msg = f"run {history_id!r} does not exist"
                raise RunNotFoundError(msg)

        await self._sql(cancel, write=True)
        self._work_changed.set()

    async def _job_suspend(self, history_id: str, deadline: float | None) -> bool:
        def suspend(conn: sqlite3.Connection) -> bool:
            cursor = conn.execute(
                "UPDATE jobs SET state = 'suspended', retry_at = ?, updated_at = ? "
                "WHERE namespace = ? AND history_id = ? AND state "
                "= 'running' AND cancelled = 0 "
                "AND NOT EXISTS (SELECT 1 FROM deliveries AS d "
                "WHERE d.namespace = jobs.namespace AND d.run_id = "
                "jobs.run_id AND d.state = 'pending')",
                (deadline, SQLiteStore._now(conn), self._namespace, history_id),
            )
            return bool(cursor.rowcount)

        return await self._sql(suspend, write=True)

    async def _job_defer(self, history_id: str) -> None:
        def defer(conn: sqlite3.Connection) -> None:
            now = SQLiteStore._now(conn)
            conn.execute(
                "UPDATE jobs SET retry_at = ?, updated_at = ? "
                "WHERE namespace = ? AND history_id = ? "
                "AND state NOT IN ('succeeded', 'failed', 'cancelled')",
                (now + 0.1, now, self._namespace, history_id),
            )

        await self._sql(defer, write=True)

    async def _job_wake(self) -> None:
        def wake(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE jobs SET retry_at = 0, updated_at = ? "
                "WHERE namespace = ? AND state = 'blocked'",
                (SQLiteStore._now(conn), self._namespace),
            )

        await self._sql(wake, write=True)

    async def _delivery_post(
        self,
        delivery_id: str,
        history_id: str,
        kind: str,
        port: str,
        payload: bytes,
        retry_id: str | None,
    ) -> str:
        def post(conn: sqlite3.Connection) -> str:
            now = SQLiteStore._now(conn)
            if retry_id is None:
                conn.execute(
                    "INSERT INTO deliveries "
                    "(id, namespace, run_id, kind, port, payload, "
                    "retry_id, created_at) "
                    "SELECT ?, namespace, run_id, ?, ?, ?, ?, ? FROM jobs "
                    "WHERE namespace = ? AND history_id = ?",
                    (
                        delivery_id,
                        kind,
                        port,
                        payload.decode(),
                        retry_id,
                        now,
                        self._namespace,
                        history_id,
                    ),
                )
                row = conn.execute(
                    "SELECT id, payload FROM deliveries WHERE id = ?", (delivery_id,)
                ).fetchone()
            else:
                conn.execute(
                    "INSERT INTO deliveries "
                    "(id, namespace, run_id, kind, port, payload, retry_id, "
                    "created_at) "
                    "SELECT ?, namespace, run_id, ?, ?, ?, ?, ? FROM jobs "
                    "WHERE namespace = ? AND history_id = ? "
                    "ON CONFLICT(namespace, run_id, kind, port, retry_id) "
                    "WHERE retry_id IS NOT NULL DO NOTHING",
                    (
                        delivery_id,
                        kind,
                        port,
                        payload.decode(),
                        retry_id,
                        now,
                        self._namespace,
                        history_id,
                    ),
                )
                run_id = cast("list[str]", json.loads(history_id))[2]
                row = conn.execute(
                    "SELECT id, payload FROM deliveries "
                    "WHERE namespace = ? AND run_id = ? AND kind = ? "
                    "AND port = ? AND retry_id = ?",
                    (self._namespace, run_id, kind, port, retry_id),
                ).fetchone()
            if row is None:
                msg = "could not persist delivery"
                raise StorageError(msg)
            if row[1] != payload.decode():
                msg = "delivery retry ID was reused with a different payload"
                raise PostConflictError(msg)
            conn.execute(
                "UPDATE jobs SET retry_at = 0, updated_at = ? "
                "WHERE namespace = ? AND history_id = ? AND state = 'suspended'",
                (now, self._namespace, history_id),
            )
            return cast("str", row[0])

        delivery_id = await self._sql(post, write=True)
        self._work_changed.set()
        return delivery_id

    async def _delivery_pending(self, history_id: str) -> list[Delivery]:
        def pending(conn: sqlite3.Connection) -> list[Delivery]:
            rows = conn.execute(
                "SELECT d.id, d.kind, d.port, d.payload, d.retry_id "
                "FROM deliveries AS d JOIN jobs AS j "
                "ON d.namespace = j.namespace AND d.run_id = j.run_id "
                "WHERE j.namespace = ? AND j.history_id = ? AND d.state = 'pending' "
                "ORDER BY d.sequence LIMIT 64",
                (self._namespace, history_id),
            ).fetchall()
            return [
                Delivery(
                    cast("str", row[0]),
                    cast("str", row[1]),
                    cast("str", row[2]),
                    cast("str", row[3]).encode(),
                    cast("str | None", row[4]),
                )
                for row in rows
            ]

        return await self._sql(pending)

    async def _delivery_acknowledge(
        self, delivery_id: str, error: DeliveryError | None = None
    ) -> None:
        def acknowledge(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE deliveries SET state = ?, error_type = ?, error_message = ?, "
                "acknowledged_at = ? WHERE namespace = ? AND id = "
                "? AND state = 'pending'",
                (
                    "accepted" if error is None else "rejected",
                    error["type"] if error else None,
                    error["message"] if error else None,
                    SQLiteStore._now(conn),
                    self._namespace,
                    delivery_id,
                ),
            )

        await self._sql(acknowledge, write=True)

    async def _delivery_receipt(
        self, delivery_id: str
    ) -> tuple[str, DeliveryError | None]:
        def receipt(conn: sqlite3.Connection) -> tuple[str, DeliveryError | None]:
            row = conn.execute(
                "SELECT state, error_type, error_message FROM "
                "deliveries WHERE namespace = ? AND id = ?",
                (self._namespace, delivery_id),
            ).fetchone()
            if row is None:
                return "pending", None
            error: DeliveryError | None = None
            if row[1] is not None:
                error = {"type": cast("str", row[1]), "message": cast("str", row[2])}
            return cast("str", row[0]), error

        return await self._sql(receipt)


__all__ = ("SQLiteScheduler", "Scheduler")
