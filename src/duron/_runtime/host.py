"""Dispatch and lifecycle ownership for locally hosted runs."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self, cast
from typing_extensions import ParamSpec, TypeVar, override

from duron._api.ports import Input, NoPorts, PortSet
from duron._api.run import Run, RunState
from duron._api.scheduler import Scheduler
from duron._api.workflow import Workflow
from duron._execution.errors import encode_error
from duron._execution.posting import event_id
from duron._history.encoding import encode_entry, random_id
from duron._runtime.claims import acquire_claim
from duron._runtime.history import entries, outcome_state, read_header, read_outcome
from duron._runtime.machine import RunMachine
from duron._runtime.run import OutputReader as _OutputReader
from duron._runtime.run import RunHandle as _Run
from duron._tasks import finish_task
from duron.errors import (
    ContractMismatchError,
    CorruptLogError,
    FailureKind,
    HistoryMismatchError,
    InterruptedEffectError,
    PortClosedError,
    PostConflictError,
    StorageError,
    WorkflowDefinitionError,
)
from duron.storage._protocol import validate_id
from duron.storage._sqlite import SQLiteStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Collection, Sequence
    from pathlib import Path
    from types import TracebackType

    from duron._api.ports import Output
    from duron._api.reference import WorkflowRef
    from duron._api.run import OutputCursor
    from duron._hints import JSONValue
    from duron._history.entries import PromiseCompleteEntry
    from duron._ref import Ref
    from duron.storage._protocol import ScheduledRun

_MAX_ATTEMPTS = 3
_P = ParamSpec("_P")
_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound=PortSet, default=NoPorts)
_V = TypeVar("_V")


@dataclass(slots=True)
class _ActiveRun:
    task: asyncio.Task[None]
    inbox: asyncio.Event
    cancelled: asyncio.Event


class SQLiteScheduler(Scheduler):
    """A single-process scheduler with exclusive ownership of its SQLite file."""

    def __init__(
        self,
        path: str | Path,
        *,
        workflows: Collection[Workflow[..., object, PortSet]] = (),
        namespace: str = "default",
        idle_timeout: float = 60.0,
    ) -> None:
        if not math.isfinite(idle_timeout) or idle_timeout < 0:
            msg = "idle_timeout must be finite and nonnegative"
            raise ValueError(msg)
        validate_id(namespace, "namespace")
        self._path = path
        self._workflows = tuple(workflows)
        self._storage: SQLiteStore | None = None
        self._registry: dict[str, Workflow[..., object, PortSet]] = {}
        for workflow in self._workflows:
            key = workflow.name
            previous = self._registry.get(key)
            if previous is not None and previous is not workflow:
                msg = f"duplicate workflow registration {key!r}"
                raise ValueError(msg)
            self._registry[key] = workflow
        self._namespace = namespace
        self._idle_timeout = idle_timeout
        self._dispatcher: asyncio.Task[None] | None = None
        self._active: dict[str, _ActiveRun] = {}
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
        self._registry = {flow.name: flow for flow in self._workflows}
        self._storage = SQLiteStore(self._path, namespace=self._namespace)
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

    def _start_dispatcher(self) -> asyncio.Task[None]:
        task = asyncio.create_task(self._dispatch())
        task.add_done_callback(self._dispatcher_done)
        return task

    def _dispatcher_done(self, task: asyncio.Task[None]) -> None:
        error = None if task.cancelled() else task.exception()
        if self._running:
            logging.getLogger(__name__).error(
                "scheduler dispatcher stopped; replacing it", exc_info=error
            )
            self._dispatcher = self._start_dispatcher()

    def _run_done(self, run_id: str, task: asyncio.Task[None]) -> None:
        del self._active[run_id]
        self._work_changed.set()
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
        cleanup = asyncio.create_task(self._close())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await finish_task(cleanup)
            raise

    async def _close(self) -> None:
        try:
            if self._dispatcher is not None:
                self._dispatcher.cancel()
                await asyncio.gather(self._dispatcher, return_exceptions=True)
                self._dispatcher = None
            tasks = [active.task for active in self._active.values()]
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
        key = ref.name
        local = self._registry.get(key)
        if local is not None and local.ref._identity != ref._identity:
            msg = "submitted reference does not match registered workflow contract"
            raise ContractMismatchError(msg)
        if isinstance(workflow, Workflow) and local is None:
            self._registry[key] = workflow
            await self._job_wake()
        seed = await self._job_submit(
            run_id, ref._identity, ref._encode(*args, **kwargs)
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
        key = ref.name
        job = await self._store.job_get(self._namespace, validate_id(run_id, "run_id"))
        if job.contract != ref._identity:
            msg = "reference does not match the persisted invocation contract"
            raise ContractMismatchError(msg)
        if isinstance(workflow, Workflow) and key not in self._registry:
            self._registry[key] = workflow
            await self._job_wake()
        return _Run(self, ref, run_id, job.seed)

    async def observe(
        self, run_id: str
    ) -> tuple[RunState, PromiseCompleteEntry | None, str | None, FailureKind | None]:
        self._check_running()
        key = validate_id(run_id, "run_id")
        job = await self._store.job_get(self._namespace, key)
        header = await read_header(self._store, run_id=key)
        definition = cast("dict[str, JSONValue]", json.loads(job.contract))
        if header is not None and (header.get("ref") != {"name": definition["name"]}):
            msg = "history contract does not match scheduling envelope"
            raise ContractMismatchError(msg)
        outcome = await read_outcome(self._store, key)
        if outcome is not None:
            return RunState(outcome_state(outcome)), outcome, None, None
        if job.state in {"succeeded", "failed", "cancelled"} and job.error_kind is None:
            return (
                RunState.BLOCKED,
                None,
                "terminal job has no committed outcome",
                FailureKind.CONTRACT_MISMATCH,
            )
        return RunState(job.state), None, job.error, job.error_kind

    def watch(self, run_id: str) -> Callable[[], Awaitable[None]]:
        self._check_running()
        return self._store.watch(run_id)

    async def cancel(self, run_id: str) -> None:
        self._check_running()
        await self._job_cancel(validate_id(run_id, "run_id"))

    def outputs(
        self,
        run_id: str,
        ref: WorkflowRef[..., object, PortSet],
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
            run_id=validate_id(run_id, "run_id"),
            identity=identity,
            state=lambda: self._output_state(run_id, port.name),
            watch=functools.partial(self.watch, run_id),
        )

    async def deliver(
        self,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
        idempotency_key: str | None = None,
    ) -> None:
        self._check_running()
        key = validate_id(run_id, "run_id")
        job = await self._store.job_get(self._namespace, key)
        contract = cast("dict[str, JSONValue]", json.loads(job.contract))
        ports = cast("list[dict[str, JSONValue]]", contract["ports"])
        if kind not in {"input", "close"} or not any(
            item.get("name") == port and item.get("direction") == "input"
            for item in ports
        ):
            msg = "delivery does not match a declared workflow port"
            raise ValueError(msg)
        message_id = (
            event_id(job.seed, "__input__:" + port, idempotency_key, kind)
            if idempotency_key is not None
            else random_id()
        )
        await self._inbox_post(message_id, key, kind, port, payload)

    async def _output_state(self, run_id: str, port: str) -> tuple[str | None, bool]:
        key = validate_id(run_id, "run_id")
        await self._store.job_get(self._namespace, key)
        terminal = await read_outcome(self._store, key) is not None
        streams = await entries(self._store, key, "stream.create", f"__output__:{port}")
        return (streams[0][1]["id"] if streams else None, terminal)

    async def _cancel_queued(self, job: ScheduledRun) -> None:
        token = await acquire_claim(self._store, job.id)
        try:
            outcome = await read_outcome(self._store, job.id)
            if outcome is not None:
                await self._store.job_state(
                    self._namespace, job.id, outcome_state(outcome)
                )
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
                ref: Ref = {"name": cast("str", definition["name"])}
                await self._store.create(
                    job.id,
                    encode_entry({
                        "type": "promise.create",
                        "source": "effect",
                        "id": job.seed,
                        "ts": 0,
                        "ref": ref,
                        "args": args,
                        "kwargs": kwargs,
                    }),
                )
                header = await read_header(self._store, run_id=job.id)
                assert header is not None
            if header.get("ref") != {"name": definition["name"]}:
                msg = "cancellation history contract does not match submission"
                raise HistoryMismatchError(msg)
            await self._store.finish(
                job.id,
                token,
                [
                    encode_entry({
                        "type": "promise.complete",
                        "promise_id": header["id"],
                        "id": random_id(),
                        "source": "effect",
                        "ts": 0,
                        "error": encode_error(asyncio.CancelledError()),
                    })
                ],
                "cancelled",
            )
        finally:
            await self._store.release(job.id, token)

    async def _serve(self, execution: RunMachine[_T]) -> None:
        changed = self._active[execution.run_id].inbox
        while True:
            changed.clear()
            deliveries = await self._store.inbox_pending(
                self._namespace, execution.run_id
            )
            for delivery in deliveries:
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
                                event_id=delivery.id,
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
                    logging.getLogger(__name__).warning(
                        "Discarding inbox message %s for run %s: %s",
                        delivery.id,
                        execution.run_id,
                        error,
                    )
                await self._store.inbox_delete(
                    self._namespace, execution.run_id, delivery.id
                )
            if len(deliveries) < 64:
                await changed.wait()

    async def _dispatch(self) -> None:
        while True:
            self._work_changed.clear()
            try:
                cursor = ""
                while candidates := await self._job_candidates(cursor):
                    for job in candidates:
                        cursor = job.id
                        await self._dispatch_job(job)
                delay = await self._store.job_delay(self._namespace, list(self._active))
            except StorageError:
                delay = 0.1
            try:
                async with asyncio.timeout(delay):
                    await self._work_changed.wait()
            except TimeoutError:
                pass

    async def _dispatch_job(self, job: ScheduledRun) -> None:
        if job.id in self._active:
            return
        workflow = None
        if not job.cancelled:
            definition = cast("dict[str, JSONValue]", json.loads(job.contract))
            workflow = self._registry.get(cast("str", definition["name"]))
            if workflow is None:
                await self._store.job_state(
                    self._namespace,
                    job.id,
                    RunState.BLOCKED,
                    "implementation unavailable",
                    FailureKind.UNAVAILABLE,
                )
                return
            if workflow.ref._identity != job.contract:
                await self._store.job_state(
                    self._namespace,
                    job.id,
                    RunState.BLOCKED,
                    "registered workflow does not match submission",
                    FailureKind.CONTRACT_MISMATCH,
                )
                return
        inbox, cancelled = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(self._execute(job, workflow, cancelled))
        self._active[job.id] = _ActiveRun(task, inbox, cancelled)
        task.add_done_callback(functools.partial(self._run_done, job.id))

    async def _execute(
        self,
        job: ScheduledRun,
        workflow: Workflow[..., object, PortSet] | None,
        cancelled: asyncio.Event,
    ) -> None:
        try:
            if job.cancelled:
                await self._cancel_queued(job)
                return
            assert workflow is not None

            async def suspend(deadline: float | None) -> bool:
                return await self._job_suspend(job.id, deadline)

            async with await RunMachine.load(
                self._store, workflow, job.id, job.inputs, run_seed=job.seed
            ) as machine:
                # Cancellation may have committed after the candidate snapshot,
                # before this run had a local cancellation event.
                if (await self._store.job_get(self._namespace, job.id)).cancelled:
                    cancelled.set()
                await self._store.job_state(self._namespace, job.id, RunState.RUNNING)
                if not await machine.run(
                    cancelled, self._serve, suspend, idle_timeout=self._idle_timeout
                ):
                    return
        except InterruptedEffectError as error:
            await self._block_job(job, error, FailureKind.INTERRUPTED_EFFECT)
        except HistoryMismatchError as error:
            await self._block_job(job, error, FailureKind.HISTORY_MISMATCH)
        except CorruptLogError as error:
            await self._block_job(job, error, FailureKind.CORRUPT_HISTORY)
        except (
            ValueError,
            TypeError,
            ContractMismatchError,
            WorkflowDefinitionError,
        ) as error:
            # Deterministic input, codec, or definition failures never succeed on retry.
            await self._store.job_attempt_failed(
                self._namespace,
                job.id,
                str(error),
                FailureKind.INVALID,
                retryable=False,
                max_attempts=_MAX_ATTEMPTS,
            )
        except Exception as error:  # ruff: ignore[blind-except]  # Abandon the attempt and retry infrastructure failures.
            # Storage and unclassified failures retry within a bounded budget.
            await self._store.job_attempt_failed(
                self._namespace,
                job.id,
                str(error),
                FailureKind.INFRASTRUCTURE,
                retryable=True,
                max_attempts=_MAX_ATTEMPTS,
            )

    async def _block_job(
        self, job: ScheduledRun, error: Exception, kind: FailureKind
    ) -> None:
        if job.cancelled:
            # Cancellation cannot repair an incompatible history. Blocked-state
            # writes deliberately yield to cancellation, so record a terminal
            # scheduler failure without committing a workflow outcome.
            await self._store.job_attempt_failed(
                self._namespace,
                job.id,
                str(error),
                kind,
                retryable=False,
                max_attempts=_MAX_ATTEMPTS,
            )
        else:
            await self._store.job_state(
                self._namespace, job.id, RunState.BLOCKED, str(error), kind
            )

    async def _job_submit(self, run_id: str, contract: bytes, inputs: bytes) -> str:
        try:
            return await self._store.job_submit(
                self._namespace,
                validate_id(run_id, "run_id"),
                contract,
                inputs,
                random_id(),
            )
        finally:
            self._work_changed.set()

    async def _job_candidates(self, after: str) -> Sequence[ScheduledRun]:
        return await self._store.job_candidates(self._namespace, after)

    async def _job_suspend(self, run_id: str, deadline: float | None) -> bool:
        return await self._store.job_suspend(self._namespace, run_id, deadline)

    async def _job_cancel(self, run_id: str) -> None:
        committed = False
        try:
            await self._store.job_cancel(self._namespace, run_id)
            committed = True
        except asyncio.CancelledError:
            committed = (await self._store.job_get(self._namespace, run_id)).cancelled
            raise
        finally:
            if committed:
                if active := self._active.get(run_id):
                    active.cancelled.set()
                self._work_changed.set()

    async def _job_wake(self) -> None:
        try:
            await self._store.job_wake(self._namespace)
        finally:
            self._work_changed.set()

    async def _inbox_post(
        self, inbox_id: str, run_id: str, kind: str, port: str, payload: bytes
    ) -> None:
        try:
            await self._store.inbox_post(
                self._namespace, inbox_id, run_id, kind, port, payload
            )
        finally:
            # A cancelled waiter can still have committed its durable input.
            if active := self._active.get(run_id):
                active.inbox.set()
            self._work_changed.set()
