from __future__ import annotations

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]
import asyncio
import contextlib
import functools
import inspect
import sys
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Generic, Self, assert_never
from typing_extensions import TypeVar

from duron._execution.errors import decode_error, encode_error
from duron._execution.invocation import EffectRegistry
from duron._execution.models import (
    HandleReference,
    canonical_json,
    digest,
    direction_claims_channel,
)
from duron._execution.registration import event_arguments
from duron._history.encoding import decode_record, encode_entry, iter_entries, random_id
from duron._history.entries import CorruptLogError
from duron._ref import ref_of
from duron._runtime.api_context import WorkflowContextImpl
from duron._runtime.append import AppendWriter, BatchLimits
from duron._runtime.channels import OwnedChannelState, OwnedSignalState
from duron._runtime.claims import acquire_claim
from duron._runtime.context import Context
from duron._runtime.effects import ExecutionSession
from duron._runtime.history import InitParams, outcome_state, read_header, read_outcome
from duron._runtime.loop import EventLoop, create_loop
from duron._runtime.ops import (
    Barrier,
    EffectCall,
    FnCall,
    OwnedChannelCreate,
    OwnedChannelExit,
    OwnedSignalSend,
    OwnedStreamNext,
    StreamCreate,
    StreamEmit,
    WaitCall,
    WaitPending,
)
from duron._runtime.replay import ReplayState
from duron._runtime.state import MachineState, Operation, Phase
from duron._runtime.task_manager import TaskManager
from duron._tasks import finish_task
from duron.codec import DefaultCodec
from duron.errors import (
    ClaimLostError,
    ExecutionCompletedError,
    HistoryMismatchError,
    InvalidHandleError,
    PortClosedError,
    PostConflictError,
    RunUnavailableError,
    StorageError,
)
from duron.storage._protocol import ClaimToken, validate_id

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine, Iterable
    from types import TracebackType

    from duron._api.ports import PortSet
    from duron._api.workflow import Workflow
    from duron._hints import JSONValue, TypeHint
    from duron._history.entries import (
        BarrierEntry,
        Entry,
        InputAcceptedEntry,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        StreamCreateEntry,
        StreamEmitEntry,
    )
    from duron._runtime.loop import OpFuture, WaitSet
    from duron.storage import EventStore

_T = TypeVar("_T")
_R = TypeVar("_R")


class RunMachine(Generic[_T]):
    """One owned run: replay, execution, ports, and terminal transitions."""

    __slots__ = (
        "_activity",
        "_append_error",
        "_append_waiters",
        "_backend",
        "_batch_limits",
        "_claim_token",
        "_codec",
        "_draining",
        "_execution",
        "_execution_error",
        "_executor",
        "_ingress_gate",
        "_loop",
        "_main",
        "_now_us",
        "_owned_reads",
        "_parking",
        "_ready",
        "_replay",
        "_started",
        "_state",
        "_streams",
        "_task",
        "_task_manager",
        "_writer",
        "ingress",
        "workflow",
    )

    workflow: Workflow[..., _T, PortSet]
    ingress: asyncio.Lock

    def __init__(
        self,
        loop: EventLoop,
        backend: EventStore,
        run_id: str,
        token: ClaimToken,
        init: Callable[[], InitParams],
        fn: Workflow[..., _T, PortSet],
        batch_limits: BatchLimits,
        executor: EffectRegistry,
    ) -> None:
        self.workflow = fn
        self.ingress = asyncio.Lock()
        self._loop = loop
        self._backend = backend
        self._state = MachineState(run_id, init()["id"])
        self._claim_token: ClaimToken | None = token
        self._batch_limits = batch_limits
        self._ingress_gate = asyncio.Lock()
        self._now_us: int = 0
        self._append_error: ClaimLostError | StorageError | None = None
        self._execution_error: BaseException | None = None
        self._owned_reads: dict[str, OpFuture] = {}
        self._executor = executor
        self._replay = ReplayState()
        self._parking = False
        # Host-reader notification: bumped on every durable append and on
        # completion, so readers can await new entries instead of polling.
        self._append_waiters: set[asyncio.Future[None]] = set()

        self._ready = asyncio.Event()
        main = self._loop.schedule_task(
            _prelude_fn(
                init,
                fn,
                executor,
                backend,
                run_id,
                functools.partial(
                    asyncio.get_running_loop().call_soon, self._ready.set
                ),
            )
        )
        self._main = main
        self._codec = fn.codec
        self._draining = asyncio.Event()
        self._streams: dict[str, TypeHint[object]] = {}
        self._task: asyncio.Task[_T] | None = None
        self._activity = asyncio.Event()
        self._started = False

        def on_task_error(error: BaseException) -> None:
            # Effect exceptions are already durable outcomes. An exception
            # escaping the worker is an infrastructure failure: stop the host
            # engine before workflow cancellation can become durable history.
            if self._execution_error is None:
                self._execution_error = error
            if self._task is not None:
                self._task.cancel()

        self._task_manager = TaskManager(on_task_error)
        self._writer = AppendWriter(
            backend,
            run_id,
            token,
            batch_limits,
            self._apply_live,
            self._notify_append,
            self._abort_append,
        )
        self._execution = ExecutionSession(
            self._state,
            executor,
            self._codec,
            backend,
            self._writer,
            self.check_ingress,
            self.wait_for_append,
        )
        # Wake host readers when the run reaches a terminal state even if no
        # further entry is appended.

        def _on_main_done(_f: object) -> None:
            if self._state.phase is Phase.REPLAYING:
                self._notify_append()

        main.add_done_callback(_on_main_done)

    @staticmethod
    async def load(
        store: EventStore,
        workflow: Workflow[..., _R, PortSet],
        run_id: str,
        inputs: bytes,
        *,
        run_seed: str | None = None,
    ) -> RunMachine[_R]:
        validate_id(run_id, "run_id")
        loop = await create_loop()
        token: ClaimToken | None = None
        try:
            token = await acquire_claim(store, run_id)
            ref = workflow.ref
            header = await read_header(store, run_id=run_id)
            args, kwargs = ref._invocation(inputs)
            if header is None:
                await store.create(
                    run_id,
                    encode_entry({
                        "type": "promise.create",
                        "id": run_seed or random_id(),
                        "source": "effect",
                        "ts": time.time_ns() // 1000,
                        "args": args,
                        "kwargs": kwargs,
                        "ref": ref_of(ref),
                    }),
                )
                header = await read_header(store, run_id=run_id)
                assert header is not None
            if (
                (run_seed is not None and header["id"] != run_seed)
                or header.get("ref") != {"name": ref.name}
                or canonical_json(header["args"]) != canonical_json(args)
                or canonical_json(header["kwargs"]) != canonical_json(kwargs)
            ):
                msg = "persisted history does not match the submitted invocation"
                raise HistoryMismatchError(msg)  # ruff: ignore[raise-within-try]
            machine = RunMachine[_R](
                loop,
                store,
                run_id,
                token,
                lambda: header,
                workflow,
                BatchLimits(),
                EffectRegistry(),
            )
        except BaseException:
            loop.close()
            if token is not None:
                await store.release(run_id, token)
            raise
        return machine

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        # Shutdown can arrive after the result is committed, during cleanup.
        cleanup = asyncio.create_task(self.close())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await finish_task(cleanup)
            raise

    @property
    def closed(self) -> bool:
        return self._state.phase is Phase.CLOSED

    @property
    def run_id(self) -> str:
        return self._state.run_id

    async def run(
        self,
        cancelled: asyncio.Event,
        serve: Callable[[RunMachine[_T]], Awaitable[None]],
        suspend: Callable[[float | None], Awaitable[bool]] | None = None,
        *,
        idle_timeout: float = 60.0,
        draining: asyncio.Event | None = None,
    ) -> bool:
        """Run to completion or durable suspension.

        Returns:
            True on completion, false on suspension.

        Raises:
            RuntimeError: If the delivery service stops unexpectedly.
            HistoryMismatchError: If replay diverges from the persisted history.
            CorruptLogError: If persisted history is malformed.

        """
        if draining is not None:
            self._draining = draining
        if await read_outcome(self._backend, self._state.run_id) is not None:
            self._state.phase = Phase.FINISHED
            return True
        ref = self.workflow.ref
        outcome: PromiseCompleteEntry = {
            "type": "promise.complete",
            "promise_id": self._state.seed,
            "id": random_id(),
            "source": "effect",
            "ts": time.time_ns() // 1000,
            "error": encode_error(asyncio.CancelledError()),
        }
        if cancelled.is_set():
            _ = self._main.cancel()
            await self.finish(outcome)
            return True
        try:
            await self.replay()
        except asyncio.CancelledError:
            self._workflow_cancelled()
            await self.finish(outcome)
            return True
        except (HistoryMismatchError, CorruptLogError):
            # Replay failures describe the persisted history or its contract;
            # committing them as run outcomes would make recovery impossible.
            raise
        except Exception as error:
            # Only an exception from the workflow itself is an application
            # outcome. Replay can also fail while reading persisted history.
            if (
                not self._main.done()
                or self._main.cancelled()
                or self._main.exception() is not error
            ):
                raise
            self._application_failure(outcome, error)
            await self.finish(outcome)
            return True
        service = asyncio.ensure_future(serve(self))
        completion = asyncio.create_task(self.result())
        cancellation = asyncio.create_task(cancelled.wait())
        drain_requested = asyncio.create_task(self._draining.wait())
        activity: asyncio.Task[bool] | None = None
        cancelled_run = False
        idle_since: float | None = None
        try:
            while not completion.done():
                self._activity.clear()
                is_draining = self._draining.is_set()
                if is_draining and not cancelled.is_set():
                    async with self.ingress:
                        await self._writer.drain()
                        if (
                            not self._main.done()
                            and not completion.done()
                            and not any(
                                op.effect is not None and op.committed is not None
                                for op in self._state.operations.values()
                            )
                        ):
                            self._parking = True
                            return False
                timeout = None
                if suspend is not None and not is_draining:
                    idle_since, ready = self._idle_state(idle_since, idle_timeout)
                    if ready and await self._suspend(suspend):
                        return False
                    if idle_since is not None:
                        timeout = max(0, idle_timeout - (time.monotonic() - idle_since))
                activity = asyncio.create_task(self._activity.wait())
                waiters = [completion, service, cancellation, activity]
                if not is_draining:
                    waiters.append(drain_requested)
                done, _pending = await asyncio.wait(
                    waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
                activity.cancel()
                await asyncio.gather(activity, return_exceptions=True)
                if service in done:
                    service.result()
                    msg = "execution delivery service stopped unexpectedly"
                    raise RuntimeError(msg)
                if cancelled.is_set():
                    async with self.ingress:
                        if completion.done():
                            break
                        await self.stop_work()
                        cancelled_run = True
                    break
            if not cancelled_run:
                try:
                    result = await completion
                except asyncio.CancelledError:
                    self._workflow_cancelled()
                except Exception as error:  # ruff: ignore[blind-except]
                    self._application_failure(outcome, error)
                else:
                    try:
                        encoded = ref.codec.encode_json(result, ref.result_type)
                    except (TypeError, ValueError) as error:
                        self._application_failure(outcome, error)
                    else:
                        del outcome["error"]
                        outcome["result"] = encoded
            async with self.ingress:
                outcome["ts"] = time.time_ns() // 1000
                await self.finish(outcome)
            return True
        finally:
            service.cancel()
            completion.cancel()
            cancellation.cancel()
            drain_requested.cancel()
            if activity is not None:
                activity.cancel()
                await asyncio.gather(activity, return_exceptions=True)
            await asyncio.gather(
                service,
                completion,
                cancellation,
                drain_requested,
                return_exceptions=True,
            )

    def _idle_state(
        self, idle_since: float | None, idle_timeout: float
    ) -> tuple[float | None, bool]:
        if not self._can_suspend():
            return None, False
        now = time.monotonic()
        if idle_since is None:
            return now, False
        return idle_since, now - idle_since >= idle_timeout

    async def _suspend(
        self, suspend: Callable[[float | None], Awaitable[bool]]
    ) -> bool:
        # Hold ingress from the final readiness check through the journal commit.
        # Its transaction rejects pending deliveries and serializes later wakeups.
        async with self.ingress:
            await self._writer.drain()
            if not self._can_suspend():
                return False
            deadlines = [
                op.params.deadline
                for op in self._loop.pending_ops()
                if isinstance(op.params, WaitCall) and op.params.deadline is not None
            ]
            self._parking = True
            parked = False
            try:
                parked = await suspend(min(deadlines, default=None))
                return parked
            finally:
                self._parking = parked

    def _can_suspend(self) -> bool:
        if (
            self._state.phase is not Phase.LIVE
            or self._main.done()
            or self.infrastructure_error is not None
            or any(op.effect is not None for op in self._state.operations.values())
            or not self._loop.is_quiescent()
            or any(
                op.started and op.effect is None and op.waiting_version is None
                for op in self._state.operations.values()
            )
        ):
            return False
        pending = self._loop.pending_ops()
        if not pending:
            return False
        for future in pending:
            op = future.params
            if isinstance(op, WaitCall):
                if (
                    not op.suspendable
                    or self._state.operations[future.id].waiting_version
                    != self._state.append_version
                    or (op.deadline is not None and op.deadline <= time.time())
                ):
                    return False
            else:
                return False
        return True

    @staticmethod
    def _workflow_cancelled() -> None:
        owner = asyncio.current_task()
        if owner is not None and owner.cancelling():
            raise asyncio.CancelledError

    def _application_failure(
        self, outcome: PromiseCompleteEntry, error: Exception
    ) -> None:
        if self.infrastructure_error is not None or isinstance(
            error, HistoryMismatchError
        ):
            raise error
        outcome["error"] = encode_error(error)

    async def replay(self) -> None:
        """Replay history and start unfinished work.

        Raises:
            RuntimeError: If this machine has already started or closed.

        """
        if self._state.phase is not Phase.REPLAYING or self._started:
            msg = "run machine has already started or closed"
            raise RuntimeError(msg)
        self._started = True
        if await self._replay.resume(
            iter_entries(self._backend, self._state.run_id),
            self._advance_replay,
            self.apply_committed,
            self._main,
            self._state.operations,
        ):
            self._writer.check_open()
            return
        self._writer.check_open()
        self._task = asyncio.create_task(self._run())
        if sys.version_info >= (3, 14):
            asyncio.future_add_to_awaited_by(self._main, self._task)

        def cb(_t: asyncio.Task[_T]) -> None:
            # Readers must stop if the engine exits
            # with an infrastructure error before the workflow finishes.
            self._notify_append()
            self._ready.set()

        self._task.add_done_callback(cb)
        await self._ready.wait()
        if self._task.done():
            _ = await self._task

    async def _run(self) -> _T:
        if self._main.done():
            return self._main.result()

        try:
            self._state.phase = Phase.LIVE
            pending = tuple(self._replay.pending.values())
            for start in range(0, len(pending), self._batch_limits.max_entries):
                await self._enqueue_ready(
                    self._enqueue_log(msg)
                    for msg in pending[start : start + self._batch_limits.max_entries]
                )
            self._replay.pending.clear()
            del pending
            self._task_manager.start()
            await asyncio.sleep(0)

            if self._now_us == 0:
                self._now_us = max(self._now_us, time.time_ns() // 1_000)
            while waitset := await self._step():
                await waitset.block(self._now_us)
                waitset = await self._step()
                now = time.time_ns() // 1_000
                if waitset and waitset.timer and waitset.timer < now:
                    self._now_us = max(self._now_us, waitset.timer)
                else:
                    self._now_us = max(self._now_us, now)

            # cleanup
            self._loop.close()
            # Let newly admitted effect workers record caller cancellation
            # before graceful cleanup can cancel their first coroutine step.
            await asyncio.sleep(0)
            await self._task_manager.close(graceful=True)
            await self._writer.drain()

            return self._main.result()
        except asyncio.CancelledError as e:
            if self._append_error is not None:
                raise self._append_error from e
            if self._execution_error is not None:
                raise self._execution_error from e
            raise

    async def close(self) -> None:
        if self._state.phase is Phase.CLOSED:
            return
        self._state.phase = Phase.CLOSED
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await self._task
            self._task = None

        try:
            if not self._loop.is_closed():
                _ = self._main.cancel()
                self._loop.close()
            await self._writer.close()
            await self._task_manager.close()
        finally:
            if self._claim_token:
                await self._backend.release(self._state.run_id, self._claim_token)
                self._claim_token = None

    @staticmethod
    async def _enqueue_ready(operations: Iterable[Coroutine[Any, Any, None]]) -> None:
        batch = list(operations)
        if len(batch) == 1:
            await batch[0]
            return
        pending = [asyncio.create_task(operation) for operation in batch]
        try:
            await asyncio.gather(*pending)
        except BaseException:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise

    async def _advance_replay(self, timestamp: int) -> WaitSet | None:
        self._now_us = max(self._now_us, timestamp)
        return await self._step()

    async def _step(self) -> WaitSet | None:
        if self._state.phase is Phase.LIVE:
            self._now_us = max(self._now_us, time.time_ns() // 1_000)
        self._loop.tick(self._now_us)

        while True:
            frontier = self._state.frontier
            await self._serve_stream_reads()
            # Storage reads yield to reporters. Rescan when a commit advanced
            # readiness before poll_completion clears the wakeup event.
            if self._state.frontier != frontier:
                continue
            result = self._loop.poll_completion(self._main)
            if result is None or not result.added:
                return result

            for start in range(0, len(result.added), self._batch_limits.max_entries):
                await self._enqueue_ready(
                    self._enqueue_op(s)
                    for s in result.added[
                        start : start + self._batch_limits.max_entries
                    ]
                )
            if self._state.phase is Phase.LIVE:
                self._task_manager.start()

    async def _serve_stream_reads(self) -> None:
        for id_, fut in tuple(self._owned_reads.items()):
            if fut.cancelled():
                del self._owned_reads[id_]
                continue
            op = fut.params
            assert isinstance(op, OwnedStreamNext)
            await op.reader.fill(
                self._backend, self._state.run_id, self._codec, self._state.frontier
            )
            if op.reader.values:
                _ = self._loop.post_completion(id_, result=op.reader.values.popleft())
                del self._owned_reads[id_]

    async def _enqueue_op(self, fut: OpFuture) -> None:
        id_ = fut.id
        now = self._now_us
        op = fut.params
        match op:
            case OwnedChannelCreate(reference, dtype, name):
                if reference.channel_id != id_:
                    msg = "stream allocation identity mismatch"
                    raise InvalidHandleError(msg)
                self._state.channels[id_] = (
                    OwnedSignalState(reference, dtype)
                    if reference.direction == "signal.receiver"
                    else OwnedChannelState(reference, dtype)
                )
                owned_creation: StreamCreateEntry = {
                    "id": id_,
                    "ts": now,
                    "source": "task",
                    "type": "stream.create",
                    "owned": reference.fields(),
                }
                if name is not None:
                    owned_creation["name"] = name
                await self._enqueue_log(owned_creation)
            case OwnedStreamNext(reader):
                if reader.stream.value not in self._state.channels:
                    msg = "reader references an unknown owned stream"
                    raise InvalidHandleError(msg)
                self._owned_reads[id_] = fut
            case OwnedChannelExit(stream_id):
                if self._state.channels[stream_id].closed:
                    _ = self._loop.post_completion(id_, result=None)
                    return
                await self._enqueue_log({
                    "id": id_,
                    "ts": now,
                    "source": "task",
                    "type": "stream.complete",
                    "stream_id": stream_id,
                })
            case OwnedSignalSend(reference, payload, key, context):
                fut.params = FnCall(
                    functools.partial(
                        self._execution.send_control, reference, payload, key
                    ),
                    list[object],
                    context,
                    {
                        "name": "duron.signal.send:"
                        + digest(
                            "duron.signal.post.v0", [reference.fields(), key, payload]
                        )
                    },
                )
                await self._enqueue_op(fut)
            case FnCall() | WaitCall() | EffectCall():
                assert not fut.external, "FnCall futures should not be external"
                promise_create_entry: PromiseCreateEntry = {
                    "id": id_,
                    "ts": now,
                    "source": "task",
                    "type": "promise.create",
                }
                if isinstance(op, FnCall) and op.ref is not None:
                    promise_create_entry["ref"] = op.ref
                if isinstance(op, EffectCall):
                    promise_create_entry["ref"] = {"name": op.operation.name}
                    for reference in op.operation.handles:
                        candidate = self._state.channels.get(reference.channel_id)
                        if (
                            candidate is None
                            or not candidate.committed
                            or candidate.closed
                            or reference
                            != replace(
                                candidate.reference,
                                owner_id=id_,
                                direction=reference.direction,
                            )
                            or (
                                not direction_claims_channel(reference.direction)
                                and not isinstance(candidate, OwnedSignalState)
                            )
                            or (
                                direction_claims_channel(reference.direction)
                                and reference.channel_id in self._state.channel_claims
                            )
                        ):
                            msg = "stream is closed, incompatible, or already owned"
                            _ = self._loop.post_completion(
                                id_, exception=InvalidHandleError(msg)
                            )
                            return
                    for reference in op.operation.handles:
                        if direction_claims_channel(reference.direction):
                            self._state.channel_claims[reference.channel_id] = id_
                    args, kwargs = event_arguments(
                        self._executor.registration(op.operation.name),
                        op.operation.inputs,
                    )
                    promise_create_entry["args"] = args
                    promise_create_entry["kwargs"] = kwargs

                operation = Operation(op, fut)
                self._state.operations[id_] = operation
                await self._enqueue_log(promise_create_entry)

                self._task_manager.add_pending(
                    operation,
                    functools.partial(self._execution.execute, operation)
                    if isinstance(op, EffectCall)
                    else functools.partial(self._task_run, id_, op),
                )

                def done(f: OpFuture) -> None:
                    if f.cancelled():
                        if isinstance(op, EffectCall) and not operation.started:
                            return
                        self._task_manager.cancel_task(f.id)

                fut.add_done_callback(done)

            case StreamCreate():
                assert not fut.external, "StreamCreate futures should not be external"

                stream_create_entry: StreamCreateEntry = {
                    "ts": now,
                    "id": id_,
                    "type": "stream.create",
                    "source": "task",
                }
                if op.name is not None:
                    stream_create_entry["name"] = op.name

                self._streams[id_] = op.dtype

                await self._enqueue_log(stream_create_entry)

            case StreamEmit(stream_id):
                stream_type = self._streams.get(stream_id)
                if stream_type is None:
                    # The stream is unknown or already closed: reject before
                    # appending so a late or duplicate event never enters the
                    # log. Stream state derives from the replayed log, so the
                    # rejection is identical in live runs and replays.
                    _ = self._loop.post_completion(
                        id_, exception=ValueError("Stream not found")
                    )
                    return
                try:
                    encoded = self._codec.encode_json(op.value, stream_type)
                except (TypeError, ValueError) as error:
                    # Validation belongs to the awaiting workflow, including
                    # when replay generates the same rejected emission.
                    _ = self._loop.post_completion(id_, exception=error)
                    return
                stream_emit_entry: StreamEmitEntry = {
                    "ts": now,
                    "id": id_,
                    "stream_id": stream_id,
                    "type": "stream.emit",
                    "value": encoded,
                    "source": "effect" if fut.external else "task",
                }
                await self._enqueue_log(stream_emit_entry)
            case Barrier():
                assert not fut.external, "Barrier futures should not be external"
                barrier_entry: BarrierEntry = {
                    "ts": now,
                    "id": id_,
                    "type": "barrier",
                    "source": "task",
                }
                await self._enqueue_log(barrier_entry)
            case _:
                assert_never(op)

    async def _enqueue_log(self, entry: Entry) -> None:
        if self._append_error is not None:
            raise self._append_error
        if (
            self._state.phase is Phase.LIVE
            and self._draining.is_set()
            and entry["type"] == "promise.create"
            and (operation := self._state.operations.get(entry["id"])) is not None
            and operation.effect is not None
        ):
            # Never commit a call that shutdown will prevent from executing.
            # This also covers calls generated beyond the replay frontier.
            return
        if self._state.phase is Phase.REPLAYING:
            self._replay.pending[entry["id"]] = entry
        elif self._claim_token is None:
            if self._state.phase is Phase.CLOSED:
                return
            msg = "Cannot append without an active backend token"
            raise RuntimeError(msg)
        else:
            if (
                entry["type"] == "stream.complete"
                and entry["source"] == "task"
                and entry["stream_id"] in self._state.channels
            ):
                channel = self._state.channels[entry["stream_id"]]
                if (
                    not isinstance(channel, OwnedSignalState)
                    and channel.owner_id is not None
                    and channel.owner_id in self._state.operations
                ):
                    self._task_manager.start()
                    await self._task_manager.join_task(channel.owner_id)
                async with self._state.channel_gate:
                    if channel.closed:
                        _ = self._loop.post_completion(entry["id"], result=None)
                        return
                    await self._writer.append([entry])
                return
            await self._writer.append([entry])

    def _abort_append(self, error: ClaimLostError | StorageError) -> None:
        # Stop further writes and propagate the infrastructure error to the engine.
        self._append_error = error
        if self._task is not None and asyncio.current_task() is not self._task:
            self._task.cancel()

    def _notify_append(self) -> None:
        self._activity.set()
        self._state.append_version += 1
        waiters = self._append_waiters
        self._append_waiters = set()
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def is_terminal(self) -> bool:
        """Whether the run has reached a terminal state.

        Once terminal, no further entries will be appended; host-side readers
        use this to stop after draining the log tail.

        Returns:
            True if the durable function has finished (in any way).

        """
        if self._task is not None:
            return self._task.done()
        return self._main.done()

    @property
    def infrastructure_error(self) -> BaseException | None:
        return self._append_error or self._execution_error

    async def wait_for_append(self, seen_version: int) -> int:
        """Wait until a new entry is appended or the run ends.

        Args:
            seen_version: The last ``append_version`` the caller observed.

        Returns:
            The current ``append_version`` once it advances past ``seen_version``
            or the run reaches a terminal state.

        Raises:
            asyncio.CancelledError: If the waiting coroutine is cancelled.

        """
        while self._state.append_version <= seen_version and not self.is_terminal():
            # Each waiter gets its own future so that one caller's cancellation
            # (e.g. a reader's wait_for timeout) cannot poison other waiters.
            waiter = asyncio.get_running_loop().create_future()
            self._append_waiters.add(waiter)
            try:
                await waiter
            except asyncio.CancelledError:
                self._append_waiters.discard(waiter)
                raise
        return self._state.append_version

    def check_ingress(self) -> None:
        if (
            self.closed
            or self._claim_token is None
            or self._execution_error is not None
        ):
            msg = "run owner is unavailable"
            raise RunUnavailableError(msg)
        self._writer.check_open()

    def _apply_live(self, offset: int, entry: Entry) -> None:
        _ = self.apply_committed(offset, entry)
        if not self._loop.is_closed():
            # Preserve the same ordered readiness as replay between batch entries.
            _ = self._loop.advance()

    def apply_committed(self, offset: int, e: Entry) -> bool:
        """Validate and apply one committed event in replay or live execution.

        Returns:
            Whether the event matched a generated operation or runtime endpoint.

        Raises:
            CorruptLogError: If the event violates committed state.
            HistoryMismatchError: If an effect differs from the workflow.

        """
        if offset <= self._state.frontier:
            raise CorruptLogError(offset, "history offsets are not strictly increasing")
        self._state.frontier = offset
        id_ = e["id"]
        if e["type"] == "input.accept":
            if e["closed"]:
                self._state.closed_inputs.add(e["port"])
            return True
        if e["type"] == "promise.complete" and e["promise_id"] == self._state.seed:
            if self._state.phase is not Phase.CLOSED:
                self._state.phase = Phase.FINISHED
            return True
        if e["type"] == "promise.complete":
            id_ = e["promise_id"]
            operation = self._state.operations.get(id_)
            if operation is None or operation.committed is None:
                raise CorruptLogError(
                    offset, "completion references an absent or completed operation"
                )
            effect = operation.effect
            if effect is not None and any(
                direction_claims_channel(reference.direction)
                and not self._state.channels[reference.channel_id].closed
                for reference in effect.operation.handles
            ):
                raise CorruptLogError(
                    offset, "effect completion leaves owned streams open"
                )
            del self._state.operations[id_]
            return_type = operation.parameters.return_type
            self._task_manager.complete_task(id_)
            if self._loop.is_closed():
                return False
            if "result" in e:
                try:
                    codec = DefaultCodec if effect is None else self._codec
                    result = codec.decode_json(e["result"], return_type)
                    return self._loop.post_completion(id_, result=result)
                except Exception as exc:
                    if effect is not None:
                        raise CorruptLogError(
                            offset,
                            "effect result cannot be decoded with the workflow codec",
                        ) from exc
                    return self._loop.post_completion(id_, exception=exc)
            elif "error" in e:
                return self._loop.post_completion(
                    id_, exception=decode_error(e["error"])
                )
            else:
                msg = (
                    "promise.complete entry carries neither a result nor an "
                    f"error: {e!r}"
                )
                raise CorruptLogError(offset, msg)
        elif e["type"] == "stream.create":
            if "owned" in e:
                created = self._state.channels.get(id_)
                if (
                    created is None
                    or HandleReference.from_fields(e["owned"]) != created.reference
                ):
                    raise CorruptLogError(offset, "owned stream creation mismatch")
                created.committed = True
                return self._loop.post_completion(id_, result=id_)
            if e["id"] not in self._streams:
                return self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
            return self._loop.post_completion(id_, result=e["id"])
        elif e["type"] == "stream.emit":
            if (owned := self._state.channels.get(e["stream_id"])) is not None:
                if owned.closed or (
                    owned.owner_id is None and not isinstance(owned, OwnedSignalState)
                ):
                    raise CorruptLogError(
                        offset, "output targets a closed or unowned stream"
                    )
                _ = self._loop.call_soon(lambda: None)
                return True
            if e["stream_id"] in self._streams:
                return self._loop.post_completion(id_, result=None)
            return self._loop.post_completion(
                id_, exception=ValueError("Stream not found")
            )
        elif e["type"] == "stream.complete":
            if (owned := self._state.channels.get(e["stream_id"])) is not None:
                if owned.closed:
                    raise CorruptLogError(offset, "owned stream has repeated EOF")
                owned.closed = True
                _ = self._loop.call_soon(lambda: None)
                return (
                    self._loop.post_completion(id_, result=None)
                    or e["source"] == "effect"
                )
            if e["stream_id"] in self._streams:
                del self._streams[e["stream_id"]]
                return self._loop.post_completion(id_, result=None)
            return self._loop.post_completion(
                id_, exception=ValueError("Stream not found")
            )
        elif e["type"] == "barrier":
            return self._loop.post_completion(id_, result=(offset, e["ts"]))
        else:
            operation = self._state.operations.get(id_)
            effect = operation.effect if operation is not None else None
            if "args" in e and "kwargs" in e:
                if effect is None:
                    msg = "persisted effect call has no matching registered operation"
                    raise HistoryMismatchError(msg)
                expected = effect.operation
                args, kwargs = event_arguments(
                    self._executor.registration(expected.name), expected.inputs
                )
                if e["args"] != args or e["kwargs"] != kwargs:
                    msg = "effect inputs mismatch"
                    raise HistoryMismatchError(msg)
                for reference in expected.handles:
                    channel = self._state.channels[reference.channel_id]
                    if not direction_claims_channel(reference.direction):
                        continue
                    if channel.owner_id is not None:
                        raise CorruptLogError(
                            offset, "stream already has a committed owner"
                        )
                    channel.owner_id = id_
            elif effect is not None:
                msg = "registered effect call is missing its durable identity"
                raise HistoryMismatchError(msg)
            if operation is None or operation.committed is not None:
                raise CorruptLogError(
                    offset, "creation references an absent or committed operation"
                )
            operation.committed = e
            return True

    async def _poll_wait(self, id_: str, op: WaitCall[object]) -> object:
        operation = self._state.operations[id_]
        try:
            while True:
                if self._parking:
                    await asyncio.sleep(0.01)
                    continue
                operation.waiting_version = None
                version = self._state.append_version
                result = await op.callable()
                if result is not WaitPending.PENDING:
                    # The host may have spent hours unloaded. Advance before
                    # recording completion so subsequent durable timers start now.
                    self._now_us = max(self._now_us, time.time_ns() // 1000)
                    return result
                operation.waiting_version = version
                self._activity.set()
                if not op.suspendable:
                    await asyncio.sleep(0.01)
                    continue
                timeout = (
                    max(0, op.deadline - time.time())
                    if op.deadline is not None
                    else None
                )
                try:
                    async with asyncio.timeout(timeout):
                        await self.wait_for_append(version)
                except TimeoutError:
                    pass
        finally:
            operation.waiting_version = None

    async def _task_run(self, id_: str, op: FnCall[object] | WaitCall[object]) -> None:
        entry: PromiseCompleteEntry = {
            "ts": -1,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": id_,
            "source": "effect",
        }
        try:
            if isinstance(op, WaitCall):
                result = await self._poll_wait(id_, op)
            elif inspect.iscoroutinefunction(op.callable):
                result = await op.callable()
            else:
                result = op.callable()
            entry["result"] = DefaultCodec.encode_json(result, op.return_type)
        except StorageError:
            # Leave this operation pending so recovery retries the storage read.
            raise
        except (Exception, asyncio.CancelledError) as e:  # ruff: ignore[blind-except]
            entry["error"] = encode_error(e)
        entry["ts"] = self._now_us
        if self._state.phase is Phase.CLOSED or self._loop.is_closed():
            # Shutdown cancellation is local execution cleanup, not a durable
            # effect outcome. Recording it would cancel the workflow on replay.
            return
        self._state.operations[id_].reporting = True
        await self._enqueue_log(entry)

    async def result(self) -> _T:
        """Wait for the durable function to complete and return its result.

        Returns:
            The result of the durable function, raises exception if the function failed.

        """
        task = self._task
        if task is None:
            return self._main.result()
        # The machine owns and joins this task during shutdown. Cancelling an
        # observer must neither cancel execution nor install shield's detached
        # exception reporter: an append interrupted by shutdown can fail after
        # the observer exits, and close() already consumes that failure.
        await asyncio.wait((task,))
        return task.result()

    async def accept_input(
        self, port: str, value: JSONValue, *, event_id: str, closed: bool = False
    ) -> None:
        validate_id(event_id, "event_id")
        payload = canonical_json([port, closed, None if closed else value])
        async with self._ingress_gate:
            previous = await self._backend.read_event(self._state.run_id, event_id)
            if previous is not None and previous[0] <= self._state.frontier:
                recorded = decode_record(previous[1], previous[0])
                if (
                    recorded["type"] != "input.accept"
                    or canonical_json([
                        recorded["port"],
                        recorded["closed"],
                        recorded.get("value"),
                    ])
                    != payload
                ):
                    msg = "input event ID was reused with different content"
                    raise PostConflictError(msg)
                return
            if port in self._state.closed_inputs:
                if closed:
                    return
                msg = "input port is closed"
                raise PortClosedError(msg)
            if self._state.phase in {Phase.CLOSED, Phase.FINISHED} or self._main.done():
                msg = "run is no longer accepting input"
                raise PortClosedError(msg)
            entry: InputAcceptedEntry = {
                "type": "input.accept",
                "id": event_id,
                "ts": time.time_ns() // 1000,
                "source": "effect",
                "port": port,
                "closed": closed,
            }
            if not closed:
                entry["value"] = value
            await self._writer.append([entry])

    async def finish(self, outcome: PromiseCompleteEntry) -> None:
        async with self._ingress_gate:
            if self._state.phase is Phase.FINISHED:
                msg = "run already has a terminal outcome"
                raise ExecutionCompletedError(msg)
            await self._writer.append([outcome], terminal_state=outcome_state(outcome))

    async def stop_work(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if not self._loop.is_closed():
            self._loop.close()
        await self._task_manager.close()
        await self._writer.drain()


async def _prelude_fn(
    init: Callable[[], InitParams],
    fn: Workflow[..., _T, PortSet],
    executor: EffectRegistry,
    backend: EventStore,
    run_id: str,
    ready: Callable[[], object],
) -> _T:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, EventLoop)

    init_params = init()

    codec = fn.codec
    signature = fn.ref.signature
    bound = signature.bind(*init_params["args"], **init_params["kwargs"])
    for name, value in bound.arguments.items():
        bound.arguments[name] = codec.decode_json(
            value, signature.parameters[name].annotation
        )

    ctx = Context(
        loop, init_params["id"], executor=executor, store=backend, run_id=run_id
    )

    _ = ready()
    return await fn._fn(
        WorkflowContextImpl(ctx, codec, workflow_name=fn.name, ports=fn.ports),
        *bound.args,
        **bound.kwargs,
    )
