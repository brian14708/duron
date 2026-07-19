from __future__ import annotations

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]
import asyncio
import contextlib
import contextvars
import functools
import inspect
import json
import sys
import time
from dataclasses import replace
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Generic, Self, assert_never, cast
from typing_extensions import TypeVar

from duron._execution.errors import decode_error, encode_error
from duron._execution.invocation import EffectRegistry
from duron._execution.models import (
    HandleReference,
    InvocationIdentity,
    canonical_json,
    digest,
    direction_claims_channel,
)
from duron._hints import UnspecifiedType
from duron._history.encoding import encode_entry, random_id
from duron._history.entries import CorruptLogError
from duron._history.pagination import iter_entries
from duron._introspection import inspect_function
from duron._runtime.append import AppendWriter, BatchLimits
from duron._runtime.attempts import AttemptStart, AttemptState
from duron._runtime.channels import OwnedChannelState, OwnedSignalState
from duron._runtime.context import Context
from duron._runtime.effects import ExecutionSession
from duron._runtime.history import (
    CURRENT_VERSION,
    InitParams,
    effect_identity,
    match_pending,
    read_header,
    read_outcome,
    record_effect_identity,
    validate_history,
)
from duron._runtime.leases import acquire_lease
from duron._runtime.loop import EventLoop, create_loop
from duron._runtime.ops import (
    Barrier,
    EffectCall,
    FnCall,
    OpMetadata,
    OwnedChannelCreate,
    OwnedChannelExit,
    OwnedSignalSend,
    OwnedStreamNext,
    StreamCreate,
    StreamEmit,
    WaitCall,
    WaitPending,
    create_op,
)
from duron._runtime.task_manager import TaskManager
from duron.codec import DefaultCodec
from duron.errors import (
    ExecutionCompletedError,
    HistoryMismatchError,
    InvalidHandleError,
    LeaseLostError,
    PortClosedError,
    PostConflictError,
    StorageError,
)
from duron.storage._protocol import LeaseToken, validate_run_id

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine, Iterable
    from types import TracebackType

    from duron._api.workflow import Workflow, _DurableFn
    from duron._hints import JSONValue, TypeHint
    from duron._history.entries import (
        BarrierEntry,
        Entry,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        RunFinishEntry,
        StreamCreateEntry,
        StreamEmitEntry,
    )
    from duron._runtime.loop import OpFuture, WaitSet
    from duron._runtime.ops import Op
    from duron.storage import EventStore

_T = TypeVar("_T")
_R = TypeVar("_R")

_CLAIM_RENEW_INTERVAL = 10.0
_CLAIM_RENEW_TIMEOUT = 5.0


class _Phase(Enum):
    REPLAYING = auto()
    LIVE = auto()
    FINISHED = auto()
    CLOSED = auto()


class RunMachine(Generic[_T]):
    """One leased run: replay, execution, ports, and terminal transitions."""

    __slots__ = (
        "_accepted_inputs",
        "_append_error",
        "_append_version",
        "_append_waiters",
        "_attempts",
        "_backend",
        "_batch_limits",
        "_channel_claims",
        "_channel_gate",
        "_closed_inputs",
        "_codec",
        "_effects",
        "_execution",
        "_execution_error",
        "_executor",
        "_frontier",
        "_ingress_gate",
        "_internal_calls",
        "_lease",
        "_loop",
        "_main",
        "_now_us",
        "_owned_channels",
        "_owned_reads",
        "_parking",
        "_pending_msg",
        "_phase",
        "_ready",
        "_renewal",
        "_run_id",
        "_running_effects",
        "_session_id",
        "_streams",
        "_task",
        "_task_manager",
        "_waiting",
        "_writer",
        "ingress",
        "workflow",
    )

    workflow: Workflow[..., _T, Any]
    ingress: asyncio.Lock

    def __init__(
        self,
        loop: EventLoop,
        backend: EventStore,
        run_id: str,
        lease: LeaseToken,
        init: Callable[[], InitParams],
        fn: _DurableFn[..., _T],
        batch_limits: BatchLimits,
        executor: EffectRegistry,
    ) -> None:
        self._loop = loop
        self._backend = backend
        self._run_id = run_id
        self._lease: LeaseToken | None = lease
        self._batch_limits = batch_limits
        self._phase = _Phase.REPLAYING
        self._ingress_gate = asyncio.Lock()
        self._accepted_inputs: dict[tuple[str, str], bytes] = {}
        self._closed_inputs: set[str] = set()
        self._now_us: int = 0
        self._append_error: LeaseLostError | StorageError | None = None
        self._execution_error: BaseException | None = None
        self._effects: dict[str, EffectCall] = {}
        self._frontier = -1
        self._owned_channels: dict[str, OwnedChannelState] = {}
        self._channel_gate = asyncio.Lock()
        self._owned_reads: dict[str, OpFuture] = {}
        self._channel_claims: dict[str, str] = {}
        self._executor = executor
        self._attempts: dict[str, AttemptState] = {}
        self._running_effects: set[str] = set()
        self._session_id = random_id()
        self._pending_msg: list[Entry] = []
        self._parking = False
        self._waiting: dict[str, int] = {}
        # Host-reader notification: bumped on every durable append and on
        # completion, so readers can await new entries instead of polling.
        self._append_version: int = 0
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
        self._internal_calls: set[str] = set()
        self._streams: dict[str, TypeHint[Any]] = {}
        self._task: asyncio.Task[_T] | None = None
        self._renewal: asyncio.Task[None] | None = None

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
            lease,
            batch_limits,
            self._apply_committed,
            self._notify_append,
            self._abort_append,
        )
        self._execution = ExecutionSession(self)
        # Wake host readers when the run reaches a terminal state even if no
        # further entry is appended.

        def _on_main_done(_f: object) -> None:
            if self._phase is _Phase.REPLAYING:
                self._notify_append()

        main.add_done_callback(_on_main_done)

    @staticmethod
    async def load(
        store: EventStore,
        workflow: Workflow[..., _R, Any],
        run_id: str,
        inputs: bytes,
        *,
        run_seed: str | None = None,
    ) -> RunMachine[_R]:
        validate_run_id(run_id)
        loop = await create_loop()
        lease: LeaseToken | None = None
        try:
            lease = await acquire_lease(store, run_id)
            ref = workflow.ref
            header = await read_header(store, run_id=run_id)
            args, kwargs = ref._invocation(inputs)
            if header is None:
                initial: InitParams = {
                    "version": CURRENT_VERSION,
                    "args": args,
                    "kwargs": kwargs,
                    "nonce": run_seed or random_id(),
                    "workflow_name": ref.name,
                    "workflow_version": ref.version,
                    "contract": cast("JSONValue", json.loads(ref._identity)),
                }
                await store.create(
                    run_id,
                    encode_entry({
                        "type": "run.create",
                        "id": random_id(),
                        "source": "effect",
                        "ts": time.time_ns() // 1000,
                        "result": cast("JSONValue", initial),
                    }),
                )
                header = await read_header(store, run_id=run_id)
                assert header is not None
            if (
                canonical_json(header.get("contract")) != ref._identity
                or canonical_json(header["args"]) != canonical_json(args)
                or canonical_json(header["kwargs"]) != canonical_json(kwargs)
            ):
                msg = "persisted history does not match the submitted invocation"
                raise HistoryMismatchError(msg)  # ruff: ignore[raise-within-try]
            machine = RunMachine[_R](
                loop,
                store,
                run_id,
                lease,
                lambda: cast("InitParams", header),
                workflow._durable,
                BatchLimits(),
                EffectRegistry(),
            )
            machine.workflow = workflow
            machine.ingress = asyncio.Lock()
        except BaseException:
            loop.close()
            if lease is not None:
                await store.release(run_id, lease)
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
            await cleanup
            raise

    @property
    def closed(self) -> bool:
        return self._phase is _Phase.CLOSED

    @property
    def run_id(self) -> str:
        return self._run_id

    async def run(
        self,
        cancelled: Callable[[], Awaitable[bool]],
        serve: Callable[[RunMachine[Any]], Awaitable[None]],
        suspend: Callable[[float | None], Awaitable[bool]] | None = None,
        *,
        idle_timeout: float = 60.0,
    ) -> bool:
        """Run to completion or durable suspension.

        Returns:
            True on completion, false on suspension.

        Raises:
            RuntimeError: If the delivery service stops unexpectedly.

        """
        if await read_outcome(self._backend, self._run_id) is not None:
            self._phase = _Phase.FINISHED
            return True
        ref = self.workflow.ref
        outcome: RunFinishEntry = {
            "type": "run.finish",
            "id": random_id(),
            "source": "effect",
            "ts": time.time_ns() // 1000,
            "cancelled": False,
        }
        if await cancelled():
            outcome["cancelled"] = True
            await self.finish(outcome)
            return True
        try:
            await self.replay()
        except asyncio.CancelledError:
            self._workflow_cancelled(outcome)
            await self.finish(outcome)
            return True
        except Exception as error:  # ruff: ignore[blind-except]
            self._application_failure(outcome, error)
            await self.finish(outcome)
            return True
        service = asyncio.ensure_future(serve(self))
        completion = asyncio.create_task(self.result())
        idle_since: float | None = None
        try:
            while not completion.done():
                done, _pending = await asyncio.wait(
                    (completion, service),
                    timeout=0.02,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if service in done:
                    service.result()
                    msg = "execution delivery service stopped unexpectedly"
                    raise RuntimeError(msg)
                if await cancelled():
                    async with self.ingress:
                        if completion.done():
                            break
                        await self.stop_work()
                        outcome["cancelled"] = True
                    break
                if suspend is not None:
                    idle_since, ready = self._idle_state(idle_since, idle_timeout)
                    if ready and await self._suspend(suspend):
                        return False
                    if idle_since is None:
                        idle_since = None
            if not outcome["cancelled"]:
                try:
                    result = await completion
                except asyncio.CancelledError:
                    self._workflow_cancelled(outcome)
                except Exception as error:  # ruff: ignore[blind-except]
                    self._application_failure(outcome, error)
                else:
                    try:
                        outcome["result"] = ref.codec.encode_json(
                            result, ref.result_type
                        )
                    except (TypeError, ValueError) as error:
                        self._application_failure(outcome, error)
            async with self.ingress:
                outcome["ts"] = time.time_ns() // 1000
                await self.finish(outcome)
            return True
        finally:
            service.cancel()
            completion.cancel()
            await asyncio.gather(service, completion, return_exceptions=True)

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
            self._phase is not _Phase.LIVE
            or self._main.done()
            or self.infrastructure_error is not None
            or self._effects
            or not self._loop.is_quiescent()
            or not self._internal_calls.issubset(self._waiting)
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
                    or self._waiting.get(future.id) != self._append_version
                    or (op.deadline is not None and op.deadline <= time.time())
                ):
                    return False
            else:
                return False
        return True

    @staticmethod
    def _workflow_cancelled(outcome: RunFinishEntry) -> None:
        owner = asyncio.current_task()
        if owner is not None and owner.cancelling():
            raise asyncio.CancelledError
        outcome["cancelled"] = True

    def _application_failure(self, outcome: RunFinishEntry, error: Exception) -> None:
        if self.infrastructure_error is not None or isinstance(
            error, HistoryMismatchError
        ):
            raise error
        outcome["error"] = encode_error(error)

    async def _resume(self) -> bool:
        recvd_msgs: dict[str, tuple[str, tuple[object, ...] | None]] = {}
        ws: WaitSet | None = None
        async for o, entry in iter_entries(self._backend, self._run_id):
            if entry["type"] == "run.create":
                continue
            self._execution.apply(o, entry)
            ts = entry["ts"]
            while ws and ws.timer and ws.timer < ts:
                self._now_us = max(self._now_us, ws.timer)
                ws = await self._step()

            self._now_us = max(self._now_us, ts)
            ws = await self._step()
            self._raise_history_error()
            if entry["source"] == "task":
                handled = self._handle_message(o, entry)
                # Cancellation may remove a waiter after its operation was
                # admitted to storage. Replay must still match that generated
                # operation even when there is no waiter left to complete.
                identity: dict[str, tuple[str, tuple[object, ...] | None]] = {
                    entry["id"]: (entry["type"], effect_identity(entry))
                }
                generated = not handled and any(
                    match_pending(pending, identity) for pending in self._pending_msg
                )
                if not handled and not generated:
                    msg = "Extra messages found in log"
                    raise HistoryMismatchError(msg, actual=entry["id"])
                # Retain only identity fields needed by match_pending.
                recvd_msgs[entry["id"]] = (entry["type"], effect_identity(entry))
            else:
                _ = self._handle_message(o, entry)
            ws = await self._step()
            self._raise_history_error()
            # Drain the matched tail: pending ops are emitted in log order, so
            # matches accumulate as a contiguous suffix.
            while self._pending_msg and match_pending(
                self._pending_msg[-1], recvd_msgs
            ):
                self._pending_msg.pop()

        # Final pass for matches the tail drain could not reach. Replay routinely
        # runs the workflow ahead of the log, so the pending queue can hold
        # unmatched ops past the crash point while recorded entries for earlier
        # ops are still unreconciled; the tail drain stops at the first non-match
        # from the end and never sees those.
        self._pending_msg = [
            msg for msg in self._pending_msg if not match_pending(msg, recvd_msgs)
        ]

        if len(recvd_msgs) > 0:
            msg = "Extra messages found in log"
            raise HistoryMismatchError(msg, actual=sorted(recvd_msgs))

        return self._main.done() and len(self._pending_msg) == 0

    def _raise_history_error(self) -> None:
        if self._main.done() and not self._main.cancelled():
            error = self._main.exception()
            if isinstance(error, HistoryMismatchError):
                raise error

    async def replay(self) -> None:
        """Replay history and start unfinished work.

        Raises:
            RuntimeError: If this machine has already started or closed.

        """
        if self._phase is not _Phase.REPLAYING or self._renewal is not None:
            msg = "run machine has already started or closed"
            raise RuntimeError(msg)
        self._renewal = asyncio.create_task(self._renew_claim())
        if await self._resume():
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

    async def _renew_claim(self) -> None:
        try:
            while True:
                await asyncio.sleep(_CLAIM_RENEW_INTERVAL)
                if self._lease is None:
                    return
                async with asyncio.timeout(_CLAIM_RENEW_TIMEOUT):
                    await self._backend.renew(self._run_id, self._lease)
        except TimeoutError as error:
            failure = StorageError("history claim renewal timed out")
            failure.__cause__ = error
            self._writer.fail(failure)
        except StorageError as error:
            self._writer.fail(error)

    async def _run(self) -> _T:
        if self._main.done():
            return self._main.result()

        try:
            self._phase = _Phase.LIVE
            for start in range(
                0, len(self._pending_msg), self._batch_limits.max_entries
            ):
                await self._enqueue_ready(
                    self._enqueue_log(msg)
                    for msg in self._pending_msg[
                        start : start + self._batch_limits.max_entries
                    ]
                )
            self._pending_msg.clear()
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
        if self._phase is _Phase.CLOSED:
            return
        self._phase = _Phase.CLOSED
        if self._renewal is not None:
            self._renewal.cancel()
            await asyncio.gather(self._renewal, return_exceptions=True)
            self._renewal = None
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
            if self._lease:
                await self._backend.release(self._run_id, self._lease)
                self._lease = None

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

    async def _step(self) -> WaitSet | None:
        self._loop.tick(self._now_us)

        while True:
            frontier = self._frontier
            await self._serve_stream_reads()
            # Storage reads yield to reporters. Rescan when a commit advanced
            # readiness before poll_completion clears the wakeup event.
            if self._frontier != frontier:
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
            if self._phase is _Phase.LIVE:
                self._task_manager.start()

    async def _serve_stream_reads(self) -> None:
        for id_, fut in tuple(self._owned_reads.items()):
            if fut.cancelled():
                del self._owned_reads[id_]
                continue
            op = cast("OwnedStreamNext", fut.params)
            state = self._owned_channels[op.reader.stream.value]
            await op.reader.fill(
                self._backend,
                self._run_id,
                self._codec,
                self._frontier,
                state.error_types,
            )
            if op.reader.values:
                _ = self._loop.post_completion(id_, result=op.reader.values.popleft())
                del self._owned_reads[id_]

    async def _enqueue_op(self, fut: OpFuture) -> None:
        id_ = fut.id
        now = self._now_us
        op = cast("Op", fut.params)
        match op:
            case OwnedChannelCreate(reference, dtype, name):
                if reference.channel_id != id_:
                    msg = "stream allocation identity mismatch"
                    raise InvalidHandleError(msg)
                self._owned_channels[id_] = (
                    OwnedSignalState(reference, dtype)
                    if reference.direction == "signal.receiver"
                    else OwnedChannelState(reference, dtype)
                )
                owned_creation: StreamCreateEntry = {
                    "id": id_,
                    "ts": now,
                    "source": "task",
                    "type": "stream.create",
                    "name": name,
                    "owned": reference.fields(),
                }
                await self._enqueue_log(owned_creation)
            case OwnedStreamNext(reader):
                if reader.stream.value not in self._owned_channels:
                    msg = "reader references an unknown owned stream"
                    raise InvalidHandleError(msg)
                self._owned_reads[id_] = fut
            case OwnedChannelExit(stream_id):
                await self._enqueue_log({
                    "id": id_,
                    "ts": now,
                    "source": "task",
                    "type": "stream.scope.exit",
                    "stream_id": stream_id,
                })
            case OwnedSignalSend(reference, payload, key, context):
                metadata = OpMetadata(
                    "signal.send",
                    "duron.signal.send",
                    digest("duron.signal.post.v0", [reference.fields(), key, payload]),
                )
                fut.params = FnCall(
                    functools.partial(
                        self._execution._send_control, reference, payload, key
                    ),
                    list[object],
                    context,
                    metadata,
                )
                await self._enqueue_op(fut)
            case FnCall() | WaitCall() | EffectCall():
                assert not fut.external, "FnCall futures should not be external"
                if isinstance(op, (FnCall, WaitCall)):
                    self._internal_calls.add(id_)
                promise_create_entry = _promise_create_entry(id_, now)
                record_effect_identity(promise_create_entry, op.metadata)
                if isinstance(op, EffectCall):
                    for reference in op.operation.handles:
                        candidate = self._owned_channels.get(reference.channel_id)
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
                                and reference.channel_id in self._channel_claims
                            )
                        ):
                            msg = "stream is closed, incompatible, or already owned"
                            _ = self._loop.post_completion(
                                id_, exception=InvalidHandleError(msg)
                            )
                            return
                    for reference in op.operation.handles:
                        if direction_claims_channel(reference.direction):
                            self._channel_claims[reference.channel_id] = id_
                    self._effects[id_] = op
                    self._attempts[id_] = AttemptState()
                    promise_create_entry["effect_call"] = {
                        "format_version": 0,
                        "descriptor": cast(
                            "JSONValue", json.loads(op.operation.descriptor)
                        ),
                        "input_digest": op.operation.input_digest,
                        "run_seed": op.operation.seed,
                        "inputs": cast("JSONValue", json.loads(op.operation.inputs)),
                    }

                await self._enqueue_log(promise_create_entry)

                self._task_manager.add_pending(
                    id_,
                    functools.partial(self._execution._effect_run, id_, op, fut)
                    if isinstance(op, EffectCall)
                    else functools.partial(self._task_run, id_, op),
                    op.context,
                    op.return_type,
                    fut,
                    op.metadata.error_types,
                    run_cancelled=isinstance(op, EffectCall),
                )

                def done(f: OpFuture) -> None:
                    if f.cancelled():
                        if (
                            isinstance(op, EffectCall)
                            and f.id not in self._running_effects
                        ):
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
                    "name": op.name,
                }
                record_effect_identity(stream_create_entry, op.metadata)

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
                encoded = self._codec.encode_json(op.value, stream_type)
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
        if self._phase is _Phase.REPLAYING:
            self._pending_msg.append(entry)
        elif self._lease is None:
            if self._phase is _Phase.CLOSED:
                return
            msg = "Cannot append without an active backend lease"
            raise RuntimeError(msg)
        else:
            if entry["type"] == "stream.scope.exit":
                channel = self._owned_channels[entry["stream_id"]]
                if (
                    not isinstance(channel, OwnedSignalState)
                    and channel.owner_id is not None
                    and channel.owner_id in self._effects
                ):
                    self._task_manager.start()
                    await self._task_manager.join_task(channel.owner_id)
                async with self._channel_gate:
                    await self._writer.append([entry])
                return
            await self._writer.append([entry])

    def _abort_append(self, error: LeaseLostError | StorageError) -> None:
        # Stop further writes and propagate the infrastructure error to the engine.
        self._append_error = error
        if self._task is not None and asyncio.current_task() is not self._task:
            self._task.cancel()

    def _notify_append(self) -> None:
        self._append_version += 1
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

    def workflow_finished(self) -> bool:
        """Observe workflow completion separately from writer/reporting cleanup.

        Returns:
            Whether the workflow body has finished.

        """
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
        while self._append_version <= seen_version and not self.is_terminal():
            # Each waiter gets its own future so that one caller's cancellation
            # (e.g. a reader's wait_for timeout) cannot poison other waiters.
            waiter = asyncio.get_running_loop().create_future()
            self._append_waiters.add(waiter)
            try:
                await waiter
            except asyncio.CancelledError:
                self._append_waiters.discard(waiter)
                raise
        return self._append_version

    def _apply_committed(self, offset: int, entry: Entry) -> None:
        self._execution.apply(offset, entry)
        _ = self._handle_message(offset, entry)
        if not self._loop.is_closed():
            # Replay advances after every record. Live batches must expose the
            # same ordered readiness to workflow tasks, including cancellation.
            _ = self._loop.advance()

    def _handle_message(self, offset: int, e: Entry) -> bool:
        self._frontier = offset
        id_ = e["id"]
        if e["type"] == "input.accept":
            if e["retry_id"] is not None:
                self._accepted_inputs[e["port"], e["retry_id"]] = canonical_json(
                    e["value"]
                )
            if e["closed"]:
                self._closed_inputs.add(e["port"])
            return True
        if e["type"] == "run.finish":
            if self._phase is not _Phase.CLOSED:
                self._phase = _Phase.FINISHED
            return True
        if e["type"] == "promise.complete":
            id_ = e["promise_id"]
            pending = self._effects.get(id_)
            if pending is not None and any(
                direction_claims_channel(reference.direction)
                and not self._owned_channels[reference.channel_id].closed
                for reference in pending.operation.handles
            ):
                raise CorruptLogError(
                    offset, "effect completion leaves owned streams open"
                )
            effect = self._effects.pop(id_, None)
            internal = id_ in self._internal_calls
            self._internal_calls.discard(id_)
            self._waiting.pop(id_, None)
            if effect is not None:
                state = self._attempts.pop(id_)
                _ = state.complete(offset=offset)
                self._running_effects.discard(id_)
            try:
                return_type, error_types = self._task_manager.complete_task(id_)
            except ValueError as completion_error:
                raise CorruptLogError(
                    offset, "completion references an absent or completed operation"
                ) from completion_error
            if self._loop.is_closed():
                return False
            if "result" in e:
                try:
                    codec = DefaultCodec if internal else self._codec
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
                    id_, exception=decode_error(e["error"], error_types)
                )
            else:
                msg = (
                    "promise.complete entry carries neither a result nor an "
                    f"error: {e!r}"
                )
                raise CorruptLogError(offset, msg)
        elif e["type"] == "stream.create":
            if "owned" in e:
                created = self._owned_channels.get(id_)
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
            if (owned := self._owned_channels.get(e["stream_id"])) is not None:
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
            if (owned := self._owned_channels.get(e["stream_id"])) is not None:
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
        elif e["type"] == "stream.scope.exit":
            channel = self._owned_channels.get(e["stream_id"])
            if channel is None:
                raise CorruptLogError(
                    offset, "scope exit references an unknown channel"
                )
            if not channel.closed:
                channel.closed = True
                _ = self._loop.call_soon(lambda: None)
            return self._loop.post_completion(id_, result=None)
        elif e["type"] == "barrier":
            return self._loop.post_completion(id_, result=(offset, e["ts"]))
        elif e["type"] == "promise.create":
            if "effect_call" in e:
                effect = self._effects.get(id_)
                if effect is None:
                    msg = "persisted effect call has no matching registered operation"
                    raise HistoryMismatchError(msg)
                data = e["effect_call"]
                expected = effect.operation
                if (
                    data["descriptor"] != json.loads(expected.descriptor)
                    or data["input_digest"] != expected.input_digest
                    or data["run_seed"] != expected.seed
                    or canonical_json(data["inputs"]) != expected.inputs
                ):
                    msg = "effect descriptor or input digest mismatch"
                    raise HistoryMismatchError(
                        msg,
                        expected=json.loads(expected.descriptor),
                        actual=data["descriptor"],
                    )
                for reference in expected.handles:
                    channel = self._owned_channels[reference.channel_id]
                    if not direction_claims_channel(reference.direction):
                        continue
                    if channel.owner_id is not None:
                        raise CorruptLogError(
                            offset, "stream already has a committed owner"
                        )
                    channel.owner_id = id_
                    channel.error_types = expected.raises
            elif id_ in self._effects:
                msg = "registered effect call is missing its durable descriptor"
                raise HistoryMismatchError(msg)
            return True
        elif e["type"] == "effect.attempt.start":
            id_ = e["promise_id"]
            effect = self._effects.get(id_)
            attempt_state = self._attempts.get(id_)
            if effect is None or attempt_state is None:
                raise CorruptLogError(
                    offset, "effect attempt references an absent or completed effect"
                )
            inv = e["invocation"]
            invocation = InvocationIdentity(
                inv["invocation_number"], inv["recovery_count"]
            )
            for reference in effect.operation.handles:
                channel = self._owned_channels[reference.channel_id]
                if reference.direction == "signal.receiver" and isinstance(
                    channel, OwnedSignalState
                ):
                    channel.read_offset = None
            self._attempts[id_] = attempt_state.apply_start(
                AttemptStart(invocation, e["session_id"], e["ts"]), offset=offset
            )
            return True
        else:
            return True

    async def _poll_wait(self, id_: str, op: WaitCall) -> object:
        try:
            while True:
                if self._parking:
                    await asyncio.sleep(0.01)
                    continue
                self._waiting.pop(id_, None)
                version = self._append_version
                result = await op.callable()
                if result is not WaitPending.PENDING:
                    # The host may have spent hours unloaded. Advance before
                    # recording completion so subsequent durable timers start now.
                    self._now_us = max(self._now_us, time.time_ns() // 1000)
                    return result
                self._waiting[id_] = version
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
            self._waiting.pop(id_, None)

    async def _task_run(self, id_: str, op: FnCall | WaitCall) -> None:
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
        if self._phase is _Phase.CLOSED or self._loop.is_closed():
            # Shutdown cancellation is local execution cleanup, not a durable
            # effect outcome. Recording it would cancel the workflow on replay.
            return
        self._task_manager.reporting(id_)
        await self._enqueue_log(entry)

    async def result(self) -> _T:
        """Wait for the durable function to complete and return its result.

        Returns:
            The result of the durable function, raises exception if the function failed.

        """
        if self._task is None:
            return self._main.result()
        return await asyncio.shield(self._task)

    async def accept_input(
        self, port: str, value: JSONValue, *, retry_id: str | None, closed: bool = False
    ) -> None:
        if retry_id is not None:
            validate_run_id(retry_id)
        payload = canonical_json(value)
        async with self._ingress_gate:
            if retry_id is not None:
                previous = self._accepted_inputs.get((port, retry_id))
                if previous is not None:
                    if previous != payload:
                        msg = "input retry ID was reused with a different value"
                        raise PostConflictError(msg)
                    return
            if port in self._closed_inputs:
                if closed:
                    return
                msg = "input port is closed"
                raise PortClosedError(msg)
            if (
                self._phase in {_Phase.CLOSED, _Phase.FINISHED}
                or self.workflow_finished()
            ):
                msg = "run is no longer accepting input"
                raise PortClosedError(msg)
            await self._writer.append([
                {
                    "type": "input.accept",
                    "id": random_id(),
                    "ts": time.time_ns() // 1000,
                    "source": "effect",
                    "port": port,
                    "value": value,
                    "retry_id": retry_id,
                    "closed": closed,
                }
            ])

    async def finish(self, outcome: RunFinishEntry) -> None:
        async with self._ingress_gate:
            if self._phase is _Phase.FINISHED:
                msg = "run already has a terminal outcome"
                raise ExecutionCompletedError(msg)
            await self._writer.append([outcome])

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
    fn: _DurableFn[..., _T],
    executor: EffectRegistry,
    backend: EventStore,
    run_id: str,
    ready: Callable[[], object],
) -> _T:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, EventLoop)

    init_params: InitParams = await create_op(
        loop,
        FnCall(
            callable=init,
            return_type=InitParams,
            context=contextvars.copy_context(),
            metadata=OpMetadata(name="duron.prelude"),
        ),
    )
    validate_history(init_params)

    codec = fn.codec
    type_info = inspect_function(fn.fn)
    args = (
        codec.decode_json(arg, fn.positional_type(i))
        for i, arg in enumerate(init_params["args"])
    )
    kwargs = {
        k: codec.decode_json(v, type_info.parameter_types.get(k, UnspecifiedType))
        for k, v in init_params["kwargs"].items()
    }

    ctx = Context(
        loop, init_params["nonce"], executor=executor, store=backend, run_id=run_id
    )

    _ = ready()
    return await fn.fn(ctx, *args, **kwargs)


def _promise_create_entry(id_: str, ts: int) -> PromiseCreateEntry:
    return {"ts": ts, "id": id_, "type": "promise.create", "source": "task"}
