from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import hashlib
import hmac
import inspect
import json
import secrets
import sys
import time
from dataclasses import dataclass, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generic,
    NotRequired,
    Self,
    assert_never,
    cast,
    overload,
)
from typing_extensions import ParamSpec, TypedDict, TypeVar

from duron._hints import UnspecifiedType
from duron._introspection import inspect_function
from duron._runtime.execution.errors import decode_error, encode_error
from duron._runtime.execution.executor import (
    EffectRegistry,
    InputSnapshot,
    input_references,
    schema_id,
)
from duron._runtime.execution.handle_data import (
    HandleReference,
    RecoveryDescriptor,
    SignalDelivery,
    SignalId,
    StreamCursor,
    StreamEvent,
    StreamId,
    StreamParameter,
    direction_claims_channel,
)
from duron._runtime.execution.handles import PostingCommandSender
from duron._runtime.execution.models import (
    EffectBodyError,
    EffectContextImpl,
    EffectId,
    ExecutionIdentity,
    InvocationIdentity,
    canonical_json,
    digest,
    forbidden_error,
)
from duron._runtime.execution.posting import (
    PostAuthority,
    PostJournal,
    PostScope,
    PostTarget,
    Receipt,
    SemanticPost,
    validate_post_key,
)
from duron._runtime.execution.reporting import (
    EffectHost,
    EffectReporter,
    ExecutionCancelled,
    ExecutionFailed,
    ExecutionRunning,
    ExecutionStatus,
    ExecutionSucceeded,
    RebindingExecutor,
)
from duron._runtime.execution.request import (
    EffectEnvelope,
    ExecutionRequest,
    decode_request,
    local_request,
    request_context,
)
from duron._runtime.execution.value_schema import validate_value
from duron._runtime.loop import EventLoop, create_loop
from duron._runtime.storage.backend import LeaseToken, validate_run_id
from duron._runtime.storage.entry import POST_ACTION_DIRECTIONS, CorruptLogError
from duron._runtime.storage.helper import decode_record, random_id
from duron._runtime.storage.pagination import iter_entries
from duron._runtime.workflow.append import AppendWriter, BatchLimits
from duron._runtime.workflow.attempts import (
    AttemptStart,
    AttemptState,
    RetryScheduled,
    delay_to_us,
)
from duron._runtime.workflow.context import Context
from duron._runtime.workflow.ops import (
    Barrier,
    EffectCall,
    FnCall,
    FutureCreate,
    OpMetadata,
    OwnedChannelCreate,
    OwnedChannelExit,
    OwnedSignalSend,
    OwnedStreamNext,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron._runtime.workflow.owned_signal import (
    ControlSignalPosting,
    OwnedSignalState,
    SignalReservation,
)
from duron._runtime.workflow.owned_stream import OwnedChannelState
from duron._runtime.workflow.stream import StreamWriter
from duron._runtime.workflow.stream_manager import StreamManager
from duron._runtime.workflow.task_manager import TaskError, TaskManager
from duron.errors import (
    ChannelClosedError,
    EffectOutcomeUnknownError,
    ExecutionCompletedError,
    HistoryMismatchError,
    InvalidExecutionRequestError,
    InvalidHandleError,
    InvalidPostError,
    LeaseLostError,
    PortClosedError,
    RequestAlreadyResolvedError,
    RunUnavailableError,
    SignalMessagePendingError,
    StaleInvocationError,
    StorageError,
    WorkflowDefinitionError,
)
from duron.types import JSONValue

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterable, Sequence
    from types import TracebackType

    from duron._runtime.decorator import DurableFn
    from duron._runtime.execution.effect import Effect
    from duron._runtime.execution.executor import Registration
    from duron._runtime.execution.models import EffectOperation
    from duron._runtime.loop import OpFuture, WaitSet
    from duron._runtime.storage.entry import (
        BarrierEntry,
        EffectAttemptStartEntry,
        EffectCallData,
        Entry,
        ErrorInfo,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        StreamCompleteEntry,
        StreamCreateEntry,
        StreamEmitEntry,
    )
    from duron._runtime.workflow.ops import Op
    from duron.codec import Codec
    from duron.storage import HistoryStore
    from duron.types import TypeHint

_T = TypeVar("_T")
_R = TypeVar("_R")
_Item = TypeVar("_Item")
_P = ParamSpec("_P")

CURRENT_VERSION: Final = 1


@dataclass(frozen=True, slots=True)
class _ExecutionRecord:
    call: EffectCallData
    invocation: InvocationIdentity | None
    completed: PromiseCompleteEntry | None
    recovering: bool


def _effect_now_us() -> int:
    return time.time_ns() // 1_000


async def _wait_effect_deadline(not_before_us: int) -> None:
    remaining = (not_before_us - _effect_now_us()) / 1_000_000
    if remaining > 0:
        await asyncio.sleep(remaining)


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str
    # Declare the API layer's run identity so schema-aware codecs persist it.
    workflow_name: NotRequired[str]
    workflow_version: NotRequired[str]
    workflow_codec_id: NotRequired[str]


class Session:
    __slots__ = (
        "_backend",
        "_batch_limits",
        "_closed",
        "_current_task",
        "_executor",
        "_lease",
        "_loop",
        "_run_id",
    )

    def __init__(self, backend: HistoryStore, /, *, run_id: str) -> None:
        """Session for running durable functions.

        Owns the backend lease and event loop that a spawned :class:`Task`
        takes over; the workflow API layer drives it via :meth:`spawn`.

        Args:
            backend: The backend to use for this session.
            run_id: The selected run within the backend.

        """
        self._closed = False
        self._executor = EffectRegistry()
        validate_run_id(run_id)
        self._backend = backend
        self._batch_limits = BatchLimits()
        self._run_id = run_id
        self._loop: EventLoop | None = None
        self._lease: LeaseToken | None = None
        self._current_task: Task[Any] | None = None

    async def __aenter__(self) -> Self:
        try:
            self._loop = await create_loop()
            self._lease = await self._backend.acquire_lease(self._run_id)
        except BaseException:
            if self._loop is not None and not self._loop.is_closed():
                self._loop.close()
            self._loop = None
            raise
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._current_task:
                await self._current_task.close()
                self._current_task = None
        finally:
            try:
                if self._lease is not None:
                    await self._backend.release_lease(self._run_id, self._lease)
                    self._lease = None
            finally:
                if self._loop:
                    if not self._loop.is_closed():
                        self._loop.close()
                    self._loop = None

    @property
    def backend(self) -> HistoryStore:
        """The backend this session owns."""
        return self._backend

    @property
    def run_id(self) -> str:
        """The selected run within this backend."""
        return self._run_id

    def spawn(self, fn: DurableFn[_P, _T], init: Callable[[], InitParams]) -> Task[_T]:
        if self._current_task is not None:
            msg = "A durable function is already running"
            raise RuntimeError(msg)
        if self._loop is None:
            msg = "Session is not started"
            raise RuntimeError(msg)
        assert self._lease is not None

        task = Task(
            self._loop,
            self._backend,
            self._run_id,
            self._lease,
            init,
            fn,
            self._batch_limits,
            self._executor,
        )
        self._current_task = task
        self._loop = None
        self._lease = None
        return task


class Task(Generic[_T]):
    """A task representing a running durable function within a session."""

    __slots__ = (
        "_append_error",
        "_append_version",
        "_append_waiters",
        "_attempts",
        "_backend",
        "_batch_limits",
        "_channel_claims",
        "_channel_gate",
        "_closed",
        "_codec",
        "_effects",
        "_execution_error",
        "_execution_index",
        "_executor",
        "_executor_api",
        "_external_completion_locks",
        "_frontier",
        "_is_live",
        "_journal",
        "_lease",
        "_loop",
        "_main",
        "_now_us",
        "_owned_channels",
        "_owned_reads",
        "_pending_msg",
        "_ready",
        "_replayed_effects",
        "_run_id",
        "_running_effects",
        "_scope_exits",
        "_session_id",
        "_signing_key",
        "_stream_manager",
        "_stream_writers",
        "_task",
        "_task_manager",
        "_writer",
    )

    def __init__(
        self,
        loop: EventLoop,
        backend: HistoryStore,
        run_id: str,
        lease: LeaseToken,
        init: Callable[[], InitParams],
        fn: DurableFn[_P, _T],
        batch_limits: BatchLimits,
        executor: EffectRegistry,
    ) -> None:
        self._loop = loop
        self._backend = backend
        self._run_id = run_id
        self._lease: LeaseToken | None = lease
        self._batch_limits = batch_limits
        self._closed = False
        self._now_us: int = 0
        self._append_error: LeaseLostError | StorageError | None = None
        self._execution_error: BaseException | None = None
        self._execution_index: dict[str, _ExecutionRecord] = {}
        self._effects: dict[str, EffectCall] = {}
        self._frontier = -1
        self._owned_channels: dict[str, OwnedChannelState] = {}
        self._channel_gate = asyncio.Lock()
        self._owned_reads: dict[str, OpFuture] = {}
        self._scope_exits: dict[str, list[str]] = {}
        self._channel_claims: dict[str, str] = {}
        self._executor = executor
        self._executor_api = EffectHost(self)
        self._signing_key = secrets.token_bytes(32)
        self._attempts: dict[str, AttemptState] = {}
        self._replayed_effects: set[str] = set()
        self._running_effects: set[str] = set()
        self._session_id = random_id()
        self._is_live: bool = False
        self._pending_msg: list[Entry] = []
        self._external_completion_locks: dict[str, asyncio.Lock] = {}
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
                functools.partial(
                    asyncio.get_running_loop().call_soon, self._ready.set
                ),
            )
        )
        self._main = main
        self._codec = fn.codec
        self._stream_manager = StreamManager()
        self._stream_writers: dict[str, StreamWriter[Any]] = {}
        self._task: asyncio.Task[_T] | None = None

        def on_task_error(error: TaskError) -> None:
            # Effect exceptions are already durable outcomes. An exception
            # escaping the worker is an infrastructure failure: stop the host
            # engine before workflow cancellation can become durable history.
            if self._execution_error is None:
                self._execution_error = error.exception
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
        self._journal = PostJournal(self._writer, self, run_id=run_id)
        # Wake host readers when the run reaches a terminal state even if no
        # further entry is appended, and release any host writer blocked in
        # wait_stream for a stream the workflow never created.

        def _on_main_done(_f: object) -> None:
            if not self._is_live:
                self._notify_append()
            self._stream_manager.close()

        main.add_done_callback(_on_main_done)

    async def _resume(self) -> bool:
        recvd_msgs: dict[str, tuple[str, tuple[object, ...] | None]] = {}
        ws: WaitSet | None = None
        async for o, entry in iter_entries(self._backend, self._run_id):
            self._journal.apply(o, entry)
            self._index_execution(o, entry)
            if entry["type"] == "signal.ack":
                signal = self._owned_channels.get(entry["signal_id"])
                if not isinstance(signal, OwnedSignalState):
                    raise CorruptLogError(
                        o, "acknowledgement references an absent signal"
                    )
                expected_delivery = await self._next_signal(signal)
                if expected_delivery is None or (
                    expected_delivery.message_id != entry["message_id"]
                    or expected_delivery.offset != entry["message_offset"]
                ):
                    raise CorruptLogError(o, "signal acknowledgement is not FIFO")
            ts = entry["ts"]
            while ws and ws.timer and ws.timer < ts:
                self._now_us = max(self._now_us, ws.timer)
                ws = await self._step()

            self._now_us = max(self._now_us, ts)
            ws = await self._step()
            self._raise_history_error()
            if entry["source"] == "task":
                if not self._handle_message(o, entry):
                    msg = "Extra messages found in log"
                    raise HistoryMismatchError(msg, actual=entry["id"])
                # Retain only identity fields needed by _match_pending.
                recvd_msgs[entry["id"]] = (entry["type"], _effect_identity(entry))
            else:
                _ = self._handle_message(o, entry)
            ws = await self._step()
            self._raise_history_error()
            # Drain the matched tail: pending ops are emitted in log order, so
            # matches accumulate as a contiguous suffix.
            while self._pending_msg and _match_pending(
                self._pending_msg[-1], recvd_msgs
            ):
                self._pending_msg.pop()

        # Final pass for matches the tail drain could not reach. Replay routinely
        # runs the workflow ahead of the log, so the pending queue can hold
        # unmatched ops past the crash point while recorded entries for earlier
        # ops are still unreconciled; the tail drain stops at the first non-match
        # from the end and never sees those.
        self._pending_msg = [
            msg for msg in self._pending_msg if not _match_pending(msg, recvd_msgs)
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

    async def start(self) -> None:
        """Resume from history and, if the run is unfinished, start executing."""
        if await self._resume():
            return
        self._task = asyncio.create_task(self._run())
        if sys.version_info >= (3, 14):
            asyncio.future_add_to_awaited_by(self._main, self._task)

        def cb(_t: asyncio.Task[_T]) -> None:
            # Readers and unopened port writers must stop if the engine exits
            # with an infrastructure error before the workflow finishes.
            self._notify_append()
            self._stream_manager.close()
            self._ready.set()

        self._task.add_done_callback(cb)
        await self._ready.wait()
        if self._task.done():
            _ = await self._task

    async def _run(self) -> _T:
        if self._main.done():
            return self._main.result()

        try:
            self._is_live = True
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
        if self._closed:
            return
        self._closed = True
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
                await self._backend.release_lease(self._run_id, self._lease)
                self._lease = None

    @staticmethod
    async def _enqueue_ready(operations: Iterable[Coroutine[Any, Any, None]]) -> None:
        pending = [asyncio.create_task(operation) for operation in operations]
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
            await self._serve_stream_reads()
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
            if self._is_live:
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
                state = self._owned_channels[stream_id]
                if isinstance(state, OwnedSignalState):
                    await self._enqueue_log({
                        "id": id_,
                        "ts": now,
                        "source": "task",
                        "type": "signal.scope.exit",
                        "signal_id": stream_id,
                    })
                    return
                if state.owner_id is not None and state.owner_id in self._effects:
                    if not self._is_live:
                        self._scope_exits.setdefault(state.owner_id, []).append(id_)
                        return
                    self._task_manager.start()
                    await self._task_manager.join_task(state.owner_id)
                if state.closed:
                    _ = self._loop.post_completion(id_, result=None)
                else:
                    owned_close: StreamCompleteEntry = {
                        "id": id_,
                        "ts": now,
                        "source": "task",
                        "type": "stream.complete",
                        "stream_id": stream_id,
                    }
                    async with self._channel_gate:
                        if not self._owned_channels[stream_id].closed:
                            await self._enqueue_log(owned_close)
                        else:
                            _ = self._loop.post_completion(id_, result=None)
            case OwnedSignalSend(reference, payload, key, context):
                metadata = OpMetadata(
                    "signal.send",
                    "duron.signal.send",
                    digest("duron.signal.post.v1", [reference.fields(), key, payload]),
                )
                fut.params = FnCall(
                    functools.partial(self._send_control, reference, payload, key),
                    list[object],
                    context,
                    metadata,
                )
                await self._enqueue_op(fut)
            case FnCall() | EffectCall():
                assert not fut.external, "FnCall futures should not be external"
                promise_create_entry = _promise_create_entry(id_, now)
                _record_effect_identity(promise_create_entry, op.metadata)
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
                        "format_version": 1,
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
                    functools.partial(self._effect_run, id_, op, fut)
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
                _record_effect_identity(stream_create_entry, op.metadata)

                self._stream_manager.create_stream(
                    id_, op.observer, op.dtype, op.name, op.metadata.error_types
                )

                await self._enqueue_log(stream_create_entry)

            case StreamEmit(stream_id):
                stream_info = self._stream_manager.get_info(stream_id)
                if stream_info is None:
                    # The stream is unknown or already closed: reject before
                    # appending so a late or duplicate event never enters the
                    # log. Stream state derives from the replayed log, so the
                    # rejection is identical in live runs and replays.
                    _ = self._loop.post_completion(
                        id_, exception=ValueError("Stream not found")
                    )
                    return
                encoded = self._codec.encode_json(op.value, stream_info[0])
                stream_emit_entry: StreamEmitEntry = {
                    "ts": now,
                    "id": id_,
                    "stream_id": stream_id,
                    "type": "stream.emit",
                    "value": encoded,
                    "source": "effect" if fut.external else "task",
                }
                await self._enqueue_log(stream_emit_entry)
            case StreamClose(stream_id, exception):
                stream_info = self._stream_manager.get_info(stream_id)
                if stream_info is None:
                    # See StreamEmit above: reject without appending.
                    _ = self._loop.post_completion(
                        id_, exception=ValueError("Stream not found")
                    )
                    return
                encoded_error = encode_error(exception) if exception else None
                stream_close_entry: StreamCompleteEntry = {
                    "ts": now,
                    "id": id_,
                    "stream_id": stream_id,
                    "type": "stream.complete",
                    "source": "effect" if fut.external else "task",
                }
                if encoded_error:
                    stream_close_entry["error"] = encoded_error
                await self._enqueue_log(stream_close_entry)
            case Barrier():
                assert not fut.external, "Barrier futures should not be external"
                barrier_entry: BarrierEntry = {
                    "ts": now,
                    "id": id_,
                    "type": "barrier",
                    "source": "task",
                }
                await self._enqueue_log(barrier_entry)
            case FutureCreate():
                assert not fut.external, "FutureCreate futures should not be external"
                promise_create_entry = _promise_create_entry(id_, now)
                self._task_manager.add_future(
                    id_, op.return_type, op.metadata.error_types, committed=False
                )
                await self._enqueue_log(promise_create_entry)

            case _:
                assert_never(op)

    async def _enqueue_log(self, entry: Entry) -> None:
        if self._append_error is not None:
            raise self._append_error
        if not self._is_live:
            self._pending_msg.append(entry)
        elif self._lease is None:
            if self._closed:
                return
            msg = "Cannot append without an active backend lease"
            raise RuntimeError(msg)
        else:
            if entry["type"] == "signal.scope.exit":
                async with self._channel_gate:
                    channel = self._owned_channels[entry["signal_id"]]
                    group: list[Entry] = []
                    if not channel.closed:
                        closure: StreamCompleteEntry = {
                            "id": random_id(),
                            "ts": entry["ts"],
                            "source": "effect",
                            "type": "stream.complete",
                            "stream_id": entry["signal_id"],
                        }
                        group.append(closure)
                    group.append(entry)
                    await self._writer.append(group)
                return
            await self._writer.append([entry])

    def _abort_append(self, error: LeaseLostError | StorageError) -> None:
        # Host replies run outside the engine. Poison writes before releasing
        # their lock, then stop the engine with the same infrastructure error.
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
        self._journal.apply(offset, entry)
        self._index_execution(offset, entry)
        _ = self._handle_message(offset, entry)
        if not self._loop.is_closed():
            # Replay advances after every record. Live batches must expose the
            # same ordered readiness to workflow tasks, including cancellation.
            _ = self._loop.advance()

    def _handle_message(self, offset: int, e: Entry) -> bool:
        self._frontier = offset
        id_ = e["id"]
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
            codec = self._codec if effect is None else effect.operation.codec
            if effect is not None:
                state = self._attempts.pop(id_)
                _ = state.complete(
                    cancelled="error" in e and e["error"]["cancelled"], offset=offset
                )
                self._replayed_effects.discard(id_)
                self._running_effects.discard(id_)
                for exit_id in self._scope_exits.pop(id_, []):
                    _ = self._loop.post_completion(exit_id, result=None)
            try:
                return_type, error_types = self._task_manager.complete_task(id_)
            except ValueError as completion_error:
                raise CorruptLogError(
                    offset, "completion references an absent or completed operation"
                ) from completion_error
            self._external_completion_locks.pop(id_, None)
            if self._loop.is_closed():
                return False
            if "result" in e:
                try:
                    result = codec.decode_json(e["result"], return_type)
                    return self._loop.post_completion(id_, result=result)
                except Exception as exc:
                    if effect is not None:
                        raise CorruptLogError(
                            offset,
                            "effect result cannot be decoded with its registered codec",
                        ) from exc
                    return self._loop.post_completion(id_, exception=exc)
            elif "error" in e:
                return self._loop.post_completion(
                    id_, exception=decode_error(e["error"], error_types)
                )
            else:
                # validate_entry only checks that `error`, when present, is an
                # object, so a completion carrying neither field reaches here.
                # Raise a typed error (still a ValueError, so existing handlers
                # keep working) rather than a bare one, per errors.py's contract
                # that every condition is branchable by type.
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
            self._stream_manager.commit_stream(id_)
            if self._stream_manager.get_info(e["id"]) is None:
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
                owned.latest_offset = offset
                _ = self._loop.call_soon(lambda: None)
                return True
            if self._stream_manager.send_to_stream(
                e["stream_id"], self._codec, offset, e["value"]
            ):
                return self._loop.post_completion(id_, result=None)
            return self._loop.post_completion(
                id_, exception=ValueError("Stream not found")
            )
        elif e["type"] == "stream.complete":
            if (owned := self._owned_channels.get(e["stream_id"])) is not None:
                if owned.closed:
                    raise CorruptLogError(offset, "owned stream has repeated EOF")
                owned.closed = True
                owned.latest_offset = offset
                _ = self._loop.call_soon(lambda: None)
                return (
                    self._loop.post_completion(id_, result=None)
                    or e["source"] == "effect"
                )
            error = (
                decode_error(
                    e["error"], self._stream_manager.error_types_of(e["stream_id"])
                )
                if "error" in e
                else None
            )
            succ = self._stream_manager.close_stream(e["stream_id"], offset, error)
            if succ:
                return self._loop.post_completion(id_, result=None)
            return self._loop.post_completion(
                id_, exception=ValueError("Stream not found")
            )
        elif e["type"] == "signal.scope.exit":
            signal = self._owned_channels.get(e["signal_id"])
            if not isinstance(signal, OwnedSignalState) or not signal.closed:
                raise CorruptLogError(
                    offset, "signal scope exit leaves the channel open"
                )
            return self._loop.post_completion(id_, result=None)
        elif e["type"] == "signal.ack":
            signal = self._owned_channels.get(e["signal_id"])
            attempt = self._attempts.get(e["receiver_id"])
            if (
                not isinstance(signal, OwnedSignalState)
                or signal.closed
                or signal.owner_id != e["receiver_id"]
                or attempt is None
                or attempt.start is None
                or attempt.start.invocation.fields() != e["invocation"]
                or (
                    signal.acknowledged_offset is not None
                    and e["message_offset"] <= signal.acknowledged_offset
                )
                or signal.latest_offset is None
                or e["message_offset"] > signal.latest_offset
            ):
                raise CorruptLogError(
                    offset, "invalid committed signal acknowledgement"
                )
            signal.acknowledged_offset = e["message_offset"]
            signal.reservation = None
            return True
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
                if not self._is_live:
                    self._replayed_effects.add(id_)
            elif id_ in self._effects:
                msg = "registered effect call is missing its durable descriptor"
                raise HistoryMismatchError(msg)
            self._task_manager.commit_future(id_)
            return True
        elif (
            e["type"] == "effect.attempt.start" or e["type"] == "effect.retry.scheduled"
        ):
            id_ = e["promise_id"]
            effect = self._effects.get(id_)
            attempt_state = self._attempts.get(id_)
            if effect is None or attempt_state is None:
                raise CorruptLogError(
                    offset, "effect attempt references an absent or completed effect"
                )
            inv = e["invocation"]
            invocation = InvocationIdentity(
                inv["logical_attempt"], inv["invocation_number"], inv["recovery_count"]
            )
            if e["type"] == "effect.attempt.start":
                for reference in effect.operation.handles:
                    channel = self._owned_channels[reference.channel_id]
                    if reference.direction == "signal.receiver" and isinstance(
                        channel, OwnedSignalState
                    ):
                        channel.reservation = None
                self._attempts[id_] = attempt_state.apply_start(
                    AttemptStart(invocation, e["session_id"], e["ts"]),
                    effect.operation.retry,
                    offset=offset,
                )
            else:
                recorded_error = decode_error(e["error"], effect.operation.raises)
                if (
                    not isinstance(recorded_error, Exception)
                    or forbidden_error(recorded_error)
                    or type(recorded_error) not in effect.operation.raises
                ):
                    raise CorruptLogError(
                        offset, "retry record must carry a declared effect failure"
                    )
                self._attempts[id_] = attempt_state.apply_retry(
                    RetryScheduled(invocation, e["ts"], e["not_before_us"]),
                    effect.operation.retry,
                    offset=offset,
                )
            return True
        else:
            return True

    @property
    def executor_api(self) -> EffectHost:
        return self._executor_api

    def _registration_and_codec(
        self, effect_id: EffectId
    ) -> tuple[Registration, Codec, str]:
        registration = self._executor.registration(effect_id)
        codec, codec_id = self._executor.codec_for(effect_id, self._codec)
        return registration, codec, codec_id

    def decode_execution(
        self, wire: EffectEnvelope, *, effect: Effect[_P, _R]
    ) -> ExecutionRequest[_R]:
        registration, codec, codec_id = self._registration_and_codec(
            effect.identity.effect_id
        )
        return decode_request(
            wire,
            effect=effect,
            registration=registration,
            codec=codec,
            codec_id=codec_id,
            run_id=self._run_id,
        )

    def validate_execution(self, request: ExecutionRequest[_R]) -> None:
        if (
            not isinstance(cast("object", request), ExecutionRequest)
            or request.run_id != self._run_id
        ):
            msg = "execution request belongs to another run"
            raise InvalidExecutionRequestError(msg)
        registration, codec, codec_id = self._registration_and_codec(
            request_context(request).identity.effect_id
        )
        fields = cast("dict[str, JSONValue]", json.loads(request.wire))
        if registration.identity != request_context(request).effect or fields[
            "registration_digest"
        ] != digest(
            "duron.registration.v1", registration.descriptor(codec_id, codec=codec)
        ):
            msg = "execution request does not match the configured registration"
            raise InvalidExecutionRequestError(msg)

    def _capability(
        self, scope: PostScope, invocation: InvocationIdentity, *, receipt_only: bool
    ) -> str:
        mode = "receipt" if receipt_only else "mutation"
        payload = canonical_json([
            cast("JSONValue", scope.fields()),
            cast("JSONValue", invocation.fields()),
            mode,
        ])
        signature = hmac.new(self._signing_key, payload, hashlib.sha256).hexdigest()
        return f"{mode}:{signature}"

    @staticmethod
    def _authority(request: ExecutionRequest[_R]) -> PostAuthority:
        identity = request_context(request).identity
        return PostAuthority(
            PostScope(identity.run_seed, "execution", identity.operation_id),
            request.capability,
            request_context(request).invocation,
            receipt_only=request.capability.startswith("receipt:"),
        )

    def _check_ingress(self) -> None:
        if self._closed or self._lease is None or self._execution_error is not None:
            msg = "run owner is unavailable"
            raise RunUnavailableError(msg)
        self._writer.check_open()

    def _index_execution(self, offset: int, entry: Entry) -> None:
        if entry["type"] == "promise.create" and "effect_call" in entry:
            if entry["id"] in self._execution_index:
                raise CorruptLogError(offset, "duplicate effect execution identity")
            self._execution_index[entry["id"]] = _ExecutionRecord(
                call=entry["effect_call"],
                invocation=None,
                completed=None,
                recovering=False,
            )
        elif (
            entry["type"] == "effect.attempt.start"
            or entry["type"] == "promise.complete"
        ):
            record = self._execution_index.get(entry["promise_id"])
            if record is None:
                if entry["type"] == "effect.attempt.start":
                    raise CorruptLogError(offset, "attempt references absent execution")
                return
            if record.completed is not None:
                raise CorruptLogError(offset, "execution changes after completion")
            if entry["type"] == "promise.complete":
                self._execution_index[entry["promise_id"]] = replace(
                    record, completed=entry
                )
            else:
                data = entry["invocation"]
                invocation = InvocationIdentity(
                    data["logical_attempt"],
                    data["invocation_number"],
                    data["recovery_count"],
                )
                self._execution_index[entry["promise_id"]] = replace(
                    record,
                    invocation=invocation,
                    recovering=record.invocation is not None
                    and record.invocation.logical_attempt == invocation.logical_attempt,
                )

    async def _execution_record(self, operation_id: str) -> _ExecutionRecord:
        record = self._execution_index.get(operation_id)
        if record is None:
            msg = "execution does not exist in this run"
            raise InvalidExecutionRequestError(msg)
        return record

    def execution_context(self, request: ExecutionRequest[_R]) -> EffectContextImpl:
        self.validate_execution(request)
        return self._executor.context(
            request_context(request), handles=self._executor_api.bind(request)
        )

    async def _record_for_request(
        self, request: ExecutionRequest[_R]
    ) -> _ExecutionRecord:
        self.validate_execution(request)
        self._check_ingress()
        record = await self._execution_record(
            request_context(request).identity.operation_id
        )
        identity = request_context(request).identity
        if (
            record.call["run_seed"] != identity.run_seed
            or record.call["input_digest"] != identity.input_digest
            or canonical_json(record.call["inputs"]) != request.inputs
        ):
            msg = "request identity or inputs differ from the committed execution"
            raise InvalidExecutionRequestError(msg)
        registration, codec, codec_id = self._registration_and_codec(identity.effect_id)
        if record.call["descriptor"] != registration.descriptor(codec_id, codec=codec):
            msg = "persisted execution registration differs from the request"
            raise InvalidExecutionRequestError(msg)
        self._check_ingress()
        return record

    async def rebind_execution(
        self, request: ExecutionRequest[_R]
    ) -> ExecutionRequest[_R]:
        self.validate_execution(request)
        self._check_ingress()
        adapter = self._executor.executor_for(
            request_context(request).identity.effect_id
        )
        if adapter is None or not isinstance(adapter, RebindingExecutor):
            msg = "rebind requires a configured authenticating dispatch adapter"
            raise InvalidExecutionRequestError(msg)
        await adapter.authorize_rebind(request.wire)
        authority = self._authority(request)
        async with self._journal.gate(authority.scope):
            record = await self._record_for_request(request)
            if record.invocation is None:
                msg = "execution has not been dispatched"
                raise StaleInvocationError(msg)
            if record.completed is None:
                state = self._attempts.get(authority.scope.id)
                if (
                    state is None
                    or state.start is None
                    or state.start.session_id != self._session_id
                    or record.invocation != request_context(request).invocation
                ):
                    msg = "only the current dispatched invocation can rebind"
                    raise StaleInvocationError(msg)
            context = replace(
                request_context(request),
                invocation=record.invocation,
                recovering=record.recovering,
                recovery=self._recovery_descriptor(
                    tuple(
                        handle.reference
                        for handle in request_context(request).recovery.handles
                    )
                ),
            )
            return request._renewed(  # pyright: ignore[reportPrivateUsage]  # ruff: ignore[private-member-access]
                context=context,
                capability=self._capability(
                    authority.scope,
                    record.invocation,
                    receipt_only=record.completed is not None,
                ),
            )

    async def report_execution(
        self, request: ExecutionRequest[_R], post: SemanticPost
    ) -> Receipt:
        _ = await self._record_for_request(request)
        # Value-only executions share no channels; their journal gates suffice.
        gate = (
            self._channel_gate
            if request_context(request).recovery.handles
            else contextlib.nullcontext()
        )
        async with gate:
            return await self._journal.post(self._authority(request), post)

    async def report_stream(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        post: SemanticPost,
    ) -> Receipt:
        _ = await self._record_for_request(request)
        if not any(
            handle.reference == reference
            for handle in request_context(request).recovery.handles
        ):
            msg = "stream is not declared by the execution"
            raise InvalidHandleError(msg)
        state = self._owned_channels.get(reference.channel_id)
        if state is None or reference != replace(
            state.reference, owner_id=state.owner_id or ""
        ):
            msg = "stream reference does not belong to this owned channel"
            raise InvalidHandleError(msg)
        async with self._channel_gate:
            return await self._journal.post(self._authority(request), post)

    def _signal_for_reference(
        self, request: ExecutionRequest[_R], reference: HandleReference
    ) -> OwnedSignalState:
        state = self._owned_channels.get(reference.channel_id)
        if (
            not isinstance(state, OwnedSignalState)
            or reference.direction not in {"signal.sender", "signal.receiver"}
            or reference
            != replace(
                state.reference,
                owner_id=request_context(request).identity.operation_id,
                direction=reference.direction,
            )
            or not any(
                handle.reference == reference
                for handle in request_context(request).recovery.handles
            )
            or (
                reference.direction == "signal.receiver"
                and state.owner_id != reference.owner_id
            )
        ):
            msg = "signal reference is not declared with this direction and owner"
            raise InvalidHandleError(msg)
        return state

    async def report_signal(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        post: SemanticPost,
    ) -> Receipt:
        _ = await self._record_for_request(request)
        _ = self._signal_for_reference(request, reference)
        if (
            post.target != PostTarget("signal", reference.channel_id)
            or POST_ACTION_DIRECTIONS.get(post.action) != reference.direction
        ):
            msg = "signal action does not match its declared direction"
            raise InvalidHandleError(msg)
        async with self._channel_gate:
            return await self._journal.post(self._authority(request), post)

    def _control_authority(self, reference: HandleReference) -> PostAuthority:
        scope = PostScope(reference.run_seed, "control", reference.channel_id)
        capability = hmac.new(
            self._signing_key,
            canonical_json([scope.run_seed, scope.kind, scope.id]),
            hashlib.sha256,
        ).hexdigest()
        return PostAuthority(scope, capability, None)

    def control_sender(
        self, signal: SignalId, *, item_type: TypeHint[_Item]
    ) -> PostingCommandSender[_Item]:
        self._check_ingress()
        state = self._owned_channels.get(signal.value)
        if (
            not isinstance(state, OwnedSignalState)
            or not state.committed
            or signal.run_seed != state.reference.run_seed
            or state.reference.schema_id != schema_id(item_type)
        ):
            msg = "control sender does not match a committed signal and payload type"
            raise InvalidHandleError(msg)
        return PostingCommandSender(
            ControlSignalPosting(
                self._control_authority(state.reference),
                signal.value,
                self._post_control,
            ),
            item_type=item_type,
            codec=self._codec,
        )

    async def _post_control(
        self, authority: PostAuthority, post: SemanticPost
    ) -> Receipt:
        self._check_ingress()
        async with self._channel_gate:
            return await self._journal.post(authority, post)

    async def _send_control(
        self, reference: HandleReference, payload: JSONValue, key: str
    ) -> list[JSONValue]:
        state = self._owned_channels.get(reference.channel_id)
        if not isinstance(state, OwnedSignalState) or reference != replace(
            state.reference, direction="signal.sender"
        ):
            msg = "control sender belongs to another signal scope"
            raise InvalidHandleError(msg)
        receipt = await self._post_control(
            self._control_authority(reference),
            SemanticPost.create(
                key, "signal.send", PostTarget("signal", reference.channel_id), payload
            ),
        )
        return [receipt.offset, receipt.duplicate]

    async def _next_signal(self, signal: OwnedSignalState) -> SignalDelivery | None:
        cursor = signal.acknowledged_offset
        while page := await self._backend.read(
            self._run_id, after=cursor, limit=128, stream_id=signal.reference.channel_id
        ):
            for offset, record in page:
                if offset > self._frontier:
                    return None
                entry = decode_record(record, offset)
                cursor = offset
                if (
                    entry["type"] == "stream.emit"
                    and entry["stream_id"] == signal.reference.channel_id
                ):
                    return SignalDelivery(entry["id"], offset, entry["value"])
        return None

    async def receive_signal(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        correlation_id: str,
    ) -> SignalDelivery:
        validate_post_key(correlation_id)
        if reference.direction != "signal.receiver":
            msg = "signal receive requires a receiver reference"
            raise InvalidHandleError(msg)
        reservation: SignalReservation | None = None
        try:
            while True:
                version = self._append_version
                async with self._channel_gate:
                    _ = await self._record_for_request(request)
                    state = self._signal_for_reference(request, reference)
                    authority = self._authority(request)
                    await self.authorize(
                        authority,
                        SemanticPost.create(
                            "receive",
                            "signal.ack",
                            PostTarget("signal", reference.channel_id),
                            "read",
                        ),
                    )
                    if state.closed:
                        msg = "signal channel is closed"
                        raise ChannelClosedError(msg)
                    if state.reservation is None:
                        state.reservation = SignalReservation(
                            request_context(request).invocation, correlation_id
                        )
                    current = state.reservation
                    if (
                        current.invocation != request_context(request).invocation
                        or current.correlation_id != correlation_id
                    ):
                        msg = "signal receiver has an outstanding receive or message"
                        raise SignalMessagePendingError(msg)
                    if reservation is None:
                        reservation = current
                        current.waiters += 1
                    if current.delivery is None:
                        current.delivery = await self._next_signal(state)
                    if current.delivery is not None:
                        return current.delivery
                await self.wait_for_append(version)
        finally:
            if reservation is not None:
                reservation.waiters -= 1
                cancelled_signal = self._owned_channels.get(reference.channel_id)
                if (
                    reservation.delivery is None
                    and reservation.waiters == 0
                    and isinstance(cancelled_signal, OwnedSignalState)
                    and cancelled_signal.reservation is reservation
                ):
                    cancelled_signal.reservation = None

    async def read_stream(
        self,
        request: ExecutionRequest[_R],
        parameter: StreamParameter[_Item],
        *,
        after: StreamCursor | None,
        limit: int,
    ) -> Sequence[StreamEvent[_Item]]:
        reference = request.stream_reference(parameter)
        stream_id = StreamId(reference.run_seed, reference.channel_id)
        if type(limit) is not int or not 1 <= limit <= 1024:
            msg = "stream inspection limit must be between 1 and 1024"
            raise ValueError(msg)
        if after is not None and after.stream != stream_id:
            msg = "inspection cursor belongs to another stream or run"
            raise InvalidHandleError(msg)
        authority = self._authority(request)
        async with self._journal.gate(authority.scope):
            _ = await self._record_for_request(request)
            await self.authorize(
                authority,
                SemanticPost.create(
                    "inspect",
                    "stream.send",
                    PostTarget("stream", reference.channel_id),
                    None,
                ),
            )
            events: list[StreamEvent[_Item]] = []
            async for offset, entry in iter_entries(
                self._backend,
                self._run_id,
                after=after.offset if after is not None else None,
                page_size=limit,
                stream_id=reference.channel_id,
            ):
                if (
                    entry["type"] == "stream.emit"
                    and entry["stream_id"] == reference.channel_id
                ):
                    value = request.decode_stream_value(parameter, entry["value"])
                    events.append(StreamEvent(value, StreamCursor(stream_id, offset)))
                    if len(events) == limit:
                        break
                elif (
                    entry["type"] == "stream.complete"
                    and entry["stream_id"] == reference.channel_id
                ):
                    break
            self._check_ingress()
            return events

    async def execution_status(
        self, request: ExecutionRequest[_R]
    ) -> ExecutionStatus[_R]:
        authority = self._authority(request)
        async with self._journal.gate(authority.scope):
            record = await self._record_for_request(request)
            await self.authorize(
                authority,
                SemanticPost.create(
                    "inspect",
                    "execution.cancel",
                    PostTarget("execution", authority.scope.id),
                    None,
                ),
            )
            if record.completed is None:
                return ExecutionRunning(record.invocation)
            entry = record.completed
            if "result" in entry:
                return ExecutionSucceeded(request.decode_result(entry["result"]))
            assert "error" in entry
            error = decode_error(
                entry["error"],
                self._executor.registration(
                    request_context(request).identity.effect_id
                ).raises,
            )
            if isinstance(error, asyncio.CancelledError):
                return ExecutionCancelled()
            return ExecutionFailed(error)

    async def authorize(self, authority: PostAuthority, post: SemanticPost, /) -> None:
        self._check_ingress()
        if authority.scope.kind == "control":
            state = self._owned_channels.get(post.target.id)
            if (
                not isinstance(state, OwnedSignalState)
                or not state.committed
                or post.action != "signal.send"
                or authority.scope
                != PostScope(
                    state.reference.run_seed, "control", state.reference.channel_id
                )
                or not hmac.compare_digest(
                    authority.capability,
                    self._control_authority(state.reference).capability,
                )
            ):
                msg = "invalid scoped control sender capability"
                raise InvalidHandleError(msg)
            return
        if authority.invocation is None:
            msg = "execution capability does not own the requested target"
            raise InvalidHandleError(msg)
        expected = self._capability(
            authority.scope, authority.invocation, receipt_only=authority.receipt_only
        )
        if not hmac.compare_digest(authority.capability, expected):
            msg = "invocation capability belongs to another owner"
            raise StaleInvocationError(msg)
        if post.target.kind == "stream":
            state = self._owned_channels.get(post.target.id)
            if state is None or state.owner_id != authority.scope.id:
                msg = "execution does not own the requested stream"
                raise InvalidHandleError(msg)
        elif post.target.kind != "signal" and post.target != PostTarget(
            "execution", authority.scope.id
        ):
            msg = "execution capability does not own the target"
            raise InvalidHandleError(msg)
        record = await self._execution_record(authority.scope.id)
        if post.target.kind == "signal":
            direction = POST_ACTION_DIRECTIONS.get(post.action)
            if direction is None:
                msg = "execution does not declare this signal direction"
                raise InvalidHandleError(msg)
            descriptor = record.call["descriptor"]
            if not isinstance(descriptor, dict) or not isinstance(
                effect_id := descriptor.get("effect_id"), str
            ):
                raise CorruptLogError(0, "effect descriptor has no registered identity")
            registration = self._executor.registration(EffectId(effect_id))
            references = input_references(
                registration, canonical_json(record.call["inputs"])
            )
            if not any(
                ref.channel_id == post.target.id
                and ref.direction == direction
                and ref.owner_id == authority.scope.id
                for ref in references
            ):
                msg = "execution does not declare this signal direction"
                raise InvalidHandleError(msg)
        if (
            record.call["run_seed"] != authority.scope.run_seed
            or record.invocation != authority.invocation
            or (authority.receipt_only and record.completed is None)
        ):
            msg = "invocation capability is fenced"
            raise StaleInvocationError(msg)
        self._check_ingress()

    def check_lifecycle(self, authority: PostAuthority, post: SemanticPost, /) -> None:
        if authority.scope.kind == "control":
            if self._owned_channels[post.target.id].closed:
                msg = "signal channel is closed"
                raise ChannelClosedError(msg)
            return
        state = self._attempts.get(authority.scope.id)
        if state is None or (
            state.retry is not None and post.action != "execution.cancel"
        ):
            msg = "execution or invocation has already reported its outcome"
            raise ExecutionCompletedError(msg)
        if post.target.kind == "stream" and self._owned_channels[post.target.id].closed:
            msg = "stream EOF is permanent"
            raise PortClosedError(msg)
        if post.target.kind == "signal" and self._owned_channels[post.target.id].closed:
            msg = "signal channel is closed"
            raise ChannelClosedError(msg)

    def stage(self, authority: PostAuthority, post: SemanticPost, /) -> Sequence[Entry]:
        id_ = authority.scope.id
        now = _effect_now_us()
        if post.action in {"signal.send", "stream.send"}:
            state = self._owned_channels[post.target.id]
            codec = (
                self._codec
                if post.action == "signal.send" and authority.scope.kind == "control"
                else self._effects[id_].operation.codec
            )
            validate_value(
                codec.decode_json(post.payload, state.item_type), state.item_type
            )
            return [
                {
                    "id": random_id(),
                    "ts": now,
                    "source": "effect",
                    "type": "stream.emit",
                    "stream_id": post.target.id,
                    "value": post.payload,
                }
            ]
        if post.action == "signal.ack":
            signal = self._owned_channels[post.target.id]
            if (
                not isinstance(signal, OwnedSignalState)
                or signal.reservation is None
                or signal.reservation.invocation != authority.invocation
                or signal.reservation.delivery is None
                or signal.reservation.delivery.message_id != post.payload
            ):
                msg = "acknowledgement does not belong to the outstanding delivery"
                raise InvalidHandleError(msg)
            assert authority.invocation is not None
            return [
                {
                    "id": random_id(),
                    "ts": now,
                    "source": "effect",
                    "type": "signal.ack",
                    "signal_id": post.target.id,
                    "message_id": signal.reservation.delivery.message_id,
                    "message_offset": signal.reservation.delivery.offset,
                    "receiver_id": id_,
                    "invocation": authority.invocation.fields(),
                }
            ]
        effect = self._effects[id_]
        operation = effect.operation
        if post.action == "stream.close":
            closure: StreamCompleteEntry = {
                "id": random_id(),
                "ts": now,
                "source": "effect",
                "type": "stream.complete",
                "stream_id": post.target.id,
            }
            return [closure]
        entry: PromiseCompleteEntry = {
            "id": random_id(),
            "ts": now,
            "type": "promise.complete",
            "source": "effect",
            "promise_id": id_,
        }
        if post.action == "execution.complete":
            result = operation.codec.decode_json(post.payload, effect.return_type)
            if effect.return_type is not UnspecifiedType:
                validate_value(result, effect.return_type, allow_unspecified=True)
            entry["result"] = post.payload
        elif post.action == "execution.cancel":
            entry["error"] = encode_error(asyncio.CancelledError())
        else:
            error = decode_error(cast("ErrorInfo", post.payload), operation.raises)
            if not isinstance(error, Exception):
                msg = "failure reporting cannot submit cancellation"
                raise InvalidPostError(msg)
            assert authority.invocation is not None
            if (
                operation.retry.should_retry(error)
                and authority.invocation.logical_attempt < operation.retry.max_attempts
            ):
                delay = operation.retry.delay(authority.invocation.logical_attempt)
                delay_us = delay_to_us(delay)
                return [
                    {
                        "id": entry["id"],
                        "ts": now,
                        "type": "effect.retry.scheduled",
                        "source": "effect",
                        "promise_id": id_,
                        "invocation": authority.invocation.fields(),
                        "error": cast("ErrorInfo", post.payload),
                        "classification": "retryable",
                        "not_before_us": now + delay_us,
                    }
                ]
            entry["error"] = cast("ErrorInfo", post.payload)
        self._task_manager.reporting(id_)
        return self._finalize_channels(operation, entry)

    def _recovery_descriptor(
        self, references: Sequence[HandleReference]
    ) -> RecoveryDescriptor:
        return RecoveryDescriptor(
            tuple(
                self._owned_channels[reference.channel_id].recovery(reference)
                for reference in references
            )
        )

    def _finalize_channels(
        self, operation: EffectOperation, entry: PromiseCompleteEntry
    ) -> Sequence[Entry]:
        closures: list[Entry] = []
        for reference in operation.handles:
            if (
                direction_claims_channel(reference.direction)
                and not self._owned_channels[reference.channel_id].closed
            ):
                closure: StreamCompleteEntry = {
                    "id": random_id(),
                    "ts": entry["ts"],
                    "source": "effect",
                    "type": "stream.complete",
                    "stream_id": reference.channel_id,
                }
                if "error" in entry:
                    closure["error"] = entry["error"]
                closures.append(closure)
        return [*closures, entry]

    async def _wait_external_report(self, id_: str) -> None:
        seen_version = 0
        while id_ in self._effects and self._attempts[id_].retry is None:
            self._check_ingress()
            if self.is_terminal():
                raise asyncio.CancelledError
            seen_version = await self.wait_for_append(seen_version)

    async def _effect_run(self, id_: str, op: EffectCall, waiter: OpFuture) -> None:
        operation = op.operation
        policy = operation.retry
        identity = ExecutionIdentity(
            operation.seed, id_, operation.identity.effect_id, operation.input_digest
        )
        entry: PromiseCompleteEntry = {
            "ts": -1,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": id_,
            "source": "effect",
        }
        committed = False
        reporter: EffectReporter[object] | None = None
        scope = PostScope(operation.seed, "execution", id_)
        self._running_effects.add(id_)

        def bind_request(
            invocation: InvocationIdentity, *, recovering: bool = False
        ) -> tuple[EffectContextImpl, ExecutionRequest[object], EffectReporter[object]]:
            context = EffectContextImpl(
                identity,
                operation.identity,
                invocation,
                recovering,
                recovery=self._recovery_descriptor(operation.handles),
            )
            request = local_request(
                run_id=self._run_id,
                context=context,
                registration=self._executor.registration(identity.effect_id),
                codec=operation.codec,
                codec_id=operation.codec_id,
                inputs=InputSnapshot(operation.inputs, operation.input_digest),
                capability=self._capability(scope, invocation, receipt_only=False),
                return_type=cast("TypeHint[object]", op.return_type),
            )
            bound = self._executor_api.bind(request)
            return self.execution_context(request), request, bound

        try:
            if waiter.cancelled():
                raise asyncio.CancelledError  # ruff: ignore[raise-within-try]
            while True:
                state = self._attempts[id_]
                plan = state.plan(policy)
                if plan is None:
                    assert state.start is not None
                    entry["error"] = encode_error(
                        EffectOutcomeUnknownError(identity, state.start.invocation)
                    )
                    break
                await _wait_effect_deadline(plan.not_before_us)
                start: EffectAttemptStartEntry = {
                    "ts": _effect_now_us(),
                    "id": random_id(),
                    "type": "effect.attempt.start",
                    "source": "effect",
                    "promise_id": id_,
                    "invocation": plan.invocation.fields(),
                    "session_id": self._session_id,
                }
                async with self._journal.gate(scope):
                    await self._enqueue_log(start)
                context, request, reporter = bind_request(
                    plan.invocation, recovering=plan.recovering
                )
                async with (
                    operation.admission
                    if operation.admission is not None
                    else contextlib.nullcontext()
                ):
                    adapter = self._executor.executor_for(identity.effect_id)
                    if adapter is not None:
                        await adapter.dispatch(request.wire, host=self._executor_api)
                        await self._wait_external_report(id_)
                    else:
                        await _invoke_effect(operation, context, reporter)
                if id_ in self._effects:
                    continue
                record = await self._execution_record(id_)
                assert record.completed is not None
                entry = record.completed
                committed = True
                break
        except asyncio.CancelledError as error:
            if self._closed:
                return
            self._task_manager.reporting(id_)
            cancelled_state = self._attempts.get(id_)
            if cancelled_state is not None and cancelled_state.start is not None:
                context, _, reporter = bind_request(cancelled_state.start.invocation)
                with contextlib.suppress(ExecutionCompletedError):
                    await reporter.cancel(
                        idempotency_key=f"local:{context.invocation_number}:cancel"
                    )
                record = await self._execution_record(id_)
                assert record.completed is not None
                entry = record.completed
                committed = True
            elif cancelled_state is not None:
                entry["error"] = encode_error(error)
            else:
                record = await self._execution_record(id_)
                assert record.completed is not None
                entry = record.completed
                committed = True
        if self._closed:
            return
        if not committed:
            entry["ts"] = _effect_now_us()
        if not committed:
            self._task_manager.reporting(id_)
            async with self._channel_gate:
                await self._writer.append(self._finalize_channels(operation, entry))

    async def _task_run(self, id_: str, op: FnCall) -> None:
        codec = self._codec

        entry: PromiseCompleteEntry = {
            "ts": -1,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": id_,
            "source": "effect",
        }
        try:
            if inspect.iscoroutinefunction(op.callable):
                result = await op.callable()
            else:
                result = op.callable()
            entry["result"] = codec.encode_json(result, op.return_type)
        except (Exception, asyncio.CancelledError) as e:  # ruff: ignore[blind-except]
            entry["error"] = encode_error(e)
        entry["ts"] = self._now_us
        if self._closed or self._loop.is_closed():
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

    async def open_stream(self, name: str) -> StreamWriter[Any]:
        """Open a writer for a named stream created by the workflow.

        Args:
            name: The name of the stream.

        Returns:
            A `StreamWriter` for appending values to the stream.

        """
        writer: StreamWriter[Any] | None = self._stream_writers.get(name)
        if writer is not None:
            return writer
        sid = await self._stream_manager.wait_stream(name)
        # Recheck after waiting: other host handles may have opened or closed
        # this stream while its creation was pending.
        writer = self._stream_writers.get(name)
        if writer is not None:
            return writer
        writer = StreamWriter[Any](sid, self._loop)
        self._stream_writers[name] = writer
        return writer

    def is_future_pending(self, future_id: str) -> bool:
        """Check if a future is still pending.

        Args:
            future_id: The ID created by [`create_future`][duron.Context.create_future].

        Returns:
            True if the future is still pending, False otherwise.

        """
        return self._task_manager.has_future(future_id)

    @overload
    async def complete_future(
        self, future_id: str, *, result: _T, result_type: TypeHint[_T] = ...
    ) -> None: ...
    @overload
    async def complete_future(
        self, future_id: str, *, exception: Exception
    ) -> None: ...
    async def complete_future(
        self,
        future_id: str,
        *,
        result: object | None = None,
        result_type: TypeHint[object] = UnspecifiedType,
        exception: Exception | None = None,
    ) -> None:
        """Complete a future with the given result or exception.

        Args:
            future_id: The ID created by [`create_future`][duron.Context.create_future].
            result: The result to complete the future with.
            result_type: The type of the result.
            exception: The exception to complete the future with.

        Raises:
            RequestAlreadyResolvedError: If the future with the given ID is
                not pending (already resolved or never created).

        """
        if not self._task_manager.has_future(future_id):
            msg = f"Promise {future_id!r} is not pending"
            raise RequestAlreadyResolvedError(msg)
        lock = self._external_completion_locks.setdefault(future_id, asyncio.Lock())
        async with lock:
            if not self._task_manager.has_future(future_id):
                msg = f"Promise {future_id!r} is not pending"
                raise RequestAlreadyResolvedError(msg)
            entry: PromiseCompleteEntry = {
                "ts": self._now_us,
                "id": random_id(),
                "type": "promise.complete",
                "promise_id": future_id,
                "source": "effect",
            }
            if exception is not None:
                entry["error"] = encode_error(exception)
            else:
                entry["result"] = self._codec.encode_json(result, result_type)
            await self._enqueue_log(entry)


async def read_header(
    backend: HistoryStore, codec: Codec, *, run_id: str
) -> dict[str, JSONValue] | None:
    """Read the selected run's persisted header, or ``None`` if it has no history.

    The header is the prelude's init result, always the first
    ``promise.complete`` in the log. It is persisted through the workflow
    codec, so it is decoded with that codec rather than assuming a
    transparent JSON dict (a custom codec can store it as an opaque string).

    Returns:
        The decoded header, or ``None`` when the selected log is empty.

    Raises:
        HistoryMismatchError: if the header carries an error, cannot be
            decoded with this codec, or is malformed.

    """
    async for _offset, entry in iter_entries(backend, run_id, page_size=1):
        if entry["type"] != "promise.complete":
            continue
        if "result" not in entry:
            msg = "persisted run header contains an error instead of initialization"
            raise HistoryMismatchError(msg)
        try:
            result = codec.decode_json(entry["result"], InitParams)
        except Exception as exc:
            msg = "persisted run header cannot be decoded with this workflow codec"
            raise HistoryMismatchError(msg) from exc
        if _is_header(result):
            _validate_history(result)
            return cast("dict[str, JSONValue]", result)
        msg = "persisted run header is malformed or uses an incompatible codec"
        raise HistoryMismatchError(msg)
    return None


def _is_header(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    header = cast("dict[object, object]", value)
    return (
        isinstance(header.get("version"), int)
        and isinstance(header.get("args"), list)
        and isinstance(header.get("kwargs"), dict)
        and isinstance(header.get("nonce"), str)
    )


async def _prelude_fn(
    init: Callable[[], InitParams],
    fn: DurableFn[..., _T],
    executor: EffectRegistry,
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
    _validate_history(init_params)

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

    ctx = Context(loop, init_params["nonce"], executor=executor)

    _ = ready()
    return await fn.fn(ctx, *args, **kwargs)


def _validate_history(init_params: object) -> None:
    if not isinstance(init_params, dict):
        msg = "Persisted history header is not an object"
        raise HistoryMismatchError(msg, expected="object", actual=init_params)
    header = cast("dict[str, object]", init_params)
    if type(header.get("version")) is not int or header["version"] != CURRENT_VERSION:
        found = header.get("version", "missing")
        msg = (
            f"Unsupported persisted history format {found!r}; expected "
            f"{CURRENT_VERSION}. Create a new run with a distinct run_id."
        )
        raise HistoryMismatchError(msg, expected=CURRENT_VERSION, actual=found)


def _promise_create_entry(id_: str, ts: int) -> PromiseCreateEntry:
    return {"ts": ts, "id": id_, "type": "promise.create", "source": "task"}


async def _invoke_effect(
    operation: EffectOperation,
    context: EffectContextImpl,
    reporter: EffectReporter[object],
) -> None:
    try:
        result = await operation.invoke(context)
    except Exception as error:  # ruff: ignore[blind-except]  # effect body boundary
        if isinstance(error, EffectBodyError):
            error = error.error
        await reporter.fail(
            error, idempotency_key=f"local:{context.invocation_number}:failure"
        )
    else:
        try:
            await reporter.complete(
                result, idempotency_key=f"local:{context.invocation_number}:result"
            )
        except (TypeError, ValueError) as error:
            msg = "effect result cannot be encoded"
            raise WorkflowDefinitionError(msg) from error


def _match_pending(
    msg: Entry, recvd_msgs: dict[str, tuple[str, tuple[object, ...] | None]]
) -> bool:
    """Match one pending op against the recorded entries.

    On a match, validates the recorded entry type and effect identity and
    forgets both copies so they cannot be matched again.

    Returns:
        True if ``msg`` was found in ``recvd_msgs``.

    """
    if msg["id"] not in recvd_msgs:
        return False
    # Membership is tested explicitly: an op with no declared effect identity
    # records a ``None`` identity, which ``dict.get`` cannot distinguish from an
    # absent key.
    persisted_type, persisted_identity = recvd_msgs[msg["id"]]
    _validate_recorded_entry(msg, persisted_type, persisted_identity)
    del recvd_msgs[msg["id"]]
    return True


def _record_effect_identity(entry: Entry, metadata: OpMetadata) -> None:
    if metadata.effect_name is None or metadata.effect_version is None:
        return
    entry["effect_name"] = metadata.effect_name
    entry["effect_version"] = metadata.effect_version


def _effect_identity(entry: Entry) -> tuple[object, ...] | None:
    name = entry.get("effect_name")
    version = entry.get("effect_version")
    if name is None and version is None:
        return None
    if entry["type"] == "promise.create" and "effect_call" in entry:
        data = entry["effect_call"]
        return (name, version, data["descriptor"], data["input_digest"])
    return (name, version)


def _validate_recorded_entry(
    expected: Entry, persisted_type: str, persisted_identity: tuple[object, ...] | None
) -> None:
    # The entry type distinguishes op kinds sharing a positional id. Note that
    # FnCall and FutureCreate both record "promise.create" and remain
    # indistinguishable here; their completions are validated separately by
    # TaskManager.complete_task.
    if persisted_type != expected["type"]:
        msg = (
            f"operation kind mismatch at id {expected['id']!r}: persisted "
            f"{persisted_type!r}, requested {expected['type']!r}"
        )
        raise HistoryMismatchError(
            msg, expected=persisted_type, actual=expected["type"]
        )
    requested = _effect_identity(expected)
    if persisted_identity != requested:
        msg = (
            f"effect identity mismatch: persisted {persisted_identity!r}, "
            f"requested {requested!r}"
        )
        raise HistoryMismatchError(msg, expected=persisted_identity, actual=requested)
