from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import inspect
import sys
import time
from typing import TYPE_CHECKING, Final, Generic, Literal, cast
from typing_extensions import (
    Any,
    NotRequired,
    ParamSpec,
    Self,
    TypedDict,
    TypeVar,
    assert_never,
    assert_type,
    overload,
)

from duron._core.context import Context
from duron._core.ops import (
    Barrier,
    FnCall,
    FutureComplete,
    FutureCreate,
    OpMetadata,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron._core.stream import StreamWriter
from duron._core.stream_manager import StreamManager
from duron._core.task_manager import TaskError, TaskManager
from duron._core.utils import decode_error, encode_error
from duron.errors import (
    HistoryMismatchError,
    LeaseLostError,
    RequestAlreadyResolvedError,
)
from duron.log._entry import CorruptLogError
from duron.log._helper import is_entry, random_id
from duron.loop import EventLoop, create_loop
from duron.tracing import NULL_SPAN, span
from duron.tracing._tracer import current_tracer
from duron.typing import JSONValue, UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from duron._core.ops import Op
    from duron._decorator.durable import DurableFn
    from duron.codec import Codec
    from duron.log import Storage
    from duron.log._entry import (
        BarrierEntry,
        Entry,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        StreamCompleteEntry,
        StreamCreateEntry,
        StreamEmitEntry,
    )
    from duron.loop import OpFuture, WaitSet
    from duron.tracing import Tracer
    from duron.tracing._tracer import OpSpan
    from duron.typing import TypeHint

_T = TypeVar("_T")
_P = ParamSpec("_P")

CURRENT_VERSION: Final = 0


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str
    # Public run identity, set by the duron._api layer. Declared here (rather
    # than smuggled in via cast) so schema-aware codecs persist them instead of
    # dropping undeclared keys.
    workflow_name: NotRequired[str]
    workflow_version: NotRequired[str]


class Session:
    __slots__ = (
        "_closed",
        "_current_task",
        "_lease",
        "_log",
        "_loop",
        "_token",
        "_tracer",
    )

    def __init__(self, log: Storage, /, *, tracer: Tracer | None = None) -> None:
        """Session for running durable functions.

        Owns the storage lease and event loop that a spawned :class:`Task`
        takes over; the ``duron._api`` layer drives it via :meth:`spawn`.

        Args:
            log: The log storage to use for this session.
            tracer: An optional tracer for tracing operations within the session.

        """
        self._closed = False
        self._log = log
        self._tracer = tracer
        self._token: contextvars.Token[Tracer] | None = None
        self._loop: EventLoop | None = None
        self._lease: bytes | None = None
        self._current_task: Task[Any] | None = None

    async def __aenter__(self) -> Self:
        self._token = current_tracer.set(self._tracer) if self._tracer else None
        try:
            self._loop = await create_loop()
            self._lease = await self._log.acquire_lease()
        except BaseException:
            if self._loop is not None and not self._loop.is_closed():
                self._loop.close()
            self._loop = None
            if self._token is not None:
                current_tracer.reset(self._token)
                self._token = None
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
                    await self._log.release_lease(self._lease)
                    self._lease = None
            finally:
                if self._loop:
                    if not self._loop.is_closed():
                        self._loop.close()
                    self._loop = None
                if tracer_token := self._token:
                    current_tracer.reset(tracer_token)
                    self._token = None

    @property
    def storage(self) -> Storage:
        """The log storage this session owns."""
        return self._log

    def spawn(self, fn: DurableFn[_P, _T], init: Callable[[], InitParams]) -> Task[_T]:
        if self._current_task is not None:
            msg = "A durable function is already running"
            raise RuntimeError(msg)
        if self._loop is None:
            msg = "Session is not started"
            raise RuntimeError(msg)

        task = Task(self._loop, self._log, self._tracer, self._lease, init, fn)
        self._current_task = task
        self._loop = None
        self._lease = None
        return task


class Task(Generic[_T]):
    """A task representing a running durable function within a session."""

    __slots__ = (
        "_append_version",
        "_append_waiters",
        "_closed",
        "_codec",
        "_external_completion_lock",
        "_is_live",
        "_lease",
        "_log",
        "_loop",
        "_main",
        "_now_us",
        "_pending_msg",
        "_ready",
        "_stream_manager",
        "_task",
        "_task_manager",
        "_tracer",
    )

    def __init__(
        self,
        loop: EventLoop,
        log: Storage,
        tracer: Tracer | None,
        lease: bytes | None,
        init: Callable[[], InitParams],
        fn: DurableFn[_P, _T],
    ) -> None:
        self._loop = loop
        self._log = log
        self._tracer = tracer
        self._lease: bytes | None = lease
        self._closed = False
        self._now_us: int = 0
        self._is_live: bool = False
        self._pending_msg: list[Entry] = []
        self._external_completion_lock = asyncio.Lock()
        # Host-reader notification: bumped on every durable append and on
        # completion, so readers can await new entries instead of polling.
        self._append_version: int = 0
        self._append_waiters: set[asyncio.Future[None]] = set()

        self._ready = asyncio.Event()
        main = self._loop.schedule_task(
            _prelude_fn(
                init,
                fn,
                functools.partial(
                    asyncio.get_running_loop().call_soon, self._ready.set
                ),
            )
        )
        self._main = main
        self._codec = fn.codec
        self._stream_manager = StreamManager()
        self._task_manager = TaskManager(
            functools.partial(self._loop.call_soon, main.cancel)
        )
        self._task: asyncio.Task[_T] | None = None
        # Wake host readers when the run reaches a terminal state even if no
        # further entry is appended, and release any host writer blocked in
        # wait_stream for a stream the workflow never created.

        def _on_main_done(_f: object) -> None:
            self._notify_append()
            self._stream_manager.close()

        main.add_done_callback(_on_main_done)

    async def _resume(self) -> bool:
        recvd_msgs: dict[str, tuple[object, object] | None] = {}
        ws: WaitSet | None = None
        async for o, entry in self._log.stream():
            ts = entry["ts"]
            while ws and ws.timer and ws.timer < ts:
                self._now_us = max(self._now_us, ws.timer)
                ws = await self._step()

            self._now_us = max(self._now_us, ts)
            ws = await self._step()
            self._raise_history_error()
            if is_entry(entry):
                if entry["source"] == "task":
                    if not self._handle_message(o, entry):
                        msg = "Extra messages found in log"
                        raise HistoryMismatchError(msg, actual=entry["id"])
                    # Only the persisted effect identity is ever read back (by
                    # _match_pending); retaining the whole entry would keep the
                    # log's entire payload in memory for the whole replay.
                    recvd_msgs[entry["id"]] = _effect_identity(entry)
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
            self._ready.set()

        self._task.add_done_callback(cb)
        await self._ready.wait()
        if self._task.done():
            _ = await self._task
        else:
            self._task.remove_done_callback(cb)

    async def _run(self) -> _T:
        if self._main.done():
            return self._main.result()

        try:
            self._is_live = True
            if self._tracer:
                self._tracer.start()
            for msg in self._pending_msg:
                await self._enqueue_log(msg)
            self._pending_msg.clear()
            self._task_manager.start()
            await asyncio.sleep(0)

            if self._now_us == 0:
                self._now_us = max(self._now_us, time.time_ns() // 1_000)
            while waitset := await self._step():
                if self._tracer:
                    await waitset.block(self._now_us, 1_000_000)
                    await self._send_traces()
                else:
                    await waitset.block(self._now_us)
                waitset = await self._step()
                now = time.time_ns() // 1_000
                if waitset and waitset.timer and waitset.timer < now:
                    self._now_us = max(self._now_us, waitset.timer)
                else:
                    self._now_us = max(self._now_us, now)

            # cleanup
            self._loop.close()
            await self._task_manager.close()
            await self._send_traces(flush=True)

            return self._main.result()
        except asyncio.CancelledError as e:
            if e.args and isinstance(e.args[0], TaskError):
                raise e.args[0].exception from e
            raise

    async def _send_traces(self, *, flush: bool = False) -> None:
        if not self._tracer:
            return
        tid = self._tracer.run_id
        data = self._tracer.pop_events(flush=flush)
        for i in range(0, len(data), 128):
            trace_entry: Entry = {
                "ts": self._now_us,
                "id": random_id(),
                "type": "trace",
                "events": data[i : i + 128],
                "metadata": {"trace.id": tid},
                "source": "trace",
            }
            await self._enqueue_log(trace_entry)

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
            if self._tracer:
                self._tracer.close()
            await self._send_traces(flush=True)
        finally:
            if self._lease:
                await self._log.release_lease(self._lease)
                self._lease = None

            if not self._loop.is_closed():
                _ = self._main.cancel()
                self._loop.close()
            await self._task_manager.close()

    async def _step(self) -> WaitSet | None:
        self._loop.tick(self._now_us)

        while True:
            result = self._loop.poll_completion(self._main)
            if result is None or not result.added:
                return result

            for s in result.added:
                await self._enqueue_op(s)

    async def _enqueue_op(self, fut: OpFuture) -> None:
        id_ = fut.id
        now = self._now_us
        op = cast("Op", fut.params)
        match op:
            case FnCall():
                assert not fut.external, "FnCall futures should not be external"
                promise_create_entry = _promise_create_entry(id_, now)
                _record_effect_identity(promise_create_entry, op.metadata)

                if tracer := self._tracer:
                    op_span = tracer.new_op_span(
                        op.metadata.get_name(), promise_create_entry
                    )
                else:
                    op_span = None
                await self._enqueue_log(promise_create_entry)

                if self._is_live:
                    await self._task_manager.add_task(
                        id_,
                        self._task_run(id_, op, op_span),
                        op.context,
                        op.return_type,
                        fut,
                        op.metadata.error_types,
                    )
                else:
                    self._task_manager.add_pending(
                        id_,
                        functools.partial(self._task_run, id_, op, op_span),
                        op.context,
                        op.return_type,
                        fut,
                        op.metadata.error_types,
                    )

                def done(f: OpFuture) -> None:
                    if f.cancelled():
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
                    id_,
                    op.observer,
                    op.dtype,
                    op.name,
                    self._tracer.new_op_span(
                        "stream:" + op.metadata.get_name(), stream_create_entry
                    )
                    if self._tracer
                    else None,
                    op.metadata.error_types,
                    op.replay_state,
                )

                await self._enqueue_log(stream_create_entry)

            case StreamEmit(stream_id):
                stream_info = self._stream_manager.get_info(stream_id)
                stream_emit_entry: StreamEmitEntry = {
                    "ts": now,
                    "id": id_,
                    "stream_id": stream_id,
                    "type": "stream.emit",
                    "value": self._codec.encode_json(
                        op.value, stream_info[0] if stream_info else UnspecifiedType
                    ),
                    "source": "effect" if fut.external else "task",
                }
                if stream_info:
                    _, op_span = stream_info
                    if op_span:
                        op_span.attach(
                            stream_emit_entry,
                            {"type": "event", "ts": -1, "kind": "stream"},
                        )
                await self._enqueue_log(stream_emit_entry)
            case StreamClose(stream_id, exception):
                stream_info = self._stream_manager.get_info(stream_id)
                stream_close_entry: StreamCompleteEntry = {
                    "ts": now,
                    "id": id_,
                    "stream_id": stream_id,
                    "type": "stream.complete",
                    "source": "effect" if fut.external else "task",
                }
                if exception:
                    stream_close_entry["error"] = encode_error(exception)
                if stream_info:
                    _, op_span = stream_info
                    if op_span:
                        op_span.end(stream_close_entry)
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
                if tracer := self._tracer:
                    _ = tracer.new_op_span(op.metadata.get_name(), promise_create_entry)
                self._task_manager.add_future(
                    id_, op.return_type, op.metadata.error_types
                )
                await self._enqueue_log(promise_create_entry)
            case FutureComplete():
                promise_complete_entry: PromiseCompleteEntry = {
                    "ts": now,
                    "id": id_,
                    "type": "promise.complete",
                    "promise_id": op.future_id,
                    "source": "effect" if fut.external else "task",
                }
                if op.exception is not None:
                    promise_complete_entry["error"] = encode_error(op.exception)
                else:
                    promise_complete_entry["result"] = self._codec.encode_json(
                        op.value, op.dtype
                    )

                if tracer := self._tracer:
                    tracer.end_op_span(op.future_id, promise_complete_entry)
                await self._enqueue_log(promise_complete_entry)
                self._loop.post_completion(id_, result=None)

            case _:
                assert_never(op)

    async def _enqueue_log(self, entry: Entry) -> None:
        if not self._is_live:
            self._pending_msg.append(entry)
        elif self._lease is None:
            if self._closed:
                return
            msg = "Cannot append without an active storage lease"
            raise RuntimeError(msg)
        else:
            try:
                offset = await self._log.append(self._lease, entry)
            except ValueError as exc:
                msg = "storage lease was lost while appending to the run"
                raise LeaseLostError(msg) from exc
            self._handle_message(offset, entry)
            self._notify_append()

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
        while self._append_version <= seen_version and not self._main.done():
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

    def _handle_message(self, offset: int, e: Entry) -> bool:
        id_ = e["id"]
        if e["type"] == "promise.complete":
            id_ = e["promise_id"]
            return_type, error_types = self._task_manager.complete_task(id_)
            if "result" in e:
                try:
                    result = self._codec.decode_json(e["result"], return_type)
                    return self._loop.post_completion(id_, result=result)
                except Exception as exc:  # noqa: BLE001
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
            if self._stream_manager.get_info(e["id"]) is None:
                return self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
            return self._loop.post_completion(id_, result=e["id"])
        elif e["type"] == "stream.emit":
            if self._stream_manager.send_to_stream(
                e["stream_id"],
                self._codec,
                offset,
                e["value"],
                replayed=not self._is_live and e["source"] == "effect",
            ):
                return self._loop.post_completion(id_, result=None)
            return self._loop.post_completion(
                id_, exception=ValueError("Stream not found")
            )
        elif e["type"] == "stream.complete":
            error = (
                decode_error(
                    e["error"], self._stream_manager.error_types_of(e["stream_id"])
                )
                if "error" in e
                else None
            )
            succ = self._stream_manager.close_stream(
                e["stream_id"], offset, error, replayed=not self._is_live
            )
            if succ:
                return self._loop.post_completion(id_, result=None)
            return self._loop.post_completion(
                id_, exception=ValueError("Stream not found")
            )
        elif e["type"] == "barrier":
            return self._loop.post_completion(id_, result=(offset, e["ts"]))
        else:
            assert_type(e["type"], Literal["promise.create", "trace"])
            return True

    async def _task_run(self, id_: str, op: FnCall, op_span: OpSpan | None) -> None:
        codec = self._codec

        entry: PromiseCompleteEntry = {
            "ts": -1,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": id_,
            "source": "effect",
        }
        with op_span.new_span(op.metadata.get_name()) if op_span else NULL_SPAN as span:
            try:
                if inspect.iscoroutinefunction(op.callable):
                    result = await op.callable()
                else:
                    result = op.callable()
                entry["result"] = codec.encode_json(result, op.return_type)
                span.set_status("OK")
            except (Exception, asyncio.CancelledError) as e:  # noqa: BLE001
                entry["error"] = encode_error(e)
                span.set_status("ERROR", str(e))

        if op_span:
            op_span.end(entry)
        entry["ts"] = self._now_us
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
        sid = await self._stream_manager.wait_stream(name)
        w: StreamWriter[Any] = StreamWriter(sid, self._loop)
        return w

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
        async with self._external_completion_lock:
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
            if self._tracer:
                self._tracer.end_op_span(future_id, entry)
            await self._enqueue_log(entry)


async def read_header(storage: Storage, codec: Codec) -> dict[str, JSONValue] | None:
    """Read the persisted run header, or ``None`` if storage holds no run.

    The header is the prelude's init result, always the first
    ``promise.complete`` in the log. It is persisted through the workflow
    codec, so it is decoded with that codec rather than assuming a
    transparent JSON dict (PickleCodec stores it as an opaque string, for
    example).

    Returns:
        The decoded header, or ``None`` when the log is empty.

    Raises:
        HistoryMismatchError: if the header carries an error, cannot be
            decoded with this codec, or is malformed.

    """
    async for _offset, entry in storage.stream():
        if not is_entry(entry) or entry["type"] != "promise.complete":
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
    init: Callable[[], InitParams], fn: DurableFn[..., _T], ready: Callable[[], object]
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

    ctx = Context(loop, init_params["nonce"])

    with span("Session"):
        _ = ready()
        return await fn.fn(ctx, *args, **kwargs)


def _validate_history(init_params: object) -> None:
    if not isinstance(init_params, dict):
        msg = "Persisted history header is not an object"
        raise HistoryMismatchError(msg, expected="object", actual=init_params)
    header = cast("dict[str, object]", init_params)
    if header.get("version") != CURRENT_VERSION:
        found = header.get("version", "missing")
        msg = (
            f"Unsupported persisted history format {found!r}; expected "
            f"{CURRENT_VERSION}. Create a new history or migrate it explicitly."
        )
        raise HistoryMismatchError(msg, expected=CURRENT_VERSION, actual=found)


def _promise_create_entry(id_: str, ts: int) -> PromiseCreateEntry:
    return {"ts": ts, "id": id_, "type": "promise.create", "source": "task"}


def _match_pending(
    msg: Entry, recvd_msgs: dict[str, tuple[object, object] | None]
) -> bool:
    """Match one pending op against the recorded entries.

    On a match, validates the recorded effect identity and forgets both
    copies so they cannot be matched again.

    Returns:
        True if ``msg`` was found in ``recvd_msgs``.

    """
    if msg["id"] not in recvd_msgs:
        return False
    # Membership is tested explicitly: an op with no declared effect identity
    # records a ``None`` identity, which ``dict.get`` cannot distinguish from an
    # absent key.
    persisted = recvd_msgs[msg["id"]]
    _validate_effect_identity(msg, persisted)
    del recvd_msgs[msg["id"]]
    return True


def _record_effect_identity(entry: Entry, metadata: OpMetadata) -> None:
    if metadata.effect_name is None or metadata.effect_version is None:
        return
    entry["effect_name"] = metadata.effect_name
    entry["effect_version"] = metadata.effect_version


def _effect_identity(entry: Entry) -> tuple[object, object] | None:
    name = entry.get("effect_name")
    version = entry.get("effect_version")
    if name is None and version is None:
        return None
    return (name, version)


def _validate_effect_identity(
    expected: Entry, persisted: tuple[object, object] | None
) -> None:
    if persisted is None:
        # Format-0 histories created before effect identities were persisted
        # remain replayable, but any newly recorded identity is enforced.
        return
    requested = _effect_identity(expected)
    if persisted != requested:
        msg = (
            f"effect identity mismatch: persisted {persisted!r}, "
            f"requested {requested!r}"
        )
        raise HistoryMismatchError(msg, expected=persisted, actual=requested)
