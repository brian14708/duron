from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import time
from typing import TYPE_CHECKING, Final, Generic, Literal, cast, overload
from typing_extensions import (
    Any,
    ParamSpec,
    Self,
    TypedDict,
    TypeVar,
    assert_never,
    assert_type,
)

from duron._core.context import Context
from duron._core.ops import (
    Barrier,
    FnCall,
    FutureComplete,
    FutureCreate,
    OpAnnotations,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron._core.signal import Signal
from duron._core.stream import OpWriter, Stream, StreamWriter, create_buffer_stream
from duron._core.stream_manager import StreamManager
from duron._core.task_manager import TaskError, TaskManager
from duron._core.utils import decode_error, encode_error
from duron.log._helper import is_entry, set_annotations
from duron.loop import EventLoop, create_loop, random_id
from duron.tracing._span import NULL_SPAN
from duron.tracing._tracer import current_tracer, span
from duron.typing import JSONValue
from duron.typing._hint import UnspecifiedType

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import TracebackType

    from duron._core.ops import Op, StreamObserver
    from duron._decorator.durable import DurableFn
    from duron.log import LogStorage
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


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")

_CURRENT_VERSION: Final = 0


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str


class Session:
    __slots__ = ("_current_task", "_lease", "_log", "_loop", "_token", "_tracer")

    def __init__(self, log: LogStorage, /, *, tracer: Tracer | None = None) -> None:
        """A session for running durable functions.

        Example:
            ```python
            async with Session(log_storage) as session:
                task = session.start(my_durable_function, arg1, arg2)
                result = await task.result()
            ```

        Args:
            log: The log storage to use for this session.
            tracer: An optional tracer for tracing operations within the session.
        """
        self._log = log
        self._tracer = tracer
        self._token: contextvars.Token[Tracer] | None = None
        self._loop: EventLoop | None = None
        self._lease: bytes | None = None
        self._current_task: Task[Any] | None = None

    async def __aenter__(self) -> Self:
        self._token = current_tracer.set(self._tracer) if self._tracer else None
        self._loop = await create_loop()
        self._lease = await self._log.acquire_lease()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._lease is not None:
            await self._log.release_lease(self._lease)
            self._lease = None
        if self._loop:
            self._loop.close()
            self._loop = None
        if self._current_task:
            await self._current_task.close()
            self._current_task = None
        if tracer_token := self._token:
            current_tracer.reset(tracer_token)

    def start(
        self, fn: DurableFn[_P, _T_co], *args: _P.args, **kwargs: _P.kwargs
    ) -> Task[_T_co]:
        """Start a new durable function within the session.

        Raises:
            RuntimeError: If a durable function is already running or the session is \
                    not started.

        Args:
            fn: The durable function to run.
            *args: Positional arguments to pass to the durable function.
            **kwargs: Keyword arguments to pass to the durable function.

        Returns:
            A `Task` representing the running durable function.
        """
        if self._current_task is not None:
            msg = "A durable function is already running"
            raise RuntimeError(msg)
        if self._lease is None or self._loop is None:
            msg = "Session is not started"
            raise RuntimeError(msg)

        codec = fn.codec

        async def init() -> InitParams:  # noqa: RUF029
            return {
                "version": _CURRENT_VERSION,
                "args": [codec.encode_json(arg) for arg in args],
                "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
                "nonce": random_id(),
            }

        self._current_task = Task(
            self._loop, self._log, self._tracer, self._lease, init, fn
        )
        self._loop = None
        self._lease = None
        return self._current_task

    def resume(self, fn: DurableFn[_P, _T_co]) -> Task[_T_co]:
        """Resume durable function within the session.

        Raises:
            RuntimeError: If a durable function is already running or the session is \
                    not started.

        Args:
            fn: The durable function to run.

        Returns:
            A `Task` representing the running durable function.
        """
        if self._current_task is not None:
            msg = "A durable function is already running"
            raise RuntimeError(msg)
        if self._lease is None or self._loop is None:
            msg = "Session is not started"
            raise RuntimeError(msg)

        async def init() -> InitParams:  # noqa: RUF029
            msg = "Not started properly"
            raise RuntimeError(msg)

        self._current_task = Task(
            self._loop, self._log, self._tracer, self._lease, init, fn
        )
        self._loop = None
        self._lease = None
        return self._current_task


class Task(Generic[_T_co]):
    """A task representing a running durable function within a session."""

    __slots__ = (
        "_codec",
        "_is_live",
        "_lease",
        "_log",
        "_loop",
        "_main",
        "_now_us",
        "_pending_msg",
        "_stream_manager",
        "_streams",
        "_task",
        "_task",
        "_task_manager",
        "_tracer",
    )

    def __init__(
        self,
        loop: EventLoop,
        log: LogStorage,
        tracer: Tracer | None,
        lease: bytes,
        init: Callable[[], Coroutine[Any, Any, InitParams]],
        fn: DurableFn[_P, _T_co],
    ) -> None:
        self._loop = loop
        self._log = log
        self._tracer = tracer
        self._lease: bytes | None = lease
        self._now_us: int = 0
        self._is_live: bool = False
        self._pending_msg: list[Entry] = []

        observers: dict[str, StreamObserver] = {}
        streams: dict[str, Stream[Any]] = {}

        for name, typ, _dtype in fn.inject:
            if typ is StreamWriter:
                stream, w = create_buffer_stream()
                streams[name] = stream
                observers[name] = w

        main = self._loop.schedule_task(_prelude_fn(init, fn))
        self._main = main
        self._codec = fn.codec
        self._stream_manager = StreamManager(
            (observer, (("name", name),)) for name, observer in observers.items()
        )
        self._streams = streams
        self._task_manager = TaskManager(
            functools.partial(self._loop.call_soon, main.cancel)
        )
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> _T_co:
        recvd_msgs: set[str] = set()
        async for o, entry in self._log.stream(None, live=False):
            ts = entry["ts"]
            self._now_us = max(self._now_us, ts)
            _ = await self._step()
            if is_entry(entry):
                await self._handle_message(o, entry)
                _ = await self._step()
            if entry["source"] == "task":
                recvd_msgs.add(entry["id"])
            while self._pending_msg:
                id_ = self._pending_msg[-1]["id"]
                if id_ not in recvd_msgs:
                    break
                self._pending_msg.pop()
                recvd_msgs.remove(id_)

        assert len(recvd_msgs) == 0
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

            self._now_us = max(self._now_us + 1, time.time_ns() // 1_000)
            while waitset := await self._step():
                if self._tracer:
                    await waitset.block(self._now_us, 1_000_000)
                    await self._send_traces()
                else:
                    await waitset.block(self._now_us)
                _ = await self._step()
                self._now_us = max(self._now_us + 1, time.time_ns() // 1_000)

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
        self._task.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await self._task

        if self._tracer:
            self._tracer.close()
        await self._send_traces(flush=True)

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
        op = cast("Op", fut.params)
        match op:
            case FnCall():
                assert not fut.external, "FnCall futures should not be external"
                promise_create_entry: PromiseCreateEntry = {
                    "ts": self._now_us,
                    "id": id_,
                    "type": "promise.create",
                    "source": "task",
                }

                set_annotations(promise_create_entry, labels=op.annotations.labels)
                if tracer := self._tracer:
                    op_span = tracer.new_op_span(
                        op.annotations.get_name(), promise_create_entry
                    )
                else:
                    op_span = None
                await self._enqueue_log(promise_create_entry)

                run = self._task_run(id_, op, op_span)

                if self._is_live:
                    self._task_manager.add_task(id_, run(), op.context, op.return_type)
                else:
                    self._task_manager.add_pending(id_, run, op.context, op.return_type)

                def done(f: OpFuture) -> None:
                    if f.cancelled():
                        self._task_manager.cancel_task(f.id)

                fut.add_done_callback(done)

            case StreamCreate():
                assert not fut.external, "StreamCreate futures should not be external"
                stream_id = id_

                stream_create_entry: StreamCreateEntry = {
                    "ts": self._now_us,
                    "id": stream_id,
                    "type": "stream.create",
                    "source": "task",
                }
                if tracer := self._tracer:
                    op_span = tracer.new_op_span(
                        "stream:" + op.annotations.get_name(), stream_create_entry
                    )
                else:
                    op_span = None

                self._stream_manager.create_stream(
                    stream_id, op.observer, op.dtype, op.annotations.labels, op_span
                )

                set_annotations(stream_create_entry, labels=op.annotations.labels)
                await self._enqueue_log(stream_create_entry)

            case StreamEmit():
                stream_emit_entry: StreamEmitEntry = {
                    "ts": self._now_us,
                    "id": id_,
                    "stream_id": op.stream_id,
                    "type": "stream.emit",
                    "value": self._codec.encode_json(op.value),
                    "source": "effect" if fut.external else "task",
                }
                stream_info = self._stream_manager.get_info(op.stream_id)
                if stream_info:
                    (op_span,) = stream_info
                    if op_span:
                        op_span.attach(
                            stream_emit_entry,
                            {"type": "event", "ts": self._now_us, "kind": "stream"},
                        )
                await self._enqueue_log(stream_emit_entry)
            case StreamClose():
                stream_info = self._stream_manager.get_info(op.stream_id)
                if stream_info:
                    (op_span,) = stream_info
                    if op.exception:
                        stream_close_entry: StreamCompleteEntry = {
                            "ts": self._now_us,
                            "id": id_,
                            "stream_id": op.stream_id,
                            "type": "stream.complete",
                            "error": encode_error(op.exception),
                            "source": "effect" if fut.external else "task",
                        }
                        if op_span:
                            op_span.end(stream_close_entry)
                        await self._enqueue_log(stream_close_entry)
                    else:
                        stream_close_entry = {
                            "ts": self._now_us,
                            "id": id_,
                            "stream_id": op.stream_id,
                            "type": "stream.complete",
                            "source": "effect" if fut.external else "task",
                        }
                        if op_span:
                            op_span.end(stream_close_entry)
                    await self._enqueue_log(stream_close_entry)
            case Barrier():
                assert not fut.external, "Barrier futures should not be external"
                barrier_entry: BarrierEntry = {
                    "ts": self._now_us,
                    "id": id_,
                    "type": "barrier",
                    "source": "task",
                }
                await self._enqueue_log(barrier_entry)
            case FutureCreate():
                assert not fut.external, "FutureCreate futures should not be external"
                promise_create_entry = {
                    "ts": self._now_us,
                    "id": id_,
                    "type": "promise.create",
                    "source": "task",
                }
                set_annotations(promise_create_entry, labels=op.annotations.labels)
                if tracer := self._tracer:
                    _ = tracer.new_op_span(
                        op.annotations.get_name(), promise_create_entry
                    )
                self._task_manager.add_future(id_, op.return_type)
                await self._enqueue_log(promise_create_entry)
            case FutureComplete():
                promise_complete_entry: PromiseCompleteEntry = {
                    "ts": self._now_us,
                    "id": id_,
                    "type": "promise.complete",
                    "promise_id": op.future_id,
                    "source": "effect" if fut.external else "task",
                }
                if op.exception is not None:
                    promise_complete_entry["error"] = encode_error(op.exception)
                else:
                    promise_complete_entry["result"] = self._codec.encode_json(op.value)

                if fut.external:
                    set_annotations(promise_complete_entry, labels={"source": "effect"})
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
            # closed
            return
        else:
            offset = await self._log.append(self._lease, entry)
            await self._handle_message(offset, entry)

    async def _handle_message(self, offset: int, e: Entry) -> None:
        if e["type"] == "promise.complete":
            id_ = e["promise_id"]
            (return_type,) = self._task_manager.complete_task(id_)
            if "error" in e:
                self._loop.post_completion(id_, exception=decode_error(e["error"]))
            elif "result" in e:
                try:
                    result = self._codec.decode_json(e["result"], return_type)
                    self._loop.post_completion(id_, result=result)
                except Exception as exc:  # noqa: BLE001
                    self._loop.post_completion(id_, exception=exc)
            else:
                msg = f"Invalid promise.complete entry: {e!r}"
                raise ValueError(msg)
        elif e["type"] == "stream.create":
            id_ = e["id"]
            if self._stream_manager.get_info(e["id"]) is None:
                self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
            else:
                self._loop.post_completion(id_, result=e["id"])
        elif e["type"] == "stream.emit":
            id_ = e["id"]
            if self._stream_manager.send_to_stream(
                e["stream_id"], self._codec, offset, e["value"]
            ):
                self._loop.post_completion(id_, result=None)
            else:
                self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
        elif e["type"] == "stream.complete":
            id_ = e["id"]
            succ = self._stream_manager.close_stream(
                e["stream_id"],
                offset,
                decode_error(e["error"]) if "error" in e else None,
            )
            if succ:
                self._loop.post_completion(id_, result=None)
            else:
                self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
        elif e["type"] == "barrier":
            self._loop.post_completion(e["id"], result=offset)
        else:
            assert_type(e["type"], Literal["promise.create", "trace"])

    def _task_run(
        self, id_: str, op: FnCall, op_span: OpSpan | None
    ) -> Callable[[], Coroutine[None, None, None]]:
        codec = self._codec

        async def _run() -> None:
            entry: PromiseCompleteEntry = {
                "ts": -1,
                "id": random_id(),
                "type": "promise.complete",
                "promise_id": id_,
                "source": "effect",
            }
            with (
                op_span.new_span(op.annotations.get_name()) if op_span else NULL_SPAN
            ) as span:
                try:
                    result = await op.callable()
                    entry["result"] = codec.encode_json(result)
                    span.set_status("OK")
                except (Exception, asyncio.CancelledError) as e:  # noqa: BLE001
                    entry["error"] = encode_error(e)
                    span.set_status("ERROR", str(e))

            if op_span:
                op_span.end(entry)
            entry["ts"] = self._now_us
            await self._enqueue_log(entry)

        return _run

    async def result(self) -> _T_co:
        """Wait for the durable function to complete and return its result.

        Returns:
            The result of the durable function, raises exception if the function failed.
        """
        return await asyncio.shield(self._task)

    @overload
    async def open_stream(self, name: str, mode: Literal["w"]) -> StreamWriter[Any]: ...
    @overload
    async def open_stream(self, name: str, mode: Literal["r"]) -> Stream[Any]: ...
    async def open_stream(
        self, name: str, mode: Literal["w", "r"]
    ) -> StreamWriter[Any] | Stream[Any]:
        """Open a stream for reading or writing.

        Args:
            name: The name of the stream.
            mode: The mode to open the stream in. Can be "r" for reading or "w" for \
                    writing.

        Returns:
            A `Stream` for reading or a `StreamWriter` for writing, depending on the \
                    mode.
        """
        if mode == "r":
            return self._streams.pop(name)

        sid = await self._stream_manager.wait_one((("name", name),))
        w: OpWriter[Any] = OpWriter(sid, self._loop)
        return w

    @overload
    async def complete_future(self, future_id: str, *, result: object) -> None: ...
    @overload
    async def complete_future(
        self, future_id: str, *, exception: Exception
    ) -> None: ...
    async def complete_future(
        self,
        future_id: str,
        *,
        result: object | None = None,
        exception: Exception | None = None,
    ) -> None:
        """Complete a future with the given result or exception.

        Raises:
            ValueError: If the future with the given ID does not exist.

        Args:
            future_id: The ID created by [`create_future`][duron.Context.create_future].
            result: The result to complete the future with.
            exception: The exception to complete the future with.
        """
        if not self._task_manager.has_future(future_id):
            msg = "Promise not found"
            raise ValueError(msg)
        now_us = self._now_us
        entry: PromiseCompleteEntry = {
            "ts": now_us,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": future_id,
            "source": "effect",
        }
        if exception is not None:
            entry["error"] = encode_error(exception)
        else:
            entry["result"] = self._codec.encode_json(result)
        if self._tracer:
            self._tracer.end_op_span(future_id, entry)
        await self._enqueue_log(entry)


async def _prelude_fn(
    init: Callable[[], Coroutine[Any, Any, InitParams]], fn: DurableFn[..., _T_co]
) -> _T_co:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, EventLoop)

    init_params: InitParams = await create_op(
        loop,
        FnCall(
            callable=init,
            return_type=InitParams,
            context=contextvars.copy_context(),
            annotations=OpAnnotations(name="duron.prelude"),
        ),
    )
    if init_params["version"] != _CURRENT_VERSION:
        msg = "version mismatch"
        raise RuntimeError(msg)
    ctx = Context(loop, init_params["nonce"])

    codec = fn.codec
    type_info = fn.type_hints
    args = tuple(
        codec.decode_json(
            arg,
            type_info.parameter_types.get(type_info.parameters[i + 1], UnspecifiedType)
            if i + 1 < len(type_info.parameters)
            else UnspecifiedType,
        )
        for i, arg in enumerate(init_params["args"])
    )
    kwargs = {
        k: codec.decode_json(v, type_info.parameter_types.get(k, UnspecifiedType))
        for k, v in sorted(init_params["kwargs"].items())
    }

    extra_kwargs: dict[str, Stream[Any] | StreamWriter[Any] | Signal[Any]] = {}
    for name, type_, dtype in fn.inject:
        if type_ is Stream:
            extra_kwargs[name], _stw = await ctx.create_stream(dtype, name=name)
        elif type_ is Signal:
            extra_kwargs[name], _sgw = await ctx.create_signal(dtype, name=name)
        elif type_ is StreamWriter:
            _, extra_kwargs[name] = await ctx.create_stream(dtype, name=name)

    with span("Session"):
        return await fn.fn(ctx, *args, **extra_kwargs, **kwargs)
