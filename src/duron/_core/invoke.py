from __future__ import annotations

import asyncio
import contextlib
import contextvars
import time
from typing import TYPE_CHECKING, Final, Generic, Literal, cast
from typing_extensions import (
    Any,
    ParamSpec,
    TypedDict,
    TypeVar,
    assert_never,
    assert_type,
    final,
    overload,
)

from duron._core.codec import decode_error, encode_error
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
from duron._loop import EventLoop, create_loop, random_id
from duron.codec import Codec
from duron.log._helper import is_entry, set_annotations
from duron.tracing._span import NULL_SPAN
from duron.tracing._tracer import Tracer, current_tracer, span
from duron.typing import JSONValue, UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine, Iterable

    from duron._core.ops import Op, StreamObserver
    from duron._decorator.durable import DurableFn
    from duron._loop import OpFuture, WaitSet
    from duron.codec import Codec
    from duron.log._entry import (
        BarrierEntry,
        Entry,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        StreamCompleteEntry,
        StreamCreateEntry,
        StreamEmitEntry,
    )
    from duron.log._storage import LogStorage
    from duron.typing import FunctionType


_T = TypeVar("_T")
_P = ParamSpec("_P")

_CURRENT_VERSION: Final = 0


@contextlib.asynccontextmanager
async def invoke(
    fn: DurableFn[_P, _T], log: LogStorage, /, *, tracer: Tracer | None = None
) -> AsyncGenerator[DurableRun[_P, _T], None]:
    """Create an invocation context for this durable function.

    Args:
        fn: Durable function to invoke
        log: Log storage for persisting operation history
        tracer: Optional tracer for observability

    Returns:
        Async context manager for Invoke instance
    """  # noqa: DOC402, DOC202
    token = current_tracer.set(tracer)
    run = DurableRun(fn, log)
    try:
        yield run
    finally:
        await run.close()
        current_tracer.reset(token)


@final
class DurableRun(Generic[_P, _T]):
    __slots__ = ("_fn", "_log", "_run", "_streams", "_watchers")

    def __init__(self, fn: DurableFn[_P, _T], log: LogStorage) -> None:
        self._fn = fn
        self._log = log
        self._run: _DurableRun | None = None
        self._watchers: dict[str, StreamObserver] = {}
        self._streams: dict[str, Stream[Any]] = {}

        for name, typ, _dtype in self._fn.inject:
            if typ is StreamWriter:
                stream, w = create_buffer_stream()
                self._streams[name] = stream
                self._watchers[name] = w

    async def start(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        """Start a new invocation of the durable function."""
        codec = self._fn.codec

        async def prelude() -> InitParams:  # noqa: RUF029
            return {
                "version": _CURRENT_VERSION,
                "args": [codec.encode_json(arg) for arg in args],
                "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
                "nonce": random_id(),
            }

        type_info = inspect_function(self._fn.fn)
        p = _invoke_prelude(self._fn, type_info, prelude)
        loop = await create_loop()
        self._run = _DurableRun(loop, p, self._log, codec, watchers=self._watchers)
        await self._run.resume()

    async def resume(self) -> None:
        """Resume a previously started invocation."""
        type_info = inspect_function(self._fn.fn)
        p = _invoke_prelude(self._fn, type_info, _resume_init)
        loop = await create_loop()
        self._run = _DurableRun(
            loop, p, self._log, self._fn.codec, watchers=self._watchers
        )
        await self._run.resume()

    async def wait(self) -> _T:
        """Wait for the durable function invocation to complete \
                and return its result.

        Raises:
            RuntimeError: If the job has not been started.

        Returns:
            The result of the durable function invocation.
        """
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        return cast("_T", await self._run.run())

    async def close(self) -> None:
        if self._run:
            await self._run.close()
            self._run = None

    @overload
    async def complete_future(self, id_: str, *, result: object) -> None: ...
    @overload
    async def complete_future(self, id_: str, *, exception: Exception) -> None: ...
    async def complete_future(
        self,
        id_: str,
        *,
        result: object | None = None,
        exception: Exception | None = None,
    ) -> None:
        """Complete an external future with the given result \
                or exception.

        Raises:
            RuntimeError: If the job has not been started.
        """
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        if exception is not None:
            await self._run.complete_external_future(id_, exception=exception)
        else:
            await self._run.complete_external_future(id_, result=result)

    @overload
    async def open_stream(self, name: str, mode: Literal["w"]) -> StreamWriter[Any]: ...
    @overload
    async def open_stream(self, name: str, mode: Literal["r"]) -> Stream[Any]: ...
    async def open_stream(
        self, name: str, mode: Literal["w", "r"]
    ) -> StreamWriter[Any] | Stream[Any]:
        """Open a runtime provided stream for reading or writing.

        Note:
            - Must be called after starting or resuming the job.

        Args:
            name: The name of the stream parameter to open.
            mode: The mode to open the stream in.

        Raises:
            RuntimeError: If called after the job has started.
            ValueError: If the stream parameter is not found.

        Returns:
            A [StreamWriter][duron.StreamWriter] for writing, \
                    or a [Stream][duron.Stream] for reading.
        """
        if self._run is None:
            msg = "open_stream() must be called after start() or resume()"
            raise RuntimeError(msg)
        for n, _, _ in self._fn.inject:
            if name == n:
                break
        else:
            msg = f"Stream parameter '{name}' not found"
            raise ValueError(msg)
        if mode == "r":
            return self._streams.pop(name)

        sid = await self._run.wait_stream((("name", name),))
        w: OpWriter[Any] = OpWriter(sid, self._run.loop())
        return w


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str


async def _invoke_prelude(
    job_fn: DurableFn[..., _T],
    type_info: FunctionType,
    init: Callable[[], Coroutine[Any, Any, InitParams]],
) -> _T:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, EventLoop)  # noqa: S101

    init_params: InitParams = await create_op(
        loop,
        FnCall(
            callable=init,
            return_type=InitParams,
            context=contextvars.copy_context(),
            annotations=OpAnnotations(
                name="@duron.prelude",
            ),
        ),
    )
    if init_params["version"] != _CURRENT_VERSION:
        msg = "version mismatch"
        raise RuntimeError(msg)
    ctx = Context(loop, init_params["nonce"])

    codec = job_fn.codec
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
    for name, type_, dtype in job_fn.inject:
        if type_ is Stream:
            extra_kwargs[name], _stw = await ctx.create_stream(dtype, name=name)
        elif type_ is Signal:
            extra_kwargs[name], _sgw = await ctx.create_signal(dtype, name=name)
        elif type_ is StreamWriter:
            _, extra_kwargs[name] = await ctx.create_stream(dtype, name=name)

    with span("InvokeRun"):
        return await job_fn.fn(ctx, *args, **extra_kwargs, **kwargs)


@final
class _DurableRun:
    __slots__ = (
        "_codec",
        "_lease",
        "_log",
        "_loop",
        "_now",
        "_pending_msg",
        "_running",
        "_stream_manager",
        "_task",
        "_task_manager",
        "_task_run",
        "_tracer",
    )

    def __init__(
        self,
        loop: EventLoop,
        task: Coroutine[Any, Any, object],
        log: LogStorage,
        codec: Codec,
        *,
        watchers: dict[str, StreamObserver],
    ) -> None:
        self._loop = loop
        self._task = self._loop.schedule_task(task)
        self._log = log
        self._codec = codec
        self._running: bool = False
        self._lease: bytes | None = None
        self._pending_msg: list[Entry] = []
        self._now = 0
        self._stream_manager = StreamManager(
            (watcher, (("name", name),)) for name, watcher in watchers.items()
        )
        self._tracer: Tracer | None = Tracer.current()
        self._task_run: asyncio.Task[object] | None = None

        def cancel_task(e: TaskError) -> None:
            self._loop.call_soon(self._task.cancel, e)

        self._task_manager = TaskManager(cancel_task)

    def loop(self) -> EventLoop:
        return self._loop

    async def close(self) -> None:
        if self._task_run:
            self._task_run.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await self._task_run
        if self._tracer:
            self._tracer.close()
        await self._send_traces(flush=True)
        if self._lease is not None:
            await self._log.release_lease(self._lease)
            self._lease = None

        if not self._loop.is_closed():
            _ = self._task.cancel()
            self._loop.close()
            await self._task_manager.close()

    def now(self) -> int:
        return self._now

    def tick_realtime(self) -> None:
        t = time.time_ns()
        t //= 1_000
        self._now = max(self._now + 1, t)

    async def resume(self) -> None:
        self._lease = await self._log.acquire_lease()
        recvd_msgs: set[str] = set()
        async for o, entry in self._log.stream(None, live=False):
            ts = entry["ts"]
            self._now = max(self._now, ts)
            _ = await self._step()
            if is_entry(entry):
                await self.handle_message(o, entry)
                _ = await self._step()
            recvd_msgs.add(entry["id"])

        msgs: list[Entry] = [
            msg for msg in self._pending_msg if msg["id"] not in recvd_msgs
        ]
        self._pending_msg = msgs
        self._task_run = asyncio.create_task(self._run())

    async def run(self) -> object:
        if self._task_run is None:
            msg = "Run has not been started. Call resume() first."
            raise RuntimeError(msg)
        return await asyncio.shield(self._task_run)

    async def _run(self) -> object:
        try:
            if self._task.done():
                return self._task.result()

            self._running = True
            if self._tracer:
                self._tracer.start()
            for msg in self._pending_msg:
                await self.enqueue_log(msg)
            self._pending_msg.clear()
            self._task_manager.start()

            self.tick_realtime()
            while waitset := await self._step():
                if self._tracer:
                    await waitset.block(self.now(), 1_000_000)
                    await self._send_traces()
                else:
                    await waitset.block(self.now())
                _ = await self._step()
                self.tick_realtime()

            # cleanup
            self._loop.close()
            await self._task_manager.close()
            await self._send_traces(flush=True)

            await self.close()
            return self._task.result()
        except asyncio.CancelledError as e:
            if e.args and isinstance(e.args[0], TaskError):
                raise e.args[0].exception from e
            raise

    async def _step(self) -> WaitSet | None:
        self._loop.tick(self.now())

        while True:
            result = self._loop.poll_completion(self._task)
            if result is None or not result.added:
                return result

            for s in result.added:
                sid = s.id
                await self.enqueue_op(sid, s)

    async def _send_traces(self, *, flush: bool = False) -> None:
        if not self._tracer:
            return
        tid = self._tracer.run_id
        data = self._tracer.pop_events(flush=flush)
        for i in range(0, len(data), 128):
            trace_entry: Entry = {
                "ts": self.now(),
                "id": random_id(),
                "type": "trace",
                "events": data[i : i + 128],
                "metadata": {"trace.id": tid},
            }
            await self.enqueue_log(trace_entry)

    async def handle_message(self, offset: int, e: Entry) -> None:
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

    async def enqueue_log(self, entry: Entry) -> None:
        if not self._running:
            self._pending_msg.append(entry)
        elif self._lease is None:
            # closed
            return
        else:
            offset = await self._log.append(self._lease, entry)
            await self.handle_message(offset, entry)

    async def enqueue_op(self, id_: str, fut: OpFuture) -> None:
        op = cast("Op", fut.params)
        match op:
            case FnCall():
                promise_create_entry: PromiseCreateEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise.create",
                }

                set_annotations(promise_create_entry, labels=op.annotations.labels)
                if self._tracer:
                    op_span = self._tracer.new_op_span(
                        op.annotations.get_name(), promise_create_entry
                    )
                else:
                    op_span = None
                await self.enqueue_log(promise_create_entry)

                async def cb() -> None:
                    entry: PromiseCompleteEntry = {
                        "ts": -1,
                        "id": random_id(),
                        "type": "promise.complete",
                        "promise_id": id_,
                    }
                    with (
                        op_span.new_span(op.annotations.get_name())
                        if op_span
                        else NULL_SPAN
                    ) as span:
                        try:
                            result = await op.callable()
                            entry["result"] = self._codec.encode_json(result)
                            span.set_status("OK")
                        except (Exception, asyncio.CancelledError) as e:  # noqa: BLE001
                            entry["error"] = encode_error(e)
                            span.set_status("ERROR", str(e))

                    if op_span:
                        op_span.end(entry)
                    entry["ts"] = self.now()
                    await self.enqueue_log(entry)

                sid = id_
                if self._running:
                    self._task_manager.add_task(sid, cb(), op.context, op.return_type)
                else:
                    self._task_manager.add_pending(sid, cb, op.context, op.return_type)

                def done(f: OpFuture) -> None:
                    if f.cancelled():
                        self._task_manager.cancel_task(f.id)

                fut.add_done_callback(done)

            case StreamCreate():
                stream_id = id_

                stream_create_entry: StreamCreateEntry = {
                    "ts": self.now(),
                    "id": stream_id,
                    "type": "stream.create",
                }
                if self._tracer:
                    op_span = self._tracer.new_op_span(
                        "stream:" + op.annotations.get_name(), stream_create_entry
                    )
                else:
                    op_span = None

                self._stream_manager.create_stream(
                    stream_id, op.observer, op.dtype, op.annotations.labels, op_span
                )

                set_annotations(stream_create_entry, labels=op.annotations.labels)
                await self.enqueue_log(stream_create_entry)

            case StreamEmit():
                stream_info = self._stream_manager.get_info(op.stream_id)
                if stream_info:
                    (op_span,) = stream_info
                    stream_emit_entry: StreamEmitEntry = {
                        "ts": self.now(),
                        "id": id_,
                        "stream_id": op.stream_id,
                        "type": "stream.emit",
                        "value": self._codec.encode_json(op.value),
                    }
                    if op_span:
                        op_span.attach(
                            stream_emit_entry,
                            {"type": "event", "ts": self.now(), "kind": "stream"},
                        )
                    await self.enqueue_log(stream_emit_entry)
            case StreamClose():
                stream_info = self._stream_manager.get_info(op.stream_id)
                if stream_info:
                    (op_span,) = stream_info
                    if op.exception:
                        stream_close_entry_err: StreamCompleteEntry = {
                            "ts": self.now(),
                            "id": id_,
                            "stream_id": op.stream_id,
                            "type": "stream.complete",
                            "error": encode_error(op.exception),
                        }
                        if op_span:
                            op_span.end(stream_close_entry_err)
                        await self.enqueue_log(stream_close_entry_err)
                    else:
                        stream_close_entry: StreamCompleteEntry = {
                            "ts": self.now(),
                            "id": id_,
                            "stream_id": op.stream_id,
                            "type": "stream.complete",
                        }
                        if op_span:
                            op_span.end(stream_close_entry)
                        await self.enqueue_log(stream_close_entry)
            case Barrier():
                barrier_entry: BarrierEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "barrier",
                }
                await self.enqueue_log(barrier_entry)
            case FutureCreate():
                promise_create_entry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise.create",
                }
                set_annotations(promise_create_entry, labels=op.annotations.labels)
                if self._tracer:
                    _ = self._tracer.new_op_span(
                        op.annotations.get_name(), promise_create_entry
                    )
                self._task_manager.add_future(id_, op.return_type)
                await self.enqueue_log(promise_create_entry)
            case FutureComplete():
                promise_complete_entry: PromiseCompleteEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise.complete",
                    "promise_id": op.future_id,
                }
                if op.exception is not None:
                    promise_complete_entry["error"] = encode_error(op.exception)
                else:
                    promise_complete_entry["result"] = self._codec.encode_json(op.value)

                if self._tracer:
                    self._tracer.end_op_span(op.future_id, promise_complete_entry)
                await self.enqueue_log(promise_complete_entry)
                self._loop.post_completion(id_, result=None)
            case _:
                assert_never(op)

    async def complete_external_future(
        self,
        id_: str,
        *,
        result: object | None = None,
        exception: Exception | None = None,
    ) -> None:
        if not self._task_manager.has_future(id_):
            msg = "Promise not found"
            raise ValueError(msg)
        now_us = self.now()
        entry: PromiseCompleteEntry = {
            "ts": now_us,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": id_,
        }
        if exception is not None:
            entry["error"] = encode_error(exception)
        elif result is not None:
            entry["result"] = self._codec.encode_json(result)
        else:
            msg = "Either result or error must be provided"
            raise ValueError(msg)
        if self._tracer:
            self._tracer.end_op_span(id_, entry)
        await self.enqueue_log(entry)

    async def wait_stream(self, matcher: Iterable[tuple[str, str]]) -> str:
        return await self._stream_manager.wait_one(matcher)


async def _resume_init() -> InitParams:  # noqa: RUF029
    msg = "not started"
    raise RuntimeError(msg)
