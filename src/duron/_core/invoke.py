from __future__ import annotations

import asyncio
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

from duron._core.context import Context
from duron._core.ops import (
    Barrier,
    ExternalPromiseComplete,
    ExternalPromiseCreate,
    FnCall,
    StreamClose,
    StreamCreate,
    StreamEmit,
)
from duron._core.signal import Signal
from duron._core.stream import ObserverStream, Stream, StreamWriter
from duron._core.stream_manager import StreamManager
from duron._core.task_manager import TaskManager
from duron._loop import EventLoop, create_loop
from duron.codec import Codec
from duron.log import derive_id, is_entry, random_id, set_annotations
from duron.tracing._span import NULL_SPAN
from duron.tracing._tracer import Tracer, current_tracer, span
from duron.typing import JSONValue, UnspecifiedType, inspect_function

if TYPE_CHECKING:
    import contextlib
    from asyncio.exceptions import CancelledError
    from collections.abc import Callable, Coroutine
    from contextvars import Token
    from types import TracebackType

    from duron._core.ops import (
        Op,
        StreamObserver,
    )
    from duron._core.signal import SignalWriter
    from duron._decorator.durable import DurableFn
    from duron._loop import OpFuture, WaitSet
    from duron.codec import Codec
    from duron.log import (
        BarrierEntry,
        Entry,
        ErrorInfo,
        LogStorage,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        StreamCompleteEntry,
        StreamCreateEntry,
        StreamEmitEntry,
    )
    from duron.typing import FunctionType


_T_co = TypeVar("_T_co", covariant=True)
_T = TypeVar("_T")
_P = ParamSpec("_P")

_CURRENT_VERSION: Final = 0


@final
class Invoke(Generic[_P, _T_co]):
    __slots__ = ("_fn", "_log", "_run", "_watchers")

    def __init__(
        self,
        fn: DurableFn[_P, _T_co],
        log: LogStorage,
    ) -> None:
        self._fn = fn
        self._log = log
        self._run: _InvokeRun | None = None
        self._watchers: list[
            tuple[
                dict[str, str],
                StreamObserver,
            ]
        ] = []

    @staticmethod
    def invoke(
        fn: DurableFn[_P, _T_co],
        log: LogStorage,
        tracer: Tracer | None,
    ) -> contextlib.AbstractAsyncContextManager[Invoke[_P, _T_co]]:
        return _InvokeGuard(Invoke(fn, log), tracer)

    async def start(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        codec = self._fn.codec

        def prelude() -> InitParams:
            return {
                "version": _CURRENT_VERSION,
                "args": [codec.encode_json(arg) for arg in args],
                "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
                "nonce": random_id(),
            }

        type_info = inspect_function(self._fn.fn)
        p = _invoke_prelude(self._fn, type_info, prelude)
        self._run = _InvokeRun(
            p,
            self._log,
            codec,
            watchers=self._watchers,
        )
        await self._run.resume()

    async def resume(self) -> None:
        type_info = inspect_function(self._fn.fn)
        prelude = _invoke_prelude(self._fn, type_info, _resume_init)
        self._run = _InvokeRun(
            prelude,
            self._log,
            self._fn.codec,
            watchers=self._watchers,
        )
        await self._run.resume()

    async def wait(self) -> _T_co:
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        return cast("_T_co", await self._run.run())

    async def close(self) -> None:
        if self._run:
            await self._run.close()
            self._run = None

    @overload
    async def complete_promise(
        self,
        id_: str,
        *,
        result: object,
    ) -> None: ...
    @overload
    async def complete_promise(
        self,
        id_: str,
        *,
        exception: Exception,
    ) -> None: ...
    async def complete_promise(
        self,
        id_: str,
        *,
        result: object | None = None,
        exception: Exception | None = None,
    ) -> None:
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        if exception is not None:
            await self._run.complete_external_promise(id_, exception=exception)
        elif result is not None:
            await self._run.complete_external_promise(id_, result=result)
        else:
            msg = "Either result or error must be provided"
            raise ValueError(msg)

    @overload
    def open_stream(self, name: str, mode: Literal["w"]) -> StreamWriter[Any]: ...
    @overload
    def open_stream(self, name: str, mode: Literal["r"]) -> Stream[Any, None]: ...
    def open_stream(
        self, name: str, mode: Literal["w", "r"]
    ) -> StreamWriter[Any] | Stream[Any, None]:
        if self._run is not None:
            msg = "open_stream() must be called before start() or resume()"
            raise RuntimeError(msg)
        for n, _, _ in self._fn.inject:
            if name == n:
                break
        else:
            msg = f"Stream parameter '{name}' not found"
            raise ValueError(msg)
        if mode == "r":
            return self.watch_stream({"name": name})
        return _StreamWriter(self, name)

    def watch_stream(
        self,
        labels: dict[str, str],
    ) -> Stream[_T_co, None]:
        if self._run is not None:
            msg = "create_watcher() must be called before start() or resume()"
            raise RuntimeError(msg)

        observer: ObserverStream[_T_co, None] = ObserverStream()
        self._watchers.append((
            labels,
            cast("ObserverStream[object, None]", observer),
        ))
        return observer

    def get_run(self) -> _InvokeRun:
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        return self._run


@final
class _InvokeGuard(Generic[_P, _T_co]):
    __slots__ = ("_job", "_token", "_tracer")

    def __init__(self, job: Invoke[_P, _T_co], tracer: Tracer | None) -> None:
        self._job = job
        self._tracer = tracer
        self._token: Token[Tracer | None] | None = None

    async def __aenter__(self) -> Invoke[_P, _T_co]:
        self._token = current_tracer.set(self._tracer)
        return self._job

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._job.close()
        if self._token:
            current_tracer.reset(self._token)


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str


async def _invoke_prelude(
    job_fn: DurableFn[..., _T_co],
    type_info: FunctionType,
    init: Callable[[], InitParams],
) -> _T_co:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, EventLoop)  # noqa: S101

    with Context(job_fn, loop) as ctx:
        init_params = await ctx.run(init)
        if init_params["version"] != _CURRENT_VERSION:
            msg = "version mismatch"
            raise RuntimeError(msg)
        loop.set_key(init_params["nonce"].encode())

        codec = job_fn.codec
        args = tuple(
            codec.decode_json(
                arg,
                type_info.parameter_types.get(
                    type_info.parameters[i + 1], UnspecifiedType
                )
                if i + 1 < len(type_info.parameters)
                else UnspecifiedType,
            )
            for i, arg in enumerate(init_params["args"])
        )
        kwargs = {
            k: codec.decode_json(v, type_info.parameter_types.get(k, UnspecifiedType))
            for k, v in sorted(init_params["kwargs"].items())
        }

        extra_kwargs: dict[str, object] = {}
        closer: list[StreamWriter[Any] | SignalWriter[Any]] = []
        for name, type_, dtype in job_fn.inject:
            if type_ is Stream:
                extra_kwargs[name], stw = await ctx.create_stream(dtype, name=name)
                closer.append(stw)
            elif type_ is Signal:
                extra_kwargs[name], sgw = await ctx.create_signal(dtype, name=name)
                closer.append(sgw)
            elif type_ is StreamWriter:
                _, extra_kwargs[name] = await ctx.create_stream(dtype, name=name)
        try:
            with span("InvokeRun"):
                return await job_fn.fn(ctx, *args, **extra_kwargs, **kwargs)
        finally:
            for c in closer:
                await c.close()


@final
class _InvokeRun:
    __slots__ = (
        "_codec",
        "_lease",
        "_log",
        "_loop",
        "_now",
        "_pending_msg",
        "_prev_ops",
        "_running",
        "_stream_manager",
        "_task",
        "_task_manager",
        "_tracer",
    )

    def __init__(
        self,
        task: Coroutine[Any, Any, object],
        log: LogStorage,
        codec: Codec,
        *,
        watchers: list[tuple[dict[str, str], StreamObserver]] | None = None,
    ) -> None:
        self._loop = create_loop(asyncio.get_running_loop())
        self._task = self._loop.create_task(task)
        self._log = log
        self._codec = codec
        self._running: bool = False
        self._lease: bytes | None = None
        self._pending_msg: list[Entry] = []
        self._prev_ops: set[str] = set()
        self._now = 0
        self._task_manager = TaskManager()
        self._stream_manager = StreamManager(watchers)
        self._tracer: Tracer | None = Tracer.current()

    async def close(self) -> None:
        if self._tracer:
            self._tracer.close()
        await self._send_traces(flush=True)
        if self._lease:
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

    async def run(self) -> object:
        if self._task.done():
            return self._task.result()

        self._running = True
        if self._tracer:
            self._tracer.start()
        for msg in self._pending_msg:
            await self.enqueue_log(msg)
        self._pending_msg.clear()
        self._task_manager.start()

        while waitset := await self._step():
            if self._tracer:
                await waitset.block(self.now(), 1_000_000)
                await self._send_traces()
            else:
                await waitset.block(self.now())
            self.tick_realtime()

        # cleanup
        self._loop.close()
        await self._task_manager.close()
        await self._send_traces(flush=True)

        return self._task.result()

    async def _step(self) -> WaitSet | None:
        self._loop.tick(self.now())

        while True:
            result = self._loop.poll_completion(self._task)
            if result is None:
                return result

            new_ops = False
            for s in result.ops:
                sid = s.id
                if sid not in self._prev_ops:
                    await self.enqueue_op(sid, s)
                    new_ops = True
            if new_ops or len(self._prev_ops) != len(result.ops):
                self._prev_ops.clear()
                self._prev_ops.update(s.id for s in result.ops)
            if not new_ops:
                return result

    async def _send_traces(self, *, flush: bool = False) -> None:
        if not self._tracer:
            return
        tid = self._tracer.instance_id
        data = self._tracer.pop_events(flush=flush)
        for i in range(0, len(data), 128):
            trace_entry: Entry = {
                "ts": self.now(),
                "id": random_id(),
                "type": "trace",
                "events": data[i : i + 128],
                "metadata": {
                    "trace.id": tid,
                },
            }
            await self.enqueue_log(trace_entry)

    async def handle_message(
        self,
        offset: int,
        e: Entry,
    ) -> None:
        if e["type"] == "promise.complete":
            id_ = e["promise_id"]
            (return_type,) = self._task_manager.complete_task(id_)
            if "error" in e:
                self._loop.post_completion(
                    id_,
                    exception=_decode_error(e["error"]),
                )
            elif "result" in e:
                try:
                    result = self._codec.decode_json(e["result"], return_type)
                    self._loop.post_completion(id_, result=result)
                except Exception as exc:  # noqa: BLE001
                    self._loop.post_completion(
                        id_,
                        exception=exc,
                    )
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
                _decode_error(e["error"]) if "error" in e else None,
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

    async def enqueue_log(
        self,
        entry: Entry,
    ) -> None:
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

                set_annotations(
                    promise_create_entry,
                    labels=op.annotations.labels,
                )
                if self._tracer:
                    op_span = self._tracer.new_op_span(
                        op.annotations.get_name(),
                        promise_create_entry,
                    )
                else:
                    op_span = None
                await self.enqueue_log(promise_create_entry)

                async def cb() -> None:
                    entry: PromiseCompleteEntry = {
                        "ts": -1,
                        "id": derive_id(id_),
                        "type": "promise.complete",
                        "promise_id": id_,
                    }
                    with (
                        op_span.new_span(op.annotations.get_name())
                        if op_span
                        else NULL_SPAN
                    ) as span:
                        try:
                            result = op.callable(*op.args, **op.kwargs)
                            if asyncio.iscoroutine(result):
                                result = await result
                            entry["result"] = self._codec.encode_json(result)
                            span.set_status("OK")
                        except (Exception, asyncio.CancelledError) as e:  # noqa: BLE001
                            entry["error"] = _encode_error(e)
                            span.set_status("ERROR", str(e))

                    if op_span:
                        op_span.end(entry)
                    entry["ts"] = self.now()
                    await self.enqueue_log(entry)

                def done(f: OpFuture) -> None:
                    if f.cancelled():
                        sid = f.id
                        if self._task_manager.has_pending(sid):
                            # pending task cancelled, should be
                            # completed by a promise.complete
                            pass
                        elif (
                            task := self._task_manager.get_task(sid)
                        ) and not task.done():
                            _ = task.get_loop().call_soon(task.cancel)

                sid = id_
                if self._running:
                    self._task_manager.add_task(
                        sid,
                        cb(),
                        op.context,
                        op.return_type,
                    )
                else:
                    self._task_manager.add_pending(sid, cb, op.context, op.return_type)
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
                        "stream:" + op.annotations.get_name(),
                        stream_create_entry,
                    )
                else:
                    op_span = None

                self._stream_manager.create_stream(
                    stream_id,
                    op.observer,
                    op.dtype,
                    op.annotations.labels,
                    op_span,
                )

                set_annotations(
                    stream_create_entry,
                    labels=op.annotations.labels,
                )
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
                            {
                                "type": "event",
                                "ts": self.now(),
                                "kind": "stream",
                            },
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
                            "error": _encode_error(op.exception),
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
            case ExternalPromiseCreate():
                promise_create_entry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise.create",
                }
                set_annotations(
                    promise_create_entry,
                    labels=op.annotations.labels,
                )
                if self._tracer:
                    _ = self._tracer.new_op_span(
                        op.annotations.get_name(), promise_create_entry
                    )
                self._task_manager.add_future(id_, op.return_type)
                await self.enqueue_log(promise_create_entry)
            case ExternalPromiseComplete():
                promise_complete_entry: PromiseCompleteEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise.complete",
                    "promise_id": op.promise_id,
                }
                if op.exception is not None:
                    promise_complete_entry["error"] = _encode_error(op.exception)
                else:
                    promise_complete_entry["result"] = self._codec.encode_json(op.value)

                if self._tracer:
                    self._tracer.end_op_span(op.promise_id, promise_complete_entry)
                await self.enqueue_log(promise_complete_entry)
                self._loop.post_completion(id_, result=None)
            case _:
                assert_never(op)

    async def complete_external_promise(
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
            "id": derive_id(id_),
            "type": "promise.complete",
            "promise_id": id_,
        }
        if exception is not None:
            entry["error"] = _encode_error(exception)
        elif result is not None:
            entry["result"] = self._codec.encode_json(result)
        else:
            msg = "Either result or error must be provided"
            raise ValueError(msg)
        if self._tracer:
            self._tracer.end_op_span(id_, entry)
        await self.enqueue_log(entry)

    async def send_stream(
        self,
        matcher: dict[str, str],
        value: object,
    ) -> int:
        cnt = 0
        ts = self.now()
        matching_streams = self._stream_manager.find_matching_streams(matcher)

        for stream_id, entry_span in matching_streams:
            entry: StreamEmitEntry = {
                "ts": ts,
                "id": random_id(),
                "type": "stream.emit",
                "stream_id": stream_id,
                "value": self._codec.encode_json(value),
            }
            if entry_span:
                entry_span.attach(
                    entry,
                    {
                        "type": "event",
                        "ts": ts,
                        "kind": "stream",
                    },
                )
            await self.enqueue_log(entry)
            cnt += 1
        return cnt

    async def close_stream(
        self,
        matcher: dict[str, str],
        exc: Exception | None = None,
    ) -> int:
        cnt = 0
        ts = self.now()
        matching_streams = self._stream_manager.find_matching_streams(matcher)

        for stream_id, entry_span in matching_streams:
            entry: StreamCompleteEntry = {
                "type": "stream.complete",
                "ts": ts,
                "id": random_id(),
                "stream_id": stream_id,
            }
            if exc:
                entry["error"] = _encode_error(exc)
            if entry_span:
                entry_span.end(entry)
            await self.enqueue_log(entry)
            cnt += 1
        return cnt


def _resume_init() -> InitParams:
    msg = "not started"
    raise RuntimeError(msg)


def _encode_error(error: Exception | CancelledError) -> ErrorInfo:
    if type(error) is asyncio.CancelledError:
        return {
            "code": -2,
            "message": repr(error),
        }
    return {
        "code": -1,
        "message": repr(error),
    }


def _decode_error(error_info: ErrorInfo) -> Exception | CancelledError:
    if error_info["code"] == -2:
        return asyncio.CancelledError()
    return Exception(f"[{error_info['code']}] {error_info['message']}")


@final
class _StreamWriter(Generic[_T]):
    __slots__ = ("_invoke", "_name")

    def __init__(self, invoke: Invoke[..., Any], name: str) -> None:
        self._invoke = invoke
        self._name = name

    async def send(self, value: _T) -> None:
        while (  # noqa: ASYNC110
            await self._invoke.get_run().send_stream({"name": self._name}, value) == 0
        ):
            await asyncio.sleep(0.1)

    async def close(self, error: Exception | None = None) -> None:
        while (  # noqa: ASYNC110
            await self._invoke.get_run().close_stream({"name": self._name}, error) == 0
        ):
            await asyncio.sleep(0.1)
