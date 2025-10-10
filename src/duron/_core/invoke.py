from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Final, Generic, Literal, cast
from typing_extensions import (
    Any,
    ParamSpec,
    TypedDict,
    TypeVar,
    assert_never,
    final,
    overload,
)

from duron._core.config import config
from duron._core.context import Context
from duron._core.ops import (
    Barrier,
    ExternalPromiseCreate,
    FnCall,
    StreamClose,
    StreamCreate,
    StreamEmit,
)
from duron._core.signal import Signal
from duron._core.stream import ObserverStream, Stream, StreamWriter
from duron._loop import EventLoop, create_loop
from duron.codec import Codec, JSONValue
from duron.log import derive_id, is_entry, random_id
from duron.typing import Unspecified, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import TracebackType

    from duron._core.ops import (
        Op,
        StreamObserver,
    )
    from duron._decorator.fn import Fn
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
    from duron.typing import FunctionType, TypeHint


_T_co = TypeVar("_T_co", covariant=True)
_T = TypeVar("_T")
_P = ParamSpec("_P")

_CURRENT_VERSION: Final = 0


@final
class Invoke(Generic[_P, _T_co]):
    __slots__ = ("_fn", "_log", "_run", "_watchers")

    def __init__(
        self,
        fn: Fn[_P, _T_co],
        log: LogStorage,
    ) -> None:
        self._fn = fn
        self._log = log
        self._run: _InvokeRun | None = None
        self._watchers: list[
            tuple[
                Callable[[dict[str, JSONValue]], bool],
                StreamObserver[object],
            ]
        ] = []

    @staticmethod
    def invoke(
        fn: Fn[_P, _T_co],
        log: LogStorage,
    ) -> contextlib.AbstractAsyncContextManager[Invoke[_P, _T_co]]:
        return _InvokeGuard(Invoke(fn, log))

    async def start(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        def get_init() -> InitParams:
            return {
                "version": _CURRENT_VERSION,
                "args": [codec.encode_json(arg) for arg in args],
                "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
                "nonce": random_id(),
            }

        codec = self._fn.codec
        type_info = inspect_function(self._fn.fn)
        prelude = _invoke_prelude(self._fn, type_info, get_init)
        self._run = _InvokeRun(
            prelude,
            self._log,
            codec,
            watchers=self._watchers,
            debug=config.debug,
        )
        await self._run.resume()

    async def resume(self) -> None:
        def cb() -> InitParams:
            msg = "not started"
            raise RuntimeError(msg)

        type_info = inspect_function(self._fn.fn)
        prelude = _invoke_prelude(self._fn, type_info, cb)
        self._run = _InvokeRun(
            prelude,
            self._log,
            self._fn.codec,
            watchers=self._watchers,
            debug=config.debug,
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
        error: BaseException,
    ) -> None: ...
    async def complete_promise(
        self,
        id_: str,
        *,
        result: object | None = None,
        error: BaseException | None = None,
    ) -> None:
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        if error is not None:
            await self._run.complete_external_promise(id_, error=error)
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

            def pred(md: dict[str, JSONValue]) -> bool:
                return md.get("param.name") == name

            return self.watch_stream(pred)
        return _StreamWriter(self, name)

    def watch_stream(
        self,
        predicate: Callable[[dict[str, JSONValue]], bool],
    ) -> Stream[_T_co, None]:
        if self._run is not None:
            msg = "create_watcher() must be called before start() or resume()"
            raise RuntimeError(msg)

        observer: ObserverStream[_T_co, None] = ObserverStream()
        self._watchers.append((
            predicate,
            cast("StreamObserver[object]", cast("StreamObserver[_T_co]", observer)),
        ))
        return observer

    def get_run(self) -> _InvokeRun:
        if self._run is None:
            msg = "Job not started"
            raise RuntimeError(msg)
        return self._run


@final
class _InvokeGuard(Generic[_P, _T_co]):
    __slots__ = ("_job",)

    def __init__(self, job: Invoke[_P, _T_co]) -> None:
        self._job = job

    async def __aenter__(self) -> Invoke[_P, _T_co]:
        return self._job

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._job.close()


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str


async def _invoke_prelude(
    job_fn: Fn[..., _T_co],
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
        extra_kwargs: dict[str, object] = {}
        for name, type_, dtype in job_fn.inject:
            with ctx.metadata({"param.name": name}):
                if type_ is Stream:
                    extra_kwargs[name], _ = await ctx.create_stream(dtype)
                elif type_ is Signal:
                    extra_kwargs[name], _ = await ctx.create_signal(dtype)
                elif type_ is StreamWriter:
                    _, extra_kwargs[name] = await ctx.create_stream(dtype)
        codec = job_fn.codec

        args = tuple(
            codec.decode_json(
                arg,
                type_info.parameter_types.get(type_info.parameters[i + 1], Unspecified)
                if i + 1 < len(type_info.parameters)
                else Unspecified,
            )
            for i, arg in enumerate(init_params["args"])
        )
        kwargs = {
            k: codec.decode_json(v, type_info.parameter_types.get(k, Unspecified))
            for k, v in sorted(init_params["kwargs"].items())
        }
        return await job_fn.fn(ctx, *args, **extra_kwargs, **kwargs)


@final
class _InvokeRun:
    __slots__ = (
        "_codec",
        "_debug",
        "_log",
        "_loop",
        "_now",
        "_pending_msg",
        "_pending_ops",
        "_pending_task",
        "_running",
        "_streams",
        "_task",
        "_tasks",
        "_watchers",
    )

    def __init__(
        self,
        task: Coroutine[Any, Any, object],
        log: LogStorage,
        codec: Codec,
        *,
        watchers: list[
            tuple[Callable[[dict[str, JSONValue]], bool], StreamObserver[object]]
        ]
        | None = None,
        debug: bool = False,
    ) -> None:
        self._loop = create_loop(asyncio.get_running_loop())
        if debug:
            self._loop.set_debug(True)
        self._task = self._loop.create_task(task)
        self._log = log
        self._codec = codec
        self._running: bytes | None = None
        self._pending_msg: list[Entry] = []
        self._pending_task: dict[
            str,
            tuple[Callable[[], Coroutine[Any, Any, None]], TypeHint[Any]],
        ] = {}
        self._pending_ops: set[str] = set()
        self._now = 0
        self._tasks: dict[str, tuple[asyncio.Future[None], TypeHint[Any]]] = {}
        self._streams: dict[
            str,
            tuple[
                list[StreamObserver[object]],
                TypeHint[Any],
                dict[str, JSONValue] | None,
            ],
        ] = {}
        self._watchers = watchers or []
        self._debug: dict[str, JSONValue] | None = (
            {"run.id": random_id()} if debug else None
        )

    async def close(self) -> None:
        for task, _ in self._tasks.values():
            _ = task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        _ = await self._step()

        _ = self._task.cancel()
        _ = await self._step()
        self._loop.close()

    def now(self) -> int:
        if self._running:
            t = time.time_ns()
            t //= 1_000
            self._now = max(self._now + 1, t)
        return self._now

    async def resume(self) -> None:
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

        self._running = await self._log.acquire_lease()
        try:
            for msg in self._pending_msg:
                await self.enqueue_log(msg)
            self._pending_msg.clear()
            for key, (task_fn, return_type) in self._pending_task.items():
                self._tasks[key] = (asyncio.create_task(task_fn()), return_type)
            self._pending_task.clear()

            while waitset := await self._step():
                await waitset.block(self.now())
            return self._task.result()
        finally:
            await self._log.release_lease(self._running)
            self._running = None

    async def _step(self) -> WaitSet | None:
        while True:
            self._loop.tick(self.now())
            result = self._loop.poll_completion(self._task)
            if result is None:
                return result

            new_ops = False
            for s in result.ops:
                sid = s.id
                if sid not in self._pending_ops:
                    self._pending_ops.add(sid)
                    await self.enqueue_op(sid, s)
                    new_ops = True
            if not new_ops:
                return result

    async def handle_message(
        self,
        offset: int,
        e: Entry,
    ) -> None:
        if e["type"] == "promise/complete":
            pending_info = self._pending_task.pop(e["promise_id"], None)
            task_info = self._tasks.get(e["promise_id"], None)

            id_ = e["promise_id"]

            return_type: TypeHint[Any] = Unspecified
            if pending_info is not None:
                _, return_type = pending_info
            elif task_info is not None:
                _, return_type = task_info
            else:
                msg = "unreachable"
                raise AssertionError(msg)

            if "error" in e:
                self._loop.post_completion(
                    id_,
                    exception=_decode_error(e["error"]),
                )
                self._pending_ops.discard(id_)
            elif "result" in e:
                try:
                    result = self._codec.decode_json(e["result"], return_type)
                    self._loop.post_completion(id_, result=result)
                except Exception as exc:  # noqa: BLE001
                    self._loop.post_completion(
                        id_,
                        exception=exc,
                    )
                self._pending_ops.discard(id_)
            else:
                msg = f"Invalid promise/complete entry: {e!r}"
                raise ValueError(msg)
        elif e["type"] == "stream/create":
            id_ = e["id"]
            if e["id"] not in self._streams:
                self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
            else:
                self._loop.post_completion(id_, result=e["id"])
            self._pending_ops.discard(id_)
        elif e["type"] == "stream/emit":
            id_ = e["id"]
            if e["stream_id"] not in self._streams:
                self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
            else:
                obs, tv, _ = self._streams[e["stream_id"]]
                for ob in obs:
                    ob.on_next(
                        offset,
                        self._codec.decode_json(e["value"], tv),
                    )
                self._loop.post_completion(id_, result=None)
            self._pending_ops.discard(id_)
        elif e["type"] == "stream/complete":
            id_ = e["id"]
            if e["stream_id"] not in self._streams:
                self._loop.post_completion(
                    id_, exception=ValueError("Stream not found")
                )
            else:
                obs, _, _ = self._streams[e["stream_id"]]
                self._loop.post_completion(id_, result=None)
                for ob in obs:
                    if "error" in e:
                        ob.on_close(offset, _decode_error(e["error"]))
                    else:
                        ob.on_close(offset, None)

                _ = self._streams.pop(e["stream_id"], None)
            self._pending_ops.discard(id_)
        elif e["type"] == "barrier":
            id_ = e["id"]
            self._loop.post_completion(id_, result=offset)
            self._pending_ops.discard(id_)

    async def enqueue_log(self, entry: Entry, *, flush: bool = False) -> None:
        if not self._running:
            self._pending_msg.append(entry)
        else:
            if self._debug:
                if "debug" in entry:
                    entry["debug"] = {
                        **entry["debug"],
                        **self._debug,
                    }
                else:
                    entry["debug"] = self._debug
            else:
                _ = entry.pop("debug", None)
            offset = await self._log.append(self._running, entry)
            if flush:
                await self._log.flush(self._running)
            await self.handle_message(offset, entry)

    async def enqueue_op(self, id_: str, fut: OpFuture[object]) -> None:
        op = cast("Op", fut.params)
        match op:
            case FnCall():
                promise_create_entry: PromiseCreateEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise/create",
                }
                if op.metadata:
                    promise_create_entry["metadata"] = op.metadata
                if self._debug:
                    promise_create_entry["debug"] = {
                        "fn.name": str(
                            getattr(op.callable, "__qualname__", op.callable)
                        ),
                    }
                await self.enqueue_log(promise_create_entry)

                async def cb() -> None:
                    now_us = self.now()
                    entry: PromiseCompleteEntry = {
                        "ts": now_us,
                        "id": derive_id(id_),
                        "type": "promise/complete",
                        "promise_id": id_,
                    }
                    try:
                        result = op.callable(*op.args, **op.kwargs)
                        if asyncio.iscoroutine(result):
                            result = await result
                        entry["result"] = self._codec.encode_json(result)
                    except Exception as e:  # noqa: BLE001
                        entry["error"] = _encode_error(e)
                    except asyncio.CancelledError as e:
                        entry["error"] = _encode_error(e)

                    await self.enqueue_log(entry)

                def done(f: OpFuture[object]) -> None:
                    if f.cancelled():
                        sid = f.id
                        if self._pending_task.get(sid, None):
                            # pending task cancelled
                            pass
                        elif task_info := self._tasks.get(sid, None):
                            task, _ = task_info
                            if not task.done():
                                _ = task.get_loop().call_soon(task.cancel)

                fut.add_done_callback(done)
                sid = id_
                if self._running:
                    self._tasks[sid] = (asyncio.create_task(cb()), op.return_type)
                else:
                    self._pending_task[sid] = (cb, op.return_type)

            case StreamCreate():
                stream_id = id_

                # Determine which observer to use
                ob = [op.observer] if op.observer else []

                # Check if any external watchers match
                for predicate, watcher in self._watchers:
                    if predicate(op.metadata or {}):
                        ob.append(watcher)

                self._streams[stream_id] = (ob, op.dtype, op.metadata)

                stream_create_entry: StreamCreateEntry = {
                    "ts": self.now(),
                    "id": stream_id,
                    "type": "stream/create",
                }
                if op.metadata:
                    stream_create_entry["metadata"] = op.metadata
                await self.enqueue_log(stream_create_entry)

            case StreamEmit():
                stream_emit_entry: StreamEmitEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "stream_id": op.stream_id,
                    "type": "stream/emit",
                    "value": self._codec.encode_json(op.value),
                }
                await self.enqueue_log(stream_emit_entry)
            case StreamClose():
                if op.exception:
                    stream_close_entry_err: StreamCompleteEntry = {
                        "ts": self.now(),
                        "id": id_,
                        "stream_id": op.stream_id,
                        "type": "stream/complete",
                        "error": _encode_error(op.exception),
                    }
                    await self.enqueue_log(stream_close_entry_err)
                else:
                    stream_close_entry: StreamCompleteEntry = {
                        "ts": self.now(),
                        "id": id_,
                        "stream_id": op.stream_id,
                        "type": "stream/complete",
                    }
                    await self.enqueue_log(stream_close_entry)
            case Barrier():
                barrier_entry: BarrierEntry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "barrier",
                }
                await self.enqueue_log(barrier_entry, flush=True)
            case ExternalPromiseCreate():
                promise_create_entry = {
                    "ts": self.now(),
                    "id": id_,
                    "type": "promise/create",
                }
                if op.metadata:
                    promise_create_entry["metadata"] = op.metadata
                self._tasks[id_] = (asyncio.Future(), op.return_type)
                await self.enqueue_log(promise_create_entry)
            case _:
                assert_never(op)

    async def complete_external_promise(
        self,
        id_: str,
        *,
        result: object | None = None,
        error: BaseException | None = None,
    ) -> None:
        if id_ not in self._tasks:
            msg = "Promise not found"
            raise ValueError(msg)
        now_us = self.now()
        entry: PromiseCompleteEntry = {
            "ts": now_us,
            "id": derive_id(id_),
            "type": "promise/complete",
            "promise_id": id_,
        }
        if error is not None:
            entry["error"] = _encode_error(error)
        elif result is not None:
            entry["result"] = self._codec.encode_json(result)
        else:
            msg = "Either result or error must be provided"
            raise ValueError(msg)
        await self.enqueue_log(entry)

    async def send_stream(
        self,
        predicate: Callable[[dict[str, JSONValue]], bool],
        value: object,
    ) -> int:
        cnt = 0
        ts = self.now()
        for stream_id, (_, _, md) in self._streams.items():
            if predicate(md or {}):
                entry: StreamEmitEntry = {
                    "ts": ts,
                    "id": random_id(),
                    "type": "stream/emit",
                    "stream_id": stream_id,
                    "value": self._codec.encode_json(value),
                }
                await self.enqueue_log(entry)
                cnt += 1
        return cnt

    async def close_stream(
        self,
        predicate: Callable[[dict[str, JSONValue]], bool],
        error: BaseException | None = None,
    ) -> int:
        cnt = 0
        ts = self.now()
        # Collect matching stream IDs first to avoid modifying dict during iteration
        matching_streams = [
            stream_id
            for stream_id, (_, _, md) in self._streams.items()
            if predicate(md or {})
        ]

        for stream_id in matching_streams:
            if error:
                entry: StreamCompleteEntry = {
                    "ts": ts,
                    "id": random_id(),
                    "type": "stream/complete",
                    "stream_id": stream_id,
                    "error": _encode_error(error),
                }
            else:
                entry = {
                    "ts": ts,
                    "id": random_id(),
                    "type": "stream/complete",
                    "stream_id": stream_id,
                }
            await self.enqueue_log(entry)
            cnt += 1
        return cnt


def _encode_error(error: BaseException) -> ErrorInfo:
    if type(error) is asyncio.CancelledError:
        return {
            "code": -2,
            "message": repr(error),
        }
    return {
        "code": -1,
        "message": repr(error),
    }


def _decode_error(error_info: ErrorInfo) -> BaseException:
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
        def pred(md: dict[str, JSONValue]) -> bool:
            return md.get("param.name") == self._name

        while (  # noqa: ASYNC110
            await self._invoke.get_run().send_stream(pred, value) == 0
        ):
            await asyncio.sleep(0.1)

    async def close(self, error: BaseException | None = None) -> None:
        def pred(md: dict[str, JSONValue]) -> bool:
            return md.get("param.name") == self._name

        while (  # noqa: ASYNC110
            await self._invoke.get_run().close_stream(pred, error) == 0
        ):
            await asyncio.sleep(0.1)
