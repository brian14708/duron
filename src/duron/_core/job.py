from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import time
from hashlib import blake2b
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
    final,
    overload,
)

from typing_extensions import TypedDict, assert_never

from duron._core.ops import (
    Barrier,
    ExternalPromiseCreate,
    FnCall,
    StreamClose,
    StreamCreate,
    StreamEmit,
)
from duron._core.stream import ObserverStream
from duron._loop import EventLoop, create_loop
from duron.codec import Codec, JSONValue
from duron.log import is_entry
from duron.typing import unspecified

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._core.fn import Fn
    from duron._core.ops import (
        Op,
        StreamObserver,
    )
    from duron._core.stream import Stream
    from duron._loop import OpFuture, WaitSet
    from duron.codec import Codec, FunctionType
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
    from duron.typing import TypeHint


_T = TypeVar("_T")
_P = ParamSpec("_P")

_CURRENT_VERSION = 0


@final
class Job(Generic[_P, _T]):
    __slots__ = ("_job_fn", "_log", "_run", "_watchers")

    def __init__(
        self,
        job_fn: Fn[_P, _T],
        log: LogStorage,
    ) -> None:
        self._job_fn = job_fn
        self._log = log
        self._run: _JobRun | None = None
        self._watchers: list[
            tuple[
                Callable[[dict[str, JSONValue]], bool],
                StreamObserver[object],
            ]
        ] = []

    async def start(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        async def get_init() -> JobInitParams:
            return {
                "version": _CURRENT_VERSION,
                "args": [codec.encode_json(arg) for arg in args],
                "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
            }

        codec = self._job_fn.codec
        type_info = codec.inspect_function(self._job_fn.fn)
        job_prelude = _job_prelude(self._job_fn, type_info, get_init)
        self._run = _JobRun(
            job_prelude,
            self._log,
            codec,
            watchers=self._watchers,
        )
        await self._run.resume()

    async def resume(self) -> None:
        async def cb() -> JobInitParams:
            raise Exception("not started")

        type_info = self._job_fn.codec.inspect_function(self._job_fn.fn)
        job = _job_prelude(self._job_fn, type_info, cb)
        self._run = _JobRun(
            job,
            self._log,
            self._job_fn.codec,
            watchers=self._watchers,
        )
        await self._run.resume()

    async def wait(self) -> _T:
        if self._run is None:
            raise RuntimeError("Job not started")
        return cast("_T", await self._run.run())

    async def close(self) -> None:
        if self._run:
            await self._run.close()
            self._run = None

    @overload
    async def complete_promise(
        self,
        id: str,
        *,
        result: object,
    ) -> None: ...
    @overload
    async def complete_promise(
        self,
        id: str,
        *,
        error: BaseException,
    ) -> None: ...
    async def complete_promise(
        self,
        id: str,
        *,
        result: object | None = None,
        error: BaseException | None = None,
    ) -> None:
        if self._run is None:
            raise RuntimeError("Job not started")
        if error is not None:
            await self._run.complete_external_promise(id, error=error)
        elif result is not None:
            await self._run.complete_external_promise(id, result=result)
        else:
            raise ValueError("Either result or error must be provided")

    async def send_stream(
        self,
        predicate: Callable[[dict[str, JSONValue]], bool],
        value: object,
    ) -> int:
        if self._run is None:
            raise RuntimeError("Job not started")
        return await self._run.send_stream(predicate, value)

    async def close_stream(
        self,
        predicate: Callable[[dict[str, JSONValue]], bool],
        error: BaseException | None = None,
    ) -> int:
        if self._run is None:
            raise RuntimeError("Job not started")
        return await self._run.close_stream(predicate, error)

    def watch_stream(
        self,
        predicate: Callable[[dict[str, JSONValue]], bool],
    ) -> Stream[_T, None]:
        if self._run is not None:
            raise RuntimeError(
                "create_watcher() must be called before start() or resume()"
            )

        observer: ObserverStream[_T, None] = ObserverStream()
        self._watchers.append((
            predicate,
            cast("StreamObserver[object]", cast("StreamObserver[_T]", observer)),
        ))
        return observer


class JobInitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]


async def _job_prelude(
    job_fn: Fn[..., _T],
    type_info: FunctionType,
    init: Callable[[], Coroutine[Any, Any, JobInitParams]],
) -> _T:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, EventLoop)
    from duron._core.context import Context

    with Context(job_fn, loop) as ctx:
        init_params = await ctx.run(init)
        if init_params["version"] != _CURRENT_VERSION:
            raise Exception("version mismatch")
        codec = job_fn.codec

        args = tuple(
            codec.decode_json(
                arg,
                type_info.parameter_types.get(type_info.parameters[i + 1], unspecified)
                if i + 1 < len(type_info.parameters)
                else unspecified,
            )
            for i, arg in enumerate(init_params["args"])
        )
        kwargs = {
            k: codec.decode_json(v, type_info.parameter_types.get(k, unspecified))
            for k, v in init_params["kwargs"].items()
        }
        return await job_fn.fn(ctx, *args, **kwargs)


@final
class _JobRun:
    __slots__ = (
        "_loop",
        "_task",
        "_log",
        "_codec",
        "_running",
        "_pending_msg",
        "_pending_task",
        "_pending_ops",
        "_now",
        "_tasks",
        "_streams",
        "_watchers",
    )

    def __init__(
        self,
        task: Coroutine[Any, Any, object],
        log: LogStorage,
        codec: Codec,
        watchers: list[
            tuple[Callable[[dict[str, JSONValue]], bool], StreamObserver[object]]
        ]
        | None = None,
    ) -> None:
        self._loop = create_loop(asyncio.get_event_loop(), b"")
        self._task = self._loop.create_task(task)
        self._log = log
        self._codec = codec
        self._running: bytes | None = None
        self._pending_msg: list[Entry] = []
        self._pending_task: dict[
            str, tuple[Callable[[], Coroutine[Any, Any, None]], TypeHint[Any]]
        ] = {}
        self._pending_ops: set[bytes] = set()
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
        async for o, entry in self._log.stream(None, False):
            ts = entry["ts"]
            self._now = max(self._now, ts)
            _ = await self._step()
            if is_entry(entry):
                await self.handle_message(o, entry)
                _ = await self._step()
            recvd_msgs.add(entry["id"])

        msgs: list[Entry] = []
        for msg in self._pending_msg:
            if msg["id"] not in recvd_msgs:
                msgs.append(msg)
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
            if result is None or self._pending_ops.issuperset(o.id for o in result.ops):
                return result

            for s in result.ops:
                sid = s.id
                if sid not in self._pending_ops:
                    self._pending_ops.add(sid)
                    await self.enqueue_op(sid, s)

    async def handle_message(
        self,
        offset: int,
        e: Entry,
    ) -> None:
        if e["type"] == "promise/complete":
            pending_info = self._pending_task.pop(e["promise_id"], None)
            task_info = self._tasks.get(e["promise_id"], None)

            id = _decode_id(e["promise_id"])

            return_type: TypeHint[Any] = unspecified
            if pending_info is not None:
                _, return_type = pending_info
            elif task_info is not None:
                _, return_type = task_info
            else:
                raise AssertionError("unreachable")

            if "error" in e:
                self._loop.post_completion(
                    id,
                    exception=_decode_error(e["error"]),
                )
                self._pending_ops.discard(id)
            elif "result" in e:
                try:
                    result = self._codec.decode_json(e["result"], return_type)
                    self._loop.post_completion(id, result=result)
                except Exception as exc:
                    self._loop.post_completion(
                        id,
                        exception=exc,
                    )
                self._pending_ops.discard(id)
            else:
                raise ValueError(f"Invalid promise/complete entry: {e!r}")
        elif e["type"] == "stream/create":
            id = _decode_id(e["id"])
            if e["id"] not in self._streams:
                self._loop.post_completion(id, exception=ValueError("Stream not found"))
            else:
                self._loop.post_completion(id, result=e["id"])
            self._pending_ops.discard(id)
        elif e["type"] == "stream/emit":
            id = _decode_id(e["id"])
            if e["stream_id"] not in self._streams:
                self._loop.post_completion(id, exception=ValueError("Stream not found"))
            else:
                obs, tv, _ = self._streams[e["stream_id"]]
                for ob in obs:
                    ob.on_next(
                        offset,
                        self._codec.decode_json(e["value"], tv),
                    )
                self._loop.post_completion(id, result=None)
            self._pending_ops.discard(id)
        elif e["type"] == "stream/complete":
            id = _decode_id(e["id"])
            if e["stream_id"] not in self._streams:
                self._loop.post_completion(id, exception=ValueError("Stream not found"))
            else:
                obs, _, _ = self._streams[e["stream_id"]]
                self._loop.post_completion(id, result=None)
                for ob in obs:
                    if "error" in e:
                        ob.on_close(offset, _decode_error(e["error"]))
                    else:
                        ob.on_close(offset, None)

                _ = self._streams.pop(e["stream_id"], None)
            self._pending_ops.discard(id)
        elif e["type"] == "barrier":
            id = _decode_id(e["id"])
            self._loop.post_completion(id, result=offset)
            self._pending_ops.discard(id)

    async def enqueue_log(self, entry: Entry, flush: bool = False):
        if not self._running:
            self._pending_msg.append(entry)
        else:
            offset = await self._log.append(self._running, entry)
            if flush:
                await self._log.flush(self._running)
            await self.handle_message(offset, entry)

    async def enqueue_op(self, id: bytes, fut: OpFuture[object]) -> None:
        op = cast("Op", fut.params)
        match op:
            case FnCall():
                promise_create_entry: PromiseCreateEntry = {
                    "ts": self.now(),
                    "id": _encode_id(id),
                    "type": "promise/create",
                }
                if op.metadata:
                    promise_create_entry["metadata"] = op.metadata
                await self.enqueue_log(promise_create_entry)

                async def cb() -> None:
                    entry: PromiseCompleteEntry = {
                        "ts": self.now(),
                        "id": _encode_id(id, 1),
                        "type": "promise/complete",
                        "promise_id": _encode_id(id),
                    }
                    try:
                        result = op.callable(*op.args, **op.kwargs)
                        if asyncio.iscoroutine(result):
                            result = await result
                        entry["result"] = self._codec.encode_json(result)
                    except Exception as e:
                        entry["error"] = _encode_error(e)
                    except asyncio.CancelledError as e:
                        entry["error"] = _encode_error(e)

                    await self.enqueue_log(entry)

                def done(f: OpFuture[object]) -> None:
                    if f.cancelled():
                        sid = _encode_id(f.id)
                        if self._pending_task.get(sid, None):
                            # pending task cancelled
                            pass
                        elif task_info := self._tasks.get(sid, None):
                            task, _ = task_info
                            if not task.done():
                                _ = task.get_loop().call_soon(task.cancel)

                fut.add_done_callback(done)
                sid = _encode_id(id)
                if self._running:
                    self._tasks[sid] = (asyncio.create_task(cb()), op.return_type)
                else:
                    self._pending_task[sid] = (cb, op.return_type)

            case StreamCreate():
                stream_id = _encode_id(id)

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
                    "id": _encode_id(id),
                    "stream_id": op.stream_id,
                    "type": "stream/emit",
                    "value": self._codec.encode_json(op.value),
                }
                await self.enqueue_log(stream_emit_entry)
            case StreamClose():
                if op.exception:
                    stream_close_entry_err: StreamCompleteEntry = {
                        "ts": self.now(),
                        "id": _encode_id(id),
                        "stream_id": op.stream_id,
                        "type": "stream/complete",
                        "error": _encode_error(op.exception),
                    }
                    await self.enqueue_log(stream_close_entry_err)
                else:
                    stream_close_entry: StreamCompleteEntry = {
                        "ts": self.now(),
                        "id": _encode_id(id),
                        "stream_id": op.stream_id,
                        "type": "stream/complete",
                    }
                    await self.enqueue_log(stream_close_entry)
            case Barrier():
                barrier_entry: BarrierEntry = {
                    "ts": self.now(),
                    "id": _encode_id(id),
                    "type": "barrier",
                }
                await self.enqueue_log(barrier_entry, flush=True)
            case ExternalPromiseCreate():
                promise_create_entry = {
                    "ts": self.now(),
                    "id": _encode_id(id),
                    "type": "promise/create",
                }
                if op.metadata:
                    promise_create_entry["metadata"] = op.metadata
                self._tasks[_encode_id(id)] = (asyncio.Future(), op.return_type)
                await self.enqueue_log(promise_create_entry)
            case _:
                assert_never(op)

    async def complete_external_promise(
        self,
        id: str,
        *,
        result: object | None = None,
        error: BaseException | None = None,
    ) -> None:
        if id not in self._tasks:
            raise ValueError("Promise not found")
        entry: PromiseCompleteEntry = {
            "ts": self.now(),
            "id": _encode_id(_decode_id(id), 1),
            "type": "promise/complete",
            "promise_id": id,
        }
        if error is not None:
            entry["error"] = _encode_error(error)
        elif result is not None:
            entry["result"] = self._codec.encode_json(result)
        else:
            raise ValueError("Either result or error must be provided")
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
                    "id": _encode_id(os.urandom(12)),
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
                    "id": _encode_id(os.urandom(12)),
                    "type": "stream/complete",
                    "stream_id": stream_id,
                    "error": _encode_error(error),
                }
            else:
                entry = {
                    "ts": ts,
                    "id": _encode_id(os.urandom(12)),
                    "type": "stream/complete",
                    "stream_id": stream_id,
                }
            await self.enqueue_log(entry)
            cnt += 1
        return cnt


def _encode_id(id: bytes, flag: int = 0) -> str:
    if flag != 0:
        id = blake2b(
            flag.to_bytes(4, "little", signed=True) + id, digest_size=12
        ).digest()
    return base64.b64encode(id).decode()


def _decode_id(encoded: str) -> bytes:
    return base64.b64decode(encoded)


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
