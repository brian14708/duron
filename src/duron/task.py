from __future__ import annotations

import asyncio
from asyncio.exceptions import CancelledError
import base64
import contextlib
import time
from collections.abc import Awaitable
from hashlib import blake2b
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
    final,
)

from typing_extensions import TypedDict, assert_never

from duron.codec import Codec, JSONValue
from duron.context import Context
from duron.event_loop import EventLoop, create_loop, wrap_future
from duron.log import is_entry
from duron.ops import FnCall, StreamClose, StreamCreate, StreamEmit, TaskRun
from duron.stream import RawStream

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import TracebackType

    from duron.codec import Codec, FunctionType
    from duron.event_loop import OpFuture, WaitSet
    from duron.fn import Fn
    from duron.log import (
        Entry,
        ErrorInfo,
        LogStorage,
        PromiseCompleteEntry,
    )
    from duron.ops import Op
    from duron.stream import Observer


_T = TypeVar("_T")
_P = ParamSpec("_P")

_CURRENT_VERSION = 0


@final
class TaskGuard(Generic[_P, _T]):
    def __init__(self, task: Task[_P, _T]) -> None:
        self._task = task

    async def __aenter__(self) -> Task[_P, _T]:
        return self._task

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._task.close()


@final
class Task(Generic[_P, _T]):
    def __init__(
        self,
        task_fn: Fn[_P, _T],
        log: LogStorage[object, object],
    ) -> None:
        self._task_fn = task_fn
        self._log = log
        self._run: _TaskRun | None = None

    async def start(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        def get_init() -> TaskInitParams:
            return {
                "version": _CURRENT_VERSION,
                "args": [codec.encode_json(arg) for arg in args],
                "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
            }

        codec = self._task_fn.codec
        type_info = codec.inspect_function(self._task_fn.fn)
        task_prelude = _task_prelude(self._task_fn, type_info, get_init)
        self._run = _TaskRun(
            TaskRun(task=task_prelude, return_type=type_info.return_type),
            self._log,
            codec,
        )
        await self._run.resume()

    async def resume(self) -> None:
        def cb() -> TaskInitParams:
            raise Exception("not started")

        type_info = self._task_fn.codec.inspect_function(self._task_fn.fn)
        task = _task_prelude(self._task_fn, type_info, cb)
        self._run = _TaskRun(
            TaskRun(task=task, return_type=type_info.return_type),
            self._log,
            self._task_fn.codec,
        )
        await self._run.resume()

    async def wait(self) -> _T:
        if self._run is None:
            raise RuntimeError("Task not started")
        return cast("_T", await self._run.run())

    async def close(self) -> None:
        if self._run:
            await self._run.close()
            self._run = None


class TaskInitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]


async def _task_prelude(
    task_fn: Fn[..., _T],
    type_info: FunctionType,
    init: Callable[[], TaskInitParams],
) -> _T:
    loop = asyncio.get_event_loop()
    assert isinstance(loop, EventLoop)
    with Context(task_fn, loop) as ctx:
        init_params = await ctx.run(init)
        if init_params["version"] != _CURRENT_VERSION:
            raise Exception("version mismatch")
        codec = task_fn.codec

        args = tuple(
            codec.decode_json(
                arg,
                type_info.parameter_types.get(type_info.parameters[i + 1])
                if i + 1 < len(type_info.parameters)
                else None,
            )
            for i, arg in enumerate(init_params["args"])
        )
        kwargs = {
            k: codec.decode_json(v, type_info.parameter_types.get(k))
            for k, v in init_params["kwargs"].items()
        }
        return await task_fn.fn(ctx, *args, **kwargs)


@final
class _TaskRun:
    def __init__(
        self,
        task: TaskRun,
        log: LogStorage[object, object],
        codec: Codec,
    ) -> None:
        self._loop = create_loop(asyncio.get_running_loop(), b"")
        self._task = self._loop.create_op(task)
        self._log = log
        self._codec = codec
        self._running: object | None = None
        self._pending_msg: list[Entry] = []
        self._pending_task: dict[
            str, tuple[Callable[[], Coroutine[Any, Any, None]], type | None]
        ] = {}
        self._pending_ops: set[bytes] = set()
        self._now = 0
        self._offset: object | None = None
        self._tasks: dict[str, tuple[asyncio.Future[None], type | None]] = {}
        self._streams: dict[
            str,
            tuple[
                RawStream[object],
                Observer[object] | None,
                type | None,
            ],
        ] = {}
        self._preflight_msg: set[str] = set()

    async def close(self) -> None:
        for task, _ in self._tasks.values():
            _ = task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        _ = await self._step()
        self._loop.close()

    def now(self) -> int:
        if self._running:
            t = time.time_ns()
            t -= t % 1_000
            self._now = max(self._now + 1_000, t)
        return self._now

    async def resume(self) -> None:
        recvd_msgs: set[str] = set()
        async for o, entry in self._log.stream(None, False):
            self._offset = o
            ts = _decode_timestamp(entry["ts"])
            self._now = max(self._now, ts)
            _ = await self._step()
            if is_entry(entry):
                await self.handle_message(entry)
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
        bg: asyncio.Task[None] | None = None
        try:
            for msg in self._pending_msg:
                await self.enqueue_log(msg)
            self._pending_msg.clear()
            for key, (task_fn, return_type) in self._pending_task.items():
                self._tasks[key] = (asyncio.create_task(task_fn()), return_type)
            self._pending_task.clear()

            bg = asyncio.create_task(self._follow_log())
            await self._step_loop()

            assert self._task.done()
            assert len(self._pending_ops) == 0

            return self._task.result()
        finally:
            if bg:
                _ = bg.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await bg
            await self._log.release_lease(self._running)
            self._running = None

    async def _follow_log(self) -> None:
        async for _, entry in self._log.stream(self._offset, True):
            self._now = max(self._now, _decode_timestamp(entry["ts"]))
            if is_entry(entry):
                await self.handle_message(entry)

    async def _step_loop(self):
        while waitset := await self._step():
            await waitset.block(self.now())

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

    async def handle_message(self, e: Entry, preflight: bool = False) -> None:
        if not preflight and e["id"] in self._preflight_msg:
            self._preflight_msg.discard(e["id"])
            return
        if preflight:
            self._preflight_msg.add(e["id"])
        if e["type"] == "promise/complete":
            pending_info = self._pending_task.pop(e["promise_id"], None)
            task_info = self._tasks.get(e["promise_id"], None)

            id = _decode_id(e["promise_id"])

            return_type = None
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
                self._loop.post_completion(id, result=self._streams[e["id"]][0])
            self._pending_ops.discard(id)
        elif e["type"] == "stream/emit":
            id = _decode_id(e["id"])
            if e["stream_id"] not in self._streams:
                self._loop.post_completion(id, exception=ValueError("Stream not found"))
            else:
                _, ob, tv = self._streams[e["stream_id"]]
                self._loop.post_completion(id, result=None)
                if ob:
                    ob.on_next(
                        self._codec.decode_json(e["value"], tv),
                    )
            self._pending_ops.discard(id)
        elif e["type"] == "stream/complete":
            id = _decode_id(e["id"])
            if e["stream_id"] not in self._streams:
                self._loop.post_completion(id, exception=ValueError("Stream not found"))
            else:
                _, ob, _ = self._streams[e["stream_id"]]
                self._loop.post_completion(id, result=None)
                if ob:
                    if "error" in e:
                        ob.on_close(_decode_error(e["error"]))
                    else:
                        ob.on_close(None)

                _ = self._streams.pop(e["stream_id"], None)
            self._pending_ops.discard(id)
        else:
            assert e["type"] == "promise/create"

    async def enqueue_log(self, entry: Entry, flush: bool = False) -> None:
        if not self._running:
            self._pending_msg.append(entry)
        else:
            await self._log.append(self._running, entry)
            if flush:
                await self._log.flush(self._running)
            await self.handle_message(entry, preflight=True)

    async def enqueue_op(self, id: bytes, fut: OpFuture) -> None:
        op = cast("Op", fut.params)
        match op:
            case FnCall():
                await self.enqueue_log({
                    "ts": _encode_timestamp(self.now()),
                    "id": _encode_id(id),
                    "type": "promise/create",
                })

                async def cb() -> None:
                    entry: PromiseCompleteEntry = {
                        "ts": _encode_timestamp(self.now()),
                        "id": _encode_id(id, 1),
                        "type": "promise/complete",
                        "promise_id": _encode_id(id),
                    }
                    try:
                        result = op.callable(*op.args, **op.kwargs)
                        if isinstance(result, Awaitable):
                            result = await cast("Awaitable[object]", result)
                        entry["result"] = self._codec.encode_json(result)
                    except Exception as e:
                        entry["error"] = _encode_error(e)
                    except asyncio.CancelledError as e:
                        entry["error"] = _encode_error(e)

                    await self.enqueue_log(entry)

                def done(f: OpFuture) -> None:
                    if f.cancelled():
                        sid = _encode_id(f.id)
                        if self._pending_task.get(sid, None):
                            # pending task cancelled
                            pass
                        elif task_info := self._tasks.get(sid, None):
                            task, _ = task_info
                            _ = task.get_loop().call_soon(task.cancel)

                fut.add_done_callback(done)
                sid = _encode_id(id)
                if self._running:
                    self._tasks[sid] = (asyncio.create_task(cb()), op.return_type)
                else:
                    self._pending_task[sid] = (cb, op.return_type)

            case TaskRun():
                await self.enqueue_log({
                    "ts": _encode_timestamp(self.now()),
                    "id": _encode_id(id),
                    "type": "promise/create",
                })


                t = self._loop.create_task(op.task)
                async def task() -> None:
                    entry: PromiseCompleteEntry = {
                        "ts": _encode_timestamp(self.now()),
                        "id": _encode_id(id, 1),
                        "type": "promise/complete",
                        "promise_id": _encode_id(id),
                    }
                    try:
                        result = await wrap_future(t)
                        entry["result"] = self._codec.encode_json(result)
                        await self.enqueue_log(entry, True)
                    except Exception as e:
                        entry["error"] = _encode_error(e)
                        await self.enqueue_log(entry, True)
                    except asyncio.CancelledError as e:
                        entry["error"] = _encode_error(e)
                        await self.enqueue_log(entry, True)

                self._tasks[_encode_id(id)] = (
                    asyncio.create_task(task()),
                    op.return_type,
                )

            case StreamCreate():
                stream: RawStream[object] = RawStream(_encode_id(id), self._loop)
                if op.observer:
                    o = op.observer
                    ty = self._codec.inspect_function(o.on_next)
                    self._streams[_encode_id(id)] = (
                        stream,
                        o,
                        ty.parameter_types[ty.parameters[0]],
                    )
                else:
                    self._streams[_encode_id(id)] = (stream, None, None)
                await self.enqueue_log({
                    "ts": _encode_timestamp(self.now()),
                    "id": _encode_id(id),
                    "type": "stream/create",
                })

            case StreamEmit():
                await self.enqueue_log({
                    "ts": _encode_timestamp(self.now()),
                    "id": _encode_id(id),
                    "stream_id": op.stream_id,
                    "type": "stream/emit",
                    "value": self._codec.encode_json(op.value),
                })
            case StreamClose():
                if op.exception:
                    await self.enqueue_log({
                        "ts": _encode_timestamp(self.now()),
                        "id": _encode_id(id),
                        "stream_id": op.stream_id,
                        "type": "stream/complete",
                        "error": _encode_error(op.exception),
                    })
                else:
                    await self.enqueue_log({
                        "ts": _encode_timestamp(self.now()),
                        "id": _encode_id(id),
                        "stream_id": op.stream_id,
                        "type": "stream/complete",
                    })
            case _:
                assert_never(op)


def _encode_id(id: bytes, flag: int = 0) -> str:
    if flag != 0:
        id = blake2b(
            flag.to_bytes(4, "little", signed=True) + id, digest_size=12
        ).digest()
    return base64.b64encode(id).decode()


def _decode_id(encoded: str) -> bytes:
    return base64.b64decode(encoded)


def _encode_timestamp(ts_ns: int) -> int:
    return ts_ns // 1_000


def _decode_timestamp(ts: int) -> int:
    return ts * 1_000


def _encode_error(error: BaseException) -> ErrorInfo:
    return {
        "code": -1,
        "message": repr(error),
    }


def _decode_error(error_info: ErrorInfo) -> BaseException:
    return Exception(f"[{error_info['code']}] {error_info['message']}")
