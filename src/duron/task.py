from __future__ import annotations

import asyncio
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
    TypedDict,
    TypeVar,
    cast,
    final,
)

from duron.context import get_context
from duron.event_loop import create_loop
from duron.log.entry import is_entry
from duron.ops import FnCall, TaskRun

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import TracebackType

    from duron.event_loop import WaitSet
    from duron.log.codec import Codec
    from duron.log.entry import (
        Entry,
        ErrorInfo,
        JSONValue,
        PromiseCompleteEntry,
    )
    from duron.log.storage import LogStorage
    from duron.mark import DurableFn
    from duron.ops import Op

    _TOffset = TypeVar("_TOffset")
    _TLease = TypeVar("_TLease")

_T = TypeVar("_T")
_P = ParamSpec("_P")

_CURRENT_VERSION = 0


def task(
    task_co: DurableFn[_P, Coroutine[Any, Any, _T]],
    log: LogStorage[_TOffset, _TLease],
) -> TaskGuard[_P, _T]:
    return TaskGuard(Task(task_co, log))


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
    ) -> None: ...


@final
class Task(Generic[_P, _T]):
    def __init__(
        self,
        task_fn: DurableFn[_P, Coroutine[Any, Any, _T]],
        log: LogStorage[_TOffset, _TLease],
        codec: Codec | None = None,
    ) -> None:
        self._task_fn = task_fn
        self._log = log
        self._run: _TaskRun | None = None

    async def start(self, *args: _P.args, **kwargs: _P.kwargs) -> None:
        codec = self._task_fn.__durable__.codec
        init: TaskInitParams = {
            "version": _CURRENT_VERSION,
            "args": [codec.encode_json(arg) for arg in args],
            "kwargs": {k: codec.encode_json(v) for k, v in kwargs.items()},
        }
        self._run = _TaskRun(
            _task_prelude(self._task_fn, lambda: init),
            cast("LogStorage[object, object]", self._log),
            codec,
        )
        await self._run.resume()

    async def resume(self) -> None:
        def cb() -> TaskInitParams:
            raise Exception("not started")

        task = _task_prelude(self._task_fn, cb)
        self._run = _TaskRun(
            task,
            cast("LogStorage[object, object]", self._log),
            self._task_fn.__durable__.codec,
        )
        await self._run.resume()

    async def wait(self) -> _T:
        if self._run is None:
            raise RuntimeError("Task not started")
        return cast("_T", await self._run.run())


class TaskInitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]


async def _task_prelude(
    task_fn: DurableFn[..., Coroutine[Any, Any, object]],
    init: Callable[[], TaskInitParams],
) -> object:
    ctx = get_context()
    init_params = await ctx.run(init)
    if init_params["version"] != _CURRENT_VERSION:
        raise Exception("version mismatch")
    codec = task_fn.__durable__.codec
    args = (codec.decode_json(arg) for arg in init_params["args"])
    kwargs = {k: codec.decode_json(v) for k, v in init_params["kwargs"].items()}
    return await task_fn(*args, **kwargs)


@final
class _TaskRun:
    def __init__(
        self,
        task: Coroutine[Any, Any, object],
        log: LogStorage[object, object],
        codec: Codec,
    ) -> None:
        self._loop = create_loop(b"")
        self._task = self._loop.create_op(TaskRun(task=task))
        self._log = log
        self._codec = codec
        self._running: object | None = None
        self._pending_msg: list[Entry] = []
        self._pending_task: dict[str, Callable[[], Coroutine[Any, Any, object]]] = {}
        self._pending_ops: set[bytes] = set()
        self._now = 0
        self._offset: object | None = None

    def now(self) -> int:
        if self._running:
            t = time.time_ns()
            t -= t % 1_000
            self._now = max(self._now, t)
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
        try:
            for msg in self._pending_msg:
                await self.enqueue_log(msg)
            self._pending_msg.clear()
            for task in self._pending_task.values():
                _ = asyncio.create_task(task())
            self._pending_task.clear()

            t1 = asyncio.create_task(self._follow_log())
            t2 = asyncio.create_task(self._step_loop())

            done, pending = await asyncio.wait(
                [t1, t2], return_when=asyncio.FIRST_COMPLETED
            )

            for t in pending:
                _ = t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t

            for t in done:
                if exc := t.exception():
                    raise exc

            return self._task.result()
        finally:
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
                    await self.enqueue_op(sid, s.params)

    async def handle_message(self, e: Entry) -> None:
        if e["type"] == "promise/complete":
            _ = self._pending_task.pop(e["promise_id"], None)
            id = _decode_id(e["promise_id"])
            if "error" in e:
                self._loop.post_completion_threadsafe(
                    id,
                    exception=_decode_error(e["error"], self._codec),
                )
                self._pending_ops.discard(id)
            elif "result" in e:
                self._loop.post_completion_threadsafe(
                    id,
                    result=self._codec.decode_json(e["result"]),
                )
                self._pending_ops.discard(id)
            else:
                raise ValueError(f"Invalid promise/complete entry: {e!r}")
        else:
            pass

    async def enqueue_log(self, entry: Entry) -> None:
        if not self._running:
            self._pending_msg.append(entry)
        else:
            _ = await self._log.append(self._running, entry)
            await self.handle_message(entry)

    async def enqueue_op(self, id: bytes, op: Op | object) -> None:
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
                        result = op.callable()
                        if isinstance(result, Awaitable):
                            result = await cast("Awaitable[object]", result)
                        entry["result"] = self._codec.encode_json(result)
                    except BaseException as e:
                        entry["error"] = _encode_error(e, self._codec)
                    await self.enqueue_log(entry)

                if self._running:
                    _ = asyncio.create_task(cb())
                else:
                    self._pending_task[_encode_id(id)] = cb

            case TaskRun():
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
                        result = await op.task
                        entry["result"] = self._codec.encode_json(result)
                    except BaseException as e:
                        entry["error"] = _encode_error(e, self._codec)

                    await self.enqueue_log(entry)

                _ = self._loop.create_task(cb())

            case _:
                raise NotImplementedError(f"Unsupported op: {op!r}")


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


def _encode_error(error: BaseException, codec: Codec) -> ErrorInfo:
    """Convert exception to ErrorInfo dict."""
    return {
        "code": -1,
        "message": str(error),
        "state": codec.encode_state(error),
    }


def _decode_error(error_info: ErrorInfo, codec: Codec) -> BaseException:
    """Convert ErrorInfo dict to exception."""
    try:
        if "state" not in error_info:
            return Exception(f"[{error_info['code']}] {error_info['message']}")
        return cast("BaseException", codec.decode_state(error_info["state"]))
    except Exception:
        return Exception(f"[{error_info['code']}] {error_info['message']}")
