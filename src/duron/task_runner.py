from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import Awaitable
from hashlib import blake2b
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
    final,
)

from duron.event_loop import create_loop
from duron.log.codec import DEFAULT_CODEC
from duron.log.entry import is_entry
from duron.ops import FnCall, TaskRun

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine

    from duron.event_loop import WaitSet
    from duron.log.codec import Codec
    from duron.log.entry import AnyEntry, Entry, ErrorInfo, PromiseCompleteEntry
    from duron.log.storage import LogStorage
    from duron.mark import DurableFn
    from duron.ops import Op

    _TOffset = TypeVar("_TOffset")
    _TLease = TypeVar("_TLease")
    _T = TypeVar("_T")


class TaskRunner:
    def __init__(self, codec: Codec | None = None) -> None:
        self._codec: Codec = codec or DEFAULT_CODEC

    async def run(
        self,
        task_id: bytes,
        task_co: DurableFn[[], Coroutine[Any, Any, _T]],
        log: LogStorage[_TOffset, _TLease],
    ) -> _T:
        return cast(
            "_T",
            await _Task(
                task_id, task_co(), cast("LogStorage[object, object]", log), self._codec
            ).run(),
        )


@final
class _Task:
    def __init__(
        self,
        id: bytes,
        task_co: Coroutine[Any, Any, object],
        log: LogStorage[object, object],
        codec: Codec,
    ) -> None:
        self._loop = create_loop(id)
        self._task = self._loop.create_op(TaskRun(task=task_co))
        self._log = log
        self._codec = codec
        self._running: object | None = None
        self._pending_msg: list[Entry] = []
        self._pending_task: dict[str, Callable[[], Coroutine[Any, Any, object]]] = {}
        self._pending_ops: set[bytes] = set()
        self._now = 0

    def now(self) -> int:
        if self._running:
            t = time.time_ns()
            t -= t % 1_000
            self._now = max(self._now, t)
        return self._now

    async def run(self) -> object:
        offset: object | None = None
        recvd_msgs: set[str] = set()
        async for o, entry in self._log.stream(None, False):
            offset = o
            ts = _decode_timestamp(entry["ts"])
            self._now = max(self._now, ts)
            _ = await self._step()
            if is_entry(entry):
                await self.handle_message(entry)
                _ = await self._step()
            recvd_msgs.add(entry["id"])

        if self._task.done():
            return self._task.result()

        self._running = await self._log.acquire_lease()
        try:
            for msg in self._pending_msg:
                if msg["id"] not in recvd_msgs:
                    await self.enqueue_log(msg)
            self._pending_msg.clear()
            for task in self._pending_task.values():
                _ = asyncio.create_task(task())
            self._pending_task.clear()

            _ = asyncio.create_task(self._follow_log(self._log.stream(offset, True)))
            while (waitset := await self._step()) is not None:
                await waitset.wait(self.now())

            return self._task.result()
        finally:
            await self._log.release_lease(self._running)
            self._running = None

    async def _follow_log(
        self, g: AsyncGenerator[tuple[object, Entry | AnyEntry], None]
    ) -> None:
        async for _, entry in g:
            if is_entry(entry):
                self._now = max(self._now, _decode_timestamp(entry["ts"]))
                await self.handle_message(entry)

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
                await self.enqueue_log(
                    {
                        "ts": _encode_timestamp(self.now()),
                        "id": _encode_id(id),
                        "type": "promise/create",
                    }
                )

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
                await self.enqueue_log(
                    {
                        "ts": _encode_timestamp(self.now()),
                        "id": _encode_id(id),
                        "type": "promise/create",
                    }
                )

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
