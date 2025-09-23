from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, final

from duron.event_loop import create_loop
from duron.log.entry import is_entry
from duron.ops import FnCall

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from duron.event_loop import WaitSet
    from duron.log.entry import Entry, ErrorInfo, UnknownEntry
    from duron.log.storage import BaseLogStorage, Lease, Offset
    from duron.ops import Op

_T = TypeVar("_T")


class TaskRunner:
    def __init__(self):
        pass

    async def run(
        self, task_id: bytes, task_co: Coroutine[Any, Any, _T], log: BaseLogStorage
    ) -> _T:
        return await _Task[_T](task_id, task_co, log).run()


@final
class _Task(Generic[_T]):
    def __init__(
        self, id: bytes, task_co: Coroutine[Any, Any, _T], log: BaseLogStorage
    ) -> None:
        self._id = id
        self._loop = create_loop(id)
        self._task = self._loop.create_task(task_co)
        self._log = log
        self._running: Lease | None = None
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

    async def run(self) -> _T:
        offset: Offset | None = None
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
            if not offset:
                await self.enqueue_log(
                    {
                        "type": "promise/create",
                        "id": _encode_id(self._id, False),
                        "ts": _encode_timestamp(self.now()),
                        "meta": {"duron.root": "true"},
                    },
                )
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

            res = self._task.result()
            await self.enqueue_log(
                {
                    "type": "promise/complete",
                    "id": _encode_id(self._id, True),
                    "ts": _encode_timestamp(self.now()),
                    "promise_id": _encode_id(self._id, False),
                    "result": res,
                },
            )
            return res
        except BaseException as e:
            await self.enqueue_log(
                {
                    "type": "promise/complete",
                    "id": _encode_id(self._id, True),
                    "ts": _encode_timestamp(self.now()),
                    "promise_id": _encode_id(self._id, False),
                    "error": exception_to_error_info(e),
                },
            )
            raise
        finally:
            await self._log.release_lease(self._running)
            self._running = None

    async def _follow_log(
        self, g: AsyncGenerator[tuple[Offset, Entry | UnknownEntry], None]
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
            id, _ = _decode_id(e["id"])
            if "error" in e:
                self._loop.post_completion_threadsafe(
                    id,
                    exception=error_info_to_exception(e["error"]),
                )
                self._pending_ops.discard(id)
            elif "result" in e:
                self._loop.post_completion_threadsafe(
                    id,
                    result=e["result"],
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
                        "id": _encode_id(id, False),
                        "type": "promise/create",
                    }
                )

                async def cb() -> None:
                    try:
                        result = op.callable()
                        if isinstance(result, Coroutine):
                            result = await cast("Coroutine[Any, Any, object]", result)
                        await self.enqueue_log(
                            {
                                "ts": _encode_timestamp(self.now()),
                                "id": _encode_id(id, True),
                                "type": "promise/complete",
                                "promise_id": _encode_id(id, False),
                                "result": result,
                            }
                        )
                    except BaseException as e:
                        await self.enqueue_log(
                            {
                                "ts": _encode_timestamp(self.now()),
                                "id": _encode_id(id, True),
                                "type": "promise/complete",
                                "promise_id": _encode_id(id, False),
                                "error": exception_to_error_info(e),
                            }
                        )

                if self._running:
                    _ = asyncio.create_task(cb())
                else:
                    self._pending_task[_encode_id(id, False)] = cb

            case _:
                raise NotImplementedError(f"Unsupported op: {op!r}")


def _encode_id(id: bytes, end: bool) -> str:
    if end:
        return base64.b64encode(id).decode() + "-"
    else:
        return base64.b64encode(id).decode() + "+"


def _decode_id(s: str) -> tuple[bytes, bool]:
    if s.endswith("-"):
        return base64.b64decode(s[:-1]), True
    elif s.endswith("+"):
        return base64.b64decode(s[:-1]), False
    else:
        raise ValueError(f"Invalid encoded id: {s!r}")


def _encode_timestamp(ts_ns: int) -> int:
    return ts_ns // 1_000


def _decode_timestamp(ts: int) -> int:
    return ts * 1_000


def exception_to_error_info(e: BaseException) -> ErrorInfo:
    return {
        "code": 1,
        "message": str(e),
    }


def error_info_to_exception(e: ErrorInfo) -> Exception:
    return Exception(f"[{e['code']}] {e['message']}")
