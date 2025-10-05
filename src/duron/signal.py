from __future__ import annotations

import asyncio
import sys
from asyncio.exceptions import CancelledError
from collections import deque
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import final

from duron.ops import Barrier, StreamClose, StreamCreate, StreamEmit, create_op

if TYPE_CHECKING:
    from types import TracebackType

    from duron.codec import JSONValue
    from duron.event_loop import EventLoop

_In = TypeVar("_In", contravariant=True)


class SignalInterrupt(Exception):
    def __init__(self, *args: object, value: Any) -> None:
        super().__init__(*args)
        self.value: Any = value


@final
class SignalWriter(Generic[_In]):
    __slots__ = ("_stream_id", "_loop")

    def __init__(self, id: str, loop: EventLoop) -> None:
        self._stream_id = id
        self._loop = loop

    async def trigger(self, value: _In, /) -> None:
        await create_op(
            self._loop,
            StreamEmit(stream_id=self._stream_id, value=value),
        )

    async def close(self, /) -> None:
        await create_op(
            self._loop,
            StreamClose(stream_id=self._stream_id, exception=None),
        )


_SENTINAL = object()


@final
class Signal(Generic[_In]):
    def __init__(self, loop: EventLoop) -> None:
        self._loop = loop
        # task -> [offset, refcnt]
        self._tasks: dict[asyncio.Task[Any], tuple[int, int]] = {}
        self._trigger: deque[tuple[int, _In]] = deque()

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:
            return
        assert task.get_loop() == self._loop
        offset = await create_op(self._loop, Barrier())
        for toffset, value in self._trigger:
            if toffset > offset:
                raise SignalInterrupt(value=value)
        _, refcnt = self._tasks.get(task, (0, 0))
        self._tasks[task] = (offset, refcnt + 1)
        self._flush()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        task = asyncio.current_task()
        if task is None:
            return
        offset_start, refcnt = self._tasks.pop(task)
        offset_end = await create_op(self._loop, Barrier())
        if refcnt > 1:
            self._tasks[task] = (offset_end, refcnt - 1)
        for toffset, value in self._trigger:
            if offset_start < toffset < offset_end:
                if sys.version_info >= (3, 11) and exc_type is CancelledError:
                    assert exc_value and exc_value.args[0] is _SENTINAL
                    _ = task.uncancel()
                raise SignalInterrupt(value=value)

    def on_next(self, offset: int, value: _In) -> None:
        self._trigger.append((offset, value))
        for t, (toffset, _refcnt) in self._tasks.items():
            if toffset < offset:
                _ = self._loop.call_soon(t.cancel, _SENTINAL)

    def on_close(self, _offset: int, _exc: BaseException | None) -> None:
        pass

    def _flush(self) -> None:
        assert len(self._tasks) > 0
        min_offset = min(offset for offset, _ in self._tasks.values())
        while self._trigger and self._trigger[0][0] < min_offset:
            _ = self._trigger.popleft()


async def create_signal(
    loop: EventLoop,
    dtype: type[_In],
    *,
    metadata: dict[str, JSONValue] | None = None,
) -> tuple[Signal[_In], SignalWriter[_In]]:
    assert asyncio.get_running_loop() is loop
    s: Signal[_In] = Signal(loop)
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            observer=s,
            metadata=metadata,
        ),
    )
    return (s, SignalWriter(sid, loop))
