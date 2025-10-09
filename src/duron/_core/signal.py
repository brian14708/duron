from __future__ import annotations

import asyncio
import sys
from asyncio.exceptions import CancelledError
from collections import deque
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from typing_extensions import final

from duron._core.ops import Barrier, StreamClose, StreamCreate, StreamEmit, create_op
from duron._loop import wrap_future

if TYPE_CHECKING:
    from types import TracebackType

    from duron._loop import EventLoop
    from duron.codec import JSONValue

_In_contra = TypeVar("_In_contra", contravariant=True)


class SignalInterrupt(Exception):  # noqa: N818
    def __init__(self, *args: object, value: object) -> None:
        super().__init__(*args)
        self.value: object = value


@final
class SignalWriter(Generic[_In_contra]):
    __slots__ = ("_loop", "_stream_id")

    def __init__(self, stream_id: str, loop: EventLoop) -> None:
        self._stream_id = stream_id
        self._loop = loop

    async def trigger(self, value: _In_contra, /) -> None:
        await wrap_future(
            create_op(
                self._loop,
                StreamEmit(stream_id=self._stream_id, value=value),
            ),
        )

    async def close(self, /) -> None:
        await wrap_future(
            create_op(
                self._loop,
                StreamClose(stream_id=self._stream_id, exception=None),
            ),
        )


_SENTINAL = object()


@final
class Signal(Generic[_In_contra]):
    def __init__(self, loop: EventLoop) -> None:
        self._loop = loop
        # task -> [offset, refcnt]
        self._tasks: dict[asyncio.Task[Any], tuple[int, int]] = {}
        self._trigger: deque[tuple[int, _In_contra]] = deque()

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:
            return
        assert task.get_loop() == self._loop  # noqa: S101
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
                    assert exc_value  # noqa: S101
                    assert exc_value.args[0] is _SENTINAL  # noqa: S101
                    _ = task.uncancel()
                raise SignalInterrupt(value=value)

    def on_next(self, offset: int, value: _In_contra) -> None:
        self._trigger.append((offset, value))
        for t, (toffset, _refcnt) in self._tasks.items():
            if toffset < offset:
                _ = self._loop.call_soon(t.cancel, _SENTINAL)

    def on_close(self, _offset: int, _exc: BaseException | None) -> None:
        pass

    def _flush(self) -> None:
        assert len(self._tasks) > 0  # noqa: S101
        min_offset = min(offset for offset, _ in self._tasks.values())
        while self._trigger and self._trigger[0][0] < min_offset:
            _ = self._trigger.popleft()


async def create_signal(
    loop: EventLoop,
    dtype: type[_In_contra],
    *,
    metadata: dict[str, JSONValue] | None = None,
) -> tuple[Signal[_In_contra], SignalWriter[_In_contra]]:
    assert asyncio.get_running_loop() is loop  # noqa: S101
    s: Signal[_In_contra] = Signal(loop)
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            observer=s,
            metadata=metadata,
        ),
    )
    return (s, SignalWriter(sid, loop))
