from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import TYPE_CHECKING, Final, Generic, cast
from typing_extensions import Any, TypeVar, final, override

from duron._core.ops import Barrier, StreamClose, StreamCreate, StreamEmit, create_op
from duron._loop import wrap_future

if TYPE_CHECKING:
    from types import TracebackType

    from duron._core.ops import OpAnnotations
    from duron._core.stream import StreamWriter
    from duron._loop import EventLoop
    from duron.typing._hint import TypeHint

_InT = TypeVar("_InT", contravariant=True)  # noqa: PLC0105


class SignalInterrupt(Exception):  # noqa: N818
    """Exception raised when a signal interrupts an in-progress operation.

    Attributes:
        value: The value passed to the signal trigger that caused the interrupt.
    """

    def __init__(self, value: object) -> None:
        super().__init__()
        self.value = value

    @override
    def __repr__(self) -> str:
        return f"SignalInterrupt(value={self.value!r})"


@final
class SignalWriter(Generic[_InT]):
    """Object for writing values to a signal to interrupt operations."""

    __slots__ = ("_loop", "_stream_id")

    def __init__(self, stream_id: str, loop: EventLoop) -> None:
        self._stream_id = stream_id
        self._loop = loop

    async def send(self, value: _InT) -> None:
        """Trigger the signal with a value, interrupting active operations.

        Args:
            value: The value to send with the interrupt.
        """
        await wrap_future(
            create_op(self._loop, StreamEmit(stream_id=self._stream_id, value=value))
        )

    async def close(self, exc: Exception | None = None) -> None:
        """Close the signal stream, preventing further triggers."""
        await wrap_future(
            create_op(self._loop, StreamClose(stream_id=self._stream_id, exception=exc))
        )


_SIGNAL_TRIGGER: Final = object()


@final
class Signal(Generic[_InT]):
    """Signal context manager for interruptible operations.

    Signal provides a mechanism for interrupting in-progress operations. When used
    as an async context manager, it monitors for trigger events. If a signal is
    triggered while code is executing within the context, a SignalInterrupt exception
    is raised with the trigger value.

    Example:
        ```python
        async with signal:
            # This code can be interrupted if signal.trigger() is called
            await long_running_operation()
        ```
    """

    def __init__(self, loop: EventLoop) -> None:
        self._loop = loop
        # task -> [offset, stack depth]
        self._tasks: dict[asyncio.Task[Any], tuple[int, int]] = {}
        self._trigger: deque[tuple[int, _InT]] = deque()

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:
            return
        assert task.get_loop() == self._loop  # noqa: S101
        offset = await create_op(self._loop, Barrier())
        for toffset, value in self._trigger:
            if toffset > offset:
                raise SignalInterrupt(value=value)
        _, depth = self._tasks.get(task, (0, -1))
        self._tasks[task] = (offset, depth + 1)
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
        offset_end = await create_op(self._loop, Barrier())

        offset_start, depth = self._tasks.pop(task)
        if depth > 0:
            self._tasks[task] = (offset_end, depth - 1)
        for toffset, value in self._trigger:
            if (
                offset_start < toffset < offset_end
                and exc_type is asyncio.CancelledError
                and (args := cast("asyncio.CancelledError", exc_value).args)
                and args[0] is _SIGNAL_TRIGGER
            ):
                if sys.version_info >= (3, 11):
                    _ = task.uncancel()
                self._flush()
                raise SignalInterrupt(value=value)

    def on_next(self, offset: int, value: _InT) -> None:
        self._trigger.append((offset, value))
        for t, (toffset, _depth) in self._tasks.items():
            if toffset < offset:
                _ = self._loop.call_soon(t.cancel, _SIGNAL_TRIGGER)

    def on_close(self, _offset: int, _exc: Exception | None) -> None:
        pass

    def _flush(self) -> None:
        if not self._tasks:
            self._trigger.clear()
            return
        min_offset = min((offset for offset, _ in self._tasks.values()))
        while self._trigger and self._trigger[0][0] < min_offset:
            _ = self._trigger.popleft()


async def create_signal(
    loop: EventLoop, dtype: TypeHint[_InT], annotations: OpAnnotations
) -> tuple[Signal[_InT], StreamWriter[_InT]]:
    s: Signal[_InT] = Signal(loop)
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype, observer=cast("Signal[object]", s), annotations=annotations
        ),
    )
    return (s, SignalWriter(sid, loop))
