from __future__ import annotations

import asyncio
import sys
from asyncio.exceptions import CancelledError
from collections import deque
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import Any, Protocol, TypeVar, final

from duron._core.ops import (
    Barrier,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron._loop import wrap_future

if TYPE_CHECKING:
    from types import TracebackType

    from duron._core.ops import (
        OpAnnotations,
    )
    from duron._loop import EventLoop
    from duron.typing._hint import TypeHint

_In = TypeVar("_In", contravariant=True)  # noqa: PLC0105


class SignalInterrupt(Exception):  # noqa: N818
    """Exception raised when a signal interrupts an in-progress operation.

    Attributes:
        value: The value passed to the signal trigger that caused the interrupt.
    """

    def __init__(self, *args: object, value: object) -> None:
        super().__init__(*args)
        self.value: object = value


class SignalWriter(Protocol, Generic[_In]):
    """Protocol for writing values to a signal to interrupt operations."""

    async def trigger(self, value: _In, /) -> None:
        """Trigger the signal with a value, interrupting active operations.

        Args:
            value: The value to send with the interrupt.
        """
        ...

    async def close(self, /) -> None:
        """Close the signal stream, preventing further triggers."""
        ...


@final
class _Writer(Generic[_In]):
    __slots__ = ("_loop", "_stream_id")

    def __init__(self, stream_id: str, loop: EventLoop) -> None:
        self._stream_id = stream_id
        self._loop = loop

    async def trigger(self, value: _In, /) -> None:
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
class Signal(Generic[_In]):
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
        # task -> [offset, refcnt]
        self._tasks: dict[asyncio.Task[Any], tuple[int, int]] = {}
        self._trigger: deque[tuple[int, _In]] = deque()

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

    def on_next(self, offset: int, value: _In) -> None:
        self._trigger.append((offset, value))
        for t, (toffset, _refcnt) in self._tasks.items():
            if toffset < offset:
                _ = self._loop.call_soon(t.cancel, _SENTINAL)

    def on_close(self, _offset: int, _exc: Exception | None) -> None:
        pass

    def _flush(self) -> None:
        assert len(self._tasks) > 0  # noqa: S101
        min_offset = min(offset for offset, _ in self._tasks.values())
        while self._trigger and self._trigger[0][0] < min_offset:
            _ = self._trigger.popleft()


async def create_signal(
    loop: EventLoop,
    dtype: TypeHint[_In],
    annotations: OpAnnotations,
) -> tuple[Signal[_In], SignalWriter[_In]]:
    assert asyncio.get_running_loop() is loop  # noqa: S101
    s: Signal[_In] = Signal(loop)
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            observer=cast("Signal[object]", s),
            annotations=annotations,
        ),
    )
    return (s, _Writer(sid, loop))
