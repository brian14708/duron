from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Literal, cast
from typing_extensions import Any, TypeVar, final, override

from duron._core.ops import StreamCreate, create_op
from duron._core.stream import OpWriter

if TYPE_CHECKING:
    from types import TracebackType

    from duron._core.ops import OpMetadata
    from duron._core.stream import StreamWriter
    from duron.loop import EventLoop
    from duron.typing._hint import TypeHint

_T = TypeVar("_T")


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


@dataclass(slots=True)
class _SignalState:
    depth: int
    triggered: Literal[False] | SignalInterrupt


@final
class Signal(Generic[_T]):
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
        self._tasks: dict[asyncio.Task[Any], _SignalState] = {}

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:
            return
        assert task.get_loop() == self._loop
        if task not in self._tasks:
            val = _SignalState(depth=0, triggered=False)
            self._tasks[task] = val
        else:
            val = self._tasks[task]
            val.depth += 1

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        task = asyncio.current_task()
        if task is None:
            return
        state = self._tasks.pop(task)
        _ = self._loop.generate_op_scope()
        triggered = state.triggered
        if state.depth > 0:
            state.triggered = False
            state.depth -= 1
            self._tasks[task] = state
        if triggered is not False:
            if sys.version_info >= (3, 11):
                _ = task.uncancel()
            raise triggered from None

    def on_next(self, _offset: int, value: _T) -> None:
        for t, state in self._tasks.items():
            if state.triggered is False:
                state.triggered = SignalInterrupt(value=value)
                _ = self._loop.call_soon(t.cancel, state.triggered)

    def on_close(self, _offset: int, _exc: Exception | None) -> None:
        pass


async def create_signal(
    loop: EventLoop, dtype: TypeHint[_T], name: str | None, metadata: OpMetadata
) -> tuple[Signal[_T], StreamWriter[_T]]:
    s: Signal[_T] = Signal(loop)
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            name=name,
            observer=cast("Signal[object]", s),
            metadata=metadata,
        ),
    )
    w: OpWriter[_T] = OpWriter(sid, loop)
    return (s, w)
