"""Typed authoring contexts supplied by the runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import ParamSpec, Protocol, TypeVar

if TYPE_CHECKING:
    import random
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime

    from duron._api.ports import Input, Interrupt, Output, Request
    from duron._runtime.execution.effect import Effect
    from duron._runtime.workflow.owned_signal import DurableCommands
    from duron._runtime.workflow.owned_stream import DurableStream
    from duron.types import TypeHint

_T = TypeVar("_T")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")
_P = ParamSpec("_P")


class WorkflowContext(Protocol):
    """Durable operations available to workflow code.

    The runtime supplies this context. Its protocol is checked statically;
    applications do not construct it or depend on its implementation.
    """

    async def call(
        self, effect: Effect[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        """Invoke an effect and durably record its result.

        Returns:
            The effect's decoded result.

        Raises:
            WorkflowDefinitionError: If ``effect`` is not a decorated effect.

        """
        ...

    async def receive(self, port: Input[_T], /) -> _T:
        """Consume the earliest unconsumed value from an input port (FIFO).

        Waiting on a closed, drained port raises `duron.errors.PortClosedError`,
        since the wait could never complete.

        Returns:
            The next value delivered to the port.

        """
        ...

    async def emit(self, port: Output[_T], value: _T, /) -> None:
        """Append a value to a durable output port. Returns after it is durable."""
        ...

    async def request(self, port: Request[_Req, _Res], value: _Req, /) -> _Res:
        """Issue a durable request and await its single reply.

        Returns:
            The response supplied by host code.

        """
        ...

    def interruptible(
        self, signal: Interrupt[_T], /
    ) -> AbstractAsyncContextManager[None]:
        """Arm a signal for the duration of the block.

        If the signal fires while an operation inside the block is awaiting, the
        block exits by raising `duron.errors.Interrupted` carrying the payload.

        Returns:
            An async context manager that arms the signal.

        """
        ...

    async def now(self) -> datetime:
        """Return a deterministic timezone-aware UTC timestamp.

        Returns:
            The current deterministic UTC time.

        """
        ...

    async def sleep(self, delay: float) -> None:
        """Sleep until a durable deadline (not ``asyncio.sleep``).

        The deadline is recorded as ``now + delay`` when the sleep starts, so a
        run resumed after a crash mid-sleep waits only for the remainder of the
        original deadline instead of restarting the full delay.
        """
        ...

    def random(self) -> random.Random:
        """Return a deterministic, replay-stable random number generator.

        Returns:
            A seeded `random.Random` instance.

        """
        ...

    async def receive_many(
        self, port: Input[_T], /, *, max_items: int | None = None, wait: bool = True
    ) -> list[_T]:
        """Consume a contiguous run of buffered values from an input port.

        With ``wait=False``, ``[]`` means the port is open but currently empty.
        A closed, drained port raises `duron.errors.PortClosedError`
        in either mode.

        Returns:
            The consumed values, in FIFO order.

        Raises:
            ValueError: if ``max_items`` is not a positive integer.

        """
        ...

    def open_stream(
        self, item_type: TypeHint[_T], *, name: str | None = None, prefetch: int = 64
    ) -> AbstractAsyncContextManager[DurableStream[_T]]:
        """Open a scoped effect-to-workflow stream with independent readers."""
        ...

    def open_commands(
        self, item_type: TypeHint[_T], *, name: str | None = None
    ) -> AbstractAsyncContextManager[DurableCommands[_T]]:
        """Open a scoped workflow-to-effect command channel with acknowledgement."""
        ...


class EffectContext(Protocol):
    """Read-only idempotency and recovery information supplied to an effect.

    Effects perform ordinary async I/O and return or raise. Runtime identities,
    channel binding, and completion authority are private to the runtime.
    """

    @property
    def idempotency_key(self) -> str:
        """Stable key for this logical effect call across retries and recovery."""
        ...

    @property
    def logical_attempt(self) -> int:
        """One-based logical attempt number, including policy-driven retries."""
        ...

    @property
    def invocation_number(self) -> int:
        """One-based physical invocation number, including crash recovery."""
        ...

    @property
    def recovery_count(self) -> int:
        """Number of crash-recovery invocations for this logical effect call."""
        ...

    @property
    def recovering(self) -> bool:
        """Whether this invocation recovers an unreported prior outcome."""
        ...
