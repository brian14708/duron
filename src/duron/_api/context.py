"""Typed authoring contexts supplied by the runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, overload
from typing_extensions import ParamSpec, Protocol, TypeVar

if TYPE_CHECKING:
    import random
    from collections.abc import Awaitable
    from datetime import datetime

    from duron._api.effect import Effect
    from duron._api.effect_run import EffectRun
    from duron._api.ports import (
        EffectEmitter,
        EffectReceiver,
        Emitter,
        Input,
        Output,
        Receiver,
    )

_T = TypeVar("_T")
_P = ParamSpec("_P")


class WorkflowContext(Protocol):
    """Durable operations available to workflow code.

    The runtime supplies this context. Its protocol is checked statically;
    applications do not construct it or depend on its implementation.
    """

    @overload
    def port(self, declaration: Input[_T], /) -> Receiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> Emitter[_T]: ...

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

    def start(
        self, effect: Effect[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> EffectRun[_T]: ...

    async def select(
        self,
        *,
        timeout: float | None = None,  # ruff: ignore[async-function-with-timeout]  # Durable deadline.
        **branches: Receiver[_T] | Awaitable[_T],
    ) -> Selection[_T | None]: ...

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


class EffectContext(Protocol):
    """Bound interaction ports supplied to an effect.

    Effects perform ordinary async I/O and return or raise. Runtime identities,
    channel binding, and completion authority are private to the runtime.
    """

    @overload
    def port(self, declaration: Input[_T], /) -> EffectReceiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> EffectEmitter[_T]: ...


_T_co = TypeVar("_T_co", covariant=True)


@dataclass(frozen=True, slots=True)
class Selection(Generic[_T_co]):
    """The name and result of the selected branch."""

    name: str
    value: _T_co
