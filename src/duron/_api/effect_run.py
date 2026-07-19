"""Scoped effect invocation contract exposed to workflow authors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload
from typing_extensions import Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Generator
    from types import TracebackType

    from duron._api.ports import Input, Output, Reader, Sender

_T_co = TypeVar("_T_co", covariant=True)
_V = TypeVar("_V")


class EffectRun(Protocol[_T_co]):
    """A scoped logical effect call; awaiting it observes its final result."""

    @overload
    def port(self, declaration: Input[_V], /) -> Sender[_V]: ...
    @overload
    def port(self, declaration: Output[_V], /) -> Reader[_V]: ...
    async def __aenter__(self) -> EffectRun[_T_co]: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def __await__(self) -> Generator[Any, None, _T_co]: ...
