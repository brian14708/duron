from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Literal,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

if TYPE_CHECKING:
    from collections.abc import Callable


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


class DurableFn(Protocol[_P, _T_co]):
    __durable__: Literal[True]

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T_co: ...


def durable() -> Callable[[Callable[_P, _T_co]], DurableFn[_P, _T_co]]:
    """
    Mark a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(fn: Callable[_P, _T_co]) -> DurableFn[_P, _T_co]:
        d = cast("DurableFn[_P, _T_co]", fn)
        d.__durable__ = True
        return d

    return decorate
