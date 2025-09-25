from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Generic,
    ParamSpec,
    TypeVar,
)

from duron.codec import DefaultCodec

if TYPE_CHECKING:
    from collections.abc import Callable

    from duron.codec import Codec


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass(slots=True)
class DurableFn(Generic[_P, _T_co]):
    codec: Codec
    fn: Callable[_P, _T_co]

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T_co:
        return self.fn(*args, **kwargs)


def durable(
    *, codec: Codec | None = None
) -> Callable[[Callable[_P, _T_co]], DurableFn[_P, _T_co]]:
    """
    Mark a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(fn: Callable[_P, _T_co]) -> DurableFn[_P, _T_co]:
        return DurableFn(codec=codec or DefaultCodec(), fn=fn)

    return decorate
