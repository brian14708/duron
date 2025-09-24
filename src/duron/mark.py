from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from duron.log.codec import DEFAULT_CODEC

if TYPE_CHECKING:
    from collections.abc import Callable

    from duron.log.codec import Codec


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass
class DurableFnParams:
    codec: Codec


class DurableFn(Protocol[_P, _T_co]):
    __durable__: DurableFnParams

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T_co: ...


def durable() -> Callable[[Callable[_P, _T_co]], DurableFn[_P, _T_co]]:
    """
    Mark a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(fn: Callable[_P, _T_co]) -> DurableFn[_P, _T_co]:
        d = cast("DurableFn[_P, _T_co]", fn)
        d.__durable__ = DurableFnParams(codec=DEFAULT_CODEC)
        return d

    return decorate
