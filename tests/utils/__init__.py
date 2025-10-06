from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ParamSpec,
    TypeVar,
    overload,
)

from duron._core.config import config as config
from duron._core.job import Job as Job

if TYPE_CHECKING:
    from collections.abc import Callable

    from duron._core.context import Context as Context


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@overload
def external(_fn: Callable[_P, _T_co]) -> Callable[_P, _T_co]: ...
@overload
def external() -> Callable[[Callable[_P, _T_co]], Callable[_P, _T_co]]: ...
def external(
    _fn: Callable[_P, _T_co] | None = None,
) -> Callable[_P, _T_co] | Callable[[Callable[_P, _T_co]], Callable[_P, _T_co]]:
    def decorate(fn: Callable[_P, _T_co]) -> Callable[_P, _T_co]:
        return fn

    if _fn is not None:
        return decorate(_fn)
    else:
        return decorate
