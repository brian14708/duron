from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
    overload,
)

from duron.config import config
from duron.task import Task, TaskGuard

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron.codec import Codec
    from duron.context import Context
    from duron.log import LogStorage


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass(slots=True)
class Fn(Generic[_P, _T_co]):
    codec: Codec
    fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]

    def __call__(
        self, ctx: Context, *args: _P.args, **kwargs: _P.kwargs
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(ctx, *args, **kwargs)

    def create_task(self, log: LogStorage) -> TaskGuard[_P, _T_co]:
        return TaskGuard(Task(self, log))


@overload
def fn(
    _fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
) -> Fn[_P, _T_co]: ...
@overload
def fn(
    *, codec: Codec | None = None
) -> Callable[
    [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
    Fn[_P, _T_co],
]: ...
def fn(
    _fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]] | None = None,
    *,
    codec: Codec | None = None,
) -> (
    Fn[_P, _T_co]
    | Callable[
        [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
        Fn[_P, _T_co],
    ]
):
    """
    Mark a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    ) -> Fn[_P, _T_co]:
        return Fn(codec=codec or config.codec, fn=fn)

    if _fn is not None:
        return decorate(_fn)
    else:
        return decorate


@overload
def effect(_fn: Callable[_P, _T_co]) -> Callable[_P, _T_co]: ...
@overload
def effect() -> Callable[[Callable[_P, _T_co]], Callable[_P, _T_co]]: ...
def effect(
    _fn: Callable[_P, _T_co] | None = None,
) -> Callable[_P, _T_co] | Callable[[Callable[_P, _T_co]], Callable[_P, _T_co]]:
    def decorate(fn: Callable[_P, _T_co]) -> Callable[_P, _T_co]:
        return fn

    if _fn is not None:
        return decorate(_fn)
    else:
        return decorate
