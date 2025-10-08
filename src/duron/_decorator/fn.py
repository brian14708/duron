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

from duron._core.config import config
from duron._core.invoke import Invoke

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from contextlib import AbstractAsyncContextManager

    from duron._core.context import Context
    from duron.codec import Codec
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

    def invoke(self, log: LogStorage) -> AbstractAsyncContextManager[Invoke[_P, _T_co]]:
        return Invoke[_P, _T_co].invoke(self, log)


@overload
def fn(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    /,
) -> Fn[_P, _T_co]: ...
@overload
def fn(
    *, codec: Codec | None = None
) -> Callable[
    [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
    Fn[_P, _T_co],
]: ...
def fn(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]] | None = None,
    /,
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
    Make a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    ) -> Fn[_P, _T_co]:
        return Fn(codec=codec or config.codec, fn=fn)

    if f is not None:
        return decorate(f)
    else:
        return decorate
