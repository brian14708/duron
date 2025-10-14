from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Concatenate,
    Generic,
    cast,
    get_args,
    get_origin,
)
from typing_extensions import (
    Any,
    AsyncContextManager,
    ParamSpec,
    TypeVar,
    overload,
)

from duron._core.config import config
from duron._core.invoke import Invoke
from duron._core.signal import Signal
from duron._core.stream import Stream, StreamWriter
from duron.typing._inspect import inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._core.context import Context
    from duron.codec import Codec
    from duron.log import LogStorage
    from duron.tracing import Tracer
    from duron.typing import TypeHint


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass(slots=True)
class DurableFn(Generic[_P, _T_co]):
    codec: Codec
    fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]
    inject: list[tuple[str, type, TypeHint[Any]]]

    def __call__(
        self,
        ctx: Context,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(ctx, *args, **kwargs)

    def invoke(
        self, log: LogStorage, /, *, tracer: Tracer | None = None
    ) -> AsyncContextManager[Invoke[_P, _T_co]]:
        return Invoke[_P, _T_co].invoke(self, log, tracer)


@overload
def durable(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    /,
) -> DurableFn[_P, _T_co]: ...
@overload
def durable(
    *,
    codec: Codec | None = None,
) -> Callable[
    [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
    DurableFn[_P, _T_co],
]: ...
def durable(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]] | None = None,
    /,
    *,
    codec: Codec | None = None,
) -> (
    DurableFn[_P, _T_co]
    | Callable[
        [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
        DurableFn[_P, _T_co],
    ]
):
    def decorate(
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    ) -> DurableFn[_P, _T_co]:
        info = inspect_function(fn)
        inject: list[tuple[str, type, TypeHint[Any]]] = []
        for name, param in info.parameter_types.items():
            ty = _special_type(param)
            if ty:
                inject.append((name, *ty))
        inject.sort()
        return DurableFn(codec=codec or config.codec, fn=fn, inject=inject)

    if f is not None:
        return decorate(f)
    return decorate


def _special_type(
    tp: TypeHint[Any],
) -> tuple[type, TypeHint[Any]] | None:
    origin = get_origin(tp)
    if origin is Stream:
        return (Stream, cast("TypeHint[Any]", get_args(tp)[0]))
    if origin is Signal:
        return (Signal, cast("TypeHint[Any]", get_args(tp)[0]))
    if origin is StreamWriter:
        return (StreamWriter, cast("TypeHint[Any]", get_args(tp)[0]))
    return None
