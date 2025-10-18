"""Durable function decorator for replayable async workflows.

This module provides the `@durable` decorator which marks async functions
as orchestration functions. Durable functions:
- Must take `Context` as their first parameter
- Can be paused, resumed, and replayed deterministically
- Support automatic injection of Stream, Signal, and StreamWriter parameters
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Concatenate, Generic, cast, get_args, get_origin
from typing_extensions import (
    Any,
    AsyncContextManager,
    ParamSpec,
    TypeVar,
    final,
    overload,
)

from duron._core.config import config
from duron._core.invoke import DurableRun
from duron._core.signal import Signal
from duron._core.stream import Stream, StreamWriter
from duron.typing._inspect import inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterable

    from duron._core.context import Context
    from duron.codec import Codec
    from duron.log import LogStorage
    from duron.tracing._tracer import Tracer
    from duron.typing import TypeHint


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@final
class DurableFn(Generic[_P, _T_co]):
    def __init__(
        self,
        codec: Codec,
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
        inject: Iterable[tuple[str, type, TypeHint[Any]]],
    ) -> None:
        self.codec = codec
        self.fn = fn
        self.inject = sorted(inject)
        functools.update_wrapper(self, fn)

    def __call__(
        self, ctx: Context, *args: _P.args, **kwargs: _P.kwargs
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(ctx, *args, **kwargs)

    def invoke(
        self, log: LogStorage, /, *, tracer: Tracer | None = None
    ) -> AsyncContextManager[DurableRun[_P, _T_co]]:
        """Create an invocation context for this durable function.

        Args:
            log: Log storage for persisting operation history
            tracer: Optional tracer for observability

        Returns:
            Async context manager for Invoke instance
        """
        return DurableRun[_P, _T_co].invoke(self, log, tracer)


@overload
def durable(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]], /
) -> DurableFn[_P, _T_co]: ...
@overload
def durable(
    *, codec: Codec | None = None
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
    """Decorator to mark async functions as durable.

    Durable functions are the main orchestration layer in Duron. They:

    - Must take [duron.Context][] as their first parameter
    - Must use [context][duron.Context] for all side effects to ensure determinism
    - Use [duron.Provided][] to mark parameters that will be injected at runtime

    Args:
        codec: Optional codec for serialization

    Example:
        ```python
        @duron.durable
        async def my_workflow(
            ctx: duron.Context, user_id: str, stream: duron.Stream[int] = duron.Provided
        ) -> User: ...
        ```

    Returns:
        [DurableFn][duron.DurableFn] that can be invoked with log storage
    """

    def decorate(
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    ) -> DurableFn[_P, _T_co]:
        info = inspect_function(fn)
        inject = (
            (name, *ty)
            for name, param in info.parameter_types.items()
            if (ty := _parse_type(param))
        )
        return DurableFn(codec=codec or config.codec, fn=fn, inject=inject)

    if f is not None:
        return decorate(f)
    return decorate


def _parse_type(tp: TypeHint[Any]) -> tuple[type, TypeHint[Any]] | None:
    origin = get_origin(tp)
    args = get_args(tp)

    if origin is Stream and args:
        return (Stream, cast("TypeHint[Any]", args[0]))
    if origin is Signal and args:
        return (Signal, cast("TypeHint[Any]", args[0]))
    if origin is StreamWriter and args:
        return (StreamWriter, cast("TypeHint[Any]", args[0]))
    return None
