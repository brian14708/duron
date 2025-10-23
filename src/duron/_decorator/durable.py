"""Durable function decorator for replayable async workflows.

This module provides the `@durable` decorator which marks async functions
as orchestration functions. Durable functions:
- Must take `Context` as their first parameter
- Can be paused, resumed, and replayed deterministically
- Support automatic injection of Stream, Signal, and StreamWriter parameters
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Concatenate, Generic, cast, get_args, get_origin
from typing_extensions import Any, ParamSpec, TypeVar, final, overload

from duron._core.config import config
from duron._core.signal import Signal
from duron._core.stream import Stream, StreamWriter
from duron.typing._inspect import inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterable

    from duron._core.context import Context
    from duron.codec import Codec
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
        self.type_hints = inspect_function(fn)


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
        DurableFn that can be passed to [Session.start][duron.Session.start]
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
