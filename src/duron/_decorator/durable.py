"""Durable function decorator for replayable async workflows.

This module provides the `@durable` decorator which marks async functions
as orchestration functions. Durable functions:
- Must take `Context` as their first parameter
- Can be paused, resumed, and replayed deterministically
- Support automatic injection of Stream, Signal, and StreamWriter parameters
"""

from __future__ import annotations

import inspect
from typing import (
    TYPE_CHECKING,
    Concatenate,
    Final,
    Generic,
    cast,
    get_args,
    get_origin,
)
from typing_extensions import Any, ParamSpec, TypeVar, final, overload

from duron._core.config import config
from duron._core.context import Context
from duron._core.signal import Signal
from duron._core.stream import Stream, StreamWriter
from duron.typing import inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Iterable

    from duron.codec import Codec
    from duron.typing import TypeHint


_T = TypeVar("_T")
_P = ParamSpec("_P")


Provided: Final = cast("Any", ...)
"""
Mark a parameter as provided when invoked.
"""


class DurableDefinitionError(TypeError):
    """Raised when a durable function has an unsupported definition."""


@final
class DurableFn(Generic[_P, _T]):
    __slots__ = ("codec", "fn", "inject")

    def __init__(
        self,
        codec: Codec,
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]],
        inject: Iterable[tuple[str, type, TypeHint[Any]]],
    ) -> None:
        self.codec = codec
        self.fn = fn
        self.inject = sorted(inject)


@overload
def durable(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]], /
) -> DurableFn[_P, _T]: ...
@overload
def durable(
    *, codec: Codec | None = None
) -> Callable[
    [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]]], DurableFn[_P, _T]
]: ...
def durable(
    f: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]] | None = None,
    /,
    *,
    codec: Codec | None = None,
) -> (
    DurableFn[_P, _T]
    | Callable[
        [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]]], DurableFn[_P, _T]
    ]
):
    """Mark async functions as durable.

    Durable functions are the main orchestration layer in Duron. They:

    - Must take [duron.Context][] as their first parameter
    - Must use [context][duron.Context] for all side effects to ensure determinism
    - Use [duron.Provided][] to mark parameters that will be injected at runtime

    Args:
        f: Function to mark as durable
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
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]],
    ) -> DurableFn[_P, _T]:
        if not inspect.iscoroutinefunction(fn):
            msg = "A durable function must be defined with async def"
            raise DurableDefinitionError(msg)
        try:
            signature = inspect.signature(fn, eval_str=True)
        except NameError:
            signature = inspect.signature(fn)
        parameters = list(signature.parameters.values())
        if not parameters:
            msg = "A durable function must have Context as its first parameter"
            raise DurableDefinitionError(msg)
        first = parameters[0]
        first_annotation = first.annotation
        valid_context = first_annotation is Context or (
            isinstance(first_annotation, str)
            and first_annotation.rsplit(".", maxsplit=1)[-1] == "Context"
        )
        if (
            first.kind
            in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
            or not valid_context
        ):
            msg = "The first durable function parameter must be annotated as Context"
            raise DurableDefinitionError(msg)
        for parameter in parameters:
            if parameter.kind in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }:
                msg = f"Variadic parameter {parameter.name!r} is not supported"
                raise DurableDefinitionError(msg)

        info = inspect_function(fn)
        injection: list[tuple[str, type, TypeHint[Any]]] = []
        for name, param in info.parameter_types.items():
            parsed = _parse_type(param)
            if parsed is None:
                continue
            parameter = signature.parameters[name]
            if parameter.default is not Provided:
                msg = f"Injected parameter {name!r} must use default value Provided"
                raise DurableDefinitionError(msg)
            injection.append((name, *parsed))
        inject = injection
        return DurableFn(codec=codec or config.codec, fn=fn, inject=inject)

    if f is not None:
        return decorate(f)
    return decorate


def _parse_type(tp: TypeHint[Any]) -> tuple[type, TypeHint[Any]] | None:
    origin = get_origin(tp)
    if not origin:
        return None

    args = get_args(tp)
    if origin is Stream and args:
        return (Stream, cast("TypeHint[Any]", args[0]))
    if origin is Signal and args:
        return (Signal, cast("TypeHint[Any]", args[0]))
    if origin is StreamWriter and args:
        return (StreamWriter, cast("TypeHint[Any]", args[0]))
    return None
