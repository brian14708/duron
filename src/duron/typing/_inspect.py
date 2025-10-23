from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, cast
from typing_extensions import Any, NamedTuple

from duron.typing._hint import UnspecifiedType

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from duron.typing._hint import TypeHint


class FunctionType(NamedTuple):
    name: str
    """
    The name of the function.
    """
    return_type: TypeHint[Any]
    """
    The return type of the function.
    """
    parameters: Sequence[str]
    """
    The names of the parameters of the function, in order.
    """
    parameter_types: Mapping[str, TypeHint[Any]]
    """
    A mapping of parameter names to their types.
    """


def inspect_function(fn: Callable[..., object]) -> FunctionType:
    try:
        sig = inspect.signature(fn, eval_str=True)
    except NameError:
        sig = inspect.signature(fn)
    return_type = (
        sig.return_annotation
        if sig.return_annotation != inspect.Parameter.empty
        else UnspecifiedType
    )

    parameter_names: list[str] = []
    parameter_types: dict[str, TypeHint[Any]] = {}
    for k, p in sig.parameters.items():
        if p.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue

        if p.kind is not inspect.Parameter.KEYWORD_ONLY:
            parameter_names.append(k)
        parameter_types[p.name] = (
            p.annotation
            if p.annotation is not inspect.Parameter.empty
            else UnspecifiedType
        )

    return FunctionType(
        name=cast("str", getattr(fn, "__name__", repr(fn))),
        return_type=return_type,
        parameters=parameter_names,
        parameter_types=parameter_types,
    )
