from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from duron.typing._hint import UnspecifiedType

if TYPE_CHECKING:
    from collections.abc import Callable

    from duron.typing._hint import TypeHint


@dataclass(slots=True)
class FunctionType:
    return_type: TypeHint[Any]
    """
    The return type of the function.
    """
    parameters: list[str]
    """
    The names of the parameters of the function, in order.
    """
    parameter_types: dict[str, TypeHint[Any]]
    """
    A mapping of parameter names to their types.
    """


def inspect_function(
    fn: Callable[..., object],
) -> FunctionType:
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
        if p.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            continue

        parameter_names.append(k)
        parameter_types[p.name] = (
            p.annotation if p.annotation != inspect.Parameter.empty else UnspecifiedType
        )

    return FunctionType(
        return_type=return_type,
        parameters=parameter_names,
        parameter_types=parameter_types,
    )
