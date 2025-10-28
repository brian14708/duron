from __future__ import annotations

import contextlib
import inspect
import weakref
from typing import TYPE_CHECKING, Annotated, cast, get_args, get_origin
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
    The name of the function.
    """
    return_annotations: Sequence[Any]
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
    parameter_annotations: Mapping[str, Sequence[Any]]
    """
    A mapping of parameter names to their types.
    """


_FUNCTION_CACHE: weakref.WeakKeyDictionary[Callable[..., object], FunctionType] = (
    weakref.WeakKeyDictionary()
)


def _unwrap_annotated(
    type_hint: TypeHint[Any],
) -> tuple[TypeHint[Any], tuple[Any, ...]]:
    """Unwrap an Annotated type to get the actual type and its annotations.

    Args:
        type_hint: A type hint that may be Annotated

    Returns:
        A tuple of (actual_type, annotations_tuple)

    """
    if get_origin(type_hint) is Annotated:
        args = get_args(type_hint)
        # First arg is the actual type, rest are annotations
        return args[0], args[1:]
    return type_hint, ()


def inspect_function(fn: Callable[..., object]) -> FunctionType:
    if fn in _FUNCTION_CACHE:
        return _FUNCTION_CACHE[fn]
    try:
        sig = inspect.signature(fn, eval_str=True)
    except NameError:
        sig = inspect.signature(fn)

    # Unwrap return type annotation
    raw_return_type = (
        sig.return_annotation
        if sig.return_annotation != inspect.Parameter.empty
        else UnspecifiedType
    )
    return_type, return_annotations = _unwrap_annotated(raw_return_type)

    parameter_names: list[str] = []
    parameter_types: dict[str, TypeHint[Any]] = {}
    parameter_annotations: dict[str, tuple[Any, ...]] = {}
    for k, p in sig.parameters.items():
        if p.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue

        if p.kind is not inspect.Parameter.KEYWORD_ONLY:
            parameter_names.append(k)

        # Unwrap parameter type annotation
        raw_param_type = (
            p.annotation
            if p.annotation is not inspect.Parameter.empty
            else UnspecifiedType
        )
        param_type, param_annots = _unwrap_annotated(raw_param_type)
        parameter_types[p.name] = param_type
        if param_annots:
            parameter_annotations[p.name] = param_annots

    ret = FunctionType(
        name=cast("str", getattr(fn, "__name__", repr(fn))),
        return_type=return_type,
        return_annotations=return_annotations,
        parameters=parameter_names,
        parameter_types=parameter_types,
        parameter_annotations=parameter_annotations,
    )
    with contextlib.suppress(TypeError):
        return _FUNCTION_CACHE.setdefault(fn, ret)
    return ret
