"""Function annotation introspection used by the runtime."""

from __future__ import annotations

import contextlib
import inspect
import weakref
from types import FunctionType as PythonFunctionType
from types import MethodType
from typing import TYPE_CHECKING, Annotated, Any, cast, get_args, get_origin
from typing_extensions import NamedTuple

from duron._hints import TypeHint, UnspecifiedType
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence


class FunctionType(NamedTuple):
    name: str
    """
    The name of the function.
    """
    return_type: TypeHint[Any]
    """
    The return type of the function.
    """
    return_annotations: Sequence[Any]
    """
    The ``Annotated`` metadata attached to the return type, if any.
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
    A mapping of parameter names to their ``Annotated`` metadata, if any.
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


def effect_signature(
    fn: Callable[..., object], *, context_type: object
) -> inspect.Signature:
    """Validate context injection and return only the caller signature.

    Returns:
        The signature excluding the injected parameter.

    Raises:
        WorkflowDefinitionError: If context injection is not well-defined.

    """
    signature = inspect.signature(fn)
    parameters = tuple(signature.parameters.values())
    if not parameters:
        msg = "an effect requires a first positional EffectContext parameter"
        raise WorkflowDefinitionError(msg)
    body = fn.__func__ if isinstance(fn, MethodType) else fn
    globals_ = body.__globals__ if isinstance(body, PythonFunctionType) else {}
    locals_: dict[str, object] = {}
    frame = inspect.currentframe()
    try:
        frame = frame.f_back if frame is not None else None
        while frame is not None:
            for key, value in frame.f_locals.items():
                locals_.setdefault(key, value)
            frame = frame.f_back
    finally:
        del frame
    annotations: dict[str, object] = {}
    raw_annotations = {
        **{p.name: p.annotation for p in parameters},
        "return": signature.return_annotation,
    }
    for name, raw_annotation in raw_annotations.items():
        annotation = raw_annotation
        if isinstance(annotation, str):
            try:
                annotation = eval(annotation, globals_, locals_)  # ruff: ignore[suspicious-eval-usage]
            except (NameError, TypeError, SyntaxError) as error:
                msg = (
                    f"effect parameter {name!r} has unresolved annotation "
                    f"{raw_annotation!r}"
                )
                raise WorkflowDefinitionError(msg) from error
            if isinstance(annotation, str):
                msg = (
                    f"effect parameter {name!r} has unresolved annotation "
                    f"{raw_annotation!r}"
                )
                raise WorkflowDefinitionError(msg)
        annotations[name] = annotation
    first = parameters[0]
    contexts: list[inspect.Parameter] = []
    for parameter in parameters:
        annotation = annotations.get(parameter.name)
        if get_origin(annotation) is Annotated:
            annotation = get_args(annotation)[0]
        if annotation is context_type or get_origin(annotation) is context_type:
            contexts.append(parameter)
    if (
        len(contexts) != 1
        or contexts[0] is not first
        or first.kind
        not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }
    ):
        msg = (
            "an effect requires exactly one EffectContext as its first "
            "positional parameter"
        )
        raise WorkflowDefinitionError(msg)
    return signature.replace(
        parameters=[
            p.replace(annotation=annotations.get(p.name, p.annotation))
            for p in parameters[1:]
        ],
        return_annotation=annotations.get("return", signature.return_annotation),
    )
