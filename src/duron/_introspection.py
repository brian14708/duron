"""Function annotation introspection used by the runtime."""

from __future__ import annotations

import inspect
from types import FunctionType as PythonFunctionType
from types import MethodType
from typing import TYPE_CHECKING, Annotated, get_args, get_origin

from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable


def effect_signature(
    fn: Callable[..., object], *, context_type: object, context_name: str = "effect"
) -> inspect.Signature:
    """Validate context injection and return only the caller signature.

    Returns:
        The signature excluding the injected parameter.

    Raises:
        WorkflowDefinitionError: If context injection is not well-defined.

    """
    signature = inspect.signature(fn)
    parameters = tuple(signature.parameters.values())
    if any(p.kind in {p.VAR_POSITIONAL, p.VAR_KEYWORD} for p in parameters):
        msg = f"variadic {context_name} parameters are not supported"
        raise WorkflowDefinitionError(msg)
    if not parameters:
        msg = f"a {context_name} requires a first positional context parameter"
        raise WorkflowDefinitionError(msg)
    body = fn.__func__ if isinstance(fn, MethodType) else fn
    globals_ = body.__globals__ if isinstance(body, PythonFunctionType) else {}
    # Resolve names from the function's own globals and closure. Walking the
    # importer's call stack makes the contract depend on unrelated local names.
    locals_ = dict(inspect.getclosurevars(body).nonlocals)
    try:
        annotations: dict[str, object] = inspect.get_annotations(
            fn, globals=globals_, locals=locals_, eval_str=True
        )
    except (NameError, TypeError, SyntaxError) as error:
        msg = f"{context_name} has unresolved annotations: {error}"
        raise WorkflowDefinitionError(msg) from error
    raw_annotations = {
        **{p.name: p.annotation for p in parameters},
        "return": signature.return_annotation,
    }
    for name, raw_annotation in raw_annotations.items():
        annotation = annotations.get(name, raw_annotation)
        if isinstance(annotation, str):
            msg = (
                f"{context_name} parameter {name!r} has unresolved annotation "
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
            f"a {context_name} requires exactly one context parameter as its "
            "first positional parameter"
        )
        raise WorkflowDefinitionError(msg)
    return signature.replace(
        parameters=[
            p.replace(annotation=annotations.get(p.name, p.annotation))
            for p in parameters[1:]
        ],
        return_annotation=annotations.get("return", signature.return_annotation),
    )
