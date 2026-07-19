"""Definition-time validation of effect context injection and signatures."""

from __future__ import annotations

import inspect
from types import FunctionType, MethodType
from typing import TYPE_CHECKING, Annotated, get_args, get_origin

from duron._api.context import EffectContext
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable


def effect_signature(fn: Callable[..., object]) -> inspect.Signature:
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
    globals_ = body.__globals__ if isinstance(body, FunctionType) else {}
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
        if annotation is EffectContext:
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
