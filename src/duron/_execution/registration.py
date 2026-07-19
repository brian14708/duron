"""Immutable effect registration and transport-neutral local dispatch."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import inspect
import json
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Literal, cast, get_args, get_origin
from typing_extensions import ParamSpec, TypeVar

from duron._execution.handles import (
    CommandSender,
    SignalReceiver,
    StreamSender,
    handle_annotation,
)
from duron._execution.models import (
    EffectContextImpl,
    HandleReference,
    InputPortParameter,
    OutputPortParameter,
    canonical_json,
    digest,
)
from duron._hints import UnspecifiedType
from duron.errors import InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from duron._api.effect import Effect
    from duron._api.ports import _Port
    from duron._hints import JSONValue, TypeHint
    from duron.codec import Codec


_P = ParamSpec("_P")
_T = TypeVar("_T")


def annotation_identity(annotation: object) -> JSONValue:
    if annotation is inspect.Parameter.empty or annotation is UnspecifiedType:
        return None
    if annotation is None:
        return "builtins:NoneType"
    if annotation is Ellipsis:
        return {"ellipsis": True}
    if isinstance(annotation, str):
        msg = f"unresolved effect annotation: {annotation!r}"
        raise WorkflowDefinitionError(msg)
    origin = get_origin(annotation)
    if origin is not None:
        if origin is Literal:
            return {
                "origin": annotation_identity(origin),
                "values": [_metadata_identity(value) for value in get_args(annotation)],
            }
        if origin is Annotated:
            payload, *metadata = get_args(annotation)
            return {
                "origin": annotation_identity(origin),
                "type": annotation_identity(payload),
                "metadata": [_metadata_identity(value) for value in metadata],
            }
        return {
            "origin": annotation_identity(origin),
            "args": [annotation_identity(arg) for arg in get_args(annotation)],
        }
    if isinstance(annotation, type):
        return f"{annotation.__module__}:{annotation.__qualname__}"
    if isinstance(annotation, (bool, int, float)):
        return annotation
    if isinstance(annotation, (list, tuple)):
        return [
            annotation_identity(arg)
            for arg in cast("list[object] | tuple[object, ...]", annotation)
        ]
    # Type variables and typing special forms have stable symbolic names. Never
    # persist a repr containing an arbitrary object's memory address.
    name = getattr(annotation, "__name__", None)
    module = getattr(annotation, "__module__", None)
    if isinstance(name, str) and isinstance(module, str):
        return f"{module}:{name}"
    msg = "effect signature contains an annotation without a stable identity"
    raise WorkflowDefinitionError(msg)


def _metadata_identity(value: object) -> JSONValue:
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    if isinstance(value, str):
        return value
    return annotation_identity(value)


def schema_id(item_type: object) -> str:
    """Return the durable schema identity bound into handle references.

    Returns:
        A digest of the annotation's stable identity.

    """
    return digest("duron.schema.v0", annotation_identity(item_type))


def registration_matches(registration: Registration, effect: Effect[_P, _T]) -> bool:
    """Whether a registration still describes the given effect definition.

    Returns:
        True when every persisted policy field matches the effect.

    """
    return (
        registration.fn is effect.fn
        and registration.name == effect.name
        and registration.signature == effect._wire_signature
        and registration.ports == effect.ports
    )


@dataclass(frozen=True, slots=True)
class Registration:
    """Immutable resolved registration owned by one run."""

    name: str
    fn: Callable[..., object]
    signature: inspect.Signature
    ports: tuple[_Port, ...] = ()


def event_arguments(
    registration: Registration, encoded: bytes
) -> tuple[list[JSONValue], dict[str, JSONValue]]:
    values = cast("dict[str, JSONValue]", json.loads(encoded))
    args: list[JSONValue] = []
    kwargs: dict[str, JSONValue] = {}
    for name, parameter in registration.signature.parameters.items():
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs[name] = values[name]
        else:
            args.append(values[name])
    return args, kwargs


def snapshot_inputs(
    registration: Registration,
    codec: Codec,
    args: tuple[object, ...],
    kwargs: Mapping[str, object],
    *,
    owner_id: str | None = None,
) -> bytes:
    bound = registration.signature.bind(*args, **kwargs)
    bound.apply_defaults()
    encoded: dict[str, JSONValue] = {}
    for name, value in bound.arguments.items():
        parameter = registration.signature.parameters[name]
        dtype = parameter.annotation
        if dtype is inspect.Parameter.empty:
            dtype = UnspecifiedType
        handle = handle_annotation(dtype)
        if handle is not None:
            item_type, direction = handle
            sender: StreamSender[object] | SignalReceiver[object]
            if isinstance(value, StreamSender):
                sender = cast("StreamSender[object]", value)
            elif isinstance(value, SignalReceiver):
                sender = cast("SignalReceiver[object]", value)
            else:
                msg = "handle argument requires the declared directional handle"
                raise InvalidHandleError(msg)
            if sender._reference.direction != direction:
                msg = "handle argument direction does not match its annotation"
                raise InvalidHandleError(msg)
            if annotation_identity(sender.item_type) != annotation_identity(item_type):
                msg = "stream argument does not match its declared payload type"
                raise InvalidHandleError(msg)
            reference = sender._for_owner(owner_id or sender._reference.owner_id)
            if reference.schema_id != schema_id(item_type):
                msg = "stream reference payload schema does not match its annotation"
                raise InvalidHandleError(msg)
            encoded[name] = {"$duron.handle": reference.fields()}
        elif _contains_handle(value):
            msg = "runtime handles require an annotated directional handle parameter"
            raise InvalidHandleError(msg)
        else:
            encoded[name] = codec.encode_json(value, dtype)
    return canonical_json(encoded)


def _contains_handle(value: object, seen: set[int] | None = None) -> bool:
    value_id = id(value)
    if isinstance(value, (StreamSender, CommandSender, SignalReceiver)):
        return True
    if not isinstance(value, (dict, list, tuple, set, frozenset)):
        return False
    if seen is None:
        seen = set()
    if value_id in seen:
        return False
    seen.add(value_id)
    items = (
        cast("dict[object, object]", value).items()
        if isinstance(value, dict)
        else cast("list[object]", value)
    )
    return any(_contains_handle(item, seen) for item in items)


def decode_inputs(
    registration: Registration,
    codec: Codec,
    encoded: bytes,
    *,
    context: EffectContextImpl | None = None,
) -> inspect.BoundArguments:
    # The source is generated by snapshot_inputs, and every decode starts from
    # fresh JSON so effects cannot mutate a later invocation's inputs.
    payloads = cast("dict[str, JSONValue]", json.loads(encoded))
    values: dict[str, object] = {}
    for name, payload in payloads.items():
        parameter = registration.signature.parameters[name]
        dtype = parameter.annotation
        if dtype is inspect.Parameter.empty:
            dtype = UnspecifiedType
        handle = handle_annotation(dtype)
        if handle is not None:
            item_type, direction = handle
            if context is None or context.handles is None:
                msg = "handle-bearing execution requires a bound stream resolver"
                raise InvalidHandleError(msg)
            reference = input_reference(payload)
            typed_item = cast("TypeHint[object]", item_type)
            if direction == "stream.sender":
                values[name] = StreamSender(
                    reference,
                    item_type=typed_item,
                    posting=context.handles.stream(
                        OutputPortParameter(name, typed_item)
                    ),
                )
            else:
                values[name] = context.handles.signal_receiver(
                    InputPortParameter(name, typed_item)
                )
        else:
            values[name] = codec.decode_json(payload, dtype)
    return inspect.BoundArguments(registration.signature, OrderedDict(values))


def input_reference(payload: JSONValue) -> HandleReference:
    if not isinstance(payload, dict) or set(payload) != {"$duron.handle"}:
        msg = "stream argument requires a tagged runtime reference"
        raise InvalidHandleError(msg)
    return HandleReference.from_fields(payload["$duron.handle"])


def input_references(
    registration: Registration, inputs: bytes
) -> tuple[HandleReference, ...]:
    values = cast("dict[str, JSONValue]", json.loads(inputs))
    return tuple(
        input_reference(values[name])
        for name, parameter in registration.signature.parameters.items()
        if handle_annotation(parameter.annotation) is not None
    )
