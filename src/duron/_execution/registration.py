"""Immutable effect registration and transport-neutral local dispatch."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import inspect
import json
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast, get_args, get_origin

from duron._execution.errors import error_key, type_key
from duron._execution.handles import (
    CommandSender,
    SignalReceiver,
    StreamSender,
    handle_annotation,
)
from duron._execution.models import (
    EffectContextImpl,
    EffectIdentity,
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
        return type_key(annotation)
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
        The versioned digest of the annotation's stable identity.

    """
    return digest("duron.schema.v0", annotation_identity(item_type))


def registration_matches(registration: Registration, effect: Effect[..., Any]) -> bool:
    """Whether a registration still describes the given effect definition.

    Returns:
        True when every persisted policy field matches the effect.

    """
    return (
        registration.fn is effect.fn
        and registration.identity == effect.identity
        and registration.raises == effect.raises
        and registration.signature == effect._wire_signature
        and registration.ports == effect.ports
    )


@dataclass(frozen=True, slots=True)
class Registration:
    """Immutable resolved registration owned by one run."""

    identity: EffectIdentity
    fn: Callable[..., object]
    signature: inspect.Signature
    raises: tuple[type[BaseException], ...]
    ports: tuple[_Port, ...] = ()

    def descriptor(self, *, codec: Codec) -> JSONValue:
        """Describe the persisted semantic registration metadata.

        Returns:
            A JSON descriptor with the signature and stable policy identifiers.

        """
        parameters: list[JSONValue] = []
        for parameter in self.signature.parameters.values():
            has_default = parameter.default is not inspect.Parameter.empty
            dtype = parameter.annotation
            if dtype is inspect.Parameter.empty:
                dtype = UnspecifiedType
            parameters.append({
                "name": parameter.name,
                "kind": parameter.kind.name,
                "annotation": annotation_identity(parameter.annotation),
                "has_default": has_default,
                "default": codec.encode_json(parameter.default, dtype)
                if has_default
                else None,
            })
        return {
            "version": 0,
            "effect_id": self.identity.effect_id.value,
            "name": self.identity.name,
            "effect_version": self.identity.version,
            "signature": parameters,
            "return_type": annotation_identity(self.signature.return_annotation),
            "raises": [error_key(t) for t in self.raises],
            "ports": [
                {
                    "name": p.name,
                    "kind": p.kind,
                    "types": [
                        annotation_identity(t) for t in get_args(p.__orig_class__)
                    ],
                }
                for p in self.ports
            ],
        }


@dataclass(frozen=True, slots=True)
class InputSnapshot:
    """Canonical encoded caller inputs, detached from caller-owned objects."""

    encoded: bytes
    input_digest: str


def snapshot_inputs(
    registration: Registration,
    codec: Codec,
    args: tuple[object, ...],
    kwargs: Mapping[str, object],
    *,
    owner_id: str | None = None,
) -> InputSnapshot:
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
            if sender._reference.direction != direction:  # pyright: ignore[reportPrivateUsage]
                msg = "handle argument direction does not match its annotation"
                raise InvalidHandleError(msg)
            if annotation_identity(sender.item_type) != annotation_identity(item_type):
                msg = "stream argument does not match its declared payload type"
                raise InvalidHandleError(msg)
            reference = sender._for_owner(owner_id or sender._reference.owner_id)  # pyright: ignore[reportPrivateUsage]
            if reference.schema_id != schema_id(item_type):
                msg = "stream reference payload schema does not match its annotation"
                raise InvalidHandleError(msg)
            encoded[name] = {"$duron.handle": reference.fields()}
        elif _contains_handle(value):
            msg = "runtime handles require an annotated directional handle parameter"
            raise InvalidHandleError(msg)
        elif parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            values = cast("tuple[object, ...]", value)
            encoded[name] = [codec.encode_json(item, dtype) for item in values]
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            values_by_name = cast("dict[str, object]", value)
            encoded[name] = {
                k: codec.encode_json(values_by_name[k], dtype)
                for k in sorted(values_by_name)
            }
        else:
            encoded[name] = codec.encode_json(value, dtype)
    payload = canonical_json(encoded)
    input_digest = digest("duron.inputs.v0", encoded)
    return InputSnapshot(payload, input_digest)


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
                    key_namespace=f"local:{context.invocation_number}:",
                )
            else:
                values[name] = context.handles.signal_receiver(
                    InputPortParameter(name, typed_item)
                )
        elif parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            values[name] = tuple(
                codec.decode_json(v, dtype) for v in cast("list[JSONValue]", payload)
            )
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            values[name] = {
                k: codec.decode_json(v, dtype)
                for k, v in cast("dict[str, JSONValue]", payload).items()
            }
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
