"""Immutable effect registration and transport-neutral local dispatch."""

from __future__ import annotations

import inspect
import json
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, cast, get_args, get_origin

from duron._hints import UnspecifiedType
from duron._runtime.execution.effect import Effect
from duron._runtime.execution.errors import error_key, type_key
from duron._runtime.execution.handle_data import (
    CommandReceiverParameter,
    CommandSenderParameter,
    HandleReference,
    StreamParameter,
)
from duron._runtime.execution.handles import (
    CommandReceiver,
    CommandSender,
    StreamSender,
    handle_annotation,
)
from duron._runtime.execution.models import (
    EffectContextImpl,
    EffectId,
    EffectIdentity,
    EffectOperation,
    RetryPolicy,
    canonical_json,
    digest,
)
from duron.codec import DefaultCodec
from duron.errors import InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from duron._runtime.execution.handles import HandleResolver
    from duron._runtime.execution.reporting import EffectExecutor
    from duron.codec import Codec
    from duron.types import JSONValue, TypeHint


def annotation_identity(annotation: object) -> JSONValue:
    if annotation is inspect.Parameter.empty or annotation is UnspecifiedType:
        return None
    if annotation is None:
        return "builtins:NoneType"
    if isinstance(annotation, str):
        msg = f"unresolved effect annotation: {annotation!r}"
        raise WorkflowDefinitionError(msg)
    origin = get_origin(annotation)
    if origin is not None:
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


def schema_id(item_type: object) -> str:
    """Return the durable schema identity bound into handle references.

    Returns:
        The versioned digest of the annotation's stable identity.

    """
    return digest("duron.schema.v1", annotation_identity(item_type))


def _normalize_builtin(codec: Codec | None) -> Codec | None:
    if type(codec) is DefaultCodec:
        return DefaultCodec
    return codec


def resolve_codec_id(codec: Codec, codec_id: str | None) -> str:
    """Require application-owned versioned IDs for custom codecs.

    Returns:
        The validated versioned codec identifier.

    Raises:
        WorkflowDefinitionError: If the codec ID is missing or conflicts.

    """
    if codec is DefaultCodec or type(codec) is DefaultCodec:
        if codec_id is not None and codec_id != "duron.json.v1":
            msg = "built-in codec IDs cannot be overridden"
            raise WorkflowDefinitionError(msg)
        return "duron.json.v1"
    if not isinstance(codec_id, str) or not codec_id:
        msg = "a custom codec requires an explicit stable codec_id"
        raise WorkflowDefinitionError(msg)
    if codec_id.startswith("duron."):
        msg = "custom codecs cannot use a built-in codec_id"
        raise WorkflowDefinitionError(msg)
    return codec_id


def registration_matches(registration: Registration, effect: Effect[..., Any]) -> bool:
    """Whether a registration still describes the given effect definition.

    Returns:
        True when every persisted policy field matches the effect.

    """
    return (
        registration.fn is effect.fn
        and registration.identity == effect.identity
        and registration.raises == effect.raises
        and registration.retry == effect.retry
    )


@dataclass(frozen=True, slots=True)
class Registration:
    """Immutable resolved registration owned by one run."""

    identity: EffectIdentity
    fn: Callable[..., object]
    signature: inspect.Signature
    raises: tuple[type[BaseException], ...]
    retry: RetryPolicy
    codec: Codec | None
    codec_id: str | None

    def descriptor(
        self, resolved_codec_id: str, *, codec: Codec | None = None
    ) -> JSONValue:
        """Describe the persisted semantic registration metadata.

        Returns:
            A JSON descriptor with stable policy and codec identifiers.

        """
        retry = self.retry
        resolved_codec = codec if codec is not None else self.codec
        if resolved_codec is None:
            resolved_codec = DefaultCodec
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
                "default": resolved_codec.encode_json(parameter.default, dtype)
                if has_default
                else None,
            })
        return {
            "version": 1,
            "effect_id": self.identity.effect_id.value,
            "name": self.identity.name,
            "effect_version": self.identity.version,
            "codec_id": resolved_codec_id,
            "signature": parameters,
            "return_type": annotation_identity(self.signature.return_annotation),
            "raises": [error_key(t) for t in self.raises],
            "retry": {
                "max_attempts": retry.max_attempts,
                "initial_delay": retry.initial_delay,
                "multiplier": retry.multiplier,
                "max_delay": retry.max_delay,
                "max_recoveries": retry.max_recoveries,
                "retry_on": [error_key(t) for t in retry.retry_on],
            },
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
            sender: (
                StreamSender[object] | CommandSender[object] | CommandReceiver[object]
            )
            if isinstance(value, StreamSender):
                sender = cast("StreamSender[object]", value)
            elif isinstance(value, CommandSender):
                sender = cast("CommandSender[object]", value)
            elif isinstance(value, CommandReceiver):
                sender = cast("CommandReceiver[object]", value)
            else:
                msg = "handle argument requires the declared directional handle"
                raise InvalidHandleError(msg)
            if sender.reference.direction != direction:
                msg = "handle argument direction does not match its annotation"
                raise InvalidHandleError(msg)
            if annotation_identity(sender.item_type) != annotation_identity(item_type):
                msg = "stream argument does not match its declared payload type"
                raise InvalidHandleError(msg)
            reference = sender.for_owner(owner_id or sender.reference.owner_id)
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
    input_digest = digest("duron.inputs.v1", encoded)
    return InputSnapshot(payload, input_digest)


def _contains_handle(value: object, seen: set[int] | None = None) -> bool:
    value_id = id(value)
    if isinstance(value, (StreamSender, CommandSender, CommandReceiver)):
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
                    posting=context.handles.stream(StreamParameter(name, typed_item)),
                    key_namespace=f"local:{context.invocation_number}:",
                )
            elif direction == "signal.sender":
                values[name] = CommandSender(
                    reference,
                    item_type=typed_item,
                    posting=context.handles.signal_sender(
                        CommandSenderParameter(name, typed_item)
                    ),
                    key_namespace=f"local:{context.invocation_number}:",
                )
            else:
                values[name] = context.handles.signal_receiver(
                    CommandReceiverParameter(name, typed_item)
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


class EffectRegistry:
    """Run-local registrations and per-effect dispatch routing."""

    def __init__(self) -> None:
        self._registrations: dict[EffectId, Registration] = {}
        self._environments: dict[
            EffectId, tuple[Codec | None, str | None, EffectExecutor | None]
        ] = {}
        self._codec_ids: dict[str, Codec] = {}
        self._executors: dict[EffectId, EffectExecutor] = {}

    def register(self, fn: Effect[..., Any]) -> EffectId:
        """Register an effect definition and its selected environment.

        Returns:
            The effect's stable identifier.

        Raises:
            WorkflowDefinitionError: If the definition or codec conflicts with
                a registered effect or the value is not an effect definition.

        """
        if not isinstance(cast("object", fn), Effect):
            msg = "executor registration requires an @duron.effect definition"
            raise WorkflowDefinitionError(msg)
        body = fn.fn
        if fn.codec is None and fn.codec_id is not None:
            msg = "codec_id requires an explicit codec"
            raise WorkflowDefinitionError(msg)
        codec = fn.codec
        codec_id = fn.codec_id
        codec = _normalize_builtin(codec)
        resolved_codec_id = (
            resolve_codec_id(codec, codec_id) if codec is not None else None
        )
        if resolved_codec_id is not None and codec is not None:
            self._check_codec_conflict(resolved_codec_id, codec)
        signature = fn.signature
        registration = Registration(
            fn.identity, body, signature, fn.raises, fn.retry, codec, resolved_codec_id
        )
        effect_id = registration.identity.effect_id
        old = self._registrations.get(effect_id)
        if old is not None and (
            old.fn is not body
            or old != registration
            or self._environments[effect_id] != (codec, codec_id, fn.executor)
        ):
            msg = "conflicting registration for effect name and version"
            raise WorkflowDefinitionError(msg)
        self._registrations[effect_id] = registration
        self._environments[effect_id] = (codec, codec_id, fn.executor)
        if resolved_codec_id is not None and codec is not None:
            self._codec_ids[resolved_codec_id] = codec
        if fn.executor is not None:
            self._executors[effect_id] = fn.executor
        return effect_id

    def executor_for(self, effect_id: EffectId) -> EffectExecutor | None:
        return self._executors.get(effect_id)

    def registration(self, effect_id: EffectId) -> Registration:
        try:
            return self._registrations[effect_id]
        except KeyError as exc:
            msg = "effect is not registered in this executor"
            raise WorkflowDefinitionError(msg) from exc

    def codec_for(self, effect_id: EffectId, default: Codec) -> tuple[Codec, str]:
        registration = self.registration(effect_id)
        if registration.codec is not None and registration.codec_id is not None:
            return registration.codec, registration.codec_id
        if default is DefaultCodec or type(default) is DefaultCodec:
            return DefaultCodec, "duron.json.v1"
        ids = [key for key, value in self._codec_ids.items() if value is default]
        if len(ids) != 1:
            msg = "effect request has no unambiguous configured codec"
            raise WorkflowDefinitionError(msg)
        return default, ids[0]

    def _check_codec_conflict(self, codec_id: str, codec: Codec) -> None:
        previous_codec = self._codec_ids.get(codec_id)
        if (
            previous_codec is not None
            and previous_codec is not codec
            and previous_codec != codec
        ):
            msg = "conflicting codec configurations for one codec_id"
            raise WorkflowDefinitionError(msg)

    def prepare(
        self,
        effect: Effect[..., Any],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        *,
        codec: Codec,
        codec_id: str,
        seed: str,
        owner_id: str | None = None,
    ) -> EffectOperation:
        effect_id = effect.identity.effect_id
        if effect_id not in self._registrations:
            _ = self.register(effect)
        registration = self.registration(effect_id)
        if (
            not registration_matches(registration, effect)
            or registration.signature != effect.signature
            or self._environments[effect_id]
            != (registration.codec, effect.codec_id, effect.executor)
        ):
            msg = "effect does not match its executor registration"
            raise WorkflowDefinitionError(msg)
        resolved_codec = registration.codec if registration.codec is not None else codec
        resolved_id = (
            registration.codec_id if registration.codec_id is not None else codec_id
        )
        resolved_codec = cast("Codec", _normalize_builtin(resolved_codec))
        self._check_codec_conflict(resolved_id, resolved_codec)
        self._codec_ids[resolved_id] = resolved_codec
        snapshot = snapshot_inputs(
            registration, resolved_codec, args, kwargs, owner_id=owner_id
        )
        references = input_references(registration, snapshot.encoded)
        if len({
            (reference.channel_id, reference.direction) for reference in references
        }) != len(references):
            msg = "an effect cannot bind one stream through multiple parameters"
            raise InvalidHandleError(msg)
        if any(
            reference.run_seed != seed or reference.codec_id != resolved_id
            for reference in references
        ):
            msg = "handle run identity or codec differs from the effect call"
            raise InvalidHandleError(msg)

        async def invoke(context: EffectContextImpl) -> object:
            return await self.run(effect_id, snapshot, context, codec=resolved_codec)

        return EffectOperation(
            registration.identity,
            seed,
            snapshot.input_digest,
            canonical_json(registration.descriptor(resolved_id, codec=resolved_codec)),
            registration.raises,
            registration.retry,
            resolved_codec,
            resolved_id,
            invoke,
            snapshot.encoded,
            references,
        )

    async def run(
        self,
        effect_id: EffectId,
        inputs: InputSnapshot,
        context: EffectContextImpl,
        *,
        codec: Codec,
    ) -> object:
        """Invoke one body; the session owns durable attempts and completion.

        Returns:
            The effect's result.

        Raises:
            WorkflowDefinitionError: If context identity or dispatch is invalid.

        """
        registration = self.registration(effect_id)
        if (
            context.effect != registration.identity
            or context.identity.effect_id != effect_id
            or context.identity.input_digest != inputs.input_digest
        ):
            msg = "effect context identity does not match registration"
            raise WorkflowDefinitionError(msg)
        bound = decode_inputs(registration, codec, inputs.encoded, context=context)
        result = registration.fn(context, *bound.args, **bound.kwargs)
        if not inspect.isawaitable(result):
            msg = "async effect did not return an awaitable"
            raise WorkflowDefinitionError(msg)
        return await result

    @staticmethod
    def context(
        context: EffectContextImpl, *, handles: HandleResolver
    ) -> EffectContextImpl:
        return replace(context, handles=handles)
