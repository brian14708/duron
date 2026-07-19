"""Versioned, detached executor requests validated against effect registrations."""

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, NewType, cast
from typing_extensions import ParamSpec, TypeVar

from duron._hints import UnspecifiedType
from duron._runtime.execution.executor import (
    decode_inputs,
    input_reference,
    registration_matches,
    schema_id,
)
from duron._runtime.execution.handle_data import (
    HandleReference,
    RecoveryDescriptor,
    StreamParameter,
)
from duron._runtime.execution.handles import (
    CommandReceiver,
    PostingCommandSender,
    PostingStream,
    handle_annotation,
)
from duron._runtime.execution.models import (
    EffectContextImpl,
    ExecutionIdentity,
    InvocationIdentity,
    canonical_json,
    digest,
)
from duron._runtime.execution.value_schema import validate_value
from duron._runtime.storage.backend import validate_run_id
from duron.errors import InvalidExecutionRequestError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron._api.context import EffectContext
    from duron._runtime.execution.effect import Effect
    from duron._runtime.execution.executor import InputSnapshot, Registration
    from duron._runtime.execution.handle_data import (
        CommandReceiverParameter,
        CommandSenderParameter,
        HandleDirection,
    )
    from duron._runtime.execution.handles import (
        SignalPosting,
        SignalReceiving,
        StreamPosting,
    )
    from duron.codec import Codec
    from duron.types import JSONValue, TypeHint

_P = ParamSpec("_P")
_R = TypeVar("_R")
_T = TypeVar("_T")
_REQUEST_TOKEN = object()

# Immutable transport bytes; construction alone grants no typed authority.
# A NewType over bytes: dispatchers just move the payload, validation happens
# in decode_request.
EffectEnvelope = NewType("EffectEnvelope", bytes)


@dataclass(frozen=True, slots=True, init=False)
class ExecutionRequest(Generic[_R]):
    """Validated request retaining its registered result codec and type."""

    run_id: str
    _context: EffectContextImpl
    _wire: EffectEnvelope = field(repr=False)
    _capability: str = field(repr=False)
    _inputs: bytes = field(repr=False)
    _codec: Codec = field(repr=False)
    _registration: Registration = field(repr=False, compare=False)
    _return_type: TypeHint[_R]
    _handle_names: tuple[str, ...]
    _decoded_inputs: dict[str, JSONValue] | None = field(repr=False, compare=False)
    _references: dict[str, tuple[HandleDirection, object, HandleReference]] = field(
        repr=False, compare=False
    )

    def __init__(
        self,
        token: object,
        *,
        run_id: str,
        context: EffectContextImpl,
        wire: EffectEnvelope,
        capability: str,
        inputs: bytes,
        codec: Codec,
        registration: Registration,
        return_type: TypeHint[_R],
        handle_parameters: tuple[str, ...],
    ) -> None:
        if token is not _REQUEST_TOKEN:
            msg = "execution requests must be decoded against a registered effect"
            raise InvalidExecutionRequestError(msg)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "_context", context)
        object.__setattr__(self, "_wire", wire)
        object.__setattr__(self, "_capability", capability)
        object.__setattr__(self, "_inputs", inputs)
        object.__setattr__(self, "_codec", codec)
        object.__setattr__(self, "_registration", registration)
        object.__setattr__(self, "_return_type", return_type)
        object.__setattr__(self, "_handle_names", handle_parameters)
        object.__setattr__(
            self,
            "_decoded_inputs",
            cast("dict[str, JSONValue]", json.loads(inputs))
            if handle_parameters
            else None,
        )
        object.__setattr__(self, "_references", {})

    @property
    def context(self) -> EffectContext:
        """Read-only idempotency and recovery information for this request."""
        return self._context

    @property
    def wire(self) -> EffectEnvelope:
        return self._wire

    @property
    def capability(self) -> str:
        return self._capability

    @property
    def inputs(self) -> bytes:
        return self._inputs

    def _renewed(
        self, *, context: EffectContextImpl, capability: str
    ) -> ExecutionRequest[_R]:
        fields = cast("dict[str, JSONValue]", json.loads(self.wire))
        fields["capability"] = capability
        context_fields = cast("dict[str, JSONValue]", fields["context"])
        context_fields["invocation"] = cast("JSONValue", context.invocation.fields())
        context_fields["recovering"] = context.recovering
        context_fields["recovery"] = context.recovery.fields()
        return ExecutionRequest(
            _REQUEST_TOKEN,
            run_id=self.run_id,
            context=context,
            wire=EffectEnvelope(canonical_json(fields)),
            capability=capability,
            inputs=self.inputs,
            codec=self._codec,
            registration=self._registration,
            return_type=self._return_type,
            handle_parameters=self._handle_names,
        )

    def encode_result(self, value: _R) -> JSONValue:
        if self._return_type is not UnspecifiedType:
            validate_value(value, self._return_type, allow_unspecified=True)
        encoded = self._codec.encode_json(value, self._return_type)
        # The owner repeats this validation for worker-produced wire payloads.
        self.decode_result(encoded)
        return encoded

    async def _invoke(self, context: EffectContextImpl) -> _R:
        """Decode inputs and invoke the registered effect with a bound context.

        The run owner supplies the bound context for channel resolution.

        Returns:
            The effect body result, before it is reported as durable.

        Raises:
            InvalidExecutionRequestError: If the context belongs to another invocation.
            WorkflowDefinitionError: If the registered body is not awaitable.

        """
        if (
            context.identity != self._context.identity
            or context.invocation != self._context.invocation
        ):
            msg = "effect context does not match this execution request"
            raise InvalidExecutionRequestError(msg)
        bound = decode_inputs(
            self._registration, self._codec, self.inputs, context=context
        )
        result = self._registration.fn(context, *bound.args, **bound.kwargs)
        if not inspect.isawaitable(result):
            msg = "async effect did not return an awaitable"
            raise WorkflowDefinitionError(msg)
        return cast("_R", await result)

    def decode_result(self, payload: JSONValue) -> _R:
        decoded = self._codec.decode_json(payload, self._return_type)
        if self._return_type is not UnspecifiedType:
            validate_value(decoded, self._return_type, allow_unspecified=True)
        return cast("_R", decoded)

    def stream_reference(self, parameter: StreamParameter[_T]) -> HandleReference:
        return self._handle_reference(
            parameter.name, parameter.item_type, "stream.sender"
        )

    def signal_sender_reference(
        self, parameter: CommandSenderParameter[_T]
    ) -> HandleReference:
        return self._handle_reference(
            parameter.name, parameter.item_type, "signal.sender"
        )

    def signal_receiver_reference(
        self, parameter: CommandReceiverParameter[_T]
    ) -> HandleReference:
        return self._handle_reference(
            parameter.name, parameter.item_type, "signal.receiver"
        )

    def _handle_reference(
        self, name: str, item_type: object, direction: HandleDirection
    ) -> HandleReference:
        # Inputs are immutable, so each parameter's reference is resolved at
        # most once even when a reader decodes thousands of events. A lookup
        # with a different direction or item type falls through to full
        # validation, which rejects it.
        cached = self._references.get(name)
        if cached is not None:
            cached_direction, cached_type, cached_reference = cached
            if cached_direction == direction and cached_type == item_type:
                return cached_reference
        if name not in self._handle_names:
            msg = "stream parameter is not declared by this execution"
            raise InvalidExecutionRequestError(msg)
        assert self._decoded_inputs is not None
        reference = input_reference(self._decoded_inputs[name])
        if (
            reference.direction != direction
            or reference.schema_id != schema_id(item_type)
            or not any(
                handle.reference == reference
                for handle in self._context.recovery.handles
            )
        ):
            msg = "stream lookup does not match the validated execution parameter"
            raise InvalidExecutionRequestError(msg)
        self._references[name] = (direction, item_type, reference)
        return reference

    def bind_signal_sender(
        self, parameter: CommandSenderParameter[_T], posting: SignalPosting
    ) -> PostingCommandSender[_T]:
        _ = self.signal_sender_reference(parameter)
        return PostingCommandSender(
            posting, item_type=parameter.item_type, codec=self._codec
        )

    def bind_signal_receiver(
        self, parameter: CommandReceiverParameter[_T], receiving: SignalReceiving[_T]
    ) -> CommandReceiver[_T]:
        reference = self.signal_receiver_reference(parameter)
        return CommandReceiver(
            reference, item_type=parameter.item_type, receiving=receiving
        )

    def decode_signal_value(
        self, parameter: CommandReceiverParameter[_T], payload: JSONValue
    ) -> _T:
        _ = self.signal_receiver_reference(parameter)
        return self._decode_payload(parameter.item_type, payload)

    def bind_stream(
        self, parameter: StreamParameter[_T], posting: StreamPosting
    ) -> PostingStream[_T]:
        _ = self.stream_reference(parameter)
        return PostingStream(posting, item_type=parameter.item_type, codec=self._codec)

    def decode_stream_value(
        self, parameter: StreamParameter[_T], payload: JSONValue
    ) -> _T:
        _ = self.stream_reference(parameter)
        return self._decode_payload(parameter.item_type, payload)

    def _decode_payload(self, item_type: TypeHint[_T], payload: JSONValue) -> _T:
        value = self._codec.decode_json(payload, item_type)
        validate_value(value, item_type)
        return cast("_T", value)


def request_context(request: ExecutionRequest[_R]) -> EffectContextImpl:
    """Return the private invocation data used by runtime and transport code.

    Returns:
        The request's internal execution context.

    """
    return request._context  # pyright: ignore[reportPrivateUsage]  # ruff: ignore[private-member-access]


def local_request(
    *,
    run_id: str,
    context: EffectContextImpl,
    registration: Registration,
    codec: Codec,
    codec_id: str,
    inputs: InputSnapshot,
    capability: str,
    return_type: TypeHint[_R],
) -> ExecutionRequest[_R]:
    wire = encode_request(
        run_id=run_id,
        context=context,
        registration=registration,
        codec=codec,
        codec_id=codec_id,
        inputs=inputs,
        capability=capability,
    )
    return ExecutionRequest(
        _REQUEST_TOKEN,
        run_id=run_id,
        context=context,
        wire=wire,
        capability=capability,
        inputs=inputs.encoded,
        codec=codec,
        registration=registration,
        return_type=return_type,
        handle_parameters=_handle_names(registration),
    )


def encode_request(
    *,
    run_id: str,
    context: EffectContextImpl,
    registration: Registration,
    codec: Codec,
    codec_id: str,
    inputs: InputSnapshot,
    capability: str,
) -> EffectEnvelope:
    """Build a detached request after operation creation and invocation commit.

    Returns:
        The versioned wire envelope.

    Raises:
        InvalidExecutionRequestError: If owner metadata is inconsistent.

    """
    _ = validate_run_id(run_id)
    _ = _text(capability)
    if (
        context.effect != registration.identity
        or context.identity.effect_id != registration.identity.effect_id
        or context.identity.input_digest != inputs.input_digest
    ):
        msg = "execution request metadata does not match the registration or inputs"
        raise InvalidExecutionRequestError(msg)
    _validate_references(
        registration,
        codec_id,
        cast("dict[str, JSONValue]", json.loads(inputs.encoded)),
        context,
    )
    return EffectEnvelope(
        canonical_json({
            "format_version": 1,
            "run_id": run_id,
            "effect_id": registration.identity.effect_id.value,
            "codec_id": codec_id,
            "registration_digest": digest(
                "duron.registration.v1", registration.descriptor(codec_id, codec=codec)
            ),
            "capability": capability,
            "context": {
                "format_version": 1,
                "run_seed": context.identity.run_seed,
                "operation_id": context.identity.operation_id,
                "input_digest": inputs.input_digest,
                "invocation": cast("JSONValue", context.invocation.fields()),
                "recovering": context.recovering,
                "recovery": context.recovery.fields(),
            },
            "inputs": cast("JSONValue", json.loads(inputs.encoded)),
        })
    )


def decode_request(
    wire: EffectEnvelope,
    *,
    effect: Effect[_P, _R],
    registration: Registration,
    codec: Codec,
    codec_id: str,
    run_id: str,
) -> ExecutionRequest[_R]:
    """Validate the entire envelope before retaining any worker authority.

    Returns:
        A request with the registered result type.

    Raises:
        InvalidExecutionRequestError: If any field or decoded input is invalid.

    """
    if type(wire) is not bytes:
        msg = "wire execution requests require immutable bytes"
        raise InvalidExecutionRequestError(msg)
    try:
        return _decode_request(
            wire,
            effect=effect,
            registration=registration,
            codec=codec,
            codec_id=codec_id,
            run_id=run_id,
        )
    except Exception as error:
        msg = "invalid execution request for the registered effect"
        raise InvalidExecutionRequestError(msg) from error


def _decode_request(
    wire: EffectEnvelope,
    *,
    effect: Effect[_P, _R],
    registration: Registration,
    codec: Codec,
    codec_id: str,
    run_id: str,
) -> ExecutionRequest[_R]:
    data = _record(
        json.loads(wire, object_pairs_hook=_unique_object),
        {
            "format_version",
            "run_id",
            "effect_id",
            "codec_id",
            "registration_digest",
            "capability",
            "context",
            "inputs",
        },
    )
    _version(data)
    if (
        _text(data["run_id"]) != validate_run_id(run_id)
        or _text(data["effect_id"]) != registration.identity.effect_id.value
        or not registration_matches(registration, effect)
        or _text(data["codec_id"]) != codec_id
        or _text(data["registration_digest"])
        != digest(
            "duron.registration.v1", registration.descriptor(codec_id, codec=codec)
        )
    ):
        msg = "request registration or run identity mismatch"
        raise ValueError(msg)
    capability = _text(data["capability"])
    fields = _record(
        data["context"],
        {
            "format_version",
            "run_seed",
            "operation_id",
            "input_digest",
            "invocation",
            "recovering",
            "recovery",
        },
    )
    _version(fields)
    counters = _record(
        fields["invocation"],
        {"format_version", "logical_attempt", "invocation_number", "recovery_count"},
    )
    _version(counters)
    invocation = InvocationIdentity(
        _counter(counters["logical_attempt"]),
        _counter(counters["invocation_number"]),
        _counter(counters["recovery_count"]),
    )
    recovering = fields["recovering"]
    if type(recovering) is not bool or (recovering and invocation.recovery_count == 0):
        msg = "invalid recovering flag"
        raise ValueError(msg)
    recovery = RecoveryDescriptor.from_fields(fields["recovery"])
    inputs = _record(data["inputs"], set(registration.signature.parameters))
    payloads = cast("dict[str, JSONValue]", inputs)
    encoded = canonical_json(payloads)
    input_digest = _text(fields["input_digest"])
    if input_digest != digest("duron.inputs.v1", payloads):
        msg = "input digest mismatch"
        raise ValueError(msg)
    for name, payload in payloads.items():
        parameter = registration.signature.parameters[name]
        schema = parameter.annotation
        if schema is inspect.Parameter.empty:
            schema = UnspecifiedType
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            if not isinstance(payload, list):
                raise TypeError
            for item in payload:
                _decode_value(codec, item, schema)
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            if not isinstance(payload, dict):
                raise TypeError
            for item in payload.values():
                _decode_value(codec, item, schema)
        elif handle_annotation(schema) is None:
            _decode_value(codec, payload, schema)
    context = EffectContextImpl(
        ExecutionIdentity(
            _text(fields["run_seed"]),
            _text(fields["operation_id"]),
            registration.identity.effect_id,
            input_digest,
        ),
        registration.identity,
        invocation,
        recovering,
        recovery=recovery,
    )
    _validate_references(registration, codec_id, payloads, context)
    return ExecutionRequest(
        _REQUEST_TOKEN,
        run_id=run_id,
        context=context,
        wire=wire,
        capability=capability,
        inputs=encoded,
        codec=codec,
        registration=registration,
        return_type=effect.return_type,
        handle_parameters=_handle_names(registration),
    )


def _validate_references(
    registration: Registration,
    codec_id: str,
    inputs: Mapping[str, JSONValue],
    context: EffectContextImpl,
) -> None:
    references: list[HandleReference] = []
    for name, parameter in registration.signature.parameters.items():
        handle = handle_annotation(parameter.annotation)
        if handle is None:
            continue
        item_type, direction = handle
        reference = input_reference(inputs[name])
        if (
            reference.run_seed != context.identity.run_seed
            or reference.owner_id != context.identity.operation_id
            or reference.direction != direction
            or reference.codec_id != codec_id
            or reference.schema_id != schema_id(item_type)
        ):
            msg = "input handle direction, schema, codec, or owner mismatch"
            raise InvalidExecutionRequestError(msg)
        references.append(reference)
    recovered = [handle.reference for handle in context.recovery.handles]
    if sorted(references, key=lambda ref: (ref.channel_id, ref.direction)) != sorted(
        recovered, key=lambda ref: (ref.channel_id, ref.direction)
    ):
        msg = "recovery descriptor does not exactly match declared input handles"
        raise InvalidExecutionRequestError(msg)


def _handle_names(registration: Registration) -> tuple[str, ...]:
    return tuple(
        name
        for name, parameter in registration.signature.parameters.items()
        if handle_annotation(parameter.annotation) is not None
    )


def _unique_object(pairs: object) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in cast("list[tuple[str, object]]", pairs):
        if key in result:
            msg = "duplicate request field"
            raise ValueError(msg)
        result[key] = value
    return result


def _decode_value(codec: Codec, payload: JSONValue, schema: object) -> None:
    value = codec.decode_json(payload, cast("TypeHint[object]", schema))
    if schema is not UnspecifiedType:
        validate_value(value, schema, allow_unspecified=True)


def _record(value: object, keys: set[str]) -> Mapping[str, object]:
    if not isinstance(value, dict) or set(cast("dict[object, object]", value)) != keys:
        msg = "invalid request object fields"
        raise ValueError(msg)
    return cast("dict[str, object]", value)


def _text(value: object) -> str:
    if not isinstance(value, str) or not value:
        msg = "request identity must be a nonempty UTF-8 string"
        raise ValueError(msg)
    _ = value.encode("utf-8")
    return value


def _version(fields: Mapping[str, object]) -> None:
    if type(fields["format_version"]) is not int or fields["format_version"] != 1:
        msg = "unsupported execution request format"
        raise ValueError(msg)


def _counter(value: object) -> int:
    if type(value) is not int:
        msg = "invocation counter must be an integer"
        raise ValueError(msg)
    return value
