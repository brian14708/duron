"""Internal invocation state bound to a run owner and effect registration."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import inspect
import json
from abc import abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Generic, cast, overload
from typing_extensions import Protocol, TypeVar, override

from duron._api.effect import Effect
from duron._api.ports import EffectEmitter, Input
from duron._execution.handles import PostingStream, SignalReceiver, handle_annotation
from duron._execution.models import (
    EffectContextImpl,
    EffectId,
    EffectOperation,
    canonical_json,
)
from duron._execution.posting import PostTarget, Receipt, SemanticPost
from duron._execution.registration import (
    Registration,
    decode_inputs,
    input_reference,
    input_references,
    registration_matches,
    schema_id,
    snapshot_inputs,
)
from duron._execution.validation import validate_value
from duron._hints import UnspecifiedType
from duron.errors import (
    DuronError,
    InvalidExecutionRequestError,
    InvalidHandleError,
    RemoteEffectError,
    WorkflowDefinitionError,
)
from duron.storage._protocol import validate_run_id

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron._api.ports import EffectReceiver, Output, _Port
    from duron._execution.handles import SignalReceiving, StreamPosting
    from duron._execution.models import (
        HandleDirection,
        HandleReference,
        InputPortParameter,
        OutputPortParameter,
    )
    from duron._execution.registration import InputSnapshot
    from duron._hints import JSONValue, TypeHint
    from duron.codec import Codec

_T = TypeVar("_T")
_R = TypeVar("_R")


@dataclass(frozen=True, slots=True, eq=False)
class _EffectOwner:
    handles: dict[str, Any]

    async def _emit(
        self, port: Output[_T], value: _T, /, *, idempotency_key: str | None = None
    ) -> None:
        await self.handles[f"_duron_port_{port.name}"].send(
            value, idempotency_key=idempotency_key
        )


class ExecutionOwner(Protocol):
    """Internal run-owner boundary shared with transport adapters."""

    def execution_context(self, request: ExecutionRequest[_R]) -> EffectContextImpl: ...
    async def report_stream(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        post: SemanticPost,
    ) -> Receipt: ...
    async def receive_signal(
        self, request: ExecutionRequest[_R], reference: HandleReference
    ) -> JSONValue: ...


class Executor(Protocol):
    """Execute one attempt and return its application result."""

    @abstractmethod
    async def execute(self, invocation: Invocation, /) -> object:
        """Await completion; raise ApplicationFailure for a final application error."""
        ...


class ApplicationFailure(Exception):  # ruff: ignore[error-suffix-on-exception-name]
    """An executor's final application failure, recorded as the underlying error."""

    def __init__(self, error: Exception) -> None:
        if not isinstance(cast("object", error), Exception):
            msg = "ApplicationFailure requires an Exception"
            raise TypeError(msg)
        super().__init__(str(error))
        self.error = error


class AsyncExecutor(Executor):
    """Execute the registered async Python effect body in the run owner's loop."""

    @override
    async def execute(self, invocation: Invocation, /) -> object:
        return await invocation._request._invoke(  # pyright: ignore[reportPrivateUsage]
            invocation._owner.execution_context(invocation._request)  # pyright: ignore[reportPrivateUsage]
        )


class _HandleReporter(Generic[_R]):
    """Internal directional handle resolver for a bound invocation."""

    def __init__(self, owner: ExecutionOwner, request: ExecutionRequest[_R]) -> None:
        self._owner = owner
        self._request = request

    def stream(self, parameter: OutputPortParameter[_T]) -> PostingStream[_T]:
        reference = self._request.stream_reference(parameter)
        return self._request.bind_stream(
            parameter, _StreamReporter(self._owner, self._request, reference)
        )

    def signal_receiver(self, parameter: InputPortParameter[_T]) -> SignalReceiver[_T]:
        reference = self._request.signal_receiver_reference(parameter)
        return self._request.bind_signal_receiver(
            parameter,
            _SignalReceiverReporter(self._owner, self._request, reference, parameter),
        )


def execution_handles(
    owner: ExecutionOwner, request: ExecutionRequest[_R]
) -> _HandleReporter[_R]:
    return _HandleReporter(owner, request)


class Invocation:
    """Session-bound arguments, immutable metadata, and typed effect ports."""

    __slots__ = ("_context", "_owner", "_request")

    def __init__(
        self, owner: ExecutionOwner, request: ExecutionRequest[object]
    ) -> None:
        self._owner = owner
        self._request = request
        self._context: EffectContextImpl | None = None

    @property
    def arguments(self) -> Mapping[str, object]:
        """Fresh decoded application arguments, including parameter defaults."""
        return self._request.arguments

    @property
    def metadata(self) -> ExecutionMetadata:
        """Immutable identity and recovery information for this attempt."""
        return self._request.metadata

    @overload
    def port(self, declaration: Input[_T], /) -> EffectReceiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> EffectEmitter[_T]: ...
    def port(self, declaration: _Port, /) -> EffectReceiver[Any] | EffectEmitter[Any]:
        if self._context is None:
            self._context, _ = self._request.bind_ports(
                self._owner.execution_context(self._request)
            )
        return self._context.port(declaration)


@dataclass(frozen=True, slots=True)
class _StreamReporter(Generic[_R]):
    owner: ExecutionOwner
    request: ExecutionRequest[_R]
    reference: HandleReference

    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt:
        return await self.owner.report_stream(
            self.request,
            self.reference,
            SemanticPost.create(
                idempotency_key,
                "stream.send",
                PostTarget("stream", self.reference.channel_id),
                payload,
            ),
        )

    async def close(self, *, idempotency_key: str) -> Receipt:
        return await self.owner.report_stream(
            self.request,
            self.reference,
            SemanticPost.create(
                idempotency_key,
                "stream.close",
                PostTarget("stream", self.reference.channel_id),
                None,
            ),
        )


@dataclass(frozen=True, slots=True)
class _SignalReceiverReporter(Generic[_R, _T]):
    owner: ExecutionOwner
    request: ExecutionRequest[_R]
    reference: HandleReference
    parameter: InputPortParameter[_T]

    async def receive(self) -> _T:
        payload = await self.owner.receive_signal(self.request, self.reference)
        return self.request.decode_signal_value(self.parameter, payload)


@dataclass(frozen=True, slots=True)
class ExecutionMetadata:
    """Invocation identity; run_seed and operation_id survive recovery."""

    run_id: str
    run_seed: str
    operation_id: str
    effect_name: str
    effect_version: str
    invocation_number: int
    recovery_count: int
    recovering: bool


@dataclass(frozen=True, slots=True)
class ExecutionRequest(Generic[_R]):
    """Internal invocation state retaining its registered result codec and type."""

    run_id: str
    _context: EffectContextImpl
    _capability: str = field(repr=False)
    _inputs: bytes = field(repr=False)
    _codec: Codec = field(repr=False)
    _registration: Registration = field(repr=False, compare=False)
    _return_type: TypeHint[_R]
    _handles: dict[str, HandleReference] = field(init=False, repr=False, compare=False)
    handle_references: tuple[HandleReference, ...] = field(init=False)
    _references: dict[str, tuple[HandleDirection, object, HandleReference]] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        names = (
            name
            for name, parameter in self._registration.signature.parameters.items()
            if handle_annotation(parameter.annotation) is not None
        )
        references = input_references(self._registration, self._inputs)
        object.__setattr__(self, "_handles", dict(zip(names, references, strict=True)))
        object.__setattr__(self, "handle_references", references)

    @property
    def metadata(self) -> ExecutionMetadata:
        """Read-only identity and recovery information for this invocation."""
        context = self._context
        return ExecutionMetadata(
            self.run_id,
            context.identity.run_seed,
            context.identity.operation_id,
            context.effect.name,
            context.effect.version,
            context.invocation_number,
            context.recovery_count,
            context.recovering,
        )

    @property
    def arguments(self) -> Mapping[str, object]:
        """Fresh decoded application arguments, excluding runtime port framing."""
        registration = self._registration
        payload = cast("dict[str, JSONValue]", json.loads(self._inputs))
        inputs = {
            name: value
            for name, value in payload.items()
            if not name.startswith("_duron_")
        }
        bound = decode_inputs(registration, self._codec, canonical_json(inputs))
        return bound.arguments

    @property
    def capability(self) -> str:
        return self._capability

    @property
    def inputs(self) -> bytes:
        return self._inputs

    def encode_result(self, value: _R) -> JSONValue:
        if self._return_type is not UnspecifiedType:
            validate_value(value, self._return_type, allow_unspecified=True)
        encoded = self._codec.encode_json(value, self._return_type)
        # Prove that the committed value can be decoded during replay.
        self.decode_result(encoded)
        return encoded

    async def _invoke(self, context: EffectContextImpl) -> _R:
        """Decode inputs and invoke the registered effect with a bound context.

        The run owner supplies the bound context for channel resolution.

        Returns:
            The effect body result, before it is reported as durable.

        Raises:
            ApplicationFailure: If the effect raises an application error.
            DuronError: If execution encounters a runtime failure.
            InvalidExecutionRequestError: If the context belongs to another invocation.
            WorkflowDefinitionError: If the effect does not return an awaitable.

        """
        if (
            context.identity != self._context.identity
            or context.invocation != self._context.invocation
        ):
            msg = "effect context does not match this execution request"
            raise InvalidExecutionRequestError(msg)
        context, bound = self.bind_ports(context)
        result = self._registration.fn(context, *bound.args, **bound.kwargs)
        if not inspect.isawaitable(result):
            msg = "async effect did not return an awaitable"
            raise WorkflowDefinitionError(msg)
        try:
            return cast("_R", await result)
        except DuronError as error:
            if not isinstance(error, RemoteEffectError) and not getattr(
                type(error), "__duron_round_trip__", False
            ):
                raise
            raise ApplicationFailure(error) from error
        except Exception as error:
            raise ApplicationFailure(error) from error

    def bind_ports(
        self, context: EffectContextImpl
    ) -> tuple[EffectContextImpl, inspect.BoundArguments]:
        bound = decode_inputs(
            self._registration, self._codec, self.inputs, context=context
        )
        if not self._registration.ports:
            return context, bound
        handles = {
            name: value
            for name, value in bound.arguments.items()
            if name.startswith("_duron_")
        }
        owner = _EffectOwner(handles)
        bindings = {
            port: handles[f"_duron_port_{port.name}"]
            if isinstance(port, Input)
            else EffectEmitter(owner, port)
            for port in self._registration.ports
        }
        parameters = [
            parameter
            for parameter in self._registration.signature.parameters.values()
            if not parameter.name.startswith("_duron_")
        ]
        arguments = {
            name: value
            for name, value in bound.arguments.items()
            if not name.startswith("_duron_")
        }
        return replace(context, _port_bindings=bindings), inspect.BoundArguments(
            self._registration.signature.replace(parameters=parameters),
            OrderedDict(arguments),
        )

    def decode_result(self, payload: JSONValue) -> _R:
        decoded = self._codec.decode_json(payload, self._return_type)
        if self._return_type is not UnspecifiedType:
            validate_value(decoded, self._return_type, allow_unspecified=True)
        return decoded

    def stream_reference(self, parameter: OutputPortParameter[_T]) -> HandleReference:
        return self._handle_reference(
            parameter.name, parameter.item_type, "stream.sender"
        )

    def signal_receiver_reference(
        self, parameter: InputPortParameter[_T]
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
        reference = self._handles.get(name)
        if reference is None:
            msg = "stream parameter is not declared by this execution"
            raise InvalidExecutionRequestError(msg)
        if reference.direction != direction or reference.schema_id != schema_id(
            item_type
        ):
            msg = "stream lookup does not match the validated execution parameter"
            raise InvalidExecutionRequestError(msg)
        self._references[name] = (direction, item_type, reference)
        return reference

    def bind_signal_receiver(
        self, parameter: InputPortParameter[_T], receiving: SignalReceiving[_T]
    ) -> SignalReceiver[_T]:
        reference = self.signal_receiver_reference(parameter)
        return SignalReceiver(
            reference, item_type=parameter.item_type, receiving=receiving
        )

    def decode_signal_value(
        self, parameter: InputPortParameter[_T], payload: JSONValue
    ) -> _T:
        _ = self.signal_receiver_reference(parameter)
        try:
            value = self._codec.decode_json(payload, parameter.item_type)
            validate_value(value, parameter.item_type)
        except Exception as error:
            msg = "port value cannot be decoded"
            raise WorkflowDefinitionError(msg) from error
        return value

    def bind_stream(
        self, parameter: OutputPortParameter[_T], posting: StreamPosting
    ) -> PostingStream[_T]:
        _ = self.stream_reference(parameter)
        return PostingStream(posting, item_type=parameter.item_type, codec=self._codec)


def local_request(
    *,
    run_id: str,
    context: EffectContextImpl,
    registration: Registration,
    codec: Codec,
    inputs: InputSnapshot,
    capability: str,
    return_type: TypeHint[_R],
) -> ExecutionRequest[_R]:
    _ = validate_run_id(run_id)
    if (
        context.effect != registration.identity
        or context.identity.effect_id != registration.identity.effect_id
        or context.identity.input_digest != inputs.input_digest
    ):
        msg = "invocation metadata does not match the registration or inputs"
        raise InvalidExecutionRequestError(msg)
    _validate_references(
        registration, cast("dict[str, JSONValue]", json.loads(inputs.encoded)), context
    )
    return ExecutionRequest(
        run_id=run_id,
        _context=context,
        _capability=capability,
        _inputs=inputs.encoded,
        _codec=codec,
        _registration=registration,
        _return_type=return_type,
    )


def _validate_references(
    registration: Registration,
    inputs: Mapping[str, JSONValue],
    context: EffectContextImpl,
) -> None:
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
            or reference.schema_id != schema_id(item_type)
        ):
            msg = "input handle direction, schema, or owner mismatch"
            raise InvalidExecutionRequestError(msg)


class EffectRegistry:
    """Run-local registrations and per-effect dispatch routing."""

    def __init__(self) -> None:
        self._default_executor = AsyncExecutor()
        self._registrations: dict[EffectId, Registration] = {}
        self._executors: dict[EffectId, Executor] = {}

    def register(self, fn: Effect[..., Any]) -> EffectId:
        """Register an effect definition and its selected environment.

        Returns:
            The effect's stable identifier.

        Raises:
            WorkflowDefinitionError: If the definition conflicts with
                a registered effect or the value is not an effect definition.

        """
        if not isinstance(cast("object", fn), Effect):
            msg = "executor registration requires an @duron.effect definition"
            raise WorkflowDefinitionError(msg)
        body = fn.fn
        signature = fn._wire_signature
        registration = Registration(fn.identity, body, signature, fn.raises, fn.ports)
        effect_id = registration.identity.effect_id
        old = self._registrations.get(effect_id)
        if old is not None and (
            old.fn is not body
            or old != registration
            or self.executor_for(effect_id)
            != (fn.executor if fn.executor is not None else self._default_executor)
        ):
            msg = "conflicting registration for effect name and version"
            raise WorkflowDefinitionError(msg)
        self._registrations[effect_id] = registration
        self._executors[effect_id] = (
            fn.executor if fn.executor is not None else self._default_executor
        )
        return effect_id

    def executor_for(self, effect_id: EffectId) -> Executor:
        return self._executors[effect_id]

    def registration(self, effect_id: EffectId) -> Registration:
        try:
            return self._registrations[effect_id]
        except KeyError as exc:
            msg = "effect is not registered in this executor"
            raise WorkflowDefinitionError(msg) from exc

    def prepare(
        self,
        effect: Effect[..., Any],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        *,
        codec: Codec,
        seed: str,
        owner_id: str | None = None,
    ) -> EffectOperation:
        effect_id = effect.identity.effect_id
        if effect_id not in self._registrations:
            _ = self.register(effect)
        registration = self.registration(effect_id)
        if not registration_matches(registration, effect) or self.executor_for(
            effect_id
        ) != (
            effect.executor if effect.executor is not None else self._default_executor
        ):
            msg = "effect does not match its executor registration"
            raise WorkflowDefinitionError(msg)
        snapshot = snapshot_inputs(registration, codec, args, kwargs, owner_id=owner_id)
        references = input_references(registration, snapshot.encoded)
        if len({
            (reference.channel_id, reference.direction) for reference in references
        }) != len(references):
            msg = "an effect cannot bind one stream through multiple parameters"
            raise InvalidHandleError(msg)
        if any(reference.run_seed != seed for reference in references):
            msg = "handle run identity differs from the effect call"
            raise InvalidHandleError(msg)

        return EffectOperation(
            registration.identity,
            seed,
            snapshot.input_digest,
            canonical_json(registration.descriptor(codec=codec)),
            registration.raises,
            snapshot.encoded,
            references,
        )
