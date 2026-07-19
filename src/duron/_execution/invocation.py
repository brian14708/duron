"""Internal invocation state bound to a run owner and effect registration."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import inspect
import json
import time
from abc import abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Generic, cast, overload
from typing_extensions import ParamSpec, Protocol, TypeVar, override

from duron._api.ports import EffectEmitter, Input, Output
from duron._execution.handles import (
    PostingChannel,
    SignalReceiver,
    StreamSender,
    handle_annotation,
)
from duron._execution.models import (
    EffectContextImpl,
    EffectOperation,
    HandleDirection,
    HandleReference,
    canonical_json,
)
from duron._execution.posting import Receipt, event_id
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
    ApplicationError,
    ChannelClosedError,
    DuronError,
    InvalidExecutionRequestError,
    InvalidHandleError,
    WorkflowDefinitionError,
)
from duron.storage._protocol import validate_id

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron._api.effect import Effect
    from duron._api.ports import EffectReceiver, _Port
    from duron._execution.models import InputPortParameter, OutputPortParameter
    from duron._hints import JSONValue, TypeHint
    from duron._history.entries import StreamEmitEntry
    from duron.codec import Codec

_T = TypeVar("_T")
_R = TypeVar("_R")
_P = ParamSpec("_P")


@dataclass(frozen=True, slots=True, eq=False)
class _EffectOwner:
    outputs: Mapping[str, StreamSender[object]]

    async def _emit(
        self, port: Output[_T], value: _T, /, *, idempotency_key: str | None = None
    ) -> None:
        await self.outputs[port.name].send(value, idempotency_key=idempotency_key)


class ExecutionOwner(Protocol):
    """Internal run-owner boundary shared with transport adapters."""

    def execution_context(self, request: ExecutionRequest[_R]) -> EffectContextImpl: ...
    async def report_stream(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        entry: StreamEmitEntry,
    ) -> Receipt: ...
    async def receive_signal(
        self, request: ExecutionRequest[_R], reference: HandleReference
    ) -> JSONValue: ...


class Executor(Protocol):
    """Execute an effect once and return its application result."""

    @abstractmethod
    async def execute(self, invocation: Invocation, /) -> object:
        """Await completion; raise ApplicationFailure for a final application error."""
        ...


class ApplicationFailure(Exception):  # ruff: ignore[error-suffix-on-exception-name]
    """An executor's final application failure, recorded as the underlying error."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


class AsyncExecutor(Executor):
    """Execute the registered async Python effect body in the run owner's loop."""

    @override
    async def execute(self, invocation: Invocation, /) -> object:
        return await invocation._request._invoke(
            invocation._owner.execution_context(invocation._request)
        )


class HandleReporter(Generic[_R]):
    """Internal directional handle resolver for a bound invocation."""

    def __init__(self, owner: ExecutionOwner, request: ExecutionRequest[_R]) -> None:
        self._owner = owner
        self._request = request

    def stream(self, parameter: OutputPortParameter[_T]) -> PostingChannel[_T]:
        reference = self._request.stream_reference(parameter)
        return PostingChannel(
            _StreamReporter(self._owner, self._request, reference),
            item_type=parameter.item_type,
            codec=self._request._codec,
        )

    def signal_receiver(self, parameter: InputPortParameter[_T]) -> SignalReceiver[_T]:
        reference = self._request.signal_receiver_reference(parameter)
        return SignalReceiver(
            reference,
            item_type=parameter.item_type,
            receiving=_SignalReceiverReporter(
                self._owner, self._request, reference, parameter
            ),
        )


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
        """Immutable identity of this effect execution."""
        return self._request.metadata

    @overload
    def port(self, declaration: Input[_T], /) -> EffectReceiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> EffectEmitter[_T]: ...
    def port(
        self, declaration: Input[_T] | Output[_T], /
    ) -> EffectReceiver[_T] | EffectEmitter[_T]:
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
            {
                "id": event_id(
                    self.reference.run_seed,
                    self.reference.owner_id,
                    idempotency_key,
                    self.reference.channel_id,
                    "stream.emit",
                ),
                "ts": time.time_ns() // 1_000,
                "source": "effect",
                "type": "stream.emit",
                "stream_id": self.reference.channel_id,
                "value": payload,
            },
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
    """Identity of a one-shot effect call."""

    run_id: str
    run_seed: str
    operation_id: str
    effect_name: str


@dataclass(frozen=True, slots=True)
class ExecutionRequest(Generic[_R]):
    """Internal invocation state retaining its registered result codec and type."""

    run_id: str
    _context: EffectContextImpl
    _inputs: bytes = field(repr=False)
    _codec: Codec = field(repr=False)
    _registration: Registration = field(repr=False, compare=False)
    _return_type: TypeHint[_R]
    _handles: dict[str, HandleReference] = field(init=False, repr=False, compare=False)
    handle_references: tuple[HandleReference, ...] = field(init=False)
    _references: dict[str, tuple[HandleDirection, object, HandleReference]] = field(
        default_factory=dict[str, tuple[HandleDirection, object, HandleReference]],
        init=False,
        repr=False,
        compare=False,
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
        """Read-only identity for this effect call."""
        context = self._context
        return ExecutionMetadata(
            self.run_id,
            context.identity.run_seed,
            context.identity.operation_id,
            context.identity.effect_name,
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
        if context.identity != self._context.identity:
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
            if not isinstance(error, (ApplicationError, ChannelClosedError)):
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
        owner = _EffectOwner({
            port.name: cast(
                "StreamSender[object]", bound.arguments[f"_duron_port_{port.name}"]
            )
            for port in self._registration.ports
            if isinstance(port, Output)
        })
        bindings: dict[_Port, EffectReceiver[object] | EffectEmitter[object]] = {
            port: cast(
                "SignalReceiver[object]", bound.arguments[f"_duron_port_{port.name}"]
            )
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


def local_request(
    *,
    run_id: str,
    context: EffectContextImpl,
    registration: Registration,
    codec: Codec,
    inputs: bytes,
    return_type: TypeHint[_R],
) -> ExecutionRequest[_R]:
    _ = validate_id(run_id, "run_id")
    if context.identity.effect_name != registration.name:
        msg = "invocation metadata does not match the registration or inputs"
        raise InvalidExecutionRequestError(msg)
    _validate_references(
        registration, cast("dict[str, JSONValue]", json.loads(inputs)), context
    )
    return ExecutionRequest(
        run_id=run_id,
        _context=context,
        _inputs=inputs,
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
        self._registrations: dict[str, Registration] = {}
        self._executors: dict[str, Executor] = {}

    def register(self, fn: Effect[_P, _T]) -> None:
        """Register an effect definition and its selected environment.

        Raises:
            WorkflowDefinitionError: If the definition conflicts with
                a registered effect.

        """
        body = fn.fn
        signature = fn._wire_signature
        registration = Registration(fn.name, body, signature, fn.ports)
        name = registration.name
        old = self._registrations.get(name)
        if old is not None and (
            old.fn is not body
            or old != registration
            or self.executor_for(name)
            != (fn.executor if fn.executor is not None else self._default_executor)
        ):
            msg = "conflicting registration for effect name"
            raise WorkflowDefinitionError(msg)
        self._registrations[name] = registration
        self._executors[name] = (
            fn.executor if fn.executor is not None else self._default_executor
        )

    def executor_for(self, name: str) -> Executor:
        return self._executors[name]

    def registration(self, name: str) -> Registration:
        try:
            return self._registrations[name]
        except KeyError as exc:
            msg = "effect is not registered in this executor"
            raise WorkflowDefinitionError(msg) from exc

    def prepare(
        self,
        effect: Effect[_P, _T],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        *,
        codec: Codec,
        seed: str,
        owner_id: str | None = None,
    ) -> EffectOperation:
        name = effect.name
        if name not in self._registrations:
            self.register(effect)
        registration = self.registration(name)
        if not registration_matches(registration, effect) or self.executor_for(
            name
        ) != (
            effect.executor if effect.executor is not None else self._default_executor
        ):
            msg = "effect does not match its executor registration"
            raise WorkflowDefinitionError(msg)
        snapshot = snapshot_inputs(registration, codec, args, kwargs, owner_id=owner_id)
        references = input_references(registration, snapshot)
        if len({
            (reference.channel_id, reference.direction) for reference in references
        }) != len(references):
            msg = "an effect cannot bind one stream through multiple parameters"
            raise InvalidHandleError(msg)
        if any(reference.run_seed != seed for reference in references):
            msg = "handle run identity differs from the effect call"
            raise InvalidHandleError(msg)

        return EffectOperation(registration.name, seed, snapshot, references)
