"""Typed session-bound executor reporting without backend writer authority."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Literal, TypeAlias, cast
from typing_extensions import ParamSpec, Protocol, TypeVar, runtime_checkable

from duron._runtime.execution.errors import encode_error
from duron._runtime.execution.handle_data import MessageId, SignalId
from duron._runtime.execution.handles import Command
from duron._runtime.execution.posting import (
    PostTarget,
    Receipt,
    SemanticPost,
    validate_post_key,
)
from duron._runtime.execution.request import request_context
from duron.errors import InvalidHandleError, InvalidPostError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from duron._runtime.execution.effect import Effect
    from duron._runtime.execution.handle_data import (
        CommandReceiverParameter,
        CommandSenderParameter,
        HandleReference,
        SignalDelivery,
        StreamCursor,
        StreamEvent,
        StreamParameter,
    )
    from duron._runtime.execution.handles import (
        CommandReceiver,
        PostingCommandSender,
        PostingStream,
    )
    from duron._runtime.execution.models import EffectContextImpl, InvocationIdentity
    from duron._runtime.execution.request import EffectEnvelope, ExecutionRequest
    from duron.types import JSONValue, TypeHint

_P = ParamSpec("_P")
_R = TypeVar("_R")
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class ExecutionRunning:
    invocation: InvocationIdentity | None


@dataclass(frozen=True, slots=True)
class ExecutionSucceeded(Generic[_R]):
    value: _R


@dataclass(frozen=True, slots=True)
class ExecutionFailed:
    error: Exception


@dataclass(frozen=True, slots=True)
class ExecutionCancelled:
    pass


ExecutionStatus: TypeAlias = (
    ExecutionRunning | ExecutionSucceeded[_R] | ExecutionFailed | ExecutionCancelled
)


class ExecutionOwner(Protocol):
    """Internal run-owner boundary shared with transport adapters."""

    def decode_execution(
        self, wire: EffectEnvelope, *, effect: Effect[_P, _R]
    ) -> ExecutionRequest[_R]: ...
    def validate_execution(self, request: ExecutionRequest[_R]) -> None: ...
    def execution_context(self, request: ExecutionRequest[_R]) -> EffectContextImpl: ...
    async def rebind_execution(
        self, request: ExecutionRequest[_R]
    ) -> ExecutionRequest[_R]: ...
    async def report_execution(
        self, request: ExecutionRequest[_R], post: SemanticPost
    ) -> Receipt: ...
    async def report_stream(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        post: SemanticPost,
    ) -> Receipt: ...
    async def read_stream(
        self,
        request: ExecutionRequest[_R],
        parameter: StreamParameter[_T],
        *,
        after: StreamCursor | None,
        limit: int,
    ) -> Sequence[StreamEvent[_T]]: ...
    async def report_signal(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        post: SemanticPost,
    ) -> Receipt: ...
    async def receive_signal(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        correlation_id: str,
    ) -> SignalDelivery: ...
    def control_sender(
        self, signal: SignalId, *, item_type: TypeHint[_T]
    ) -> PostingCommandSender[_T]: ...
    async def execution_status(
        self, request: ExecutionRequest[_R]
    ) -> ExecutionStatus[_R]: ...


class EffectExecutor(Protocol):
    """Select and dispatch an effect invocation to an execution environment."""

    async def dispatch(self, request: EffectEnvelope, *, host: EffectHost) -> None: ...


@runtime_checkable
class RebindingExecutor(Protocol):
    """Optional recovery hook that authenticates a detached request."""

    async def authorize_rebind(self, request: EffectEnvelope, /) -> None: ...


class EffectHost:
    """Run-owner API for effect execution and durable reporting."""

    def __init__(self, owner: ExecutionOwner) -> None:
        self._owner = owner

    def control_sender(
        self, signal: SignalId, *, item_type: TypeHint[_T]
    ) -> PostingCommandSender[_T]:
        return self._owner.control_sender(signal, item_type=item_type)

    def decode(
        self, wire: EffectEnvelope, *, effect: Effect[_P, _R]
    ) -> ExecutionRequest[_R]:
        return self._owner.decode_execution(wire, effect=effect)

    def bind(self, request: ExecutionRequest[_R]) -> EffectReporter[_R]:
        self._owner.validate_execution(request)
        return EffectReporter(self._owner, request)

    async def execute(self, wire: EffectEnvelope, *, effect: Effect[_P, _R]) -> Receipt:
        """Invoke a registered effect and report its durable outcome.

        Returns:
            The committed report receipt.

        """
        request = self.decode(wire, effect=effect)
        reporter = self.bind(request)
        context = self._owner.execution_context(request)
        key = f"executor:{context.invocation_number}:outcome"
        try:
            value = await request._invoke(context)  # pyright: ignore[reportPrivateUsage]  # ruff: ignore[private-member-access]
        except Exception as error:  # ruff: ignore[blind-except]  # effect boundary
            return await reporter.fail(error, idempotency_key=key)
        return await reporter.complete(value, idempotency_key=key)

    async def rebind(self, request: ExecutionRequest[_R]) -> EffectReporter[_R]:
        rebound = await self._owner.rebind_execution(request)
        return self.bind(rebound)


class EffectReporter(Generic[_R]):
    """One bound reporter retaining the registered final-result type."""

    def __init__(self, owner: ExecutionOwner, request: ExecutionRequest[_R]) -> None:
        self._owner = owner
        self._request = request

    def stream(self, parameter: StreamParameter[_T]) -> PostingStream[_T]:
        reference = self._request.stream_reference(parameter)
        return self._request.bind_stream(
            parameter, _StreamReporter(self._owner, self._request, reference)
        )

    def signal_sender(
        self, parameter: CommandSenderParameter[_T]
    ) -> PostingCommandSender[_T]:
        reference = self._request.signal_sender_reference(parameter)
        return self._request.bind_signal_sender(
            parameter, _SignalSenderReporter(self._owner, self._request, reference)
        )

    def signal_receiver(
        self, parameter: CommandReceiverParameter[_T]
    ) -> CommandReceiver[_T]:
        reference = self._request.signal_receiver_reference(parameter)
        return self._request.bind_signal_receiver(
            parameter,
            _SignalReceiverReporter(self._owner, self._request, reference, parameter),
        )

    async def read_events(
        self,
        parameter: StreamParameter[_T],
        *,
        after: StreamCursor | None = None,
        limit: int = 1024,
    ) -> Sequence[StreamEvent[_T]]:
        return await self._owner.read_stream(
            self._request, parameter, after=after, limit=limit
        )

    async def complete(self, value: _R, *, idempotency_key: str) -> Receipt:
        validate_post_key(idempotency_key)
        payload = self._request.encode_result(value)
        return await self._report(idempotency_key, "execution.complete", payload)

    async def fail(self, error: Exception, *, idempotency_key: str) -> Receipt:
        validate_post_key(idempotency_key)
        if not isinstance(cast("object", error), Exception):
            msg = "fail requires a Python Exception; use cancel for cancellation"
            raise InvalidPostError(msg)
        return await self._report(
            idempotency_key, "execution.fail", cast("JSONValue", encode_error(error))
        )

    async def cancel(self, *, idempotency_key: str) -> Receipt:
        return await self._report(idempotency_key, "execution.cancel", None)

    async def status(self) -> ExecutionStatus[_R]:
        return await self._owner.execution_status(self._request)

    async def _report(
        self,
        key: str,
        action: Literal["execution.complete", "execution.fail", "execution.cancel"],
        payload: JSONValue,
    ) -> Receipt:
        return await self._owner.report_execution(
            self._request,
            SemanticPost.create(
                key,
                action,
                PostTarget(
                    "execution", request_context(self._request).identity.operation_id
                ),
                payload,
            ),
        )


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
class _SignalSenderReporter(Generic[_R]):
    owner: ExecutionOwner
    request: ExecutionRequest[_R]
    reference: HandleReference

    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt:
        return await self.owner.report_signal(
            self.request,
            self.reference,
            SemanticPost.create(
                idempotency_key,
                "signal.send",
                PostTarget("signal", self.reference.channel_id),
                payload,
            ),
        )


@dataclass(frozen=True, slots=True)
class _SignalReceiverReporter(Generic[_R, _T]):
    owner: ExecutionOwner
    request: ExecutionRequest[_R]
    reference: HandleReference
    parameter: CommandReceiverParameter[_T]

    def rebind(self, message: Command[_T]) -> Command[_T]:
        if message.id.signal != SignalId(
            self.reference.run_seed, self.reference.channel_id
        ):
            msg = "message belongs to another signal"
            raise InvalidHandleError(msg)
        return Command(message.id, message.value, self)

    async def receive(self, correlation_id: str) -> Command[_T]:
        delivery = await self.owner.receive_signal(
            self.request, self.reference, correlation_id
        )
        message_id = MessageId(
            SignalId(self.reference.run_seed, self.reference.channel_id),
            delivery.message_id,
        )
        return Command(
            message_id,
            self.request.decode_signal_value(self.parameter, delivery.payload),
            self,
        )

    async def ack(self, message_id: MessageId) -> Receipt:
        if message_id.signal != SignalId(
            self.reference.run_seed, self.reference.channel_id
        ):
            msg = "acknowledgement message belongs to another signal"
            raise InvalidPostError(msg)
        return await self.owner.report_signal(
            self.request,
            self.reference,
            SemanticPost.create(
                f"signal:ack:{message_id.value}",
                "signal.ack",
                PostTarget("signal", self.reference.channel_id),
                message_id.value,
            ),
        )
