from __future__ import annotations

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]
import asyncio
import contextlib
import copy
import time
from dataclasses import replace
from typing import TYPE_CHECKING
from typing_extensions import TypeVar

from duron._execution.errors import decode_error, encode_error
from duron._execution.invocation import (
    ApplicationFailure,
    ExecutionRequest,
    HandleReporter,
    Invocation,
    local_request,
)
from duron._execution.models import (
    EffectContextImpl,
    ExecutionIdentity,
    HandleReference,
    direction_claims_channel,
)
from duron._execution.posting import Receipt, event_content, event_id
from duron._execution.registration import event_arguments
from duron._execution.validation import validate_value
from duron._hints import UnspecifiedType
from duron._history.encoding import decode_record, random_id
from duron._runtime.channels import OwnedSignalState
from duron._runtime.state import Phase
from duron.errors import (
    ChannelClosedError,
    ExecutionCompletedError,
    InvalidExecutionRequestError,
    InvalidHandleError,
    InvalidPostError,
    PortClosedError,
    PostConflictError,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from duron._execution.invocation import EffectRegistry
    from duron._execution.models import EffectOperation
    from duron._hints import JSONValue
    from duron._history.entries import (
        Entry,
        PromiseCompleteEntry,
        StreamCompleteEntry,
        StreamEmitEntry,
    )
    from duron._runtime.append import AppendWriter
    from duron._runtime.state import MachineState, Operation
    from duron.codec import Codec
    from duron.storage._protocol import EventStore


_R = TypeVar("_R")


def _effect_now_us() -> int:
    return time.time_ns() // 1_000


class ExecutionSession:
    """Owner authority and effect reporting for one run."""

    def __init__(
        self,
        state: MachineState,
        registry: EffectRegistry,
        codec: Codec,
        backend: EventStore,
        writer: AppendWriter,
        check_ingress: Callable[[], None],
        wait_for_append: Callable[[int], Awaitable[int]],
    ) -> None:
        self._state = state
        self._registry = registry
        self._codec = codec
        self._backend = backend
        self._writer = writer
        self._check_ingress = check_ingress
        self._wait_for_append = wait_for_append

    def _validate_execution(self, request: ExecutionRequest[_R]) -> None:
        if (
            request.run_id != self._state.run_id
            or request._context.identity.run_seed != self._state.seed
        ):
            msg = "execution request belongs to another run"
            raise InvalidExecutionRequestError(msg)
        registration = self._registry.registration(
            request._context.identity.effect_name
        )
        if registration is not request._registration:
            msg = "execution request does not match the configured registration"
            raise InvalidExecutionRequestError(msg)

    def execution_context(self, request: ExecutionRequest[_R]) -> EffectContextImpl:
        self._validate_execution(request)
        return replace(request._context, handles=HandleReporter(self, request))

    async def _operation_for_request(
        self, request: ExecutionRequest[_R]
    ) -> Operation | None:
        self._validate_execution(request)
        self._check_ingress()
        id_ = request._context.identity.operation_id
        operation = self._state.operations.get(id_)
        call = operation.committed if operation is not None else None
        if call is None:
            existing = await self._backend.read_event(self._state.run_id, id_)
            if existing is not None and existing[0] <= self._state.frontier:
                entry = decode_record(existing[1], existing[0])
                if entry["type"] == "promise.create" and entry["source"] == "task":
                    call = entry
        if call is None:
            msg = "execution does not exist in this run"
            raise InvalidExecutionRequestError(msg)
        args, kwargs = event_arguments(request._registration, request.inputs)
        if (
            call.get("ref") != {"name": request._context.identity.effect_name}
            or call.get("args") != args
            or call.get("kwargs") != kwargs
        ):
            msg = "request identity or inputs differ from the committed execution"
            raise InvalidExecutionRequestError(msg)
        return operation

    async def _receipt(self, entry: Entry) -> Receipt | None:
        existing = await self._backend.read_event(self._state.run_id, entry["id"])
        if existing is None or existing[0] > self._state.frontier:
            return None
        offset, record = existing
        recorded = decode_record(record, offset)
        if event_content(recorded) != event_content(entry):
            msg = "event ID was already committed with different content"
            raise PostConflictError(msg)
        return Receipt(offset, duplicate=True)

    async def report_execution(
        self, request: ExecutionRequest[_R], entry: PromiseCompleteEntry
    ) -> Receipt:
        entry = copy.deepcopy(entry)
        async with self._state.channel_gate:
            operation = await self._operation_for_request(request)
            id_ = request._context.identity.operation_id
            if entry["promise_id"] != id_:
                msg = "execution request does not own the outcome"
                raise InvalidExecutionRequestError(msg)
            if receipt := await self._receipt(entry):
                return receipt
            if operation is None:
                msg = "execution has already reported its outcome"
                raise ExecutionCompletedError(msg)
            effect = operation.effect
            if effect is None:
                msg = "request does not identify an effect"
                raise InvalidExecutionRequestError(msg)
            if "result" in entry:
                result = self._codec.decode_json(entry["result"], effect.return_type)
                if effect.return_type is not UnspecifiedType:
                    validate_value(result, effect.return_type, allow_unspecified=True)
            elif "error" in entry and not entry["error"].get("cancelled"):
                error = decode_error(entry["error"])
                if not isinstance(error, Exception):
                    msg = "failure reporting cannot submit cancellation"
                    raise InvalidPostError(msg)
            operation.reporting = True
            offsets = await self._writer.append(
                self._finalize_channels(effect.operation, entry)
            )
            return Receipt(offsets[-1], duplicate=False)

    async def report_stream(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        entry: StreamEmitEntry,
    ) -> Receipt:
        entry = copy.deepcopy(entry)
        async with self._state.channel_gate:
            operation = await self._operation_for_request(request)
            id_ = request._context.identity.operation_id
            state = self._state.channels.get(reference.channel_id)
            if (
                state is None
                or state.owner_id != id_
                or reference != replace(state.reference, owner_id=id_)
                or reference.direction != "stream.sender"
                or reference not in request.handle_references
                or entry["stream_id"] != reference.channel_id
            ):
                msg = "stream reference does not belong to this execution"
                raise InvalidHandleError(msg)
            if receipt := await self._receipt(entry):
                return receipt
            if operation is None:
                msg = "execution has already reported its outcome"
                raise ExecutionCompletedError(msg)
            if state.closed:
                msg = "stream EOF is permanent"
                raise PortClosedError(msg)
            validate_value(
                self._codec.decode_json(entry["value"], state.item_type),
                state.item_type,
            )
            offsets = await self._writer.append([entry])
            return Receipt(offsets[-1], duplicate=False)

    def _signal_for_reference(
        self, request: ExecutionRequest[_R], reference: HandleReference
    ) -> OwnedSignalState:
        state = self._state.channels.get(reference.channel_id)
        if (
            not isinstance(state, OwnedSignalState)
            or reference.direction != "signal.receiver"
            or reference
            != replace(state.reference, owner_id=request._context.identity.operation_id)
            or reference not in request.handle_references
            or state.owner_id != reference.owner_id
        ):
            msg = "signal reference is not declared with this direction and owner"
            raise InvalidHandleError(msg)
        return state

    async def send_control(
        self, reference: HandleReference, payload: JSONValue, key: str
    ) -> list[JSONValue]:
        entry: StreamEmitEntry = {
            "id": event_id(reference.run_seed, reference.channel_id, key),
            "ts": _effect_now_us(),
            "type": "stream.emit",
            "source": "effect",
            "stream_id": reference.channel_id,
            "value": copy.deepcopy(payload),
        }
        async with self._state.channel_gate:
            self._check_ingress()
            state = self._state.channels.get(reference.channel_id)
            if (
                not isinstance(state, OwnedSignalState)
                or not state.committed
                or reference != replace(state.reference, direction="signal.sender")
            ):
                msg = "control sender belongs to another signal scope"
                raise InvalidHandleError(msg)
            if receipt := await self._receipt(entry):
                return [receipt.offset, receipt.duplicate]
            if state.closed:
                msg = "signal channel is closed"
                raise ChannelClosedError(msg)
            validate_value(
                self._codec.decode_json(entry["value"], state.item_type),
                state.item_type,
            )
            offsets = await self._writer.append([entry])
            return [offsets[-1], False]

    async def _next_signal(
        self, signal: OwnedSignalState
    ) -> tuple[int, JSONValue] | None:
        cursor = signal.read_offset
        while page := await self._backend.read(
            self._state.run_id,
            after=cursor,
            limit=128,
            stream_id=signal.reference.channel_id,
        ):
            for offset, record in page:
                if offset > self._state.frontier:
                    return None
                entry = decode_record(record, offset)
                cursor = offset
                if (
                    entry["type"] == "stream.emit"
                    and entry["stream_id"] == signal.reference.channel_id
                ):
                    return offset, entry["value"]
        return None

    async def receive_signal(
        self, request: ExecutionRequest[_R], reference: HandleReference
    ) -> JSONValue:
        while True:
            version = self._state.append_version
            async with self._state.channel_gate:
                operation = await self._operation_for_request(request)
                state = self._signal_for_reference(request, reference)
                if operation is None:
                    msg = "execution has already reported its outcome"
                    raise ExecutionCompletedError(msg)
                delivery = await self._next_signal(state)
                if delivery is not None:
                    state.read_offset, payload = delivery
                    return payload
                if state.closed:
                    msg = "signal channel is closed"
                    raise ChannelClosedError(msg)
            await self._wait_for_append(version)

    def _finalize_channels(
        self, operation: EffectOperation, entry: PromiseCompleteEntry
    ) -> Sequence[Entry]:
        closures: list[Entry] = []
        for reference in operation.handles:
            if (
                direction_claims_channel(reference.direction)
                and not self._state.channels[reference.channel_id].closed
            ):
                closure: StreamCompleteEntry = {
                    "id": random_id(),
                    "ts": entry["ts"],
                    "source": "effect",
                    "type": "stream.complete",
                    "stream_id": reference.channel_id,
                }
                closures.append(closure)
        return [*closures, entry]

    async def execute(self, pending: Operation) -> None:
        op = pending.effect
        assert op is not None
        waiter = pending.waiter
        id_ = waiter.id
        operation = op.operation
        identity = ExecutionIdentity(operation.seed, id_, operation.name)
        pending.started = True

        def bind_invocation() -> Invocation:
            context = EffectContextImpl(identity)
            request = local_request(
                run_id=self._state.run_id,
                context=context,
                registration=self._registry.registration(identity.effect_name),
                codec=self._codec,
                inputs=operation.inputs,
                return_type=op.return_type,
            )
            return Invocation(self, request)

        entry: PromiseCompleteEntry = {
            "id": event_id(operation.seed, id_, "executor:outcome"),
            "ts": _effect_now_us(),
            "source": "effect",
            "type": "promise.complete",
            "promise_id": id_,
        }
        try:
            if waiter.cancelled():
                raise asyncio.CancelledError  # ruff: ignore[raise-within-try]
            invocation = bind_invocation()
            request = invocation._request
            _ = await self._operation_for_request(request)
            adapter = self._registry.executor_for(identity.effect_name)
            try:
                value = await adapter.execute(invocation)
            except ApplicationFailure as failure:
                entry["error"] = encode_error(failure.error)
            else:
                try:
                    entry["result"] = request.encode_result(value)
                except (TypeError, ValueError) as error:
                    entry["error"] = encode_error(error)
            entry["ts"] = _effect_now_us()
            await self.report_execution(request, entry)
        except asyncio.CancelledError:
            if self._state.phase is Phase.CLOSED:
                return
            if id_ in self._state.operations:
                invocation = bind_invocation()
                entry.pop("result", None)
                entry["id"] = event_id(operation.seed, id_, "executor:cancel")
                entry["ts"] = _effect_now_us()
                entry["error"] = encode_error(asyncio.CancelledError())
                with contextlib.suppress(ExecutionCompletedError):
                    await self.report_execution(invocation._request, entry)
