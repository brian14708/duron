from __future__ import annotations

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]
import asyncio
import contextlib
import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, cast
from typing_extensions import TypeVar

from duron._execution.errors import decode_error, encode_error
from duron._execution.invocation import (
    ApplicationFailure,
    ExecutionRequest,
    Invocation,
    execution_handles,
    local_request,
)
from duron._execution.models import (
    EffectContextImpl,
    EffectId,
    ExecutionIdentity,
    HandleReference,
    InvocationIdentity,
    canonical_json,
    direction_claims_channel,
)
from duron._execution.posting import (
    PostAuthority,
    PostJournal,
    PostScope,
    PostTarget,
    Receipt,
    SemanticPost,
)
from duron._execution.registration import InputSnapshot, input_references
from duron._execution.validation import validate_value
from duron._hints import UnspecifiedType
from duron._history.encoding import decode_record, random_id
from duron._history.entries import POST_ACTION_DIRECTIONS, CorruptLogError
from duron._runtime.channels import OwnedSignalState
from duron.errors import (
    ChannelClosedError,
    ExecutionCompletedError,
    InvalidExecutionRequestError,
    InvalidHandleError,
    InvalidPostError,
    PortClosedError,
    RunUnavailableError,
    StaleInvocationError,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from duron._execution.models import EffectOperation
    from duron._hints import JSONValue, TypeHint
    from duron._history.entries import (
        EffectAttemptStartEntry,
        EffectCallData,
        Entry,
        ErrorInfo,
        PromiseCompleteEntry,
        StreamCompleteEntry,
    )
    from duron._runtime.loop import OpFuture
    from duron._runtime.machine import RunMachine
    from duron._runtime.ops import EffectCall


_R = TypeVar("_R")


@dataclass(frozen=True, slots=True)
class _ExecutionRecord:
    call: EffectCallData
    invocation: InvocationIdentity | None
    completed: PromiseCompleteEntry | None


def _effect_now_us() -> int:
    return time.time_ns() // 1_000


class ExecutionSession:
    """Invocation authority, reporting, and effect attempts for one run owner."""

    def __init__(self, run: RunMachine[Any]) -> None:
        self._run = run
        self._signing_key = secrets.token_bytes(32)
        self._execution_index: dict[str, _ExecutionRecord] = {}
        self._journal = PostJournal(run._writer, self)

    def apply(self, offset: int, entry: Entry) -> None:
        self._journal.apply(offset, entry)
        self._index_execution(offset, entry)

    def validate_execution(self, request: ExecutionRequest[_R]) -> None:
        if (
            not isinstance(cast("object", request), ExecutionRequest)
            or request.run_id != self._run._run_id
        ):
            msg = "execution request belongs to another run"
            raise InvalidExecutionRequestError(msg)
        registration = self._run._executor.registration(
            request._context.identity.effect_id
        )
        if registration is not request._registration:
            msg = "execution request does not match the configured registration"
            raise InvalidExecutionRequestError(msg)

    def _capability(self, scope: PostScope, invocation: InvocationIdentity) -> str:
        payload = canonical_json([
            cast("JSONValue", scope.fields()),
            cast("JSONValue", invocation.fields()),
        ])
        return hmac.new(self._signing_key, payload, hashlib.sha256).hexdigest()

    @staticmethod
    def _authority(request: ExecutionRequest[_R]) -> PostAuthority:
        identity = request._context.identity
        return PostAuthority(
            PostScope(identity.run_seed, "execution", identity.operation_id),
            request.capability,
            request._context.invocation,
        )

    def _check_ingress(self) -> None:
        if (
            self._run.closed
            or self._run._lease is None
            or self._run._execution_error is not None
        ):
            msg = "run owner is unavailable"
            raise RunUnavailableError(msg)
        self._run._writer.check_open()

    def _index_execution(self, offset: int, entry: Entry) -> None:
        if entry["type"] == "promise.create" and "effect_call" in entry:
            if entry["id"] in self._execution_index:
                raise CorruptLogError(offset, "duplicate effect execution identity")
            self._execution_index[entry["id"]] = _ExecutionRecord(
                call=entry["effect_call"], invocation=None, completed=None
            )
        elif (
            entry["type"] == "effect.attempt.start"
            or entry["type"] == "promise.complete"
        ):
            record = self._execution_index.get(entry["promise_id"])
            if record is None:
                if entry["type"] == "effect.attempt.start":
                    raise CorruptLogError(offset, "attempt references absent execution")
                return
            if record.completed is not None:
                raise CorruptLogError(offset, "execution changes after completion")
            if entry["type"] == "promise.complete":
                self._execution_index[entry["promise_id"]] = replace(
                    record, completed=entry
                )
            else:
                data = entry["invocation"]
                invocation = InvocationIdentity(
                    data["invocation_number"], data["recovery_count"]
                )
                self._execution_index[entry["promise_id"]] = replace(
                    record, invocation=invocation
                )

    def _execution_record(self, operation_id: str) -> _ExecutionRecord:
        record = self._execution_index.get(operation_id)
        if record is None:
            msg = "execution does not exist in this run"
            raise InvalidExecutionRequestError(msg)
        return record

    def execution_context(self, request: ExecutionRequest[_R]) -> EffectContextImpl:
        self.validate_execution(request)
        return replace(request._context, handles=execution_handles(self, request))

    def _record_for_request(self, request: ExecutionRequest[_R]) -> _ExecutionRecord:
        self.validate_execution(request)
        self._check_ingress()
        record = self._execution_record(request._context.identity.operation_id)
        identity = request._context.identity
        if (
            record.call["run_seed"] != identity.run_seed
            or record.call["input_digest"] != identity.input_digest
            or canonical_json(record.call["inputs"]) != request.inputs
        ):
            msg = "request identity or inputs differ from the committed execution"
            raise InvalidExecutionRequestError(msg)
        registration = self._run._executor.registration(identity.effect_id)
        if record.call["descriptor"] != registration.descriptor(codec=self._run._codec):
            msg = "persisted execution registration differs from the request"
            raise InvalidExecutionRequestError(msg)
        self._check_ingress()
        return record

    async def report_execution(
        self, request: ExecutionRequest[_R], post: SemanticPost
    ) -> Receipt:
        _ = self._record_for_request(request)
        # Value-only executions share no channels; their journal gates suffice.
        gate = (
            self._run._channel_gate
            if request.handle_references
            else contextlib.nullcontext()
        )
        async with gate:
            return await self._journal.post(self._authority(request), post)

    async def report_stream(
        self,
        request: ExecutionRequest[_R],
        reference: HandleReference,
        post: SemanticPost,
    ) -> Receipt:
        _ = self._record_for_request(request)
        if reference not in request.handle_references:
            msg = "stream is not declared by the execution"
            raise InvalidHandleError(msg)
        state = self._run._owned_channels.get(reference.channel_id)
        if state is None or reference != replace(
            state.reference, owner_id=state.owner_id or ""
        ):
            msg = "stream reference does not belong to this owned channel"
            raise InvalidHandleError(msg)
        async with self._run._channel_gate:
            return await self._journal.post(self._authority(request), post)

    def _signal_for_reference(
        self, request: ExecutionRequest[_R], reference: HandleReference
    ) -> OwnedSignalState:
        state = self._run._owned_channels.get(reference.channel_id)
        if (
            not isinstance(state, OwnedSignalState)
            or reference.direction not in {"signal.sender", "signal.receiver"}
            or reference
            != replace(
                state.reference,
                owner_id=request._context.identity.operation_id,
                direction=reference.direction,
            )
            or reference not in request.handle_references
            or (
                reference.direction == "signal.receiver"
                and state.owner_id != reference.owner_id
            )
        ):
            msg = "signal reference is not declared with this direction and owner"
            raise InvalidHandleError(msg)
        return state

    def _control_authority(self, reference: HandleReference) -> PostAuthority:
        scope = PostScope(reference.run_seed, "control", reference.channel_id)
        capability = hmac.new(
            self._signing_key,
            canonical_json([scope.run_seed, scope.kind, scope.id]),
            hashlib.sha256,
        ).hexdigest()
        return PostAuthority(scope, capability, None)

    async def _post_control(
        self, authority: PostAuthority, post: SemanticPost
    ) -> Receipt:
        self._check_ingress()
        async with self._run._channel_gate:
            return await self._journal.post(authority, post)

    async def _send_control(
        self, reference: HandleReference, payload: JSONValue, key: str
    ) -> list[JSONValue]:
        state = self._run._owned_channels.get(reference.channel_id)
        if not isinstance(state, OwnedSignalState) or reference != replace(
            state.reference, direction="signal.sender"
        ):
            msg = "control sender belongs to another signal scope"
            raise InvalidHandleError(msg)
        receipt = await self._post_control(
            self._control_authority(reference),
            SemanticPost.create(
                key, "signal.send", PostTarget("signal", reference.channel_id), payload
            ),
        )
        return [receipt.offset, receipt.duplicate]

    async def _next_signal(
        self, signal: OwnedSignalState
    ) -> tuple[int, JSONValue] | None:
        cursor = signal.read_offset
        while page := await self._run._backend.read(
            self._run._run_id,
            after=cursor,
            limit=128,
            stream_id=signal.reference.channel_id,
        ):
            for offset, record in page:
                if offset > self._run._frontier:
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
        if reference.direction != "signal.receiver":
            msg = "signal receive requires a receiver reference"
            raise InvalidHandleError(msg)
        while True:
            version = self._run._append_version
            async with self._run._channel_gate:
                _ = self._record_for_request(request)
                state = self._signal_for_reference(request, reference)
                authority = self._authority(request)
                record = self._authorize_invocation(authority)
                if record.completed is not None:
                    msg = "execution has already reported its outcome"
                    raise ExecutionCompletedError(msg)
                delivery = await self._next_signal(state)
                if delivery is not None:
                    state.read_offset, payload = delivery
                    return payload
                if state.closed:
                    msg = "signal channel is closed"
                    raise ChannelClosedError(msg)
            await self._run.wait_for_append(version)

    def _authorize_invocation(self, authority: PostAuthority) -> _ExecutionRecord:
        self._check_ingress()
        if authority.invocation is None:
            msg = "execution capability does not own the requested target"
            raise InvalidHandleError(msg)
        expected = self._capability(authority.scope, authority.invocation)
        if not hmac.compare_digest(authority.capability, expected):
            msg = "invocation capability belongs to another owner"
            raise StaleInvocationError(msg)
        record = self._execution_record(authority.scope.id)
        if (
            record.call["run_seed"] != authority.scope.run_seed
            or record.invocation != authority.invocation
        ):
            msg = "invocation capability is fenced"
            raise StaleInvocationError(msg)
        self._check_ingress()
        return record

    async def authorize(self, authority: PostAuthority, post: SemanticPost, /) -> None:
        self._check_ingress()
        if authority.scope.kind == "control":
            state = self._run._owned_channels.get(post.target.id)
            if (
                not isinstance(state, OwnedSignalState)
                or not state.committed
                or post.action != "signal.send"
                or authority.scope
                != PostScope(
                    state.reference.run_seed, "control", state.reference.channel_id
                )
                or not hmac.compare_digest(
                    authority.capability,
                    self._control_authority(state.reference).capability,
                )
            ):
                msg = "invalid scoped control sender capability"
                raise InvalidHandleError(msg)
            return
        record = self._authorize_invocation(authority)
        if post.target.kind == "stream":
            state = self._run._owned_channels.get(post.target.id)
            if state is None or state.owner_id != authority.scope.id:
                msg = "execution does not own the requested stream"
                raise InvalidHandleError(msg)
        elif post.target.kind != "signal" and post.target != PostTarget(
            "execution", authority.scope.id
        ):
            msg = "execution capability does not own the target"
            raise InvalidHandleError(msg)
        if post.target.kind == "signal":
            direction = POST_ACTION_DIRECTIONS.get(post.action)
            if direction is None:
                msg = "execution does not declare this signal direction"
                raise InvalidHandleError(msg)
            descriptor = record.call["descriptor"]
            if not isinstance(descriptor, dict) or not isinstance(
                effect_id := descriptor.get("effect_id"), str
            ):
                raise CorruptLogError(0, "effect descriptor has no registered identity")
            registration = self._run._executor.registration(EffectId(effect_id))
            references = input_references(
                registration, canonical_json(record.call["inputs"])
            )
            if not any(
                ref.channel_id == post.target.id
                and ref.direction == direction
                and ref.owner_id == authority.scope.id
                for ref in references
            ):
                msg = "execution does not declare this signal direction"
                raise InvalidHandleError(msg)

    def check_lifecycle(self, authority: PostAuthority, post: SemanticPost, /) -> None:
        if authority.scope.kind == "control":
            if self._run._owned_channels[post.target.id].closed:
                msg = "signal channel is closed"
                raise ChannelClosedError(msg)
            return
        state = self._run._attempts.get(authority.scope.id)
        if state is None:
            msg = "execution or invocation has already reported its outcome"
            raise ExecutionCompletedError(msg)
        if (
            post.target.kind == "stream"
            and self._run._owned_channels[post.target.id].closed
        ):
            msg = "stream EOF is permanent"
            raise PortClosedError(msg)
        if (
            post.target.kind == "signal"
            and self._run._owned_channels[post.target.id].closed
        ):
            msg = "signal channel is closed"
            raise ChannelClosedError(msg)

    def stage(self, authority: PostAuthority, post: SemanticPost, /) -> Sequence[Entry]:
        id_ = authority.scope.id
        now = _effect_now_us()
        if post.action in {"signal.send", "stream.send"}:
            state = self._run._owned_channels[post.target.id]
            validate_value(
                self._run._codec.decode_json(post.payload, state.item_type),
                state.item_type,
            )
            return [
                {
                    "id": random_id(),
                    "ts": now,
                    "source": "effect",
                    "type": "stream.emit",
                    "stream_id": post.target.id,
                    "value": post.payload,
                }
            ]
        effect = self._run._effects[id_]
        operation = effect.operation
        if post.action == "stream.close":
            closure: StreamCompleteEntry = {
                "id": random_id(),
                "ts": now,
                "source": "effect",
                "type": "stream.complete",
                "stream_id": post.target.id,
            }
            return [closure]
        entry: PromiseCompleteEntry = {
            "id": random_id(),
            "ts": now,
            "type": "promise.complete",
            "source": "effect",
            "promise_id": id_,
        }
        if post.action == "execution.complete":
            result = self._run._codec.decode_json(post.payload, effect.return_type)
            if effect.return_type is not UnspecifiedType:
                validate_value(result, effect.return_type, allow_unspecified=True)
            entry["result"] = post.payload
        elif post.action == "execution.cancel":
            entry["error"] = encode_error(asyncio.CancelledError())
        else:
            error = decode_error(cast("ErrorInfo", post.payload), operation.raises)
            if not isinstance(error, Exception):
                msg = "failure reporting cannot submit cancellation"
                raise InvalidPostError(msg)
            entry["error"] = cast("ErrorInfo", post.payload)
        self._run._task_manager.reporting(id_)
        return self._finalize_channels(operation, entry)

    def _finalize_channels(
        self, operation: EffectOperation, entry: PromiseCompleteEntry
    ) -> Sequence[Entry]:
        closures: list[Entry] = []
        for reference in operation.handles:
            if (
                direction_claims_channel(reference.direction)
                and not self._run._owned_channels[reference.channel_id].closed
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

    async def _effect_run(self, id_: str, op: EffectCall, waiter: OpFuture) -> None:
        operation = op.operation
        identity = ExecutionIdentity(
            operation.seed, id_, operation.identity.effect_id, operation.input_digest
        )
        entry: PromiseCompleteEntry = {
            "ts": -1,
            "id": random_id(),
            "type": "promise.complete",
            "promise_id": id_,
            "source": "effect",
        }
        committed = False
        invocation: Invocation | None = None
        scope = PostScope(operation.seed, "execution", id_)
        self._run._running_effects.add(id_)

        def bind_invocation(
            attempt: InvocationIdentity, *, recovering: bool = False
        ) -> Invocation:
            context = EffectContextImpl(
                identity, operation.identity, attempt, recovering
            )
            request = local_request(
                run_id=self._run._run_id,
                context=context,
                registration=self._run._executor.registration(identity.effect_id),
                codec=self._run._codec,
                inputs=InputSnapshot(operation.inputs, operation.input_digest),
                capability=self._capability(scope, attempt),
                return_type=cast("TypeHint[object]", op.return_type),
            )
            return Invocation(self, request)

        try:
            if waiter.cancelled():
                raise asyncio.CancelledError  # ruff: ignore[raise-within-try]
            state = self._run._attempts[id_]
            plan = state.plan()
            assert plan is not None
            start: EffectAttemptStartEntry = {
                "ts": _effect_now_us(),
                "id": random_id(),
                "type": "effect.attempt.start",
                "source": "effect",
                "promise_id": id_,
                "invocation": plan.invocation.fields(),
                "session_id": self._run._session_id,
            }
            async with self._journal.gate(scope):
                await self._run._enqueue_log(start)
            invocation = bind_invocation(plan.invocation, recovering=plan.recovering)
            request = invocation._request
            _ = self._record_for_request(request)
            _ = self._authorize_invocation(self._authority(request))
            adapter = self._run._executor.executor_for(identity.effect_id)
            action: Literal["execution.complete", "execution.fail"]
            try:
                value = await adapter.execute(invocation)
            except ApplicationFailure as failure:
                action = "execution.fail"
                payload = cast("JSONValue", encode_error(failure.error))
            else:
                action = "execution.complete"
                try:
                    payload = request.encode_result(value)
                except (TypeError, ValueError) as error:
                    action = "execution.fail"
                    payload = cast("JSONValue", encode_error(error))
            await self.report_execution(
                request,
                SemanticPost.create(
                    f"executor:{invocation.metadata.invocation_number}:outcome",
                    action,
                    PostTarget("execution", id_),
                    payload,
                ),
            )
            record = self._execution_record(id_)
            assert record.completed is not None
            entry = record.completed
            committed = True
        except asyncio.CancelledError as error:
            if self._run.closed:
                return
            self._run._task_manager.reporting(id_)
            cancelled_state = self._run._attempts.get(id_)
            if cancelled_state is not None and cancelled_state.start is not None:
                invocation = bind_invocation(cancelled_state.start.invocation)
                with contextlib.suppress(ExecutionCompletedError):
                    await self.report_execution(
                        invocation._request,
                        SemanticPost.create(
                            f"executor:{invocation.metadata.invocation_number}:cancel",
                            "execution.cancel",
                            PostTarget("execution", id_),
                            None,
                        ),
                    )
                record = self._execution_record(id_)
                assert record.completed is not None
                entry = record.completed
                committed = True
            elif cancelled_state is not None:
                entry["error"] = encode_error(error)
            else:
                record = self._execution_record(id_)
                assert record.completed is not None
                entry = record.completed
                committed = True
        if self._run.closed:
            return
        if not committed:
            entry["ts"] = _effect_now_us()
        if not committed:
            self._run._task_manager.reporting(id_)
            async with self._run._channel_gate:
                await self._run._writer.append(
                    self._finalize_channels(operation, entry)
                )
