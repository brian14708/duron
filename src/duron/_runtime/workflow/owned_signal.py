"""Scoped acknowledged control channels and bounded invocation-local delivery."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import TypeVar, override

from duron._runtime.execution.handle_data import SignalDelivery, SignalId
from duron._runtime.execution.handles import (
    CommandReceiver,
    CommandSender,
    PostingCommandSender,
)
from duron._runtime.execution.posting import PostTarget, Receipt, SemanticPost
from duron._runtime.workflow.ops import OwnedChannelExit, OwnedSignalSend, create_op
from duron._runtime.workflow.owned_stream import OwnedChannelState
from duron.errors import CorruptLogError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from duron._runtime.execution.handle_data import HandleRecovery, HandleReference
    from duron._runtime.execution.models import InvocationIdentity
    from duron._runtime.execution.posting import PostAuthority
    from duron._runtime.loop import EventLoop
    from duron.codec import Codec
    from duron.types import JSONValue, TypeHint

_T = TypeVar("_T")


@dataclass(slots=True)
class SignalReservation:
    invocation: InvocationIdentity
    correlation_id: str
    delivery: SignalDelivery | None = None
    waiters: int = 0


@dataclass(slots=True)
class OwnedSignalState(OwnedChannelState):
    acknowledged_offset: int | None = None
    reservation: SignalReservation | None = None

    @override
    def recovery(self, reference: HandleReference) -> HandleRecovery:
        base = super(OwnedSignalState, self).recovery(reference)
        delivered = self.reservation.delivery if self.reservation is not None else None
        return replace(
            base,
            acknowledged_offset=self.acknowledged_offset,
            delivered_offset=delivered.offset if delivered is not None else None,
        )


@dataclass(frozen=True, slots=True)
class ControlSignalPosting:
    authority: PostAuthority
    channel_id: str
    post: Callable[[PostAuthority, SemanticPost], Awaitable[Receipt]]

    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt:
        return await self.post(
            self.authority,
            SemanticPost.create(
                idempotency_key,
                "signal.send",
                PostTarget("signal", self.channel_id),
                payload,
            ),
        )


@dataclass(frozen=True, slots=True)
class _WorkflowSignalPosting:
    loop: EventLoop
    reference: HandleReference

    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt:
        result = await create_op(
            self.loop,
            OwnedSignalSend(
                self.reference, payload, idempotency_key, contextvars.copy_context()
            ),
        )
        msg = "workflow signal send has an invalid receipt result"
        if not isinstance(result, list):
            raise CorruptLogError(0, msg)
        fields = cast("list[object]", result)
        if (
            len(fields) != 2
            or type(fields[0]) is not int
            or type(fields[1]) is not bool
        ):
            raise CorruptLogError(0, msg)
        return Receipt(fields[0], fields[1])


class DurableCommands(Generic[_T]):
    def __init__(
        self,
        loop: EventLoop,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        codec: Codec,
    ) -> None:
        self._loop = loop
        self._reference = reference
        sender_reference = replace(reference, direction="signal.sender")
        self.sender = CommandSender(
            sender_reference,
            item_type=item_type,
            posting=PostingCommandSender(
                _WorkflowSignalPosting(loop, sender_reference),
                item_type=item_type,
                codec=codec,
            ),
            key_namespace=f"workflow:{reference.channel_id}:",
            key_factory=lambda: f"workflow:{loop.generate_op_id()}",
        )
        self.receiver = CommandReceiver(reference, item_type=item_type)

    @property
    def id(self) -> SignalId:
        return SignalId(self._reference.run_seed, self._reference.channel_id)

    async def close_scope(self) -> None:
        if not self._loop.is_closed():
            await create_op(self._loop, OwnedChannelExit(self._reference.channel_id))
        await self.receiver.join_producers()
