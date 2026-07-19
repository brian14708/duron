"""Typed channel handles; durable authority stays with the bound execution."""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from dataclasses import dataclass, field, replace
from typing import (
    TYPE_CHECKING,
    Annotated,
    Generic,
    NoReturn,
    cast,
    get_args,
    get_origin,
)
from typing_extensions import Protocol, TypeVar, override

from duron._runtime.execution.posting import validate_post_key
from duron._runtime.execution.value_schema import validate_schema, validate_value
from duron.errors import InvalidHandleError, PortClosedError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import SupportsIndex

    from duron._runtime.execution.handle_data import (
        CommandReceiverParameter,
        CommandSenderParameter,
        HandleDirection,
        HandleReference,
        MessageId,
        StreamCursor,
        StreamEvent,
        StreamParameter,
    )
    from duron._runtime.execution.posting import Receipt
    from duron.codec import Codec
    from duron.types import JSONValue, TypeHint

_T = TypeVar("_T")


class HandleResolver(Protocol):
    def stream(self, parameter: StreamParameter[_T]) -> PostingStream[_T]: ...
    def signal_sender(
        self, parameter: CommandSenderParameter[_T]
    ) -> PostingCommandSender[_T]: ...
    def signal_receiver(
        self, parameter: CommandReceiverParameter[_T]
    ) -> CommandReceiver[_T]: ...
    async def read_events(
        self,
        parameter: StreamParameter[_T],
        *,
        after: StreamCursor | None = None,
        limit: int = 1024,
    ) -> Sequence[StreamEvent[_T]]: ...


class StreamPosting(Protocol):
    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt: ...
    async def close(self, *, idempotency_key: str) -> Receipt: ...


class _LiveHandle:
    @override
    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        msg = "live channel handles cannot be pickled; pass a typed handle parameter"
        raise InvalidHandleError(msg)


class _PostingChannel(_LiveHandle, Generic[_T]):
    """Shared encode/validate contract for external posting handles."""

    def __init__(self, *, item_type: TypeHint[_T], codec: Codec) -> None:
        validate_schema(item_type)
        self._item_type = item_type
        self._codec = codec

    def _encode(self, value: _T, idempotency_key: str) -> JSONValue:
        validate_post_key(idempotency_key)
        validate_value(value, self._item_type)
        payload = self._codec.encode_json(value, self._item_type)
        # Prove the payload round-trips before it is committed durably.
        validate_value(
            self._codec.decode_json(payload, self._item_type), self._item_type
        )
        return payload


class PostingStream(_PostingChannel[_T]):
    """External output posting always requires a semantic key."""

    def __init__(
        self, posting: StreamPosting, *, item_type: TypeHint[_T], codec: Codec
    ) -> None:
        super().__init__(item_type=item_type, codec=codec)
        self._posting = posting

    async def send(self, value: _T, *, idempotency_key: str) -> Receipt:
        return await self._posting.send(
            self._encode(value, idempotency_key), idempotency_key=idempotency_key
        )

    async def close(self, *, idempotency_key: str) -> Receipt:
        validate_post_key(idempotency_key)
        return await self._posting.close(idempotency_key=idempotency_key)


class _ProducerScope(_LiveHandle):
    def __init__(self) -> None:
        self._producers: set[asyncio.Task[object]] = set()

    def track_producer(self) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._producers.add(task)

    async def join_producers(self) -> None:
        for task in self._producers:
            if not task.done() and task is not asyncio.current_task():
                task.cancel()
        for task in self._producers:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                if task is not asyncio.current_task():
                    await task
        self._producers.clear()


class StreamSender(_ProducerScope, Generic[_T]):
    """Effect-facing output; omitted keys identify new invocation-local work."""

    def __init__(
        self,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        posting: PostingStream[_T] | None = None,
        key_namespace: str = "local:",
    ) -> None:
        super().__init__()
        validate_schema(item_type)
        if reference.direction != "stream.sender":
            msg = "a stream sender requires a stream.sender reference"
            raise InvalidHandleError(msg)
        self.reference = reference
        self.item_type = item_type
        self._posting = posting
        self._closing = False
        self._close_committed = False
        self._close_key: str | None = None
        self._key_namespace = key_namespace

    def for_owner(self, owner_id: str) -> HandleReference:
        return replace(self.reference, owner_id=owner_id)

    def _bound(self) -> PostingStream[_T]:
        if self._posting is None:
            msg = "workflow stream senders must be passed to an owning effect"
            raise InvalidHandleError(msg)
        return self._posting

    def _key(self, key: str | None) -> str:
        return (
            key if key is not None else f"{self._key_namespace}{secrets.token_hex(24)}"
        )

    async def send(self, value: _T, *, idempotency_key: str | None = None) -> Receipt:
        if self._closing and not (
            self._close_committed and idempotency_key is not None
        ):
            msg = "stream sender is closing or closed"
            raise PortClosedError(msg)
        return await self._bound().send(
            value, idempotency_key=self._key(idempotency_key)
        )

    async def close(self, *, idempotency_key: str | None = None) -> Receipt:
        posting = self._bound()
        if self._close_key is None:
            self._close_key = self._key(idempotency_key)
        key = self._close_key if idempotency_key is None else idempotency_key
        self._closing = True
        receipt = await posting.close(idempotency_key=key)
        self._close_committed = True
        return receipt

    async def __aenter__(self) -> StreamSender[_T]:
        _ = self._bound()
        return self

    async def __aexit__(
        self, exc_type: object, _exc_value: object, _traceback: object
    ) -> None:
        if exc_type is None:
            _ = await self.close()


class SignalPosting(Protocol):
    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt: ...


class SignalAcknowledgement(Protocol):
    async def ack(self, message_id: MessageId) -> Receipt: ...


@dataclass(frozen=True, slots=True)
class Command(_LiveHandle, Generic[_T]):
    id: MessageId
    value: _T
    _acknowledgement: SignalAcknowledgement = field(repr=False, compare=False)

    async def ack(self) -> Receipt:
        return await self._acknowledgement.ack(self.id)


class SignalReceiving(Protocol[_T]):
    async def receive(self, correlation_id: str) -> Command[_T]: ...
    def rebind(self, message: Command[_T]) -> Command[_T]: ...


class PostingCommandSender(_PostingChannel[_T]):
    """External signal posting requires an explicit semantic key."""

    def __init__(
        self, posting: SignalPosting, *, item_type: TypeHint[_T], codec: Codec
    ) -> None:
        super().__init__(item_type=item_type, codec=codec)
        self._posting = posting

    async def send(self, value: _T, *, idempotency_key: str) -> Receipt:
        return await self._posting.send(
            self._encode(value, idempotency_key), idempotency_key=idempotency_key
        )


class CommandSender(_LiveHandle, Generic[_T]):
    """Effect-facing signal sender with optional invocation-local keys."""

    def __init__(
        self,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        posting: PostingCommandSender[_T] | None = None,
        key_namespace: str = "local:",
        key_factory: Callable[[], str] | None = None,
    ) -> None:
        validate_schema(item_type)
        if reference.direction != "signal.sender":
            msg = "a signal sender requires a signal.sender reference"
            raise InvalidHandleError(msg)
        self.reference = reference
        self.item_type = item_type
        self._posting = posting
        self._key_namespace = key_namespace
        self._key_factory = key_factory

    def for_owner(self, owner_id: str) -> HandleReference:
        return replace(self.reference, owner_id=owner_id)

    async def send(self, value: _T, *, idempotency_key: str | None = None) -> Receipt:
        if self._posting is None:
            msg = "signal sender is not bound to an active owner"
            raise InvalidHandleError(msg)
        key = (
            idempotency_key
            if idempotency_key is not None
            else self._key_factory()
            if self._key_factory is not None
            else f"{self._key_namespace}{secrets.token_hex(24)}"
        )
        return await self._posting.send(value, idempotency_key=key)


class CommandReceiver(_ProducerScope, Generic[_T]):
    """One receiver owner with one outstanding delivery until committed ack."""

    def __init__(
        self,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        receiving: SignalReceiving[_T] | None = None,
    ) -> None:
        super().__init__()
        validate_schema(item_type)
        if reference.direction != "signal.receiver":
            msg = "a signal receiver requires a signal.receiver reference"
            raise InvalidHandleError(msg)
        self.reference = reference
        self.item_type = item_type
        self._receiving = receiving

    def for_owner(self, owner_id: str) -> HandleReference:
        return replace(self.reference, owner_id=owner_id)

    async def receive(self) -> Command[_T]:
        if self._receiving is None:
            msg = "workflow signal receivers must be passed to an owning effect"
            raise InvalidHandleError(msg)
        # Transport adapters retry this private call with the same correlation ID.
        return await self._receiving.receive(secrets.token_hex(24))

    def rebind(self, message: Command[_T]) -> Command[_T]:
        """Renew a retained message's ack binding after execution rebinding.

        This does no I/O. Ack still checks current authority and only recovers a
        committed receipt or acknowledges the current outstanding delivery.

        Returns:
            An immutable message bound to this receiver's acknowledgement path.

        Raises:
            InvalidHandleError: The receiver is unbound or the signal differs.

        """
        if self._receiving is None:
            msg = "signal receiver is not bound to an active owner"
            raise InvalidHandleError(msg)
        validate_value(message.value, self.item_type)
        return self._receiving.rebind(message)


def handle_annotation(annotation: object) -> tuple[object, HandleDirection] | None:
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    directions: dict[object, HandleDirection] = {
        StreamSender: "stream.sender",
        CommandSender: "signal.sender",
        CommandReceiver: "signal.receiver",
    }
    if annotation in directions:
        msg = "handle annotations require a concrete payload type"
        raise InvalidHandleError(msg)
    direction = directions.get(get_origin(annotation))
    if direction is None:
        return None
    arguments = cast("tuple[object, ...]", get_args(annotation))
    if len(arguments) != 1:
        msg = "handle annotations require one payload type"
        raise InvalidHandleError(msg)
    validate_schema(arguments[0])
    return (type(None) if arguments[0] is None else arguments[0], direction)
