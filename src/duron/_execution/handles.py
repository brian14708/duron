"""Typed channel handles; durable authority stays with the bound execution."""

from __future__ import annotations

import secrets
from dataclasses import replace
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

from duron._execution.posting import validate_post_key
from duron._execution.validation import validate_schema, validate_value
from duron.errors import InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import SupportsIndex

    from duron._execution.models import (
        HandleDirection,
        HandleReference,
        InputPortParameter,
        OutputPortParameter,
    )
    from duron._execution.posting import Receipt
    from duron._hints import JSONValue, TypeHint
    from duron.codec import Codec

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)


class HandleResolver(Protocol):
    def stream(self, parameter: OutputPortParameter[_T]) -> PostingStream[_T]: ...
    def signal_receiver(
        self, parameter: InputPortParameter[_T]
    ) -> SignalReceiver[_T]: ...


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
        try:
            validate_value(value, self._item_type)
            payload = self._codec.encode_json(value, self._item_type)
            # Prove the payload round-trips before it is committed durably.
            validate_value(
                self._codec.decode_json(payload, self._item_type), self._item_type
            )
        except Exception as error:
            msg = "port value cannot be encoded"
            raise WorkflowDefinitionError(msg) from error
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


class StreamSender(_LiveHandle, Generic[_T]):
    """Effect-facing output; omitted keys identify new invocation-local work."""

    def __init__(
        self,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        posting: PostingStream[_T] | None = None,
        key_namespace: str = "local:",
    ) -> None:
        validate_schema(item_type)
        if reference.direction != "stream.sender":
            msg = "a stream sender requires a stream.sender reference"
            raise InvalidHandleError(msg)
        self._reference = reference
        self.item_type = item_type
        self._posting = posting
        self._key_namespace = key_namespace

    def _for_owner(self, owner_id: str) -> HandleReference:
        return replace(self._reference, owner_id=owner_id)

    async def send(self, value: _T, *, idempotency_key: str | None = None) -> Receipt:
        if self._posting is None:
            msg = "workflow stream senders must be passed to an owning effect"
            raise InvalidHandleError(msg)
        key = (
            idempotency_key
            if idempotency_key is not None
            else f"{self._key_namespace}{secrets.token_hex(24)}"
        )
        return await self._posting.send(value, idempotency_key=key)


class SignalPosting(Protocol):
    async def send(self, payload: JSONValue, *, idempotency_key: str) -> Receipt: ...


class SignalReceiving(Protocol[_T_co]):
    async def receive(self) -> _T_co: ...


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
    """Workflow-facing input sender with replay-stable default keys."""

    def __init__(
        self,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        posting: PostingCommandSender[_T],
        key_factory: Callable[[], str],
    ) -> None:
        validate_schema(item_type)
        if reference.direction != "signal.sender":
            msg = "a signal sender requires a signal.sender reference"
            raise InvalidHandleError(msg)
        self._reference = reference
        self.item_type = item_type
        self._posting = posting
        self._key_factory = key_factory

    async def send(self, value: _T, *, idempotency_key: str | None = None) -> Receipt:
        key = idempotency_key if idempotency_key is not None else self._key_factory()
        return await self._posting.send(value, idempotency_key=key)


class SignalReceiver(_LiveHandle, Generic[_T]):
    """Effect-facing input with an invocation-local cursor."""

    def __init__(
        self,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        receiving: SignalReceiving[_T] | None = None,
    ) -> None:
        validate_schema(item_type)
        if reference.direction != "signal.receiver":
            msg = "a signal receiver requires a signal.receiver reference"
            raise InvalidHandleError(msg)
        self._reference = reference
        self.item_type = item_type
        self._receiving = receiving

    def _for_owner(self, owner_id: str) -> HandleReference:
        return replace(self._reference, owner_id=owner_id)

    async def receive(self) -> _T:
        if self._receiving is None:
            msg = "workflow signal receivers must be passed to an owning effect"
            raise InvalidHandleError(msg)
        return await self._receiving.receive()


def handle_annotation(annotation: object) -> tuple[object, HandleDirection] | None:
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    directions: dict[object, HandleDirection] = {
        StreamSender: "stream.sender",
        SignalReceiver: "signal.receiver",
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
