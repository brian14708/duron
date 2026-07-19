"""Owned stream state and bounded, independently consumed durable readers."""

from __future__ import annotations

import contextvars
from collections import deque
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import TypeVar

from duron._execution.errors import decode_error
from duron._execution.handles import (
    CommandSender,
    PostingCommandSender,
    SignalReceiver,
    StreamSender,
)
from duron._execution.models import (
    HandleReference,
    StreamCursor,
    StreamEvent,
    StreamId,
    digest,
)
from duron._execution.posting import Receipt
from duron._execution.validation import validate_value
from duron._history.encoding import decode_record
from duron._runtime.ops import (
    OwnedChannelExit,
    OwnedSignalSend,
    OwnedStreamNext,
    create_op,
)
from duron.errors import CorruptLogError

if TYPE_CHECKING:
    from duron._api.run import OutputCursor
    from duron._hints import JSONValue, TypeHint
    from duron._runtime.loop import EventLoop
    from duron.codec import Codec
    from duron.storage import EventStore


_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class StreamEOF:
    error: BaseException | None = None


@dataclass(slots=True)
class OwnedChannelState:
    reference: HandleReference
    item_type: TypeHint[object]
    committed: bool = False
    owner_id: str | None = None
    closed: bool = False
    error_types: tuple[type[BaseException], ...] = ()


@dataclass(slots=True)
class StreamReadBuffer(Generic[_T]):
    stream: StreamId
    item_type: TypeHint[_T]
    prefetch: int
    scanned: int | None = None
    values: deque[StreamEvent[_T] | StreamEOF] = field(default_factory=deque)
    eof: bool = False
    codec: Codec | None = None

    async def fill(
        self,
        backend: EventStore,
        run_id: str,
        codec: Codec,
        frontier: int,
        declared: tuple[type[BaseException], ...],
    ) -> None:
        # Drain decoded values before fetching another page. During replay the
        # backend also contains entries beyond the currently applied frontier.
        if self.values:
            return
        while len(self.values) < self.prefetch and not self.eof:
            if self.scanned is not None and self.scanned >= frontier:
                return
            page = await backend.read(
                run_id,
                after=self.scanned,
                limit=self.prefetch,
                stream_id=self.stream.value,
            )
            if not page:
                self.scanned = frontier
                return
            for offset, record in page:
                if offset > frontier:
                    self.scanned = frontier
                    return
                if len(self.values) >= self.prefetch:
                    return
                entry = decode_record(record, offset)
                self.scanned = offset
                if (
                    entry["type"] == "stream.emit"
                    and entry["stream_id"] == self.stream.value
                ):
                    value = (self.codec or codec).decode_json(
                        entry["value"], self.item_type
                    )
                    validate_value(value, self.item_type)
                    self.values.append(
                        StreamEvent(value, StreamCursor(self.stream, offset))
                    )
                elif (
                    entry["type"] == "stream.complete"
                    or entry["type"] == "stream.scope.exit"
                ) and entry["stream_id"] == self.stream.value:
                    error = (
                        decode_error(entry["error"], declared)
                        if entry["type"] == "stream.complete" and "error" in entry
                        else None
                    )
                    self.values.append(StreamEOF(error))
                    self.eof = True
                    return


class Stream(Generic[_T]):
    def __init__(
        self,
        loop: EventLoop,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        prefetch: int,
        codec: Codec | None = None,
    ) -> None:
        self._loop = loop
        self._reference = reference
        self._item_type = item_type
        self._prefetch = prefetch
        self._codec = codec
        self._sender = StreamSender(reference, item_type=item_type)
        self._reader = self.reader()

    @property
    def sender(self) -> StreamSender[_T]:
        """Effect-facing sender owned by this stream scope."""
        return self._sender

    def reader(
        self, *, prefetch: int | None = None, after: OutputCursor | None = None
    ) -> _StreamReader[_T]:
        if after is not None and (
            after.stream
            != digest(
                "duron.output.v0",
                [self._reference.run_seed, self._reference.channel_id],
            )
        ):
            msg = "output cursor belongs to a different run or port"
            raise ValueError(msg)
        return _StreamReader(
            self._loop,
            StreamReadBuffer(
                StreamId(self._reference.run_seed, self._reference.channel_id),
                self._item_type,
                self._prefetch if prefetch is None else prefetch,
                scanned=after.offset if after is not None else None,
                codec=self._codec,
            ),
            offset=after.offset if after is not None else 0,
        )

    def __aiter__(self) -> _StreamReader[_T]:
        return self._reader

    async def _close_scope(self) -> None:
        if self._loop.is_closed():
            return
        await create_op(self._loop, OwnedChannelExit(self._reference.channel_id))


class _StreamReader(Generic[_T]):
    def __init__(
        self, loop: EventLoop, buffer: StreamReadBuffer[_T], *, offset: int
    ) -> None:
        self.loop, self.buffer, self.offset = loop, buffer, offset
        self.ended = False

    @property
    def cursor(self) -> OutputCursor:
        from duron._api.run import OutputCursor  # ruff: ignore[import-outside-top-level]  # Host/run import cycle.

        return OutputCursor(
            digest(
                "duron.output.v0",
                [self.buffer.stream.run_seed, self.buffer.stream.value],
            ),
            self.offset,
        )

    def __aiter__(self) -> _StreamReader[_T]:
        return self

    async def __anext__(self) -> _T:
        if self.ended:
            raise StopAsyncIteration
        item = await create_op(self.loop, OwnedStreamNext(self.buffer))
        if isinstance(item, StreamEOF):
            self.ended = True
            if item.error is not None:
                raise item.error
            raise StopAsyncIteration
        self.offset = item.cursor.offset
        return cast("_T", item.value)


@dataclass(slots=True)
class OwnedSignalState(OwnedChannelState):
    read_offset: int | None = None


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


class Commands(Generic[_T]):
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
        self._sender = CommandSender(
            sender_reference,
            item_type=item_type,
            posting=PostingCommandSender(
                _WorkflowSignalPosting(loop, sender_reference),
                item_type=item_type,
                codec=codec,
            ),
            key_factory=lambda: f"workflow:{loop.generate_op_id()}",
        )
        self._receiver = SignalReceiver(reference, item_type=item_type)

    @property
    def sender(self) -> CommandSender[_T]:
        """Workflow-facing sender owned by this command scope."""
        return self._sender

    @property
    def receiver(self) -> SignalReceiver[_T]:
        """Effect-facing receiver owned by this command scope."""
        return self._receiver

    async def _close_scope(self) -> None:
        if not self._loop.is_closed():
            await create_op(self._loop, OwnedChannelExit(self._reference.channel_id))
