"""Owned stream state and bounded, independently consumed durable readers."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import TypeVar

from duron._runtime.execution.errors import decode_error
from duron._runtime.execution.handle_data import (
    HandleRecovery,
    HandleReference,
    StreamCursor,
    StreamEvent,
    StreamId,
    direction_claims_channel,
)
from duron._runtime.execution.handles import StreamSender
from duron._runtime.execution.value_schema import validate_value
from duron._runtime.storage.helper import decode_record
from duron._runtime.workflow.ops import OwnedChannelExit, OwnedStreamNext, create_op
from duron.errors import InvalidHandleError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from duron._runtime.loop import EventLoop
    from duron.codec import Codec
    from duron.storage import HistoryStore
    from duron.types import TypeHint

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
    latest_offset: int | None = None
    closed: bool = False
    error_types: tuple[type[BaseException], ...] = ()

    def recovery(self, reference: HandleReference) -> HandleRecovery:
        if (
            direction_claims_channel(reference.direction)
            and self.owner_id != reference.owner_id
        ):
            msg = "stream reference does not match its committed producer"
            raise InvalidHandleError(msg)
        return HandleRecovery(reference, self.latest_offset, closed=self.closed)


@dataclass(slots=True)
class StreamReadBuffer(Generic[_T]):
    stream: StreamId
    item_type: TypeHint[_T]
    prefetch: int
    scanned: int | None = None
    values: deque[StreamEvent[_T] | StreamEOF] = field(default_factory=deque)
    eof: bool = False

    async def fill(
        self,
        backend: HistoryStore,
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
                    value = codec.decode_json(entry["value"], self.item_type)
                    validate_value(value, self.item_type)
                    self.values.append(
                        StreamEvent(
                            cast("_T", value), StreamCursor(self.stream, offset)
                        )
                    )
                elif (
                    entry["type"] == "stream.complete"
                    and entry["stream_id"] == self.stream.value
                ):
                    error = (
                        decode_error(entry["error"], declared)
                        if "error" in entry
                        else None
                    )
                    self.values.append(StreamEOF(error))
                    self.eof = True
                    return


class DurableStream(Generic[_T]):
    def __init__(
        self,
        loop: EventLoop,
        reference: HandleReference,
        *,
        item_type: TypeHint[_T],
        prefetch: int,
    ) -> None:
        self._loop = loop
        self._reference = reference
        self._item_type = item_type
        self._prefetch = prefetch
        self.sender = StreamSender(reference, item_type=item_type)
        self._reader = self.reader()

    def reader(self) -> AsyncIterator[_T]:
        return self._iterate(
            StreamReadBuffer(
                StreamId(self._reference.run_seed, self._reference.channel_id),
                self._item_type,
                self._prefetch,
            )
        )

    def __aiter__(self) -> AsyncIterator[_T]:
        return self._reader

    async def _iterate(self, reader: StreamReadBuffer[_T]) -> AsyncIterator[_T]:
        while True:
            item = await create_op(self._loop, OwnedStreamNext(reader))
            if isinstance(item, StreamEOF):
                if item.error is not None:
                    raise item.error
                return
            yield cast("_T", item.value)

    async def close_scope(self) -> None:
        await self.sender.join_producers()
        if self._loop.is_closed():
            return
        await create_op(self._loop, OwnedChannelExit(self._reference.channel_id))
