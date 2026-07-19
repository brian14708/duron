from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import Any, TypeVar, final, override

from duron._core.ops import (
    Barrier,
    OpMetadata,
    StreamClose,
    StreamCreate,
    StreamEmit,
    StreamReplayState,
    create_op,
)
from duron.loop import EventLoop, LoopClosedError, wrap_future

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence
    from types import TracebackType

    from duron._core.ops import StreamObserver
    from duron.typing import TypeHint

_T = TypeVar("_T")


@final
class StreamClosed(Exception):  # noqa: N818
    """Exception raised when attempting to read from a closed stream.

    This exception is raised when a stream consumer tries to get the next value
    from a stream that has been closed. If the stream was closed with an error,
    that error is available via the reason property.

    Attributes:
        offset: The operation offset at which the stream was closed.
        reason: The exception that caused the stream to close, if any.

    """

    __slots__ = ("offset",)

    def __init__(self, offset: int, reason: Exception | None) -> None:
        super().__init__(f"Stream closed at offset {offset}")
        self.offset = offset
        self.__cause__ = reason

    @property
    def reason(self) -> Exception | None:
        return cast("Exception | None", self.__cause__)


@final
class StreamWriter(Generic[_T]):
    """Protocol for writing values to a stream."""

    __slots__ = ("_closed", "_loop", "_replay_state", "_stream_id")

    def __init__(
        self,
        stream_id: str,
        loop: EventLoop,
        replay_state: StreamReplayState | None = None,
    ) -> None:
        self._stream_id = stream_id
        self._loop = loop
        self._replay_state = replay_state
        self._closed = False

    async def send(self, value: _T, /) -> None:
        """Send a value to the stream.

        Args:
            value: The value to send to stream consumers.

        Raises:
            RuntimeError: If the stream is already closed.

        """
        if self._closed:
            msg = "Cannot send to a closed stream"
            raise RuntimeError(msg)
        if self._skip_replayed():
            return
        await wrap_future(
            create_op(self._loop, StreamEmit(stream_id=self._stream_id, value=value))
        )

    async def close(self, exception: Exception | None = None, /) -> None:
        """Close the stream, optionally with an error.

        Args:
            exception: Optional exception to signal an error condition to consumers.

        Raises:
            RuntimeError: If the stream is already closed.

        """
        if self._closed:
            msg = "Cannot send to a closed stream"
            raise RuntimeError(msg)
        if self._skip_replayed():
            self._closed = True
            return
        await wrap_future(
            create_op(
                self._loop, StreamClose(stream_id=self._stream_id, exception=exception)
            )
        )
        self._closed = True

    def _skip_replayed(self) -> bool:
        """Consume one event already recorded before a crash and replayed.

        Only writers opted into replay adoption (streaming-effect calls)
        carry a replay state, and their re-running external producer adopts
        the entries its prior execution recorded instead of re-recording
        them.

        Returns:
            True if the caller should skip recording this event.

        """
        state = self._replay_state
        if state is None or state.remaining == 0:
            return False
        state.remaining -= 1
        return True

    async def __aenter__(self) -> StreamWriter[_T]:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._closed:
            return
        with contextlib.suppress(LoopClosedError):
            if exc_value is None or isinstance(exc_value, Exception):
                await self.close(exc_value)
            else:
                await self.close(
                    RuntimeError(f"StreamWriter exited with exception: {exc_value}")
                )


class Stream(ABC, AsyncIterable[_T], Generic[_T]):
    """Abstract base class for readable streams."""

    @abstractmethod
    async def next(self, *, block: bool) -> Sequence[_T]:
        """Wait for and return the next value from the stream.

        Args:
            block: If True, wait until at least one value is available.

        Returns:
            A sequence of emitted values. The sequence may contain multiple values
            when the reader catches up with the stream.

        Raises:
            StreamClosed: When the stream has been closed.

        """
        ...

    # collect methods

    async def collect(self) -> list[_T]:
        """Consume all values from the stream and return them as a list.

        Returns:
            A list containing all values emitted by the stream.

        """
        return [e async for e in self]


async def create_stream(
    loop: EventLoop,
    dtype: TypeHint[_T],
    name: str | None,
    metadata: OpMetadata,
    *,
    adopt_replayed: bool = False,
) -> tuple[Stream[_T], StreamWriter[_T]]:
    s, observer = create_buffer_stream()
    writer = await _open_writer(
        loop, dtype, name, metadata, observer, adopt_replayed=adopt_replayed
    )
    return (s, writer)


async def create_sink(
    loop: EventLoop, dtype: TypeHint[_T], name: str | None, metadata: OpMetadata
) -> StreamWriter[_T]:
    """Create a write-only stream.

    No observer is registered, so emitted values are recorded durably without
    being buffered or decoded in the worker. Host code reads them from storage.

    Returns:
        A writer for the stream.

    """
    return await _open_writer(loop, dtype, name, metadata, None)


async def _open_writer(
    loop: EventLoop,
    dtype: TypeHint[_T],
    name: str | None,
    metadata: OpMetadata,
    observer: StreamObserver | None,
    *,
    adopt_replayed: bool = False,
) -> StreamWriter[_T]:
    replay_state = StreamReplayState() if adopt_replayed else None
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            observer=observer,
            name=name,
            metadata=metadata,
            replay_state=replay_state,
        ),
    )
    return StreamWriter(sid, loop, replay_state=replay_state)


def create_buffer_stream() -> tuple[Stream[Any], StreamObserver]:
    s: _BufferStream[Any] = _BufferStream()
    return (s, s)


class _BufferStream(Stream[_T], Generic[_T]):
    __slots__ = ("_buffer", "_cursor", "_event", "_loop", "_write_cursor")

    def __init__(self) -> None:
        super().__init__()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None
        self._buffer: deque[tuple[int, _T | StreamClosed]] = deque()
        self._cursor: int = 0
        self._write_cursor: int = -1

    @final
    @override
    async def next(self, *, block: bool) -> Sequence[_T]:
        if not self._event:
            self._loop = asyncio.get_running_loop()
            self._event = asyncio.Event()

        if not block:
            begin, end = await self._next_cursor()
            return self._pop(begin, end)

        while True:
            self._event.clear()
            begin = self._cursor
            while self._write_cursor < begin:
                await self._event.wait()
                self._event.clear()

            begin, end = await self._next_cursor()
            items = self._pop(begin, end)
            if items:
                return items

    async def _next_cursor(self) -> tuple[int, int | None]:
        if not isinstance(self._loop, EventLoop):
            return (0, None)

        def cb(f: asyncio.Future[tuple[int, int]]) -> None:
            if not f.cancelled():
                offset, _ = f.result()
                self._cursor = max(self._cursor, offset)

        begin = self._cursor
        op = create_op(self._loop, Barrier())
        op.add_done_callback(cb)
        end, _ = await asyncio.shield(op)
        self._cursor = max(self._cursor, end)
        return (begin, end)

    def _pop(self, begin: int, end: int | None) -> Sequence[_T]:
        if end is None:
            if not self._buffer:
                return ()
            end = self._buffer[-1][0] + 1

        result: list[_T] = []
        while self._buffer:
            t, item = self._buffer[0]
            if t >= end:
                break
            if isinstance(item, StreamClosed):
                if len(result) > 0:
                    break
                raise item

            self._buffer.popleft()
            if t >= begin:
                result.append(item)

        return result

    @final
    @override
    async def __aiter__(self) -> AsyncGenerator[_T]:
        if not self._event:
            self._loop = asyncio.get_running_loop()
            self._event = asyncio.Event()

        self._event.clear()
        while True:
            while self._buffer:
                _t, item = self._buffer[0]
                if isinstance(item, StreamClosed):
                    self._buffer.popleft()
                    if item.reason is not None:
                        raise item
                    return
                self._buffer.popleft()
                yield item

            _ = await self._event.wait()
            self._event.clear()

    def on_next(self, offset: int, value: object) -> None:
        self._buffer.append((offset, cast("_T", value)))
        self._write_cursor = offset
        if self._loop and self._event:
            _ = self._loop.call_soon(self._event.set)

    def on_close(self, offset: int, exc: Exception | None) -> None:
        self._buffer.append((offset, StreamClosed(offset=offset, reason=exc)))
        self._write_cursor = offset
        if self._loop and self._event:
            _ = self._loop.call_soon(self._event.set)
