from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
from abc import ABC, abstractmethod
from asyncio.exceptions import CancelledError
from collections import deque
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Concatenate, Generic, cast
from typing_extensions import Any, ParamSpec, TypeVar, final, override

from duron._core.ops import (
    Barrier,
    FnCall,
    OpMetadata,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron.loop import EventLoop, LoopClosedError, wrap_future

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
    from types import TracebackType

    from duron._core.ops import StreamObserver
    from duron.typing import TypeHint

    _P = ParamSpec("_P")

_T = TypeVar("_T")
_U = TypeVar("_U")


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

    __slots__ = ("_closed", "_loop", "_stream_id")

    def __init__(self, stream_id: str, loop: EventLoop) -> None:
        self._stream_id = stream_id
        self._loop = loop
        self._closed = False

    async def send(self, value: _T, /) -> None:
        """Send a value to the stream.

        Raises:
            RuntimeError: If the stream is already closed.

        Args:
            value: The value to send to stream consumers.

        """
        if self._closed:
            msg = "Cannot send to a closed stream"
            raise RuntimeError(msg)
        await wrap_future(
            create_op(self._loop, StreamEmit(stream_id=self._stream_id, value=value))
        )

    async def close(self, exception: Exception | None = None, /) -> None:
        """Close the stream, optionally with an error.

        Raises:
            RuntimeError: If the stream is already closed.

        Args:
            exception: Optional exception to signal an error condition to consumers.

        """
        if self._closed:
            msg = "Cannot send to a closed stream"
            raise RuntimeError(msg)
        await wrap_future(
            create_op(
                self._loop, StreamClose(stream_id=self._stream_id, exception=exception)
            )
        )
        self._closed = True

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
            A tuple of (offset, value) where offset is the operation offset and
            value is the emitted stream value.

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

    async def discard(self) -> None:
        """Consume all values from the stream without collecting them."""
        async for _ in self:
            pass

    # stream methods

    def map(self, fn: Callable[[_T], _U]) -> Stream[_U]:
        """Transform stream values using a mapping function.

        Args:
            fn: Function to apply to each value in the stream.

        Returns:
            A new stream that yields transformed values.

        """
        return _Map(self, fn)


async def create_stream(
    loop: EventLoop, dtype: TypeHint[_T], name: str | None, metadata: OpMetadata
) -> tuple[Stream[_T], StreamWriter[_T]]:
    assert asyncio.get_running_loop() is loop
    s, w = create_buffer_stream()
    sid = await create_op(
        loop, StreamCreate(dtype=dtype, observer=w, name=name, metadata=metadata)
    )
    writer: StreamWriter[_T] = StreamWriter(sid, loop)
    return (s, writer)


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


@final
class _Map(Stream[_U], Generic[_T, _U]):
    __slots__ = ("_fn", "_stream")

    def __init__(self, stream: Stream[_T], fn: Callable[[_T], _U]) -> None:
        super().__init__()
        self._stream = stream
        self._fn = fn

    @override
    async def next(self, *, block: bool) -> Sequence[_U]:
        return tuple(map(self._fn, await self._stream.next(block=block)))

    @override
    async def __aiter__(self) -> AsyncGenerator[_U]:
        async for item in self._stream:
            yield self._fn(item)


@contextlib.asynccontextmanager
async def run_stateful(
    loop: EventLoop,
    dtype: TypeHint[Any],
    reducer: Callable[[_T, _U], _T],
    fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
    /,
    initial: _T,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> AsyncGenerator[tuple[Stream[_U], Awaitable[_T]], None]:
    assert asyncio.get_running_loop() is loop

    name = cast("str", getattr(fn, "__name__", repr(fn)))
    stream: _StatefulStream[_U, _T] = _StatefulStream(
        reducer, fn, initial, *args, **kwargs
    )
    sink: StreamWriter[_U] = StreamWriter(
        await create_op(
            loop,
            StreamCreate(
                dtype=dtype, name=None, observer=stream, metadata=OpMetadata(name=name)
            ),
        ),
        loop,
    )
    task = cast(
        "asyncio.Task[_T]",
        create_op(
            loop,
            FnCall(
                callable=functools.partial(stream.worker, sink),
                return_type=type(initial),
                context=contextvars.copy_context(),
                metadata=OpMetadata(name=name),
            ),
        ),
    )
    try:
        yield (stream, task)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@final
class _StatefulStream(_BufferStream[_U], Generic[_U, _T]):
    __slots__ = (
        "_args",
        "_closed",
        "_current",
        "_enabled",
        "_fn",
        "_kwargs",
        "_reducer",
    )

    def __init__(
        self,
        reducer: Callable[[_T, _U], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
        /,
        initial: _T,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        super().__init__()
        self._reducer = reducer
        self._closed: bool | Exception = False
        self._current: _T = initial
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._enabled = True

    async def worker(self, sink: StreamWriter[_U]) -> _T:
        gen = None
        state = self._current
        if self._closed is True:
            return state
        if self._closed is not False:
            raise self._closed
        self._enabled = False
        try:
            gen = self._fn(state, *self._args, **self._kwargs)
            try:
                state_partial = await anext(gen)
                while True:
                    state = self._reducer(state, state_partial)
                    await sink.send(state_partial)
                    state_partial = await gen.asend(state)
            finally:
                await gen.aclose()
        except StopAsyncIteration:
            await sink.close()
            return state
        except Exception as e:
            await sink.close(e)
            raise
        except CancelledError:
            self.on_close(-1, RuntimeError("worker cancelled"))
            raise

    @override
    def on_next(self, offset: int, value: object) -> None:
        if self._enabled:
            self._current = self._reducer(self._current, cast("_U", value))
        super().on_next(offset, value)

    @override
    def on_close(self, offset: int, exc: Exception | None) -> None:
        if self._enabled:
            self._closed = True if exc is None else exc
        super().on_close(offset, exc)
