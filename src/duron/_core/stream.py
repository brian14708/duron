from __future__ import annotations

import asyncio
import contextlib
import contextvars
from abc import ABC, abstractmethod
from asyncio.exceptions import CancelledError
from collections import deque
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Concatenate, Generic, cast
from typing_extensions import Any, ParamSpec, Protocol, TypeVar, final, override

from duron._core.ops import (
    Barrier,
    FnCall,
    OpMetadata,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron.loop import EventLoop, wrap_future

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
    from types import TracebackType

    from duron._core.ops import StreamObserver
    from duron.typing import TypeHint

    _P = ParamSpec("_P")

_T = TypeVar("_T")
_U = TypeVar("_U")
_InT = TypeVar("_InT", contravariant=True)  # noqa: PLC0105


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


class StreamWriter(
    AbstractAsyncContextManager["StreamWriter[_InT]"], Protocol, Generic[_InT]
):
    """Protocol for writing values to a stream."""

    async def send(self, value: _InT, /) -> None:
        """Send a value to the stream.

        Args:
            value: The value to send to stream consumers.
        """
        ...

    async def close(self, error: Exception | None = None, /) -> None:
        """Close the stream, optionally with an error.

        Args:
            error: Optional exception to signal an error condition to consumers.
        """
        ...


@final
class OpWriter(Generic[_InT]):
    __slots__ = ("_closed", "_loop", "_stream_id")

    def __init__(self, stream_id: str, loop: EventLoop) -> None:
        self._stream_id = stream_id
        self._loop = loop
        self._closed = False

    async def send(self, value: _InT, /) -> None:
        await wrap_future(
            create_op(self._loop, StreamEmit(stream_id=self._stream_id, value=value))
        )

    async def close(self, exception: Exception | None = None, /) -> None:
        await wrap_future(
            create_op(
                self._loop, StreamClose(stream_id=self._stream_id, exception=exception)
            )
        )
        self._closed = True

    async def __aenter__(self) -> StreamWriter[_InT]:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._closed:
            return
        if not exc_value:
            await self.close()
        elif isinstance(exc_value, Exception):
            await self.close(exc_value)
        else:
            await self.close(
                Exception(f"StreamWriter exited with exception: {exc_value}")
            )


class Stream(ABC, Generic[_T]):
    """Abstract base class for readable streams."""

    @abstractmethod
    async def next(self) -> Iterator[_T]:
        """Wait for and return the next value from the stream.

        Returns:
            A tuple of (offset, value) where offset is the operation offset and
            value is the emitted stream value.

        Raises:
            StreamClosed: When the stream has been closed.
        """
        ...

    @abstractmethod
    async def next_nowait(self) -> Iterator[_T] | None:
        """Yield available values from the stream without blocking.

        Yields values that have already been emitted up to the current barrier
        offset. Does not wait for new values.

        Raises:
            RuntimeError: If called outside of the context's event loop.
            StreamClosed: If the stream has been closed.

        Returns:
            An iterator over the available stream values.
        """
        ...

    async def __aiter__(self) -> AsyncGenerator[_T]:
        try:
            while True:
                for val in await self.next():
                    yield val
        except StreamClosed as e:
            if e.reason is not None:
                raise e.reason from None

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
    writer: OpWriter[_T] = OpWriter(sid, loop)
    return (s, writer)


def create_buffer_stream() -> tuple[Stream[Any], StreamObserver]:
    s: _BufferStream[Any] = _BufferStream()
    return (s, s)


class _BufferStream(Stream[_T], Generic[_T]):
    __slots__ = ("_buffer", "_event", "_loop")

    def __init__(self) -> None:
        super().__init__()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None
        self._buffer: deque[tuple[int, _T | StreamClosed]] = deque()
        self._cursor: int = -1

    @final
    @override
    async def next(self) -> Iterator[_T]:
        if not self._event:
            self._loop = asyncio.get_running_loop()
            self._event = asyncio.Event()

        self._event.clear()
        while not (it := await self.next_nowait()):
            _ = await self._event.wait()
            self._event.clear()

        return it

    @final
    @override
    async def next_nowait(self) -> Iterator[_T] | None:
        if not self._loop:
            self._loop = asyncio.get_running_loop()
        if isinstance(self._loop, EventLoop):
            offset = start_cursor = self._cursor

            op = create_op(self._loop, Barrier())

            def cb(f: asyncio.Future[tuple[int, int]]) -> None:
                if not f.cancelled():
                    offset, _ = f.result()
                    self._cursor = max(self._cursor, offset)

            op.add_done_callback(cb)
            offset, _ = await asyncio.shield(op)
            self._cursor = max(self._cursor, offset)
        else:
            start_cursor = -1
            offset = -1

        result: list[_T] = []
        for t, item in self._buffer:
            if t > offset and offset != -1:
                break

            if t < start_cursor:
                continue

            if isinstance(item, StreamClosed):
                if len(result) > 0:
                    break
                raise item
            result.append(item)

        if self._buffer:
            t, item = self._buffer[0]
            if (
                isinstance(item, StreamClosed)
                and t <= start_cursor
                and len(result) == 0
            ):
                raise item

        while self._buffer:
            t, item = self._buffer[0]
            if t > offset and offset != -1:
                break
            if isinstance(item, StreamClosed):
                break
            self._buffer.popleft()
        return iter(result) if result else None

    def on_next(self, offset: int, value: object) -> None:
        self._buffer.append((offset, cast("_T", value)))
        if self._loop and self._event:
            _ = self._loop.call_soon(self._event.set)

    def on_close(self, offset: int, exc: Exception | None) -> None:
        self._buffer.append((offset, StreamClosed(offset=offset, reason=exc)))
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
    async def next(self) -> Iterator[_U]:
        return map(self._fn, await self._stream.next())

    @override
    async def next_nowait(self) -> Iterator[_U] | None:
        it = await self._stream.next_nowait()
        return map(self._fn, it) if it is not None else None


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
    sink: StreamWriter[_U] = OpWriter(
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
                callable=lambda: stream.worker(sink),
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
