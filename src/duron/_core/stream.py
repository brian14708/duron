from __future__ import annotations

import asyncio
import contextlib
import contextvars
from abc import ABC, abstractmethod
from asyncio.exceptions import CancelledError
from collections import deque
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Concatenate, Generic, cast
from typing_extensions import Any, ParamSpec, Protocol, Self, TypeVar, final, override

from duron._core.ops import (
    FnCall,
    OpAnnotations,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron.loop import wrap_future

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
    from types import TracebackType

    from duron._core.context import Context
    from duron._core.ops import StreamObserver
    from duron.loop import EventLoop
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


class EmptyStream(Exception):  # noqa: N818
    __slots__ = ()


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
    async def _next(self) -> tuple[int, _T]: ...
    @abstractmethod
    def _next_nowait(self, offset: int, /) -> tuple[int, _T]: ...

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> _T:
        try:
            _, val = await self._next()
        except StreamClosed as e:
            if e.reason is not None:
                raise e.reason from None
            raise StopAsyncIteration from None
        return val

    async def next(self) -> _T:
        """Wait for and return the next value from the stream.

        Returns:
            A tuple of (offset, value) where offset is the operation offset and
            value is the emitted stream value.

        Raises:
            StreamClosed: When the stream has been closed.
        """  # noqa: DOC502
        _, val = await self._next()
        return val

    async def next_nowait(self, ctx: Context) -> AsyncGenerator[_T]:
        """Yield available values from the stream without blocking.

        Yields values that have already been emitted up to the current barrier
        offset. Does not wait for new values.

        Args:
            ctx: The duron context for determining the current barrier offset.

        Yields:
            Tuples of (offset, value) for each available value.
        """
        offset = await ctx.barrier()
        try:
            while True:
                _, val = self._next_nowait(offset)
                yield val
        except EmptyStream:
            return

    # collect methods

    async def collect(self) -> list[_T]:
        """Consume all values from the stream and return them as a list.

        Returns:
            A list containing all values emitted by the stream.
        """
        result: list[_T] = [e async for e in self]
        return result

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

    def broadcast(self, n: int) -> AbstractAsyncContextManager[Sequence[Stream[_T]]]:
        """Broadcast stream values to multiple consumers.

        Args:
            n: Number of broadcast streams to create.

        Returns:
            An async context manager yielding a sequence of n streams, each \
                    receiving all values from the source stream.
        """
        return _Broadcast(self, n)


async def create_stream(
    loop: EventLoop, dtype: TypeHint[_T], annotations: OpAnnotations
) -> tuple[Stream[_T], StreamWriter[_T]]:
    """Create a new bidirectional stream for inter-operation communication.

    Creates a stream for sending values between operations with deterministic
    replay. The stream can be consumed via async iteration or context manager,
    while the writer is used to send values from other operations.

    Args:
        loop: The event loop to create the stream on.
        dtype: Type hint for values sent through the stream.
        annotations: Operation annotations for tracing and debugging.

    Returns:
        A tuple of (stream, writer) where stream is used to consume values and \
                writer is used to send values and close the stream.
    """
    assert asyncio.get_running_loop() is loop
    s, w = create_buffer_stream()
    sid = await create_op(
        loop, StreamCreate(dtype=dtype, observer=w, annotations=annotations)
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

    @final
    @override
    async def _next(self) -> tuple[int, _T]:
        if not self._event:
            self._loop = asyncio.get_running_loop()
            self._event = asyncio.Event()

        while not self._buffer:
            self._event.clear()
            _ = await self._event.wait()

        t, item = self._buffer.popleft()
        if isinstance(item, StreamClosed):
            raise item
        return t, item

    @final
    @override
    def _next_nowait(self, offset: int) -> tuple[int, _T]:
        while self._buffer and self._buffer[0][0] <= offset:
            t, item = self._buffer.popleft()
            if isinstance(item, StreamClosed):
                raise item
            return t, item
        raise EmptyStream

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
    async def _next(self) -> tuple[int, _U]:
        t, val = await self._stream._next()  # noqa: SLF001
        return t, self._fn(val)

    @override
    def _next_nowait(self, offset: int) -> tuple[int, _U]:
        t, val = self._stream._next_nowait(offset)  # noqa: SLF001
        return t, self._fn(val)


@final
class _Broadcast(Generic[_T]):
    __slots__ = ("_n", "_parent", "_task")

    def __init__(self, parent: Stream[_T], n: int) -> None:
        self._parent = parent
        self._task: asyncio.Task[None] | None = None
        self._n = n

    async def _pump(self, writers: list[StreamObserver]) -> None:
        try:
            while True:
                o, v = await self._parent._next()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                for s in writers:
                    s.on_next(o, v)
        except StreamClosed as e:
            for s in writers:
                s.on_close(e.offset, e.reason)

    async def __aenter__(self) -> Sequence[Stream[_T]]:
        writers: list[StreamObserver] = []
        streams: list[Stream[_T]] = []
        for _i in range(self._n):
            s, w = create_buffer_stream()
            streams.append(s)
            writers.append(w)
        self._task = asyncio.create_task(self._pump(writers))
        return streams

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._task:
            _ = self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task


@contextlib.asynccontextmanager
async def run_stateful(
    loop: EventLoop,
    dtype: TypeHint[Any],
    initial: _T,
    reducer: Callable[[_T, _U], _T],
    fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> AsyncGenerator[tuple[Stream[_U], Awaitable[_T]], None]:
    assert asyncio.get_running_loop() is loop

    name = cast("str", getattr(fn, "__name__", repr(fn)))
    stream: _StatefulStream[_U, _T] = _StatefulStream(
        initial, reducer, fn, *args, **kwargs
    )
    sink: StreamWriter[_U] = OpWriter(
        await create_op(
            loop,
            StreamCreate(
                dtype=dtype, observer=stream, annotations=OpAnnotations(name=name)
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
                annotations=OpAnnotations(name=name),
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
        initial: _T,
        reducer: Callable[[_T, _U], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
        /,
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
