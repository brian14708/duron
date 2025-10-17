from __future__ import annotations

import asyncio
import contextlib
import contextvars
from abc import ABC, abstractmethod
from asyncio.exceptions import CancelledError
from collections import deque
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Concatenate, Generic, cast
from typing_extensions import (
    Any,
    ParamSpec,
    Protocol,
    TypeVar,
    final,
    override,
)

from duron._core.ops import (
    FnCall,
    OpAnnotations,
    StreamClose,
    StreamCreate,
    StreamEmit,
    create_op,
)
from duron._loop import wrap_future
from duron.typing import UnspecifiedType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator, Sequence
    from contextlib import AbstractAsyncContextManager
    from types import TracebackType

    from duron._core.context import Context
    from duron._loop import EventLoop
    from duron.typing import TypeHint

    _P = ParamSpec("_P")

_T = TypeVar("_T")
_U = TypeVar("_U")
_Result = TypeVar("_Result", covariant=True, default=None)  # noqa: PLC0105
_In = TypeVar("_In", contravariant=True)  # noqa: PLC0105


class StreamWriter(Protocol, Generic[_In]):
    """Protocol for writing values to a stream."""

    async def send(self, value: _In, /) -> None:
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
class _Writer(Generic[_In]):
    __slots__ = ("_loop", "_stream_id")

    def __init__(self, stream_id: str, loop: EventLoop) -> None:
        self._stream_id = stream_id
        self._loop = loop

    async def send(self, value: _In, /) -> None:
        await wrap_future(
            create_op(
                self._loop,
                StreamEmit(stream_id=self._stream_id, value=value),
            ),
        )

    async def close(self, exception: Exception | None = None, /) -> None:
        await wrap_future(
            create_op(
                self._loop,
                StreamClose(stream_id=self._stream_id, exception=exception),
            ),
        )


class Stream(ABC, Awaitable[_Result], Generic[_T, _Result]):
    """Abstract base class for readable streams.

    Usage as async iterator:
        ```python
        stream, writer = await ctx.create_stream(int)
        async for value in stream:
            print(value)
        ```

    Usage as context manager:
        ```python
        async with stream as ops:
            offset, value = await ops.next()
        ```
    """

    @abstractmethod
    async def _start(self) -> None: ...
    @abstractmethod
    async def _next(self) -> tuple[int, _T]: ...
    @abstractmethod
    def _next_nowait(self, offset: int, /) -> tuple[int, _T]: ...
    @abstractmethod
    async def _shutdown(self) -> None: ...

    def __init__(self) -> None:
        self._started: bool = False

    def __aiter__(self) -> AsyncGenerator[_T]:
        assert not self._started  # noqa: S101
        self._started = True
        return self.__agen()

    async def __agen(self) -> AsyncGenerator[_T]:
        try:
            await self._start()
            while True:
                _, val = await self._next()
                yield val
        except StreamClosed as e:
            if e.reason:
                raise e.reason from None
        finally:
            await self._shutdown()

    async def __aenter__(self) -> StreamOp[_T, _Result]:
        assert not self._started  # noqa: S101
        self._started = True
        await self._start()
        return StreamOp(self)

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self._shutdown()

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

    def map(self, fn: Callable[[_T], _U]) -> Stream[_U, _Result]:
        """Transform stream values using a mapping function.

        Args:
            fn: Function to apply to each value in the stream.

        Returns:
            A new stream that yields transformed values.
        """
        return _Map(self, fn)

    def broadcast(
        self,
        n: int,
    ) -> AbstractAsyncContextManager[Sequence[Stream[_T, None]]]:
        """Broadcast stream values to multiple consumers.

        Args:
            n: Number of broadcast streams to create.

        Returns:
            An async context manager yielding a sequence of n streams, each
            receiving all values from the source stream.
        """
        return _Broadcast(self, n)


@final
class StreamOp(Generic[_T, _Result]):
    """Operations on a stream obtained from using it as an async context manager.

    StreamOp provides advanced methods for consuming stream values.
    """

    def __init__(self, stream: Stream[_T, _Result]) -> None:
        self._stream = stream

    async def next(self) -> tuple[int, _T]:
        """Wait for and return the next value from the stream.

        Returns:
            A tuple of (offset, value) where offset is the operation offset and
            value is the emitted stream value.

        Raises:
            StreamClosed: When the stream has been closed.
        """  # noqa: DOC502
        return await self._stream._next()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    async def next_nowait(self, ctx: Context) -> AsyncGenerator[tuple[int, _T]]:
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
                yield self._stream._next_nowait(offset)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        except EmptyStream:
            return


async def create_stream(
    loop: EventLoop,
    dtype: TypeHint[_T],
    annotations: OpAnnotations,
) -> tuple[Stream[_T, None], StreamWriter[_T]]:
    """Create a new bidirectional stream for inter-operation communication.

    Creates a stream for sending values between operations with deterministic
    replay. The stream can be consumed via async iteration or context manager,
    while the writer is used to send values from other operations.

    Args:
        loop: The event loop to create the stream on.
        dtype: Type hint for values sent through the stream.
        annotations: Operation annotations for tracing and debugging.

    Returns:
        A tuple of (stream, writer) where stream is used to consume values and
        writer is used to send values and close the stream.
    """
    assert asyncio.get_running_loop() is loop  # noqa: S101
    s: ObserverStream[_T, None] = ObserverStream()
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            observer=cast("ObserverStream[object, None]", s),
            annotations=annotations,
        ),
    )
    return (s, _Writer(sid, loop))


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

    def __init__(
        self,
        *args: object,
        offset: int,
        reason: Exception | None,
    ) -> None:
        super().__init__(*args)
        self.offset = offset
        self.__cause__ = reason

    @property
    def reason(self) -> Exception | None:
        return cast("Exception | None", self.__cause__)


class EmptyStream(Exception):  # noqa: N818
    __slots__ = ()


class ObserverStream(Stream[_T, _Result], Generic[_T, _Result]):
    def __init__(self) -> None:
        super().__init__()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None
        self._buffer: deque[tuple[int, _T | StreamClosed]] = deque()
        self._waiter: Awaitable[_Result] | None = None

    @override
    async def _start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._event = asyncio.Event()

    @final
    @override
    async def _next(self) -> tuple[int, _T]:
        assert self._event is not None  # noqa: S101

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

    @override
    async def _shutdown(self) -> None:
        pass

    def _send(self, offset: int, value: _T) -> None:
        self._buffer.append((offset, value))
        if self._loop and self._event:
            _ = self._loop.call_soon(self._event.set)

    def _send_close(self, offset: int, exc: Exception | None) -> None:
        self._buffer.append((offset, StreamClosed(offset=offset, reason=exc)))
        if self._loop and self._event:
            _ = self._loop.call_soon(self._event.set)

    def on_next(self, offset: int, value: _T) -> None:
        self._send(offset, value)

    def on_close(self, offset: int, exc: Exception | None) -> None:
        self._send_close(offset, exc)

    @override
    def __await__(self) -> Generator[Any, Any, _Result]:
        if self._waiter is None:
            msg = "Stream is not started"
            raise RuntimeError(msg)
        return self._waiter.__await__()


@final
class _Map(Stream[_U, _Result], Generic[_T, _U, _Result]):
    def __init__(self, stream: Stream[_T, _Result], fn: Callable[[_T], _U]) -> None:
        super().__init__()
        self._stream = stream
        self._fn = fn

    @override
    async def _start(self) -> None:
        return await self._stream._start()  # noqa: SLF001

    @override
    async def _next(self) -> tuple[int, _U]:
        t, val = await self._stream._next()  # noqa: SLF001
        return t, self._fn(val)

    @override
    def _next_nowait(self, offset: int) -> tuple[int, _U]:
        t, val = self._stream._next_nowait(offset)  # noqa: SLF001
        return t, self._fn(val)

    @override
    async def _shutdown(self) -> None:
        return await self._stream._shutdown()  # noqa: SLF001

    @override
    def __await__(self) -> Generator[Any, Any, _Result]:
        return self._stream.__await__()


@final
class _Broadcast(Generic[_T]):
    def __init__(self, parent: Stream[_T, Any], n: int) -> None:
        self._parent = parent
        self._task: asyncio.Task[None] | None = None
        self._streams: list[ObserverStream[_T, None]] = [
            ObserverStream() for _ in range(n)
        ]

    async def _pump(self) -> None:
        async with self._parent as parent:
            try:
                while True:
                    o, v = await parent.next()
                    for s in self._streams:
                        s._send(o, v)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            except StreamClosed as e:
                for s in self._streams:
                    s._send_close(e.offset, e.reason)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    async def __aenter__(self) -> Sequence[Stream[_T, None]]:
        self._task = asyncio.create_task(self._pump())
        return tuple(self._streams)

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


def run_stateful(
    loop: EventLoop,
    dtype: TypeHint[Any],
    initial: _T,
    reducer: Callable[[_T, _U], _T],
    fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> AbstractAsyncContextManager[Stream[_U, _T]]:
    assert asyncio.get_running_loop() is loop  # noqa: S101
    s: _StatefulRun[_U, _T] = _StatefulRun(
        loop,
        initial,
        reducer,
        fn,
        *args,
        **kwargs,
    )
    return _StatefulGuard(loop, s, dtype)


@final
class _StatefulGuard(Generic[_U, _T]):
    def __init__(
        self,
        loop: EventLoop,
        stateful: _StatefulRun[_U, _T],
        dtype: TypeHint[Any],
    ) -> None:
        self._loop = loop
        self._stream = stateful
        self._task: asyncio.Future[object] | None = None
        self._dtype = dtype

    async def __aenter__(self) -> _StatefulRun[_U, _T]:
        sid = await create_op(
            self._loop,
            StreamCreate(
                dtype=self._dtype,
                observer=cast("_StatefulRun[object, _T]", self._stream),
                annotations=OpAnnotations(
                    name=self._stream.name(),
                ),
            ),
        )
        sink: StreamWriter[_U] = _Writer(sid, self._loop)
        self._task = self._stream.start_worker(sink)
        return self._stream

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


@final
class _StatefulRun(ObserverStream[_U, _T], Generic[_U, _T]):
    def __init__(
        self,
        loop: EventLoop,
        initial: _T,
        reducer: Callable[[_T, _U], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        super().__init__()
        self._event_loop = loop
        self._reducer = reducer
        self._closed: bool | Exception = False
        self._current: _T = initial
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._enabled = True
        self._task: asyncio.Future[_T] | None = None

    def name(self) -> str:
        return cast("str", getattr(self._fn, "__name__", repr(self._fn)))

    def start_worker(self, sink: StreamWriter[_U]) -> asyncio.Future[object]:
        op = create_op(
            self._event_loop,
            FnCall(
                callable=self._worker,
                args=(sink,),
                kwargs={},
                return_type=UnspecifiedType,
                context=contextvars.copy_context(),
                annotations=OpAnnotations(name=self.name()),
            ),
        )
        self._task = cast("asyncio.Future[_T]", op)
        return op

    async def _worker(self, sink: StreamWriter[_U]) -> _T:
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
            assert self._loop  # noqa: S101
            await sink.close()
            return state
        except Exception as e:
            await sink.close(e)
            raise
        except CancelledError:
            self._send_close(-1, RuntimeError("worker cancelled"))
            raise

    @override
    def __await__(self) -> Generator[Any, Any, _T]:
        if not self._task:
            msg = "Stream is not started"
            raise RuntimeError(msg)
        return self._task.__await__()

    @override
    def on_next(self, offset: int, value: _U) -> None:
        if self._enabled:
            self._current = self._reducer(self._current, value)
        super().on_next(offset, value)

    @override
    def on_close(self, offset: int, exc: Exception | None) -> None:
        if self._enabled:
            self._closed = True if exc is None else exc
        super().on_close(offset, exc)
