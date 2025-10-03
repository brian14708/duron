from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from asyncio.exceptions import CancelledError
from collections import deque
from typing import TYPE_CHECKING, Concatenate, Generic, Protocol, TypeVar, cast

from typing_extensions import final, override

from duron.ops import FnCall, StreamClose, StreamCreate, StreamEmit, create_op

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Sequence
    from contextlib import AbstractAsyncContextManager
    from types import TracebackType

    from typing_extensions import ParamSpec

    from duron.context import Context
    from duron.event_loop import EventLoop

    _P = ParamSpec("_P")

_T = TypeVar("_T")
_U = TypeVar("_U")
_In = TypeVar("_In", contravariant=True)


class Observer(Generic[_In], Protocol):
    def on_next(self, log_offset: int, value: _In, /) -> None: ...
    def on_close(self, log_offset: int, error: BaseException | None, /) -> None: ...


@final
class Sink(Generic[_T]):
    __slots__ = ("_stream_id", "_loop")

    def __init__(self, id: str, loop: EventLoop) -> None:
        self._stream_id = id
        self._loop = loop

    async def send(self, value: _T, /) -> None:
        await create_op(
            self._loop,
            StreamEmit(stream_id=self._stream_id, value=value),
        )

    async def close(self, error: BaseException | None = None, /) -> None:
        await create_op(
            self._loop,
            StreamClose(stream_id=self._stream_id, exception=error),
        )


class Stream(Generic[_T], ABC):
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
        assert not self._started
        self._started = True
        return self.__agen()

    async def __agen(self) -> AsyncGenerator[_T]:
        try:
            await self._start()
            while True:
                _, val = await self._next()
                yield val
        except EndOfStream as e:
            if e.reason:
                raise e.reason from None
        finally:
            await self._shutdown()

    async def __aenter__(self) -> StreamOp[_T]:
        assert not self._started
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
        result: list[_T] = []
        async for e in self:
            result.append(e)
        return result

    async def discard(self) -> None:
        async for _ in self:
            pass

    # stream methods

    def map(self, fn: Callable[[_T], _U]) -> Stream[_U]:
        return _Map(self, fn)

    def broadcast(self, n: int) -> AbstractAsyncContextManager[Sequence[Stream[_T]]]:
        return _Broadcast(self, n)


@final
class StreamOp(Generic[_T]):
    def __init__(self, stream: Stream[_T]) -> None:
        self._stream = stream

    async def next(self) -> tuple[int, _T]:
        return await self._stream._next()  # pyright: ignore[reportPrivateUsage]

    async def next_nowait(self, ctx: Context) -> AsyncGenerator[tuple[int, _T]]:
        offset = await ctx.barrier()
        try:
            while True:
                yield self._stream._next_nowait(offset)  # pyright: ignore[reportPrivateUsage]
        except EmptyStream:
            return


async def create_stream(
    loop: EventLoop, dtype: type[_T]
) -> tuple[Stream[_T], Sink[_T]]:
    assert asyncio.get_running_loop() is loop
    s: _ObserverStream[_T] = _ObserverStream()
    sid = await create_op(
        loop,
        StreamCreate(
            dtype=dtype,
            observer=s,
        ),
    )
    return (s, Sink(sid, loop))


@final
class EndOfStream(Exception):
    __slots__ = ("offset",)

    def __init__(
        self, *args: object, offset: int, reason: BaseException | None
    ) -> None:
        super().__init__(*args)
        self.offset = offset
        self.__cause__ = reason

    @property
    def reason(self) -> Exception | None:
        return cast("Exception | None", self.__cause__)


@final
class EmptyStream(Exception):
    __slots__ = ()


class _ObserverStream(Generic[_T], Stream[_T]):
    def __init__(self) -> None:
        super().__init__()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None
        self._buffer: deque[tuple[int, _T | EndOfStream]] = deque()

    @override
    async def _start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._event = asyncio.Event()

    @final
    @override
    async def _next(self) -> tuple[int, _T]:
        assert self._event is not None

        while not self._buffer:
            self._event.clear()
            _ = await self._event.wait()

        t, item = self._buffer.popleft()
        if isinstance(item, EndOfStream):
            raise item
        return t, item

    @final
    @override
    def _next_nowait(self, offset: int) -> tuple[int, _T]:
        while self._buffer and self._buffer[0][0] <= offset:
            t, item = self._buffer.popleft()
            if isinstance(item, EndOfStream):
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

    def _send_close(self, offset: int, exc: BaseException | None) -> None:
        self._buffer.append((offset, EndOfStream(offset=offset, reason=exc)))
        if self._loop and self._event:
            _ = self._loop.call_soon(self._event.set)

    def on_next(self, offset: int, value: _T) -> None:
        self._send(offset, value)

    def on_close(self, offset: int, exc: BaseException | None) -> None:
        self._send_close(offset, exc)


@final
class _Map(Generic[_T, _U], Stream[_U]):
    def __init__(self, stream: Stream[_T], fn: Callable[[_T], _U]) -> None:
        super().__init__()
        self._stream = stream
        self._fn = fn

    @override
    async def _start(self) -> None:
        return await self._stream._start()

    @override
    async def _next(self) -> tuple[int, _U]:
        t, val = await self._stream._next()
        return t, self._fn(val)

    @override
    def _next_nowait(self, offset: int) -> tuple[int, _U]:
        t, val = self._stream._next_nowait(offset)
        return t, self._fn(val)

    @override
    async def _shutdown(self) -> None:
        return await self._stream._shutdown()


@final
class _Broadcast(Generic[_T]):
    def __init__(self, parent: Stream[_T], n: int) -> None:
        self._parent = parent
        self._task: asyncio.Task[None] | None = None
        self._streams: list[_ObserverStream[_T]] = [_ObserverStream() for _ in range(n)]

    async def _pump(self):
        async with self._parent as parent:
            try:
                while True:
                    o, v = await parent.next()
                    for s in self._streams:
                        s._send(o, v)  # pyright: ignore[reportPrivateUsage]
            except EndOfStream as e:
                for s in self._streams:
                    s._send_close(e.offset, e.reason)  # pyright: ignore[reportPrivateUsage]

    async def __aenter__(self) -> Sequence[Stream[_T]]:
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


def resumable(
    loop: EventLoop,
    dtype: type | None,
    initial: _T,
    reducer: Callable[[_T, _U], _T],
    fn: Callable[Concatenate[_T, _P], AsyncGenerator[_U, _T]],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> AbstractAsyncContextManager[Stream[_U]]:
    assert asyncio.get_running_loop() is loop
    s: _Resumable[_U, _T] = _Resumable(
        initial,
        reducer,
        fn,
        *args,
        **kwargs,
    )
    return _ResumableGuard(loop, s, dtype)


@final
class _ResumableGuard(Generic[_U, _T]):
    def __init__(
        self, loop: EventLoop, resumable: _Resumable[_U, _T], dtype: type | None
    ) -> None:
        self._loop = loop
        self._stream = resumable
        self._task: asyncio.Future[object] | None = None
        self._dtype = dtype

    async def __aenter__(self) -> Stream[_U]:
        sid = await create_op(
            self._loop,
            StreamCreate(
                dtype=self._dtype,
                observer=self._stream,
            ),
        )
        sink: Sink[_T] = Sink(sid, self._loop)
        self._task = create_op(
            self._loop,
            FnCall(
                callable=self._stream.worker,
                args=(sink,),
                kwargs={},
                return_type=None,
            ),
        )
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
class _Resumable(Generic[_U, _T], _ObserverStream[_U]):
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
        self._closed: bool | BaseException = False
        self._current: _T = initial
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._enabled = True

    async def worker(self, sink: Sink[_U]):
        gen = None
        try:
            if self._closed is True:
                return
            self._enabled = False

            state = self._current
            gen = self._fn(state, *self._args, **self._kwargs)
            state_partial = await anext(gen)
            while True:
                state = self._reducer(state, state_partial)
                await sink.send(state_partial)
                state_partial = await gen.asend(state)
        except StopAsyncIteration:
            await sink.close()
        except Exception as e:
            await sink.close(e)
            raise
        except CancelledError as e:
            self._send_close(-1, e)
            raise
        finally:
            if gen:
                await gen.aclose()

    @override
    def on_next(self, offset: int, value: _U):
        if self._enabled:
            self._current = self._reducer(self._current, value)
        super().on_next(offset, value)

    @override
    def on_close(self, offset: int, exc: BaseException | None):
        if self._enabled:
            self._closed = True if exc is None else exc
        super().on_close(offset, exc)
