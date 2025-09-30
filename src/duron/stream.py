from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Concatenate,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    final,
)

from typing_extensions import override

from duron.event_loop import wrap_future
from duron.ops import Barrier, FnCall, StreamClose, StreamCreate, StreamEmit

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable

    from duron.event_loop import EventLoop

    _P = ParamSpec("_P")

_In = TypeVar("_In", contravariant=True)
_Out = TypeVar("_Out", covariant=True)
_T = TypeVar("_T")


class EndOfStream(Exception):
    pass


class StreamState(Enum):
    INITIAL = auto()
    STARTED = auto()
    CLOSED = auto()


class Observer(Generic[_In], Protocol):
    def on_next(self, log_offset: int, value: _In, /) -> None: ...
    def on_close(self, log_offset: int, error: BaseException | None, /) -> None: ...


@final
class StreamHandle(Generic[_In]):
    def __init__(self, id: str, loop: EventLoop) -> None:
        self._stream_id = id
        self._loop = loop
        self._target_loop: asyncio.AbstractEventLoop | None = None
        self._event = asyncio.Event()

    async def send(self, value: _In, /) -> None:
        if self._target_loop is None:
            _ = await self._loop.create_op(
                StreamEmit(stream_id=self._stream_id, value=value),
            )
        else:
            _ = await self._loop.create_host_op(
                StreamEmit(stream_id=self._stream_id, value=value),
            )

    async def close(self, error: BaseException | None = None, /) -> None:
        if self._target_loop is None:
            _ = await self._loop.create_op(
                StreamClose(stream_id=self._stream_id, exception=error),
            )
        else:
            _ = await self._loop.create_host_op(
                StreamClose(stream_id=self._stream_id, exception=error),
            )
        self._event.set()

    async def wait(self) -> None:
        _ = await self._event.wait()

    def to_host(self) -> StreamHandle[_In]:
        s: StreamHandle[_In] = StreamHandle(self._stream_id, self._loop)
        s._target_loop = self._loop.host_loop()
        return s


class Stream(Generic[_Out], ABC):
    @staticmethod
    def from_iterator(it: AsyncIterator[_Out]) -> Stream[_Out]:
        return _AsyncIter(it)

    def __init__(self) -> None:
        self._state: StreamState = StreamState.INITIAL

    async def discard(self) -> None:
        async for _ in self:
            pass

    async def collect(self) -> list[_Out]:
        result: list[_Out] = []
        async for v in self:
            result.append(v)
        return result

    @final
    async def start(self) -> None:
        if self._state == StreamState.CLOSED:
            raise RuntimeError("Stream has already been stopped")
        elif self._state != StreamState.STARTED:
            await self._start()
            self._state = StreamState.STARTED

    async def close(self) -> None:
        if self._state != StreamState.CLOSED:
            await self._close()
            self._state = StreamState.CLOSED

    def __aiter__(self) -> AsyncIterator[_Out]:
        return self

    async def __anext__(self) -> _Out:
        if self._state == StreamState.CLOSED:
            raise RuntimeError("Stream has already been stopped")
        elif self._state != StreamState.STARTED:
            await self._start()
            self._state = StreamState.STARTED
        try:
            return await self._get()

        except EndOfStream:
            await self.close()
            raise StopAsyncIteration from None
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception:
            await self.close()
            raise

    @abstractmethod
    async def _get(self) -> _Out: ...
    @abstractmethod
    async def _start(self) -> None: ...
    @abstractmethod
    async def _close(self) -> None: ...

    def peek(self) -> AsyncGenerator[_Out]:
        raise NotImplementedError("peek is not supported for this stream")

    async def to_host(self) -> Stream[_Out]:
        raise NotImplementedError("to_host is not supported for this stream")

    def map(self, fn: Callable[[_Out], _T]) -> Stream[_T]:
        return _Map(self, fn)

    def tee(self) -> tuple[Stream[_Out], Stream[_Out]]:
        return _IntoBuffer(self).tee()


@final
class _AsyncIter(Generic[_Out], Stream[_Out]):
    def __init__(
        self,
        it: AsyncIterator[_Out],
    ) -> None:
        super().__init__()
        self._it = aiter(it)

    @override
    async def _get(self) -> _Out:
        try:
            return await anext(self._it)
        except StopAsyncIteration:
            raise EndOfStream from None

    @override
    async def _start(self) -> None: ...

    @override
    async def _close(self) -> None: ...


@final
class _Map(Generic[_In, _T], Stream[_T]):
    def __init__(self, source: Stream[_In], fn: Callable[[_In], _T]) -> None:
        super().__init__()
        self._source = source
        self._fn = fn

    @override
    async def _get(self) -> _T:
        return self._fn(await self._source._get())

    @override
    async def _start(self) -> None:
        await self._source.start()

    @override
    async def _close(self) -> None:
        await self._source.close()

    @override
    async def peek(self) -> AsyncGenerator[_T]:
        async for v in self._source.peek():
            yield self._fn(v)

    @override
    async def to_host(self) -> Stream[_T]:
        self._source = await self._source.to_host()
        return self


@dataclass(slots=True)
class _Sentinel:
    exception: BaseException | type[EndOfStream]


class _BufferedStream(Generic[_T], Stream[_T]):
    def __init__(self, parent: _BufferedStream[_T] | None = None) -> None:
        super().__init__()
        self.__buffer: deque[tuple[int, _T | _Sentinel]] = deque()
        self.__subscribers: list[_BufferedStream[_T]] = []
        self.__pending_subscribers = 1
        self.__parent = parent
        self.__event = asyncio.Event()

    @override
    async def _get(self) -> _T:
        while not self.__buffer:
            self.__event.clear()
            _ = await self.__event.wait()

        _, item = self.__buffer.popleft()
        if isinstance(item, _Sentinel):
            raise item.exception
        return item

    def _send(self, offset: int, value: _T):
        self.__buffer.append((offset, value))
        self.__event.set()
        for s in self.__subscribers:
            s._send(offset, value)

    def _send_close(self, offset: int, exc: BaseException | None = None):
        self.__buffer.append((offset, _Sentinel(exc or EndOfStream)))
        self.__event.set()
        for s in self.__subscribers:
            s._send_close(offset, exc)

    async def _peek(self, offset: int) -> AsyncGenerator[_T]:
        while self.__buffer and self.__buffer[0][0] <= offset:
            _, item = self.__buffer.popleft()
            if isinstance(item, _Sentinel):
                raise item.exception
            else:
                yield item

    @override
    async def _start(self) -> None:
        if self.__parent:
            await self.__parent.start()

    @override
    async def _close(self) -> None:
        if self.__parent:
            await self.__parent.close()

    @override
    async def close(self) -> None:
        self.__pending_subscribers -= 1
        if self.__pending_subscribers == 0:
            await super().close()

    @override
    def tee(self) -> tuple[Stream[_T], Stream[_T]]:
        assert self._state == StreamState.INITIAL, "can only tee before starting"
        parent = self.__parent or self
        a: _BufferedStream[_T] = _BufferedStream(parent)
        parent.__subscribers.append(a)
        parent.__pending_subscribers += 1
        return self, a

    @override
    async def to_host(self) -> Stream[_T]:
        return self


@final
class _IntoBuffer(Generic[_T], _BufferedStream[_T]):
    def __init__(self, stream: Stream[_T]) -> None:
        super().__init__()
        self._stream = stream
        self._task: asyncio.Task[None] | None = None

    @override
    async def _start(self) -> None:
        async def pump() -> None:
            i = 0
            try:
                async for v in self._stream:
                    self._send(i, v)
                    i += 1
                self._send_close(i, None)
            except Exception as e:
                self._send_close(i, e)

        self._task = asyncio.create_task(pump())

    @override
    async def _close(self) -> None:
        if self._task:
            _ = self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await super()._close()

    @override
    async def to_host(self) -> Stream[_T]:
        self._stream = await self._stream.to_host()
        return self


@final
class ResumableStream(Generic[_In, _T], _BufferedStream[_T]):
    def __init__(
        self,
        loop: EventLoop,
        initial: _T,
        reducer: Callable[[_T, _In], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_In, _T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        super().__init__()
        self._loop = loop
        self._reducer = reducer
        self._closed: bool | BaseException = False
        self._current: _T = initial
        self._op = self._loop.create_op(
            StreamCreate(observer=cast("Observer[object]", cast("Observer[_In]", self)))
        )
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._task: asyncio.Future[object] | None = None
        self._target_loop: asyncio.AbstractEventLoop | None = None

    def on_next(self, offset: int, val: _In):
        self._current = self._reducer(self._current, val)
        self._send(offset, self._current)

    def on_close(self, offset: int, exc: BaseException | None):
        self._closed = True if exc is None else exc
        self._send_close(offset, exc)

    @override
    async def _start(self) -> None:
        stream = cast("StreamHandle[_In]", await self._op).to_host()

        async def worker():
            if self._closed is True:
                return

            gen = self._fn(self._current, *self._args, **self._kwargs)
            try:
                state_partial = await anext(gen)
                while True:
                    await stream.send(state_partial)
                    state_partial = await gen.asend(self._current)
            except StopAsyncIteration as _e:
                await stream.close()
            except Exception as e:
                await stream.close(e)
                raise
            finally:
                await gen.aclose()

        self._task = wrap_future(
            self._loop.create_op(
                FnCall(callable=worker, args=(), kwargs={}, return_type=None),
            ),
            loop=self._target_loop,
        )

    @override
    async def _close(self) -> None:
        if self._task:
            _ = self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await super()._close()

    @override
    async def to_host(self) -> Stream[_T]:
        self._target_loop = self._loop.host_loop()
        await self.start()
        return self


@final
class LogStream(Generic[_T], _BufferedStream[_T]):
    def __init__(self, loop: EventLoop) -> None:
        super().__init__()
        self._loop = loop

    def on_next(self, _offset: int, val: _T):
        self._send(_offset, val)

    def on_close(self, _offset: int, exc: BaseException | None):
        self._send_close(_offset, exc)

    @override
    async def peek(self) -> AsyncGenerator[_T]:
        assert asyncio.get_event_loop() is self._loop, (
            "peek can only be used in the context loop"
        )
        offset = cast("int", await self._loop.create_op(Barrier()))
        async for value in self._peek(offset):
            yield value
