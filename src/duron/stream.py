from __future__ import annotations

import asyncio
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

from duron.ops import FnCall, StreamClose, StreamCreate, StreamEmit

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from duron.event_loop import EventLoop

    _P = ParamSpec("_P")

_In = TypeVar("_In", contravariant=True)
_Out = TypeVar("_Out", covariant=True)


class Observer(Generic[_In], Protocol):
    def on_next(self, value: _In, /) -> None: ...
    def on_close(self, error: BaseException | None, /) -> None: ...


class AmbientRawStream(Protocol[_In]):
    async def send(self, value: _In, /) -> None: ...

    async def close(self, error: BaseException | None = None, /) -> None: ...


@final
class RawStream(Generic[_In]):
    def __init__(self, id: str, loop: EventLoop) -> None:
        self._stream_id = id
        self._loop = loop
        self._target_loop: asyncio.AbstractEventLoop | None = None
        self._event = asyncio.Event()

    async def send(self, value: _In, /) -> None:
        _ = await self._loop.create_op(
            StreamEmit(stream_id=self._stream_id, value=value),
            loop=self._target_loop,
        )

    async def close(self, error: BaseException | None = None, /) -> None:
        _ = await self._loop.create_op(
            StreamClose(stream_id=self._stream_id, exception=error),
            loop=self._target_loop,
        )
        self._event.set()

    async def wait(self) -> None:
        _ = await self._event.wait()

    def to_ambient(self) -> AmbientRawStream[_In]:
        s: RawStream[_In] = RawStream(self._stream_id, self._loop)
        s._target_loop = self._loop.ambient_loop()
        return s


@final
class _StreamObserver(Generic[_In, _Out], Observer[_In]):
    def __init__(self, initial: _Out, reducer: Callable[[_Out, _In], _Out]):
        self.current = initial
        self.enable = True
        self._reducer = reducer
        self.data: list[_Out] = []
        self.closed: bool | BaseException = False

    @override
    def on_next(self, val: _In):
        if self.enable:
            self.current = self._reducer(self.current, val)
            self.data.append(self.current)

    @override
    def on_close(self, exc: BaseException | None):
        self.closed = True if exc is None else exc


@final
class StreamTask(Generic[_In, _Out]):
    def __init__(
        self,
        loop: EventLoop,
        initial: _Out,
        reducer: Callable[[_Out, _In], _Out],
        fn: Callable[Concatenate[_Out, _P], AsyncGenerator[_In, _Out]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        self._loop = loop
        self._reducer = reducer
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._obs = _StreamObserver(initial, self._reducer)
        self._op = self._loop.create_op(
            StreamCreate(observer=cast("Observer[object]", self._obs))
        )
        self._queue: asyncio.Queue[tuple[_Out] | None | BaseException] | None = None

    def __aiter__(self) -> StreamTask[_In, _Out]:
        return self

    async def __anext__(self) -> _Out:
        if self._queue is None:
            self._queue = await self._setup_stream()

        item = await self._queue.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item[0]

    async def discard(self) -> None:
        async for _ in self:
            ...

    async def _setup_stream(self) -> asyncio.Queue[tuple[_Out] | None | BaseException]:
        queue: asyncio.Queue[tuple[_Out] | None | BaseException] = asyncio.Queue()
        stream = cast("RawStream[_In]", await self._op).to_ambient()

        async def worker():
            try:
                state = self._obs.current
                self._obs.enable = False

                for d in self._obs.data:
                    await queue.put((d,))
                if self._obs.closed is True:
                    return
                elif isinstance(self._obs.closed, BaseException):
                    raise self._obs.closed

                gen = self._fn(state, *self._args, **self._kwargs)
                state_partial = await gen.__anext__()

                while True:
                    state = self._reducer(state, state_partial)
                    await stream.send(state_partial)
                    await queue.put((state,))
                    state_partial = await gen.asend(state)
            except StopAsyncIteration as _e:
                await stream.close()
            except BaseException as e:
                await queue.put(e)
                raise
            finally:
                await queue.put(None)

        _ = self._loop.create_op(
            FnCall(callable=worker, args=(), kwargs={}, return_type=None)
        )
        return queue
