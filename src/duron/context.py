from __future__ import annotations

import asyncio
from contextvars import ContextVar
from random import Random
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    ParamSpec,
    TypeVar,
    cast,
    final,
)

from duron.ops import Barrier, FnCall, StreamCreate
from duron.stream import PeekStream, ResumableStream

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine
    from contextvars import Token
    from types import TracebackType

    from duron.event_loop import EventLoop
    from duron.fn import Fn
    from duron.stream import Observer, Stream, StreamHandle

    _T = TypeVar("_T")
    _S = TypeVar("_S")
    _P = ParamSpec("_P")


_context: ContextVar[Context | None] = ContextVar("duron_context", default=None)


@final
class Context:
    def __init__(self, task: Fn[..., object], loop: EventLoop) -> None:
        self._loop: EventLoop = loop
        self._task = task
        self._token: Token[Context | None] | None = None

    def __enter__(self) -> Context:
        assert self._token is None, "Context is already active"
        token = _context.set(self)
        self._token = token
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        if self._token:
            _context.reset(self._token)

    @staticmethod
    def current() -> Context:
        ctx = _context.get()
        if ctx is None:
            raise RuntimeError("No duron context is active")
        return ctx

    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        type_info = self._task.codec.inspect_function(fn)
        return cast(
            "_T",
            await self._loop.create_op(
                FnCall(
                    callable=fn,
                    args=args,
                    kwargs=kwargs,
                    return_type=type_info.return_type,
                )
            ),
        )

    def stream(
        self,
        initial: _T,
        reducer: Callable[[_T, _S], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_S, _T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Stream[_T]:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return ResumableStream(
            self._loop,
            initial,
            reducer,
            fn,
            *args,
            **kwargs,
        )

    async def barrier(self) -> int:
        return cast(
            "int",
            await self._loop.create_op(Barrier()),
        )

    async def create_stream(self, observer: Observer[_T] | None) -> StreamHandle[_T]:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        o = cast("Observer[object]", observer) if observer else None
        return cast(
            "StreamHandle[_T]",
            await self._loop.create_op(StreamCreate(observer=o)),
        )

    async def create_peek_stream(self) -> tuple[PeekStream[_T], StreamHandle[_T]]:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        ps: PeekStream[_T] = PeekStream(self._loop)
        return (
            ps,
            cast(
                "StreamHandle[_T]",
                await self._loop.create_op(
                    StreamCreate(observer=cast("Observer[object]", ps))
                ),
            ),
        )

    def time(self) -> float:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return self._loop.time()

    def time_ns(self) -> int:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return self._loop.time_ns()

    def random(self) -> Random:
        if asyncio.get_event_loop() is not self._loop:
            raise RuntimeError("Context random can only be used in the context loop")
        return Random(self._loop.generate_op_id())
