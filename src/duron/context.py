from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
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
    get_args,
    get_origin,
)

from duron.ops import Barrier, FnCall, create_op
from duron.stream import create_stream, resumable

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from contextvars import Token
    from types import TracebackType

    from typing_extensions import AsyncContextManager

    from duron.event_loop import EventLoop
    from duron.fn import Fn
    from duron.stream import Sink, Stream

    _T = TypeVar("_T")
    _S = TypeVar("_S")
    _P = ParamSpec("_P")

_context: ContextVar[Context | None] = ContextVar("duron_context", default=None)


@final
class Context:
    __slots__ = ("_loop", "_task", "_token")

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
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        type_info = self._task.codec.inspect_function(fn)
        op = create_op(
            self._loop,
            FnCall(
                callable=fn,
                args=args,
                kwargs=kwargs,
                return_type=type_info.return_type,
            ),
        )
        return cast("_T", await op)

    def stream(
        self,
        initial: _T,
        reducer: Callable[[_T, _S], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_S, _T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AsyncContextManager[Stream[_S]]:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        dtype: type | None = None
        return_type = self._task.codec.inspect_function(fn).return_type
        if get_origin(return_type) is AsyncGenerator:
            dtype, _ = get_args(return_type)
        r = resumable(
            self._loop,
            dtype,
            initial,
            reducer,
            fn,
            *args,
            **kwargs,
        )
        return r

    async def create_stream(self, dtype: type[_T]) -> tuple[Stream[_T], Sink[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return await create_stream(self._loop, dtype)

    async def barrier(self) -> int:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return await create_op(self._loop, Barrier())

    def time(self) -> float:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return self._loop.time()

    def time_ns(self) -> int:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return self._loop.time_us() * 1_000

    def random(self) -> Random:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context random can only be used in the context loop")
        return Random(self._loop.generate_op_id())
