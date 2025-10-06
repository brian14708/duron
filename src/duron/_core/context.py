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
    overload,
)

from duron._core.ops import Barrier, FnCall, create_op
from duron._core.signal import create_signal
from duron._core.stream import create_stream, resumable

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from contextvars import Token
    from types import TracebackType

    from typing_extensions import AsyncContextManager

    from duron._core.fn import Fn
    from duron._core.options import RunOptions
    from duron._core.signal import Signal, SignalWriter
    from duron._core.stream import Stream, StreamWriter
    from duron._loop import EventLoop
    from duron.codec import JSONValue

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

    @overload
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T]],
        options: RunOptions[_T] | None = ...,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    @overload
    async def run(
        self,
        fn: Callable[_P, _T],
        options: RunOptions[_T] | None = ...,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T] | _T],
        options: RunOptions[_T] | None = None,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return_type = (
            options and options.return_type
        ) or self._task.codec.inspect_function(fn).return_type
        metadata = options.metadata if options else None
        op = create_op(
            self._loop,
            FnCall(
                callable=fn,
                args=args,
                kwargs=kwargs,
                return_type=return_type,
                metadata=metadata,
            ),
        )
        return cast("_T", await op)

    def stream(
        self,
        initial: _T,
        reducer: Callable[[_T, _S], _T],
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_S, _T]],
        options: RunOptions[_S] | None = None,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AsyncContextManager[Stream[_S]]:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        dtype: type | None = None
        if options and options.return_type:
            dtype = options.return_type
        else:
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

    async def create_stream(
        self,
        dtype: type[_T],
        *,
        external: bool = False,
        metadata: dict[str, JSONValue] | None = None,
    ) -> tuple[Stream[_T], StreamWriter[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return await create_stream(
            self._loop, dtype, external=external, metadata=metadata
        )

    async def create_signal(
        self,
        dtype: type[_T],
        *,
        metadata: dict[str, JSONValue] | None = None,
    ) -> tuple[Signal[_T], SignalWriter[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Context time can only be used in the context loop")
        return await create_signal(self._loop, dtype, metadata=metadata)

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
