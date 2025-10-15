from __future__ import annotations

import asyncio
import contextvars
from contextvars import ContextVar
from random import Random
from typing import TYPE_CHECKING, cast
from typing_extensions import (
    Any,
    AsyncContextManager,
    ParamSpec,
    TypeVar,
    final,
    overload,
)

from duron._core.ops import (
    Barrier,
    ExternalPromiseComplete,
    ExternalPromiseCreate,
    FnCall,
    OpAnnotations,
    create_op,
)
from duron._core.signal import create_signal
from duron._core.stream import create_stream, run_stream
from duron._decorator.effect import CheckpointFn, EffectFn
from duron.typing import inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from contextvars import Token
    from types import TracebackType

    from duron._core.signal import Signal, SignalWriter
    from duron._core.stream import Stream, StreamWriter
    from duron._decorator.durable import DurableFn
    from duron._loop import EventLoop
    from duron.typing import TypeHint

    _T = TypeVar("_T")
    _S = TypeVar("_S")
    _P = ParamSpec("_P")

_context: ContextVar[Context | None] = ContextVar("duron.context", default=None)
_annotation: ContextVar[OpAnnotations | None] = ContextVar(
    "duron.context.annotation", default=None
)


@final
class Context:
    __slots__ = ("_fn", "_loop", "_token")

    def __init__(self, task: DurableFn[..., object], loop: EventLoop) -> None:
        self._loop: EventLoop = loop
        self._fn = task
        self._token: Token[Context | None] | None = None

    def __enter__(self) -> Context:
        assert self._token is None, "Context is already active"  # noqa: S101
        token = _context.set(self)
        self._token = token
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._token:
            _context.reset(self._token)

    @staticmethod
    def current() -> Context:
        ctx = _context.get()
        if ctx is None:
            msg = "No duron context is active"
            raise RuntimeError(msg)
        return ctx

    @overload
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T]] | EffectFn[_P, _T],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    @overload
    async def run(
        self,
        fn: Callable[_P, _T] | CheckpointFn[_P, _T, Any],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T] | _T]
        | EffectFn[_P, _T]
        | CheckpointFn[_P, _T, Any],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)

        if isinstance(fn, CheckpointFn):
            async with self.run_stream(
                cast("CheckpointFn[_P, _T, Any]", fn), *args, **kwargs
            ) as stream:
                await stream.discard()
                return await stream

        if isinstance(fn, EffectFn):
            return_type = fn.return_type
        else:
            return_type = inspect_function(fn).return_type

        callable_ = fn.fn if isinstance(fn, EffectFn) else fn
        op = create_op(
            self._loop,
            FnCall(
                callable=callable_,
                args=args,
                kwargs=kwargs,
                return_type=return_type,
                context=contextvars.copy_context(),
                annotations=OpAnnotations.extend(
                    _annotation.get(),
                    name=cast("str", getattr(callable_, "__name__", repr(callable_))),
                ),
            ),
        )
        return cast("_T", await op)

    def run_stream(
        self,
        fn: CheckpointFn[_P, _T, _S],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AsyncContextManager[Stream[_S, _T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return run_stream(
            self._loop,
            fn.action_type,
            fn.initial(),
            fn.reducer,
            fn.fn,
            *args,
            **kwargs,
        )

    async def create_stream(
        self,
        dtype: TypeHint[_T],
        /,
        *,
        name: str | None = None,
        external: bool = False,
    ) -> tuple[Stream[_T, None], StreamWriter[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return await create_stream(
            self._loop,
            dtype,
            external=external,
            annotations=OpAnnotations.extend(
                _annotation.get(),
                name=name,
                labels={"name": name} if name else None,
            ),
        )

    async def create_signal(
        self, dtype: TypeHint[_T], /, *, name: str | None = None
    ) -> tuple[Signal[_T], SignalWriter[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return await create_signal(
            self._loop,
            dtype,
            annotations=OpAnnotations.extend(
                _annotation.get(),
                labels={"name": name} if name else None,
            ),
        )

    async def create_promise(
        self, dtype: type[_T], /, *, name: str | None = None
    ) -> tuple[str, asyncio.Future[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        fut = create_op(
            self._loop,
            ExternalPromiseCreate(
                return_type=dtype,
                annotations=OpAnnotations.extend(
                    _annotation.get(),
                    name=name,
                    labels={"name": name} if name else None,
                ),
            ),
        )
        return (
            fut.id,
            cast("asyncio.Future[_T]", fut),
        )

    async def barrier(self) -> int:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return await create_op(self._loop, Barrier())

    def time(self) -> float:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return self._loop.time()

    def time_ns(self) -> int:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return self._loop.time_us() * 1_000

    def random(self) -> Random:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context random can only be used in the context loop"
            raise RuntimeError(msg)
        return Random(self._loop.generate_op_id())  # noqa: S311

    @overload
    async def complete_promise(
        self,
        id_: str,
        *,
        result: object,
    ) -> None: ...
    @overload
    async def complete_promise(
        self,
        id_: str,
        *,
        exception: Exception,
    ) -> None: ...
    async def complete_promise(
        self,
        id_: str,
        *,
        result: object | None = None,
        exception: Exception | None = None,
    ) -> None:
        await create_op(
            self._loop,
            ExternalPromiseComplete(
                promise_id=id_,
                value=result,
                exception=exception,
            ),
        )
