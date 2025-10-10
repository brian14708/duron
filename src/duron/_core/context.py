from __future__ import annotations

import asyncio
import binascii
from contextlib import contextmanager
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

from duron._core.ops import Barrier, ExternalPromiseCreate, FnCall, create_op
from duron._core.signal import create_signal
from duron._core.stream import create_stream, run_stream
from duron._decorator.op import CheckpointOp, Op
from duron.typing import inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Generator
    from contextvars import Token
    from types import TracebackType

    from duron._core.signal import Signal, SignalWriter
    from duron._core.stream import Stream, StreamWriter
    from duron._decorator.fn import Fn
    from duron._loop import EventLoop
    from duron.codec import JSONValue
    from duron.typing import TypeHint

    _T = TypeVar("_T")
    _S = TypeVar("_S")
    _P = ParamSpec("_P")

_context: ContextVar[Context | None] = ContextVar("duron_context", default=None)
_metadata: ContextVar[dict[str, JSONValue] | None] = ContextVar(
    "duron_metadata", default=None
)


@final
class Context:
    __slots__ = ("_fn", "_loop", "_token")

    def __init__(self, task: Fn[..., object], loop: EventLoop) -> None:
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
        fn: Callable[_P, Coroutine[Any, Any, _T]] | Op[_P, _T],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    @overload
    async def run(
        self,
        fn: Callable[_P, _T] | CheckpointOp[_P, _T, Any],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T] | _T]
        | Op[_P, _T]
        | CheckpointOp[_P, _T, Any],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)

        if isinstance(fn, CheckpointOp):
            async with self.run_stream(fn, *args, **kwargs) as stream:
                await stream.discard()
                return await stream

        if isinstance(fn, Op):
            return_type = fn.return_type
            metadata = fn.metadata
        else:
            return_type = inspect_function(fn).return_type
            metadata = None

        op = create_op(
            self._loop,
            FnCall(
                callable=fn.fn if isinstance(fn, Op) else fn,
                args=args,
                kwargs=kwargs,
                return_type=return_type,
                metadata=self._get_metadata(metadata),
            ),
        )
        return cast("_T", await op)

    def run_stream(
        self,
        fn: CheckpointOp[_P, _T, _S],
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
            fn,
            *args,
            **kwargs,
        )

    async def create_stream(
        self,
        dtype: TypeHint[_T],
        *,
        external: bool = False,
    ) -> tuple[Stream[_T, None], StreamWriter[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return await create_stream(
            self._loop,
            dtype,
            external=external,
            metadata=self._get_metadata(None),
        )

    async def create_signal(
        self,
        dtype: TypeHint[_T],
    ) -> tuple[Signal[_T], SignalWriter[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return await create_signal(self._loop, dtype, metadata=self._get_metadata(None))

    async def create_promise(
        self,
        dtype: type[_T],
    ) -> tuple[str, asyncio.Future[_T]]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        fut = create_op(
            self._loop,
            ExternalPromiseCreate(metadata=self._get_metadata(None), return_type=dtype),
        )
        return (
            binascii.b2a_base64(fut.id, newline=False).decode(),
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

    @contextmanager
    def metadata(self, metadata: dict[str, JSONValue]) -> Generator[None, None, None]:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        if not metadata:
            yield
            return

        current = _metadata.get()
        merged = {**current, **metadata} if current is not None else metadata
        token = _metadata.set(merged)
        try:
            yield
        finally:
            _metadata.reset(token)

    @staticmethod
    def _get_metadata(
        merge: dict[str, JSONValue] | None,
    ) -> dict[str, JSONValue] | None:
        current = _metadata.get()
        if merge is None:
            return current
        if current is None:
            return merge
        return {**current, **merge}
