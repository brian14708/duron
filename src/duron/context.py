from __future__ import annotations

from contextvars import ContextVar
from random import Random
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast, final

from typing_extensions import overload

from duron.ops import FnCall

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from contextvars import Token
    from types import TracebackType

    from duron.event_loop import EventLoop
    from duron.fn import Fn

    _T = TypeVar("_T")
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

    @classmethod
    def current(cls) -> Context:
        ctx = _context.get()
        if ctx is None:
            raise RuntimeError("No duron context is active")
        return ctx

    @overload
    async def run(
        self,
        fn: Callable[_P, Awaitable[_T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    @overload
    async def run(
        self,
        fn: Callable[_P, _T],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    async def run(
        self,
        fn: Callable[_P, Awaitable[_T] | _T],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
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

    def time(self) -> float:
        return self._loop.time()

    def time_ns(self) -> int:
        return self._loop.time_ns()

    def random(self) -> Random:
        return Random(self._loop.generate_op_id())
