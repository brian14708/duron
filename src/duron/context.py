from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar, cast

from typing_extensions import overload

from duron.event_loop import EventLoop
from duron.ops import FnCall

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    _T = TypeVar("_T")


class Context:
    def __init__(self):
        loop = asyncio.get_event_loop()
        assert isinstance(loop, EventLoop)
        self._loop: EventLoop = loop
        pass

    @overload
    async def run(self, fn: Callable[[], Awaitable[_T]]) -> _T: ...
    @overload
    async def run(self, fn: Callable[[], _T]) -> _T: ...
    async def run(self, fn: Callable[[], Awaitable[_T] | _T]) -> _T:
        return cast("_T", await self._loop.create_op(FnCall(callable=fn)))


def get_context() -> Context:
    """
    Get the current duron execution context.
    """
    return Context()
