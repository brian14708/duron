from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, TypeVar, cast

from duron.event_loop import EventLoop
from duron.ops import FnCall

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    _T = TypeVar("_T")


class Context:
    def __init__(self):
        loop = asyncio.get_event_loop()
        assert isinstance(loop, EventLoop)
        self._loop: EventLoop = loop
        pass

    async def run(self, fn: Callable[..., Coroutine[Any, Any, _T] | _T]) -> _T:
        return cast("_T", await self._loop.create_op(FnCall(callable=fn)))
