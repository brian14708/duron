from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, final

from duron.event_loop import create_loop
from duron.ops import FnCall

if TYPE_CHECKING:
    from duron.ops import Op

_T = TypeVar("_T")


class TaskRunner:
    def __init__(self):
        pass

    async def run(self, task_id: bytes, task_co: Coroutine[Any, Any, _T]) -> _T:
        return await _Task[_T](task_id, task_co).run()


@final
class _Task(Generic[_T]):
    def __init__(self, id: bytes, task_co: Coroutine[Any, Any, _T]) -> None:
        self._loop = create_loop(id)
        self._task = self._loop.create_task(task_co)

    def now(self) -> int:
        return time.time_ns()

    async def run(self) -> _T:
        self._loop.tick(self.now())
        while (waitset := self._loop.poll_completion(self._task)) is not None:
            for op in waitset.ops:
                await self.enqueue_op(op.id, op.params)
            await waitset.wait(self.now())
            self._loop.tick(self.now())
        return self._task.result()

    async def enqueue_op(self, id: bytes, op: Op | object) -> None:
        match op:
            case FnCall():
                try:
                    result = op.callable()
                    if isinstance(result, Coroutine):

                        def cb(t: asyncio.Future[object]) -> None:
                            try:
                                res = t.result()
                                self._loop.post_completion_threadsafe(
                                    id,
                                    result=res,
                                )
                            except BaseException as e:
                                self._loop.post_completion_threadsafe(
                                    id,
                                    exception=e,
                                )

                        asyncio.create_task(
                            cast("Coroutine[Any, Any, object]", result)
                        ).add_done_callback(cb)
                    else:
                        self._loop.post_completion_threadsafe(
                            id,
                            result=result,
                        )
                except BaseException as e:
                    self._loop.post_completion_threadsafe(
                        id,
                        exception=e,
                    )

            case _:
                raise NotImplementedError(f"Unsupported op: {op!r}")
