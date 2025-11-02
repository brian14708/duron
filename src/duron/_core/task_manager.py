from __future__ import annotations

import asyncio
import contextlib
import sys
from typing import TYPE_CHECKING
from typing_extensions import Any, NamedTuple, TypeVar, final

if TYPE_CHECKING:
    import contextvars
    from collections.abc import Callable, Coroutine

    from duron.typing import TypeHint

_T = TypeVar("_T")


class TaskError(NamedTuple):
    exception: BaseException


@final
class TaskManager:
    __slots__ = (
        "_cleanup_task",
        "_done_tasks",
        "_futures",
        "_on_error",
        "_pending_task",
        "_tasks",
    )

    def __init__(self, on_error: Callable[[TaskError], Any]) -> None:
        self._pending_task: dict[
            str,
            tuple[
                Callable[[], Coroutine[Any, Any, None]],
                contextvars.Context,
                TypeHint[Any],
                asyncio.Future[object],
            ],
        ] = {}
        self._tasks: dict[str, tuple[asyncio.Future[None], TypeHint[Any]]] = {}
        self._done_tasks: asyncio.Queue[asyncio.Future[None] | None] = asyncio.Queue()
        self._futures: dict[str, TypeHint[Any]] = {}
        self._cleanup_task: asyncio.Task[None] = asyncio.create_task(self._cleanup())
        self._on_error: Callable[[TaskError], Any] = on_error

    async def _cleanup(self) -> None:
        while True:
            done_task = await self._done_tasks.get()
            if done_task is None:
                break
            await done_task

    def add_pending(
        self,
        task_id: str,
        task_fn: Callable[[], Coroutine[Any, Any, None]],
        context: contextvars.Context,
        return_type: TypeHint[Any],
        waiter: asyncio.Future[object],
    ) -> None:
        self._pending_task[task_id] = (task_fn, context, return_type, waiter)

    async def add_task(
        self,
        task_id: str,
        future: Coroutine[Any, Any, None],
        context: contextvars.Context,
        return_type: TypeHint[Any],
        waiter: asyncio.Future[object],
    ) -> None:
        t = _create_task_context(future, context=context)
        t.add_done_callback(self._done_callback)
        if sys.version_info >= (3, 14):
            asyncio.future_add_to_awaited_by(t, waiter)
        self._tasks[task_id] = (t, return_type)
        await asyncio.sleep(0)

    def _done_callback(self, t: asyncio.Task[Any]) -> None:
        if t.cancelled():
            pass
        elif (e := t.exception()) is not None:
            self._on_error(TaskError(e))

    def add_future(self, task_id: str, return_type: TypeHint[Any]) -> None:
        self._futures[task_id] = return_type

    def has_future(self, task_id: str) -> bool:
        return task_id in self._futures

    def cancel_task(self, task_id: str) -> None:
        t = self._tasks.get(task_id, None)
        if t and not t[0].done():
            _ = t[0].cancel()

    def has_pending(self, task_id: str) -> bool:
        return task_id in self._pending_task

    def start(self) -> None:
        for task_id, (
            task_fn,
            context,
            return_type,
            waiter,
        ) in self._pending_task.items():
            t = _create_task_context(task_fn(), context=context)
            t.add_done_callback(self._done_callback)
            if sys.version_info >= (3, 14):
                asyncio.future_add_to_awaited_by(t, waiter)
            self._tasks[task_id] = (t, return_type)
        self._pending_task.clear()

    async def close(self) -> None:
        self._done_tasks.put_nowait(None)
        await self._cleanup_task
        while self._tasks:
            cancel = [(id_, task) for id_, (task, _) in self._tasks.items()]
            for tid, task in cancel:
                _ = task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                if tid in self._tasks:
                    del self._tasks[tid]

    def complete_task(self, task_id: str) -> tuple[TypeHint[Any],]:
        if p := self._pending_task.pop(task_id, None):
            _, _, return_type, _ = p
            return (return_type,)
        if t := self._tasks.pop(task_id, None):
            fut, return_type = t
            self._done_tasks.put_nowait(fut)
            return (return_type,)
        if f := self._futures.pop(task_id, None):
            return (f,)

        msg = f"Task {task_id} not found"
        raise ValueError(msg)


if sys.version_info >= (3, 11):
    _create_task_context = asyncio.create_task

else:

    def _create_task_context(
        coro: Coroutine[Any, Any, _T],
        *,
        context: contextvars.Context,
        name: str | None = None,
    ) -> asyncio.Task[_T]:
        return context.run(asyncio.create_task, coro, name=name)
