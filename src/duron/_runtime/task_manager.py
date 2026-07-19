from __future__ import annotations

import asyncio
import contextlib
import sys
from typing import TYPE_CHECKING, Any, final

if TYPE_CHECKING:
    import contextvars
    from collections.abc import Callable, Coroutine

    from duron._hints import TypeHint


@final
class TaskManager:
    __slots__ = (
        "_cleanup_task",
        "_closed",
        "_done_tasks",
        "_futures",
        "_on_error",
        "_pending_task",
        "_reporting",
        "_tasks",
    )

    def __init__(self, on_error: Callable[[BaseException], None]) -> None:
        self._pending_task: dict[
            str,
            tuple[
                Callable[[], Coroutine[Any, Any, None]],
                contextvars.Context,
                TypeHint[Any],
                asyncio.Future[object],
                tuple[type[BaseException], ...],
                bool,
            ],
        ] = {}
        self._tasks: dict[
            str,
            tuple[asyncio.Future[None], TypeHint[Any], tuple[type[BaseException], ...]],
        ] = {}
        self._done_tasks: asyncio.Queue[asyncio.Future[None] | None] = asyncio.Queue()
        self._futures: dict[
            str, tuple[TypeHint[Any], tuple[type[BaseException], ...]]
        ] = {}
        self._cleanup_task: asyncio.Task[None] = asyncio.create_task(self._cleanup())
        self._closed = False
        self._reporting: set[str] = set()
        self._on_error = on_error

    async def _cleanup(self) -> None:
        while True:
            done_task = await self._done_tasks.get()
            if done_task is None:
                break
            # The done callback reports infrastructure errors to the session.
            # Cleanup must still join all workers after one of them fails.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await done_task

    def add_pending(
        self,
        task_id: str,
        task_fn: Callable[[], Coroutine[Any, Any, None]],
        context: contextvars.Context,
        return_type: TypeHint[Any],
        waiter: asyncio.Future[object],
        error_types: tuple[type[BaseException], ...] = (),
        *,
        run_cancelled: bool = False,
    ) -> None:
        self._pending_task[task_id] = (
            task_fn,
            context,
            return_type,
            waiter,
            error_types,
            run_cancelled,
        )

    def _done_callback(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self._on_error(error)

    def add_future(
        self,
        task_id: str,
        return_type: TypeHint[Any],
        error_types: tuple[type[BaseException], ...] = (),
    ) -> None:
        self._futures[task_id] = (return_type, error_types)

    def cancel_task(self, task_id: str) -> None:
        t = self._tasks.get(task_id, None)
        if t and not t[0].done() and task_id not in self._reporting:
            _ = t[0].cancel()

    def reporting(self, task_id: str) -> None:
        """Separate effect cancellation from committing its selected outcome."""
        self._reporting.add(task_id)

    async def join_task(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if task is not None:
            await asyncio.shield(task[0])

    def start(self) -> None:
        for task_id, (
            task_fn,
            context,
            return_type,
            waiter,
            error_types,
            run_cancelled,
        ) in self._pending_task.items():
            if waiter.cancelled() and not run_cancelled:
                self.add_future(task_id, return_type, error_types)
                continue
            t = asyncio.create_task(task_fn(), context=context)
            t.add_done_callback(self._done_callback)
            if sys.version_info >= (3, 14):
                asyncio.future_add_to_awaited_by(t, waiter)
            self._tasks[task_id] = (t, return_type, error_types)
        self._pending_task.clear()

    async def close(self, *, graceful: bool = False) -> None:
        if self._closed:
            return
        while self._tasks:
            cancel = [(id_, task) for id_, (task, _, _) in self._tasks.items()]
            for tid, task in cancel:
                if not graceful or tid not in self._reporting:
                    _ = task.cancel()
            await asyncio.gather(*(task for _, task in cancel), return_exceptions=True)
            for tid, _ in cancel:
                if tid in self._tasks:
                    del self._tasks[tid]
        self._done_tasks.put_nowait(None)
        await asyncio.shield(self._cleanup_task)
        self._pending_task.clear()
        self._futures.clear()
        self._reporting.clear()
        self._closed = True

    def complete_task(
        self, task_id: str
    ) -> tuple[TypeHint[Any], tuple[type[BaseException], ...]]:
        self._reporting.discard(task_id)
        if p := self._pending_task.pop(task_id, None):
            _, _, return_type, _, error_types, _ = p
            return (return_type, error_types)
        if t := self._tasks.pop(task_id, None):
            fut, return_type, error_types = t
            self._done_tasks.put_nowait(fut)
            return (return_type, error_types)
        if f := self._futures.pop(task_id, None):
            return f

        msg = f"Task {task_id} not found"
        raise ValueError(msg)
