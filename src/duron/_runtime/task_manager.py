from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING, Any, final

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._runtime.state import Operation


@final
class TaskManager:
    """Host workers and cleanup; logical operations belong to the run machine."""

    def __init__(self, on_error: Callable[[BaseException], None]) -> None:
        self._pending: dict[
            str, tuple[Operation, Callable[[], Coroutine[Any, Any, None]]]
        ] = {}
        self._tasks: dict[str, tuple[asyncio.Task[None], Operation]] = {}
        self._retired: set[asyncio.Task[None]] = set()
        self._on_error = on_error

    def add_pending(
        self, operation: Operation, task_fn: Callable[[], Coroutine[Any, Any, None]]
    ) -> None:
        self._pending[operation.waiter.id] = (operation, task_fn)

    def _done_callback(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self._on_error(error)

    def cancel_task(self, task_id: str) -> None:
        worker = self._tasks.get(task_id)
        if worker is not None:
            task, operation = worker
            if not task.done() and not operation.reporting:
                task.cancel()

    async def join_task(self, task_id: str) -> None:
        worker = self._tasks.get(task_id)
        if worker is not None:
            await asyncio.shield(worker[0])

    def start(self) -> None:
        for task_id, (operation, task_fn) in tuple(self._pending.items()):
            # Shutdown may leave generated effect calls uncommitted so replay
            # can safely admit them on the next owner.
            if operation.effect is not None and operation.committed is None:
                continue
            del self._pending[task_id]
            if operation.waiter.cancelled() and operation.effect is None:
                continue
            task = asyncio.create_task(task_fn(), context=operation.parameters.context)
            task.add_done_callback(self._done_callback)
            if sys.version_info >= (3, 14):
                asyncio.future_add_to_awaited_by(task, operation.waiter)
            self._tasks[task_id] = (task, operation)
            # Effects handle cancellation even before their first coroutine step.
            # Their started flag is set by the effect body to preserve that window.
            if operation.effect is None:
                operation.started = True

    async def close(self, *, graceful: bool = False) -> None:
        while self._tasks or self._retired:
            workers = list(self._tasks.items())
            for _task_id, (task, operation) in workers:
                if not graceful or not operation.reporting:
                    task.cancel()
            await asyncio.gather(
                *(task for _, (task, _) in workers),
                *self._retired,
                return_exceptions=True,
            )
            for task_id, _worker in workers:
                self._tasks.pop(task_id, None)
        self._pending.clear()

    def complete_task(self, task_id: str) -> None:
        self._pending.pop(task_id, None)
        worker = self._tasks.pop(task_id, None)
        if worker is not None:
            task, _operation = worker
            self._retired.add(task)
            task.add_done_callback(self._retired.discard)
