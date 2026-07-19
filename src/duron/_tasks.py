"""Join cleanup work even when its caller is cancelled repeatedly."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from asyncio import Task

_T = TypeVar("_T")


async def finish_task(task: Task[_T]) -> _T:
    """Finish an already started task without forwarding caller cancellation.

    Call from cancellation handlers to join mandatory cleanup before re-raising
    the original cancellation. The task's own exception still propagates.

    Returns:
        The joined task's result.

    """
    while not task.done():
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.shield(task)
    return task.result()
