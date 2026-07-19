"""Cancellation-safe acquisition of offloaded storage claims."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from duron._tasks import finish_task

if TYPE_CHECKING:
    from duron.storage import ClaimToken, EventStore


async def acquire_claim(store: EventStore, run_id: str) -> ClaimToken:
    claim = asyncio.create_task(store.claim(run_id))
    try:
        return await asyncio.shield(claim)
    except asyncio.CancelledError:
        # Offloaded claims can acquire ownership even after their waiter exits.
        async def release() -> None:
            with contextlib.suppress(Exception):
                token = await claim
                await store.release(run_id, token)

        await finish_task(asyncio.create_task(release()))
        raise
