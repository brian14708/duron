"""Cancellation-safe acquisition of offloaded storage leases."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from duron._history.encoding import random_id

if TYPE_CHECKING:
    from duron.storage import EventStore, LeaseToken


async def acquire_lease(store: EventStore, run_id: str) -> LeaseToken:
    claim = asyncio.create_task(store.claim(run_id, random_id()))
    try:
        return await asyncio.shield(claim)
    except asyncio.CancelledError:
        # Offloaded claims can acquire ownership even after their waiter exits.
        with contextlib.suppress(Exception):
            lease = await claim
            await store.release(run_id, lease)
        raise
