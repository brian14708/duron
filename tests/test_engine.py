from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from typing_extensions import override

import pytest

import duron
from duron.errors import LeaseLostError, StorageError
from duron.storage import SQLiteStore
from tests.helpers import execute

if TYPE_CHECKING:
    from collections.abc import Sequence

    from duron.storage import Event, LeaseToken


async def test_committed_append_with_lost_acknowledgement_is_retried() -> None:
    class Store(SQLiteStore):
        lost_ack = False

        @override
        async def append(
            self,
            run_id: str,
            lease: LeaseToken,
            records: Sequence[Event],
            *,
            batch_id: str,
        ) -> Sequence[int]:
            offsets = await super().append(run_id, lease, records, batch_id=batch_id)
            if not self.lost_ack:
                self.lost_ack = True
                msg = "commit succeeded but connection dropped before acknowledgement"
                raise StorageError(msg)
            return offsets

    calls = 0

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        nonlocal calls
        calls += 1
        return 42

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(work)

    async with Store(":memory:") as store:
        assert await execute(flow, store, "uncertain") == 42
        before = await store.read("uncertain")
        assert await execute(flow, store, "uncertain") == 42
        assert await store.read("uncertain") == before
    assert calls == 1


async def test_failed_renewal_stops_local_work_and_allows_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("duron._runtime.machine._CLAIM_RENEW_INTERVAL", 0.01)
    cleaned = asyncio.Event()
    finish = asyncio.Event()

    class Store(SQLiteStore):
        lose_claim = True

        @override
        async def renew(
            self, run_id: str, lease: LeaseToken, *, ttl: float = 30.0
        ) -> None:
            if self.lose_claim:
                await self.release(run_id, lease)
                msg = "owner lost its claim"
                raise LeaseLostError(msg)
            await super().renew(run_id, lease, ttl=ttl)

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        try:
            await finish.wait()
            return 7
        finally:
            cleaned.set()

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(work)

    async with Store(":memory:") as store:
        with pytest.raises(LeaseLostError):
            await asyncio.wait_for(execute(flow, store, "recover"), timeout=2)
        assert cleaned.is_set()
        store.lose_claim = False
        finish.set()
        assert await execute(flow, store, "recover") == 7
