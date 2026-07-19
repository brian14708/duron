from __future__ import annotations

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]
import asyncio
from itertools import starmap
from typing import TYPE_CHECKING
from typing_extensions import override

import pytest

import duron
from duron._history.encoding import decode_record
from duron.errors import StorageError
from duron.storage import SQLiteStore
from tests.helpers import assert_unloaded, execute

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from duron.storage import ClaimToken, Event, TerminalState


async def test_shutdown_during_append_recovers_without_background_errors() -> None:
    committed = asyncio.Event()
    errors: list[dict[str, object]] = []

    class Store(SQLiteStore):
        paused = False

        @override
        async def append(
            self, run_id: str, token: ClaimToken, records: Sequence[Event]
        ) -> Sequence[int]:
            offsets = await super().append(run_id, token, records)
            entries = starmap(decode_record, zip(records, offsets, strict=True))
            if not self.paused and any(
                entry["type"] == "promise.create" and entry["source"] == "task"
                for entry in entries
            ):
                self.paused = True
                committed.set()
                await asyncio.Event().wait()
            return offsets

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        await ctx.sleep(0)
        return 42

    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        async with Store(":memory:") as store:
            execution = asyncio.create_task(execute(flow, store, "shutdown-append"))
            try:
                await asyncio.wait_for(committed.wait(), 3)
            finally:
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
            assert await execute(flow, store, "shutdown-append") == 42
        await asyncio.sleep(0)
        assert errors == []
    finally:
        loop.set_exception_handler(previous)


@pytest.mark.parametrize("commit", ["append", "finish"])
async def test_lost_commit_acknowledgement_recovers_without_repeating_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, commit: str
) -> None:
    append, finish = SQLiteStore.append, SQLiteStore.finish
    lost_ack = False
    calls: list[bool] = []

    async def uncertain_commit(
        self: SQLiteStore,
        run_id: str,
        token: ClaimToken,
        records: Sequence[Event],
        state: TerminalState | None = None,
    ) -> Sequence[int]:
        nonlocal lost_ack
        offsets = (
            await append(self, run_id, token, records)
            if state is None
            else await finish(self, run_id, token, records, state)
        )
        if not lost_ack:
            lost_ack = True
            msg = "commit succeeded but acknowledgement was lost"
            raise StorageError(msg)
        return offsets

    monkeypatch.setattr(SQLiteStore, commit, uncertain_commit)

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        calls.append(True)
        return 42

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(work)

    path = tmp_path / "uncertain.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.submit(flow, "uncertain")
            assert await run.result() == 42
            # Observers can see the outcome before the owner's retry finishes.
            await assert_unloaded(scheduler._store, run.id)
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "uncertain")
            assert await run.result() == 42
    assert lost_ack
    assert calls == [True]
