from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

import duron
from duron._runtime.machine import RunMachine
from duron.errors import FailureKind, RunFailedError
from duron.storage import SQLiteStore

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from duron.storage import ClaimToken, Event


async def test_shutdown_drains_effect_and_defers_next_call(tmp_path: Path) -> None:
    started, finish = asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    @duron.effect
    async def first(_ctx: duron.EffectContext) -> int:
        calls.append("first")
        started.set()
        await finish.wait()
        return 1

    @duron.effect
    async def second(_ctx: duron.EffectContext) -> int:
        calls.append("second")
        return 2

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(first) + await ctx.call(second)

    path = tmp_path / "drain.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.submit(flow, "operation")
            await started.wait()
            shutdown = asyncio.create_task(scheduler.shutdown())
            await asyncio.sleep(0)
            assert not shutdown.done()
            with pytest.raises(RuntimeError, match="shutting down"):
                await scheduler.submit(flow, "rejected")
            assert (await run.status()).state is duron.RunState.RUNNING
            finish.set()
            assert await shutdown == ()
            assert calls == ["first"]
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            assert await run.result() == 3
            assert calls == ["first", "second"]


async def test_context_exit_drains_effect_and_commits_result(tmp_path: Path) -> None:
    started, finish = asyncio.Event(), asyncio.Event()
    calls: list[bool] = []

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        calls.append(True)
        started.set()
        await finish.wait()
        return 42

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(work)

    path = tmp_path / "context.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            await scheduler.submit(flow, "operation")
            await started.wait()
            asyncio.get_running_loop().call_soon(finish.set)
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            assert await run.result() == 42
            assert calls == [True]


async def test_shutdown_timeout_awaits_cleanup_and_reports_forced_runs(
    tmp_path: Path,
) -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> None:
        await ctx.call(work)

    path = tmp_path / "timeout.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            await scheduler.submit(flow, "operation")
            await started.wait()
            assert await scheduler.shutdown(timeout=0.01) == ("operation",)
            assert cleaned.is_set()
            assert await scheduler.shutdown() == ("operation",)
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            with pytest.raises(RunFailedError) as error:
                await run.result()
            assert error.value.kind is FailureKind.INTERRUPTED_EFFECT


async def test_concurrent_shutdown_and_cancelled_waiter_share_cleanup() -> None:
    started, finish = asyncio.Event(), asyncio.Event()

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> None:
        started.set()
        await finish.wait()

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> None:
        await ctx.call(work)

    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(":memory:") as scheduler:
            await scheduler.submit(flow, "operation")
            await started.wait()
            first = asyncio.create_task(scheduler.shutdown())
            await asyncio.sleep(0)
            second = asyncio.create_task(scheduler.shutdown(timeout=0))
            await asyncio.sleep(0)
            first.cancel()
            await asyncio.sleep(0)
            assert not first.done()
            assert not second.done()
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await first
            assert await second == ()


async def test_shutdown_preserves_workflow_coordination_for_active_effect(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()

    class Ports(duron.PortSet):
        commands = duron.Input[int]()

    @duron.effect(ports=Ports)
    async def work(ctx: duron.EffectContext) -> int:
        started.set()
        return await ctx.port(Ports.commands).receive()

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        async with ctx.start(work) as job:
            await ctx.sleep(0.01)
            await job.port(Ports.commands).send(42)
            return await job

    path = tmp_path / "coordination.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            await scheduler.submit(flow, "operation")
            await started.wait()
            assert await scheduler.shutdown() == ()
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            assert await run.result() == 42


async def test_shutdown_during_replay_defers_uncommitted_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    replaying, proceed = asyncio.Event(), asyncio.Event()
    calls: list[bool] = []
    original = RunMachine[object].replay

    async def replay(self: RunMachine[object]) -> None:
        replaying.set()
        await proceed.wait()
        await original(self)

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        calls.append(True)
        return 42

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(work)

    monkeypatch.setattr(RunMachine, "replay", replay)
    path = tmp_path / "replay.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            await scheduler.submit(flow, "operation")
            await replaying.wait()
            shutdown = asyncio.create_task(scheduler.shutdown())
            await asyncio.sleep(0)
            proceed.set()
            assert await shutdown == ()
            assert calls == []
        monkeypatch.setattr(RunMachine, "replay", original)
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            assert await run.result() == 42
            assert calls == [True]


async def test_shutdown_waits_for_accepted_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitting, proceed = asyncio.Event(), asyncio.Event()
    original = SQLiteStore.job_submit

    async def submit(
        self: SQLiteStore,
        namespace: str,
        run_id: str,
        contract: bytes,
        inputs: bytes,
        seed: str,
    ) -> str:
        submitting.set()
        await proceed.wait()
        return await original(self, namespace, run_id, contract, inputs, seed)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    monkeypatch.setattr(SQLiteStore, "job_submit", submit)
    path = tmp_path / "submission.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            submission = asyncio.create_task(scheduler.submit(flow, "operation"))
            await submitting.wait()
            shutdown = asyncio.create_task(scheduler.shutdown())
            await asyncio.sleep(0)
            assert not shutdown.done()
            proceed.set()
            assert (await submission).id == "operation"
            assert await shutdown == ()
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            assert await run.result() == 42


async def test_shutdown_waits_for_inflight_effect_call_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committing, proceed = asyncio.Event(), asyncio.Event()
    started, finish = asyncio.Event(), asyncio.Event()
    original = SQLiteStore.append
    calls: list[bool] = []

    async def append(
        self: SQLiteStore, run_id: str, token: ClaimToken, records: Sequence[Event]
    ) -> Sequence[int]:
        offsets = await original(self, run_id, token, records)
        if not committing.is_set():
            committing.set()
            await proceed.wait()
        return offsets

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        calls.append(True)
        started.set()
        await finish.wait()
        return 42

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(work)

    monkeypatch.setattr(SQLiteStore, "append", append)
    path = tmp_path / "commit.db"
    async with asyncio.timeout(3):
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            await scheduler.submit(flow, "operation")
            await committing.wait()
            shutdown = asyncio.create_task(scheduler.shutdown())
            await asyncio.sleep(0)
            assert not shutdown.done()
            proceed.set()
            await started.wait()
            assert not shutdown.done()
            finish.set()
            assert await shutdown == ()
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            assert await run.result() == 42
            assert calls == [True]


async def test_shutdown_during_rejected_idle_suspension_does_not_lose_wakeup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    suspending, proceed = asyncio.Event(), asyncio.Event()

    async def suspend(
        _self: duron.SQLiteScheduler, _run_id: str, _deadline: float | None
    ) -> bool:
        suspending.set()
        await proceed.wait()
        # Simulate a concurrent delivery preventing durable idle suspension.
        return False

    class Ports(duron.PortSet):
        commands = duron.Input[int]()

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.port(Ports.commands).receive()

    path = tmp_path / "suspending.db"
    async with asyncio.timeout(3):
        with monkeypatch.context() as patch:
            patch.setattr("duron._runtime.host.SQLiteScheduler._job_suspend", suspend)
            async with duron.SQLiteScheduler(
                path, workflows=[flow], idle_timeout=0
            ) as scheduler:
                await scheduler.submit(flow, "operation")
                await suspending.wait()
                shutdown = asyncio.create_task(scheduler.shutdown(timeout=0.5))
                await asyncio.sleep(0)
                proceed.set()
                assert await shutdown == ()
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            run = await scheduler.open(flow, "operation")
            await run.port(Ports.commands).send(42)
            assert await run.result() == 42


@pytest.mark.parametrize("invalid_timeout", [-1.0, float("inf"), float("nan"), True])
async def test_shutdown_rejects_invalid_timeouts(invalid_timeout: float) -> None:
    async with duron.SQLiteScheduler(":memory:") as scheduler:
        with pytest.raises(ValueError, match="timeout"):
            await scheduler.shutdown(timeout=invalid_timeout)
