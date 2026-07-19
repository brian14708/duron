from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, assert_type

import pytest

import duron
from duron._runtime.context import read_inputs
from duron.errors import PortClosedError

if TYPE_CHECKING:
    from pathlib import Path

    from duron.codec import JSONValue
    from duron.storage import EventStore


class Ports(duron.PortSet):
    left = duron.Input[int]()
    right = duron.Input[int]()
    ready = duron.Output[bool]()


async def test_select_preserves_losing_messages_and_replays(tmp_path: Path) -> None:
    observed: list[list[int | None]] = []

    @duron.effect
    async def work(_ctx: duron.EffectContext) -> int:
        await asyncio.sleep(60)
        return 99

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> list[int | None]:
        with pytest.raises(ValueError, match="distinct"):
            await ctx.select(a=ctx.port(Ports.left), b=ctx.port(Ports.left))
        first = await ctx.select(left=ctx.port(Ports.left), right=ctx.port(Ports.right))
        assert_type(first, duron.Selection[int | None])
        assert first.name == "left"
        second = await ctx.select(
            right=ctx.port(Ports.right), work=ctx.call(work), timeout=60
        )
        assert second.name == "right"
        with pytest.raises(PortClosedError):
            await ctx.select(right=ctx.port(Ports.right))
        observed.append([first.value, second.value])
        await ctx.port(Ports.ready).emit(True)
        await ctx.port(Ports.left).receive()
        return [first.value, second.value]

    async with duron.SQLiteScheduler(
        tmp_path / "select.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.submit(flow.ref, "selection")
        _ = assert_type(run, duron.Run[list[int | None], Ports])
        await run.port(Ports.left).send(1)
        await run.port(Ports.right).send(2)
        await run.port(Ports.right).close()
        assert await anext(run.port(Ports.ready).reader())
    async with duron.SQLiteScheduler(
        tmp_path / "select.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.open(flow, "selection")
        await run.port(Ports.left).send(3)
        assert await asyncio.wait_for(run.result(), 5) == [1, 2]
    assert observed == [[1, 2], [1, 2]]


async def test_select_completion_failure_and_cancellation_cleanup(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    @duron.effect
    async def work(_ctx: duron.EffectContext, name: str) -> int:
        calls.append(name)
        try:
            if name == "fail":
                msg = "failed branch"
                raise ValueError(msg)
            await asyncio.sleep(0.02 if name == "fast" else 60)
            return 1
        finally:
            calls.append(f"cleanup:{name}")

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> list[int | None]:
        selected = await ctx.select(
            fast=ctx.call(work, "fast"), slow=ctx.call(work, "slow")
        )
        assert selected.name == "fast"
        with pytest.raises(ValueError, match="failed branch"):
            await ctx.select(failure=ctx.call(work, "fail"), input=ctx.port(Ports.left))
        receiver = ctx.port(Ports.left)
        task = asyncio.create_task(ctx.select(input=receiver))
        await ctx.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await ctx.port(Ports.ready).emit(True)
        value = await ctx.port(Ports.left).receive()
        return [selected.value, value]

    async with duron.SQLiteScheduler(
        tmp_path / "cleanup.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.submit(flow.ref, "cleanup")
        assert await asyncio.wait_for(anext(run.port(Ports.ready).reader()), 5)
        await run.port(Ports.left).send(7)
        assert await asyncio.wait_for(run.result(), 5) == [1, 7]
        assert await (await scheduler.open(flow.ref, "cleanup")).result() == [1, 7]
    assert sorted(calls) == [
        "cleanup:fail",
        "cleanup:fast",
        "cleanup:slow",
        "fail",
        "fast",
        "slow",
    ]


async def test_select_timeout_keeps_original_deadline_after_suspension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed = asyncio.Event()

    async def observe(
        store: EventStore, run_id: str, port: str, after: int, *, limit: int = 1024
    ) -> list[tuple[int, JSONValue, bool]]:
        result = await read_inputs(store, run_id, port, after, limit=limit)
        observed.set()
        return result

    monkeypatch.setattr("duron._runtime.context.read_inputs", observe)

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> str:
        choice = await ctx.select(input=ctx.port(Ports.left), timeout=0.05)
        return choice.name

    async with duron.SQLiteScheduler(
        tmp_path / "deadline.db", workflows=[flow], idle_timeout=0.01
    ) as scheduler:
        await scheduler.submit(flow.ref, "deadline")
        await asyncio.wait_for(observed.wait(), 5)
    await asyncio.sleep(0.1)
    async with duron.SQLiteScheduler(
        tmp_path / "deadline.db", workflows=[flow], idle_timeout=0.01
    ) as scheduler:
        run = await scheduler.open(flow.ref, "deadline")
        assert await asyncio.wait_for(run.result(), 0.2) == "timeout"
