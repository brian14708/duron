from __future__ import annotations

import asyncio

import pytest

import duron
from duron.storage import MemoryStore


async def test_undeclared_dependencies_warn_once_and_replay() -> None:
    backend = MemoryStore()
    incoming = duron.Input[int]("incoming")
    outgoing = duron.Output[int]("outgoing")
    approval = duron.Request[int, bool]("approval")
    stop = duron.Interrupt[str]("stop")
    calls: list[int] = []

    @duron.effect(name="double")
    async def double(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        return value * 2

    @duron.workflow(name="dependencies")
    async def flow(ctx: duron.WorkflowContext) -> int:
        async with ctx.interruptible(stop):
            first = await ctx.receive(incoming)
            second = (await ctx.receive_many(incoming, max_items=1))[0]
            values = await asyncio.gather(
                ctx.call(double, first), ctx.call(double, second)
            )
            for value in values:
                await ctx.emit(outgoing, value)
            return sum(values) if await ctx.request(approval, first) else 0

    async def execute(*, replay: bool) -> None:
        async with duron.open_run(flow(), backend, run_id="dependencies") as run:
            if not replay:
                await run.input(incoming).send(2)
                await run.input(incoming).send(3)
                await (await run.requests(approval).next()).respond(True)
            assert await run.result() == 10
            assert await run.output(outgoing).collect() == [4, 6]

    assert flow.requires == ()
    for replay in (False, True):
        with pytest.warns(UserWarning, match="used undeclared") as caught:
            await execute(replay=replay)
        assert len(caught) == 5
        messages = [str(warning.message) for warning in caught]
        for label in (
            "Interrupt 'stop'",
            "Input 'incoming'",
            "Effect 'double'",
            "Output 'outgoing'",
            "Request 'approval'",
        ):
            assert any(label in message for message in messages)
    assert calls == [2, 3]
