from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, cast
from typing_extensions import override

import pytest

import duron
from duron import AsyncExecutor
from duron.codec import JSONValue
from duron.errors import HistoryMismatchError, PortClosedError
from duron.storage import Event
from tests.helpers import execute, host

if TYPE_CHECKING:
    from pathlib import Path

    from duron import Invocation
    from duron._execution.models import EffectContextImpl
    from duron.storage import EventStore


class ProducerPorts(duron.PortSet):
    output = duron.Output[int]()


async def test_effect_replay_preserves_time_randomness_and_typed_dependencies(
    backend: EventStore,
) -> None:
    calls: list[int] = []
    observed: list[dict[str, JSONValue]] = []
    checkpoint = asyncio.Event()
    multiplier = 2

    @duron.effect
    async def pause(_ctx: duron.EffectContext) -> None:
        if len(observed) == 1:
            checkpoint.set()
            await asyncio.Event().wait()

    @duron.effect
    async def double(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        return value * multiplier

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> dict[str, JSONValue]:
        for invalid in (-1, float("nan"), float("inf"), True):
            with pytest.raises(ValueError, match="finite and nonnegative"):
                await ctx.sleep(invalid)
        now = await ctx.now()
        value = ctx.random().random()
        await ctx.sleep(0)
        results = await asyncio.gather(ctx.call(double, 2), ctx.call(double, 3))
        snapshot: dict[str, JSONValue] = {
            "time": now.isoformat(),
            "random": value,
            "results": list(results),
        }
        observed.append(snapshot)
        await ctx.call(pause)
        return snapshot

    async with host(flow, backend, "replay"):
        await asyncio.wait_for(checkpoint.wait(), 3)
    first = await execute(flow, backend, "replay")
    assert observed == [first, first]
    assert await execute(flow, backend, "replay") == first
    assert first["results"] == [4, 6]
    assert len(calls) == len(set(calls)) == 2


async def test_ambiguous_effect_recovers_on_each_restart(backend: EventStore) -> None:
    started = asyncio.Event()
    invocations: list[tuple[int, int, bool]] = []

    @duron.effect
    async def interrupted(ctx: duron.EffectContext) -> int:
        recovery = cast("EffectContextImpl", ctx)
        invocations.append((
            recovery.invocation_number,
            recovery.recovery_count,
            recovery.recovering,
        ))
        if recovery.recovery_count < 2:
            started.set()
            await asyncio.Event().wait()
        return 7

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(interrupted)

    for _attempt in range(2):
        started.clear()
        async with host(flow, backend, "recovery"):
            await asyncio.wait_for(started.wait(), timeout=5)

    for _ in range(2):
        assert await execute(flow, backend, "recovery") == 7
    assert invocations == [(1, 0, False), (2, 1, True), (3, 2, True)]


async def test_async_executor_subclass_runs_registered_body(
    backend: EventStore,
) -> None:
    calls: list[int] = []
    local_calls: list[int] = []

    class LocalExecutor(AsyncExecutor):
        @override
        async def execute(self, invocation: Invocation, /) -> object:
            assert "value" in invocation.arguments
            return await super().execute(invocation)

    @duron.effect(executor=LocalExecutor(), raises=(ValueError,))
    async def double(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        if value == 0:
            msg = "zero"
            raise ValueError(msg)
        return value * 2

    @duron.effect
    async def local(_ctx: duron.EffectContext, value: int) -> int:
        local_calls.append(value)
        return value + 1

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        with pytest.raises(ValueError, match="zero"):
            _ = await ctx.call(double, 0)
        return await ctx.call(double, await ctx.call(local, 2))

    for _ in range(2):
        assert await execute(flow, backend, "remote") == 6
    assert len(calls) == len(set(calls)) == 2
    assert local_calls == [2]


async def test_stream_readers_cross_prefetch_boundaries_and_replay(
    backend: EventStore,
) -> None:
    calls: list[bool] = []
    observed: list[list[list[int]]] = []
    checkpoint = asyncio.Event()

    @duron.effect
    async def pause(_ctx: duron.EffectContext) -> None:
        if len(observed) == 1:
            checkpoint.set()
            await asyncio.Event().wait()

    @duron.effect(ports=ProducerPorts)
    async def produce(ctx: duron.EffectContext) -> None:
        calls.append(True)
        for value in range(7):
            await ctx.port(ProducerPorts.output).emit(value)

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> list[list[int]]:
        async with ctx.start(produce) as job:
            second = job.port(ProducerPorts.output).reader(prefetch=2)
            assert await anext(second) == 0
            live = [value async for value in job.port(ProducerPorts.output)]
            await job
            catch_up = [
                value
                async for value in job.port(ProducerPorts.output).reader(
                    after=second.cursor, prefetch=2
                )
            ]
            observed.append([live, catch_up])
            await ctx.call(pause)
            return [live, catch_up]

    async with host(flow, backend, "stream-readers"):
        await asyncio.wait_for(checkpoint.wait(), 3)
    for _ in range(2):
        assert await execute(flow, backend, "stream-readers") == [
            list(range(7)),
            list(range(1, 7)),
        ]
    assert calls == [True]
    assert len(observed) == 2


async def test_old_history_is_rejected_without_writes(backend: EventStore) -> None:
    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 1

    assert await execute(flow, backend, "template") == 1
    records = await backend.read("template")
    header: JSONValue = None
    for _, record in records:
        header = cast("JSONValue", json.loads(record.payload))
        assert isinstance(header, dict)
        if header["type"] == "run.create":
            break
    else:
        pytest.fail("run did not persist its history header")
    assert isinstance(header, dict)
    result = header["result"]
    assert isinstance(result, dict)
    result["version"] = 1
    old = Event(record.id, json.dumps(header).encode())
    lease = await backend.claim("old", "old-claim")
    await backend.append("old", lease, [old], batch_id="old-header")
    await backend.release("old", lease)
    before = await backend.read("old")
    with pytest.raises(HistoryMismatchError, match="history format"):
        await execute(flow, backend, "old")
    assert await backend.read("old") == before


async def test_nonblocking_input_distinguishes_empty_from_closed(
    tmp_path: Path,
) -> None:
    class ClosurePorts(duron.PortSet):
        incoming = duron.Input[int]()
        ready = duron.Output[list[int]]()

    @duron.workflow(ports=ClosurePorts)
    async def flow(ctx: duron.WorkflowContext) -> int:
        await ctx.port(ClosurePorts.ready).emit(
            await ctx.port(ClosurePorts.incoming).receive_many(wait=False)
        )
        value = await ctx.port(ClosurePorts.incoming).receive()
        with pytest.raises(PortClosedError):
            await ctx.port(ClosurePorts.incoming).receive()
        with pytest.raises(PortClosedError):
            await ctx.port(ClosurePorts.incoming).receive_many(wait=False)
        return value

    async with duron.SQLiteScheduler(
        tmp_path / "closure.db", workflows=[flow]
    ) as scheduler:
        active = await scheduler.submit(flow.ref, "input-closure")
        assert await anext(active.port(ClosurePorts.ready).reader()) == []
        await active.port(ClosurePorts.incoming).send(7)
        await active.port(ClosurePorts.incoming).close()
        assert await active.result() == 7
