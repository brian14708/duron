from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from typing_extensions import override

import pytest

import duron
from duron.codec import JSONValue
from duron.errors import ApplicationError, PortClosedError
from duron.execution import AsyncExecutor
from tests.helpers import execute
from tests.modern_workflows import Ports as ModernPorts
from tests.modern_workflows import define_grouped_workflow

if TYPE_CHECKING:
    from pathlib import Path

    from duron.execution import Invocation
    from duron.storage import EventStore


class ProducerPorts(duron.PortSet):
    output = duron.Output[int]()


async def test_native_annotations_and_task_groups_recover(tmp_path: Path) -> None:
    flow = define_grouped_workflow()
    path = tmp_path / "task-group.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "grouped", {"value": 10})
        assert await anext(run.port(ModernPorts.ready).reader()) == 42
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow.ref, "grouped")
        await run.port(ModernPorts.finish).send(True)
        assert await run.result() == 42


async def test_effect_replay_preserves_time_randomness_and_typed_dependencies(
    tmp_path: Path,
) -> None:
    class Ports(duron.PortSet):
        ready = duron.Output[bool]()
        finish = duron.Input[bool]()

    calls: list[int] = []
    observed: list[dict[str, JSONValue]] = []
    multiplier = 2

    @duron.effect
    async def double(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        return value * multiplier

    @duron.workflow(ports=Ports)
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
        await ctx.port(Ports.ready).emit(True)
        await ctx.port(Ports.finish).receive()
        return snapshot

    path = tmp_path / "replay.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "replay")
        assert await anext(run.port(Ports.ready).reader()) is True
    first = observed[0]
    multiplier = 3
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow, "replay")
        await run.port(Ports.finish).send(True)
        assert await run.result() == first
    assert observed == [first, first]
    assert first["results"] == [4, 6]
    assert sorted(calls) == [2, 3]


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

    @duron.effect(executor=LocalExecutor())
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
        with pytest.raises(ApplicationError, match="zero"):
            _ = await ctx.call(double, 0)
        return await ctx.call(double, await ctx.call(local, 2))

    assert await execute(flow, backend, "custom-executor") == 6
    assert calls == [0, 3]
    assert local_calls == [2]


async def test_application_errors_survive_replay(tmp_path: Path) -> None:
    class BusinessError(Exception):
        pass

    class Ports(duron.PortSet):
        ready = duron.Output[bool]()
        finish = duron.Input[bool]()

    calls: list[bool] = []
    observed: list[bool] = []

    @duron.effect
    async def fail(_ctx: duron.EffectContext) -> None:
        calls.append(True)
        message = "business failure"
        raise BusinessError(message) from ValueError("invalid value")

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> None:
        with pytest.raises(ApplicationError, match="business failure") as caught:
            await ctx.call(fail)
        error = caught.value
        assert (
            error.type_name
            == f"{BusinessError.__module__}:{BusinessError.__qualname__}"
        )
        assert isinstance(error.cause, ApplicationError)
        assert error.cause.type_name == "builtins:ValueError"
        assert error.cause.message == "invalid value"
        observed.append(True)
        await ctx.port(Ports.ready).emit(True)
        await ctx.port(Ports.finish).receive()
        raise error

    path = tmp_path / "errors.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "errors")
        assert await anext(run.port(Ports.ready).reader()) is True
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow, "errors")
        await run.port(Ports.finish).send(True)
        with pytest.raises(ApplicationError, match="business failure") as caught:
            await run.result()
        assert caught.value.type_name.endswith(".BusinessError")
        assert isinstance(caught.value.cause, ApplicationError)
        assert caught.value.cause.type_name == "builtins:ValueError"
    assert observed == [True, True]
    assert calls == [True]


async def test_cyclic_exception_arguments_are_not_persisted(
    backend: EventStore,
) -> None:
    @duron.effect
    async def fail(_ctx: duron.EffectContext) -> None:
        value: list[object] = []
        value.append(value)
        raise ValueError(value)

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> str:
        with pytest.raises(ApplicationError, match=r"\["):
            await ctx.call(fail)
        return "caught"

    assert await execute(flow, backend, "cyclic-error") == "caught"


async def test_stream_readers_cross_prefetch_boundaries_with_independent_cursors(
    backend: EventStore,
) -> None:
    calls: list[bool] = []

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
            return [live, catch_up]

    assert await execute(flow, backend, "stream-readers") == [
        list(range(7)),
        list(range(1, 7)),
    ]
    assert calls == [True]


async def test_replay_uses_names_without_fingerprinting_code(tmp_path: Path) -> None:
    calls: list[int] = []

    class Ports(duron.PortSet):
        ready = duron.Output[int]()
        finish = duron.Input[bool]()

    @duron.effect(name="fetch")
    async def original(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        return value + 1

    @duron.effect(name="fetch")
    async def changed(_ctx: duron.EffectContext, value: int) -> int:
        raise AssertionError(value)

    def define(operation: duron.Effect[..., int]) -> duron.Workflow[..., int, Ports]:
        @duron.workflow(name="named", ports=Ports)
        async def flow(ctx: duron.WorkflowContext, value: int) -> int:
            result = await ctx.call(operation, value)
            await ctx.port(Ports.ready).emit(result)
            await ctx.port(Ports.finish).receive()
            return result

        return flow

    path = tmp_path / "names.db"
    async with duron.SQLiteScheduler(path, workflows=[define(original)]) as scheduler:
        run = await scheduler.submit(define(original).ref, "named", 1)
        assert await anext(run.port(Ports.ready).reader()) == 2
    replacement = define(changed)
    async with duron.SQLiteScheduler(path, workflows=[replacement]) as scheduler:
        run = await scheduler.open(replacement, "named")
        await run.port(Ports.finish).send(True)
        assert await run.result() == 2
    assert calls == [1]


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
