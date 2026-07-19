from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, assert_type

import pytest

import duron
from duron.errors import (
    ChannelClosedError,
    PostConflictError,
    RunUnavailableError,
    WorkflowDefinitionError,
)
from tests.helpers import execute, host
from tests.test_codec import IntegerCodec

if TYPE_CHECKING:
    from duron import ExecutionMetadata, Executor, Invocation
    from duron.storage import EventStore


class Ports(duron.PortSet):
    commands = duron.Input[int]()
    events = duron.Output[int]()


async def test_effect_recovery_replays_all_inputs_and_deduplicates_output(
    backend: EventStore,
) -> None:
    received = asyncio.Event()
    calls: list[list[int]] = []

    @duron.effect(ports=Ports)
    async def worker(ctx: duron.EffectContext) -> int:
        await ctx.port(Ports.events).emit(1, idempotency_key="started")
        with pytest.raises(PostConflictError):
            await ctx.port(Ports.events).emit(2, idempotency_key="started")
        first = await ctx.port(Ports.commands).receive()
        assert_type(first, int)
        values = [first, await ctx.port(Ports.commands).receive()]
        with pytest.raises(ChannelClosedError):
            await ctx.port(Ports.commands).receive()
        calls.append(values)
        if len(calls) == 1:
            received.set()
            await asyncio.Event().wait()
        return sum(values)

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> list[int]:
        async with ctx.start(worker) as job:
            assert_type(job.port(Ports.events), duron.Reader[int])
            await job.port(Ports.commands).send(7)
            await job.port(Ports.commands).send(8)
            await job.port(Ports.commands).close()
            events = [value async for value in job.port(Ports.events)]
            assert assert_type(await job, int) == 15
            return events

    async with host(flow, backend, "recover-ports"):
        await asyncio.wait_for(received.wait(), 5)
    assert await asyncio.wait_for(execute(flow, backend, "recover-ports"), 5) == [1]
    assert calls == [[7, 8], [7, 8]]


async def test_remote_effect_ports_recover_with_application_codec(
    backend: EventStore,
) -> None:
    class WorkerPorts(duron.PortSet):
        commands = duron.Input[int]()
        events = duron.Output[int]()

    answered = asyncio.Event()
    calls: list[int] = []
    dispatched: list[Invocation] = []
    invocations: list[ExecutionMetadata] = []

    class RemoteExecutor:
        @staticmethod
        async def execute(invocation: Invocation, /) -> object:
            if dispatched:
                with pytest.raises(RunUnavailableError):
                    await (
                        dispatched[0]
                        .port(WorkerPorts.events)
                        .emit(0, idempotency_key="stale")
                    )
            dispatched.append(invocation)
            assert invocation.arguments == {"seed": 1}
            invocations.append(invocation.metadata)
            assert invocation.metadata.run_id == "remote-recovery"
            command = await assert_type(
                invocation.port(WorkerPorts.commands), duron.EffectReceiver[int]
            ).receive()
            seed = invocation.arguments["seed"]
            assert isinstance(seed, int)
            value = seed + command
            await assert_type(
                invocation.port(WorkerPorts.events), duron.EffectEmitter[int]
            ).emit(value, idempotency_key="result")
            with pytest.raises(WorkflowDefinitionError, match="not bound"):
                invocation.port(Ports.events)
            calls.append(value)
            if len(calls) == 1:
                answered.set()
                await asyncio.Event().wait()
            return value

    executor: Executor = RemoteExecutor()

    @duron.effect(ports=WorkerPorts, executor=executor)
    async def worker(_ctx: duron.EffectContext, seed: int = 1) -> int:
        msg = f"custom adapter must not invoke the Python effect body (seed={seed})"
        raise AssertionError(msg)

    @duron.workflow(codec=IntegerCodec())
    async def flow(ctx: duron.WorkflowContext) -> int:
        async with ctx.start(worker) as job:
            await job.port(WorkerPorts.commands).send(2)
            assert [value async for value in job.port(WorkerPorts.events)] == [3]
            return await job

    async with host(flow, backend, "remote-recovery"):
        await asyncio.wait_for(answered.wait(), 5)
    assert await asyncio.wait_for(execute(flow, backend, "remote-recovery"), 5) == 3
    assert calls == [3, 3]
    first, second = invocations
    assert (first.run_seed, first.operation_id) == (
        second.run_seed,
        second.operation_id,
    )
    assert (first.invocation_number, first.recovery_count, first.recovering) == (
        1,
        0,
        False,
    )
    assert (second.invocation_number, second.recovery_count, second.recovering) == (
        2,
        1,
        True,
    )


async def test_transport_failure_recovers_with_awaited_result(
    backend: EventStore,
) -> None:
    invocations: list[ExecutionMetadata] = []

    class RemoteExecutor:
        @staticmethod
        async def execute(invocation: Invocation, /) -> object:
            invocations.append(invocation.metadata)
            if len(invocations) == 1:
                msg = "delivery outcome unknown"
                raise ConnectionError(msg)
            return 7

    @duron.effect(executor=RemoteExecutor())
    async def worker(_ctx: duron.EffectContext) -> int:
        msg = "custom adapter must not invoke the Python effect body"
        raise AssertionError(msg)

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(worker)

    with pytest.raises(ConnectionError, match="delivery outcome unknown"):
        await execute(flow, backend, "transport-recovery")
    assert await asyncio.wait_for(execute(flow, backend, "transport-recovery"), 5) == 7
    assert [invocation.invocation_number for invocation in invocations] == [1, 2]
    assert invocations[0].operation_id == invocations[1].operation_id


async def test_effect_run_scope_owns_failures_and_rejects_escaped_endpoints(
    backend: EventStore,
) -> None:
    calls: list[bool] = []

    @duron.effect(ports=Ports)
    async def worker(ctx: duron.EffectContext) -> int:
        calls.append(True)
        await ctx.port(Ports.events).emit(1)
        msg = "worker failed"
        raise ValueError(msg)

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> None:
        events: duron.Reader[int] | None = None
        with pytest.raises(ValueError, match="worker failed"):  # ruff: ignore[pytest-raises-with-multiple-statements]  # Scope exit must surface the unobserved failure.
            async with ctx.start(worker) as job:
                events = job.port(Ports.events)
                assert [value async for value in events] == [1]
        assert events is not None
        with pytest.raises(RuntimeError, match="scope"):
            events.reader()

    for _ in range(2):
        await execute(flow, backend, "scope")
    assert calls == [True]
