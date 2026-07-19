from __future__ import annotations

import asyncio
from collections import Counter
from typing import TYPE_CHECKING, assert_type

import pytest

import duron
from duron._history.encoding import decode_record
from duron.errors import (
    ApplicationError,
    ChannelClosedError,
    ExecutionCompletedError,
    InterruptedEffectError,
    PostConflictError,
    RunUnavailableError,
    WorkflowDefinitionError,
)
from tests.helpers import IntegerCodec, execute

if TYPE_CHECKING:
    from duron.execution import ExecutionMetadata, Executor, Invocation
    from duron.storage import EventStore


class Ports(duron.PortSet):
    commands = duron.Input[int]()
    events = duron.Output[int]()


async def test_output_keys_are_scoped_to_stream(backend: EventStore) -> None:
    class Outputs(duron.PortSet):
        first = duron.Output[int]()
        second = duron.Output[int]()

    @duron.effect(ports=Outputs)
    async def worker(ctx: duron.EffectContext) -> int:
        for port, value in ((Outputs.first, 1), (Outputs.second, 2)):
            await ctx.port(port).emit(value, idempotency_key="executor:outcome")
        return 3

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> list[int]:
        async with ctx.start(worker) as job:
            first = [value async for value in job.port(Outputs.first)]
            second = [value async for value in job.port(Outputs.second)]
            return [*first, *second, await job]

    assert await execute(flow, backend, "output-keys") == [1, 2, 3]


async def test_effect_ports_deliver_inputs_and_deduplicate_output(
    backend: EventStore,
) -> None:
    calls: list[list[int]] = []

    @duron.effect(ports=Ports)
    async def worker(ctx: duron.EffectContext) -> int:
        await asyncio.gather(
            ctx.port(Ports.events).emit(1, idempotency_key="started"),
            ctx.port(Ports.events).emit(1, idempotency_key="started"),
        )
        with pytest.raises(PostConflictError):
            await ctx.port(Ports.events).emit(2, idempotency_key="started")
        first = await ctx.port(Ports.commands).receive()
        assert_type(first, int)
        values = [first, await ctx.port(Ports.commands).receive()]
        with pytest.raises(ChannelClosedError):
            await ctx.port(Ports.commands).receive()
        calls.append(values)
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

    assert await execute(flow, backend, "recover-ports") == [1]
    assert calls == [[7, 8]]


async def test_custom_executor_ports_use_application_codec(backend: EventStore) -> None:
    class WorkerPorts(duron.PortSet):
        commands = duron.Input[int]()
        events = duron.Output[int]()

    calls: list[int] = []
    dispatched: list[Invocation] = []

    class RemoteExecutor:
        @staticmethod
        async def execute(invocation: Invocation, /) -> object:
            dispatched.append(invocation)
            assert invocation.arguments == {"seed": 1}
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

    assert await execute(flow, backend, "remote-recovery") == 3
    with pytest.raises(RunUnavailableError):
        await dispatched[0].port(WorkerPorts.events).emit(3, idempotency_key="result")
    assert calls == [3]
    assert len(dispatched) == 1


async def test_transport_failure_blocks_reopen_without_redispatch(
    backend: EventStore,
) -> None:
    invocations: list[ExecutionMetadata] = []

    class RemoteExecutor:
        @staticmethod
        async def execute(invocation: Invocation, /) -> object:
            invocations.append(invocation.metadata)
            msg = "delivery outcome unknown"
            raise ConnectionError(msg)

    @duron.effect(executor=RemoteExecutor())
    async def worker(_ctx: duron.EffectContext) -> int:
        msg = "custom adapter must not invoke the Python effect body"
        raise AssertionError(msg)

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(worker)

    with pytest.raises(ConnectionError, match="delivery outcome unknown"):
        await execute(flow, backend, "transport-recovery")
    with pytest.raises(InterruptedEffectError):
        await execute(flow, backend, "transport-recovery")
    assert len(invocations) == 1


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
        with pytest.raises(ApplicationError, match="worker failed"):  # ruff: ignore[pytest-raises-with-multiple-statements]  # Scope exit must surface the unobserved failure.
            async with ctx.start(worker) as job:
                events = job.port(Ports.events)
                assert [value async for value in events] == [1]
        assert events is not None
        with pytest.raises(RuntimeError, match="scope"):
            events.reader()

    await execute(flow, backend, "scope")
    assert calls == [True]


async def test_scope_exit_records_one_eof_for_open_and_closed_streams(
    backend: EventStore,
) -> None:
    @duron.effect(ports=Ports)
    async def closed(ctx: duron.EffectContext) -> int:
        await ctx.port(Ports.events).emit(1)
        return 1

    @duron.effect(ports=Ports)
    async def open_stream(ctx: duron.EffectContext) -> int:
        await ctx.port(Ports.events).emit(2)
        await asyncio.Event().wait()
        return 2

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext) -> None:
        async with ctx.start(closed) as job:
            assert await job == 1
        async with ctx.start(open_stream) as job:
            assert await anext(job.port(Ports.events).reader()) == 2

    await execute(flow, backend, "scope-eof")
    history = [
        decode_record(record, offset)
        for offset, record in await backend.read("scope-eof")
    ]
    streams = [
        entry["id"]
        for entry in history
        if entry["type"] == "stream.create"
        and "owned" in entry
        and isinstance(entry["owned"], dict)
        and entry["owned"].get("direction") == "stream.sender"
    ]
    assert len(streams) == 2
    closures = Counter(
        entry["stream_id"] for entry in history if entry["type"] == "stream.complete"
    )
    assert [closures[stream] for stream in streams] == [1, 1]


async def test_completed_effect_deduplicates_late_output_from_history() -> None:
    invocations: list[Invocation] = []

    class RemoteExecutor:
        @staticmethod
        async def execute(invocation: Invocation, /) -> object:
            invocations.append(invocation)
            await invocation.port(Ports.events).emit(7, idempotency_key="progress")
            return 7

    @duron.effect(ports=Ports, executor=RemoteExecutor())
    async def worker(_ctx: duron.EffectContext) -> int:
        raise AssertionError

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        async with ctx.start(worker) as job:
            value = await job
            assert [item async for item in job.port(Ports.events)] == [value]
        await ctx.port(Ports.events).emit(value)
        return await ctx.port(Ports.commands).receive()

    async with asyncio.timeout(3), duron.SQLiteScheduler(":memory:") as scheduler:
        run = await scheduler.submit(flow, "late-output")
        assert await anext(run.port(Ports.events).reader()) == 7
        emitter = invocations[0].port(Ports.events)
        await emitter.emit(7, idempotency_key="progress")
        with pytest.raises(PostConflictError):
            await emitter.emit(8, idempotency_key="progress")
        with pytest.raises(ExecutionCompletedError):
            await emitter.emit(7, idempotency_key="new")
        await run.port(Ports.commands).send(42)
        assert await run.result() == 42
