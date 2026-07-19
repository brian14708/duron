from __future__ import annotations

import asyncio
import contextvars
from typing import TYPE_CHECKING

import pytest

import duron
from duron._history.encoding import decode_record
from duron.errors import Interrupted, PortClosedError, WorkflowDefinitionError
from tests.helpers import execute


def test_port_contracts_reject_mutation_and_ambiguous_inheritance() -> None:
    class Left(duron.PortSet):
        incoming = duron.Input[int]()

    class Right(duron.PortSet):
        incoming = duron.Input[int]()

    with pytest.raises(WorkflowDefinitionError, match="ambiguous"):
        type("Ambiguous", (Left, Right), {})

    with pytest.raises(WorkflowDefinitionError, match="immutable"):
        Left.incoming = duron.Input[int]()
    with pytest.raises(WorkflowDefinitionError, match="immutable"):
        del Left.incoming

    class Child(Left):
        pass

    class Diamond(Child, Left):
        pass

    assert Diamond.incoming is Left.incoming


if TYPE_CHECKING:
    from pathlib import Path

    from duron.storage import EventStore


async def test_concurrent_port_creation_with_shared_context_replays(
    backend: EventStore,
) -> None:
    class Ports(duron.PortSet):
        events = duron.Output[int]()

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> None:
        context = contextvars.copy_context()
        await asyncio.gather(
            *(
                asyncio.create_task(ctx.port(Ports.events).emit(value), context=context)
                for value in (1, 2)
            )
        )

    await execute(flow, backend, "shared-context")
    before = await backend.read("shared-context")
    entries = [decode_record(record, offset) for offset, record in before]
    assert [entry["value"] for entry in entries if entry["type"] == "stream.emit"] == [
        1,
        2,
    ]
    assert sum(entry["type"] == "stream.create" for entry in entries) == 1
    await execute(flow, backend, "shared-context")
    assert await backend.read("shared-context") == before


async def test_interruptible_input_consumes_queued_and_live_values_and_replays(
    tmp_path: Path,
) -> None:
    class Ports(duron.PortSet):
        stop = duron.Input[str]()
        phase = duron.Input[bool]()
        ready = duron.Output[str]()

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> list[str]:
        assert await ctx.port(Ports.stop).receive_many(wait=False) == []
        await ctx.port(Ports.ready).emit("queue")
        _ = await ctx.port(Ports.phase).receive()
        with pytest.raises(Interrupted) as queued:
            async with ctx.port(Ports.stop).interruptible():
                pytest.fail("queued input did not interrupt on entry")
        assert await ctx.port(Ports.stop).receive() == "next"

        receiver = asyncio.create_task(ctx.port(Ports.stop).receive())
        await ctx.sleep(0)
        with pytest.raises(WorkflowDefinitionError, match="active consumer"):
            async with ctx.port(Ports.stop).interruptible():
                pytest.fail("overlapping consumer was accepted")
        _ = receiver.cancel()
        _ = await asyncio.gather(receiver, return_exceptions=True)

        try:
            async with ctx.port(Ports.stop).interruptible():
                with pytest.raises(WorkflowDefinitionError, match="consumer"):
                    _ = await ctx.port(Ports.stop).receive()
                await ctx.port(Ports.ready).emit("armed")
                await ctx.sleep(3600)
        except Interrupted as error:
            live = str(error.value)
        else:
            pytest.fail("live input did not interrupt")
        assert await ctx.port(Ports.stop).receive() == "after"

        async with ctx.port(Ports.stop).interruptible():
            await ctx.sleep(0)
        try:
            async with ctx.port(Ports.stop).interruptible():
                await ctx.port(Ports.ready).emit("closing")
                await ctx.sleep(3600)
        except PortClosedError:
            pass
        else:
            pytest.fail("input closure did not stop the scope")
        with pytest.raises(PortClosedError):
            async with ctx.port(Ports.stop).interruptible():
                pytest.fail("closed input was accepted")
        return [str(queued.value.value), live]

    async with duron.SQLiteScheduler(
        tmp_path / "interrupt.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.submit(flow.ref, "interrupt-input")
        reader = run.port(Ports.ready).reader()
        assert await anext(reader) == "queue"
        await run.port(Ports.stop).send("queued")
        await run.port(Ports.stop).send("next")
        await run.port(Ports.phase).send(True)
        assert await anext(reader) == "armed"
        await run.port(Ports.stop).send("live")
        await run.port(Ports.stop).send("after")
        assert await anext(reader) == "closing"
        await run.port(Ports.stop).close()
        assert await run.result() == ["queued", "live"]
        assert await (await scheduler.open(flow.ref, "interrupt-input")).result() == [
            "queued",
            "live",
        ]


async def test_interruptible_input_resumes_after_host_shutdown(tmp_path: Path) -> None:
    class Ports(duron.PortSet):
        stop = duron.Input[str]()
        ready = duron.Output[bool]()

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> str:
        try:
            async with ctx.port(Ports.stop).interruptible():
                await ctx.port(Ports.ready).emit(True)
                await ctx.sleep(3600)
        except Interrupted as error:
            return str(error.value)
        return "unexpected"

    async def stop_host() -> None:
        async with duron.SQLiteScheduler(
            tmp_path / "resume.db", workflows=[flow]
        ) as scheduler:
            run = await scheduler.submit(flow.ref, "resume-input")
            assert await anext(run.port(Ports.ready).reader())
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await stop_host()
    async with duron.SQLiteScheduler(
        tmp_path / "resume.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.open(flow.ref, "resume-input")
        await run.port(Ports.stop).send("resumed")
        assert await run.result() == "resumed"
