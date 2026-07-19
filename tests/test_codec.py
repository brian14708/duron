from __future__ import annotations

import math
from typing import TYPE_CHECKING, cast
from typing_extensions import TypeVar

import pytest

import duron
from duron import AsyncExecutor
from duron.codec import DefaultCodec
from duron.errors import HistoryMismatchError
from tests.helpers import execute

if TYPE_CHECKING:
    from pathlib import Path

    from duron.codec import JSONValue, TypeHint
    from duron.storage import EventStore

_T = TypeVar("_T")


class IntegerCodec:
    @staticmethod
    def encode_json(value: object, annotation: TypeHint[object]) -> JSONValue:
        assert annotation is int
        assert type(value) is int
        return f"integer:{value}"

    @staticmethod
    def decode_json(encoded: JSONValue, annotation: TypeHint[_T]) -> _T:
        assert annotation is int
        assert isinstance(encoded, str)
        assert encoded.startswith("integer:")
        return cast("_T", int(encoded.removeprefix("integer:")))


def test_default_codec_rejects_nested_nonfinite_values() -> None:
    with pytest.raises(TypeError, match="not JSON-serializable"):
        DefaultCodec.encode_json({"values": [math.nan]}, object)


async def test_workflow_codec_handles_application_values_and_replay(
    backend: EventStore, tmp_path: Path
) -> None:
    class Ports(duron.PortSet):
        incoming = duron.Input[int]()
        outgoing = duron.Output[int]()

    class ProducerPorts(duron.PortSet):
        output = duron.Output[int]()
        commands = duron.Input[int]()

    calls: list[int] = []

    @duron.effect
    async def double(_ctx: duron.EffectContext, value: int, step: int = 2) -> int:
        calls.append(value)
        return value * step

    @duron.effect(executor=AsyncExecutor(), ports=ProducerPorts)
    async def produce(ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        command = await ctx.port(ProducerPorts.commands).receive()
        result = value + command
        await ctx.port(ProducerPorts.output).emit(result)
        return result

    @duron.workflow(codec=IntegerCodec(), ports=Ports)
    async def flow(ctx: duron.WorkflowContext, seed: int) -> int:
        await ctx.sleep(0)
        value = seed + await ctx.port(Ports.incoming).receive()
        value = await ctx.call(double, value)
        async with ctx.start(produce, value) as job:
            await job.port(ProducerPorts.commands).send(3)
            values = [item async for item in job.port(ProducerPorts.output)]
            result = await job
            assert values == [result]
        await ctx.port(Ports.outgoing).emit(result)
        await ctx.port(Ports.incoming).receive()
        return result

    async with duron.SQLiteScheduler(
        tmp_path / "codec.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.submit(flow.ref, "codec", 1)
        await run.port(Ports.incoming).send(4)
        assert await anext(run.port(Ports.outgoing).reader()) == 13
    async with duron.SQLiteScheduler(
        tmp_path / "codec.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.open(flow.ref, "codec")
        await run.port(Ports.incoming).send(1)
        assert await run.result() == 13
        assert [item async for item in run.port(Ports.outgoing).reader()] == [13]
    assert calls == [5, 10]

    @duron.workflow
    async def ordinary(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 4)

    assert await execute(ordinary, backend, "json") == 8


async def test_workflow_version_guards_incompatible_codec_changes(
    backend: EventStore,
) -> None:
    async def echo(_ctx: duron.WorkflowContext, value: int) -> int:
        return value

    original = duron.workflow(name="echo", version="1", codec=IntegerCodec())(echo)
    changed = duron.workflow(name="echo", version="2")(echo)
    assert await execute(original, backend, "codec-version", 4) == 4
    before = await backend.read("codec-version")
    with pytest.raises(HistoryMismatchError, match="invocation"):
        await execute(changed, backend, "codec-version", 4)
    assert await backend.read("codec-version") == before
