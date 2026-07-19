from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, cast, get_origin
from typing_extensions import TypeVar

import pytest

import duron
from duron.codec import DefaultCodec
from duron.errors import ApplicationError, ContractMismatchError, HistoryMismatchError
from duron.execution import AsyncExecutor
from tests.helpers import IntegerCodec, execute

if TYPE_CHECKING:
    from pathlib import Path

    from duron.codec import JSONValue, TypeHint
    from duron.storage import EventStore

_T = TypeVar("_T")


async def test_annotated_workflow_arguments_keep_codec_metadata_on_recovery(
    tmp_path: Path,
) -> None:
    class Ports(duron.PortSet):
        ready = duron.Output[int]()
        finish = duron.Input[bool]()

    class TaggedCodec:
        @staticmethod
        def encode_json(value: object, annotation: TypeHint[object]) -> JSONValue:
            if get_origin(annotation) is Annotated:
                return {"tag": DefaultCodec.encode_json(value, annotation)}
            return DefaultCodec.encode_json(value, annotation)

        @staticmethod
        def decode_json(encoded: JSONValue, annotation: TypeHint[_T]) -> _T:
            if get_origin(annotation) is Annotated:
                assert isinstance(encoded, dict)
                return DefaultCodec.decode_json(encoded["tag"], annotation)
            return DefaultCodec.decode_json(encoded, annotation)

    observed: list[int] = []

    @duron.workflow(codec=TaggedCodec(), ports=Ports)
    async def flow(
        ctx: duron.WorkflowContext,
        value: Annotated[int, "tag"],
        /,
        *,
        increment: Annotated[int, "tag"] = 2,
    ) -> int:
        result = value + increment
        observed.append(result)
        await ctx.port(Ports.ready).emit(result)
        await ctx.port(Ports.finish).receive()
        return result

    path = tmp_path / "annotated.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "annotated", 40)
        assert await anext(run.port(Ports.ready).reader()) == 42
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow.ref, "annotated")
        await run.port(Ports.finish).send(True)
        assert await run.result() == 42
    assert observed == [42, 42]


async def test_workflow_can_handle_output_encoding_errors_on_recovery(
    tmp_path: Path,
) -> None:
    class Ports(duron.PortSet):
        output = duron.Output[int]()
        finish = duron.Input[bool]()

    class RejectingCodec:
        @staticmethod
        def encode_json(value: object, annotation: TypeHint[object]) -> JSONValue:
            if type(value) is int and value in {0, 1}:
                msg = "rejected output"
                if value == 0:
                    raise ValueError(msg)
                raise TypeError(msg)
            return DefaultCodec.encode_json(value, annotation)

        @staticmethod
        def decode_json(encoded: JSONValue, annotation: TypeHint[_T]) -> _T:
            return DefaultCodec.decode_json(encoded, annotation)

    handled: list[int] = []

    @duron.workflow(codec=RejectingCodec(), ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        for value in (0, 1):
            try:
                await ctx.port(Ports.output).emit(value)
            except (TypeError, ValueError):
                handled.append(value)
        await ctx.port(Ports.output).emit(2)
        await ctx.port(Ports.finish).receive()
        return 2

    path = tmp_path / "output-errors.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "output-errors")
        assert await anext(run.port(Ports.output).reader()) == 2
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow.ref, "output-errors")
        await run.port(Ports.finish).send(True)
        assert await run.result() == 2
        assert [value async for value in run.port(Ports.output)] == [2]
    assert handled == [0, 1, 0, 1]


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


async def test_replay_rejects_changed_encoded_arguments(backend: EventStore) -> None:
    async def echo(_ctx: duron.WorkflowContext, value: int) -> int:
        return value

    original = duron.workflow(name="echo", codec=IntegerCodec())(echo)
    changed = duron.workflow(name="echo")(echo)
    assert await execute(original, backend, "codec-arguments", 4) == 4
    before = await backend.read("codec-arguments")
    with pytest.raises(HistoryMismatchError, match="invocation"):
        await execute(changed, backend, "codec-arguments", 4)
    assert await backend.read("codec-arguments") == before


def test_default_codec_rejects_non_canonical_values() -> None:
    cycle: list[object] = []
    cycle.append(cycle)
    for value in ({"values": [math.nan]}, "\ud800", 1 << 5000, cycle):
        with pytest.raises(TypeError, match="not JSON-serializable"):
            DefaultCodec.encode_json(value, object)


async def test_recreated_custom_defaults_reopen_runs(tmp_path: Path) -> None:
    @dataclass
    class Config:
        value: int

    class ConfigCodec:
        @staticmethod
        def encode_json(value: object, annotation: TypeHint[object]) -> JSONValue:
            if isinstance(value, Config):
                return value.value
            return DefaultCodec.encode_json(value, annotation)

        @staticmethod
        def decode_json(encoded: JSONValue, annotation: TypeHint[_T]) -> _T:
            if annotation is object:
                assert type(encoded) is int
                return cast("_T", Config(encoded))
            return DefaultCodec.decode_json(encoded, annotation)

    def define(value: int) -> duron.Workflow[..., int]:
        default = Config(value)

        @duron.workflow(name="stable-default", codec=ConfigCodec())
        async def flow(_ctx: duron.WorkflowContext, config: object = default) -> int:
            assert isinstance(config, Config)
            return config.value

        return flow

    original, recreated = define(7), define(7)
    changed_default = define(8)
    path = tmp_path / "defaults.db"
    async with duron.SQLiteScheduler(path, workflows=[original]) as scheduler:
        run = await scheduler.submit(original, "default")
        assert await run.result() == 7
    async with duron.SQLiteScheduler(path, workflows=[recreated]) as scheduler:
        run = await scheduler.open(recreated, "default")
        assert await run.result() == 7
        with pytest.raises(ContractMismatchError):
            await scheduler.submit(changed_default.ref, "default")


async def test_application_error_details_survive_replay(tmp_path: Path) -> None:
    class Ports(duron.PortSet):
        ready = duron.Output[bool]()
        finish = duron.Input[bool]()

    codec = IntegerCodec()
    calls: list[bool] = []
    observed: list[int] = []

    @duron.effect
    async def fail(_ctx: duron.EffectContext) -> None:
        calls.append(True)
        type_name = "payment_rejected"
        raise ApplicationError(
            type_name, "try again", details=codec.encode_json(409, int)
        )

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> None:
        with pytest.raises(ApplicationError, match="try again") as caught:
            await ctx.call(fail)
        assert caught.value.type_name == "payment_rejected"
        observed.append(codec.decode_json(caught.value.details, int))
        await ctx.port(Ports.ready).emit(True)
        await ctx.port(Ports.finish).receive()
        raise caught.value

    path = tmp_path / "error-details.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "details")
        assert await anext(run.port(Ports.ready).reader()) is True
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow, "details")
        await run.port(Ports.finish).send(True)
        with pytest.raises(ApplicationError) as caught:
            await run.result()
        assert codec.decode_json(caught.value.details, int) == 409
    assert observed == [409, 409]
    assert calls == [True]
