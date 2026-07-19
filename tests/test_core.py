from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, cast

import pytest

import duron
from duron.errors import (
    HistoryMismatchError,
    Interrupted,
    InvalidExecutionRequestError,
    PortClosedError,
    RemoteEffectError,
)
from duron.execution import RetryPolicy, StreamSender
from duron.storage import HistoryRecord

if TYPE_CHECKING:
    from duron.execution.remote import EffectEnvelope, EffectHost
    from duron.storage import HistoryStore
    from duron.types import JSONValue, TypeHint


async def test_effect_replay_preserves_time_randomness_and_typed_dependencies(
    backend: HistoryStore,
) -> None:
    calls: list[str] = []
    multiplier = 2

    @duron.effect
    async def double(ctx: duron.EffectContext, value: int) -> int:
        calls.append(ctx.idempotency_key)
        return value * multiplier

    @duron.workflow(requires=(double,))
    async def flow(ctx: duron.WorkflowContext) -> tuple[str, float, list[int]]:
        now = await ctx.now()
        value = ctx.random().random()
        await ctx.sleep(0)
        results = await asyncio.gather(ctx.call(double, 2), ctx.call(double, 3))
        return now.isoformat(), value, list(results)

    first = await duron.run(flow(), backend, run_id="replay")
    assert await duron.run(flow(), backend, run_id="replay") == first
    assert first[2] == [4, 6]
    assert len(calls) == len(set(calls)) == 2


async def test_retry_is_durable_and_does_not_rerun_after_completion(
    backend: HistoryStore,
) -> None:
    attempts: list[int] = []

    @duron.effect(retry=RetryPolicy(max_attempts=2, retry_on=(ValueError,)))
    async def flaky(ctx: duron.EffectContext) -> int:
        attempts.append(ctx.logical_attempt)
        if len(attempts) == 1:
            msg = "retry"
            raise ValueError(msg)
        return 7

    @duron.workflow(requires=(flaky,))
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(flaky)

    for _ in range(2):
        assert await duron.run(flow(), backend, run_id="retry") == 7
    assert attempts == [1, 2]


async def test_remote_dispatch_invokes_registered_effect_through_public_api(
    backend: HistoryStore,
) -> None:
    calls: list[str] = []
    local_calls: list[int] = []

    class Dispatcher:
        @staticmethod
        async def dispatch(request: EffectEnvelope, *, host: EffectHost) -> None:
            with pytest.raises(InvalidExecutionRequestError, match="authenticating"):
                _ = await host.rebind(host.decode(request, effect=double))
            _ = await host.execute(request, effect=double)

    @duron.effect(executor=Dispatcher(), raises=(ValueError,))
    async def double(ctx: duron.EffectContext, value: int) -> int:
        calls.append(ctx.idempotency_key)
        if value == 0:
            msg = "zero"
            raise ValueError(msg)
        return value * 2

    @duron.effect
    async def local(_ctx: duron.EffectContext, value: int) -> int:
        local_calls.append(value)
        return value + 1

    @duron.workflow(requires=(double, local))
    async def flow(ctx: duron.WorkflowContext) -> int:
        with pytest.raises(ValueError, match="zero"):
            _ = await ctx.call(double, 0)
        return await ctx.call(double, await ctx.call(local, 2))

    for _ in range(2):
        assert await duron.run(flow(), backend, run_id="remote") == 6
    assert len(calls) == len(set(calls)) == 2
    assert local_calls == [2]


async def test_effect_selects_its_own_codec(backend: HistoryStore) -> None:
    calls: list[int] = []

    class IntegerCodec:
        @staticmethod
        def encode_json(value: object, _annotation: TypeHint[object]) -> JSONValue:
            assert type(value) is int
            return f"integer:{value}"

        @staticmethod
        def decode_json(encoded: JSONValue, _annotation: TypeHint[object]) -> object:
            assert isinstance(encoded, str)
            assert encoded.startswith("integer:")
            return int(encoded.removeprefix("integer:"))

    @duron.effect(codec=IntegerCodec(), codec_id="tests.integer.v1")
    async def double(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        return value * 2

    @duron.workflow(requires=(double,))
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 3)

    for _ in range(2):
        assert await duron.run(flow(), backend, run_id="effect-codec") == 6
    assert calls == [3]


async def test_inputs_outputs_requests_and_cursor_reconnect(
    backend: HistoryStore,
) -> None:
    incoming = duron.Input[int]("values")
    outgoing = duron.Output[int]("events")
    approval = duron.Request[int, bool]("approval")

    @duron.workflow(requires=(incoming, outgoing, approval))
    async def flow(ctx: duron.WorkflowContext) -> int:
        first = await ctx.receive(incoming)
        second = await ctx.receive(incoming)
        await ctx.emit(outgoing, first)
        if await ctx.request(approval, second):
            await ctx.emit(outgoing, second)
        return first + second

    async with duron.open_run(flow(), backend, run_id="ports") as run:
        await run.input(incoming).send(3)
        await run.input(incoming).send(4)
        reader = run.output(outgoing)
        assert await reader.next() == 3
        cursor = reader.cursor
        request = await run.requests(approval).next()
        assert request.value == 4
        pending_before = request.is_pending
        await request.respond(True)
        pending_after = request.is_pending
        assert pending_before
        assert not pending_after
        assert await run.result() == 7
        assert await run.output(outgoing, after=cursor).collect() == [4]
        with pytest.raises(ValueError, match="cursor"):
            run.output(duron.Output[int]("other"), after=cursor)
    async with duron.open_run(flow(), backend, run_id="ports") as run:
        assert await run.result() == 7
        assert await run.output(outgoing).collect() == [3, 4]
    async with duron.open_run(flow(), backend, run_id="other") as run:
        with pytest.raises(ValueError, match="cursor"):
            run.output(outgoing, after=cursor)
        await run.input(incoming).send(1)
        await run.input(incoming).send(2)
        await (await run.requests(approval).next()).respond(False)
        assert await run.result() == 3


async def test_interrupt_changes_control_flow_and_replays(
    backend: HistoryStore,
) -> None:
    cancel = duron.Interrupt[str]("cancel")
    ready = duron.Output[bool]("ready")

    @duron.workflow(requires=(cancel, ready))
    async def flow(ctx: duron.WorkflowContext) -> str:
        try:
            async with ctx.interruptible(cancel):
                await ctx.emit(ready, True)
                await ctx.sleep(3600)
        except Interrupted as error:
            return str(error.value)
        return "unexpected"

    async with duron.open_run(flow(), backend, run_id="interrupt") as run:
        assert await run.output(ready).next()
        await run.interrupt(cancel).send("stopped")
        assert await run.result() == "stopped"
    assert await duron.run(flow(), backend, run_id="interrupt") == "stopped"


async def test_effect_stream_and_body_failure_are_terminal(
    backend: HistoryStore,
) -> None:
    calls: list[bool] = []

    @duron.effect
    async def produce(_ctx: duron.EffectContext, output: StreamSender[int]) -> None:
        calls.append(True)
        await output.send(1)
        await output.close()
        await output.send(2)

    @duron.workflow(requires=(produce,))
    async def flow(ctx: duron.WorkflowContext) -> list[int]:
        async with ctx.open_stream(int) as stream:
            task = asyncio.create_task(ctx.call(produce, stream.sender))
            values = [value async for value in stream]
            with pytest.raises(RemoteEffectError, match="closed"):
                await task
            return values

    for _ in range(2):
        assert await duron.run(flow(), backend, run_id="body-error") == [1]
    assert calls == [True]


async def test_stream_readers_cross_prefetch_boundaries_and_replay(
    backend: HistoryStore,
) -> None:
    calls: list[bool] = []

    @duron.effect
    async def produce(_ctx: duron.EffectContext, output: StreamSender[int]) -> None:
        calls.append(True)
        for value in range(7):
            await output.send(value)

    @duron.workflow(requires=(produce,))
    async def flow(ctx: duron.WorkflowContext) -> tuple[list[int], list[int]]:
        async with ctx.open_stream(int, prefetch=2) as stream:
            second = stream.reader()
            producer = asyncio.create_task(ctx.call(produce, stream.sender))
            live = [value async for value in stream]
            await producer
            catch_up = [value async for value in second]
            return live, catch_up

    for _ in range(2):
        assert await duron.run(flow(), backend, run_id="stream-readers") == (
            list(range(7)),
            list(range(7)),
        )
    assert calls == [True]


async def test_old_history_is_rejected_without_writes(backend: HistoryStore) -> None:
    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 1

    assert await duron.run(flow(), backend, run_id="template") == 1
    records = await backend.read("template")
    header: JSONValue = None
    for _, record in records:
        header = cast("JSONValue", json.loads(record.payload))
        assert isinstance(header, dict)
        if header["type"] == "promise.complete":
            break
    else:
        pytest.fail("run did not persist its history header")
    assert isinstance(header, dict)
    result = header["result"]
    assert isinstance(result, dict)
    result["version"] = 0
    old = HistoryRecord(record.id, json.dumps(header).encode())
    lease = await backend.acquire_lease("old")
    await backend.append("old", lease, [old])
    await backend.release_lease("old", lease)
    before = await backend.read("old")
    with pytest.raises(HistoryMismatchError, match="history format"):
        await duron.run(flow(), backend, run_id="old")
    assert await backend.read("old") == before


async def test_nonblocking_input_distinguishes_empty_from_closed(
    backend: HistoryStore,
) -> None:
    incoming = duron.Input[int]("values")
    ready = duron.Output[list[int]]("ready")

    @duron.workflow(requires=(incoming, ready))
    async def flow(ctx: duron.WorkflowContext) -> int:
        await ctx.emit(ready, await ctx.receive_many(incoming, wait=False))
        value = await ctx.receive(incoming)
        with pytest.raises(PortClosedError):
            await ctx.receive_many(incoming, wait=False)
        return value

    async with duron.open_run(flow(), backend, run_id="input-closure") as active:
        assert await active.output(ready).next() == []
        writer = active.input(incoming)
        await writer.send(7)
        await writer.close()
        assert await active.result() == 7
