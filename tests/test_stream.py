from __future__ import annotations

import asyncio
import operator
import random
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Annotated

import pytest

from duron import (
    Context,
    Reducer,
    Session,
    SignalInterrupt,
    StreamClosed,
    durable,
    effect,
)
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron import StreamWriter


@pytest.mark.asyncio
async def test_stream() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        stream, handle = await ctx.create_stream(int)

        async with handle as h:

            async def f() -> None:
                for i in range(50):
                    x = ctx.random().randint(1, 10)
                    await asyncio.sleep(0.001 * x)
                    await h.send(i)

            _ = await asyncio.gather(
                asyncio.create_task(f()),
                asyncio.create_task(f()),
                asyncio.create_task(f()),
                asyncio.create_task(f()),
            )
        assert sum(await stream.collect()) == 4900

    log = MemoryLogStorage()
    async with Session(log) as t:
        await (await t.start(activity)).result()

    async with Session(log) as t:
        await (await t.start(activity)).result()


@pytest.mark.asyncio
async def test_stream_host() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        stream, handle = await ctx.create_stream(int)

        async def task(stream: StreamWriter[int]) -> None:
            async with stream as s:
                for i in range(50):
                    await s.send(i)

        await ctx.run(task, handle)
        assert sum(await stream.collect()) == 1225

    log = MemoryLogStorage()
    async with Session(log) as t:
        await (await t.start(activity)).result()


@pytest.mark.asyncio
async def test_run() -> None:
    sleep_idx = 3
    all_states: list[str] = []

    @durable()
    async def activity(ctx: Context) -> None:
        @effect
        async def f(
            s: Annotated[str, Reducer(operator.add)],
        ) -> AsyncGenerator[str, str]:
            while len(s) < 100:
                if len(s) == sleep_idx:
                    await asyncio.sleep(99999)
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                all_states.append(s)
            yield ""

        _ = await ctx.run(f, "")

    log = MemoryLogStorage()
    while True:
        async with Session(log) as t:
            run = await t.start(activity)
            try:
                _ = await asyncio.wait_for(run.result(), 0.1)
                break
            except asyncio.TimeoutError as _e:
                sleep_idx += 20

    for s in all_states:
        assert all_states[-1].startswith(s)


@pytest.mark.asyncio
async def test_stream_map() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        @effect
        async def f(
            s: Annotated[str, Reducer(operator.add)],
        ) -> AsyncGenerator[str, str]:
            while len(s) < 100:
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                await asyncio.sleep(0)

        async with ctx.stream(f, "") as (stream, result):
            async for s in stream.map(lambda s: s.upper()):
                assert s == s.upper()
            _ = await result
            return

    log = MemoryLogStorage()
    async with Session(log) as t:
        await (await t.start(activity)).result()


@pytest.mark.asyncio
async def test_stream_peek() -> None:
    @durable()
    async def activity(ctx: Context) -> list[int]:
        stream, write = await ctx.create_stream(int)

        async def f() -> None:
            async with write as w:
                for i in range(30):
                    await asyncio.sleep(random.random() * 0.001)
                    await w.send(i)

        x = asyncio.create_task(ctx.run(f))
        sample: list[int] = []
        while True:
            data: list[int] = []
            try:
                data.extend(await stream.next_nowait() or ())
                await asyncio.sleep(0.003)
            except StreamClosed:
                break
            finally:
                if data:
                    sample.append(data[0])
        await x
        return sample

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
    for _ in range(4):
        async with Session(log) as t:
            b = await (await t.start(activity)).result()
        assert a == b


@pytest.mark.asyncio
async def test_stream_cross_loop() -> None:
    @durable()
    async def activity(ctx: Context) -> list[str]:
        @effect
        async def f(
            s: Annotated[str, Reducer(operator.add)],
        ) -> AsyncGenerator[str, str]:
            while len(s) < 5:
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                await asyncio.sleep(0)

        async with ctx.stream(f, "") as (stream, _result):
            s = stream.map(lambda x: x * 2)

            async def g() -> list[str]:
                result: list[str] = [x async for x in s]
                return result

            return await ctx.run(g)

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
    for _ in range(4):
        async with Session(log) as t:
            b = await (await t.start(activity)).result()
        assert a == b


@pytest.mark.asyncio
async def test_next_timing() -> None:
    @durable()
    async def activity(ctx: Context) -> list[list[list[int]]]:
        stream, write = await ctx.create_stream(int)
        signal1, sig1_write = await ctx.create_signal(str)
        results: list[list[list[int]]] = []

        # Emit values continuously
        async def emitter() -> None:
            async with write as w:
                for i in range(50):
                    await w.send(i)
                    await asyncio.sleep(0.001)

        # Trigger signal at specific time
        async def trigger_signal() -> None:
            async with sig1_write as w:
                for _i in range(5):
                    await asyncio.sleep(0.015)
                    await w.send("interrupt_1")

        a = asyncio.create_task(emitter())
        b = asyncio.create_task(trigger_signal())
        # Consume with signal context
        for i in range(5):
            batch: list[list[int]] = []
            try:
                async with signal1:
                    # Consume values until interrupted
                    while True:
                        if i % 2 == 0:
                            batch.append(list(await stream.next()))
                        else:
                            batch.append(list(await stream.next_nowait() or ()))
                            await asyncio.sleep(0.002)
            except StreamClosed:
                results.append(batch)
            except SignalInterrupt:
                results.append(batch)
        await asyncio.gather(a, b)
        return results

    # First run
    log = MemoryLogStorage()
    async with Session(log) as t:
        result1 = await (await t.start(activity)).result()
    a = len(await log.entries())
    async with Session(log) as t:
        result2 = await (await t.start(activity)).result()
    b = len(await log.entries())
    for chunk1, chunk2 in zip(result1, result2, strict=True):
        length = min(len(chunk1), len(chunk2))
        assert len(chunk2) >= len(chunk1)
        assert chunk1[:length] == chunk2[:length]
    assert a == b
