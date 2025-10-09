from __future__ import annotations

import asyncio
import operator
import random
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest

from duron import Context, RunOptions, StreamClosed, fn, op
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron import StreamWriter


@pytest.mark.asyncio
async def test_stream() -> None:
    @fn()
    async def activity(ctx: Context) -> None:
        stream, handle = await ctx.create_stream(int)

        async def f() -> None:
            for i in range(50):
                x = ctx.random().randint(1, 10)
                await asyncio.sleep(0.001 * x)
                await handle.send(i)

        _ = await asyncio.gather(
            asyncio.create_task(f()),
            asyncio.create_task(f()),
            asyncio.create_task(f()),
            asyncio.create_task(f()),
        )
        await handle.close()
        assert sum(await stream.collect()) == 4900

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        await t.wait()

    async with activity.invoke(log) as t:
        await t.resume()
        await t.wait()


@pytest.mark.asyncio
async def test_stream_host() -> None:
    @fn()
    async def activity(ctx: Context) -> None:
        stream, handle = await ctx.create_stream(int, external=True)

        async def task(stream: StreamWriter[int]) -> None:
            for i in range(50):
                await stream.send(i)
            await stream.close()

        await ctx.run(task, RunOptions(), handle)
        assert sum(await stream.collect()) == 1225

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        await t.wait()


@pytest.mark.asyncio
async def test_run() -> None:
    sleep_idx = 3
    all_states: list[str] = []

    @fn()
    async def activity(ctx: Context) -> None:
        @op(
            checkpoint=True,
            action_type=str,
            initial=lambda: "",
            reducer=operator.add,
        )
        async def f(s: str) -> AsyncGenerator[str, str]:
            while len(s) < 100:
                if len(s) == sleep_idx:
                    await asyncio.sleep(99999)
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                all_states.append(s)
            yield ""

        _ = await ctx.run(f)

    log = MemoryLogStorage()
    while True:
        async with activity.invoke(log) as t:
            await t.start()
            try:
                _ = await asyncio.wait_for(t.wait(), 0.1)
                break
            except asyncio.TimeoutError as _e:
                sleep_idx += 20

    for s in all_states:
        assert all_states[-1].startswith(s)


@pytest.mark.asyncio
async def test_stream_map() -> None:
    @fn()
    async def activity(ctx: Context) -> None:
        @op(
            checkpoint=True,
            action_type=str,
            initial=lambda: "",
            reducer=operator.add,
        )
        async def f(s: str) -> AsyncGenerator[str, str]:
            while len(s) < 100:
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                await asyncio.sleep(0)

        async with ctx.run_stream(f) as stream:
            async for s in stream.map(lambda s: s.upper()):
                assert s == s.upper()
            return

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        await t.wait()


@pytest.mark.asyncio
async def test_stream_peek() -> None:
    @fn()
    async def activity(ctx: Context) -> list[int]:
        stream, write = await ctx.create_stream(int, external=True)

        async def f() -> None:
            for i in range(30):
                await asyncio.sleep(random.random() * 0.001)
                await write.send(i)
            await write.close()

        x = asyncio.create_task(ctx.run(f))
        sample: list[int] = []
        async with stream as s:
            while True:
                data: list[int] = []
                try:
                    async for _, u in s.next_nowait(ctx):
                        data.append(u)
                    await asyncio.sleep(0.003)
                except StreamClosed:
                    break
                finally:
                    if data:
                        sample.append(data[0])
        await x
        return sample

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()
    for _ in range(4):
        async with activity.invoke(log) as t:
            await t.resume()
            b = await t.wait()
        assert a == b


@pytest.mark.asyncio
async def test_stream_cross_loop() -> None:
    @fn()
    async def activity(ctx: Context) -> list[str]:
        @op(
            checkpoint=True,
            action_type=str,
            initial=lambda: "",
            reducer=operator.add,
        )
        async def f(s: str) -> AsyncGenerator[str, str]:
            while len(s) < 5:
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                await asyncio.sleep(0)

        async with ctx.run_stream(f) as stream:
            s = stream.map(lambda x: x * 2)

            async def g() -> list[str]:
                result: list[str] = [x async for x in s]
                return result

            return await ctx.run(g)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()
    for _ in range(4):
        async with activity.invoke(log) as t:
            await t.resume()
            b = await t.wait()
        assert a == b
