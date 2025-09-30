from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

import pytest

from duron import fn
from duron.context import Context
from duron.contrib.storage import MemoryLogStorage
from duron.stream import EndOfStream, PeekStream, Stream, StreamHandle

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.mark.asyncio
async def test_stream():
    @fn()
    async def activity(ctx: Context) -> None:
        state = Observer()
        stream: StreamHandle[int] = await ctx.create_stream(state)

        async def f():
            for i in range(50):
                x = ctx.random().randint(1, 10)
                await asyncio.sleep(0.001 * x)
                await stream.send(i)

        _ = await asyncio.gather(
            asyncio.create_task(f()),
            asyncio.create_task(f()),
            asyncio.create_task(f()),
            asyncio.create_task(f()),
        )
        await stream.close()
        assert state.total == 4900

    log = MemoryLogStorage()
    async with activity.create_task(log) as t:
        await t.start()
        await t.wait()

    async with activity.create_task(log) as t:
        await t.resume()
        await t.wait()


class Observer:
    total: int = 0

    def on_next(self, _offset: int, val: int):
        self.total += val

    def on_close(self, _offset: int, _exc: BaseException | None):
        pass


@pytest.mark.asyncio
async def test_stream_host():
    @fn()
    async def activity(ctx: Context) -> None:
        state = Observer()
        stream = (await ctx.create_stream(state)).to_host()

        async def task(stream: StreamHandle[int]):
            for i in range(50):
                await stream.send(i)
            await stream.close()

        await ctx.run(task, stream)
        assert state.total == 1225

    log = MemoryLogStorage()
    async with activity.create_task(log) as t:
        await t.start()
        await t.wait()


@pytest.mark.asyncio
async def test_run():
    sleep_idx = 3
    all_states: list[str] = []

    @fn()
    async def activity(ctx: Context) -> None:
        async def f(s: str) -> AsyncGenerator[str, str]:
            while len(s) < 100:
                if len(s) == sleep_idx:
                    await asyncio.sleep(99999)
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk
                all_states.append(s)
            yield ""

        stream = ctx.stream("", lambda s, p: s + p, f)
        await stream.discard()

    log = MemoryLogStorage()
    while True:
        async with activity.create_task(log) as t:
            await t.start()
            try:
                _ = await asyncio.wait_for(t.wait(), 0.1)
                break
            except asyncio.TimeoutError as _e:
                sleep_idx += 20

    for s in all_states:
        assert all_states[-1].startswith(s)


@pytest.mark.asyncio
async def test_stream_map():
    @fn()
    async def activity(ctx: Context) -> None:
        async def f(s: str) -> AsyncGenerator[str, str]:
            while len(s) < 100:
                chunk = chr(ord("a") + random.randint(0, 25))
                s = yield chunk

        stream = ctx.stream("", lambda s, p: s + p, f)
        async for s in stream.map(lambda s: s.upper()):
            assert s == s.upper()
        return

    log = MemoryLogStorage()
    async with activity.create_task(log) as t:
        await t.start()
        await t.wait()


@pytest.mark.asyncio
async def test_stream_generator():
    @fn()
    async def activity(_ctx: Context) -> None:
        async def f() -> AsyncGenerator[int]:
            for i in range(100):
                yield i
                await asyncio.sleep(0)

        stream = Stream[int].from_iterator(f())
        m = stream.map(lambda x: x * 2)

        a, m = m.tee()
        b, m = m.tee()
        c, m = m.tee()
        d, m = m.tee()

        assert await d.collect() == list(range(0, 200, 2))
        assert sum(await a.map(lambda x: x / 2).collect()) == 4950
        await b.close()
        await c.close()
        await m.close()

    log = MemoryLogStorage()
    async with activity.create_task(log) as t:
        await t.start()
        await t.wait()


@pytest.mark.asyncio
async def test_stream_peek():
    @fn()
    async def activity(ctx: Context) -> list[int]:
        p: tuple[PeekStream[int], StreamHandle[int]] = await ctx.create_peek_stream()
        rd, write = p
        write = write.to_host()

        async def f():
            for i in range(30):
                await asyncio.sleep(random.random() * 0.001)
                await write.send(i)
            await write.close()

        x = asyncio.create_task(ctx.run(f))
        sample: list[int] = []
        while True:
            data: list[int] = []
            try:
                async for u in rd.peek():
                    data.append(u)
                await asyncio.sleep(0.003)
            except EndOfStream:
                break
            finally:
                if data:
                    sample.append(data[0])
        await x
        return sample

    log = MemoryLogStorage()
    async with activity.create_task(log) as t:
        await t.start()
        a = await t.wait()
    for _ in range(4):
        async with activity.create_task(log) as t:
            await t.resume()
            b = await t.wait()
        assert a == b
