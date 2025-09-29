from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

import pytest

from duron import fn
from duron.context import Context
from duron.contrib.storage import MemoryLogStorage
from duron.stream import AmbientRawStream, RawStream

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.mark.asyncio
async def test_stream():
    @fn()
    async def activity(ctx: Context) -> None:
        state = Observer()
        stream: RawStream[int] = await ctx.create_stream(state)

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

    def on_next(self, val: int):
        self.total += val

    def on_close(self, _exc: BaseException | None):
        pass


@pytest.mark.asyncio
async def test_stream_host():
    @fn()
    async def activity(ctx: Context) -> None:
        state = Observer()
        stream: AmbientRawStream[int] = (await ctx.create_stream(state)).to_ambient()

        async def task(stream: AmbientRawStream[int]):
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
