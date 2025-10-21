from __future__ import annotations

import asyncio
import contextlib
from typing import cast

import pytest

from duron import Context, Signal, SignalInterrupt, durable, invoke
from duron.contrib.storage import MemoryLogStorage
from duron.typing._hint import Provided


@pytest.mark.asyncio
async def test_signal() -> None:
    @durable()
    async def activity(ctx: Context) -> list[int]:
        signal, h = await ctx.create_signal(int)

        async with h as handle:
            await handle.send(1)

            async def trigger() -> None:
                await asyncio.sleep(0.1)
                await handle.send(2)
                await asyncio.sleep(0.1)
                await handle.send(3)

            t = asyncio.create_task(trigger())

            val: list[int] = []
            try:
                async with signal:
                    try:
                        async with signal:
                            await asyncio.sleep(9999)
                    except SignalInterrupt as e:
                        val.append(cast("int", e.value))
                    await asyncio.sleep(9999)
            except SignalInterrupt as e:
                val.append(cast("int", e.value))
            async with signal:
                await asyncio.sleep(0)
            await t
        return val

    log = MemoryLogStorage()
    async with invoke(activity, log) as t:
        await t.start()
        assert await t.wait() == [2, 3]

    async with invoke(activity, log) as t:
        await t.resume()
        assert await t.wait() == [2, 3]


@pytest.mark.asyncio
async def test_signal_timing() -> None:
    @durable()
    async def activity(ctx: Context, s: Signal[int] = Provided) -> list[list[int]]:
        async def tracker(signal: Signal[int], t: float) -> list[int]:
            values: list[int] = []
            while True:
                await asyncio.sleep(t)
                try:
                    async with signal:
                        await asyncio.sleep(9999)
                except SignalInterrupt as e:
                    values.append(cast("int", e.value))
                    if len(values) > 10:
                        return values

        rnd = ctx.random()
        return await asyncio.gather(*[
            (tracker(s, rnd.random() * 0.01)) for _ in range(4)
        ])

    log = MemoryLogStorage()
    async with invoke(activity, log) as t:
        signal = t.open_stream("s", "w")
        await t.start()

        async def push() -> None:
            i = 0
            s = await signal
            while True:
                i += 1
                await asyncio.sleep(0.001)
                await s.send(i)

        pusher = asyncio.create_task(push())
        a = await t.wait()
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher

    async with invoke(activity, log) as t:
        await t.resume()
        assert await t.wait() == a
