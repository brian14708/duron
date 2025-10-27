from __future__ import annotations

import asyncio
import contextlib
import random
import sys
from typing import cast

import pytest

from duron import Context, Provided, Session, Signal, SignalInterrupt, durable
from duron.contrib.storage import MemoryLogStorage


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
    async with Session(log) as t:
        assert await (await t.start(activity)).result() == [2, 3]

    async with Session(log) as t:
        assert await (await t.start(activity)).result() == [2, 3]


@pytest.mark.asyncio
async def test_signal_timing() -> None:
    @durable()
    async def activity(ctx: Context, s: Signal[int] = Provided) -> list[list[int]]:
        async def tracker(signal: Signal[int], t: float) -> list[int]:
            values: list[int] = []
            while True:
                cnt = 0
                await asyncio.sleep(t)
                try:
                    async with signal:
                        while True:
                            cnt += 1
                            await asyncio.sleep(0.0001)
                            await ctx.run(asyncio.sleep, 0.001)
                except SignalInterrupt as e:
                    values.append(cast("int", e.value) + cnt)
                    if len(values) > 10:
                        return values

        rnd = ctx.random()
        return await asyncio.gather(*[
            (tracker(s, rnd.random() * 0.01)) for _ in range(4)
        ])

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity)
        signal = await run.open_stream("s", "w")

        async def push() -> None:
            i = 0
            while True:
                i += 1
                await asyncio.sleep(0.001)
                await signal.send(i)

        pusher = asyncio.create_task(push())
        a = await run.result()
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher

    async with Session(log) as t:
        assert await (await t.start(activity)).result() == a


@pytest.mark.asyncio
async def test_timeout_timing() -> None:
    @durable()
    async def activity(ctx: Context) -> list[int]:
        async def work() -> None:
            await asyncio.sleep(random.random() * 0.02)

        async def tracker() -> int:
            values = 0
            for _i in range(100):
                with contextlib.suppress(asyncio.TimeoutError):
                    if sys.version_info >= (3, 11):
                        async with asyncio.timeout(0.01):
                            await ctx.run(work)
                            values += 1
            return values

        return await asyncio.gather(*(tracker() for _ in range(4)))

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity)
        a = await run.result()

    async with Session(log) as t:
        b = await (await t.start(activity)).result()

    assert a == b
