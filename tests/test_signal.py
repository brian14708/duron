from __future__ import annotations

import asyncio
from typing import cast

import pytest

from duron import Context, SignalInterrupt, fn
from duron.contrib.storage import MemoryLogStorage


@pytest.mark.asyncio
async def test_signal():
    @fn()
    async def activity(ctx: Context) -> list[int]:
        signal, handle = await ctx.create_signal(int)

        await handle.trigger(1)

        async def trigger():
            await asyncio.sleep(0.1)
            await handle.trigger(2)
            await asyncio.sleep(0.1)
            await handle.trigger(3)

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
    async with activity.invoke(log) as t:
        await t.start()
        assert await t.wait() == [2, 3]

    async with activity.invoke(log) as t:
        await t.resume()
        assert await t.wait() == [2, 3]
