from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from duron import durable
from duron.contrib.codecs import PickleCodec
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron.context import Context


@pytest.mark.asyncio
async def test_task():
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity(ctx: Context, i: str) -> str:
        x = await asyncio.gather(
            ctx.run(u),
            ctx.run(u),
        )
        _ = await ctx.run(lambda: asyncio.sleep(0.1))
        return i + ":".join(x)

    IDS = {
        "04nH6MWw6eE/vJ5r",
        "5lAazvU2oa7hwmxv",
        "9mcIBsvU2ej9uDsV",
        "DzHchUp69P+z34eq",
        "MKbyO056hUi5M5mu",
        "P48ejug7cFGpdQkY",
        "bsHHUcRaDpZ4duNt",
        "nPpEbQJ0ukEIrYob",
        "q6LeJqEWRCR0zLTm",
        "r8oZz+wBVqWRYBuu",
    }

    log = MemoryLogStorage()
    async with activity(log) as t:
        await t.start("test")
        a = await t.wait()
    assert set(e["id"] for e in await log.entries()) == IDS

    async with activity(log) as t:
        await t.start("test")
        b = await t.wait()
    assert a == b

    log2 = MemoryLogStorage((await log.entries())[:-2])
    async with activity(log2) as t:
        await t.start("test")
        c = await t.wait()
    assert a == c
    assert set(e["id"] for e in await log2.entries()) == IDS


@pytest.mark.asyncio
async def test_task_error():
    @durable()
    async def activity(ctx: Context):
        _ = await ctx.run(lambda: asyncio.sleep(0.1))

        async def error():
            raise ValueError("test error")

        _ = await ctx.run(error)

    log = MemoryLogStorage()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        async with activity(log) as t:
            await t.start()
            await t.wait()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        async with activity(log) as t:
            await t.start()
            await t.wait()


@pytest.mark.asyncio
async def test_resume():
    sleep = 9999

    @durable()
    async def activity(ctx: Context, s: str) -> str:
        _ = await ctx.run(lambda: asyncio.sleep(sleep))
        return s

    log = MemoryLogStorage()
    async with activity(log) as t:
        await t.start("hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await asyncio.wait_for(t.wait(), 0.1)

    async with activity(log) as t:
        sleep = 0
        await t.resume()
        x = await t.wait()
    assert x == "hello"


@dataclass
class CustomPoint:
    x: int
    y: int


@pytest.mark.asyncio
async def test_serialize():
    @durable(codec=PickleCodec())
    async def activity(ctx: Context) -> CustomPoint:
        pt = await ctx.run(lambda: CustomPoint(x=1, y=2))
        return CustomPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with activity(log) as t:
        await t.start()
        a = await t.wait()
        assert type(a) is CustomPoint
        assert a.x == 6 and a.y == 12
