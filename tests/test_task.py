import asyncio
import random
import uuid
from typing import ClassVar

import pytest

from duron import durable, get_context, task
from duron.log.storage.simple import MemoryLogStorage


@pytest.mark.asyncio
async def test_task():
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity(i: str) -> str:
        ctx = get_context()
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
    async with task(activity, log) as t:
        await t.start("test")
        a = await t.wait()
    assert set(e["id"] for e in await log.entries()) == IDS

    async with task(activity, log) as t:
        await t.start("test")
        b = await t.wait()
    assert a == b

    log2 = MemoryLogStorage((await log.entries())[:-2])
    async with task(activity, log2) as t:
        await t.start("test")
        c = await t.wait()
    assert a == c
    assert set(e["id"] for e in await log2.entries()) == IDS


@pytest.mark.asyncio
async def test_task_error():
    @durable()
    async def activity():
        ctx = get_context()
        _ = await ctx.run(lambda: asyncio.sleep(0.1))

        async def error():
            raise ValueError("test error")

        _ = await ctx.run(error)

    log = MemoryLogStorage()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        async with task(activity, log) as t:
            await t.start()
            await t.wait()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        async with task(activity, log) as t:
            await t.start()
            await t.wait()


@pytest.mark.asyncio
async def test_resume():
    sleep = 9999

    @durable()
    async def activity(s: str) -> str:
        ctx = get_context()
        _ = await ctx.run(lambda: asyncio.sleep(sleep))
        return s

    log = MemoryLogStorage()
    async with task(activity, log) as t:
        await t.start("hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await asyncio.wait_for(t.wait(), 0.1)

    async with task(activity, log) as t:
        sleep = 0
        await t.resume()
        x = await t.wait()
    assert x == "hello"


try:
    import pydantic

    class Point(pydantic.BaseModel):
        model_config: ClassVar[pydantic.ConfigDict] = pydantic.ConfigDict(
            extra="forbid"
        )

        x: int
        y: int

    @pytest.mark.asyncio
    async def test_pydantic():
        @durable()
        async def activity() -> Point:
            ctx = get_context()
            pt = await ctx.run(lambda: Point(x=1, y=2))
            return Point(x=pt.x + 5, y=pt.y + 10)

        log = MemoryLogStorage()
        async with task(activity, log) as t:
            await t.start()
            a = await t.wait()
            assert type(a) is Point
            assert a.x == 6 and a.y == 12

except ImportError:
    pass
