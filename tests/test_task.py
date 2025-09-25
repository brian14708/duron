from __future__ import annotations

import asyncio
import base64
import pickle
import random
import uuid
from dataclasses import dataclass

import pytest

from duron import durable, get_context, task
from duron.log.storage import MemoryLogStorage


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


@dataclass
class CustomPoint:
    x: int
    y: int


class PickleCodec:
    def encode_json(self, result: object) -> str:
        return base64.b64encode(pickle.dumps(result)).decode()

    def decode_json(self, encoded: object) -> object:
        if not isinstance(encoded, str):
            raise TypeError(f"Expected a string, got {type(encoded).__name__}")
        return pickle.loads(base64.b64decode(encoded.encode()))


@pytest.mark.asyncio
async def test_serialize():
    @durable(codec=PickleCodec())
    async def activity() -> CustomPoint:
        ctx = get_context()
        pt = await ctx.run(lambda: CustomPoint(x=1, y=2))
        return CustomPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with task(activity, log) as t:
        await t.start()
        a = await t.wait()
        assert type(a) is CustomPoint
        assert a.x == 6 and a.y == 12
