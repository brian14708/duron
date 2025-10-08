from __future__ import annotations

import asyncio
import contextlib
import random
import uuid
from dataclasses import dataclass

import pytest

from duron import Context, fn
from duron.contrib.codecs import PickleCodec
from duron.contrib.storage import MemoryLogStorage


@pytest.mark.asyncio
async def test_invoke():
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @fn()
    async def activity(ctx: Context, i: str) -> str:
        x = await asyncio.gather(
            ctx.run(u),
            ctx.run(u),
        )
        _ = await ctx.run(lambda: asyncio.sleep(0.1))
        return i + ":".join(x)

    IDS = {
        "+qPYuDgKBdMkb8ME",
        "9nLMU+itD7QHcCsf",
        "BCLA1azFK4LrrEHg",
        "D7qSBNZIThKa2P+H",
        "VdptC8Lv0z2kPe3n",
        "rThvjdqQNneQA3Am",
        "syoQHz+dg0KA3xJY",
        "uJ/iXS2GC5Hz6v1R",
    }

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start("test")
        a = await t.wait()
    assert set(e["id"] for e in await log.entries()) == IDS

    async with activity.invoke(log) as t:
        await t.start("test")
        b = await t.wait()
    assert a == b

    log2 = MemoryLogStorage((await log.entries())[:-2])
    async with activity.invoke(log2) as t:
        await t.start("test")
        c = await t.wait()
    assert a == c
    assert set(e["id"] for e in await log2.entries()) == IDS


@pytest.mark.asyncio
async def test_invoke_error():
    @fn()
    async def activity(ctx: Context):
        _ = await ctx.run(lambda: asyncio.sleep(0.1))

        async def error():
            raise ValueError("test error")

        _ = await ctx.run(error)

    log = MemoryLogStorage()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        async with activity.invoke(log) as t:
            await t.start()
            await t.wait()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        async with activity.invoke(log) as t:
            await t.start()
            await t.wait()


@pytest.mark.asyncio
async def test_resume():
    sleep = 9999

    @fn()
    async def activity(ctx: Context, s: str) -> str:
        _ = await ctx.run(lambda: asyncio.sleep(sleep))
        return s

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start("hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await asyncio.wait_for(t.wait(), 0.1)

    async with activity.invoke(log) as t:
        sleep = 0
        await t.resume()
        x = await t.wait()
    assert x == "hello"


@pytest.mark.asyncio
async def test_cancel():
    @fn()
    async def activity(ctx: Context, s: str) -> str:
        with contextlib.suppress(BaseException):
            _ = await asyncio.wait_for(ctx.run(lambda: asyncio.sleep(9999)), 0.1)
        _ = await asyncio.wait_for(ctx.run(lambda: asyncio.sleep(9999)), 0.1)
        return s

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start("hello")
        try:
            _ = await t.wait()
        except Exception as e:
            assert "Timeout" in repr(e)
    async with activity.invoke(log) as t:
        await t.resume()
        try:
            _ = await t.wait()
        except Exception as e:
            assert "Timeout" in repr(e)


@dataclass
class CustomPoint:
    x: int
    y: int


@pytest.mark.asyncio
async def test_serialize():
    @fn(codec=PickleCodec())
    async def activity(ctx: Context) -> CustomPoint:
        def new_pt() -> CustomPoint:
            return CustomPoint(x=1, y=2)

        pt = await ctx.run(new_pt)
        return CustomPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()
        assert type(a) is CustomPoint
        assert a.x == 6 and a.y == 12


@pytest.mark.asyncio
async def test_random():
    @fn()
    async def activity(ctx: Context) -> int:
        assert ctx.time_ns() == ctx.time_ns()
        return ctx.random().randint(1, 100)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        b = await t.wait()

    assert a == b


@pytest.mark.asyncio
async def test_external_promise():
    @fn
    async def activity(ctx: Context) -> int:
        a, b = await ctx.create_promise(int)
        assert a == "9mcIBsvU2ej9uDsV"
        return await b

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()

        async def do():
            while True:
                try:
                    await t.complete_promise("9mcIBsvU2ej9uDsV", result=9)
                    break
                except Exception:
                    await asyncio.sleep(0.1)

        bg = asyncio.create_task(do())
        assert await t.wait() == 9
        await bg


@pytest.mark.asyncio
async def test_external_stream():
    @fn
    async def activity(ctx: Context) -> int:
        stream, _ = await ctx.create_stream(int, metadata={"name": "test"})
        t = 0
        async for value in stream:
            t += value
            if value == -1:
                return t
        return -1

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()

        async def do():
            while True:
                n = await t.send_stream(lambda d: d.get("name") == "test", 0)
                if n == 0:
                    await asyncio.sleep(0.1)
                else:
                    break
            for i in range(10):
                n = await t.send_stream(lambda d: d.get("name") == "test", i)
            n = await t.send_stream(lambda d: d.get("name") == "test", -1)

        bg = asyncio.create_task(do())
        assert await t.wait() == 44
        await bg


@pytest.mark.asyncio
async def test_watch_stream():
    @fn
    async def activity(ctx: Context) -> int:
        _, sink = await ctx.create_stream(int, metadata={"name": "output"})
        for i in range(10):
            await sink.send(i)
        await sink.close()
        return 42

    log = MemoryLogStorage()

    async with activity.invoke(log) as invoke:
        output_stream = invoke.watch_stream(lambda d: d.get("name") == "output")

        await invoke.start()

        async def bg() -> list[int]:
            values: list[int] = []
            async for value in output_stream:
                values.append(value)
            return values

        b = asyncio.create_task(bg())
        result = await invoke.wait()
        assert result == 42
        assert await b == list(range(10))
