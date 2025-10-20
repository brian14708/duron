from __future__ import annotations

import asyncio
import contextlib
import random
import uuid
from dataclasses import dataclass

import pytest

from duron import Context, Provided, Stream, StreamWriter, durable
from duron.contrib.codecs import PickleCodec
from duron.contrib.storage import MemoryLogStorage


@pytest.mark.asyncio
async def test_invoke() -> None:
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
        _ = await ctx.run(asyncio.sleep, 0.1)
        return i + ":".join(x)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start("test")
        a = await t.wait()

    async with activity.invoke(log) as t:
        await t.start("test")
        b = await t.wait()
    assert a == b

    log2 = MemoryLogStorage((await log.entries())[:-2])
    async with activity.invoke(log2) as t:
        await t.start("test")
        c = await t.wait()
    assert a == c


@pytest.mark.asyncio
async def test_invoke_error() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        _ = await ctx.run(asyncio.sleep, 0.1)

        def error() -> int:
            msg = "test error"
            raise ValueError(msg)

        _ = await ctx.run(error)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        with pytest.raises(Exception, match="test error"):
            await t.wait()
    async with activity.invoke(log) as t:
        await t.start()
        with pytest.raises(Exception, match="test error"):
            await t.wait()


@pytest.mark.asyncio
async def test_resume() -> None:
    sleep = 9999

    @durable()
    async def activity(ctx: Context, s: str) -> str:
        _ = await ctx.run(asyncio.sleep, sleep)
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
async def test_cancel() -> None:
    @durable()
    async def activity(ctx: Context, s: str) -> str:
        with contextlib.suppress(asyncio.TimeoutError):
            _ = await asyncio.wait_for(ctx.run(asyncio.sleep, 9999), 0.1)
        _ = await asyncio.wait_for(ctx.run(asyncio.sleep, 9999), 0.1)
        return s

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start("hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await t.wait()
    async with activity.invoke(log) as t:
        await t.resume()
        with pytest.raises(asyncio.TimeoutError):
            _ = await t.wait()


@pytest.mark.asyncio
async def test_timing() -> None:
    @durable()
    async def activity(
        ctx: Context,
    ) -> int:
        cnt = 0

        rnd = ctx.random()

        async def do(t: float) -> bool:
            try:
                _ = await asyncio.wait_for(ctx.run(asyncio.sleep, t), 0.1)
            except asyncio.TimeoutError:
                return True
            return False

        for f in await asyncio.gather(*[do(rnd.random() * 0.2) for _ in range(100)]):
            if f:
                cnt += 1
        return cnt

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()
    async with activity.invoke(log) as t:
        await t.resume()
        assert a == await t.wait()


@dataclass
class CustomPoint:
    x: int
    y: int


@pytest.mark.asyncio
async def test_serialize() -> None:
    @durable(codec=PickleCodec())
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
        assert a.x == 6
        assert a.y == 12


@pytest.mark.asyncio
async def test_random() -> None:
    @durable()
    async def activity(ctx: Context) -> int:
        assert ctx.time_ns() == ctx.time_ns()
        await asyncio.sleep(0)
        return ctx.random().randint(1, 100)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()

    async with activity.invoke(log) as t:
        await t.start()
        b = await t.wait()

    assert a == b


@pytest.mark.asyncio
async def test_external_promise() -> None:
    v: dict[str, str] = {}

    @durable
    async def activity(ctx: Context) -> int:
        a, b = await ctx.create_future(int)
        v["data"] = a
        return await b

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()

        async def do() -> None:
            while True:
                if v.get("data") is None:
                    await asyncio.sleep(0.01)
                    continue
                await t.complete_future(v["data"], result=9)
                break

        bg = asyncio.create_task(do())
        assert await t.wait() == 9
        await bg


@pytest.mark.asyncio
async def test_external_stream() -> None:
    @durable
    async def activity(_ctx: Context, test: Stream[int] = Provided) -> int:
        t = 0
        async for value in test:
            t += value
            if value == -1:
                return t
        return -1

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        test: StreamWriter[int] = t.open_stream("test", "w")
        await t.start()

        async def do() -> None:
            await test.send(0)
            for i in range(10):
                await test.send(i)
            await test.send(-1)

        bg = asyncio.create_task(do())
        assert await t.wait() == 44
        await bg


@pytest.mark.asyncio
async def test_external_stream_write() -> None:
    @durable
    async def activity(_ctx: Context, writer: StreamWriter[int] = Provided) -> int:
        for i in range(10):
            await writer.send(i)
        await writer.close()
        return 42

    log = MemoryLogStorage()

    async with activity.invoke(log) as invoke:
        output_stream = invoke.open_stream("writer", "r")

        await invoke.start()

        async def bg() -> list[int]:
            values: list[int] = [value async for value in output_stream]
            return values

        b = asyncio.create_task(bg())
        result = await invoke.wait()
        assert result == 42
        assert await b == list(range(10))


@durable
async def activity(ctx: Context) -> int:
    for _i in range(100):
        _ = await ctx.barrier()
    return 42


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_performance() -> None:
    log = MemoryLogStorage()
    async with activity.invoke(log) as invoke:
        await invoke.start()
        _ = await invoke.wait()
