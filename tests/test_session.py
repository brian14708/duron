from __future__ import annotations

import asyncio
import contextlib
import random
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

import pytest

from duron import Context, Provided, Session, Stream, StreamWriter, durable
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
        x = await asyncio.gather(ctx.run(u), ctx.run(u))
        _ = await ctx.run(asyncio.sleep, 0.1)
        return i + ":".join(x)

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity, "test")
        a = await run.result()

    async with Session(log) as t:
        run = await t.start(activity, "test")
        b = await run.result()
    assert a == b

    log2 = MemoryLogStorage((await log.entries())[:-2])
    async with Session(log2) as t:
        run = await t.start(activity, "test")
        c = await run.result()
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
    async with Session(log) as t:
        run = await t.start(activity)
        with pytest.raises(Exception, match="test error"):
            await run.result()
    async with Session(log) as t:
        run = await t.start(activity)
        with pytest.raises(Exception, match="test error"):
            await run.result()


@pytest.mark.asyncio
async def test_resume() -> None:
    sleep = 9999

    @durable()
    async def activity(ctx: Context, s: str) -> str:
        _ = await ctx.run(asyncio.sleep, sleep)
        return s

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity, "hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await asyncio.wait_for(run.result(), 0.1)

    async with Session(log) as t:
        sleep = 0
        x = await (await t.start(activity, "hello")).result()
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
    async with Session(log) as t:
        run = await t.start(activity, "hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await run.result()
    async with Session(log) as t:
        with pytest.raises(asyncio.TimeoutError):
            _ = await (await t.resume(activity)).result()


@pytest.mark.asyncio
async def test_timing() -> None:
    @durable()
    async def activity(ctx: Context) -> int:
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
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
    async with Session(log) as t:
        b = await (await t.resume(activity)).result()
    assert a == b


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
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
        assert type(a) is CustomPoint
        assert a.x == 6
        assert a.y == 12


@pytest.mark.asyncio
async def test_random() -> None:
    @durable()
    async def activity(ctx: Context) -> list[int]:
        await asyncio.sleep(0)
        return [await ctx.time_ns(), ctx.random().randint(1, 100)]

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()

    async with Session(log) as t:
        b = await (await t.resume(activity)).result()

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
    async with Session(log) as t:
        run = await t.start(activity)

        async def do() -> None:
            while True:
                if v.get("data") is None:
                    await asyncio.sleep(0.01)
                    continue
                await run.complete_future(v["data"], result=9)
                break

        bg = asyncio.create_task(do())
        assert await run.result() == 9
        await bg


@pytest.mark.asyncio
async def test_external_stream() -> None:
    @durable
    async def activity(_ctx: Context, test: Stream[int] = Provided) -> int:
        t = 0
        async for value in test:
            t += value
        return t

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity)
        test = await run.open_stream("test", "w")

        async def do() -> None:
            async with test as s:
                await s.send(0)
                for i in range(10):
                    await s.send(i)
                await s.send(-1)

        bg = asyncio.create_task(do())
        assert (await asyncio.gather(run.result(), bg))[0] == 44


@pytest.mark.asyncio
async def test_external_stream_write() -> None:
    @durable
    async def activity(_ctx: Context, writer: StreamWriter[int] = Provided) -> int:
        async with writer as writer_:
            for i in range(10):
                await writer_.send(i)
            return 42

    log = MemoryLogStorage()

    async with Session(log) as sess:
        run = await sess.start(activity)
        output_stream = await run.open_stream("writer", "r")

        async def bg() -> list[int]:
            values: list[int] = [value async for value in output_stream]
            return values

        b = asyncio.create_task(bg())
        result = await run.result()
        assert result == 42
        assert await b == list(range(10))


@pytest.mark.asyncio
async def test_invoke_wait_multiple() -> None:
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity(ctx: Context, i: str) -> str:
        x = await asyncio.gather(ctx.run(u), ctx.run(u))
        _ = await ctx.run(asyncio.sleep, 0.1)
        return i + ":".join(x)

    log = MemoryLogStorage()
    async with Session(log) as t:
        _ = await t.start(activity, "test")
        await asyncio.sleep(0)
    async with Session(log) as t:
        run = await t.resume(activity)
        while True:
            try:
                _ = await asyncio.wait_for(run.result(), 0.001)
                break
            except asyncio.TimeoutError:
                continue


@pytest.mark.asyncio
async def test_time() -> None:
    @durable()
    async def activity(ctx: Context) -> Sequence[int]:
        async def do() -> int:
            t = ctx.random().random()
            await asyncio.sleep(t * 0.1)
            return await ctx.time_ns()

        await asyncio.sleep(0)
        return await asyncio.gather(*[do() for _ in range(10)])

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()

    async with Session(log) as t:
        b = await (await t.start(activity)).result()

    assert a == b


@pytest.mark.asyncio
async def test_mismatch() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        _ = await ctx.time_ns()
        _ = await ctx.time_ns()

    @durable()
    async def activity2(ctx: Context) -> None:
        _ = await ctx.time_ns()
        _ = await ctx.time_ns()
        _ = await ctx.time_ns()

    @durable()
    async def activity3(ctx: Context) -> None:
        _ = await ctx.time_ns()

    log = MemoryLogStorage()
    async with Session(log) as t:
        await (await t.start(activity)).result()

    async with Session(log, readonly=True) as t:
        await t.verify(activity)

    async with Session(log) as t:
        with pytest.raises(RuntimeError, match="not complete"):
            await t.verify(activity2)

    async with Session(log) as t:
        with pytest.raises(RuntimeError, match="Extra"):
            await t.verify(activity3)


@pytest.mark.asyncio
async def test_fast_error() -> None:
    @durable()
    async def activity(_ctx: Context) -> None:
        msg = "test error"
        raise ValueError(msg)

    log = MemoryLogStorage()
    async with Session(log) as t:
        with pytest.raises(ValueError, match="test error"):
            await (await t.start(activity)).result()


@durable
async def activity(ctx: Context) -> int:
    for _i in range(100):
        _ = await ctx.time_ns()
    return 42


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_performance() -> None:
    log = MemoryLogStorage()
    async with Session(log) as run:
        _ = await (await run.start(activity)).result()
