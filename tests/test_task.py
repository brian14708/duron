import asyncio
import random
import uuid
from typing import ClassVar

import pytest

import duron.context
import duron.task_runner
from duron.log.storage.simple import MemoryLogStorage
from duron.mark import durable


@pytest.mark.asyncio
async def test_task():
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity() -> str:
        ctx = duron.context.Context()
        x = await asyncio.gather(
            ctx.run(u),
            ctx.run(u),
        )
        _ = await ctx.run(lambda: asyncio.sleep(0.1))
        return ":".join(x)

    IDS = {
        "7evDQ22rsIP/UYdw",
        "mIH0bXAyM4oPgKm+",
        "7KUhMsBawSVc+MPu",
        "DKAqXHcJF+qW4GaI",
        "BVl/eYXx4b7qmIvo",
        "dKNNCb6Im+ms3aI7",
        "W3z2PXIiVl6oap5/",
        "C9BcE55l4Maj1X/1",
    }

    tr = duron.task_runner.TaskRunner()
    log = MemoryLogStorage()
    a = await tr.run(b"1", activity, log)
    assert set(e["id"] for e in await log.entries()) == IDS
    b = await tr.run(b"1", activity, log)
    assert a == b
    log2 = MemoryLogStorage((await log.entries())[:-2])
    c = await tr.run(b"1", activity, log2)
    assert a == c
    assert set(e["id"] for e in await log2.entries()) == IDS


@pytest.mark.asyncio
async def test_task_error():
    @durable()
    async def activity():
        ctx = duron.context.Context()
        _ = await ctx.run(lambda: asyncio.sleep(0.1))

        async def error():
            raise ValueError("test error")

        _ = await ctx.run(error)

    tr = duron.task_runner.TaskRunner()
    log = MemoryLogStorage()
    with pytest.raises(check=lambda v: "test error" in str(v)):
        _ = await tr.run(b"1", activity, log)
    with pytest.raises(check=lambda v: "test error" in str(v)):
        _ = await tr.run(b"1", activity, log)


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
            ctx = duron.context.Context()
            pt = await ctx.run(lambda: Point(x=1, y=2))
            return Point(x=pt.x + 5, y=pt.y + 10)

        tr = duron.task_runner.TaskRunner()
        log = MemoryLogStorage()
        a = await tr.run(b"1", activity, log)
        assert type(a) is Point
        assert a.x == 6 and a.y == 12
except ImportError:
    pass
