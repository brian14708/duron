import asyncio
import uuid

import pytest

import duron.context
import duron.task_runner
from duron.log.storage.simple import MemoryLogStorage
from duron.mark import durable


@pytest.mark.asyncio
async def test_task_runner():
    @durable()
    async def activity() -> str:
        ctx = duron.context.Context()
        x = await asyncio.gather(
            ctx.run(lambda: str(uuid.uuid4())),
            ctx.run(lambda: str(uuid.uuid4())),
        )
        _ = await ctx.run(lambda: asyncio.sleep(0.1))
        return ":".join(x)

    tr = duron.task_runner.TaskRunner()
    log = MemoryLogStorage()
    a = await tr.run(b"1", activity, log)
    b = await tr.run(b"1", activity, log)
    assert a == b
    log2 = MemoryLogStorage((await log.entries())[:-2])
    c = await tr.run(b"1", activity, log2)
    assert a == c
    assert len(await log.entries()) == len(await log2.entries())


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
