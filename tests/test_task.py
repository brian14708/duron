import asyncio
import uuid

import pytest

import duron.context
import duron.task_runner


@pytest.mark.asyncio
async def test_task_runner():
    async def activity() -> str:
        ctx = duron.context.Context()
        x = await ctx.run(lambda: str(uuid.uuid4()))
        _ = await ctx.run(lambda: asyncio.sleep(0.1))
        return x

    tr = duron.task_runner.TaskRunner()
    _ = await tr.run(b"123", activity())
