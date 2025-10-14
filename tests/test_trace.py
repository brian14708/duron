from __future__ import annotations

import contextvars

import pytest

from duron import Context, durable
from duron.contrib.storage import MemoryLogStorage
from duron.tracing import Tracer

test_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "test_var", default="no_value"
)


@pytest.mark.asyncio
async def test_contextvars() -> None:
    def u(v: str) -> None:
        assert test_var.get() == v

    @durable()
    async def activity(ctx: Context) -> None:
        _ = await ctx.run(u, "no_value")
        test_var.set("value1")
        _ = await ctx.run(u, "value1")

    log = MemoryLogStorage()
    async with activity.invoke(log, tracer=Tracer("abc")) as t:
        await t.start()
        await t.wait()
