from __future__ import annotations

import contextvars
import json
from typing import TYPE_CHECKING

import pytest

from duron import Context, durable
from duron.contrib.storage import MemoryLogStorage
from duron.log._helper import is_entry
from duron.tracing import Tracer, span

if TYPE_CHECKING:
    from duron.typing import JSONValue


@pytest.mark.asyncio
async def test_contextvars() -> None:
    test_var: contextvars.ContextVar[str] = contextvars.ContextVar(
        "test_var", default="no_value"
    )

    def u(v: str) -> None:
        assert test_var.get() == v

    @durable()
    async def activity(ctx: Context) -> None:
        _ = await ctx.run(u, "no_value")
        test_var.set("value1")
        _ = await ctx.run(u, "value1")

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        await t.wait()


@pytest.mark.asyncio
async def test_trace() -> None:
    @durable()
    async def activity(_ctx: Context) -> None:  # noqa: RUF029
        with span("hello_span") as s:
            s.record(foo="foobar")
            s.set_status("OK")

    log = MemoryLogStorage()
    async with activity.invoke(log, tracer=Tracer("abc")) as t:
        await t.start()
        await t.wait()

    events: list[dict[str, JSONValue]] = []
    for entry in await log.entries():
        if is_entry(entry) and entry["type"] == "trace":
            events.extend(entry["events"])
    assert '"foobar"' in json.dumps(events)
