from __future__ import annotations

import contextlib
import contextvars
import json
from typing import TYPE_CHECKING

from duron import Context, Session, durable
from duron.contrib.storage import MemoryLogStorage
from duron.log._helper import is_entry
from duron.tracing import Tracer, span
from duron.tracing._tracer import TracerState

if TYPE_CHECKING:
    from duron.typing import JSONValue


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
    async with Session(log) as t:
        await (await t.start(activity)).result()


async def test_trace() -> None:
    @durable()
    async def activity(_ctx: Context) -> None:
        with span("hello_span") as s:
            s.record(foo="foobar")
            s.set_status("OK")

    log = MemoryLogStorage()
    async with Session(log, tracer=Tracer("abc")) as t:
        await (await t.start(activity)).result()

    events: list[dict[str, JSONValue]] = []
    for entry in await log.entries():
        if is_entry(entry) and entry["type"] == "trace":
            events.extend(entry["events"])
    assert '"foobar"' in json.dumps(events)


def test_tracer_state_machine_and_event_ordering() -> None:
    tracer = Tracer("trace", run_id="run")
    with tracer.new_span("discarded"):
        pass
    assert tracer.pop_events(flush=True) == []

    open_span = tracer.new_span("open")
    with contextlib.ExitStack() as stack:
        stack.enter_context(open_span)
        tracer.start()
        assert tracer._state is TracerState.STARTED  # pyright: ignore[reportPrivateUsage] # noqa: SLF001
        buffered = tracer.pop_events(flush=True)
        assert [event["type"] for event in buffered] == ["span.start"]

    completed = tracer.pop_events(flush=True)
    assert [event["type"] for event in completed] == ["span.end"]
    assert completed[0]["status"] == "OK"


def test_tracer_pop_batching_and_close_open_spans() -> None:
    tracer = Tracer("trace", run_id="run")
    tracer.start()
    with tracer.new_span("one"):
        pass
    assert tracer.pop_events(flush=False) == []

    stack = contextlib.ExitStack()
    stack.enter_context(tracer.new_span("open"))
    tracer.close()
    stack.close()
    events = tracer.pop_events(flush=True)
    assert [event["type"] for event in events] == [
        "span.start",
        "span.end",
        "span.start",
        "span.end",
    ]
    assert events[-1]["status"] == "ERROR"
    assert events[-1]["status_message"] == "tracer closed"
    assert tracer._state is TracerState.CLOSED  # pyright: ignore[reportPrivateUsage] # noqa: SLF001

    tracer.emit_event({
        "type": "event",
        "kind": "log",
        "ts": 1,
        "attributes": {"message": "ignored"},
    })
    assert tracer.pop_events(flush=True) == []
