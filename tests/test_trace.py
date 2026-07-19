from __future__ import annotations

import contextlib
import contextvars
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, cast

import duron
from duron.contrib.storage import MemoryStorage
from duron.tracing import Tracer, span
from duron.tracing._tracer import TracerState

if TYPE_CHECKING:
    from duron.typing import JSONValue


@duron.effect(executor="inline")
def read_var(check: str, value: str) -> None:
    assert value == check


async def test_contextvars() -> None:
    test_var: contextvars.ContextVar[str] = contextvars.ContextVar(
        "test_var", default="no_value"
    )

    @duron.workflow
    async def activity(ctx: duron.WorkflowContext) -> None:
        _ = await ctx.call(read_var, "no_value", test_var.get())
        test_var.set("value1")
        _ = await ctx.call(read_var, "value1", test_var.get())

    await duron.run(activity(), MemoryStorage())


async def test_trace() -> None:
    @duron.workflow
    async def activity(_ctx: duron.WorkflowContext) -> None:
        with span("hello_span") as s:
            s.record(foo="foobar")
            s.set_status("OK")

    storage = MemoryStorage()
    await duron.run(activity(), storage, tracer=Tracer("abc"))

    events: list[dict[str, JSONValue]] = []
    for entry in await storage.entries():
        if entry.get("type") == "trace":
            events.extend(entry["events"])  # type: ignore[typeddict-item]
    assert '"foobar"' in json.dumps(events)


async def test_completed_stream_trace_is_named_and_not_cancelled() -> None:
    @duron.effect
    async def numbers() -> AsyncIterator[int]:
        yield 1

    @duron.workflow
    async def activity(ctx: duron.WorkflowContext) -> None:
        async with ctx.stream(numbers) as stream:
            assert [value async for value in stream] == [1]

    storage = MemoryStorage()
    await duron.run(activity(), storage, tracer=Tracer("trace"))

    events: list[dict[str, JSONValue]] = []
    for entry in await storage.entries():
        metadata = entry.get("metadata", {})
        if trace_event := metadata.get("trace.event"):
            events.append(cast("dict[str, JSONValue]", trace_event))
        if entry.get("type") == "trace":
            events.extend(entry["events"])  # type: ignore[typeddict-item]

    starts = {
        event["span_id"]: cast("str", event["name"])
        for event in events
        if event["type"] == "span.start"
    }
    ends = {
        event["span_id"]: event["status"]
        for event in events
        if event["type"] == "span.end"
    }
    number_spans = [span_id for span_id, name in starts.items() if "numbers" in name]
    assert number_spans
    assert all(ends.get(span_id) == "OK" for span_id in number_spans)


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
