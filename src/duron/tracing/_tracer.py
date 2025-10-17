from __future__ import annotations

import enum
import logging
import os
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import blake2b
from typing import TYPE_CHECKING, Literal, cast
from typing_extensions import Self, override

from duron.log._helper import set_annotations
from duron.tracing._span import NULL_SPAN

if TYPE_CHECKING:
    from collections.abc import Mapping
    from contextlib import AbstractContextManager
    from contextvars import Token
    from types import TracebackType

    from duron.log._entry import (
        Entry,
        PromiseCompleteEntry,
        PromiseCreateEntry,
        StreamCompleteEntry,
        StreamCreateEntry,
    )
    from duron.tracing._events import Event, LinkRef, SpanEnd, SpanStart, TraceEvent
    from duron.tracing._span import Span
    from duron.typing import JSONValue

current_tracer: ContextVar[Tracer | None] = ContextVar("duron.tracer", default=None)
_current_span: ContextVar[_TracerSpan | None] = ContextVar(
    "duron.tracer.span", default=None
)


class TracerState(enum.Enum):
    INIT = "init"
    STARTED = "started"
    CLOSED = "closed"


class Tracer:
    __slots__: tuple[str, ...] = (
        "_events",
        "_init_buffer",
        "_lock",
        "_open_spans",
        "_state",
        "instance_id",
        "trace_id",
    )

    def __init__(
        self,
        trace_id: str,
        /,
        *,
        instance_id: str | None = None,
    ) -> None:
        self.trace_id: str = trace_id
        self.instance_id: str = instance_id or _trace_id()
        self._events: list[TraceEvent] = []
        self._lock = threading.Lock()
        self._state: TracerState = TracerState.INIT
        self._init_buffer: list[TraceEvent] = []
        self._open_spans: dict[str, SpanStart] = {}

    def emit_event(self, event: TraceEvent) -> None:
        with self._lock:
            if self._state == TracerState.CLOSED:
                return

            if self._state == TracerState.INIT:
                # Buffer events in INIT state
                self._init_buffer.append(event)
                # Track open/closed spans
                if event["type"] == "span.start":
                    self._open_spans[event["span_id"]] = event
                elif event["type"] == "span.end":
                    self._open_spans.pop(event["span_id"], None)
            else:  # STARTED state
                self._events.append(event)
                # Track open spans even in STARTED state for close()
                if event["type"] == "span.start":
                    self._open_spans[event["span_id"]] = event
                elif event["type"] == "span.end":
                    self._open_spans.pop(event["span_id"], None)

    def start(self) -> None:
        """Transition to STARTED state, clear completed spans, emit remaining."""
        with self._lock:
            if self._state != TracerState.INIT:
                return

            # Find all span IDs that have been completed (have both start and end)
            completed_span_ids: set[str] = set()
            span_ends: set[str] = set()

            for event in self._init_buffer:
                if event["type"] == "span.end":
                    span_ends.add(event["span_id"])

            for event in self._init_buffer:
                if event["type"] == "span.start" and event["span_id"] in span_ends:
                    completed_span_ids.add(event["span_id"])

            # Filter out completed spans (both start and end) and their related events
            # Keep only span.start events for incomplete spans and other events
            for event in self._init_buffer:
                event_span_id = event.get("span_id")

                # Skip completed span events (both start and end)
                if event_span_id in completed_span_ids:
                    continue

                # Skip span.end events for incomplete spans (keep only starts)
                if event["type"] == "span.end":
                    continue

                # Emit everything else (span.start for incomplete spans, other events)
                self._events.append(event)

            # Clear the init buffer
            self._init_buffer.clear()

            # Transition to STARTED state
            self._state = TracerState.STARTED

    def close(self) -> None:
        """Transition to CLOSED state and mark all open spans as failed."""
        with self._lock:
            if self._state == TracerState.CLOSED or self._state == TracerState.INIT:
                return

            for span_id in list(self._open_spans.keys()):
                end_event: SpanEnd = {
                    "type": "span.end",
                    "span_id": span_id,
                    "ts": time.time_ns() // 1000,
                    "status": "ERROR",
                    "status_message": "tracer closed",
                }
                self._events.append(end_event)

            self._open_spans.clear()

            self._state = TracerState.CLOSED

    def pop_events(self, *, flush: bool) -> list[dict[str, JSONValue]]:
        with self._lock:
            if len(self._events) < 16 and not flush:
                return []

            old, self._events = self._events, []
            return cast("list[dict[str, JSONValue]]", old)

    def new_span(
        self,
        name: str,
        attributes: Mapping[str, JSONValue] | None = None,
        links: tuple[LinkRef, ...] | None = None,
    ) -> AbstractContextManager[Span]:
        parent = _current_span.get()
        return _TracerSpan(
            _random_id(),
            tracer=self,
            name=name,
            attributes=dict(attributes) if attributes else None,
            parent_id=parent.id if parent else None,
            links=links,
        )

    def new_op_span(
        self,
        name: str,
        entry: PromiseCreateEntry | StreamCreateEntry,
    ) -> OpSpan:
        event: SpanStart = {
            "type": "span.start",
            "name": name,
            "span_id": _derive_id(entry["id"]),
            "ts": time.time_ns() // 1000,
        }
        set_annotations(
            entry,
            metadata={
                "trace.id": self.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )
        return OpSpan(
            id=_derive_id(entry["id"]),
            tracer=self,
        )

    def end_op_span(
        self, origin_entry_id: str, entry: PromiseCompleteEntry | StreamCompleteEntry
    ) -> None:
        OpSpan(id=_derive_id(origin_entry_id), tracer=self).end(entry)

    @staticmethod
    def current() -> Tracer | None:
        return current_tracer.get()


@dataclass(slots=True)
class OpSpan:
    id: str
    tracer: Tracer

    def new_span(
        self, name: str, attributes: Mapping[str, JSONValue] | None = None
    ) -> AbstractContextManager[Span]:
        link: LinkRef = {
            "span_id": self.id,
            "trace_id": self.tracer.trace_id,
        }
        return self.tracer.new_span(name, attributes, links=(link,))

    def end(self, entry: PromiseCompleteEntry | StreamCompleteEntry) -> None:
        event: SpanEnd = {
            "type": "span.end",
            "span_id": self.id,
            "ts": time.time_ns() // 1000,
            "status": "ERROR" if "error" in entry else "OK",
        }
        set_annotations(
            entry,
            metadata={
                "trace.id": self.tracer.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )

    def attach(self, entry: Entry, event: Event) -> None:
        event["span_id"] = self.id
        set_annotations(
            entry,
            metadata={
                "trace.id": self.tracer.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )


@dataclass(slots=True)
class _TracerSpan:
    id: str
    tracer: Tracer
    name: str
    parent_id: str | None = None
    attributes: dict[str, JSONValue] | None = None
    links: tuple[LinkRef, ...] | None = None
    _token: Token[_TracerSpan | None] | None = None
    _status: Literal["OK", "ERROR"] | None = None
    _status_message: str | None = None

    def __enter__(self) -> Self:
        start_ns = time.time_ns()
        evnt: SpanStart = {
            "type": "span.start",
            "span_id": self.id,
            "ts": start_ns // 1000,
            "name": self.name,
        }
        if self.parent_id:
            evnt["parent_span_id"] = self.parent_id
        if self.attributes:
            evnt["attributes"] = self.attributes
            self.attributes = None
        if self.links:
            evnt["links"] = self.links
            self.links = None
        self.tracer.emit_event(evnt)
        token = _current_span.set(self)
        self._token = token
        return self

    def record(self, key: str, value: JSONValue) -> None:
        if a := self.attributes:
            a[key] = value
        else:
            self.attributes = {key: value}

    def set_status(
        self, status: Literal["OK", "ERROR"], message: str | None = None
    ) -> None:
        self._status = status
        self._status_message = message

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Set status based on exception if not explicitly set
        if self._status is None:
            if exc_type is not None:
                self._status = "ERROR"
                if self._status_message is None and exc_value is not None:
                    self._status_message = str(exc_value)
            else:
                self._status = "OK"

        end_ns = time.time_ns()
        evnt: SpanEnd = {
            "type": "span.end",
            "span_id": self.id,
            "ts": end_ns // 1000,
            "status": self._status,
        }
        if self.attributes:
            evnt["attributes"] = self.attributes
            self.attributes = None
        if self._status_message:
            evnt["status_message"] = self._status_message

        self.tracer.emit_event(evnt)
        if self._token:
            _current_span.reset(self._token)


class _LoggingHandler(logging.Handler):
    @override
    def emit(self, record: logging.LogRecord) -> None:
        if tracer := current_tracer.get():
            span = _current_span.get()
            event: Event = {
                "type": "event",
                "kind": "log",
                "ts": time.time_ns() // 1000,
                "attributes": {
                    "level": record.levelname,
                    "message": record.getMessage(),
                },
            }
            if span:
                event["span_id"] = span.id
            tracer.emit_event(event)


def setup_tracing(
    level: int = logging.INFO,
    *,
    logger: logging.Logger | None = None,
) -> _LoggingHandler:
    target_logger = logger if logger is not None else logging.getLogger()

    # Check if handler already exists to avoid duplicates
    for handler in target_logger.handlers:
        if isinstance(handler, _LoggingHandler):
            return handler

    handler = _LoggingHandler()
    handler.setLevel(level)
    target_logger.addHandler(handler)

    # Ensure the logger level allows messages through
    if target_logger.level == logging.NOTSET or target_logger.level > level:
        target_logger.setLevel(level)

    return handler


def span(
    name: str,
    metadata: Mapping[str, JSONValue] | None = None,
) -> AbstractContextManager[Span]:
    if tracer := current_tracer.get():
        return tracer.new_span(name, metadata)
    return NULL_SPAN


def _random_id() -> str:
    return os.urandom(8).hex()


def _derive_id(base: str) -> str:
    return blake2b(base.encode(), digest_size=8).hexdigest()


def _trace_id() -> str:
    data = bytearray(16)
    data[:6] = (time.time_ns() // 1_000_000).to_bytes(6, "big")
    data[6:] = os.urandom(10)
    data[6] = (data[6] & 0x0F) | 0x70
    data[8] = (data[8] & 0x3F) | 0x80
    return data.hex()
