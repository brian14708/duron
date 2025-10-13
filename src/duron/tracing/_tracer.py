from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import blake2b
from typing import TYPE_CHECKING, cast
from typing_extensions import Self, override

from duron.log import set_metadata
from duron.tracing._span import NULL_SPAN

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from contextvars import Token
    from types import TracebackType

    from duron.codec import JSONValue
    from duron.log import (
        Entry,
        PromiseCreateEntry,
        StreamCreateEntry,
    )
    from duron.tracing._events import Event, LinkRef, SpanEnd, SpanStart, TraceEvent
    from duron.tracing._span import Span

current_tracer: ContextVar[Tracer | None] = ContextVar("duron.tracer", default=None)
_current_span: ContextVar[Span | None] = ContextVar("duron.span", default=None)


class Tracer:
    __slots__: tuple[str, ...] = ("_events", "_lock", "instance_id", "trace_id")

    def __init__(
        self,
        trace_id: str,
        /,
        *,
        instance_id: str | None = None,
    ) -> None:
        self.trace_id: str = trace_id
        self.instance_id: str = instance_id or uuid.uuid4().hex
        self._events: list[TraceEvent] = []
        self._lock = threading.Lock()

    def emit_event(self, event: TraceEvent) -> None:
        with self._lock:
            self._events.append(event)

    def flush_events(self) -> tuple[str, list[dict[str, JSONValue]]]:
        with self._lock:
            old, self._events = self._events, []
            return self.instance_id, cast("list[dict[str, JSONValue]]", old)

    def new_span(
        self,
        attributes: dict[str, JSONValue] | None = None,
        links: list[LinkRef] | None = None,
    ) -> AbstractContextManager[Span]:
        parent = _current_span.get()
        return _TracerSpan(
            _random_id(),
            tracer=self,
            attributes=attributes,
            parent_id=parent.id if parent else None,
            links=links,
        )

    def new_entry_span(
        self,
        entry: PromiseCreateEntry | StreamCreateEntry,
        attributes: dict[str, JSONValue],
    ) -> EntrySpan:
        event: SpanStart = {
            "type": "span.start",
            "span_id": _derive_id(entry["id"]),
            "ts": time.time_ns() // 1000,
        }
        if attributes:
            event["attributes"] = attributes
        set_metadata(
            entry,
            {
                "trace.id": self.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )
        return EntrySpan(_derive_id(entry["id"]), tracer=self)

    @staticmethod
    def current() -> Tracer | None:
        return current_tracer.get()


@dataclass(slots=True)
class EntrySpan:
    id: str
    tracer: Tracer

    def __init__(
        self,
        id_: str,
        tracer: Tracer,
    ) -> None:
        self.id = id_
        self.tracer = tracer

    def new_span(
        self, attributes: dict[str, JSONValue] | None = None
    ) -> AbstractContextManager[Span]:
        link: LinkRef = {
            "span_id": self.id,
            "trace_id": self.tracer.trace_id,
        }
        return self.tracer.new_span(attributes, links=[link])

    def end(self, entry: Entry) -> None:
        event: SpanEnd = {
            "type": "span.end",
            "span_id": self.id,
            "ts": time.time_ns() // 1000,
        }
        set_metadata(
            entry,
            {
                "trace.id": self.tracer.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )

    def attach(self, entry: Entry, event: Event) -> None:
        event["span_id"] = self.id
        set_metadata(
            entry,
            {
                "trace.id": self.tracer.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )


@dataclass(slots=True)
class _TracerSpan:
    id: str
    tracer: Tracer
    parent_id: str | None = None
    attributes: dict[str, JSONValue] | None = None
    links: list[LinkRef] | None = None
    _token: Token[Span | None] | None = None

    def __enter__(self) -> Self:
        start_ns = time.time_ns()
        evnt: SpanStart = {
            "type": "span.start",
            "span_id": self.id,
            "ts": start_ns // 1000,
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

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        end_ns = time.time_ns()
        evnt: SpanEnd = {
            "type": "span.end",
            "span_id": self.id,
            "ts": end_ns // 1000,
        }
        if self.attributes:
            evnt["attributes"] = self.attributes
            self.attributes = None

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
    metadata: dict[str, JSONValue] | None = None,
) -> AbstractContextManager[Span]:
    if tracer := current_tracer.get():
        return tracer.new_span(metadata)
    return NULL_SPAN


def _random_id() -> str:
    return os.urandom(8).hex()


def _derive_id(base: str) -> str:
    return blake2b(base.encode(), digest_size=8).hexdigest()
