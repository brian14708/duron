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
    from duron.log import Entry
    from duron.tracing._events import LinkRef, Log, SpanEnd, SpanStart, TraceEvent
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

    def emit_log(self, span_id: str | None, level: str, msg: str) -> None:
        with self._lock:
            event: Log = {
                "type": "log",
                "message": msg,
                "level": level,
                "ts_us": time.time_ns() // 1000,
            }
            if span_id:
                event["span_id"] = span_id
            self._events.append(event)

    def emit_event(self, event: TraceEvent) -> None:
        with self._lock:
            self._events.append(event)

    def flush_events(self) -> tuple[str, list[dict[str, JSONValue]]]:
        with self._lock:
            old, self._events = self._events, []
            return self.instance_id, cast("list[dict[str, JSONValue]]", old)

    def new_span(
        self, metadata: dict[str, JSONValue] | None = None
    ) -> AbstractContextManager[Span]:
        parent = _TracerSpan.current()
        return _TracerSpan(
            _random_id(),
            tracer=self,
            attributes=metadata,
            parent_id=parent.id if parent else None,
        )

    def new_entry_span(
        self, entry: Entry, metadata: dict[str, JSONValue]
    ) -> AbstractContextManager[Span]:
        event: SpanStart = {
            "type": "span/start",
            "span_id": _derive_id(entry["id"]),
            "ts_us": time.time_ns() // 1000,
            "trace_id": self.trace_id,
        }
        parent = _TracerSpan.current()
        set_metadata(entry, {"trace": [cast("dict[str, JSONValue]", event)]})
        return _TracerSpan(
            _random_id(),
            tracer=self,
            parent_id=parent.id if parent else None,
            links=[{"span_id": entry["id"], "trace_id": self.trace_id}],
            attributes=metadata,
        )

    def end_entry_span(self, entry: Entry, id_: str, span: Span) -> Span:
        event: SpanEnd = {
            "type": "span/end",
            "span_id": _derive_id(id_),
            "ts_us": time.time_ns() // 1000,
            "trace_id": self.trace_id,
        }
        set_metadata(entry, {"trace": [cast("dict[str, JSONValue]", event)]})
        return span

    @staticmethod
    def current() -> Tracer | None:
        return current_tracer.get()


@dataclass(slots=True)
class _TracerSpan:
    id: str
    tracer: Tracer
    start_ns: int | None = None
    end_ns: int | None = None
    attributes: dict[str, JSONValue] | None = None
    parent_id: str | None = None
    links: list[LinkRef] | None = None
    _token: Token[Span | None] | None = None

    def __enter__(self) -> Self:
        self.start_ns = time.time_ns()
        evnt: SpanStart = {
            "type": "span/start",
            "span_id": self.id,
            "ts_us": self.start_ns // 1000,
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
        self.end_ns = time.time_ns()
        evnt: SpanEnd = {
            "type": "span/end",
            "span_id": self.id,
            "ts_us": self.end_ns // 1000,
        }
        if self.attributes:
            evnt["attributes"] = self.attributes
            self.attributes = None

        self.tracer.emit_event(evnt)
        if self._token:
            _current_span.reset(self._token)

    @staticmethod
    def current() -> Span | None:
        return _current_span.get()


class _LoggingHandler(logging.Handler):
    @override
    def emit(self, record: logging.LogRecord) -> None:
        if tracer := current_tracer.get():
            span = _current_span.get()
            tracer.emit_log(
                span.id if span else None, record.levelname, record.getMessage()
            )


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
