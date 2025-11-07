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
from typing_extensions import NamedTuple, Self, override

from duron.log._helper import set_metadata
from duron.tracing._span import NULL_SPAN

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from contextlib import AbstractContextManager
    from contextvars import Token
    from types import TracebackType

    from duron.log._entry import Entry, PromiseCompleteEntry, StreamCompleteEntry
    from duron.tracing._events import Event, LinkRef, SpanEnd, SpanStart, TraceEvent
    from duron.tracing._span import Span
    from duron.typing import JSONValue

current_tracer: ContextVar[Tracer] = ContextVar("duron.tracer")
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
        "run_id",
        "trace_id",
    )

    def __init__(self, trace_id: str, /, *, run_id: str | None = None) -> None:
        """Create a new Tracer with the given trace_id and optional run_id.

        `trace_id` should be unique and remain constant across retries of the same
        task. `run_id` should be unique for each a task run.

        Args:
            trace_id (str): The trace identifier
            run_id (str | None): The run identifier. If None, a random id will be \
                    generated.

        """
        self.trace_id: str = trace_id
        self.run_id: str = run_id or _trace_id()
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
                if event["type"] == "span.start":
                    # only keep span.start events in INIT state
                    self._init_buffer.append(event)
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
        with self._lock:
            if self._state != TracerState.INIT:
                return

            for event in self._init_buffer:
                event_span_id = event.get("span_id")

                if event_span_id in self._open_spans:
                    self._events.append(event)

            self._init_buffer.clear()
            self._state = TracerState.STARTED

    def close(self) -> None:
        with self._lock:
            if self._state in {TracerState.CLOSED, TracerState.INIT}:
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
            if len(self._events) < 4 and not flush:
                return []

            old, self._events = self._events, []
            return cast("list[dict[str, JSONValue]]", old)

    def new_span(
        self,
        name: str,
        attributes: Mapping[str, JSONValue] | None = None,
        links: Iterable[LinkRef] | None = None,
    ) -> AbstractContextManager[Span]:
        parent = _current_span.get()
        id_ = _random_id()
        evnt: SpanStart = {"type": "span.start", "span_id": id_, "ts": -1, "name": name}
        if parent:
            evnt["parent_span_id"] = parent.id
        if attributes:
            evnt["attributes"] = attributes
        if links:
            evnt["links"] = tuple(links)

        return _TracerSpan(id_, tracer=self, start_event=evnt)

    def new_op_span(self, name: str, entry: Entry) -> OpSpan:
        id_ = _derive_id(entry["id"])
        event: SpanStart = {
            "type": "span.start",
            "name": name,
            "span_id": id_,
            "ts": time.time_ns() // 1000,
        }
        set_metadata(
            entry,
            {
                "trace.id": self.trace_id,
                "trace.event": cast("dict[str, JSONValue]", event),
            },
        )
        return OpSpan(id=id_, tracer=self)

    def end_op_span(
        self, origin_entry_id: str, entry: PromiseCompleteEntry | StreamCompleteEntry
    ) -> None:
        OpSpan(id=_derive_id(origin_entry_id), tracer=self).end(entry)

    @staticmethod
    def current() -> Tracer | None:
        return current_tracer.get(None)


class OpSpan(NamedTuple):
    id: str
    tracer: Tracer

    def new_span(
        self, name: str, attributes: Mapping[str, JSONValue] | None = None
    ) -> AbstractContextManager[Span]:
        link: LinkRef = {"span_id": self.id, "trace_id": self.tracer.trace_id}
        return self.tracer.new_span(name, attributes, links=(link,))

    def end(self, entry: PromiseCompleteEntry | StreamCompleteEntry) -> None:
        event: SpanEnd = {
            "type": "span.end",
            "span_id": self.id,
            "ts": time.time_ns() // 1000,
            "status": "ERROR" if "error" in entry else "OK",
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
        event["ts"] = time.time_ns() // 1000
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
    start_event: SpanStart | None
    _token: Token[_TracerSpan | None] | None = None
    _status: Literal["OK", "ERROR"] | None = None
    _status_message: str | None = None
    _attributes: dict[str, JSONValue] | None = None

    def __enter__(self) -> Self:
        start_ns = time.time_ns()
        if self.start_event:
            self.start_event["ts"] = start_ns // 1000
            self.tracer.emit_event(self.start_event)
            self.start_event = None
            token = _current_span.set(self)
            self._token = token
        return self

    def record(self, **kwargs: JSONValue) -> None:
        if a := self._attributes:
            for key, value in kwargs.items():
                a[key] = value
        else:
            self._attributes = kwargs

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
        if self._token:
            if self._status is None:
                if exc_type is not None:
                    self._status = "ERROR"
                    if exc_value is not None:
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
            if self._attributes:
                evnt["attributes"] = self._attributes
                self._attributes = None
            if self._status_message:
                evnt["status_message"] = self._status_message

            self.tracer.emit_event(evnt)
            _current_span.reset(self._token)


class _LoggingHandler(logging.Handler):
    @override
    def emit(self, record: logging.LogRecord) -> None:
        if tracer := current_tracer.get(None):
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
    level: int = logging.INFO, *, logger: logging.Logger | None = None
) -> None:
    """Configure logging integration to capture log messages as trace events.

    Installs a logging handler that emits log records as trace events, attaching
    them to the current span if one exists. This enables correlation of log
    messages with trace spans.

    Args:
        level: Minimum logging level to capture (default: logging.INFO)
        logger: Target logger to configure. If None, configures the root logger.

    Raises:
        RuntimeError: if the logging handler is already configured.

    """
    target_logger = logger if logger is not None else logging.getLogger()

    # Check if handler already exists to avoid duplicates
    for handler in target_logger.handlers:
        if isinstance(handler, _LoggingHandler):
            msg = "Logging handler for tracing is already configured."
            raise RuntimeError(msg)  # noqa: TRY004

    handler = _LoggingHandler()
    handler.setLevel(level)
    target_logger.addHandler(handler)

    # Ensure the logger level allows messages through
    if target_logger.level == logging.NOTSET or target_logger.level > level:
        target_logger.setLevel(level)


def span(
    name: str, metadata: Mapping[str, JSONValue] | None = None
) -> AbstractContextManager[Span]:
    """Create a new tracing span within the current tracer context.

    Args:
        name: Human-readable name for the span (e.g., "fetch_user", "process_data")
        metadata: Optional attributes to attach to the span. Must be JSON-serializable.

    Returns:
        A context manager that enters the span on entry and exits on exit.

    """
    if tracer := current_tracer.get(None):
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
