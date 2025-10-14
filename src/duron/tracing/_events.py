from typing import Literal
from typing_extensions import NotRequired, TypedDict

from duron.codec import JSONValue


class LinkRef(TypedDict):
    trace_id: NotRequired[str]
    span_id: str


class SpanStart(TypedDict):
    type: Literal["span.start"]
    span_id: str
    ts: int
    name: str
    attributes: NotRequired[dict[str, JSONValue]]

    parent_span_id: NotRequired[str]
    links: NotRequired[list[LinkRef]]


class SpanEnd(TypedDict):
    type: Literal["span.end"]
    span_id: str
    ts: int
    attributes: NotRequired[dict[str, JSONValue]]


class Event(TypedDict):
    type: Literal["event"]
    span_id: NotRequired[str]
    ts: int
    kind: Literal["log", "stream"]
    attributes: NotRequired[dict[str, JSONValue]]


TraceEvent = SpanStart | SpanEnd | Event
