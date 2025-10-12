from typing import Literal
from typing_extensions import NotRequired, TypedDict

from duron.codec import JSONValue


class LinkRef(TypedDict):
    trace_id: NotRequired[str]
    span_id: str


class SpanStart(TypedDict):
    type: Literal["span/start"]
    trace_id: NotRequired[str]
    span_id: str
    ts_us: int
    attributes: NotRequired[dict[str, JSONValue]]

    parent_span_id: NotRequired[str]
    links: NotRequired[list[LinkRef]]


class SpanEnd(TypedDict):
    type: Literal["span/end"]
    trace_id: NotRequired[str]
    span_id: str
    ts_us: int
    attributes: NotRequired[dict[str, JSONValue]]


class Log(TypedDict):
    type: Literal["log"]
    trace_id: NotRequired[str]
    span_id: NotRequired[str]
    ts_us: int
    attributes: NotRequired[dict[str, JSONValue]]

    message: str
    level: str


TraceEvent = SpanStart | SpanEnd | Log
