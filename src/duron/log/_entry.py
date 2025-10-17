from __future__ import annotations

from typing import Literal
from typing_extensions import NotRequired, TypedDict

from duron.typing import JSONValue


class BaseEntry(TypedDict):
    id: str
    """
    Identifier for this log entry, unique within the log.
    """

    ts: int
    """
    Timestamp for this log entry, in microseconds since the Unix epoch.
    """

    labels: NotRequired[dict[str, str]]
    """
    Arbitrary string labels associated with this log entry.
    """

    metadata: NotRequired[dict[str, JSONValue]]
    """
    Non-essential metadata associated with this log entry.
    """


class ErrorInfo(TypedDict):
    code: int
    message: str


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise.create"]


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise.complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(BaseEntry):
    type: Literal["stream.create"]


class StreamEmitEntry(BaseEntry):
    type: Literal["stream.emit"]
    stream_id: str
    value: JSONValue


class StreamCompleteEntry(BaseEntry):
    type: Literal["stream.complete"]
    stream_id: str
    error: NotRequired[ErrorInfo]


class BarrierEntry(BaseEntry):
    type: Literal["barrier"]


class TraceEntry(BaseEntry):
    type: Literal["trace"]
    events: list[dict[str, JSONValue]]


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
    | BarrierEntry
    | TraceEntry
)
"""
Concrete log entry types used within Duron.
"""
