from __future__ import annotations

from typing import Literal
from typing_extensions import NotRequired, TypedDict

from duron.errors import CorruptLogError as CorruptLogError  # noqa: PLC0414
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

    metadata: NotRequired[dict[str, JSONValue]]
    """
    Non-essential metadata associated with this log entry.
    """

    source: Literal["task", "effect", "trace"]
    """
    The operation that generated this log entry.
    """

    effect_name: NotRequired[str]
    """Stable public effect name for replay validation, when applicable."""

    effect_version: NotRequired[str]
    """Stable public effect version for replay validation, when applicable."""


class ErrorInfo(TypedDict):
    type: str
    module: str
    message: str
    args: list[JSONValue]
    cancelled: bool
    key: NotRequired[str]
    cause: NotRequired[ErrorInfo]


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise.create"]


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise.complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(BaseEntry):
    type: Literal["stream.create"]
    name: str | None


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
