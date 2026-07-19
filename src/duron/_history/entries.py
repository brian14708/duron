from __future__ import annotations

from typing import Literal, NotRequired
from typing_extensions import TypedDict

from duron._hints import JSONValue
from duron._ref import Ref
from duron.errors import CorruptLogError as CorruptLogError  # ruff: ignore[useless-import-alias]


class BaseEntry(TypedDict):
    id: str
    """
    Identifier for this log entry, unique within the log.
    """

    ts: int
    """
    Timestamp for this log entry, in microseconds since the Unix epoch.
    """

    source: Literal["task", "effect"]
    """
    The operation that generated this log entry.
    """


class ErrorInfo(TypedDict):
    type: str
    message: str
    details: NotRequired[JSONValue]
    cancelled: NotRequired[Literal[True]]
    cause: NotRequired[ErrorInfo]


class InputAcceptedEntry(BaseEntry):
    type: Literal["input.accept"]
    port: str
    value: NotRequired[JSONValue]
    closed: bool


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise.create"]
    ref: NotRequired[Ref]
    args: NotRequired[list[JSONValue]]
    kwargs: NotRequired[dict[str, JSONValue]]


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise.complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(BaseEntry):
    type: Literal["stream.create"]
    name: NotRequired[str]
    owned: NotRequired[JSONValue]


class StreamEmitEntry(BaseEntry):
    type: Literal["stream.emit"]
    stream_id: str
    value: JSONValue


class StreamCompleteEntry(BaseEntry):
    type: Literal["stream.complete"]
    stream_id: str


class BarrierEntry(BaseEntry):
    type: Literal["barrier"]


Entry = (
    InputAcceptedEntry
    | PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
    | BarrierEntry
)
"""
Concrete log entry types used within Duron.
"""
