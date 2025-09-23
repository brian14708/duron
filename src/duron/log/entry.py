from typing import Literal

from typing_extensions import NotRequired, TypedDict


class BaseEntry(TypedDict):
    id: str
    timestamp: int
    metadata: NotRequired[dict[str, str]]


class ErrorInfo(TypedDict):
    code: int
    message: str
    data: object


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise/create"]


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise/complete"]
    promise_id: str
    result: object
    error: ErrorInfo | None


class StreamCreateEntry(BaseEntry):
    type: Literal["stream/create"]


class StreamEmitEntry(BaseEntry):
    type: Literal["stream/emit"]
    stream_id: str
    value: object
    state: object


class StreamCloseEntry(BaseEntry):
    type: Literal["stream/close"]
    stream_id: str
    error: ErrorInfo | None


class UnknownEntry(BaseEntry):
    type: str


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCloseEntry
    | UnknownEntry
)
