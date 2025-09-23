from typing import Literal, TypeGuard

from typing_extensions import NotRequired, TypedDict


class BaseEntry(TypedDict):
    id: str
    # Unix timestamp in microseconds
    ts: int
    meta: NotRequired[dict[str, str]]


class ErrorInfo(TypedDict):
    code: int
    message: str
    data: NotRequired[object]


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise/create"]


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise/complete"]
    promise_id: str
    result: NotRequired[object]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(BaseEntry):
    type: Literal["stream/create"]


class StreamEmitEntry(BaseEntry):
    type: Literal["stream/emit"]
    stream_id: str
    value: NotRequired[object]
    state: NotRequired[object]


class StreamCloseEntry(BaseEntry):
    type: Literal["stream/close"]
    stream_id: str
    error: NotRequired[ErrorInfo]


class UnknownEntry(BaseEntry):
    type: str


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCloseEntry
)


def is_entry(entry: Entry | UnknownEntry) -> TypeGuard[Entry]:
    return entry["type"] in {
        "promise/create",
        "promise/complete",
        "stream/create",
        "stream/emit",
        "stream/close",
    }
