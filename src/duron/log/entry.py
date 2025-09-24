from typing import Literal, TypeGuard

from typing_extensions import NotRequired, TypedDict

JSONValue = dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None


class BaseEntry(TypedDict):
    id: str
    # Unix timestamp in microseconds
    ts: int
    meta: NotRequired[dict[str, str]]


class ErrorInfo(TypedDict):
    code: int
    message: str
    state: NotRequired[str]  # opaque


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise/create"]


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise/complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(BaseEntry):
    type: Literal["stream/create"]


class StreamEmitEntry(BaseEntry):
    type: Literal["stream/emit"]
    stream_id: str
    value: NotRequired[JSONValue]
    state: NotRequired[str]  # opaque


class StreamCloseEntry(BaseEntry):
    type: Literal["stream/close"]
    stream_id: str
    error: NotRequired[ErrorInfo]


class AnyEntry(BaseEntry):
    type: str


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCloseEntry
)


def is_entry(entry: Entry | AnyEntry) -> TypeGuard[Entry]:
    return entry["type"] in {
        "promise/create",
        "promise/complete",
        "stream/create",
        "stream/emit",
        "stream/close",
    }
