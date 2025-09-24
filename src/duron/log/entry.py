from typing import Literal, TypeGuard

from typing_extensions import NotRequired, TypedDict

JSONValue = dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None


class _BaseEntry(TypedDict):
    id: str
    # Unix timestamp in microseconds
    ts: int
    meta: NotRequired[dict[str, str]]


class ErrorInfo(TypedDict):
    code: int
    message: str
    state: NotRequired[str]  # opaque


class PromiseCreateEntry(_BaseEntry):
    type: Literal["promise/create"]


class PromiseCompleteEntry(_BaseEntry):
    type: Literal["promise/complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(_BaseEntry):
    type: Literal["stream/create"]


class StreamEmitEntry(_BaseEntry):
    type: Literal["stream/emit"]
    stream_id: str
    value: NotRequired[JSONValue]
    state: NotRequired[str]  # opaque


class StreamCompleteEntry(_BaseEntry):
    type: Literal["stream/complete"]
    stream_id: str
    error: NotRequired[ErrorInfo]


class AnyEntry(_BaseEntry):
    type: str


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
)


def is_entry(entry: Entry | AnyEntry) -> TypeGuard[Entry]:
    return entry["type"] in {
        "promise/create",
        "promise/complete",
        "stream/create",
        "stream/emit",
        "stream/complete",
    }
