from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, cast

from typing_extensions import TypedDict, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import Literal, TypeGuard

    from typing_extensions import NotRequired


_TOffset = TypeVar("_TOffset")
_TLease = TypeVar("_TLease")


class _BaseEntry(TypedDict):
    id: str
    # Unix timestamp in microseconds
    ts: int
    meta: NotRequired[dict[str, str]]


class ErrorInfo(TypedDict):
    code: int
    message: str


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
    state: NotRequired[JSONValue]


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


JSONValue = dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None


def is_json_value(x: object) -> TypeGuard[JSONValue]:
    if x is None or isinstance(x, (bool, int, float, str)):
        return True
    if isinstance(x, list):
        return all(is_json_value(item) for item in cast("list[object]", x))
    if isinstance(x, dict):
        return all(
            isinstance(k, str) and is_json_value(v)
            for k, v in cast("dict[object, object]", x).items()
        )
    return False


class LogStorage(Protocol, Generic[_TOffset, _TLease]):
    def stream(
        self, start: _TOffset | None, live: bool
    ) -> AsyncGenerator[tuple[_TOffset, AnyEntry], None]: ...

    async def acquire_lease(self) -> _TLease: ...

    async def release_lease(self, token: _TLease): ...

    async def append(self, token: _TLease, entry: Entry): ...

    async def flush(self, token: _TLease): ...
