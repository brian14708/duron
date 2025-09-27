from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Literal

from typing_extensions import NotRequired, TypedDict, TypeVar

from duron.codec import JSONValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import TypeGuard


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
    value: JSONValue


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


class LogStorage(ABC, Generic[_TOffset, _TLease]):
    @abstractmethod
    def stream(
        self, start: _TOffset | None, live: bool, /
    ) -> AsyncGenerator[tuple[_TOffset, AnyEntry], None]: ...

    @abstractmethod
    async def acquire_lease(self) -> _TLease: ...

    @abstractmethod
    async def release_lease(self, token: _TLease, /): ...

    @abstractmethod
    async def append(self, token: _TLease, entry: Entry, /): ...

    @abstractmethod
    async def flush(self, token: _TLease, /): ...
