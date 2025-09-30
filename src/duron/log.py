from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal

from typing_extensions import NotRequired, TypedDict

from duron.codec import JSONValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import TypeGuard


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


class BarrierEntry(_BaseEntry):
    type: Literal["barrier"]


class AnyEntry(_BaseEntry):
    type: str


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
    | BarrierEntry
)


def is_entry(entry: Entry | AnyEntry) -> TypeGuard[Entry]:
    return entry["type"] in {
        "promise/create",
        "promise/complete",
        "stream/create",
        "stream/emit",
        "stream/complete",
        "barrier",
    }


class LogStorage(ABC):
    @abstractmethod
    def stream(
        self, start: int | None, live: bool, /
    ) -> AsyncGenerator[tuple[int, AnyEntry], None]: ...

    @abstractmethod
    async def acquire_lease(self) -> bytes: ...

    @abstractmethod
    async def release_lease(self, token: bytes, /): ...

    @abstractmethod
    async def append(self, token: bytes, entry: Entry, /) -> int: ...

    @abstractmethod
    async def flush(self, token: bytes, /): ...
