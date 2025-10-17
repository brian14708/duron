from __future__ import annotations

import binascii
import os
from abc import abstractmethod
from hashlib import blake2b
from typing import TYPE_CHECKING, Literal, Protocol
from typing_extensions import NotRequired, TypedDict

from duron.typing import JSONValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping
    from typing import TypeGuard


class BaseEntry(TypedDict):
    id: str
    # Unix timestamp in microseconds
    ts: int
    labels: NotRequired[dict[str, str]]
    metadata: NotRequired[dict[str, JSONValue]]


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


AnyEntry = BaseEntry


Entry = (
    PromiseCreateEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
    | BarrierEntry
    | TraceEntry
)


def is_entry(entry: Entry | AnyEntry) -> TypeGuard[Entry]:
    return entry.get("type") in {
        "promise.create",
        "promise.complete",
        "stream.create",
        "stream.emit",
        "stream.complete",
        "barrier",
        "trace",
    }


def set_annotations(
    entry: Entry,
    *,
    metadata: Mapping[str, JSONValue] | None = None,
    labels: Mapping[str, str] | None = None,
) -> None:
    if metadata:
        m = entry.get("metadata")
        if m is None:
            entry["metadata"] = {**metadata}
        else:
            m.update(metadata)

    if labels:
        lb = entry.get("labels")
        if lb is None:
            entry["labels"] = {**labels}
        else:
            lb.update(labels)


class LogStorage(Protocol):
    __slots__: tuple[str, ...] = ()

    @abstractmethod
    def stream(
        self,
        start: int | None,
        /,
        *,
        live: bool,
    ) -> AsyncGenerator[tuple[int, AnyEntry], None]: ...

    @abstractmethod
    async def acquire_lease(self) -> bytes: ...

    @abstractmethod
    async def release_lease(self, token: bytes, /) -> None: ...

    @abstractmethod
    async def append(self, token: bytes, entry: Entry, /) -> int: ...


def random_id() -> str:
    return binascii.b2a_base64(os.urandom(12), newline=False).decode()


def derive_id(base: str, *, context: bytes = b"", key: bytes = b"") -> str:
    return binascii.b2a_base64(
        blake2b(
            binascii.a2b_base64(base), salt=context, key=key, digest_size=12
        ).digest(),
        newline=False,
    ).decode()
