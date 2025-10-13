from __future__ import annotations

import binascii
import os
from abc import abstractmethod
from hashlib import blake2b
from typing import TYPE_CHECKING, Literal, Protocol
from typing_extensions import NotRequired, TypedDict

from duron.codec import JSONValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from typing import TypeGuard


class _BaseEntry(TypedDict):
    id: str
    # Unix timestamp in microseconds
    ts: int
    metadata: NotRequired[dict[str, JSONValue]]


class ErrorInfo(TypedDict):
    code: int
    message: str


class PromiseCreateEntry(_BaseEntry):
    type: Literal["promise.create"]
    labels: NotRequired[dict[str, str]]


class PromiseCompleteEntry(_BaseEntry):
    type: Literal["promise.complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(_BaseEntry):
    type: Literal["stream.create"]
    labels: NotRequired[dict[str, str]]


class StreamEmitEntry(_BaseEntry):
    type: Literal["stream.emit"]
    stream_id: str
    value: JSONValue


class StreamCompleteEntry(_BaseEntry):
    type: Literal["stream.complete"]
    stream_id: str
    error: NotRequired[ErrorInfo]


class BarrierEntry(_BaseEntry):
    type: Literal["barrier"]


class TraceEntry(_BaseEntry):
    type: Literal["trace"]
    events: list[dict[str, JSONValue]]


AnyEntry = _BaseEntry


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


def set_metadata(entry: Entry, *metadata: dict[str, JSONValue] | None) -> None:
    m = entry.get("metadata")
    for md in metadata:
        if md:
            if m is None:
                entry["metadata"] = m = {**md}
            else:
                m.update(md)


def set_labels(entry: Entry, *labels: dict[str, str] | None) -> None:
    if entry["type"] not in {"promise.create", "stream.create"}:
        return

    # Type narrowing for entries that support labels
    if entry["type"] == "promise.create":
        promise_entry: PromiseCreateEntry = entry
        lb = promise_entry.get("labels")
        for label_dict in labels:
            if label_dict:
                if lb is None:
                    promise_entry["labels"] = lb = {**label_dict}
                else:
                    lb.update(label_dict)
    elif entry["type"] == "stream.create":
        stream_entry: StreamCreateEntry = entry
        lb = stream_entry.get("labels")
        for label_dict in labels:
            if label_dict:
                if lb is None:
                    stream_entry["labels"] = lb = {**label_dict}
                else:
                    lb.update(label_dict)


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

    @abstractmethod
    async def flush(self, token: bytes, /) -> None: ...


def random_id() -> str:
    return binascii.b2a_base64(os.urandom(12), newline=False).decode()


def derive_id(base: str, *, context: bytes = b"", key: bytes = b"") -> str:
    return binascii.b2a_base64(
        blake2b(
            binascii.a2b_base64(base), salt=context, key=key, digest_size=12
        ).digest(),
        newline=False,
    ).decode()
