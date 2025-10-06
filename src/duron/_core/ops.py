from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from typing_extensions import overload

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._loop import EventLoop, OpFuture
    from duron.codec import JSONValue


_In = TypeVar("_In", contravariant=True)


@dataclass(slots=True)
class FnCall:
    callable: Callable[..., Coroutine[Any, Any, object] | object]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    return_type: type | None = None
    metadata: dict[str, JSONValue] | None = None


class StreamObserver(Generic[_In], Protocol):
    def on_next(self, log_offset: int, value: _In, /) -> None: ...
    def on_close(self, log_offset: int, error: BaseException | None, /) -> None: ...


@dataclass(slots=True)
class StreamCreate:
    observer: StreamObserver[Any] | None
    dtype: type | None
    metadata: dict[str, JSONValue] | None = None


@dataclass(slots=True)
class StreamEmit:
    stream_id: str
    value: object


@dataclass(slots=True)
class StreamClose:
    stream_id: str
    exception: BaseException | None


@dataclass(slots=True)
class Barrier: ...


@dataclass(slots=True)
class ExternalPromiseCreate:
    return_type: type | None = None
    metadata: dict[str, JSONValue] | None = None


Op = FnCall | StreamCreate | StreamEmit | StreamClose | Barrier | ExternalPromiseCreate


@overload
def create_op(loop: EventLoop, params: FnCall) -> OpFuture[object]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> OpFuture[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> OpFuture[None]: ...
@overload
def create_op(loop: EventLoop, params: StreamClose) -> OpFuture[None]: ...
@overload
def create_op(loop: EventLoop, params: Barrier) -> OpFuture[int]: ...
@overload
def create_op(loop: EventLoop, params: ExternalPromiseCreate) -> OpFuture[object]: ...
def create_op(loop: EventLoop, params: Op) -> OpFuture[Any]:
    return loop.create_op(params, external=asyncio.get_running_loop() is not loop)
