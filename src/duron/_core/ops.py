from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from typing_extensions import overload

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Coroutine

    from duron._loop import EventLoop
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


Op = FnCall | StreamCreate | StreamEmit | StreamClose | Barrier


@overload
def create_op(loop: EventLoop, params: FnCall) -> asyncio.Future[object]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> asyncio.Future[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: StreamClose) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: Barrier) -> asyncio.Future[int]: ...
def create_op(loop: EventLoop, params: Op) -> asyncio.Future[Any]:
    return loop.create_op(params)
