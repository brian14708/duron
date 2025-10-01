from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import overload

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Coroutine

    from duron.event_loop import EventLoop
    from duron.stream import Observer


@dataclass(slots=True)
class FnCall:
    callable: Callable[..., Coroutine[Any, Any, object]]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    return_type: type | None = None


@dataclass(slots=True)
class StreamCreate:
    observer: Observer[Any] | None
    dtype: type | None


@dataclass(slots=True)
class StreamEmit:
    stream_id: str
    value: object


@dataclass(slots=True)
class StreamClose:
    stream_id: str
    exception: BaseException | None


@dataclass(slots=True)
class Barrier:
    pass


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
