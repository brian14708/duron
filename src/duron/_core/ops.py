from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import Any, NamedTuple, Protocol, overload

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Coroutine
    from contextvars import Context

    from duron.loop import EventLoop, OpFuture
    from duron.typing import TypeHint


class OpMetadata(NamedTuple):
    name: str | None = None

    def get_name(self) -> str:
        return self.name or "<unnamed>"


class FnCall(NamedTuple):
    callable: Callable[[], Coroutine[Any, Any, object]]
    return_type: TypeHint[Any]
    context: Context
    metadata: OpMetadata


class StreamObserver(Protocol):
    def on_next(self, log_offset: int, value: object, /) -> None: ...
    def on_close(self, log_offset: int, error: Exception | None, /) -> None: ...


class StreamCreate(NamedTuple):
    observer: StreamObserver | None
    name: str | None
    dtype: TypeHint[Any]
    metadata: OpMetadata


class StreamEmit(NamedTuple):
    stream_id: str
    value: object


class StreamClose(NamedTuple):
    stream_id: str
    exception: Exception | None


class Barrier(NamedTuple): ...


class FutureCreate(NamedTuple):
    return_type: TypeHint[Any]
    metadata: OpMetadata


class FutureComplete(NamedTuple):
    future_id: str
    value: object
    dtype: TypeHint[Any]
    exception: Exception | None


Op = (
    FnCall
    | StreamCreate
    | StreamEmit
    | StreamClose
    | Barrier
    | FutureCreate
    | FutureComplete
)


@overload
def create_op(loop: EventLoop, params: FnCall) -> asyncio.Future[Any]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> asyncio.Future[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: StreamClose) -> asyncio.Future[None]: ...
@overload
def create_op(
    loop: EventLoop, params: Barrier
) -> asyncio.Future[
    tuple[int, int]  # log_offset, timestamp_us
]: ...
@overload
def create_op(loop: EventLoop, params: FutureCreate) -> OpFuture: ...
@overload
def create_op(loop: EventLoop, params: FutureComplete) -> asyncio.Future[None]: ...
def create_op(loop: EventLoop, params: Op) -> asyncio.Future[Any]:
    return loop.create_op(params)
