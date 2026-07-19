from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload
from typing_extensions import NamedTuple, Protocol

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Coroutine
    from contextvars import Context

    from duron._runtime.execution.handle_data import HandleReference, StreamEvent
    from duron._runtime.execution.models import EffectOperation
    from duron._runtime.loop import EventLoop, OpFuture
    from duron._runtime.workflow.owned_stream import StreamEOF, StreamReadBuffer
    from duron.types import JSONValue, TypeHint


class OpMetadata(NamedTuple):
    name: str | None = None
    effect_name: str | None = None
    effect_version: str | None = None
    error_types: tuple[type[BaseException], ...] = ()

    def get_name(self) -> str:
        return self.name or "<unnamed>"


class FnCall(NamedTuple):
    callable: Callable[[], Coroutine[Any, Any, object] | object]
    return_type: TypeHint[Any]
    context: Context
    metadata: OpMetadata


class EffectCall(NamedTuple):
    operation: EffectOperation
    return_type: TypeHint[Any]
    context: Context

    @property
    def metadata(self) -> OpMetadata:
        identity = self.operation.identity
        return OpMetadata(
            identity.name, identity.name, identity.version, self.operation.raises
        )


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


class OwnedChannelCreate(NamedTuple):
    reference: HandleReference
    dtype: TypeHint[Any]
    name: str | None


class OwnedStreamNext(NamedTuple):
    reader: StreamReadBuffer[Any]


class OwnedChannelExit(NamedTuple):
    stream_id: str


class OwnedSignalSend(NamedTuple):
    reference: HandleReference
    payload: JSONValue
    key: str
    context: Context


class FutureCreate(NamedTuple):
    return_type: TypeHint[Any]
    metadata: OpMetadata


Op = (
    FnCall
    | EffectCall
    | StreamCreate
    | StreamEmit
    | StreamClose
    | Barrier
    | FutureCreate
    | OwnedChannelCreate
    | OwnedStreamNext
    | OwnedChannelExit
    | OwnedSignalSend
)


@overload
def create_op(loop: EventLoop, params: OwnedChannelCreate) -> asyncio.Future[str]: ...
@overload
def create_op(
    loop: EventLoop, params: OwnedStreamNext
) -> asyncio.Future[StreamEvent[object] | StreamEOF]: ...
@overload
def create_op(loop: EventLoop, params: OwnedChannelExit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: OwnedSignalSend) -> asyncio.Future[object]: ...
@overload
def create_op(loop: EventLoop, params: EffectCall) -> asyncio.Future[object]: ...
@overload
def create_op(loop: EventLoop, params: FnCall) -> asyncio.Future[Any]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> asyncio.Future[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: StreamClose) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: Barrier) -> asyncio.Future[tuple[int, int]]: ...
@overload
def create_op(loop: EventLoop, params: FutureCreate) -> OpFuture: ...
def create_op(loop: EventLoop, params: Op) -> asyncio.Future[Any]:
    return loop.create_op(params)
