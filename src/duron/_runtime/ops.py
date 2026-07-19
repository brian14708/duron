from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, overload
from typing_extensions import NamedTuple

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Coroutine
    from contextvars import Context

    from duron._execution.models import EffectOperation, HandleReference, StreamEvent
    from duron._hints import JSONValue, TypeHint
    from duron._runtime.channels import StreamEOF, StreamReadBuffer
    from duron._runtime.loop import EventLoop


class OpMetadata(NamedTuple):
    name: str | None = None
    effect_name: str | None = None
    effect_version: str | None = None
    error_types: tuple[type[BaseException], ...] = ()


class FnCall(NamedTuple):
    """A runtime operation whose result uses Duron's fixed JSON encoding."""

    callable: Callable[[], Coroutine[Any, Any, object] | object]
    return_type: TypeHint[Any]
    context: Context
    metadata: OpMetadata


class WaitPending(Enum):
    PENDING = "pending"


class WaitCall(NamedTuple):
    """A replayable poll that may release its host until delivery or deadline."""

    callable: Callable[[], Coroutine[Any, Any, object]]
    return_type: TypeHint[Any]
    context: Context
    metadata: OpMetadata
    deadline: float | None = None
    suspendable: bool = True


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


class StreamCreate(NamedTuple):
    name: str | None
    dtype: TypeHint[Any]
    metadata: OpMetadata


class StreamEmit(NamedTuple):
    stream_id: str
    value: object


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


Op = (
    FnCall
    | WaitCall
    | EffectCall
    | StreamCreate
    | StreamEmit
    | Barrier
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
def create_op(loop: EventLoop, params: WaitCall) -> asyncio.Future[Any]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> asyncio.Future[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: Barrier) -> asyncio.Future[tuple[int, int]]: ...
def create_op(loop: EventLoop, params: Op) -> asyncio.Future[Any]:
    return loop.create_op(params)
