from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, overload
from typing_extensions import TypeVar

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable, Coroutine
    from contextvars import Context

    from duron._execution.models import EffectOperation, HandleReference, StreamEvent
    from duron._hints import JSONValue, TypeHint
    from duron._ref import Ref
    from duron._runtime.channels import StreamEOF, StreamReadBuffer
    from duron._runtime.loop import EventLoop


_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)


@dataclass(frozen=True, slots=True)
class FnCall(Generic[_T_co]):
    """A runtime operation whose result uses Duron's fixed JSON encoding."""

    callable: Callable[[], Coroutine[Any, Any, _T_co] | _T_co]
    return_type: TypeHint[_T_co]
    context: Context
    ref: Ref | None = None


class WaitPending(Enum):
    PENDING = "pending"


@dataclass(frozen=True, slots=True)
class WaitCall(Generic[_T_co]):
    """A replayable poll that may release its host until delivery or deadline."""

    callable: Callable[[], Coroutine[Any, Any, _T_co | WaitPending]]
    return_type: TypeHint[_T_co]
    context: Context
    deadline: float | None = None
    suspendable: bool = True


@dataclass(frozen=True, slots=True)
class EffectCall(Generic[_T_co]):
    operation: EffectOperation
    return_type: TypeHint[_T_co]
    context: Context


@dataclass(frozen=True, slots=True)
class StreamCreate:
    name: str | None
    dtype: TypeHint[object]


@dataclass(frozen=True, slots=True)
class StreamEmit:
    stream_id: str
    value: object


@dataclass(frozen=True, slots=True)
class Barrier:
    pass


@dataclass(frozen=True, slots=True)
class OwnedChannelCreate:
    reference: HandleReference
    dtype: TypeHint[object]
    name: str | None


@dataclass(frozen=True, slots=True)
class OwnedStreamNext(Generic[_T]):
    reader: StreamReadBuffer[_T]


@dataclass(frozen=True, slots=True)
class OwnedChannelExit:
    stream_id: str


@dataclass(frozen=True, slots=True)
class OwnedSignalSend:
    reference: HandleReference
    payload: JSONValue
    key: str
    context: Context


Op = (
    FnCall[object]
    | WaitCall[object]
    | EffectCall[object]
    | StreamCreate
    | StreamEmit
    | Barrier
    | OwnedChannelCreate
    | OwnedStreamNext[Any]
    | OwnedChannelExit
    | OwnedSignalSend
)


@overload
def create_op(loop: EventLoop, params: OwnedChannelCreate) -> asyncio.Future[str]: ...
@overload
def create_op(
    loop: EventLoop, params: OwnedStreamNext[_T]
) -> asyncio.Future[StreamEvent[_T] | StreamEOF]: ...
@overload
def create_op(loop: EventLoop, params: OwnedChannelExit) -> asyncio.Future[None]: ...
@overload
def create_op(
    loop: EventLoop, params: OwnedSignalSend
) -> asyncio.Future[tuple[int, bool]]: ...
@overload
def create_op(loop: EventLoop, params: EffectCall[_T]) -> asyncio.Future[_T]: ...
@overload
def create_op(loop: EventLoop, params: FnCall[_T]) -> asyncio.Future[_T]: ...
@overload
def create_op(loop: EventLoop, params: WaitCall[_T]) -> asyncio.Future[_T]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> asyncio.Future[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: Barrier) -> asyncio.Future[tuple[int, int]]: ...
def create_op(loop: EventLoop, params: Op) -> Awaitable[object]:
    return loop.create_op(params)
