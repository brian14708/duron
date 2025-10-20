from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, NamedTuple
from typing_extensions import Any, Protocol, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping, Sequence
    from contextvars import Context

    from duron._loop import EventLoop, OpFuture
    from duron.typing import TypeHint

    _T = TypeVar("_T")


class OpAnnotations(NamedTuple):
    labels: Mapping[str, str] | None = None
    name: str | None = None

    def get_name(self) -> str:
        return self.name or "<unnamed>"

    def extend(
        self, *, name: str | None = None, labels: Mapping[str, str] | None = None
    ) -> OpAnnotations:
        return OpAnnotations(
            labels=_merge_dict(self.labels, labels),
            name=name if name is not None else self.name,
        )


def _merge_dict(
    base: Mapping[str, _T] | None, extra: Mapping[str, _T] | None
) -> dict[str, _T] | None:
    if base is None:
        return {**extra} if extra else None
    if extra is None:
        return {**base} if base else None
    return {**base, **extra}


class FnCall(NamedTuple):
    callable: Callable[..., Coroutine[Any, Any, object]]
    args: Sequence[object]
    kwargs: Mapping[str, object]
    return_type: TypeHint[Any]
    context: Context
    annotations: OpAnnotations


class StreamObserver(Protocol):
    def on_next(self, log_offset: int, value: object, /) -> None: ...
    def on_close(self, log_offset: int, error: Exception | None, /) -> None: ...


class StreamCreate(NamedTuple):
    observer: StreamObserver | None
    dtype: TypeHint[Any]
    annotations: OpAnnotations


class StreamEmit(NamedTuple):
    stream_id: str
    value: object


class StreamClose(NamedTuple):
    stream_id: str
    exception: Exception | None


class Barrier(NamedTuple): ...


class FutureCreate(NamedTuple):
    return_type: TypeHint[Any]
    annotations: OpAnnotations


class FutureComplete(NamedTuple):
    future_id: str
    value: object
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
def create_op(loop: EventLoop, params: FnCall) -> asyncio.Future[object]: ...
@overload
def create_op(loop: EventLoop, params: StreamCreate) -> asyncio.Future[str]: ...
@overload
def create_op(loop: EventLoop, params: StreamEmit) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: StreamClose) -> asyncio.Future[None]: ...
@overload
def create_op(loop: EventLoop, params: Barrier) -> asyncio.Future[int]: ...
@overload
def create_op(loop: EventLoop, params: FutureCreate) -> OpFuture: ...
@overload
def create_op(loop: EventLoop, params: FutureComplete) -> asyncio.Future[None]: ...
def create_op(loop: EventLoop, params: Op) -> asyncio.Future[Any]:
    return loop.create_op(params, external=asyncio.get_running_loop() is not loop)
