from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar
from typing_extensions import Never, dataclass_transform, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping, Sequence
    from contextvars import Context

    from duron._loop import EventLoop, OpFuture
    from duron.codec import JSONValue
    from duron.typing import TypeHint

    _T = TypeVar("_T")

    @dataclass_transform(frozen_default=True)
    def frozen(_cls: type[_T]) -> type[_T]: ...

else:
    frozen = dataclass(slots=True)

_In_contra = TypeVar("_In_contra", contravariant=True)

_EMPTY_DICT: dict[str, Never] = {}


@frozen
class OpAnnotations:
    metadata: Mapping[str, JSONValue]
    labels: Mapping[str, str]
    name: str = "<unnamed>"

    @staticmethod
    def extend(
        base: OpAnnotations | None,
        *,
        name: str | None = None,
        metadata: Mapping[str, JSONValue] | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> OpAnnotations:
        if not base:
            return OpAnnotations(
                metadata={**metadata} if metadata else _EMPTY_DICT,
                labels={**labels} if labels else _EMPTY_DICT,
                name=name or "<unnamed>",
            )

        return OpAnnotations(
            metadata={**base.metadata, **metadata} if metadata else base.metadata,
            labels={**base.labels, **labels} if labels else base.labels,
            name=name or base.name,
        )


@frozen
class FnCall:
    callable: Callable[..., Coroutine[Any, Any, object] | object]
    args: Sequence[object]
    kwargs: Mapping[str, object]
    return_type: TypeHint[Any]
    context: Context
    annotations: OpAnnotations


class StreamObserver(Protocol, Generic[_In_contra]):
    def on_next(self, log_offset: int, value: _In_contra, /) -> None: ...
    def on_close(self, log_offset: int, error: BaseException | None, /) -> None: ...


@frozen
class StreamCreate:
    observer: StreamObserver[Any] | None
    dtype: TypeHint[Any]
    annotations: OpAnnotations


@frozen
class StreamEmit:
    stream_id: str
    value: object


@frozen
class StreamClose:
    stream_id: str
    exception: BaseException | None


@frozen
class Barrier: ...


@frozen
class ExternalPromiseCreate:
    return_type: TypeHint[Any]
    annotations: OpAnnotations


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
