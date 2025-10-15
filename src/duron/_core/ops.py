from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar
from typing_extensions import dataclass_transform, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping
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

_EMPTY_DICT: dict[str, Any] = {}


@frozen
class OpAnnotations:
    _metadata: dict[str, JSONValue]
    _labels: dict[str, str]
    _name: str | None

    @property
    def metadata(self) -> Mapping[str, JSONValue]:
        return self._metadata

    @property
    def labels(self) -> Mapping[str, str]:
        return self._labels

    @property
    def name(self) -> str:
        if (name := self._name) is not None:
            return name
        return "<unnamed>"

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
                _metadata={**metadata} if metadata else _EMPTY_DICT,
                _labels={**labels} if labels else _EMPTY_DICT,
                _name=name,
            )

        # OpAnnotations is immutable, so it's safe to use the existing dicts
        return OpAnnotations(
            _metadata={**base._metadata, **metadata} if metadata else base._metadata,  # noqa: SLF001
            _labels={**base._labels, **labels} if labels else base._labels,  # noqa: SLF001
            _name=name if name is not None else base._name,  # noqa: SLF001
        )


@frozen
class FnCall:
    callable: Callable[..., Coroutine[Any, Any, object] | object]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    return_type: TypeHint[Any]
    context: Context
    annotations: OpAnnotations


class StreamObserver(Protocol, Generic[_In_contra]):
    def on_next(self, log_offset: int, value: _In_contra, /) -> None: ...
    def on_close(self, log_offset: int, error: Exception | None, /) -> None: ...


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
    exception: Exception | None


@frozen
class Barrier: ...


@frozen
class ExternalPromiseCreate:
    return_type: TypeHint[Any]
    annotations: OpAnnotations


@frozen
class ExternalPromiseComplete:
    promise_id: str
    value: object
    exception: Exception | None


Op = (
    FnCall
    | StreamCreate
    | StreamEmit
    | StreamClose
    | Barrier
    | ExternalPromiseCreate
    | ExternalPromiseComplete
)


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
@overload
def create_op(loop: EventLoop, params: ExternalPromiseComplete) -> OpFuture[None]: ...
def create_op(loop: EventLoop, params: Op) -> OpFuture[Any]:
    return loop.create_op(params, external=asyncio.get_running_loop() is not loop)
