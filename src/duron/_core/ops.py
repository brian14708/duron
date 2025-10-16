from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast
from typing_extensions import (
    Any,
    Protocol,
    TypeVar,
    dataclass_transform,
    overload,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping
    from contextvars import Context

    from duron._loop import EventLoop, OpFuture
    from duron.typing import TypeHint

    _T = TypeVar("_T")

    @dataclass_transform(frozen_default=True)
    def frozen(_cls: type[_T]) -> type[_T]: ...

else:
    frozen = dataclass(slots=True)

_EMPTY_DICT: Final = cast("dict[str, Any]", {})


@frozen
class OpAnnotations:
    _labels: dict[str, str]
    _name: str | None

    @property
    def labels(self) -> Mapping[str, str]:
        return self._labels

    @property
    def name(self) -> str:
        return name if (name := self._name) is not None else "<unnamed>"

    @staticmethod
    def extend(
        base: OpAnnotations | None,
        *,
        name: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> OpAnnotations:
        if not base:
            return OpAnnotations(
                _labels={**labels} if labels else _EMPTY_DICT,
                _name=name,
            )

        # OpAnnotations is immutable, so it's safe to use the existing dicts
        return OpAnnotations(
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


class StreamObserver(Protocol):
    def on_next(self, log_offset: int, value: object, /) -> None: ...
    def on_close(self, log_offset: int, error: Exception | None, /) -> None: ...


@frozen
class StreamCreate:
    observer: StreamObserver | None
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
def create_op(loop: EventLoop, params: ExternalPromiseCreate) -> OpFuture: ...
@overload
def create_op(
    loop: EventLoop, params: ExternalPromiseComplete
) -> asyncio.Future[None]: ...
def create_op(loop: EventLoop, params: Op) -> asyncio.Future[Any]:
    return loop.create_op(params, external=asyncio.get_running_loop() is not loop)
