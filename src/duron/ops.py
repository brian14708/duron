from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron.stream import Observer


@dataclass(slots=True)
class FnCall:
    callable: Callable[..., Coroutine[Any, Any, object]]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    return_type: type | None = None


@dataclass(slots=True)
class TaskRun:
    task: Coroutine[Any, Any, object]
    return_type: type | None = None


@dataclass(slots=True)
class StreamCreate:
    observer: Observer[object] | None


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


Op = FnCall | StreamCreate | StreamEmit | StreamClose | TaskRun | Barrier
