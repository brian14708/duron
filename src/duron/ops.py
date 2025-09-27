from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine


@dataclass(slots=True)
class FnCall:
    callable: Callable[..., Awaitable[object] | object]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    return_type: type | None = None


@dataclass(slots=True)
class TaskRun:
    task: Coroutine[Any, Any, object]
    return_type: type | None = None


Op = FnCall | TaskRun
