from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


@dataclass(slots=True)
class FnCall:
    callable: Callable[..., Coroutine[Any, Any, object] | object]


Op = FnCall
