from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from duron.codec import JSONValue

_T = TypeVar("_T")


@dataclass(slots=True)
class RunOptions:
    metadata: dict[str, JSONValue] | None = None
