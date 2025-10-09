from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duron.codec import JSONValue


@dataclass(slots=True)
class RunOptions:
    metadata: dict[str, JSONValue] | None = None
