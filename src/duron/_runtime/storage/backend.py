from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing_extensions import Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence


def validate_run_id(run_id: object) -> str:
    if not isinstance(run_id, str) or not run_id:
        msg = "run_id must be a nonempty string"
        raise ValueError(msg)
    return run_id


@dataclass(frozen=True, slots=True)
class LeaseToken:
    value: bytes


@dataclass(frozen=True, slots=True)
class HistoryRecord:
    id: str
    payload: bytes
    stream_id: str | None = None


class HistoryStore(Protocol):
    """Opaque append-only run histories with last-acquirer-wins fencing."""

    async def acquire_lease(self, run_id: str) -> LeaseToken: ...

    async def release_lease(self, run_id: str, lease: LeaseToken) -> None: ...

    async def append(
        self, run_id: str, lease: LeaseToken, records: Sequence[HistoryRecord]
    ) -> Sequence[int]: ...

    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, HistoryRecord]]: ...
