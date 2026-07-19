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
class Event:
    """Opaque durable event stored in one run's append-only log."""

    id: str
    payload: bytes
    stream_id: str | None = None


class EventReader(Protocol):
    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, Event]]: ...


class EventStore(EventReader, Protocol):
    """Append-only event store with expiring, exclusive write claims."""

    async def claim(
        self, run_id: str, request_id: str, *, ttl: float = 30.0
    ) -> LeaseToken:
        """Acquire or retry a claim without stealing or reviving old ownership."""
        ...

    async def renew(
        self, run_id: str, lease: LeaseToken, *, ttl: float = 30.0
    ) -> None: ...

    async def create(self, run_id: str, header: Event) -> bool:
        """Atomically create an absent run with its immutable first event."""
        ...

    async def release(self, run_id: str, lease: LeaseToken) -> None: ...

    async def append(
        self, run_id: str, lease: LeaseToken, records: Sequence[Event], *, batch_id: str
    ) -> Sequence[int]: ...

    async def wait_for_change(self) -> None: ...
