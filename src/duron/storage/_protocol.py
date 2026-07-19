from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from typing_extensions import Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from duron.errors import FailureKind


TerminalState = Literal["succeeded", "failed", "cancelled"]


def validate_id(value: object, name: str) -> str:
    """Require a nonempty string identifier.

    Returns:
        The validated value.

    Raises:
        ValueError: If the value is not a nonempty string.

    """
    if not isinstance(value, str) or not value:
        msg = f"{name} must be a nonempty string"
        raise ValueError(msg)
    try:
        _ = value.encode("utf-8")
    except UnicodeEncodeError as error:
        msg = f"{name} must be valid UTF-8 text"
        raise ValueError(msg) from error
    return value


@dataclass(frozen=True, slots=True)
class ClaimToken:
    value: bytes


@dataclass(frozen=True, slots=True)
class Event:
    """Opaque durable event whose ID fixes its payload and routing metadata."""

    id: str
    payload: bytes
    stream_id: str | None = None


class EventReader(Protocol):
    async def read_event(self, run_id: str, event_id: str) -> tuple[int, Event] | None:
        """Look up one immutable event by ID, without acquiring a claim."""
        ...

    async def read(
        self,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, Event]]: ...


class EventStore(EventReader, Protocol):
    """Append-only event store with exclusive write claims."""

    async def claim(self, run_id: str) -> ClaimToken:
        """Acquire ownership until release or store closure."""
        ...

    async def create(self, run_id: str, header: Event) -> bool:
        """Atomically create an absent run with its immutable first event."""
        ...

    async def release(self, run_id: str, token: ClaimToken) -> None: ...

    async def append(
        self, run_id: str, token: ClaimToken, records: Sequence[Event]
    ) -> Sequence[int]:
        """Atomically insert new IDs or return offsets if all IDs already exist.

        Event IDs fix immutable content. Mixed existing and new IDs are rejected.
        All calls, including retries, require a valid token.
        """
        ...

    async def finish(
        self,
        run_id: str,
        token: ClaimToken,
        records: Sequence[Event],
        state: TerminalState,
    ) -> Sequence[int]:
        """Append final events and mark an associated job terminal atomically.

        Uses the same claim and immutable retry semantics as append. The caller
        determines the terminal state; event payloads remain opaque to storage.
        """
        ...


@dataclass(frozen=True, slots=True)
class InboxMessage:
    id: str
    kind: str
    port: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class ScheduledRun:
    id: str
    contract: bytes
    inputs: bytes
    state: str
    cancelled: bool
    error: str | None
    error_kind: FailureKind | None
    seed: str


class JobStore(Protocol):
    """Scheduler job and inbox operations over one namespace.

    ``namespace`` and ``run_id`` identify a job and its event history.
    Every method is atomic. Terminal jobs (``succeeded``, ``failed``,
    ``cancelled``) never change state again.
    """

    async def job_get(self, namespace: str, run_id: str) -> ScheduledRun: ...
    async def job_submit(
        self, namespace: str, run_id: str, contract: bytes, inputs: bytes, seed: str
    ) -> str:
        """Insert a queued job and return the stored seed.

        An existing job with a different contract or inputs raises
        ``SubmissionConflictError``.
        """
        ...

    async def job_candidates(
        self, namespace: str, after: str
    ) -> Sequence[ScheduledRun]: ...
    async def job_delay(
        self, namespace: str, active: Sequence[str]
    ) -> float | None: ...

    async def job_state(
        self,
        namespace: str,
        run_id: str,
        state: str,
        error: str | None = None,
        kind: FailureKind | None = None,
    ) -> None:
        """Update a nonterminal job."""
        ...

    async def job_attempt_failed(
        self,
        namespace: str,
        run_id: str,
        error: str,
        kind: FailureKind,
        *,
        retryable: bool,
        max_attempts: int,
    ) -> None:
        """Record one failed attempt."""
        ...

    async def job_cancel(self, namespace: str, run_id: str) -> None: ...
    async def job_suspend(
        self, namespace: str, run_id: str, deadline: float | None
    ) -> bool: ...
    async def job_wake(self, namespace: str) -> None: ...
    async def inbox_post(
        self,
        namespace: str,
        inbox_id: str,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
    ) -> None: ...
    async def inbox_pending(
        self, namespace: str, run_id: str
    ) -> Sequence[InboxMessage]: ...
    async def inbox_delete(
        self, namespace: str, run_id: str, inbox_id: str
    ) -> None: ...
