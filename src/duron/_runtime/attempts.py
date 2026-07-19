"""Validated crash-recovery transitions for effect invocations."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import cast

from duron._execution.models import InvocationIdentity
from duron.errors import CorruptLogError


@dataclass(frozen=True, slots=True)
class AttemptStart:
    invocation: InvocationIdentity
    session_id: str
    started_at_us: int


@dataclass(frozen=True, slots=True)
class AttemptPlan:
    invocation: InvocationIdentity
    recovering: bool


@dataclass(frozen=True, slots=True)
class AttemptState:
    """Committed state only; apply a transition after its append commits."""

    start: AttemptStart | None = None
    completed: bool = False

    def plan(self) -> AttemptPlan | None:
        if self.completed:
            return None
        if self.start is None:
            return AttemptPlan(InvocationIdentity(1, 0), recovering=False)
        previous = self.start.invocation
        return AttemptPlan(
            InvocationIdentity(
                previous.invocation_number + 1, previous.recovery_count + 1
            ),
            recovering=True,
        )

    def apply_start(self, start: AttemptStart, *, offset: int) -> AttemptState:
        if self.completed:
            raise CorruptLogError(offset, "effect attempt starts after completion")
        session_id = cast("object", start.session_id)
        if (
            type(start.started_at_us) is not int
            or start.started_at_us < 0
            or not isinstance(session_id, str)
            or not session_id
        ):
            raise CorruptLogError(
                offset, "invalid attempt timestamp or session identity"
            )
        plan = self.plan()
        if plan is None or start.invocation != plan.invocation:
            raise CorruptLogError(offset, "effect attempt counters are out of order")
        if self.start is not None:
            if start.session_id == self.start.session_id:
                raise CorruptLogError(
                    offset, "ambiguous invocation recovery requires a new session"
                )
            if start.started_at_us < self.start.started_at_us:
                raise CorruptLogError(
                    offset, "effect invocation timestamp moved backwards"
                )
        return AttemptState(start=start)

    def complete(self, *, offset: int) -> AttemptState:
        if self.completed:
            raise CorruptLogError(offset, "effect has duplicate final outcomes")
        return replace(self, completed=True)
