"""Validated retry transitions, independent of workflow operation allocation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import cast

from duron._runtime.execution.models import InvocationIdentity, RetryPolicy
from duron.errors import CorruptLogError


def delay_to_us(delay: float) -> int:
    """Convert seconds to integer microseconds without float overflow.

    Keeps huge but finite accepted delays representable as integer UTC
    microseconds by splitting the integer and fractional parts before
    scaling.

    Returns:
        The delay in microseconds.

    """
    return int(delay) * 1_000_000 + int((delay - int(delay)) * 1_000_000)


@dataclass(frozen=True, slots=True)
class AttemptStart:
    invocation: InvocationIdentity
    session_id: str
    started_at_us: int


@dataclass(frozen=True, slots=True)
class RetryScheduled:
    invocation: InvocationIdentity
    recorded_at_us: int
    not_before_us: int


@dataclass(frozen=True, slots=True)
class AttemptPlan:
    invocation: InvocationIdentity
    not_before_us: int
    recovering: bool


@dataclass(frozen=True, slots=True)
class AttemptState:
    """Committed state only; callers apply a transition after its append commits."""

    start: AttemptStart | None = None
    retry: RetryScheduled | None = None
    completed: bool = False

    def plan(self, policy: RetryPolicy) -> AttemptPlan | None:
        """Plan the next invocation without making it visible or starting work.

        Returns:
            A plan, or None when ambiguous recovery has exhausted its budget.

        """
        if self.completed:
            return None
        if self.start is None:
            return AttemptPlan(InvocationIdentity(1, 1, 0), 0, recovering=False)
        previous = self.start.invocation
        if self.retry is not None:
            return AttemptPlan(
                InvocationIdentity(
                    previous.logical_attempt + 1,
                    previous.invocation_number + 1,
                    previous.recovery_count,
                ),
                self.retry.not_before_us,
                recovering=False,
            )
        if (
            policy.max_recoveries is not None
            and previous.recovery_count >= policy.max_recoveries
        ):
            return None
        delay = policy.delay(previous.logical_attempt)
        return AttemptPlan(
            InvocationIdentity(
                previous.logical_attempt,
                previous.invocation_number + 1,
                previous.recovery_count + 1,
            ),
            self.start.started_at_us + delay_to_us(delay),
            recovering=True,
        )

    def apply_start(
        self, start: AttemptStart, policy: RetryPolicy, *, offset: int
    ) -> AttemptState:
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
        plan = self.plan(policy)
        if plan is None or start.invocation != plan.invocation:
            raise CorruptLogError(
                offset, "effect attempt counters are out of order or exceed policy"
            )
        if start.started_at_us < plan.not_before_us:
            raise CorruptLogError(
                offset, "effect attempt starts before its durable deadline"
            )
        if (
            self.start is not None
            and plan.recovering
            and start.session_id == self.start.session_id
        ):
            raise CorruptLogError(
                offset, "ambiguous invocation recovery requires a new session"
            )
        if start.invocation.logical_attempt > policy.max_attempts:
            raise CorruptLogError(
                offset, "effect logical attempt exceeds its retry budget"
            )
        return AttemptState(start=start)

    def apply_retry(
        self, retry: RetryScheduled, policy: RetryPolicy, *, offset: int
    ) -> AttemptState:
        if self.completed or self.start is None or self.retry is not None:
            raise CorruptLogError(
                offset, "retry scheduling requires one unfinished invocation"
            )
        if retry.invocation != self.start.invocation:
            raise CorruptLogError(
                offset, "retry scheduling refers to a different invocation"
            )
        if retry.invocation.logical_attempt >= policy.max_attempts:
            raise CorruptLogError(
                offset, "retry scheduling exceeds the logical attempt budget"
            )
        if (
            type(retry.recorded_at_us) is not int
            or type(retry.not_before_us) is not int
            or retry.recorded_at_us < self.start.started_at_us
            or retry.not_before_us < retry.recorded_at_us
        ):
            raise CorruptLogError(
                offset, "retry scheduling has an invalid absolute deadline"
            )
        return replace(self, retry=retry)

    def complete(self, *, cancelled: bool = False, offset: int) -> AttemptState:
        if self.completed:
            raise CorruptLogError(offset, "effect has duplicate final outcomes")
        if self.retry is not None and not cancelled:
            raise CorruptLogError(
                offset, "effect outcome has no matching attempt after retry scheduling"
            )
        return replace(self, completed=True)
