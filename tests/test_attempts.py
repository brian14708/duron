from __future__ import annotations

from dataclasses import replace

import pytest

from duron._runtime.execution.models import InvocationIdentity, RetryPolicy
from duron._runtime.workflow.attempts import AttemptStart, AttemptState, RetryScheduled
from duron.errors import CorruptLogError


def test_known_retries_and_ambiguous_recovery_have_independent_durable_counts() -> None:
    policy = RetryPolicy(max_attempts=3, max_recoveries=2, initial_delay=1)
    state = AttemptState()
    plan = state.plan(policy)
    assert plan is not None
    assert plan.invocation == InvocationIdentity(1, 1, 0)
    state = state.apply_start(
        AttemptStart(plan.invocation, "first", 1_000_000), policy, offset=1
    )

    # Reconstructing committed state across restarts preserves both budgets.
    recovered = replace(state)
    plan = recovered.plan(policy)
    assert plan is not None
    assert plan.recovering
    assert plan.invocation == InvocationIdentity(1, 2, 1)
    assert plan.not_before_us == 2_000_000
    recovered = recovered.apply_start(
        AttemptStart(plan.invocation, "second", plan.not_before_us), policy, offset=2
    )
    state = recovered.apply_retry(
        RetryScheduled(plan.invocation, 2_500_000, 3_500_000), policy, offset=3
    )
    plan = state.plan(policy)
    assert plan is not None
    assert not plan.recovering
    assert plan.invocation == InvocationIdentity(2, 3, 1)
    assert plan.not_before_us == 3_500_000
    state = state.apply_start(
        AttemptStart(plan.invocation, "second", plan.not_before_us), policy, offset=4
    )
    plan = state.plan(policy)
    assert plan is not None
    assert plan.recovering
    assert plan.invocation == InvocationIdentity(2, 4, 2)
    assert plan.not_before_us == 5_500_000
    state = state.apply_start(
        AttemptStart(plan.invocation, "third", plan.not_before_us), policy, offset=5
    )
    assert state.plan(policy) is None

    # An observed failure can still consume the remaining logical retry budget.
    state = state.apply_retry(
        RetryScheduled(plan.invocation, 6_000_000, 8_000_000), policy, offset=6
    )
    plan = state.plan(policy)
    assert plan is not None
    assert plan.invocation == InvocationIdentity(3, 5, 2)
    state = state.apply_start(
        AttemptStart(plan.invocation, "third", plan.not_before_us), policy, offset=7
    )
    assert state.plan(policy) is None
    assert state.complete(offset=8).completed


def test_default_one_attempt_policy_preserves_unlimited_ambiguous_recovery() -> None:
    policy = RetryPolicy()
    state = AttemptState()
    for invocation in range(1, 20):
        plan = state.plan(policy)
        assert plan is not None
        assert plan.invocation == InvocationIdentity(1, invocation, invocation - 1)
        state = state.apply_start(
            AttemptStart(plan.invocation, str(invocation), invocation),
            policy,
            offset=invocation,
        )


def test_zero_recoveries_forbids_only_reinvocation_of_ambiguous_start() -> None:
    policy = RetryPolicy(max_attempts=2, max_recoveries=0)
    state = AttemptState()
    plan = state.plan(policy)
    assert plan is not None
    state = state.apply_start(
        AttemptStart(plan.invocation, "session", 1), policy, offset=1
    )
    assert state.plan(policy) is None
    state = state.apply_retry(RetryScheduled(plan.invocation, 2, 2), policy, offset=2)
    plan = state.plan(policy)
    assert plan is not None
    assert plan.invocation == InvocationIdentity(2, 2, 0)
    state = state.apply_start(
        AttemptStart(plan.invocation, "session", 2), policy, offset=3
    )
    assert state.plan(policy) is None


def test_cancellation_during_backoff_is_final_and_never_starts_a_retry() -> None:
    policy = RetryPolicy(max_attempts=2)
    first = InvocationIdentity(1, 1, 0)
    state = AttemptState().apply_start(
        AttemptStart(first, "session", 1), policy, offset=1
    )
    state = state.apply_retry(RetryScheduled(first, 2, 10), policy, offset=2)
    state = state.complete(cancelled=True, offset=3)
    assert state.plan(policy) is None
    with pytest.raises(CorruptLogError, match="after completion"):
        state.apply_start(
            AttemptStart(InvocationIdentity(2, 2, 0), "session", 10), policy, offset=4
        )


def test_duplicate_out_of_order_and_early_starts_are_corrupt() -> None:
    policy = RetryPolicy(max_attempts=3, initial_delay=1)
    first = AttemptStart(InvocationIdentity(1, 1, 0), "first", 1)
    state = AttemptState().apply_start(first, policy, offset=1)
    for invalid in (
        first,
        AttemptStart(InvocationIdentity(2, 2, 0), "first", 2),
        AttemptStart(InvocationIdentity(1, 3, 2), "other", 2_000_000),
        AttemptStart(InvocationIdentity(1, 2, 1), "first", 2_000_000),
        AttemptStart(InvocationIdentity(1, 2, 1), "other", 2),
    ):
        with pytest.raises(CorruptLogError):
            state.apply_start(invalid, policy, offset=9)


def test_retry_requires_matching_started_invocation_and_remaining_budget() -> None:
    policy = RetryPolicy(max_attempts=2)
    first = InvocationIdentity(1, 1, 0)
    retry = RetryScheduled(first, 2, 10)
    with pytest.raises(CorruptLogError, match="unfinished invocation"):
        AttemptState().apply_retry(retry, policy, offset=1)
    state = AttemptState().apply_start(
        AttemptStart(first, "first", 1), policy, offset=1
    )
    for invalid in (
        RetryScheduled(InvocationIdentity(1, 2, 1), 2, 10),
        RetryScheduled(first, 0, 10),
        RetryScheduled(first, 2, 1),
    ):
        with pytest.raises(CorruptLogError):
            state.apply_retry(invalid, policy, offset=2)
    scheduled = state.apply_retry(retry, policy, offset=2)
    with pytest.raises(CorruptLogError, match="unfinished invocation"):
        scheduled.apply_retry(retry, policy, offset=3)
    with pytest.raises(CorruptLogError, match="no matching attempt"):
        scheduled.complete(offset=3)
    with pytest.raises(CorruptLogError, match="budget"):
        state.apply_retry(retry, RetryPolicy(), offset=2)
