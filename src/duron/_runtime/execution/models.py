"""Immutable effect identity, invocation context, and execution policies."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, cast

from duron._runtime.execution.handle_data import RecoveryDescriptor
from duron.errors import DuronError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from duron._runtime.execution.handle_data import HandleReference
    from duron._runtime.execution.handles import HandleResolver
    from duron._runtime.storage.entry import (
        ExecutionIdentityData,
        InvocationIdentityData,
    )
    from duron.codec import Codec
    from duron.types import JSONValue


class EffectBodyError(Exception):
    """Carry StopIteration across coroutine boundaries without PEP 479 wrapping."""

    def __init__(self, error: StopIteration) -> None:
        self.error = error
        super().__init__(str(error))


def canonical_json(value: JSONValue) -> bytes:
    """Encode the version-1 canonical JSON representation.

    Returns:
        Deterministic UTF-8 bytes with finite JSON values.

    """
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest(domain: str, value: JSONValue) -> str:
    return hashlib.sha256(
        domain.encode("ascii") + b"\0" + canonical_json(value)
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class EffectId:
    """Opaque stable identifier returned by an executor registration."""

    value: str


@dataclass(frozen=True, slots=True)
class EffectIdentity:
    """Versioned public identity of an effect definition."""

    name: str
    version: str
    format_version: Literal[1] = 1
    _effect_id: EffectId = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        name = cast("object", self.name)
        version = cast("object", self.version)
        if (
            not isinstance(name, str)
            or not isinstance(version, str)
            or not name
            or not version
        ):
            msg = "effect name and version must be nonempty strings"
            raise WorkflowDefinitionError(msg)
        object.__setattr__(
            self,
            "_effect_id",
            EffectId(digest("duron.effect.v1", [self.name, self.version])),
        )

    @property
    def effect_id(self) -> EffectId:
        return self._effect_id


@dataclass(frozen=True, slots=True)
class ExecutionIdentity:
    """One durable effect call, stable across retries and crash recovery."""

    run_seed: str
    operation_id: str
    effect_id: EffectId
    input_digest: str
    format_version: Literal[1] = 1
    _idempotency_key: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_idempotency_key",
            digest(
                "duron.external-action.v1",
                [
                    self.run_seed,
                    self.operation_id,
                    self.effect_id.value,
                    self.input_digest,
                ],
            ),
        )

    @property
    def idempotency_key(self) -> str:
        return self._idempotency_key

    def fields(self) -> ExecutionIdentityData:
        return {
            "format_version": self.format_version,
            "run_seed": self.run_seed,
            "operation_id": self.operation_id,
            "effect_id": self.effect_id.value,
            "input_digest": self.input_digest,
        }


@dataclass(frozen=True, slots=True)
class InvocationIdentity:
    """Durable counters for a physical invocation of a logical attempt."""

    logical_attempt: int
    invocation_number: int
    recovery_count: int
    format_version: Literal[1] = 1

    def __post_init__(self) -> None:
        _integer(self.logical_attempt, "logical_attempt", minimum=1)
        _integer(self.invocation_number, "invocation_number", minimum=1)
        _integer(self.recovery_count, "recovery_count", minimum=0)
        if self.invocation_number != self.logical_attempt + self.recovery_count:
            msg = "invocation counters are inconsistent"
            raise WorkflowDefinitionError(msg)

    def fields(self) -> InvocationIdentityData:
        return {
            "format_version": self.format_version,
            "logical_attempt": self.logical_attempt,
            "invocation_number": self.invocation_number,
            "recovery_count": self.recovery_count,
        }


@dataclass(frozen=True, slots=True)
class EffectContextImpl:
    """Identity and recovery information supplied to an effect body.

    Effects return or raise; completion authority is owned by the runtime.
    """

    identity: ExecutionIdentity
    effect: EffectIdentity
    invocation: InvocationIdentity
    recovering: bool = False
    recovery: RecoveryDescriptor = field(default_factory=RecoveryDescriptor)
    handles: HandleResolver | None = field(default=None, repr=False, compare=False)

    @property
    def idempotency_key(self) -> str:
        return self.identity.idempotency_key

    @property
    def logical_attempt(self) -> int:
        return self.invocation.logical_attempt

    @property
    def invocation_number(self) -> int:
        return self.invocation.invocation_number

    @property
    def recovery_count(self) -> int:
        return self.invocation.recovery_count


def _integer(value: int, name: str, *, minimum: int) -> None:
    if type(value) is not int or value < minimum:
        msg = f"{name} must be an integer >= {minimum}"
        raise WorkflowDefinitionError(msg)


def _finite(
    value: object, name: str, *, minimum: float, inclusive: bool = True
) -> None:
    try:
        finite = isinstance(value, (int, float)) and math.isfinite(value)
    except OverflowError:
        finite = False
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not finite
        or (value < minimum if inclusive else value <= minimum)
    ):
        relation = ">=" if inclusive else ">"
        msg = f"{name} must be finite and {relation} {minimum}"
        raise WorkflowDefinitionError(msg)


def validate_policy_error_types(types: tuple[type[BaseException], ...]) -> None:
    _ = validate_error_types(types, "retry policy")
    for error_type in cast("tuple[object, ...]", types):
        if (
            not isinstance(error_type, type)
            or not issubclass(error_type, Exception)
            or issubclass(asyncio.CancelledError, error_type)
            or error_type is StopIteration
        ):
            msg = "retry errors must round-trip durably and exclude cancellation"
            raise WorkflowDefinitionError(msg)


def validate_error_types(
    types: tuple[type[BaseException], ...], owner: str
) -> tuple[type[BaseException], ...]:
    for t in cast("tuple[object, ...]", types):
        if not (isinstance(t, type) and issubclass(t, Exception)):
            msg = f"{owner} declares an unsupported exception type: {t!r}"
            raise WorkflowDefinitionError(msg)
        if (
            issubclass(t, DuronError)
            and t.__dict__.get("__duron_round_trip__") is not True
        ):
            msg = f"{owner} declares a runtime error without round-trip support: {t!r}"
            raise WorkflowDefinitionError(msg)
    return tuple(types)


@dataclass(frozen=True, slots=True, kw_only=True)
class RetryPolicy:
    """Logical failure budget and independent crash-recovery budget.

    `max_attempts` includes the first attempt. Only exact types in `retry_on`
    are retried after reported failure; the default retries none. Delays grow
    from `initial_delay` by `multiplier`, capped at `max_delay` seconds.

    `max_recoveries=None` allows unlimited reinvocations after an ambiguous
    outcome. Zero forbids recovery. Exhaustion raises `EffectOutcomeUnknownError`.
    Retries and recoveries retain the same effect idempotency key.
    """

    max_attempts: int = 1
    initial_delay: float = 0
    multiplier: float = 2
    max_delay: float = 60
    max_recoveries: int | None = None
    retry_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        _integer(self.max_attempts, "max_attempts", minimum=1)
        if self.max_recoveries is not None:
            _integer(self.max_recoveries, "max_recoveries", minimum=0)
        _finite(self.initial_delay, "initial_delay", minimum=0)
        _finite(self.multiplier, "multiplier", minimum=1)
        _finite(self.max_delay, "max_delay", minimum=0)
        object.__setattr__(self, "retry_on", tuple(self.retry_on))
        validate_policy_error_types(self.retry_on)

    def delay(self, failed_attempt: int) -> float:
        """Compute bounded exponential backoff without exponent overflow.

        Returns:
            Delay in seconds, saturated at max_delay.

        """
        _integer(failed_attempt, "failed_attempt", minimum=1)
        if self.initial_delay == 0 or self.max_delay == 0:
            return 0
        if self.multiplier == 1 or failed_attempt == 1:
            return min(self.initial_delay, self.max_delay)
        if self.initial_delay >= self.max_delay:
            return self.max_delay
        exponent = failed_attempt - 1
        threshold = (
            math.log(self.max_delay) - math.log(self.initial_delay)
        ) / math.log(self.multiplier)
        if exponent >= threshold:
            return self.max_delay
        try:
            return min(self.max_delay, self.initial_delay * self.multiplier**exponent)
        except OverflowError:
            return min(
                self.max_delay,
                math.exp(
                    math.log(self.initial_delay) + exponent * math.log(self.multiplier)
                ),
            )

    def should_retry(self, error: Exception) -> bool:
        return not forbidden_error(error) and type(error) in self.retry_on


def forbidden_error(error: BaseException) -> bool:
    """Reject runtime failures anywhere in a possibly cyclic cause chain.

    A :class:`~duron.errors.DuronError` is forbidden unless its class opts in
    to durable round-tripping via ``__duron_round_trip__`` (the same marker
    :func:`~duron._runtime.execution.errors.is_round_tripping_error_type` consults).

    Returns:
        Whether the chain contains an unsafe error or a cycle.

    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None:
        if id(current) in seen:
            return True
        seen.add(id(current))
        if not isinstance(current, Exception) or (
            isinstance(current, DuronError)
            and getattr(type(current), "__duron_round_trip__", False) is not True
        ):
            return True
        current = current.__cause__
    return False


@dataclass(frozen=True, slots=True)
class EffectOperation:
    """Resolved, snapshotted call handed from the public API to the session."""

    identity: EffectIdentity
    seed: str
    input_digest: str
    descriptor: bytes
    raises: tuple[type[BaseException], ...]
    retry: RetryPolicy
    codec: Codec
    codec_id: str
    invoke: Callable[[EffectContextImpl], Awaitable[object]]
    inputs: bytes
    handles: tuple[HandleReference, ...]
    admission: asyncio.Semaphore | None = field(default=None, repr=False, compare=False)
