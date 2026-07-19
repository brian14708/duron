from __future__ import annotations

from types import MappingProxyType
from typing import Final, Literal, NotRequired
from typing_extensions import TypedDict

from duron.errors import CorruptLogError as CorruptLogError  # ruff: ignore[useless-import-alias]
from duron.types import JSONValue


class BaseEntry(TypedDict):
    id: str
    """
    Identifier for this log entry, unique within the log.
    """

    ts: int
    """
    Timestamp for this log entry, in microseconds since the Unix epoch.
    """

    metadata: NotRequired[dict[str, JSONValue]]
    """
    Non-essential metadata associated with this log entry.
    """

    source: Literal["task", "effect"]
    """
    The operation that generated this log entry.
    """

    effect_name: NotRequired[str]
    """Stable public effect name for replay validation, when applicable."""

    effect_version: NotRequired[str]
    """Stable public effect version for replay validation, when applicable."""


class ExecutionIdentityData(TypedDict):
    format_version: Literal[1]
    run_seed: str
    operation_id: str
    effect_id: str
    input_digest: str


class InvocationIdentityData(TypedDict):
    format_version: Literal[1]
    logical_attempt: int
    invocation_number: int
    recovery_count: int


class RuntimeErrorInfo(TypedDict):
    version: Literal[1]
    kind: Literal["effect_outcome_unknown"]
    identity: ExecutionIdentityData
    invocation: InvocationIdentityData


class ErrorInfo(TypedDict):
    type: str
    module: str
    message: str
    args: list[JSONValue]
    cancelled: bool
    key: str
    cause: NotRequired[ErrorInfo]
    runtime: NotRequired[RuntimeErrorInfo]


class EffectCallData(TypedDict):
    format_version: Literal[1]
    descriptor: JSONValue
    input_digest: str
    run_seed: str
    inputs: JSONValue


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise.create"]
    effect_call: NotRequired[EffectCallData]


class EffectAttemptStartEntry(BaseEntry):
    type: Literal["effect.attempt.start"]
    promise_id: str
    invocation: InvocationIdentityData
    session_id: str


class EffectRetryScheduledEntry(BaseEntry):
    type: Literal["effect.retry.scheduled"]
    promise_id: str
    invocation: InvocationIdentityData
    error: ErrorInfo
    classification: Literal["retryable"]
    not_before_us: int


class PromiseCompleteEntry(BaseEntry):
    type: Literal["promise.complete"]
    promise_id: str
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]


class StreamCreateEntry(BaseEntry):
    type: Literal["stream.create"]
    name: str | None
    owned: NotRequired[JSONValue]


class StreamEmitEntry(BaseEntry):
    type: Literal["stream.emit"]
    stream_id: str
    value: JSONValue


class StreamCompleteEntry(BaseEntry):
    type: Literal["stream.complete"]
    stream_id: str
    error: NotRequired[ErrorInfo]


class SignalAckEntry(BaseEntry):
    type: Literal["signal.ack"]
    signal_id: str
    message_id: str
    message_offset: int
    receiver_id: str
    invocation: InvocationIdentityData


class SignalScopeExitEntry(BaseEntry):
    type: Literal["signal.scope.exit"]
    signal_id: str


class BarrierEntry(BaseEntry):
    type: Literal["barrier"]


PostAction = Literal[
    "stream.send",
    "stream.close",
    "signal.send",
    "signal.ack",
    "execution.complete",
    "execution.fail",
    "execution.cancel",
]

PostTargetKind = Literal["execution", "stream", "signal"]
POST_ACTION_TARGETS: Final = MappingProxyType({
    "stream.send": "stream",
    "stream.close": "stream",
    "signal.send": "signal",
    "signal.ack": "signal",
    "execution.complete": "execution",
    "execution.fail": "execution",
    "execution.cancel": "execution",
})

# The handle direction each signal action acts on; the single definition of the
# action-to-direction correspondence used by both authorization and reporting.
POST_ACTION_DIRECTIONS: Final = MappingProxyType({
    "signal.send": "signal.sender",
    "signal.ack": "signal.receiver",
})


class PostScopeData(TypedDict):
    format_version: Literal[1]
    run_seed: str
    kind: Literal["execution", "control"]
    id: str


class PostTargetData(TypedDict):
    kind: PostTargetKind
    id: str


class PostReceiptEntry(BaseEntry):
    type: Literal["post.receipt"]
    format_version: Literal[1]
    scope: PostScopeData
    key: str
    action: PostAction
    target: PostTargetData
    payload: JSONValue
    payload_digest: str
    event_ids: list[str]


Entry = (
    PromiseCreateEntry
    | EffectAttemptStartEntry
    | EffectRetryScheduledEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
    | SignalAckEntry
    | SignalScopeExitEntry
    | BarrierEntry
    | PostReceiptEntry
)
"""
Concrete log entry types used within Duron.
"""
