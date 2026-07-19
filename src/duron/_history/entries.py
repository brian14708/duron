from __future__ import annotations

from types import MappingProxyType
from typing import Final, Literal, NotRequired
from typing_extensions import TypedDict

from duron._hints import JSONValue
from duron.errors import CorruptLogError as CorruptLogError  # ruff: ignore[useless-import-alias]


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

    post: NotRequired[PostData]
    """Idempotency identity attached to a post's final mutation."""


class InvocationIdentityData(TypedDict):
    format_version: Literal[0]
    invocation_number: int
    recovery_count: int


class ErrorInfo(TypedDict):
    type: str
    module: str
    message: str
    args: list[JSONValue]
    cancelled: bool
    key: str
    cause: NotRequired[ErrorInfo]


class EffectCallData(TypedDict):
    format_version: Literal[0]
    descriptor: JSONValue
    input_digest: str
    run_seed: str
    inputs: JSONValue


class RunCreateEntry(BaseEntry):
    type: Literal["run.create"]
    result: JSONValue


class InputAcceptedEntry(BaseEntry):
    type: Literal["input.accept"]
    port: str
    value: JSONValue
    closed: bool
    retry_id: str | None


class RunFinishEntry(BaseEntry):
    type: Literal["run.finish"]
    result: NotRequired[JSONValue]
    error: NotRequired[ErrorInfo]
    cancelled: bool


class PromiseCreateEntry(BaseEntry):
    type: Literal["promise.create"]
    effect_call: NotRequired[EffectCallData]


class EffectAttemptStartEntry(BaseEntry):
    type: Literal["effect.attempt.start"]
    promise_id: str
    invocation: InvocationIdentityData
    session_id: str


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


class StreamScopeExitEntry(BaseEntry):
    type: Literal["stream.scope.exit"]
    stream_id: str


class BarrierEntry(BaseEntry):
    type: Literal["barrier"]


PostAction = Literal[
    "stream.send",
    "stream.close",
    "signal.send",
    "execution.complete",
    "execution.fail",
    "execution.cancel",
]

PostTargetKind = Literal["execution", "stream", "signal"]
POST_ACTION_TARGETS: Final = MappingProxyType({
    "stream.send": "stream",
    "stream.close": "stream",
    "signal.send": "signal",
    "execution.complete": "execution",
    "execution.fail": "execution",
    "execution.cancel": "execution",
})

# The handle direction each signal action acts on; the single definition of the
# action-to-direction correspondence used by both authorization and reporting.
POST_ACTION_DIRECTIONS: Final = MappingProxyType({"signal.send": "signal.sender"})


class PostScopeData(TypedDict):
    format_version: Literal[0]
    run_seed: str
    kind: Literal["execution", "control"]
    id: str


class PostTargetData(TypedDict):
    kind: PostTargetKind
    id: str


class PostData(TypedDict):
    scope: PostScopeData
    key: str
    action: PostAction


Entry = (
    RunCreateEntry
    | InputAcceptedEntry
    | RunFinishEntry
    | PromiseCreateEntry
    | EffectAttemptStartEntry
    | PromiseCompleteEntry
    | StreamCreateEntry
    | StreamEmitEntry
    | StreamCompleteEntry
    | StreamScopeExitEntry
    | BarrierEntry
)
"""
Concrete log entry types used within Duron.
"""
