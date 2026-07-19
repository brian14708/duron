"""Public error hierarchy for Duron.

Duron-specific conditions have dedicated classes for type-based handling.
The intermediate `WorkflowError` and `StorageError` bases group related
conditions. Invalid arguments can also raise standard Python exceptions.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from duron.codec import DefaultCodec

if TYPE_CHECKING:
    from duron._hints import JSONValue

__all__ = (
    "ApplicationError",
    "ChannelClosedError",
    "ClaimBusyError",
    "ClaimLostError",
    "ContractMismatchError",
    "CorruptLogError",
    "DuronError",
    "ExecutionCompletedError",
    "FailureKind",
    "HistoryMismatchError",
    "Interrupted",
    "InterruptedEffectError",
    "InvalidExecutionRequestError",
    "InvalidHandleError",
    "InvalidPostError",
    "PortClosedError",
    "PostConflictError",
    "RunCancelledError",
    "RunFailedError",
    "RunNotFoundError",
    "RunUnavailableError",
    "StorageError",
    "SubmissionConflictError",
    "WorkflowDefinitionError",
    "WorkflowError",
)


class FailureKind(StrEnum):
    """Why the scheduler stopped running a job without a workflow outcome."""

    HISTORY_MISMATCH = "history_mismatch"
    INTERRUPTED_EFFECT = "interrupted_effect"
    CONTRACT_MISMATCH = "contract_mismatch"
    CORRUPT_HISTORY = "corrupt_history"
    UNAVAILABLE = "unavailable"
    INVALID = "invalid"
    INFRASTRUCTURE = "infrastructure"


class DuronError(Exception):
    """Base class for Duron-specific errors."""


class WorkflowError(DuronError):
    """Category base for workflow lifecycle and definition errors."""


class WorkflowDefinitionError(WorkflowError):
    """A workflow or effect was declared or invoked incorrectly."""


class HistoryMismatchError(WorkflowError):
    """A replay diverged from the recorded history.

    The structured ``expected`` and ``actual`` fields name what diverged.
    """

    def __init__(
        self,
        message: str,
        *,
        expected: object | None = None,
        actual: object | None = None,
    ) -> None:
        super().__init__(message)
        self.expected = expected
        self.actual = actual


class InterruptedEffectError(HistoryMismatchError):
    """A committed effect call has no final outcome and cannot be dispatched again."""

    def __init__(self, operation_id: str) -> None:
        super().__init__(f"effect {operation_id!r} was interrupted before its outcome")
        self.operation_id = operation_id


class SubmissionConflictError(WorkflowError):
    """A run ID already identifies a different immutable invocation."""


class ContractMismatchError(WorkflowError):
    """A workflow reference does not match the persisted or registered contract."""


class RunCancelledError(WorkflowError):
    """The durable workflow acknowledged cancellation."""


class RunFailedError(WorkflowError):
    """No workflow outcome is available because the run is blocked or failed.

    ``kind`` classifies the failure. ``INFRASTRUCTURE`` means the retry budget
    was exhausted; ``INVALID`` means a deterministic input or codec failure.
    A blocked run may resume when its implementation becomes available, but an
    interrupted effect is never invoked again.
    """

    def __init__(self, message: str, kind: FailureKind) -> None:
        super().__init__(message)
        self.kind = kind


class RunNotFoundError(WorkflowError):
    """The selected persisted run does not exist."""


class StorageError(DuronError):
    """Category base for storage and ownership failures."""


class ClaimLostError(StorageError):
    """The storage claim was lost while appending to the run."""


class ClaimBusyError(StorageError):
    """Another owner holds the history claim."""


class CorruptLogError(StorageError, ValueError):
    """Persisted log data cannot be decoded or validated."""

    def __init__(self, offset: int, message: str) -> None:
        super().__init__(f"Corrupt log entry at offset {offset}: {message}")
        self.offset = offset


class PortClosedError(DuronError):
    """An input is closed for delivery, or closed and drained for receiving."""


class RunUnavailableError(DuronError):
    """The run has no active owner accepting executor operations."""


class InvalidHandleError(DuronError):
    """A handle's identity, direction, type, or ownership is invalid."""


class InvalidExecutionRequestError(DuronError, ValueError):
    """A worker request does not match its registered execution schema."""


class ChannelClosedError(DuronError):
    """An explicit effect stream or signal channel is permanently closed."""


class InvalidPostError(DuronError, ValueError):
    """A posting key, action, or payload is malformed."""


class PostConflictError(DuronError):
    """A delivery ID, append batch, or posting key was reused with new content."""


class ExecutionCompletedError(DuronError):
    """A new mutation was attempted after final execution completion."""


class ApplicationError(DuronError):
    """A persisted application failure from an effect or workflow.

    ``details`` contains optional JSON data. Use an application codec explicitly
    to encode and decode richer details with a known schema. Original exception
    classes and constructor arguments are never reconstructed.
    """

    def __init__(
        self,
        type_name: str,
        message: str,
        *,
        details: JSONValue = None,
        cause: ApplicationError | None = None,
    ) -> None:
        super().__init__(message)
        self.type_name = type_name
        self.message = message
        self.details: JSONValue = DefaultCodec.encode_json(details, object)
        self.__cause__: BaseException | None = cause

    @property
    def cause(self) -> BaseException | None:
        """The explicit cause, also available through Python's ``__cause__``."""
        return self.__cause__


class Interrupted(DuronError):  # ruff: ignore[error-suffix-on-exception-name]
    """A signal interrupted an interruptible block.

    Attributes:
        value: The payload sent with the signal that caused the interrupt.

    """

    def __init__(self, value: object) -> None:
        super().__init__("interruptible block interrupted by signal")
        self.value = value
