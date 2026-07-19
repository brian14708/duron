"""Public error hierarchy for Duron.

Duron-specific conditions have dedicated classes for type-based handling.
The intermediate `WorkflowError` and `StorageError` bases group related
conditions. Invalid arguments can also raise standard Python exceptions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from duron._hints import JSONValue

__all__ = (
    "ChannelClosedError",
    "ClaimBusyError",
    "ContractMismatchError",
    "CorruptLogError",
    "DuronError",
    "ExecutionCompletedError",
    "HistoryMismatchError",
    "Interrupted",
    "InvalidEntryError",
    "InvalidExecutionRequestError",
    "InvalidHandleError",
    "InvalidPostError",
    "InvalidRunStateError",
    "LeaseLostError",
    "PortClosedError",
    "PostConflictError",
    "RemoteEffectError",
    "RunCancelledError",
    "RunNotFoundError",
    "RunUnavailableError",
    "StaleInvocationError",
    "StorageError",
    "SubmissionConflictError",
    "WorkflowDefinitionError",
    "WorkflowError",
)


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


class InvalidRunStateError(WorkflowError):
    """An operation was attempted against a run in the wrong state."""


class SubmissionConflictError(WorkflowError):
    """A run ID already identifies a different immutable invocation."""


class ContractMismatchError(WorkflowError):
    """A workflow reference does not match the persisted or registered contract."""


class RunCancelledError(WorkflowError):
    """The durable workflow acknowledged cancellation."""


class RunNotFoundError(WorkflowError):
    """The selected persisted run does not exist."""


class StorageError(DuronError):
    """Category base for storage and lease failures."""


class InvalidEntryError(DuronError, ValueError):
    """An append batch contains an invalid or unserializable entry."""


class LeaseLostError(StorageError):
    """The storage lease was lost while appending to the run."""


class ClaimBusyError(StorageError):
    """Another owner holds an unexpired history claim."""


class CorruptLogError(StorageError, ValueError):
    """Persisted log data cannot be decoded or validated."""

    def __init__(self, offset: int, message: str) -> None:
        super().__init__(f"Corrupt log entry at offset {offset}: {message}")
        self.offset = offset


class PortClosedError(DuronError):
    """An input is closed for delivery, or closed and drained for receiving."""


class RunUnavailableError(DuronError):
    """The run has no active owner accepting executor operations."""


class StaleInvocationError(DuronError):
    """An executor operation used a fenced invocation capability."""


class InvalidHandleError(DuronError):
    """A handle's identity, direction, type, or ownership is invalid."""


class InvalidExecutionRequestError(DuronError, ValueError):
    """A worker request does not match its registered execution schema."""


class ChannelClosedError(DuronError):
    """An explicit effect stream or signal channel is permanently closed.

    Unlike other runtime errors, a recorded ``ChannelClosedError`` replays as
    its real type: ``__duron_round_trip__`` opts the class into durable
    round-tripping for error declarations and reconstruction.
    """

    __duron_round_trip__: ClassVar[bool] = True


class InvalidPostError(DuronError, ValueError):
    """A posting key, action, or payload is malformed."""


class PostConflictError(DuronError):
    """A delivery ID, append batch, or posting key was reused with new content."""


class ExecutionCompletedError(DuronError):
    """A new mutation was attempted after final execution completion."""


class RemoteEffectError(DuronError):
    """An effect raised an exception that could not be reconstructed locally.

    Carries the recorded type name, message, and JSON-compatible arguments.
    Undeclared custom effect errors and errors whose constructors reject the
    recorded arguments use this wrapper, including on first execution.
    """

    def __init__(
        self, type_name: str, message: str, args: tuple[JSONValue, ...] = ()
    ) -> None:
        super().__init__(message)
        self.type_name = type_name
        self.remote_args = args


class Interrupted(DuronError):  # ruff: ignore[error-suffix-on-exception-name]
    """A signal interrupted an interruptible block.

    Attributes:
        value: The payload sent with the signal that caused the interrupt.

    """

    def __init__(self, value: object) -> None:
        super().__init__("interruptible block interrupted by signal")
        self.value = value
