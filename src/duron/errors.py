"""Public error hierarchy for Duron.

Duron-specific conditions have dedicated classes for type-based handling.
The intermediate `WorkflowError` and `StorageError` bases group related
conditions. Invalid arguments can also raise standard Python exceptions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from duron._runtime.execution.models import ExecutionIdentity, InvocationIdentity
    from duron.types import JSONValue


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


class StorageError(DuronError):
    """Category base for storage and lease failures."""


class InvalidEntryError(DuronError, ValueError):
    """An append batch contains an invalid or unserializable entry."""


class LeaseLostError(StorageError):
    """The storage lease was lost while appending to the run."""


class CorruptLogError(StorageError, ValueError):
    """Persisted log data cannot be decoded or validated."""

    def __init__(self, offset: int, message: str) -> None:
        super().__init__(f"Corrupt log entry at offset {offset}: {message}")
        self.offset = offset


class PortClosedError(DuronError):
    """A receive was attempted on a closed, drained input port."""


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
    round-tripping, and is the single marker consulted by both the encode
    side (``forbidden_error``) and the decode side
    (``is_round_tripping_error_type``).
    """

    __duron_round_trip__: ClassVar[bool] = True


class SignalMessagePendingError(DuronError):
    """A receiver already has an outstanding receive or unacknowledged message."""


class InvalidPostError(DuronError, ValueError):
    """A posting key, action, or payload is malformed."""


class PostConflictError(DuronError):
    """A semantic posting key was reused for a different event."""


class ExecutionCompletedError(DuronError):
    """A new mutation was attempted after final execution completion."""


class RequestAlreadyResolvedError(DuronError):
    """A request was resolved more than once."""


class UndeclaredErrorTypeError(DuronError):
    """An exception type was used that was not declared for the operation.

    `PendingRequest.fail` rejects undeclared custom types before recording a
    response. Declare them with `Request(..., raises=(MyError,))`. Built-in
    exceptions such as `ValueError` do not require a declaration.
    """


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


class EffectOutcomeUnknownError(DuronError):
    """Crash recovery stopped without knowing the external action's outcome."""

    def __init__(
        self, identity: ExecutionIdentity, invocation: InvocationIdentity
    ) -> None:
        super().__init__(
            f"Effect outcome is unknown for operation {identity.operation_id!r} "
            f"after {invocation.invocation_number} invocations "
            f"({invocation.recovery_count} recoveries)"
        )
        self.identity = identity
        self.invocation = invocation

    @property
    def logical_attempt(self) -> int:
        return self.invocation.logical_attempt

    @property
    def invocation_number(self) -> int:
        return self.invocation.invocation_number

    @property
    def recovery_count(self) -> int:
        return self.invocation.recovery_count


class Interrupted(DuronError):  # ruff: ignore[error-suffix-on-exception-name]
    """A signal interrupted an interruptible block.

    Attributes:
        value: The payload sent with the signal that caused the interrupt.

    """

    def __init__(self, value: object) -> None:
        super().__init__("interruptible block interrupted by signal")
        self.value = value
