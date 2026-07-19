"""Public error hierarchy for Duron.

Every concrete condition Duron raises has its own class, so callers branch on
types (``except HistoryMismatchError``) rather than attributes. The
intermediate :class:`WorkflowError` and :class:`StorageError` bases group
related conditions for broad catches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duron.typing import JSONValue


class DuronError(Exception):
    """Base class for every error Duron raises."""


class WorkflowError(DuronError):
    """Category base for workflow lifecycle and definition errors."""


class WorkflowDefinitionError(WorkflowError):
    """A workflow or effect was declared or invoked incorrectly."""


class HistoryMismatchError(WorkflowError):
    """A replay diverged from the recorded history.

    The structured ``expected`` and ``actual`` fields name what diverged.
    """

    def __init__(
        self, message: str = "", *, expected: object = None, actual: object = None
    ) -> None:
        super().__init__(message)
        self.expected = expected
        self.actual = actual


class InvalidRunStateError(WorkflowError):
    """An operation was attempted against a run in the wrong state."""


class StorageError(DuronError):
    """Category base for storage and lease failures."""


class LeaseLostError(StorageError):
    """The storage lease was lost while appending to the run."""


class CorruptLogError(StorageError, ValueError):
    """Persisted log data cannot be decoded or validated."""

    def __init__(self, offset: int, message: str) -> None:
        super().__init__(f"Corrupt log entry at offset {offset}: {message}")
        self.offset = offset


class PortClosedError(DuronError):
    """A blocking receive was attempted on a closed, drained port."""


class RequestAlreadyResolvedError(DuronError):
    """A request was resolved more than once."""


class UndeclaredErrorTypeError(DuronError):
    """An exception type was used that was not declared for the operation.

    Only exception types declared via ``@duron.effect(raises=[...])`` or
    ``Request(..., raises=[...])`` round-trip as themselves across replay;
    undeclared types decode as :class:`RemoteEffectError`.
    """


class RemoteEffectError(DuronError):
    """An effect raised an exception that could not be reconstructed locally.

    Carries just enough to identify and, when the type is declared on the
    effect or port, re-raise the original.
    """

    def __init__(
        self, type_name: str, message: str, args: tuple[JSONValue, ...] = ()
    ) -> None:
        super().__init__(message)
        self.type_name = type_name
        self.remote_args = args


class Interrupted(DuronError):  # noqa: N818
    """A signal interrupted an interruptible block.

    Attributes:
        value: The payload sent with the signal that caused the interrupt.

    """

    def __init__(self, value: object) -> None:
        super().__init__("interruptible block interrupted by signal")
        self.value = value
