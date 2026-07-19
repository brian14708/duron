from __future__ import annotations

from asyncio import CancelledError
from typing import TYPE_CHECKING

from duron.errors import ApplicationError

if TYPE_CHECKING:
    from duron._history.entries import ErrorInfo

_MAX_CAUSE_DEPTH = 50


def _message(error: BaseException) -> str:
    try:
        return str(error)
    except Exception:  # ruff: ignore[blind-except]  # Error formatting must not block persistence.
        return "<exception message unavailable>"


def encode_error(error: Exception | CancelledError) -> ErrorInfo:
    """Encode an application failure or cancellation as bounded JSON data.

    Returns:
        The failure's type, message, optional details, and explicit cause chain.

    """
    return _encode(error, _MAX_CAUSE_DEPTH, set())


def _encode(
    error: Exception | CancelledError, depth: int, active: set[int]
) -> ErrorInfo:
    type_name = (
        error.type_name
        if isinstance(error, ApplicationError)
        else f"{type(error).__module__}:{type(error).__qualname__}"
    )
    result: ErrorInfo = {"type": type_name, "message": _message(error)}
    if isinstance(error, CancelledError):
        result["cancelled"] = True
    if depth <= 0 or id(error) in active:
        result["message"] = "<cause chain truncated>"
        return result
    if isinstance(error, ApplicationError):
        result["details"] = error.details
    active.add(id(error))
    try:
        cause = error.__cause__
        if isinstance(cause, Exception):
            result["cause"] = _encode(cause, depth - 1, active)
    finally:
        active.remove(id(error))
    return result


def decode_error(error_info: ErrorInfo) -> ApplicationError | CancelledError:
    """Decode fixed failure data without looking up exception classes.

    Returns:
        An application error, or cancellation when its marker is present.

    """
    if error_info.get("cancelled"):
        error: ApplicationError | CancelledError = CancelledError(error_info["message"])
    else:
        error = ApplicationError(
            error_info["type"], error_info["message"], details=error_info.get("details")
        )
    if cause := error_info.get("cause"):
        error.__cause__ = decode_error(cause)
    return error
