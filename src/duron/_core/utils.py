from __future__ import annotations

import builtins
from asyncio import CancelledError
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron.log._entry import ErrorInfo
    from duron.typing import JSONValue


class RemoteEffectError(Exception):
    """Safe fallback for an effect exception that cannot be reconstructed."""

    def __init__(
        self, remote_type: str, message: str, args: tuple[JSONValue, ...] = ()
    ) -> None:
        super().__init__(message)
        self.remote_type = remote_type
        self.remote_args = args


_SAFE_EXCEPTIONS: dict[str, type[Exception]] = {
    f"builtins:{name}": cls
    for name in (
        "ArithmeticError",
        "AssertionError",
        "AttributeError",
        "EOFError",
        "IndexError",
        "KeyError",
        "LookupError",
        "NameError",
        "NotImplementedError",
        "OSError",
        "RuntimeError",
        "StopAsyncIteration",
        "StopIteration",
        "TypeError",
        "ValueError",
        "ZeroDivisionError",
    )
    if isinstance((cls := getattr(builtins, name)), type) and issubclass(cls, Exception)
}


def register_error_type(exception_type: type[Exception]) -> None:
    """Allow a specific exception class to be reconstructed during replay."""
    key = f"{exception_type.__module__}:{exception_type.__qualname__}"
    _SAFE_EXCEPTIONS[key] = exception_type


def encode_error(error: Exception | CancelledError) -> ErrorInfo:
    error_type = type(error)
    result: ErrorInfo = {
        "type": error_type.__qualname__,
        "module": error_type.__module__,
        "message": str(error),
        "args": [_safe_json(arg) for arg in error.args],
        "cancelled": isinstance(error, CancelledError),
    }
    if isinstance(error.__cause__, Exception):
        result["cause"] = encode_error(error.__cause__)
    return result


def decode_error(error_info: ErrorInfo) -> Exception | CancelledError:
    data = cast("Mapping[str, object]", error_info)
    if data.get("cancelled") is True:
        error: Exception | CancelledError = CancelledError(
            *_decode_args(data.get("args"))
        )
    else:
        module = data.get("module")
        type_name = data.get("type")
        message = data.get("message")
        if not isinstance(module, str) or not isinstance(type_name, str):
            return RemoteEffectError("unknown", "Malformed persisted effect error")
        qualified_type = f"{module}:{type_name}"
        args = _decode_args(data.get("args"))
        fallback = RemoteEffectError(
            qualified_type, message if isinstance(message, str) else repr(args), args
        )
        if exception_type := _SAFE_EXCEPTIONS.get(qualified_type):
            try:
                error = exception_type(*args)
            except (TypeError, ValueError):
                error = fallback
        else:
            error = fallback

    cause = data.get("cause")
    if isinstance(cause, dict):
        error.__cause__ = decode_error(cast("ErrorInfo", cause))
    return error


def _safe_json(value: object) -> JSONValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_safe_json(item) for item in cast("list[object]", value)]
    if isinstance(value, dict):
        mapping = cast("dict[object, object]", value)
        if all(isinstance(key, str) for key in mapping):
            return {cast("str", key): _safe_json(item) for key, item in mapping.items()}
        return {"repr": repr(mapping), "type": type(mapping).__qualname__}
    return {"repr": repr(value), "type": type(value).__qualname__}


def _decode_args(value: object) -> tuple[JSONValue, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(cast("list[JSONValue]", value))
