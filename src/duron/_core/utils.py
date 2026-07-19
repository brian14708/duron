from __future__ import annotations

import builtins
from asyncio import CancelledError
from typing import TYPE_CHECKING, cast

from duron.errors import RemoteEffectError

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from duron.log._entry import ErrorInfo
    from duron.typing import JSONValue


# Bounds recursion when persisting an exception's __cause__ chain, so a deep or
# cyclic chain cannot overflow the stack on the durable-append path.
_MAX_CAUSE_DEPTH = 50

# Built-in exceptions round-trip as their real type during replay: their
# module:qualname identity is stable, so they are recognized by module rather
# than by an enumerated list. StopIteration is deliberately absent:
# asyncio.Future.set_exception rejects it, so a reconstructed StopIteration
# could never be delivered to the awaiting op (it decodes as RemoteEffectError).
_BUILTIN_MODULE = "builtins"

_NEVER_ROUND_TRIP: frozenset[str] = frozenset({"builtins:StopIteration"})


def _is_builtin_key(key: str) -> bool:
    """Return whether ``key`` names a built-in exception type.

    Subclasses are included: ``FileNotFoundError``, ``TimeoutError`` and the
    rest are built-ins too, and callers must not have to declare them.

    Returns:
        ``True`` if ``key`` is a built-in exception Duron reconstructs.

    """
    return key.startswith(f"{_BUILTIN_MODULE}:") and key not in _NEVER_ROUND_TRIP


def error_key(error_type: type[BaseException]) -> str:
    """Return the recorded identity for an exception type.

    The ``"{module}:{qualname}"`` format is defined here, so the encode and
    decode sides cannot drift apart.

    Returns:
        The stable key recorded in the log for ``error_type``.

    """
    return f"{error_type.__module__}:{error_type.__qualname__}"


def _resolve_builtin(key: str) -> type[BaseException] | None:
    """Resolve a recorded built-in key back to its class.

    Returns:
        The built-in exception class ``key`` names, or ``None`` if ``key`` is
        not a built-in exception Duron reconstructs.

    """
    if not _is_builtin_key(key):
        return None
    name = key.split(":", 1)[1]
    cls = getattr(builtins, name, None)
    return cls if isinstance(cls, type) and issubclass(cls, BaseException) else None


def is_builtin_error_type(error_type: type[BaseException]) -> bool:
    """Return whether ``error_type`` round-trips as itself without declaration.

    Returns:
        ``True`` if ``error_type`` is a built-in exception, which always
        round-trips as its real type during replay.

    """
    key = error_key(error_type)
    # Built-in subclasses are recognized by their module, so a subclass is a
    # built-in even though it is not itself a ``builtins`` attribute.
    return _resolve_builtin(key) is not None


def is_round_tripping_error_type(
    error_type: type[BaseException], declared: Iterable[type[BaseException]] = ()
) -> bool:
    """Return whether a recorded ``error_type`` replays as its real type.

    This is the single acceptance predicate for error round-tripping, shared by
    the decode side (which reconstructs the type from the log) and the host side
    (which decides whether a locally raised error may be recorded). Keeping one
    copy stops the two from drifting into rejecting an error the other would
    faithfully replay.

    Args:
        error_type: The exception type to check.
        declared: Exception types declared for the operation that produced the
            error (``@duron.effect(raises=[...])`` or ``Request(..., raises=[...])``).

    Returns:
        ``True`` if ``error_type`` replays as itself rather than decoding to
        :class:`~duron.errors.RemoteEffectError`.

    """
    if is_builtin_error_type(error_type):
        return True
    return any(error_type is t for t in declared)


def encode_error(error: Exception | CancelledError) -> ErrorInfo:
    return _encode(error, _MAX_CAUSE_DEPTH)


def _encode(error: Exception | CancelledError, depth: int) -> ErrorInfo:
    error_type = type(error)
    result: ErrorInfo = {
        "type": error_type.__qualname__,
        "module": error_type.__module__,
        "message": str(error),
        "args": [_safe_json(arg) for arg in error.args],
        "cancelled": isinstance(error, CancelledError),
        "key": error_key(error_type),
    }
    if depth > 1 and isinstance(error.__cause__, Exception):
        result["cause"] = _encode(error.__cause__, depth - 1)
    return result


def decode_error(
    error_info: ErrorInfo, error_types: Iterable[type[BaseException]] = ()
) -> Exception | CancelledError:
    """Reconstruct a recorded error.

    Args:
        error_info: The recorded error payload.
        error_types: Exception types declared for the operation that produced
            the error (``@duron.effect(raises=[...])`` or
            ``Request(..., raises=[...])``). A recorded error whose type is
            declared here — or is a built-in exception — is reconstructed as
            its real type; anything else decodes as :class:`RemoteEffectError`.

    Returns:
        The reconstructed exception.

    """
    declared = {error_key(t): t for t in error_types}
    return _decode(error_info, declared)


def _decode(
    error_info: ErrorInfo, declared: Mapping[str, type[BaseException]]
) -> Exception | CancelledError:
    data = cast("Mapping[str, object]", error_info)
    legacy = _decode_legacy(data)
    if legacy is not None:
        return legacy
    if data.get("cancelled") is True:
        error: Exception | CancelledError = CancelledError(
            *_decode_args(data.get("args"))
        )
    else:
        error = _decode_non_cancelled(data, declared)

    cause = data.get("cause")
    if isinstance(cause, dict):
        error.__cause__ = _decode(cast("ErrorInfo", cause), declared)
    return error


def _decode_legacy(data: Mapping[str, object]) -> Exception | CancelledError | None:
    """Decode the pre-structured error shape still valid in version-0 logs.

    Histories written before the structured encoding persisted errors as
    ``{"code": int, "message": str}`` with code ``-2`` marking cancellation.
    Those logs still pass header validation (the format version is unchanged),
    so their cancellation entries must keep decoding to ``CancelledError`` or
    replay control flow diverges.

    Returns:
        The reconstructed error, or ``None`` if ``data`` is not legacy-shaped.

    """
    if "cancelled" in data or "type" in data or not isinstance(data.get("code"), int):
        return None
    if data["code"] == -2:
        return CancelledError()
    message = data.get("message")
    return RemoteEffectError(
        "unknown", f"[{data['code']}] {message if isinstance(message, str) else ''}"
    )


def _decode_non_cancelled(
    data: Mapping[str, object], declared: Mapping[str, type[BaseException]]
) -> Exception:
    message = data.get("message")
    args = _decode_args(data.get("args"))
    key = data.get("key")
    module = data.get("module")
    type_name = data.get("type")
    display = (
        f"{module}:{type_name}"
        if isinstance(module, str) and isinstance(type_name, str)
        else "unknown"
    )
    # New histories record the explicit module:qualname key. Histories from
    # format 0 before that field existed are keyed by the same format, so
    # reconstruct it from the module/type fields when the key is absent.
    lookup_key = key if isinstance(key, str) else display
    exception_type = _resolve_builtin(lookup_key) or declared.get(lookup_key)
    if exception_type is not None:
        # ``args`` alone often cannot rebuild the original object: a declared
        # type whose __init__ takes no parameters (or only keyword parameters)
        # raises when handed the positional args encode_error recorded, and the
        # value would then decode as RemoteEffectError instead of the declared
        # type — on the *first* execution too, since an appended entry is fed
        # back through _handle_message immediately. Fall back through
        # progressively less faithful constructors so the type itself always
        # survives; if every constructor rejects the recorded data we cannot
        # rebuild the object at all, and RemoteEffectError is the honest answer.
        for candidate in (args, (), (message,)):
            try:
                return cast("Exception", exception_type(*candidate))
            except Exception:  # noqa: BLE001, S112, PERF203
                continue
    return RemoteEffectError(
        display, message if isinstance(message, str) else repr(args), args
    )


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
