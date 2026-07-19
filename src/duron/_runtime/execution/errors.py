from __future__ import annotations

import builtins
import math
import sys
from asyncio import CancelledError
from typing import TYPE_CHECKING, cast

from duron._runtime.execution.models import (
    EffectId,
    ExecutionIdentity,
    InvocationIdentity,
)
from duron._runtime.storage.helper import validate_error
from duron.errors import EffectOutcomeUnknownError, RemoteEffectError

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from duron._runtime.storage.entry import ErrorInfo
    from duron.types import JSONValue


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


def type_key(error_type: type) -> str:
    """Return the recorded identity for a type.

    The ``"{module}:{qualname}"`` format is defined here, so the encode and
    decode sides cannot drift apart.

    Returns:
        The stable key recorded in the log for ``error_type``.

    """
    return f"{error_type.__module__}:{error_type.__qualname__}"


def error_key(error_type: type[BaseException]) -> str:
    """Return the recorded identity for an exception type.

    Returns:
        The stable key recorded in the log for ``error_type``.

    """
    return type_key(error_type)


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


def _resolve_round_trippable(key: str) -> type[BaseException] | None:
    """Resolve a recorded key to a runtime error class that opted in to replay.

    Returns:
        The class ``key`` names when it carries ``__duron_round_trip__``, or
        ``None`` otherwise.

    """
    module, _, qualname = key.partition(":")
    module_obj = sys.modules.get(module)
    cls: object = module_obj
    for part in qualname.split("."):
        cls = getattr(cls, part, None)
    if (
        isinstance(cls, type)
        and issubclass(cls, BaseException)
        and getattr(cls, "__duron_round_trip__", False) is True
        and error_key(cls) == key
    ):
        return cls
    return None


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


def _is_marked_round_trip(error_type: type[BaseException]) -> bool:
    return getattr(error_type, "__duron_round_trip__", False) is True


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
    if is_builtin_error_type(error_type) or _is_marked_round_trip(error_type):
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
    if isinstance(error, EffectOutcomeUnknownError):
        result["runtime"] = {
            "version": 1,
            "kind": "effect_outcome_unknown",
            "identity": error.identity.fields(),
            "invocation": error.invocation.fields(),
        }
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
    validate_error(error_info, 0)
    declared = {error_key(t): t for t in error_types}
    return _decode(error_info, declared)


def _decode(
    error_info: ErrorInfo, declared: Mapping[str, type[BaseException]]
) -> Exception | CancelledError:
    data = cast("Mapping[str, object]", error_info)
    error: Exception | CancelledError
    if "runtime" in error_info:
        runtime = error_info["runtime"]
        identity = runtime["identity"]
        invocation = runtime["invocation"]
        error = EffectOutcomeUnknownError(
            ExecutionIdentity(
                identity["run_seed"],
                identity["operation_id"],
                EffectId(identity["effect_id"]),
                identity["input_digest"],
            ),
            InvocationIdentity(
                invocation["logical_attempt"],
                invocation["invocation_number"],
                invocation["recovery_count"],
            ),
        )
    elif data.get("cancelled") is True:
        error = CancelledError(*_decode_args(data.get("args")))
    else:
        error = _decode_non_cancelled(data, declared)

    cause = data.get("cause")
    if isinstance(cause, dict):
        error.__cause__ = _decode(cast("ErrorInfo", cause), declared)
    return error


def _decode_non_cancelled(
    data: Mapping[str, object], declared: Mapping[str, type[BaseException]]
) -> Exception:
    message = data.get("message")
    args = _decode_args(data.get("args"))
    key = cast("str", data["key"])
    module = data.get("module")
    type_name = data.get("type")
    display = (
        f"{module}:{type_name}"
        if isinstance(module, str) and isinstance(type_name, str)
        else "unknown"
    )
    exception_type = (
        _resolve_round_trippable(key) or _resolve_builtin(key) or declared.get(key)
    )
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
        for candidate in (args, (message,), ()):
            try:
                return cast("Exception", exception_type(*candidate))
            except Exception:  # ruff: ignore[blind-except, try-except-continue]
                continue
    return RemoteEffectError(
        display, message if isinstance(message, str) else repr(args), args
    )


def _safe_json(value: object) -> JSONValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {"repr": repr(value), "type": "float"}
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
