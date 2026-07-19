"""Serialization contracts: the ``Codec`` protocol and the default JSON codec."""

from __future__ import annotations

import math
from typing import TypeGuard, cast
from typing_extensions import Protocol, TypeVar

from duron._hints import JSONValue, TypeHint

__all__ = ("Codec", "DefaultCodec", "JSONValue", "TypeHint")

_T = TypeVar("_T")


class Codec(Protocol):
    """Protocol for encoding/decoding Python objects to/from JSON-serializable values.

    Implement this protocol to provide custom serialization for types not supported
    by the default codec (e.g., dataclasses, Pydantic models, custom objects).
    Select it on the workflow to encode application values throughout that run.
    Runtime framing uses fixed JSON. Incompatible encoding changes require a
    new workflow name. Decoding must return the type described by the
    supplied hint or raise; workflow code trusts this generic contract. The
    codec can be used explicitly for structured ``ApplicationError.details``.
    Runtime error metadata uses fixed JSON; exception objects are never passed
    to the codec.
    """

    def encode_json(
        self, result: object, annotated_type: TypeHint[object], /
    ) -> JSONValue:
        """Convert a Python object to a JSON-serializable value for persistence.

        Args:
            result: The object to encode (e.g., operation result or argument)
            annotated_type: Type hint for the object's type

        Returns:
            A JSON-serializable value (None, bool, int, float, str, list, or dict)

        Raises:
            TypeError: If the object cannot be serialized

        """
        ...

    def decode_json(self, encoded: JSONValue, expected_type: TypeHint[_T], /) -> _T:
        """Reconstruct a Python object from a JSON value during log replay.

        Args:
            encoded: The JSON value to decode
            expected_type: Type hint for the expected return type

        Returns:
            The reconstructed Python object

        """
        ...


_MAX_DEPTH = 64
_MAX_INT_BITS = 4096


def _is_json_value(x: object) -> TypeGuard[JSONValue]:
    """Return whether ``x`` is a finite, canonical JSON-serializable value.

    Only exact builtin types qualify, so enum and other subclasses are
    rejected rather than silently decoded as their base type. Strings must be
    valid UTF-8 (no lone surrogates), integers are bounded, nesting is limited
    to a fixed depth, and cyclic containers are rejected.

    Returns:
        True if ``x`` contains only canonical JSON values and finite floats.

    """
    return _check_json(x, 0, set())


def _check_json(x: object, depth: int, active: set[int]) -> bool:
    kind = type(x)
    if kind is float:
        return math.isfinite(cast("float", x))
    if kind is int:
        return cast("int", x).bit_length() <= _MAX_INT_BITS
    if kind is str:
        return _is_utf8(cast("str", x))
    if x is None or kind is bool:
        return True
    if kind not in {list, dict} or depth >= _MAX_DEPTH or id(x) in active:
        return False
    active.add(id(x))
    try:
        if kind is list:
            return all(
                _check_json(item, depth + 1, active) for item in cast("list[object]", x)
            )
        return all(
            type(k) is str and _is_utf8(k) and _check_json(v, depth + 1, active)
            for k, v in cast("dict[object, object]", x).items()
        )
    finally:
        active.discard(id(x))


def _is_utf8(value: str) -> bool:
    try:
        _ = value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


class DefaultCodec:
    """Encode finite JSON values and return decoded values unchanged.

    Supports `None`, booleans, numbers, strings, lists, and dictionaries with
    string keys. Annotations do not trigger type validation or reconstruction.
    Use a custom [Codec][duron.codec.Codec] for other Python types.
    """

    @staticmethod
    def encode_json(result: object, _annotated_type: TypeHint[object]) -> JSONValue:
        if _is_json_value(result):
            return result
        msg = f"Result is not JSON-serializable: {type(result).__name__} value"
        raise TypeError(msg)

    @staticmethod
    def decode_json(encoded: JSONValue, _expected_type: TypeHint[_T]) -> _T:
        return cast("_T", encoded)
