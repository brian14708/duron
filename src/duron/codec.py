"""Serialization contracts: the ``Codec`` protocol and the default JSON codec."""

from __future__ import annotations

import math
from typing import Any, TypeGuard, cast
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
    workflow version change. Decoding must return the type described by the
    supplied hint or raise; workflow code trusts this generic contract.
    """

    def encode_json(
        self, result: object, annotated_type: TypeHint[Any], /
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


def _is_json_value(x: object) -> TypeGuard[JSONValue]:
    """Return whether ``x`` is a finite JSON-serializable value.

    Returns:
        True if ``x`` contains only JSON values and finite floats.

    """
    if isinstance(x, float):
        return math.isfinite(x)
    if x is None or isinstance(x, (bool, int, str)):
        return True
    if isinstance(x, list):
        return all(_is_json_value(item) for item in cast("list[object]", x))
    if isinstance(x, dict):
        return all(
            isinstance(k, str) and _is_json_value(v)
            for k, v in cast("dict[object, object]", x).items()
        )
    return False


class DefaultCodec:
    """Encode finite JSON values and return decoded values unchanged.

    Supports `None`, booleans, numbers, strings, lists, and dictionaries with
    string keys. Annotations do not trigger type validation or reconstruction.
    Use a custom [Codec][duron.codec.Codec] for other Python types.
    """

    @staticmethod
    def encode_json(result: object, _annotated_type: TypeHint[Any]) -> JSONValue:
        if _is_json_value(result):
            return result
        msg = f"Result is not JSON-serializable: {result!r}"
        raise TypeError(msg)

    @staticmethod
    def decode_json(encoded: JSONValue, _expected_type: TypeHint[_T]) -> _T:
        return cast("_T", encoded)
