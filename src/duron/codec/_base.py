from __future__ import annotations

from typing import TYPE_CHECKING, TypeGuard, cast
from typing_extensions import Any, Protocol

if TYPE_CHECKING:
    from duron.typing import JSONValue, TypeHint


class Codec(Protocol):
    """Protocol for encoding/decoding Python objects to/from JSON-serializable values.

    Implement this protocol to provide custom serialization for types not supported
    by the default codec (e.g., dataclasses, Pydantic models, custom objects).
    """

    def encode_json(
        self, result: object, annotated_type: TypeHint[Any], /
    ) -> JSONValue:
        """Convert a Python object to a JSON-serializable value for persistence.

        Args:
            result: The object to encode (e.g., operation result or argument)

        Returns:
            A JSON-serializable value (None, bool, int, float, str, list, or dict)

        Raises:
            TypeError: If the object cannot be serialized
        """
        ...

    def decode_json(
        self, encoded: JSONValue, expected_type: TypeHint[Any], /
    ) -> object:
        """Reconstruct a Python object from a JSON value during log replay.

        Args:
            encoded: The JSON value to decode
            expected_type: Type hint for the expected return type

        Returns:
            The reconstructed Python object
        """
        ...


class DefaultCodec:
    """Default implementation of [Codec][duron.codec.Codec] that only \
            handles basic JSON-serializable types.
    """

    @staticmethod
    def encode_json(result: object, _annotated_type: TypeHint[Any]) -> JSONValue:
        if DefaultCodec._is_json_value(result):
            return result
        msg = f"Result is not JSON-serializable: {result!r}"
        raise TypeError(msg)

    @staticmethod
    def decode_json(encoded: JSONValue, _expected_type: TypeHint[Any]) -> object:
        return encoded

    @staticmethod
    def _is_json_value(x: object) -> TypeGuard[JSONValue]:
        if x is None or isinstance(x, (bool, int, float, str)):
            return True
        if isinstance(x, list):
            return all(
                DefaultCodec._is_json_value(item) for item in cast("list[object]", x)
            )
        if isinstance(x, dict):
            return all(
                isinstance(k, str) and DefaultCodec._is_json_value(v)
                for k, v in cast("dict[object, object]", x).items()
            )
        return False
