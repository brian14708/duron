from __future__ import annotations

import binascii
import pickle  # noqa: S403
from typing import TYPE_CHECKING, cast
from typing_extensions import Any

if TYPE_CHECKING:
    from duron.typing import JSONValue, TypeHint


class PickleCodec:
    """A [codec][duron.codec.Codec] that uses Python's pickle module \
            for serialization and deserialization.
    """

    @staticmethod
    def encode_json(result: object, _annotated_type: TypeHint[Any]) -> str:
        return binascii.b2a_base64(pickle.dumps(result), newline=False).decode()

    @staticmethod
    def decode_json(encoded: JSONValue, _expected_type: TypeHint[Any]) -> object:
        if not isinstance(encoded, str):
            msg = f"Expected a string, got {type(encoded).__name__}"
            raise TypeError(msg)
        return pickle.loads(binascii.a2b_base64(encoded.encode()))  # noqa: S301


try:
    from pydantic import TypeAdapter

    def _type_adapter(t: TypeHint[object], /) -> TypeAdapter[object]:
        return TypeAdapter(t)

except ModuleNotFoundError:

    def _type_adapter(_t: TypeHint[object], /) -> TypeAdapter[object]:
        msg = "Pydantic is not installed. Please install pydantic to use PydanticCodec."
        raise ModuleNotFoundError(msg)


class PydanticCodec:
    @staticmethod
    def encode_json(result: object, annotated_type: TypeHint[Any]) -> JSONValue:
        return cast(
            "JSONValue",
            _type_adapter(
                cast("type[object]", annotated_type) if annotated_type else type(result)
            ).dump_python(result, mode="json", exclude_none=True),
        )

    @staticmethod
    def decode_json(encoded: JSONValue, expected_type: TypeHint[Any]) -> object:
        return _type_adapter(expected_type).validate_python(encoded)
