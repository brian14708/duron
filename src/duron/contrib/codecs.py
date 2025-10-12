from __future__ import annotations

import binascii
import pickle  # noqa: S403
from typing import TYPE_CHECKING
from typing_extensions import Any, final

if TYPE_CHECKING:
    from duron.codec import JSONValue
    from duron.typing import TypeHint


@final
class PickleCodec:
    __slots__ = ()

    @staticmethod
    def encode_json(result: object) -> str:
        return binascii.b2a_base64(pickle.dumps(result), newline=False).decode()

    @staticmethod
    def decode_json(encoded: JSONValue, _expected_type: TypeHint[Any]) -> object:
        if not isinstance(encoded, str):
            msg = f"Expected a string, got {type(encoded).__name__}"
            raise TypeError(msg)
        return pickle.loads(binascii.a2b_base64(encoded.encode()))  # noqa: S301
