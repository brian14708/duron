from __future__ import annotations

import base64
import pickle
from typing import TYPE_CHECKING, final

from typing_extensions import override

from duron.codec import Codec

if TYPE_CHECKING:
    from duron.codec import JSONValue


@final
class PickleCodec(Codec):
    __slots__ = ()

    @override
    def encode_json(self, result: object) -> str:
        return base64.b64encode(pickle.dumps(result)).decode()

    @override
    def decode_json(self, encoded: JSONValue, _expected_type: type | None) -> object:
        if not isinstance(encoded, str):
            raise TypeError(f"Expected a string, got {type(encoded).__name__}")
        return pickle.loads(base64.b64decode(encoded.encode()))
