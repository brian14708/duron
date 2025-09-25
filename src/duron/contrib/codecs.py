from __future__ import annotations

import base64
import pickle
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duron.log import JSONValue


class PickleCodec:
    def encode_json(self, result: object) -> str:
        return base64.b64encode(pickle.dumps(result)).decode()

    def decode_json(self, encoded: JSONValue) -> object:
        if not isinstance(encoded, str):
            raise TypeError(f"Expected a string, got {type(encoded).__name__}")
        return pickle.loads(base64.b64decode(encoded.encode()))
