from __future__ import annotations

import base64
import pickle  # noqa: S403
from typing import TYPE_CHECKING, final
from typing_extensions import override

from duron.codec import Codec

if TYPE_CHECKING:
    from typing_extensions import Any

    from duron.codec import JSONValue
    from duron.typing import TypeHint


@final
class PickleCodec(Codec):
    __slots__ = ()

    @override
    def encode_json(self, result: object) -> str:
        return base64.b64encode(pickle.dumps(result)).decode()

    @override
    def decode_json(self, encoded: JSONValue, _expected_type: TypeHint[Any]) -> object:
        if not isinstance(encoded, str):
            msg = f"Expected a string, got {type(encoded).__name__}"
            raise TypeError(msg)
        return pickle.loads(base64.b64decode(encoded.encode()))  # noqa: S301
