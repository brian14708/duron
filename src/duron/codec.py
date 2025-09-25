from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import Protocol, override

from duron.log import is_json_value

if TYPE_CHECKING:
    from duron.log import JSONValue


class Codec(Protocol):
    def encode_json(self, result: object, /) -> JSONValue: ...
    def decode_json(self, encoded: JSONValue, /) -> object: ...


@final
class DefaultCodec(Codec):
    @override
    def encode_json(self, result: object) -> JSONValue:
        if is_json_value(result):
            return result
        raise TypeError(f"Result is not JSON-serializable: {result!r}")

    @override
    def decode_json(self, encoded: JSONValue) -> object:
        return encoded
