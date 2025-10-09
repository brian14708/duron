from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeGuard, cast, final
from typing_extensions import TypeAliasType, override

if TYPE_CHECKING:
    from typing_extensions import Any

    from duron.typing import TypeHint

    JSONValue = (
        dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None
    )
else:
    JSONValue = TypeAliasType(
        "JSONValue",
        "dict[str, JSONValue] | list[JSONValue] | str | int | float | bool | None",
    )


def is_json_value(x: object) -> TypeGuard[JSONValue]:
    if x is None or isinstance(x, (bool, int, float, str)):
        return True
    if isinstance(x, list):
        return all(is_json_value(item) for item in cast("list[object]", x))
    if isinstance(x, dict):
        return all(
            isinstance(k, str) and is_json_value(v)
            for k, v in cast("dict[object, object]", x).items()
        )
    return False


class Codec(ABC):
    __slots__: tuple[str, ...] = ()

    @abstractmethod
    def encode_json(self, result: object, /) -> JSONValue: ...

    @abstractmethod
    def decode_json(
        self,
        encoded: JSONValue,
        expected_type: TypeHint[Any],
        /,
    ) -> object: ...


@final
class DefaultCodec(Codec):
    __slots__ = ()

    @override
    def encode_json(self, result: object) -> JSONValue:
        if is_json_value(result):
            return result
        msg = f"Result is not JSON-serializable: {result!r}"
        raise TypeError(msg)

    @override
    def decode_json(self, encoded: JSONValue, _expected_type: TypeHint[Any]) -> object:
        return encoded
