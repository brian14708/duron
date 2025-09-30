from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeGuard, cast, final

from typing_extensions import TypeAliasType, override

if TYPE_CHECKING:
    from collections.abc import Callable

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


@dataclass(slots=True)
class FunctionType:
    return_type: type | None
    parameters: list[str]
    parameter_types: dict[str, type | None]


class Codec(ABC):
    __slots__: tuple[str, ...] = ()

    @abstractmethod
    def encode_json(self, result: object, /) -> JSONValue: ...

    @abstractmethod
    def decode_json(
        self, encoded: JSONValue, expected_type: type | None, /
    ) -> object: ...

    def inspect_function(
        self,
        fn: Callable[..., object],
    ) -> FunctionType:
        sig = inspect.signature(fn, eval_str=True)
        return_type = (
            sig.return_annotation
            if sig.return_annotation != inspect.Parameter.empty
            else None
        )

        parameter_names: list[str] = []
        parameter_types: dict[str, type | None] = {}
        for k, p in sig.parameters.items():
            if (
                p.kind == inspect.Parameter.VAR_POSITIONAL
                or p.kind == inspect.Parameter.VAR_KEYWORD
            ):
                continue

            parameter_names.append(k)
            parameter_types[p.name] = (
                p.annotation if p.annotation != inspect.Parameter.empty else None
            )

        return FunctionType(
            return_type=return_type,
            parameters=parameter_names,
            parameter_types=parameter_types,
        )


@final
class DefaultCodec(Codec):
    __slots__ = ()

    @override
    def encode_json(self, result: object) -> JSONValue:
        if is_json_value(result):
            return result
        raise TypeError(f"Result is not JSON-serializable: {result!r}")

    @override
    def decode_json(self, encoded: JSONValue, _expected_type: type | None) -> object:
        return encoded
