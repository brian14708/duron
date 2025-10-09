from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeGuard, cast, final
from typing_extensions import TypeAliasType, override

from duron.typing import unspecified

if TYPE_CHECKING:
    from collections.abc import Callable
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


@dataclass(slots=True)
class FunctionType:
    return_type: TypeHint[Any]
    parameters: list[str]
    parameter_types: dict[str, TypeHint[Any]]


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

    def inspect_function(  # noqa: PLR6301
        self,
        fn: Callable[..., object],
    ) -> FunctionType:
        try:
            sig = inspect.signature(fn, eval_str=True)
        except NameError:
            sig = inspect.signature(fn)
        return_type = (
            sig.return_annotation
            if sig.return_annotation != inspect.Parameter.empty
            else unspecified
        )

        parameter_names: list[str] = []
        parameter_types: dict[str, TypeHint[Any]] = {}
        for k, p in sig.parameters.items():
            if p.kind in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }:
                continue

            parameter_names.append(k)
            parameter_types[p.name] = (
                p.annotation if p.annotation != inspect.Parameter.empty else unspecified
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
        msg = f"Result is not JSON-serializable: {result!r}"
        raise TypeError(msg)

    @override
    def decode_json(self, encoded: JSONValue, _expected_type: TypeHint[Any]) -> object:
        return encoded
