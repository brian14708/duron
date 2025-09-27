from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from typing_extensions import Protocol, override

from duron.log import is_json_value

if TYPE_CHECKING:
    from collections.abc import Callable

    from duron.log import JSONValue


@dataclass(slots=True)
class FunctionType:
    return_type: type | None
    parameters: list[str]
    parameter_types: dict[str, type | None]


class Codec(Protocol):
    def encode_json(self, result: object, /) -> JSONValue: ...
    def decode_json(
        self, encoded: JSONValue, expected_type: type | None, /
    ) -> object: ...

    def inspect_function(
        self,
        fn: Callable[..., object],
    ) -> FunctionType: ...


@final
class DefaultCodec(Codec):
    @override
    def encode_json(self, result: object) -> JSONValue:
        if is_json_value(result):
            return result
        raise TypeError(f"Result is not JSON-serializable: {result!r}")

    @override
    def decode_json(self, encoded: JSONValue, _expected_type: type | None) -> object:
        return encoded

    @override
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
