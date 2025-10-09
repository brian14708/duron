from __future__ import annotations

from collections.abc import AsyncGenerator

from duron.typing import FunctionType, inspect_function


def test_no_parameters() -> None:
    def simple_func() -> int:
        return 42

    result = inspect_function(simple_func)

    assert isinstance(result, FunctionType)
    assert result.parameters == []
    assert result.parameter_types == {}
    assert result.return_type is int


def test_no_parameters_no_return_type() -> None:
    def simple_func() -> None:
        pass

    result = inspect_function(simple_func)

    assert isinstance(result, FunctionType)
    assert result.parameters == []
    assert result.parameter_types == {}
    assert result.return_type is None


def test_type_annotated_parameters() -> None:
    def typed_func(_x: int, _y: str, _z: float) -> bool:
        return True

    result = inspect_function(typed_func)

    assert result.parameters == ["_x", "_y", "_z"]
    assert result.parameter_types == {"_x": int, "_y": str, "_z": float}
    assert result.return_type is bool


def test_return_type_annotation() -> None:
    def func_with_return() -> dict[str, int]:
        return {}

    result = inspect_function(func_with_return)

    assert result.parameters == []
    assert result.parameter_types == {}
    assert result.return_type == dict[str, int]


def test_complex_type_annotations() -> None:
    def complex_func(_x: int | str, _y: list[dict[str, int]]) -> tuple[int, ...]:
        return (1, 2, 3)

    result = inspect_function(complex_func)

    assert result.parameters == ["_x", "_y"]
    assert result.parameter_types == {"_x": int | str, "_y": list[dict[str, int]]}
    assert result.return_type == tuple[int, ...]


def test_async_function() -> None:
    async def async_func(_a: int, _b: str) -> bool:  # noqa: RUF029
        return True

    result = inspect_function(async_func)

    assert result.parameters == ["_a", "_b"]
    assert result.parameter_types == {"_a": int, "_b": str}
    assert result.return_type is bool


def test_varargs_and_kwargs() -> None:
    def func_with_varargs(_a: int, *_args: str, **_kwargs: bool) -> None:
        pass

    result = inspect_function(func_with_varargs)

    assert result.parameters == ["_a"]
    assert result.parameter_types == {"_a": int}
    assert result.return_type is None


def test_positional_only_and_keyword_only() -> None:
    def func_with_special_args(_a: int, /, _b: str, *, _c: float) -> bool:
        return True

    result = inspect_function(func_with_special_args)

    assert result.parameters == ["_a", "_b", "_c"]
    assert result.parameter_types == {"_a": int, "_b": str, "_c": float}
    assert result.return_type is bool


def test_iterator() -> None:
    async def generator() -> AsyncGenerator[int]:  # noqa: RUF029
        yield 1

    result = inspect_function(generator)

    assert result.parameters == []
    assert result.parameter_types == {}
    assert result.return_type == AsyncGenerator[int]
