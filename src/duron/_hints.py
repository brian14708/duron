"""Conditional type-alias definitions for :mod:`duron.types`.

Kept in a dedicated module because the ``MYPY``-gated ``TypeHint`` alias is
rejected by pyright when used in type expressions within its own module.
"""

from __future__ import annotations

from types import UnionType
from typing import TYPE_CHECKING, Final, TypeAlias
from typing_extensions import TypeAliasType, TypeVar

_T = TypeVar("_T")


class _UnspecifiedType:
    def __bool__(self) -> bool:
        return False


UnspecifiedType: Final = _UnspecifiedType()
"""
Internal value indicating that a parameter was not specified.
"""

MYPY = False
if MYPY:
    TypeHint = TypeAliasType(
        "TypeHint", type[_T] | _UnspecifiedType | UnionType, type_params=(_T,)
    )
    """
    A type representing [typing_extensions.TypeForm][] or
    an internal unspecified-type sentinel.
    """
else:
    from typing_extensions import TypeForm

    TypeHint = TypeAliasType(
        "TypeHint", TypeForm[_T] | _UnspecifiedType, type_params=(_T,)
    )

if TYPE_CHECKING:
    JSONValue: TypeAlias = (
        dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None
    )
    """
        Recursive type representing any valid JSON value.
    """
else:
    JSONValue = TypeAliasType(
        "JSONValue",
        "dict[str, JSONValue] | list[JSONValue] | str | int | float | bool | None",
    )
