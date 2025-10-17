from __future__ import annotations

from types import UnionType
from typing import Final, cast
from typing_extensions import Any, TypeAliasType, TypeVar

_T = TypeVar("_T")


class _Unspecified:
    def __bool__(self) -> bool:
        return False


Unspecified: Final = _Unspecified()
Provided: Final = cast("Any", ...)
"""
Mark a parameter as provided when invoked.
"""


MYPY = False
if MYPY:
    TypeHint = TypeAliasType(
        "TypeHint",
        type[_T] | _Unspecified | UnionType,
        type_params=(_T,),
    )
else:
    from typing_extensions import TypeForm

    TypeHint = TypeAliasType("TypeHint", TypeForm[_T] | _Unspecified, type_params=(_T,))
