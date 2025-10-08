from types import UnionType

from typing_extensions import TypeAliasType, TypeVar

_T = TypeVar("_T")


class _Unspecified:
    def __bool__(self) -> bool:
        return False


unspecified = _Unspecified()


MYPY = False
if MYPY:
    TypeHint = TypeAliasType(
        "TypeHint", type[_T] | _Unspecified | UnionType, type_params=(_T,)
    )
else:
    from typing_extensions import TypeForm

    TypeHint = TypeAliasType("TypeHint", TypeForm[_T] | _Unspecified, type_params=(_T,))

__all__ = ["TypeHint", "unspecified"]
