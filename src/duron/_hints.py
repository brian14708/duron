"""Conditional type-alias definitions for :mod:`duron`.

Kept in a dedicated module because the ``MYPY``-gated ``TypeHint`` alias is
rejected by pyright when used in type expressions within its own module.
"""

from __future__ import annotations

import sys
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Final,
    Literal,
    TypeAlias,
    cast,
    get_args,
    get_origin,
)
from typing_extensions import TypeAliasType, TypeVar

if sys.version_info >= (3, 12):
    from typing import TypeAliasType as _NativeTypeAliasType
else:
    _NativeTypeAliasType = TypeAliasType

if TYPE_CHECKING:
    from collections.abc import Callable

_T = TypeVar("_T")


def resolve_alias(annotation: object) -> object:
    """Expand a type alias, substituting arguments of generic aliases.

    Returns:
        The annotation underlying the alias.

    Raises:
        TypeError: If an alias cycle has no concrete payload type.

    """
    seen: set[int] = set()
    while True:
        origin = get_origin(annotation)
        alias = (
            origin
            if isinstance(origin, (TypeAliasType, _NativeTypeAliasType))
            else annotation
        )
        if not isinstance(alias, (TypeAliasType, _NativeTypeAliasType)):
            return annotation
        if id(alias) in seen:
            msg = "type alias has no concrete payload type"
            raise TypeError(msg)
        seen.add(id(alias))
        annotation = _substitute(
            alias.__value__,
            dict(zip(alias.__type_params__, get_args(annotation), strict=False)),
        )


def _substitute(annotation: object, bindings: dict[object, object]) -> object:
    if isinstance(annotation, TypeVar):
        return bindings.get(annotation, annotation)
    args = get_args(annotation)
    if not args or not bindings:
        return annotation
    origin = get_origin(annotation)
    if origin is Literal:
        return annotation
    if origin is Annotated:
        args = args[:1]
    substituted = tuple(_substitute(arg, bindings) for arg in args)
    if args == substituted:
        return annotation
    copy_with = getattr(annotation, "copy_with", None)
    if copy_with is not None:
        return cast("Callable[[tuple[object, ...]], object]", copy_with)(substituted)
    if origin is UnionType:
        result = cast("Any", substituted[0])
        for argument in substituted[1:]:
            result |= argument
        return cast("object", result)
    return cast("object", cast("Any", origin)[substituted])


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
