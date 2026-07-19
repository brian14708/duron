"""Runtime checks at typed handle and executor wire boundaries."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import inspect
import math
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    NotRequired,
    Required,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)
from typing_extensions import TypeVar, is_typeddict

from duron._hints import JSONValue, UnspecifiedType, resolve_alias
from duron.codec import _is_json_value

if TYPE_CHECKING:
    from collections.abc import Callable

    from duron._hints import TypeHint
    from duron.codec import Codec


_T = TypeVar("_T")


def _validation_hint(annotation: object) -> TypeHint[Any]:
    return cast(
        "TypeHint[Any]",
        UnspecifiedType if annotation is inspect.Parameter.empty else annotation,
    )


class CheckedCodec:
    def __init__(self, codec: Codec) -> None:
        self.codec = codec

    def encode_json(self, value: object, annotation: TypeHint[Any]) -> JSONValue:
        validate_value(value, _validation_hint(annotation), allow_unspecified=True)
        encoded = self.codec.encode_json(value, annotation)
        self.decode_json(encoded, annotation)
        return encoded

    def decode_json(self, encoded: JSONValue, annotation: TypeHint[_T]) -> _T:
        value = self.codec.decode_json(encoded, annotation)
        validate_value(value, _validation_hint(annotation), allow_unspecified=True)
        return value


def checked(codec: Codec) -> CheckedCodec:
    return codec if isinstance(codec, CheckedCodec) else CheckedCodec(codec)


def validate_schema(schema: object, *, allow_unspecified: bool = False) -> None:
    """Require a concrete, runtime-checkable payload schema.

    Raises:
        TypeError: If the annotation cannot be validated at a wire boundary.

    """  # ruff: ignore[docstring-extraneous-exception]
    _validate_schema(schema, allow_unspecified=allow_unspecified, seen=[])


def _validate_schema(
    schema: object, *, allow_unspecified: bool, seen: list[object]
) -> None:
    if schema is JSONValue:
        return
    if schema in seen:
        return
    seen.append(schema)
    schema = resolve_alias(schema)
    if schema is Any or schema is UnspecifiedType or schema is object:
        if allow_unspecified:
            return
        msg = "a payload requires a specified supported type"
        raise TypeError(msg)
    origin = get_origin(schema)
    if schema is None or (origin is None and isinstance(schema, type)):
        if isinstance(schema, type) and (
            schema in {list, dict, tuple, set, frozenset}
            or getattr(schema, "_is_protocol", False)
        ):
            msg = "payload containers require concrete item types"
            raise TypeError(msg)
        if is_typeddict(schema):
            for annotation in _typed_dict_annotations(schema).values():
                _validate_schema(
                    annotation, allow_unspecified=allow_unspecified, seen=seen
                )
        return
    args = get_args(schema)
    if origin in {Annotated, Required, NotRequired}:
        _validate_schema(args[0], allow_unspecified=allow_unspecified, seen=seen)
        return
    if origin is Literal and args:
        if all(type(value) in {str, int, bool, bytes, type(None)} for value in args):
            return
    elif (
        (origin in {Union, UnionType} and len(args) >= 2)
        or (origin in {list, set, frozenset} and len(args) == 1)
        or (origin is dict and len(args) == 2)
        or origin is tuple
    ):
        for argument in args:
            if (
                argument is Ellipsis
                and origin is tuple
                and len(args) == 2
                and args[1] is Ellipsis
            ):
                continue
            _validate_schema(argument, allow_unspecified=allow_unspecified, seen=seen)
        return
    msg = "unsupported payload type annotation"
    raise TypeError(msg)


def validate_value(
    value: object, schema: object, *, allow_unspecified: bool = False
) -> None:
    """Check decoded values before exposing a typed result or handle payload.

    Raises:
        TypeError: If the value does not satisfy the declared schema.

    """
    validate_schema(schema, allow_unspecified=allow_unspecified)
    if not _matches(value, schema):
        msg = "decoded payload does not match its declared type"
        raise TypeError(msg)


def _matches(value: object, schema: object) -> bool:
    if schema is JSONValue:
        return _is_json_value(value)
    schema = resolve_alias(schema)
    if schema is Any or schema is UnspecifiedType or schema is object:
        return True
    if schema is None:
        return value is None
    origin = get_origin(schema)
    if origin is None and isinstance(schema, type):
        if schema in {bool, int, str, bytes, type(None)}:
            return type(value) is schema
        if schema is float:
            try:
                return type(value) in {int, float} and math.isfinite(
                    cast("int | float", value)
                )
            except OverflowError:
                return False
        if is_typeddict(schema):
            if not isinstance(value, dict):
                return False
            annotations = _typed_dict_annotations(schema)
            required = set(
                cast("frozenset[str]", getattr(schema, "__required_keys__", ()))
            )
            for key, annotation in annotations.items():
                if get_origin(annotation) is Required:
                    required.add(key)
                elif get_origin(annotation) is NotRequired:
                    required.discard(key)
            typed_value = cast("dict[str, object]", value)
            keys = set(typed_value)
            if not required.issubset(keys) or not keys.issubset(annotations):
                return False
            return all(_matches(typed_value[key], annotations[key]) for key in keys)
        return isinstance(value, schema)
    args = get_args(schema)
    if origin in {Annotated, Required, NotRequired}:
        return _matches(value, args[0])
    if origin in {Union, UnionType}:
        return any(_matches(value, argument) for argument in args)
    if origin is Literal:
        return any(type(value) is type(option) and value == option for option in args)
    if origin is dict:
        return isinstance(value, dict) and all(
            _matches(key, args[0]) and _matches(item, args[1])
            for key, item in cast("dict[object, object]", value).items()
        )
    if origin is tuple:
        if not isinstance(value, tuple):
            return False
        items = cast("tuple[object, ...]", value)
        if len(args) == 2 and args[1] is Ellipsis:
            return all(_matches(item, args[0]) for item in items)
        return len(items) == len(args) and all(map(_matches, items, args))
    if origin in {list, set, frozenset}:
        return isinstance(value, cast("type[object]", origin)) and all(
            _matches(item, args[0]) for item in cast("list[object]", value)
        )
    return False


def _typed_dict_annotations(schema: object) -> dict[str, object]:
    # basedmypy marks the typeshed signature incomplete despite this typed use.
    typed_get_type_hints = cast("Callable[..., dict[str, object]]", get_type_hints)
    return typed_get_type_hints(schema, include_extras=True)
