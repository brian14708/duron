"""Shared symbolic references for definitions and errors."""

from __future__ import annotations

import binascii
from hashlib import blake2b
from typing import cast
from typing_extensions import TypedDict


class Ref(TypedDict):
    """The shared JSON reference for workflows, effects, and errors."""

    name: str


def hash_id(value: bytes, *, context: bytes = b"", key: bytes = b"") -> str:
    return binascii.b2a_base64(
        blake2b(value, salt=context, key=key, digest_size=12).digest(), newline=False
    ).decode()


def ref_of(value: object, *, name: str | None = None) -> Ref:
    """Return a symbolic reference.

    Returns:
        The definition's name, using its module and qualified name by default.

    """
    if name is None and not isinstance(value, type):
        name = cast("str | None", getattr(value, "name", None))
    if name is None:
        module = getattr(value, "__module__", "?")
        qualname = getattr(value, "__qualname__", type(value).__qualname__)
        name = f"{module}:{qualname}"
    return {"name": name}
