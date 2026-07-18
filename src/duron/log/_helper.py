from __future__ import annotations

import binascii
import os
from typing import TYPE_CHECKING, TypeGuard, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron.log._entry import BaseEntry, Entry
    from duron.typing import JSONValue

from duron.log._entry import CorruptLogError

_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "promise.create": (),
    "promise.complete": ("promise_id",),
    "stream.create": ("name",),
    "stream.emit": ("stream_id", "value"),
    "stream.complete": ("stream_id",),
    "barrier": (),
    "trace": ("events",),
}


def set_metadata(entry: Entry, metadata: Mapping[str, JSONValue]) -> None:
    if metadata:
        m = entry.get("metadata")
        if m is None:
            entry["metadata"] = {**metadata}
        else:
            m.update(metadata)


def is_entry(entry: Entry | BaseEntry) -> TypeGuard[Entry]:
    return entry.get("type") in {
        "promise.create",
        "promise.complete",
        "stream.create",
        "stream.emit",
        "stream.complete",
        "barrier",
        "trace",
    }


def validate_entry(value: object, offset: int) -> Entry:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "entry must be a JSON object")
    entry = cast("dict[str, object]", value)
    for field, expected_type in (("id", str), ("ts", int), ("source", str)):
        if not isinstance(entry.get(field), expected_type):
            raise CorruptLogError(offset, f"missing or invalid {field!r} field")
    if entry["source"] not in {"task", "effect", "trace"}:
        raise CorruptLogError(offset, f"invalid source {entry['source']!r}")
    if not is_entry(cast("BaseEntry", entry)):
        raise CorruptLogError(offset, f"unknown entry type {entry.get('type')!r}")
    type_name = cast("str", entry["type"])
    for field in _REQUIRED_FIELDS[type_name]:
        if field not in entry:
            raise CorruptLogError(offset, f"missing {field!r} for {type_name}")
    if type_name in {"promise.complete", "stream.complete"}:
        error = entry.get("error")
        if error is not None and not isinstance(error, dict):
            raise CorruptLogError(offset, "error must be an object")
    if type_name == "trace" and not isinstance(entry.get("events"), list):
        raise CorruptLogError(offset, "trace events must be a list")
    metadata = entry.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise CorruptLogError(offset, "metadata must be a JSON object")
    return cast("Entry", entry)


def random_id() -> str:
    return binascii.b2a_base64(os.urandom(12), newline=False).decode()
