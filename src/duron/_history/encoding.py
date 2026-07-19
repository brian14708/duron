# pyright: reportPrivateUsage=false

from __future__ import annotations

import binascii
import json
import os
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron._history.entries import Entry
    from duron.storage._protocol import EventReader

from duron._history.entries import CorruptLogError
from duron.codec import _is_json_value
from duron.storage._protocol import Event

_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "promise.create": (),
    "promise.complete": ("promise_id",),
    "stream.create": (),
    "stream.emit": ("stream_id", "value"),
    "stream.complete": ("stream_id",),
    "input.accept": ("port", "closed"),
    "barrier": (),
}


def _valid_ref(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    fields = cast("dict[str, object]", value)
    return set(fields) == {"name"} and isinstance(fields["name"], str)


def _valid_error(value: object) -> bool:
    while isinstance(value, dict):
        fields = cast("dict[str, object]", value)
        if (
            not set(fields).issubset({
                "type",
                "message",
                "details",
                "cancelled",
                "cause",
            })
            or not isinstance(fields.get("type"), str)
            or not isinstance(fields.get("message"), str)
            or ("cancelled" in fields and fields["cancelled"] is not True)
        ):
            return False
        if "details" in fields and not _is_json_value(fields["details"]):
            return False
        if "cause" not in fields:
            return True
        value = fields["cause"]
    return False


def _valid_entry(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    fields = cast("dict[str, object]", value)
    kind = fields.get("type")
    if (
        not isinstance(kind, str)
        or kind not in _REQUIRED_FIELDS
        or not isinstance(fields.get("id"), str)
        or not fields["id"]
        or type(fields.get("ts")) is not int
        or not isinstance(fields.get("source"), str)
        or fields.get("source") not in {"task", "effect"}
        or any(key not in fields for key in _REQUIRED_FIELDS[kind])
    ):
        return False
    for key in ("promise_id", "stream_id", "port", "name"):
        if key in fields and not isinstance(fields[key], str):
            return False
    if kind == "input.accept" and (
        type(fields["closed"]) is not bool
        or (not fields["closed"] and "value" not in fields)
    ):
        return False
    if kind == "promise.complete" and (("result" in fields) == ("error" in fields)):
        return False
    for key, expected in (("args", list), ("kwargs", dict), ("error", dict)):
        if key in fields and not isinstance(fields[key], expected):
            return False
    if "ref" in fields and not _valid_ref(fields["ref"]):
        return False
    if "error" in fields and not _valid_error(fields["error"]):
        return False
    return ("args" in fields) == ("kwargs" in fields)


def encode_entry(entry: Entry) -> Event:
    """Encode one typed runtime entry for opaque storage.

    Returns:
        The immutable serialized record.

    Raises:
        CorruptLogError: If the entry cannot be encoded.

    """
    try:
        payload = json.dumps(entry, allow_nan=False, separators=(",", ":")).encode(
            "utf-8"
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise CorruptLogError(0, str(exc)) from exc
    return Event(entry["id"], payload, _entry_stream_id(entry))


def decode_record(record: Event, offset: int) -> Entry:
    """Decode one opaque record and verify its stored identity.

    Returns:
        The decoded runtime entry.

    Raises:
        CorruptLogError: If the payload or stored identity is invalid.

    """
    try:
        value = json.loads(record.payload)
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise CorruptLogError(offset, f"invalid history payload: {exc}") from exc
    if not _valid_entry(value):
        raise CorruptLogError(offset, "invalid history entry fields")
    entry = cast("Entry", value)
    if entry["id"] != record.id:
        raise CorruptLogError(offset, "history record id does not match its payload")
    if _entry_stream_id(entry) != record.stream_id:
        raise CorruptLogError(offset, "history stream id does not match its payload")
    return entry


def _entry_stream_id(entry: Entry) -> str | None:
    if entry["type"] == "input.accept":
        return "__input__:" + entry["port"]
    if entry["type"] == "stream.create":
        return entry["id"]
    if entry["type"] == "stream.emit" or entry["type"] == "stream.complete":
        return entry["stream_id"]
    return None


async def iter_entries(
    backend: EventReader,
    run_id: str,
    *,
    after: int | None = None,
    page_size: int = 1024,
    stream_id: str | None = None,
) -> AsyncGenerator[tuple[int, Entry], None]:
    """Read bounded pages until the observed end, including after short pages.

    Yields:
        Committed offsets and entries in increasing order.

    """
    cursor = after
    while page := await backend.read(
        run_id, after=cursor, limit=page_size, stream_id=stream_id
    ):
        for offset, record in page:
            yield offset, decode_record(record, offset)
            cursor = offset


def random_id() -> str:
    """Return a random 12-byte token id (log entries and host-side op ids).

    Returns:
        A fresh base64-encoded random token.

    """
    return binascii.b2a_base64(os.urandom(12), newline=False).decode()
