from __future__ import annotations

from typing import TYPE_CHECKING, TypeGuard

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron.log._entry import BaseEntry, Entry
    from duron.typing._hint import JSONValue


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
