"""IDs and receipts for keyed events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from duron._execution.models import canonical_json, digest
from duron.errors import InvalidPostError

if TYPE_CHECKING:
    from duron._hints import JSONValue
    from duron._history.entries import Entry


MAX_POST_KEY_BYTES = 1024


def validate_post_key(key: str) -> None:
    """Validate a bounded UTF-8 key before any authority lookup or mutation.

    Raises:
        InvalidPostError: If the key is empty, oversized, or invalid UTF-8.

    """
    try:
        size = len(key.encode("utf-8"))
    except UnicodeError as error:
        msg = "a posting key must be valid UTF-8"
        raise InvalidPostError(msg) from error
    if not 0 < size <= MAX_POST_KEY_BYTES:
        msg = f"a posting key must contain 1 through {MAX_POST_KEY_BYTES} UTF-8 bytes"
        raise InvalidPostError(msg)


def event_id(run_seed: str, owner_id: str, key: str, *scope: str) -> str:
    validate_post_key(key)
    return digest("duron.post.id.v0", [run_seed, owner_id, key, *scope])


def event_content(entry: Entry) -> bytes:
    return canonical_json(
        cast(
            "JSONValue",
            {field: value for field, value in entry.items() if field != "ts"},
        )
    )


@dataclass(frozen=True, slots=True)
class Receipt:
    """Offset of a committed event, including whether this was a retry."""

    offset: int
    duplicate: bool
