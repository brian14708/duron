"""Runtime queries over authoritative history, indexed for SQLite hosts."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Final, NotRequired, cast
from typing_extensions import TypedDict

from duron._hints import JSONValue
from duron._history.encoding import decode_record
from duron._history.pagination import iter_entries
from duron.errors import HistoryMismatchError
from duron.storage._protocol import Event
from duron.storage._sqlite import SQLiteStore

if TYPE_CHECKING:
    from duron._history.entries import Entry, RunFinishEntry
    from duron._runtime.ops import OpMetadata
    from duron.storage import EventStore
    from duron.storage._protocol import EventReader


async def read_outcome(store: EventReader, run_id: str) -> RunFinishEntry | None:
    for _offset, entry in await entries(store, run_id, "run.finish"):
        if entry["type"] == "run.finish":
            return entry
    return None


async def entries(
    reader: EventReader, run_id: str, kind: str, key: str | None = None
) -> list[tuple[int, Entry]]:
    if isinstance(reader, SQLiteStore):
        return await _sqlite_entries(reader, run_id, kind, key)
    return [
        (offset, entry)
        async for offset, entry in iter_entries(reader, run_id)
        if entry["type"] == kind and (key is None or entry_key(entry) == key)
    ]


def entry_key(entry: Entry) -> object:
    if entry["type"] == "stream.create":
        return entry["name"]
    if entry["type"] == "promise.complete":
        return entry["promise_id"]
    if entry["type"] == "input.accept":
        return json.dumps(
            [entry["port"], entry["retry_id"]],
            separators=(",", ":"),
            ensure_ascii=False,
        )
    return None


async def _sqlite_entries(
    store: SQLiteStore, run_id: str, kind: str, key: str | None
) -> list[tuple[int, Entry]]:
    def read() -> list[tuple[int, Entry]]:
        with store._connection() as conn:
            query = (
                "SELECT offset,id,data,stream_id FROM log_entries "
                "WHERE run_id=? AND event_type=?"
            )
            params: list[str] = [run_id, kind]
            if key is not None:
                query += " AND event_key=?"
                params.append(key)
            rows = conn.execute(query + " ORDER BY offset", params).fetchall()
            return [
                (offset, decode_record(Event(id_, bytes(data), stream), offset))
                for offset, id_, data, stream in rows
            ]

    return await asyncio.to_thread(read)


CURRENT_VERSION: Final = 0


class InitParams(TypedDict):
    version: int
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    nonce: str
    workflow_name: NotRequired[str]
    workflow_version: NotRequired[str]
    contract: NotRequired[JSONValue]


async def read_header(
    backend: EventStore, *, run_id: str
) -> dict[str, JSONValue] | None:
    """Read the selected run's persisted header, or ``None`` if it has no history.

    The header is the first ``run.create`` entry. Runtime framing uses fixed
    JSON; only application arguments use the workflow codec.

    Returns:
        The decoded header, or ``None`` when the selected log is empty.

    Raises:
        HistoryMismatchError: If the header is missing, malformed, or declares
            an unsupported history format.

    """
    async for _offset, entry in iter_entries(backend, run_id, page_size=1):
        if entry["type"] != "run.create":
            msg = "persisted run is missing its creation header"
            raise HistoryMismatchError(msg)
        result = entry["result"]
        if _is_header(result):
            validate_history(result)
            return cast("dict[str, JSONValue]", result)
        msg = "persisted run header is malformed"
        raise HistoryMismatchError(msg)
    return None


def _is_header(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    header = cast("dict[object, object]", value)
    return (
        isinstance(header.get("version"), int)
        and isinstance(header.get("args"), list)
        and isinstance(header.get("kwargs"), dict)
        and isinstance(header.get("nonce"), str)
    )


def validate_history(init_params: object) -> None:
    if not isinstance(init_params, dict):
        msg = "Persisted history header is not an object"
        raise HistoryMismatchError(msg, expected="object", actual=init_params)
    header = cast("dict[str, object]", init_params)
    if type(header.get("version")) is not int or header["version"] != CURRENT_VERSION:
        found = header.get("version", "missing")
        msg = (
            f"Unsupported persisted history format {found!r}; expected "
            f"{CURRENT_VERSION}. Create a new run with a distinct run_id."
        )
        raise HistoryMismatchError(msg, expected=CURRENT_VERSION, actual=found)


def match_pending(
    msg: Entry, recvd_msgs: dict[str, tuple[str, tuple[object, ...] | None]]
) -> bool:
    """Match one pending op against the recorded entries.

    On a match, validates the recorded entry type and effect identity and
    forgets both copies so they cannot be matched again.

    Returns:
        True if ``msg`` was found in ``recvd_msgs``.

    """
    if msg["id"] not in recvd_msgs:
        return False
    # Membership is tested explicitly: an op with no declared effect identity
    # records a ``None`` identity, which ``dict.get`` cannot distinguish from an
    # absent key.
    persisted_type, persisted_identity = recvd_msgs[msg["id"]]
    validate_recorded_entry(msg, persisted_type, persisted_identity)
    del recvd_msgs[msg["id"]]
    return True


def record_effect_identity(entry: Entry, metadata: OpMetadata) -> None:
    if metadata.effect_name is None or metadata.effect_version is None:
        return
    entry["effect_name"] = metadata.effect_name
    entry["effect_version"] = metadata.effect_version


def effect_identity(entry: Entry) -> tuple[object, ...] | None:
    name = entry.get("effect_name")
    version = entry.get("effect_version")
    if name is None and version is None:
        return None
    if entry["type"] == "promise.create" and "effect_call" in entry:
        data = entry["effect_call"]
        return (name, version, data["descriptor"], data["input_digest"])
    return (name, version)


def validate_recorded_entry(
    expected: Entry, persisted_type: str, persisted_identity: tuple[object, ...] | None
) -> None:
    if persisted_type != expected["type"]:
        msg = (
            f"operation kind mismatch at id {expected['id']!r}: persisted "
            f"{persisted_type!r}, requested {expected['type']!r}"
        )
        raise HistoryMismatchError(
            msg, expected=persisted_type, actual=expected["type"]
        )
    requested = effect_identity(expected)
    if persisted_identity != requested:
        msg = (
            f"effect identity mismatch: persisted {persisted_identity!r}, "
            f"requested {requested!r}"
        )
        raise HistoryMismatchError(msg, expected=persisted_identity, actual=requested)
