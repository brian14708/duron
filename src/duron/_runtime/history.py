"""Runtime queries over authoritative history, indexed for SQLite hosts."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import TypedDict

from duron._hints import JSONValue
from duron._history.encoding import decode_record, iter_entries
from duron._ref import Ref
from duron.errors import HistoryMismatchError
from duron.storage._sqlite import SQLiteStore

if TYPE_CHECKING:
    from duron._history.entries import Entry, PromiseCompleteEntry
    from duron.storage._protocol import EventReader, TerminalState


async def read_outcome(store: EventReader, run_id: str) -> PromiseCompleteEntry | None:
    header = await read_header(store, run_id=run_id)
    if header is None:
        return None
    for _offset, entry in await entries(
        store, run_id, "promise.complete", header["id"]
    ):
        if entry["type"] == "promise.complete":
            return entry
    return None


async def entries(
    reader: EventReader, run_id: str, kind: str, key: str | None = None
) -> list[tuple[int, Entry]]:
    if isinstance(reader, SQLiteStore):
        return [
            (offset, decode_record(event, offset))
            for offset, event in await reader.read_by_type(run_id, kind, key)
        ]
    return [
        (offset, entry)
        async for offset, entry in iter_entries(reader, run_id)
        if entry["type"] == kind and (key is None or entry_key(entry) == key)
    ]


def entry_key(entry: Entry) -> object:
    if entry["type"] == "stream.create":
        return entry.get("name")
    if entry["type"] == "promise.complete":
        return entry["promise_id"]
    if entry["type"] == "input.accept":
        return entry["port"]
    return None


class InitParams(TypedDict):
    args: list[JSONValue]
    kwargs: dict[str, JSONValue]
    id: str
    ref: Ref


async def read_header(backend: EventReader, *, run_id: str) -> InitParams | None:
    """Read the selected run's persisted header, or ``None`` if it has no history.

    The header is the first ``promise.create`` entry. Runtime framing uses fixed
    JSON; only application arguments use the workflow codec.

    Returns:
        The decoded header, or ``None`` when the selected log is empty.

    Raises:
        HistoryMismatchError: If the header is missing or malformed.

    """
    async for _offset, entry in iter_entries(backend, run_id, page_size=1):
        if entry["type"] != "promise.create":
            msg = "persisted run is missing its creation header"
            raise HistoryMismatchError(msg)
        args, kwargs, ref = entry.get("args"), entry.get("kwargs"), entry.get("ref")
        if args is not None and kwargs is not None and ref is not None:
            header: InitParams = {
                "args": args,
                "kwargs": kwargs,
                "id": entry["id"],
                "ref": ref,
            }
            return header
        msg = "persisted run header is malformed"
        raise HistoryMismatchError(msg)
    return None


def outcome_state(outcome: PromiseCompleteEntry) -> TerminalState:
    if "error" in outcome:
        return "cancelled" if outcome["error"].get("cancelled") else "failed"
    return "succeeded"
