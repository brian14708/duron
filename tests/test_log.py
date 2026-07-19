from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from duron.errors import (
    ClaimBusyError,
    ClaimLostError,
    FailureKind,
    PostConflictError,
    StorageError,
)
from duron.storage import ClaimToken, Event, SQLiteStore

if TYPE_CHECKING:
    from pathlib import Path

    from duron.storage import EventStore


async def test_claim_release_and_fencing(backend: EventStore) -> None:
    old = await backend.claim("a")
    with pytest.raises(ClaimBusyError):
        await backend.claim("a")
    await backend.append("a", old, [Event("before", b"before")])
    await backend.release("a", old)
    current = await backend.claim("a")
    other = await backend.claim("b")
    for invalid in (old, other, ClaimToken(b"wrong")):
        with pytest.raises(ClaimLostError):
            await backend.append("a", invalid, [])
        await backend.release("a", invalid)
    await backend.append("a", current, [Event("after", b"after")])
    assert [record.payload for _, record in await backend.read("a")] == [
        b"before",
        b"after",
    ]
    await backend.release("a", current)
    await backend.release("a", current)
    with pytest.raises(ClaimLostError):
        await backend.append("a", current, [])


async def test_uncertain_append_retries_and_offset_resume(backend: EventStore) -> None:
    token = await backend.claim("run")
    records = [
        Event("binary", b"\x00\xffnot JSON", "events"),
        Event("empty", b""),
        Event("json", b'{"opaque": "\xe4\xb8\xad"}', "events"),
    ]
    offsets = list(await backend.append("run", token, records[:1]))
    assert list(await backend.append("run", token, records[:1])) == offsets
    assert (
        list(await backend.append("run", token, [Event("binary", b"ignored", "other")]))
        == offsets
    )
    gap = await backend.claim("other")
    await backend.append("other", gap, [Event("gap", b"gap")])
    offsets.extend(await backend.append("run", token, records[1:]))
    assert list(await backend.read("run")) == list(zip(offsets, records, strict=True))
    assert list(await backend.read("run", after=offsets[0], limit=1)) == [
        (offsets[1], records[1])
    ]
    assert list(await backend.read("run", after=offsets[0], stream_id="events")) == [
        (offsets[2], records[2])
    ]
    assert list(await backend.read("missing")) == []
    assert await backend.read_event("run", "binary") == (offsets[0], records[0])
    assert await backend.read_event("other", "binary") is None
    assert await backend.read_event("run", "missing") is None
    await backend.release("run", token)
    next_owner = await backend.claim("run")
    assert list(await backend.append("run", next_owner, records[:1])) == offsets[:1]


async def test_partial_retry_and_duplicate_ids_reject_the_entire_batch(
    backend: EventStore,
) -> None:
    token = await backend.claim("run")
    existing = Event("existing", b"original")
    await backend.append("run", token, [existing])
    for error, batch in (
        (PostConflictError, [Event("new", b"new"), Event("existing", b"replacement")]),
        (StorageError, [Event("duplicate", b"first"), Event("duplicate", b"second")]),
    ):
        with pytest.raises(error):
            await backend.append("run", token, batch)
        assert [record for _, record in await backend.read("run")] == [existing]
    await backend.append("run", token, [Event("new", b"accepted")])


async def test_concurrent_batches_remain_contiguous(backend: EventStore) -> None:
    token = await backend.claim("run")
    batches = await asyncio.gather(
        *(
            backend.append(
                "run", token, [Event(f"{index}-a", b"a"), Event(f"{index}-b", b"b")]
            )
            for index in range(8)
        )
    )
    rows = list(await backend.read("run"))
    assert len(rows) == 16
    assert [offset for offset, _ in rows] == sorted(
        offset for batch in batches for offset in batch
    )
    for index in range(0, len(rows), 2):
        assert rows[index][1].id.removesuffix("-a") == rows[index + 1][
            1
        ].id.removesuffix("-b")


async def test_sqlite_reopen_releases_claims_and_preserves_event_ids(
    tmp_path: Path,
) -> None:
    path = tmp_path / "history.db"
    store = SQLiteStore(path)
    token = await store.claim("run")
    record = Event("saved", b"\xffpersisted")
    offsets = list(await store.append("run", token, [record]))
    await store.close()
    async with SQLiteStore(path) as reopened:
        with pytest.raises(ClaimLostError):
            await reopened.append("run", token, [record])
        token = await reopened.claim("run")
        assert list(await reopened.append("run", token, [record])) == offsets
        assert list(await reopened.read("run")) == [(offsets[0], record)]
        await reopened.release("run", token)


async def test_completed_job_retains_event_ids(tmp_path: Path) -> None:
    path = tmp_path / "completed.db"
    record = Event(
        "effect-outcome",
        b'{"type":"promise.complete","promise_id":"effect","result":1}',
    )
    async with SQLiteStore(path) as store:
        await store.job_submit("default", "run", b'{"name":"flow"}', b"{}", "root")
        old = await store.claim("run")
        offsets = await store.append("run", old, [record])
        assert (await store.job_get("default", "run")).state == "queued"
        await store.release("run", old)
        current = await store.claim("run")
        await store.job_state(
            "default", "run", "running", "temporary failure", FailureKind.INFRASTRUCTURE
        )
        final = Event(
            "root-outcome",
            b'{"type":"promise.complete","promise_id":"root","result":null}',
        )
        await store.inbox_post("default", "delivery", "run", "input", "commands", b"1")
        # Generic append never interprets a workflow outcome or updates its job.
        final_offsets = await store.append("run", current, [final])
        assert (await store.job_get("default", "run")).state == "running"
        assert len(await store.inbox_pending("default", "run")) == 1
        assert await store.finish("run", current, [final], "succeeded") == final_offsets
        assert await store.finish("run", current, [final], "succeeded") == final_offsets
        assert await store.inbox_pending("default", "run") == []
        with pytest.raises(ClaimLostError):
            await store.finish("run", old, [final], "succeeded")
        job = await store.job_get("default", "run")
        assert job.state == "succeeded"
        assert job.error is None
        assert job.error_kind is None
        await store.release("run", current)
    async with SQLiteStore(path) as reopened:
        token = await reopened.claim("run")
        assert await reopened.append("run", token, [record]) == offsets
        assert await reopened.read_event("run", record.id) == (offsets[0], record)
        assert (await reopened.job_get("default", "run")).state == "succeeded"


@pytest.mark.parametrize("version", [0, 20261020])
async def test_sqlite_rejects_unsupported_schema(tmp_path: Path, version: int) -> None:
    path = tmp_path / "old.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE leases (run_id TEXT PRIMARY KEY, token BLOB)")
        connection.execute(f"PRAGMA user_version={version}")
        connection.commit()
    async with SQLiteStore(path) as store:
        with pytest.raises(StorageError, match="unsupported SQLite schema version"):
            await store.claim("run")
