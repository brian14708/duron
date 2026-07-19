from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from duron.errors import ClaimBusyError, LeaseLostError, PostConflictError, StorageError
from duron.storage import Event, LeaseToken, SQLiteStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from duron.storage import EventStore


@pytest.fixture
async def stores(tmp_path: Path) -> AsyncIterator[tuple[EventStore, EventStore]]:
    path = tmp_path / "history.db"
    async with SQLiteStore(path) as first, SQLiteStore(path) as second:
        yield first, second


async def test_claim_retries_expiry_and_fencing(
    stores: tuple[EventStore, EventStore],
) -> None:
    first, second = stores
    old = await first.claim("a", "old", ttl=0.03)
    assert await second.claim("a", "old") == old
    with pytest.raises(ClaimBusyError):
        await second.claim("a", "new")
    await first.append("a", old, [Event("before", b"before")], batch_id="before")
    await asyncio.sleep(0.04)
    with pytest.raises(LeaseLostError):
        await first.renew("a", old)
    current = await second.claim("a", "new")
    with pytest.raises(LeaseLostError):
        await first.claim("a", "old")
    other = await first.claim("b", "other")
    for invalid in (old, other, LeaseToken(b"wrong")):
        with pytest.raises(LeaseLostError):
            await first.append("a", invalid, [], batch_id="invalid")
        await first.release("a", invalid)
    await second.renew("a", current)
    await second.append("a", current, [Event("after", b"after")], batch_id="after")
    await first.append("b", other, [Event("before", b"independent")], batch_id="before")
    assert [record.payload for _, record in await first.read("a")] == [
        b"before",
        b"after",
    ]
    assert [record.payload for _, record in await second.read("b")] == [b"independent"]
    await second.release("a", current)
    await second.release("a", current)
    with pytest.raises(LeaseLostError):
        await second.claim("a", "new")
    with pytest.raises(LeaseLostError):
        await second.append("a", current, [], batch_id="after-release")


async def test_uncertain_append_retries_and_offset_resume(
    stores: tuple[EventStore, EventStore],
) -> None:
    store, reader = stores
    lease = await store.claim("run", "owner")
    records = [
        Event("binary", b"\x00\xffnot JSON", "events"),
        Event("empty", b""),
        Event("json", b'{"opaque": true}', "events"),
    ]
    offsets = list(await store.append("run", lease, records[:1], batch_id="first"))
    assert (
        list(await reader.append("run", lease, records[:1], batch_id="first"))
        == offsets
    )
    with pytest.raises(PostConflictError):
        await store.append("run", lease, records[1:], batch_id="first")
    gap = await store.claim("other", "owner")
    await store.append("other", gap, [Event("gap", b"gap")], batch_id="gap")
    offsets.extend(await store.append("run", lease, records[1:], batch_id="second"))
    assert list(await reader.read("run")) == list(zip(offsets, records, strict=True))
    assert list(await reader.read("run", after=offsets[0], limit=1)) == [
        (offsets[1], records[1])
    ]
    assert list(await reader.read("run", after=offsets[0], stream_id="events")) == [
        (offsets[2], records[2])
    ]
    assert list(await reader.read("missing")) == []
    await store.release("run", lease)
    next_owner = await reader.claim("run", "next-owner")
    assert (
        list(await reader.append("run", next_owner, records[:1], batch_id="first"))
        == offsets[:1]
    )


async def test_duplicate_ids_reject_the_entire_batch(
    stores: tuple[EventStore, EventStore],
) -> None:
    store, reader = stores
    lease = await store.claim("run", "owner")
    existing = Event("existing", b"original")
    await store.append("run", lease, [existing], batch_id="original")
    for index, batch in enumerate((
        [Event("new", b"new"), Event("existing", b"replacement")],
        [Event("duplicate", b"first"), Event("duplicate", b"second")],
    )):
        with pytest.raises(StorageError):
            await store.append("run", lease, batch, batch_id=f"rejected-{index}")
        assert [record for _, record in await reader.read("run")] == [existing]
    await store.append("run", lease, [Event("new", b"accepted")], batch_id="accepted")


async def test_concurrent_batches_remain_contiguous(
    stores: tuple[EventStore, EventStore],
) -> None:
    store, reader = stores
    lease = await store.claim("run", "owner")
    batches = await asyncio.gather(
        *(
            store.append(
                "run",
                lease,
                [Event(f"{index}-a", b"a"), Event(f"{index}-b", b"b")],
                batch_id=f"batch-{index}",
            )
            for index in range(8)
        )
    )
    rows = list(await reader.read("run"))
    assert len(rows) == 16
    assert [offset for offset, _ in rows] == sorted(
        offset for batch in batches for offset in batch
    )
    for index in range(0, len(rows), 2):
        assert rows[index][1].id.removesuffix("-a") == rows[index + 1][
            1
        ].id.removesuffix("-b")


async def test_sqlite_reopen_preserves_claims_and_batch_receipts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "history.db"
    first = SQLiteStore(path)
    lease = await first.claim("run", "owner")
    record = Event("saved", b"\xffpersisted")
    offsets = list(await first.append("run", lease, [record], batch_id="saved"))
    await first.close()
    async with SQLiteStore(path) as reopened:
        assert await reopened.claim("run", "owner") == lease
        with pytest.raises(ClaimBusyError):
            await reopened.claim("run", "second")
        assert (
            list(await reopened.append("run", lease, [record], batch_id="saved"))
            == offsets
        )
        assert list(await reopened.read("run")) == [(offsets[0], record)]
        await reopened.release("run", lease)


async def test_sqlite_rejects_unsupported_schema(tmp_path: Path) -> None:
    path = tmp_path / "old.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE leases (run_id TEXT PRIMARY KEY, token BLOB)")
        connection.commit()
    async with SQLiteStore(path) as store:
        with pytest.raises(StorageError, match="unsupported SQLite schema version"):
            await store.claim("run", "owner")
