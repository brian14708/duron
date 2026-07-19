from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from duron.errors import LeaseLostError, StorageError
from duron.storage import HistoryRecord, LeaseToken, MemoryStore, SQLiteStore

if TYPE_CHECKING:
    from pathlib import Path

    from duron.storage import HistoryStore


@pytest.fixture(params=["memory", "sqlite"])
def stores(
    request: pytest.FixtureRequest, tmp_path: Path
) -> tuple[HistoryStore, HistoryStore]:
    if request.param == "memory":
        store = MemoryStore()
        return store, store
    path = tmp_path / "history.db"
    return SQLiteStore(path), SQLiteStore(path)


async def test_fencing_and_run_isolation(
    stores: tuple[HistoryStore, HistoryStore],
) -> None:
    first, second = stores
    old = await first.acquire_lease("a")
    other = await first.acquire_lease("b")
    await first.append("a", old, [HistoryRecord("before", b"before")])
    current = await second.acquire_lease("a")
    for invalid in (old, other, LeaseToken(b"wrong")):
        with pytest.raises(LeaseLostError):
            await first.append("a", invalid, [])
        await first.release_lease("a", invalid)
    assert list(await second.append("a", current, [])) == []
    await second.append("a", current, [HistoryRecord("after", b"after")])
    await first.append("b", other, [HistoryRecord("before", b"independent")])
    assert [record for _, record in await first.read("a")] == [
        HistoryRecord("before", b"before"),
        HistoryRecord("after", b"after"),
    ]
    assert [record for _, record in await second.read("b")] == [
        HistoryRecord("before", b"independent")
    ]
    await second.release_lease("a", current)
    await second.release_lease("a", current)
    with pytest.raises(LeaseLostError):
        await second.append("a", current, [HistoryRecord("late", b"late")])


async def test_opaque_bytes_and_offset_resume(
    stores: tuple[HistoryStore, HistoryStore],
) -> None:
    store, reader = stores
    lease = await store.acquire_lease("run")
    records = [
        HistoryRecord("binary", b"\x00\xffnot JSON"),
        HistoryRecord("empty", b""),
        HistoryRecord("json", b'{"unsupported": "runtime does not inspect this"}'),
    ]
    offsets = list(await store.append("run", lease, records[:1]))
    gap = await store.acquire_lease("other")
    await store.append("other", gap, [HistoryRecord("gap", b"gap")])
    offsets.extend(await store.append("run", lease, records[1:]))
    assert offsets == sorted(set(offsets))
    assert list(await reader.read("run")) == list(zip(offsets, records, strict=True))
    assert list(await reader.read("run", after=offsets[0], limit=1)) == [
        (offsets[1], records[1])
    ]
    assert list(await reader.read("run", after=offsets[-1])) == []
    assert list(await reader.read("missing")) == []


async def test_duplicate_ids_reject_the_entire_batch(
    stores: tuple[HistoryStore, HistoryStore],
) -> None:
    store, reader = stores
    lease = await store.acquire_lease("run")
    existing = HistoryRecord("existing", b"original")
    await store.append("run", lease, [existing])
    for batch in (
        [HistoryRecord("new", b"new"), HistoryRecord("existing", b"replacement")],
        [HistoryRecord("duplicate", b"first"), HistoryRecord("duplicate", b"second")],
    ):
        with pytest.raises(StorageError):
            await store.append("run", lease, batch)
        assert [record for _, record in await reader.read("run")] == [existing]
    await store.append("run", lease, [HistoryRecord("new", b"accepted")])
    assert [record.payload for _, record in await reader.read("run")] == [
        b"original",
        b"accepted",
    ]


async def test_concurrent_batches_remain_contiguous(
    stores: tuple[HistoryStore, HistoryStore],
) -> None:
    store, reader = stores
    lease = await store.acquire_lease("run")
    batches = await asyncio.gather(
        *(
            store.append(
                "run",
                lease,
                [HistoryRecord(f"{index}-a", b"a"), HistoryRecord(f"{index}-b", b"b")],
            )
            for index in range(8)
        )
    )
    rows = list(await reader.read("run"))
    assert len(rows) == 16
    assert [offset for offset, _ in rows] == sorted(
        offset for batch in batches for offset in batch
    )
    assert {record.id for _, record in rows} == {
        f"{index}-{suffix}" for index in range(8) for suffix in ("a", "b")
    }
    for index in range(0, len(rows), 2):
        assert rows[index][1].id.removesuffix("-a") == rows[index + 1][
            1
        ].id.removesuffix("-b")


async def test_sqlite_reopen_preserves_records_and_fences_writer(
    tmp_path: Path,
) -> None:
    path = tmp_path / "history.db"
    first = SQLiteStore(path)
    old = await first.acquire_lease("run")
    record = HistoryRecord("saved", b"\xffpersisted")
    offsets = list(await first.append("run", old, [record]))
    reopened = SQLiteStore(path)
    assert list(await reopened.read("run")) == [(offsets[0], record)]
    current = await reopened.acquire_lease("run")
    with pytest.raises(LeaseLostError):
        await first.append("run", old, [])
    await first.release_lease("run", old)
    await reopened.append("run", current, [HistoryRecord("next", b"next")])
    assert [record.id for _, record in await first.read("run")] == ["saved", "next"]
