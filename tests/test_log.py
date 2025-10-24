from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from duron.contrib.storage import FileLogStorage, MemoryLogStorage

if TYPE_CHECKING:
    from duron.log import Entry, LogStorage


def make_entry(id_: str) -> Entry:
    return {"type": "promise.create", "id": id_, "ts": -1, "source": "effect"}


async def impl_test_log_storage(storage: LogStorage) -> None:
    lease = await storage.acquire_lease()

    for i in range(3):
        _ = await storage.append(lease, make_entry(str(i)))

    lease2 = await storage.acquire_lease()
    _ = await storage.append(lease2, make_entry("4"))
    with pytest.raises(Exception, match="Invalid lease token"):
        _ = await storage.append(lease, make_entry("5"))

    entries: list[str] = []
    async for _o, entry_data in storage.stream():
        entries.append(entry_data["id"])
    assert len(entries) == 4


@pytest.mark.asyncio
async def test_file_storage_basic() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test.log"
        storage = FileLogStorage(log_file)
        await impl_test_log_storage(storage)


@pytest.mark.asyncio
async def test_memory_storage_basic() -> None:
    storage = MemoryLogStorage()
    await impl_test_log_storage(storage)
