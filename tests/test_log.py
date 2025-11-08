from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from duron.contrib.storage import FileLogStorage, MemoryLogStorage, SQLiteLogManager

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


@pytest.mark.asyncio
async def test_sqlite_storage_basic() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        manager = SQLiteLogManager(db_path)
        storage = await manager.create_log("task-1")
        await impl_test_log_storage(storage)


@pytest.mark.asyncio
async def test_sqlite_storage_multiplex() -> None:
    """Test that multiple tasks can have separate logs in the same database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        manager = SQLiteLogManager(db_path)

        # Create logs for two different tasks
        storage1 = await manager.create_log("task-1")
        storage2 = await manager.create_log("task-2")

        # Write entries to each log
        lease1 = await storage1.acquire_lease()
        lease2 = await storage2.acquire_lease()

        await storage1.append(lease1, make_entry("task1-entry1"))
        await storage1.append(lease1, make_entry("task1-entry2"))
        await storage2.append(lease2, make_entry("task2-entry1"))
        await storage2.append(lease2, make_entry("task2-entry2"))
        await storage2.append(lease2, make_entry("task2-entry3"))

        # Verify each task has its own entries
        entries1: list[str] = []
        async for _o, entry_data in storage1.stream():
            entries1.append(entry_data["id"])
        assert entries1 == ["task1-entry1", "task1-entry2"]

        entries2: list[str] = []
        async for _o, entry_data in storage2.stream():
            entries2.append(entry_data["id"])
        assert entries2 == ["task2-entry1", "task2-entry2", "task2-entry3"]


@pytest.mark.asyncio
async def test_sqlite_storage_multiprocess_simulation() -> None:
    """Test lease behavior with multiple storage instances (simulating processes)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create first manager and storage (simulating process 1)
        manager1 = SQLiteLogManager(db_path)
        storage1 = await manager1.create_log("shared-task")
        lease1 = await storage1.acquire_lease()
        await storage1.append(lease1, make_entry("from-process-1"))

        # Create second manager and storage (simulating process 2)
        manager2 = SQLiteLogManager(db_path)
        storage2 = await manager2.create_log("shared-task")

        # Process 2 force-acquires the lease
        lease2 = await storage2.acquire_lease()
        await storage2.append(lease2, make_entry("from-process-2"))

        # Process 1's lease is now invalid
        with pytest.raises(ValueError, match="Invalid lease token"):
            await storage1.append(lease1, make_entry("should-fail"))

        # Both processes can read all entries
        entries1: list[str] = []
        async for _o, entry_data in storage1.stream():
            entries1.append(entry_data["id"])
        assert entries1 == ["from-process-1", "from-process-2"]

        entries2: list[str] = []
        async for _o, entry_data in storage2.stream():
            entries2.append(entry_data["id"])
        assert entries2 == ["from-process-1", "from-process-2"]
