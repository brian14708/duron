from __future__ import annotations

import fcntl
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from duron.contrib.storage import FileStorage, MemoryStorage, SQLiteLogManager
from duron.log import CorruptLogError

if TYPE_CHECKING:
    from duron.log import Entry, Storage


def make_entry(id_: str) -> Entry:
    return {"type": "promise.create", "id": id_, "ts": -1, "source": "effect"}


def make_metadata_entry(id_: str) -> Entry:
    return cast(
        "Entry",
        {
            "type": "promise.create",
            "id": id_,
            "ts": -1,
            "source": "effect",
            "metadata": {"trace.id": "trace-1", "nested": {"value": 3}},
        },
    )


async def impl_test_log_storage(storage: Storage) -> None:
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
    await storage.release_lease(lease2)


async def impl_test_lease_contract(storage: Storage, second_storage: Storage) -> None:
    first = await storage.acquire_lease()
    second = await second_storage.acquire_lease()

    assert first != second
    with pytest.raises(ValueError, match="Invalid lease token"):
        await storage.append(first, make_entry("stale"))

    await storage.release_lease(first)
    await second_storage.append(second, make_entry("current"))
    await second_storage.release_lease(second)
    await second_storage.release_lease(second)

    with pytest.raises(ValueError, match="Invalid lease token"):
        await second_storage.append(second, make_entry("released"))


async def test_file_storage_basic() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test.log"
        storage = FileStorage(log_file)
        await impl_test_log_storage(storage)


async def test_file_storage_uses_data_file_as_lock() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test.log"
        storage = FileStorage(log_file)

        lease = await storage.acquire_lease()
        assert log_file.exists()
        assert not log_file.with_suffix(log_file.suffix + ".lock").exists()
        await storage.release_lease(lease)


async def test_file_storage_data_file_lock_waits_for_other_writer() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test.log"
        log_file.touch()
        storage = FileStorage(log_file)

        with log_file.open("a+b") as held_lock:
            fcntl.flock(held_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def release_soon() -> None:
                time.sleep(0.05)
                fcntl.flock(held_lock, fcntl.LOCK_UN)

            releaser = threading.Thread(target=release_soon)
            releaser.start()
            # A contending writer waits for the OS lock (last-acquirer-wins)
            # instead of failing a valid operation with BlockingIOError.
            token = await storage.acquire_lease()
            releaser.join()
        assert token


async def test_file_storage_lease_contract_across_instances() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.log"
        await impl_test_lease_contract(FileStorage(path), FileStorage(path))


async def test_memory_storage_basic() -> None:
    storage = MemoryStorage()
    await impl_test_log_storage(storage)


async def test_memory_storage_lease_contract() -> None:
    storage = MemoryStorage()
    await impl_test_lease_contract(storage, storage)


async def test_sqlite_storage_basic() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        manager = SQLiteLogManager(db_path)
        storage = await manager.create_log("task-1")
        await impl_test_log_storage(storage)


async def test_sqlite_storage_lease_contract_across_instances() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.db"
        first = await SQLiteLogManager(path).create_log("task")
        second = await SQLiteLogManager(path).create_log("task")
        await impl_test_lease_contract(first, second)


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


@pytest.mark.parametrize("storage_kind", ["file", "memory", "sqlite"])
async def test_storage_round_trip_preserves_metadata(storage_kind: str) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage: Storage
        if storage_kind == "file":
            storage = FileStorage(Path(tmpdir) / "test.log")
        elif storage_kind == "memory":
            storage = MemoryStorage()
        else:
            manager = SQLiteLogManager(Path(tmpdir) / "test.db")
            storage = await manager.create_log("task")

        lease = await storage.acquire_lease()
        expected = make_metadata_entry("metadata")
        await storage.append(lease, expected)
        entries = [entry async for _offset, entry in storage.stream()]
        assert entries == [expected]
        await storage.release_lease(lease)


async def test_file_storage_rejects_corrupt_middle_entry() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.log"
        path.write_bytes(
            b'{"type":"promise.create","id":"1","ts":-1,"source":"effect"}\n'
            b"not-json\n"
            b'{"type":"promise.create","id":"2","ts":-1,"source":"effect"}\n'
        )
        with pytest.raises(CorruptLogError) as exc_info:
            _ = [entry async for _offset, entry in FileStorage(path).stream()]
        assert exc_info.value.offset > 0


async def test_file_storage_ignores_truncated_final_line() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.log"
        path.write_bytes(
            b'{"type":"promise.create","id":"1","ts":-1,"source":"effect"}\n'
            b'{"type":"promise.create"'
        )
        entries = [entry async for _offset, entry in FileStorage(path).stream()]
        assert [entry["id"] for entry in entries] == ["1"]


async def test_sqlite_storage_rejects_invalid_schema() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.db"
        manager = SQLiteLogManager(path)
        storage = await manager.create_log("task")
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                "INSERT INTO log_entries (task_id, id, data) VALUES (?, ?, jsonb(?))",
                ("task", "broken", b'{"id":"broken"}'),
            )
            conn.commit()
        finally:
            conn.close()

        with pytest.raises(CorruptLogError, match="invalid 'ts'"):
            _ = [entry async for _offset, entry in storage.stream()]


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"type": "promise.complete"}, "missing 'promise_id'"),
        ({"type": "stream.emit", "stream_id": "s"}, "missing 'value'"),
        ({"type": "trace", "events": {}}, "trace events must be a list"),
    ],
)
async def test_memory_storage_rejects_invalid_entry_schema(
    entry: dict[str, object], message: str
) -> None:
    storage = MemoryStorage()
    lease = await storage.acquire_lease()
    invalid_entry = {"id": "bad", "ts": 1, "source": "task", **entry}
    await storage.append(lease, cast("Entry", invalid_entry))
    with pytest.raises(CorruptLogError, match=message):
        _ = [item async for _offset, item in storage.stream()]


async def test_file_storage_append_truncates_torn_tail() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test.log"
        storage = FileStorage(log_file)
        token = await storage.acquire_lease()
        _ = await storage.append(token, make_entry("a"))

        # Simulate an interrupted append: a partial line with no newline.
        with log_file.open("ab") as f:
            _ = f.write(b'{"type": "promise.cre')

        # The torn remnant is tolerated on read...
        assert [e["id"] async for _, e in storage.stream()] == ["a"]

        # ...and dropped by the next append instead of fusing with the new
        # entry into one corrupt interior line that would block later resumes.
        _ = await storage.append(token, make_entry("b"))
        assert [e["id"] async for _, e in storage.stream()] == ["a", "b"]
