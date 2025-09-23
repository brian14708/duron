from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from duron.log.storage.file import FileLogStorage

if TYPE_CHECKING:
    from duron.log.entry import Entry
    from duron.log.storage.base import Offset


def make_entry(id: str) -> Entry:
    return {"type": "test", "id": id, "timestamp": -1}


@pytest.mark.asyncio
async def test_file_storage_basic():
    """Test basic file storage operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test.log"
        storage = FileLogStorage(log_file)

        lease = await storage.acquire_lease()

        for i in range(3):
            _ = await storage.append(lease, make_entry(str(i)))

        lease2 = await storage.acquire_lease()
        try:
            _ = await storage.append(lease2, make_entry("4"))
            _ = await storage.append(lease, make_entry("5"))
            raise AssertionError("Expected exception for invalid lease")
        except Exception:
            pass

        entries: list[str] = []
        offset: Offset | None = None
        async for o, entry_data in storage.stream(None, False):
            entries.append(entry_data["id"])
            offset = o
        assert len(entries) == 4

        _ = asyncio.create_task(storage.append(lease2, make_entry("5")))
        async for _, entry_data in storage.stream(offset, True):
            if entry_data["id"] == "5":
                break
