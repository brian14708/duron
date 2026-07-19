from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from duron.storage import MemoryStore, SQLiteStore

if TYPE_CHECKING:
    from pathlib import Path

    from duron.storage import HistoryStore


@pytest.fixture(params=["memory", "sqlite"])
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> HistoryStore:
    if request.param == "memory":
        return MemoryStore()
    return SQLiteStore(tmp_path / "backend.db")
