from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from duron.storage import SQLiteStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest.fixture
async def backend() -> AsyncIterator[SQLiteStore]:
    async with SQLiteStore(":memory:") as store:
        yield store
