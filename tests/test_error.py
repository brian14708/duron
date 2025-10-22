from __future__ import annotations

import asyncio
import random
import uuid
from typing import TYPE_CHECKING
from typing_extensions import override

import pytest

from duron import Context, Session, durable
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron.log import BaseEntry, Entry


class FlakyLogStorage(MemoryLogStorage):
    def __init__(
        self, entries: list[BaseEntry] | None = None, fail_at: int = 2
    ) -> None:
        super().__init__(entries)
        self.fail_at = fail_at
        self.call_count = 0

    @override
    async def append(self, token: bytes, entry: Entry) -> int:
        self.call_count += 1
        if self.call_count == self.fail_at:
            msg = "Simulated storage failure"
            raise RuntimeError(msg)
        return await super().append(token, entry)


@pytest.mark.asyncio
async def test_error_storage() -> None:
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity(ctx: Context, i: str) -> str:
        x = await asyncio.gather(ctx.run(u), ctx.run(u))
        _ = await ctx.run(lambda: asyncio.sleep(0.1))
        return i + ":".join(x)

    for i in range(1, 5):
        log = FlakyLogStorage(None, i)
        async with Session(log) as t:
            with pytest.raises(RuntimeError, match="Simulated storage failure"):
                await t.start(activity, "test").result()
