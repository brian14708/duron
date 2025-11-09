from __future__ import annotations

import pydantic
import pytest

from duron import Context, Session, durable
from duron.contrib.codecs import PydanticCodec
from duron.contrib.storage import MemoryLogStorage


class PydanticPoint(pydantic.BaseModel):
    x: int
    y: int


@pytest.mark.asyncio
async def test_pydantic_serialize() -> None:
    @durable(codec=PydanticCodec())
    async def activity(ctx: Context) -> PydanticPoint:
        def new_pt() -> PydanticPoint:
            return PydanticPoint(x=1, y=2)

        pt = await ctx.run(new_pt)
        return PydanticPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
        assert type(a) is PydanticPoint
        assert a.x == 6
        assert a.y == 12
