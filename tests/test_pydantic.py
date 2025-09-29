from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from pydantic import BaseModel, TypeAdapter
from typing_extensions import override

from duron import fn
from duron.codec import Codec
from duron.context import Context
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron.codec import JSONValue


class PydanticPoint(BaseModel):
    x: int
    y: int


class PydanticCodec(Codec):
    @override
    def encode_json(self, result: object) -> JSONValue:
        return cast(
            "JSONValue", TypeAdapter(type(result)).dump_python(result, mode="json")
        )

    @override
    def decode_json(self, encoded: JSONValue, expected_type: type | None) -> object:
        return cast("object", TypeAdapter(expected_type).validate_python(encoded))


@pytest.mark.asyncio
async def test_pydantic_serialize():
    @fn(codec=PydanticCodec())
    async def activity(ctx: Context) -> PydanticPoint:
        async def new_pt() -> PydanticPoint:
            return PydanticPoint(x=1, y=2)

        pt = await ctx.run(new_pt)
        return PydanticPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with activity.create_task(log) as t:
        await t.start()
        a = await t.wait()
        assert type(a) is PydanticPoint
        assert a.x == 6 and a.y == 12
