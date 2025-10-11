from __future__ import annotations

from typing import TYPE_CHECKING, cast
from typing_extensions import Any, override

import pydantic
import pytest

from duron import Context, fn
from duron.codec import Codec
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron.codec import JSONValue
    from duron.typing import TypeHint


class PydanticPoint(pydantic.BaseModel):
    x: int
    y: int


@pytest.mark.asyncio
async def test_pydantic_serialize() -> None:
    class PydanticCodec(Codec):
        @override
        def encode_json(self, result: object) -> JSONValue:
            return cast(
                "JSONValue",
                pydantic.TypeAdapter(type(result)).dump_python(result, mode="json"),
            )

        @override
        def decode_json(
            self, encoded: JSONValue, expected_type: TypeHint[Any]
        ) -> object:
            return cast(
                "object", pydantic.TypeAdapter(expected_type).validate_python(encoded)
            )

    @fn(codec=PydanticCodec())
    async def activity(ctx: Context) -> PydanticPoint:
        def new_pt() -> PydanticPoint:
            return PydanticPoint(x=1, y=2)

        pt = await ctx.run(new_pt)
        return PydanticPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with activity.invoke(log) as t:
        await t.start()
        a = await t.wait()
        assert type(a) is PydanticPoint
        assert a.x == 6
        assert a.y == 12
