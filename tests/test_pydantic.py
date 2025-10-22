from __future__ import annotations

from typing import TYPE_CHECKING, cast
from typing_extensions import Any

import pydantic
import pytest

from duron import Context, Session, durable
from duron.contrib.storage import MemoryLogStorage

if TYPE_CHECKING:
    from duron.typing import JSONValue, TypeHint


class PydanticPoint(pydantic.BaseModel):
    x: int
    y: int


@pytest.mark.asyncio
async def test_pydantic_serialize() -> None:
    class PydanticCodec:
        @staticmethod
        def encode_json(result: object) -> JSONValue:
            return cast(
                "JSONValue",
                pydantic.TypeAdapter(type(result)).dump_python(result, mode="json"),
            )

        @staticmethod
        def decode_json(encoded: JSONValue, expected_type: TypeHint[Any]) -> object:
            return cast(
                "object", pydantic.TypeAdapter(expected_type).validate_python(encoded)
            )

    @durable(codec=PydanticCodec())
    async def activity(ctx: Context) -> PydanticPoint:
        def new_pt() -> PydanticPoint:
            return PydanticPoint(x=1, y=2)

        pt = await ctx.run(new_pt)
        return PydanticPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await t.start(activity).result()
        assert type(a) is PydanticPoint
        assert a.x == 6
        assert a.y == 12
