# `duron.codec` module

`DefaultCodec` encodes finite JSON values and returns decoded JSON unchanged.
It accepts `None`, `bool`, `int`, finite `float`, `str`, lists of JSON values,
and dictionaries with string keys. Tuples, bytes, dates, dataclasses, and model
objects require a custom codec. `NaN` and infinities are rejected, including
inside containers.

The default codec does not validate values against annotations or reconstruct
Python types from them. Effect execution and effect-channel boundaries perform
their own schema checks; annotations on workflow values and named ports are not
a universal runtime validator.

## Implement a custom codec

A codec must encode every value it handles as finite JSON and reconstruct
values according to the supplied type hint. Workflow codecs also see Duron's
JSON containers for coordination, so support ordinary JSON values alongside
application-specific types.

```python
from dataclasses import dataclass
from typing import Any

import duron
from duron.codec import DefaultCodec
from duron.storage import MemoryStore
from duron.types import JSONValue, TypeHint


@dataclass(frozen=True)
class Order:
    number: str


class OrderCodec:
    def encode_json(self, value: object, annotated_type: TypeHint[Any], /) -> JSONValue:
        if isinstance(value, Order):
            return {"number": value.number}
        return DefaultCodec.encode_json(value, annotated_type)

    def decode_json(
        self, encoded: JSONValue, expected_type: TypeHint[Any], /
    ) -> object:
        if expected_type is Order:
            if not isinstance(encoded, dict) or not isinstance(
                encoded.get("number"), str
            ):
                raise TypeError("invalid encoded order")
            return Order(encoded["number"])
        return DefaultCodec.decode_json(encoded, expected_type)


@duron.workflow(codec=OrderCodec(), codec_id="orders.json.v1")
async def echo(ctx: duron.WorkflowContext, order: Order) -> Order:
    return order


async def example() -> None:
    store = MemoryStore()
    for _ in range(2):
        assert await duron.run(echo(Order("A-1")), store, run_id="order") == Order(
            "A-1"
        )
```

This codec handles an `Order` as a directly annotated value. Supporting
`list[Order]`, unions, or nested models requires recursive handling of those
type hints; the fallback to `DefaultCodec` does not add it.

Select the codec with `@duron.workflow(codec=..., codec_id=...)`. Effects inherit
the workflow codec unless they select their own with `@duron.effect`. Custom
codec IDs must be nonempty, stable strings outside the reserved `duron.` prefix.
The built-in codec's ID is `duron.json.v1`. Treat encoding changes as definition
changes: use a new codec ID and definition version for incompatible contracts.

## ::: duron.codec
