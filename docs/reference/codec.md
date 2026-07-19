# `duron.codec` module

A codec converts application values to JSON for persistence and reconstructs them
when read. Select a codec on the workflow; its effects and ports use the same
codec.

## Default codec

`DefaultCodec` encodes finite JSON values and returns decoded JSON unchanged.
It accepts `None`, `bool`, `int`, finite `float`, `str`, lists of JSON values,
and dictionaries with string keys. Tuples, bytes, dates, dataclasses, and model
objects require a custom codec. `NaN` and infinities are rejected, including
inside containers. Only exact built-in JSON types are accepted; subclasses such
as enums need a custom codec. Strings must be valid UTF-8. Cyclic containers,
nesting beyond 64 container levels, and integers exceeding 4096 bits are rejected.

## Type validation

The default codec itself does not validate annotations or reconstruct Python
types. The runtime wraps the selected codec with checks at workflow, effect,
and port boundaries. Values must match their declared types. Missing annotations
and `Any` allow arbitrary values supported by the codec.

`Codec.decode_json` is generic: given `TypeHint[T]`, it must return `T` or
raise if it cannot decode the value. Duron validates decoded values and checks
that encoded values can be decoded to the declared type before committing them.
A custom codec must reconstruct external data as needed; its decoder can run
during encoding and must be free of external side effects. Validation checks
type conformance, not equality of the original and reconstructed objects.

## Implement a custom codec

A codec must encode every value it handles as finite JSON and reconstruct
values according to the supplied type hint. Select one codec on the workflow;
it handles workflow and effect arguments and results and port payloads.
Duron handles its own runtime metadata separately.

```python
from dataclasses import dataclass
from typing import TypeVar, cast

import duron
from duron.codec import DefaultCodec, JSONValue, TypeHint

T = TypeVar("T")


@dataclass(frozen=True)
class Order:
    number: str


class OrderCodec:
    def encode_json(
        self, value: object, annotated_type: TypeHint[object], /
    ) -> JSONValue:
        if isinstance(value, Order):
            return {"number": value.number}
        return DefaultCodec.encode_json(value, annotated_type)

    def decode_json(self, encoded: JSONValue, expected_type: TypeHint[T], /) -> T:
        if expected_type is Order:
            if not isinstance(encoded, dict) or not isinstance(
                encoded.get("number"), str
            ):
                raise TypeError("invalid encoded order")
            return cast(T, Order(encoded["number"]))
        return DefaultCodec.decode_json(encoded, expected_type)


@duron.workflow(codec=OrderCodec())
async def echo(ctx: duron.WorkflowContext, order: Order) -> Order:
    return order


async def example() -> None:
    async with duron.SQLiteScheduler(":memory:", workflows=[echo]) as scheduler:
        for _ in range(2):
            run = await scheduler.submit(echo, "order", Order("A-1"))
            assert await run.result() == Order("A-1")
```

This codec handles an `Order` as a directly annotated value. Supporting
`list[Order]`, unions, or nested models requires recursive handling of those
type hints; the fallback to `DefaultCodec` does not add it.

## Evolve a codec

Effects can be reused by workflows with different codecs. Make `decode_json` accept previously persisted wire values as the format
evolves, using in-band version tags when needed. Duron does not fingerprint
codec implementations. Choose a new workflow name and run ID for incompatible
changes.
Remote adapters must preserve the active workflow's serialization contract.

## Encode failure details

For structured failure details, explicitly encode a value with the codec and
pass the JSON to `ApplicationError(..., details=...)`. When catching the error,
decode `error.details` with the application schema. Exception objects are never
passed to the workflow codec.

## ::: duron.codec
