# `duron.codec` module

`DefaultCodec` encodes finite JSON values and returns decoded JSON unchanged.
It accepts `None`, `bool`, `int`, finite `float`, `str`, lists of JSON values,
and dictionaries with string keys. Tuples, bytes, dates, dataclasses, and model
objects require a custom codec. `NaN` and infinities are rejected, including
inside containers.

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
it handles application arguments, effect arguments and results, and port and
channel payloads. Runtime headers and handle references use
Duron's fixed JSON framing and never pass through the application codec.

```python
from dataclasses import dataclass
from typing import Any, TypeVar, cast

import duron
from duron.codec import DefaultCodec
from duron.codec import JSONValue, TypeHint

T = TypeVar("T")


@dataclass(frozen=True)
class Order:
    number: str


class OrderCodec:
    def encode_json(self, value: object, annotated_type: TypeHint[Any], /) -> JSONValue:
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

Select the codec with `@duron.workflow(codec=...)`. Effects always use the
calling workflow's codec and can be reused by workflows with different codecs.
Bump the workflow's `version` when the encoding contract changes incompatibly,
and use a new run ID. Duron checks the definition version when reopening a
run; it cannot detect a codec implementation change under the same version.
Remote adapters must preserve the active workflow's serialization contract.

## ::: duron.codec
