from __future__ import annotations

import asyncio
import random
import uuid
from typing import TYPE_CHECKING
from typing_extensions import override

import pytest

from duron import Context, RemoteEffectError, Session, durable
from duron._core.utils import decode_error, encode_error
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
                await (await t.start(activity, "test")).result()


def test_structured_errors_reconstruct_safe_types_and_fallback() -> None:
    value_error = decode_error(encode_error(ValueError("bad", 3)))
    assert isinstance(value_error, ValueError)
    assert value_error.args == ("bad", 3)

    unknown = decode_error({
        "module": "example.remote",
        "type": "UnknownError",
        "message": "remote failure",
        "args": ["remote failure"],
        "cancelled": False,
    })
    assert isinstance(unknown, RemoteEffectError)
    assert unknown.remote_type == "example.remote:UnknownError"


def test_structured_errors_preserve_cause_and_cancellation() -> None:
    cause = KeyError("missing")
    error = RuntimeError("outer")
    error.__cause__ = cause
    decoded = decode_error(encode_error(error))
    assert isinstance(decoded, RuntimeError)
    assert isinstance(decoded.__cause__, KeyError)
    decoded_cancel = decode_error(encode_error(asyncio.CancelledError()))
    assert isinstance(decoded_cancel, asyncio.CancelledError)
