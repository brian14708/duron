from __future__ import annotations

import asyncio
import contextlib
import random
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing_extensions import override

import pytest

from duron import (
    Context,
    Provided,
    Session,
    SessionError,
    Stream,
    StreamWriter,
    durable,
    effect,
)
from duron._core.utils import RemoteEffectError
from duron.contrib.codecs import PickleCodec
from duron.contrib.storage import MemoryLogStorage
from duron.log._helper import is_entry


async def test_invoke() -> None:
    @effect
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity(ctx: Context, i: str) -> str:
        x = await asyncio.gather(ctx.run(u), ctx.run(u))
        _ = await ctx.run(asyncio.sleep, 0.1)
        return i + ":".join(x)

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity, "test")
        a = await run.result()

    async with Session(log) as t:
        run = await t.start(activity, "test")
        b = await run.result()
    assert a == b

    log2 = MemoryLogStorage((await log.entries())[:-2])
    async with Session(log2) as t:
        run = await t.start(activity, "test")
        c = await run.result()
    assert a == c


async def test_invoke_error() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        _ = await ctx.run(asyncio.sleep, 0.1)

        def error() -> int:
            msg = "test error"
            raise ValueError(msg)

        _ = await ctx.run(error)

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity)
        with pytest.raises(Exception, match="test error"):
            await run.result()


async def test_effect_error_replay_preserves_exception_branch() -> None:
    @durable()
    async def activity(ctx: Context) -> str:
        def fail() -> None:
            msg = "expected"
            raise ValueError(msg)

        try:
            await ctx.run(fail)
        except ValueError:
            return "handled"
        return "unhandled"

    log = MemoryLogStorage()
    async with Session(log) as session:
        assert await (await session.start(activity)).result() == "handled"
    async with Session(log) as session:
        assert await (await session.start(activity)).result() == "handled"


async def test_unknown_effect_error_replays_as_safe_fallback() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        class LocalError(Exception):
            pass

        def fail() -> None:
            msg = "remote"
            raise LocalError(msg)

        await ctx.run(fail)

    log = MemoryLogStorage()
    async with Session(log) as session:
        with pytest.raises(RemoteEffectError):
            await (await session.start(activity)).result()
    async with Session(log) as session:
        with pytest.raises(RemoteEffectError):
            await (await session.start(activity)).result()


async def test_resume() -> None:
    sleep = 9999

    @durable()
    async def activity(ctx: Context, s: str) -> str:
        _ = await ctx.run(asyncio.sleep, sleep)
        return s

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity, "hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await asyncio.wait_for(run.result(), 0.1)

    async with Session(log) as t:
        sleep = 0
        x = await (await t.start(activity, "hello")).result()
    assert x == "hello"


async def test_cancel() -> None:
    @durable()
    async def activity(ctx: Context, s: str) -> str:
        with contextlib.suppress(asyncio.TimeoutError):
            _ = await asyncio.wait_for(ctx.run(asyncio.sleep, 9999), 0.1)
        _ = await asyncio.wait_for(ctx.run(asyncio.sleep, 9999), 0.1)
        return s

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity, "hello")
        with pytest.raises(asyncio.TimeoutError):
            _ = await run.result()
    async with Session(log) as t:
        with pytest.raises(asyncio.TimeoutError):
            _ = await (await t.resume(activity)).result()


async def test_timing() -> None:
    @durable()
    async def activity(ctx: Context) -> int:
        cnt = 0

        rnd = ctx.random()

        async def do(t: float) -> bool:
            try:
                _ = await asyncio.wait_for(ctx.run(asyncio.sleep, t), 0.1)
            except asyncio.TimeoutError:
                return True
            return False

        for f in await asyncio.gather(*[do(rnd.random() * 0.2) for _ in range(100)]):
            if f:
                cnt += 1
        return cnt

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
    async with Session(log) as t:
        b = await (await t.resume(activity)).result()
    assert a == b


@dataclass
class CustomPoint:
    x: int
    y: int


async def test_serialize() -> None:
    @durable(codec=PickleCodec())
    async def activity(ctx: Context) -> CustomPoint:
        def new_pt() -> CustomPoint:
            return CustomPoint(x=1, y=2)

        pt = await ctx.run(new_pt)
        return CustomPoint(x=pt.x + 5, y=pt.y + 10)

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()
        assert type(a) is CustomPoint
        assert a.x == 6
        assert a.y == 12


async def test_random() -> None:
    @durable()
    async def activity(ctx: Context) -> list[int]:
        await asyncio.sleep(0)
        return [await ctx.time_ns(), ctx.random().randint(1, 100)]

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()

    async with Session(log) as t:
        b = await (await t.resume(activity)).result()

    assert a == b


async def test_readonly_session_only_allows_verification() -> None:
    @durable()
    async def activity(_ctx: Context) -> int:
        return 42

    log = MemoryLogStorage()
    async with Session(log) as session:
        assert await (await session.start(activity)).result() == 42

    before = await log.entries()
    async with Session(log, readonly=True) as session:
        with pytest.raises(SessionError, match="readonly"):
            await session.start(activity)
    async with Session(log, readonly=True) as session:
        with pytest.raises(SessionError, match="readonly"):
            await session.resume(activity)
    async with Session(log, readonly=True) as session:
        await session.verify(activity)
    assert await log.entries() == before


async def test_task_close_is_idempotent() -> None:
    @durable()
    async def activity(_ctx: Context) -> int:
        return 42

    log = MemoryLogStorage()
    async with Session(log) as session:
        task = await session.start(activity)
        assert await task.result() == 42
        await task.close()
        await task.close()


async def test_session_exit_is_idempotent() -> None:
    session = Session(MemoryLogStorage())
    await session.__aenter__()  # noqa: PLC2801
    await session.__aexit__(None, None, None)
    await session.__aexit__(None, None, None)


async def test_session_enter_failure_cleans_up_state() -> None:
    class FailingLeaseStorage(MemoryLogStorage):
        @override
        async def acquire_lease(self) -> bytes:
            msg = "lease unavailable"
            raise RuntimeError(msg)

    session = Session(FailingLeaseStorage())
    with pytest.raises(RuntimeError, match="lease unavailable"):
        await session.__aenter__()  # noqa: PLC2801
    assert session._loop is None  # pyright: ignore[reportPrivateUsage] # noqa: SLF001
    assert session._token is None  # pyright: ignore[reportPrivateUsage] # noqa: SLF001


async def test_external_promise() -> None:
    v: dict[str, str] = {}

    @durable
    async def activity(ctx: Context) -> int:
        a, b = await ctx.create_future(int)
        v["data"] = a
        return await b

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity)

        async def do() -> None:
            while True:
                if v.get("data") is None:
                    await asyncio.sleep(0.01)
                    continue
                assert run.is_future_pending(v["data"])
                await run.complete_future(v["data"], result=9)
                assert not run.is_future_pending(v["data"])
                break

        bg = asyncio.create_task(do())
        assert await run.result() == 9
        await bg


async def test_external_promise_concurrent_completion_is_atomic() -> None:
    future_ids: list[str] = []

    @durable
    async def activity(ctx: Context) -> int:
        future_id, future = await ctx.create_future(int)
        future_ids.append(future_id)
        return await future

    log = MemoryLogStorage()
    async with Session(log) as session:
        run = await session.start(activity)

        async def complete(value: int) -> int:
            await run.complete_future(future_ids[0], result=value)
            return value

        completions = await asyncio.gather(
            complete(1), complete(2), return_exceptions=True
        )
        assert sum(isinstance(item, ValueError) for item in completions) == 1
        winner = next(item for item in completions if isinstance(item, int))
        assert await run.result() == winner

    completion_entries = [
        entry
        for entry in await log.entries()
        if is_entry(entry)
        and entry["type"] == "promise.complete"
        and entry["promise_id"] == future_ids[0]
    ]
    assert len(completion_entries) == 1


async def test_external_stream() -> None:
    @durable
    async def activity(_ctx: Context, test: Stream[int] = Provided) -> int:
        t = 0
        async for value in test:
            t += value
        return t

    log = MemoryLogStorage()
    async with Session(log) as t:
        run = await t.start(activity)
        test = await run.open_stream("test", "w")

        async def do() -> None:
            async with test as s:
                await s.send(0)
                for i in range(10):
                    await s.send(i)
                await s.send(-1)

        bg = asyncio.create_task(do())
        assert (await asyncio.gather(run.result(), bg))[0] == 44


async def test_external_stream_write() -> None:
    @durable
    async def activity(_ctx: Context, writer: StreamWriter[int] = Provided) -> int:
        async with writer as writer_:
            for i in range(10):
                await writer_.send(i)
            return 42

    log = MemoryLogStorage()

    async with Session(log) as sess:
        run = await sess.start(activity)
        output_stream = await run.open_stream("writer", "r")

        async def bg() -> list[int]:
            values: list[int] = [value async for value in output_stream]
            return values

        b = asyncio.create_task(bg())
        result = await run.result()
        assert result == 42
        assert await b == list(range(10))


async def test_invoke_wait_multiple() -> None:
    async def u() -> str:
        for _ in range(random.randint(1, 10)):
            await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @durable()
    async def activity(ctx: Context, i: str) -> str:
        x = await asyncio.gather(ctx.run(u), ctx.run(u))
        _ = await ctx.run(asyncio.sleep, 0.1)
        return i + ":".join(x)

    log = MemoryLogStorage()
    async with Session(log) as t:
        _ = await t.start(activity, "test")
        await asyncio.sleep(0)
    async with Session(log) as t:
        run = await t.resume(activity)
        while True:
            try:
                _ = await asyncio.wait_for(run.result(), 0.001)
                break
            except asyncio.TimeoutError:
                continue


async def test_time() -> None:
    @durable()
    async def activity(ctx: Context) -> Sequence[int]:
        async def do() -> int:
            t = ctx.random().random()
            await asyncio.sleep(t * 0.1)
            return await ctx.time_ns()

        await asyncio.sleep(0)
        return await asyncio.gather(*[do() for _ in range(10)])

    log = MemoryLogStorage()
    async with Session(log) as t:
        a = await (await t.start(activity)).result()

    async with Session(log) as t:
        b = await (await t.start(activity)).result()

    assert a == b


async def test_mismatch() -> None:
    @durable()
    async def activity(ctx: Context) -> None:
        _ = await ctx.time_ns()
        _ = await ctx.time_ns()

    @durable()
    async def activity2(ctx: Context) -> None:
        _ = await ctx.time_ns()
        _ = await ctx.time_ns()
        _ = await ctx.time_ns()

    @durable()
    async def activity3(ctx: Context) -> None:
        _ = await ctx.time_ns()

    log = MemoryLogStorage()
    async with Session(log) as t:
        await (await t.start(activity)).result()

    async with Session(log, readonly=True) as t:
        await t.verify(activity)

    async with Session(log) as t:
        with pytest.raises(RuntimeError, match="not complete"):
            await t.verify(activity2)

    async with Session(log) as t:
        with pytest.raises(RuntimeError, match="Extra"):
            await t.verify(activity3)


async def test_fast_error() -> None:
    @durable()
    async def activity(_ctx: Context) -> None:
        msg = "test error"
        raise ValueError(msg)

    log = MemoryLogStorage()
    async with Session(log) as t:
        with pytest.raises(ValueError, match="test error"):
            await (await t.start(activity)).result()


@durable
async def activity(ctx: Context) -> int:
    for _i in range(100):
        _ = await ctx.time_ns()
    return 42


@pytest.mark.benchmark
async def test_performance() -> None:
    log = MemoryLogStorage()
    async with Session(log) as run:
        _ = await (await run.start(activity)).result()
