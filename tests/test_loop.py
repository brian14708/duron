import asyncio
import contextlib
import random
import time

import pytest
from typing_extensions import overload

import duron.event_loop


@pytest.mark.asyncio
async def test_timer():
    async def timer() -> int:
        await asyncio.sleep(0.1)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                asyncio.wait_for(asyncio.sleep(10000), timeout=0.2), timeout=10000
            )
        return 0

    loop = duron.event_loop.create_loop(b"")
    loop.tick(time.time_ns())
    tsk = loop.create_task(timer())
    while (waitset := loop.poll_completion(tsk)) is not None:
        await waitset.wait(time.time_ns())
        loop.tick(time.time_ns())
    assert tsk.result() == 0


def op_single() -> set[bytes]:
    async def op1(x: int):
        _ = await duron.event_loop.create_op(x)

    async def op():
        first = asyncio.create_task(op1(1))
        _ = await asyncio.gather(
            duron.event_loop.create_op(2),
            asyncio.create_task(op1(3)),
            duron.event_loop.create_op(4),
            asyncio.create_task(op1(5)),
        )
        await first
        _ = await duron.event_loop.create_op(6)

    ids: set[bytes] = set()
    loop = duron.event_loop.create_loop(b"tsk")
    loop.tick(time.time_ns())
    tsk = loop.create_task(op())

    @overload
    def tick(n: int, expect: set[int]) -> list[bytes]: ...
    @overload
    def tick(n: int, expect: None) -> None: ...
    def tick(n: int, expect: set[int] | None) -> list[bytes] | None:
        waitset = loop.poll_completion(tsk)
        if expect:
            assert (
                waitset
                and len(waitset.ops) == n
                and set(o.params for o in waitset.ops).issubset(expect)
            )
            ids.update(o.id for o in waitset.ops)
            return list(o.id for o in waitset.ops)
        else:
            assert waitset is None
            return None

    for i in range(5, 0, -1):
        ws = tick(i, {1, 2, 3, 4, 5})
        loop.post_completion_threadsafe(ws[random.randint(0, i - 1)], result=i - 1)
    ws = tick(1, {6})
    loop.post_completion_threadsafe(ws[0], result=6)
    tick(0, None)

    return ids


def test_op():
    BASELINE = {
        b"\x89U\x82\xd9\xe9\xa1\x01\x0fb\xab}\xba",
        b"\xa2VE\xcb\xf3\x81\x82\xe75@\xe9\xdf",
        b"\xb2\x1d\xb3\xd5c\xe3J\no\xa6U\x18",
        b"\xe7y\tDQ-\xe8\xfb\x9dBX\xde",
        b"\t\xf9?\xf8kJ\xd4\ry8\xf2\xae",
        b"\xc6\xbbEu\xdf\xd1uc\xf5M\x11'",
    }
    for _ in range(4):
        assert op_single() == BASELINE
