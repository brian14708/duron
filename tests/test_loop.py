import asyncio
import contextlib
import random
import time
from typing_extensions import overload

import pytest

from duron._loop import create_loop  # noqa: PLC2701


@pytest.mark.asyncio
async def test_timer() -> None:
    async def timer() -> int:
        await asyncio.sleep(0.1)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                asyncio.wait_for(asyncio.sleep(10000), timeout=0.2),
                timeout=10000,
            )
        return 0

    loop = create_loop(asyncio.get_event_loop())
    loop.tick(time.time_ns() // 1000)
    tsk = loop.create_task(timer())
    while (waitset := loop.poll_completion(tsk)) is not None:
        await waitset.block(time.time_ns() // 1000)
        loop.tick(time.time_ns() // 1000)
    assert tsk.result() == 0


def op_single() -> set[bytes]:
    async def op1(x: int) -> None:
        _ = await loop.create_op(x)

    async def op() -> None:
        first = asyncio.create_task(op1(1))
        _ = await asyncio.gather(
            loop.create_op(2),
            asyncio.create_task(op1(3)),
            loop.create_op(4),
            asyncio.create_task(op1(5)),
        )
        await first
        _ = await loop.create_op(6)

    ids: set[bytes] = set()
    loop = create_loop(asyncio.get_event_loop())
    loop.tick(time.time_ns() // 1000)
    tsk = loop.create_task(op())

    @overload
    def tick(n: int, expect: set[int]) -> list[bytes]: ...
    @overload
    def tick(n: int, expect: None) -> None: ...
    def tick(n: int, expect: set[int] | None) -> list[bytes] | None:
        waitset = loop.poll_completion(tsk)
        if expect:
            assert waitset
            assert len(waitset.ops) == n
            assert {o.params for o in waitset.ops}.issubset(expect)
            ids.update(o.id for o in waitset.ops)
            return [o.id for o in waitset.ops]
        assert waitset is None
        return None

    for i in range(5, 0, -1):
        ws = tick(i, {1, 2, 3, 4, 5})
        loop.post_completion(ws[random.randint(0, i - 1)], result=i - 1)
    ws = tick(1, {6})
    loop.post_completion(ws[0], result=6)
    tick(0, None)

    return ids


@pytest.mark.asyncio
async def test_op() -> None:  # noqa: RUF029
    baseline = {
        b"\tD\xea@/\x91\xbd\xa7\x91\x9d\x16y",
        b"*M\x95\x15_\xafX\xa1\xa0\xd6\xf4S",
        b"\x947\x8er\xa1\xa2\xb3\x1c\xef8\xfe\xde",
        b"\xa8Cc_8\xdf\x99WFK9W",
        b"\xc33\xea\x8f]\x1c^\x8b]n\xa3r",
        b"\xc9\x9dA\x14\xb7*\xb9X\x99$D\x02",
    }
    for _ in range(4):
        assert op_single() == baseline
