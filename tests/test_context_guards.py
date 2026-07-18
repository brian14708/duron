from __future__ import annotations

import subprocess  # noqa: S404
import sys


def test_context_loop_guard_survives_optimized_mode() -> None:
    code = """
import asyncio
from duron._core.context import Context
from duron.loop import create_loop

async def main():
    loop = await create_loop()
    ctx = Context(loop, "seed")
    try:
        ctx.random()
    except RuntimeError:
        loop.close()
        return
    raise AssertionError("cross-loop Context use did not raise")

asyncio.run(main())
"""
    subprocess.run([sys.executable, "-O", "-c", code], check=True, shell=False)  # noqa: S603
