from __future__ import annotations

import asyncio
import logging
import random
import sys
from collections.abc import AsyncGenerator
from pathlib import Path

import duron
from duron.contrib.storage import FileLogStorage
from duron.tracing import Tracer, setup_tracing

logger = logging.getLogger(__name__)


@duron.effect
async def work(name: str) -> str:
    print("⚡ Preparing to greet...")
    await asyncio.sleep(2)
    print("⚡ Greeting...")
    return f"Hello, {name}!"


@duron.effect
async def generate_lucky_number() -> int:
    logger.info("⚡ Generating lucky number...")
    await asyncio.sleep(1)
    logger.info("⚡ Lucky number generated.")
    return random.randint(1, 100)


@duron.effect(stateful=True, initial=lambda: 0, reducer=int.__add__)
async def count_up(count: int, target: int) -> AsyncGenerator[int, int]:
    await asyncio.sleep(0.5)
    while count < target:
        count = yield 10
        logger.info("⚡ Current count: %s", count)
        await asyncio.sleep(0.05)


@duron.durable
async def greeting_flow(ctx: duron.Context, name: str) -> str:
    message, lucky_number = await asyncio.gather(
        ctx.run(work, name), ctx.run(generate_lucky_number)
    )
    _ = await ctx.run(count_up, lucky_number)
    return f"{message} Your lucky number is {lucky_number}."


async def run_workflow(name: str, log_file: Path) -> str:
    log = FileLogStorage(log_file)
    async with duron.Session(log, tracer=Tracer("1" * 32)) as job:
        return await job.start(greeting_flow, name).result()


def main() -> None:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    setup_tracing()

    result = asyncio.run(run_workflow("Alice", Path(sys.argv[1])))
    logger.info("Result: %s", result)


if __name__ == "__main__":
    main()
