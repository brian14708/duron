from __future__ import annotations

import asyncio
import logging
import random
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import duron
from duron.contrib.storage import FileStorage
from duron.tracing import Tracer, setup_tracing

logger = logging.getLogger(__name__)

progress = duron.Output[int]("progress")


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


@duron.effect
async def count_up(target: int) -> AsyncIterator[int]:
    curr = 0
    await asyncio.sleep(0.5)
    while curr < target:
        curr += 10
        yield curr
        await asyncio.sleep(0.05)


@duron.workflow
async def greeting_flow(ctx: duron.WorkflowContext, name: str) -> str:
    message, lucky_number = await asyncio.gather(
        ctx.call(work, name), ctx.call(generate_lucky_number)
    )
    async with ctx.stream(count_up, lucky_number) as counting:
        async for value in counting:
            await ctx.emit(progress, value)
    return f"{message} Your lucky number is {lucky_number}."


async def run_workflow(name: str, log_file: Path) -> str:
    storage = FileStorage(log_file)
    return await duron.run(greeting_flow(name), storage, tracer=Tracer("1" * 32))


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
