from __future__ import annotations

import asyncio
import logging
import random
import sys
from pathlib import Path

import duron
from duron.contrib.storage import FileLogStorage
from duron.tracing import Tracer, setup_tracing

logger = logging.getLogger(__name__)


@duron.op
async def work(name: str) -> str:
    print("⚡ Preparing to greet...")
    await asyncio.sleep(2)
    print("⚡ Greeting...")
    return f"Hello, {name}!"


@duron.op
async def generate_lucky_number() -> int:
    print("⚡ Generating lucky number...")
    await asyncio.sleep(1)
    logger.warning("Generating a random lucky number between 1 and 100.")
    print("⚡ Lucky number generated.")
    return random.randint(1, 100)


@duron.fn
async def greeting_flow(ctx: duron.Context, name: str) -> str:
    message, lucky_number = await asyncio.gather(
        ctx.run(work, name), ctx.run(generate_lucky_number)
    )
    return f"{message} Your lucky number is {lucky_number}."


async def run_workflow(name: str, log_file: Path) -> str:
    log = FileLogStorage(log_file)
    async with (
        greeting_flow.invoke(log, tracer=Tracer("1" * 32)) as job,
    ):
        await job.start(name)
        return await job.wait()


def main() -> None:
    setup_tracing()
    result = asyncio.run(run_workflow("Alice", Path(sys.argv[1])))
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
