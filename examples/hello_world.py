from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path

import duron
from duron.contrib.storage import FileLogStorage


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
    print("⚡ Lucky number generated.")
    return random.randint(1, 100)


@duron.fn
async def greeting_flow(ctx: duron.Context, name: str) -> str:
    message, lucky_number = await asyncio.gather(
        ctx.run(work, None, name), ctx.run(generate_lucky_number)
    )
    return f"{message} Your lucky number is {lucky_number}."


async def run_workflow(name: str, log_file: Path) -> str:
    async with greeting_flow.invoke(FileLogStorage(log_file)) as job:
        await job.start(name)
        return await job.wait()


def main() -> None:
    result = asyncio.run(run_workflow("Alice", Path(sys.argv[1])))
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
