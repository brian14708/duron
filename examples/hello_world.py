from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import duron


@duron.effect
async def greeting(_ctx: duron.EffectContext, name: str) -> str:
    print(f"greeting {name}")
    await asyncio.sleep(0.1)
    return f"Hello, {name}!"


@duron.workflow
async def hello(ctx: duron.WorkflowContext, name: str) -> str:
    return await ctx.call(greeting, name)


async def run_workflow(name: str, database: Path, *, run_id: str) -> str:
    async with duron.SQLiteScheduler(database, workflows=[hello]) as scheduler:
        run = await scheduler.submit(hello, run_id, name)
        return await run.result()


def main() -> None:
    database = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("hello.db")
    print(asyncio.run(run_workflow("Alice", database, run_id="hello-alice")))


if __name__ == "__main__":
    main()
