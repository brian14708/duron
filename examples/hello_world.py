from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import duron
from duron.storage import SQLiteStore


@duron.effect
async def greeting(ctx: duron.EffectContext, name: str) -> str:
    # External services should receive this key with the request.
    print(f"effect idempotency key: {ctx.idempotency_key}")
    await asyncio.sleep(0.1)
    return f"Hello, {name}!"


@duron.workflow(requires=(greeting,))
async def hello(ctx: duron.WorkflowContext, name: str) -> str:
    return await ctx.call(greeting, name)


async def run_workflow(name: str, database: Path, *, run_id: str) -> str:
    store = SQLiteStore(database)
    return await duron.run(hello(name), store, run_id=run_id)


def main() -> None:
    database = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("hello.db")
    print(asyncio.run(run_workflow("Alice", database, run_id="hello-alice")))


if __name__ == "__main__":
    main()
