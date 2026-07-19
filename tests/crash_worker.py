"""Subprocess used to test recovery without graceful shutdown."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import patch

import duron
from duron.storage import LeaseToken, SQLiteStore


class CrashPorts(duron.PortSet):
    ready = duron.Output[int]()
    resume = duron.Input[int]()


@duron.effect(name="crash.work")
async def work(_ctx: duron.EffectContext, directory: str) -> int:
    with (Path(directory) / "calls").open("a") as file:
        file.write("effect\n")
    return 7


@duron.workflow(name="crash.flow", ports=CrashPorts)
async def flow(ctx: duron.WorkflowContext, directory: str) -> int:
    value = await ctx.call(work, directory)
    await ctx.port(CrashPorts.ready).emit(value)
    return value + await ctx.port(CrashPorts.resume).receive()


async def main(directory: str) -> None:
    original = SQLiteStore.claim
    claims: dict[str, LeaseToken] = {}

    async def claim(
        self: SQLiteStore, run_id: str, request_id: str, *, ttl: float = 30
    ) -> LeaseToken:
        token = await original(self, run_id, request_id, ttl=ttl)
        claims[run_id] = token
        return token

    with patch.object(SQLiteStore, "claim", claim):
        async with (
            SQLiteStore(Path(directory) / "crash.db") as store,
            duron.SQLiteScheduler(
                Path(directory) / "crash.db", workflows=[flow]
            ) as scheduler,
        ):
            run = await scheduler.submit(flow, "crash", directory)
            assert await anext(run.port(CrashPorts.ready).reader()) == 7
            # Shorten the run lease only once the effect and output are durable.
            key = json.dumps(["run", "default", "crash"], separators=(",", ":"))
            await store.renew(key, claims[key], ttl=0.2)
            sys.stdout.write("ready\n")
            sys.stdout.flush()
            await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
