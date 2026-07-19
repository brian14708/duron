"""Subprocess used to test recovery without graceful shutdown."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import duron


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


@duron.effect(name="crash.unfinished")
async def unfinished(_ctx: duron.EffectContext, directory: str) -> int:
    with (Path(directory) / "unfinished-calls").open("a") as file:
        file.write("effect\n")
    sys.stdout.write("started\n")
    sys.stdout.flush()
    await asyncio.Event().wait()
    return 7


@duron.workflow(name="crash.unfinished.flow")
async def unfinished_flow(ctx: duron.WorkflowContext, directory: str) -> int:
    return await ctx.call(unfinished, directory)


async def main(directory: str) -> None:
    async with duron.SQLiteScheduler(
        Path(directory) / "crash.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.submit(flow, "crash", directory)
        assert await anext(run.port(CrashPorts.ready).reader()) == 7
        sys.stdout.write("ready\n")
        sys.stdout.flush()
        await asyncio.Event().wait()


async def run_unfinished(directory: str) -> None:
    async with duron.SQLiteScheduler(
        Path(directory) / "crash.db", workflows=[unfinished_flow]
    ) as scheduler:
        await scheduler.submit(unfinished_flow, "unfinished", directory)
        await asyncio.Event().wait()


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[2] == "unfinished":
        asyncio.run(run_unfinished(sys.argv[1]))
    else:
        asyncio.run(main(sys.argv[1]))
