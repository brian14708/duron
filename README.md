# Duron

[![CI](https://github.com/brian14708/duron/actions/workflows/ci.yaml/badge.svg)](https://github.com/brian14708/duron/actions/workflows/ci.yaml)
[![PyPI - Version](https://img.shields.io/pypi/v/duron)](https://pypi.org/project/duron)
[![Python Versions](https://img.shields.io/pypi/pyversions/duron)](https://pypi.org/project/duron)
[![License](https://img.shields.io/github/license/brian14708/duron.svg)](https://github.com/brian14708/duron/blob/main/LICENSE)

**Durable workflows for modern Python.** Build resilient async applications with native support for streaming and interruption.

- 💬 **Interactive workflows** — AI agents, chatbots, and human-in-the-loop automation with bidirectional streaming
- ⚡ **Crash recovery** — Deterministic replay from append-only logs means workflows survive restarts
- 🎯 **Graceful interruption** — Cancel or redirect operations mid-execution with signals
- 🔌 **Zero dependencies** — Pure Python built on asyncio, fully typed
- 🧩 **Pluggable storage** — Bring your own database or filesystem backend

## Install

Duron requires **Python 3.10+**.

```bash
uv pip install duron
```

## Quickstart

```python
# /// script
# dependencies = ["duron"]
# ///

import asyncio
from pathlib import Path
import duron
from duron.contrib.storage import FileLogStorage


@duron.effect
async def check_fraud(amount: float, recipient: str) -> float:
    print("Executing risk check...")
    await asyncio.sleep(0.5)
    return 0.85  # High risk score


@duron.effect
async def execute_transfer(amount: float, recipient: str) -> str:
    print("Executing transfer...")
    await asyncio.sleep(1)
    return f"Transferred ${amount} to {recipient}"


@duron.durable
async def transfer_workflow(
    ctx: duron.Context,
    amount: float,
    recipient: str,
    events: duron.StreamWriter[list[str]] = duron.Provided,
) -> str:
    async with events:
        # report progress through events stream
        await events.send(["log", f"Checking transfer: ${amount} → {recipient}"])
        # durable function calls
        risk = await ctx.run(check_fraud, amount, recipient)

        if risk > 0.8:
            approval_id, approval = await ctx.create_future(bool)
            await events.send(["log", "⚠️  High risk - approval required"])
            await events.send(["approval", approval_id])

            if not await approval:
                await events.send(["log", "❌ Transfer rejected by manager"])
                return "Transfer rejected"

        result = await ctx.run(execute_transfer, amount, recipient)
        await events.send(["log", f"✓ {result}"])
        return result


async def main():
    async with duron.Session(FileLogStorage(Path("transfer.jsonl"))) as session:
        task = await session.start(transfer_workflow, 10000.0, "suspicious-account")
        stream = await task.open_stream("events", "r")

        async def handle_events():
            async for event_type, data in stream:
                if event_type == "log":
                    print(data)
                elif event_type == "approval":
                    if task.is_future_pending(data): # if not pending means it was a rerun
                        decision = await asyncio.to_thread(input, "Approve? (y/n): ")
                        await task.complete_future(
                            data, result=(decision.lower() == "y")
                        )

        await asyncio.gather(task.result(), handle_events())


if __name__ == "__main__":
    asyncio.run(main())
```

## Next steps

- Read the [getting started guide](https://brian14708.github.io/duron/getting-started/)
- Explore a more advanced example with streams and signals: [examples/agent.py](https://github.com/brian14708/duron/blob/main/examples/agent.py)
