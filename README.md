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
from typing import Optional, TypedDict
import duron
from duron.contrib.storage import FileLogStorage


class Event(TypedDict):
    """
    Event type for communicating workflow progress and approvals.

    Fields:
        message: Log or status message for the user.
        approval_id: Durable future ID to request approval (None for normal logs).
    """

    message: str
    approval_id: Optional[str]


# -----------------------
# Effect definitions
# -----------------------


@duron.effect
async def check_fraud(amount: float, recipient: str) -> float:
    """Simulate a risk engine returning a fraud probability."""
    print("Executing risk check...")
    await asyncio.sleep(0.5)
    return 0.85


@duron.effect
async def execute_transfer(amount: float, recipient: str) -> str:
    """Simulate a real transfer execution."""
    print("Executing transfer...")
    await asyncio.sleep(1)
    return f"Transferred ${amount} to {recipient}"


# -----------------------
# Durable workflow
# -----------------------


@duron.durable
async def transfer_workflow(
    ctx: duron.Context,
    amount: float,
    recipient: str,
    events: duron.StreamWriter[Event] = duron.Provided,
) -> str:
    """
    Durable workflow to execute a transfer with fraud detection
    and optional manager approval.
    """
    async with events:
        # Log start of transfer
        await events.send({
            "message": f"Checking transfer: ${amount} → {recipient}",
            "approval_id": None,
        })

        # Step 1: Fraud check
        risk = await ctx.run(check_fraud, amount, recipient)

        # Step 2: Approval required if high risk
        if risk > 0.8:
            approval_id, approval = await ctx.create_future(bool)
            await events.send({
                "message": "⚠️  High risk - approval required",
                "approval_id": approval_id,
            })

            if not await approval:
                await events.send({
                    "message": "❌ Transfer rejected by manager",
                    "approval_id": None,
                })
                return "Transfer rejected"

        # Step 3: Execute transfer
        result = await ctx.run(execute_transfer, amount, recipient)
        await events.send({"message": f"✓ {result}", "approval_id": None})
        return result


# -----------------------
# Host process
# -----------------------


async def main():
    """
    Run the workflow locally with file-based state storage.
    """
    async with duron.Session(FileLogStorage(Path("transfer.jsonl"))) as session:
        task = await session.start(transfer_workflow, 10000.0, "suspicious-account")
        stream = await task.open_stream("events", "r")

        async def handle_events():
            async for event in stream:
                # Always print message
                print(event["message"])

                # If approval_id is present, prompt for manager decision
                # If the future is not pending, it means it was already resolved (e.g., workflow resumed)
                if event["approval_id"] and task.is_future_pending(
                    event["approval_id"]
                ):
                    decision = await asyncio.to_thread(input, "Approve? (y/n): ")
                    await task.complete_future(
                        event["approval_id"], result=(decision.lower() == "y")
                    )

        await asyncio.gather(task.result(), handle_events())


if __name__ == "__main__":
    asyncio.run(main())
```

## Next steps

- Read the [getting started guide](https://brian14708.github.io/duron/getting-started/)
- Explore a more advanced example with streams and signals: [examples/agent.py](https://github.com/brian14708/duron/blob/main/examples/agent.py)
