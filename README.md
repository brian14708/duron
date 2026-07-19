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

Duron replays completed operations from their persisted log position; it does not
provide content-based memoization. External effects have at-least-once semantics:
use idempotency keys or transactions for side effects that must not be duplicated.

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
from duron.contrib.storage import FileStorage

# -----------------------
# Typed ports (shared by workflow and host code)
# -----------------------

events = duron.Output[str]("events")
approval = duron.Request[str, bool]("approval")


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
async def execute_transfer(amount: float, recipient: str, *, idempotency_key: str) -> str:
    """Simulate a real transfer execution."""
    print("Executing transfer...")
    await asyncio.sleep(1)
    return f"Transferred ${amount} to {recipient}"


# -----------------------
# Durable workflow
# -----------------------


@duron.workflow
async def transfer_workflow(
    ctx: duron.WorkflowContext, amount: float, recipient: str
) -> str:
    """Execute a transfer with fraud detection and optional manager approval."""
    await ctx.emit(events, f"Checking transfer: ${amount} → {recipient}")

    risk = await ctx.call(check_fraud, amount, recipient)

    if risk > 0.8:
        approved = await ctx.request(approval, f"Approve ${amount} → {recipient}?")
        if not approved:
            await ctx.emit(events, "❌ Transfer rejected by manager")
            return "Transfer rejected"

    result = await ctx.call(
        execute_transfer, amount, recipient, idempotency_key=ctx.idempotency_key
    )
    await ctx.emit(events, f"✓ {result}")
    return result


# -----------------------
# Host process
# -----------------------


async def main():
    """Run the workflow locally with file-based state storage."""
    storage = FileStorage(Path("transfer.jsonl"))

    async def handle_approval(prompt: str) -> bool:
        decision = await asyncio.to_thread(input, f"{prompt} (y/n): ")
        return decision.lower() == "y"

    async def show_events(entries):
        async for _offset, event in entries:
            print(event)

    invocation = (
        transfer_workflow(10000.0, "suspicious-account")
        .output(events, show_events)
        .serve(approval, handle_approval)
    )
    result = await duron.run(invocation, storage)
    print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
```

Duron also provides `duron.contrib.storage.MemoryStorage` for tests and
`duron.contrib.storage.SQLiteStorage` for a single-run SQLite database. Tests use the
same invocation wiring and `duron.run` entry point. Use its direct await form for
ordinary execution, or its async context form for imperative port coordination.
Each storage backend holds exactly one run; `duron.run` holds an opaque fencing
lease while it owns that run's execution.

## Next steps

- Read the [getting started guide](https://brian14708.github.io/duron/getting-started/)
- Explore a more advanced example with streams and signals: [examples/agent.py](https://github.com/brian14708/duron/blob/main/examples/agent.py)
