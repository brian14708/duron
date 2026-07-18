---
hide:
  - toc
  - navigation
---

# Getting Started

Duron is a Python library that makes async workflows replayable. You can pause, resume, or rerun async functions without redoing completed steps. This guide will walk you through the core concepts and get you building your first durable workflow.

## Installation

Duron requires **Python 3.10 or higher**.

Install via pip:

```bash
pip install duron
```

Or if you're using [uv](https://docs.astral.sh/uv/):

```bash
uv add duron
```

## Core Concepts

Duron introduces two fundamental building blocks for creating replayable workflows:

### 1. Durable Functions (`@duron.durable`)

Durable functions are the orchestrators of your workflow. They define the control flow and coordinate multiple operations. Key characteristics:

- **Always take [`Context`](reference/duron.md#duron.Context) as the first parameter** - This is your handle to run effects and create streams/signals
- **Deterministic** - The same inputs always produce the same execution path
- **Replayable** - When resumed, Duron replays logged results to restore state without re-executing completed steps
- **No side effects** - All I/O must go through effects

```python
@duron.durable
async def my_workflow(ctx: duron.Context, arg: str) -> str:
    # Orchestration logic here
    result = await ctx.run(some_effect, arg)
    return result
```

### 2. Effect Functions (`@duron.effect`)

Effects wrap any code that interacts with the outside world. This includes:

- API calls
- Database queries
- File I/O
- Random number generation
- Any non-deterministic operation

Duron records each effect result at its deterministic operation position. During
replay, a completed operation is not executed again. This is **not** content-based
memoization: calling the same effect at a different workflow position creates a
different operation.

```python
@duron.effect
async def fetch_data(url: str) -> dict:
    # A completed invocation at this workflow position is replayed from the log.
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()
```

## Your First Workflow

Let's build a simple greeting workflow that demonstrates the core concepts:

```python
import asyncio
import random
from pathlib import Path

import duron
from duron.contrib.storage import FileLogStorage


@duron.effect
async def work(name: str) -> str:
    print("⚡ Preparing to greet...")
    await asyncio.sleep(2)  # Simulate I/O
    print("⚡ Greeting...")
    return f"Hello, {name}!"


@duron.effect
async def generate_lucky_number() -> int:
    print("⚡ Generating lucky number...")
    await asyncio.sleep(1)  # Simulate I/O
    return random.randint(1, 100)


@duron.durable
async def greeting_flow(ctx: duron.Context, name: str) -> str:
    # Run both effects in parallel
    message, lucky_number = await asyncio.gather(
        ctx.run(work, name),
        ctx.run(generate_lucky_number)
    )
    return f"{message} Your lucky number is {lucky_number}."


async def main():
    # Create a file-based log storage
    storage = FileLogStorage(Path("log.jsonl"))

    async with duron.Session(storage) as session:
        task = await session.start(greeting_flow, "Alice")
        result = await task.result()

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `hello.py` and run it:

```bash
python hello.py
```

You'll see output like:

```
⚡ Preparing to greet...
⚡ Generating lucky number...
⚡ Greeting...
Hello, Alice! Your lucky number is 42.
```

## Understanding Replay

The magic of Duron is in its replay behavior. Run the same script again:

```bash
python hello.py
```

**Notice**: No "⚡" output! Duron replayed the results from `log.jsonl` without re-executing the effects. The workflow completes instantly, but produces the **exact same result**.

This is powerful for:

- **Crash recovery** - If your process crashes mid-workflow, resume from the last checkpoint
- **Development** - Test workflow logic without hitting external services repeatedly
- **Debugging** - Reproduce exact execution paths
- **Cost savings** - Don't re-run expensive API calls

### Forcing a Fresh Run

To start fresh, delete the log file:

```bash
rm log.jsonl
python hello.py
```

Now you'll see the effects execute again (and potentially get a different lucky number).

## Storage Backends

Duron is storage-agnostic. It ships with three built-in options:

### File Storage (Recommended for Development)

```python
from pathlib import Path
from duron.contrib.storage import FileLogStorage

storage = FileLogStorage(Path("logs/workflow.jsonl"))
```

Stores logs as JSON Lines in a file. Great for:

- Local development
- Single-machine workflows
- Debugging (logs are human-readable)

### Memory Storage (Testing Only)

```python
from duron.contrib.storage import MemoryLogStorage

storage = MemoryLogStorage()
```

Stores logs in memory. Use for:

- Unit tests
- Temporary workflows
- Benchmarking

**Note**: Memory storage is lost when the process exits.

### SQLite Storage

```python
from pathlib import Path
from duron.contrib.storage import SQLiteLogManager

manager = SQLiteLogManager(Path("logs/workflows.db"))
storage = await manager.create_log("workflow-instance-id")
```

SQLite stores multiple named workflow logs in one database and uses database-backed
lease tokens. A newer lease fences out an older token. Use a stable, unique task ID
for each persisted workflow instance.

### Custom Storage

Implement the `LogStorage` protocol for your own backend:

```python
from collections.abc import AsyncGenerator

from duron.log import BaseEntry, Entry

class MyStorage:
    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # Yield existing entries in increasing offset order.
        if False:
            yield (0, {})

    async def acquire_lease(self) -> bytes:
        # Return a new opaque fencing token.
        return b"token"

    async def release_lease(self, lease: bytes) -> None:
        # Release only if `lease` is still current.
        ...

    async def append(self, lease: bytes, entry: Entry) -> int:
        # Atomically validate the token, append the complete entry, and return
        # its monotonically increasing offset.
        return 0
```

## Advanced Features

### Streams

Streams allow workflows to produce and consume values over time. Perfect for:

- Multi-step agent interactions
- Progress reporting
- Event-driven workflows

```python
from duron import Provided, Stream, StreamWriter

@duron.durable
async def producer(
    ctx: duron.Context,
    output: StreamWriter[str] = Provided
) -> None:
    async with output as o:
        for i in range(5):
            await o.send(f"Message {i}")
            await asyncio.sleep(1)

async def main():
    async with duron.Session(storage) as session:
        task = await session.start(producer)
        stream: Stream[str] = await task.open_stream("output", "r")

        async for message in stream:
            print(f"Received: {message}")

        await task.result()
```

### Signals

Signals enable external interruption of long-running operations:

```python
from duron import Signal, SignalInterrupt, Provided

@duron.durable
async def interruptible_task(
    ctx: duron.Context,
    signal: Signal[None] = Provided
) -> str:
    try:
        async with signal:
            await ctx.run(long_running_effect)
            return "Completed"
    except SignalInterrupt:
        return "Interrupted by user"

async def main():
    async with duron.Session(storage) as session:
        task = await session.start(interruptible_task)
        signal_writer = await task.open_stream("signal", "w")

        # Later... send interrupt signal
        await signal_writer.send(None)

        result = await task.result()
        print(result)  # "Interrupted by user"
```

## Runtime Guarantees

- Effects have **at-least-once external side-effect semantics**. A process can perform
  an external action and crash before its result is durably appended. Effects that
  mutate external systems should therefore use idempotency keys or transactions.
- A writable session holds a storage lease. Built-in backends use opaque fencing
  tokens. Lease acquisition is immediate and last-acquirer-wins; stale tokens cannot
  append after reacquisition. File storage requires `fcntl` locking and is therefore
  unavailable on platforms that cannot provide that guarantee.
- A readonly session cannot start or resume live work. It may only verify a completed
  persisted history and never appends entries.
- File storage is intended for a local single-host workflow log. Memory storage is
  process-local and non-durable. SQLite supports multiple named logs in one database.

### Tracing

Enable tracing to understand workflow execution:

```python
from duron.tracing import Tracer, setup_tracing

async def main():
    setup_tracing()  # Configure logging

    async with duron.Session(
        storage,
        tracer=Tracer("session-123")
    ) as session:
        task = await session.start(greeting_flow, "Alice")
        result = await task.result()
```

Traces are logged to your storage backend for analysis. Upload the jsonl to [Trace UI](https://brian14708.github.io/duron/trace-ui/) for visualization.
