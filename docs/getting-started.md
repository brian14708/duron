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

- **Always take [`Context`][duron.Context] as the first parameter** - This is your handle to run effects and create streams/signals
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

Duron records each effect's return value so it runs **once per unique input**, even across restarts.

```python
@duron.effect
async def fetch_data(url: str) -> dict:
    # This will only execute once per unique URL
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

    # Invoke the workflow
    async with duron.invoke(greeting_flow, storage) as job:
        await job.start("Alice")
        result = await job.wait()

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

Duron is storage-agnostic. It ships with two built-in options:

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

### Custom Storage

Implement the `LogStorage` protocol for your own backend:

```python
from duron.log import LogStorage

class MyStorage(LogStorage):
    async def read_log(self, lease_id: str) -> list[Entry]:
        # Read from your storage (database, S3, etc.)
        ...

    async def append_log(self, lease_id: str, entries: list[Entry]) -> None:
        # Append to your storage
        ...

    # ... implement other methods
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
    for i in range(5):
        await output.send(f"Message {i}")
        await asyncio.sleep(1)

async def main():
    async with duron.invoke(producer, storage) as job:
        stream: Stream[str] = job.open_stream("output", "r")

        await job.start()

        async with stream as s:
            async for message in s:
                print(f"Received: {message}")

        await job.wait()
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
    async with duron.invoke(interruptible_task, storage) as job:
        signal_writer = job.open_stream("signal", "w")

        await job.start()

        # Later... send interrupt signal
        await signal_writer.send(None)

        result = await job.wait()
        print(result)  # "Interrupted by user"
```

### Tracing

Enable tracing to understand workflow execution:

```python
from duron.tracing import Tracer, setup_tracing

async def main():
    setup_tracing()  # Configure logging

    async with duron.invoke(
        greeting_flow,
        storage,
        tracer=Tracer("session-123")
    ) as job:
        await job.start("Alice")
        result = await job.wait()
```

Traces are logged to your storage backend for analysis. Upload the jsonl to [Trace UI](https://brian14708.github.io/duron/trace-ui/) for visualization.
