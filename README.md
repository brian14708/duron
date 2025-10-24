# Duron

[![CI](https://github.com/brian14708/duron/actions/workflows/ci.yaml/badge.svg)](https://github.com/brian14708/duron/actions/workflows/ci.yaml)
[![PyPI - Version](https://img.shields.io/pypi/v/duron)](https://pypi.org/project/duron)
[![License](https://img.shields.io/github/license/brian14708/duron.svg)](https://github.com/brian14708/duron/blob/main/LICENSE)

Duron is a lightweight durable execution runtime for Python async workflows. It provides replayable execution primitives that work standalone or as building blocks for complex workflow engines.

## Why Duron?

- 🪶 **Zero extra deps** — Lightweight library that layers on top of asyncio; add Duron without bloating your stack.
- 🧩 **Pluggable architecture** — Bring your own storage or infra components and swap them without changing orchestration code.
- 🔄 **Streams & signals** — Model long-running conversations, live data feeds, and feedback loops with built-in primitives.
- 🐍 **Python-native & typed** — Type hints make replay serialization predictable, and everything feels like idiomatic Python.
- 🔭 **Built-in tracing** — Detailed logs help you inspect replays and surface observability data wherever you need it.

## Install

Duron requires **Python 3.10+**.

```bash
uv pip install duron
```

## Quickstart

Duron wraps async orchestration (`@duron.durable`) and effectful steps (`@duron.effect`) so complex workflows stay deterministic—even when they touch the outside world.

```python
import asyncio
import random
from pathlib import Path

import duron
from duron.contrib.storage import FileLogStorage


# Effects encapsulate side effects (I/O, randomness, API calls)
@duron.effect
async def work(name: str) -> str:
    print("⚡ Preparing to greet...")
    await asyncio.sleep(2)
    print("⚡ Greeting...")
    return f"Hello, {name}!"


@duron.effect
async def generate_lucky_number() -> int:
    print("⚡ Generating lucky number...")
    await asyncio.sleep(1)
    return random.randint(1, 100)


# Durable functions orchestrate workflow logic via ctx.run()
# They're deterministically replayed from logs on resume
@duron.durable
async def greeting_flow(ctx: duron.Context, name: str) -> str:
    # Run effects concurrently - results are logged for replay
    message, lucky_number = await asyncio.gather(
        ctx.run(work, name),
        ctx.run(generate_lucky_number),
    )
    return f"{message} Your lucky number is {lucky_number}."


async def main():
    # Session manages execution and log storage
    async with duron.Session(FileLogStorage(Path("log.jsonl"))) as session:
        # Starts new workflow or resumes from existing log
        task = await session.start(greeting_flow, "Alice")
        result = await task.result()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

## Next steps

- Read the [getting started guide](https://brian14708.github.io/duron/getting-started/)
- Explore a more advanced example with streams and signals: [examples/agent.py](https://github.com/brian14708/duron/blob/main/examples/agent.py)
