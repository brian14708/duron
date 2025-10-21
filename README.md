# 🌀 Duron

[![PyPI - Version](https://img.shields.io/pypi/v/duron)](https://pypi.org/project/duron)

Duron is a Python library that makes async work _replayable_. You can pause, resume, or rerun async functions without redoing completed steps. Wrap your side effects once, keep orchestration deterministic, and Duron logs every result so repeated runs stay safe.

## Why Duron?

- 🔁 **Restart-safe** — Rerun a job and Duron replays prior results automatically. No duplicate work.
- 🧵 **Async-first** — Write `async def` functions and orchestrate them with familiar `await` syntax.
- 🔍 **Typed & traceable** — Decorators capture type hints so inputs and outputs serialize cleanly.
- 🗄️ **Storage-agnostic** — Start with file-based logging or plug in your own backend to match your stack.
- 🚀 **Drop-in ready** — Works in CLI tools, web backends, or long-lived agents—no special runtime or extra dependencies required.

## Install

Duron requires **Python 3.10+**.

```bash
pip install duron
```

## Quickstart

Duron defines two kinds of functions:

- `@duron.durable` — deterministic orchestration. It replays from logs, ensuring that control flow only advances when every prior step is known.
- `@duron.effect` — side effects. Wrap anything that touches the outside world (APIs, databases, file I/O). Duron records its return value so it runs once per unique input.

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
    message, lucky_number = await asyncio.gather(
        ctx.run(work, name), ctx.run(generate_lucky_number)
    )
    return f"{message} Your lucky number is {lucky_number}."


async def main():
    async with duron.invoke(greeting_flow, FileLogStorage(Path("log.jsonl"))) as job:
        await job.start("Alice")
        result = await job.wait()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

To see Duron handle _live input_ and _external events_, check out [`examples/agent.py`](./examples/agent.py).
