---
hide:
  - toc
  - navigation
---

# Duron

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
    async with duron.Session(FileLogStorage(Path("log.jsonl"))) as job:
        result = await job.start(greeting_flow, "Alice").result()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```
