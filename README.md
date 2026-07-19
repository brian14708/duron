# Duron

[![CI](https://github.com/brian14708/duron/actions/workflows/ci.yaml/badge.svg)](https://github.com/brian14708/duron/actions/workflows/ci.yaml)
[![PyPI - Version](https://img.shields.io/pypi/v/duron)](https://pypi.org/project/duron)

Duron runs durable async Python workflows. It records operations and replays
them after a restart to recover unfinished work. Effects perform external or
nondeterministic work outside replay.

Requires Python 3.11 or later.

```bash
uv pip install duron
```

## Run a workflow

```python
import asyncio

import duron


@duron.effect
async def greet(_ctx: duron.EffectContext, name: str) -> str:
    print(f"greeting {name}")
    await asyncio.sleep(0.1)
    return f"Hello, {name}!"


@duron.workflow
async def hello(ctx: duron.WorkflowContext, name: str) -> str:
    return await ctx.call(greet, name)


async def main() -> None:
    async with duron.SQLiteScheduler("duron.db", workflows=[hello]) as scheduler:
        run = await scheduler.submit(hello, "hello-alice", "Alice")
        print(await run.result())


asyncio.run(main())
```

Submitting the same workflow and arguments with the same run ID returns the
existing run. Register implementations with `workflows=[hello]` when constructing
the scheduler. It also recovers unfinished runs without resubmitting them.

Effects may run more than once after a failure. Use application operation IDs
to deduplicate external changes such as payments.

Duron also supports custom event stores and typed input and output ports.
Output ports support `async for` and provide
`reader()` for resumable cursors.
See the [getting started guide](https://brian14708.github.io/duron/getting-started/)
and [documentation](https://brian14708.github.io/duron/) for details.

## Develop locally

From a repository checkout:

```bash
uv sync
uv run pytest
uv run ruff check
uv run ruff format --check
uv run --group docs zensical build
```

Preview the documentation with `uv run --group docs zensical serve`.
