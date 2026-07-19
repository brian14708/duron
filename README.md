# Duron

[![CI](https://github.com/brian14708/duron/actions/workflows/ci.yaml/badge.svg)](https://github.com/brian14708/duron/actions/workflows/ci.yaml)
[![PyPI - Version](https://img.shields.io/pypi/v/duron)](https://pypi.org/project/duron)

Duron runs durable async Python workflows. A workflow coordinates work through
Duron's context; effects perform external I/O and other nondeterministic work.
Duron records operations and their outcomes so a workflow can replay them after
a restart and continue unfinished work.

Requires Python 3.11 or later. Tested on CPython 3.11 through 3.15.

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

Run this example again with the same database to read the recorded greeting
without calling the effect again. The run ID, `"hello-alice"`, identifies one
invocation: submitting the same workflow and arguments with that ID returns
the existing run.

## Restart and recover

Register workflow implementations with `workflows=[hello]` so the scheduler can
recover unfinished runs when it opens the database. You do not need to submit
those runs again.

`SQLiteScheduler` owns its database file exclusively. Use one active scheduler
per file; close it before another process opens the same database. After a
crash, a new scheduler can recover immediately.

Completed effects replay their recorded outcomes. If an effect's outcome was
not recorded before a failure, Duron blocks the run rather than invoking it
again. Use application operation IDs to reconcile uncertain external changes
such as payments before starting a new run.

## Interact with running workflows

Typed input and output ports let applications interact with workflows and
effects. Output ports support `async for` and resumable readers.
See the [getting started guide](https://brian14708.github.io/duron/getting-started/)
and [documentation](https://brian14708.github.io/duron/) for details.

## Develop locally

From a repository checkout:

```bash
uv sync
uv run pytest
uv run ruff check
uv run ruff format --check
uv run pyright --verifytypes duron --ignoreexternal
uv run --group docs zensical build
```

Preview the documentation with `uv run --group docs zensical serve`.
