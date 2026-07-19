# Duron

[![CI](https://github.com/brian14708/duron/actions/workflows/ci.yaml/badge.svg)](https://github.com/brian14708/duron/actions/workflows/ci.yaml)
[![PyPI - Version](https://img.shields.io/pypi/v/duron)](https://pypi.org/project/duron)

Duron runs durable async Python workflows. It records workflow operations in an
append-only history, then replays that history after a restart. Effects perform
work outside the replayed workflow.

Duron requires Python 3.11 or later.

Your application owns the process that runs a workflow and reopens it after a
restart. Duron provides replay and persistence; it does not run a background
scheduler or start replacement workers.

```bash
uv pip install duron
```

## Run a workflow

```python
import asyncio

import duron
from duron.storage import SQLiteStore


@duron.effect
async def greet(ctx: duron.EffectContext, name: str) -> str:
    # Pass this key to an external service with its request.
    print(f"external idempotency key: {ctx.idempotency_key}")
    await asyncio.sleep(0.1)
    return f"Hello, {name}!"


@duron.workflow(requires=(greet,))
async def hello(ctx: duron.WorkflowContext, name: str) -> str:
    return await ctx.call(greet, name)


async def main() -> None:
    store = SQLiteStore("duron.db")
    print(await duron.run(hello("Alice"), store, run_id="hello-alice"))


asyncio.run(main())
```

Calling `hello(...)` binds its inputs without starting execution. The bound
call is single-use; execution starts when `duron.run(...)` is awaited. Reusing
`run_id` with the same workflow identity and inputs resumes its history.
Use a new `run_id` to start a new run.

Effects have at-least-once semantics. A process can stop after an external
system commits a change but before Duron records the result. Pass
`ctx.idempotency_key` to external APIs that support idempotency so they can
deduplicate repeated calls. For example, a payment client call inside an effect
can use
`await payments.charge(account, cents, idempotency_key=ctx.idempotency_key)`.

List effects and ports in `@duron.workflow(requires=(...))` to declare the
workflow's external dependencies. Using an undeclared object emits a Python
warning. The declaration is inspectable as `hello.requires`; it does not check
service availability or change replay history.

See the [getting started guide](https://brian14708.github.io/duron/getting-started/)
and [hello world example](https://github.com/brian14708/duron/blob/main/examples/hello_world.py).

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
