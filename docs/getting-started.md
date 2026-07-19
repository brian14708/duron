# Getting started

Install Duron with `uv pip install duron` in Python 3.11 or later. Run the
complete [README example](index.md), or from this repository:

```bash
uv run python examples/hello_world.py hello.db
```

The first invocation prints its effect's greeting. Reusing the database and run
ID returns the committed result without performing the effect again.

## Define workflows and effects

A workflow coordinates durable operations through `WorkflowContext`. Effects
perform external I/O through ordinary async Python. Annotate their first
parameter as `WorkflowContext` or `EffectContext`, and annotate arguments and
results. Call effects with `await ctx.call(effect, ...)`.

```python
import duron


@duron.effect
async def greet(_ctx: duron.EffectContext, name: str) -> str:
    return f"Hello, {name}!"


@duron.workflow(name="greetings.hello", version="0")
async def hello(ctx: duron.WorkflowContext, name: str) -> str:
    return await ctx.call(greet, name)


async def example() -> str:
    async with duron.SQLiteScheduler("app.db", workflows=[hello]) as scheduler:
        run = await scheduler.submit(hello, "hello-alice", "Alice")
        return await run.result()
```

The scheduler creates its schema, persists invocation envelopes, and starts
execution automatically. Use `":memory:"` for transient tests. Eligible runs
execute concurrently as asyncio tasks, without a scheduler concurrency limit.
Idle runs waiting for inputs, durable sleeps, or input
selections unload and release their history claims. Deliveries and deadlines
resume them through replay. Runs with active effects remain loaded.
The default idle grace period is 60 seconds; configure `idle_timeout` on the
scheduler when a shorter or longer period is appropriate.

## Observe and cancel

`submit(workflow_or_ref, run_id, ...)` snapshots and durably accepts normalized
arguments. Retrying the same invocation returns the same run. Reusing its ID
with different arguments or a different reference raises
`SubmissionConflictError`. Use a new ID for a new business operation.

`await scheduler.open(hello, "hello-alice")` observes an existing invocation
and checks the contract. It does not start a new invocation or transfer
ownership. Unknown IDs raise `RunNotFoundError`.

Run handles provide `result()`, `status()`, `cancel()`, and typed `port(...)`.
Dropping a handle or cancelling a result waiter leaves execution running.
`cancel()` accepts an idempotent cancellation request; completion can still win
the race. A cancelled workflow's result raises `RunCancelledError`.

Closing the scheduler stops its local execution attempts and cleans up effects.
It does not cancel durable jobs. A later scheduler on the same database
recovers incomplete work when started with `workflows=[hello]`. Register
implementations through `workflows` for recovery on startup. Local submission of
a workflow also registers its implementation; submitting a reference and opening
a handle do not register code. Multiple schedulers cannot
steal an unexpired
history claim. Run IDs belong to the configured `namespace`.

## Interactive workflows

Declare optional ports on a `PortSet`:

```python
class Ports(duron.PortSet):
    items = duron.Input[str]()
    events = duron.Output[str]()


@duron.workflow(ports=Ports)
async def process(ctx: duron.WorkflowContext) -> str:
    item = await ctx.port(Ports.items).receive()
    await ctx.port(Ports.events).emit(item)
    return item
```

Applications send through `run.port(Ports.items)` and read
`run.port(Ports.events)`. Sends succeed after the current execution owner
commits acceptance to history. Output cursors resume after reconnection.

Effects have optional ports too. Use `async with ctx.start(effect) as job`,
interact through `job.port(...)`, and `await job`. `ctx.select(...)` chooses among
named inputs and awaitables while preserving losing input messages.

## Contracts and recovery

`Workflow[P, T, Ports]` contains executable code; `workflow.ref` is an
executable-free `WorkflowRef[P, T, Ports]`. Producers can declare shared
references with `@duron.workflow_ref(name=..., version=..., ports=...)` on an
annotated contract stub. The stub body is discarded. Workers explicitly
register implementations; references never trigger automatic Python imports.

Names, versions, argument and result schema identities, port directions and
schemas, and codec identity form the contract. Unknown implementations remain
blocked until a compatible worker is registered. Mismatches are explicit errors.
Use `codec_id` for a configured codec's wire contract and new workflow versions
for incompatible changes.

Keep workflow control flow deterministic. Use `ctx.now()`, `ctx.random()`, and
`ctx.sleep()` for replay-safe time, randomness, and deadlines. Committed effect
results replay; uncommitted external work can run again. Use stable application
operation IDs to deduplicate external mutations.

Unsupported database schemas and declared history versions are rejected without
migration or replacement. The current history format is version 0.

See [workflow ports](guides/host-ports.md), [effect runs](guides/effect-runs.md),
[recovery](guides/recovery.md), and [storage](reference/storage.md).
