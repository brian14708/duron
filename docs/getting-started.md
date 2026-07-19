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


@duron.workflow(name="greetings.hello")
async def hello(ctx: duron.WorkflowContext, name: str) -> str:
    return await ctx.call(greet, name)


async def example() -> str:
    async with duron.SQLiteScheduler("app.db", workflows=[hello]) as scheduler:
        run = await scheduler.submit(hello, "hello-alice", "Alice")
        return await run.result()
```

Entering the scheduler context creates the database and starts execution.
`workflows=[hello]` registers the code needed to execute and recover runs.
Use `":memory:"` for a temporary database in tests.

## Submit and observe runs

A run ID identifies one business operation. `submit(workflow_or_ref, run_id, ...)`
saves the workflow contract and arguments, including defaults, before returning
a handle. Retrying the same submission returns the existing run. Reusing the ID
with different arguments or a different reference raises `SubmissionConflictError`.
Use a new ID to start a new operation.

To observe an existing run, call `await scheduler.open(hello, "hello-alice")`.
This checks its contract without creating another run. An unknown ID raises
`RunNotFoundError`.

Use the handle to observe or interact with the run:

| Operation | Purpose |
| --- | --- |
| `await run.result()` | Wait for the result or raise the run's error. |
| `await run.status()` | Read its current state and scheduling diagnostic. |
| `await run.cancel()` | Request cancellation. |
| `run.port(declaration)` | Send input or read output through a declared port. |

Dropping a handle or cancelling a task waiting on `result()` leaves the workflow
running. `cancel()` requests cancellation explicitly. Repeated requests are safe,
but completion can still win the race. A cancelled run's `result()` raises
`RunCancelledError`.

## Close and reopen the scheduler

Closing the scheduler stops local execution and waits for effect cleanup. It
does not request cancellation of persisted runs. To recover unfinished work,
open the same database with the workflow implementations registered. You do not
need to submit the runs again.

During recovery, completed effects return their recorded outcomes. An effect
call without a recorded outcome blocks the run and is never invoked again.
Reconcile any uncertain external changes before starting a new run. See
[recovery](guides/recovery.md) for details.

Only one active scheduler can own a database file, across all namespaces.
Close it before opening another; a second active owner raises `StorageError`.
Run IDs are unique within the configured `namespace`. Unsupported database
schemas are rejected without migration or replacement.

Runs execute concurrently without a scheduler concurrency limit. A run waiting
for input or a durable deadline can suspend after 60 idle seconds to free
resources. Input delivery, cancellation, or the deadline resumes it. Set
`idle_timeout` to change the grace period. Runs with active effects stay loaded.

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
`run.port(Ports.events)`. Sends return after durable enqueueing, without waiting
for workflow acceptance. Output cursors let readers resume after reconnection.

Effects have optional ports too. Use `async with ctx.start(effect) as job`,
interact through `job.port(...)`, and `await job`. `ctx.select(...)` chooses among
named inputs and awaitables while preserving losing input messages.

## Workflow references and contracts

`Workflow[P, T, Ports]` contains executable code; `workflow.ref` is an
executable-free `WorkflowRef[P, T, Ports]`. Producers can declare shared
references with `@duron.workflow_ref(name=..., ports=...)` on an
annotated contract stub. The stub body is discarded. The scheduler must have an
implementation registered to execute the run; references never trigger automatic
Python imports.

Use `workflow.ref` when the executable definition is available.

Submitting or opening an executable workflow registers its code locally if no
implementation is registered under that name. Passing a reference does not.

A reference describes the workflow's name, arguments, result, ports, and codec.
The submitting code and implementation must agree on this contract. Runs without
an implementation stay blocked until compatible code is registered.

Names default to the function's module and qualified name and can be overridden
with `name=...`. Keep changes compatible with existing runs, or choose a new
name for incompatible definitions. See
[definition identity](reference/duron.md#definition-identity) for the full rules.

## Write workflows that can replay

Keep workflow control flow deterministic. Use `ctx.now()`, `ctx.random()`, and
`ctx.sleep()` for replay-safe time, randomness, and deadlines. Committed effect
results replay; a committed call without a final outcome blocks the run.
Use stable application operation IDs to reconcile uncertain external work.

See [workflow ports](guides/host-ports.md), [effect runs](guides/effect-runs.md),
[recovery](guides/recovery.md), and [storage](reference/storage.md).
