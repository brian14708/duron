# Effect runs and ports

Effects declare interaction ports in a `PortSet`, like workflows.
Lookup of `Input[T]` returns a receiver of plain values in workflows and effects.

```python
import asyncio

import duron


class Ports(duron.PortSet):
    commands = duron.Input[int]()
    events = duron.Output[int]()


@duron.effect(ports=Ports)
async def worker(ctx: duron.EffectContext) -> int:
    value = await ctx.port(Ports.commands).receive() * 2
    await ctx.port(Ports.events).emit(value, idempotency_key="result")
    return value


@duron.workflow
async def flow(ctx: duron.WorkflowContext) -> int:
    async with ctx.start(worker) as job:
        await job.port(Ports.commands).send(21)
        async for value in job.port(Ports.events):
            assert value == 42
        return await job


async def main() -> None:
    async with duron.SQLiteScheduler(":memory:", workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "work")
        assert await run.result() == 42


asyncio.run(main())
```

`ctx.start` creates a scoped, awaitable `EffectRun`. Caller endpoints are
available through `job.port(...)` after entry, and repeated awaits observe one
logical result.
`await ctx.call(effect, ...)` remains convenient when no interaction is needed.
Calling an effect with ports does not automatically service its inputs, so such
a call can wait indefinitely for interaction.

## Recovery and lifetime

Inputs are FIFO. Each effect invocation reads the full input history from the
beginning, then waits for new messages. Repeated port lookup shares the same
invocation-local cursor. Closing an input prevents new messages; queued values
remain readable until drained. When an interrupted effect restarts, its cursor resets,
including messages already handled by an earlier invocation. The effect body
restarts too; its local state is not restored. Use stable application IDs to
deduplicate external work. Recorded effect results replay without invoking the
body again.

An effect output can supply an `idempotency_key` to identify the same
semantic post across invocations. Reusing the key with different data raises
`PostConflictError`. Without a key, posts identify invocation-local work.
Output logs close after the effect completes, including on failure. Readers
drain committed values and end cleanly. Await `job` to observe completion or
failure. `reader(after=cursor, prefetch=64)` provides independent iteration and
cursor resumption within the same logical run and port.

Cancelling a result waiter leaves the effect running inside its scope. Scope
exit cancels unfinished work and drains local cleanup. An unobserved completed
failure is raised on normal exit. Endpoints reject access outside the scope.
Cancellation cannot roll back external actions or force a detached worker to
stop. Workflow suspension preserves committed inputs and outputs
for recovery. Remote recovery dispatches a new invocation; earlier invocation
objects cannot gain the new attempt's reporting authority.

## Competing operations

Use `ctx.select` for named awaitables, bound workflow inputs, or a mixture:

```python
selected = await ctx.select(
    primary=ctx.call(primary, key), fallback=ctx.call(fallback, key), timeout=30
)
```

The first observed completion wins, including a failure. Losing operations are
cancelled and drained; they may already have performed external work. The
choice is durable and replays unchanged. When input preservation matters,
pass a workflow input endpoint directly instead of its `receive()` coroutine.
See [workflow ports](host-ports.md#select-inputs-and-operations).
