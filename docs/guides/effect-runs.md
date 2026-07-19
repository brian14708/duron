# Effect runs and ports

Use `ctx.start(effect, ...)` when a workflow needs to send messages to an effect
or read its progress. Declare the effect's inputs and outputs in a `PortSet`,
then interact with them through the scoped `EffectRun`:

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

## Send input

Inputs are FIFO. An effect reads committed inputs, then waits for new messages.
Repeated port lookup shares a cursor. Closing an input prevents new messages;
queued values remain readable until drained, then `receive()` raises
`ChannelClosedError`.

## Read output

An effect output can supply an `idempotency_key` to identify an event.
Reusing the key with different data raises `PostConflictError`. Without a key,
each emit creates a new event. Retrying an identical keyed emit succeeds
without appending another event.

Output logs close after the effect completes, including on failure. Readers
drain committed values and end cleanly. Await `job` to observe completion or
failure. `reader(after=cursor, prefetch=64)` provides independent iteration and
cursor resumption within the same logical run and port.

## Scope lifetime and recovery

Cancelling a result waiter leaves the effect running inside its scope. Scope
exit cancels unfinished work and drains local cleanup. An unobserved completed
failure is raised on normal exit. Endpoints reject access outside the scope.
Cancellation cannot roll back external actions or force a detached worker to
stop. Committed inputs and outputs remain in history after shutdown.

On recovery, recorded effect outcomes replay without invoking the body again.
An effect call with no recorded final outcome is reported as interrupted and
is never dispatched again.

## Competing operations

Use `ctx.select` for named awaitables, bound workflow inputs, or a mixture.
For example, given effects `primary` and `fallback` and an argument `key`:

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
