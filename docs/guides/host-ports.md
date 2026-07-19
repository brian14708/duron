# Workflow and host ports

Declare ports once in a `PortSet`. Typed lookup returns receiving and emitting
capabilities inside a workflow, and sending and reading capabilities outside it.

```python
import asyncio

import duron


class Ports(duron.PortSet):
    names = duron.Input[str]()
    events = duron.Output[str]()


@duron.workflow(ports=Ports)
async def greet(ctx: duron.WorkflowContext) -> str:
    name = await ctx.port(Ports.names).receive()
    message = f"Hello, {name}!"
    await ctx.port(Ports.events).emit(message)
    return message


async def main() -> None:
    async with duron.SQLiteScheduler(":memory:", workflows=[greet]) as scheduler:
        run = await scheduler.submit(greet, "greeting")
        await run.port(Ports.names).send("Alice", idempotency_key="name")
        await run.port(Ports.names).close()
        async for message in run.port(Ports.events):
            print(message)
        assert await run.result() == "Hello, Alice!"


asyncio.run(main())
```

Input delivery is routed to the active owner and succeeds after history commit.
A queued run must acquire execution capacity before accepting a delivery.
Reusing an
`idempotency_key` with the same encoded value deduplicates delivery; conflicting
reuse raises `PostConflictError`. Omit the key to send a new message each time.
Closing an input preserves queued values. Receiving after it drains raises
`PortClosedError`. Use `receive_many(max_items=..., wait=...)` for batches.

Each `events.reader(after=cursor, prefetch=64)` creates an independent reader.
Direct iteration creates a fresh reader. Persist `reader.cursor`
alongside the consumer's committed work, then pass it as `after` on reconnection.
`prefetch` bounds reader buffering; it does not apply producer backpressure.
Outputs drain before EOF. Await `run.result()` to observe its result or failure.

Disconnecting a reader does not close workflow ports. Reads reconnect
after execution recovery by reopening the run and resuming output cursors.

## Select inputs and operations

```python
choice = await ctx.select(
    command=ctx.port(Ports.commands), work=ctx.call(fetch, key), timeout=60
)
```

`Selection` has `name` and `value`; a timeout returns `("timeout", None)`.
Pass input endpoints directly to consume only the selected message. Losing
inputs remain available. Awaitable losers are cancelled and drained before
return. A winning failure is raised after cleanup. A closed, drained input
is ready and raises `PortClosedError` if selected.

Selection journals its decision. Replay observes the same choice, and a
suspended timeout retains its original absolute deadline. Branch declaration
order resolves simultaneous observed readiness. This is observation order,
not a global ordering of storage commits. Inputs are exclusively claimed during
selection; overlapping receive or interruption consumers are rejected.

For a block that should be interrupted by one input, use
`async with ctx.port(Ports.stop).interruptible():`. A queued or future value is
consumed and raised as `duron.errors.Interrupted`, with its `value` payload.
The input remains exclusively claimed until the scope exits.
