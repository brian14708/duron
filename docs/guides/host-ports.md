# Workflow and host ports

Ports let an application exchange messages with a running workflow. Declare
inputs and outputs in a `PortSet`. Inside the workflow, receive inputs and emit
outputs through `ctx.port(...)`. In the application, use `run.port(...)` to send
inputs and read outputs.

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

## Send and close inputs

Input `send()` and `close()` return after durable enqueueing; they do not wait
for the workflow to accept the message. The active owner processes messages in
insertion order. Messages for closed inputs or terminal runs are discarded.
Processing rejections are logged rather than returned to the sender. Local
encoding and contract validation still raise immediately.

Reusing an `idempotency_key` with the same encoded value deduplicates delivery;
conflicting reuse raises `PostConflictError`. Omit the key to send a new message
each time.

Closing an input preserves queued values. Receiving after it drains raises
`PortClosedError`. Use `receive_many(max_items=..., wait=...)` for batches.

## Read and resume outputs

Each call to `run.port(Ports.events).reader(after=cursor, prefetch=64)` creates
an independent reader. Direct iteration creates a fresh reader. Persist
`reader.cursor` alongside the consumer's committed work, then pass it as `after` on reconnection.
`prefetch` bounds reader buffering; it does not apply producer backpressure.

Readers drain committed output before ending. A blocked run can still provide
committed output, but its reader may then wait indefinitely. Await `run.result()` to observe the
result or failure.

Disconnecting a reader does not close workflow ports. To reconnect, reopen the
run and create a reader with the saved cursor. An unfinished effect blocks
the run, but committed output remains readable.

## Select inputs and operations

Use `ctx.select` to wait for a named input or operation, optionally with a
timeout. Inside the workflow above, you could replace the input receive with:

```python
choice = await ctx.select(name=ctx.port(Ports.names), timeout=60)
if choice.name == "timeout":
    return "No name received"
return f"Hello, {choice.value}!"
```

The returned `Selection` has `name` and `value`. A timeout has `name="timeout"`
and `value=None`. Pass input endpoints directly to consume only the selected message. Losing
inputs remain available. Awaitable losers are cancelled and drained before
return. A winning failure is raised after cleanup. A closed, drained input
is ready and raises `PortClosedError` if selected.

Selection journals its decision. Replay observes the same choice, and a
suspended timeout retains its original absolute deadline. Branch declaration
order resolves simultaneous observed readiness. This is observation order,
not a global ordering of storage commits. Inputs are exclusively claimed during
selection; overlapping receive or interruption consumers are rejected.

## Interrupt a block with input

For a block that should be interrupted by one input, use
`async with ctx.port(Ports.names).interruptible():`. A queued or future value is
consumed and raised as `duron.errors.Interrupted`, with its `value` payload.
The input remains exclusively claimed until the scope exits.
