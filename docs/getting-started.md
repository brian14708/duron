---
hide:
  - toc
  - navigation
---

# Getting Started

Duron is a Python library that makes async workflows replayable. You can pause,
resume, or rerun async functions without redoing completed steps. This guide
walks through the core concepts and gets you building your first durable
workflow.

## Installation

Duron requires **Python 3.10 or higher**.

Install via pip:

```bash
pip install duron
```

Or if you're using [uv](https://docs.astral.sh/uv/):

```bash
uv add duron
```

## Core Concepts

Duron is built around five explicit concepts:

- A **workflow** (`@duron.workflow`) is durable orchestration code. Calling it
  binds arguments and returns an **invocation**
  ([`Invocation`](reference/duron.md#duron.Invocation)) — inert until you run it.
- An **effect** (`@duron.effect`) is external or nondeterministic work a workflow
  calls.
- [`duron.run`](reference/duron.md#duron.run) executes an invocation against a
  storage backend. Await it directly for the result, or use it as an async
  context yielding a typed [`Run`](reference/duron.md#duron.Run) handle when
  host code needs imperative port access.
- **Wiring** methods on the invocation
  ([`feed`](reference/duron.md#duron.Invocation.feed),
  [`output`](reference/duron.md#duron.Invocation.output),
  [`serve`](reference/duron.md#duron.Invocation.serve)) connect declarative host
  pumps to the run while it executes. The yielded `Run` supports imperative
  coordination across ports.
- **Ports** ([`Input`](reference/duron.md#duron.Input),
  [`Output`](reference/duron.md#duron.Output),
  [`Signal`](reference/duron.md#duron.Signal),
  [`Request`](reference/duron.md#duron.Request)) are typed, named channels between
  workflow code and host code.

### Workflows (`@duron.workflow`)

Workflows orchestrate your logic. They:

- **Take [`WorkflowContext`](reference/duron.md#duron.WorkflowContext) as the first
  parameter** — your handle to call effects, use ports, and access deterministic
  utilities.
- **Are deterministic** — the same inputs always produce the same execution path.
- **Are replayable** — when resumed, Duron replays logged results to restore state
  without re-executing completed steps.
- **Have no direct side effects** — all I/O goes through effects.

```python
@duron.workflow
async def my_workflow(ctx: duron.WorkflowContext, arg: str) -> str:
    result = await ctx.call(some_effect, arg)
    return result
```

### Effects (`@duron.effect`)

Effects wrap any code that interacts with the outside world — API calls, database
queries, file I/O, randomness, or any nondeterministic operation. Only
`@duron.effect` callables are accepted by `ctx.call`.

Duron records each effect result at its deterministic operation position. During
replay, a completed operation is not executed again. This is **not** content-based
memoization: calling the same effect at a different workflow position creates a
different operation.

```python
@duron.effect
async def fetch_data(url: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()
```

Synchronous effects require an explicit executor policy, because running them
inline blocks the worker event loop:

```python
@duron.effect(executor="thread")
def resize_image(data: bytes) -> bytes:
    ...
```

## Your First Workflow

Let's build a simple greeting workflow:

```python
import asyncio
import random
from pathlib import Path

import duron
from duron.contrib.storage import FileStorage


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


@duron.workflow
async def greeting_flow(ctx: duron.WorkflowContext, name: str) -> str:
    message, lucky_number = await asyncio.gather(
        ctx.call(work, name),
        ctx.call(generate_lucky_number),
    )
    return f"{message} Your lucky number is {lucky_number}."


async def main():
    storage = FileStorage(Path("run.jsonl"))

    result = await duron.run(greeting_flow("Alice"), storage)

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `hello.py` and run it:

```bash
python hello.py
```

You'll see output like:

```
⚡ Preparing to greet...
⚡ Generating lucky number...
⚡ Greeting...
Hello, Alice! Your lucky number is 42.
```

## Understanding Replay

The magic of Duron is in its replay behavior. Run the same script again:

```bash
python hello.py
```

**Notice**: No "⚡" output! `duron.run` found the existing run and replayed
the results from `run.jsonl` without re-executing the effects. The workflow
completes instantly, but produces the **exact same result**.

To start fresh, delete the log file:

```bash
rm run.jsonl
python hello.py
```

### Run reuse

`duron.run` reuses the run a storage backend already holds: empty storage starts
fresh, and a persisted run whose workflow name, version, and normalized encoded
inputs match is resumed (a completed run simply returns its recorded result).

Because each storage backend holds exactly one run, re-running the same workflow
with **different** inputs on the same storage raises
`HistoryMismatchError` — that storage is already bound to the first run's inputs. To
run again with new inputs, point `duron.run` at a different storage location
(e.g. a new file path).

## Storage Backends

Each storage backend holds exactly one run, so run identity and storage location
coincide.

```python
from duron.contrib.storage import MemoryStorage, FileStorage, SQLiteStorage

MemoryStorage()                 # in-memory; ideal for tests
FileStorage("run.jsonl")        # JSON Lines on disk; locks the data file with fcntl
SQLiteStorage("run.db")         # single-run SQLite database
```

Implement the [`Storage`](reference/log.md) protocol for a custom backend.

## Typed Ports

Ports are module-level declarations shared by workflow and host code, so external
names are never duplicated.

Declare the payload type by subscripting (`duron.Output[str]("events")`) or
passing it explicitly (`duron.Output("events", str)`). A bare
`duron.Output("events")` — including the `events: duron.Output[str] =
duron.Output("events")` annotation form — leaves the type unspecified, so prefer
one of the typed forms.

### Outputs

A workflow emits to a durable output log; host code reads it as the run
executes by wiring a sink with [`output`](reference/duron.md#duron.Invocation.output).

```python
events = duron.Output[str]("events")


@duron.workflow
async def producer(ctx: duron.WorkflowContext) -> None:
    for i in range(5):
        await ctx.emit(events, f"Message {i}")


async def main():
    async def show(entries):
        async for _offset, message in entries:
            print(f"Received: {message}")

    await duron.run(producer().output(events, show), storage)
```

The sink is an async function that receives a single async iterator of
`(offset, value)` pairs, where `offset` is the durable storage offset of each
value; the iterator ends when the run finishes. Wiring the same port more than
once attaches independent readers (fan-out). For coordinated reads across
several ports — read some outputs, then decide what to send — use the
imperative [`Run`](reference/duron.md#duron.Run) handle instead (see below).

Offsets make consumers restart-safe: checkpoint each processed offset, and on
resume skip entries at or before the saved one:

```python
async def store_events(entries):
    async for offset, message in entries:
        if offset <= last_saved_offset:
            continue
        await save_message_and_checkpoint(message, offset)


await duron.run(
    producer().output(events, store_events),
    storage,
)
```

Imperative readers expose the same checkpoint as `reader.offset`, updated before
each value is yielded.

### Inputs

Host code sends values into a workflow input queue; the workflow consumes them
FIFO. Wire an async source with [`feed`](reference/duron.md#duron.Invocation.feed);
the input is closed once the source is exhausted. Feeds are restart-safe: when a
crashed run is resumed, the source is re-iterated from the start, but values
already recorded in the durable log are skipped, so nothing is delivered twice.

```python
commands = duron.Input[str]("commands")


@duron.workflow
async def consumer(ctx: duron.WorkflowContext) -> list[str]:
    seen: list[str] = []
    while (value := await ctx.receive(commands)) != "stop":
        seen.append(value)
    return seen


async def source():
    yield "hello"
    yield "stop"


# host side
result = await duron.run(consumer().feed(commands, source()), storage)
```

### Signals

Signals interrupt an awaited operation inside an armed scope. Feed them like any
other host→workflow port.

```python
cancel = duron.Signal[str]("cancel")


@duron.workflow
async def interruptible_task(ctx: duron.WorkflowContext) -> str:
    try:
        async with ctx.interruptible(cancel):
            return await ctx.call(long_running_effect)
    except duron.Interrupted as exc:
        return f"Interrupted: {exc.value}"


async def cancel_after(delay: float):
    await asyncio.sleep(delay)
    yield "user cancelled"


# host side
await duron.run(interruptible_task().feed(cancel, cancel_after(5.0)), storage)
```

### Requests

Requests replace application-defined future protocols: a workflow issues a durable
request and awaits one reply. Answer each with
[`serve`](reference/duron.md#duron.Invocation.serve).

```python
approval = duron.Request[str, bool]("approval")


@duron.workflow
async def transfer(ctx: duron.WorkflowContext, amount: float) -> str:
    if amount > 1000 and not await ctx.request(approval, f"Approve ${amount}?"):
        return "rejected"
    return "approved"


# host side — handler receives the request payload, returns the reply
result = await duron.run(transfer(5000.0).serve(approval, lambda prompt: True), storage)
```

### Coordinating across ports imperatively

When a decision spans multiple ports — read some outputs, then decide what to
send back — use the [`Run`](reference/duron.md#duron.Run) yielded by the
`duron.run(...)` context. It can read and write across every port with ordinary
sequential code, while the context makes the execution lifetime explicit.

```python
async with duron.run(transfer(5000.0), storage) as run:
    pending = await run.requests(approval).next()
    # inspect other ports, prompt a human, etc.
    await pending.respond(True)
    result = await run.result()
```

## Errors

Every error Duron raises derives from [`DuronError`](reference/errors.md),
with a concrete class per condition (e.g. `HistoryMismatchError`,
`LeaseLostError`, `PortClosedError`) grouped under the `WorkflowError` and
`StorageError` category bases, so you can catch precisely or broadly.

An effect that fails re-raises the recorded exception during replay. Declare the
exception types an effect (or request port) can fail with so they round-trip as
their real type; undeclared types flatten to `RemoteEffectError`. Built-in
exceptions (`ValueError`, `KeyError`, ...) always round-trip without declaration:

```python
class PaymentDeclined(Exception): ...


@duron.effect(raises=[PaymentDeclined])
async def charge(card: str, amount: float) -> Receipt: ...


approval = duron.Request[str, bool]("approval", raises=[PaymentDeclined])
```

## Testing

Because [`duron.run`](reference/duron.md#duron.run) yields the same imperative
handle in production and tests, there is no separate harness. Use
[`MemoryStorage`](reference/log.md) for a fast, process-local run:

```python
from duron.contrib.storage import MemoryStorage


async def test_approval() -> None:
    async with duron.run(transfer(5000.0), MemoryStorage()) as run:
        request = await run.requests(approval).next()
        await request.respond(True)
        assert await run.result() == "approved"
```

To test **crash recovery**, cancel the `duron.run(...)` task partway through and
re-run against the same storage. To test **replay determinism**,
run once, then run again and assert the result matches.

## Runtime Guarantees

- Effects have **at-least-once external side-effect semantics**. A process can
  perform an external action and crash before its result is durably appended.
  Effects that mutate external systems should use `ctx.idempotency_key` or
  transactions.
- A run holds an opaque fencing lease while `duron.run` owns its execution.
  Acquisition is immediate and last-acquirer-wins; stale tokens cannot append
  after reacquisition. Losing the lease raises `LeaseLostError`.
- Cancelling a direct `duron.run(...)` await, or cancelling or raising from its
  async context, stops local execution but does not mark the run as cancelled —
  it remains resumable via a later `duron.run(...)`.
- `FileStorage` requires `fcntl` locking on the data file and is a local single-host log;
  `MemoryStorage` is process-local and non-durable.

### Tracing

Enable tracing to understand workflow execution:

```python
from duron.tracing import Tracer, setup_tracing


async def main():
    setup_tracing()

    result = await duron.run(
        greeting_flow("Alice"),
        storage,
        tracer=Tracer("run-123"),
    )
```

Traces are logged to your storage backend. Upload the JSON Lines to the
[Trace UI](https://brian14708.github.io/duron/trace-ui/) for visualization.
