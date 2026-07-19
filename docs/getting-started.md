# Getting started

Install Duron with `uv pip install duron` in a Python 3.11+ environment, then
save and run the complete example in the [README](index.md).

Alternatively, from a repository checkout, run the
[hello world example](https://github.com/brian14708/duron/blob/main/examples/hello_world.py)
with a SQLite history:

```bash
uv run python examples/hello_world.py hello.db
```

With a fresh database, the first run prints an effect idempotency key and
`Hello, Alice!`. Run the command again with the same database. Duron replays the
recorded effect result, so the effect body does not print its key again.

## Define workflow and effect boundaries

Define both workflows and effects with `async def`. A workflow coordinates
durable operations through `WorkflowContext`. An effect performs external I/O
through ordinary async Python.

`WorkflowContext` and `EffectContext` are protocols for static type checking.
The runtime supplies their private implementations. Annotate the first
parameter with the appropriate protocol; applications do not construct contexts.
Effect code receives read-only idempotency and recovery information. The context
is the first positional parameter. Annotate payloads and results too, and put
generic arguments on port constructors, for example `duron.Input[str]("names")`.
An annotation on a variable alone does not supply a port's runtime payload type.

Keep network requests, filesystem access, nondeterministic values, and external
state changes inside effects. Call an effect with `await ctx.call(effect, ...)`.
Do not call an effect directly from a workflow.

Declare the effects and named ports a workflow uses with
`@duron.workflow(requires=(fetch, incoming, events))`. Use the same declaration
objects in the body. The tuple is available as `workflow.requires`, so host
code and tooling can inspect the boundary without running the workflow.

Omitting `requires` declares no external dependencies. An undeclared effect or
port emits a `UserWarning` on its first use in each workflow execution,
including replay. Execution continues under normal Python warning filters.
Unused declarations are allowed: a dependency may only be used on a conditional
path. Calls in workflow helper functions are checked through the same context.
Application clients used inside effects and dynamically opened effect channels
are not separate declarations.

These declarations document dependencies; they do not inject services, check
service availability, or prove a host is serving a port. Checks only cover paths
that execute. Declarations are not recorded in history, so correcting the list
does not require changing the workflow version. To make missing declarations
fail in tests, use Python's standard warning filter:

```python
import warnings

warnings.filterwarnings(
    "error", message="workflow .* used undeclared", category=UserWarning
)
```

A workflow function becomes a definition. Calling it binds its arguments
without starting execution. Pass a fresh bound call to `duron.run` each time
you run it:

```python
from duron.storage import SQLiteStore

store = SQLiteStore("app.db")
result = await duron.run(hello("Alice"), store, run_id="hello-alice")
```

`duron.run` returns the typed workflow result. For host-side access to ports,
use `async with duron.open_run(...) as active`. The context owns execution and
cleanup; `active.result()` waits for the typed result.

A normal exit from `open_run` waits for the workflow to finish, even if the body
never calls `active.result()`. A workflow waiting for an input or request still
needs a host to supply it. If the host context raises or is cancelled, Duron
cleans up local execution and releases its lease; committed history remains
available to resume. Cancelling a task that only awaits `active.result()` does
not cancel the workflow, because that wait is shielded. See the
[host reference](reference/host.md) for reader and shutdown behavior.

## Resume a run

Use the same store, `run_id`, workflow identity, and normalized inputs to resume
a run. Completed operations replay by their position in the history. Duron does
not look up effect results by their arguments. Workflow code runs again during
replay, rebuilding local variables and control flow. Keep operation order,
branches, and task creation deterministic; recorded results do not make arbitrary
Python I/O or mutable global state replay-safe.

Duron does not restart your process or discover pending runs. Your application
must reopen them. A durable timer can only advance execution while a run is
open, even if its deadline passed while the host was stopped.

Effects may run again after an ambiguous crash. Send
`EffectContext.idempotency_key` to an external system that supports idempotency
with every mutating request. The key stays stable across retries and recovery
of that logical call. For multiple distinct mutations within one effect, derive
separate stable keys from it, such as `f"{ctx.idempotency_key}:charge"` and
`f"{ctx.idempotency_key}:receipt"`.

Use explicit names and versions for definitions that survive deployments:

```python
@duron.workflow(name="orders.checkout", version="2", requires=(submit_order,))
async def checkout(ctx: duron.WorkflowContext, order_id: str) -> str:
    return await ctx.call(submit_order, order_id)
```

Bump the definition version for incompatible behavior, signature, annotation,
or codec changes. Effect descriptors also record retry policy and declared
errors, so changing those can invalidate replay. Versions are identity checks,
not code routing: Duron does not load an older implementation for you. Keep
compatible definitions available to finish existing runs, and start a new
`run_id` for an incompatible definition. Configure definitions when decorating
them; construct a new definition instead of mutating its fields.

The current history format is version 1. Duron rejects version 0 and other
unsupported declared versions before execution, leaving their records unchanged.
There is no automatic migration. Use a new `run_id` for a new history.

## Use durable time and randomness

Inside workflows, use `await ctx.now()` for deterministic UTC time and
`ctx.random()` for a replay-stable random generator. Calculate elapsed seconds
with `((await ctx.now()) - started_at).total_seconds()`, where `started_at` is
an earlier `await ctx.now()` value. Use `await ctx.sleep(seconds)` for a durable
deadline. A restart waits only for the remainder of that deadline.

Inside effects and host tasks, use ordinary clocks, random generators, and
`asyncio.sleep`.

## Connect inputs, outputs, and requests

Declare shared, typed ports at module scope. Use the same declaration in the
workflow and host code:

```python
import asyncio

import duron
from duron.storage import HistoryStore

names = duron.Input[str]("names")
events = duron.Output[str]("events")
approval = duron.Request[str, bool]("approval")


@duron.workflow(requires=(names, events, approval))
async def welcome(ctx: duron.WorkflowContext) -> str:
    name = await ctx.receive(names)
    if not await ctx.request(approval, name):
        return "Declined"
    message = f"Hello, {name}!"
    await ctx.emit(events, message)
    return message


async def host(store: HistoryStore) -> str:
    async with duron.open_run(welcome(), store, run_id="welcome-1") as active:

        async def feed() -> None:
            writer = active.input(names)
            await writer.send("Alice")
            await writer.close()

        async def consume() -> None:
            async for message in active.output(events):
                print(message)

        async def serve() -> None:
            async for request in active.requests(approval):
                await request.respond(True)

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(feed())
            tasks.create_task(consume())
            tasks.create_task(serve())
            result = await active.result()
        return result
```

Host feed, consume, and serve tasks are ordinary async tasks, not durable
workflow operations. Recreate them when you reopen a run. The fixed input feed
above demonstrates a single session: `InputWriter.send` has no idempotency key,
so blindly repeating the feed after a restart can enqueue duplicate values.

Inputs deliver values FIFO and consume each value once. A closed, drained input
raises `PortClosedError` from `ctx.receive`. Use `ctx.receive_many` to read a
batch. With `wait=False`, an open but currently empty input returns an empty
list; a closed, drained input raises `PortClosedError` in either mode.

Outputs are durable logs. Each reader has its own cursor. Requests pair one
workflow payload with one host response. Unresolved requests appear again when
a host reconnects; use `request.id` as an idempotency key if handling a request
changes external state. A second response raises `RequestAlreadyResolvedError`.
A host can call `request.fail(error)` instead of `respond`; declare custom error
types with the request's `raises` option. An undeclared custom type is rejected
with `UndeclaredErrorTypeError`, leaving the request pending. Built-in errors
such as `ValueError` need no declaration. See the [error reference](reference/errors.md).

## Interrupt an awaited operation

Declare an `Interrupt` and arm it around the workflow block that can stop:

```python
from duron.errors import Interrupted

stop = duron.Interrupt[str]("stop")


@duron.workflow(requires=(stop,))
async def wait_for_work(ctx: duron.WorkflowContext) -> str:
    try:
        async with ctx.interruptible(stop):
            await ctx.sleep(3600)
    except Interrupted as exc:
        return f"Stopped: {exc.value}"
    return "Finished"
```

Host code calls `await active.interrupt(stop).send("operator request")`.
`Run.interrupt` returns an `InterruptSender`. An interrupt stops an awaited
operation in the armed block; it does not undo an external change already made
by an effect.

## Stream effect results and send commands

Use `ctx.open_stream(item_type)` to create an effect-to-workflow stream. Pass
its `sender` to an effect as a `duron.execution.StreamSender[T]`:

```python
import asyncio

import duron
from duron.execution import StreamSender


@duron.effect
async def produce(ctx: duron.EffectContext, output: StreamSender[int]) -> None:
    async with output:
        for number in range(3):
            await output.send(number, idempotency_key=f"number:{number}")


@duron.workflow(requires=(produce,))
async def collect(ctx: duron.WorkflowContext) -> list[int]:
    async with ctx.open_stream(int) as stream:
        producer = asyncio.create_task(ctx.call(produce, stream.sender))
        values = [value async for value in stream]
        await producer
        return values
```

Close the sender when production finishes. Use explicit semantic keys when a
send must deduplicate across recovery. Omitted keys identify new
invocation-local work. Reusing a key with different content raises
`PostConflictError`. Each `stream.reader()` starts an independent reader;
`prefetch` bounds decoded items buffered by a reader, not total history size or
producer throughput. Stream EOF and the effect's final result are separate;
await the producer task as well as reading the stream.

Use `ctx.open_commands(item_type)` for workflow-to-effect control. Pass
`commands.receiver` to an effect as a `CommandReceiver[T]`, then send through
`commands.sender`, a `CommandSender[T]`. The effect receives a `Command[T]`
with a `value` and calls `await command.ack()` after handling it. One receiver
owner holds one outstanding delivery until acknowledgement commits.

Keep streams and commands inside their opening scope. They are runtime-owned
handles, not serializable application values.

## Bound concurrent work

Use ordinary async tasks to express a bounded batch of effect calls:

```python
@duron.workflow(requires=(transform,))
async def process(ctx: duron.WorkflowContext, items: list[str]) -> list[str]:
    results: list[str] = []
    for start in range(0, len(items), 8):
        batch = items[start : start + 8]
        results.extend(
            await asyncio.gather(*(ctx.call(transform, item) for item in batch))
        )
    return results
```

This admits at most eight calls per batch and returns results in input order.
Keep the input order and task creation order stable across replay.

## Configure retries and recovery

Import `RetryPolicy` from `duron.execution`:

```python
from duron.execution import RetryPolicy


@duron.effect(
    retry=RetryPolicy(
        max_attempts=3, retry_on=(TimeoutError,), initial_delay=1, max_recoveries=5
    )
)
async def fetch(ctx: duron.EffectContext, key: str) -> str:
    return await client.fetch(key, idempotency_key=ctx.idempotency_key)
```

`max_attempts` includes the first logical attempt. `retry_on` lists exact
exception types eligible for retry. Backoff uses `initial_delay`, `multiplier`,
and `max_delay`, with deadlines recorded in history. The default policy makes
one logical attempt and retries no reported failures. Merely increasing
`max_attempts` does not enable retry without a matching `retry_on` type.

Crash recovery has a separate `max_recoveries` budget. An unreported effect
outcome may require a new physical invocation without consuming a new logical
attempt. Inspect `ctx.logical_attempt`, `ctx.invocation_number`,
`ctx.recovery_count`, and `ctx.recovering` inside the effect.

The default `max_recoveries=None` permits unlimited recovery; `0` forbids a
new invocation after an ambiguous outcome. Exhausting this budget raises
`EffectOutcomeUnknownError`; it does not prove that the external action failed.
Types in `retry_on` are also included in the effect's declared errors. Other
custom errors to catch in workflow code belong in `@duron.effect(raises=(...))`.

## Supply typed application services

An effect can close over an application service. Construct the definition with
the service it will use:

```python
from typing import Protocol

from duron.execution import EffectExecutor


class OrderClient(Protocol):
    async def fetch(self, key: str, *, idempotency_key: str) -> str: ...


def make_fetch(
    client: OrderClient, executor: EffectExecutor | None = None
) -> duron.Effect[[str], str]:
    @duron.effect(name="orders.fetch", version="1", executor=executor)
    async def fetch(ctx: duron.EffectContext, key: str) -> str:
        return await client.fetch(key, idempotency_key=ctx.idempotency_key)

    return fetch
```

Manage the service lifetime in the host that constructs the effect. Ordinary
local effects pass no executor. The run owner can use
`make_fetch(client, dispatcher)` to select an adapter for this effect.
`duron.run` uses those definitions directly and takes no run-wide executor
registry.

The built-in `EffectHost.execute` uses the run's registered Python function;
another call to `make_fetch` creates a different closure and cannot replace it
at that boundary. A remote adapter must provide its own worker service and
transport integration. See the [remote reference](reference/remote.md).

## Use a custom codec

Implement `duron.codec.Codec.encode_json(value, annotated_type)` and
`decode_json(encoded, expected_type)`. Encoded values must be finite JSON
values. Select the codec on the workflow:

```python
@duron.workflow(
    codec=application_codec, codec_id="myapp.json.v2", version="2", requires=(fetch,)
)
async def flow(ctx: duron.WorkflowContext, key: str) -> str:
    return await ctx.call(fetch, key)
```

Custom codecs require an explicit, stable `codec_id`. The `duron.` prefix is
reserved. An effect can select its own codec with
`@duron.effect(codec=application_codec, codec_id="myapp.json.v2")`.
Bump the relevant definition version and codec ID when the encoding contract
changes.

The default codec accepts only finite JSON values: `None`, booleans, numbers,
strings, lists, and dictionaries with string keys. It does not reconstruct
dataclasses, tuples, dates, or other Python objects from annotations. See the
[codec reference](reference/codec.md) for a complete custom codec example.

## Choose a history store

Use `MemoryStore` for in-process histories and `SQLiteStore(path)` for
durable SQLite histories. A store holds multiple runs selected by `run_id`.

Implement `HistoryStore` to use another database. Its methods acquire and
release leases, append opaque `HistoryRecord` values with optional `stream_id`
metadata, and read ordered pages with optional stream filtering. Filtered reads
preserve the original history offsets. See the [storage reference](reference/storage.md).
Keep record payloads unchanged. Append each batch atomically, reject duplicate
record IDs, and enforce lease fencing in the same transaction. If an append
outcome is ambiguous after a crash, read the history before deciding whether
to submit a new batch. The repository's `tests/test_log.py` exercises these
contracts through a store fixture; substitute your implementation in that
fixture to check it against the same behavior.

Leases use last-acquirer-wins ownership. Acquiring a lease replaces the current
owner even when that owner is still live. Reject writes from an old lease.
A lease fences history writes, not external side effects.

## Dispatch effects to another process

Implement `EffectExecutor.dispatch(envelope, *, host)`. Select it on each dispatched effect with
`@duron.effect(executor=dispatcher)`.
Transport and reporting types are in `duron.execution.remote`.

The executor receives an `EffectEnvelope` (opaque immutable `bytes`) and an
`EffectHost` owned by the active run. Calling
`await host.execute(envelope, effect=registered_effect)` decodes inputs,
invokes the run's registered function in the host's process, and durably reports
its outcome as a `Receipt`. This is useful for local adapters. Execution in
another process requires an application transport using the lower-level request and
reporter methods; Duron does not supply a detached Python worker runner.

Authenticate worker traffic and enforce ownership before authorizing a rebind.
An executor that supports `EffectHost.rebind` also implements
`authorize_rebind(envelope)`; rebinding is rejected if this hook is absent.
Keep remote delivery and completion retries idempotent. See the
[remote execution reference](reference/remote.md) for the protocol signatures.

## Keep application policies outside Duron

Implement result caching inside effects or application services. A cache key
describes the application data; `ctx.idempotency_key` identifies one durable
effect call and is not a content-cache key.

Use Python logging inside effects and host tasks. Workflow code replays, so a
log statement in a workflow body can repeat after restart.

Offload blocking code from an async effect with
`await asyncio.to_thread(blocking_function, argument)`. The effect still owns
the idempotency and cancellation consequences of the blocking work.

To reconnect an output consumer, save `reader.cursor` after handling each
value and reopen with `active.output(events, after=saved_cursor)`. Persist the
cursor alongside the consumer's own committed work. A cursor belongs to one run
and named output stream. Saving it before committing that work can lose a value;
saving it afterward can repeat a value after a crash, so make consumption
idempotent.
