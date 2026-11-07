# `duron` module

The package root is the authoring API for defining workflows and effects,
declaring ports, and running workflows with `SQLiteScheduler`. It also exports
the scheduler and run contracts used by callers. Customize effect execution
through `duron.execution`, serialization through `duron.codec`, handle failures
through `duron.errors`, and access lower-level persistence contracts through
`duron.storage`.

## Run lifecycle

`SQLiteScheduler` owns its database file exclusively. Use one active scheduler
per file; close it before another process opens the same database. After a
crash, a new scheduler can recover immediately.

| Operation | Behavior |
| --- | --- |
| `Scheduler` | Abstract interface for typed submission, observation, and async context management. |
| `SQLiteScheduler(path, workflows=())` | Execute runs using an owned SQLite database. |
| `workflow.ref` | Obtain an executable-free typed reference. |
| `await scheduler.submit(workflow_or_ref, id, *args, **kwargs)` | Durably accept an immutable invocation and deduplicate identical retries. Local workflow submission also registers its implementation. |
| `await scheduler.open(workflow_or_ref, id)` | Observe an existing invocation and check its contract. An executable workflow also registers locally if no implementation is registered. |
| `await run.result()` | Observe the typed terminal result or failure. |
| `await run.status()` | Return immutable `RunStatus(state: RunState, detail: str \| None)` with the latest scheduling diagnostic. |
| `await run.cancel()` | Accept an idempotent cancellation request. |
| `run.port(declaration)` | Bind a typed caller endpoint without activating execution. |
| `await scheduler.shutdown(timeout=30)` | Drain active effects and close; return run IDs forced to stop after the grace period. |

Arguments are normalized and saved at submission. Closing the scheduler context
calls `shutdown()` with its default 30-second grace period. Call
`shutdown(timeout=...)` explicitly before leaving the context to choose another
grace period.
Shutdown rejects submissions and defers new effect calls before recording them.
Active effects can finish and commit; waiting workflows unload for recovery.
After the grace period, remaining local runs are stopped and cleanup is awaited.
Zero requests immediate shutdown. Repeated shutdown calls share the first
operation and its timeout. Cancelling a shutdown waiter does not abort cleanup.
Cancelling a task waiting on `run.result()` leaves the workflow running.

### Status and suspension

`await run.status()` returns a `RunStatus`. Compare its `state` with a `RunState`
member; `detail` provides the latest scheduling diagnostic when available.

| State | Meaning |
| --- | --- |
| `QUEUED` | Waiting for execution or an infrastructure retry. |
| `RUNNING` | Executing locally. |
| `SUSPENDED` | Waiting for input or a deadline with local execution and the history claim released. |
| `BLOCKED` | Unable to continue, for example because an implementation is missing or an effect was interrupted. |
| `SUCCEEDED` | Completed successfully. |
| `FAILED` | The workflow failed or the scheduler stopped after a failure. |
| `CANCELLED` | A terminal cancellation outcome was recorded. |

Input delivery, cancellation, and durable deadlines resume suspended runs
automatically. The scheduler waits 60 seconds after a run becomes idle before
suspending it; configure this grace period with `idle_timeout`.

Application cancellation records a terminal cancelled outcome. Host shutdown
drains active effects before stopping local execution; effects left without an
outcome after the grace period block recovery.

## Workflows and references

`Workflow[P, T, Ports]`, `WorkflowRef[P, T, Ports]`, and `Run[T, Ports]` preserve
argument, result, and port types; `Ports` defaults to `NoPorts`. Declare shared
references with `workflow_ref` on annotated async contract stubs.

Pass executable workflows directly for embedded use. References carry workflow
contracts without executable code. Register implementations with `workflows=[...]`
for replay on startup. Each host permits one implementation per name. There is
no public registry object or automatic module discovery.

## Definition identity

Workflow and effect names identify definitions. Both default to
`"{module}:{qualname}"` and accept a `name` override. Duron does not fingerprint
source code or codec implementations. Authors keep changes compatible with
existing runs or choose new names for incompatible definitions.

Workflow references also carry parameter and result schemas, encoded defaults,
and port declarations for submission checks and routing. Replay checks recorded
operations and arguments. User-defined schema classes are identified by module
and qualified name; their field definitions are not fingerprinted.

## Typed port lookup

Both decorators accept `ports=Ports`, where `Ports` subclasses `PortSet`.
Declare attributes such as `events = Output[str]()`. Attribute names are durable
port names; the bound declarations are immutable.
Inherited ports retain their declarations and cannot be overridden, replaced,
or deleted. Multiple inheritance rejects distinct declarations with the same name.
Omit `ports` for an operation without ports.

| Declaration | Workflow lookup | Effect lookup | Caller lookup |
| --- | --- | --- | --- |
| `Input[T]` | `Receiver[T]` | `EffectReceiver[T]` | `Sender[T]` |
| `Output[T]` | `Emitter[T]` | `EffectEmitter[T]` | `Reader[T]` |

`ctx.port(Ports.events)` and `run.port(Ports.events)` bind the same declaration
to different typed capabilities. Lookup performs no I/O and rejects declarations
outside the registered set by identity. Static typing checks payloads and
operations. Runtime checks enforce declared value types, set membership, and
execution lifetime. Workflow emitters take only their value;
effect emitters accept an optional `idempotency_key`.

Port declarations are invariant: `Input[int]` cannot be widened to
`Input[int | str]`, because the declaration also grants its caller a writing
capability. Read-only endpoint protocols remain covariant, and `Sender[T]`
is contravariant.

## Context operations

`WorkflowContext` provides `port`, `call`, `start`, `select`, `now`,
`sleep`, and `random`. `EffectContext` provides `port`. These are
protocols implemented by the runtime; applications do not construct contexts.

`ctx.start(effect, ...)` returns an `EffectRun[Result]` for use in an
async context. `job.port(...)` returns caller endpoints, and `await job` observes
the result. Leaving the scope cancels unfinished work and waits for cleanup.
Endpoints cannot be used outside the scope. See [effect runs](../guides/effect-runs.md).

`ctx.select(name=input_or_awaitable, ..., timeout=seconds)` returns a
`Selection` with `name` and `value`. A timeout has `name="timeout"` and `value=None`.
It journals the winner, preserves losing input messages, and cancels and drains
losing awaitables. Failures win like successful completions and are raised after
cleanup. Declaration order breaks observed readiness ties. Selection requires
at least one branch, distinct inputs or awaitables, and finite nonnegative
timeouts. The branch name `timeout` is reserved. Value types are inferred
jointly; a branch name does not statically narrow heterogeneous values.

See [host ports](../guides/host-ports.md) for complete declarations and examples.

## ::: duron
