# `duron` module

The package root exports workflow and effect definitions, typed contexts,
run handles, endpoint types, executor contracts, and execution contracts. Codec type aliases live in `duron.codec`.
Low-level storage contracts live in a dedicated module.

## Run lifecycle

| Operation | Behavior |
| --- | --- |
| `Scheduler` | Abstract interface for typed submission, observation, and async context management. |
| `SQLiteScheduler(path, workflows=())` | Execute runs using an owned SQLite database. |
| `workflow.ref` | Obtain an executable-free typed reference. |
| `await scheduler.submit(workflow_or_ref, id, *args, **kwargs)` | Durably accept an immutable invocation and deduplicate identical retries. Local workflow submission also registers its implementation. |
| `await scheduler.open(workflow_or_ref, id)` | Observe an existing invocation and check reference identity. |
| `await run.result()` | Observe the typed terminal result or failure. |
| `await run.status()` | Return immutable `RunStatus(state: RunState, detail: str \| None)` with the latest scheduling diagnostic. |
| `await run.cancel()` | Accept an idempotent cancellation request. |
| `run.port(declaration)` | Bind a typed caller endpoint without activating execution. |

Arguments are normalized and snapshotted at submission. Scheduler context exit
abandons local attempts for recovery. Cancelling a result waiter leaves the
workflow running.

Compare `status.state` with `RunState.QUEUED`, `RUNNING`, `SUSPENDED`, `BLOCKED`, `SUCCEEDED`,
`FAILED`, or `CANCELLED`. `status.detail` explains blocked implementations or
infrastructure retries when available. Application cancellation records a terminal
cancelled outcome; host shutdown leaves unfinished work recoverable.
`SUSPENDED` means an idle run has released its local execution and history claim;
deliveries, cancellation, and durable deadlines resume it automatically.
Schedulers wait 60 seconds by default after a run becomes idle before suspending it;
configure this with `idle_timeout`.

`Workflow[P, T, Ports]`, `WorkflowRef[P, T, Ports]`, and `Run[T, Ports]` preserve
argument, result, and port types; `Ports` defaults to `NoPorts`. Declare shared
references with `workflow_ref` on annotated async contract stubs.

Pass executable workflows directly for embedded use. References carry workflow
contracts without executable code. Register implementations with `workflows=[...]`
for recovery on startup. Each host permits one implementation per name and
version. There is no public registry object or automatic module discovery.

## Typed port lookup

Both decorators accept `ports=Ports`, where `Ports` subclasses `PortSet`.
Declare attributes such as `events = Output[str]()`. Attribute names are durable
port names; declarations are immutable and cannot be reused under another name.
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
effect capabilities additionally accept an optional `idempotency_key`.

Port declarations are invariant: `Input[int]` cannot be widened to
`Input[int | str]`, because the declaration also grants its caller a writing
capability. Read-only endpoint protocols remain covariant, and `Sender[T]`
is contravariant.

`WorkflowContext` provides `port`, `call`, `start`, `select`, `now`,
`sleep`, and `random`. `EffectContext` provides `port`. These are
protocols implemented by the runtime; applications do not construct contexts.

`ctx.start(effect, ...)` returns an `EffectRun[Result]` for use in an
async context. `job.port(...)` returns caller endpoints, and `await job` observes
the result. Scope exit owns cancellation and cleanup, and rejects escaped
endpoints. See [effect runs](../guides/effect-runs.md).

`ctx.select(name=input_or_awaitable, ..., timeout=seconds)` returns a
`Selection` with `name` and `value`. The timeout result is `("timeout", None)`.
It journals the winner, preserves losing input messages, and cancels and drains
losing awaitables. Failures win like successful completions and are raised after
cleanup. Declaration order breaks observed readiness ties. Selection requires
distinct branches and finite nonnegative timeouts. Value types are inferred
jointly; a branch name does not statically narrow heterogeneous values.

See [host ports](../guides/host-ports.md) for complete declarations and examples.

## ::: duron
