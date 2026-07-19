# Effect execution

Import execution types from `duron`.

An `Executor` implements `async execute(invocation, /) -> object`. It awaits
one attempt and returns its application result. Select an executor with
`@duron.effect(executor=...)`. Omitted executors resolve to `AsyncExecutor`
when registered; ordinary effects use the same execution path.

```text
Effect ----1----> Executor
  |                  |
  | 0..* attempts    | execute(invocation) -> result
  v                  v
Invocation       Runtime ----> Run History
  | arguments      validates     outcome + port closures
  | metadata       and commits   atomically
  |
  +--0..*--> Port (Input, Output)
```

## Executor

`Executor` is a structural protocol. Implementing its method is sufficient;
explicit subclasses must implement the abstract `execute` method.
`AsyncExecutor` runs the registered async Python body in the owner's event
loop. Subclass it and override `execute` to wrap normal execution, delegating
with `await super().execute(invocation)`.

The runtime validates and encodes the returned value using the effect's result
type and workflow codec, then commits the outcome and port closures atomically.
Invalid results and codec failures propagate to the owner.

Raise `ApplicationFailure(error)` for a final application failure. The runtime
records the underlying exception using the effect's existing error policy;
the wrapper is never persisted. `AsyncExecutor` translates body exceptions
automatically, while runtime errors such as lease loss, fencing, and invalid
port bindings propagate to the owner.

Other executor exceptions propagate and leave the attempt without a durable
outcome. Reopening the run supplies a fresh invocation for recovery. Executors
must await their work until a result or failure is known and clean up their
own tasks when cancelled.

## Invocation

`Invocation` is a nongeneric, slotted object supplied by the runtime. It exposes:

- `arguments`: fresh decoded application values by parameter name, including
  defaults and excluding internal port framing. Variadic arguments are tuples
  or dictionaries under their parameter names.
- `metadata`: immutable `ExecutionMetadata` with run ID and seed, operation ID,
  effect name and version, invocation number, recovery count, and recovering flag.
- `port(declaration)`: a typed `EffectReceiver[T]` or `EffectEmitter[T]`
  for a declared input or output port.

The run seed and operation ID identify the logical call across recovery.
Invocation counters identify the attempt. The invocation remains in the owner's
process; an executor chooses its own transport for remote work.

## Progress and Recovery

Forward progress through declared output ports with stable idempotency keys.
An identical repeated emit is deduplicated; changing its payload under the same
key raises `PostConflictError`. Workflow consumers read progress through
`async with ctx.start(effect) as job` and `job.port(Ports.events)`.

Each attempt reads input ports from the beginning. Recovery repeats execution
and can repeat external work, so use stable application IDs for remote delivery
and mutations. Previous invocations retain no new authority after recovery.
The owner retains the history storage lease.

Host shutdown cancels execution without recording a durable cancelled outcome.
Effect scope cancellation records cancellation and port closures. Neither
automatically terminates a remote job; the executor owns its transport and
remote cancellation policy.

See [effect runs](../guides/effect-runs.md) for scoped ports.

## ::: duron.ApplicationFailure

## ::: duron.AsyncExecutor

## ::: duron.ExecutionMetadata

## ::: duron.Executor

## ::: duron.Invocation
