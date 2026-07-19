# Effect execution

Import execution types from `duron.execution`.

An `Executor` implements `async execute(invocation, /) -> object`. It awaits
one call and returns its application result. Select an executor with
`@duron.effect(executor=...)`. Omitted executors resolve to `AsyncExecutor`
when registered; ordinary effects use the same execution path.

## Choose or implement an executor

`Executor` is a structural protocol. Implementing its method is sufficient;
explicit subclasses must implement the abstract `execute` method.
`AsyncExecutor` runs the registered async Python body in the owner's event
loop. Subclass it and override `execute` to wrap normal execution, delegating
with `await super().execute(invocation)`.

## Results and failures

The runtime validates and encodes the returned value using the effect's result
type and workflow codec, then commits the outcome and port closures atomically.
Result validation or encoding failures that raise `TypeError` or `ValueError`
are recorded as application failures. The effect is not invoked again.

Raise `ApplicationFailure(error)` for a final application failure. The runtime
records the underlying exception as an `ApplicationError`; the wrapper is
never persisted. `AsyncExecutor` translates body exceptions automatically, while
runtime errors such as claim loss and invalid port bindings propagate to the
owner. See [application failures](errors.md#application-failures).

Other executor exceptions propagate and leave the call without a durable
outcome. Reopening reports an interrupted effect and does not dispatch it
again. Executors must await their work until a result or failure is known and
clean up their own tasks when cancelled.

## Invocation

`Invocation` is supplied by the runtime. It exposes:

- `arguments`: fresh decoded application values by parameter name, including
  defaults.
- `metadata`: immutable `ExecutionMetadata` with run ID and seed, operation ID,
  and effect name.
- `port(declaration)`: a typed `EffectReceiver[T]` or `EffectEmitter[T]`
  for a declared input or output port.

The run seed and operation ID identify the call. For remote execution, the
executor owns the transport, authentication, and remote cancellation policy.

## Report progress

Forward progress through declared output ports with stable idempotency keys.
An identical repeated emit is deduplicated; changing its payload under the same
key raises `PostConflictError`. Workflow consumers read progress through
`async with ctx.start(effect) as job` and `job.port(Ports.events)`.

## Interruption and cancellation

Duron does not retry an effect call. A missing outcome blocks the run on replay,
even if the external work may have finished. Use stable application IDs to
inspect or reconcile remote work before starting a new run.

Host shutdown cancels execution without recording a durable cancelled outcome.
Effect scope cancellation records cancellation and port closures. Neither
automatically terminates a remote job; the executor owns its transport and
remote cancellation policy.

See [effect runs](../guides/effect-runs.md) for scoped ports.

## ::: duron.execution.ApplicationFailure

## ::: duron.execution.AsyncExecutor

## ::: duron.execution.ExecutionMetadata

## ::: duron.execution.Executor

## ::: duron.execution.Invocation
