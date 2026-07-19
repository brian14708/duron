# `duron` module

The package root contains the workflow authoring and live run API:

- `Workflow`, `WorkflowContext`, and `workflow`
- `Effect`, `EffectContext`, and `effect`
- `run` for a result; `open_run` and `Run` for host ports
- `Input`, `Output`, `Request`, and `Interrupt`

Host-side handles live in [`duron.host`](host.md). Effect configuration and
channels live in [`duron.execution`](execution.md), and worker transport lives
in [`duron.execution.remote`](remote.md). History storage lives in
[`duron.storage`](storage.md), and exceptions live in [`duron.errors`](errors.md).

## Core concepts by author

| Author | Concept | Why it is public |
| --- | --- | --- |
| Workflow | `@workflow` and `WorkflowContext` | Define replayed coordination and durable operations. |
| Workflow | `@effect` and `EffectContext` | Isolate external I/O and carry a stable idempotency key. |
| Workflow and host | `Input`, `Output`, `Request`, `Interrupt` | Share typed, named boundaries between a workflow and its host. |
| Host | `run` | Resume a workflow and await its typed result in one call. |
| Host | `open_run` and `Run` | Keep a run open while feeding inputs, reading outputs, serving requests, or interrupting it. |
| Executor | `EffectExecutor` | Select an execution environment on each effect and dispatch its opaque envelope. |
| Executor | `EffectHost` | Execute and report an effect through the run owner's authority. |
| Storage | `HistoryStore` and `HistoryRecord` | Persist ordered, opaque history with atomic append and lease fencing. |

`Workflow` and `Effect` are the definition types produced by their decorators;
`WorkflowCall` is the bound call produced by invoking a workflow definition.
Type checkers infer these types in ordinary code, so most applications do not
need to import them explicitly. `Codec` is an optional extension for changing
value encoding. `RetryPolicy` is an optional effect policy.

## Typed contexts

`WorkflowContext` and `EffectContext` are public protocols with private runtime
implementations. Their contracts are checked statically; there is no runtime
protocol check or public context constructor.

`WorkflowContext` exposes effect calls, typed port operations, deterministic
`now`, `sleep`, and `random`, plus scoped effect channels and batched input reads.
Use differences between `now()` values for elapsed-time calculations.

`EffectContext` exposes only read-only `idempotency_key`, `logical_attempt`,
`invocation_number`, `recovery_count`, and `recovering` properties. Runtime
identities, handle binding, and completion authority remain internal. Tests
calling an effect's `fn` directly may supply an object implementing this protocol.

## Declare external dependencies

Use `@workflow(requires=(effect, input_port, output_port, request_port, interrupt))`
to declare existing effect and port objects. No dependency wrapper or registry
is needed. `Workflow.requires` is a read-only tuple copied from the supplied
sequence when the function is decorated.
The type checker checks that entries are effects or ports; decoration does not
add a runtime type-validation step.

The default `requires=()` declares no dependencies. `WorkflowContext` emits a
standard `UserWarning` the first time it uses each undeclared object in an
execution. This also runs during replay; ordinary warning filters may suppress
repeated reports or turn them into errors. Warnings include the workflow name,
dependency kind and name, and the declaration needed to fix them. Matching uses
the declaration object, not its name: another object may select a different
executor, codec, or schema. Reuse the same objects in the declaration and body.

Declarations may include dependencies unused on the current branch. They do
not validate service health or host handler availability. Host access through
`Run` is not checked, and declarations do not create ports or affect persisted
identity. An unexecuted path cannot produce an undeclared-use warning.

## ::: duron
