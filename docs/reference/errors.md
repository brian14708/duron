# `duron.errors` module

## Application failures

Effects record ordinary exceptions as outcomes. Built-in errors such as
`ValueError` and `TimeoutError` can be caught by their normal type in workflow
code. Declare custom types with `@duron.effect(raises=(MyError,))`. Matching uses
exact types, so declaring a base class does not declare all its subclasses.

An undeclared custom effect failure arrives as `RemoteEffectError`, including
on the first execution: outcomes pass through persistence before delivery to
the workflow. This wrapper exposes `type_name`, the message, and `remote_args`.

Error persistence records the type, message, arguments, and explicit cause
chain. It does not serialize arbitrary exception attributes. Use errors that
can be reconstructed from JSON-compatible positional arguments. If Duron
cannot reconstruct a declared type, it falls back to `RemoteEffectError`.
`StopIteration` is not a supported round-tripping built-in failure.

## Runtime failures

| Error | Meaning and response |
| --- | --- |
| `SubmissionConflictError` | A run ID identifies a different immutable invocation. |
| `ContractMismatchError` | A reference differs from the persisted or registered contract. |
| `RunCancelledError` | The workflow acknowledged durable cancellation. |
| `RunNotFoundError` | An operation selected an absent run. |
| `HistoryMismatchError` | The selected definition, inputs, or operation sequence disagrees with history. Use compatible code for that run, or start a new run. |
| `WorkflowDefinitionError` | The definition or port interface is invalid, or input consumers overlap with an interruptible scope. |
| `CorruptLogError` | A persisted record cannot be decoded or validated. Inspect the underlying history and its reported offset. |
| `LeaseLostError` | Another owner acquired this run. Stop using the stale owner. |
| `PortClosedError` | A send targeted a closed or terminal input, or a receive found a closed, drained input. |
| `Interrupted` | An interruptible scope consumed one input; `value` contains its payload. |
| `ChannelClosedError` | A scoped effect channel is closed. This runtime error supports durable round-tripping. |
| `PostConflictError` | A delivery, append batch, or semantic posting key was retried with different content. |

`DuronError` groups Duron-specific conditions. Invalid arguments may also raise
standard `TypeError` or `ValueError`; cancellation uses `asyncio.CancelledError`.
Runtime ownership and history errors generally cannot be declared as application
failures. Let cancellation propagate.

## ::: duron.errors
