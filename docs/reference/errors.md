# `duron.errors` module

## Application failures

Effects record ordinary exceptions as outcomes. Built-in errors such as
`ValueError` and `TimeoutError` can be caught by their normal type in workflow
code. Declare custom types with `@duron.effect(raises=(MyError,))`; types in
`RetryPolicy.retry_on` are automatically included. Matching uses exact types,
so declaring a base class does not declare all its subclasses.

An undeclared custom effect failure arrives as `RemoteEffectError`, including
on the first execution: outcomes pass through persistence before delivery to
the workflow. This wrapper exposes `type_name`, the message, and `remote_args`.

Error persistence records the type, message, arguments, and explicit cause
chain. It does not serialize arbitrary exception attributes. Use errors that
can be reconstructed from JSON-compatible positional arguments. If Duron
cannot reconstruct a declared type, it falls back to `RemoteEffectError`.
`StopIteration` is not a supported round-tripping built-in failure.

Host request failures have a stricter entry boundary:
`PendingRequest.fail(error)` rejects undeclared custom errors with
`UndeclaredErrorTypeError` and leaves the request pending. Declare those types
on `Request(..., raises=(MyError,))` before calling `fail`. Built-in exceptions
such as `ValueError` need no declaration.

## Runtime failures

| Error | Meaning and response |
| --- | --- |
| `HistoryMismatchError` | The selected definition, inputs, or operation sequence disagrees with history. Use compatible code for that run, or start a new run. |
| `CorruptLogError` | A persisted record cannot be decoded or validated. Inspect the underlying history and its reported offset. |
| `LeaseLostError` | Another owner acquired this run. Stop using the stale owner. |
| `EffectOutcomeUnknownError` | The crash-recovery budget is exhausted. Reconcile the external action before deciding what to do next. |
| `PortClosedError` | An input is closed and drained, including a nonblocking batch receive. |
| `Interrupted` | An armed interrupt changed workflow control flow; `value` contains its payload. |
| `ChannelClosedError` | A scoped effect channel is closed. This runtime error supports durable round-tripping. |
| `PostConflictError` | A semantic posting key was reused with a different action or payload. Use a stable key for each distinct action. |

`DuronError` groups Duron-specific conditions. Invalid arguments may also raise
standard `TypeError` or `ValueError`; cancellation uses `asyncio.CancelledError`.
Runtime ownership and history errors generally cannot be declared as retryable
application failures. Cancellation does not consume a policy-driven retry.

## ::: duron.errors
