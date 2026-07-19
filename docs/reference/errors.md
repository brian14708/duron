# `duron.errors` module

Application failures are recorded as outcomes and delivered as `ApplicationError`.
Runtime failures describe problems with definitions, history, ownership, or
scheduling. Cancellation is handled separately; let `asyncio.CancelledError`
propagate so the runtime can complete cleanup.

## Application failures

Effects record ordinary exceptions as outcomes. Every persisted application
failure is delivered as `ApplicationError`, both during the first execution and
on replay. Uncaught workflow failures observed through `run.result()` use the
same type. Cancellation remains separate.

`ApplicationError` exposes `type_name`, `message`, optional JSON `details`, and
`cause` (also available as Python's `__cause__`). For ordinary exceptions,
`type_name` is the module-qualified class name, such as `builtins:ValueError`.
Duron stores a bounded explicit cause chain; it does not reconstruct exception
classes, constructor arguments, or arbitrary attributes.

Raise `ApplicationError` directly to supply a stable application error code
and structured details. Re-raising it preserves its type name and details
across further durable boundaries:

```python
from duron.errors import ApplicationError

# Inside an effect:
raise ApplicationError(
    "payment_rejected", "Payment was declined", details={"code": 409}
)
```

For richer details, explicitly call an application codec's `encode_json` before
raising and `decode_json` with a known schema when catching the error. Duron
persists details as finite JSON; it never sends exception objects to the codec.

## Runtime failures

### Submission and run lifecycle

| Error | Meaning and response |
| --- | --- |
| `SubmissionConflictError` | A run ID identifies a different immutable invocation. |
| `ContractMismatchError` | A reference differs from the persisted or registered contract. |
| `RunCancelledError` | The workflow acknowledged durable cancellation. |
| `RunFailedError` | No workflow outcome is available because the run is blocked or scheduling failed. `kind` is a `FailureKind`, including `INTERRUPTED_EFFECT` for an effect without an outcome. |
| `RunNotFoundError` | An operation selected an absent run. |

### Definitions and history

| Error | Meaning and response |
| --- | --- |
| `HistoryMismatchError` | The selected definition, inputs, or operation sequence disagrees with history. Use compatible code for that run, or start a new run. |
| `InterruptedEffectError` | Replay found an effect call without a final outcome. The scheduler marks the run `BLOCKED` and will not invoke the effect again. |
| `WorkflowDefinitionError` | The definition or port interface is invalid, or input consumers overlap with an interruptible scope. |
| `CorruptLogError` | A persisted record cannot be decoded or validated. Inspect the underlying history and its reported offset. |

### Ports and messages

| Error | Meaning and response |
| --- | --- |
| `PortClosedError` | A workflow input is closed and drained, or an effect output is closed. Host sends to closed inputs or terminal runs are discarded asynchronously. |
| `Interrupted` | An interruptible scope consumed one input; `value` contains its payload. |
| `ChannelClosedError` | A scoped effect channel is closed. An uncaught instance in an effect becomes an application failure. |
| `PostConflictError` | A delivery ID or posting key conflicts with existing content, or an append batch mixes existing and new IDs. |

### Storage and execution ownership

| Error | Meaning and response |
| --- | --- |
| `StorageError` | A storage operation failed, or another scheduler or store owns the database file. |
| `ClaimLostError` | The write claim no longer belongs to this owner. Stop using the stale owner. |
| `ClaimBusyError` | Another owner holds the claim. Wait for release. |
| `RunUnavailableError` | The effect's run owner is no longer accepting operations. |
| `InvalidHandleError` | An endpoint has an invalid identity, direction, type, or owner. |
| `InvalidExecutionRequestError` | An executor request does not match the registered call. |
| `InvalidPostError` | A posting key, action, or payload is invalid. |
| `ExecutionCompletedError` | A mutation was attempted after the effect completed. |

`DuronError` groups Duron-specific conditions. Invalid arguments may also raise
standard `TypeError` or `ValueError`; cancellation uses `asyncio.CancelledError`.
Ownership and history errors propagate as runtime failures.

## ::: duron.errors
