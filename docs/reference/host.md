# `duron.host` module

Host-side handles returned by `Run` and the bound `WorkflowCall` type live here.
Most code can rely on inferred return types. Use these imports when annotating
helpers or persisting an `OutputCursor`.

## Run lifetime

`duron.open_run(call, store, run_id=...)` acquires a lease and starts or resumes
the workflow when its async context is entered. Each bound `WorkflowCall` is
single-use; call the workflow definition again for every execution.

Normal context exit waits for the result and propagates workflow failure.
An exception or cancellation in the host context tears down local tasks and
releases the lease, preserving committed history for a later resume. It does
not roll back external effects or guarantee that a detached worker stops.
Cancelling a waiter on `Run.result()` alone leaves execution running.

Host tasks belong inside the context and must be recreated on resume. Inputs,
requests, and interrupts can keep a workflow waiting indefinitely until a host
supplies the required value. Opening the context and then immediately leaving
it normally is not a way to suspend that workflow.

## Port handles

| Handle | Behavior |
| --- | --- |
| `InputWriter` | `send(value)` appends an input; `close()` records end of input. Each send is a new value, including a repeated send after host recovery. |
| `InterruptSender` | `send(value)` delivers a notification for an interruptible workflow block. It has no `close()` method. |
| `OutputReader` | Reads the durable output log, starting at the beginning or strictly after an `OutputCursor`. |
| `RequestReader` | Yields unresolved requests, including pending requests from before host recovery. |
| `PendingRequest` | Exposes `id`, `value`, `created_at`, and `is_pending`; `respond(value)` or `fail(error)` resolves it once. |

An input's consumed position is durable, but the host's decision to send is
not. If the host cannot tell whether a send committed, repeating it can enqueue
a duplicate. Include application message IDs where deduplication matters.

Use `PendingRequest.id` to deduplicate external work performed by a request
handler. Multiple readers may see the same unresolved request. A second
resolution raises `RequestAlreadyResolvedError`, even with the same value.
`is_pending` is a snapshot, not a reservation. Custom failures must be declared
on the request; see [errors](errors.md).

## Output cursors and reader exhaustion

Each output reader has an independent cursor. Within one reader, `next()`,
`collect()`, and `async for` share a cursor. Async iteration finishes when the
output closes or the run reaches its terminal state, after draining committed
output. `next()` raises `InvalidRunStateError` when no next value remains.
`collect()` waits for exhaustion, so use streaming iteration for a long-lived run.
Request readers offer the same iteration helpers over unresolved requests.

`reader.cursor` is the position of its last delivered value. Persist its
`run_id`, `stream_name`, and `offset` alongside the consumer's committed work,
then reconstruct an `OutputCursor` and pass it to
`active.output(port, after=cursor)`. The cursor must belong to the same run and
output stream. It is distinct from the `StreamCursor` used for effect streams.

Readers do not propagate the workflow's result or failure. Always await
`active.result()` or let normal context exit observe it.

## ::: duron.host
    options:
      inherited_members: [send, next, collect]
