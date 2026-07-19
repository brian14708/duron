# Host ports

`OutputCursor` and the scheduler and run types are exported
from `duron`.

## Port operations

| Operation | Behavior |
| --- | --- |
| `await run.port(Ports.items).send(value, idempotency_key=None)` | Await owner-committed FIFO input acceptance. |
| `await run.port(Ports.items).close()` | Close an input after queued values; reject subsequent new sends. |
| `run.port(Ports.events).reader(after=None, prefetch=64)` | Create an independent resumable output reader. |
| `async for value in run.port(Ports.events)` | Iterate a fresh output reader. |

Sending with the same key and encoded value deduplicates retries, including
after input closure. Conflicting reuse raises `PostConflictError`. Omitting a
key sends a new message. The scheduler routes to the current execution owner;
producers never acquire or steal its claim. Separate scheduler instances can
deliver through the same database. Successful sends acknowledge acceptance,
not application-level processing. An interrupted send may already be committed.

## Readers and lifetime

Create readers from any open run handle. Dropping a reader leaves execution
running and workflow ports open. Scheduler context exit stops local execution
attempts for recovery; reopen runs and recreate readers in a new active context.
Await `run.result()` to observe completion or failure.

Each output reader has a cursor. Persist its opaque `stream` identity and `offset`
alongside committed consumer work, reconstruct an `OutputCursor`, then pass it
to `run.port(Ports.events).reader(after=cursor)`. Cursors are valid only for the same
persisted run and output port, including across reconnects. Matching display IDs
in another namespace or database do not make a cursor interchangeable.
`prefetch` must be a positive integer and bounds reader buffering.
Readers drain committed values before exhaustion. They do not propagate the
workflow's failure; use `await run.result()` for that.

## ::: duron.OutputCursor
