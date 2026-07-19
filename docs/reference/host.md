# Host ports

Use a run handle to send workflow input and read workflow output. The scheduler,
run types, and `OutputCursor` are exported from `duron`. For a complete example,
see [workflow and host ports](../guides/host-ports.md).

## Send and close inputs

| Operation | Behavior |
| --- | --- |
| `await run.port(Ports.items).send(value, idempotency_key=None)` | Durably enqueue input without waiting for the workflow to accept it. |
| `await run.port(Ports.items).close()` | Enqueue closure after earlier values. |

The scheduler delivers queued input to the local run owner in insertion order.
A successful send confirms that the message was durably enqueued. Messages for
closed inputs or terminal runs are discarded, and processing rejections are
logged. An interrupted send may already have committed.

To retry a send, reuse its `idempotency_key` and encoded value. An identical
retry is deduplicated, including after input closure. Reusing the key with a
different value raises `PostConflictError`. Without a key, each send creates a
new message.

Use handles from the single active scheduler that owns the database file.
Ownership covers all namespaces; a second active scheduler on that file raises
`StorageError`. Sending input does not acquire or replace the run's write claim.

## Read outputs

| Operation | Behavior |
| --- | --- |
| `run.port(Ports.events).reader(after=None, prefetch=64)` | Create an independent output reader, optionally resuming from a cursor. |
| `async for value in run.port(Ports.events)` | Iterate a fresh output reader. |

Readers drain committed values before ending. They do not propagate workflow
failures; await `run.result()` to observe the result or error. A blocked run or a
scheduling failure without a workflow outcome leaves outputs open, so a reader
may wait indefinitely after draining the available values.

`prefetch` must be a positive integer. It bounds reader buffering without
applying backpressure to the producer.

## Resume from a cursor

Persist the reader's `cursor` alongside the consumer's committed work. Store both
its opaque `stream` identity and its `offset`. After reconnecting, reconstruct
an `OutputCursor` and pass it to `run.port(Ports.events).reader(after=cursor)`.

A cursor belongs to one persisted run and output port. It remains valid across
reconnects. The same run ID in another namespace or database identifies a
different run and cannot use that cursor.

## Reader and scheduler lifetime

Dropping a reader leaves execution running and workflow ports open. Closing
the scheduler stops local execution. Open a new scheduler context, reopen the
run, and recreate its readers to continue reading. If an unfinished effect
blocks recovery, its committed output is still available.

## ::: duron.OutputCursor
