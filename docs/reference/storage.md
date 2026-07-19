# `duron.storage` module

Applications normally use `duron.SQLiteScheduler(path, ...)`, which owns and
closes its store. The `duron.storage` module exposes lower-level contracts for
storage integrations:

| Contract | Purpose |
| --- | --- |
| `EventReader` | Read history and look up events by ID. |
| `EventStore` | Create history, append events, and commit completion. |
| `JobStore` | Persist scheduler jobs and input delivery. |

`SQLiteStore` implements all three. `SQLiteScheduler` uses SQLite
directly; it does not accept a custom store.

## SQLite storage

`SQLiteStore(path, namespace="default")` keeps run histories in a SQLite
database. Run IDs are unique within a namespace, so separate namespaces can
reuse the same IDs. Jobs, input deliveries, and history share that namespace.

Use `async with SQLiteStore(path)` or `await store.close()` to release its
connection and all run claims. A later operation opens a new connection. Each
`SQLiteStore(":memory:")` has an independent, transient database that disappears
when its connection closes.

### Durability and retention

The store uses WAL mode, `synchronous=NORMAL`, and a five-second busy timeout.
SQLite remains consistent after a power loss, but the most recent committed
transactions may be lost. Cancelling a write's waiter does not guarantee that
the write was rolled back; retry uncertain operations with their original IDs.

Empty databases are initialized automatically. Unsupported schemas are rejected
without migration or replacement. History is retained after completion; there
is currently no compaction API.

## Ownership

Only one scheduler or store may own a database file at a time, across all
namespaces. A second owner raises `StorageError`. Close the first owner before
opening another. A process crash releases ownership automatically, so recovery
can begin immediately.

### Run claims

`claim(run_id)` acquires write ownership of a run and returns a fresh
`ClaimToken`. A repeated claim raises `ClaimBusyError` until the current claim
is released or the store closes. Claims stay in memory and do not expire.
`release(run_id, token)` is idempotent; a stale token cannot append or release
a newer claim.

Recovery replays completed operations; an effect without a recorded outcome
blocks the run.

## Create and append

`create(run_id, header)` atomically creates a history with its first `Event`.
It returns `False` if history already exists and leaves it unchanged. Creation
does not acquire or replace a claim.

`append(run_id, token, records)` commits a batch atomically and returns its
offsets. Every call, including a retry, requires a valid claim. Each `Event`
contains an ID, an opaque byte payload, and optional `stream_id` routing
metadata. IDs must be unique within a batch.

Event IDs identify immutable content. If all IDs already exist, append returns
their original offsets in request order without comparing payloads. If none
exist, it inserts the whole batch. Mixing existing and new IDs raises
`PostConflictError`. Callers must preserve payloads and routing metadata when
retrying an ID.

`append()` does not interpret outcomes or update scheduler jobs.
`finish(run_id, token, records, state)` atomically appends final events and marks
an associated job terminal, clearing its inbox. Pass a `TerminalState`:
`"succeeded"`, `"failed"`, or `"cancelled"`. The batch must be nonempty. It uses
the same claim and event-ID retry rules as `append()`; preserve the state as
well as the events when retrying. Histories without scheduler jobs can also be
finished. Existing terminal jobs retain their state.

## Read history

`read_event(run_id, event_id)` returns one `(offset, Event)` pair, or `None` if
that ID is absent from the run. SQLite uses its unique event-ID index. The
runtime uses this lookup for duplicate reports and accepted inputs, avoiding
an in-memory copy of every historical payload.


`read(run_id, after=None, limit=1024, stream_id=None)` returns ordered
`(offset, Event)` pairs after the exclusive `after` position. Use `None` to read
from the beginning and the last returned offset to advance. Offsets are opaque
ordered positions; they need not be contiguous within a run. `limit` must be a
positive integer.

Reads need no claim. An empty page means no matching records are currently
available; it does not establish that a run has finished. Capture
`wait = store.watch()` before reading, then `await wait()` if no data is ready.
To watch only one run, use `store.watch(run_id)`. This waits for a local commit
without polling. `watch()` is specific to `SQLiteStore`, rather than part of the storage
protocol.

Pass a nonempty `stream_id` to read only matching events. The limit counts
matches, and offsets remain the original history offsets. Custom stores must
preserve this metadata and support filtering. Runtime event decoding and replay
validation belong to Duron, rather than the storage protocol.

## Scheduler persistence

`JobStore` persists immutable submissions, scheduling state, cancellation
requests, and queued input. Submission commits before returning a handle.
Input sends acknowledge durable enqueueing; the run owner accepts messages into
history asynchronously and then removes them from the queue. Reprocessing after
a crash uses the same event ID to avoid duplicate acceptance.

SQLite commits a workflow's final outcome and terminal job state together.
It also coordinates suspension with input delivery so a message cannot be lost
while a run suspends. Messages for closed inputs or terminal runs are discarded.
Processing rejections are logged; they are not returned to the original sender.
See [host ports](host.md) for the application-facing delivery contract.

## ::: duron.storage
