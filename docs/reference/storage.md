# `duron.storage` module

`EventStore` is the history storage protocol used by the execution engine.
`SQLiteStore` is the built-in implementation. Stores persist opaque
`Event` values and use `LeaseToken` values to fence writers. `EventReader` is
the read-only subset. These low-level contracts support runtime development and
focused storage tests. Applications use `duron.SQLiteScheduler(path, ...)`, which
creates and closes its own store.

`SQLiteStore(path)` opens filesystem-backed SQLite connections as needed, uses
WAL mode and `synchronous=NORMAL`, and has a five-second busy timeout. This
preserves SQLite consistency while a power loss may lose the most recent
committed transactions.
Use `async with SQLiteStore(path)` or `await store.close()` to release the
connection promptly. Use `SQLiteStore(":memory:")` for transient tests: each
store instance has its own database, which disappears when the connection closes.

`claim(run_id, request_id, *, ttl=30.0)` acquires exclusive write ownership.
Another live owner causes `ClaimBusyError`; ownership is never stolen before
expiration. Claim lifetimes must be finite, positive, and at most 300 seconds.
The store supplies the clock used to check expiration.

Retry a claim with its original request ID after an uncertain response. The
same request returns the original token while that claim is valid. After
expiration, release, or replacement, that request raises `LeaseLostError`
instead of acquiring ownership again. Use a fresh request ID for a new attempt.

`renew(run_id, token, *, ttl=30.0)` extends only a still-valid claim.
`release(run_id, token)` releases only matching ownership and is idempotent.
Stale tokens cannot renew, append, or release another owner's claim. Active
execution renews every ten seconds. Renewal failure stops local execution and
leaves incomplete history available for recovery.

`append(run_id, token, records, *, batch_id)` commits a whole batch or none of it.
Record IDs must be unique within
the run; a duplicate rejects the batch. A successful append returns one ordered
offset per record. Retrying the same batch ID and records returns the original
offsets without writing again. Changed records under that ID raise
`PostConflictError`. Batch receipts survive ownership changes; all appends,
including retries, require a currently valid claim. The engine retries storage
errors with a stable batch ID for a bounded deadline before abandoning an
attempt for recovery.
SQLite receipts retain a SHA-256 digest of the framed batch rather than a
second copy of its payload.

`read` returns records after its exclusive `after` offset, in order, up to
`limit`.

Treat offsets as opaque ordered positions, not array indices: they need not be
contiguous within a run or start at a particular offset.
Use `after=None` to read from the beginning and the returned offset to advance.
`limit` must be a positive integer. Reads need no lease and an empty page means
there are currently no matching records, not that a live run is finished.

`Event.stream_id` is optional, nonempty routing metadata stored atomically
with its opaque payload. `read(..., stream_id="...")` returns only matching records
within the selected run, with `limit` counting matches. Offsets remain the original
history offsets, so filtered and full-history reads share the same exclusive
`after` cursor. Omitting the filter (or passing `None`) reads all records.
Custom stores must implement this filtering and preserve the metadata on reads.

The runtime tags stream creation, emission, completion, and scope-exit records
and verifies that the metadata matches the payload when decoding. Effect stream readers,
signal receivers, and typed output readers use filtered reads. Replay reads
the full history. SQLite runtime queries use indexed generated columns for
outcomes, named streams, and accepted inputs. Custom history
stores use a scan fallback; storage payloads remain opaque to the store API.

`stream.scope.exit` records workflow scope exit using `stream_id`. It closes the
channel if still open; otherwise it only acknowledges scope exit. Explicit and
effect-driven EOF use `stream.complete`.

`SQLiteStore` uses an explicit `INTEGER PRIMARY KEY` offset that remains stable
across `VACUUM`, plus a partial `(run_id, stream_id, offset)` index for tagged
records. Payloads remain opaque `BLOB` values. Existing databases without this
schema are rejected without migration; use a new database for this pre-1.0 schema.

The SQLite schema and Duron's declared history format are separate checks. An
unsupported SQLite schema requires a new database. An unsupported history
format requires a fresh run history; the runtime accepts format version 0 and
rejects other declared versions without rewriting their records.

## Invocation and accepted input

`create(run_id, header)` atomically creates an absent history. It returns
`False` if history exists and leaves that history unchanged. Creation does not
acquire or replace ownership. The private execution engine checks the immutable
header against the scheduling envelope before executing.

Input values and close markers are committed history acceptance records under
the current owner's claim. Retry IDs are scoped to the run and port. The engine
reconstructs accepted but unread values from history. Store implementations do
not parse input payloads or provide an input posting API.

`SQLiteScheduler` owns dispatch and SQL transactions directly. It stores jobs
and deliveries in structured tables rather than replaying scheduler commands:

| Table | Queryable fields |
| --- | --- |
| `jobs` | Namespace, run ID, workflow name/version, JSON contract and inputs, state, cancellation request, error, retry deadline, creation/update times. |
| `deliveries` | Namespace, run ID, insertion sequence, port, kind, JSON payload, retry IDs, pending/accepted/rejected state, error and acknowledgement time. |
| `log_entries` | Original history payload and offset, plus generated namespace, job ID, event type, source, timestamp in microseconds, and lookup key. |
| `leases`, `claim_requests`, `append_batches` | Fenced run ownership and idempotent storage receipts. |

Job identities are unique within a namespace. Partial indexes select due,
nonterminal jobs and pending deliveries; workflow and history indexes support
inspection without replaying the event log. A suspended job with no deadline
has a NULL `retry_at`; a delivery or cancellation makes it eligible again.
Posting and waking a job happen in one transaction, and suspension checks
pending deliveries in that same write transaction. Delivery order uses an
integer insertion sequence, not timestamp ordering.

Submission commits before acknowledgement. Appending a terminal history event
updates the corresponding job in the same transaction. Observers still validate
and consult authoritative history; job completion can be reconciled without
repeating effects. There is no global scheduler journal claim or in-memory
projection of all retained jobs. History and delivery receipts remain retained;
there is currently no compaction API.

SQLite schema compatibility uses `PRAGMA user_version` (currently 20261019,
the schema release date in `YYYYMMDD` form). Empty
databases are initialized atomically. Other versions, including nonempty legacy
databases with version 0, are rejected without migration. History format version
0 remains unchanged and unsupported declared history versions are rejected.

SQLite thread cancellation can leave a write committed after its waiter stops.
Stable claim request IDs and append batch IDs allow uncertain outcomes to be
retried. Successful port sends always require history acceptance, even when the
scheduler retains pending delivery rows.

## ::: duron.storage
