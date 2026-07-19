# `duron.storage` module

`HistoryStore` is the storage protocol used by `duron.run`. `MemoryStore` and
`SQLiteStore` are the built-in implementations. Stores persist opaque
`HistoryRecord` values and use `LeaseToken` values to fence writers.

`MemoryStore` state belongs to its instance and disappears with the process.
`SQLiteStore(path)` opens filesystem-backed SQLite connections as needed, uses
WAL mode and `synchronous=FULL`, and has a five-second busy timeout. Store
objects need no explicit `close()`. Use `MemoryStore` for transient tests rather
than passing `":memory:"` as a SQLite path.

Acquiring a lease replaces the current lease. The last acquirer owns the run,
even if the previous owner remains live. A stale owner cannot append with its
old token.
Releasing an old token must not release the new owner's lease. Leases do not
expire by time, and there is no renewal method or scheduler in this protocol.

`append` commits a whole batch or none of it. Record IDs must be unique within
the run; a duplicate rejects the batch. A successful append returns one ordered
offset per record. `read` returns records after its exclusive `after` offset, in
order, up to `limit`. An uncertain append outcome is resolved by reading the
history before deciding whether to send a new batch.

Treat offsets as opaque ordered positions, not array indices: they need not be
contiguous within a run, and the built-in stores do not share a starting offset.
Use `after=None` to read from the beginning and the returned offset to advance.
`limit` must be a positive integer. Reads need no lease and an empty page means
there are currently no matching records, not that a live run is finished.

`HistoryRecord.stream_id` is optional, nonempty routing metadata stored atomically
with its opaque payload. `read(..., stream_id="...")` returns only matching records
within the selected run, with `limit` counting matches. Offsets remain the original
history offsets, so filtered and full-history reads share the same exclusive
`after` cursor. Omitting the filter (or passing `None`) reads all records.
Custom stores must implement this filtering and preserve the metadata on reads.

The runtime tags stream creation, emission, and completion records and verifies
that the metadata matches the payload when decoding. Effect stream readers,
signal receivers, and remote stream inspection use filtered reads. Replay and
named port discovery read the full history.

`SQLiteStore` uses an explicit `INTEGER PRIMARY KEY` offset that remains stable
across `VACUUM`, plus a partial `(run_id, stream_id, offset)` index for tagged
records. Payloads remain opaque `BLOB` values. Existing databases without this
schema are rejected without migration; use a new database for this pre-1.0 schema.
`MemoryStore` indexes stream offsets in memory as well.

The SQLite schema and Duron's declared history format are separate checks. An
unsupported SQLite schema requires a new database. An unsupported history
format requires a fresh run history; the runtime accepts format version 1 and
rejects other declared versions without rewriting their records.

## ::: duron.storage
