# MVCC Index Method Contract

This note records the design decisions that govern custom index methods in
MVCC mode. The implementation remains experimental, but these decisions are
the target contract for lifecycle, storage, caching, and conflict behavior.

## Same-index writers

An FTS index permits one active writer transaction at a time. A transaction
acquires a lease before constructing or restoring a Tantivy writer, and the
lease is reentrant for that transaction. `OPTIMIZE INDEX` uses the same lease.

Lease contention returns `WriteWriteConflict`. MVCC already treats this error
as a transaction-aborting conflict, which gives applications one retry rule:
roll back the complete transaction and replay it from a new snapshot. A lease
conflict is never retried inside the VDBE because triggers, generated values,
and other statement effects are not generally replayable.

Leases are acquired in statement execution order and contention fails
immediately rather than waiting, so lease acquisition cannot deadlock. Leases
are transaction-scoped: statement and savepoint rollback do not release them,
while commit, transaction rollback, abandoned commit, and connection close do.
The lease key is the backing object's MVCC table ID, so dropping and recreating
an index cannot inherit the old lease.

WAL retains its existing database-wide single-writer behavior and does not use
the MVCC per-index lease. WAL cache ownership is checked separately against the
connection and WAL snapshot.

## Stable identity

The execution context identifies an index with:

- the runtime address identity of the shared `Database` object;
- a schema-incarnation runtime ID derived from that database identity, schema
  generation, method, table, and index name;
- the logical schema root, which must be zero for a custom index; and
- the persistent FTS incarnation stored in the control record.

Connection-local database slots and SQL names are lookup inputs, not identity.
The physical backing B-tree has its own non-zero root and, under MVCC, stable
table ID. Dropping and recreating an index always writes a new persistent FTS
incarnation even when the SQL name or a physical root is reused. Runtime
database identity prevents transaction-owned state from crossing
detach/reopen boundaries; the control-record incarnation prevents stale state
after drop/recreate or recovery.

Leases use the backing MVCC table ID. FTS caches additionally compare the
persistent incarnation and manifest generation. Deterministic yield selection
uses database slot, schema generation, logical root, method, and index name so
selection remains reproducible.

## Snapshot and manifest identity

Every index-method operation receives an immutable execution context. It
contains the database and index identities, journal mode, transaction mode,
transaction ID, schema generation, and a core snapshot identity:

- WAL read position for WAL transactions;
- MVCC begin timestamp and transaction ID for MVCC transactions.

The method reads its FTS manifest generation through its MVCC-aware backing
cursor because it is method-owned transactional data.

A transaction ID permits reuse only within the same transaction. Reuse across
transactions additionally requires an equal index incarnation and manifest
generation. Physical root publication during checkpoint invalidates cursor
factories but does not invalidate immutable FTS data when the manifest
generation is unchanged.

## FTS backing records

Storage format v1 keeps immutable chunk records but adds one
transactional control record. The control record contains a format version,
index incarnation, monotonically increasing manifest generation, compact file
catalog, and checksum. It is updated atomically with base-table and chunk
changes.

Chunk identity is `(path, chunk_no)`; bytes are payload, not part of logical
identity. The current backing B-tree still stores bytes in its physical key,
so loading validates and rejects duplicate logical chunk identities. The MVCC
writer lease and replace path preserve one value per identity during normal
writes. A later storage-format revision should enforce this structurally. The
control record is the explicit conflict point and the cheap cache-validation
token. Existing experimental indexes without the control record are rebuilt
rather than interpreted as the new format.

## Tantivy synchronous boundary

Tantivy never drives pager I/O. On a cold open, an explicit resumable state
machine scans and validates the backing records and preloads a complete,
transaction-bound immutable file snapshot. On a warm cross-transaction open,
it seeks only the control record; equal incarnation and generation reuse the
immutable snapshot without scanning file chunks. Until Tantivy offers a
reliable file-access plan, correctness requires a complete cold snapshot.
Retention is bounded by an attachment-level aggregate byte and connection
count budget. Immutable bytes are shared between the hot snapshot and its
base-comparison catalog rather than copied.

Tantivy writes target an in-memory, cursor-private overlay. Tantivy commit and
merge finish before an explicit resumable flush publishes the resulting
manifest and chunks through MVCC-aware backing storage. Cancellation discards
the unpublished snapshot or overlay. No pager, schema, cache, or lease lock is
held while waiting for an I/O completion.

This boundary trades first-query latency and transient memory for a simple
cooperative-I/O contract. Manifest catalogs, range reads, and cache validation
reduce repeated loading without reintroducing synchronous pager stepping.

## Performance and memory policy

A cold query scans and validates the complete backing directory before opening
Tantivy. A repeated query in the same transaction reuses that immutable
snapshot directly. A query in a later transaction reads only the control record
when its incarnation and manifest generation are unchanged; an FTS-changing
commit causes one full reload. Unrelated base-table commits therefore pay the
control-record validation cost but do not reload segment files.

The current experimental defaults are a 64 MiB Tantivy writer budget, 512 KiB
backing chunks, a 64 MiB hot-file budget, and a 128 MiB chunk budget. An
attachment retains at most four connection snapshots and at most 192 MiB of
aggregate read-snapshot and retained-writer bytes. Admission and eviction enforce
the aggregate limit; an individual snapshot larger than the limit is used for
the current operation but is not retained. These constants are implementation
defaults rather than public compatibility guarantees.

Automatic foreground maintenance considers groups of eight segments and reads
at most 64,000 documents or 32 MiB of source segment data per merge.
`OPTIMIZE INDEX` is the explicit unbounded compaction operation. Both ordinary
flush and optimize complete Tantivy's in-memory work before their resumable
backing-store publication phase, so rollback restores the prior manifest.

The FTS Criterion suites report WAL separately from MVCC with auto-checkpoint
disabled, default, and forced policies. They cover cold and warm reads, pooled
connections, selectivity, commit and segment churn, merge boundaries, and
1/2/4/8-writer scaling for independent indexes and same-index lease contention.
The memory benchmark has query-churn and update-churn profiles for WAL and MVCC
with one and four connections.

## Lifecycle and caches

Statement finalization has two stages. `prepare_statement_commit` performs all
fallible or resumable work before the statement savepoint is released.
`statement_committed` may then publish only transaction-private in-memory
state. Transaction commit, transaction rollback, savepoint rollback, statement
abort, and close have distinct infallible hooks; none may perform pager I/O.

Read cursors reuse immutable Tantivy state immediately within the same MVCC
transaction. A later transaction must validate the control record first.
Pending and flushing maps are always fresh per cursor.

A flushed writer may be retained by its connection after statement success.
Within MVCC it is tagged with the owning transaction ID and protected by that
transaction's lease. Only successful transaction commit removes the private
owner tag; a later transaction must validate incarnation and generation before
reuse. Statement rollback, savepoint rollback, transaction rollback, conflict,
drop, and connection close discard retained writer state. WAL uses the same
connection-local cache with WAL-position validation.

## Multi-writer scope

True concurrent writers to one FTS index are not part of the initial contract.
Concurrent MVCC transactions may write different FTS index incarnations when
their base rows and other declared resources do not conflict. Same-index
concurrency requires the separate immutable-segment and transactional-manifest
design described in the FTS MVCC implementation plan; it must be justified by
correctness analysis and benchmark results before replacing leases.
