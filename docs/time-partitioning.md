# Time-partitioned recorder tables

Time partitioning keeps append-only event data in separate SQLite-compatible
files while exposing one logical SQL table. It is intended for embedded
recorders, video analytics plugins, and other workloads that retain or remove
whole time ranges.

## Configuration

Create the logical table in the main database, then register its physical
schema and path resolver on every connection that uses it:

```rust
use turso_core::partition::{PartitionConfig, VideoAnalyticsPathResolver};

connection.execute(
    "CREATE TABLE events (
        id INTEGER PRIMARY KEY,
        ts INTEGER NOT NULL,
        camera TEXT NOT NULL,
        event_type TEXT NOT NULL
    ) PARTITION BY (ts)",
)?;

connection.register_partitioned_table(
    "events",
    PartitionConfig::new(
        Box::new(VideoAnalyticsPathResolver::daily(
            archive_directory,
            "line_crossing".to_string(),
        )?),
        "CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            ts INTEGER NOT NULL,
            camera TEXT NOT NULL,
            event_type TEXT NOT NULL
        );
        CREATE INDEX idx_events_ts ON events(ts);
        CREATE INDEX idx_events_type_ts ON events(event_type, ts)"
            .to_string(),
        "ts".to_string(),
    ),
)?;
```

The partition key must have `INTEGER` affinity, be `NOT NULL`, and contain Unix
time in microseconds. Daily boundaries are UTC. Foreign keys are not supported
on partitioned tables because their target would live in a different database
file. Registration validates the physical schema before any event file is
created. The schema string may contain only `CREATE TABLE` and `CREATE INDEX`
statements, so a newly created file cannot be seeded or mutated as a side
effect of configuration. Configuration is connection-local and is not
persisted in the database.

`DefaultPathResolver::daily` writes `events_YYYY-MM-DD.db` files.
Table names that are not already portable filename components are encoded in
the physical filename, so quoted SQL identifiers cannot escape the configured
partition directory.
`VideoAnalyticsPathResolver::daily` writes
`YYYY-MM-DD/<plugin-id>.bin` files.
Plugin identifiers must contain only ASCII letters, digits, `-`, and `_`, must
fit within 128 bytes, and must not be reserved Windows device names. Validation
is performed when the resolver is constructed, before any filesystem access.

## SQL behavior

`INSERT ... VALUES` automatically creates and attaches the required daily
file. Bound parameters can be reused across days. Every row in one multi-row
`INSERT` must belong to the same daily file, so a routing error cannot leave a
partially written statement.

`SELECT` reads the logical table normally. Filtering, joins, CTEs, aggregates,
window functions, `DISTINCT`, global `ORDER BY`, and `LIMIT/OFFSET` operate over
the combined archive. Simple timestamp comparisons, `BETWEEN`, and `IN`
predicates prune unrelated files and are pushed down so local timestamp indexes
remain useful.

Before each root statement that references a partitioned table, the connection
reconciles that table with the resolver's files. A newly published file therefore
appears in already-prepared queries, while a file removed between statements
disappears without rebuilding the connection. Recorder inserts use a narrower
check of only their target time range, so retained days and unrelated plugins do
not add filesystem scans to the ingestion hot path. Transaction-opening
statements reconcile every registered table before freezing their file set.

Catalog discovery, attachment, and first-file initialization currently use
synchronous local-filesystem operations. Partitioned tables should therefore be
driven through the synchronous statement APIs; fully nonblocking
`step_with_waker` integration is not part of the current contract.

## Retention

Prefer the checked API for retention:

```rust
for partition in connection.list_partitions("events")? {
    if partition.range_end <= retention_cutoff_micros {
        connection.delete_partition(
            "events",
            std::path::Path::new(&partition.file_path),
        )?;
    }
}
```

`delete_partition` refuses to detach a file used by an active statement,
detaches only the selected partition, removes its database and WAL coordination
sidecars, and invalidates prepared plans. On Unix, externally unlinked files
are reconciled before the next statement. Applications should still use the
API when they need active-reader protection and portable Windows behavior.

New files are assembled under a private temporary name, checkpointed, and then
explicitly flushed before being published without overwriting an existing path.
On Unix, the containing directory is flushed after publication and checked
deletion so the directory entry survives a successful operation followed by a
crash. Concurrent creators therefore see one complete file rather than a
partially initialized database.

One recorder process must own SQL access and checked retention for an archive.
The no-clobber publication protocol makes simultaneous file creation safe, but
it does not make ordinary WAL reads, writes, or deletion coordination safe
across independent processes. External importers should publish only closed,
checkpointed files and must not keep them open after publication.

An external importer that atomically replaces a file at an existing path must
follow the same rule: checkpoint and close the replacement before publishing
it, and do not publish separate WAL sidecars. Reconciliation compares file
identities, detaches the old inode, removes its stale sidecars, and validates
the replacement before serving it.

`unregister_partitioned_table` is allowed only outside a transaction. It
detaches every managed file but leaves the archive on disk, so registering the
same configuration again discovers the existing days cleanly.

## Transaction and schema boundaries

A transaction may write repeatedly to one physical partition. A write to a
different day or a different partitioned table is rejected because independent
SQLite files cannot be committed atomically by the current WAL implementation.
`ROLLBACK`, `COMMIT`, and named savepoints restore the routing state together
with database pages.

Recorder tables are append-oriented. The following operations currently fail
explicitly instead of silently affecting only the empty logical table:

- `UPDATE` and row-level `DELETE`;
- `INSERT ... SELECT` and `DEFAULT VALUES`;
- `ALTER TABLE`, `DROP TABLE`, logical `CREATE INDEX`, and triggers.

Declare physical indexes in `PartitionConfig::schema_sql`. Unique constraints
are enforced by each daily file, not globally across the archive, so globally
unique event identifiers should be generated by the application. Remove old
data through whole-file retention rather than row-level deletion. Attached
partition aliases may be queried for diagnostics, but direct SQL writes and
schema changes through those aliases are rejected so timestamp routing and
partition pruning remain correct.

Changing the logical or physical schema is an offline archive migration: every
retained daily file must be migrated to the new schema and index set before the
new configuration is registered. Discovery rejects mismatched files instead of
serving a partially migrated archive.
