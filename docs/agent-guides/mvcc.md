---
name: mvcc
description: MVCC feature - snapshot isolation, versioning, limitations
---
# MVCC Guide

Multi-Version Concurrency Control. A supported journal mode.

**CRITICAL**: Ignore MVCC when debugging unless the bug is MVCC-specific.

## Enabling MVCC

```sql
PRAGMA journal_mode = 'mvcc';
```

Runtime configuration, not a compile-time feature flag. Per-database setting.

## How It Works

Standard WAL: single version per page, readers see snapshot at read mark time.

MVCC: multiple row versions, snapshot isolation. Each transaction sees consistent snapshot at begin time.

### Key Differences from WAL

| Aspect | WAL | MVCC |
|--------|-----|------|
| Write granularity | Every commit writes full pages | Affected rows only
| Readers/Writers | Don't block each other | Don't block each other |
| Persistence | `.db-wal` | `.db-log` (logical log) |
| Isolation | Snapshot (page-level) | Snapshot (row-level) |

### Versioning

Each row version tracks:
- `begin` - timestamp when visible
- `end` - timestamp when deleted/replaced
- `btree_resident` - existed before MVCC enabled

## Architecture

```
Database
  └─ mv_store: MvStore
      ├─ rows: SkipMap<RowID, Vec<RowVersion>>
      ├─ txs: SkipMap<TxID, Transaction>
      ├─ Storage (.db-log file)
      └─ CheckpointStateMachine
```

**Per-connection**: `mv_tx` tracks current MVCC transaction.

**Shared**: `MvStore` with lock-free `crossbeam_skiplist` structures.

## Key Files

- `core/mvcc/mod.rs` - Module overview
- `core/mvcc/database/mod.rs` - Transactions and version storage
- `core/mvcc/cursor.rs` - Merged MVCC + B-tree cursor
- `core/mvcc/persistent_storage/logical_log.rs` - Disk format
- `core/mvcc/database/checkpoint.rs` - Checkpoint logic
- `core/mvcc/database/replay.rs` - Replay using B-tree descriptors
- `core/mvcc/database/legacy_recovery.rs` - Compatibility replay for older logs
- `core/storage/btree_descriptor.rs` - B-tree kind and durable comparison metadata
- `core/schema/mvcc.rs` - Catalog cache recovery and root-page rewriting
- `core/schema/portable.rs` - SQL names and primary keys for portable log consumers

## Checkpointing

Flushes row versions to B-tree periodically.

```sql
PRAGMA mvcc_checkpoint_threshold = <pages>;
```

The checkpoint selects a committed snapshot, collects B-tree lifecycle changes and row
versions, and opens a pager transaction. It destroys dropped B-trees, allocates new ones,
and writes rows using the stored comparison metadata. Root mappings and the durable
replay boundary become visible only after the pager transaction commits. Log truncation
follows the existing WAL durability protocol.

### B-tree lifecycle and catalog boundaries

`CreateBtree` records a logical object ID and a `BtreeDescriptor` in the transaction.
An index descriptor contains the resolved sort direction, collation, NULL ordering,
rowid presence, and uniqueness information. `Destroy` records the logical object ID
being dropped. Savepoint and transaction rollback undo these changes; commit publishes
object lifetimes together with the committed timestamp.

Each new logical-log frame includes a lifecycle batch, including an empty batch for
transactions that only change rows. Version 4 readers use those batches to replay
storage changes and decode index keys without consulting SQL definitions. Descriptor
encoding uses stable collation and type-order names rather than process-local IDs.
Readers still accept version 2 and 3 frames, including older frames preceding a
version 4 append. Their schema-based inference is isolated in `legacy_recovery.rs`.

Checkpoint does not derive creation or deletion from `sqlite_schema` row versions and
does not reconstruct `Schema` to find index ordering. Catalog rows use ordinary row
version selection. The schema adapter rewrites logical root placeholders after
allocation and updates the SQL schema cache after publication. Portable-log consumers
use a separate schema adapter to attach SQL names and primary keys.

The SQLite database file remains unchanged. On opening a checkpointed database,
bootstrap imports descriptors from the already-loaded schema once. This is also the
compatibility boundary for databases created before descriptor logging; the SQLite
file does not contain a separate persistent B-tree descriptor catalog.

## Current Limitations

- `PRAGMA wal_checkpoint(TRUNCATE)` blocks other transactions, including reads. Passive checkpointing does not, but is still behind `--experimental-mvcc-passive-checkpoint`.
- Row versions live in memory, so large working sets use a lot of memory.
- Indexes on `WITHOUT ROWID` tables are not supported.

## Testing

```bash
# Run MVCC-specific tests
cargo test mvcc

# TCL tests with MVCC
make test-mvcc
```

Use `#[turso_macros::test(mvcc)]` attribute for MVCC-enabled tests.

```rust
#[turso_macros::test(mvcc)]
fn test_something() {
    // runs with MVCC enabled
}
```

## References

- `core/mvcc/mod.rs` documents data anomalies (dirty reads, lost updates, etc.)
- Snapshot isolation vs serializability: MVCC provides the former, not the latter
