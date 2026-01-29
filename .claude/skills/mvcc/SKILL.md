---
name: mvcc
description: Overview of Experimental MVCC feature - snapshot isolation, versioning, limitations
---
# MVCC Guide (Experimental)

Multi-Version Concurrency Control. **Work in progress, not production-ready.**

**CRITICAL**: Ignore MVCC when debugging unless the bug is MVCC-specific.

## Enabling MVCC

```sql
PRAGMA journal_mode = 'experimental_mvcc';
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
- `core/mvcc/database/mod.rs` - Main implementation (~3000 lines)
- `core/mvcc/cursor.rs` - Merged MVCC + B-tree cursor
- `core/mvcc/persistent_storage/logical_log.rs` - Disk format
- `core/mvcc/database/checkpoint_state_machine.rs` - Checkpoint logic

## Checkpointing

Flushes row versions to B-tree periodically.

```sql
PRAGMA mvcc_checkpoint_threshold = <pages>;
```

Process: acquire lock → begin pager txn → write rows → commit → truncate log → fsync → release.

## Current Limitations

**Not implemented:**
- Garbage collection (old versions accumulate)
- Recovery from logical log on restart

**Known issues:**
- Checkpoint blocks other transactions, even reads!
- No spilling to disk; memory use concerns

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
