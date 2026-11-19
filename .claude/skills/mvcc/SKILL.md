---
name: mvcc
description: MVCC journal mode - snapshot isolation, row versions, logical log, checkpoint and GC settings, how to test
---
# MVCC

Multi-version concurrency control, enabled per database:

```sql
PRAGMA journal_mode = 'mvcc';
```

It is a runtime setting, not a compile-time feature flag. Most bugs are not MVCC-specific, so rule MVCC out first unless the bug only reproduces with `journal_mode=mvcc`.

## Differences from WAL

| Aspect | WAL | MVCC |
|--------|-----|------|
| Commit writes | Full pages | Affected rows only |
| Readers and writers | Do not block each other | Do not block each other |
| Persistence | `.db-wal` | `.db-log` (logical log), replayed on restart |
| Isolation | Snapshot (page level) | Snapshot (row level) |

MVCC gives snapshot isolation, not serializability. The module docs in `core/mvcc/` describe the data anomalies.

Each row version has a begin timestamp (when it becomes visible), an end timestamp (when it is deleted or replaced) and a flag saying the row already existed in the B-tree before MVCC wrote it.

## Checkpointing and garbage collection

Checkpointing writes row versions to the B-tree and truncates the logical log. Old versions are dropped by garbage collection. Settings are `PRAGMA mvcc_checkpoint_threshold` (the logical log size in bytes that starts a checkpoint; default about 4 MB; -1 disables, 0 checkpoints after every commit), `mvcc_gc_threshold` and `mvcc_group_commit`. See `docs/sql-reference/pragmas.mdx`.

## Limitations

- `PRAGMA wal_checkpoint(TRUNCATE)` blocks other transactions, including reads. Passive checkpointing does not, but is behind `--experimental-mvcc-passive-checkpoint`.
- Row versions live in memory, so large working sets use a lot of memory.

## Testing

`cargo test mvcc` runs the MVCC tests. To run a test with MVCC on:

```rust
#[turso_macros::test(mvcc)]
fn test_something() {}
```
