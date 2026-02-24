# TLA+ Formal Verification of WAL Checkpoint Protocol

This directory contains a TLA+ model of the Turso multi-process WAL checkpoint
protocol. The model verifies that no committed data is lost after any
combination of process or power crashes, and that readers always see consistent
snapshots.

## What it models

- Write transactions with WAL frame writes and commits
- Read transactions with snapshot isolation (reader holds max_frame at start)
- Checkpoint (Restart and Truncate modes): backfill, salt change, DB sync,
  optional WAL truncation, header flush
- Reader-aware checkpoint: safe backfill boundary respects active readers
- Partial checkpoint: if readers block full backfill, checkpoint finalizes
  without restarting the WAL log
- Process crashes and power crashes (all volatile state lost)
- Recovery from durable state

Source files modeled:
- `core/storage/multi_process_wal.rs`
- `core/storage/wal.rs`
- `core/storage/pager.rs`
- `core/storage/tshm.rs`

## Running the model checker

### 1. Download TLC

Download `tla2tools.jar` from https://github.com/tlaplus/tlaplus/releases
and place it in this directory.

### 2. Verify the protocol (all invariants pass)

```bash
java -jar tla2tools.jar -config WALCheckpoint.cfg WALCheckpoint.tla
```

Expected: all invariants pass (`NoCrashDataLoss`, `WriterExclusion`,
`NoStaleReads`, `TypeOK`).

State space: ~112,000 distinct states, depth 45, completes in ~2 seconds
(2 processes, 2 writes).

## Invariants

- `NoCrashDataLoss`: every committed write ID is recoverable from durable
  storage (DB file + visible WAL frames). "Visible" means the frame's salt
  matches the durable WAL header salt.

- `WriterExclusion`: at most one process is in a writer phase at a time.

- `NoStaleReads`: every active reader's snapshot is consistent — all committed
  writes within the reader's snapshot boundary are recoverable. This verifies
  that checkpoint backfill respects active reader boundaries.

- `TypeOK`: type invariant including `nbackfills <= shared_max_frame` and
  `wal_hdr_durable.salt` bounds.

## Key safety property

The WAL header flush (which changes the durable salt, making old frames
invisible) is deferred to `prepare_wal_start()` on the next write. This
ensures it only runs AFTER the pager has synced the DB file via `CkptSyncDb`.
Flushing the header before the DB sync would cause data loss on power crash
(old frames become invisible while DB pages are only buffered).

Similarly, WAL file truncation (Truncate checkpoint mode) only runs in the
`TruncateWalFile` phase AFTER `SyncDbFile`, ensuring all backfilled data is
durable before the WAL frames are destroyed.
