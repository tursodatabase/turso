# TLA+ Formal Verification of WAL Checkpoint Protocol

This directory contains a TLA+ model of the Turso multi-process WAL checkpoint
protocol with WalIndex shared mmap and TSHM seqlock-style reader validation.
The model verifies that no committed data is lost after any combination of
process or power crashes, that readers always see consistent snapshots, and
that the WalIndex shared memory structure remains coherent.

## What it models

- Write transactions with WAL frame writes and commits
- Read transactions with snapshot isolation (reader holds max_frame at start)
- **WalIndex shared mmap**: zero-I/O frame lookup structure with generation
  counter, salt tracking, and max_frame commit
- **TSHM writer_state seqlock**: readers load S1, sync from WalIndex, claim
  TSHM slot, re-validate S2; retry if S1 != S2 (closes TOCTOU gap)
- **sync_shared_from_wal_index**: reader zero-I/O sync from WalIndex mmap
  (only increases max_frame, syncs salt)
- WalIndex clear/populate lifecycle: clear on WAL restart, populate on
  bootstrap, rebuild after partial checkpoint
- Checkpoint (Restart and Truncate modes): backfill, salt change, DB sync,
  optional WAL truncation, header flush
- Reader-aware checkpoint: safe backfill boundary respects active readers
- Partial checkpoint: if readers block full backfill, checkpoint finalizes
  without restarting the WAL log and rebuilds the WalIndex
- Process crashes (per-process volatile state lost, shared mmap survives)
- Power crashes (all non-durable state lost, including shared mmap)
- Recovery from durable state

Source files modeled:
- `core/storage/multi_process_wal.rs`
- `core/storage/wal.rs`
- `core/storage/pager.rs`
- `core/storage/tshm.rs`
- `core/storage/wal_index.rs`

## Running the model checker

### 1. Download TLC

Download `tla2tools.jar` from https://github.com/tlaplus/tlaplus/releases
and place it in this directory.

### 2. Verify the protocol (all invariants pass)

```bash
java -jar tla2tools.jar -config WALCheckpoint.cfg WALCheckpoint.tla
```

Expected: all 6 invariants pass (`NoCrashDataLoss`, `WriterExclusion`,
`NoStaleReads`, `WalIndexCoherence`, `SeqlockSafety`, `TypeOK`).

State space: ~54,000,000 distinct states, depth 75, completes in ~13 minutes
with 8 workers (2 processes, 2 writes).

## Invariants

- `NoCrashDataLoss`: every committed write ID is recoverable from durable
  storage (DB file + visible WAL frames). "Visible" means the frame's salt
  matches the durable WAL header salt.

- `WriterExclusion`: at most one process is in a writer phase at a time.

- `NoStaleReads`: every active reader's snapshot is consistent — all committed
  writes within the reader's snapshot boundary are recoverable. This verifies
  that checkpoint backfill respects active reader boundaries, and that the
  seqlock-based begin_read_tx doesn't expose stale snapshots.

- `WalIndexCoherence`: when the WalIndex has frames (max_frame > 0), every
  visible entry corresponds to a durable WAL frame. Ensures WalIndex lookups
  never return frame IDs pointing to non-existent or stale WAL data.

- `SeqlockSafety`: a reader in "reading" phase with a validated WalIndex
  generation has a generation <= the current WalIndex generation. Validates
  that the seqlock mechanism prevents readers from using invalidated WalIndex
  state.

- `TypeOK`: type invariant including `nbackfills <= shared_max_frame`,
  `wal_hdr_durable.salt` bounds, and WalIndex/TSHM counter bounds.

## Key safety properties

### Salt-based frame visibility

The WAL header flush (which changes the durable salt, making old frames
invisible) is deferred to `prepare_wal_start()` on the next write. This
ensures it only runs AFTER the pager has synced the DB file via `CkptSyncDb`.
Flushing the header before the DB sync would cause data loss on power crash
(old frames become invisible while DB pages are only buffered).

Similarly, WAL file truncation (Truncate checkpoint mode) only runs in the
`TruncateWalFile` phase AFTER `SyncDbFile`, ensuring all backfilled data is
durable before the WAL frames are destroyed.

### WalIndex lifecycle

The WalIndex is cleared (with the new salt and bumped generation) at WAL
restart. It is repopulated from the durable WAL during `begin_write_tx` when
the writer detects a stale or empty WalIndex. Readers cache the WalIndex
generation at `begin_read_tx` and assert it hasn't changed during `find_frame`.

### TOCTOU prevention via seqlock

The seqlock in `begin_read_tx` prevents the TOCTOU race where a writer could
checkpoint+truncate the WAL between a reader's sync and TSHM slot claim.
By re-validating `writer_state` after the slot claim, any concurrent writer
activity is detected and the reader retries with fresh state.
