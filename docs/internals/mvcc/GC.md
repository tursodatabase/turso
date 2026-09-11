# MVCC Garbage Collection

## Overview

The MVCC store keeps every row version in memory: inserts, updates, deletes,
and rolled-back garbage. Without GC, memory grows monotonically with write
volume. GC reclaims versions that no active reader can see and that are
redundant with the B-tree.

GC is driven by two parameters computed at GC time:

- **LWM (low-water mark)**: `min(tx.begin_ts)` across Active/Preparing
  transactions, or `u64::MAX` if none. Tells GC which versions are still
  visible to some reader.
- **ckpt_max** (`durable_txid_max`): the highest committed timestamp
  whose data has been written to the B-tree. Tells GC when B-tree fallthrough
  is safe.

All GC logic lives in a single function, `gc_version_chain`, shared by both
checkpoint-time and background GC. The four rules are applied in order:

1. **Aborted garbage** (`begin=None, end=None`) — remove unconditionally.
2. **Superseded versions** (`end=Timestamp(e), e ≤ lwm`) — remove, unless
   doing so would let the dual cursor surface a stale B-tree row (tombstone
   guard).
3. **Sole-survivor current version** (`end=None, b ≤ ckpt_max, b < lwm`,
   chain length = 1) — remove, because the B-tree has the same data.
4. **TxID references** (`begin=TxID` or `end=TxID`) — keep, the owning
   transaction hasn't resolved yet.

The same code works under both blocking checkpoint (`lwm = u64::MAX`, all
versions reclaimable) and a future non-blocking checkpoint (`lwm` finite,
pinned by the oldest reader).

## When GC Runs

GC is triggered automatically in the `Finalize` stage of checkpoint
(`checkpoint_state_machine.rs`), in two phases:

1. `gc_checkpointed_versions()` — iterates only the checkpoint write set
   (rows just written to B-tree). O(checkpointed rows).
2. `drop_unused_row_versions()` — sweeps all table and index rows. Computes
   LWM once, then applies `gc_version_chain` to every chain. O(total rows).

Both run while the checkpoint lock is still held, before it is released.

## The Dual Cursor Invariant

Readers merge B-tree rows with MVCC SkipMap versions via a dual cursor. For
each B-tree row, the cursor checks `is_btree_invalidating_version` against
every version in the SkipMap entry. If any version invalidates, the B-tree row
is hidden and the visible MVCC version (if any) is returned instead. If the
SkipMap has **no entry** for the RowID, the B-tree row is returned as-is.

### SkipMap as write buffer

`chain_is_write_buffer_for` is false only for a **sole materialized current**
inside the published `durable_txid_max` boundary (Rule 3 shape). On **Truncate**,
when no checkpoint is in progress and the B-tree is readable, such chains are
omitted from MVCC merge/shadow so B-tree fallthrough serves the key. **Passive**
keeps SkipMap cover for all chains (concurrent Passive can leave table/index
B-trees briefly disagreeing). Passive Finalize keeps last currents (`unlink_empty`);
Truncate Finalize runs Rule 3. If both peeks briefly hold the same key,
`next`/`prev` advance both sides after emitting once.

This means GC must maintain:

> If a row exists in the B-tree, either the SkipMap correctly represents the
> row's current state for all active readers, **or** the SkipMap has no entry
> / is ignored as non-write-buffer (B-tree fallthrough, only safe when B-tree
> data is up to date for that reader).

Two hazards follow from this:

- **Removing a tombstone before its deletion is checkpointed** resurrects a
  deleted row — the dual cursor falls through to the stale B-tree row.
- **Removing the current version while leaving superseded versions** causes
  data loss — the superseded version's `end` timestamp still invalidates the
  B-tree row, but there's no MVCC version to serve reads.

These are guarded by Rule 2's tombstone guard and Rule 3's
`drop_current_if_in_btree` gate respectively.

## Rule Details

### Rule 2: Tombstone Guard

When removing a superseded version (`e ≤ lwm`), we check whether the chain
has a **committed current version** (`end=None, begin=Timestamp(_)`). If it
does, the current version takes over B-tree invalidation and removal is safe.

If no committed current version exists, the superseded version may be the only
thing hiding a stale B-tree row. Removal is only safe when:

- `e ≤ ckpt_max` — the deletion has been checkpointed, B-tree no longer has
  the row.
- But NOT when `e == 0 && ckpt_max == 0` — recovery tombstones before the
  first real checkpoint (see Recovery below).

Pending inserts (`begin=TxID`) do not count as committed current — they might
roll back.

### Rule 3: Drop current if already in B-tree

A current version is redundant with the B-tree when `b ≤ ckpt_max` and
`b < lwm`. We only remove it when it is the **last** version in the chain —
otherwise superseded versions would hide the B-tree row without providing data.

`drop_current_if_in_btree` controls whether Rule 3 runs:
- **true** on Truncate Finalize / Truncate incremental
- **false** on all Passive paths (Finalize `unlink_empty`, mid-Passive write-set
  GC, incremental) — currents stay as SkipMap cover; write-buffer reads still
  prefer B-tree for sole materialized currents after publish

Rule 3 also guards recovery versions: `b=0` versions are protected by
requiring `ckpt_max > 0` (see Recovery below).

## Recovery Versions

Log recovery stamps versions with `LOGICAL_LOG_RECOVERY_COMMIT_TIMESTAMP = 0`.
Since `durable_txid_max` is advanced via `NonZeroU64`, it stays at 0
until the first real transaction is checkpointed. This means `ckpt_max == 0`
acts as a natural "recovery data not yet checkpointed" flag:

- **Rule 2**: `e == 0 && ckpt_max == 0` → retain (recovery tombstone, B-tree
  may still have the row).
- **Rule 3**: `b == 0 && ckpt_max == 0` → `(b > 0 || ckpt_max > 0)` is false
  → retain (recovery insert, B-tree may not have the row).

Once `ckpt_max > 0`, the first real checkpoint has processed recovery data
alongside it, so recovery versions become collectible by the normal rules.

The recovery transaction itself is removed from `txs` at the end of
`commit_load_tx` to prevent pinning LWM to 0 (which would disable Rules 2-3).

## SkipMap Entry Removal

Truncate Finalize uses `_and_slots` (Rule 3 + unlink). Passive Finalize uses
`unlink_empty` (history + empty slots, keep currents). Mid-Passive write-set GC
and incremental leave currents and empty slots so write-set row ids still
resolve. Writers retry via `*_still_mapped` if an Arc was unlinked; index unlink
bumps `index_rows_epoch`.

### Passive `backfill_floor` publication

Passive Rule 2 needs `materialized_at <= backfill_floor` (`nbackfills`). After
`wal.checkpoint` + DB sync, MVCC calls `wal.publish_backfill` and keeps the
checkpoint guard until Finalize. On Passive `Busy`, finish without advancing
`nbackfills`.

Rule 3 is on for Truncate; off for all Passive GC paths. Truncate write-buffer
reads provide B-tree fallthrough for sole materialized currents; Passive does not
fall through while currents remain.

## Non-blocking Checkpoint Readiness

The GC rules are designed to work with both blocking and non-blocking
checkpoints — the LWM parameter naturally constrains what can be collected
when readers coexist with the checkpoint.

**What works today**: all four GC rules, LWM, recovery protection, tombstone
guard, under-lock empty-slot drain with writer retry, write-buffer read filter.

**What needs work for non-blocking checkpoint**:

- More soak / concurrent-simulator coverage of Passive GC racing writers on
  empty-slot drain.

### Why Rule 3 cannot simply be turned on for Passive

This was investigated directly: forcing `drop_current_if_in_btree = true` on
Passive Finalize (keeping the empty-slot unlink) reproduces real corruption —
`test_conflict_abort_ckpt_indexed_update_savepoint_integrity_check_passive`
("row missing from index") and `test_passive_concurrent_transfer_preserves_sum_and_count`
("total balance changed") both fail. The cause is **not** table/index publish
skew; it reproduces with one table and no index at all
(`passive_reader_snapshot_survives_later_write_after_row_versions_gc`). Once
Rule 3 empties a chain's SkipMap slot, the slot is unlinked entirely. A later,
unrelated write to that same row inserts a *new* chain with only its own
(future, invisible) version — a reader whose snapshot predates that write now
finds "no visible SkipMap version" and falls through to the physical B-tree,
which the later Passive auto-checkpoint has already overwritten: a
snapshot-isolation violation. Passive has no equivalent of Truncate's
`blocking_checkpoint_lock`, which excludes all MVCC transactions for the
duration of the write phase, so it cannot inherit that guarantee. Making Rule
3 safe for Passive needs either real page-level MVCC (so B-tree fallthrough is
isolated per reader) or serializing Passive's physical writes against readers
for rows whose chain is currently empty — both bigger than a GC-only change.
See the `gc_version_chain` doc comment in `core/mvcc/database/mod.rs` for the
full argument.

## Key Files

| File | Contents |
|------|----------|
| `core/mvcc/database/mod.rs` | `gc_version_chain`, `chain_is_write_buffer_for`, `compute_lwm`, `drop_unused_row_versions`, `gc_table_row_versions`, `gc_index_row_versions`, recovery tx cleanup in `commit_load_tx` |
| `core/mvcc/database/checkpoint_state_machine.rs` | `gc_checkpointed_versions`, auto-trigger wiring in `Finalize` |
| `core/mvcc/database/tests.rs` | 39 GC tests (unit, quickcheck, integration, e2e) |
