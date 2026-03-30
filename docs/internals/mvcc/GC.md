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

This means GC must maintain:

> If a row exists in the B-tree, either the SkipMap correctly represents the
> row's current state for all active readers, **or** the SkipMap has no entry
> (B-tree fallthrough, only safe when B-tree data is up to date).

Two hazards follow from this:

- **Removing a tombstone before its deletion is checkpointed** resurrects a
  deleted row — the dual cursor falls through to the stale B-tree row.
- **Removing the current version while leaving superseded versions** causes
  data loss — the superseded version's `end` timestamp still invalidates the
  B-tree row, but there's no MVCC version to serve reads.

These are guarded by Rule 2's tombstone guard and Rule 3's sole-survivor
condition respectively.

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

### Rule 3: Sole Survivor

A current version is redundant with the B-tree when `b ≤ ckpt_max` and
`b < lwm`. But we only remove it when it's the **sole** remaining version in
the chain. If superseded versions remain, removing the current version would
leave orphaned invalidators that hide the B-tree row without providing data.

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

After GC empties a version chain, the SkipMap entry is handled differently
depending on the GC path:

- **Checkpoint-time GC** (`gc_checkpointed_versions`): removes empty entries
  using a re-check-under-lock pattern. This is a TOCTOU gap (a writer could
  insert between the lock release and `remove()`), but safe under the current
  **blocking** checkpoint — no concurrent writers exist.

- **Background GC** (`gc_table_row_versions`, `gc_index_row_versions`): leaves
  empty entries in place (lazy removal). This avoids the TOCTOU race entirely.
  Empty entries are reused by `get_or_insert_with` on subsequent inserts, and
  cleaned up by the next checkpoint-time GC pass.

## Non-blocking Checkpoint Readiness

The GC rules are designed to work with both blocking and non-blocking
checkpoints — the LWM parameter naturally constrains what can be collected
when readers coexist with the checkpoint.

**What works today**: all four GC rules, LWM computation, recovery version
protection, tombstone guard, lazy removal in background GC.

**What needs work for non-blocking checkpoint**:

- **Checkpoint-time entry removal**: the re-check-under-lock pattern in
  `gc_checkpointed_versions` has a TOCTOU gap. Under non-blocking checkpoint,
  concurrent writers could lose inserted versions. Fix: either hold the write
  lock across the emptiness check and `remove()`, or switch to lazy removal
  (same as background GC).

## Key Files

| File | Contents |
|------|----------|
| `core/mvcc/database/mod.rs` | `gc_version_chain`, `compute_lwm`, `drop_unused_row_versions`, `gc_table_row_versions`, `gc_index_row_versions`, recovery tx cleanup in `commit_load_tx` |
| `core/mvcc/database/checkpoint_state_machine.rs` | `gc_checkpointed_versions`, auto-trigger wiring in `Finalize` |
| `core/mvcc/database/tests.rs` | 39 GC tests (unit, quickcheck, integration, e2e) |
