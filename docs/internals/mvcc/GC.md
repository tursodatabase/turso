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
checkpoint-time and background GC. The rules are applied in order:

1. **Aborted garbage** (`begin=None, end=None`) — remove unconditionally.
2. **Superseded versions** (`end=Timestamp(e), e ≤ lwm`) — remove, unless
   doing so would let the dual cursor surface a stale B-tree row (tombstone
   guard).
3. **Sole-survivor current version** (`end` unpacks as `None`, `b ≤ ckpt_max`,
   `b < lwm`, chain length = 1) — **retire** it at a clock timestamp. The
   version stays in the chain and still reads as current. Incremental GC holds
   only the checkpoint read lock, so other transactions exist and a cursor may
   already be positioned on the version. Dropping it makes the next column
   fetch return empty or NULL columns.
3b. **Retired versions** (`retired_at < lwm`) — remove. No transaction that
    could still see the version is open. With no readers (`lwm == MAX`),
    retire and 3b run in the same call, which is the same memory win as a
    drop.
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
keeps SkipMap cover for unretired chains (concurrent Passive can leave
table/index B-trees briefly disagreeing). A version retired for this
transaction (`begin_ts > retired_at`) falls through to the B-tree. Both modes
retire last currents (Rule 3) and drop them in Rule 3b once `retired_at < lwm`.
If both peeks briefly hold the same key, `next`/`prev` advance both sides after
emitting once.

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

These are guarded by Rule 2's tombstone guard and Rule 3's retire-then-3b
path. Rule 3 never `clear()`s a current version. It stamps `retired_at` in
the spare tag bits of packed `end` (`RETIRED_TAG`). `unpack` still reports
`None`, so the version stays current for end-visibility. A transaction with
`begin_ts <= retired_at` keeps seeing it. A transaction with
`begin_ts > retired_at` treats it as absent and reads the B-tree, which holds
the same state.

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

### Rule 3: Retire current if already in B-tree

A current version is redundant with the B-tree when `b ≤ ckpt_max` and
`b < lwm`. Rule 3 only fires when it is the **last** version in the chain.
Otherwise superseded versions would hide the B-tree row without providing data.

Blocking Truncate Finalize holds the write lock, so no MVCC transaction is
open and a drop would be safe. Incremental GC (`gc_incremental`) holds only
the checkpoint **read** lock. Other transactions exist, and a cursor already
on the SkipMap re-reads the chain on every column fetch. Dropping the version
returns empty or NULL columns. That is why Rule 3 always **retires** instead
of `clear()`:

- it stamps a clock timestamp on the packed `end` (`RowVersion::retired_at`)
- `end()` still unpacks as `None`
- the version stays in the chain for transactions that began at or before
  that timestamp
- later transactions read the B-tree copy

`drop_current_if_in_btree` is on for every live GC path. Tests can still pass
`false` to keep a current version.

**Rule 3b** removes a retired version once `retired_at < lwm`. It runs after
Rule 3 so the same call can retire and reclaim. With no readers, `lwm` is
`u64::MAX` (or `retired_at` is `0` in the unit-test wrapper) and 3b drops
immediately. Truncate Finalize stays a memory win.

Live GC takes `retire_ts` from the clock, under the same `get_timestamp`
callback that samples the LWM, so a later `begin_ts` is always larger.

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

Truncate Finalize uses `_and_slots` (Rule 3 + 3b + unlink). Passive Finalize
uses `unlink_empty` (history + retire currents + unlink empty slots).
Mid-checkpoint write-set GC and incremental retire currents but leave empty
slots so write-set row ids still resolve. Writers retry via `*_still_mapped`
if an Arc was unlinked. Index unlink bumps `index_rows_epoch`.

### Passive `backfill_floor` publication

Passive Rule 2 needs `materialized_at <= backfill_floor` (`nbackfills`). After
`wal.checkpoint` + DB sync, MVCC calls `wal.publish_backfill` and keeps the
checkpoint guard until Finalize. On Passive `Busy`, finish without advancing
`nbackfills`.

Rule 3 is on for both modes. It retires rather than removes. Truncate
write-buffer reads provide B-tree fallthrough for sole materialized currents.
A transaction falls through for a retired version only when it began after
`retired_at`.

## Non-blocking Checkpoint Readiness

The GC rules are designed to work with both blocking and non-blocking
checkpoints — the LWM parameter naturally constrains what can be collected
when readers coexist with the checkpoint.

**What works today**: the GC rules (including retire and Rule 3b), LWM, recovery
protection, tombstone guard, under-lock empty-slot drain with writer retry,
write-buffer read filter.

**What needs work for non-blocking checkpoint**:

- More soak / concurrent-simulator coverage of Passive GC racing writers on
  empty-slot drain.

### Why Rule 3 retires instead of dropping

Incremental GC is the reason retirement exists. Truncate checkpoint is a
blocking write lock with no concurrent MVCC transactions, so dropping a
materialized current version there is safe. Incremental GC shares the
checkpoint read lock. Other transactions stay open, and a cursor already on
the SkipMap re-reads the chain per column fetch (`read_visible_into_record`).
If the version is gone, that fetch returns an empty row. That is the "total
balance changed" failure, a row read with NULL columns.

The same failure shows up if Passive Rule 3 `clear()`s a current version
while a reader is open
(`test_passive_concurrent_transfer_preserves_sum_and_count`). B-tree reads
are snapshot-isolated at the WAL mark, so falling through after retirement is
fine for a transaction that never sourced the row from the chain. A
positioned cursor is not: it must keep the chain copy until
`retired_at < lwm`.

Rule 3b's `retired_at < lwm` is "no transaction that could see it is open",
which is the condition under which physical removal is safe.

## Key Files

| File | Contents |
|------|----------|
| `core/mvcc/database/mod.rs` | `PackedTs` retirement tags, `gc_version_chain` / `gc_version_chain_with_retire`, `chain_is_write_buffer_for`, `compute_lwm`, `drop_unused_row_versions`, `gc_table_row_versions`, `gc_index_row_versions`, recovery tx cleanup in `commit_load_tx` |
| `core/mvcc/database/checkpoint_state_machine.rs` | `gc_checkpointed_versions`, auto-trigger wiring in `Finalize` |
| `core/mvcc/database/tests.rs` | 39 GC tests (unit, quickcheck, integration, e2e) |
