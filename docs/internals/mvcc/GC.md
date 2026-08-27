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

`drop_current_if_in_btree` controls whether Rule 3 runs. It is on for every GC
path, but the two checkpoint modes act differently when it fires:

- **Truncate** removes the version. Its blocking lock excludes every MVCC
  transaction during the write phase, so no cursor can be positioned on it.
- **Passive** cannot remove it: a transaction may have a cursor positioned on
  that version, and the cursor re-reads the chain on every column fetch — if the
  version vanished it would return an empty row (that was the "total balance
  changed" corruption). Passive therefore **retires** the version instead: it
  stamps it with a fresh clock timestamp (`RowVersion::retired_at`, stored in
  the spare tag bits of the packed `end`). The version still reads as current
  (`end() == None`) and stays in the chain. Passive fires Rule 3 only when the
  version is materialized and every reader's WAL mark can reach it
  (`materialized_for_readers`) and `b < lwm`.

A retired version is then handled by the visibility predicates
(`is_visible_to`, `is_btree_invalidating_version`):

- a transaction with `begin_ts <= retired_at` sees it exactly as before, so an
  already-positioned cursor keeps its row;
- a transaction with `begin_ts > retired_at` treats it as absent: it neither
  shows it nor hides the B-tree row, so the row is read from the B-tree, which
  holds the same state at that transaction's pinned WAL mark.

**Rule 3b** removes a retired version once `retired_at < lwm`, i.e. once every
transaction that could still see it has ended. With no transaction open this
happens in the same checkpoint (write-set GC retires, Finalize sweep reclaims),
so a single writer's version store stays flat across checkpoints. The retire
timestamp comes from the clock (`MvStore::take_retire_ts`, or the
`get_timestamp` callback on the inline path), which is what guarantees every
later `begin_ts` is larger.

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
`unlink_empty` (history + retire currents + unlink empty slots). Mid-Passive
write-set GC and incremental retire currents but leave empty slots so write-set
row ids still resolve. Writers retry via `*_still_mapped` if an Arc was
unlinked; index unlink bumps `index_rows_epoch`.

### Passive `backfill_floor` publication

Passive Rule 2 needs `materialized_at <= backfill_floor` (`nbackfills`). After
`wal.checkpoint` + DB sync, MVCC calls `wal.publish_backfill` and keeps the
checkpoint guard until Finalize. On Passive `Busy`, finish without advancing
`nbackfills`.

Rule 3 is on for both modes; Passive retires rather than removes (see Rule 3
above). Truncate write-buffer reads provide B-tree fallthrough for sole
materialized currents; under Passive a transaction falls through only for
versions retired before it began.

## Non-blocking Checkpoint Readiness

The GC rules are designed to work with both blocking and non-blocking
checkpoints — the LWM parameter naturally constrains what can be collected
when readers coexist with the checkpoint.

**What works today**: all four GC rules, LWM, recovery protection, tombstone
guard, under-lock empty-slot drain with writer retry, write-buffer read filter.

**What needs work for non-blocking checkpoint**:

- More soak / concurrent-simulator coverage of Passive GC racing writers on
  empty-slot drain.

### Why Passive retires instead of dropping (history)

Before retirement existed, Passive kept every row's last version forever: the
version store grew without bound under a single writer, and each checkpoint's
GC sweep walked a larger store than the last. Simply turning Rule 3 on for
Passive was tried and corrupted data
(`test_passive_concurrent_transfer_preserves_sum_and_count`, "total balance
changed"; `test_conflict_abort_ckpt_indexed_update_savepoint_integrity_check_passive`,
"row missing from index"). Two separate things had to be true for it to work:

1. **B-tree reads must be snapshot-isolated.** An MVCC transaction takes a WAL
   read mark at `BEGIN`, and `mvcc_refresh_if_db_changed` must not advance the
   connection's WAL view while that mark is held; the checkpoint's backfill
   stops at the lowest reader mark (`determine_max_safe_checkpoint_frame`).
   With that in place a transaction that falls through to the B-tree reads the
   page as of its own mark, whatever a later Passive checkpoint wrote.
2. **A positioned cursor must not lose its version.** The dual cursor re-reads
   the chain on every column fetch (`read_visible_into_record`); a version
   removed between positioning and fetch yields an empty row. That is why the
   version is retired rather than removed: transactions that could have
   positioned on it still see it, and only transactions that begin afterwards
   — which never sourced it from the chain — read the B-tree instead.

Rule 3b's `retired_at < lwm` is exactly "no transaction that could see it is
open", which is the condition under which physical removal is safe.

## Key Files

| File | Contents |
|------|----------|
| `core/mvcc/database/mod.rs` | `gc_version_chain`, `chain_is_write_buffer_for`, `compute_lwm`, `drop_unused_row_versions`, `gc_table_row_versions`, `gc_index_row_versions`, recovery tx cleanup in `commit_load_tx` |
| `core/mvcc/database/checkpoint_state_machine.rs` | `gc_checkpointed_versions`, auto-trigger wiring in `Finalize` |
| `core/mvcc/database/tests.rs` | 39 GC tests (unit, quickcheck, integration, e2e) |
