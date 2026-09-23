# Lean proof: MVCC garbage collection keeps snapshot reads

This directory has a Lean 4 model of how the MVCC store handles one row. It
also has a machine-checked proof that garbage collection (GC) never changes
what a transaction reads. The proof also covers the Truncate checkpoint,
which writes the row to the B-tree, and the write, commit and rollback steps.

## Why this code

GC removes row versions from memory. A wrong GC rule causes data loss or a
snapshot isolation violation: a reader can miss its row, see a deleted row
again, or see a newer row.

The GC rules in `gc_version_chain` changed many times in 2026. Examples are
the commits "fix snapshot isolation violation in inline GC" (June) and
"reclaim stamped chains safely" (September), and issue #7638, where GC made a
deleted row come back. The rules depend on the visibility rules, the dual
cursor, the commit-time conflict check and the checkpoint. Tests cover only a
small part of the possible orders of these steps.

## What is proved

`MvccGc/Proof/Main.lean` has the results. `Reachable s` means that the model
can get to state `s` from any start value of the row, through any order of
these steps:

- Begin.
- Write (`MvccLazyCursor::insert`, the `Insert` opcode).
- Delete (`MvccLazyCursor::delete`, the `Delete` opcode).
- Prepare, with the first-committer-wins check.
- Commit, rewrite of transaction ids to timestamps, and finish.
- Abort, rollback and remove.
- Inline GC with each permitted low-water mark.
- Truncate checkpoint.

| Theorem | Statement |
|---|---|
| `snapshot_reads` | Every active transaction reads the row as of its begin timestamp, or its own last write. |
| `next_reader_reads` | A transaction that begins at this time reads the latest committed value. |
| `gc_keeps_reads` | An inline GC pass does not change what an active transaction reads. |
| `btree_durable` | The B-tree holds the value of the row at the durable boundary (`durable_txid_max`). |
| `no_panic` | No step takes a panic path of the engine, for example `resolve_begin_timestamp` on a missing transaction. |

The proof is an inductive invariant: `Inv` in `MvccGc/Proof/Basic.lean`, with
35 parts. `inv_initial` shows that the invariant holds at the start. For each
step, one theorem shows that the step keeps the invariant. `inv_gc` in
`MvccGc/Proof/Gc.lean` is the GC step. There is no `sorry` and no new axiom.

The proof of `inv_gc` has this core argument. Between two checkpoints, every
timestamp in the chain is above `ckpt_max`, and no version has a
materialization stamp. Then `gc_version_chain` is a filter. It removes
aborted garbage, and a version that ended at `e ≤ lwm` when that version is
not B-tree resident and a current version exists. Every open reader began
after `e`, so it cannot see the removed version. If the removed version hid
the B-tree row, a resident version in the chain still hides it.

## Model and Rust code

Each definition in `MvccGc/Rules.lean` copies one Rust function:

| Lean | Rust (`core/mvcc/database/mod.rs` unless noted) |
|---|---|
| `isBeginVisible`, `isEndVisible`, `isVisible` | `is_begin_visible`, `is_end_visible`, `RowVersion::is_visible_to` |
| `isBtreeInvalidating` | `RowVersion::is_btree_invalidating_version` |
| `chainIsWriteBuffer`, `btreeCovers` | `chain_is_write_buffer_for`, `btree_covers_chain_for_tx` |
| `mvccSide`, `btreeSideValid`, `readRow` | `skipmap_row_while_uncovered`, `query_btree_version_is_valid`, the dual cursor |
| `gcChain` | `MvStore::gc_version_chain` |
| `checkpointSelect` | `maybe_get_checkpointable_versions` (`checkpoint_state_machine.rs`) |
| `stampChain` | `MvStore::stamp_chain_materialized` |
| `rewriteVersion`, `rollbackVersion` | `RowVersion::rewrite_txid_to_timestamp`, `rollback_row_version` |
| `isWriteWriteConflict`, `hasVersionConflict` | `is_write_write_conflict`, `check_version_conflicts` |
| `deleteFromChain`, `insertVersion` | `delete_from_table_or_index`, `insert_version_raw` |
| `stepWrite`, `stepDelete` | `MvccLazyCursor::insert`, `MvccLazyCursor::delete` (`core/mvcc/cursor.rs`) |
| `computeLwm` | `compute_lwm` |
| `stepCheckpoint` | Truncate path of `CheckpointStateMachine`: collect, write, `TruncateWal`, `GcTableRows`, `Finalize` |

`MvccGc/System.lean` puts these definitions together as a transition system
for one row.

## Limits of the model

- One row. GC, visibility and the checkpoint decision work on one version
  chain at a time. Index rows use the same chain rules, but the model does
  not include their read path.
- Truncate checkpoint mode only. Passive mode is behind
  `--experimental-mvcc-passive-checkpoint`.
- Each step is atomic. This agrees with the per-chain locks. But the dual
  cursor reads the B-tree side and the SkipMap side at different times, and
  the model does not split a read.
- Prepare gets the end timestamp and does the commit-time conflict check in
  one step, so a transaction in `Preparing` always commits. The model does
  not include speculative reads or commit dependencies (Hekaton).
- The model does not include recovery from the logical log, statement
  (savepoint) rollback, `sqlite_schema` rows, sequences, or the contents of
  the finalized-state cache. The model keeps the cache, but no chain version
  refers to it.
- WAL assumption: after a Truncate checkpoint, the backfill boundary is equal
  to the materialization position of that checkpoint. `Action.realWal`
  states this. A Rust experiment agreed: both are `(3,0)` after the first
  checkpoint in a test database.

## Finding: the B-tree "cover" path is not safe for writes

The model also includes the case where the WAL backfill boundary is behind
the materialization position (`checkpoint _ true`). Then a sole stamped
current version can stay in the chain, and `btree_covers_chain_for_tx` makes
readers skip the chain. The search finds this sequence in one transaction:

1. The chain has one stamped current version V0 of the row. The B-tree has
   the same value, and the reader skips the chain.
2. `INSERT ... ON CONFLICT DO UPDATE` changes the row.
   `read_from_table_or_index` returns nothing, because the chain is covered.
   The cursor is on the B-tree row, so
   `insert_btree_resident_to_table_or_index` adds V1. V0 does not get an end.
3. `DELETE` of the row ends only V1.
4. A `SELECT` in the same transaction shows V0 again.

At commit, `check_version_conflicts` finds V0 as a live committed version and
returns `WriteWriteConflict`. Thus the wrong state does not get committed.
But the transaction reads a deleted row, and each upsert of a covered row
fails at commit.

`UPDATE` and `INSERT OR REPLACE` do not have this problem. In MVCC mode,
their bytecode has a `Delete` before the `Insert`, and
`delete_from_table_or_index` ends V0. The `DO UPDATE` branch of an upsert has
only an `Insert`.

At this time, a Truncate checkpoint removes every chain, because the backfill
boundary is equal to the stamp position. Thus the covered state does not
occur, and `btree_covers_chain_for_tx` returns true only in a state that the
engine does not get to. If a later change makes the cover path live, the
write path must read the chain without the cover rule.

## How to run

The Lean version is in `lean-toolchain` (Lean 4.34.0, no Mathlib).

```bash
make check           # check all proofs (lake build)
make search          # bounded search with the real WAL behavior; checks every invariant part
make counterexample  # bounded search without the WAL assumption; prints the trace above
```

`lake exe search <txs> <ops> <checkpoints> <max states> [flags]` does a
breadth-first search. `--no-backfill-behind` applies the WAL assumption.
`--check-invariant` also does a check of each invariant part on each state
(`MvccGc/Invariant.lean`). With 3 transactions, 3 writes and 2 checkpoints,
the search visits 3,805,316 states in approximately 90 seconds.
