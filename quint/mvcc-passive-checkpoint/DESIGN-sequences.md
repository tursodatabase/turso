# Sequences and their compaction

This section describes how `nextval` and `setval` store a sequence in MVCC
mode, and how the checkpoint compacts the sequence. The Quint model
`MvccPassiveSequence.qnt` states the same design. Quint checks it with the
TLC model checker (`quint verify --backend tlc`).

## Where a sequence lives

A sequence keeps its state in a backing table with `value INTEGER PRIMARY
KEY` (`sequence_backing_table_sql`). So the row key is the sequence value.
The current value is the last row (`Last`) for an ascending sequence and the
first row (`Rewind`) for a descending sequence.

- A non-CYCLE `nextval` reads the current value, computes the next value and
  inserts one new row (`emit_disk_read_nextval`). In MVCC mode it runs in its
  own inner transaction (`op_sequence_begin_inner_tx`) and deletes nothing.
  Two calls that read the same current value insert the same key. The commit
  validation (`check_version_conflicts`) lets the first commit win. The other
  inner transaction rolls back and the statement retries with a new inner
  transaction (`op_sequence_commit_inner_tx`).
- A CYCLE `nextval` also deletes every other row in the same transaction
  (`emit_backing_table_compaction`, called under `if seq.cycle` in
  `emit_disk_read_nextval`). So a CYCLE table holds one row.
- `setval(v)` deletes every row and inserts `v`, in the user's transaction
  (`translate/expr/functions.rs`, "setval keeps its own cursor lifecycle").
  So `setval` can write a key that is lower than the current value.

Because a non-CYCLE `nextval` deletes nothing, its backing table grows by one
row for each call. The checkpoint removes the old rows.

## Compaction in the checkpoint

The checkpoint state `CompactSequences` runs after the checkpoint wrote its
rows and before the pager commit. `pending_sequence_compactions` lists the
non-CYCLE sequences. For each sequence, `SeqCompactDriver::step` does this
inside the checkpoint's pager transaction:

1. Find the watermark row. Use `Last` for an ascending sequence and `Rewind`
   for a descending sequence (`SeqCompactPhase::SeekWatermark`).
2. Scan the table. Take every row with a key that is not the watermark.

What happens to these rows depends on the mode.

- Blocking (TRUNCATE): delete the row from the B-tree, then clear its version
  chain (`purge_row_versions_during_checkpoint`). No transaction is open,
  because the checkpoint holds the checkpoint lock.
- Passive: do not change the B-tree or the chain. Record the row in
  `compacted`. The state machine moves the list to `pending_seq_deletes`. In
  the publish window, `apply_passive_publish_window_ordered` takes a new
  clock timestamp `T` under the clock lock. Then
  `apply_checkpoint_publish_window` calls `seqcompact_commit_delete(row, T)`
  for each recorded row. That function (`seqcompact_stamp_delete`) does one
  of three things:
  - If the chain has a live committed version, set its end to `T`.
  - If the chain has a version with an end, do nothing.
  - Otherwise the row is only in the B-tree. Add a marker version with no
    begin and end `T`.

  The next checkpoint sees an end in `(old boundary, snapshot]`, writes the
  delete to the B-tree and stamps the version. Then GC can drop it. The
  delete is not in the logical log.

## The rules of the design

| Rule | Code | Without it |
|------|------|------------|
| S1. The watermark follows the direction: `Last` for ascending, `Rewind` for descending. | `SeqCompactPhase::SeekWatermark` | `CompactionKeepsCurrent` fails. A descending sequence keeps its largest row and deletes the row with its current value, so a reader sees the sequence go back. Variant `noWatermarkDirection`. |
| S2. Passive mode records the deletes and applies them in the publish window. | `if self.passive` in `SeqCompactDriver::step` | `ReadsCorrect` fails. An inline delete and chain purge removes a row while a reader with an older snapshot still needs it as its current value. The reader then sees an empty table, and its `nextval` hands out the start value again, so `UniqueValues` also fails. Variant `noDeferDeletes`. |
| S3. The recorded deletes use a new clock timestamp, not the snapshot timestamp. | `apply_passive_publish_window_ordered` | `NoStrandedVersions` fails. A delete at the snapshot timestamp is at the new boundary, so no later checkpoint collects it. The version is never stamped, GC never drops it, and the B-tree row stays. This is a leak, not a wrong read. Variant `deleteTsSnapshot`. |
| S4. Compaction skips CYCLE sequences. | `pending_sequence_compactions` | No error. A CYCLE table has at most one row in every checkpoint, so compaction finds nothing to delete. Variant `noSkipCycle`. |
| S5. Blocking mode purges the version chain of each deleted row. | `purge_row_versions_during_checkpoint` | No error for the current value. The chain keeps a live version of a row that the B-tree no longer has. The deleted rows are below the watermark, so the current value does not change. Variant `blockingNoPurge`. |

## What the model checks

The model has one sequence with values `1..MaxVal`, two connections, the
passive or the blocking checkpoint, and GC with the rules of
`gc_version_chain`. A connection can read the current value, call `nextval`,
or call `setval`. It checks these properties in every reachable state.

- `ReadsCorrect`: every read of the current value, also the read inside
  `nextval`, returns the current value at the reader's begin timestamp. The
  expected value comes from the history of committed writes and compaction
  deletes, each at its timestamp.
- `CompactionKeepsCurrent`: a compaction delete never deletes the row that
  holds the current value at the timestamp of the delete. With
  `ReadsCorrect`, this means that a reader never sees a compaction change
  the value of the sequence.
- `UniqueValues`: without `setval` and CYCLE, no two committed `nextval`
  calls return the same value.
- `RecoveryCorrect`: if the process crashes now, the B-tree and the logical
  log records above `persistent_tx_ts_max` give the current value of the
  committed history. So `nextval` after recovery continues after every value
  that a committed transaction got.
- `NoStrandedVersions`: when the checkpoint is idle, every version whose last
  event is at or below the boundary is stamped, so GC can drop it.

The main configuration is module `main` in
`MvccPassiveSequenceConfigs.qnt`. It has values 1..3, two connections, a
clock bound of 8 and two checkpoint passes. The sequence is ascending,
`setval` is off, and the mode is passive. `make check` explores 4,008,777
distinct states in about 3.5 minutes on 3 workers and finds no error. TLC
also finds no error for a
descending sequence, a CYCLE sequence and the blocking mode. The variants
`descending`, `cycle` and `blocking` check these with the same constants.

The main configuration has `setval` off because of finding 1. With `setval`
on, the code breaks `CompactionKeepsCurrent`.

Four reachability checks fail as they must. So the model reaches these
states:

- `NeverCompacts`: the compaction records a delete.
- `NeverRecordsDelete`: the publish window applies a recorded delete.
- `NeverDeletesInBtree`: a later checkpoint writes the delete to the B-tree.
- `NeverRecoversAfterCompaction`: a crash replays log records on top of a
  B-tree with a compaction delete.

## Findings

1. A `setval` that commits during a passive checkpoint can lose the current
   value. TLC finds this with `setval` on (variant `setvalRace`):

   1. `nextval` commits 1 at ts 2, `nextval` commits 2 at ts 4.
   2. `setval(1)` prepares at ts 6. It ends rows 1 and 2 and inserts a new
      version of row 1.
   3. The checkpoint takes snapshot 4, below the `Preparing` commit. The
      B-tree at the snapshot has rows 1 and 2. The watermark is 2, so the
      compaction records a delete of row 1.
   4. `setval` finishes. The publish window takes ts 7.
      `seqcompact_stamp_delete` finds the live version of row 1, which is the
      `setval` row from ts 6, and ends it at 7.
   5. A reader at ts 8 finds no rows. The correct current value is 1. The
      next `nextval` returns the start value, which is 1 again.

   If the `setval` begins after the snapshot and commits before the publish
   window, the same bug can occur. This order is not verified with its own
   trace.

   The recorded delete names a row key, not a version. The publish window
   ends whatever version is live at that time. Only a version with a begin
   at or below the snapshot timestamp was in the B-tree that the compaction
   scanned. A possible fix: if the begin of the live version is above the
   snapshot timestamp, skip the row. With
   `FixStampSnapshotVersion = true` and `setval` on, TLC finds no error
   (variant `setvalFixed`, values 1..2, clock bound 6: 4,376,693
   distinct states). Not verified in Rust. A
   Rust test must show the bug first.
2. The code gives two reasons for the clock timestamp. "Above
   `durable_txid_max_new`" matters (rule S3). "Above every concurrent
   reader's begin" can be unnecessary for the current value. The deleted
   rows are below the watermark, and the watermark row stays visible to
   every reader that can see the deleted rows. This is not verified: the
   model has no timestamp that is above the boundary but below a reader's
   begin, because every clock timestamp is unique.
3. The CYCLE skip (S4) is not needed for correctness. Each CYCLE `nextval`
   deletes every other row in its own transaction, and each checkpoint
   writes a state at one snapshot. So the B-tree never holds two rows of a
   CYCLE sequence, and the watermark scan has nothing to delete. The skip
   only saves the scan.
4. The blocking purge (S5) does not protect the current value. It keeps the
   version store and the B-tree in agreement on the rows below the watermark.
   A full scan of the backing table can see the difference. The model does
   not check full scans.

## Not modeled

- The `sequence_allocations` registry (`register_sequence_allocation`,
  `remove_sequence_allocations`, `sequence_watermark`). It serves
  forward-only cursor scans, not `nextval` or compaction.
- The `is_called` flag, `min`/`max` other than `1..MaxVal`, increments other
  than 1 and -1, and the initial row that `CREATE SEQUENCE` writes.
- More than one sequence, and the root-page lookup in
  `pending_sequence_compactions` for a table without a checkpointed root.
- The exclusive-transaction path that skips the inner transaction and
  compacts inline. AUTOINCREMENT (`emit_disk_advance_past`) only moves the
  value forward, like `nextval`.
- The dual-cursor double look. A read looks at each chain once. Rule R3 in
  `DESIGN.md` covers the double look.
- Collection of each key in its own step, and the `begin_tx` window between
  the read mark and the clock lock. `MvccPassiveCheckpoint.qnt` covers both.
- The GC rules of the blocking mode. Both modes use the passive rules.
