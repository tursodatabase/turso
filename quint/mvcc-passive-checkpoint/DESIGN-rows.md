# Row protocol: aborts, dirty keys, GC samples and the WAL

This part extends the row model in `MvccPassiveCheckpoint.qnt` with the row
parts that `DESIGN.md` leaves out. Each part is behind a constant. When the
constant is off, the model is the same as before, state for state.

## Aborts

A writer can roll back while it is `Active` or `Preparing`
(`rollback_tx_inner`, mod.rs:7144). The model covers a `Preparing` writer
that rolls back before its log append. Three causes lead to this case. One
is a write-write conflict (`CommitState::Commit`). One is a failed commit
dependency (`WaitForDependencies`). One is a failed dirty mark
(`MarkDirtyKeys`).

1. The code stores `Aborted` first. Then it edits each chain in the write set
   (`rollback_version_chain`, mod.rs:7264). It removes each version that the
   writer began. On each version that the writer ended, it calls
   `set_end(None)`, which also sets `materialized_at` to `ORIGIN`
   (mod.rs:10997).
2. A marker for a B-tree row (begin None, end TxID) becomes begin None and
   end None. GC Rule 1 drops it (`gc_version_chain`, mod.rs:8581).
3. Between steps 1 and 2, readers, collection and stamping resolve the TxID
   to "not committed". That gives the same result as the edited chain, or it
   keeps more. So the model does the rollback in one step (`WriteAbort`).

A reader can read a `Preparing` version. The reader then uses the version as
a committed one. It also takes a commit dependency on the writer
(`is_visible_to`, `is_btree_invalidating_version` at mod.rs:11065,
`register_commit_dependency` at mod.rs:11129). If the writer rolls back, it sets `abort_now` on each
dependent reader, and the reader's commit fails with
`CommitDependencyAborted`. In the model, a read depends on each `Preparing`
writer of the same key whose timestamp is below the reader's `begin_ts`.
If one of those writers rolled back or is still `Preparing`, `ReadsCorrect`
does not count a wrong read.

The checkpoint needs nothing new for aborts. `checkpoint_snapshot_ts` stays
below every `Preparing` commit, and collection and stamping skip a TxID that
did not commit (`committed_ts`, `stamp_chain_materialized`).

## Dirty keys

Collection visits only the keys in `checkpoint_dirty_table_keys` and
`checkpoint_dirty_index_keys`, unless a full scan is required. Each entry
holds a stamp, which is the highest commit timestamp that marked the key.

1. Mark. In `MarkDirtyKeys` (`step_mark_dirty_keys`, mod.rs:2737), a commit
   marks each key in its write set with its commit timestamp
   (`mark_checkpoint_dirty_key`, a `fetch_max`). This is after the commit is
   `Preparing` with its timestamp, and before the log append and
   `CommitEnd`. If the mark fails, the commit fails and rolls back.
2. Collect. The walk reads the stamp of each key it visits. A key with a stamp
   at or below `snapshot_ts` is a prune candidate (`record_prune_candidate`,
   checkpoint_state_machine.rs:1734). In a full scan, the `DirtyStamps` phase
   records the candidates (`record_dirty_stamps`).
3. Prune. After GC, `prune_dirty_keys` (checkpoint_state_machine.rs:1741)
   removes each candidate. If a writer raised the stamp after collection, it
   marks the key again with the new stamp.
4. Rollback. If no version in the chain keeps the key dirty, a `Preparing`
   writer that rolls back removes its keys
   (`unmark_checkpoint_dirty_key_if_stamp`, mod.rs:7883).
5. Failed mark. A mark again in step 3 or 4 can fail on allocation. Then the
   stamp is lost. `require_checkpoint_full_scan` raises the lost stamp into
   `checkpoint_full_scan_min_snapshot_ts` and increments
   `checkpoint_full_scan_generation`. A checkpoint reads the generation at its
   start (checkpoint_state_machine.rs:2503). If the generation is not zero,
   the checkpoint does a full scan. At its end, `clear_checkpoint_full_scan`
   has two conditions: `snapshot_ts` is at or above the lost stamp, and the
   generation did not change. If both are true, it sets the generation to
   zero.

The property is `CollectsEveryChange`: when collection ends, every key with a
committed change in `(old boundary, snapshot_ts]` is in the frame. A commit
at or below `snapshot_ts` was not `Preparing` at the time of the snapshot. So
its mark was already in the map, and the property holds.

## GC samples

`sample_gc_lwm` (mod.rs:8139) reads `lwm` under the clock lock, before any
chain lock. The floor is also read before the chain loop (`gc_incremental`,
`gc_floor_reader_mark`). `gc_chain_now` (mod.rs:8154) then applies the rules
to each chain under its lock. If the sample is `u64::MAX` (no reader), it
computes `lwm` again. The model has `GcSample`, then `GcApply` on any
number of keys, then `GcEnd`.

A stale `lwm` that is not `MAX` is safe. A reader that begins after the sample
has a `begin_ts` above the clock, so it does not need a version that ended
before the sample. A stale floor is also safe: a new reader pins the end of
the WAL, which is at or above every `materialized_at`.

## WAL restart (not modeled)

`WalPos` is `(checkpoint_seq, frame)` and compares in that order
(mod.rs:4265). A passive checkpoint never restarts the WAL. The next pager
write restarts it in `try_restart_log_before_write` (wal.rs:5367). The restart
has three conditions. The writer holds read mark 0. The WAL is fully
backfilled (`max_frame == nbackfills > 0`). No reader holds read marks 1 to
4. A
reader at read mark 0 reads the database file only, and it does not block the
restart (`restart_ignores_db_file_readers`). In MVCC only the checkpoint
writes pages, so a restart can only happen at the start of the next
checkpoint's pager transaction.

This analysis is not verified with TLC.

- Every reader that is open across a restart has a read mark `(s, N)`, where
  `N` is the last frame of generation `s`, and it reads at read mark 0. A
  backfill takes read mark 0 exclusively, so it gets `Busy` while that reader
  is open. The database file does not change under the reader.
- After the restart, `backfill_floor` becomes `(s+1, 0)`, which is above the
  old reader's `(s, N)`. So `BackfillFloorBelowReaders` is false across a
  restart. This is harmless: no frame has a position between `(s, N)` and
  `(s+1, 0)`, so no `materialized_at` falls between them, and the GC floor
  gives the same result.
- `gc_floor_reader_mark` (checkpoint_state_machine.rs:2237) joins the lowest
  frame in read marks 1 to 4 with the current `checkpoint_seq`. That is
  correct, because a reader in marks 1 to 4 blocks the restart, so its frame
  is always in the current generation.

## Rules

| Rule | Code | Without it |
|------|------|------------|
| A reader that read a `Preparing` write takes a commit dependency. If the writer rolls back, the reader aborts. | `register_commit_dependency`, `abort_now` | `ReadsCorrect` fails (`abortsNoDependency`). The reader read the row value of a write that never happened. |
| A commit marks its keys dirty while it is `Preparing`, before `CommitEnd`. | `step_mark_dirty_keys` | `CollectsEveryChange` fails (`dirtyMarkAfterCommit`). The commit finishes, a checkpoint takes a snapshot above it and walks the dirty map before the mark. |
| A full scan stays required until a checkpoint with `snapshot_ts` at or above the lost stamp ends. | `clear_checkpoint_full_scan` | `CollectsEveryChange` fails (`dirtyClearFullScanEarly`). A re-mark fails for a `Preparing` commit. The next full scan has a snapshot below that commit and clears the flag. Then the commit is never collected. |
| If the generation changed, the clear does nothing. | `compare_exchange` in `clear_checkpoint_full_scan` | No error in the row model (`dirtyNoGenerationCas`). See Findings. |
| If the sample is `MAX`, `gc_chain_now` computes `lwm` again. | `gc_chain_now` | `ReadsCorrect` fails (`gcSampleNoRecheck`). GC samples "no reader", a reader begins and makes its first look, and Rule 3 empties the chain. This is rule R3 again. |
| Stamp only rows whose pager write finished. | `written_table_rowids` | Not modeled. `WriteRow` skips a table-row write in two cases. In one, this or an earlier checkpoint destroyed the table. In the other, the row is a delete in a B-tree that this checkpoint created. Those are schema cases, so the schema model must check them. |
| If a passive checkpoint gets `Busy`, it goes on without backfill. | `CheckpointWal` | Not needed as a separate rule. The model already lets a backfill copy zero frames, and `Busy` gives the same state. |

## Checks

The counts below describe earlier configurations. The checkpoint-stop results
near the end of this file describe the current configurations.

All runs use one key, five clock timestamps and the invariants of the main
configuration plus `CollectsEveryChange`. TLC finds no error in any of them.
The counts are the ones that `make variants` gives. The time is for `quint verify --backend tlc` with 3 workers.

| Configuration | Readers | Checkpoints | Distinct states | Time |
|--------|---------|-------------|-----------------|------|
| `aborts` | 1 | 2 | 3,164,577 | 2 minutes |
| `abortsKeepMaterialized` | 1 | 2 | 3,338,621 | 2 minutes |
| `dirtyKeys` (with aborts, one failed re-mark) | 1 | 3 | 45,359,101 | 26 minutes |
| `dirtyNoGenerationCas` (two failed re-marks) | 0 | 3 | 2,517,764 | 2 minutes |
| `gcSample` | 2 | 2 | not counted | not run in full |

`gcSample` is the largest variant. It is too large for a full run. The same configuration with four clock timestamps has
8,066,627 distinct states, and TLC finds no error.

## Findings

1. A rollback leaves the restored current version with `materialized_at =
   ORIGIN` (`set_end(None)` resets it). The B-tree still has that version, but
   GC Rule 3 cannot drop it. No later checkpoint stamps it again, because the
   key has no change in its range. The version stays in memory until the next
   write to the row. This also happens for every update or delete of an
   `Active` transaction that rolls back, for example after a constraint error.
   TLC reaches this state (the `NeverLeavesUnstampedCurrent` check fails).
   Rollback can keep the old `materialized_at`. TLC finds no error with that
   rule (`abortsKeepMaterialized`). This is a memory leak, not a
   correctness bug.
2. The generation `compare_exchange` in `clear_checkpoint_full_scan` is not
   needed in the row model. A re-mark can fail at the same time as a clear
   only in a rollback. The stamp that it loses belongs to a version that GC
   already dropped, so there is nothing left to collect. A failed mark in
   `seqcompact_commit_delete` (mod.rs:9002) is a third source, and it is not in
   this model. Keep the check for that case.

## Not modeled

- Aborts of `Active` writers. Their versions are invisible to readers. Their
  only effect on the checkpoint is the reset of `materialized_at` in
  finding 1.
- Aborts after the log append. `cleanup_dropped_commit` treats such a commit
  as committed.
- WAL restart. See the analysis above, which is not verified.
- Index keys. They use the same dirty-key code as table keys, with their own
  map.

## Checkpoint stops and later stamps

`CheckpointFailures` lets a pass stop at each modeled phase. `CheckpointFail`
keeps committed WAL frames, the published boundary, and completed backfill.
It ends the owner's pager pin only while the pass owns a pager transaction.
The MVCC snapshot stays registered (`MvccPassiveCheckpoint.qnt:788`). This matches cleanup at
`core/mvcc/database/checkpoint_state_machine.rs:1079`.
The cleanup discards staged root operations at line 1087 and ends the pager
read transaction at line 1098 of that file.

The pager commit and publish stay separate (`MvccPassiveCheckpoint.qnt:619` and `MvccPassiveCheckpoint.qnt:643`).
The code ends the pager transaction at
`core/mvcc/database/checkpoint_state_machine.rs:3333`.
It can yield before publish at line 3341 of that file.
The boundary store follows at line 2232 of that file.

`NoStrandedVersions` requires a successful completed pass to leave no
unstamped version whose resolved last event is at or below `durable`
(`MvccPassiveCheckpoint.qnt:1011`). The property allows a failed pass to leave repair work.
It then checks the result of the next successful pass.
It does not prove that a later pass will finish.

`failedPublish` models probe 4 in the supplied Rust results.
A nonempty pass publishes, fails before stamping, and leaves versions below
the collection boundary. A later successful pass does not collect them.
`FixRecollectUnstamped` makes collection include those versions
(`MvccPassiveCheckpoint.qnt:580`). The later pass writes their row and stamps them
(`MvccPassiveCheckpoint.qnt:724`). This is a proposed model fix. A Rust fix is not verified.

`NeverFailsAfterPublish` requires a nonempty pager commit, an unstamped
version below the published boundary, and a failure at `published`
(`MvccPassiveCheckpoint.qnt:1061`). `NeverRepairsVersions` requires that failure first.
A later pass must then stamp an unstamped version below its old boundary
(`MvccPassiveCheckpoint.qnt:1062`). The witness cannot pass through a no-op checkpoint.

## Connection owners and mode T

The checkpoint stores its owner at start (`MvccPassiveCheckpoint.qnt:537`).
Its pager commit releases that connection's pin (`MvccPassiveCheckpoint.qnt:619`).
The MVCC snapshot keeps its begin timestamp and remains registered.

`CheckpointBeginPager` skips the pager transaction and publish for an empty pass
(`MvccPassiveCheckpoint.qnt:602`). A new metadata boundary still counts as work
(`3a95ac963:core/mvcc/database/checkpoint_state_machine.rs:2467`).
The model tracks the transaction with `pagerOwned`.
Only failure during `writing` can clear an owner pin through cleanup
(`MvccPassiveCheckpoint.qnt:788`).

An explicit owner must be between statements, in the `active` reader state
(`MvccPassiveCheckpoint.qnt:537`). This represents the active-statement check
(`3a95ac963:core/connection.rs:2590`, `3a95ac963:core/connection.rs:4833`).
A stale pinned mark stops the pass before pager writes
(`MvccPassiveCheckpoint.qnt:602`). The owner keeps its pin.
Rust reports `BusySnapshot` for that write upgrade
(`3a95ac963:core/storage/wal.rs:3598`).

The pager commit changes the connection transaction state at
`core/storage/pager.rs:3429` at base `3a95ac963`.
`set_tx_state` only writes that field (`core/connection.rs:4845` at that base).
The MVCC transaction id has a separate setter (`core/connection.rs:4886`).
The model therefore keeps the registered snapshot when it releases the pager pin.

`FixRefuseCheckpointOwner` refuses an owner with an open reader.
The main configuration enables this proposed fix.
The code-today owner variants leave the fix off.

Mode T means the passive flag is on and the caller requests TRUNCATE.
`CheckpointPublish` and `CheckpointPublishBoundary` form separate steps
(`MvccPassiveCheckpoint.qnt:643` and `MvccPassiveCheckpoint.qnt:649`).
`ReaderBegin` can occur between them.
The code takes this publish path without the clock wrapper at
`core/mvcc/database/checkpoint_state_machine.rs:3356`.
The wrapper that orders a passive publish is at line 2250 of that file.

The row model has no root map or header cookie.
Its split publish therefore adds an interleaving point but cannot detect
half-published schema state. That schema race is not verified.
A pinned reader makes mode T fail at `published`, before the stamp
(`MvccPassiveCheckpoint.qnt:788`). `truncateFlagOn` checks the resulting stranded versions.
`truncateFlagOnFixed` and `reachTruncateRepair` check the later stamp repair.
The model keeps WAL frame numbers. It does not model WAL restart.

The owner model keeps the MVCC snapshot after it releases the pager pin
(`MvccPassiveCheckpoint.qnt:238`, `MvccPassiveCheckpoint.qnt:239`).
`GcKeepsVersionsAboveReaders` checks both sets (`MvccPassiveCheckpoint.qnt:1030`).
The code floor still counts the retained registered mark (`MvccPassiveCheckpoint.qnt:824`).
`ownCheckpointGc` therefore expects no GC violation.
Its earlier expected failure assumed a different relation between those sets.
The pinned-only control, `ownCheckpointGcPinnedFloor`, now expects the GC check to fail.
It ignores that registered mark after the pin disappears.
The new simulator trace uses a registered owner and a nonempty pager commit.
At step 25, owner `r1` has begin timestamp 5 and mark 0, then commits frame 1.
The first empty pass keeps every reader pin. No `committed` state has frame 0.
The GC check sees an ended version stamped at frame 1 above the retained mark 0.
A wrong read from this GC step is not verified.
`reachReaderMarkBelowFloor` checks both sets too (`MvccPassiveCheckpoint.qnt:1078`).
TLC reaches that retained-mark state after 38,049 states.

| Owner control | Result | Distinct states |
|---|---|---:|
| `ownCheckpoint` | `ReadsCorrect` fails as expected | 148,132 |
| `ownCheckpointBackfillFloor` | `ReadsCorrect` fails as expected | 146,017 |
| `ownCheckpointKeepsMark` | `ReadsCorrect` fails as expected | 149,126 |
| `ownCheckpointKeepsMarkBackfillFloor` | `ReadsCorrect` fails as expected | 150,509 |
| `ownCheckpointGc` | No GC-floor violation | 32,893,504 |
| `ownCheckpointGcBackfillFloor` | `GcKeepsVersionsAboveReaders` fails as expected | 128,575 |
| `ownCheckpointGcPinnedFloor` | `GcKeepsVersionsAboveReaders` fails as expected | 135,575 |
| `reachStampedAboveReader` | `NeverStampedAboveReader` fails as expected | 17,532 |
| `reachReaderMarkBelowFloor` | `NeverReaderMarkBelowBackfillFloor` fails as expected | 38,049 |

## Logical log truncate ordering

`CheckpointTruncateDecide` checks the log against the published boundary
(`MvccPassiveCheckpoint.qnt:697`). With `SplitLogTruncate`, completion occurs in
`CheckpointTruncateApply` (`MvccPassiveCheckpoint.qnt:708`).
`WriteLogAppend` can occur between these actions.
The code checks the boundary at
`core/mvcc/persistent_storage/logical_log.rs:1089`.
It submits the truncate at line 1072 of that file.
The storage lock ends before completion at
`core/mvcc/persistent_storage/mod.rs:238`.
Whether an I/O backend permits the model's destructive ordering is not verified.

`logTruncateRace` checks recovery after the split truncate.
`reachTruncateAppend` requires a nonempty pager commit and a truncate that
actually discards an append above the boundary (`MvccPassiveCheckpoint.qnt:1063`).
`logTruncateAtomic` checks the unsplit control at the same bounds.
This control prevents the race. It is not a repair after a failure.

## Stamp and publish rules from the Rust fix

The read-only fix worktree is
`/home/peristocles/.t3/worktrees/turso/t3-7761a9a5-3a389ac9d7146b01`.
Its checked head is `633aeb51a`. Source references in this section use that head.
The earlier sections use the requested base, `3a95ac963`.

`FixStampAtPagerCommit` stamps each written chain in `CheckpointCommit`
(`MvccPassiveCheckpoint.qnt:619`). The stamp is the new WAL frame number.
The code calls `stamp_committed_versions` immediately after pager commit at
`core/mvcc/database/checkpoint_state_machine.rs:3301`.
The helper reads the pager WAL position at
`core/mvcc/database/checkpoint_state_machine.rs:3569`.
It stamps only versions whose resolved terminal event is within the snapshot
(`core/mvcc/database/mod.rs:8675`).
The model uses one atomic commit-and-stamp step at these one-key bounds.
The Rust helper locks each chain separately. Interleavings between those
chain locks are not verified.

`FixCleanupPublishes` makes `CheckpointFail` publish a committed pass before
it clears the checkpoint state (`MvccPassiveCheckpoint.qnt:788`).
`ApplyCheckpointPublish` applies the same boundary update for normal publish
and cleanup (`MvccPassiveCheckpoint.qnt:654`).
Rust selects this cleanup at
`core/mvcc/database/checkpoint_state_machine.rs:1040`.
It uses the passive window at
`core/mvcc/database/checkpoint_state_machine.rs:1044` and the blocking window at
`core/mvcc/database/checkpoint_state_machine.rs:1048`.
Publish errors, allocation failures, and failures inside the publish window
are not verified by these atomic model steps.

`rustFix` checks all main invariants and `NoStrandedVersions` with both constants.
It also refuses an owner with an open snapshot, as the row main configuration does.
`rustFixOwner` turns that refusal off and checks the same promises through `Safety`
(`MvccPassiveCheckpoint.qnt:1052`). The saved trace has a wrong read.
The reader expects 0 at begin timestamp 1 and receives 2 after backfill.
The regenerated owner trace includes the new pager phase and a real frame commit.
The two Rust fixes do not restore the owner's pager pin in this model.
Rust reproduction of this trace is not verified.

`rustFixTruncate` checks mode T with both constants.
`rustFixPinnedFloor` uses the lowest pinned mark as its GC floor.
All four variants use one key, one reader, four timestamps, and two passes.
`reachCleanupPublish` requires failure after a nonempty pager commit and an
actual cleanup publish (`MvccPassiveCheckpoint.qnt:1057`).
`reachEarlyStampGc` requires GC to drop a version stamped by the current pass
while a registered reader remains open (`MvccPassiveCheckpoint.qnt:914`).
`reachEarlyStampGcAfterFailure` requires a published-pass failure first, then
GC after a later empty pass completes (`MvccPassiveCheckpoint.qnt:920`).
The first GC witness uses the lowest pinned mark as a control.
The after-failure witness uses the code GC floor.
Both GC witnesses record the actual removal in `AfterGc`
(`MvccPassiveCheckpoint.qnt:908`).
These witnesses use the same bounds as `rustFix`.

The saved traces are in `traces/bugs/`. The owner trace is
`MvccPassiveCheckpoint-rustFixOwner.itf.json`.
The early-stamp GC trace is
`MvccPassiveCheckpoint-reachEarlyStampGc.itf.json`.
The simulator did not capture the after-failure GC witness in 200,000 walks
of 40 steps. TLC reached it after 144,369 states.
An ITF trace for that witness is not verified.

## Checkpoint-stop results

All new leak variants use one key and two passes.
The passive failure variants use no reader and three clock timestamps.
The mode T variants use one reader and four clock timestamps.
The truncate-order variants use no reader, three timestamps, and one pass.
Each fixed variant checks every main invariant and `NoStrandedVersions`.
The results below count distinct TLC states with eight workers.
Counts from an interrupted search are not complete state-space counts.

| Configuration | Result | Invariant | Distinct states |
|---|---|---|---:|
| `failedPublish` | Fail (expected) | `NoStrandedVersions` | 2,790 |
| `failedPublishFixed` | Pass | All listed invariants | 10,167 |
| `reachFailedPublish` | Fail (expected) | `NeverFailsAfterPublish` | 731 |
| `reachRepairedVersions` | Fail (expected) | `NeverRepairsVersions` | 4,685 |
| `logTruncateRace` | Fail (expected) | `RecoveryCorrect` | 205 |
| `reachTruncateAppend` | Fail (expected) | `NeverLosesAppendAtTruncate` | 503 |
| `logTruncateAtomic` | Pass | All listed invariants | 615 |
| `truncateFlagOn` | Fail (expected) | `NoStrandedVersions` | 34,195 |
| `truncateFlagOnFixed` | Pass | All listed invariants | 300,115 |
| `reachTruncateRepair` | Fail (expected) | `NeverRepairsVersions` | 73,059 |
| `refuseCheckpointOwner` | Pass | All listed invariants | 210,694 |
| `rustFix` | Pass | All listed invariants | 502,396 |
| `rustFixOwner` | Fail (expected) | `Safety` | 23,598 |
| `rustFixTruncate` | Pass | All listed invariants | 457,428 |
| `reachCleanupPublish` | Fail (expected) | `NeverCleanupPublishes` | 1,086 |
| `rustFixPinnedFloor` | Pass | All listed invariants | 1,037,362 |
| `reachEarlyStampGc` | Fail (expected) | `NeverGcEarlyStampWithReader` | 7,970 |
| `reachEarlyStampGcAfterFailure` | Fail (expected) | `NeverGcEarlyStampAfterFailure` | 144,369 |
