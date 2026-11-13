# What the model covers, and what the code does around it

This file lists each MVCC mechanism in the code, the checkpoint modes where it
runs, and whether a spec in this folder models it. It also sorts each
mechanism into design (a guarantee needs it), optimization (the same result
with less work or memory), or dead (it runs in a mode where nothing needs it).
The mechanism table comes from a source read. The checkpoint-stop section below
gives later model results and supplied Rust probe results. Other bug leads
remain not verified.

The mechanism table uses code at commit 719fcc411. The checkpoint-stop section
uses the requested base, 3a95ac963. Short names in this file:

- `mod.rs` is `core/mvcc/database/mod.rs`.
- `csm.rs` is `core/mvcc/database/checkpoint_state_machine.rs`.
- `cursor.rs` is `core/mvcc/cursor.rs`.
- Row spec is `MvccPassiveCheckpoint.qnt`. Schema spec is `MvccPassiveSchema.qnt`. Sequence spec is `MvccPassiveSequence.qnt`.

## The three modes

The code has two switches, not one. The first is the flag
`experimental_mvcc_passive_checkpoint` (`MvStore::uses_passive_checkpoint`, mod.rs:7952).
The second is the `CheckpointMode` of one checkpoint. Together they give three modes.

| Mode | Flag | Checkpoint mode | How it starts |
|---|---|---|---|
| B (blocking) | off | Truncate | Auto checkpoint (mod.rs:3706), explicit checkpoints (connection.rs:2615) |
| P (passive) | on | Passive | Auto checkpoint (mod.rs:3702), explicit PASSIVE |
| T (flag on, TRUNCATE) | on | Truncate | `PRAGMA wal_checkpoint(TRUNCATE)` (vdbe/execute.rs:593, connection.rs:2605), journal mode change (vdbe/execute.rs:18792) |

The code gates some paths on the flag and others on the checkpoint mode or on
"blocking lock held". Mode T gets a mix of both.
The row and sequence specs model P and T. The sequence spec also has a mode
"blocking", which matches B for locks but uses the P GC rules
(`MvccPassiveSequence.qnt:80`). The T mode uses `Mode = "truncateFlagOn"`.
The checkpoint-stop section gives its results and limits.

What the blocking lock gives in each mode:

- In B, every MVCC transaction holds `blocking_checkpoint_lock` in read mode for its whole life
  (`begin_tx` takes it at mod.rs:6729, `remove_tx` releases it at mod.rs:6862). The checkpoint
  takes it in write mode at `AcquireLock` (csm.rs:2516). `TursoRwLock::write` is a try lock
  (core/storage/wal.rs:350). So a B checkpoint does not wait for readers. It fails with `Busy`
  if any transaction is open. An auto checkpoint drops that error (mod.rs:3755-3759).
  Conclusion: while a B checkpoint runs, `txs` is empty and no commit is `Preparing`.
- In P and T, `begin_tx` never takes the lock (mod.rs:6423, mod.rs:6728-6729). So in T the
  write lock excludes other checkpoints and VACUUM, but not readers or writers. T takes the
  lock only at `CommitPagerTxn` (csm.rs:3246-3252), after collection and after
  `CompactSequences`.

## 1. Table of mechanisms

The kind column has three values. "Design" means a guarantee needs it. "Optimization" means
the same result with less work or memory. The row names the rule that it replaces. "Dead in B"
means that it runs in B but nothing in B needs it.

### Commit path

| Mechanism | Code | Modes (gate) | Modeled? | Kind |
|---|---|---|---|---|
| `Initial`: take end_ts and publish `Preparing` under the clock lock | mod.rs:3087-3310 | B, P, T (no gate) | Row spec `WritePrepare` | Design (R1 depends on it) |
| `Commit`: write-write validation | mod.rs:3312-3333, `check_rowid_for_conflicts` mod.rs:2071 | B, P, T (no gate) | Not modeled (DESIGN.md "leaves out write-write conflicts") | Design |
| `WaitForDependencies`, commit dependencies, `abort_now` | mod.rs:3335, `register_commit_dependency` mod.rs:11129, rollback sets `abort_now` mod.rs:7172 | B, P, T (no gate) | Row spec `WriteAbort` with `Aborts`, `GuardReadersAbortOnDependency` | Design |
| `BuildLogRecord` and `canonical_log_id` | mod.rs:2458-2470 | B, P, T (no gate) | Schema spec (ids, `FixPersistIds`) | Design today. SIMPLIFY step 3 removes it |
| `MarkDirtyKeys` | mod.rs:2728, `step_mark_dirty_keys` mod.rs:2737, `mark_tx_write_set_dirty` mod.rs:7905, `mark_checkpoint_dirty_key` mod.rs:7861 | B, P, T (no gate) | Row spec `WriteMarkDirty`, `WriteLateMark` (`DirtyKeys = TRUE`) | Optimization (replaces a walk of the whole version store). Dead in B, see section 2 |
| Logical log append and fsync | mod.rs:3408-3586 | B, P, T | Row spec `WriteLogAppend` (one atomic step) | Design |
| Group commit (`AwaitGroupCommit`, `SyncGroupPrefix`, `GroupPrefixSynced`) | mod.rs:1449, mod.rs:3448, mod.rs:3558, mod.rs:3572, `group_commit.rs` | B, P, T (`group_commit_enabled`) | Not modeled | Optimization (replaces one fsync per commit) |
| `CommitEnd`, `publish_committed_tx` | mod.rs:3629, mod.rs:3677, mod.rs:7346 | B, P, T | Row spec `WriteFinish`. Schema spec `DdlFinish` for the header | Design |
| TxID to timestamp rewrite | `rewrite_live_versions_for_committed_tx` mod.rs:7383 | B, P, T | Not modeled (the model stores timestamps) | Optimization (replaces a lookup of the tx state on each read) |
| `finalized_tx_states` insert at `remove_tx` | mod.rs:6829-6835, mod.rs:6871 | B, P, T | Not modeled | Design (TxID resolution after the tx leaves `txs`) |
| `gc_incremental` on commit | call mod.rs:3730-3732, body mod.rs:8203. Trigger `should_gc` mod.rs:8123 | B, P, T. In B it takes the lock in read mode (mod.rs:8207-8219) and unlinks empty slots (mod.rs:8296). P and T leave empty slots | Row spec `Gc`, `GcSample`, `GcApply`, `GcEnd` (GC at any moment) | Optimization (replaces "GC only at checkpoint end") |
| `gc_in_progress`, `gc_last_lwm` short cut, `gc_table_cursor` budget | mod.rs:8226-8240, mod.rs:8019, mod.rs:8263-8310 | B, P, T | Not modeled | Optimization (scheduling only) |
| Auto checkpoint trigger | mod.rs:3700-3720 | B, P, T (`should_checkpoint`) | Not modeled | Design (bounds the log) |
| Rollback | `rollback_tx_inner` mod.rs:7144, `rollback_version_chain` mod.rs:7264 | B, P, T | Row spec `WriteAbort` (Preparing only) | Design |
| Rollback resets `materialized_at` | `set_end(None)` mod.rs:7273, mod.rs:10997 | B, P, T | Row spec `AbortKeepsMaterialized` | Leak (DESIGN-rows finding 1) |
| Rollback unmarks dirty keys | mod.rs:7209, `unmark_checkpoint_dirty_key_if_stamp` mod.rs:7883 | B, P, T (no gate) | Row spec `WriteAbort` with `DirtyKeys` | Optimization bookkeeping. Dead in B |
| `cleanup_dropped_commit` (abort after log append counts as commit) | mod.rs:7292 | B, P, T | Not modeled | Design |
| Exclusive tx and commit lock | mod.rs:7400, mod.rs:7673, mod.rs:7713 | B, P, T | Not modeled (only its cookie effect, schema S8) | Design |

### Begin and read path

| Mechanism | Code | Modes (gate) | Modeled? | Kind |
|---|---|---|---|---|
| Lifetime read guard on the blocking lock | mod.rs:6410-6420, mod.rs:6729, release mod.rs:6862 | B only (gate: flag off) | Sequence spec `TxBegin` waits while a blocking checkpoint runs (MvccPassiveSequence.qnt:246) | Design for B |
| Read mark taken before the clock lock | mod.rs:6757 | B, P, T | Row spec `ReaderPin`, `ReaderBegin`. Schema spec S21 | Design |
| `schema_generation` check at begin | mod.rs:6768. Bump at csm.rs:2147 | B, P, T (no gate on the check or the bump) | Schema spec S10 (`noGenerationGate`: not needed) | Not needed in the model |
| `schema_still_valid_for_tx` | mod.rs:7978 | P, T (gate: flag) | Schema spec S10 | Not needed in the model |
| `Busy` while a DDL commit has not published its cookie | mod.rs:6779, mod.rs:7685 | B, P, T | Schema spec S8 (`noBusy` fails) | Design |
| Root page to table id at begin_ts | cursor.rs:613-623 | P, T use `try_get_` and return `SchemaUpdated`. B uses `get_` (gate: flag) | Schema spec S7 (`noOpenFallback`: not needed) | Not needed in the model |
| `is_btree_readable_at` (binding covers begin_ts, `materialized_at <= read_mark`) | mod.rs:10695, cursor.rs:833-860 | B, P, T | Schema spec S1, S2, S3, S15, S16 | S1 design. S2, S3 not needed in the model |
| Dual cursor looks at the chain twice | cursor.rs:977, cursor.rs:1064, cursor.rs:1233 | B, P, T | Row spec `ReadLooksAtChainTwice`, `ReaderReadStart`, `ReaderReadEnd` | Design today. SIMPLIFY step 6 |
| B-tree covers the chain (skip the chain, read the B-tree) | `btree_covers_chain_for_snapshot` mod.rs:6070-6087, `chain_is_write_buffer_for` mod.rs:6024, cursor.rs:1203 | B only (with the flag on, mod.rs:6076 returns false) | Not modeled | Optimization (replaces "read the chain, else the B-tree") |
| `checkpoint_in_progress` test inside it | mod.rs:6079 | Reached only in B, but only the P/T path sets the flag (csm.rs:2486-2496) | Not modeled | Dead in B (always false) |
| Index lockstep merge | `index_chain_invalidates_btree` mod.rs:6099, cursor.rs:524, debug compare cursor.rs:667-672 | B, P, T | Not modeled (no indexes in the row spec) | Optimization (replaces `query_btree_version_is_valid` per key, mod.rs:6126) |
| Row map epochs | cursor.rs:479-484, cursor.rs:907-948, mod.rs:8829, mod.rs:8841 | B, P, T | Not modeled | Optimization (replaces a fresh skip list seek per step) |
| Sequence allocation registry | mod.rs:6881, mod.rs:6920, mod.rs:6954 | B, P, T | Not modeled (DESIGN-sequences "Not modeled") | Optimization for forward-only scans |

### Checkpoint states

| Mechanism | Code | Modes (gate) | Modeled? | Kind |
|---|---|---|---|---|
| `PrepareCheckpoint`: `checkpoint_in_progress` CAS | csm.rs:2479-2499 | P, T (gate: flag) | Row spec `ck.pc` (one checkpoint at a time) | Design for P |
| `AcquireLock` | csm.rs:2511-2534 | B only (P and T skip it, csm.rs:2501-2509) | Sequence spec `CheckpointStart` with `Mode == "blocking"` | Design for B |
| Snapshot below every `Preparing` commit | `checkpoint_snapshot_ts` mod.rs:7843 | B, P, T (no gate) | Row spec `SnapshotTs`, R1 `GuardSnapshotBelowPreparing` | Design for P and T. Dead in B (no `Preparing` tx exists while B holds the lock) |
| Full-scan generation | read csm.rs:2503, csm.rs:2528. `require_checkpoint_full_scan` mod.rs:7925. `clear_checkpoint_full_scan` mod.rs:7932, called csm.rs:3523 | B, P, T (no gate) | Row spec `RequireFullScan`, `Remark`, `CheckpointClearFullScan` | Design for the dirty-key optimization. Dead in B with no dirty keys |
| `BuildLocalSchemaView` | csm.rs:2535 | P, T (gate: flag) | Schema spec S18 | Design for P |
| `index_id_to_index` from the shared schema | csm.rs:909-923, refresh csm.rs:851, csm.rs:2531 | B (gate: flag off) | Not modeled | Design for B |
| `CollectTableRows`, `CollectIndexRows` (dirty walk or full walk) | csm.rs:1271-1300, csm.rs:1313, csm.rs:1329, csm.rs:1568-1591, csm.rs:1651 | B, P, T. A set `full_scan_generation` selects the full walk | Row spec `CheckpointCollect`. Index rows not modeled | Design (collect). The dirty walk is the optimization |
| Clamp of a future drop to "live" | `maybe_get_checkpointable_versions` csm.rs:1078-1130 | B, P, T | Row spec `CheckpointCollect`, schema spec `CheckpointObj` | Design for P and T. In B no future commit exists |
| Prune candidates | `record_prune_candidate` csm.rs:1734, `record_dirty_stamps` csm.rs:1530 | B, P, T (no gate) | Row spec `CheckpointPrune` | Optimization bookkeeping. Dead in B |
| Debug check that every changed key is dirty | csm.rs:1763, csm.rs:1787 | B, P, T (debug) | `CollectsEveryChange` | Check |
| `persistent_tx_ts_max` row in the same frame | `maybe_stage_mvcc_metadata_write` csm.rs:2423, staged at csm.rs:2632-2633 | B, P, T | Row spec `CheckpointCommit`, `RecoveryCorrect` | Design |
| Write rows, `written_table_rowids` | csm.rs:2400-2411 | B, P, T | Not modeled (DESIGN-rows: schema cases) | Design |
| `CompactSequences` | csm.rs:3208-3239, driver csm.rs:705-725, `pending_sequence_compactions` csm.rs:792 (skips CYCLE at csm.rs:817) | P records deletes. B and T delete inline and purge the chain (`purge_row_versions_during_checkpoint` mod.rs:8988). Gate: `CheckpointMode::Passive` (csm.rs:3224), not the flag | Sequence spec `CheckpointCompact`, including mode T (see checkpoint-stop section) | Design (bounds the backing table) |
| `CommitPagerTxn`, header stage | csm.rs:3241-3295 | B, P, T. T takes the lock here (csm.rs:3246-3252) | Row spec `CheckpointCommit`, schema spec issue11499 | Design |
| Publish window | `apply_checkpoint_publish_window` csm.rs:2154-2205 | P: under the clock lock (`apply_passive_publish_window_ordered` csm.rs:2208). B and T: under the blocking lock (csm.rs:3313-3320), no clock lock | Row spec `CheckpointPublish`, schema spec `CheckpointPublish` | Design |
| Publish drain bit | `checkpoint_publish_in_progress`, mod.rs:7957-7968, csm.rs:3298-3309 | P (gate: mode) | Not modeled | Dead (not verified). Only checkpoints CAS it, and `checkpoint_in_progress` already allows one checkpoint. No `begin_tx` reads it (rg finds only mod.rs:4442, mod.rs:4655, mod.rs:7959, mod.rs:7966), so the comment at csm.rs:2211-2212 is wrong. The busy retry at csm.rs:3300-3309 cannot fire |
| Sequence deletes at a fresh clock ts | csm.rs:2163-2167, `seqcompact_commit_delete` mod.rs:8998, `seqcompact_stamp_delete` mod.rs:9007 | P only (only csm.rs:2217 passes `Some(ts)`) | Sequence spec `CheckpointPublish`, S3 | Design |
| Root bindings: retire, alloc, visible | csm.rs:2169-2185, mod.rs:4809, mod.rs:4848, mod.rs:4859 | B, P, T | Schema spec S4, S6 | Design for P |
| Patch `sqlite_schema` roots, `dropped_root_pages`, generation bump | `publish_checkpointed_schema_roots` csm.rs:2021-2148 | B, P, T | Schema spec S11, S12, S13 | Design |
| `durable_txid_max` store | csm.rs:2186-2188 and again csm.rs:3481-3483 | B, P, T | Row spec `CheckpointPublish` | Design. The second store writes the same value |
| `CheckpointWal` | csm.rs:3373-3397 | B, P, T. `Busy` goes on only in P (gate: mode) | Row spec `CheckpointBackfill` | Design |
| `SyncDbFile`, publish `nBackfill` | csm.rs:3399-3438, csm.rs:1960 (a mode that restarts the log skips it) | B, P, T | Row spec `CheckpointBackfill` | Design |
| `TruncateLogicalLog` | csm.rs:1919-1944, `LogicalLog::truncate` core/mvcc/persistent_storage/logical_log.rs:1083-1095 | B: whole log, asserted (csm.rs:3326-3338). P and T: if one record is above the boundary, keep all. Gate: `clears_whole_logical_log` csm.rs:1942, true for Truncate mode with the flag off | Row spec `CheckpointTruncateDecide` and `CheckpointTruncateApply` (see checkpoint-stop section) | Design |
| `TruncateWal` (WAL restart) | csm.rs:3440-3470 | B, T (gate `should_restart_log`) | Not modeled | Design for B |
| Set `backfill_floor` | csm.rs:3484-3489 | B, P, T (no gate) | Row spec `CheckpointBackfill`, `GcFloor` | Design for P (R5). Dead in B |
| `gc_rootpage_entries(lwm)` | csm.rs:3490-3491, mod.rs:4870 | B, P, T | Schema spec S5 | Design for P |
| `GcTableRows`, `GcIndexRows`: stamp, then GC | csm.rs:3492-3516, csm.rs:2252-2329, csm.rs:2331-2398 | B, P, T (no gate) | Row spec `CheckpointStamp`, then `Gc` | Stamp: design for P, dead in B. GC here: optimization (replaces the later sweep) |
| `materialized_at` stamp | `stamp_chain_materialized` mod.rs:8651, frame from `wal_pos()` at csm.rs:2257 | B, P, T | Row spec `CheckpointStamp` | Design for P (R2). Dead in B |
| `PruneDirtyKeys` | csm.rs:3518-3527, csm.rs:1741 | B, P, T (no gate) | Row spec `CheckpointPrune`, `CheckpointPruneDone` | Optimization bookkeeping. Dead in B |
| `Finalize` sweep, B and T | `drop_unused_row_versions_and_slots` csm.rs:3531-3535, mod.rs:8188 | B and T (gate: `blocking_checkpoint_lock_held`) | Not modeled for B. Sequence spec "blocking" uses the P rules | B: replaceable by "remove every entry" |
| `Finalize` sweep, P | `drop_unused_row_versions_unlink_empty_at` csm.rs:3536-3543, mod.rs:8195 | P | Row spec `Gc` | Optimization (memory) |
| Release locks | csm.rs:1022-1034 | B, P, T | Not modeled | Design |

### GC rules and floors

| Mechanism | Code | Modes (gate) | Modeled? | Kind |
|---|---|---|---|---|
| Rule 1 (aborted garbage) | mod.rs:8591-8592 | B, P, T | Row spec `Gc` | Design (memory) |
| Rule 2, P form (`materialized_at <= floor`) | mod.rs:8604-8609 | P, T (gate: flag) | Row spec `Gc`, R2 | Design |
| Rule 2, B form (`e > ckpt_max` and `in_btree or no current`) | mod.rs:8610-8619 | B (gate: flag off) | Not modeled | Design for B |
| Rule 3 (last current, idle only) | mod.rs:8623-8640. B also needs `b <= ckpt_max` (mod.rs:8633) | B, P, T. Gates: `lwm == MAX`, `materialized_for_readers` | Row spec `GuardDropCurrentOnlyWhenIdle`, R3 | Design (memory) |
| `gc_chain_now` checks `lwm == MAX` again | mod.rs:8154-8170 | B, P, T | Row spec `GuardGcRechecksIdleLwm` | Design |
| `compute_min_reader_mark` in the floor | mod.rs:10718, used mod.rs:8251, mod.rs:8376-8378, mod.rs:8454-8457, mod.rs:8494-8497 | B, P, T | Row spec `GcFloorRule` | Optimization at most. The owner model can lower the floor after its pager pin disappears (`MvccPassiveCheckpoint.qnt:1078`). A wrong read without this term is not verified |
| Pinned pager read marks in the floor | `gc_floor_reader_mark` csm.rs:2237-2250 | Checkpoint GC in B, P, T. Not in `gc_incremental` | Row spec `floorPinned` | Design option |
| `prune_finalized_tx_states` | mod.rs:8542, only from `drop_unused_row_versions_inner` mod.rs:8434 | B, P, T (only in a full sweep) | Not modeled | Design |
| Empty slot unlink, writer retry | mod.rs:8296, mod.rs:8380, mod.rs:8478, mod.rs:8517. Retry in `insert_version` mod.rs:8727-8752 | B, P, T (P keeps slots until `Finalize`) | Not modeled | Optimization (memory) |
| Chain shrink | `shrink_version_chain_allocation` mod.rs:8691 | B, P, T | Not modeled | Optimization (memory) |

### Recovery and other paths

| Mechanism | Code | Modes (gate) | Modeled? | Kind |
|---|---|---|---|---|
| Recovery: replay log records above `persistent_tx_ts_max` | `maybe_recover_logical_log` mod.rs:9609, boundary mod.rs:9663-9681 | B, P, T | Row, schema, sequence specs `RecoveryCorrect` | Design |
| Recovery WAL backfill (TRUNCATE) | mod.rs:9275-9290 | B, P, T | Not modeled | Design |
| Bootstrap ids from root pages | `bootstrap` mod.rs:5121 | B, P, T | Schema spec `Boot` | Design today |
| VACUUM reset with `SkipMap::clear` | mod.rs:4958-4983, gate mod.rs:4891 | B, P, T | Not modeled | Design |
| Rowid allocator cache | `table_id_to_last_rowid` mod.rs:4484, dropped in `gc_rootpage_entries` mod.rs:4880 | B, P, T | Not modeled | Optimization (cache) |
| Busy retries | B begin mod.rs:6729-6731. DDL cookie mod.rs:6800. B checkpoint csm.rs:2516-2519. P publish csm.rs:3299-3309. P backfill csm.rs:3381-3395. Auto checkpoint swallows errors mod.rs:3755-3759 | See each | Schema spec `Busy` (S8). Row spec backfill of zero frames | Mixed |

## 2. What blocking mode (B) runs that only the passive design needs

Facts that hold during a B checkpoint, all verified in code:

- `txs` is empty. The checkpoint holds `blocking_checkpoint_lock` in write mode
  (csm.rs:2516). Every transaction holds it in read mode for its life (mod.rs:6729,
  mod.rs:6862). VACUUM already relies on this and asserts `txs.is_empty()` under the same
  lock (mod.rs:4891-4898).
- The checkpoint does not wait for readers. It returns `Busy` (csm.rs:2517-2519). An auto
  checkpoint drops the error (mod.rs:3755-3759). So under steady load, B checkpoints can be
  skipped again and again (not verified how often).
- No commit is `Preparing`. The snapshot is `last_committed_tx_ts`.
- The checkpoint writes every committed change and then truncates the whole logical log
  (csm.rs:1921-1931, asserts at csm.rs:3326-3338).
- New readers cannot start until the checkpoint releases the lock (mod.rs:6729).

So at the end of a B checkpoint, the B-tree holds the full committed state, the log is
empty, and no transaction exists. The version store holds nothing that the B-tree does not
have. Each item below exists only to protect a concurrent reader or a concurrent commit.

| # | What B runs | Code | What B can do instead | What must stay true |
|---|---|---|---|---|
| 1 | Mark dirty keys on every commit | mod.rs:2728, mod.rs:2737, mod.rs:7905 | With the flag off, skip the mark. Collect with a walk of the whole store (`collect_table_rows_from_store` csm.rs:1313, `collect_index_rows_from_store` csm.rs:1595). With item 6, the store holds only keys that changed after the last B checkpoint. So the full walk visits the same keys as the dirty walk | The store must be empty after every B checkpoint. If a checkpoint fails or is skipped, the full walk still finds every change, because nothing removes a changed key before it is written |
| 2 | Unmark dirty keys on rollback | mod.rs:7209 | Nothing | Follows from item 1 |
| 3 | Prune candidates, `PruneDirtyKeys`, full-scan generation and its CAS | csm.rs:1734, csm.rs:1741, csm.rs:3518-3527, mod.rs:7921-7950 | Nothing. If the dirty maps stay, B can clear them after the checkpoint. B can also set the generation to 0. Every lost stamp is at or below the snapshot | No `Preparing` commit during the checkpoint (true in B) |
| 4 | Snapshot clamp below `Preparing` commits | mod.rs:7843-7858 | Use `last_committed_tx_ts`. Keep an assert that no tx is `Preparing` | `txs` empty |
| 5 | `materialized_at` stamp in `GcTableRows`, `GcIndexRows` | csm.rs:2286-2293, csm.rs:2380-2386, mod.rs:8651 | Nothing. In B the stamp is the WAL position after `TruncateWal` (csm.rs:2257 runs after csm.rs:3440-3470), so it does not name the frame of the write. Not verified: whether that position can equal `WalPos::ORIGIN` (seq 0, frame 0). If it can, Rule 3 never drops that version in B (mod.rs:8597-8599) | None |
| 6 | GC rules at the end: `GcTableRows`, `GcIndexRows`, then `drop_unused_row_versions_and_slots` | csm.rs:3492-3516, csm.rs:3531-3535, mod.rs:8188, mod.rs:8408 | Remove every entry of `rows` and of each `index_rows` inner map, one by one. Do not call `SkipMap::clear` (core/skiplist/base.rs:1515): it only marks towers in batches of 100 and leaves the unlink to later searches. Then empty `finalized_tx_states`, because no tx exists and no version is left that names a TxID | `txs` empty. Every committed version is in the B-tree. No pager-level reader without an MVCC tx. In B a reader takes the gate before its read mark (mod.rs:6410-6420), so this holds for MVCC readers. Not verified for other pager readers (for example schema reads or attached databases) |
| 7 | `backfill_floor`, `compute_min_reader_mark`, `gc_floor_reader_mark` in GC | csm.rs:3484-3489, mod.rs:10718, csm.rs:2237 | Nothing in the checkpoint GC. `gc_incremental` between checkpoints only needs Rule 2 B form, which uses `ckpt_max`, not the floor. Rule 3 in B also reads the floor (mod.rs:8631), but item 6 already drops those versions | Same as item 6 |
| 8 | Rule 3 tests `lwm == MAX` | mod.rs:8635 | Always true in a B checkpoint | `txs` empty |
| 9 | Versioned root bindings: retire with `end_ts`, `gc_rootpage_entries(lwm)`, `materialized_at` on bindings, `STAGED` | csm.rs:2169-2185, mod.rs:4859, mod.rs:4870, mod.rs:10695 | Replace the binding at publish and delete retired bindings at once. No reader can see the old binding | Not verified: statements prepared before the checkpoint must get `SchemaUpdated` or must map their root again. The cookie check and the generation check (mod.rs:6768) give this today |
| 10 | `schema_generation` bump | csm.rs:2147 | Not verified that B needs it. The bump makes every statement prepared before it fail with `SchemaUpdated` at the next begin (mod.rs:6768). In B the cursor maps roots with `get_table_id_from_root_page_at` (cursor.rs:621-622) | Prepared statements must not use a stale root |
| 11 | `checkpoint_in_progress` test in `btree_covers_chain_for_snapshot` | mod.rs:6079 | Delete it. It is always false in B | None |
| 12 | Sequence chain purge (S5) | mod.rs:8988, csm.rs:713-725 | Item 6 drops the chain anyway. Keep the B-tree delete | Same as item 6 |
| 13 | GC of sequence and row versions inside `GcTableRows` and again in `Finalize` | csm.rs:3492, csm.rs:3535 | One pass (item 6) | None |

B needs these items and must keep them:

- the blocking lock
- write-write validation and commit dependencies
- the `persistent_tx_ts_max` row in the same frame
- the truncate of the whole log, the WAL truncate, and recovery
- `gc_incremental` with Rule 2 B form between checkpoints
 Rule 2
B form frees superseded versions under load. It never frees a version that the next
checkpoint must write (mod.rs:8615-8618).

Warning about mode T. If `blocking_checkpoint_lock_held` is true, `Finalize` picks the B
sweep (csm.rs:3531). That is also true in T. The comment at csm.rs:3533 says "That lock waits
out open MVCC txs". In T it does not, because readers never take the lock (mod.rs:6423). So
the "drop everything" change must test the flag (or `txs.is_empty()` plus no pinned read
mark), not the lock.

## 3. Gaps in the model, ranked by risk

1. Mode T now appears in the row and sequence specs. The sequence model checks inline compaction and Busy before stamping. See the checkpoint-stop section for the result. WAL restart and the blocking final GC sweep remain outside these models (`MvccPassiveSequence.qnt:48`, `MvccPassiveCheckpoint.qnt:43`). Their effects in T are not verified.
2. Mode B GC rules are not modeled. Three B paths change reads: Rule 2 B form (mod.rs:8610-8619), Rule 3 with `b <= ckpt_max` (mod.rs:8633), and the B-tree fall-through read (mod.rs:6070-6087, cursor.rs:1203). B is the default mode. The sequence spec uses the P GC rules in all three modes (`MvccPassiveSequence.qnt:51`). It does not verify the B rules.
3. Mode T publish runs without the clock wrapper (`core/mvcc/database/checkpoint_state_machine.rs:3356`). The row model splits publish at `MvccPassiveCheckpoint.qnt:649` and allows `ReaderBegin` between the steps. The row model has no header or root map. The schema model still applies publish in one step (`MvccPassiveSchema.qnt:874`). A half-published schema seen by a new reader is not verified.
4. Mode T `Finalize` uses `reader_mark_floor = STAGED` (mod.rs:8188). So it ignores readers that pinned a read mark but are not in `txs` yet (rule R5). If the R5 argument holds after a WAL restart, `backfill_floor` still protects them. Not verified.
5. Index rows are not in any spec. They have their own GC loop (mod.rs:8348), their own dirty map and the lockstep cursor (cursor.rs:524). A correct table with a wrong index still gives wrong query results.
6. `finalized_tx_states` pruning (mod.rs:8542) is not modeled. Assume that GC prunes a TxID and a version still names it. Then visibility and conflict checks treat it as "visible" or "no conflict" (comment at mod.rs:6846-6853).
7. Write-write validation (mod.rs:2071) against a GC that drops versions is not modeled. If GC drops a version, a conflict can stay hidden and an update can get lost.
8. Group commit and the order of log fsync and `CommitEnd` (mod.rs:3448-3586) are not modeled. This is the durability promise of a commit.
9. WAL restart (core/storage/wal.rs:5367) and the value of `backfill_floor` across it are not verified (DESIGN-rows "WAL restart").
10. `cleanup_dropped_commit` (mod.rs:7292) treats an abort after the log append as a commit. Not modeled. It decides whether recovery and readers agree.
11. Reads of `sqlite_schema` through the dual cursor and GC of its versions (#8093, #8094) are not modeled (DESIGN-schema "Not modeled").
12. The sync object map and stable table ids are not modeled (SYNC.md).
13. The rowid allocator cache is dropped with retired bindings (mod.rs:4880). Not modeled. A wrong cache can reuse a rowid.
14. If the B stamp after `TruncateWal` (csm.rs:2257) is `ORIGIN`, Rule 3 never drops those versions in B. Memory only. Not verified.
15. The row and sequence specs now store the checkpoint owner (`MvccPassiveCheckpoint.qnt:537`, `MvccPassiveSequence.qnt:434`). The sequence `TxCommit` can start a checkpoint on the outer reader's connection (`MvccPassiveSequence.qnt:374`). The model separates the pager pin from the registered MVCC snapshot. The schema spec still omits this owner path.
16. The schema spec has one GC action (`GcDrop`). It drops a whole chain in one step under Rule 3 alone. It has no GC sample before the chain lock, no Rules 1 and 2, and no chain budget (`gc_index_incremental`).

## 4. Optimizations the design does not need, and the rule each one stands in for

| Optimization | Code | Simpler rule for the spec | Refinement the code must keep |
|---|---|---|---|
| Dirty-key map with stamps, prune, full-scan generation | mod.rs:2737, mod.rs:7861-7950, csm.rs:1329, csm.rs:1734-1760 | Collect every key whose chain has an event in `(old boundary, snapshot_ts]`. The full walk of the version store is the reference | At the end of collection, the dirty set contains that key set (`CollectsEveryChange`) |
| `gc_incremental` on commit, budget, cursor, `gc_last_lwm`, `gc_in_progress` | mod.rs:8203-8340 | GC can apply `gc_version_chain` to any one chain at any time, with a sampled `lwm` and floor (row spec `Gc`, `GcSample`, `GcApply`) | Each call applies the same rules. Scheduling is free |
| GC inside `GcTableRows`, `GcIndexRows` | csm.rs:2295-2301, csm.rs:2387-2393 | Same rule as above. Only the stamp is part of the design | The stamp happens before the GC of the same chain |
| `Finalize` full sweep and empty slot unlink | mod.rs:8188, mod.rs:8195, mod.rs:8296 | An empty chain is the same as no chain | If GC unlinks the chain of a writer, the writer tries again (mod.rs:8727-8752) |
| Chain shrink | mod.rs:8691 | None (memory only) | None |
| B-tree fall-through read (B only) | mod.rs:6070-6087 | If the chain has a visible or ended version, a read uses the chain. Otherwise it reads the B-tree (DESIGN.md "How a reader reads a row") | The chain value must equal the B-tree value at the read mark |
| Index lockstep merge | mod.rs:6099, cursor.rs:524 | `query_btree_version_is_valid` per key (mod.rs:6126) | Same answer. The debug compare at cursor.rs:667-672 checks this |
| Row map epochs | cursor.rs:907-948 | Seek the skip list again on every step | After an insert, do not reuse a cached position |
| `compute_min_reader_mark` in the floor | mod.rs:10718 | Floor = `backfill_floor` (`floorBackfill`) or the lowest pinned read mark (`floorPinned`) | A smaller floor is always safe. It keeps more versions |
| CYCLE skip in compaction | csm.rs:817 | Compact every sequence (sequence S4) | The skip only saves the scan |
| TxID to timestamp rewrite | mod.rs:7383 | A version's begin is the commit ts of its writer | Resolution through `txs`, then `finalized_tx_states`, gives the same ts |
| `finalized_tx_states` | mod.rs:6871, mod.rs:8542 | A finished tx keeps its state forever | Prune a TxID only after no version names it (`collect_referenced_txids` mod.rs:8530) |
| Group commit | mod.rs:3448-3586 | Each commit appends and fsyncs its own record before `CommitEnd` | A commit returns only after a fsync covers its record |
| All-or-nothing log truncate | logical_log.rs:1083-1095 | Drop the records at or below the boundary | Keeping more records is always safe for recovery |
| Sequence registry | mod.rs:6881-6960 | A scan reads the backing table at its snapshot | Same rows for forward-only scans |
| Dirty marks, stamps and floors in mode B | section 2 | B rule: after a B checkpoint the version store is empty | At the drop of the store, `txs` and the log are empty |

Suggested spec work that follows from this:

1. Add mode B to the row spec as its own configuration. The checkpoint is one atomic step
   under "no tx open", and it ends with "store empty". Check `ReadsCorrect` and
   `RecoveryCorrect` with Rule 2 B form, Rule 3 with `b <= ckpt_max`, and the B-tree
   fall-through read.
2. Reproduce the mode T sequence trace in Rust. The model allows an open
   reader at `CheckpointStart` (`MvccPassiveSequence.qnt:434`) and purges inline
   at `CheckpointCompact` (`MvccPassiveSequence.qnt:500`). A Rust failure is
   not verified. Section 5 gives the model result.

## 5. Checkpoint stops, owners, and mode T

The added model paths use Rust source at base `3a95ac963`.
The supplied probe report is
`/home/peristocles/scratch-modelgaps/probes/RESULTS.md`.
Probe 4 reports stranded versions at line 87 of that file.
Probe 5 reports leaked root pages at line 126 of that file.
No Rust probe was repeated for these model changes.
The recollect and resume repairs below are model proposals.
Their Rust implementations are not verified.

### Failure points and repairs

A pass can fail before commit, between commit and publish, or after publish.
Each model keeps durable writes and completed publications when it fails.
The schema failure drops staged root operations.
The cleanup in Rust starts at
`core/mvcc/database/checkpoint_state_machine.rs:1079`.
It clears staged roots at
`core/mvcc/database/checkpoint_state_machine.rs:1087`.
The pager commit completes at
`core/mvcc/database/checkpoint_state_machine.rs:3333`.
The next failure point is the publish yield at
`core/mvcc/database/checkpoint_state_machine.rs:3341`.
The boundary store is at
`core/mvcc/database/checkpoint_state_machine.rs:2232`.
These are separate model steps.

| Model action | Source step or proposed behavior |
|---|---|
| `MvccPassiveCheckpoint.qnt:788` | Cleanup releases an owner pin only for a pager transaction it owns, `core/mvcc/database/checkpoint_state_machine.rs:1079` |
| `MvccPassiveSchema.qnt:961` | Cleanup drops staged roots, `core/mvcc/database/checkpoint_state_machine.rs:1087` |
| `MvccPassiveSequence.qnt:642` | Cleanup and TRUNCATE Busy, `core/mvcc/database/checkpoint_state_machine.rs:1079`, `core/mvcc/database/checkpoint_state_machine.rs:3421` |
| `MvccPassiveSchema.qnt:933` | Failure phase before the stamp, `core/mvcc/database/checkpoint_state_machine.rs:3377`. Logical log contents are not modeled here |
| `MvccPassiveSchema.qnt:978` | Proposed repair. Restore the committed root operations and publish before starting another pass. Rust fix not verified |
| `MvccPassiveCheckpoint.qnt:697` | Test the boundary before truncate, `core/mvcc/persistent_storage/logical_log.rs:1088` |
| `MvccPassiveCheckpoint.qnt:708` | Complete the submitted file truncate, `core/mvcc/persistent_storage/logical_log.rs:1074`. The destructive I/O ordering is not verified |
| `MvccPassiveCheckpoint.qnt:649` | Mode T boundary store, `core/mvcc/database/checkpoint_state_machine.rs:2232`, without the passive clock wrapper at `core/mvcc/database/checkpoint_state_machine.rs:2250` |
| `MvccPassiveSequence.qnt:260` | Keep the saved outer transaction while the inner transaction begins, `core/vdbe/execute.rs:14386`, `core/vdbe/execute.rs:14394` |
| `MvccPassiveSequence.qnt:275` | Remove the outer MVCC transaction, `core/mvcc/database/mod.rs:6817` |
| `MvccPassiveSequence.qnt:374` | The inner commit can start an auto-checkpoint, `core/vdbe/execute.rs:14502`, `core/mvcc/database/mod.rs:3700` |

`NoStrandedVersions` checks the row store after a successful completed pass
(`MvccPassiveCheckpoint.qnt:1011`). A failed pass can leave pending stamp work.
The fixed configuration must complete a later pass without leaving that work.
This is a safety check after completion. Eventual completion is not verified.
`NoLeakedPages` checks every committed schema image without a pending-publish
exception (`MvccPassiveSchema.qnt:1200`).

The failure witnesses require a nonempty pager commit and the named failure.
The row witness also requires an unstamped version below the published boundary
(`MvccPassiveCheckpoint.qnt:1061`). The schema witness requires an allocation in that frame
(`MvccPassiveSchema.qnt:1229`).
The repair witnesses require the failure first, then an actual stamp or publish
(`MvccPassiveCheckpoint.qnt:1062` and `MvccPassiveSchema.qnt:1230`).
They do not count a no-op checkpoint as repair.

### Sequence mode T and checkpoint owners

Mode T has no `Registered == Set()` gate at checkpoint start and no checkpoint
gate at transaction begin (`MvccPassiveSequence.qnt:434` and `MvccPassiveSequence.qnt:246`).
The compactor deletes B-tree rows and purges their chains inline
(`MvccPassiveSequence.qnt:500`). The Rust driver pairs delete and purge at
`core/mvcc/database/checkpoint_state_machine.rs:736`.
A pinned reader makes the WAL checkpoint fail with Busy
(`MvccPassiveSequence.qnt:592` and `MvccPassiveSequence.qnt:642`).
The model stops before the final stamp and sweep.
The code's Busy error path is at
`core/mvcc/database/checkpoint_state_machine.rs:3449`.
Its final blocking sweep is at
`core/mvcc/database/checkpoint_state_machine.rs:3576`.

`reachTruncateBusy` requires a nonempty pager commit, a pinned reader, and
failure at `published` (`MvccPassiveSequence.qnt:814`).
`reachPurgeBeforeBusy` also requires that the same pass purged a chain
(`MvccPassiveSequence.qnt:815`).
A model read failure in mode T is a lead. A Rust reproduction is not verified.
The supplied sequence probe did not reproduce a wrong read
(`/home/peristocles/scratch-modelgaps/probes/RESULTS.md:55`).

`CheckpointOwnerReaders` enables outer snapshots in the sequence spec.
`TxCommit` picks `ckNow`. A successful inner commit can then start a checkpoint
on that connection while the outer snapshot stays registered (`MvccPassiveSequence.qnt:374`).
The pager commit releases the owner's pager pin (`MvccPassiveSequence.qnt:527`).
`FixRefuseCheckpointOwner` refuses this checkpoint and keeps the outer pin.
Both the row and sequence main configurations enable this proposed fix
(`MvccPassiveCheckpoint.qnt:537` and `MvccPassiveSequence.qnt:434`).
`reachAutoCheckpointOwner` requires a nonempty pager commit from the auto path
while the outer reader remains registered (`MvccPassiveSequence.qnt:812`).

The sequence main bounds use two values, two connections, `MaxTs = 6`,
and two passes. The owner-off variants keep their earlier bounds.
The mode T variants use two values, two connections, `MaxTs = 6`, and two passes.
The owner variants use two values, one connection, `MaxTs = 5`, and two passes.
`MaxTs` bounds transaction begins and commits (`MvccPassiveSequence.qnt:69`).
Each passive publish advances the clock (`MvccPassiveSequence.qnt:578`).
`TypeOK` permits the extra publish ticks (`MvccPassiveSequence.qnt:789`).
The new leak bounds appear in `DESIGN-rows.md` and `DESIGN-schema.md`.
The sequence model still reads each chain once and omits WAL restart.
It does not verify the unlocked schema publish race.

### The Rust fix uses early stamps and cleanup publication

The read-only fix worktree is
`/home/peristocles/.t3/worktrees/turso/t3-7761a9a5-3a389ac9d7146b01`.
Its checked head is `633aeb51a`. The following source references use that head.
The diff against `origin/main` adds two rules.
The pager commit calls `stamp_committed_versions` before the publish yield
(`core/mvcc/database/checkpoint_state_machine.rs:3301`).
Cleanup publishes a committed pass with a pending header
(`core/mvcc/database/checkpoint_state_machine.rs:1040`).
Passive cleanup uses `apply_passive_publish_window_ordered` at
`core/mvcc/database/checkpoint_state_machine.rs:1045`.
Blocking cleanup uses `apply_checkpoint_publish_window` at
`core/mvcc/database/checkpoint_state_machine.rs:1048`.

Each model has `FixStampAtPagerCommit` and `FixCleanupPublishes` constants.
They stay off in the earlier controls. The `rustFix*` variants turn both on.
They do not enable the earlier recollect-or-resume repairs.

| Model step | Model action | Rust source |
|---|---|---|
| Stamp after pager commit | `MvccPassiveCheckpoint.qnt:619`, `MvccPassiveSchema.qnt:796`, `MvccPassiveSequence.qnt:527` | `core/mvcc/database/checkpoint_state_machine.rs:3301`, `core/mvcc/database/checkpoint_state_machine.rs:3568` |
| Publish in normal or failure path | `MvccPassiveCheckpoint.qnt:654`, `MvccPassiveSchema.qnt:879`, `MvccPassiveSequence.qnt:571` | `core/mvcc/database/checkpoint_state_machine.rs:1040`, `core/mvcc/database/checkpoint_state_machine.rs:2162` |
| Failure calls cleanup publish | `MvccPassiveCheckpoint.qnt:788`, `MvccPassiveSchema.qnt:961`, `MvccPassiveSequence.qnt:642` | `core/mvcc/database/checkpoint_state_machine.rs:1043` |
| Record GC of an early stamp with an open reader | `MvccPassiveCheckpoint.qnt:908`, `MvccPassiveCheckpoint.qnt:914` | `core/mvcc/database/mod.rs:8651` stamps the terminal event. The model then applies its existing GC rules |

The row and sequence `rustFixOwner` configurations allow a checkpoint on the
connection of an open outer snapshot. Their `Safety` values include all main
invariants (`MvccPassiveCheckpoint.qnt:1052`, `MvccPassiveSequence.qnt:806`).
Both saved owner traces contain a wrong snapshot read.
Both regenerated traces include the new pager phase and a real frame commit.
The row trace expects 0 and returns 2. The sequence trace expects 0 and returns 1.
The reader's MVCC snapshot remains registered after its pager pin disappears.
The fix does not address that path in these models.
Rust reproductions of these saved traces are not verified.

The sequence `rustFixTruncate` configuration also checks `Safety`.
Its saved trace reads 0 where the snapshot expects 1 after inline compaction
purges the older chain (`MvccPassiveSequence.qnt:500`).
This trace fails before the second pass commits its pager transaction.
Early stamping and cleanup publication cannot repair that earlier purge.
The sequence model still omits the final blocking GC sweep.
A Rust reproduction is not verified.

The sequence model does not represent the backing table root or its initial row
(`MvccPassiveSequence.qnt:48`). Its compaction treats that table as available.
Rust resolves an unpublished negative root through `current_root_page`
(`core/mvcc/database/checkpoint_state_machine.rs:805` at base `3a95ac963`).
It skips a sequence if that root cannot resolve
(`core/mvcc/database/checkpoint_state_machine.rs:827` at that base).
A Rust replay must satisfy this root requirement before it creates the modeled values.
Whether the saved trace meets all Rust root and initial-row conditions is not verified.

`reachCleanupPublish` in each model requires a nonempty committed frame and
an actual publish from failure cleanup (`MvccPassiveCheckpoint.qnt:1057`,
`MvccPassiveSchema.qnt:1227`, `MvccPassiveSequence.qnt:810`).
The schema witness also requires an allocated root in that frame.
`reachEarlyStampGc` uses the lowest pinned mark as a control.
It records an actual GC removal with a registered reader open
(`MvccPassiveCheckpoint.qnt:914`).
`reachEarlyStampGcAfterFailure` uses the code GC floor.
It adds a failed published pass and completion
of a later empty pass (`MvccPassiveCheckpoint.qnt:920`).
Each passive publish and each cleanup publish is one model step.
Errors inside that step and contention on the cleanup window are not verified.

The simulator did not capture `reachEarlyStampGcAfterFailure` in 200,000
walks of 40 steps. TLC reached it after 144,369 states.
An ITF trace for that witness is not verified.

The saved ITF traces are in `traces/bugs/` in this worktree.
A second copy is in `/home/peristocles/scratch-modelgaps/stops-model-traces/`.
`MvccPassiveCheckpoint-rustFixOwner.itf.json`,
`MvccPassiveSequence-rustFixOwner.itf.json`, and
`MvccPassiveSequence-rustFixTruncate.itf.json` contain the wrong reads.
`MvccPassiveCheckpoint-reachEarlyStampGc.itf.json` contains the GC witness.

The pager commit changes the connection transaction state at
`core/storage/pager.rs:3429` at base `3a95ac963`.
`set_tx_state` only writes that field (`core/connection.rs:4845` at that base).
The MVCC transaction id has a separate setter (`core/connection.rs:4886`).
The model therefore keeps the registered snapshot when it releases the pager pin.

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

### Review corrections: empty passes, owners, and purge witnesses

The empty-pass rule uses `5dd5ae137:core/mvcc/database/checkpoint_state_machine.rs:2650`.
A pass with neither data work nor a new metadata boundary skips the pager transaction.
The models enter `published` directly as the entry phase of the WAL checkpoint.
They do not call `CheckpointCommit` or `CheckpointPublish` on that path.
A higher boundary counts as metadata work, as the base metadata helper does
(`3a95ac963:core/mvcc/database/checkpoint_state_machine.rs:2467`).

`CheckpointBeginPager` chooses this path in the row model
(`MvccPassiveCheckpoint.qnt:602`). The sequence collection does the same before
compaction (`MvccPassiveSequence.qnt:465`). A nonempty pass acquires its pager
transaction in `MvccPassiveSequence.qnt:478`.
`pagerOwned` is true only during that transaction.
Pager commit clears it. Failure cleanup clears an owner pin only while it is true
(`MvccPassiveCheckpoint.qnt:788`, `MvccPassiveSequence.qnt:642`).
This matches the cleanup lock-state checks
(`5dd5ae137:core/mvcc/database/checkpoint_state_machine.rs:1058`).
Both `TypeOK` checks reject a `committed` state with frame 0
(`MvccPassiveCheckpoint.qnt:1034`, `MvccPassiveSequence.qnt:789`).

Explicit owners must be between statements. Rows select only `active` readers
(`MvccPassiveCheckpoint.qnt:537`). Sequences select only saved outer transactions with no inner statement
(`MvccPassiveSequence.qnt:434`). The explicit checkpoint also blocks
new operations on its owner until it stops.
These gates represent the active-statement refusal at
`3a95ac963:core/connection.rs:2590` and `3a95ac963:core/connection.rs:4833`.
The nextval auto-checkpoint starts separately in `TxCommit`
(`MvccPassiveSequence.qnt:374`). It can retain an outer transaction.

Before a pager write, a pinned owner's mark must equal the current WAL length
(`MvccPassiveCheckpoint.qnt:602`, `MvccPassiveSequence.qnt:478`).
A stale snapshot stops the pass before it owns a pager transaction.
It keeps the owner's pin. This represents `BusySnapshot`
(`3a95ac963:core/storage/wal.rs:3598`).

`purged` records each version that inline compaction actually removes
(`MvccPassiveSequence.qnt:514`). Empty chains contribute no record.
`RemovedRowNeeded` requires an open snapshot that needs that version's row as
its current sequence value (`MvccPassiveSequence.qnt:663`).
The reader must have an active read or a saved outer snapshot.
The reader's saved B-tree image must lack that row.
Its current read path must also lack that row.
`NeverPurgesBeforeBusy` requires that state, a nonempty pager commit, and Busy
at `published`. The weaker `NeverTruncateBusy` records compaction reaching Busy.

The regenerated `rustFixTruncate` trace skips its first empty pager transaction.
Its second pass removes row 1 before pager commit, then fails at `compacted`.
The outer reader at timestamp 4 and mark 0 expects 1 and receives 0.
The trace has no `committed` state with frame 0.
The sequence purge failure remains in the model after the empty-pass correction.
The backing-root and initial-row requirements above remain not verified in Rust.

### Verification results

Every command uses `WORKERS=8`. Shards execute one after another in this folder.
The commands set the following values on the make command line:

```sh
QUINT=/home/peristocles/.local/share/quint/node_modules/.bin/quint
JAVA_HOME=/home/peristocles/.local/share/mise/installs/java/temurin-21
JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=/home/peristocles/scratch-modelgaps/tlc-tmp-u4
```

The command-line value for `JAVA_TOOL_OPTIONS` overrides the Makefile default.
No command uses `/tmp` or the TLC `-cleanup` flag.
Counterexample counts depend on the order of the eight workers.
They count the states found before the first error. Passing counts cover all
reachable states within the stated bounds.

| Spec | Configuration | Result | Invariant | Distinct states |
|---|---|---|---|---:|
| Rows | `main` | Pass | All listed invariants | 17,344,980 |
| Schema | `main` | Pass | All listed invariants | 21,177,855 |
| Sequence | `main` | Pass | All listed invariants | 1,407,267 |

| Spec | Configuration | Result | Invariant | Distinct states |
|---|---|---|---|---:|
| Rows | `failedPublish` | Fail (expected) | `NoStrandedVersions` | 2,790 |
| Rows | `failedPublishFixed` | Pass | All listed invariants | 10,167 |
| Rows | `reachFailedPublish` | Fail (expected) | `NeverFailsAfterPublish` | 731 |
| Rows | `reachRepairedVersions` | Fail (expected) | `NeverRepairsVersions` | 4,685 |
| Rows | `logTruncateRace` | Fail (expected) | `RecoveryCorrect` | 205 |
| Rows | `reachTruncateAppend` | Fail (expected) | `NeverLosesAppendAtTruncate` | 503 |
| Rows | `logTruncateAtomic` | Pass | All listed invariants | 615 |
| Rows | `truncateFlagOn` | Fail (expected) | `NoStrandedVersions` | 34,195 |
| Rows | `truncateFlagOnFixed` | Pass | All listed invariants | 300,115 |
| Rows | `reachTruncateRepair` | Fail (expected) | `NeverRepairsVersions` | 73,059 |
| Rows | `refuseCheckpointOwner` | Pass | All listed invariants | 210,694 |
| Rows | `rustFix` | Pass | All listed invariants | 502,396 |
| Rows | `rustFixOwner` | Fail (expected) | `Safety` | 23,598 |
| Rows | `rustFixTruncate` | Pass | All listed invariants | 457,428 |
| Rows | `reachCleanupPublish` | Fail (expected) | `NeverCleanupPublishes` | 1,086 |
| Rows | `rustFixPinnedFloor` | Pass | All listed invariants | 1,037,362 |
| Rows | `reachEarlyStampGc` | Fail (expected) | `NeverGcEarlyStampWithReader` | 7,970 |
| Rows | `reachEarlyStampGcAfterFailure` | Fail (expected) | `NeverGcEarlyStampAfterFailure` | 144,369 |
| Schema | `failedCommit` | Fail (expected) | `NoLeakedPages` | 772 |
| Schema | `failedCommitFixed` | Pass | All listed invariants | 4,206 |
| Schema | `reachFailedCommit` | Fail (expected) | `NeverFailsBeforePublish` | 230 |
| Schema | `reachRepairedRoots` | Fail (expected) | `NeverRepairsRoots` | 552 |
| Schema | `rustFix` | Pass | All listed invariants | 125,104 |
| Schema | `reachCleanupPublish` | Fail (expected) | `NeverCleanupPublishes` | 454 |
| Sequence | `truncateFlagOn` | Fail (expected) | `ReadsCorrect` | 18,618 |
| Sequence | `truncateFlagOnStranded` | Fail (expected) | `NoStrandedVersions` | 7,587 |
| Sequence | `reachTruncateBusy` | Fail (expected) | `NeverTruncateBusy` | 8,122 |
| Sequence | `reachPurgeBeforeBusy` | Fail (expected) | `NeverPurgesBeforeBusy` | 32,112 |
| Sequence | `ownCheckpoint` | Fail (expected) | `ReadsCorrect` | 2,874 |
| Sequence | `refuseCheckpointOwner` | Pass | All listed invariants | 9,184 |
| Sequence | `reachAutoCheckpointOwner` | Fail (expected) | `NeverAutoCheckpointOnReader` | 855 |
| Sequence | `rustFix` | Pass | All listed invariants | 15,971 |
| Sequence | `rustFixOwner` | Fail (expected) | `Safety` | 4,544 |
| Sequence | `rustFixTruncate` | Fail (expected) | `Safety` | 261,184 |
| Sequence | `reachCleanupPublish` | Fail (expected) | `NeverCleanupPublishes` | 1,350 |

`make typecheck` passes for all three specs.
`make check` passes for all three main configurations after the review fixes.
The sequential variant shards check every row and sequence configuration.
They also rerun schema `rustFix`. The schema source did not change in this review.
Its other table entries retain the earlier results.

68 of 69 scheduled configurations finished and matched their comments.
The `MvccPassiveCheckpoint-gcSample` search did not finish within the review time box.
Its result is not verified.

The unchanged schema `alter` and `indexes` variants remain not verified from round 1.
The full set of 109 configurations is therefore not verified.

The review log is `/home/peristocles/scratch-modelgaps/round2-checks.log`.
The full review result table is `/home/peristocles/scratch-modelgaps/round2-results.md`.
The type-check log is `/home/peristocles/scratch-modelgaps/round2-typecheck.log`.
The sequence mode-T checks repeat after the final purge-predicate restriction.
Both `truncateFlagOn` and `rustFixTruncate` still fail as their comments require.
The purge bug therefore remains reachable in the corrected model.
A Rust reproduction remains not verified.

The trace guide is `/home/peristocles/scratch-modelgaps/stops-model-traces/README.md`.
It records the new nonempty owner commit and the corrected sequence purge trace.
The archived guide and traces retain the round 1 evidence.
The simulator found traces for both leak failures and their repair witnesses in round 1.
The after-failure GC witness has a TLC result but no ITF trace.
All completed TLC checks match their current comments.
