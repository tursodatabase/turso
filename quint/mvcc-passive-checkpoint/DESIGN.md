# MVCC passive checkpoint design

This document describes the design of the passive checkpoint in MVCC mode, as
the code implements it today. The Quint model `MvccPassiveCheckpoint.qnt` in
this directory states the same design formally. Quint checks it with the TLC
model checker (`quint verify --backend tlc`).

A passive checkpoint copies committed row versions from the in-memory version
store into the B-tree. Writers keep committing and readers keep reading while
it runs. The feature is behind `--experimental-mvcc-passive-checkpoint`.

## Where data lives

Four stores hold row data.

| Store | Contents | Survives a crash |
|-------|----------|------------------|
| Version store (`MvStore::rows`) | Version chains: each version has a begin timestamp, an end timestamp and `materialized_at` | No |
| Logical log | One record per committed transaction | Yes |
| WAL | Frames that checkpoints wrote with a pager transaction | Yes |
| Database file | Frames that a WAL checkpoint backfilled | Yes |

Three numbers in memory connect them.

- `durable_txid_max` is the boundary of the last checkpoint. Every commit at or
  below it is in the B-tree. The same number is also stored in the B-tree as
  `persistent_tx_ts_max`, in the same WAL frame as the rows.
- `backfill_floor` is the WAL position up to which the database file holds the
  frames.
- `materialized_at` on a version is the WAL frame from which the B-tree shows
  the last event of that version. The last event is the end of the version.
  For a version with no end, it is the begin. The value `ORIGIN` means that no
  frame shows it yet.

Each reader has two numbers.

- `begin_ts` comes from the logical clock. It decides which versions are
  visible.
- `read_mark` is the WAL frame that the reader pinned. It decides which B-tree
  the reader sees: the database file with every WAL frame up to `read_mark` on
  top.

## The checkpoint, step by step

One checkpoint runs at a time. The `checkpoint_in_progress` flag makes sure of
that (`CheckpointState::PrepareCheckpoint`).

1. Snapshot. Under the clock lock, take `snapshot_ts` as the lowest of
   `last_committed_tx_ts` and one less than the timestamp of every commit that
   is still `Preparing` (`MvStore::checkpoint_snapshot_ts`).
2. Collect. Walk the version store. For each row that changed after the old
   boundary and at or before `snapshot_ts`, record the row value at
   `snapshot_ts` (`collect_table_rows`, `collect_index_rows`). Writers keep
   committing during this walk.
3. Write and commit. Write the recorded rows and the new boundary
   (`persistent_tx_ts_max = max(old, snapshot_ts)`) in one pager transaction.
   The pager commit appends one WAL frame `F` (`CommitPagerTxn`).
4. Publish. Under the clock lock, set `durable_txid_max` to the new boundary,
   and publish root pages, the page 1 header and the sequence deletes
   (`apply_checkpoint_publish_window`).
5. Backfill. Run a PASSIVE WAL checkpoint: copy frames into the database file,
   but stop at the lowest read mark of a pinned reader. Then sync the database
   file and publish `nBackfill` (`CheckpointWal`, `SyncDbFile`).
6. Truncate the logical log. If a record in it is above the new boundary,
   keep all of it (`LogicalLog::truncate`).
7. Set `backfill_floor` to the backfilled position (`TruncateWal`).
8. Stamp and collect garbage. On every row that the checkpoint wrote, set
   `materialized_at = F` on each version whose last event is at or below
   `snapshot_ts`. Then run the GC rules (`GcTableRows`, `GcIndexRows`,
   `Finalize`).

## How a reader reads a row

A reader with `begin_ts = b` and `read_mark = m` reads row `k` like this.

1. If the chain of `k` has a version that is visible at `b`, use that version.
2. If the chain has a version that ended before `b`, the row does not exist for
   this reader.
3. Otherwise read `k` from the B-tree at read mark `m`.

In passive mode the reader never skips the chain in favor of the B-tree
(`btree_covers_chain_for_snapshot` returns `false`). The dual cursor
(`MvccLazyCursor`) has a version store side and a B-tree side. The two sides
look at the chain at different moments.

## How GC drops versions

GC runs on the commit path (`gc_incremental`) and at the end of a checkpoint.
It uses two values.

- `lwm` is the lowest `begin_ts` of a registered reader, or infinity.
- The floor is the lowest read mark that GC respects. Today the code uses the
  lowest of the registered readers' read marks and `backfill_floor`. The
  checkpoint also adds the pinned read marks (`gc_floor_reader_mark`).

The passive rules are in `gc_version_chain`.

- Rule 2 drops a version that ended at `e < lwm`. The version must also have
  a `materialized_at` at or below the floor.
- Rule 3 drops the last version of a chain, but only a current version. The
  version must have a `materialized_at` at or below the floor, and `lwm` must
  be infinity (no reader is open).

## The five rules of the design

Each rule below is one guard in the model. When a guard is off, TLC finds a
bug. The table lists the property that fails.

| Rule | Code | Without it |
|------|------|------------|
| R1. `snapshot_ts` is below every `Preparing` commit. | `checkpoint_snapshot_ts` | `RecoveryCorrect` fails. A commit with a lower timestamp finishes after the snapshot. The boundary covers it, but the B-tree does not have it, so recovery skips its log record. |
| R2. Before GC drops a version, the B-tree at every reader's mark must show its effect (`materialized_at <= floor`). | `gc_version_chain` | `RecoveryCorrect` fails. GC drops a version before the checkpoint collects it. Then the log is cut and the row is lost. |
| R3. While a reader is open, Rule 3 keeps the last current version. | `gc_version_chain` | `ReadsCorrect` fails. The B-tree side skips the row because the chain shadows it. Then GC empties the chain, and the version store side finds nothing. |
| R4. WAL backfill stops at the lowest pinned read mark. | `Wal::checkpoint` (PASSIVE) | `BackfillBelowReaders` fails. The database file gets pages that are newer than a reader's snapshot. |
| R5. The GC floor covers readers that pinned a read mark but are not in `txs` yet. | `backfill_floor` in the floor | `ReadsCorrect` fails. `begin_tx` reads `wal_pos()` before it takes the clock lock, and GC drops a version that this reader still needs. |

## What the model checks

The model has writers, readers, one checkpoint at a time and GC. GC can run at
any moment. It checks these properties in every reachable state.

- `ReadsCorrect`: every read returns the row value at the reader's
  `begin_ts`, for both orders of the two dual-cursor looks.
- `RecoveryCorrect`: if the process crashes in this state, the database file,
  all WAL frames, and the logical log records above `persistent_tx_ts_max`
  rebuild every logged commit.
- `BackfillBelowReaders` and `BackfillFloorBelowReaders`: the WAL rules that
  the B-tree side of a read depends on.

TLC checks a bounded model, not every possible size. The main configuration
(module `main` in `MvccPassiveCheckpointConfigs.qnt`) has one key, two
readers, five clock timestamps and two checkpoint passes. `make check` explores
16,786,385 distinct states in about 10 minutes on 3 workers and finds no
error. Most variants use the same size.

TLC visits each order of the keys and the readers. For this reason the main
configuration has one key. The model was also checked with two keys, two
readers and four clock timestamps: TLC explored 25,403,590 distinct states
and found no error.

Four reachability checks (`NeverDropsCurrentRow`, `NeverTruncatesLog`,
`NeverReadsBtreeAtOldMark`, `NeverBackfills`) fail as they must. So the model
reaches each of these states. GC empties a chain. The log is cut. A reader
reads the B-tree through an old read mark. The WAL is backfilled.

Three more documents cover the other parts. Each has its own spec.

| Document | Spec | Part |
|----------|------|------|
| `DESIGN-rows.md` | `MvccPassiveCheckpoint.qnt` | Aborts, dirty keys and GC samples, added to this row model |
| `DESIGN-schema.md` | `MvccPassiveSchema.qnt` | Table ids, root pages, the schema cookie, and the open issues they cause |
| `DESIGN-sequences.md` | `MvccPassiveSequence.qnt` | Sequence rows and their compaction |

The row model still leaves out indexes, WAL restart and write-write
conflicts.

## Findings

These results come from the model. Each one is a lead for a code change. Each
change needs its own test in the Rust code before it goes in.

1. The GC floor can be one number. `backfill_floor` is always at or below every
   pinned read mark, because the WAL backfill stops at those marks and new
   readers pin a mark at the end of the WAL. TLC checks this as the invariant
   `BackfillFloorBelowReaders`. So the reader marks in the floor never lower
   it, and `compute_min_reader_mark` adds nothing to row GC. A floor of "lowest
   pinned read mark" is also safe and lets GC drop versions sooner. A floor of
   "registered readers only" is not safe (rule R5). A checkpoint on the
   connection of an open transaction breaks this result. Its backfill ignores
   that reader's mark, so `backfill_floor` moves above the mark
   (`reachReaderMarkBelowFloor`). Until that path refuses the checkpoint, the
   reader marks stay in the floor.
2. Rule 3 needs "no reader open" only because the dual cursor looks at the
   chain twice. When one look decides both sides of a read, TLC finds no error
   with Rule 3 under load. The variant `rule3UnderLoadOneLook` checks this
   with one key (20,771,050 distinct states). Rule 3 idle-only is the reason that the version store grows
   under a steady insert load, so this is the change with the largest payoff.
   The model reads one key in one step. A real scan walks the skip list while
   GC unlinks entries. So the cursor change needs its own
   design and its own model before it goes in.
3. The `begin_tx` window between `wal_pos()` and the clock lock is real. The
   code is safe today only because `backfill_floor` is in the floor. A
   comment in `gc_floor_reader_mark` gives the pinned read marks as the reason,
   but the commit-path GC does not use the pinned marks.

## Run the model

See `README.md`.
