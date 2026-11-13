# Plan: a simpler passive checkpoint with the same guarantees

This plan uses the model results to make the MVCC passive checkpoint simpler.
Each step keeps the invariants that the specs check today. Each step removes
special cases from the code or closes open issues. Nothing in this plan is
implemented yet.

## The guarantees to keep

Every step must keep these invariants of the main configurations:

- Rows: `ReadsCorrect`, `RecoveryCorrect`, `BackfillBelowReaders` and
  `BackfillFloorBelowReaders`.
- Schema: `ReadsCorrect`, `OpenCorrect`, `RecoveryCorrect`, `NoSharedOwner`, `KindsMatch`,
  `PagesAccounted`, `NoLiveDestroy`, `SchemaRowCurrent` and
  `CookieNotBehind`.
- Sequences: `ReadsCorrect`, `CompactionKeepsCurrent`, `UniqueValues`,
  `RecoveryCorrect` and `NoStrandedVersions`.

## Why the code has so many special cases

Most schema bugs come from one design choice. A table or an index has two
names over its life. Before its first checkpoint, its name is a temporary
negative id. After the checkpoint, its name is its root page. The checkpoint
renames the object while writers, the logical log, cursors and recovery can
still use the old name.

Many special cases exist only to manage that rename:

- the negative placeholder roots, `has_unpublished_schema_changes` and its
  check for owned negative ids
- the patch of `sqlite_schema` versions by version id (#7956, #8077)
- the page lists `created_btrees`, `ended_created_roots`, `freed_root_pages`
  and `dropped_root_pages` (#7957, #8004)
- the lookup from a root page to an id at a snapshot, with retire and the
  stale owner rule
- the reprepare on a change of `schema_generation`
- the recovery rule that maps root page `N` to id `-N`, and
  `bump_next_table_id_below`

The rename causes #11501, #11502, #7960, #8077 and #11751.

## Steps

For each step, change the spec first. `make check`, `make variants` and
`make bug-traces` must give the expected results. Then change the Rust code,
with a test that fails before the change and passes after it. When the trace
replay test of `HARNESS.md` exists, the bug traces of the step must fail
before the change and pass after it.

### 1. Publish merges and never installs an old copy

No actor writes back a whole copy of shared state that it read earlier. The
publish computes the header schema cookie at publish time, or takes the larger
of the two values (`FixHeaderCookieMax`). A DDL commit changes only its own
object in the shared schema (`FixDdlRebases`).

- Closes: #11499 (the cookie part) and #8004.
- Size: small.

### 2. Sequence compaction and rollback fixes

The checkpoint deletes only the sequence rows that its snapshot saw. A version
that began after `snapshot_ts` stays (`setvalFixed`). A rollback keeps the
`materialized_at` of the version that it makes current again
(`abortsKeepMaterialized`).

- Closes: #11750 and #11749.
- Size: small. TLC finds no error with either fix.

### 3. One permanent id for each table and index

Each table and index gets one permanent id at `CREATE`, from a range that page
numbers never reach. The logical log, the version store and prepared
statements use that id for the life of the object. The root page becomes a
versioned binding from the id to a page. The checkpoint writes the binding in
the same WAL frame as the rows. The `rootpage` column of `sqlite_schema` still
gets the page at the checkpoint, so the file stays a valid SQLite file.

- Model fixes it covers: `FixDisjointIds`, `FixPersistIds`, `FixAlterPatch`
  and `FixAlterRecovery`.
- Closes: #11501, #11502, #7960, #8077 and #11751.
- Removes: most of the rename special cases in the list above.
- Size: large.

Sync also derives table ids from root pages. `SYNC.md` lists what sync
expects and what this step must keep.

Existing databases must still open, and so must their logical logs. An
existing object can get an id derived from its root page at bootstrap. A
one-time migration at open is the other option. Before any code, make sure
that bootstrap can derive the id for every object in an existing file.

### 4. One GC floor

GC uses one floor. With `backfill_floor` alone, TLC finds no error
(`floorBackfill`), because the invariant `BackfillFloorBelowReaders` holds in
the model. The lowest pinned read mark alone also passes and frees memory
sooner (`floorPinned`). A floor of only the registered readers is not safe
(`floorRegistered` fails). In the model, the backfill always stops at the
mark of every open reader (`CheckpointBackfill`). A Rust run of
`Connection::checkpoint` on the connection of an open reader moved
`backfill_floor` above that reader's mark. This step must check that path
first.

The check found a bug on that path. A checkpoint on the connection of an
open MVCC transaction ignores that transaction's read mark. SQL reaches it:
`nextval` inside `BEGIN CONCURRENT` commits an inner transaction, and that
commit runs the auto-checkpoint. The transaction then reads rows that
committed after it began, with either floor (`ownCheckpoint`,
`ownCheckpointBackfillFloor`). On the same path, GC with `backfill_floor`
alone drops versions above the reader's mark
(`ownCheckpointGcBackfillFloor` fails), and the lowest pinned read mark keeps
them (`ownCheckpointGcPinnedFloor` passes). In the concurrent simulator, an
assertion that `backfill_floor` is not above the read mark of an open
transaction fails in 46 of 200 seeds.

So the reader marks stay in the floor until the fix for this path merges.
GC asserts that its floor is not above the read mark of an open transaction.

- Removed: `rootpage_gc_protected`. The invariant `StampsAtOrAfterBinding`
  holds, and GC asserts it on each chain (`reachRootGuardFires` shows that
  the model reaches the state where the guard fired).
- Removes after the fix: `compute_min_reader_mark` from GC and the merge of
  two values in `gc_floor_reader_mark`.
- Size: medium.

### 5. Delete guards that the model does not need

The model finds no error with each of these guards off. That result only
says "not shown needed at these bounds". The model leaves out some paths, so
it does not mean "safe to delete". Delete one guard at a time, after step 3.
Each deletion follows "How to delete a guard" in
`.claude/skills/quint-modeling/SKILL.md`: a reachability check, a negative
control, an inductive invariant, and a Rust assertion before the deletion.

| Guard | Code | Variant |
|---|---|---|
| The begin bound of a binding in the readable check | `is_btree_readable_at` | `noCoversBegin` |
| The end bound of a binding in the readable check | `is_btree_readable_at` | `noCoversEnd` |
| The retire of a stale live owner | `record_rootpage_alloc` | `noAllocRetiresStale` |
| The fallback for an unmapped root page | `MvccLazyCursor::new` | `noOpenFallback` |
| The schema generation check at begin | `begin_tx` | `noGenerationGate` |
| The CYCLE skip in compaction | `pending_sequence_compactions` | `noSkipCycle` |
| The purge of a blocking chain | sequence compaction | `blockingNoPurge` |

The generation compare in `clear_checkpoint_full_scan` is also not needed in
the row model (`dirtyNoGenerationCas`). Keep it for now, because the model
does not cover the sequence path that also raises the generation.

Step 3 can make several of these guards dead code.

### 6. One look at the chain for each read

Today a read can look at the version chain twice, once for the version store
and once for the B-tree. Because of that, GC Rule 3 waits until no reader
is active. If one look decides both sides of a read, Rule 3 can run under
load (`rule3UnderLoadOneLook` passes, `rule3UnderLoad` fails). Rule 3 then
frees row versions while writers insert, and the version store stops growing
without a bound under a constant insert load.

- Size: large. It needs a cursor design and its own model of a scan over a
  skip list while GC removes entries.

### 7. Blocking mode does only what it needs

The specs model only the passive checkpoint, but blocking mode (the flag off)
runs most of the passive machinery. Every commit marks dirty keys, and the
checkpoint ends with the GC rules (`MODEL-GAPS.md`, section 2). If a transaction is
open, a blocking checkpoint fails with `Busy`. Otherwise it writes every change
and empties the log. So after it, the version store holds nothing that the
B-tree does not have.

- Rule: after a blocking checkpoint, the version store is empty.
- Code: with the flag off, skip dirty keys, stamps and floors. At the end
  of the checkpoint, remove every entry one by one. Do not call
  `SkipMap::clear`, because it leaves most of the work to later operations.
- The gate must be the flag, not "blocking lock held". With the flag on, an
  explicit `TRUNCATE` checkpoint also holds that lock, but readers do not take
  it.
- Model first: add blocking mode to the row spec as its own configuration.
- Size: medium.

A third mode needs a test before any change: the flag on with an explicit
`PRAGMA wal_checkpoint(TRUNCATE)`. No spec models it, and it mixes the
blocking and passive paths while readers stay open. `MODEL-GAPS.md`,
section 3, item 1 gives the lead.

## Order

The order is 1, 2, 7, 3, 4, 5, 6. Steps 1 and 2 are small and close four
issues. Step 7 is independent of the passive design and removes work from
the default mode.
Step 3 removes the most code but needs a compatibility plan. Step 5 waits
for step 3. Step 6 is a separate project with the largest performance payoff.
