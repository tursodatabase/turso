# Schema changes and table ids under the passive checkpoint

This section describes how the passive checkpoint creates and destroys the
B-trees of tables and indexes. It also describes how readers find the right
B-tree at the same time, and how recovery rebuilds the tables after a crash.
The Quint model `MvccPassiveSchema.qnt` states the same design formally.
Quint checks it with the TLC model checker (`quint verify --backend tlc`).

## Names and placeholders

A `CREATE TABLE` or `CREATE INDEX` in MVCC mode does not create a B-tree. The
object gets a negative table id, and its `sqlite_schema` row stores the same
negative number as its root page. This is a placeholder. A later checkpoint
creates the real B-tree and gives the object a positive root page. A `DROP`
does not free pages either. A later checkpoint destroys the B-tree, and the
pager puts its pages on the free list. A later create can reuse such a page.

Four places hold schema state.

| State | Contents | Code |
|-------|----------|------|
| Version store rows of `sqlite_schema` | One row per object with its root page, a begin and an end timestamp | `MvStore::rows` |
| Root bindings | One `RootEntry {root_page, begin, end, materialized_at}` per table id | `MvStore::table_id_to_rootpage` |
| Shared schema | Parsed objects with their root pages, and `dropped_root_pages` | `Database::schema` |
| B-tree | Pages, and the `sqlite_schema` rows that a checkpoint wrote | WAL and database file |

Two counters tell a reader that its compiled statement is stale.

- The schema cookie in the database header. Every DDL commit changes it
  (`publish_committed_tx`).
- `schema_generation`. Every checkpoint publish that changes root pages bumps
  it (`publish_checkpointed_schema_roots`).

## Table ids and recovery

`get_next_table_id` (mod.rs:5394) hands out negative table ids, starting
right below the root pages that exist. When a checkpoint gives a table the
page `P`, `bump_next_table_id_below` (mod.rs:4771) moves the next id below
`-P`. It cannot move ids that are already in use.

A logical log record of a row carries a table id. If the table has a root page `P`
at commit time, `canonical_log_id` (mod.rs:2458) writes `-P`. If it has no
root page yet, it writes the temporary id. The record of a `CREATE` carries the
`sqlite_schema` row, whose rootpage is the temporary id.

Recovery has two steps. Bootstrap reads the B-tree `sqlite_schema` rows and
gives each table with root page `P` the table id `-P` (mod.rs:5383). Then
recovery replays the log records above `persistent_tx_ts_max`. Take a `CREATE` record
with a negative rootpage. That number can already be the id of a table with a
page. Then recovery panics (mod.rs:10224). A row record goes to the table whose id it carries.

## Indexes

An index is a schema object with its own B-tree. `CREATE INDEX` makes one.
A `UNIQUE` or non-rowid `PRIMARY KEY` in `CREATE TABLE` also makes one. The
index gets its own temporary id from `get_next_table_id`. It also gets its
own `sqlite_schema` row with `type = 'index'`. The checkpoint creates its B-tree with
`SpecialWrite::BTreeCreateIndex` and destroys it with `BTreeDestroyIndex`
(`collect_table_row_versions`, checkpoint_state_machine.rs:1386). An insert
into a table writes the index rows in the same transaction. The same log
record carries them, each with the id of its own object
(`canonical_log_id`).

Recovery does not know whether an id belongs to a table or to an index.
Bootstrap gives the object with page `P` the id `-P`, whatever its kind. A
row record goes to the object whose id it carries. So when a temporary id
`-N` of one object equals `-P` of another object, the row lands in the
wrong B-tree. If the other object is an index, a table row lands in an
index B-tree (#7960).

## ALTER TABLE

`ALTER TABLE ... ADD COLUMN` updates the table's `sqlite_schema` row with a
new SQL text (`translate_update_for_schema_change`, core/translate/update.rs:147,
called from core/translate/alter.rs:1532). In MVCC an update ends the old
row version and inserts a new version with the same rowid and a new version
id. The new version copies the `rootpage` column from the old version that
the `ALTER` transaction reads. Before a checkpoint publishes the table's
page, that is the temporary id. The `ALTER` also bumps the cookie.

The checkpoint walks the versions of a `sqlite_schema` row oldest first
(`maybe_get_checkpointable_versions`, checkpoint_state_machine.rs:1078). Take
an ended version and the version after it. If both have the same B-tree
identity (the same kind and `rootpage`), the list keeps only the newer one
(`is_schema_metadata_only_rewrite`, line 617). Otherwise the ended version
counts as a `DROP` and the newer version as a `CREATE`.

The publish patches the payload of one version only: the version with the
version id that the checkpoint collected (`existing.id == vid` in
`publish_checkpointed_schema_roots`, line 2046). If that version has an end,
the publish adds the root to `dropped_root_pages`
(`ended_created_roots`).

## The header cookie

The checkpoint stages the page 1 header in `CommitPagerTxn`, with the schema
cookie of that moment (checkpoint_state_machine.rs:3262). The publish window
writes the staged header over `global_header` (line 2199).

## A DDL commit

1. The statement runs. The connection makes a copy of the shared schema and
   changes the copy. `DROP TABLE` of a table with a positive root adds the
   root to `dropped_root_pages` in the copy (`op_drop_table`,
   core/vdbe/execute.rs:13749). `CREATE` inserts a live binding with no root
   page (`insert_table_id_to_rootpage`).
2. The commit takes its timestamp and enters `Preparing`. Other readers can
   now see its row changes.
3. `publish_committed_tx` publishes the header with the new cookie.
4. `publish_schema_if_newer` installs the connection's copy as the shared
   schema, because its version is newer (`Database::update_schema_if_newer`).

## The checkpoint, schema part

The steps below add to the steps in `DESIGN.md`.

1. Snapshot. The checkpoint builds a schema at `snapshot_ts`
   (`BuildLocalSchemaView`) and keys each index by the binding that owns its
   root page at `snapshot_ts` (checkpoint_state_machine.rs:2577).
2. Collect. A `sqlite_schema` row that changed in `(old boundary, snapshot_ts]`
   gives a special write (`collect_table_rows`, checkpoint_state_machine.rs
   ~1380-1475).
   - A create with a negative root gives `BTreeCreate`. A drop that committed
     after `snapshot_ts` is clamped to "live", so the B-tree is still created
     (`maybe_get_checkpointable_versions`, line 1104).
   - A drop of a negative root gives `RootMapOp::Remove`. No B-tree exists.
   - A drop of a positive root gives `BTreeDestroy`, for the binding that
     owned the page at the drop timestamp (`resolve_dropped_binding`). When no
     binding owned it, the checkpoint skips the drop.
3. Write. `BTreeCreate` calls `btree_create` and stages `RootMapOp::Alloc`
   (`ckpt_rootmap_alloc`, `staged_roots`). `BTreeDestroy` calls
   `btree_destroy`, records the page in `freed_root_pages`, and stages
   `RootMapOp::Retire` with the drop timestamp. Rows of a table that this pass
   destroys are not written.
4. Commit. One pager transaction writes the pages and the `sqlite_schema` rows
   as WAL frame `F`.
5. Publish, under the clock lock (`apply_passive_publish_window_ordered`).
   - Apply every `Retire` and `Remove`, then every `Alloc`. An `Alloc` is
     `record_rootpage_alloc` with `begin = snapshot_ts`, followed by
     `publish_rootpage_visible`, which sets `materialized_at = F`.
     `record_rootpage_alloc` also ends any other live binding of the same page.
   - `publish_checkpointed_schema_roots` patches the payload of each created
     `sqlite_schema` row to the positive root. It keeps the row's begin and end
     (#7956). It rewrites negative roots to positive in the shared schema. It
     removes from `dropped_root_pages` only the pages that this pass freed
     (#7957). It adds the root of a created object whose row already has an
     end (#7956). It bumps `schema_generation`.
6. Finish. `gc_rootpage_entries(lwm)` drops ended bindings with `end <= lwm`.

## How a reader opens a table

1. The `Transaction` opcode compares the connection's schema with the shared
   schema and reads `schema_generation` (`mvcc_begin_schema_generation`).
2. The pager pins a WAL read mark.
3. `begin_tx` runs under the clock lock. If `schema_generation` moved, it
   returns `SchemaUpdated`. If a DDL commit is `Preparing` and has not
   published its cookie, it returns `Busy`
   (`exclusive_tx_has_unpublished_schema_cookie`). Then it takes `begin_ts`
   and the header.
4. The `Transaction` opcode compares the header cookie with the cookie of the
   compiled schema. A mismatch gives `SchemaUpdated`.
5. `MvccLazyCursor::new` maps the compiled root page to a table id at
   `begin_ts` (`try_get_table_id_from_root_page_at`). A negative root is the
   table id. A positive root maps to the binding with that page that is live
   or ends after `begin_ts`, the one with the lowest end. No binding gives
   `SchemaUpdated` (cursor.rs:620).
6. The B-tree side of the cursor opens the B-tree only when
   `is_btree_readable_at` holds: the binding covers `begin_ts`
   (`begin <= begin_ts < end`) and `materialized_at <= read_mark`
   (`is_btree_allocated`, cursor.rs:833).

## What the model checks

The model has two tables, the pages 2 and 3, one reader, DDL commits, row
inserts, one checkpoint at a time and GC. Each table has at most one row,
which its own commit inserts after `CREATE`. A WAL frame holds the full image
of every page and the B-tree `sqlite_schema` rows. A create gets any free
page, so the model covers every allocation order. The checkpoint processes
tables in any order, so a destroy and a create that reuses the page can
happen in one pass. Table ids, root pages, the rootpage column, the header
cookie and the ids in log records are numbers, as in the code.

Three constants add parts that the main configuration leaves out to keep
it small. Their off value removes the part from the model.

- `Indexes` and `IdxTable`: the objects in `Indexes` are indexes of the table
  `IdxTable`. `CREATE INDEX` runs only while the table has no row, so every
  index row comes from a table insert. `DROP TABLE` runs only after its
  indexes are dropped. Off: `Indexes = Set()`.
- `Alters`: `ALTER TABLE ADD COLUMN`, at most once per table. The table's
  `sqlite_schema` row then has two versions, each with its own `rootpage`
  column (`cat.get(o).r1` and `cat.get(o).r2`). Off: `false`.
- `GuardFrozenMark`: every statement of a transaction reads pages at the WAL
  mark that its begin pinned. When it is off, each new statement reads pages
  at the newest frame, but `is_btree_readable_at` still uses the begin mark.

It checks these properties in every reachable state.

- `ReadsCorrect`: a reader reads only its own table's pages, and gets the
  row of its table at `begin_ts`. A read of a table that does not exist at
  `begin_ts` must fail.
- `OpenCorrect`: no reader panics (an unmapped page in
  `MvccLazyCursor::new`, or a negative page in the pager). No open fails for
  a table that the reader's snapshot sees.
- `NoSharedOwner`: no page has two live bindings, and no two tables share a
  temporary id.
- `RecoveryCorrect`: if the process crashes now, recovery does not panic.
  It rebuilds every table that the log has as committed, and the row of each
  one. Each table in the B-tree catalog points at its own page, and every
  allocated page belongs to a table in the catalog. A replayed `ALTER`
  record keeps the table on its page.
- `PagesAccounted`: every allocated page is a root of the shared schema or is
  in `dropped_root_pages`, so `integrity_check` does not report it.
- `CookieNotBehind`: the header cookie is never behind the cookie of the
  shared schema.
- `KindsMatch`: a row record that recovery replays, and a root page that a
  reader opens, resolve to an object of the same kind and the same table. No
  table row lands in an index B-tree, and no index row lands in a table
  B-tree.
- `NoLiveDestroy`: no checkpoint destroys the B-tree of an object that is
  not dropped at the checkpoint's snapshot.
- `SchemaRowCurrent`: the current `sqlite_schema` row of a live table points
  at the table's live B-tree. A negative `rootpage` is right only while the
  table has no page.

A committed write that is lost after a crash breaks `RecoveryCorrect`.

## Configurations

The configurations are modules in `MvccPassiveSchemaConfigs.qnt`. The
module `main` is the design: every special case
of the code is on, and every `Fix*` constant applies one fix. It has
`MaxTs = 4` and two checkpoint passes. It keeps `Indexes = Set()` and
`Alters = false`, and it also checks `KindsMatch`, `NoLiveDestroy` and
`SchemaRowCurrent`.

TLC visits both orders of the two tables. With `MaxTs = 4`, TLC finds
15,985,399 distinct states and no error. That run takes about 17 minutes on
2 workers.

The code today is the design with every `Fix*` constant set to `false`
(the module `codeToday`). TLC finds a `RecoveryCorrect` error after 640
distinct states. The variants `issue*` switch off one fix each. All
variants use `MaxTs = 4`, except `indexes`.

| Fix | What it does | Issue | Variant | Invariant that fails without it |
|-----|--------------|-------|---------|-------------------------------|
| `FixDisjointIds` | Temporary ids come from a range that no page number reaches. | #11501 | `issue11501` | `RecoveryCorrect` (recovery panics) |
| `FixPersistIds` | The B-tree keeps each table's temporary id, and bootstrap maps that id to the table too. | #11502 | `issue11502` | `RecoveryCorrect` (a row is lost) |
| `FixHeaderCookieMax` | The publish never lowers the header cookie (the patch in the issue). | #11499 | `issue11499` | `CookieNotBehind` |
| `FixInnerTxChecks` | An inner transaction (`nextval`) checks the generation and the cookie like a normal begin. | #11500 | `issue11500` | `ReadsCorrect` |
| `FixDdlRebases` | A DDL commit changes only its own table in the shared schema. It does not install its full copy of the schema. | #8004 | `issue8004` | `PagesAccounted` |
| `FixDisjointIds` | The same fix as for #11501: no temporary id can name the page of an index. | #7960 | `issue7960` | `KindsMatch` (a row lands in a B-tree of another kind) |
| `FixAlterPatch` | The publish patches every version of the `sqlite_schema` row that still carries the temporary id, and only a `DROP` puts the root in `dropped_root_pages`. | #8077 | `issue8077`, `issue8077Row` | `NoLiveDestroy`, `SchemaRowCurrent` |
| `FixAlterRecovery` | Recovery maps a replayed `sqlite_schema` update whose `rootpage` is a temporary id to the table that bootstrap knows by that id. It needs `FixPersistIds`. | none, a model finding | `noAlterRecovery` | `RecoveryCorrect` (recovery panics) |

The id fixes and `FixAlterRecovery` are design statements, not patches.
Each one closes its issue in the model. Other fixes can close them too, for example a log record
that names the table by its `sqlite_schema` row.

Two main-check configurations switch on the parts that the main
configuration leaves out, with all fixes on. TLC must find no error.

- `indexes`: table `t1` and index `i1`, `MaxTs = 5`. TLC explores
  7,878,718 distinct states and finds no error.
- `alter`: two tables with `Alters`, `MaxTs = 4`. This size was not
  checked. With `MaxTs = 3`, TLC finds 5,084,599 distinct states and no
  error.

With `Indexes = Set()`, `Alters = false` and `GuardFrozenMark = true`, the
model is the model from before these parts were added. `noAllocRetiresStale`
gives 15,985,399 distinct states and no error.

## The rules

Each row is one special case of the code today. "Needed" means this: when
the case is off, TLC finds the error in the last column. The variant files
are modules in `MvccPassiveSchemaConfigs.qnt`.

| Rule | Code | Without it | Variant |
|------|------|------------|---------|
| S1. The B-tree side opens a B-tree only at `materialized_at <= read_mark`. | `is_btree_readable_at` | Needed. `ReadsCorrect` fails: a reader pinned frame 0, began after the snapshot, and opens the page of a new table after the publish. At its mark the page is free. | `noMatCheck` |
| S2. The binding must cover `begin_ts` from below (`begin <= begin_ts`). | `RootEntry::covers` | Not needed. A reader pins its mark before it takes `begin_ts`, so `materialized_at <= read_mark` already gives `begin <= begin_ts`. | `noCoversBegin` |
| S3. The binding must cover `begin_ts` from above (`begin_ts < end`). | `RootEntry::covers` | Not needed. Only a reader with a stale schema can open a dropped table after its drop, and the cookie check stops it. | `noCoversEnd` |
| S4. `BTreeDestroy` retires the binding and keeps it. | `ckpt_rootmap_retire` | Needed. `ReadsCorrect` fails: a reader opened the table before the destroy. GC dropped the row from the version store. The destroy removes the binding, so the cursor does not open the B-tree, and the read finds no row. | `noRetire` |
| S5. `gc_rootpage_entries` drops a retired binding only at `end <= lwm`. | `gc_rootpage_entries` | Needed. `ReadsCorrect` fails in the same way as S4. | `noBindingGcLwm` |
| S6. `record_rootpage_alloc` ends a stale live owner of the reused page. | `record_rootpage_alloc` | Not needed. Every destroy stages `Retire` or `Remove` for the old owner in the same publish window, and the window applies them before `Alloc`. | `noAllocRetiresStale` |
| S7. An unmapped root page gives `SchemaUpdated`, not a panic. | `MvccLazyCursor::new` | Not needed. With S8 and S9 the cursor never meets an unmapped page. | `noOpenFallback` |
| S8. `begin_tx` returns `Busy` while a DDL commit has not published its cookie. | `exclusive_tx_has_unpublished_schema_cookie` | Needed (#9955). `ReadsCorrect` fails: a reader begins after a `DROP` took its timestamp but before the cookie changed, and reads the dropped table. | `noBusy` |
| S9. The cookie of the header must equal the cookie of the compiled schema. | `Transaction` opcode | Needed. `ReadsCorrect` fails. This is the SQLite rule, not a passive special case. | `noCookieCheck` |
| S10. If `schema_generation` moved, `begin_tx` returns `SchemaUpdated`. | `begin_tx_with_schema_generation_and_checkpoint_read_guard` | Not needed for reads. A statement with a negative root maps to its table id, and the cursor takes the page from the binding (S16). | `noGenerationGate` |
| S11. The publish patches the `sqlite_schema` payload and keeps its end. | `publish_checkpointed_schema_roots` | Needed (#7956). `RecoveryCorrect` fails: a `DROP` commits after the snapshot. The publish puts back the collected row without the end. The next pass does not destroy the table, and a crash brings it back. | `noPatchKeepsEnd` |
| S12. The publish adds the root of a created object that is already dropped to `dropped_root_pages`. | `ended_created_roots` | Needed (#7956). `PagesAccounted` fails. | `noEndedCreated` |
| S13. The publish clears only the roots that this pass freed. | `freed_root_pages` | Needed (#7957). `PagesAccounted` fails: a `DROP` that commits after the snapshot adds its root, and the publish clears it while the page is still allocated. | `noFreedOnly` |
| S14. GC keeps the rows of a new B-tree until every reader mark reaches its creation frame. | `rootpage_gc_protected`, removed | Not needed. The stamp gives each written version the same `materialized_at` as the binding, and GC already checks it against the same floor (`StampsAtOrAfterBinding`). GC asserts this invariant on each chain. | `reachRootGuardFires` |
| S15. A binding with `materialized_at = STAGED` is never readable. | `is_btree_readable_at` | Not modeled. `Alloc` and `publish_rootpage_visible` run in one clock-locked window, and the model applies them in one step. The check matters only for a reader without a WAL mark, whose mark is `STAGED` (`read_tx_mark`). The model has no such reader. | none |
| S16. If the B-tree becomes readable, a cursor with a negative root takes the page from the binding. | `is_btree_allocated` (cursor.rs:833, commit 367362427) | Needed (#8537). `OpenCorrect` fails: a cursor opened before the publish seeks the negative root. | `issue8537` |
| S17. The `TruncateWal` assert looks only at this pass's staged roots, not at the whole schema. | `CheckpointState::TruncateWal` (line 3481) | Needed. With the code's DDL commit, the shared schema can keep a negative root after the publish gave it a page. A scan of the whole schema then fires. | `reachOwnedNegative` |
| S18. Indexes are keyed by the binding that owns their root at `snapshot_ts`. | `BuildLocalSchemaView` | Not switched off. The model gives an index the same binding, create and destroy steps as a table, and keys its rows by its own object. | none |
| S19. `resolve_dropped_binding` picks the binding that covers the drop timestamp. | `resolve_dropped_binding` | Modeled, not switched off. Not verified. | none |
| S20. An ended `sqlite_schema` version followed by a version with the same `rootpage` is not a `DROP`. | `is_schema_metadata_only_rewrite` (line 617) | Modeled with `Alters`, not switched off. It compares raw `rootpage` values, so a temporary id and the page of the same table count as two B-trees (#8077). | none |
| S21. Every statement of a transaction reads pages at the WAL mark of its begin. | `Transaction::read_mark`, the pager read lock | Needed. `ReadsCorrect` fails: a reader begins, an insert commits, a checkpoint writes the row, and the reader's next statement reads the row from the new frame. This is the symptom of #8087. | `noFrozenMark` |

Reachability checks fail as they must. Another table reuses a freed page
(`reachReuse`). A reader reads the old owner of a reused page through an old
read mark (`reachOldOwner`). Recovery replays a row record above a
checkpoint boundary (`reachReplaysRow`). A reader gets `Busy`, and it gets
`SchemaUpdated` from the cookie check and from the generation check.

## Open issues

Each counterexample below is the trace that TLC prints for the variant.

- #11501, caught (`issue11501`, `RecoveryCorrect`). `CREATE t1` gets id -2
  and commits. `CREATE t2` gets id -3 and prepares. A checkpoint with the
  snapshot before `t2` gives `t1` page 3. `t2` commits after the snapshot, so
  its record stays in the log. On a crash, bootstrap gives `t1` the id -3, and
  the replay of `t2` with rootpage -3 panics.
- #11502, caught (`issue11502`, `RecoveryCorrect`). `CREATE t1` commits with
  id -100. A checkpoint takes its snapshot. `INSERT INTO t1` commits. `t1` has
  no root page yet, so the record carries -100. The checkpoint gives `t1`
  page 2 and commits. On a crash, bootstrap knows `t1` only as -2, and the row
  record with -100 finds no table. The row is lost. With `FixDisjointIds` off
  as well, the id can name another table, as in the issue.
- #11499, caught (`issue11499`, `CookieNotBehind`). A checkpoint stages the
  header with cookie 0 at its pager commit. A `CREATE` commits with cookie 1.
  The publish writes cookie 0. The lost `INSERT` in the issue is a second bug
  in the retry loop of `Statement`, which the model leaves out.
- #11500, caught (`issue11500`, `ReadsCorrect`). A statement compiles against
  `t1`. `DROP t1` commits. The statement's inner transaction begins at a new
  snapshot without a schema check and reads `t1`, which its snapshot does not
  have. With a reused page, the same path reads another table, as in the
  issue. The model shows the read side only, not the write of `nextval`.
- #8004, caught (`issue8004`, `PagesAccounted`). `CREATE t1` commits.
  `DROP t1` runs and copies the schema, where `t1` has no page, so it records
  no dropped root. A checkpoint creates the B-tree of `t1` on page 2, because
  the drop came after its snapshot, and the publish adds 2 to
  `dropped_root_pages`. The drop commits and installs its copy. Page 2 is
  allocated, but nothing names it. The same overwrite also turns published
  roots back into negative ones (`reachOwnedNegative`).
- #8537, caught (`issue8537`, `OpenCorrect`). The variant switches S16 off. The code on
  this branch has S16 (commit 367362427), so the issue looks fixed here.
- #7960, caught (`issue7960`, `KindsMatch`). `Indexes = {i1}` on table
  `t1`, with `FixDisjointIds` and `FixPersistIds` off. `CREATE TABLE t1`
  commits with id -2. `CREATE INDEX i1` takes id -3 and prepares. A
  checkpoint takes its snapshot before `i1` and gives `t1` page 3. `i1`
  commits. `INSERT INTO t1` logs the table row with -2 and the index row
  with -3. On a crash, bootstrap gives `t1` the id -3, so the index row lands
  in the table B-tree. TLC finds this mirror image first. The issue has the
  other direction, where the index gets the page and a table row lands in
  the index. `KindsMatch` catches both. `FixDisjointIds` alone closes it: with
  `FixPersistIds` still off, a probe run finds no `KindsMatch` error in
  2,232,934 distinct states. The row is then lost as in #11502, and
  `FixPersistIds` closes that. `indexes` has both fixes and no error.
- #8077, caught (`issue8077` with `NoLiveDestroy`, and `issue8077Row`
  with `SchemaRowCurrent`). The variant has `Alters` on and `FixAlterPatch` off.
  `CREATE t1` commits at 1 with id -100. `ALTER t1` reads version 1, writes
  version 2 with rootpage -100, and prepares at 2. A checkpoint takes the
  snapshot 1. The `ALTER` commits. The checkpoint creates the B-tree of
  `t1` on page 2 from version 1, and the publish patches only version 1.
  The next checkpoint sees version 1 (page 2) end at 2, followed by version
  2 (-100). The roots differ, so it destroys page 2 and creates a new empty
  B-tree. The row of `t1` is gone. A run of the Rust test from the issue at
  24c03ed9a still fails with the same symptom.
- #8087, #8091, #8092, #8093 and #8094 are fixed at 24c03ed9a. The Rust test
  of each issue passes there (five runs each). The fixing commits are not
  identified. In the model, the read mark that a transaction pins at its
  begin protects the reader in all five cases. `noFrozenMark` switches
  that off, and TLC finds the trace of #8087. A reader begins at 2. An
  `INSERT` commits at 3. A checkpoint writes the row. The reader's next
  statement reads the row from the new frame. #8091 (a replacement table on
  the reused page) and #8092 (a destroyed page) are the same kind of read
  through a newer frame. TLC reports only the first trace, so these two
  are not verified one by one. `reachOldOwner` shows that a reader with a frozen mark
  reads the old owner of a reused page. #8093 and #8094 read `sqlite_schema`
  rows. The model does not read the catalog through the dual cursor, and it
  does not collect garbage of `sqlite_schema` versions, so it cannot express
  them.

## Findings

These results come from the model. Each one is a lead for a code change, and
each change needs its own test in the Rust code first.

1. The open issues #11501, #11502, #11499, #11500 and #8004 each give a
   counterexample in the model of the code today, and each fix above closes
   its issue in the model.
2. #11502 is not fixed by keeping ids and pages apart. A row record that a
   commit logs between the checkpoint snapshot and the publish still carries
   the temporary id, and after the checkpoint no table has that id at
   recovery. Recovery must know the temporary id of a checkpointed table, or
   the log record must name the table in a way that survives the checkpoint.
3. Six special cases are not needed in the model: S2, S3, S6, S7, S10 and
   S14. Each one has a reason in the table above. The model leaves out
   writes through stale roots, index rows and readers without a WAL mark.
   Some of these cases can still be needed there. S10 is the most likely.
4. S7 (`try_get_table_id_from_root_page_at` returning `None`) is never
   reached in the model. Its comment describes a reader that begins between a
   drop's commit timestamp and its cookie publish. S8 (#9955) closed that
   window later.
5. #8077 has two causes. The publish patches one version of the
   `sqlite_schema` row by its version id, so the `ALTER` version keeps the
   temporary id. Also, `is_schema_metadata_only_rewrite` compares raw
   `rootpage` values. So the next checkpoint reads a page and then a
   temporary id of the same table as a `DROP` plus a `CREATE`. The first
   pass also puts the live root into `dropped_root_pages`, because version 1
   has an end. `FixAlterPatch` closes all three in the model.
6. An `ALTER` that commits after a checkpoint snapshot and before its
   publish logs the `sqlite_schema` row with the temporary id. No patch in
   memory can change that record. After a crash, recovery replays it above
   the boundary. In the code today, no table has that id, so the table gets
   a new empty id and loses its page. With `FixPersistIds`, the id names the
   checkpointed table, and recovery panics as in #11501. Recovery must treat
   such a record as an update of that table (`FixAlterRecovery`). This is not
   verified in Rust.
7. #8087, #8091, #8092, #8093 and #8094 pass at 24c03ed9a. In the model the
   frozen read mark of the transaction (S21) is what protects these readers.

## Not modeled

- `index_id_to_index` keyed by a binding other than the index's own (S18).
- An index created on a table that has rows. `CREATE INDEX` then fills the
  index.
- `DROP TABLE` that drops its indexes in the same commit.
- Row updates and deletes. `MvccPassiveCheckpoint.qnt` covers the row
  protocol.
- More than one `ALTER` per table, `ALTER` of an index row (rename), and
  the new column in the row data.
- Reads of `sqlite_schema` through the dual cursor, and garbage collection
  of `sqlite_schema` versions (#8093, #8094).
- Writes through a stale root page. The model checks the read side of #11500.
- The retry loop of `Statement` (the second bug of #11499).
- Readers that see the publish window half applied. The model applies the
  window in one step. In the code an active reader reads the bindings without
  the clock lock.
- Readers without a WAL mark, and the `STAGED` mark.
- Logical log truncation, aborts, VACUUM, and schema rows without a B-tree
  (views, triggers, sequences).
- Multi-page B-trees. A page is either free or the whole B-tree of one table.
- Two tables and an index in one configuration. `indexes` has one table
  and one index, so two tables with an index are not verified.
