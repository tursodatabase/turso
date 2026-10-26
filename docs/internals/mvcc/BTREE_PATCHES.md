# B-tree patches

## Overview

A row version in the MVCC store holds the full SQLite record of its row. Most
committed versions are copies of a row that is also in the B-tree, or a small
change of one. Under constant load, the passive checkpoint keeps such a version
for each changed row, because GC Rule 3 removes the last version of a chain
only when no transaction is open (see [GC.md](GC.md)).

A version of a row that is in the B-tree can store less than its record:

| `PayloadKind` | `data` | The record of the version |
|---|---|---|
| `Record` | the record | `data` |
| `BtreePatch` | a column patch | the B-tree record of the row, with the patch applied |
| `BtreeRecord` | none | the B-tree record of the row |

A reader that sees a `BtreePatch` or `BtreeRecord` version reads the B-tree
record of the row at its own WAL read mark. The two kinds come from two places:

- **The commit (B-tree patch).** An UPDATE of a row whose old record is in the
  B-tree makes the new version with its full record. The cursor also makes a
  patch from the B-tree record to the new record, and the transaction keeps the
  patch. After the commit writes the logical log, `RewriteLiveVersions` stores
  the patch in place of the record. It does not do this for a version that
  another transaction already ended, because the commit of that transaction can
  read the record. The logical log and its format do not change: the log still
  gets the full record.
- **GC Rule 4 (B-tree record).** A current version that every reader sees in the
  B-tree frees its record. The test is the same as the test of Rule 3, but Rule
  4 also runs while transactions are open, because the chain stays in the
  store. A cursor at the row still finds the version. Rule 4 does not change a
  version that a transaction ended.

`MvStore::btree_patches_enabled` turns both on. It is on by default. Index rows
and `sqlite_schema` rows always store their records.

## Patch format

`core/mvcc/database/row_patch.rs` has the codec:

```text
patch = varint(column_count) varint(set_count)
        { varint(column_gap) varint(serial_type) value_bytes }*
```

A patch sets columns to values. Thus a patch makes the same record from every
base that has the right value in each column that the patch does not set.

A patch also sets every column that the patch of the replaced version sets
(`also_set`). Thus the patch of a version sets each column that changed since
its base, and it makes the record of the version from every B-tree record
between its base and that record. For example: the B-tree record is `(1, 2)`,
version V1 is `(9, 2)`, and version V2 is `(9, 5)`. The patch of V2 sets column
0 and column 1. A patch of V2 that sets only column 1 is correct on `(9, 2)`,
but wrong on `(1, 2)`.

A version keeps its full record when its patch is more than half of the record.

## Which B-tree record a reader sees

`B(K, M)` is the B-tree record of row K for a reader whose WAL read mark is M.
The design depends on these facts of the current code:

1. A transaction captures its WAL read mark before the clock gives it its begin
   timestamp, and the read mark does not change during the transaction
   (`begin_tx`).
2. A checkpoint writes, for each row, the newest committed version at or before
   its snapshot timestamp, or it deletes the row. It samples the snapshot before
   it writes WAL frames. After its pager commit, it stamps each version that it
   wrote with `materialized_at`, the WAL position of the commit.
3. A new reader gets a read mark at or after the last committed WAL frame.
4. GC Rule 3 drops a chain and Rule 4 frees a record only when every reader has
   a mark at or after `materialized_at` of the version. The floor includes the
   MVCC transactions, the readers that hold a WAL read mark but have no MVCC
   transaction yet (`gc_floor_reader_mark`), and the WAL backfill floor for
   readers of the database file. Rule 3 already depends on facts 1 to 4.

From facts 1 and 2: a reader R that can see version V does not see a B-tree
record that is newer than V. Such a record comes from a checkpoint whose
snapshot includes a version newer than V, so the snapshot is after the begin
timestamp of R. R captured its read mark before its begin timestamp, so before
the frames of that checkpoint.

From facts 3 and 4: when a version stores a patch, every reader that can see
the version sees a B-tree record that is at or after the base of the patch in
the history of the row:

- **The new version replaces the B-tree row.** The writer T sees no version of
  K in the store. If an older version of K was in the store, GC removed it only
  after every reader had a mark at or after the frame of its record (fact 4),
  and later readers have newer marks (fact 3). Thus every reader that exists
  when T writes, or that begins later, sees `B(K, M_T)`.
- **The new version replaces a `BtreeRecord` version U.** Rule 4 freed the
  record of U only when every reader had a mark at or after `materialized_at`
  of U. Thus every reader that can see the new version sees the record of U or
  a newer record.
- **The new version replaces a `BtreePatch` version U.** The patch of the new
  version also sets the columns of the patch of U, so it starts from the same
  base as the patch of U. By induction, every reader sees a record at or after
  that base.

Both bounds together: each reader of a version sees a record from the history of
the row between the base of the patch and the record of the version itself. The
patch sets each column that changed in that range, so it makes the record of the
version from each of these records.

### Truncate checkpoint

The truncate checkpoint holds the blocking checkpoint lock, which waits for all
MVCC transactions. Each transaction holds the read side of the lock for its
whole life. Thus no transaction spans a truncate checkpoint, and all
transactions between two checkpoints read the same B-tree. Finalize runs Rule 3
with no open transaction, so it drops the chains of all rows that the
checkpoint wrote.

### Passive checkpoint

Readers keep older WAL read marks while a passive checkpoint writes. The bounds
of the previous section hold for these readers too. No change to the WAL or to
the rules of pager snapshots is necessary.

An example: the B-tree record of row K is `(10, 100)`. Version V1 is `(11, 100)`
and stores a patch that sets column 0. A writer T begins, and then a passive
checkpoint writes V1 to the B-tree. T keeps its older read mark, so it still
sees `(10, 100)` in the B-tree. T writes V2, `(10, 101)`. The patch of V2 goes
from `(10, 100)` and also sets column 0 (from the patch of V1), so it sets
column 0 to 10 and column 1 to 101. A reader that begins after the commit of T
sees `(11, 100)` in the B-tree, and the patch makes `(10, 101)` from it too.

## GC Rule 4

Rule 4 runs in `gc_version_chain` after Rule 3. It frees the record of a
current version when the checkpoint wrote the version (`materialized_at`), the
version is in the durable bound, and the reader-mark floor is at or after
`materialized_at`. Only the GC passes of the checkpoint run Rule 4
(`free_btree_records`), because only their floor includes the readers that hold
a WAL read mark but have no MVCC transaction yet. The incremental GC of the
commit path does not run it.

Each passive Finalize runs Rule 4 on the chains of all user tables. Thus a
version whose readers had older marks at its checkpoint frees its record at a
later checkpoint.

## The checkpoint

The checkpoint writes the record of a `BtreePatch` version: `WriteRowStateMachine`
seeks the row, reads its B-tree record, applies the patch, and writes the
result. The B-tree record that the checkpoint reads is the newest one. It is
between the base of the patch and the record of the version, so the result is
right.

After the checkpoint writes the record of version V, no version needs a second
patch:

- A reader with a mark after the commit frame sees the record of V. The patch of
  V makes the record of V from it, because a patch applied to its own result
  gives the same record. A version newer than V has a patch that sets each
  column that changed since its base, which is at or before V.
- A reader with an older mark still sees the old record. It cannot see a version
  newer than V (fact 1 and fact 2).

The checkpoint never writes a `BtreeRecord` version: Rule 4 frees a record only
after a checkpoint wrote it, and later checkpoints skip versions at or below the
durable bound. `WriteRowStateMachine` asserts this.

## I/O on reads

`read_from_table_or_index`, `read_visible_into_record` and `find_visible_version`
do no I/O. They return the patch or report that the record is in the B-tree
(`VisibleRecord::InBtree`). The callers:

| Caller | What it reads | Change |
|---|---|---|
| `MvccLazyCursor::current_row` | the record, for `Column` and `RowData` | reads the B-tree record with a second `BTreeCursor`, `btree_record_reader`, and applies the patch. `current_row` can return `IOResult::IO` |
| `MvccLazyCursor::insert` | visibility only; the patch base for the new version | the base is the record at the B-tree side of the dual cursor, or a read with `btree_record_reader`. All I/O happens before the store changes |
| `MvccLazyCursor::delete`, and `insert` when it replaces a version | for portable logical changes, the record of the version that the statement ends | reads the record first. After the store ends the version, stores the record in it again (`store_record_of_ended_version`), because the commit reads the primary key from it |
| `MvccLazyCursor::seek` (`eq_only` fast path) | visibility only | none |
| `find_last_visible_version` (`eq_seek_row`) | a copy of the row, for `current_row` | none: `current_row` handles the kind |
| Checkpoint (`WriteRowStateMachine`) | the record to write | seeks the row with its own cursor and applies the patch |
| Logical log serializer | the records of versions that the committing transaction began | none: these versions keep their records until the log write |
| `MvStore::read` and tests | a `Row` | a `Row` carries its `PayloadKind`. `Row::payload` asserts that it holds a record |

`btree_record_reader` is a cursor of its own, so a read does not move the B-tree
side of the dual cursor. It skips the seek when it is already at the row.

## Recovery

Recovery replays full records from the logical log, so after a restart the store
holds `Record` versions. A checkpoint writes them, and then Rule 4 frees their
records. A database file and a logical log do not record the payload kinds, so
the change keeps backward compatibility.

## Compared with a simpler change

The simpler change is to let GC Rule 3 drop the last version of a chain under
load, after the checkpoint wrote it. Rule 3 is idle-only for two reasons
(PR 8622 and PR 8992):

- A dual-cursor scan can skip the B-tree row as shadowed, and then find the
  chain gone. The row is then missing from the scan.
- A cursor at the version reads the chain again for each column. If the chain
  is gone, the cursor returns empty columns.

Rule 4 does not remove the chain. It only replaces what the version stores, so
neither problem can occur. Rule 4 frees the row bytes of a current version, but
not the version, its chain, or its skip-list node. The simpler change frees all
of them, but it needs a dual cursor that does not lose a row when a chain goes
away during a scan. Both changes can be combined later.

| | B-tree patches and Rule 4 | Simpler change |
|---|---|---|
| Frees | the record of a current version (Rule 4), most of the record of an updated version (patch) | the whole chain |
| Risk | reads of a freed record need the B-tree, and the patch base must be right (proof above) | lost rows and empty columns in dual-cursor scans (PR 8622, PR 8992) |
| Size | codec, payload kind, cursor reads, checkpoint write, Rule 4 | dual-cursor change, Rule 3 change |
