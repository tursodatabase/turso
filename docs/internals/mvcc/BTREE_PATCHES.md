# B-tree patches

## Overview

A row version in the MVCC store holds the full SQLite record of its row. An
UPDATE usually changes few columns of a row that is already in the B-tree. The
new version can store a patch of the B-tree record in place of its record:

| `PayloadKind` | `data` | The record of the version |
|---|---|---|
| `Record` | the record | `data` |
| `BtreePatch` | a column patch | the B-tree record of the row, with the patch applied |

A reader that sees a `BtreePatch` version reads the B-tree record of the row and
applies the patch.

An UPDATE of a row whose old record is in the B-tree makes the new version with
its full record. The cursor also makes a patch from the B-tree record to the new
record, and the transaction keeps the patch. After the commit writes the logical
log, `RewriteLiveVersions` stores the patch in place of the record. It does not
do this for a version that another transaction already ended, because the commit
of that transaction can read the record. The logical log and its format do not
change: the log still gets the full record.

`MvStore::btree_patches_enabled` turns patches on. It is on by default. The
passive checkpoint does not use patches yet. Index rows and `sqlite_schema` rows
always store their records.

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

In MVCC mode, only a checkpoint writes the B-tree. The truncate checkpoint holds
the blocking checkpoint lock, which waits for all MVCC transactions. Each
transaction holds the read side of the lock for its whole life. Thus no
transaction spans a truncate checkpoint, and all transactions between two
checkpoints read the same B-tree.

A patch goes from the B-tree record that its writer reads. Every reader of the
version reads before the next checkpoint, so it reads the same B-tree record.
The next checkpoint writes the record of the version or of a newer version.
Then Finalize runs GC Rule 3 with no open transaction, so it drops the chains of
all rows that the checkpoint wrote. If a version with a patch stays, for example
after a checkpoint that failed after its pager commit, the B-tree record is the
base of the patch or the record of the version. The patch makes the record of
the version from both.

## The checkpoint

The checkpoint writes the record of a `BtreePatch` version: `WriteRowStateMachine`
seeks the row, reads its B-tree record, applies the patch, and writes the
result. The B-tree record that it reads is the base of the patch.

## I/O on reads

`read_from_table_or_index`, `read_visible_into_record` and `find_visible_version`
do no I/O. They return the patch (`VisibleRecord::InBtree`). The callers:

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
holds `Record` versions. A database file and a logical log do not record the
payload kinds, so the change keeps backward compatibility.
