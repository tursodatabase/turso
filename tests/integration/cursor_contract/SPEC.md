# Cursor contract: B-tree and B-tree + MVCC overlay

This file defines what each cursor opcode promises. The step function
`core/storage/spec_model/spec.rs` is the contract in code (section 2e). The reference models
(`model.rs`, `index_model.rs`) call it, and the test programs follow it. SQLite is the reference: `vdbe.c` and
`btree.c` at master `9a938e2e8d`.

Status: draft. Every rule marked **DECISION** needs a human OK.

## 0. Test layout

| File | Purpose |
|---|---|
| `program.rs`, `index_program.rs`, `mod.rs` | Harness. Builds a VDBE program from a list of table or index operations, runs it on one engine, returns what each operation saw. Engines: WAL, MVCC, ephemeral, spec-model (a `:spec-model:` database, section 2e). Layouts: all in memory, checkpointed. |
| `model.rs`, `index_model.rs` | Reference models: the table and index stores with their compare functions, on top of the step function `turso_core::storage::spec_model::spec`. No database. |
| `walk.rs`, `index_walk.rs` | Random walks. Generate operations, run them on all engines and on the test model, compare, shrink a failure. Known bugs are filtered by name; `NO_FILTER=1` turns the filters off. |
| `index_tests.rs`, `overlay_tests.rs`, `defined_tests.rs`, tests in `mod.rs` | Fixed scenarios. `defined_tests.rs` has the peer writes of section 1 and the `Found`-style seeks of section 2b. A test named `known_bug_*` asserts that a bug is still open (section 5b). `index_tests.rs::collation` has the scenarios of section 2c: DESC and NOCASE seeks, scans and range loops, `Found`, and the replacement of a key by a case variant. |

Run: `cargo test -p core_tester --test integration_tests cursor_contract`. Walk parameters:
`SEED`, `ITERS`; the index walk also has `IDEF` (`default`, `desc`, `nocase`, `rtrim`,
`mixed`) to fix the index definition of section 2c. Long walks: `random_walk_long` and `index_random_walk_long`, with
`--ignored`. They are the only ignored tests.

## 1. Abstract state

One store per tree, in one snapshot:

- table: finite map `rowid -> record`
- index: finite set of keys. A key is `(columns..., rowid)`. Order is the record order
  with collation and sort flags. NULL equals NULL and sorts first.

All cursors of a program share one transaction. Other transactions are outside this
contract: the harness has no second connection. In MVCC a write of another transaction is
not visible in our snapshot, so an entry that another transaction deleted is still there for
our cursor. A `Delete` of it is legal. It ends with the defined error `WriteWriteConflict`
when the other transaction committed or holds the row (`core/mvcc/database/mod.rs`, around
line 2167). So "the entry is under the cursor" means "in our snapshot". Only a delete by a
cursor of our own transaction (a peer, see below) moves our cursor into the gap.

A cursor has a position and a null-row flag. The position is one of:

| Position | Meaning |
|---|---|
| `Invalid` | not on a row: fresh cursor, ended scan, empty tree, jumped seek |
| `At k` | on the entry with key `k` |
| `Gap(lo, hi)` | between two entries, `lo` and `hi` can be absent |
| `Seeked k` | a seek for `k` missed: "the place where `k` would go" |

Rules that hold for all opcodes:

- A write through a cursor changes the store. Every other cursor of the tree keeps its
  position by key, as SQLite's saved cursors do (D5): a cursor `At k` stays `At k` if `k` is
  still in the store, and goes to the gap `Gone k` if the write deleted `k` (`Next` and
  `Prev` go on from the gap, a read and `Delete` are Undefined, U8 and U4). A cursor at the
  end stays at the end. The null-row flag and the direction stay. Every write ends every seek
  token. A `Clear` puts every cursor at the end.
- `Column` and `RowId` return NULL when the null-row flag is set, and when the position is
  `Invalid`. They return the row when the position is `At k`. At any other position the read
  is Undefined (U8).
- Each opcode is either defined, or its precondition is violated. A violated
  precondition is **Undefined**: the step function has no result and the generator never emits it.
  An engine must refuse an Undefined sequence in a debug build (section 2d). So an opcode is
  either supported or refused, and the translator never emits a refused one.

## 2. Opcode contract

`jump` means the jump target is taken. "ended" is `Invalid`.

| Opcode | Precondition | Result |
|---|---|---|
| `OpenRead/OpenWrite` | none | position `Invalid`, null-row flag clear. Re-opening a cursor id resets it |
| `Rewind` | none | `At min`, else `Invalid` and jump. Clears null-row |
| `Last` | none | `At max`, else `Invalid` and jump. Clears null-row |
| `Next` | last move was `Rewind`, `SeekGE/GT`, `Next`, `Found`/`NotFound`/`NoConflict` (hit or miss), `SeekRowid` hit | `At succ`, no jump on end; jump taken when a row was found; end gives `Invalid` and stays ended |
| `Prev` | last move was `Last`, `SeekLE/LT`, `Prev` | symmetric to `Next` |
| `Next/Prev` from `Gap(lo,hi)` | as above | `Next` gives `At hi`. `Prev` gives `At lo`. No row is skipped and none is repeated |
| `SeekGE k` | none | `At (min key >= k)`, else `Invalid` and jump |
| `SeekGT k` | none | `At (min key > k)`, else `Invalid` and jump |
| `SeekLE k` / `SeekLT k` | none | `At (max key <= k)` / `(max key < k)`, else `Invalid` and jump |
| `SeekRowid`, `NotExists` | table | hit: `At k`. Miss: `Seeked k` and jump |
| `Found`, `NotFound`, `NoConflict` | index | an eq-only `SeekGE` on the key as a prefix. Hit: `At` the first matching entry, forward direction, so a read and `Next` are defined and `Prev` is Undefined (U1). Miss: `Seeked key`, a read is Undefined (U8), `Next` goes to the first entry after the key. `NoConflict` also jumps when the key has a NULL, even if an entry matches, and then the cursor has no position |
| `IdxGE/GT/LE/LT k` | position is `At` | jump if the entry compares `>=`, `>`, `<=`, `<` the prefix `k`. No state change. On a non-`At` position the step function returns jump (Turso behaviour; SQLite asserts) |
| `Column`, `RowId` | none | see section 1. Position unchanged |
| `NullRow` | none | null-row flag set. Only `Column`, `RowId`, `Next`, `Prev`, `Rewind`, `Last`, `Seek*`, `NotExists` may follow. `Next` and `Prev` find no row and do not jump (SQLite: `OP_Next` on an invalid cursor, used by LEFT JOIN loops); afterwards the cursor has no position. `Insert`, `Delete` after it are Undefined |
| `NewRowid` | table | returns `r` with `r` not in the store and `r > max key` (D6). Position after it: no position; a read is Undefined (U8). Only a no-seek `Insert` of `r` may follow |
| `Insert k v` | see section 3 | store has `k -> v` (overwrite if present). Position: `At k` (**DECISION D1**) |
| `Delete` | position is `At` | key removed. Position: `Gap(pred, succ)` (**DECISION D2**). `Column`/`RowId` before the next move: Undefined |
| `IdxInsert key` | see section 3 | key added. A key equal to a stored key replaces it, the new bytes win. On a unique index with `NO_OP_DUPLICATE`, a key whose unique columns are in the index is a no-op, and the position is lost; without the flag it is the error "UNIQUE constraint failed". Position: `At key` |
| `IdxDelete key` | none | key removed. Missing key: with P5 the opcode reports a corrupt index; without P5 it is a no-op. Position afterwards: none |
| `Count` | none | result is the number of entries. Position afterwards: none; a read or a move is Undefined (U2, U8) |
| `Clear` | WAL only | store empty. Every cursor on the tree is `Invalid`. In MVCC the contract is "returns an error" (current limitation) |

## 2b. Index cursors and the other btree opcodes

An index store is a finite set of keys `(cols, rowid)`. `cols` is a list of values that
may be NULL. Order: compare `cols` one by one (NULL is smallest, NULL equals NULL), then
`rowid`. A prefix `p` is a list of values, at most as long as `cols`. `cmp_prefix k p`
compares only the first `|p|` columns of `k`; equal prefix means equal. The position
rules of section 1 are the same. The seek token holds a prefix instead of a rowid.

| Opcode | Precondition | Result |
|---|---|---|
| `SeekGE p` / `SeekGT p` | none | first key with `cmp_prefix k p` `>= 0` / `> 0`; else `Invalid` and jump |
| `SeekLE p` / `SeekLT p` | none | last key with `cmp_prefix k p` `<= 0` / `< 0`; else `Invalid` and jump |
| `Found p` / `NotFound p` | none | an eq-only `SeekGE p`. `Found` jumps iff some key has prefix `p`, `NotFound` iff none. Hit: `At` the first key with prefix `p`, direction forward, token `p` (a hit token allows an insert of the same whole key only, so with a prefix it allows nothing, U5). Miss: `Seeked p`, direction forward, token `p`; `Next` goes to the first key after `p` |
| `NoConflict p` | none | if `p` has a NULL: jump, no position (a read is Undefined, U8). Else like `NotFound` |
| `IdxGE/GT/LE/LT p` | position is `At` | jump iff `cmp_prefix k p` is `>= 0`, `> 0`, `<= 0`, `< 0`. State unchanged. On `Invalid` the step function jumps (Turso) |
| `IdxInsert key use_seek` | null-row clear | key added (an equal key is replaced, the new bytes win), position `At key`, all tokens cleared. Without `use_seek` it always seeks. With `use_seek` the cursor must hold the token for `key.cols`, unless `key.cols` has a NULL (Turso seeks then). A token of a hit allows only the same whole key |
| `IdxDelete key` | with P5: key is in the set | key removed, no position (`Lost`): a read or a move is Undefined (U2, U8). All tokens cleared. Without P5 a missing key is a no-op, also with the position `Lost` |
| `Delete` | position `At k`, `k` in the set | same as for tables: position `Gone k` |
| `Column i`, `IdxRowId` | none | `cols[i]` and `rowid` of the entry at `At k`; NULL on `Invalid` or null-row; Undefined (U8) at `Seeked`, `Gone` and `Lost` |
| `SeekEnd` | none | position `Lost`, token "append": the next `Insert`/`IdxInsert` without a seek is legal iff its key is above every key in the store |
| `Clear` | WAL only | store empty; every cursor on the tree (also ours) is `Invalid` |
| `DeferredSeek idx tbl` | idx cursor `At k`; table has rowid `k.rowid` | table cursor behaves like `SeekRowid` hit, with no jump. A missing row is corruption: Undefined |

`ColumnRange`, `ColumnHasField` and `RowData` read the same row as `Column`; the harness
may use any of them for a read and they must agree. `OpenDup`, `OpenEphemeral` and
`OpenAutoindex` give a btree with no MVCC: the same contract, tested as a third engine
(`Ephemeral`). `CreateBtree`, `Destroy` and `BlobRead/Write/Len` are not modelled here
(`blob_io.rs` covers blobs).

## 2c. Column order and collation

Each index column has a sort order (`ASC` or `DESC`) and a collation (`BINARY`, `NOCASE` or
`RTRIM`). The columns have no declared type, so values keep their type. The test
model knows three value types: NULL, integer and text. Evidence: SQLite `vdbeaux.c`
(`sqlite3VdbeRecordCompareWithSkip`, `vdbeRecordCompareDebug`) and `vdbe.c`
(`OP_Seek*`, `OP_Idx*`, `OP_Found`), checked with a test program on SQLite 3.54.1.

**Order of one column.** The result of `cmp(stored, key)` for column `i`:

1. Types decide first: NULL < integer < text. The collation does not matter across types.
   A text value never equals an integer (no affinity): the key `'1'` does not find the
   stored `1`.
2. Two integers compare as numbers. NULL equals NULL.
3. Two texts compare with the collation of the index column (not of the seek key):
   - `BINARY`: byte order, then length.
   - `NOCASE`: the ASCII letters `A` to `Z` are folded to lower case. Other bytes are not
     folded. So the characters `[ \ ] ^ _ `` ` `` sort before the letters, and `É` is not `é`.
   - `RTRIM`: trailing spaces (only `0x20`) are removed from both sides, then `BINARY`.
4. A `DESC` column negates the result of steps 1 to 3. NULL then comes last, and text
   comes before integers.
5. The trailing rowid is always `ASC` and `BINARY`, also in a `DESC` index.

Two values are equal when `cmp` is zero: `'A'` equals `'a'` under `NOCASE`, `'a'`
equals `'a '` under `RTRIM`. Text with a NUL byte is not generated (SQLite stops the
`NOCASE` compare at a NUL byte).

**Prefix compare.** `cmp_prefix k p` compares the first `|p|` columns, each with its own sign
and collation. If all are equal, the prefix is equal and the result is zero. A `DESC`
column does not flip the tie-break: `SeekGE` still lands on the first entry of the equal
prefix, in index order, and `Next` moves along the index order, so for a `DESC` column it
moves to smaller values. `Seek*` and `Idx*` use this compare as in 2b.

**Equality opcodes.** `Found`, `NotFound` and `NoConflict` test `cmp_prefix = 0` under the
column collations. `DESC` does not matter. A hit is on the first match in index order (section 2b).

**Same key.** Two full keys that are equal under the collations and have the same rowid are
the same entry. An `IdxInsert` of a key that is equal to a stored key replaces the entry, also
when the bytes differ (a case variant under `NOCASE`): a later read returns the new bytes. This
is what the B-tree does: it overwrites the cell. A test must not `IdxDelete` with a key that
is equal but not identical (Undefined, U7). A delete followed by an insert of a key that
differs only by case (or trailing spaces) is legal. A later read must return the new bytes.

**Keys without a rowid.** An index with `has_rowid = false` (the indexes of `DISTINCT` and of
compound selects) stores only the columns. Two keys with equal columns are the same entry.

## 2d. Undefined sequences

**Rule.** When an op violates its precondition, a debug build of the engine panics with a
message that starts with `unsupported: U<n>: <why>`. It must be a panic, not an error:
an error is a defined outcome (for example "UNIQUE constraint failed") that a caller can
handle or ignore, and a panic cannot be mistaken for one. The spec-model storage (section
2e) does this. A release build may do anything (for now). The step function calls the op illegal (`step` returns `None`) and the generator
never emits it. `undefined_tests.rs` checks both sides: the test model says illegal, and every
engine refuses. These tests are `known_bug_u*` tests until an engine refuses.

| Id | Undefined sequence | Decision |
|---|---|---|
| U1 | `Next` without a forward position, or `Prev` without a backward one (`Rewind` then `Prev`, `Last` then `Next`, `Next` then `Prev`). Also `Prev` after a `SeekRowid` or `NotExists` hit: only `Next` may follow it, and a backward scan starts with `Last`, `SeekLE` or `SeekLT` | D3 |
| U2 | `Next` or `Prev` from a position that has no direction: after `Count`, `NewRowid`, `IdxDelete`, a `SeekRowid` or `NotExists` that missed | section 1 |
| U3 | After `NullRow`: `Insert`, `IdxInsert`, `Delete`, `Count`, `NewRowid` (`Next` and `Prev` are legal and find no row) | section 2 |
| U4 | `Delete` with no entry under the cursor: fresh cursor, scan ended, failed seek | D4 |
| U5 | `Insert` without `REQUIRE_SEEK`, or `IdxInsert` with `use_seek`, without the seek token of the same key. The token of a `Found` or `NotFound` hit allows only the same whole key, so with a prefix it allows nothing (SQLite seeks again); a miss allows every key with that prefix. Two places need no token: the entry of the key is under the cursor (an update in place), and the cursor is in the gap of its own `Delete` of the key (an update that deletes and inserts the row) | section 3 |
| U6 | `IdxGE/GT/LE/LT` on a cursor whose entry was deleted (by itself or by another cursor) | SQLite asserts, `vdbe.c:7112` |
| U7 | `IdxDelete` with P5 of a key that is not in the index, and of a key that is equal under the collation but not identical (section 2c). The opcode reports a corrupt index | section 2c |
| U8 | A read (`Column`, `RowId`, `IdxRowId`) at a position that is not defined: after `Count`, `SeekEnd`, `NewRowid`, a `Delete` (own, or of the entry by a peer), `IdxDelete`, an exact seek that missed (also `Found`, `NotFound`), `NoConflict` with a NULL | section 1 |
| U9 | Retired. It was: a use of the old position after a write by another cursor. A peer now keeps its position by key (section 1). The id is not used again | D5 |

The goal is that no op sequence has an open result: it has one defined result, or it is in
this list. `NewRowid` is defined by a predicate (D6): any rowid that is unused and
above every key. SQLite documents only "not previously used as a key". A column with
AUTOINCREMENT needs a stricter rule (never below an earlier rowid); it is not modelled.

## 2e. Spec-model storage

`core/storage/spec_model/` is the contract in code, for debug builds. `spec.rs` is a pure step
function: a cursor shape and an opcode give the new shape and the write, or the rule of
section 2d. `cursor.rs` is `SpecModelCursor`, a `CursorTrait` implementation that keeps a tree in
a sorted collection (a map for a table, a sorted list of records for an index, ordered with
the B-tree record compare) and does no I/O. An illegal opcode panics with
`unsupported: U<n>: ...`.

**The `:spec-model:` database.** A database opened with the path `:spec-model:` is like
`:memory:`, but every tree is a spec-model tree: user tables and indexes, `sqlite_schema` (root 1), the
backing trees of index methods, and the ephemeral trees of `OpenEphemeral`, `OpenAutoindex`
and `OpenDup`. A release build refuses
the path. `Database::new` picks the storage once, from the path
(`storage::tree_store::tree_storage_for_path`). The pager of each connection then holds a
`TreeStore`. The opcodes that open a cursor or create, destroy or clear a tree, and the
transaction and savepoint entry points of the pager, ask it. The VDBE has no code for the
spec-model: every other call goes through `CursorTrait`. The pager still allocates the root
pages, keeps the header and takes the locks.

A version of the database is an immutable map from root page to tree. A read transaction
keeps the version it started with. A write transaction changes a copy, and the first write
to a tree copies that tree. Commit publishes the copy, rollback drops it. A savepoint (also
the statement savepoint) keeps the copy of its start, and a rollback to it puts that copy
back. Temp tables stay on a real B-tree: the temp database has no tree store.

**The spec-model has the WAL rules only.** One write transaction runs at a time and a read
transaction sees the version it started with. MVCC (concurrent writers, row-level
`WriteWriteConflict`, `BEGIN CONCURRENT`) is not modelled yet. The data structure would not
change. Only the commit rule would: check the write set against the versions published since
the snapshot. For now `PRAGMA journal_mode = 'mvcc'` does nothing, and the sqltest tests that
need MVCC carry `@skip-if spec-model` or `@skip-file-if spec-model`.

These features return the error `unsupported in spec-model storage: <feature>`: `ATTACH`,
`VACUUM INTO`, `WITHOUT ROWID` tables, materialized views, the page pragmas `page_count`,
`page_size`, `freelist_count`, `max_page_count`, and the encryption pragmas. `VACUUM` does
nothing. `PRAGMA wal_checkpoint` copies only the pages, which hold no rows. `PRAGMA
journal_mode` with a value does not change the mode; it returns the current mode. `integrity_check` checks the empty pages and returns
`ok`. The sqltest runner opens `:spec-model:` with `--spec-model` (`--backend rust`), and skips
the tests that need a pre-built database file.

A process that opened a spec-model tree prints counters (trees, cursors, steps per opcode) to
stderr at exit.

**What the spec-model cursor infers.** The cursor sees only the calls of `CursorTrait`. An opcode
can make several calls, and it can make none. The spec-model cursor maps calls to the ops of
`spec.rs`:

- `seek_to_last` starts `NewRowid`. The reads and the random seeks after it are part of it,
  and `next` or a seek that misses ends it. After a random seek that misses, the next insert
  uses that seek.
- On an index, an eq-only `SeekGE` is `Found`. It is also the seek of `NotFound`,
  `NoConflict`, `IdxInsert` and `IdxDelete`.
- An eq-only seek followed by an insert of the same key can be the own seek of `Insert` with
  `REQUIRE_SEEK` or of `IdxInsert` without `USE_SEEK`, or a seek opcode and an insert that
  uses its token. The calls are the same. The cursor keeps both readings (they differ only
  in the direction) and refuses an op only when it is illegal in both.
- `holds_row` (the no-op check of `Insert`) and `duplicate` (`OpenDup`) are generic
  `CursorTrait` methods.
- An insert in place compares keys with the compare of the tree (`Tree::same_entry`), not
  byte by byte. A hit token names one entry when it has all key columns. The backing trees
  of an index method compare only a prefix of their records.

What the cursor cannot see, and what it costs:

| Sequence | Why | Cost |
|---|---|---|
| `NoConflict` with a NULL | the opcode makes no call | U8 after it is not found. U3 after `NullRow`, `NoConflict` with a NULL and a write is refused although it is legal (the walk filter `MI1` avoids it) |
| `IdxDelete` | a seek, a read and `Delete`; the cursor ends in the gap with a direction | U2 (`Next` after `IdxDelete`) is not found |
| `IdxDelete` with P5 of a missing key | the VDBE returns `Corrupt` after the seek and calls no delete | U7 is an error, not a refusal |
| `Insert` or `IdxInsert` that seeks, after `NullRow` | the own seek reads as a seek opcode, which is legal after `NullRow` | U3 is not found |
| `IdxInsert` with `NO_OP_DUPLICATE` of a duplicate | the opcode stops after the seek | the position stays on the duplicate instead of no position |
| U1 and U2 after an eq-only seek and an insert of the same key | two readings | refused only when both readings are illegal |

The undefined tests mark these cases `spec_model_gap`: the `:spec-model:` engine must not
refuse them, so that a better inference shows up. Every other undefined test requires the
refusal from the `:spec-model:` engine.

`spec::after_peer_write` gives the shape of the other cursors after a write (section 1).
The spec-model storage applies it lazily, when a cursor sees that the tree version moved.

The test models use the same step function. They differ from the spec-model storage only in the
store and the compare: a list of `(rowid, value)` and a list of `(cols, rowid)` with the
compare of section 2c.

The spec-model storage gives `max + 1` for `NewRowid`, or 1 for an empty table. This is a
choice of the spec-model, not a requirement: the contract is only the predicate of D6 (unused, and above every
key). The test models check an engine's rowid against that predicate and never against
`max + 1`. WAL and MVCC may return different values, and both are correct. The step function accepts
the rowid with an append token: the next no-seek `Insert` must be above every key.

## 3. Insert seek contract

Turso differs from SQLite: it seeks only when `REQUIRE_SEEK` is set. Otherwise the
caller must have left the cursor at a `Seeked k` or `At k` position for the same key.

- `Insert k` without `REQUIRE_SEEK` is legal only if the previous opcode on that cursor
  was `NotExists`, `SeekRowid`, `NoConflict`, `Found`/`NotFound`, `NewRowid` or `SeekEnd`
  for key `k`, and no write or delete on the same tree happened in between.
- It is also legal when the cursor is `At k` (an update in place), and when the cursor is in
  the gap of its own `Delete` of `k` (an `UPDATE` that deletes the row and inserts it again).
  The B-tree is then at the place of `k`.
- The step function keeps a seek token for this. Any other write by any cursor on the tree
  clears the token.
- MVCC ignores position, so a missing token only breaks WAL. The harness runs both and
  compares them. That is how a wrong token shows up.

## 4. Decisions

- **D1.** After `Insert`, the cursor is `At k`. SQLite promises this only when no page was
  balanced. Turso saves the position after balance, so it can promise it always.
- **D2.** After `Delete`, the cursor is in the gap. This is SQLite `SAVEPOSITION`. Turso's
  `skip_advance` exists for this.
- **D3.** A direction change (`Next` after `Prev` and the reverse) is Undefined. SQLite's
  own asserts forbid it. The MVCC overlay has no direction state. Do not make it work: a
  debug build refuses it (U1, section 2d).
- **D4.** `Delete` with no current row is Undefined. Today BTree ignores it and MVCC
  panics. A debug build refuses it with an `unsupported` panic (U4).
- **D5.** Another cursor's write leaves our cursor at its position by key, as SQLite's saved
  cursors do. If the write deleted our entry, we are in the gap. (This was U9, Undefined.
  Window functions need it: the corpus has 2,096 tests where an `OpenDup` reader of the
  window buffer stays on its row while the writer cursor runs `NewRowid`, `Insert` and
  `Delete`.)
- **D6.** `NewRowid` returns a value with a predicate, not an exact number.

## 5. Known gaps between this contract and the code

Each row is a test target. Class: **bug** = code is wrong against the contract, **probe** =
unclear, write the test and see, **by design** = contract already accounts for it.

| # | Gap | Source | Class |
|---|---|---|---|
| G1 | `Prev` after a non-balancing `Delete` skips one row | btree.rs notes 6.1 | bug |
| G2 | After a peer deletes our row, `RowId`/`Column` read the next row and `Delete` removes it | 6.5 | by design (U8, U4: the cursor is in the gap) |
| G3 | Peer write clears the null-row flag, then `Column` reads the real row | 6.4 | bug, see 5b |
| G4 | MVCC `Column` after `Insert` that updates a B-tree-resident row reads the old record | mvcc 3.8 | probe, likely bug (D1) |
| G5 | Overlay key dropped when a chain becomes "covered" and the B-tree peek differs | mvcc D3 | probe |
| G6 | B-tree rows already skipped as shadowed are not revisited after rollback to a savepoint | mvcc D14 | probe |
| G7 | Deferred seek ignores a missing table row, so `Column` reads the next row | opcodes 16 | probe (SQLite: corrupt) |
| G8 | Failed range seek leaves `has_record` true on the neighbour (BTree), nearest row (MVCC), `End` (`exists`) | opcodes 4, 8 | probe: contract says `Invalid`, so `Column` must be NULL |
| G9 | `OpenWrite` keeps a cursor with the same root page and its position | opcodes 1 | probe |
| G10 | `Delete` without a row: BTree silent, MVCC panics | opcodes 12 | by design (D4) |
| G11 | `NewRowid` on MVCC does not reuse rowids; `GoNext` leaves a garbage position | mvcc 3.5 | by design (D6) |
| G12 | MVCC `Clear` errors, `Destroy` is a no-op | opcodes 19 | by design |
| G13 | `Count` moves MVCC cursor to the end | opcodes 18 | by design (position Undefined) |
| G14 | `NullRow` does not invalidate the cursor; `Next` after it clears the flag | opcodes 17 | by design (`Next` finds no row; a read after it is Undefined) |
| G15 | `test_cursor_with_btree_and_mvcc_fuzz` was `#[ignore]` ("fails constantly on main"). The cause was the test, not the cursor: its Checkpoint op restarted the database while a connection was still open. Fixed with one line and re-enabled; 39 seeds pass, with checkpoints and restarts | database.rs:4832 | done (no cursor bug found by this fuzzer) |

Two claims from the Turso notes turned out wrong when checked against the SQLite notes:
table `SeekLE/LT` with a text or blob key does match SQLite (both go to the last row), and
`prev_largest_reg` is documented as unused. They are not gaps.

## 5b. Bugs found by the random walks

Each one is pinned by a `known_bug_<id>_*` test. The test runs the contract check inside
`known_bug(id, expected, body)` (`mod.rs`), which requires that the check still fails. When
you fix a bug, the test fails with "does not fail any more". Then remove the `known_bug`
wrapper: the body is a normal regression test. "WAL" means the B-tree path. An id with the
prefix `IDX_` is the same bug as the id without it, found with an index cursor.

SQL reachability: CL1 (issue 9285) and D1 are reproduced from SQL. D1: a one-pass `UPDATE` of
a table with a `UNIQUE ON CONFLICT REPLACE` column scans, seeks, deletes and inserts with one
table cursor; after the `Insert`, `Next` loses the rows that are in the B-tree (MVCC, rows
checkpointed; `known_bug_d1_update_with_replace_conflict_then_next`, PR 12484). For G4, N1,
N4, OV1, R1 and O1 about 100 hand-written statements and 2,100 random transactions (with and without
a checkpoint, compared with sqlite3 3.51.0) gave no difference. G4 is tracked in issue 8197,
which says that no ordinary SQL reaches it. The translator uses two-pass plans (an
ephemeral table) wherever one cursor would write and another would read the same tree.

| Id | Engine | Bug |
|---|---|---|
| D1 | MVCC, checkpointed | After `Insert`, `Next` and `Prev` lose the position. From SQL: `INSERT INTO t VALUES (3,3,0)`, checkpoint, `INSERT INTO t VALUES (1,1,0),(2,2,0)`, then `UPDATE t SET u = CASE WHEN id = 1 THEN 2 ELSE u END, v = v + 1` with `u UNIQUE ON CONFLICT REPLACE` leaves row 3 at `v = 0` |
| G4 | MVCC, checkpointed | After an overwrite by `Insert` on the same cursor, `Column` reads the old record |
| R1 | all | `RowData` and `Column` disagree: on a null row, after `IdxInsert`, on a cursor without an entry, and with a pending `DeferredSeek` |
| O1 | ephemeral | `OpenDup` rejects index cursors |
| OV1 | MVCC | Index: an equality-only prefix seek, then `Next`, skips overlay keys or ends early. `Found` and `NotFound` hits reach it too: `Found (1)`, `Next`, `Next`: the second `Next` ends the scan (entries in memory) |
| N1 | MVCC | `SeekRowid` or `NotExists` hit, then `Next`: rows are lost (overlay tests T1 to T3 show the cases) |
| N4 | MVCC, checkpointed | `Delete` of a row written in this run, then `Next` or `Prev`: nothing is found |
| CL1 | MVCC | Index, NOCASE or RTRIM: `IdxDelete` of `('a', 1)` and `IdxInsert` of `('A', 1)` (equal under the collation, same rowid): a later read of the entry returns the old bytes `'a'`, not `'A'`. SQLite and WAL return the new bytes. Needs no checkpointed row. Reachable from SQL (`UPDATE t SET a='A'`, then a covering index scan): issue 9285 |
| U1 | all | Undefined: `Next` or `Prev` in the wrong direction (D3). No engine refuses it with `unsupported` |
| U2 | all | Undefined: `Next` or `Prev` after `Count`, `NewRowid`, `IdxDelete`. No engine refuses it |
| U3 | all | Undefined: `Insert`, `IdxInsert`, `Delete`, `Count` or `NewRowid` after `NullRow`. WAL answers a `NewRowid` with 1 (was N3). No engine refuses it. (`Next` and `Prev` after `NullRow` were U3; the corpus emits them in LEFT JOIN loops, and SQLite allows them) |
| U4 | WAL, MVCC | Undefined: `Delete` without an entry (D4). WAL panics on `current_page >= 0`, MVCC with "Cannot delete: no current row". Neither message says `unsupported` |
| U5 | all | Undefined: an insert without the seek token of its key, also after a `Found` or `NotFound` hit (was N11: WAL and ephemeral write the new key at the hit and leave the index out of order). No engine refuses it |
| U6 | all | Undefined: `IdxGE/GT/LE/LT` after the entry was deleted (was N8; SQLite asserts, `vdbe.c:7112`). The engines compare the next entry or the end |
| U7 | all | Undefined: `IdxDelete` of a missing key, and the collation-equal variant (section 2c). WAL and MVCC report a corrupt index, not `unsupported`. (`IdxInsert` of an existing key was U7; it is now defined as a replace) |
| U8 | all | Undefined: a read at an undefined position (was N2 and N7: WAL panics with an overflow after `Count`, and reports "Corrupt database" after `SeekEnd`). No engine refuses it |
| U9 | - | Retired: a peer now keeps its position by key (D5). The rows that were U9 (G2, G3, N5, N6, N9, N10, N12, IDX_G2, IDX_G3, IDX_N6) are now defined: `defined_tests.rs` runs them, and the engines that disagree are G3, G4, PW2, PD1 and PD2 below. G4 is reached by a peer overwrite too: `SeekRowid(0, 20)`, `Insert(1, 20, 999)`, `Column(0)` reads 200 in MVCC checkpointed |
| G3 | WAL, ephemeral | A cursor with the null-row flag reads the real row after a peer write. `Rewind(0)`, `NullRow(0)`, `Insert(1, 5)`, `RowId(0)` gives 10, not NULL. The ephemeral engine also ignores a `NullRow` that comes after the peer write. Table and index |
| PW2 | WAL | Index: after an own `IdxInsert` and a write by a peer, the cursor is not on its new entry. `IdxInsert(0, (0,0,70))`, `IdxInsert(1, (8,8,80))`, `Observe(0)` gives `(1,10,1)`, not `(0,0,70)`; in walks a read also gives NULL, and `Prev` returns the new entry again |
| PD1 | WAL, ephemeral | `Prev` from the gap of an own `Delete`, after a peer insert, goes wrong. Rows 8, 20: `SeekGT(0, 19)`, `Delete(0)`, `Last(1)`, `Delete(1)`, `Insert(0, 16)`, `Prev(1)` panics (`self.current_page >= 0`). Rows 10, 20, 30: `SeekGT(0, 25)`, `Delete(0)`, `Last(1)`, `Delete(1)`, `Insert(0, 16)`, `Prev(1)` gives 10, not 16 |
| PD2 | WAL | Index: `Delete` of the entry under the cursor after a peer `IdxDelete` panics (`idx < cell_count`). Index `(a NOCASE, b)`, entries `(NULL,'[')`, `('B',NULL)`, `('_','É')`: `Found(1, 'B')`, `IdxDelete(0, ('_','É'))`, `Delete(1)` |
| FM1 | all | Index: `Next` after a `Found`, `NotFound` or `NoConflict` that missed does not go to the first entry after the key. MVCC ends the scan: `NotFound (1, 15)`, `Next` gives nothing, not `(2, 10)`. WAL and ephemeral skip an entry: entries `(3,2,10)`, `(3,2,13)`, `Found (3, 1)` (miss), `Next` gives rowid 13, not 10; `NoConflict (2, 20)`, `Next` gives `(3,NULL,5)`, not `(2,30,4)` |

Not modelled: `DeferredSeek`, direction switches (D3), covered-chain drops in the overlay,
a failed eq-only seek that leaves the cursor on a neighbour. An eq-only `SeekGE` for a key
that is equal to no entry jumps in every engine, even when larger entries exist. This is the
hint of the translator, not a bug, so the tests of section 2c use eq-only seeks only for keys
that have an equal entry. The index walk does not generate eq-only seeks (OV1 covers them).
With a DESC or collation index, OV1 is the same as with the default index. The table walk does not
generate `SeekEnd` and `Clear`.

## 6. Open questions

1. D1 to D6 above.
2. The index seek `FIXME` at `execute.rs:6928` (no affinity for index keys): in scope for
   this contract or not?
3. Model order for index keys with DESC columns and non-default collations: done, see
   section 2c. Not covered: BLOB values, REAL values, affinity, custom collations, text with
   a NUL byte, UTF-16 databases, expression indexes.
