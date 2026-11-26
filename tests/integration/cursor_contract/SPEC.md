# Cursor contract: B-tree and B-tree + MVCC overlay

This file defines what each cursor opcode promises. The reference model (`model.rs`,
`index_model.rs`) and the test programs follow it. SQLite is the reference: `vdbe.c` and
`btree.c` at master `9a938e2e8d`.

Status: draft. Every rule marked **DECISION** needs a human OK.

## 0. Test layout

| File | Purpose |
|---|---|
| `program.rs`, `index_program.rs`, `mod.rs` | Harness. Builds a VDBE program from a list of table or index operations, runs it on one engine, returns what each operation saw. Engines: WAL, MVCC, ephemeral. Layouts: all in memory, checkpointed. |
| `model.rs`, `index_model.rs` | Reference model: the contract as a pure Rust function. No database. |
| `walk.rs`, `index_walk.rs` | Random walks. Generate operations, run them on all engines and on the model, compare, shrink a failure. Known bugs are filtered by name; `NO_FILTER=1` turns the filters off. |
| `index_tests.rs`, `overlay_tests.rs`, tests in `mod.rs` | Fixed scenarios. A test marked `#[ignore]` pins a bug that is still open. |

Run: `cargo test -p core_tester --test integration_tests cursor_contract`. Add
`-- --include-ignored` to see the open bugs fail. Walk parameters: `SEED`, `ITERS`. Long
walks: `random_walk_long` and `index_random_walk_long`, with `--ignored`.

## 1. Abstract state

One store per tree, in one snapshot:

- table: finite map `rowid -> record`
- index: finite set of keys. A key is `(columns..., rowid)`. Order is the record order
  with collation and sort flags. NULL equals NULL and sorts first.

A cursor has a position and a null-row flag. The position is one of:

| Position | Meaning |
|---|---|
| `Invalid` | not on a row: fresh cursor, ended scan, empty tree, jumped seek |
| `At k` | on the entry with key `k` |
| `Gap(lo, hi)` | between two entries, `lo` and `hi` can be absent |
| `Seeked k` | a seek for `k` missed: "the place where `k` would go" |

Rules that hold for all opcodes:

- A write through any cursor changes the store. Other cursors on the same tree keep
  their logical position. `At k` stays `At k` while `k` exists. If `k` is deleted, it
  becomes `Gap(pred k, succ k)`. SQLite saves other cursors by key and never adjusts them.
- Rows that another cursor inserts after `At k` are seen by the next `Next`. There is no
  snapshot inside one transaction.
- `Column` and `RowId` return NULL when the null-row flag is set, and when the position is
  `Invalid` or `Gap`. They return the row when the position is `At k`.
- Each opcode is either defined, or its precondition is violated. A violated
  precondition is **Undefined**: the model has no result, the generator never emits it, and
  a test may check that Turso does not corrupt data.

## 2. Opcode contract

`jump` means the jump target is taken. "ended" is `Invalid`.

| Opcode | Precondition | Result |
|---|---|---|
| `OpenRead/OpenWrite` | none | position `Invalid`, null-row flag clear. Re-opening a cursor id resets it |
| `Rewind` | none | `At min`, else `Invalid` and jump. Clears null-row |
| `Last` | none | `At max`, else `Invalid` and jump. Clears null-row |
| `Next` | last move was `Rewind`, `SeekGE/GT`, `Next`, `Found`, `SeekRowid` hit | `At succ`, no jump on end; jump taken when a row was found; end gives `Invalid` and stays ended |
| `Prev` | last move was `Last`, `SeekLE/LT`, `Prev` | symmetric to `Next` |
| `Next/Prev` from `Gap(lo,hi)` | as above | `Next` gives `At hi`. `Prev` gives `At lo`. No row is skipped and none is repeated |
| `SeekGE k` | none | `At (min key >= k)`, else `Invalid` and jump |
| `SeekGT k` | none | `At (min key > k)`, else `Invalid` and jump |
| `SeekLE k` / `SeekLT k` | none | `At (max key <= k)` / `(max key < k)`, else `Invalid` and jump |
| `SeekRowid`, `NotExists` | table | hit: `At k`. Miss: `Seeked k` and jump |
| `Found`, `NotFound`, `NoConflict` | index | match means some entry has the given prefix. Hit gives `At` some match (not always the first). Miss gives `Seeked key`. `NoConflict` also jumps when the key has a NULL, even if an entry matches |
| `IdxGE/GT/LE/LT k` | position is `At` | jump if the entry compares `>=`, `>`, `<=`, `<` the prefix `k`. No state change. On a non-`At` position the model returns jump (Turso behaviour; SQLite asserts) |
| `Column`, `RowId` | none | see section 1. Position unchanged |
| `NullRow` | none | null-row flag set. Only `Column`, `RowId`, `Rewind`, `Last`, `Seek*`, `NotExists` may follow. `Next`, `Prev`, `Insert`, `Delete` after it are Undefined |
| `NewRowid` | table | returns `r` with `r` not in the store and `r > max key`. WAL gives `max+1` (or 1). MVCC may give a larger value, because the allocator never reuses rowids. Position after it: Undefined. Only a no-seek `Insert` of `r` may follow |
| `Insert k v` | see section 3 | store has `k -> v` (overwrite if present). Position: `At k` (**DECISION D1**) |
| `Delete` | position is `At` | key removed. Position: `Gap(pred, succ)` (**DECISION D2**). `Column`/`RowId` before the next move: Undefined |
| `IdxInsert key` | see section 3 | key added. A full-key duplicate is an error (unique check) or no-op, as the flags say. Position: `At key` |
| `IdxDelete key` | none | key removed. Missing key is an error when P5 is set, else no-op. Position: Undefined |
| `Count` | none | result is the number of entries. Position afterwards: Undefined |
| `Clear` | WAL only | store empty. Every cursor on the tree is `Invalid`. In MVCC the contract is "returns an error" (current limitation) |

## 2b. Index cursors and the other btree opcodes

An index store is a finite set of keys `(cols, rowid)`. `cols` is a list of values that
may be NULL. Order: compare `cols` one by one (NULL is smallest, NULL equals NULL), then
`rowid`. A prefix `p` is a list of values, at most as long as `cols`. `cmp_prefix k p`
compares only the first `|p|` columns of `k`; equal prefix means equal. The position
rules of section 1 are the same. Two more states: `Matched p` (a `Found`-style opcode hit:
the cursor is on some entry with prefix `p`, not always the first; reads are
Unspec and `Next`/`Prev` are illegal) and the seek token holds a prefix instead of a rowid.

| Opcode | Precondition | Result |
|---|---|---|
| `SeekGE p` / `SeekGT p` | none | first key with `cmp_prefix k p` `>= 0` / `> 0`; else `Invalid` and jump |
| `SeekLE p` / `SeekLT p` | none | last key with `cmp_prefix k p` `<= 0` / `< 0`; else `Invalid` and jump |
| `Found p` / `NotFound p` | none | some key has prefix `p`. `Found` jumps iff yes, `NotFound` iff no. Hit gives `Matched p`, miss gives `Seeked p` (token `p`) |
| `NoConflict p` | none | if `p` has a NULL: jump, position Unspec. Else like `NotFound` |
| `IdxGE/GT/LE/LT p` | position is `At` | jump iff `cmp_prefix k p` is `>= 0`, `> 0`, `<= 0`, `< 0`. State unchanged. On `Invalid` the model jumps (Turso) |
| `IdxInsert key use_seek` | full key not in the set; null-row clear | key added, position `At key`, all tokens cleared. Without `use_seek` it always seeks. With `use_seek` the cursor must hold the token for `key.cols`, unless `key.cols` has a NULL (Turso seeks then) |
| `IdxDelete key` | key is in the set | key removed, position Unspec (`Lost`), all tokens cleared |
| `Delete` | position `At k`, `k` in the set | same as for tables: position `Gone k` |
| `Column i`, `IdxRowId` | none | `cols[i]` and `rowid` of the entry at `At k`; NULL on `Invalid` or null-row; Unspec otherwise |
| `SeekEnd` | none | position `Lost`, token "append": the next `Insert`/`IdxInsert` without a seek is legal iff its key is above every key in the store |
| `Clear` | WAL only | store empty; every cursor on the tree (also ours) is `Invalid` |
| `DeferredSeek idx tbl` | idx cursor `At k`; table has rowid `k.rowid` | table cursor behaves like `SeekRowid` hit, with no jump. A missing row is corruption: Undefined |

`ColumnRange`, `ColumnHasField` and `RowData` read the same row as `Column`; the harness
may use any of them for a read and they must agree. `OpenDup`, `OpenEphemeral` and
`OpenAutoindex` give a btree with no MVCC: the same contract, tested as a third engine
(`Ephemeral`). `CreateBtree`, `Destroy` and `BlobRead/Write/Len` are not modelled here
(`blob_io.rs` covers blobs).

## 3. Insert seek contract

Turso differs from SQLite: it seeks only when `REQUIRE_SEEK` is set. Otherwise the
caller must have left the cursor at a `Seeked k` or `At k` position for the same key.

- `Insert k` without `REQUIRE_SEEK` is legal only if the previous opcode on that cursor
  was `NotExists`, `SeekRowid`, `NoConflict`, `Found`/`NotFound`, `NewRowid` or `SeekEnd`
  for key `k`, and no write or delete on the same tree happened in between.
- The model keeps a seek token for this. Any other write by any cursor on the tree
  clears the token.
- MVCC ignores position, so a missing token only breaks WAL. The harness runs both and
  compares them. That is how a wrong token shows up.

## 4. Decisions

- **D1.** After `Insert`, the cursor is `At k`. SQLite promises this only when no page was
  balanced. Turso saves the position after balance, so it can promise it always.
- **D2.** After `Delete`, the cursor is in the gap. This is SQLite `SAVEPOSITION`. Turso's
  `skip_advance` exists for this.
- **D3.** A direction change (`Next` after `Prev` and the reverse) is Undefined. SQLite's
  own asserts forbid it. The MVCC overlay has no direction state. Do not fix it; add a debug
  assertion if we want to enforce this.
- **D4.** `Delete` with no current row is Undefined. Today BTree ignores it and MVCC
  panics.
- **D5.** Peer reads follow SQLite: a cursor whose row was deleted by another cursor reads
  NULL.
- **D6.** `NewRowid` returns a value with a predicate, not an exact number.

## 5. Known gaps between this contract and the code

Each row is a test target. Class: **bug** = code is wrong against the contract, **probe** =
unclear, write the test and see, **by design** = contract already accounts for it.

| # | Gap | Source | Class |
|---|---|---|---|
| G1 | `Prev` after a non-balancing `Delete` skips one row | btree.rs notes 6.1 | bug |
| G2 | After a peer deletes our row, `RowId`/`Column` read the next row and `Delete` removes it | 6.5 | bug (D5) |
| G3 | Peer write clears the null-row flag, then `Column` reads the real row | 6.4 | bug |
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
| G14 | `NullRow` does not invalidate the cursor; `Next` after it clears the flag | opcodes 17 | by design (Undefined) |
| G15 | `test_cursor_with_btree_and_mvcc_fuzz` was `#[ignore]` ("fails constantly on main"). The cause was the test, not the cursor: its Checkpoint op restarted the database while a connection was still open. Fixed with one line and re-enabled; 39 seeds pass, with checkpoints and restarts | database.rs:4832 | done (no cursor bug found by this fuzzer) |

Two claims from the Turso notes turned out wrong when checked against the SQLite notes:
table `SeekLE/LT` with a text or blob key does match SQLite (both go to the last row), and
`prev_largest_reg` is documented as unused. They are not gaps.

## 5b. Bugs found by the random walks

Each one is pinned by an `#[ignore]`d test. "WAL" means the B-tree path.

| Id | Engine | Bug |
|---|---|---|
| D1 | MVCC, checkpointed | After `Insert`, `Next` and `Prev` lose the position |
| G2 | WAL, MVCC | A peer deletes our row: WAL reads the next row, MVCC reads the deleted row |
| G3 | WAL | A peer write clears the null-row flag |
| N1 | MVCC | `SeekRowid` or `NotExists` hit, then `Next`: rows are lost (overlay tests T1 to T4 show the cases) |
| N2 | WAL | `Count`, then `RowId` or `Column`: overflow panic at `pager.rs:609` |
| N4 | MVCC, checkpointed | `Delete` of a row written in this run, then `Next` or `Prev`: nothing is found |
| N5 | WAL | `Prev` panics after a peer insert, when the only row was deleted |
| N6 | WAL | Own delete and a peer insert, then `Next`: wrong row |
| N7 | WAL | Index: `Last; SeekEnd; Observe` gives "Corrupt database" |
| N9 | WAL | Index: a seek-positioned cursor loses its position on a peer write |
| N10 | WAL | Index: an ended cursor finds a peer's new entry |
| N11 | WAL | Index: `Found` hit, then `IdxInsert` with `use_seek`: the new key goes before the match and the index is out of order. SQLite seeks again. The model still lets a hit set the token; it must not |
| N12 | WAL | Index: panic on `idx < cell_count` |
| N8 | model | Index: `IdxGT` after own `IdxDelete` compares the successor. Probably a model choice. Open |

Not modelled: `DeferredSeek`, direction switches (D3), covered-chain drops in the overlay,
a failed eq-only seek that leaves the cursor on a neighbour. The table walk does not
generate `SeekEnd` and `Clear`.

## 6. Open questions

1. D1 to D6 above.
2. The index seek `FIXME` at `execute.rs:6928` (no affinity for index keys): in scope for
   this contract or not?
3. Model order for index keys with DESC columns and non-default collations: start with
   BINARY and ASC only?
