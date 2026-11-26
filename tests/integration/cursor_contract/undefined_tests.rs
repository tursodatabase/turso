//! Undefined sequences (SPEC 2d). The model has no result for them: `step` returns `None`.
//! Today an engine does something arbitrary: it works by luck, reads another row or panics
//! with an assertion. The contract is that a debug build refuses the sequence with an error
//! or a panic whose message contains `unsupported`, so that the translator never emits it.
//!
//! Each test checks that the model calls the last op of the program illegal and the ops
//! before it legal. Then it runs the program on every engine and requires the refusal. Every
//! test is a `known_bug_*` test, because no engine refuses yet. When an engine does, the
//! test fails with "does not fail any more": remove the wrapper.
//!
//! A release build may still do anything. The tests exist only in debug builds.

use super::index_model::{ColDef, Coll, IReplay, IdxDef, Order, Val};
use super::index_program::{run_index, IOp, IndexRun, Row};
use super::model::Replay;
use super::program::{Op, ReadMode};
use super::{known_bug, run, DbKind, Layout};

/// What every failure message of this file contains. `known_bug` checks for it, so that a
/// test fails for this reason and not for another one.
const EXPECTED: &str = "undefined sequence";

const CONFIGS: [(DbKind, Layout); 5] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
    (DbKind::Ephemeral, Layout::AllInMemory),
];

const INDEX_CONFIGS: [(DbKind, Layout); 4] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
];

const TABLE_ROWS: &[(i64, i64)] = &[(10, 100), (20, 200), (30, 300)];

/// The engine result for the last op: `None` means the program ran without an error.
fn judge(config: &str, op: &str, error: Option<&str>) {
    match error {
        None => panic!("{config}: the engine accepted {op}, an undefined sequence"),
        Some(e) if e.contains("unsupported") => {}
        Some(e) => panic!(
            "{config}: the engine refused {op}, an undefined sequence, \
             without an `unsupported` message: {e}"
        ),
    }
}

/// Table cursors. Table rows are `(rowid, value)`.
#[track_caller]
fn rejects_table(rows: &[(i64, i64)], ncursors: usize, ops: &[Op]) {
    let (last, prefix) = ops.split_last().expect("a program");
    let mut model = Replay::new(rows, ncursors);
    for op in prefix {
        assert!(
            model.apply(op, None).is_some(),
            "{op:?} is illegal in the model: it must be the last op"
        );
    }
    assert!(
        model.apply(last, None).is_none(),
        "the model must call {last:?} illegal"
    );
    for (kind, layout) in CONFIGS {
        let got = run(kind, layout, rows, ncursors, ops);
        judge(
            &format!("{kind:?}/{layout:?}"),
            &format!("{last:?}"),
            got.as_ref().err().map(String::as_str),
        );
    }
}

/// Index cursors.
#[track_caller]
fn rejects_index(idef: IdxDef, rows: &[Row], nidx: usize, ops: &[IOp]) {
    let (last, prefix) = ops.split_last().expect("a program");
    let mut model = IReplay::new(rows, nidx, false, idef);
    for op in prefix {
        assert!(
            model.apply(op).is_some(),
            "{op:?} is illegal in the model: it must be the last op"
        );
    }
    let state = format!("{:?}", model.st);
    assert!(
        model.apply(last).is_none(),
        "the model must call {last:?} illegal in {state}"
    );
    for (kind, layout) in INDEX_CONFIGS {
        let cfg = IndexRun {
            kind,
            layout,
            rows,
            idef,
            nidx,
            ntbl: 0,
            mode: ReadMode::Column,
        };
        let outcome = run_index(&cfg, ops);
        judge(
            &format!("{kind:?}/{layout:?}"),
            &format!("{last:?}"),
            outcome.error.as_deref(),
        );
    }
}

/// A table program whose last op is Undefined. `$id` is the rule of SPEC 2d.
macro_rules! table_case {
    ($name:ident, $id:literal, $ncur:expr, [$($op:expr),+ $(,)?]) => {
        #[test]
        fn $name() {
            known_bug($id, EXPECTED, || rejects_table(TABLE_ROWS, $ncur, &[$($op),+]));
        }
    };
}

/// The same for an index program with the definition `$idef` and the setup rows `$rows`.
macro_rules! index_case {
    ($name:ident, $id:literal, $idef:expr, $rows:expr, $ncur:expr, [$($op:expr),+ $(,)?]) => {
        #[test]
        fn $name() {
            known_bug($id, EXPECTED, || rejects_index($idef, &$rows, $ncur, &[$($op),+]));
        }
    };
}

fn v(x: i64) -> Val {
    Val::Int(x)
}

/// Entries `(a, b, rowid)`: (1,10,1) (1,20,2) (2,10,3) (2,30,4) (3,NULL,5) (NULL,5,6).
fn index_rows() -> Vec<Row> {
    vec![
        (1, v(1), v(10)),
        (2, v(1), v(20)),
        (3, v(2), v(10)),
        (4, v(2), v(30)),
        (5, v(3), Val::Null),
        (6, Val::Null, v(5)),
    ]
}

/// One entry `('a', 1, rowid 1)`.
fn text_rows() -> Vec<Row> {
    vec![(1, Val::text("a"), v(1))]
}

fn default_idef() -> IdxDef {
    IdxDef::default()
}

fn nocase() -> IdxDef {
    IdxDef::new(
        ColDef::new(Order::Asc, Coll::NoCase),
        ColDef::new(Order::Asc, Coll::Binary),
    )
}

fn del(cur: usize, a: i64, b: i64, rowid: i64) -> IOp {
    IOp::IdxDelete {
        cur,
        a: v(a),
        b: v(b),
        rowid,
    }
}

fn add(cur: usize, a: i64, b: i64, rowid: i64, use_seek: bool) -> IOp {
    IOp::IdxInsert {
        cur,
        a: v(a),
        b: v(b),
        rowid,
        use_seek,
    }
}

fn insert(cur: usize, rowid: i64, require_seek: bool) -> Op {
    Op::Insert {
        cur,
        rowid,
        value: 1,
        require_seek,
    }
}

// ---------------------------------------------------------------------------------------
// U1: a direction change (D3). `Next` needs a forward position, `Prev` a backward one.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u1_prev_after_rewind,
    "U1",
    1,
    [Op::Rewind(0), Op::Prev(0)]
);
table_case!(
    known_bug_u1_next_after_last,
    "U1",
    1,
    [Op::Last(0), Op::Next(0)]
);
table_case!(
    known_bug_u1_prev_after_seek_rowid_hit,
    "U1",
    1,
    [Op::SeekRowid(0, 20), Op::Prev(0)]
);
table_case!(
    known_bug_u1_prev_after_not_exists_hit,
    "U1",
    1,
    [Op::NotExists(0, 20), Op::Prev(0)]
);
table_case!(
    known_bug_u1_prev_after_next,
    "U1",
    1,
    [Op::Rewind(0), Op::Next(0), Op::Prev(0)]
);
index_case!(
    known_bug_u1_idx_prev_after_rewind,
    "U1",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), IOp::Prev(0)]
);
index_case!(
    known_bug_u1_idx_next_after_last,
    "U1",
    default_idef(),
    index_rows(),
    1,
    [IOp::Last(0), IOp::Next(0)]
);
index_case!(
    known_bug_u1_idx_prev_after_next,
    "U1",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), IOp::Next(0), IOp::Prev(0)]
);

// ---------------------------------------------------------------------------------------
// U2: `Next` or `Prev` from a position that has no direction: after `Count`, `NewRowid`,
// `Found`, `NotFound` or `IdxDelete`.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u2_next_after_count,
    "U2",
    1,
    [Op::Rewind(0), Op::Count(0), Op::Next(0)]
);
table_case!(
    known_bug_u2_next_after_new_rowid,
    "U2",
    1,
    [Op::NewRowid(0), Op::Next(0)]
);
index_case!(
    known_bug_u2_idx_next_after_found,
    "U2",
    default_idef(),
    index_rows(),
    1,
    [IOp::Found(0, vec![v(1)]), IOp::Next(0)]
);
index_case!(
    known_bug_u2_idx_next_after_not_found,
    "U2",
    default_idef(),
    index_rows(),
    1,
    [IOp::NotFound(0, vec![v(9)]), IOp::Next(0)]
);
index_case!(
    known_bug_u2_idx_next_after_idx_delete,
    "U2",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), del(0, 1, 10, 1), IOp::Next(0)]
);

// ---------------------------------------------------------------------------------------
// U3: after `NullRow` only reads and a new position are legal.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u3_next_after_null_row,
    "U3",
    1,
    [Op::Rewind(0), Op::NullRow(0), Op::Next(0)]
);
table_case!(
    known_bug_u3_prev_after_null_row,
    "U3",
    1,
    [Op::Last(0), Op::NullRow(0), Op::Prev(0)]
);
table_case!(
    known_bug_u3_insert_after_null_row,
    "U3",
    1,
    [Op::NullRow(0), insert(0, 25, true)]
);
table_case!(
    known_bug_u3_delete_after_null_row,
    "U3",
    1,
    [Op::Rewind(0), Op::NullRow(0), Op::Delete(0)]
);
table_case!(
    known_bug_u3_count_after_null_row,
    "U3",
    1,
    [Op::NullRow(0), Op::Count(0)]
);
table_case!(
    known_bug_u3_new_rowid_after_null_row,
    "U3",
    1,
    [Op::NullRow(0), Op::NewRowid(0)]
);
index_case!(
    known_bug_u3_idx_next_after_null_row,
    "U3",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), IOp::NullRow(0), IOp::Next(0)]
);
index_case!(
    known_bug_u3_idx_insert_after_null_row,
    "U3",
    default_idef(),
    index_rows(),
    1,
    [IOp::NullRow(0), add(0, 7, 7, 70, false)]
);
index_case!(
    known_bug_u3_idx_delete_after_null_row,
    "U3",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), IOp::NullRow(0), IOp::Delete(0)]
);

// ---------------------------------------------------------------------------------------
// U4: `Delete` needs a cursor that is on an entry (D4).
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u4_delete_on_fresh_cursor,
    "U4",
    1,
    [Op::Delete(0)]
);
table_case!(
    known_bug_u4_delete_after_scan_end,
    "U4",
    1,
    [
        Op::Rewind(0),
        Op::Next(0),
        Op::Next(0),
        Op::Next(0),
        Op::Delete(0)
    ]
);
table_case!(
    known_bug_u4_delete_after_seek_miss,
    "U4",
    1,
    [Op::SeekRowid(0, 15), Op::Delete(0)]
);
index_case!(
    known_bug_u4_idx_delete_on_fresh_cursor,
    "U4",
    default_idef(),
    index_rows(),
    1,
    [IOp::Delete(0)]
);
index_case!(
    known_bug_u4_idx_delete_after_failed_seek,
    "U4",
    default_idef(),
    index_rows(),
    1,
    [IOp::SeekGE(0, vec![v(9)]), IOp::Delete(0)]
);

// ---------------------------------------------------------------------------------------
// U5: an insert that skips the seek (`REQUIRE_SEEK` clear, or `use_seek`) needs the seek
// token of the same key (SPEC 3).
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u5_insert_without_seek_token,
    "U5",
    1,
    [insert(0, 25, false)]
);
table_case!(
    known_bug_u5_insert_after_seek_of_another_key,
    "U5",
    1,
    [Op::SeekRowid(0, 15), insert(0, 25, false)]
);
index_case!(
    known_bug_u5_idx_insert_without_seek_token,
    "U5",
    default_idef(),
    index_rows(),
    1,
    [add(0, 7, 7, 70, true)]
);
index_case!(
    known_bug_u5_idx_insert_after_found_hit,
    "U5",
    default_idef(),
    index_rows(),
    1,
    [IOp::Found(0, vec![v(1), v(10)]), add(0, 1, 10, 9, true)]
);
index_case!(
    known_bug_u5_idx_insert_after_not_found_hit,
    "U5",
    default_idef(),
    index_rows(),
    1,
    [IOp::NotFound(0, vec![v(1), v(10)]), add(0, 1, 10, 9, true)]
);
index_case!(
    known_bug_u5_idx_insert_after_seek_of_another_key,
    "U5",
    default_idef(),
    index_rows(),
    1,
    [IOp::NotFound(0, vec![v(8)]), add(0, 7, 7, 70, true)]
);

// ---------------------------------------------------------------------------------------
// U6: `IdxGE/GT/LE/LT` need a cursor that is on an entry that still exists. After a delete
// SQLite does not restore the cursor and asserts (`vdbe.c:7112`).
// ---------------------------------------------------------------------------------------

index_case!(
    known_bug_u6_compare_after_own_idx_delete,
    "U6",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), del(0, 1, 10, 1), IOp::IdxGT(0, vec![v(1)])]
);
index_case!(
    known_bug_u6_compare_after_own_delete,
    "U6",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), IOp::Delete(0), IOp::IdxGE(0, vec![v(1)])]
);
index_case!(
    known_bug_u6_compare_after_peer_delete,
    "U6",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(1, vec![v(1)]),
        del(0, 1, 10, 1),
        IOp::IdxGT(1, vec![v(1)])
    ]
);

// ---------------------------------------------------------------------------------------
// U7: `IdxDelete` with P5 of a key that is not in the index. A key that is equal under the
// collation but not identical counts as missing. (An `IdxInsert` of a key that is in the
// index is defined: it replaces the entry.)
// ---------------------------------------------------------------------------------------

index_case!(
    known_bug_u7_idx_delete_of_missing_key,
    "U7",
    default_idef(),
    index_rows(),
    1,
    [del(0, 9, 9, 99)]
);
index_case!(
    known_bug_u7_idx_delete_of_equal_but_not_identical_key,
    "U7",
    nocase(),
    text_rows(),
    1,
    [IOp::IdxDelete {
        cur: 0,
        a: Val::text("A"),
        b: v(1),
        rowid: 1,
    }]
);

// ---------------------------------------------------------------------------------------
// U8: a read (`Column`, `RowId`, `IdxRowId`, `Observe`) needs a position that is on an
// entry, on no entry, or on a row that a peer deleted. After `Count`, `SeekEnd`, `NewRowid`,
// an own `Delete`, `IdxDelete`, a seek that missed, `Found`, `NotFound` and `NoConflict` with
// a NULL the position is not defined, and the read is refused. `SeekEnd` is a no-op in
// SQLite when the cursor is already on an entry, so no engine can define it.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u8_read_after_count,
    "U8",
    1,
    [Op::Rewind(0), Op::Count(0), Op::RowId(0)]
);
table_case!(
    known_bug_u8_column_after_count,
    "U8",
    1,
    [Op::Last(0), Op::Count(0), Op::Column(0, 1)]
);
table_case!(
    known_bug_u8_read_after_new_rowid,
    "U8",
    1,
    [Op::NewRowid(0), Op::RowId(0)]
);
table_case!(
    known_bug_u8_read_after_delete,
    "U8",
    1,
    [Op::SeekRowid(0, 20), Op::Delete(0), Op::RowId(0)]
);
table_case!(
    known_bug_u8_read_after_seek_miss,
    "U8",
    1,
    [Op::SeekRowid(0, 15), Op::Column(0, 1)]
);
index_case!(
    known_bug_u8_idx_read_after_seek_end,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::Last(0), IOp::SeekEnd(0), IOp::Observe(0)]
);
index_case!(
    known_bug_u8_idx_read_after_seek_end_on_fresh_cursor,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::SeekEnd(0), IOp::Observe(0)]
);
index_case!(
    known_bug_u8_idx_read_after_count,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), IOp::Count(0), IOp::IdxRowId(0)]
);
index_case!(
    known_bug_u8_idx_read_after_idx_delete,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), del(0, 1, 10, 1), IOp::Observe(0)]
);
index_case!(
    known_bug_u8_idx_read_after_delete,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::SeekGE(0, vec![v(1)]), IOp::Delete(0), IOp::Observe(0)]
);
index_case!(
    known_bug_u8_idx_read_after_found,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::Found(0, vec![v(1)]), IOp::Observe(0)]
);
index_case!(
    known_bug_u8_idx_read_after_not_found,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::NotFound(0, vec![v(9)]), IOp::Observe(0)]
);
index_case!(
    known_bug_u8_idx_read_after_no_conflict_with_null,
    "U8",
    default_idef(),
    index_rows(),
    1,
    [IOp::NoConflict(0, vec![Val::Null]), IOp::Observe(0)]
);

// ---------------------------------------------------------------------------------------
// U9: a write by one cursor leaves every other cursor of the tree without a position. A
// read, `Next`, `Prev`, `Delete`, `NullRow` or an `Idx*` compare from the old position is
// refused. Only a seek, `Rewind` or `Last` places the cursor again. SQLite saves the other
// cursors by key, but Turso does not promise this and no plan needs it.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u9_next_after_peer_insert,
    "U9",
    2,
    [Op::SeekRowid(0, 20), insert(1, 25, true), Op::Next(0)]
);
table_case!(
    known_bug_u9_prev_after_peer_insert,
    "U9",
    2,
    [Op::Last(0), insert(1, 5, true), Op::Prev(0)]
);
table_case!(
    known_bug_u9_read_after_peer_insert,
    "U9",
    2,
    [Op::SeekRowid(0, 20), insert(1, 25, true), Op::RowId(0)]
);
table_case!(
    known_bug_u9_column_after_peer_overwrite,
    "U9",
    2,
    [
        Op::SeekRowid(0, 20),
        Op::Insert {
            cur: 1,
            rowid: 20,
            value: 999,
            require_seek: true
        },
        Op::Column(0, 1)
    ]
);
table_case!(
    known_bug_u9_column_after_peer_delete,
    "U9",
    2,
    [
        Op::SeekRowid(0, 20),
        Op::SeekRowid(1, 20),
        Op::Delete(1),
        Op::Column(0, 1)
    ]
);
table_case!(
    known_bug_u9_delete_after_peer_delete,
    "U9",
    2,
    [
        Op::SeekRowid(0, 20),
        Op::SeekRowid(1, 20),
        Op::Delete(1),
        Op::Delete(0)
    ]
);
table_case!(
    known_bug_u9_next_after_own_delete_and_peer_insert,
    "U9",
    2,
    [
        Op::SeekRowid(0, 20),
        Op::Delete(0),
        insert(1, 25, true),
        Op::Next(0)
    ]
);
table_case!(
    known_bug_u9_next_of_ended_cursor_after_peer_insert,
    "U9",
    2,
    [
        Op::Rewind(0),
        Op::Next(0),
        Op::Next(0),
        Op::Next(0),
        insert(1, 40, true),
        Op::Next(0)
    ]
);
table_case!(
    known_bug_u9_read_of_null_row_after_peer_insert,
    "U9",
    2,
    [
        Op::Rewind(0),
        Op::NullRow(0),
        insert(1, 5, true),
        Op::RowId(0)
    ]
);
table_case!(
    known_bug_u9_null_row_without_position,
    "U9",
    2,
    [insert(1, 25, true), Op::NullRow(0)]
);
index_case!(
    known_bug_u9_idx_next_after_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(2)]),
        add(1, 7, 7, 70, false),
        IOp::Next(0)
    ]
);
index_case!(
    known_bug_u9_idx_read_after_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(2)]),
        add(1, 7, 7, 70, false),
        IOp::Observe(0)
    ]
);
index_case!(
    known_bug_u9_idx_read_after_own_insert_and_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        add(0, 7, 7, 70, false),
        add(1, 8, 8, 80, false),
        IOp::Observe(0)
    ]
);
index_case!(
    known_bug_u9_idx_read_after_peer_delete,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(1)]),
        IOp::SeekGE(1, vec![v(1)]),
        del(1, 1, 10, 1),
        IOp::Observe(0)
    ]
);
index_case!(
    known_bug_u9_idx_delete_after_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(1)]),
        add(1, 7, 7, 70, false),
        IOp::Delete(0)
    ]
);
index_case!(
    known_bug_u9_idx_compare_after_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(1)]),
        add(1, 7, 7, 70, false),
        IOp::IdxGT(0, vec![v(1)])
    ]
);
index_case!(
    known_bug_u9_idx_next_of_ended_cursor_after_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(9)]),
        add(1, 7, 7, 70, false),
        IOp::Next(0)
    ]
);
index_case!(
    known_bug_u9_idx_next_after_own_delete_and_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::SeekGE(0, vec![v(1)]),
        IOp::Delete(0),
        add(1, 7, 7, 70, false),
        IOp::Next(0)
    ]
);
index_case!(
    known_bug_u9_idx_read_of_null_row_after_peer_insert,
    "U9",
    default_idef(),
    index_rows(),
    2,
    [
        IOp::Rewind(0),
        IOp::NullRow(0),
        add(1, 7, 7, 70, false),
        IOp::Observe(0)
    ]
);
