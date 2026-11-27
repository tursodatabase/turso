//! Undefined sequences (SPEC 2d). The model has no result for them: `step` returns `None`.
//! Today an engine does something arbitrary: it works by luck, reads another row or panics
//! with an assertion. The contract is that a debug build refuses the sequence with a panic
//! whose message starts with `unsupported`, so that the translator never emits it. An error is
//! not enough: an error is a defined outcome that a caller can handle or ignore.
//!
//! Each test checks that the model calls the last op of the program illegal and the ops
//! before it legal. Then it runs the program on every engine and requires the refusal. Every
//! test is a `known_bug_*` test, because no engine refuses yet. When an engine does, the
//! test fails with "does not fail any more": remove the wrapper.
//!
//! The `:model:` engine (`DbKind::Model`) is not in the `known_bug` part: it must refuse
//! every sequence. A case marked `model_gap` is one that the model engine cannot see, because
//! it infers the ops from the calls of the VDBE on `CursorTrait`. The test then requires that
//! the model engine does not refuse it, so that a better inference shows up.
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

/// Asserts that the model calls the last op of the table program illegal and the ops before
/// it legal.
#[track_caller]
fn assert_table_illegal(rows: &[(i64, i64)], ncursors: usize, ops: &[Op]) {
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
}

/// The same for an index program.
#[track_caller]
fn assert_index_illegal(idef: IdxDef, rows: &[Row], nidx: usize, ops: &[IOp]) {
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
}

fn index_run(
    idef: IdxDef,
    rows: &[Row],
    nidx: usize,
    kind: DbKind,
    layout: Layout,
) -> IndexRun<'_> {
    IndexRun {
        kind,
        layout,
        rows,
        idef,
        nidx,
        ntbl: 0,
        mode: ReadMode::Column,
    }
}

/// The error of the `:model:` engine for the table program, if any.
fn model_table_error(rows: &[(i64, i64)], ncursors: usize, ops: &[Op]) -> Option<String> {
    run(DbKind::Model, Layout::AllInMemory, rows, ncursors, ops).err()
}

/// The error of the `:model:` engine for the index program, if any.
fn model_index_error(idef: IdxDef, rows: &[Row], nidx: usize, ops: &[IOp]) -> Option<String> {
    let cfg = index_run(idef, rows, nidx, DbKind::Model, Layout::AllInMemory);
    run_index(&cfg, ops).error
}

/// Requires that the `:model:` engine does not refuse a program of a `model_gap` case.
#[track_caller]
fn model_misses(error: Option<String>, why: &str) {
    if let Some(e) = &error {
        assert!(
            !e.starts_with("panic: unsupported"),
            "the model engine refuses this program now, so it is not a gap any more \
             ({why}): remove `model_gap` from the case. {e}"
        );
    }
}

/// The engine result for the last op: `None` means the program ran without an error.
fn judge(config: &str, op: &str, error: Option<&str>) {
    match error {
        None => panic!("{config}: the engine accepted {op}, an undefined sequence"),
        // The harness writes a caught panic as `panic: <message>`.
        Some(e) if e.starts_with("panic: unsupported") => {}
        Some(e) => panic!(
            "{config}: the engine refused {op}, an undefined sequence, \
             but not with an `unsupported` panic: {e}"
        ),
    }
}

/// Table cursors. Table rows are `(rowid, value)`.
#[track_caller]
fn rejects_table(rows: &[(i64, i64)], ncursors: usize, ops: &[Op]) {
    assert_table_illegal(rows, ncursors, ops);
    let last = ops.last().expect("a program");
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
    assert_index_illegal(idef, rows, nidx, ops);
    let last = ops.last().expect("a program");
    for (kind, layout) in INDEX_CONFIGS {
        let cfg = index_run(idef, rows, nidx, kind, layout);
        let outcome = run_index(&cfg, ops);
        judge(
            &format!("{kind:?}/{layout:?}"),
            &format!("{last:?}"),
            outcome.error.as_deref(),
        );
    }
}

/// A table program whose last op is Undefined. `$id` is the rule of SPEC 2d. The `:model:`
/// engine must refuse it, unless the case gives a `model_gap` reason.
macro_rules! table_case {
    ($name:ident, $id:literal, $ncur:expr, [$($op:expr),+ $(,)?]) => {
        #[test]
        fn $name() {
            let ops = [$($op),+];
            assert_table_illegal(TABLE_ROWS, $ncur, &ops);
            judge(
                "Model",
                &format!("{:?}", ops.last().unwrap()),
                model_table_error(TABLE_ROWS, $ncur, &ops).as_deref(),
            );
            known_bug($id, EXPECTED, || rejects_table(TABLE_ROWS, $ncur, &ops));
        }
    };
    ($name:ident, $id:literal, $ncur:expr, [$($op:expr),+ $(,)?], model_gap: $why:literal) => {
        #[test]
        fn $name() {
            let ops = [$($op),+];
            model_misses(model_table_error(TABLE_ROWS, $ncur, &ops), $why);
            known_bug($id, EXPECTED, || rejects_table(TABLE_ROWS, $ncur, &ops));
        }
    };
}

/// The same for an index program with the definition `$idef` and the setup rows `$rows`.
macro_rules! index_case {
    ($name:ident, $id:literal, $idef:expr, $rows:expr, $ncur:expr, [$($op:expr),+ $(,)?]) => {
        #[test]
        fn $name() {
            let ops = [$($op),+];
            let rows = $rows;
            assert_index_illegal($idef, &rows, $ncur, &ops);
            judge(
                "Model",
                &format!("{:?}", ops.last().unwrap()),
                model_index_error($idef, &rows, $ncur, &ops).as_deref(),
            );
            known_bug($id, EXPECTED, || rejects_index($idef, &rows, $ncur, &ops));
        }
    };
    ($name:ident, $id:literal, $idef:expr, $rows:expr, $ncur:expr, [$($op:expr),+ $(,)?], model_gap: $why:literal) => {
        #[test]
        fn $name() {
            let ops = [$($op),+];
            let rows = $rows;
            model_misses(model_index_error($idef, &rows, $ncur, &ops), $why);
            known_bug($id, EXPECTED, || rejects_index($idef, &rows, $ncur, &ops));
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
// U2: `Next` or `Prev` from a position that has no direction: after `Count`, `NewRowid` or
// `IdxDelete`. (`Found` and `NotFound` are eq-only seeks, so `Next` is legal after them.)
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
    known_bug_u2_idx_next_after_idx_delete,
    "U2",
    default_idef(),
    index_rows(),
    1,
    [IOp::Rewind(0), del(0, 1, 10, 1), IOp::Next(0)],
    model_gap: "IdxDelete calls an eq-only seek and Delete; Delete leaves a direction"
);

// ---------------------------------------------------------------------------------------
// U3: after `NullRow` only reads, `Next`, `Prev` and a new position are legal.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u3_insert_after_null_row,
    "U3",
    1,
    [Op::NullRow(0), insert(0, 25, true)],
    model_gap: "an insert that seeks makes the calls of a seek opcode and an insert, and that seek is legal after NullRow"
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
    known_bug_u3_idx_insert_after_null_row,
    "U3",
    default_idef(),
    index_rows(),
    1,
    [IOp::NullRow(0), add(0, 7, 7, 70, false)],
    model_gap: "an insert that seeks makes the calls of a seek opcode and an insert, and that seek is legal after NullRow"
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
    [del(0, 9, 9, 99)],
    model_gap: "the VDBE returns Corrupt after the seek misses and calls no delete"
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
    }],
    model_gap: "the VDBE returns Corrupt after the seek misses and calls no delete"
);

// ---------------------------------------------------------------------------------------
// U8: a read (`Column`, `RowId`, `IdxRowId`, `Observe`) needs a position that is on an
// entry or on no entry. After `Count`, `SeekEnd`, `NewRowid`, a `Delete` (own or of a peer),
// `IdxDelete`, an exact seek that missed (also `Found` and `NotFound`) and `NoConflict` with
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
    [IOp::NoConflict(0, vec![Val::Null]), IOp::Observe(0)],
    model_gap: "NoConflict with a NULL makes no call on the cursor"
);

// ---------------------------------------------------------------------------------------
// Peer writes keep a cursor at its position by key (`peer_tests.rs`). When the peer deleted
// the entry under the cursor, the cursor is in the gap and a read or a `Delete` is refused.
// ---------------------------------------------------------------------------------------

table_case!(
    known_bug_u8_column_after_peer_delete,
    "U8",
    2,
    [
        Op::SeekRowid(0, 20),
        Op::SeekRowid(1, 20),
        Op::Delete(1),
        Op::Column(0, 1)
    ]
);
table_case!(
    known_bug_u4_delete_after_peer_delete,
    "U4",
    2,
    [
        Op::SeekRowid(0, 20),
        Op::SeekRowid(1, 20),
        Op::Delete(1),
        Op::Delete(0)
    ]
);
index_case!(
    known_bug_u8_idx_read_after_peer_delete,
    "U8",
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
