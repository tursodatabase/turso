//! Sequences that the contract defines where SQLite says nothing.
//!
//! Peer writes (SPEC section 1): a write by one cursor leaves every other cursor of the tree
//! at its position by key, as SQLite's saved cursors do. If the write deleted the entry under
//! the peer, the peer is in the gap: `Next` and `Prev` go on, a read is Undefined (U8).
//!
//! `Found`, `NotFound` and `NoConflict` (SPEC 2b) are eq-only seeks: a hit is on the first
//! match and `Next` goes on from it; after a miss `Next` goes to the first entry after the key.
//!
//! Each test runs one program on every engine and requires that the engine agrees with the
//! model. A test named `known_bug_*` asserts that an engine still disagrees (SPEC 5b).

use super::index_model::{self, ColDef, Coll, IdxDef, Order, Val};
use super::index_program::{IOp, Row};
use super::model;
use super::program::{Op, ReadMode};
use super::{known_bug, DbKind, Layout};
use std::panic::{catch_unwind, AssertUnwindSafe};

const TABLE_CONFIGS: [(DbKind, Layout); 5] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
    (DbKind::Ephemeral, Layout::AllInMemory),
];

/// The ephemeral engine has no index `OpenDup` (O1), so index programs with two cursors do
/// not run on it.
const INDEX_CONFIGS: [(DbKind, Layout); 4] = [
    (DbKind::Wal, Layout::AllInMemory),
    (DbKind::Wal, Layout::Checkpointed),
    (DbKind::Mvcc, Layout::AllInMemory),
    (DbKind::Mvcc, Layout::Checkpointed),
];

const TABLE_ROWS: &[(i64, i64)] = &[(10, 100), (20, 200), (30, 300)];

/// Runs `check` for every config and panics with the list of the configs that disagree.
/// A program that the model calls illegal is a mistake in the test, not a bug of an engine.
fn on_every_config(configs: &[(DbKind, Layout)], check: impl Fn(DbKind, Layout)) {
    let mut failed = Vec::new();
    for (kind, layout) in configs {
        let Err(payload) = catch_unwind(AssertUnwindSafe(|| check(*kind, *layout))) else {
            continue;
        };
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            !message.contains("the model says op"),
            "the model refuses the program: {message}"
        );
        failed.push(format!("{kind:?}/{layout:?}"));
    }
    assert!(
        failed.is_empty(),
        "disagree with the model: {}",
        failed.join(" ")
    );
}

#[track_caller]
fn agrees_table(ncur: usize, ops: &[Op]) {
    agrees_table_rows(TABLE_ROWS, ncur, ops);
}

#[track_caller]
fn agrees_table_rows(rows: &[(i64, i64)], ncur: usize, ops: &[Op]) {
    on_every_config(&TABLE_CONFIGS, |kind, layout| {
        model::assert_agrees(kind, layout, rows, ncur, ops)
    });
}

#[track_caller]
fn agrees_index(ncur: usize, ops: &[IOp]) {
    agrees_index_rows(&index_rows(), ncur, ops);
}

#[track_caller]
fn agrees_index_rows(rows: &[Row], ncur: usize, ops: &[IOp]) {
    on_every_config(&INDEX_CONFIGS, |kind, layout| {
        index_model::assert_agrees(kind, layout, ReadMode::Column, rows, ncur, ops)
    });
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

fn add(cur: usize, a: i64, b: i64, rowid: i64) -> IOp {
    IOp::IdxInsert {
        cur,
        a: v(a),
        b: v(b),
        rowid,
        use_seek: false,
    }
}

fn insert(cur: usize, rowid: i64) -> Op {
    Op::Insert {
        cur,
        rowid,
        value: 1,
        require_seek: true,
    }
}

// ---------------------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------------------

#[test]
fn next_after_peer_insert() {
    agrees_table(2, &[Op::SeekRowid(0, 20), insert(1, 25), Op::Next(0)]);
}

#[test]
fn prev_after_peer_insert() {
    agrees_table(2, &[Op::Last(0), insert(1, 5), Op::Prev(0)]);
}

#[test]
fn read_after_peer_insert() {
    agrees_table(2, &[Op::SeekRowid(0, 20), insert(1, 25), Op::RowId(0)]);
}

/// G4: MVCC with the row in the B-tree reads the old record.
#[test]
fn known_bug_g4_column_after_peer_overwrite() {
    known_bug("G4", "disagree with the model: Mvcc/Checkpointed", || {
        let overwrite = Op::Insert {
            cur: 1,
            rowid: 20,
            value: 999,
            require_seek: true,
        };
        agrees_table(2, &[Op::SeekRowid(0, 20), overwrite, Op::Column(0, 1)]);
    });
}

#[test]
fn next_after_peer_delete() {
    let ops = [
        Op::SeekRowid(0, 20),
        Op::SeekRowid(1, 20),
        Op::Delete(1),
        Op::Next(0),
    ];
    agrees_table(2, &ops);
}

#[test]
fn next_after_own_delete_and_peer_insert() {
    let ops = [
        Op::SeekRowid(0, 20),
        Op::Delete(0),
        insert(1, 25),
        Op::Next(0),
    ];
    agrees_table(2, &ops);
}

#[test]
fn next_of_ended_cursor_after_peer_insert() {
    let ops = [
        Op::Rewind(0),
        Op::Next(0),
        Op::Next(0),
        Op::Next(0),
        insert(1, 40),
        Op::Next(0),
    ];
    agrees_table(2, &ops);
}

/// G3: WAL and ephemeral lose the null-row flag at the peer write and read the row.
#[test]
fn known_bug_g3_read_of_null_row_after_peer_insert() {
    known_bug(
        "G3",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed Ephemeral/AllInMemory",
        || {
            let ops = [Op::Rewind(0), Op::NullRow(0), insert(1, 5), Op::RowId(0)];
            agrees_table(2, &ops);
        },
    );
}

/// G3: the ephemeral engine ignores `NullRow` after the peer write and reads a row.
#[test]
fn known_bug_g3_null_row_after_peer_insert() {
    known_bug(
        "G3",
        "disagree with the model: Ephemeral/AllInMemory",
        || {
            agrees_table(2, &[insert(1, 25), Op::NullRow(0), Op::Column(0, 1)]);
        },
    );
}

// ---------------------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------------------

#[test]
fn idx_next_after_peer_insert() {
    agrees_index(
        2,
        &[IOp::SeekGE(0, vec![v(2)]), add(1, 7, 7, 70), IOp::Next(0)],
    );
}

#[test]
fn idx_read_after_peer_insert() {
    agrees_index(
        2,
        &[
            IOp::SeekGE(0, vec![v(2)]),
            add(1, 7, 7, 70),
            IOp::Observe(0),
        ],
    );
}

/// PW2: WAL is not on its new entry after the peer write: it reads `(1, 10, 1)`.
#[test]
fn known_bug_pw2_idx_read_after_own_insert_and_peer_insert() {
    known_bug(
        "PW2",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed",
        || {
            agrees_index(2, &[add(0, 0, 0, 70), add(1, 8, 8, 80), IOp::Observe(0)]);
        },
    );
}

#[test]
fn idx_next_after_peer_delete() {
    let ops = [
        IOp::SeekGE(0, vec![v(1)]),
        IOp::SeekGE(1, vec![v(1)]),
        IOp::IdxDelete {
            cur: 1,
            a: v(1),
            b: v(10),
            rowid: 1,
        },
        IOp::Next(0),
    ];
    agrees_index(2, &ops);
}

#[test]
fn idx_delete_after_peer_insert() {
    agrees_index(
        2,
        &[IOp::SeekGE(0, vec![v(1)]), add(1, 7, 7, 70), IOp::Delete(0)],
    );
}

#[test]
fn idx_compare_after_peer_insert() {
    let ops = [
        IOp::SeekGE(0, vec![v(1)]),
        add(1, 7, 7, 70),
        IOp::IdxGT(0, vec![v(1)]),
    ];
    agrees_index(2, &ops);
}

#[test]
fn idx_next_of_ended_cursor_after_peer_insert() {
    agrees_index(
        2,
        &[IOp::SeekGE(0, vec![v(9)]), add(1, 7, 7, 70), IOp::Next(0)],
    );
}

#[test]
fn idx_next_after_own_delete_and_peer_insert() {
    let ops = [
        IOp::SeekGE(0, vec![v(1)]),
        IOp::Delete(0),
        add(1, 7, 7, 70),
        IOp::Next(0),
    ];
    agrees_index(2, &ops);
}

/// G3: WAL loses the null-row flag at the peer write and reads the entry.
#[test]
fn known_bug_g3_idx_read_of_null_row_after_peer_insert() {
    known_bug(
        "G3",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed",
        || {
            let ops = [
                IOp::Rewind(0),
                IOp::NullRow(0),
                add(1, 7, 7, 70),
                IOp::Observe(0),
            ];
            agrees_index(2, &ops);
        },
    );
}

// ---------------------------------------------------------------------------------------
// Found-style seeks
// ---------------------------------------------------------------------------------------

/// OV1: MVCC with the entries in memory ends the scan after the first `Next`.
#[test]
fn known_bug_ov1_idx_next_after_found_hit() {
    known_bug("OV1", "disagree with the model: Mvcc/AllInMemory", || {
        agrees_index(1, &[IOp::Found(0, vec![v(1)]), IOp::Next(0), IOp::Next(0)]);
    });
}

#[test]
fn idx_read_after_found_hit() {
    agrees_index(1, &[IOp::NotFound(0, vec![v(2)]), IOp::Observe(0)]);
}

/// FM1: MVCC ends the scan.
#[test]
fn known_bug_fm1_idx_next_after_found_miss() {
    known_bug(
        "FM1",
        "disagree with the model: Mvcc/AllInMemory Mvcc/Checkpointed",
        || {
            agrees_index(1, &[IOp::NotFound(0, vec![v(1), v(15)]), IOp::Next(0)]);
        },
    );
}

/// FM1: WAL skips `(2, 30, 4)` and reads `(3, NULL, 5)`; MVCC ends the scan.
#[test]
fn known_bug_fm1_idx_next_after_no_conflict_miss() {
    known_bug(
        "FM1",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed Mvcc/AllInMemory Mvcc/Checkpointed",
        || {
            let ops = [
                IOp::NoConflict(0, vec![v(2), v(20)]),
                IOp::Next(0),
                IOp::Observe(0),
            ];
            agrees_index(1, &ops);
        },
    );
}

/// FM1: with two entries `(3, 2)`, a miss of `(3, 1)` and `Next`: WAL and ephemeral skip the
/// first entry (rowid 10) and land on rowid 13; MVCC ends the scan.
#[test]
fn known_bug_fm1_next_after_found_miss_skips_an_entry() {
    known_bug(
        "FM1",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed Mvcc/AllInMemory Mvcc/Checkpointed",
        || {
            let rows = [(10, v(3), v(2)), (13, v(3), v(2))];
            let ops = [IOp::Found(0, vec![v(3), v(1)]), IOp::Next(0), IOp::Observe(0)];
            agrees_index_rows(&rows, 1, &ops);
        },
    );
}

/// PD1: WAL and ephemeral: `Prev` from the gap of an own `Delete`, after a peer insert, goes
/// wrong. With rows 8 and 20: both rows are deleted, the peer inserts 16, and `Prev` panics
/// (`self.current_page >= 0`); the model ends the scan.
#[test]
fn known_bug_pd1_prev_from_gap_after_peer_insert() {
    known_bug(
        "PD1",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed Ephemeral/AllInMemory",
        || {
            let ops = [
                Op::SeekGT(0, 19),
                Op::Delete(0),
                Op::Last(1),
                Op::Delete(1),
                insert(0, 16),
                Op::Prev(1),
            ];
            agrees_table_rows(&[(8, 53), (20, 250)], 2, &ops);
        },
    );
}

/// PD1: with rows 10, 20, 30: cursor 1 deletes 20, the peer inserts 16, and `Prev` of cursor 1
/// gives 10, not 16. MVCC gives 16.
#[test]
fn known_bug_pd1_prev_from_gap_skips_peer_insert() {
    known_bug(
        "PD1",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed Ephemeral/AllInMemory",
        || {
            let ops = [
                Op::SeekGT(0, 25),
                Op::Delete(0),
                Op::Last(1),
                Op::Delete(1),
                insert(0, 16),
                Op::Prev(1),
                Op::RowId(1),
            ];
            agrees_table(2, &ops);
        },
    );
}

/// PD2: WAL, index `(a NOCASE, b)` with entries `(NULL, '[')`, `('B', NULL)`, `('_', 'É')`:
/// cursor 1 on `('B', NULL)`, a peer `IdxDelete` of `('_', 'É')`, then `Delete` of cursor 1
/// panics (`idx < cell_count`).
#[test]
fn known_bug_pd2_idx_delete_after_peer_idx_delete() {
    known_bug(
        "PD2",
        "disagree with the model: Wal/AllInMemory Wal/Checkpointed",
        || {
            let text = |x: &str| Val::text(x);
            let rows = [
                (3, text("_"), text("É")),
                (5, Val::Null, text("[")),
                (6, text("B"), Val::Null),
            ];
            let idef = IdxDef::new(
                ColDef::new(Order::Asc, Coll::NoCase),
                ColDef::new(Order::Asc, Coll::Binary),
            );
            let ops = [
                IOp::Found(1, vec![text("B")]),
                IOp::IdxDelete {
                    cur: 0,
                    a: text("_"),
                    b: text("É"),
                    rowid: 3,
                },
                IOp::Delete(1),
            ];
            on_every_config(&INDEX_CONFIGS, |kind, layout| {
                index_model::assert_agrees_def(kind, layout, ReadMode::Column, idef, &rows, 2, &ops)
            });
        },
    );
}
