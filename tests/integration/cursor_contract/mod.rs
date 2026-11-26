//! Cursor contract tests: hand-emitted VDBE cursor programs run against a WAL database and
//! an MVCC database. See `SPEC.md`. This module only runs programs and returns
//! observations. A model or oracle can be added on top of `run`.

mod index_model;
mod index_program;
#[cfg(test)]
mod index_tests;
mod index_walk;
mod model;
#[cfg(test)]
mod overlay_tests;
mod program;
#[cfg(all(test, debug_assertions))]
mod undefined_tests;
mod walk;

use crate::common::TempDatabase;
use program::{compile, decode_record, Obs, Op, ReadMode, Target};
use std::panic::{catch_unwind, AssertUnwindSafe};
use turso_core::namespace::NamespaceId;
use turso_core::vdbe::StepResult;
use turso_core::{Connection, QueryMode, Statement, Value};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DbKind {
    Wal,
    Mvcc,
    /// An ephemeral b-tree (`OpenEphemeral`, `OpenAutoindex`): no MVCC and no schema. The
    /// program writes the setup rows itself. `Layout` has no meaning for it.
    Ephemeral,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Layout {
    /// Setup rows stay in the WAL (or in the MVCC row versions).
    AllInMemory,
    /// Setup rows are checkpointed, so the cursors read them from the B-tree.
    Checkpointed,
}

/// Runs `ops` on `ncursors` table cursors in one write transaction and returns one
/// observation per op. A panic or an engine error is returned as `Err`.
pub fn run(
    kind: DbKind,
    layout: Layout,
    setup_rows: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
) -> Result<Vec<Obs>, String> {
    run_mode(kind, layout, setup_rows, ncursors, ops, ReadMode::Column)
}

/// Like `run`, with the column reads emitted as `mode` says.
pub fn run_mode(
    kind: DbKind,
    layout: Layout,
    setup_rows: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
    mode: ReadMode,
) -> Result<Vec<Obs>, String> {
    catch_panic(|| run_inner(kind, layout, setup_rows, ncursors, ops, mode))
}

/// The engine state is dropped after a panic, so it is fine to look at the result.
fn catch_panic<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown panic".to_string());
            Err(format!("panic: {msg}"))
        }
    }
}

/// Creates the database of `kind` and puts the setup rows into it with SQL. `schema` is the
/// DDL, `fill` is the SQL of one setup row. An ephemeral run only creates the schema, because
/// the program writes the rows. After this the connection is in a write transaction.
fn open_database(
    kind: DbKind,
    layout: Layout,
    schema: &[&str],
    fill: &[String],
) -> Result<(TempDatabase, std::sync::Arc<Connection>), String> {
    let db = match kind {
        DbKind::Wal | DbKind::Ephemeral => TempDatabase::new_empty(),
        DbKind::Mvcc => TempDatabase::new_with_mvcc("cursor_contract.db"),
    };
    let conn = db.connect_limbo();
    let sql = |text: &str| conn.execute(text).map_err(|e| format!("{text}: {e}"));
    for text in schema {
        sql(text)?;
    }
    if kind != DbKind::Ephemeral {
        for text in fill {
            sql(text)?;
        }
    }
    if layout == Layout::Checkpointed && kind != DbKind::Ephemeral {
        // This pragma returns a row, so it is read with `query`.
        let mut stmt = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(|e| e.to_string())?
            .ok_or("no checkpoint statement")?;
        stmt.run_collect_rows().map_err(|e| e.to_string())?;
    }
    // Open the write transaction with SQL. In MVCC a statement that starts outside a
    // transaction re-parses its SQL text when the connection schema Arc is not the shared
    // one, and a hand-built program has no SQL text. Inside a transaction the schema is
    // fixed and the program runs as built.
    sql("BEGIN IMMEDIATE")?;
    Ok((db, conn))
}

/// Runs the program and returns the rows that it gave. When the engine fails in the middle,
/// the rows before the failure are kept and the error is returned next to them.
fn step_rows(
    program: turso_core::vdbe::Program,
    db: &TempDatabase,
    conn: &std::sync::Arc<Connection>,
) -> Result<(Vec<Vec<Value>>, Option<String>), String> {
    let mut stmt = Statement::new(
        program,
        conn.clone(),
        conn.get_pager(),
        QueryMode::Normal,
        0,
    )
    .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    loop {
        match stmt.step() {
            Ok(StepResult::Done) => return Ok((rows, None)),
            Ok(StepResult::Row) => {
                rows.push(stmt.row().unwrap().get_values().cloned().collect());
            }
            Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                db.io.step().map_err(|e| e.to_string())?
            }
            Ok(StepResult::Interrupt | StepResult::Busy) => {
                return Ok((rows, Some("busy or interrupted".to_string())))
            }
            Err(e) => return Ok((rows, Some(e.to_string()))),
        }
    }
}

fn run_inner(
    kind: DbKind,
    layout: Layout,
    setup_rows: &[(i64, i64)],
    ncursors: usize,
    ops: &[Op],
    mode: ReadMode,
) -> Result<Vec<Obs>, String> {
    let fill: Vec<String> = setup_rows
        .iter()
        .map(|(id, v)| format!("INSERT INTO t VALUES ({id}, {v})"))
        .collect();
    let (db, conn) = open_database(
        kind,
        layout,
        &["CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)"],
        &fill,
    )?;
    let root_page = table_root(&conn);
    let target = match kind {
        DbKind::Ephemeral => Target::Ephemeral(setup_rows),
        _ => Target::Persistent,
    };
    let program = compile(&conn, root_page, &target, ops, ncursors, mode)?;
    let (rows, error) = step_rows(program, &db, &conn)?;
    if let Some(error) = error {
        return Err(error);
    }
    rows.iter()
        .zip(ops)
        .map(|(row, op)| to_obs(row, *op))
        .collect()
}

fn table_root(conn: &std::sync::Arc<Connection>) -> i64 {
    conn.with_schema_mut(|s| s.get_btree_table(NamespaceId::ROOT, "t").unwrap().root_page)
        .unwrap()
}

fn to_obs(row: &[Value], op: Op) -> Result<Obs, String> {
    let int = |v: &Value| match v {
        Value::Null => Ok(None),
        Value::Numeric(_) => Ok(v.as_int()),
        other => Err(format!("unexpected value {other:?}")),
    };
    let mut obs = Obs {
        jumped: int(&row[0])? == Some(1),
        rowid: int(&row[1])?,
        col: int(&row[2])?,
    };
    // `RowData` mode: the column comes from the record that the op read.
    if let (Op::Column(_, col), Value::Blob(_)) = (op, &row[3]) {
        obs.col = decode_record(&row[3])?.get(col).copied().flatten();
    }
    Ok(obs)
}

/// Runs `body` and requires that it panics. Use it in a `known_bug_*` test: the body checks
/// the contract, and it fails today because of the bug `id`. When the bug is fixed, the body
/// no longer panics, this function fails the test, and the fix must remove the `known_bug`
/// wrapper so that the body becomes a normal regression test.
///
/// `expected` is a part of the panic message. It makes sure that the body fails for the
/// reason of the bug and not for another reason. An empty `expected` accepts any panic.
#[cfg(test)]
pub(crate) fn known_bug(id: &str, expected: &str, body: impl FnOnce()) {
    let Err(payload) = catch_unwind(AssertUnwindSafe(body)) else {
        panic!(
            "known bug {id} does not fail any more: remove the `known_bug` wrapper from this test"
        );
    };
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic>");
    assert!(
        message.contains(expected),
        "known bug {id} fails in another way than expected.\nexpected a panic with: {expected:?}\ngot: {message}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use Op::*;

    const ROWS: &[(i64, i64)] = &[(10, 100), (20, 200), (30, 300)];

    fn obs(jumped: bool, rowid: Option<i64>, col: Option<i64>) -> Obs {
        Obs { jumped, rowid, col }
    }

    /// Every engine and layout. Layout has no meaning for the ephemeral engine, so it runs once.
    const CONFIGS: [(DbKind, Layout); 5] = [
        (DbKind::Wal, Layout::AllInMemory),
        (DbKind::Wal, Layout::Checkpointed),
        (DbKind::Mvcc, Layout::AllInMemory),
        (DbKind::Mvcc, Layout::Checkpointed),
        (DbKind::Ephemeral, Layout::AllInMemory),
    ];

    /// Runs the program on every engine, layout and read mode and asserts that they all give
    /// `expected`.
    #[track_caller]
    fn check_all(rows: &[(i64, i64)], ncursors: usize, ops: &[Op], expected: &[Obs]) {
        check_modes(rows, ncursors, ops, expected, &ReadMode::ALL)
    }

    /// Like `check_all`, for some of the read modes only.
    #[track_caller]
    fn check_modes(
        rows: &[(i64, i64)],
        ncursors: usize,
        ops: &[Op],
        expected: &[Obs],
        modes: &[ReadMode],
    ) {
        for (kind, layout) in CONFIGS {
            for mode in modes {
                let mode = *mode;
                let got = run_mode(kind, layout, rows, ncursors, ops, mode);
                assert_eq!(
                    got.as_deref(),
                    Ok(expected),
                    "{kind:?} / {layout:?} / {mode:?} differs for {ops:?}"
                );
            }
            // The model must agree with the engine on every hand-written scenario.
            model::assert_agrees_prefix(kind, layout, rows, ncursors, ops);
        }
    }

    /// Runs the program on every engine and layout and returns the results, to compare them.
    fn run_all(
        rows: &[(i64, i64)],
        ncursors: usize,
        ops: &[Op],
    ) -> Vec<(DbKind, Layout, Result<Vec<Obs>, String>)> {
        let mut out = Vec::new();
        for (kind, layout) in CONFIGS {
            out.push((kind, layout, run(kind, layout, rows, ncursors, ops)));
            // Scenarios that run through `run_all` are checked against the model too.
            model::assert_agrees_prefix(kind, layout, rows, ncursors, ops);
        }
        out
    }

    #[test]
    fn rewind_next_scan() {
        let ops = [
            Rewind(0),
            RowId(0),
            Column(0, 1),
            Next(0),
            RowId(0),
            Next(0),
            Column(0, 1),
            Next(0),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, Some(10), None),
            obs(false, None, Some(100)),
            obs(true, None, None),
            obs(false, Some(20), None),
            obs(true, None, None),
            obs(false, None, Some(300)),
            // The scan ended: no jump, and the cursor reads NULL.
            obs(false, None, None),
            obs(false, None, None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn rewind_on_empty_table_jumps() {
        let ops = [Rewind(0), RowId(0), Last(0), Column(0, 1)];
        let expected = [
            obs(true, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, None, None),
        ];
        check_all(&[], 1, &ops, &expected);
    }

    #[test]
    fn last_prev_scan() {
        let ops = [
            Last(0),
            RowId(0),
            Prev(0),
            RowId(0),
            Prev(0),
            Prev(0),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, Some(30), None),
            obs(true, None, None),
            obs(false, Some(20), None),
            obs(true, None, None),
            obs(false, None, None),
            obs(false, None, None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn seek_rowid_hit_and_miss() {
        let ops = [
            SeekRowid(0, 20),
            RowId(0),
            Column(0, 1),
            // A miss jumps.
            SeekRowid(0, 25),
            NotExists(0, 30),
            RowId(0),
            NotExists(0, 31),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, Some(20), None),
            obs(false, None, Some(200)),
            obs(true, None, None),
            obs(false, None, None),
            obs(false, Some(30), None),
            obs(true, None, None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn range_seeks_hit() {
        let ops = [
            SeekGE(0, 20),
            RowId(0),
            SeekGT(0, 20),
            RowId(0),
            SeekLE(0, 20),
            RowId(0),
            SeekLT(0, 20),
            RowId(0),
            // Keys between rows.
            SeekGE(0, 11),
            RowId(0),
            SeekLE(0, 29),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, Some(20), None),
            obs(false, None, None),
            obs(false, Some(30), None),
            obs(false, None, None),
            obs(false, Some(20), None),
            obs(false, None, None),
            obs(false, Some(10), None),
            obs(false, None, None),
            obs(false, Some(20), None),
            obs(false, None, None),
            obs(false, Some(20), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn range_seeks_miss_jump() {
        // Only the `jumped` flag is checked: what the cursor reads after a failed
        // range seek is gap G8 in SPEC.md, see `failed_range_seek_reads_null`.
        let ops = [SeekGE(0, 31), SeekGT(0, 30), SeekLE(0, 9), SeekLT(0, 10)];
        for (kind, layout, got) in run_all(ROWS, 1, &ops) {
            let got = got.unwrap_or_else(|e| panic!("{kind:?}/{layout:?}: {e}"));
            assert!(got.iter().all(|o| o.jumped), "{kind:?}/{layout:?}: {got:?}");
        }
    }

    #[test]
    fn seek_then_scan_forward_and_back() {
        let ops = [
            SeekGE(0, 15),
            Next(0),
            RowId(0),
            SeekLE(0, 25),
            Prev(0),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(true, None, None),
            obs(false, Some(30), None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, Some(10), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn insert_then_scan() {
        let ops = [
            Insert {
                cur: 0,
                rowid: 15,
                value: 150,
                require_seek: true,
            },
            RowId(0),
            Column(0, 1),
            Rewind(0),
            Next(0),
            RowId(0),
            Next(0),
            RowId(0),
            Column(0, 1),
            Next(0),
            Next(0),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, Some(15), None),
            obs(false, None, Some(150)),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, Some(15), None),
            obs(true, None, None),
            obs(false, Some(20), None),
            obs(false, None, Some(200)),
            obs(true, None, None),
            obs(false, None, None),
            obs(false, None, None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn insert_overwrites_existing_row() {
        let ops = [
            Insert {
                cur: 0,
                rowid: 20,
                value: 222,
                require_seek: true,
            },
            SeekRowid(0, 20),
            Column(0, 1),
            Count(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, None, None),
            obs(false, None, Some(222)),
            obs(false, Some(3), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn insert_after_not_exists_without_seek() {
        // `NotExists` leaves the cursor at the place of the key, so no seek is needed (SPEC 3).
        let ops = [
            NotExists(0, 25),
            Insert {
                cur: 0,
                rowid: 25,
                value: 250,
                require_seek: false,
            },
            Rewind(0),
            Next(0),
            Next(0),
            RowId(0),
            Column(0, 1),
            Count(0),
        ];
        let expected = [
            obs(true, None, None),
            obs(false, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(true, None, None),
            obs(false, Some(25), None),
            obs(false, None, Some(250)),
            obs(false, Some(4), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn delete_then_next() {
        let ops = [
            SeekRowid(0, 20),
            Delete(0),
            Next(0),
            RowId(0),
            Column(0, 1),
            Count(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, Some(30), None),
            obs(false, None, Some(300)),
            obs(false, Some(2), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn delete_then_prev() {
        let ops = [SeekRowid(0, 20), Delete(0), Prev(0), RowId(0)];
        let expected = [
            obs(false, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, Some(10), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn delete_every_row_with_scan() {
        let ops = [
            Rewind(0),
            Delete(0),
            Next(0),
            Delete(0),
            Next(0),
            Delete(0),
            Next(0),
            Count(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, None, None),
            obs(false, None, None),
            obs(false, Some(0), None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    #[test]
    fn null_row_reads_null() {
        let ops = [
            Rewind(0),
            NullRow(0),
            RowId(0),
            Column(0, 1),
            Rewind(0),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, None, None),
            obs(false, None, None),
            obs(false, None, None),
            obs(false, None, None),
            obs(false, Some(10), None),
        ];
        // `RowData` on a null-row cursor: see `row_data_on_null_row_reads_null`.
        check_modes(
            ROWS,
            1,
            &ops,
            &expected,
            &[
                ReadMode::Column,
                ReadMode::ColumnRange,
                ReadMode::ColumnHasField,
            ],
        );
    }

    #[test]
    fn peer_cursor_insert_is_seen_by_next() {
        // SPEC 1: there is no snapshot inside one transaction.
        let ops = [
            Rewind(0),
            Insert {
                cur: 1,
                rowid: 15,
                value: 150,
                require_seek: true,
            },
            Next(0),
            RowId(0),
        ];
        let expected = [
            obs(false, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, Some(15), None),
        ];
        check_all(ROWS, 2, &ops, &expected);
    }

    #[test]
    fn new_rowid_is_larger_than_max() {
        // D6: only the predicate holds, MVCC may skip rowids.
        let ops = [
            NewRowid(0),
            InsertNew {
                cur: 0,
                value: 1,
                require_seek: false,
            },
            Last(0),
            RowId(0),
        ];
        for (kind, layout, got) in run_all(ROWS, 1, &ops) {
            let got = got.unwrap_or_else(|e| panic!("{kind:?}/{layout:?}: {e}"));
            let new = got[0].rowid.expect("NewRowid gives a rowid");
            assert!(new > 30, "{kind:?}/{layout:?}: new rowid {new}");
            assert_eq!(got[3].rowid, Some(new), "{kind:?}/{layout:?}");
        }
    }

    /// SPEC G8: after a failed range seek the contract says `Invalid`, so `RowId` is NULL.
    /// Both engines agree with the contract here.
    #[test]
    fn failed_range_seek_reads_null() {
        let ops = [SeekGE(0, 31), RowId(0), SeekLE(0, 9), RowId(0)];
        let expected = [
            obs(true, None, None),
            obs(false, None, None),
            obs(true, None, None),
            obs(false, None, None),
        ];
        check_all(ROWS, 1, &ops, &expected);
    }

    // Everything below is a known gap between an engine and the contract (or between
    // WAL and MVCC). The `known_bug_*` tests assert that the bug is still there: each body
    // checks what the contract says, and `known_bug` requires that the body fails. When a
    // fix lands, the test fails and the fix must remove the wrapper.

    /// SPEC 1: a null-row cursor reads NULL. `RowData` returns the real row in WAL and in
    /// ephemeral, and fails with "cursor has no record" in MVCC. The index variant is
    /// `rowdata_on_null_row_reads_null` in `index_tests.rs`.
    ///
    /// Fails today because of R1; see SPEC section 5b.
    #[test]
    fn known_bug_r1_row_data_on_null_row_reads_null() {
        known_bug("R1", "RowDataRaw differs", || {
            let ops = [Rewind(0), NullRow(0), Column(0, 1)];
            let expected = [
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
            ];
            check_modes(ROWS, 1, &ops, &expected, &[ReadMode::RowDataRaw]);
        });
    }

    /// D1: after `Insert` the cursor is `At k`, so `Next` goes to the following row.
    /// MVCC with the rows in the B-tree: `Next` does not jump and the cursor reads
    /// nothing, as if the scan had ended. WAL and MVCC with rows in memory agree.
    ///
    /// Fails today because of D1; see SPEC section 5b.
    #[test]
    fn known_bug_d1_insert_then_next_from_new_row() {
        known_bug("D1", "Checkpointed / Column differs", || {
            let ops = [
                Insert {
                    cur: 0,
                    rowid: 25,
                    value: 1,
                    require_seek: true,
                },
                Next(0),
                RowId(0),
            ];
            let expected = [
                obs(false, None, None),
                obs(true, None, None),
                obs(false, Some(30), None),
            ];
            check_all(ROWS, 1, &ops, &expected);
        });
    }

    /// Same as above, with `Prev`.
    ///
    /// Fails today because of D1; see SPEC section 5b.
    #[test]
    fn known_bug_d1_insert_then_prev_from_new_row() {
        known_bug("D1", "Checkpointed / Column differs", || {
            let ops = [
                Insert {
                    cur: 0,
                    rowid: 25,
                    value: 1,
                    require_seek: true,
                },
                Prev(0),
                RowId(0),
            ];
            let expected = [
                obs(false, None, None),
                obs(true, None, None),
                obs(false, Some(20), None),
            ];
            check_all(ROWS, 1, &ops, &expected);
        });
    }

    /// Same as above, when the cursor was moved by a seek before the `Insert`.
    ///
    /// Fails today because of D1; see SPEC section 5b.
    #[test]
    fn known_bug_d1_seek_insert_then_next() {
        known_bug("D1", "Checkpointed / Column differs", || {
            let ops = [
                SeekRowid(0, 20),
                Insert {
                    cur: 0,
                    rowid: 25,
                    value: 1,
                    require_seek: true,
                },
                Next(0),
                RowId(0),
            ];
            let expected = [
                obs(false, None, None),
                obs(false, None, None),
                obs(true, None, None),
                obs(false, Some(30), None),
            ];
            check_all(ROWS, 1, &ops, &expected);
        });
    }

    /// G4: `Insert` that replaces a row. `Column` on the same cursor must read the new
    /// record. MVCC with the row in the B-tree reads the old record (200).
    ///
    /// Fails today because of G4; see SPEC section 5b.
    #[test]
    fn known_bug_g4_column_after_update_by_insert() {
        known_bug("G4", "Checkpointed / Column differs", || {
            let ops = [
                Insert {
                    cur: 0,
                    rowid: 20,
                    value: 222,
                    require_seek: true,
                },
                Column(0, 1),
            ];
            let expected = [obs(false, None, None), obs(false, None, Some(222))];
            check_all(ROWS, 1, &ops, &expected);
        });
    }

    /// G4, with a peer cursor that updates the row.
    ///
    /// Fails today because of G4; see SPEC section 5b.
    #[test]
    fn known_bug_g4_column_after_update_by_peer() {
        known_bug("G4", "Checkpointed / Column differs", || {
            let ops = [
                SeekRowid(0, 20),
                Insert {
                    cur: 1,
                    rowid: 20,
                    value: 999,
                    require_seek: true,
                },
                Column(0, 1),
            ];
            let expected = [
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, Some(999)),
            ];
            check_all(ROWS, 2, &ops, &expected);
        });
    }

    /// G2 / D5: a peer deletes the row of our cursor, so we read NULL. WAL reads the next
    /// row (30), MVCC reads the deleted row (20).
    ///
    /// Fails today because of G2; see SPEC section 5b.
    #[test]
    fn known_bug_g2_peer_delete_then_read() {
        known_bug("G2", "Wal / AllInMemory / Column differs", || {
            let ops = [
                SeekRowid(0, 20),
                SeekRowid(1, 20),
                Delete(1),
                RowId(0),
                Column(0, 1),
            ];
            let expected = [
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
            ];
            check_all(ROWS, 2, &ops, &expected);
        });
    }

    /// G2: after the peer deleted our row, `Delete` on our cursor must not delete another
    /// row. WAL deletes the next row (count 1). MVCC does nothing (count 2).
    ///
    /// Fails today because of G2; see SPEC section 5b.
    #[test]
    fn known_bug_g2_peer_delete_then_delete() {
        known_bug("G2", "Wal / AllInMemory / Column differs", || {
            let ops = [
                SeekRowid(0, 20),
                SeekRowid(1, 20),
                Delete(1),
                Delete(0),
                Count(0),
            ];
            // Rows: 10, 20, 30. The peer removes 20 and our `Delete` must remove nothing.
            let rows = &ROWS[..3];
            let expected = [
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
                obs(false, Some(2), None),
            ];
            check_all(rows, 2, &ops, &expected);
        });
    }

    /// G3: a write by a peer must not clear the null-row flag. WAL clears it and reads the
    /// real row. MVCC keeps the flag.
    ///
    /// Fails today because of G3; see SPEC section 5b.
    #[test]
    fn known_bug_g3_peer_write_keeps_null_row() {
        known_bug("G3", "Wal / AllInMemory / Column differs", || {
            let ops = [
                Rewind(0),
                NullRow(0),
                Insert {
                    cur: 1,
                    rowid: 5,
                    value: 5,
                    require_seek: true,
                },
                RowId(0),
                Column(0, 1),
            ];
            let expected = [
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
                obs(false, None, None),
            ];
            check_all(ROWS, 2, &ops, &expected);
        });
    }

    // Gaps found by the random walk (`walk.rs`). Each test is the minimal program that the
    // shrinker printed, checked against the model on all four configs. The proposed gap ids
    // N1 to N6 are the names of the generator filters. They are not in SPEC.md yet.

    /// Runs the program on all four configs and asserts that the engine agrees with the model.
    #[track_caller]
    fn agree_all(rows: &[(i64, i64)], ncursors: usize, ops: &[Op]) {
        for kind in [DbKind::Wal, DbKind::Mvcc] {
            for layout in [Layout::AllInMemory, Layout::Checkpointed] {
                model::assert_agrees(kind, layout, rows, ncursors, ops);
            }
        }
    }

    /// N1: `Next` after a `SeekRowid` or `NotExists` that hit. SPEC section 2 allows it.
    /// MVCC does not jump and reads nothing, as if the scan had ended.
    ///
    /// Fails today because of N1; see SPEC section 5b.
    #[test]
    fn known_bug_n1_next_after_seek_rowid_hit() {
        known_bug("N1", "Mismatch", || {
            agree_all(ROWS, 1, &[SeekRowid(0, 20), Next(0), RowId(0)]);
            agree_all(ROWS, 1, &[NotExists(0, 20), Next(0), RowId(0)]);
        });
    }

    /// N2: reading a cursor after `Count`. The position is open, but WAL panics with an
    /// arithmetic overflow in `cell_table_leaf_read_header`.
    ///
    /// Fails today because of N2; see SPEC section 5b.
    #[test]
    fn known_bug_n2_read_after_count_does_not_panic() {
        known_bug("N2", "attempt to multiply with overflow", || {
            for ops in [
                vec![Rewind(0), Count(0), RowId(0)],
                vec![Last(0), Count(0), Column(0, 1)],
            ] {
                for (kind, layout, got) in run_all(ROWS, 1, &ops) {
                    assert!(got.is_ok(), "{kind:?}/{layout:?} {ops:?}: {got:?}");
                }
            }
        });
    }

    /// N4: `Delete` of a row that an `Insert` wrote in this transaction (a new row or an
    /// update). MVCC with checkpointed rows loses the position: `Next` and `Prev` after the
    /// `Delete` find nothing.
    ///
    /// Fails today because of N4; see SPEC section 5b.
    #[test]
    fn known_bug_n4_delete_of_inserted_row_then_move() {
        known_bug("N4", "Mismatch", || {
            let insert = |rowid| Insert {
                cur: 0,
                rowid,
                value: 1,
                require_seek: true,
            };
            agree_all(
                ROWS,
                1,
                &[insert(25), SeekRowid(0, 25), Delete(0), Next(0), RowId(0)],
            );
            agree_all(
                ROWS,
                1,
                &[insert(25), SeekRowid(0, 25), Delete(0), Prev(0), RowId(0)],
            );
            // An update of a row that is in the B-tree.
            agree_all(
                ROWS,
                1,
                &[insert(20), SeekRowid(0, 20), Delete(0), Next(0), RowId(0)],
            );
        });
    }

    /// N4, with a row that a peer cursor wrote: `Delete` itself panics ("no current row").
    ///
    /// Fails today because of N4; see SPEC section 5b.
    #[test]
    fn known_bug_n4_delete_of_peer_inserted_row() {
        known_bug("N4", "Cannot delete: no current row", || {
            let ops = [
                NewRowid(1),
                NewRowid(0),
                InsertNew {
                    cur: 0,
                    value: 1,
                    require_seek: true,
                },
                SeekRowid(1, 31),
                Delete(1),
            ];
            model::assert_agrees(DbKind::Mvcc, Layout::Checkpointed, ROWS, 2, &ops);
        });
    }

    /// N5: a cursor deletes the last row, a peer inserts a row, then `Prev` on the first
    /// cursor. WAL panics on `self.current_page >= 0`. `Next` is fine.
    ///
    /// Fails today because of N5; see SPEC section 5b.
    #[test]
    fn known_bug_n5_prev_after_delete_emptied_table_and_peer_insert() {
        known_bug("N5", "self.current_page >= 0", || {
            let rows = [(8, 53)];
            let ops = [
                Last(0),
                Delete(0),
                Insert {
                    cur: 1,
                    rowid: 16,
                    value: 9,
                    require_seek: true,
                },
                Prev(0),
            ];
            agree_all(&rows, 2, &ops);
        });
    }

    /// N6: the cursor deletes its row, a peer inserts a row, then `Next` or `Prev`. WAL lands
    /// on the wrong row. Per the contract the cursor is in the gap of the deleted key.
    ///
    /// Fails today because of N6; see SPEC section 5b.
    #[test]
    fn known_bug_n6_move_after_delete_and_peer_insert() {
        known_bug("N6", "Mismatch", || {
            let insert = |rowid| Insert {
                cur: 1,
                rowid,
                value: 1,
                require_seek: true,
            };
            // The deleted row was the last one: `Next` must end. A row that the peer inserted
            // before the gap must not be found.
            agree_all(ROWS, 2, &[SeekRowid(0, 30), Delete(0), insert(25), Next(0)]);
            // The peer inserts the deleted key again: `Next` goes past it.
            agree_all(ROWS, 2, &[SeekRowid(0, 20), Delete(0), insert(20), Next(0)]);
            // The deleted row was the first one: `Prev` must end.
            agree_all(ROWS, 2, &[SeekRowid(0, 10), Delete(0), insert(5), Prev(0)]);
        });
    }
}
