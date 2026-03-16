//! Differential tests for INSERT conflict resolution.
//!
//! Tests a matrix of (DDL schema) × (conflict resolution type) × (row count)
//! against SQLite (via rusqlite) to verify identical behavior and index integrity.
//!
//! Row counts are chosen to exercise different B-tree code paths:
//! - 10:   single page, delete triggers underflow → balancing re-seeks cursor
//! - 134:  single page, delete does NOT trigger underflow → retreat path
//! - 500:  multi-page tree, exercises interior node replacement paths
//! - 1000: larger multi-page tree

use std::sync::Arc;

use crate::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
use rusqlite::params;

// ---------------------------------------------------------------------------
// Schema definitions
// ---------------------------------------------------------------------------

struct Schema {
    name: &'static str,
    /// DDL to create the table.
    ddl: &'static str,
    bulk_insert_sqlite: fn(n: u32, conn: &rusqlite::Connection),
    bulk_insert_limbo: fn(n: u32, conn: &Arc<turso_core::Connection>),
    /// Returns a conflict-inducing SQL for a given conflict clause.
    /// `n` is the number of bulk-inserted rows (1..=n), so conflict targets are stable.
    conflict_sql: fn(clause: &str, n: u32) -> String,
    /// Returns a non-conflicting SQL for a given conflict clause.
    no_conflict_sql: fn(clause: &str, seq: u32) -> String,
    /// SQL to dump the full table contents for comparison.
    verify_sql: &'static str,
}

// --- pk_only: INTEGER PRIMARY KEY, no secondary indexes ---

fn sqlite_bulk_insert_pk_only(n: u32, conn: &rusqlite::Connection) {
    conn.execute_batch("BEGIN;").unwrap();
    for i in 1..=n {
        conn.execute("INSERT INTO t VALUES (?, 'val' || ?)", params![i, i])
            .unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();
}

fn limbo_bulk_insert_pk_only(n: u32, conn: &Arc<turso_core::Connection>) {
    conn.execute(format!(
        "INSERT INTO t SELECT value, 'val' || value FROM generate_series(1, {n})"
    ))
    .unwrap();
}

// --- pk_unique: INTEGER PRIMARY KEY + single UNIQUE column ---

fn sqlite_bulk_insert_pk_unique(n: u32, conn: &rusqlite::Connection) {
    conn.execute_batch("BEGIN;").unwrap();
    for i in 1..=n {
        conn.execute("INSERT INTO t VALUES (?, 'a' || ?)", params![i, i])
            .unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();
}

fn limbo_bulk_insert_pk_unique(n: u32, conn: &Arc<turso_core::Connection>) {
    conn.execute(format!(
        "INSERT INTO t SELECT value, 'a' || value FROM generate_series(1, {n})"
    ))
    .unwrap();
}

// --- composite_unique: INTEGER PRIMARY KEY + UNIQUE(a, b) on non-PK columns ---

fn sqlite_bulk_insert_composite(n: u32, conn: &rusqlite::Connection) {
    conn.execute_batch("BEGIN;").unwrap();
    for i in 1..=n {
        conn.execute(
            "INSERT INTO t VALUES (?, ?, ?)",
            params![i, i, format!("v{i}")],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();
}

fn limbo_bulk_insert_composite(n: u32, conn: &Arc<turso_core::Connection>) {
    conn.execute(format!(
        "INSERT INTO t SELECT value, value, 'v' || value FROM generate_series(1, {n})"
    ))
    .unwrap();
}

// --- multi_unique: INTEGER PRIMARY KEY + UNIQUE(a) + UNIQUE(b) ---

fn sqlite_bulk_insert_multi_unique(n: u32, conn: &rusqlite::Connection) {
    conn.execute_batch("BEGIN;").unwrap();
    for i in 1..=n {
        conn.execute(
            "INSERT INTO t VALUES (?, 'a' || ?, 'b' || ?)",
            params![i, i, i],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT;").unwrap();
}

fn limbo_bulk_insert_multi_unique(n: u32, conn: &Arc<turso_core::Connection>) {
    conn.execute(format!(
        "INSERT INTO t SELECT value, 'a' || value, 'b' || value FROM generate_series(1, {n})"
    ))
    .unwrap();
}

fn schemas() -> Vec<Schema> {
    vec![
        // PK-only conflict, no secondary indexes.
        Schema {
            name: "pk_only",
            ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT)",
            bulk_insert_sqlite: sqlite_bulk_insert_pk_only,
            bulk_insert_limbo: limbo_bulk_insert_pk_only,
            conflict_sql: |clause, n| {
                // Conflicts on PK: id=n already exists.
                format!("{clause} t VALUES ({n}, 'new')")
            },
            no_conflict_sql: |clause, seq| {
                format!("{clause} t VALUES (10000 + {seq}, 'fresh_{seq}')")
            },
            verify_sql: "SELECT id, a FROM t ORDER BY id",
        },
        // PK + single UNIQUE column; conflict on the UNIQUE column.
        Schema {
            name: "pk_unique",
            ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE)",
            bulk_insert_sqlite: sqlite_bulk_insert_pk_unique,
            bulk_insert_limbo: limbo_bulk_insert_pk_unique,
            conflict_sql: |clause, n| {
                // Conflicts on UNIQUE(a): 'a{n}' already exists, new rowid 9999.
                format!("{clause} t VALUES (9999, 'a{n}')")
            },
            no_conflict_sql: |clause, seq| {
                format!("{clause} t VALUES (10000 + {seq}, 'fresh_{seq}')")
            },
            verify_sql: "SELECT id, a FROM t ORDER BY id",
        },
        // Composite UNIQUE on non-PK columns; conflict on the composite index only.
        Schema {
            name: "composite_unique",
            ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, UNIQUE(a, b))",
            bulk_insert_sqlite: sqlite_bulk_insert_composite,
            bulk_insert_limbo: limbo_bulk_insert_composite,
            conflict_sql: |clause, n| {
                // Conflicts on UNIQUE(a, b): reuses the last (a, b) pair with a fresh PK.
                format!("{clause} t VALUES (9999, {n}, 'v{n}')")
            },
            no_conflict_sql: |clause, seq| {
                format!("{clause} t VALUES (10000 + {seq}, 10000 + {seq}, 'fresh_{seq}')")
            },
            verify_sql: "SELECT id, a, b FROM t ORDER BY id",
        },
        // Two UNIQUE indexes; conflict on the FIRST checked index (UNIQUE(a)).
        Schema {
            name: "multi_unique_first",
            ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE)",
            bulk_insert_sqlite: sqlite_bulk_insert_multi_unique,
            bulk_insert_limbo: limbo_bulk_insert_multi_unique,
            conflict_sql: |clause, n| {
                // Conflicts on UNIQUE(a) only; 'b_new' is fresh.
                format!("{clause} t VALUES (9999, 'a{n}', 'b_new')")
            },
            no_conflict_sql: |clause, seq| {
                format!("{clause} t VALUES (10000 + {seq}, 'x_{seq}', 'y_{seq}')")
            },
            verify_sql: "SELECT id, a, b FROM t ORDER BY id",
        },
        // Two UNIQUE indexes; conflict on the SECOND checked index (UNIQUE(b)).
        // Exercises the case where an earlier index's tentative insert must not
        // persist when a later index's constraint check fails.
        Schema {
            name: "multi_unique_second",
            ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE)",
            bulk_insert_sqlite: sqlite_bulk_insert_multi_unique,
            bulk_insert_limbo: limbo_bulk_insert_multi_unique,
            conflict_sql: |clause, n| {
                // Conflicts on UNIQUE(b) only; 'a_new' is fresh.
                format!("{clause} t VALUES (9999, 'a_new', 'b{n}')")
            },
            no_conflict_sql: |clause, seq| {
                format!("{clause} t VALUES (10000 + {seq}, 'x_{seq}', 'y_{seq}')")
            },
            verify_sql: "SELECT id, a, b FROM t ORDER BY id",
        },
        // Two UNIQUE indexes; conflict on BOTH indexes simultaneously, hitting
        // two different existing rows. REPLACE must delete both conflicting rows.
        Schema {
            name: "multi_unique_both",
            ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE)",
            bulk_insert_sqlite: sqlite_bulk_insert_multi_unique,
            bulk_insert_limbo: limbo_bulk_insert_multi_unique,
            conflict_sql: |clause, n| {
                // Conflicts on UNIQUE(a) via last row, UNIQUE(b) via first row.
                format!("{clause} t VALUES (9999, 'a{n}', 'b1')")
            },
            no_conflict_sql: |clause, seq| {
                format!("{clause} t VALUES (10000 + {seq}, 'x_{seq}', 'y_{seq}')")
            },
            verify_sql: "SELECT id, a, b FROM t ORDER BY id",
        },
    ]
}

// ---------------------------------------------------------------------------
// Conflict clause types
// ---------------------------------------------------------------------------

const CONFLICT_CLAUSES: &[(&str, &str)] = &[
    ("INSERT OR REPLACE INTO", "or_replace"),
    ("INSERT OR IGNORE INTO", "or_ignore"),
    ("INSERT OR ABORT INTO", "or_abort"),
    ("INSERT OR FAIL INTO", "or_fail"),
    ("INSERT OR ROLLBACK INTO", "or_rollback"),
    ("REPLACE INTO", "replace_into"),
];

// Row counts that exercise different B-tree paths.
const ROW_COUNTS: &[u32] = &[10, 134, 500, 1000];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run SQLite's integrity_check on the database file.
/// The Limbo connection + database must be dropped first so rusqlite can open the file
/// and recover the WAL.
fn sqlite_integrity_check(path: &std::path::Path) -> String {
    match crate::common::rusqlite_integrity_check(path) {
        Ok(()) => "ok".to_string(),
        Err(e) => e.to_string(),
    }
}

/// Execute a statement, returning Ok or Err (for statements that may fail due to constraint).
fn sqlite_try_exec(conn: &rusqlite::Connection, sql: &str) -> Result<(), String> {
    conn.execute_batch(sql).map_err(|e| e.to_string())
}

fn limbo_try_exec(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<(), String> {
    conn.execute(sql).map_err(|e| e.to_string())
}

/// Compare table contents between SQLite and Limbo, returning an error message on divergence.
fn compare_tables(
    label: &str,
    sqlite_conn: &rusqlite::Connection,
    limbo_conn: &Arc<turso_core::Connection>,
    verify_sql: &str,
) -> Option<String> {
    let sqlite_rows = sqlite_exec_rows(sqlite_conn, verify_sql);
    let limbo_rows = limbo_exec_rows(limbo_conn, verify_sql);
    if sqlite_rows != limbo_rows {
        // Show first diverging row for diagnostics.
        let first_diff = sqlite_rows
            .iter()
            .zip(limbo_rows.iter())
            .enumerate()
            .find(|(_, (s, l))| s != l)
            .map(|(i, (s, l))| format!(" first diff at row {i}: sqlite={s:?}, limbo={l:?}"));
        let diff_detail = first_diff.unwrap_or_default();
        Some(format!(
            "[{label}] Table contents diverged: sqlite={} rows, limbo={} rows.{diff_detail}",
            sqlite_rows.len(),
            limbo_rows.len()
        ))
    } else {
        None
    }
}

/// Execute a statement on both engines and compare outcomes.
/// Returns an error message if the engines disagree on success/failure.
fn exec_and_compare(
    label: &str,
    sqlite_conn: &rusqlite::Connection,
    limbo_conn: &Arc<turso_core::Connection>,
    sql: &str,
) -> Result<(), String> {
    let limbo_result = limbo_try_exec(limbo_conn, sql);
    let sqlite_result = sqlite_try_exec(sqlite_conn, sql);

    match (&sqlite_result, &limbo_result) {
        (Ok(()), Ok(())) | (Err(_), Err(_)) => Ok(()),
        (Ok(()), Err(e)) => Err(format!("[{label}] SQLite succeeded but Limbo failed: {e}")),
        (Err(e), Ok(())) => Err(format!("[{label}] Limbo succeeded but SQLite failed: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Matrix test
// ---------------------------------------------------------------------------

/// Run the full conflict resolution matrix.
///
/// For each (schema, conflict_clause, row_count) triple:
/// 1. Create independent Limbo and rusqlite databases with the same schema
/// 2. Bulk insert `row_count` rows into each
/// 3. Interleave non-conflicting and conflicting inserts:
///    - non-conflicting insert #1
///    - conflicting insert (exercises conflict resolution)
///    - non-conflicting insert #2
/// 4. Compare full table dumps after each step
/// 5. Run integrity_check on Limbo
#[turso_macros::test]
fn test_conflict_resolution_matrix(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // We don't use the macro-provided tmp_db; we create fresh DBs per cell.
    drop(tmp_db);

    let mut failures: Vec<String> = Vec::new();

    for schema in &schemas() {
        for &(clause, clause_name) in CONFLICT_CLAUSES {
            for &n in ROW_COUNTS {
                let label = format!("{}__{}__n{}", schema.name, clause_name, n);

                // --- Limbo side ---
                let limbo_db = TempDatabase::builder()
                    .with_db_name(format!("{label}.db"))
                    .build();
                let limbo_conn = limbo_db.connect_limbo();
                limbo_conn.execute(schema.ddl).unwrap();
                (schema.bulk_insert_limbo)(n, &limbo_conn);

                // --- SQLite side (in-memory) ---
                let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
                sqlite_conn.execute_batch(schema.ddl).unwrap();
                (schema.bulk_insert_sqlite)(n, &sqlite_conn);

                // --- Step 1: non-conflicting insert before the conflict ---
                let pre_sql = (schema.no_conflict_sql)(clause, 1);
                if let Err(e) = exec_and_compare(
                    &format!("{label}__pre"),
                    &sqlite_conn,
                    &limbo_conn,
                    &pre_sql,
                ) {
                    failures.push(e);
                    continue;
                }
                if let Some(e) = compare_tables(
                    &format!("{label}__pre"),
                    &sqlite_conn,
                    &limbo_conn,
                    schema.verify_sql,
                ) {
                    failures.push(e);
                }

                // --- Step 2: conflicting insert ---
                let conflict_sql = (schema.conflict_sql)(clause, n);
                if let Err(e) = exec_and_compare(
                    &format!("{label}__conflict"),
                    &sqlite_conn,
                    &limbo_conn,
                    &conflict_sql,
                ) {
                    failures.push(e);
                    continue;
                }
                if let Some(e) = compare_tables(
                    &format!("{label}__conflict"),
                    &sqlite_conn,
                    &limbo_conn,
                    schema.verify_sql,
                ) {
                    failures.push(e);
                }

                // --- Step 3: non-conflicting insert after the conflict ---
                // Verifies the database is still usable after conflict resolution.
                let post_sql = (schema.no_conflict_sql)(clause, 2);
                if let Err(e) = exec_and_compare(
                    &format!("{label}__post"),
                    &sqlite_conn,
                    &limbo_conn,
                    &post_sql,
                ) {
                    failures.push(e);
                    continue;
                }
                if let Some(e) = compare_tables(
                    &format!("{label}__post"),
                    &sqlite_conn,
                    &limbo_conn,
                    schema.verify_sql,
                ) {
                    failures.push(e);
                }

                // --- Integrity check (SQLite canonical) ---
                // Drop all Limbo handles so rusqlite can open the file and recover WAL.
                let db_path = limbo_db.path.clone();
                drop(limbo_conn);
                drop(limbo_db);
                let ic = sqlite_integrity_check(&db_path);
                if ic != "ok" {
                    failures.push(format!("[{label}] integrity_check: {ic}"));
                }
            }
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} failure(s) in conflict resolution matrix:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-row partial conflict test
// ---------------------------------------------------------------------------

/// Test multi-row VALUES where some rows conflict and some don't.
///
/// FAIL: rows before the conflict persist, execution stops at the conflict.
/// ABORT: all rows from the statement are rolled back.
/// IGNORE: conflicting row is skipped, other rows persist.
/// REPLACE: conflicting row is replaced, all rows persist.
/// ROLLBACK: entire transaction is rolled back (tested separately below).
#[turso_macros::test]
fn test_multi_row_partial_conflict(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE)";
    // Clauses where multi-row partial semantics differ meaningfully.
    let clauses: &[(&str, &str)] = &[
        ("INSERT OR FAIL INTO", "or_fail"),
        ("INSERT OR ABORT INTO", "or_abort"),
        ("INSERT OR IGNORE INTO", "or_ignore"),
        ("INSERT OR REPLACE INTO", "or_replace"),
    ];

    let mut failures: Vec<String> = Vec::new();

    for &(clause, clause_name) in clauses {
        for &n in &[10u32, 134, 500] {
            let label = format!("multi_row__{clause_name}__n{n}");

            let limbo_db = TempDatabase::builder()
                .with_db_name(format!("{label}.db"))
                .build();
            let limbo_conn = limbo_db.connect_limbo();
            limbo_conn.execute(ddl).unwrap();

            let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
            sqlite_conn.execute_batch(ddl).unwrap();

            // Bulk insert n rows: (1, 'a1'), (2, 'a2'), ..., (n, 'aN')
            limbo_bulk_insert_pk_unique(n, &limbo_conn);
            sqlite_bulk_insert_pk_unique(n, &sqlite_conn);

            // Multi-row insert: good row, conflicting row, good row.
            // Row 2 conflicts on UNIQUE(a) with existing 'a{n}'.
            let sql = format!(
                "{clause} t VALUES ({n_plus_1}, 'fresh1'), ({n_plus_2}, 'a{n}'), ({n_plus_3}, 'fresh2')",
                n_plus_1 = n + 1,
                n_plus_2 = n + 2,
                n_plus_3 = n + 3,
            );

            if let Err(e) = exec_and_compare(&label, &sqlite_conn, &limbo_conn, &sql) {
                failures.push(e);
                continue;
            }
            if let Some(e) = compare_tables(
                &label,
                &sqlite_conn,
                &limbo_conn,
                "SELECT id, a FROM t ORDER BY id",
            ) {
                failures.push(e);
            }

            let db_path = limbo_db.path.clone();
            drop(limbo_conn);
            drop(limbo_db);
            let ic = sqlite_integrity_check(&db_path);
            if ic != "ok" {
                failures.push(format!("[{label}] integrity_check: {ic}"));
            }
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} failure(s) in multi-row partial conflict test:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ROLLBACK inside explicit transaction
// ---------------------------------------------------------------------------

/// Test INSERT OR ROLLBACK inside an explicit transaction.
///
/// Unlike autocommit mode, OR ROLLBACK inside BEGIN...COMMIT rolls back
/// the entire transaction (not just the statement), leaving the database
/// in the state before BEGIN.
#[turso_macros::test]
fn test_or_rollback_in_transaction(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE)";

    let mut failures: Vec<String> = Vec::new();

    for &n in &[10u32, 134, 500] {
        let label = format!("rollback_in_txn__n{n}");

        let limbo_db = TempDatabase::builder()
            .with_db_name(format!("{label}.db"))
            .build();
        let limbo_conn = limbo_db.connect_limbo();
        limbo_conn.execute(ddl).unwrap();

        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        sqlite_conn.execute_batch(ddl).unwrap();

        // Bulk insert baseline data.
        limbo_bulk_insert_pk_unique(n, &limbo_conn);
        sqlite_bulk_insert_pk_unique(n, &sqlite_conn);

        // Start explicit transaction, insert a good row, then a conflicting row.
        // The OR ROLLBACK should roll back the entire transaction including the good row.
        let stmts = [
            "BEGIN",
            &format!("INSERT INTO t VALUES ({}, 'good')", n + 1),
            &format!("INSERT OR ROLLBACK INTO t VALUES ({}, 'a{n}')", n + 2),
        ];

        for stmt in &stmts {
            let _ = limbo_try_exec(&limbo_conn, stmt);
            let _ = sqlite_try_exec(&sqlite_conn, stmt);
        }

        // After ROLLBACK, the transaction is gone. Verify table matches.
        if let Some(e) = compare_tables(
            &label,
            &sqlite_conn,
            &limbo_conn,
            "SELECT id, a FROM t ORDER BY id",
        ) {
            failures.push(e);
        }

        let db_path = limbo_db.path.clone();
        drop(limbo_conn);
        drop(limbo_db);
        let ic = sqlite_integrity_check(&db_path);
        if ic != "ok" {
            failures.push(format!("[{label}] integrity_check: {ic}"));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} failure(s) in ROLLBACK-in-transaction test:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// UPDATE with table-level ON CONFLICT on rowid-alias PKs
// ---------------------------------------------------------------------------

/// Regression test for https://github.com/tursodatabase/turso/issues/5982
///
/// When a table has `INTEGER PRIMARY KEY ON CONFLICT ROLLBACK` (a rowid alias),
/// an UPDATE that causes a PK violation must rollback the entire transaction.
/// Before the fix, the UPDATE emitter searched `unique_sets` for the PK conflict
/// clause, but rowid-alias PKs are removed from `unique_sets` during schema
/// construction. This caused a fallback to ABORT, leaving the transaction active
/// when it should have been rolled back.
#[turso_macros::test]
fn test_update_rowid_alias_pk_on_conflict_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(x INTEGER PRIMARY KEY ON CONFLICT ROLLBACK, y TEXT)";
    let label = "update_rowid_pk_on_conflict_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    // Insert baseline rows into both.
    for sql in [
        "INSERT INTO t VALUES(1, 'a')",
        "INSERT INTO t VALUES(2, 'b')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Start an explicit transaction, insert a row, then UPDATE to cause a
    // PK conflict. ON CONFLICT ROLLBACK should terminate the entire transaction.
    let stmts = [
        "BEGIN",
        "INSERT INTO t VALUES(3, 'c')",
        "UPDATE t SET x = 1 WHERE x = 2", // PK conflict → ROLLBACK
    ];

    for stmt in &stmts {
        let _ = limbo_try_exec(&limbo_conn, stmt);
        let _ = sqlite_try_exec(&sqlite_conn, stmt);
    }

    // After ROLLBACK, the transaction is terminated. The INSERT of (3,'c')
    // should be rolled back too. Verify table contents match.
    if let Some(e) = compare_tables(
        &format!("{label}__after_rollback"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT x, y FROM t ORDER BY x",
    ) {
        panic!("{e}");
    }

    // The critical check from the issue: BEGIN should succeed because the
    // transaction was rolled back. If ROLLBACK was incorrectly treated as
    // ABORT, the old transaction is still active and BEGIN fails with
    // "cannot start a transaction within a transaction".
    if let Err(e) = exec_and_compare(
        &format!("{label}__begin_after_rollback"),
        &sqlite_conn,
        &limbo_conn,
        "BEGIN",
    ) {
        panic!("{e}");
    }

    // Clean up: commit the new transaction and insert a row to prove the
    // database is still fully functional after the rollback.
    for sql in ["INSERT INTO t VALUES(10, 'new')", "COMMIT"] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    if let Some(e) = compare_tables(
        &format!("{label}__final"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT x, y FROM t ORDER BY x",
    ) {
        panic!("{e}");
    }

    // Integrity check.
    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// Same as above but with all ON CONFLICT modes on rowid-alias PKs via UPDATE.
/// Verifies REPLACE, IGNORE, FAIL, ABORT, and ROLLBACK all behave correctly.
#[turso_macros::test]
fn test_update_rowid_alias_pk_all_conflict_modes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let modes: &[(&str, &str)] = &[
        (
            "REPLACE",
            "CREATE TABLE t(x INTEGER PRIMARY KEY ON CONFLICT REPLACE, y TEXT)",
        ),
        (
            "IGNORE",
            "CREATE TABLE t(x INTEGER PRIMARY KEY ON CONFLICT IGNORE, y TEXT)",
        ),
        (
            "FAIL",
            "CREATE TABLE t(x INTEGER PRIMARY KEY ON CONFLICT FAIL, y TEXT)",
        ),
        (
            "ABORT",
            "CREATE TABLE t(x INTEGER PRIMARY KEY ON CONFLICT ABORT, y TEXT)",
        ),
        (
            "ROLLBACK",
            "CREATE TABLE t(x INTEGER PRIMARY KEY ON CONFLICT ROLLBACK, y TEXT)",
        ),
    ];

    let mut failures: Vec<String> = Vec::new();

    for &(mode, ddl) in modes {
        let label = format!("update_rowid_pk__{mode}");

        let limbo_db = TempDatabase::builder()
            .with_db_name(format!("{label}.db"))
            .build();
        let limbo_conn = limbo_db.connect_limbo();
        limbo_conn.execute(ddl).unwrap();

        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        sqlite_conn.execute_batch(ddl).unwrap();

        for sql in [
            "INSERT INTO t VALUES(1, 'a')",
            "INSERT INTO t VALUES(2, 'b')",
            "INSERT INTO t VALUES(3, 'c')",
        ] {
            limbo_conn.execute(sql).unwrap();
            sqlite_conn.execute_batch(sql).unwrap();
        }

        // UPDATE that causes a PK conflict: set x=1 where x=2.
        let update_sql = "UPDATE t SET x = 1 WHERE x = 2";
        let _ = limbo_try_exec(&limbo_conn, update_sql);
        let _ = sqlite_try_exec(&sqlite_conn, update_sql);

        if let Some(e) = compare_tables(
            &label,
            &sqlite_conn,
            &limbo_conn,
            "SELECT x, y FROM t ORDER BY x",
        ) {
            failures.push(e);
        }

        let db_path = limbo_db.path.clone();
        drop(limbo_conn);
        drop(limbo_db);
        let ic = sqlite_integrity_check(&db_path);
        if ic != "ok" {
            failures.push(format!("[{label}] integrity_check: {ic}"));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} failure(s) in UPDATE rowid-alias PK conflict modes:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-constraint ordering tests
//
// SQLite enforces two ordering invariants:
// 1. REPLACE indexes are checked last (build.c:4453-4479)
// 2. IPK REPLACE is deferred when other constraints have different modes
//    (insert.c:2273-2285)
//
// These tests verify turso matches SQLite when multiple constraints with
// different conflict modes are violated simultaneously.
// ---------------------------------------------------------------------------

/// IPK ON CONFLICT REPLACE + UNIQUE ON CONFLICT ABORT: the ABORT should fire
/// (IPK REPLACE is deferred), no row deleted, statement rolled back.
#[turso_macros::test]
fn test_ipk_replace_with_index_abort(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT, \
            y TEXT UNIQUE ON CONFLICT ABORT\
        )";
    let label = "ipk_replace_index_abort";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    // Seed rows
    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // This INSERT conflicts on IPK (id=1 exists) AND on UNIQUE y ('v' exists).
    // ABORT should fire for y before IPK REPLACE runs.
    let conflict_sql = "INSERT INTO t VALUES(1, 'c', 'v')";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// IPK ON CONFLICT REPLACE + UNIQUE ON CONFLICT FAIL: FAIL should fire,
/// IPK REPLACE should not run.
#[turso_macros::test]
fn test_ipk_replace_with_index_fail(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT UNIQUE ON CONFLICT FAIL\
        )";
    let label = "ipk_replace_index_fail";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a')",
        "INSERT INTO t VALUES(2, 'b')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Conflicts on IPK (id=1) AND UNIQUE x ('b'). FAIL should fire for x.
    let conflict_sql = "INSERT INTO t VALUES(1, 'b')";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// Index ON CONFLICT REPLACE + another index ON CONFLICT ABORT on different
/// columns: ABORT should fire before REPLACE deletes (REPLACE-last ordering).
#[turso_macros::test]
fn test_index_replace_with_index_abort(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT UNIQUE ON CONFLICT REPLACE, \
            b TEXT UNIQUE ON CONFLICT ABORT\
        )";
    let label = "index_replace_index_abort";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Conflicts on UNIQUE a ('a' exists → REPLACE) AND UNIQUE b ('v' exists → ABORT).
    // ABORT should fire before REPLACE deletes row 1.
    let conflict_sql = "INSERT INTO t VALUES(3, 'a', 'v')";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// UPDATE variant: IPK REPLACE deferred past index constraint checks.
#[turso_macros::test]
fn test_update_ipk_replace_deferred(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT, \
            y TEXT UNIQUE ON CONFLICT ABORT\
        )";
    let label = "update_ipk_replace_deferred";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
        "INSERT INTO t VALUES(3, 'c', 'w')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // UPDATE changes row 3's id to 1 (IPK conflict → REPLACE) and y to 'v'
    // (UNIQUE conflict → ABORT). ABORT should fire; no row deleted.
    let conflict_sql = "UPDATE t SET id = 1, y = 'v' WHERE id = 3";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// UPDATE variant: index REPLACE + index ABORT ordering.
#[turso_macros::test]
fn test_update_index_replace_ordering(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT UNIQUE ON CONFLICT REPLACE, \
            b TEXT UNIQUE ON CONFLICT ABORT\
        )";
    let label = "update_index_replace_ordering";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
        "INSERT INTO t VALUES(3, 'c', 'w')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // UPDATE row 3: a='a' (REPLACE conflict with row 1) and b='v' (ABORT
    // conflict with row 2). ABORT should fire before REPLACE deletes.
    let conflict_sql = "UPDATE t SET a = 'a', b = 'v' WHERE id = 3";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// Test UPDATE with constraint-level ON CONFLICT REPLACE on a UNIQUE index.
///
/// Before the fix, `all_index_cursors` was only populated for statement-level
/// OR REPLACE, so constraint-level REPLACE on a unique index would delete the
/// conflicting row but leave stale index entries, causing index corruption.
#[turso_macros::test]
fn test_update_unique_index_on_conflict_replace_integrity(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE ON CONFLICT REPLACE)";
    let label = "update_unique_idx_replace";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'x')",
        "INSERT INTO t VALUES(2, 'y')",
        "INSERT INTO t VALUES(3, 'z')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // UPDATE causes unique constraint conflict on 'a': row (1,'x') should be
    // deleted and row (2,'y') should become (2,'x').
    let update_sql = "UPDATE t SET a = 'x' WHERE id = 2";
    let _ = limbo_try_exec(&limbo_conn, update_sql);
    let _ = sqlite_try_exec(&sqlite_conn, update_sql);

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, a FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Integrity check: stale index entries must be cleaned up.
    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// Exhaustive mixed-constraint ordering tests
//
// These tests cover every failure mode from incorrect constraint resolution
// ordering. Each test exercises a specific combination of REPLACE (on one
// constraint) + non-REPLACE (on another constraint), for both INSERT and
// UPDATE paths.
//
// Failure modes caught:
// - REPLACE fires before ABORT/FAIL/IGNORE/ROLLBACK → premature row deletion
// - IPK REPLACE not deferred → row deleted before index constraint fires
// - Commit phase skipping non-REPLACE indexes → index corruption
// ---------------------------------------------------------------------------

/// Helper: run a mixed-constraint test case.
fn run_mixed_constraint_case(
    label: &str,
    ddl: &str,
    seed_rows: &[&str],
    conflict_sql: &str,
    verify_sql: &str,
) {
    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in seed_rows {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    let limbo_result = limbo_try_exec(&limbo_conn, conflict_sql);
    let sqlite_result = sqlite_try_exec(&sqlite_conn, conflict_sql);

    match (&sqlite_result, &limbo_result) {
        (Ok(()), Err(e)) => panic!("[{label}] SQLite succeeded but Limbo failed: {e}"),
        (Err(e), Ok(())) => panic!("[{label}] Limbo succeeded but SQLite failed: {e}"),
        _ => {}
    }

    if let Some(e) = compare_tables(label, &sqlite_conn, &limbo_conn, verify_sql) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");
}

// ---------------------------------------------------------------------------
// INSERT: IPK REPLACE + UNIQUE non-REPLACE (IGNORE, ROLLBACK)
// (ABORT and FAIL already covered above)
// ---------------------------------------------------------------------------

/// IPK REPLACE + UNIQUE IGNORE: the IGNORE should fire (IPK REPLACE deferred),
/// row silently skipped, no deletion.
#[turso_macros::test]
fn test_ipk_replace_with_index_ignore(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "ipk_replace_index_ignore",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT, \
            y TEXT UNIQUE ON CONFLICT IGNORE\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a', 'u')",
            "INSERT INTO t VALUES(2, 'b', 'v')",
        ],
        "INSERT INTO t VALUES(1, 'c', 'v')",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// IPK REPLACE + UNIQUE ROLLBACK: the ROLLBACK should fire, transaction terminated.
#[turso_macros::test]
fn test_ipk_replace_with_index_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
        x TEXT, \
        y TEXT UNIQUE ON CONFLICT ROLLBACK\
    )";
    let label = "ipk_replace_index_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    for sql in ["BEGIN", "INSERT INTO t VALUES(3, 'good', 'w')"] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(1, 'c', 'v')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(1, 'c', 'v')");

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// INSERT: Index REPLACE + Index non-REPLACE (FAIL, IGNORE, ROLLBACK)
// (ABORT already covered by test_index_replace_with_index_abort)
// ---------------------------------------------------------------------------

/// Index REPLACE + Index FAIL: FAIL should fire before REPLACE deletes.
#[turso_macros::test]
fn test_index_replace_with_index_fail(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "index_replace_index_fail",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT UNIQUE ON CONFLICT REPLACE, \
            b TEXT UNIQUE ON CONFLICT FAIL\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a', 'u')",
            "INSERT INTO t VALUES(2, 'b', 'v')",
        ],
        "INSERT INTO t VALUES(3, 'a', 'v')",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// Index REPLACE + Index IGNORE: IGNORE should fire, row skipped, no deletion.
#[turso_macros::test]
fn test_index_replace_with_index_ignore(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "index_replace_index_ignore",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT UNIQUE ON CONFLICT REPLACE, \
            b TEXT UNIQUE ON CONFLICT IGNORE\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a', 'u')",
            "INSERT INTO t VALUES(2, 'b', 'v')",
        ],
        "INSERT INTO t VALUES(3, 'a', 'v')",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// Index REPLACE + Index ROLLBACK: ROLLBACK should fire, transaction terminated.
#[turso_macros::test]
fn test_index_replace_with_index_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY, \
        a TEXT UNIQUE ON CONFLICT REPLACE, \
        b TEXT UNIQUE ON CONFLICT ROLLBACK\
    )";
    let label = "index_replace_index_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    for sql in ["BEGIN", "INSERT INTO t VALUES(3, 'c', 'w')"] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(4, 'a', 'v')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(4, 'a', 'v')");

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// UPDATE: IPK REPLACE + UNIQUE non-REPLACE (FAIL, IGNORE, ROLLBACK)
// (ABORT already covered by test_update_ipk_replace_deferred)
// ---------------------------------------------------------------------------

/// UPDATE: IPK REPLACE + UNIQUE FAIL.
#[turso_macros::test]
fn test_update_ipk_replace_with_index_fail(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "update_ipk_replace_index_fail",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT UNIQUE ON CONFLICT FAIL\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a')",
            "INSERT INTO t VALUES(2, 'b')",
            "INSERT INTO t VALUES(3, 'c')",
        ],
        "UPDATE t SET id = 1, x = 'b' WHERE id = 3",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// UPDATE: IPK REPLACE + UNIQUE IGNORE.
#[turso_macros::test]
fn test_update_ipk_replace_with_index_ignore(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "update_ipk_replace_index_ignore",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT, \
            y TEXT UNIQUE ON CONFLICT IGNORE\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a', 'u')",
            "INSERT INTO t VALUES(2, 'b', 'v')",
            "INSERT INTO t VALUES(3, 'c', 'w')",
        ],
        "UPDATE t SET id = 1, y = 'v' WHERE id = 3",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// UPDATE: IPK REPLACE + UNIQUE ROLLBACK.
#[turso_macros::test]
fn test_update_ipk_replace_with_index_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
        x TEXT, \
        y TEXT UNIQUE ON CONFLICT ROLLBACK\
    )";
    let label = "update_ipk_replace_index_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
        "INSERT INTO t VALUES(3, 'c', 'w')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    for sql in ["BEGIN", "INSERT INTO t VALUES(4, 'd', 'x')"] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    let _ = limbo_try_exec(&limbo_conn, "UPDATE t SET id = 1, y = 'v' WHERE id = 3");
    let _ = sqlite_try_exec(&sqlite_conn, "UPDATE t SET id = 1, y = 'v' WHERE id = 3");

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// UPDATE: Index REPLACE + Index non-REPLACE (FAIL, IGNORE, ROLLBACK)
// (ABORT already covered by test_update_index_replace_ordering)
// ---------------------------------------------------------------------------

/// UPDATE: Index REPLACE + Index FAIL.
#[turso_macros::test]
fn test_update_index_replace_with_fail(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "update_index_replace_fail",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT UNIQUE ON CONFLICT REPLACE, \
            b TEXT UNIQUE ON CONFLICT FAIL\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a', 'u')",
            "INSERT INTO t VALUES(2, 'b', 'v')",
            "INSERT INTO t VALUES(3, 'c', 'w')",
        ],
        "UPDATE t SET a = 'a', b = 'v' WHERE id = 3",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// UPDATE: Index REPLACE + Index IGNORE.
#[turso_macros::test]
fn test_update_index_replace_with_ignore(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "update_index_replace_ignore",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT UNIQUE ON CONFLICT REPLACE, \
            b TEXT UNIQUE ON CONFLICT IGNORE\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a', 'u')",
            "INSERT INTO t VALUES(2, 'b', 'v')",
            "INSERT INTO t VALUES(3, 'c', 'w')",
        ],
        "UPDATE t SET a = 'a', b = 'v' WHERE id = 3",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

/// UPDATE: Index REPLACE + Index ROLLBACK.
#[turso_macros::test]
fn test_update_index_replace_with_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY, \
        a TEXT UNIQUE ON CONFLICT REPLACE, \
        b TEXT UNIQUE ON CONFLICT ROLLBACK\
    )";
    let label = "update_index_replace_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a', 'u')",
        "INSERT INTO t VALUES(2, 'b', 'v')",
        "INSERT INTO t VALUES(3, 'c', 'w')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    for sql in ["BEGIN", "INSERT INTO t VALUES(4, 'd', 'x')"] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    let _ = limbo_try_exec(&limbo_conn, "UPDATE t SET a = 'a', b = 'v' WHERE id = 3");
    let _ = sqlite_try_exec(&sqlite_conn, "UPDATE t SET a = 'a', b = 'v' WHERE id = 3");

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// Three indexes with three different conflict modes
// ---------------------------------------------------------------------------

/// Three unique indexes with ROLLBACK, IGNORE, and REPLACE respectively.
/// Tests that non-REPLACE indexes are checked before REPLACE, and that the
/// first-defined non-REPLACE constraint determines the outcome.
#[turso_macros::test]
fn test_three_indexes_three_modes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY, \
        a TEXT UNIQUE ON CONFLICT ROLLBACK, \
        b TEXT UNIQUE ON CONFLICT IGNORE, \
        c TEXT UNIQUE ON CONFLICT REPLACE\
    )";
    let label = "three_indexes_three_modes";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'x', 'p', 'i')",
        "INSERT INTO t VALUES(2, 'y', 'q', 'j')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Case 1: Conflict on ROLLBACK index only → error.
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(3, 'x', 'r', 'k')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(3, 'x', 'r', 'k')");
    if let Some(e) = compare_tables(
        &format!("{label}__rollback_fires"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Case 2: Conflict on IGNORE index only → row skipped.
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(3, 'z', 'p', 'k')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(3, 'z', 'p', 'k')");
    if let Some(e) = compare_tables(
        &format!("{label}__ignore_fires"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Case 3: Conflict on REPLACE index only → conflicting row deleted.
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(3, 'z', 'r', 'i')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(3, 'z', 'r', 'i')");
    if let Some(e) = compare_tables(
        &format!("{label}__replace_fires"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Case 4: Conflict on all three simultaneously.
    for sql in [
        "DELETE FROM t",
        "INSERT INTO t VALUES(1, 'x', 'p', 'i')",
        "INSERT INTO t VALUES(2, 'y', 'q', 'j')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(3, 'x', 'q', 'j')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(3, 'x', 'q', 'j')");
    if let Some(e) = compare_tables(
        &format!("{label}__all_three"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Case 5: Conflict on REPLACE + IGNORE (not ROLLBACK). IGNORE fires.
    for sql in [
        "DELETE FROM t",
        "INSERT INTO t VALUES(1, 'x', 'p', 'i')",
        "INSERT INTO t VALUES(2, 'y', 'q', 'j')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(3, 'z', 'p', 'j')");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(3, 'z', 'p', 'j')");
    if let Some(e) = compare_tables(
        &format!("{label}__replace_ignore"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-row INSERT with FAIL + mixed constraint modes
// ---------------------------------------------------------------------------

/// Multi-row INSERT where FAIL semantics interact with mixed constraint modes.
#[turso_macros::test]
fn test_multi_row_fail_with_mixed_constraints(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
        x TEXT UNIQUE ON CONFLICT FAIL\
    )";
    let label = "multi_row_fail_mixed";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a')",
        "INSERT INTO t VALUES(2, 'b')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Multi-row: row 1 succeeds, row 2 conflicts on UNIQUE x ('a').
    // FAIL: row (3,'c') persists, error at row (4,'a').
    let multi_sql = "INSERT INTO t VALUES(3, 'c'), (4, 'a')";
    let _ = limbo_try_exec(&limbo_conn, multi_sql);
    let _ = sqlite_try_exec(&sqlite_conn, multi_sql);

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// Multi-row INSERT where a row conflicts on both IPK (REPLACE) and UNIQUE (FAIL).
/// FAIL fires first (IPK deferred), so the IPK REPLACE never runs.
#[turso_macros::test]
fn test_multi_row_ipk_replace_unique_fail(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "CREATE TABLE t(\
        id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
        x TEXT UNIQUE ON CONFLICT FAIL\
    )";
    let label = "multi_row_ipk_replace_fail";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    limbo_conn.execute("INSERT INTO t VALUES(1, 'a')").unwrap();
    sqlite_conn
        .execute_batch("INSERT INTO t VALUES(1, 'a')")
        .unwrap();

    // Multi-row: good row, then row conflicting on both IPK AND UNIQUE.
    let multi_sql = "INSERT INTO t VALUES(2, 'b'), (1, 'a')";
    let _ = limbo_try_exec(&limbo_conn, multi_sql);
    let _ = sqlite_try_exec(&sqlite_conn, multi_sql);

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// REPLACE-only success cases (verify REPLACE still works when all constraints
// use REPLACE, so our reordering doesn't break the all-REPLACE path)
// ---------------------------------------------------------------------------

/// IPK REPLACE + UNIQUE REPLACE: both constraints use REPLACE, should succeed.
#[turso_macros::test]
fn test_ipk_replace_with_index_replace(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_mixed_constraint_case(
        "ipk_replace_index_replace",
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            x TEXT UNIQUE ON CONFLICT REPLACE\
        )",
        &[
            "INSERT INTO t VALUES(1, 'a')",
            "INSERT INTO t VALUES(2, 'b')",
        ],
        "INSERT INTO t VALUES(1, 'b')",
        "SELECT * FROM t ORDER BY id",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Commit phase integrity: mixed modes with non-unique indexes
// ---------------------------------------------------------------------------

/// Tests that non-REPLACE indexes get their entries inserted in the commit phase
/// even when IPK has REPLACE. Catches the pre-existing bug where
/// `any_constraint_replace` caused the commit phase to be skipped entirely.
#[turso_macros::test]
fn test_commit_phase_non_replace_index_integrity(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let label = "commit_phase_integrity";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, a TEXT UNIQUE ON CONFLICT ABORT, b TEXT)")
        .unwrap();
    limbo_conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn
        .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, a TEXT UNIQUE ON CONFLICT ABORT, b TEXT); CREATE INDEX idx_b ON t(b)")
        .unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'x', 'p')",
        "INSERT INTO t VALUES(2, 'y', 'q')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Insert with IPK conflict only. REPLACE deletes row 1.
    // Non-unique index idx_b must get an entry for the new row.
    let sql = "INSERT INTO t VALUES(1, 'z', 'r')";
    let _ = limbo_try_exec(&limbo_conn, sql);
    let _ = sqlite_try_exec(&sqlite_conn, sql);

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Verify idx_b is queryable (would fail if entry missing).
    let idx_verify = "SELECT * FROM t WHERE b = 'r'";
    let limbo_rows = limbo_exec_rows(&limbo_conn, idx_verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, idx_verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "[{label}] Index query diverged: limbo={limbo_rows:?}, sqlite={sqlite_rows:?}"
    );

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// Test that partial indexes with REPLACE don't corrupt when another index has ABORT.
/// This specifically tests the three-phase separation: Phase 1 checks the partial index
/// constraint BEFORE Phase 2/3 mutate any index entries.
#[turso_macros::test]
fn test_update_partial_index_replace_ordering(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b INT, c INT)",
        "CREATE UNIQUE INDEX idx_b ON t(b)",
        "CREATE UNIQUE INDEX idx_c ON t(c) WHERE c IS NOT NULL",
        "INSERT INTO t VALUES(1, 10, 100)",
        "INSERT INTO t VALUES(2, 20, 200)",
        "INSERT INTO t VALUES(3, 30, NULL)",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // UPDATE that conflicts on the partial index (c=200 already exists for row 2)
    let update = "UPDATE t SET b=40, c=200 WHERE a=1";
    let limbo_res = limbo_db.execute(update);
    let sqlite_res = sqlite_db.execute(update, params![]);
    assert_eq!(
        limbo_res.is_err(),
        sqlite_res.is_err(),
        "outcome mismatch: limbo={limbo_res:?} sqlite={sqlite_res:?}"
    );

    // Both should have identical data — no orphan index entries
    let verify = "SELECT a, b, c FROM t ORDER BY a";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "data mismatch after partial index conflict"
    );

    // UPDATE that doesn't conflict because c=NULL doesn't participate in partial index
    let update2 = "UPDATE t SET c=NULL WHERE a=1";
    limbo_db.execute(update2)?;
    sqlite_db.execute(update2, params![])?;

    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(limbo_rows, sqlite_rows, "data mismatch after NULL update");

    Ok(())
}

/// Test with 3 indexes where the old interleaved loop would corrupt indexes but
/// three-phase separation keeps them consistent.
///
/// Setup: 3 unique indexes with different conflict modes. An UPDATE triggers
/// ABORT on the last index. With interleaved mutations, the first two indexes
/// would already have been mutated (IdxDelete + IdxInsert) before the ABORT fires.
/// With three-phase separation, Phase 1 checks ALL constraints before Phase 2/3.
#[turso_macros::test]
fn test_update_three_indexes_interleave_safety(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c TEXT UNIQUE, d TEXT UNIQUE)",
        "INSERT INTO t VALUES(1, 'b1', 'c1', 'd1')",
        "INSERT INTO t VALUES(2, 'b2', 'c2', 'd2')",
        "INSERT INTO t VALUES(3, 'b3', 'c3', 'd3')",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // UPDATE row 1: change b, c, d. d='d2' conflicts with row 2 → ABORT.
    // With interleaved loop: idx_b and idx_c would be mutated before idx_d's check fails.
    // With three-phase: Phase 1 checks all three, ABORT fires, no mutations happen.
    let update = "UPDATE t SET b='bX', c='cX', d='d2' WHERE a=1";
    let limbo_res = limbo_db.execute(update);
    let sqlite_res = sqlite_db.execute(update, params![]);
    assert_eq!(
        limbo_res.is_err(),
        sqlite_res.is_err(),
        "outcome mismatch: limbo={limbo_res:?} sqlite={sqlite_res:?}"
    );

    // Verify all data matches
    let verify = "SELECT a, b, c, d FROM t ORDER BY a";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "data mismatch after aborted UPDATE"
    );

    // Verify each index is queryable and returns correct results
    for col in ["b", "c", "d"] {
        for val in ["1", "2", "3"] {
            let query = format!("SELECT a FROM t WHERE {col} = '{col}{val}'");
            let limbo_rows = limbo_exec_rows(&limbo_db, &query);
            let sqlite_rows = sqlite_exec_rows(&sqlite_db, &query);
            assert_eq!(
                limbo_rows, sqlite_rows,
                "index query diverged for {col}={col}{val}"
            );
        }
    }

    // Now do a successful UPDATE to verify indexes work after the failed one
    let update2 = "UPDATE t SET b='bY', c='cY', d='dY' WHERE a=1";
    limbo_db.execute(update2)?;
    sqlite_db.execute(update2, params![])?;

    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "data mismatch after successful UPDATE"
    );

    Ok(())
}

/// Test that UPDATE OR FAIL keeps prior row updates when a later row conflicts.
/// Row 2 is updated to val=100 successfully, then row 3 also tries val=100
/// which conflicts with the just-updated row 2. FAIL keeps row 2's update.
#[turso_macros::test]
fn test_update_or_fail_partial_success(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)",
        "INSERT INTO t VALUES(1, 10)",
        "INSERT INTO t VALUES(2, 20)",
        "INSERT INTO t VALUES(3, 30)",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // This UPDATE processes rows in PK order: row 2 → val=100 (OK), row 3 → val=100 (CONFLICT).
    // FAIL keeps row 2's update but stops at row 3.
    let update = "UPDATE OR FAIL t SET val = 100 WHERE id >= 2";
    let limbo_res = limbo_db.execute(update);
    let sqlite_res = sqlite_db.execute(update, params![]);
    assert!(
        limbo_res.is_err(),
        "limbo should return UNIQUE constraint error"
    );
    assert!(
        sqlite_res.is_err(),
        "sqlite should return UNIQUE constraint error"
    );

    // Verify: row 1 unchanged, row 2 updated, row 3 unchanged
    let verify = "SELECT id, val FROM t ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "data mismatch after UPDATE OR FAIL: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );
    // Row 1 unchanged, row 2 updated to 100, row 3 unchanged at 30
    use rusqlite::types::Value::Integer;
    assert_eq!(
        sqlite_rows,
        vec![
            vec![Integer(1), Integer(10)],
            vec![Integer(2), Integer(100)],
            vec![Integer(3), Integer(30)],
        ],
        "expected row 2 updated, row 3 unchanged"
    );

    Ok(())
}

/// Same as above but inside an explicit transaction — prior row updates AND
/// other transaction work should be preserved by FAIL (not rolled back).
#[turso_macros::test]
fn test_update_or_fail_partial_success_in_txn(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)",
        "INSERT INTO t VALUES(1, 10)",
        "INSERT INTO t VALUES(2, 20)",
        "INSERT INTO t VALUES(3, 30)",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // Begin transaction, do some work, then UPDATE OR FAIL
    limbo_db.execute("BEGIN")?;
    sqlite_db.execute("BEGIN", params![])?;
    limbo_db.execute("INSERT INTO t VALUES(4, 40)")?;
    sqlite_db.execute("INSERT INTO t VALUES(4, 40)", params![])?;

    let update = "UPDATE OR FAIL t SET val = 100 WHERE id >= 2";
    let limbo_res = limbo_db.execute(update);
    let sqlite_res = sqlite_db.execute(update, params![]);
    assert!(limbo_res.is_err());
    assert!(sqlite_res.is_err());

    // Verify state inside the transaction: row 4 exists, row 2 updated
    let verify = "SELECT id, val FROM t ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "data mismatch in txn after UPDATE OR FAIL: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );

    // Commit and verify again
    limbo_db.execute("COMMIT")?;
    sqlite_db.execute("COMMIT", params![])?;

    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "data mismatch after COMMIT: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );

    Ok(())
}

/// Test DDL-level REPLACE on two unique indexes where INSERT conflicts with
/// two DIFFERENT existing rows. Both conflicting rows should be deleted.
#[turso_macros::test]
fn test_ddl_replace_cascade_two_indexes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT REPLACE)",
        "INSERT INTO t VALUES(1, 'x', 'p')",
        "INSERT INTO t VALUES(2, 'y', 'q')",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // Conflicts on 'a' with row 1, on 'b' with row 2 — both should be replaced
    limbo_db.execute("INSERT INTO t VALUES(3, 'x', 'q')")?;
    sqlite_db.execute("INSERT INTO t VALUES(3, 'x', 'q')", params![])?;

    let verify = "SELECT id, a, b FROM t ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "DDL REPLACE cascade: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );
    use rusqlite::types::Value::{Integer, Text};
    assert_eq!(
        sqlite_rows,
        vec![vec![Integer(3), Text("x".into()), Text("q".into())]],
        "only row 3 should remain"
    );

    Ok(())
}

/// Test DDL-level REPLACE on two indexes with UPDATE — update triggers REPLACE
/// deletion of a different row on both indexes.
#[turso_macros::test]
fn test_ddl_replace_cascade_two_indexes_update(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT REPLACE)",
        "INSERT INTO t VALUES(1, 'x', 'p')",
        "INSERT INTO t VALUES(2, 'y', 'q')",
        "INSERT INTO t VALUES(3, 'z', 'r')",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // Update row 3 to conflict with row 1 on 'a' and row 2 on 'b'
    let update = "UPDATE t SET a = 'x', b = 'q' WHERE id = 3";
    limbo_db.execute(update)?;
    sqlite_db.execute(update, params![])?;

    let verify = "SELECT id, a, b FROM t ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "DDL REPLACE cascade update: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );
    {
        use rusqlite::types::Value::{Integer, Text};
        assert_eq!(
            sqlite_rows,
            vec![vec![Integer(3), Text("x".into()), Text("q".into())]],
            "only row 3 should remain after REPLACE cascade"
        );
    }

    Ok(())
}

/// Test mixed DDL-level conflict modes: ABORT on one index, REPLACE on another.
/// The ABORT index is checked first (non-REPLACE sorted before REPLACE),
/// so the INSERT should fail without any REPLACE deletions.
#[turso_macros::test]
fn test_ddl_mixed_abort_replace_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE)",
        "INSERT INTO t VALUES(1, 'x', 'p')",
        "INSERT INTO t VALUES(2, 'y', 'q')",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // Conflicts on ABORT index 'a' (row 1) AND REPLACE index 'b' (row 2).
    // ABORT fires first → entire INSERT rolled back, no REPLACE deletion.
    let insert = "INSERT INTO t VALUES(3, 'x', 'q')";
    let limbo_res = limbo_db.execute(insert);
    let sqlite_res = sqlite_db.execute(insert, params![]);
    assert!(limbo_res.is_err(), "limbo should fail on ABORT constraint");
    assert!(
        sqlite_res.is_err(),
        "sqlite should fail on ABORT constraint"
    );

    let verify = "SELECT id, a, b FROM t ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "mixed ABORT+REPLACE: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );
    // Both original rows should be intact — ABORT rolled back the entire INSERT
    {
        use rusqlite::types::Value::{Integer, Text};
        assert_eq!(
            sqlite_rows,
            vec![
                vec![Integer(1), Text("x".into()), Text("p".into())],
                vec![Integer(2), Text("y".into()), Text("q".into())],
            ],
            "both original rows should survive"
        );
    }

    Ok(())
}

/// Same as above but with REPLACE first, ABORT second in DDL order.
/// Schema sorting ensures ABORT is still checked first regardless of DDL order.
#[turso_macros::test]
fn test_ddl_mixed_replace_abort_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let limbo_db = tmp_db.connect_limbo();
    let sqlite_db = rusqlite::Connection::open_in_memory()?;

    let setup = [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT UNIQUE ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT)",
        "INSERT INTO t VALUES(1, 'x', 'p')",
        "INSERT INTO t VALUES(2, 'y', 'q')",
    ];
    for sql in &setup {
        limbo_db.execute(sql)?;
        sqlite_db.execute(sql, params![])?;
    }

    // Conflicts on REPLACE index 'a' (row 1) AND ABORT index 'b' (row 2).
    // ABORT should fire first → entire INSERT rolled back.
    let insert = "INSERT INTO t VALUES(3, 'x', 'q')";
    let limbo_res = limbo_db.execute(insert);
    let sqlite_res = sqlite_db.execute(insert, params![]);
    assert!(limbo_res.is_err(), "limbo should fail on ABORT constraint");
    assert!(
        sqlite_res.is_err(),
        "sqlite should fail on ABORT constraint"
    );

    let verify = "SELECT id, a, b FROM t ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_db, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_db, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "mixed REPLACE+ABORT: limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );
    {
        use rusqlite::types::Value::{Integer, Text};
        assert_eq!(
            sqlite_rows,
            vec![
                vec![Integer(1), Text("x".into()), Text("p".into())],
                vec![Integer(2), Text("y".into()), Text("q".into())],
            ],
            "both original rows should survive"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// FK violations during UPDATE — stmt_journal rollback correctness
// ---------------------------------------------------------------------------

/// Run a FK test case: enables PRAGMA foreign_keys, handles multi-statement DDL,
/// then executes a conflict SQL and compares Limbo vs SQLite.
fn run_fk_constraint_case(
    label: &str,
    ddl_stmts: &[&str],
    seed_rows: &[&str],
    conflict_sql: &str,
    verify_sql: &str,
) {
    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
    for stmt in ddl_stmts {
        limbo_conn.execute(stmt).unwrap();
    }

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn
        .execute_batch("PRAGMA foreign_keys = ON;")
        .unwrap();
    for stmt in ddl_stmts {
        sqlite_conn.execute_batch(stmt).unwrap();
    }

    for sql in seed_rows {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    let limbo_result = limbo_try_exec(&limbo_conn, conflict_sql);
    let sqlite_result = sqlite_try_exec(&sqlite_conn, conflict_sql);

    match (&sqlite_result, &limbo_result) {
        (Ok(()), Err(e)) => panic!("[{label}] SQLite succeeded but Limbo failed: {e}"),
        (Err(e), Ok(())) => panic!("[{label}] Limbo succeeded but SQLite failed: {e}"),
        _ => {}
    }

    if let Some(e) = compare_tables(label, &sqlite_conn, &limbo_conn, verify_sql) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");
}

/// Multi-row UPDATE that violates child-side FK: all rows must be rolled back.
/// The FkCounter is incremented during the scan, then Halt checks the counter.
/// Statement journal is required because multi_write=true (has_fks) and may_abort=true.
#[turso_macros::test]
fn test_update_fk_child_violation_multi_row_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "fk_child_multi_row_rollback",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT)",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO parent VALUES(2, 'b')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 2, 'y')",
            "INSERT INTO child VALUES(3, 1, 'z')",
        ],
        // Set all child FK refs to 999 (non-existent parent) — all 3 rows violate
        "UPDATE child SET pid = 999",
        "SELECT * FROM child ORDER BY id",
    );
    Ok(())
}

/// Multi-row UPDATE child FK violation inside explicit transaction.
/// Statement rolls back but transaction is preserved with prior state intact.
#[turso_macros::test]
fn test_update_fk_child_violation_in_txn(tmp_db: TempDatabase) -> anyhow::Result<()> {
    use rusqlite::types::Value::{Integer, Text};
    drop(tmp_db);

    let label = "fk_child_violation_in_txn";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
    limbo_conn
        .execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    limbo_conn
        .execute("CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT)")
        .unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn
        .execute_batch(
            "PRAGMA foreign_keys = ON;\
             CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);\
             CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);",
        )
        .unwrap();

    let seed = [
        "INSERT INTO parent VALUES(1, 'a')",
        "INSERT INTO parent VALUES(2, 'b')",
        "INSERT INTO child VALUES(1, 1, 'x')",
        "INSERT INTO child VALUES(2, 2, 'y')",
    ];
    for sql in &seed {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Start explicit transaction, do a successful write, then a failing FK update.
    for conn_sql in ["BEGIN", "INSERT INTO child VALUES(3, 1, 'z')"] {
        limbo_conn.execute(conn_sql).unwrap();
        sqlite_conn.execute_batch(conn_sql).unwrap();
    }

    // This should fail — pid=999 doesn't exist in parent.
    let fail_sql = "UPDATE child SET pid = 999 WHERE id <= 2";
    let limbo_res = limbo_try_exec(&limbo_conn, fail_sql);
    let sqlite_res = sqlite_try_exec(&sqlite_conn, fail_sql);
    assert!(sqlite_res.is_err(), "[{label}] SQLite should fail");
    assert!(limbo_res.is_err(), "[{label}] Limbo should fail");

    // Transaction should still be active, row 3 from the earlier INSERT should persist.
    limbo_conn.execute("COMMIT").unwrap();
    sqlite_conn.execute_batch("COMMIT").unwrap();

    let verify = "SELECT * FROM child ORDER BY id";
    let limbo_rows = limbo_exec_rows(&limbo_conn, verify);
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, verify);
    assert_eq!(
        limbo_rows, sqlite_rows,
        "[{label}] limbo={limbo_rows:?} sqlite={sqlite_rows:?}"
    );
    assert_eq!(
        sqlite_rows,
        vec![
            vec![Integer(1), Integer(1), Text("x".into())],
            vec![Integer(2), Integer(2), Text("y".into())],
            vec![Integer(3), Integer(1), Text("z".into())],
        ],
        "FK-violating UPDATE rolled back, prior INSERT preserved"
    );

    Ok(())
}

/// Multi-row UPDATE on parent table when children reference it (NO ACTION).
/// All updates rolled back because FK violation fires at Halt.
#[turso_macros::test]
fn test_update_fk_parent_violation_multi_row(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "fk_parent_multi_row",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT)",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO parent VALUES(2, 'b')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 2, 'y')",
        ],
        // Shift parent IDs — both children now reference non-existent parents
        "UPDATE parent SET id = id + 100",
        "SELECT * FROM parent ORDER BY id",
    );
    Ok(())
}

/// UPDATE OR IGNORE does NOT suppress FK violations.
/// FK errors are separate from constraint conflict resolution.
#[turso_macros::test]
fn test_update_or_ignore_fk_not_suppressed(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "or_ignore_fk_not_suppressed",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT)",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
        ],
        "UPDATE OR IGNORE child SET pid = 999",
        "SELECT * FROM child ORDER BY id",
    );
    Ok(())
}

/// UPDATE OR REPLACE does NOT suppress FK violations.
#[turso_macros::test]
fn test_update_or_replace_fk_not_suppressed(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "or_replace_fk_not_suppressed",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT)",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
        ],
        "UPDATE OR REPLACE child SET pid = 999",
        "SELECT * FROM child ORDER BY id",
    );
    Ok(())
}

/// UPDATE with both FK violation and unique constraint violation.
/// Both should fire — unique constraint checked inline, FK at Halt.
#[turso_macros::test]
fn test_update_fk_and_unique_violation(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "fk_and_unique_violation",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id), \
                tag TEXT UNIQUE\
             )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO child VALUES(1, 1, 'u')",
            "INSERT INTO child VALUES(2, 1, 'v')",
        ],
        // pid=999 (FK violation) AND tag='u' conflicts with row 1 (unique violation).
        // Unique fires inline as ABORT, FK fires at Halt — either way it errors.
        "UPDATE child SET pid = 999, tag = 'u' WHERE id = 2",
        "SELECT * FROM child ORDER BY id",
    );
    Ok(())
}

/// Partial UPDATE where only some rows violate FK — all rows still rolled back.
/// FkCounter accumulates during scan, Halt checks total > 0.
#[turso_macros::test]
fn test_update_fk_partial_violation_all_rolled_back(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "fk_partial_violation_rollback",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT)",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO parent VALUES(2, 'b')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Only row 3 switches to non-existent parent; rows 1,2 stay valid.
        // But stmt journal rolls back ALL because FkCheck fires once at end.
        "UPDATE child SET pid = CASE WHEN id = 3 THEN 999 ELSE pid END",
        "SELECT * FROM child ORDER BY id",
    );
    Ok(())
}

/// UPDATE parent with CASCADE — child rows auto-update, no violation.
/// Verifies stmt_journal doesn't interfere with successful CASCADE.
#[turso_macros::test]
fn test_update_fk_cascade_multi_row(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_constraint_case(
        "fk_cascade_multi_row",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE, \
                val TEXT\
             )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'a')",
            "INSERT INTO parent VALUES(2, 'b')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        "UPDATE parent SET id = id + 100",
        "SELECT * FROM child ORDER BY id",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Gap 2: NOT NULL ON CONFLICT REPLACE cascading into other constraints
//
// When a column has `NOT NULL ON CONFLICT REPLACE DEFAULT <val>`, inserting
// NULL substitutes the default. If that default then collides with another
// constraint, the *second* constraint's resolution mode applies.
// ---------------------------------------------------------------------------

/// NOT NULL REPLACE + UNIQUE ABORT (INSERT):
/// Column c is NOT NULL ON CONFLICT REPLACE DEFAULT 'x' with a UNIQUE ON
/// CONFLICT ABORT index. Insert row with c='x', then insert with c=NULL.
/// NOT NULL REPLACE substitutes 'x', which collides with UNIQUE ABORT -> error.
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_abort_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT ABORT\
        )";
    let label = "notnull_replace_cascade_unique_abort_insert";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    // Seed: insert a row with c='x'.
    {
        let sql = "INSERT INTO t VALUES(1, 'x')";
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Insert with c=NULL. NOT NULL REPLACE substitutes default 'x',
    // which collides with existing row's UNIQUE ABORT -> should error.
    let conflict_sql = "INSERT INTO t VALUES(2, NULL)";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + UNIQUE FAIL (INSERT):
/// Multi-row insert where one row's NULL->default causes UNIQUE FAIL.
/// Prior rows should be preserved.
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_fail_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT FAIL\
        )";
    let label = "notnull_replace_cascade_unique_fail_insert";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    // Seed: insert a row with c='x'.
    {
        let sql = "INSERT INTO t VALUES(1, 'x')";
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Multi-row insert: row (2,'y') succeeds, row (3,NULL) -> default 'x' -> FAIL.
    // FAIL semantics: row (2,'y') persists, error at row (3,NULL).
    let conflict_sql = "INSERT INTO t VALUES(2, 'y'), (3, NULL)";
    let _ = limbo_try_exec(&limbo_conn, conflict_sql);
    let _ = sqlite_try_exec(&sqlite_conn, conflict_sql);

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + UNIQUE IGNORE (INSERT):
/// The conflicting row (NULL->default->'x') should be silently skipped.
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_ignore_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT IGNORE\
        )";
    let label = "notnull_replace_cascade_unique_ignore_insert";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    {
        let sql = "INSERT INTO t VALUES(1, 'x')";
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Insert with c=NULL. NOT NULL REPLACE substitutes 'x', which collides
    // with UNIQUE IGNORE -> row silently skipped.
    let conflict_sql = "INSERT INTO t VALUES(2, NULL)";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + UNIQUE ROLLBACK (INSERT):
/// In a transaction, NOT NULL REPLACE substitutes default which collides with
/// UNIQUE ROLLBACK -> entire transaction rolled back.
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_rollback_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT ROLLBACK\
        )";
    let label = "notnull_replace_cascade_unique_rollback_insert";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    {
        let sql = "INSERT INTO t VALUES(1, 'x')";
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Begin transaction, insert a good row, then the conflicting one.
    for sql in ["BEGIN", "INSERT INTO t VALUES(2, 'y')"] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    // Insert with c=NULL -> default 'x' -> UNIQUE ROLLBACK -> txn rolled back.
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(3, NULL)");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(3, NULL)");

    // After ROLLBACK, the transaction is gone. Row (2,'y') should be gone too.
    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + UNIQUE REPLACE (INSERT):
/// Both constraints use REPLACE. NULL gets substituted with default, then
/// the UNIQUE conflict causes REPLACE (delete old row + insert new one).
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_replace_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT REPLACE\
        )";
    let label = "notnull_replace_cascade_unique_replace_insert";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    {
        let sql = "INSERT INTO t VALUES(1, 'x')";
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Insert with c=NULL -> default 'x' -> UNIQUE REPLACE -> old row (1,'x')
    // deleted, new row (2,'x') inserted.
    let conflict_sql = "INSERT INTO t VALUES(2, NULL)";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + PK ABORT (INSERT):
/// Column is INTEGER PRIMARY KEY ON CONFLICT ABORT, another column is
/// NOT NULL ON CONFLICT REPLACE DEFAULT <val> — but the PK value causes ABORT.
#[turso_macros::test]
fn test_notnull_replace_cascade_pk_abort_insert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY ON CONFLICT ABORT, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x'\
        )";
    let label = "notnull_replace_cascade_pk_abort_insert";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'a')",
        "INSERT INTO t VALUES(2, 'b')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Insert with duplicate PK (id=1) and c=NULL. NOT NULL REPLACE substitutes
    // default 'x' for c, but PK conflict fires ABORT -> error.
    let conflict_sql = "INSERT INTO t VALUES(1, NULL)";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + UNIQUE ABORT (UPDATE):
/// Same cascade as test #1 but triggered via UPDATE SET c = NULL.
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_abort_update(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT ABORT\
        )";
    let label = "notnull_replace_cascade_unique_abort_update";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'x')",
        "INSERT INTO t VALUES(2, 'y')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // UPDATE row 2: set c=NULL. NOT NULL REPLACE substitutes 'x',
    // which collides with row 1's UNIQUE ABORT -> should error.
    let conflict_sql = "UPDATE t SET c = NULL WHERE id = 2";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// NOT NULL REPLACE + UNIQUE IGNORE (UPDATE):
/// UPDATE SET c = NULL triggers NOT NULL REPLACE -> default 'x' -> UNIQUE
/// IGNORE -> update silently skipped.
#[turso_macros::test]
fn test_notnull_replace_cascade_unique_ignore_update(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT IGNORE\
        )";
    let label = "notnull_replace_cascade_unique_ignore_update";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    for sql in [
        "INSERT INTO t VALUES(1, 'x')",
        "INSERT INTO t VALUES(2, 'y')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // UPDATE row 2: set c=NULL -> default 'x' -> UNIQUE IGNORE -> skip update.
    let conflict_sql = "UPDATE t SET c = NULL WHERE id = 2";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// Multiple NOT NULL REPLACE columns with different defaults, each default
/// collides with a different UNIQUE constraint. Tests that both cascades
/// are handled correctly.
#[turso_macros::test]
fn test_notnull_replace_cascade_multi_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            a TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'da' UNIQUE ON CONFLICT ABORT, \
            b TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'db' UNIQUE ON CONFLICT ABORT\
        )";
    let label = "notnull_replace_cascade_multi_column";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    // Seed rows whose values match the defaults of the other columns.
    for sql in [
        "INSERT INTO t VALUES(1, 'da', 'b1')",
        "INSERT INTO t VALUES(2, 'a2', 'db')",
    ] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Insert with both columns NULL. NOT NULL REPLACE substitutes 'da' for a
    // (collides with row 1 on UNIQUE ABORT) and 'db' for b (collides with
    // row 2 on UNIQUE ABORT). Should error on whichever ABORT fires first.
    let conflict_sql = "INSERT INTO t VALUES(3, NULL, NULL)";
    if let Err(e) = exec_and_compare(label, &sqlite_conn, &limbo_conn, conflict_sql) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, a, b FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Also test with one NULL at a time to verify each cascade independently.
    // Column a NULL -> 'da' -> conflicts with row 1 ABORT.
    let conflict_sql2 = "INSERT INTO t VALUES(3, NULL, 'b3')";
    if let Err(e) = exec_and_compare(
        &format!("{label}__a_only"),
        &sqlite_conn,
        &limbo_conn,
        conflict_sql2,
    ) {
        panic!("{e}");
    }

    // Column b NULL -> 'db' -> conflicts with row 2 ABORT.
    let conflict_sql3 = "INSERT INTO t VALUES(3, 'a3', NULL)";
    if let Err(e) = exec_and_compare(
        &format!("{label}__b_only"),
        &sqlite_conn,
        &limbo_conn,
        conflict_sql3,
    ) {
        panic!("{e}");
    }

    if let Some(e) = compare_tables(
        &format!("{label}__final"),
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, a, b FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

/// In-transaction ROLLBACK case: INSERT in BEGIN..COMMIT where NOT NULL
/// REPLACE -> UNIQUE ROLLBACK fires. Transaction should be rolled back entirely,
/// including prior successful inserts within the transaction.
#[turso_macros::test]
fn test_notnull_replace_cascade_in_txn_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let ddl = "\
        CREATE TABLE t(\
            id INTEGER PRIMARY KEY, \
            c TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'x' UNIQUE ON CONFLICT ROLLBACK\
        )";
    let label = "notnull_replace_cascade_in_txn_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute(ddl).unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(ddl).unwrap();

    // Seed: one row with the default value.
    {
        let sql = "INSERT INTO t VALUES(1, 'x')";
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Begin transaction, insert good rows, then the cascade-conflicting one.
    for sql in [
        "BEGIN",
        "INSERT INTO t VALUES(2, 'a')",
        "INSERT INTO t VALUES(3, 'b')",
    ] {
        let _ = limbo_try_exec(&limbo_conn, sql);
        let _ = sqlite_try_exec(&sqlite_conn, sql);
    }

    // Insert with c=NULL -> default 'x' -> UNIQUE ROLLBACK -> entire txn rolled back.
    let _ = limbo_try_exec(&limbo_conn, "INSERT INTO t VALUES(4, NULL)");
    let _ = sqlite_try_exec(&sqlite_conn, "INSERT INTO t VALUES(4, NULL)");

    // After ROLLBACK, rows (2,'a') and (3,'b') from the txn should be gone.
    // Only the original seed row (1,'x') should remain.
    if let Some(e) = compare_tables(
        label,
        &sqlite_conn,
        &limbo_conn,
        "SELECT id, c FROM t ORDER BY id",
    ) {
        panic!("{e}");
    }

    // Verify the transaction was actually terminated: BEGIN should succeed.
    if let Err(e) = exec_and_compare(
        &format!("{label}__begin_after_rollback"),
        &sqlite_conn,
        &limbo_conn,
        "BEGIN",
    ) {
        panic!("{e}");
    }

    // Clean up: commit the new (empty) transaction.
    let _ = limbo_try_exec(&limbo_conn, "COMMIT");
    let _ = sqlite_try_exec(&sqlite_conn, "COMMIT");

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// ---------------------------------------------------------------------------
// Gap 4: FK CASCADE interaction with REPLACE constraint resolution
//
// When a PK or UNIQUE REPLACE fires, it deletes the conflicting row. If that
// row is referenced by child tables via foreign keys, the FK action (CASCADE,
// SET NULL, SET DEFAULT, RESTRICT) should fire as part of the deletion.
// These tests verify that Limbo matches SQLite for this interaction.
// ---------------------------------------------------------------------------

/// Helper: run a FK+REPLACE cascade test with multi-table verification.
///
/// Creates fresh databases, enables foreign_keys, runs DDL + seeds, executes
/// `conflict_sql`, compares outcomes (success/failure), compares all tables
/// listed in `verify_queries`, and runs integrity check.
fn run_fk_cascade_replace_case(
    label: &str,
    ddl_stmts: &[&str],
    seed_rows: &[&str],
    conflict_sql: &str,
    verify_queries: &[(&str, &str)], // (table_label, query)
) {
    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();
    for stmt in ddl_stmts {
        limbo_conn.execute(stmt).unwrap();
    }

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn
        .execute_batch("PRAGMA foreign_keys = ON;")
        .unwrap();
    for stmt in ddl_stmts {
        sqlite_conn.execute_batch(stmt).unwrap();
    }

    for sql in seed_rows {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    let limbo_result = limbo_try_exec(&limbo_conn, conflict_sql);
    let sqlite_result = sqlite_try_exec(&sqlite_conn, conflict_sql);

    match (&sqlite_result, &limbo_result) {
        (Ok(()), Err(e)) => panic!("[{label}] SQLite succeeded but Limbo failed: {e}"),
        (Err(e), Ok(())) => panic!("[{label}] Limbo succeeded but SQLite failed: {e}"),
        _ => {}
    }

    for (tbl_label, query) in verify_queries {
        if let Some(e) = compare_tables(
            &format!("{label}__{tbl_label}"),
            &sqlite_conn,
            &limbo_conn,
            query,
        ) {
            panic!("{e}");
        }
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");
}

// ---------------------------------------------------------------------------
// Test 1: PK REPLACE deletes parent -> FK CASCADE deletes child rows (INSERT)
// ---------------------------------------------------------------------------

/// Parent table has `INTEGER PRIMARY KEY ON CONFLICT REPLACE`. Child table
/// references parent via `ON DELETE CASCADE`. Re-inserting an existing PK
/// triggers REPLACE (delete old parent row), which cascades to delete children.
#[turso_macros::test]
fn test_fk_cascade_replace_pk_insert_cascade_delete(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_pk_insert_cascade",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Re-insert parent(1,...) — PK REPLACE deletes old parent(1),
        // FK CASCADE should delete child rows 1 and 2.
        "INSERT INTO parent VALUES(1, 'alice_new')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 2: PK REPLACE deletes parent -> FK SET NULL updates child rows (INSERT)
// ---------------------------------------------------------------------------

/// Same as test 1, but child FK has `ON DELETE SET NULL`. Child's FK column
/// should become NULL when the parent row is replaced.
#[turso_macros::test]
fn test_fk_cascade_replace_pk_insert_set_null(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_pk_insert_set_null",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE SET NULL, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Re-insert parent(1,...) — PK REPLACE deletes old parent(1),
        // FK SET NULL should set child rows 1,2 pid to NULL.
        "INSERT INTO parent VALUES(1, 'alice_new')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 3: PK REPLACE deletes parent -> FK SET DEFAULT updates child rows (INSERT)
// ---------------------------------------------------------------------------

/// Same pattern but child FK has `ON DELETE SET DEFAULT`. Child's FK column
/// should become the DEFAULT value (here: 2, referencing an existing parent).
#[turso_macros::test]
fn test_fk_cascade_replace_pk_insert_set_default(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_pk_insert_set_default",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER DEFAULT 2 REFERENCES parent(id) ON DELETE SET DEFAULT, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Re-insert parent(1,...) — PK REPLACE deletes old parent(1),
        // FK SET DEFAULT should set child rows 1,2 pid to DEFAULT (2).
        "INSERT INTO parent VALUES(1, 'alice_new')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 4: PK REPLACE deletes parent -> FK RESTRICT blocks operation (INSERT)
// ---------------------------------------------------------------------------

/// Same pattern but child FK has `ON DELETE RESTRICT`. The REPLACE should be
/// blocked by RESTRICT — the entire INSERT fails.
#[turso_macros::test]
fn test_fk_cascade_replace_pk_insert_restrict(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_pk_insert_restrict",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE RESTRICT, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Re-insert parent(1,...) — PK REPLACE tries to delete old parent(1),
        // but FK RESTRICT blocks the deletion. INSERT should fail.
        "INSERT INTO parent VALUES(1, 'alice_new')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 5: UNIQUE REPLACE deletes row referenced by FK CASCADE (INSERT)
// ---------------------------------------------------------------------------

/// Table has `UNIQUE(name) ON CONFLICT REPLACE`. Another table references it
/// via FK CASCADE. Inserting a duplicate name causes REPLACE -> delete -> cascade.
#[turso_macros::test]
fn test_fk_cascade_replace_unique_insert_cascade(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_unique_insert_cascade",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY, \
                name TEXT UNIQUE ON CONFLICT REPLACE\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Insert a new row with name='alice' (conflicts with UNIQUE on row 1).
        // REPLACE deletes row 1, FK CASCADE deletes children 1 and 2.
        "INSERT INTO parent VALUES(3, 'alice')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 6: Mixed PK REPLACE + UNIQUE ABORT, INSERT triggers PK REPLACE -> FK cascade
// ---------------------------------------------------------------------------

/// Table has both PK ON CONFLICT REPLACE and UNIQUE ON CONFLICT ABORT.
/// Insert that collides with PK (triggers REPLACE + FK cascade) but NOT with UNIQUE.
#[turso_macros::test]
fn test_fk_cascade_replace_mixed_pk_replace_unique_abort(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_mixed_pk_replace_unique_abort",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT UNIQUE ON CONFLICT ABORT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // Collides on PK (id=1) but NOT on UNIQUE (name='charlie' is fresh).
        // PK REPLACE deletes old parent(1), FK CASCADE deletes children 1,2.
        "INSERT INTO parent VALUES(1, 'charlie')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 7: PK REPLACE -> FK CASCADE (UPDATE)
// ---------------------------------------------------------------------------

/// UPDATE that changes a parent PK to an existing value. PK REPLACE fires,
/// FK CASCADE deletes child rows of the replaced parent.
#[turso_macros::test]
fn test_fk_cascade_replace_pk_update_cascade(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_pk_update_cascade",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO parent VALUES(3, 'charlie')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // UPDATE parent(3) to id=1 — PK REPLACE deletes old parent(1),
        // FK CASCADE deletes children 1 and 2.
        "UPDATE parent SET id = 1 WHERE id = 3",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 8: UNIQUE REPLACE -> FK CASCADE (UPDATE)
// ---------------------------------------------------------------------------

/// UPDATE causes unique collision on parent table. REPLACE deletes the
/// conflicting row, FK CASCADE deletes its children.
#[turso_macros::test]
fn test_fk_cascade_replace_unique_update_cascade(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_unique_update_cascade",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY, \
                name TEXT UNIQUE ON CONFLICT REPLACE\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
                val TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO parent VALUES(3, 'charlie')",
            "INSERT INTO child VALUES(1, 1, 'x')",
            "INSERT INTO child VALUES(2, 1, 'y')",
            "INSERT INTO child VALUES(3, 2, 'z')",
        ],
        // UPDATE parent(3) name to 'alice' — UNIQUE REPLACE deletes parent(1),
        // FK CASCADE deletes children 1 and 2.
        "UPDATE parent SET name = 'alice' WHERE id = 3",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 9: Multi-level cascade (Parent -> Child -> Grandchild)
// ---------------------------------------------------------------------------

/// PK REPLACE on parent triggers CASCADE on child, which cascades to grandchild.
/// Three-level FK chain: parent -> child -> grandchild, all ON DELETE CASCADE.
#[turso_macros::test]
fn test_fk_cascade_replace_multi_level(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);
    run_fk_cascade_replace_case(
        "fk_cascade_replace_multi_level",
        &[
            "CREATE TABLE parent(\
                id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
                name TEXT\
            )",
            "CREATE TABLE child(\
                id INTEGER PRIMARY KEY, \
                pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
                val TEXT\
            )",
            "CREATE TABLE grandchild(\
                id INTEGER PRIMARY KEY, \
                cid INTEGER REFERENCES child(id) ON DELETE CASCADE, \
                info TEXT\
            )",
        ],
        &[
            "INSERT INTO parent VALUES(1, 'alice')",
            "INSERT INTO parent VALUES(2, 'bob')",
            "INSERT INTO child VALUES(1, 1, 'c1')",
            "INSERT INTO child VALUES(2, 1, 'c2')",
            "INSERT INTO child VALUES(3, 2, 'c3')",
            "INSERT INTO grandchild VALUES(1, 1, 'g1')",
            "INSERT INTO grandchild VALUES(2, 1, 'g2')",
            "INSERT INTO grandchild VALUES(3, 2, 'g3')",
            "INSERT INTO grandchild VALUES(4, 3, 'g4')",
        ],
        // Re-insert parent(1,...) — PK REPLACE deletes old parent(1),
        // CASCADE deletes children 1,2 which cascades to grandchildren 1,2,3.
        "INSERT INTO parent VALUES(1, 'alice_new')",
        &[
            ("parent", "SELECT id, name FROM parent ORDER BY id"),
            ("child", "SELECT id, pid, val FROM child ORDER BY id"),
            (
                "grandchild",
                "SELECT id, cid, info FROM grandchild ORDER BY id",
            ),
        ],
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 10: REPLACE cascade + subsequent ABORT in same statement context
// ---------------------------------------------------------------------------

/// In a transaction, REPLACE fires FK cascade (deletes children), then a
/// SUBSEQUENT unique constraint (ABORT) fires on a DIFFERENT column of the
/// SAME statement. The statement journal should roll back the REPLACE + cascade.
/// Verify children are restored.
#[turso_macros::test]
fn test_fk_cascade_replace_then_abort_stmt_rollback(tmp_db: TempDatabase) -> anyhow::Result<()> {
    drop(tmp_db);

    let label = "fk_cascade_replace_then_abort_stmt_rollback";

    let limbo_db = TempDatabase::builder()
        .with_db_name(format!("{label}.db"))
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    limbo_conn.execute("PRAGMA foreign_keys = ON").unwrap();

    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn
        .execute_batch("PRAGMA foreign_keys = ON;")
        .unwrap();

    // Parent has PK REPLACE + a second UNIQUE column with ABORT.
    // Child references parent via FK CASCADE.
    let ddl_stmts = [
        "CREATE TABLE parent(\
            id INTEGER PRIMARY KEY ON CONFLICT REPLACE, \
            name TEXT, \
            code TEXT UNIQUE ON CONFLICT ABORT\
        )",
        "CREATE TABLE child(\
            id INTEGER PRIMARY KEY, \
            pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
            val TEXT\
        )",
    ];
    for stmt in &ddl_stmts {
        limbo_conn.execute(stmt).unwrap();
        sqlite_conn.execute_batch(stmt).unwrap();
    }

    let seed = [
        "INSERT INTO parent VALUES(1, 'alice', 'A')",
        "INSERT INTO parent VALUES(2, 'bob', 'B')",
        "INSERT INTO child VALUES(1, 1, 'x')",
        "INSERT INTO child VALUES(2, 1, 'y')",
        "INSERT INTO child VALUES(3, 2, 'z')",
    ];
    for sql in &seed {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // Start a transaction so we can verify the stmt journal rolls back
    // without terminating the transaction.
    for sql in ["BEGIN", "INSERT INTO parent VALUES(4, 'dave', 'D')"] {
        limbo_conn.execute(sql).unwrap();
        sqlite_conn.execute_batch(sql).unwrap();
    }

    // This INSERT conflicts on PK (id=1 -> REPLACE, triggers FK CASCADE
    // deleting children 1,2) AND on UNIQUE code ('B' -> ABORT).
    // ABORT should fire (non-REPLACE checked before REPLACE), so the PK
    // REPLACE + FK CASCADE should be rolled back by the statement journal.
    let conflict_sql = "INSERT INTO parent VALUES(1, 'new', 'B')";
    let limbo_result = limbo_try_exec(&limbo_conn, conflict_sql);
    let sqlite_result = sqlite_try_exec(&sqlite_conn, conflict_sql);

    match (&sqlite_result, &limbo_result) {
        (Ok(()), Err(e)) => panic!("[{label}] SQLite succeeded but Limbo failed: {e}"),
        (Err(e), Ok(())) => panic!("[{label}] Limbo succeeded but SQLite failed: {e}"),
        _ => {}
    }

    // The transaction should still be active. Commit it.
    limbo_conn.execute("COMMIT").unwrap();
    sqlite_conn.execute_batch("COMMIT").unwrap();

    // Verify: parent should have rows 1,2,4 (1 and 2 unchanged, 4 from
    // the earlier INSERT). Children 1,2,3 should all be restored.
    for (tbl_label, query) in [
        ("parent", "SELECT id, name, code FROM parent ORDER BY id"),
        ("child", "SELECT id, pid, val FROM child ORDER BY id"),
    ] {
        if let Some(e) = compare_tables(
            &format!("{label}__{tbl_label}"),
            &sqlite_conn,
            &limbo_conn,
            query,
        ) {
            panic!("{e}");
        }
    }

    let db_path = limbo_db.path.clone();
    drop(limbo_conn);
    drop(limbo_db);
    let ic = sqlite_integrity_check(&db_path);
    assert_eq!(ic, "ok", "[{label}] integrity_check: {ic}");

    Ok(())
}

// =============================================================================
// Gap 1: Statement-level OR clause overriding DDL-level constraint modes
// =============================================================================

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_replace_uq_abort_or_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR ABORT overrides PK REPLACE + UQ ABORT",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR ABORT INTO t VALUES(1, 'z', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_replace_uq_abort_or_fail(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR FAIL overrides PK REPLACE + UQ ABORT",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR FAIL INTO t VALUES(1, 'z', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_replace_uq_abort_or_ignore(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR IGNORE overrides PK REPLACE + UQ ABORT (pk)",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR IGNORE INTO t VALUES(1, 'z', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_replace_uq_abort_or_ignore_uq(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR IGNORE overrides PK REPLACE + UQ ABORT (uq)",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR IGNORE INTO t VALUES(3, 'x', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_replace_uq_abort_or_replace(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR REPLACE overrides UQ ABORT",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR REPLACE INTO t VALUES(3, 'x', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_abort_uq_replace_or_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR ABORT overrides UQ REPLACE",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR ABORT INTO t VALUES(3, 'x', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_abort_uq_replace_or_ignore(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR IGNORE overrides UQ REPLACE",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR IGNORE INTO t VALUES(3, 'x', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_pk_abort_uq_replace_or_replace_pk(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR REPLACE overrides PK ABORT",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR REPLACE INTO t VALUES(1, 'z', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_pk_replace_uq_abort_or_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR ABORT overrides PK REPLACE",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR ABORT t SET a = 1 WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_pk_replace_uq_abort_or_ignore(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR IGNORE overrides PK REPLACE (pk)",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR IGNORE t SET a = 1 WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_pk_replace_uq_abort_or_replace_uq(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR REPLACE overrides UQ ABORT (uq)",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR REPLACE t SET b = 'x' WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_pk_abort_uq_replace_or_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR ABORT overrides UQ REPLACE",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR ABORT t SET b = 'x' WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_pk_abort_uq_replace_or_ignore(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR IGNORE overrides UQ REPLACE",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR IGNORE t SET b = 'x' WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_pk_abort_uq_replace_or_replace_pk(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR REPLACE overrides PK ABORT",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR REPLACE t SET a = 1 WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_multirow_insert_or_fail_uq_replace(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("multirow_or_fail_uq_replace.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)",
        "INSERT INTO t VALUES(1, 'x', 'original')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let conflict_sql = "INSERT OR FAIL INTO t VALUES(2, 'y', 'new'), (3, 'x', 'conflict')";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "multi-row OR FAIL overrides UQ REPLACE",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_mixed_pk_replace_uq_ignore_or_abort_pk(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR ABORT flattens PK REPLACE + UQ IGNORE (pk)",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT IGNORE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR ABORT INTO t VALUES(1, 'z', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_mixed_pk_replace_uq_ignore_or_abort_uq(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR ABORT flattens PK REPLACE + UQ IGNORE (uq)",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT IGNORE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR ABORT INTO t VALUES(3, 'x', 'third')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_or_replace_dual_conflict(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR REPLACE with dual PK+UQ conflict",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "INSERT OR REPLACE INTO t VALUES(1, 'y', 'replaced')",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_update_or_replace_dual_conflict(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "UPDATE OR REPLACE with dual PK+UQ conflict",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT ABORT, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')", "INSERT INTO t VALUES(3, 'z', 'third')"],
        "UPDATE OR REPLACE t SET a = 1, b = 'y' WHERE a = 3",
        "SELECT * FROM t ORDER BY a",
    );
}

#[turso_macros::test]
fn test_statement_or_overrides_ddl_insert_or_ignore_dual_conflict(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "INSERT OR IGNORE overrides DDL REPLACE — silently skips",
        &["CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)"],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "INSERT OR IGNORE INTO t VALUES(1, 'y', 'ignored')",
        "SELECT * FROM t ORDER BY a",
    );
}

// ---------------------------------------------------------------------------
// Gap 3: BEFORE trigger modifying SET values causing constraint violation
// ---------------------------------------------------------------------------

/// BEFORE INSERT trigger modifies NEW.b to a value that violates UNIQUE ABORT.
/// The trigger fires after the INSERT values are set but before constraint checks.
#[turso_macros::test]
fn test_trigger_before_insert_causes_unique_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE INSERT trigger causes UNIQUE ABORT",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE ON CONFLICT ABORT)",
            "CREATE TRIGGER tr_before_ins BEFORE INSERT ON t BEGIN UPDATE t SET b = NEW.b WHERE 0; END",
            // Trigger that overwrites NEW.b to collide:
            "CREATE TRIGGER tr_rewrite BEFORE INSERT ON t BEGIN SELECT RAISE(ABORT, 'trigger-abort') WHERE NEW.b = 'collide'; END",
        ],
        &["INSERT INTO t VALUES(1, 'x')"],
        "INSERT INTO t VALUES(2, 'collide')",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE UPDATE trigger causes NOT NULL violation by setting a column to NULL.
#[turso_macros::test]
fn test_trigger_before_update_sets_null_notnull_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE UPDATE trigger RAISE ABORT on NOT NULL",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL, c TEXT)",
            "CREATE TRIGGER tr_chk BEFORE UPDATE ON t BEGIN SELECT RAISE(ABORT, 'notnull-trigger') WHERE NEW.b IS NULL; END",
        ],
        &["INSERT INTO t VALUES(1, 'x', 'data')", "INSERT INTO t VALUES(2, 'y', 'data2')"],
        "UPDATE t SET b = NULL WHERE a = 1",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE UPDATE trigger fires RAISE(ABORT) on a multi-row UPDATE.
/// Statement journal must roll back the partial changes.
#[turso_macros::test]
fn test_trigger_before_update_raise_abort_multirow(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE UPDATE RAISE(ABORT) multi-row rollback",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c TEXT)",
            "CREATE TRIGGER tr_abort BEFORE UPDATE ON t BEGIN SELECT RAISE(ABORT, 'stop') WHERE NEW.a = 3; END",
        ],
        &["INSERT INTO t VALUES(1, 'a', '1')", "INSERT INTO t VALUES(2, 'b', '2')", "INSERT INTO t VALUES(3, 'c', '3')"],
        "UPDATE t SET c = 'updated'",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE DELETE trigger fires RAISE(ABORT) on multi-row DELETE.
#[turso_macros::test]
fn test_trigger_before_delete_raise_abort_multirow(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE DELETE RAISE(ABORT) multi-row rollback",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "CREATE TRIGGER tr_del_abort BEFORE DELETE ON t BEGIN SELECT RAISE(ABORT, 'no-delete') WHERE OLD.a = 2; END",
        ],
        &["INSERT INTO t VALUES(1, 'a')", "INSERT INTO t VALUES(2, 'b')", "INSERT INTO t VALUES(3, 'c')"],
        "DELETE FROM t",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE INSERT trigger with RAISE(IGNORE) — should skip the conflicting row silently.
#[turso_macros::test]
fn test_trigger_before_insert_raise_ignore(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE INSERT RAISE(IGNORE) skips row",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "CREATE TRIGGER tr_ign BEFORE INSERT ON t BEGIN SELECT RAISE(IGNORE) WHERE NEW.a = 2; END",
        ],
        &["INSERT INTO t VALUES(1, 'first')"],
        "INSERT INTO t VALUES(2, 'ignored')",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE INSERT trigger with RAISE(FAIL) — partial rows committed.
#[turso_macros::test]
fn test_trigger_before_insert_raise_fail_multirow(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("trigger_raise_fail.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
        "CREATE TRIGGER tr_fail BEFORE INSERT ON t BEGIN SELECT RAISE(FAIL, 'stop-here') WHERE NEW.a = 3; END",
        "INSERT INTO t VALUES(1, 'first')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // Multi-row insert: rows 2 succeeds, row 3 fails (FAIL keeps prior changes)
    let conflict_sql = "INSERT INTO t VALUES(2, 'second'), (3, 'third'), (4, 'fourth')";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "trigger RAISE(FAIL) multi-row",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// BEFORE UPDATE trigger + UNIQUE ON CONFLICT REPLACE: trigger fires, then REPLACE
/// deletes conflicting row.
#[turso_macros::test]
fn test_trigger_before_update_with_unique_replace(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE UPDATE trigger + UNIQUE REPLACE",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE ON CONFLICT REPLACE, c TEXT)",
            "CREATE TRIGGER tr_log BEFORE UPDATE ON t BEGIN INSERT INTO log VALUES(OLD.a, NEW.b); END",
            "CREATE TABLE log(old_a INTEGER, new_b TEXT)",
        ],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "UPDATE t SET b = 'x' WHERE a = 2",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE INSERT trigger + IPK ON CONFLICT REPLACE: trigger fires before constraint
/// resolution replaces the existing row.
#[turso_macros::test]
fn test_trigger_before_insert_with_ipk_replace(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE INSERT trigger + IPK REPLACE",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT)",
            "CREATE TRIGGER tr_ins_log BEFORE INSERT ON t BEGIN INSERT INTO log VALUES(NEW.a, NEW.b); END",
            "CREATE TABLE log(new_a INTEGER, new_b TEXT)",
        ],
        &["INSERT INTO t VALUES(1, 'original')"],
        "INSERT INTO t VALUES(1, 'replaced')",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE UPDATE trigger + OR IGNORE statement clause: trigger fires, then constraint
/// violation handled by IGNORE (skip the row).
#[turso_macros::test]
fn test_trigger_before_update_or_ignore_unique(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE UPDATE trigger + OR IGNORE",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c TEXT)",
            "CREATE TRIGGER tr_upd BEFORE UPDATE ON t BEGIN INSERT INTO log VALUES(OLD.a, NEW.b); END",
            "CREATE TABLE log(old_a INTEGER, new_b TEXT)",
        ],
        &["INSERT INTO t VALUES(1, 'x', 'first')", "INSERT INTO t VALUES(2, 'y', 'second')"],
        "UPDATE OR IGNORE t SET b = 'x' WHERE a = 2",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE INSERT trigger fires RAISE(ROLLBACK) inside an explicit transaction.
/// The entire transaction should be rolled back.
#[turso_macros::test]
fn test_trigger_before_insert_raise_rollback_in_txn(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("trigger_raise_rollback_txn.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
        "CREATE TRIGGER tr_rb BEFORE INSERT ON t BEGIN SELECT RAISE(ROLLBACK, 'roll-it-back') WHERE NEW.a = 3; END",
        "INSERT INTO t VALUES(1, 'committed')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // Start explicit transaction, insert some rows, then trigger ROLLBACK
    for s in ["BEGIN", "INSERT INTO t VALUES(2, 'in-txn')"] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let conflict_sql = "INSERT INTO t VALUES(3, 'triggers-rollback')";
    let sqlite_err = sqlite_try_exec(&sqlite_conn, conflict_sql);
    let limbo_err = limbo_try_exec(&limbo_conn, conflict_sql);
    assert!(sqlite_err.is_err(), "SQLite should fail");
    assert!(limbo_err.is_err(), "Limbo should fail");
    // After ROLLBACK, the transaction is gone — row 2 should not exist
    let diff = compare_tables(
        "trigger RAISE(ROLLBACK) in txn",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// BEFORE UPDATE trigger on a table with mixed DDL modes (PK REPLACE + UQ ABORT).
/// Trigger fires RAISE(ABORT) conditionally, interacting with the three-phase architecture.
#[turso_macros::test]
fn test_trigger_before_update_mixed_ddl_raise_abort(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE UPDATE trigger + mixed DDL (PK REPLACE + UQ ABORT) RAISE(ABORT)",
        &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b TEXT UNIQUE ON CONFLICT ABORT, c TEXT)",
            "CREATE TRIGGER tr_mixed BEFORE UPDATE ON t BEGIN SELECT RAISE(ABORT, 'trigger-blocked') WHERE NEW.c = 'blocked'; END",
        ],
        &["INSERT INTO t VALUES(1, 'x', 'ok')", "INSERT INTO t VALUES(2, 'y', 'ok')"],
        "UPDATE t SET c = 'blocked' WHERE a = 1",
        "SELECT * FROM t ORDER BY a",
    );
}

/// BEFORE DELETE trigger + FK constraint: trigger fires before the FK check.
#[turso_macros::test]
fn test_trigger_before_delete_with_fk(tmp_db: TempDatabase) {
    drop(tmp_db);
    run_fk_constraint_case(
        "BEFORE DELETE trigger + FK violation",
        &[
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
            "CREATE TRIGGER tr_del_parent BEFORE DELETE ON parent BEGIN SELECT RAISE(ABORT, 'trigger-says-no') WHERE OLD.id = 1; END",
        ],
        &["INSERT INTO parent VALUES(1, 'p1')", "INSERT INTO parent VALUES(2, 'p2')", "INSERT INTO child VALUES(1, 1)", "INSERT INTO child VALUES(2, 2)"],
        "DELETE FROM parent WHERE id = 1",
        "SELECT * FROM parent ORDER BY id",
    );
}

/// Multi-row UPDATE where BEFORE trigger changes a column that would violate
/// a UNIQUE constraint on a different row. Tests three-phase constraint check ordering.
#[turso_macros::test]
fn test_trigger_before_update_changes_value_unique_conflict(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("trigger_chg_unique.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c INT)",
        // This trigger modifies c to track that it fired; it does NOT change b
        "CREATE TRIGGER tr_mark BEFORE UPDATE ON t BEGIN UPDATE t SET c = c + 100 WHERE a = OLD.a; END",
        "INSERT INTO t VALUES(1, 'x', 0)",
        "INSERT INTO t VALUES(2, 'y', 0)",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // This UPDATE should cause a UNIQUE violation because b='x' already exists on row 1
    let conflict_sql = "UPDATE t SET b = 'x' WHERE a = 2";
    let sqlite_err = sqlite_try_exec(&sqlite_conn, conflict_sql);
    let limbo_err = limbo_try_exec(&limbo_conn, conflict_sql);
    match (&sqlite_err, &limbo_err) {
        (Ok(()), Err(e)) => panic!("SQLite ok, Limbo failed: {e}"),
        (Err(e), Ok(())) => panic!("Limbo ok, SQLite failed: {e}"),
        _ => {}
    }
    let diff = compare_tables(
        "trigger changes value + unique conflict",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

// ---------------------------------------------------------------------------
// Gap 5: Autocommit FAIL semantics with partial row changes
// ---------------------------------------------------------------------------

/// INSERT OR FAIL in autocommit mode: rows before the failing row are committed.
#[turso_macros::test]
fn test_autocommit_fail_insert_partial_committed(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("autocommit_fail_insert.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE)",
        "INSERT INTO t VALUES(1, 'existing')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // Row (2,'new') succeeds, row (3,'existing') fails on UNIQUE — prior rows committed
    let conflict_sql = "INSERT OR FAIL INTO t VALUES(2, 'new'), (3, 'existing')";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "autocommit FAIL INSERT partial",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// INSERT OR FAIL in explicit transaction: partial rows survive within the transaction.
#[turso_macros::test]
fn test_explicit_txn_fail_insert_partial_in_txn(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("txn_fail_insert.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE)",
        "INSERT INTO t VALUES(1, 'existing')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    {
        let s = "BEGIN";
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let conflict_sql = "INSERT OR FAIL INTO t VALUES(2, 'new'), (3, 'existing')";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    // Row 2 should be visible within the transaction (FAIL keeps prior changes)
    let diff = compare_tables(
        "txn FAIL INSERT partial (before commit)",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    {
        let s = "COMMIT";
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let diff = compare_tables(
        "txn FAIL INSERT partial (after commit)",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// UPDATE OR FAIL in autocommit mode: partial row updates are committed.
#[turso_macros::test]
fn test_autocommit_fail_update_partial_committed(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("autocommit_fail_update.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c TEXT)",
        "INSERT INTO t VALUES(1, 'x', 'first')",
        "INSERT INTO t VALUES(2, 'y', 'second')",
        "INSERT INTO t VALUES(3, 'z', 'third')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // UPDATE sets b='x' for all rows — row 1 is fine (already 'x'), row 2 conflicts
    let conflict_sql = "UPDATE OR FAIL t SET b = 'x'";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "autocommit FAIL UPDATE partial",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// UPDATE OR FAIL in explicit transaction: partial updates survive, transaction can commit.
#[turso_macros::test]
fn test_explicit_txn_fail_update_partial_in_txn(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("txn_fail_update.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c TEXT)",
        "INSERT INTO t VALUES(1, 'x', 'first')",
        "INSERT INTO t VALUES(2, 'y', 'second')",
        "INSERT INTO t VALUES(3, 'z', 'third')",
        "BEGIN",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let conflict_sql = "UPDATE OR FAIL t SET b = 'x'";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    {
        let s = "COMMIT";
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let diff = compare_tables(
        "txn FAIL UPDATE (after commit)",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// INSERT OR FAIL with PK conflict in autocommit: partial rows committed.
#[turso_macros::test]
fn test_autocommit_fail_insert_pk_conflict(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("autocommit_fail_pk.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
        "INSERT INTO t VALUES(1, 'existing')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    let conflict_sql = "INSERT OR FAIL INTO t VALUES(2, 'ok'), (1, 'dup-pk'), (3, 'never')";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "autocommit FAIL INSERT PK partial",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// UPDATE OR FAIL with NOT NULL violation in autocommit mode.
#[turso_macros::test]
fn test_autocommit_fail_update_notnull(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("autocommit_fail_notnull.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL, c TEXT)",
        "INSERT INTO t VALUES(1, 'x', 'first')",
        "INSERT INTO t VALUES(2, 'y', 'second')",
        "INSERT INTO t VALUES(3, 'z', 'third')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // NOT NULL violation on row 2 (set b=NULL conditionally)
    let conflict_sql = "UPDATE OR FAIL t SET b = CASE WHEN a = 2 THEN NULL ELSE b END";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "autocommit FAIL UPDATE NOT NULL",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// INSERT OR FAIL with mixed PK and UNIQUE conflicts: first conflict stops execution.
#[turso_macros::test]
fn test_autocommit_fail_insert_mixed_constraints(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("autocommit_fail_mixed.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE, c TEXT)",
        "INSERT INTO t VALUES(1, 'x', 'first')",
        "INSERT INTO t VALUES(2, 'y', 'second')",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // Row (3,'z','ok') succeeds, row (4,'x','dup') fails on UNIQUE b='x'
    let conflict_sql = "INSERT OR FAIL INTO t VALUES(3, 'z', 'ok'), (4, 'x', 'dup')";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "autocommit FAIL INSERT mixed constraints",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}

/// UPDATE OR FAIL with CHECK constraint violation in autocommit mode.
#[turso_macros::test]
fn test_autocommit_fail_update_check_constraint(tmp_db: TempDatabase) {
    drop(tmp_db);
    let limbo_db = TempDatabase::builder()
        .with_db_name("autocommit_fail_check.db")
        .build();
    let limbo_conn = limbo_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER CHECK(b > 0))",
        "INSERT INTO t VALUES(1, 10)",
        "INSERT INTO t VALUES(2, 20)",
        "INSERT INTO t VALUES(3, 30)",
    ] {
        sqlite_try_exec(&sqlite_conn, s).unwrap();
        limbo_try_exec(&limbo_conn, s).unwrap();
    }
    // Set b to negative for row where a=2 — CHECK violation
    let conflict_sql = "UPDATE OR FAIL t SET b = CASE WHEN a = 2 THEN -1 ELSE b END";
    assert!(sqlite_try_exec(&sqlite_conn, conflict_sql).is_err());
    assert!(limbo_try_exec(&limbo_conn, conflict_sql).is_err());
    let diff = compare_tables(
        "autocommit FAIL UPDATE CHECK",
        &sqlite_conn,
        &limbo_conn,
        "SELECT * FROM t ORDER BY a",
    );
    assert!(diff.is_none(), "{diff:?}");
    let ic = sqlite_integrity_check(&limbo_db.path);
    assert_eq!(ic, "ok");
}
