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
