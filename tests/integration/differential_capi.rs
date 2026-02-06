//! Differential tests comparing Turso C API behavior with SQLite.
//!
//! These tests prepare identical SQL statements against both Turso (via turso_core)
//! and SQLite (via rusqlite), then compare the results of API calls like
//! `stmt_readonly` and `stmt_isexplain` to ensure compatibility.
//!
//! ## Known Differences
//!
//! Some behaviors differ from SQLite and are documented here:
//!
//! 1. **DDL statements (CREATE TABLE, CREATE INDEX, etc.)**: In SQLite, these are NOT
//!    readonly. In Turso, they are currently marked as readonly because `change_cnt_on`
//!    is not set for DDL. This is a known compatibility issue tracked in separate tests.
//!
//! 2. **Transaction lock modes (BEGIN IMMEDIATE, BEGIN EXCLUSIVE)**: SQLite marks these
//!    as NOT readonly while Turso marks them as readonly. See tests with `_known_difference` suffix.
//!
//! 3. **PRAGMA wal_checkpoint**: SQLite marks this as NOT readonly, Turso marks it as readonly.
//!
//! 4. **ANALYZE**: SQLite marks this as NOT readonly, Turso marks it as readonly.
//!
//! 5. **Unsupported features**: VACUUM and REINDEX are not yet supported in Turso.

use crate::common::TempDatabase;
use tempfile::TempDir;

/// Struct to hold both Turso and SQLite connections for differential testing.
struct DifferentialTestDb {
    turso_db: TempDatabase,
    sqlite_conn: rusqlite::Connection,
}

impl DifferentialTestDb {
    fn new() -> Self {
        // Create SQLite database
        let sqlite_path = TempDir::new().unwrap().keep().join("sqlite_test.db");
        let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        sqlite_conn
            .pragma_update(None, "journal_mode", "wal")
            .unwrap();

        // Create Turso database
        let turso_db = TempDatabase::new_empty();

        Self {
            turso_db,
            sqlite_conn,
        }
    }

    fn new_with_table() -> Self {
        let db = Self::new();

        // Create the same table in both databases
        db.sqlite_conn
            .execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
                [],
            )
            .unwrap();

        let turso_conn = db.turso_db.connect_limbo();
        turso_conn
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)")
            .unwrap();

        db
    }
}

/// Helper to test stmt_readonly differential behavior
fn assert_readonly_matches(db: &DifferentialTestDb, sql: &str, description: &str) {
    // Prepare statement on SQLite
    let sqlite_stmt = db.sqlite_conn.prepare(sql).unwrap();
    let sqlite_readonly = sqlite_stmt.readonly();

    // Prepare statement on Turso
    let turso_conn = db.turso_db.connect_limbo();
    let turso_stmt = turso_conn.prepare(sql).unwrap();
    let turso_readonly = turso_stmt.is_readonly();

    assert_eq!(
        turso_readonly, sqlite_readonly,
        "stmt_readonly mismatch for {description}: SQL=\"{sql}\", turso={turso_readonly}, sqlite={sqlite_readonly}"
    );
}

/// Helper to test stmt_isexplain differential behavior
fn assert_isexplain_matches(db: &DifferentialTestDb, sql: &str, description: &str) {
    // Prepare statement on SQLite
    let sqlite_stmt = db.sqlite_conn.prepare(sql).unwrap();
    let sqlite_isexplain = sqlite_stmt.is_explain();

    // Prepare statement on Turso
    let turso_conn = db.turso_db.connect_limbo();
    let turso_stmt = turso_conn.prepare(sql).unwrap();
    let turso_isexplain = turso_stmt.is_explain();

    assert_eq!(
        turso_isexplain, sqlite_isexplain,
        "stmt_isexplain mismatch for {description}: SQL=\"{sql}\", turso={turso_isexplain}, sqlite={sqlite_isexplain}"
    );
}

/// Helper to document known readonly differences
/// Returns (turso_value, sqlite_value) for documentation purposes
fn document_readonly_difference(db: &DifferentialTestDb, sql: &str) -> (bool, bool) {
    let sqlite_stmt = db.sqlite_conn.prepare(sql).unwrap();
    let sqlite_readonly = sqlite_stmt.readonly();

    let turso_conn = db.turso_db.connect_limbo();
    let turso_stmt = turso_conn.prepare(sql).unwrap();
    let turso_readonly = turso_stmt.is_readonly();

    (turso_readonly, sqlite_readonly)
}

// =============================================================================
// stmt_readonly differential tests - PASSING (compatible behaviors)
// =============================================================================

#[test]
fn test_differential_readonly_select() {
    let db = DifferentialTestDb::new_with_table();

    // Basic SELECT statements should be read-only
    assert_readonly_matches(&db, "SELECT * FROM test", "basic SELECT");
    assert_readonly_matches(&db, "SELECT * FROM test WHERE id = 1", "SELECT with WHERE");
    assert_readonly_matches(
        &db,
        "SELECT id, name FROM test ORDER BY id",
        "SELECT with ORDER BY",
    );
    assert_readonly_matches(
        &db,
        "SELECT COUNT(*) FROM test",
        "SELECT with aggregate function",
    );
    assert_readonly_matches(&db, "SELECT * FROM test LIMIT 10", "SELECT with LIMIT");
    assert_readonly_matches(
        &db,
        "SELECT * FROM test WHERE id IN (1, 2, 3)",
        "SELECT with IN clause",
    );
}

#[test]
fn test_differential_readonly_insert() {
    let db = DifferentialTestDb::new_with_table();

    // INSERT statements should NOT be read-only
    assert_readonly_matches(
        &db,
        "INSERT INTO test (name, value) VALUES ('test', 1.0)",
        "basic INSERT",
    );
    assert_readonly_matches(
        &db,
        "INSERT INTO test DEFAULT VALUES",
        "INSERT DEFAULT VALUES",
    );
    assert_readonly_matches(
        &db,
        "INSERT OR REPLACE INTO test (id, name) VALUES (1, 'replace')",
        "INSERT OR REPLACE",
    );
    assert_readonly_matches(
        &db,
        "INSERT OR IGNORE INTO test (id, name) VALUES (1, 'ignore')",
        "INSERT OR IGNORE",
    );
}

#[test]
fn test_differential_readonly_update() {
    let db = DifferentialTestDb::new_with_table();

    // UPDATE statements should NOT be read-only
    assert_readonly_matches(&db, "UPDATE test SET name = 'updated'", "basic UPDATE");
    assert_readonly_matches(
        &db,
        "UPDATE test SET name = 'updated' WHERE id = 1",
        "UPDATE with WHERE",
    );
    assert_readonly_matches(
        &db,
        "UPDATE OR REPLACE test SET name = 'updated'",
        "UPDATE OR REPLACE",
    );
}

#[test]
fn test_differential_readonly_delete() {
    let db = DifferentialTestDb::new_with_table();

    // DELETE statements should NOT be read-only
    assert_readonly_matches(&db, "DELETE FROM test", "basic DELETE");
    assert_readonly_matches(&db, "DELETE FROM test WHERE id = 1", "DELETE with WHERE");
}

#[test]
fn test_differential_readonly_basic_transaction_control() {
    let db = DifferentialTestDb::new_with_table();

    // These transaction control statements are read-only in both
    assert_readonly_matches(&db, "BEGIN", "BEGIN");
    assert_readonly_matches(&db, "BEGIN DEFERRED", "BEGIN DEFERRED");
    assert_readonly_matches(&db, "COMMIT", "COMMIT");
    assert_readonly_matches(&db, "ROLLBACK", "ROLLBACK");
}

#[test]
fn test_differential_readonly_pragma() {
    let db = DifferentialTestDb::new_with_table();

    // PRAGMA queries should typically be read-only
    assert_readonly_matches(&db, "PRAGMA table_info(test)", "PRAGMA table_info");
    assert_readonly_matches(&db, "PRAGMA database_list", "PRAGMA database_list");
    assert_readonly_matches(&db, "PRAGMA page_count", "PRAGMA page_count");
    assert_readonly_matches(&db, "PRAGMA page_size", "PRAGMA page_size");
}

#[test]
fn test_differential_readonly_subqueries() {
    let db = DifferentialTestDb::new_with_table();

    // Subqueries in SELECT should be read-only
    assert_readonly_matches(
        &db,
        "SELECT * FROM test WHERE id IN (SELECT id FROM test WHERE id > 0)",
        "SELECT with subquery",
    );
    assert_readonly_matches(
        &db,
        "SELECT (SELECT COUNT(*) FROM test) AS cnt",
        "SELECT with scalar subquery",
    );
}

#[test]
fn test_differential_readonly_expressions() {
    let db = DifferentialTestDb::new();

    // Pure expression SELECTs should be read-only
    assert_readonly_matches(&db, "SELECT 1", "SELECT literal");
    assert_readonly_matches(&db, "SELECT 1 + 2", "SELECT expression");
    assert_readonly_matches(&db, "SELECT 'hello'", "SELECT string literal");
    assert_readonly_matches(&db, "SELECT NULL", "SELECT NULL");
    assert_readonly_matches(&db, "SELECT typeof(1)", "SELECT typeof");
    assert_readonly_matches(&db, "SELECT abs(-5)", "SELECT abs");
    assert_readonly_matches(&db, "SELECT coalesce(NULL, 1)", "SELECT coalesce");
}

#[test]
fn test_differential_readonly_with_cte() {
    let db = DifferentialTestDb::new_with_table();

    // CTE (Common Table Expression) with SELECT should be read-only
    assert_readonly_matches(
        &db,
        "WITH cte AS (SELECT * FROM test) SELECT * FROM cte",
        "CTE SELECT",
    );
}

#[test]
fn test_differential_readonly_union() {
    let db = DifferentialTestDb::new_with_table();

    // UNION queries should be read-only
    assert_readonly_matches(
        &db,
        "SELECT id FROM test UNION SELECT id FROM test",
        "UNION",
    );
    assert_readonly_matches(
        &db,
        "SELECT id FROM test UNION ALL SELECT id FROM test",
        "UNION ALL",
    );
    assert_readonly_matches(
        &db,
        "SELECT id FROM test INTERSECT SELECT id FROM test",
        "INTERSECT",
    );
    assert_readonly_matches(
        &db,
        "SELECT id FROM test EXCEPT SELECT id FROM test",
        "EXCEPT",
    );
}

// =============================================================================
// stmt_readonly - KNOWN DIFFERENCES (documented for future fixes)
// =============================================================================

/// Tests for known differences between Turso and SQLite stmt_readonly behavior.
/// These tests document the differences rather than assert equality.
/// TODO: Fix CREATE TABLE to match SQLite behavior.
#[test]
fn test_differential_readonly_ddl_known_difference() {
    let db = DifferentialTestDb::new_with_table();

    // CREATE TABLE: SQLite=false (NOT readonly), Turso=true (readonly)
    // This is a known bug: CREATE TABLE modifies the database but doesn't set change_cnt_on
    let (turso, sqlite) = document_readonly_difference(&db, "CREATE TABLE new_table (id INTEGER)");
    assert!(
        turso,
        "Turso should currently mark CREATE TABLE as readonly (known bug)"
    );
    assert!(
        !sqlite,
        "SQLite correctly marks CREATE TABLE as NOT readonly"
    );
}

#[test]
fn test_differential_readonly_ddl_create_index() {
    let db = DifferentialTestDb::new_with_table();

    // CREATE INDEX: Both correctly mark as NOT readonly
    assert_readonly_matches(
        &db,
        "CREATE INDEX idx_test_name ON test (name)",
        "CREATE INDEX",
    );
    assert_readonly_matches(
        &db,
        "CREATE UNIQUE INDEX idx_test_id ON test (id)",
        "CREATE UNIQUE INDEX",
    );
}

#[test]
fn test_differential_readonly_transaction_lock_modes_known_difference() {
    let db = DifferentialTestDb::new_with_table();

    // BEGIN IMMEDIATE/EXCLUSIVE: SQLite=false, Turso=true
    // SQLite marks these as NOT readonly because they acquire locks
    let (turso, sqlite) = document_readonly_difference(&db, "BEGIN IMMEDIATE");
    assert!(turso, "Turso currently marks BEGIN IMMEDIATE as readonly");
    assert!(
        !sqlite,
        "SQLite correctly marks BEGIN IMMEDIATE as NOT readonly"
    );

    let (turso, sqlite) = document_readonly_difference(&db, "BEGIN EXCLUSIVE");
    assert!(turso, "Turso currently marks BEGIN EXCLUSIVE as readonly");
    assert!(
        !sqlite,
        "SQLite correctly marks BEGIN EXCLUSIVE as NOT readonly"
    );
}

#[test]
fn test_differential_readonly_wal_checkpoint_known_difference() {
    let db = DifferentialTestDb::new_with_table();

    // PRAGMA wal_checkpoint: SQLite=false, Turso=true
    // Checkpointing modifies the database file (copies WAL to main file)
    let (turso, sqlite) = document_readonly_difference(&db, "PRAGMA wal_checkpoint");
    assert!(turso, "Turso currently marks wal_checkpoint as readonly");
    assert!(
        !sqlite,
        "SQLite correctly marks wal_checkpoint as NOT readonly"
    );

    let (turso, sqlite) = document_readonly_difference(&db, "PRAGMA wal_checkpoint(TRUNCATE)");
    assert!(
        turso,
        "Turso currently marks wal_checkpoint(TRUNCATE) as readonly"
    );
    assert!(
        !sqlite,
        "SQLite correctly marks wal_checkpoint(TRUNCATE) as NOT readonly"
    );
}

#[test]
fn test_differential_readonly_analyze_known_difference() {
    let db = DifferentialTestDb::new_with_table();

    // ANALYZE: SQLite=false, Turso=true
    // ANALYZE updates sqlite_stat tables
    let (turso, sqlite) = document_readonly_difference(&db, "ANALYZE");
    assert!(turso, "Turso currently marks ANALYZE as readonly");
    assert!(!sqlite, "SQLite correctly marks ANALYZE as NOT readonly");

    let (turso, sqlite) = document_readonly_difference(&db, "ANALYZE test");
    assert!(turso, "Turso currently marks ANALYZE table as readonly");
    assert!(
        !sqlite,
        "SQLite correctly marks ANALYZE table as NOT readonly"
    );
}

// =============================================================================
// stmt_isexplain differential tests - ALL PASSING
// =============================================================================

#[test]
fn test_differential_isexplain_normal() {
    let db = DifferentialTestDb::new_with_table();

    // Normal statements should return 0
    assert_isexplain_matches(&db, "SELECT * FROM test", "normal SELECT");
    assert_isexplain_matches(
        &db,
        "INSERT INTO test (name) VALUES ('test')",
        "normal INSERT",
    );
    assert_isexplain_matches(&db, "UPDATE test SET name = 'updated'", "normal UPDATE");
    assert_isexplain_matches(&db, "DELETE FROM test", "normal DELETE");
    assert_isexplain_matches(&db, "BEGIN", "normal BEGIN");
    assert_isexplain_matches(&db, "COMMIT", "normal COMMIT");
}

#[test]
fn test_differential_isexplain_explain() {
    let db = DifferentialTestDb::new_with_table();

    // EXPLAIN statements should return 1
    assert_isexplain_matches(&db, "EXPLAIN SELECT * FROM test", "EXPLAIN SELECT");
    assert_isexplain_matches(
        &db,
        "EXPLAIN INSERT INTO test (name) VALUES ('test')",
        "EXPLAIN INSERT",
    );
    assert_isexplain_matches(
        &db,
        "EXPLAIN UPDATE test SET name = 'updated'",
        "EXPLAIN UPDATE",
    );
    assert_isexplain_matches(&db, "EXPLAIN DELETE FROM test", "EXPLAIN DELETE");
}

#[test]
fn test_differential_isexplain_explain_query_plan() {
    let db = DifferentialTestDb::new_with_table();

    // EXPLAIN QUERY PLAN statements should return 2
    assert_isexplain_matches(
        &db,
        "EXPLAIN QUERY PLAN SELECT * FROM test",
        "EXPLAIN QUERY PLAN SELECT",
    );
    assert_isexplain_matches(
        &db,
        "EXPLAIN QUERY PLAN INSERT INTO test (name) VALUES ('test')",
        "EXPLAIN QUERY PLAN INSERT",
    );
    assert_isexplain_matches(
        &db,
        "EXPLAIN QUERY PLAN UPDATE test SET name = 'updated'",
        "EXPLAIN QUERY PLAN UPDATE",
    );
    assert_isexplain_matches(
        &db,
        "EXPLAIN QUERY PLAN DELETE FROM test",
        "EXPLAIN QUERY PLAN DELETE",
    );
}

#[test]
fn test_differential_isexplain_with_joins() {
    // Create database with multiple tables
    let db = DifferentialTestDb::new();

    db.sqlite_conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .unwrap();
    db.sqlite_conn
        .execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            [],
        )
        .unwrap();

    let turso_conn = db.turso_db.connect_limbo();
    turso_conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    turso_conn
        .execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
        .unwrap();
    drop(turso_conn);

    // Test EXPLAIN on JOINs
    assert_isexplain_matches(
        &db,
        "EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        "EXPLAIN JOIN",
    );
    assert_isexplain_matches(
        &db,
        "EXPLAIN QUERY PLAN SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        "EXPLAIN QUERY PLAN JOIN",
    );
}

#[test]
fn test_differential_isexplain_expressions() {
    let db = DifferentialTestDb::new();

    // EXPLAIN on pure expressions
    assert_isexplain_matches(&db, "EXPLAIN SELECT 1", "EXPLAIN SELECT literal");
    assert_isexplain_matches(&db, "EXPLAIN SELECT 1 + 2", "EXPLAIN SELECT expression");
    assert_isexplain_matches(
        &db,
        "EXPLAIN QUERY PLAN SELECT 1",
        "EXPLAIN QUERY PLAN SELECT literal",
    );
}

// =============================================================================
// Combined readonly + isexplain tests
// =============================================================================

#[test]
fn test_differential_explain_readonly_interaction() {
    let db = DifferentialTestDb::new_with_table();

    // EXPLAIN of a modifying statement should still be read-only
    // (because EXPLAIN itself doesn't modify anything)
    let sql = "EXPLAIN INSERT INTO test (name) VALUES ('test')";

    let sqlite_stmt = db.sqlite_conn.prepare(sql).unwrap();
    let turso_conn = db.turso_db.connect_limbo();
    let turso_stmt = turso_conn.prepare(sql).unwrap();

    // Both isexplain should be 1
    assert_eq!(sqlite_stmt.is_explain(), 1);
    assert_eq!(turso_stmt.is_explain(), 1);

    // EXPLAIN statements are read-only because they only show the plan
    assert_eq!(
        turso_stmt.is_readonly(),
        sqlite_stmt.readonly(),
        "EXPLAIN INSERT readonly mismatch"
    );
}

#[test]
fn test_differential_explain_query_plan_readonly_interaction() {
    let db = DifferentialTestDb::new_with_table();

    let sql = "EXPLAIN QUERY PLAN DELETE FROM test";

    let sqlite_stmt = db.sqlite_conn.prepare(sql).unwrap();
    let turso_conn = db.turso_db.connect_limbo();
    let turso_stmt = turso_conn.prepare(sql).unwrap();

    // Both isexplain should be 2
    assert_eq!(sqlite_stmt.is_explain(), 2);
    assert_eq!(turso_stmt.is_explain(), 2);

    // EXPLAIN QUERY PLAN statements are read-only
    assert_eq!(
        turso_stmt.is_readonly(),
        sqlite_stmt.readonly(),
        "EXPLAIN QUERY PLAN DELETE readonly mismatch"
    );
}
