//! Regression tests for reading databases whose schema format predates
//! descending-index support (schema format < 4, header byte offset 44-47).
//!
//! Descending indexes were introduced in SQLite schema format 4. For any lower
//! format SQLite ignores the `DESC` qualifier on index columns: such an index is
//! stored ascending on disk, and SQLite scans it ascending regardless of the
//! `DESC` keyword in its `CREATE INDEX` text. Turso historically derived the
//! index direction purely from the SQL text and ignored the format number, so on
//! a legacy file it would scan the index in the wrong direction and return rows
//! in the wrong order. These tests pin Turso to SQLite's behavior.

use std::path::Path;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags};

use crate::common::{limbo_exec_rows, sqlite_exec_rows};

/// Overwrite the 4-byte big-endian schema format number at header offset 44.
fn patch_schema_format(path: &Path, val: u32) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[44..48].copy_from_slice(&val.to_be_bytes());
    std::fs::write(path, bytes).unwrap();
}

/// Build a database that looks like one written by a pre-3.3.0 SQLite: an index
/// whose `CREATE INDEX` text says `DESC`, but whose on-disk b-tree is ascending
/// (because those versions ignored `DESC`), with the schema format set to
/// `format` (< 4). We synthesize it by creating a normal *ascending* index, then
/// rewriting only the stored SQL text to say `DESC` via `writable_schema`.
fn build_legacy_desc_db(path: &Path, wal: bool, format: u32) {
    let conn = rusqlite::Connection::open(path).unwrap();
    if wal {
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }
    conn.execute_batch(
        "CREATE TABLE t(x INTEGER);
         CREATE INDEX idx ON t(x);
         INSERT INTO t VALUES (1),(2),(3);
         PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE INDEX idx ON t(x DESC)' WHERE name='idx';",
    )
    .unwrap();
    if wal {
        conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .unwrap();
    }
    drop(conn);
    patch_schema_format(path, format);
}

fn turso_ints(path: &Path, sql: &str) -> Vec<i64> {
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    ints(limbo_exec_rows(&conn, sql))
}

fn sqlite_ints(path: &Path, sql: &str) -> Vec<i64> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    ints(sqlite_exec_rows(&conn, sql))
}

fn ints(rows: Vec<Vec<rusqlite::types::Value>>) -> Vec<i64> {
    rows.into_iter()
        .map(|r| match r[0] {
            rusqlite::types::Value::Integer(i) => i,
            ref other => panic!("expected integer column, got {other:?}"),
        })
        .collect()
}

/// On a legacy-format file the `DESC` index must be treated as ascending, so an
/// indexed `ORDER BY x` returns ascending rows — matching SQLite, and matching
/// the actual on-disk order. Covers schema formats 0 (which SQLite treats as 1)
/// and 1, in both rollback and WAL journal modes.
fn assert_legacy_matches_sqlite(wal: bool, format: u32) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("legacy.db");
    build_legacy_desc_db(&db, wal, format);

    for sql in [
        "SELECT x FROM t ORDER BY x",
        "SELECT x FROM t ORDER BY x ASC",
        "SELECT x FROM t ORDER BY x DESC",
    ] {
        let expected = sqlite_ints(&db, sql);
        let got = turso_ints(&db, sql);
        assert_eq!(
            got, expected,
            "turso diverged from sqlite for `{sql}` (wal={wal}, schema_format={format}): \
             turso={got:?} sqlite={expected:?}"
        );
    }

    // Spell the correct answers out explicitly: the on-disk index is ascending,
    // so ascending order is 1,2,3 even though the schema text says DESC.
    assert_eq!(turso_ints(&db, "SELECT x FROM t ORDER BY x"), vec![1, 2, 3]);
    assert_eq!(
        turso_ints(&db, "SELECT x FROM t ORDER BY x DESC"),
        vec![3, 2, 1]
    );
}

#[test]
fn legacy_format1_desc_index_treated_ascending_rollback() {
    assert_legacy_matches_sqlite(false, 1);
}

#[test]
fn legacy_format1_desc_index_treated_ascending_wal() {
    // WAL header (read/write version 2/2) with schema_format=1.
    assert_legacy_matches_sqlite(true, 1);
}

#[test]
fn legacy_format0_desc_index_treated_ascending() {
    // schema_format 0 is valid (empty/legacy) and SQLite treats it as format 1,
    // so DESC is likewise ignored.
    assert_legacy_matches_sqlite(false, 0);
    assert_legacy_matches_sqlite(true, 0);
}

/// Guard against over-masking: a real format-4 descending index must still be
/// honored. Here the on-disk b-tree genuinely is descending, so ascending order
/// requires a reverse scan and `ORDER BY x` yields 1,2,3 while a forward scan
/// (`ORDER BY x DESC`) yields 3,2,1.
#[test]
fn format4_desc_index_still_honored() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("modern.db");
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE t(x INTEGER);
             CREATE INDEX idx ON t(x DESC);
             INSERT INTO t VALUES (1),(2),(3);",
        )
        .unwrap();
    }
    // sqlite writes format 4 for a DESC index; sanity-check that.
    assert_eq!(std::fs::read(&db).unwrap()[44..48], [0, 0, 0, 4]);

    for sql in [
        "SELECT x FROM t ORDER BY x",
        "SELECT x FROM t ORDER BY x DESC",
    ] {
        assert_eq!(turso_ints(&db, sql), sqlite_ints(&db, sql), "sql={sql}");
    }
    assert_eq!(turso_ints(&db, "SELECT x FROM t ORDER BY x"), vec![1, 2, 3]);
    assert_eq!(
        turso_ints(&db, "SELECT x FROM t ORDER BY x DESC"),
        vec![3, 2, 1]
    );
}
