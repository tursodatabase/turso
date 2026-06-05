//! Differential file-format compatibility tests (issue #2576).
//!
//! SQLite (via the bundled `rusqlite`) writes a database file; Turso opens the
//! *same on-disk file* and must read identical row data back. This exercises
//! Turso's on-disk format reader against SQLite as the ground-truth writer.
//!
//! This first cut is a small, deterministic, fixed-seed proof of compatibility.
//! Randomized / long-running variants are layered on in follow-up work behind
//! `#[ignore]` so default CI stays bounded.

use core_tester::common::{TempDatabase, TempDatabaseBuilder, limbo_exec_rows, sqlite_exec_rows};
use rusqlite::params;
use tempfile::TempDir;

/// Build a SQLite database on disk that exercises a few format features:
/// a non-default page size, all storage classes, and an overflow-page blob.
/// Returns the temp dir (kept alive by the caller) and the db path.
fn build_sqlite_db(page_size: u32) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("ff.db");

    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "page_size", page_size).unwrap();
    // Use a rollback journal so the whole database lives in the main file,
    // not a sidecar WAL, keeping the read path purely about the file format.
    conn.pragma_update(None, "journal_mode", "delete").unwrap();

    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, r REAL, s TEXT, b BLOB, n)",
        (),
    )
    .unwrap();

    // A blob large enough to force overflow pages at any page size.
    let big_blob = vec![0xABu8; 16 * 1024];

    conn.execute(
        "INSERT INTO t (id, i, r, s, b, n) VALUES (1, ?1, ?2, ?3, ?4, NULL)",
        params![-42i64, 3.5f64, "hello world", big_blob],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO t (id, i, r, s, b, n) VALUES (2, ?1, ?2, ?3, ?4, ?5)",
        params![9_000_000_000i64, -0.0001f64, "", Vec::<u8>::new(), 1i64],
    )
    .unwrap();

    // Ensure everything is flushed to the main database file.
    drop(conn);
    (dir, path)
}

/// Smallest possible differential check: one SQLite-written file, one table,
/// read back identically by both engines.
#[test]
fn file_format_read_matches_sqlite_default_page_size() {
    let (_dir, path) = build_sqlite_db(4096);

    // SQLite ground truth (reopen the file we just wrote).
    let sqlite_conn = rusqlite::Connection::open(&path).unwrap();
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, "SELECT * FROM t ORDER BY id");

    // Turso reads the same on-disk file.
    let db: TempDatabase = TempDatabaseBuilder::new().with_db_path(&path).build();
    let limbo_conn = db.connect_limbo();
    let limbo_rows = limbo_exec_rows(&limbo_conn, "SELECT * FROM t ORDER BY id");

    assert_eq!(
        limbo_rows, sqlite_rows,
        "row mismatch reading SQLite-written file in Turso\nturso: {limbo_rows:?}\nsqlite: {sqlite_rows:?}"
    );
}
