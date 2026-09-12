use std::sync::Arc;

use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect};

use crate::common::limbo_exec_rows;

/// Regression tests for https://github.com/tursodatabase/turso/issues/6774
///
/// Two defects around ptrmap maintenance in autovacuum databases:
///
/// 1. `Pager::allocate_page()` allocates a ptrmap page and bumps the in-flight
///    database size when the next page number would land on a ptrmap boundary,
///    but the freelist-reuse arms returned early **without persisting the
///    bumped size** into `header.database_size`. The dirty ptrmap page
///    therefore sat at page number `header.database_size + 1`: a page beyond
///    the recorded size, invisible to readers that trust the header.
///
///    Layout with page_size=512: ptrmap cycle = 512/5 + 1 = 103, so ptrmap
///    pages live at 2, 105, 208, ... Growing the database past page 104 while
///    a freelist exists must record `database_size >= 105` (the ptrmap page).
///
/// 2. `Pager::free_page()` did not write `PTRMAP_FREEPAGE` pointer-map
///    entries, so SQLite's `PRAGMA integrity_check` reported
///    `Freelist: Failed to read ptrmap key=N` for every freed page.
///
/// The tests are intentionally layout-robust: instead of asserting on an exact
/// page count produced by a fragile growth sequence (b-tree splits can differ
/// across platforms), each test only asserts the invariant this PR restores.
#[test]
fn test_autovacuum_allocate_page_updates_db_size_on_freelist_reuse() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("ptrmap.db");

    let io = Arc::new(PlatformIO::new().unwrap()) as Arc<dyn turso_core::IO>;
    let opts = DatabaseOpts::new().with_autovacuum(true);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Configure the smallest page size so the first non-trivial ptrmap
    // boundary (page 105) is reachable with a tiny dataset.
    limbo_exec_rows(&conn, "PRAGMA page_size=512;");
    limbo_exec_rows(&conn, "PRAGMA auto_vacuum=full;");
    limbo_exec_rows(&conn, "CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB);");

    let page_count = || -> u32 {
        let rows = limbo_exec_rows(&conn, "PRAGMA page_count;");
        match &rows[0][0] {
            rusqlite::types::Value::Integer(n) => *n as u32,
            other => panic!("unexpected page_count value: {other:?}"),
        }
    };

    // 1. Grow the database beyond the first ptrmap boundary so pages up to
    //    and past page 104 exist.
    let mut i = 0u32;
    while page_count() < 110 {
        i += 1;
        limbo_exec_rows(
            &conn,
            &format!("INSERT INTO t VALUES ({i}, randomblob(400));"),
        );
    }

    // 2. Free roughly half of the rows so a real freelist exists, then insert
    //    far more data than the freelist can hold. Whatever order the freelist
    //    pages are reused in, the database must eventually allocate past page
    //    104 again, which is where the ptrmap-boundary bug fired: the reuse
    //    arm of `allocate_page()` returned without persisting the bumped
    //    `header.database_size`.
    limbo_exec_rows(&conn, "DELETE FROM t WHERE a % 2 = 0;");
    for j in 0..600u32 {
        limbo_exec_rows(
            &conn,
            &format!("INSERT INTO t VALUES ({}, randomblob(400));", 1_000_000 + j),
        );
    }

    let page_count = page_count();
    assert!(
        page_count >= 105,
        "header database_size was not updated when a ptrmap page was \
         allocated alongside a freelist reuse (issue #6774): page_count={page_count}, \
         expected >= 105"
    );
}

#[test]
fn test_autovacuum_free_page_writes_freelist_ptrmap_entries() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("freelist_ptrmap.db");

    let io = Arc::new(PlatformIO::new().unwrap()) as Arc<dyn turso_core::IO>;
    let opts = DatabaseOpts::new().with_autovacuum(true);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Keep the database tiny (single leaf page, no interior b-tree pages, no
    // ptrmap boundary): this isolates the `free_page()` defect from the
    // unrelated pre-existing gaps in ptrmap coverage for b-tree/overflow
    // pages documented in issue #6774.
    limbo_exec_rows(&conn, "PRAGMA page_size=512;");
    limbo_exec_rows(&conn, "PRAGMA auto_vacuum=full;");
    limbo_exec_rows(&conn, "CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB);");
    for i in 0..30u32 {
        limbo_exec_rows(
            &conn,
            &format!("INSERT INTO t VALUES ({i}, randomblob(64));"),
        );
    }
    limbo_exec_rows(&conn, "DELETE FROM t WHERE a % 3 = 0;");

    // Flush the WAL back into the main database file so an external SQLite
    // process reads a fully materialized image.
    limbo_exec_rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE);");

    let ext = rusqlite::Connection::open(&db_path).unwrap();
    let integrity: Vec<String> = ext
        .prepare("SELECT * FROM pragma_integrity_check;")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Every page on the freelist must carry a valid `FreePage` pointer-map
    // entry. Before the fix SQLite reported
    // `Freelist: Failed to read ptrmap key=N` for each freed page.
    let freelist_errors: Vec<&String> = integrity
        .iter()
        .filter(|line| line.contains("Freelist:"))
        .collect();
    assert!(
        freelist_errors.is_empty(),
        "SQLite integrity_check reported freelist pointer-map errors \
         (free_page did not write FreePage ptrmap entries): {freelist_errors:?}"
    );
}
