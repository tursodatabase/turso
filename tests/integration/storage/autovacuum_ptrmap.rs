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
///    pages live at 2, 105, 208, ... Growing the database to exactly 104 pages
///    and then allocating while a freelist exists must record
///    `database_size = 105` (the ptrmap page).
///
/// 2. `Pager::free_page()` did not write `PTRMAP_FREEPAGE` pointer-map
///    entries, so SQLite's `PRAGMA integrity_check` reported
///    `Freelist: Failed to read ptrmap key=N` for every freed page.
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

    // 1. Grow the database to exactly the page right before the second ptrmap
    //    page (page 105).
    let mut i = 0u32;
    while page_count() < 104 {
        i += 1;
        limbo_exec_rows(
            &conn,
            &format!("INSERT INTO t VALUES ({i}, randomblob(400));"),
        );
    }
    assert_eq!(
        page_count(),
        104,
        "database should stop right before the page-105 ptrmap boundary"
    );

    // 2. Free some pages so `header.freelist_trunk_page` becomes non-zero.
    //    Deleting rows empties leaf pages, which are moved to the freelist.
    limbo_exec_rows(&conn, "DELETE FROM t WHERE a % 7 = 0;");

    // 3. Allocate again. `allocate_page()` must allocate ptrmap page 105
    //    *and* persist the bumped database size even though the returned page
    //    comes from the freelist. Before the fix the header stayed at 104
    //    while the ptrmap page 105 was dirtied.
    limbo_exec_rows(&conn, "INSERT INTO t VALUES (1000000, randomblob(400));");

    let page_count = page_count();
    assert!(
        page_count >= 105,
        "header database_size was not updated when a ptrmap page was \
         allocated alongside a freelist reuse (issue #6774): page_count={page_count}, \
         expected >= 105"
    );

    // 4. Flush the WAL back into the main database file so an external SQLite
    //    process reads a fully materialized image.
    limbo_exec_rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE);");

    let ext = rusqlite::Connection::open(&db_path).unwrap();
    let integrity: Vec<String> = ext
        .prepare("SELECT * FROM pragma_integrity_check;")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // 5. Every page on the freelist must carry a valid `FreePage` pointer-map
    //    entry. Before the fix SQLite reported
    //    `Freelist: Failed to read ptrmap key=N` for each freed page.
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
