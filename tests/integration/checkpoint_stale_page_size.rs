use turso_core::CheckpointMode;

use crate::common::TempDatabase;

#[turso_macros::test]
fn test_checkpoint_uses_database_page_size(db: TempDatabase) {
    let stale = db.connect_limbo();
    let writer = db.connect_limbo();

    stale.execute("PRAGMA page_size=1024").unwrap();
    writer.execute("CREATE TABLE t(x)").unwrap();
    writer
        .execute("INSERT INTO t VALUES(randomblob(5000))")
        .unwrap();

    stale
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();

    assert_sqlite_reads(&db, 1024);
}

#[turso_macros::test]
fn test_checkpoint_from_connection_with_default_page_size(db: TempDatabase) {
    let stale = db.connect_limbo();
    let writer = db.connect_limbo();

    writer.execute("PRAGMA page_size=1024").unwrap();
    writer.execute("CREATE TABLE t(x)").unwrap();
    writer
        .execute("INSERT INTO t VALUES(randomblob(5000))")
        .unwrap();

    stale
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();

    assert_sqlite_reads(&db, 1024);
}

fn assert_sqlite_reads(db: &TempDatabase, expected_page_size: i64) {
    let sqlite = rusqlite::Connection::open(&db.path).unwrap();
    let count: i64 = sqlite
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let page_size: i64 = sqlite
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    assert_eq!(page_size, expected_page_size);
}
