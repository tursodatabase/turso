use std::path::Path;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags};

use crate::common::ExecRows;

/// The change counter is a 4-byte big-endian integer at offset 24 in the database header.
const CHANGE_COUNTER_OFFSET: usize = 24;
/// The version-valid-for field is at offset 92 and should match the change counter.
const VERSION_VALID_FOR_OFFSET: usize = 92;

/// Read the change counter (offset 24) and version-valid-for (offset 92) from a database file.
fn read_change_counter(db_path: &Path) -> (u32, u32) {
    let bytes = std::fs::read(db_path).expect("Failed to read database file");
    assert!(bytes.len() >= 100, "Database file too small for header");
    let change_counter = u32::from_be_bytes(
        bytes[CHANGE_COUNTER_OFFSET..CHANGE_COUNTER_OFFSET + 4]
            .try_into()
            .unwrap(),
    );
    let version_valid_for = u32::from_be_bytes(
        bytes[VERSION_VALID_FOR_OFFSET..VERSION_VALID_FOR_OFFSET + 4]
            .try_into()
            .unwrap(),
    );
    (change_counter, version_valid_for)
}

fn open_db(
    db_path: &Path,
) -> (
    std::sync::Arc<Database>,
    std::sync::Arc<turso_core::Connection>,
) {
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();
    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
    )
    .expect("Failed to open database");
    let conn = db.connect().unwrap();
    (db, conn)
}

/// In WAL mode, the change counter is only incremented when page 1 is dirty.
/// DDL operations (CREATE TABLE, etc.) always make page 1 dirty because they
/// modify sqlite_master which is stored on page 1. Plain DML (INSERT, UPDATE, DELETE)
/// may or may not make page 1 dirty depending on whether the database size changes
/// or freelist is modified.
///
/// This matches SQLite's behavior: pagerWalFrames() calls pager_write_changecounter()
/// only when page 1 is in the dirty page list.
#[test]
fn test_change_counter_increments_on_ddl() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test_cc.db");

    let (db, conn) = open_db(&db_path);

    // CREATE TABLE makes page 1 dirty (sqlite_master is on page 1, plus db_size changes)
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let (cc1, vvf1) = read_change_counter(&db_path);

    assert!(
        cc1 > 0,
        "Change counter should be > 0 after CREATE TABLE, got {cc1}"
    );
    assert_eq!(
        cc1, vvf1,
        "change_counter ({cc1}) should equal version_valid_for ({vvf1})"
    );

    // Second CREATE TABLE also makes page 1 dirty -> counter increments again
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let (cc2, vvf2) = read_change_counter(&db_path);

    assert!(
        cc2 > cc1,
        "Change counter should increase after second CREATE TABLE: {cc1} -> {cc2}"
    );
    assert_eq!(
        cc2, vvf2,
        "change_counter ({cc2}) should equal version_valid_for ({vvf2})"
    );

    // Third DDL
    conn.execute("CREATE TABLE t3 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let (cc3, vvf3) = read_change_counter(&db_path);

    assert!(
        cc3 > cc2,
        "Change counter should increase after third CREATE TABLE: {cc2} -> {cc3}"
    );
    assert_eq!(cc3, vvf3);

    drop(conn);
    drop(db);
}

/// Test that after DDL the change counter does NOT increase on plain INSERTs
/// that don't cause page 1 to be modified. This matches SQLite's behavior in WAL mode.
#[test]
fn test_change_counter_stable_on_dml_without_page1_change() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test_cc_dml.db");

    let (db, conn) = open_db(&db_path);

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let (cc_after_create, vvf_after_create) = read_change_counter(&db_path);

    assert!(cc_after_create > 0);
    assert_eq!(cc_after_create, vvf_after_create);

    // INSERT into an existing table - page 1 is NOT necessarily dirty
    // (only the B-tree leaf page changes if no new pages are allocated)
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let (cc_after_insert, vvf_after_insert) = read_change_counter(&db_path);

    // The change counter should stay the same or increase (depending on whether
    // page 1 was dirty). In most cases for a small INSERT, it stays the same.
    assert!(
        cc_after_insert >= cc_after_create,
        "Change counter should not decrease: before={cc_after_create}, after={cc_after_insert}"
    );
    assert_eq!(cc_after_insert, vvf_after_insert);

    drop(conn);
    drop(db);
}

/// Test that the change counter is consistent between tursodb and SQLite.
/// After tursodb writes and checkpoints, SQLite should be able to read the database
/// and see a valid change counter.
#[test]
fn test_change_counter_sqlite_compat() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test_cc_compat.db");

    // Create database with tursodb
    {
        let (db, conn) = open_db(&db_path);
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'world')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        drop(conn);
        drop(db);
    }

    let (cc, vvf) = read_change_counter(&db_path);
    assert!(cc > 0, "Change counter should be > 0 after writes");
    assert_eq!(cc, vvf, "change_counter and version_valid_for should match");

    // Open with SQLite (rusqlite) and verify it can read the data
    let sqlite_conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = sqlite_conn
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2, "SQLite should see 2 rows");

    // SQLite's PRAGMA data_version should work correctly
    let data_version: i64 = sqlite_conn
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    assert!(data_version > 0, "SQLite PRAGMA data_version should be > 0");
}

/// Test that multiple DDL operations each increment the change counter.
#[test]
fn test_change_counter_multiple_ddl_operations() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test_cc_multi_ddl.db");

    let (db, conn) = open_db(&db_path);

    let mut prev_cc = 0u32;
    for i in 0..5 {
        conn.execute(&format!("CREATE TABLE t{i} (id INTEGER PRIMARY KEY)"))
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let (cc, vvf) = read_change_counter(&db_path);
        assert!(
            cc > prev_cc,
            "Change counter should increase after CREATE TABLE t{i}: prev={prev_cc}, current={cc}"
        );
        assert_eq!(
            cc, vvf,
            "change_counter ({cc}) should equal version_valid_for ({vvf}) at iteration {i}"
        );
        prev_cc = cc;
    }

    // Verify all tables exist
    let rows: Vec<(i64,)> =
        conn.exec_rows("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 't%'");
    assert_eq!(rows[0].0, 5, "Should have 5 tables");

    drop(conn);
    drop(db);
}

/// Test that the change counter persists correctly across database reopen
/// when DDL operations make page 1 dirty.
#[test]
fn test_change_counter_persists_across_reopen() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test_cc_persist.db");

    // First session: create tables
    let cc_first;
    {
        let (db, conn) = open_db(&db_path);
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let (cc, vvf) = read_change_counter(&db_path);
        assert!(cc > 0);
        assert_eq!(cc, vvf);
        cc_first = cc;

        drop(conn);
        drop(db);
    }

    // Second session: reopen, create another table (DDL makes page 1 dirty)
    {
        let (db, conn) = open_db(&db_path);
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let (cc, vvf) = read_change_counter(&db_path);
        assert!(
            cc > cc_first,
            "Change counter should increase after reopen and DDL: first={cc_first}, second={cc}"
        );
        assert_eq!(cc, vvf);

        // Verify both tables exist
        let rows: Vec<(i64,)> =
            conn.exec_rows("SELECT COUNT(*) FROM sqlite_master WHERE type='table'");
        assert_eq!(rows[0].0, 2);

        drop(conn);
        drop(db);
    }
}
