use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags};

use crate::common::ExecRows;

/// Regression test for issue where sqlite_sequence was not persisted after checkpoint,
/// causing AUTOINCREMENT IDs to be reused after restart.
#[test]
fn mvcc_autoincrement_restart_durability() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.pragma_update("journal_mode", "'mvcc'").unwrap();

        conn.execute("CREATE TABLE test_table(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO test_table(value) VALUES ('first'), ('second'), ('third')")
            .unwrap();

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM test_table ORDER BY id");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (1, "first".to_string()));
        assert_eq!(rows[1], (2, "second".to_string()));
        assert_eq!(rows[2], (3, "third".to_string()));

        conn.execute("DELETE FROM test_table WHERE id = 3").unwrap();

        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='test_table'");
        assert_eq!(seq_rows.len(), 1);
        assert_eq!(seq_rows[0], ("test_table".to_string(), 3));

        drop(conn);
        drop(db);
    }

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();

        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='test_table'");
        assert_eq!(
            seq_rows.len(),
            1,
            "sqlite_sequence table should exist after restart"
        );
        assert_eq!(
            seq_rows[0],
            ("test_table".to_string(), 3),
            "sqlite_sequence should show max ID 3"
        );

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM test_table ORDER BY id");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, "first".to_string()));
        assert_eq!(rows[1], (2, "second".to_string()));

        conn.execute("INSERT INTO test_table(value) VALUES ('fourth')")
            .unwrap();

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM test_table ORDER BY id");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (1, "first".to_string()));
        assert_eq!(rows[1], (2, "second".to_string()));
        assert_eq!(
            rows[2],
            (4, "fourth".to_string()),
            "New insert should get ID 4, not reuse deleted ID 3"
        );

        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='test_table'");
        assert_eq!(seq_rows.len(), 1);
        assert_eq!(
            seq_rows[0],
            ("test_table".to_string(), 4),
            "sqlite_sequence should now show max ID 4"
        );

        drop(conn);
        drop(db);
    }
}

/// Test  if sqlite_sequence rebuilding incorrectly includes rolled-back transactions
#[test]
fn mvcc_autoincrement_rollback_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("rollback_test.db");
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();

    conn.pragma_update("journal_mode", "'mvcc'").unwrap();

    conn.execute("CREATE TABLE rollback_test(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
        .unwrap();

    conn.execute("INSERT INTO rollback_test(value) VALUES ('committed1'), ('committed2')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO rollback_test(value) VALUES ('rollback1'), ('rollback2')")
        .unwrap();

    let rows: Vec<(i64, String)> =
        conn.exec_rows("SELECT id, value FROM rollback_test ORDER BY id");
    assert_eq!(rows.len(), 4, "Should see all rows within transaction");
    assert_eq!(rows[2], (3, "rollback1".to_string()));
    assert_eq!(rows[3], (4, "rollback2".to_string()));

    conn.execute("ROLLBACK").unwrap();

    let rows: Vec<(i64, String)> =
        conn.exec_rows("SELECT id, value FROM rollback_test ORDER BY id");
    assert_eq!(
        rows.len(),
        2,
        "Should only see committed rows after rollback"
    );
    assert_eq!(rows[0], (1, "committed1".to_string()));
    assert_eq!(rows[1], (2, "committed2".to_string()));

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // sqlite_sequence should NOT have rolled-back values
    let seq_rows: Vec<(String, i64)> =
        conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='rollback_test'");
    assert_eq!(seq_rows.len(), 1);
    assert_eq!(
        seq_rows[0],
        ("rollback_test".to_string(), 2),
        "sqlite_sequence should only reflect committed max ID (2), not rolled-back IDs (4)"
    );

    conn.execute("INSERT INTO rollback_test(value) VALUES ('after_rollback')")
        .unwrap();

    // Should get ID 3, not 5
    let rows: Vec<(i64, String)> =
        conn.exec_rows("SELECT id, value FROM rollback_test ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[2],
        (3, "after_rollback".to_string()),
        "Next ID should be 3, not influenced by rolled-back IDs"
    );

    drop(conn);
    drop(db);
}

/// Test to verify that rebuilt sqlite_sequence entries are properly persisted
#[test]
fn mvcc_autoincrement_sequence_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("persistence_test.db");
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.pragma_update("journal_mode", "'mvcc'").unwrap();

        conn.execute("CREATE TABLE test_persist(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO test_persist(value) VALUES ('test1'), ('test2'), ('test3')")
            .unwrap();

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM test_persist ORDER BY id");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2], (3, "test3".to_string()));

        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='test_persist'");
        assert_eq!(seq_rows.len(), 1);
        assert_eq!(seq_rows[0], ("test_persist".to_string(), 3));

        drop(conn);
        drop(db);
    }

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();

        // Critical test: sqlite_sequence should have persisted across restart
        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='test_persist'");
        assert_eq!(
            seq_rows.len(),
            1,
            "sqlite_sequence table should exist and contain our entry after restart"
        );
        assert_eq!(
            seq_rows[0],
            ("test_persist".to_string(), 3),
            "sqlite_sequence should have persisted max ID 3"
        );

        conn.execute("INSERT INTO test_persist(value) VALUES ('after_restart')")
            .unwrap();

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM test_persist ORDER BY id");
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[3],
            (4, "after_restart".to_string()),
            "Should get ID 4, proving sequence persisted correctly"
        );

        drop(conn);
        drop(db);
    }
}

/// Test to verify that sqlite_sequence rebuilding incorrectly includes uncommitted transactions
/// by using two separate connections to simulate concurrent transactions
#[test]
fn mvcc_autoincrement_uncommitted_transaction_handling() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("uncommitted_test.db");
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());

    // Connection 1: commits data
    let db1 = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn1 = db1.connect().unwrap();
    conn1.pragma_update("journal_mode", "'mvcc'").unwrap();

    // Connection 2: has uncommitted transaction
    let db2 = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn2 = db2.connect().unwrap();

    conn1
        .execute("CREATE TABLE uncommitted_test(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
        .unwrap();

    // Connection 1: Insert committed data
    conn1
        .execute("INSERT INTO uncommitted_test(value) VALUES ('committed1'), ('committed2')")
        .unwrap();

    // Connection 2: Start transaction with uncommitted data
    conn2.execute("BEGIN").unwrap();
    conn2
        .execute("INSERT INTO uncommitted_test(value) VALUES ('uncommitted1'), ('uncommitted2')")
        .unwrap();

    conn2.execute("ROLLBACK").unwrap();

    // Trigger checkpoint to rebuild sqlite_sequence
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let seq_rows: Vec<(String, i64)> =
        conn1.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='uncommitted_test'");
    assert_eq!(seq_rows.len(), 1);
    assert_eq!(
        seq_rows[0],
        ("uncommitted_test".to_string(), 2),
        "sqlite_sequence should only reflect committed max ID (2), not uncommitted IDs (4)"
    );

    conn1
        .execute("INSERT INTO uncommitted_test(value) VALUES ('after_uncommitted')")
        .unwrap();

    let rows: Vec<(i64, String)> =
        conn1.exec_rows("SELECT id, value FROM uncommitted_test ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[2],
        (3, "after_uncommitted".to_string()),
        "Next ID should be 3, not influenced by uncommitted IDs"
    );

    drop(conn1);
    drop(conn2);
    drop(db1);
    drop(db2);
}

/// This test checks if the RowidAllocator in-memory counter gets out of sync with
/// the rebuilt sqlite_sequence table, potentially leading to duplicate IDs
#[test]
fn mvcc_autoincrement_allocator_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("allocator_test.db");
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.pragma_update("journal_mode", "'mvcc'").unwrap();

        conn.execute(
            "CREATE TABLE allocator_test(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)",
        )
        .unwrap();
        conn.execute("INSERT INTO allocator_test(value) VALUES ('data1'), ('data2'), ('data3')")
            .unwrap();

        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='allocator_test'");
        assert_eq!(seq_rows.len(), 1);
        assert_eq!(seq_rows[0], ("allocator_test".to_string(), 3));

        drop(conn);
        drop(db);
    }

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.execute("INSERT INTO allocator_test(value) VALUES ('after_restart')")
            .unwrap();

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM allocator_test ORDER BY id");
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[3], (4, "after_restart".to_string()));

        let seq_rows: Vec<(String, i64)> =
            conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name='allocator_test'");
        assert_eq!(seq_rows.len(), 1);
        assert_eq!(seq_rows[0], ("allocator_test".to_string(), 4));

        conn.execute("INSERT INTO allocator_test(value) VALUES ('second_after_restart')")
            .unwrap();

        let rows: Vec<(i64, String)> =
            conn.exec_rows("SELECT id, value FROM allocator_test ORDER BY id");
        assert_eq!(rows.len(), 5);
        assert_eq!(
            rows[4],
            (5, "second_after_restart".to_string()),
            "Should get sequential IDs"
        );

        drop(conn);
        drop(db);
    }
}
