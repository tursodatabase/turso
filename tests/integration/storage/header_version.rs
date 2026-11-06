use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags};
use turso_sqlite::SqliteDialect;

use crate::common::ExecRows;
use asserting::prelude::*;

/// Read header version bytes (write_version at offset 18, read_version at offset 19) from database file
fn read_header_versions(db_path: &Path) -> (u8, u8) {
    let bytes = std::fs::read(db_path).expect("Failed to read database file");
    assert_that!(&bytes).has_at_least_length(20);
    // Offset 18 = write_version, Offset 19 = read_version (per SQLite format)
    (bytes[18], bytes[19])
}

/// Create a Legacy mode database using rusqlite (journal_mode = DELETE)
fn create_legacy_db(db_path: &Path) {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "delete").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ())
        .unwrap();
    conn.execute("INSERT INTO t (val) VALUES ('test')", ())
        .unwrap();
    drop(conn);

    // Verify it's Legacy mode (version 1)
    assert_that!(read_header_versions(db_path)).is_equal_to((1, 1));
}

/// Create a WAL mode database using rusqlite
fn create_wal_db(db_path: &Path) {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "wal").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ())
        .unwrap();
    conn.execute("INSERT INTO t (val) VALUES ('test')", ())
        .unwrap();
    // Checkpoint to ensure header is updated
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
        .unwrap();
    drop(conn);

    // Verify it's WAL mode (version 2)
    assert_that!(read_header_versions(db_path)).is_equal_to((2, 2));
}

/// Open database with limbo and close it, then check header versions
fn open_with_limbo_and_check(db_path: &Path, enable_mvcc: bool) -> (u8, u8) {
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database with limbo");

    // Connect to ensure initialization is complete
    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Enable MVCC if requested
    if enable_mvcc {
        conn.pragma_update("journal_mode", "'mvcc'")
            .expect("enable mvcc");
    }

    // Do a simple query to ensure everything is initialized
    if let Some(mut stmt) = conn.query("SELECT 1").unwrap() {
        stmt.run_with_row_callback(|_| Ok(())).unwrap();
    }

    // Drop connection and database
    drop(conn);
    drop(db);

    // Read header versions after limbo has closed
    read_header_versions(db_path)
}

#[test]
fn test_legacy_db_opened_without_mvcc_converts_to_wal() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a Legacy mode database
    create_legacy_db(&db_path);

    // Open with limbo (without MVCC)
    let (write_ver, read_ver) = open_with_limbo_and_check(&db_path, false);

    // Should be converted to WAL mode (version 2)
    assert_eq!(
        write_ver, 2,
        "Legacy DB opened without MVCC should convert to WAL (write_version=2), got {write_ver}"
    );
    assert_eq!(
        read_ver, 2,
        "Legacy DB opened without MVCC should convert to WAL (read_version=2), got {read_ver}"
    );
}

#[test]
fn test_legacy_db_opened_with_mvcc_converts_to_mvcc() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a Legacy mode database
    create_legacy_db(&db_path);

    // Open with limbo (with MVCC enabled)
    let (write_ver, read_ver) = open_with_limbo_and_check(&db_path, true);

    // Should be converted to MVCC mode (version 255)
    assert_eq!(
        write_ver, 255,
        "Legacy DB opened with MVCC should convert to MVCC (write_version=255), got {write_ver}"
    );
    assert_eq!(
        read_ver, 255,
        "Legacy DB opened with MVCC should convert to MVCC (read_version=255), got {read_ver}"
    );
}

#[test]
fn test_wal_db_opened_without_mvcc_stays_wal() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    // Open with limbo (without MVCC)
    let (write_ver, read_ver) = open_with_limbo_and_check(&db_path, false);

    // Should stay WAL mode (version 2)
    assert_eq!(
        write_ver, 2,
        "WAL DB opened without MVCC should stay WAL (write_version=2), got {write_ver}"
    );
    assert_eq!(
        read_ver, 2,
        "WAL DB opened without MVCC should stay WAL (read_version=2), got {read_ver}"
    );
}

#[test]
fn test_wal_db_opened_with_mvcc_converts_to_mvcc() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    // Open with limbo (with MVCC enabled)
    let (write_ver, read_ver) = open_with_limbo_and_check(&db_path, true);

    // Should be converted to MVCC mode (version 255)
    assert_eq!(
        write_ver, 255,
        "WAL DB opened with MVCC should convert to MVCC (write_version=255), got {write_ver}"
    );
    assert_eq!(
        read_ver, 255,
        "WAL DB opened with MVCC should convert to MVCC (read_version=255), got {read_ver}"
    );
}

#[test]
fn test_mvcc_db_opened_without_mvcc_flag_stays_mvcc() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database first
    create_wal_db(&db_path);

    // Open with limbo with MVCC to convert it to MVCC mode
    let _ = open_with_limbo_and_check(&db_path, true);

    // Verify it's now MVCC
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));

    // Now open WITHOUT MVCC flag - should auto-enable MVCC and stay at version 255
    let (write_ver, read_ver) = open_with_limbo_and_check(&db_path, false);

    assert_eq!(
        write_ver, 255,
        "MVCC DB opened without MVCC flag should stay MVCC (write_version=255), got {write_ver}"
    );
    assert_eq!(
        read_ver, 255,
        "MVCC DB opened without MVCC flag should stay MVCC (read_version=255), got {read_ver}"
    );
}

#[test]
fn test_mvcc_db_opened_with_mvcc_stays_mvcc() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database first
    create_wal_db(&db_path);

    // Open with limbo with MVCC to convert it to MVCC mode
    let _ = open_with_limbo_and_check(&db_path, true);

    // Verify it's now MVCC
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));

    // Open again with MVCC flag - should stay MVCC
    let (write_ver, read_ver) = open_with_limbo_and_check(&db_path, true);

    assert_eq!(
        write_ver, 255,
        "MVCC DB opened with MVCC should stay MVCC (write_version=255), got {write_ver}"
    );
    assert_eq!(
        read_ver, 255,
        "MVCC DB opened with MVCC should stay MVCC (read_version=255), got {read_ver}"
    );
}

/// Create a WAL mode database with a non-empty WAL file (no checkpoint)
fn create_wal_db_with_pending_wal(db_path: &Path) {
    // Use exclusive locking mode to prevent automatic checkpoint on close
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "wal").unwrap();
    // Disable automatic checkpointing
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ())
        .unwrap();
    conn.execute("INSERT INTO t (val) VALUES ('test')", ())
        .unwrap();

    // Check WAL file before closing
    let wal_path = db_path.with_extension("db-wal");

    // If WAL doesn't exist yet, it means the data is still in memory
    // We need another connection to force the WAL to be created
    if !wal_path.exists() {
        // Open a second connection to force WAL file creation
        let conn2 = rusqlite::Connection::open(db_path).unwrap();
        conn2.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        let _: i64 = conn2
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap();
        drop(conn2);
    }

    drop(conn);

    // Verify WAL file exists and is non-empty
    let wal_exists = wal_path.exists();
    let wal_size = if wal_exists {
        std::fs::metadata(&wal_path).unwrap().len()
    } else {
        0
    };

    // If WAL doesn't exist or is empty, the test setup didn't work as expected
    // This is okay - it means we don't have pending WAL data to test
    // The test will still verify that mode switching works
    if !wal_exists || wal_size == 0 {
        eprintln!("Note: WAL file is empty or doesn't exist - test will verify mode switching without pending WAL data");
    }

    // Verify it's WAL mode (version 2)
    assert_that!(read_header_versions(db_path)).is_equal_to((2, 2));
}

/// Test switching from WAL to MVCC mode via PRAGMA when there's a non-empty WAL.
/// This tests the scenario where:
/// 1. A database is opened in WAL mode with pending WAL data
/// 2. User runs `PRAGMA journal_mode = "mvcc"` to switch to MVCC
/// 3. The Checkpoint instruction should use the newly opened MvStore
#[test]
fn test_pragma_journal_mode_wal_to_mvcc_with_pending_wal() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Step 1: Create a WAL mode database with rusqlite
    create_wal_db_with_pending_wal(&db_path);

    // Step 2: Open with limbo WITHOUT MVCC to create WAL data, then close without checkpointing
    {
        let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = DatabaseOpts::new();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .expect("Failed to open database with limbo (non-MVCC)");

        let conn = db.connect(ConnectOpts::new()).unwrap();

        // Insert some data to ensure WAL has content
        conn.execute("INSERT INTO t (val) VALUES ('limbo_data')")
            .expect("INSERT should work");

        // Drop without checkpointing - this should leave data in WAL
        drop(conn);
        drop(db);
    }

    // Verify WAL file exists and has content
    let wal_path = db_path.with_extension("db-wal");
    assert!(
        wal_path.exists(),
        "WAL file should exist after limbo operations"
    );
    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    assert!(wal_size > 0, "WAL file should be non-empty");

    // Step 3: Reopen with MVCC enabled and try to switch via PRAGMA
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database with limbo (MVCC enabled)");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Switch to MVCC mode via PRAGMA
    // This should work even with pending WAL data
    let result = conn
        .pragma_update("journal_mode", "'mvcc'")
        .expect("PRAGMA journal_mode update should not fail");

    // Verify the journal mode was set
    assert!(!result.is_empty(), "PRAGMA should return a result");
    let mode = result[0][0].to_string();
    assert_eq!(
        mode, "mvcc",
        "Journal mode should be mvcc after PRAGMA, got {mode}"
    );

    // Verify we can still query data (both original and new data)
    let mut stmt = conn.prepare("SELECT val FROM t ORDER BY val").unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let val: String = row.get::<String>(0).unwrap();
        rows.push(val);
        Ok(())
    })
    .unwrap();
    assert!(
        rows.contains(&"test".to_string()),
        "Should have original test row"
    );
    assert!(
        rows.contains(&"limbo_data".to_string()),
        "Should have limbo_data row"
    );

    drop(conn);
    drop(db);

    // Verify header is now MVCC (version 255)
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));
}

/// Test switching from MVCC to WAL mode via PRAGMA.
/// This tests the scenario where:
/// 1. A database is in MVCC mode with data
/// 2. User runs `PRAGMA journal_mode = "wal"` to switch to WAL
/// 3. The data should be preserved and mode should switch correctly
#[test]
fn test_pragma_journal_mode_mvcc_to_wal() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Step 1: Create a WAL mode database and convert to MVCC
    create_wal_db(&db_path);

    // Step 2: Open and switch to MVCC mode via PRAGMA, then add some data
    {
        let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = DatabaseOpts::new();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .expect("Failed to open database with limbo");

        let conn = db.connect(ConnectOpts::new()).unwrap();

        // Switch to MVCC mode via PRAGMA
        conn.pragma_update("journal_mode", "'mvcc'")
            .expect("PRAGMA journal_mode = 'mvcc' should work");

        // Insert some data in MVCC mode
        conn.execute("INSERT INTO t (val) VALUES ('mvcc_data')")
            .expect("INSERT should work in MVCC mode");

        drop(conn);
        drop(db);
    }

    // Verify it's now MVCC mode
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));

    // Step 3: Reopen and switch to WAL mode via PRAGMA
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    // Open with MVCC true since the file is MVCC, but we'll switch to WAL
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open MVCC database with limbo");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    let result = conn
        .pragma_query("journal_mode")
        .expect("PRAGMA journal_mode update should not fail");

    assert!(!result.is_empty(), "PRAGMA should return a result");
    let mode = result[0][0].to_string();
    assert_eq!(
        mode, "mvcc",
        "Journal mode should be wal after PRAGMA, got {mode}"
    );

    // Switch to WAL mode via PRAGMA
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("PRAGMA journal_mode update should not fail");

    // Verify the journal mode was set
    assert!(!result.is_empty(), "PRAGMA should return a result");
    let mode = result[0][0].to_string();
    assert_eq!(
        mode, "wal",
        "Journal mode should be wal after PRAGMA, got {mode}"
    );

    // Verify we can still query data (both original and MVCC data)
    let rows: Vec<(String,)> = conn.exec_rows("SELECT val FROM t ORDER BY val");

    dbg!(&rows);
    assert!(
        rows.contains(&("test".to_string(),)),
        "Should have original test row"
    );
    assert!(
        rows.contains(&("mvcc_data".to_string(),)),
        "Should have mvcc_data row"
    );

    drop(conn);
    drop(db);

    // Verify header is now WAL (version 2)
    assert_that!(read_header_versions(&db_path)).is_equal_to((2, 2));
}

/// Test switching modes multiple times: WAL -> MVCC -> WAL -> MVCC
/// This ensures mode switching is robust and doesn't corrupt data
#[test]
fn test_pragma_journal_mode_multiple_switches() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Switch to MVCC
    let result = conn
        .pragma_update("journal_mode", "'mvcc'")
        .expect("Switch to MVCC should work");
    assert_eq!(result[0][0].to_string(), "mvcc");

    // Verify header is MVCC (version 255)
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));

    // Insert data in MVCC mode
    conn.execute("INSERT INTO t (val) VALUES ('after_mvcc_switch')")
        .expect("INSERT should work");

    // Switch back to WAL
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("Switch to WAL should work");
    assert_eq!(result[0][0].to_string(), "wal");

    // Verify header is MVCC (version 255)
    assert_that!(read_header_versions(&db_path)).is_equal_to((2, 2));

    // Insert data in WAL mode
    conn.execute("INSERT INTO t (val) VALUES ('after_wal_switch')")
        .expect("INSERT should work");

    // Switch to MVCC again
    let result = conn
        .pragma_update("journal_mode", "'mvcc'")
        .expect("Switch to MVCC again should work");
    assert_eq!(result[0][0].to_string(), "mvcc");

    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));

    // Insert data in MVCC mode
    conn.execute("INSERT INTO t (val) VALUES ('after_second_mvcc_switch')")
        .expect("INSERT should work");

    // Verify all data is present
    let mut stmt = conn.prepare("SELECT val FROM t ORDER BY val").unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let val: String = row.get::<String>(0).unwrap();
        rows.push(val);
        Ok(())
    })
    .unwrap();

    assert!(
        rows.contains(&"test".to_string()),
        "Should have original row"
    );
    assert!(
        rows.contains(&"after_mvcc_switch".to_string()),
        "Should have after_mvcc_switch row"
    );
    assert!(
        rows.contains(&"after_wal_switch".to_string()),
        "Should have after_wal_switch row"
    );
    assert!(
        rows.contains(&"after_second_mvcc_switch".to_string()),
        "Should have after_second_mvcc_switch row"
    );

    drop(conn);
    drop(db);

    // Verify final header is MVCC (version 255)
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));
}

/// Test that PRAGMA journal_mode query returns the current mode correctly
#[test]
fn test_pragma_journal_mode_query() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Query current journal mode (should be WAL)
    if let Some(mut stmt) = conn.query("PRAGMA journal_mode").unwrap() {
        stmt.run_with_row_callback(|row| {
            let mode: String = row.get::<String>(0).unwrap();
            assert_eq!(mode, "wal", "Initial mode should be WAL, got {mode}");
            Ok(())
        })
        .unwrap();
    }

    drop(conn);
    drop(db);

    // Now open and switch to MVCC via PRAGMA, then verify query returns mvcc
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Switch to MVCC mode via PRAGMA
    conn.pragma_update("journal_mode", "'mvcc'")
        .expect("PRAGMA journal_mode = 'mvcc' should work");

    // Query current journal mode (should be mvcc after switching)
    if let Some(mut stmt) = conn.query("PRAGMA journal_mode").unwrap() {
        stmt.run_with_row_callback(|row| {
            let mode: String = row.get::<String>(0).unwrap();
            assert_eq!(
                mode, "mvcc",
                "Mode should be mvcc after PRAGMA switch, got {mode}"
            );
            Ok(())
        })
        .unwrap();
    }

    drop(conn);
    drop(db);
}

/// Test that data inserted after mode switch persists after reopening
#[test]
fn test_pragma_journal_mode_data_persistence_after_switch() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    // Open and switch to MVCC, insert data
    {
        let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = DatabaseOpts::new();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .expect("Failed to open database");

        let conn = db.connect(ConnectOpts::new()).unwrap();

        // Switch to MVCC
        conn.pragma_update("journal_mode", "'mvcc'")
            .expect("Switch to MVCC should work");

        // Insert data after switch
        conn.execute("INSERT INTO t (val) VALUES ('persisted_data')")
            .expect("INSERT should work");

        drop(conn);
        drop(db);
    }

    // Reopen and verify data persisted
    {
        let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = DatabaseOpts::new();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .expect("Failed to reopen database");

        let conn = db.connect(ConnectOpts::new()).unwrap();

        // Verify data persisted
        let mut stmt = conn
            .prepare("SELECT val FROM t WHERE val = 'persisted_data'")
            .unwrap();
        let mut found = false;
        stmt.run_with_row_callback(|_| {
            found = true;
            Ok(())
        })
        .unwrap();
        assert!(found, "Data inserted after mode switch should persist");

        drop(conn);
        drop(db);
    }
}

/// Regression for #9504: after switching an existing database with an
/// AUTOINCREMENT table to MVCC, the sequence must continue instead of reusing
/// id 1 and replacing that row, both right after the switch and after a reopen.
/// Reopening without writes must not write to the log.
#[test]
fn test_autoincrement_continues_after_switch_to_mvcc_and_reopen() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");
    let posts = |conn: &Arc<turso_core::Connection>| -> Vec<(i64, String)> {
        conn.exec_rows("SELECT id, title FROM posts ORDER BY id")
    };
    let row = |id: i64, title: &str| (id, title.to_string());

    {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)")
            .unwrap();
        conn.execute("INSERT INTO posts (title) VALUES ('a'), ('b')")
            .unwrap();
    }
    {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.pragma_update("journal_mode", "'mvcc'").unwrap();
        let other_conn = db.connect(ConnectOpts::new()).unwrap();
        other_conn
            .execute("INSERT INTO posts (title) VALUES ('c')")
            .unwrap();
        assert_eq!(
            posts(&other_conn),
            vec![row(1, "a"), row(2, "b"), row(3, "c")]
        );
    }
    let log_path = tmp_dir.path().join("test.db-log");
    let log_size = || std::fs::metadata(&log_path).unwrap().len();
    let log_size_before_reopen = log_size();
    for _ in 0..2 {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        assert_eq!(posts(&conn).len(), 3);
    }
    assert_eq!(log_size(), log_size_before_reopen);
    {
        let db = Database::open_file_with_flags(
            Arc::new(turso_core::PlatformIO::new().unwrap()),
            db_path.to_str().unwrap(),
            OpenFlags::ReadOnly,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .expect("a read-only open must not need to write");
        let conn = db.connect(ConnectOpts::new()).unwrap();
        assert_eq!(posts(&conn).len(), 3);
    }
    {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("INSERT INTO posts (title) VALUES ('d')")
            .unwrap();
        assert_eq!(
            posts(&conn),
            vec![row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d")]
        );
        let sequence: Vec<(String, i64)> = conn.exec_rows("SELECT name, seq FROM sqlite_sequence");
        assert_eq!(sequence, vec![("posts".to_string(), 4)]);
    }
}

/// A database switched to MVCC by a build that did not sync the
/// AUTOINCREMENT watermarks gets them synced and committed on the next open.
#[test]
fn test_autoincrement_watermark_is_committed_when_opening_mvcc_database() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");
    {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)")
            .unwrap();
        conn.execute("INSERT INTO posts (title) VALUES ('a'), ('b')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    mark_header_as_mvcc(&db_path);
    {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM posts");
        assert_eq!(rows, vec![(2,)]);
    }
    let db = open_db(&db_path);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("INSERT INTO posts (title) VALUES ('c')")
        .unwrap();
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, title FROM posts ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string())
        ]
    );
}

/// Set the header's read and write versions to 255 (MVCC), as a build that
/// switched to MVCC without syncing the AUTOINCREMENT watermarks left them.
/// With the `checksum` feature, page 1 ends in an 8-byte checksum of the rest
/// of the page, so it is written again for the changed header.
fn mark_header_as_mvcc(db_path: &Path) {
    let mut bytes = std::fs::read(db_path).unwrap();
    bytes[18] = 255;
    bytes[19] = 255;
    #[cfg(feature = "checksum")]
    {
        const PAGE_SIZE: usize = 4096;
        const CHECKSUM_SIZE: usize = 8;
        let checksum = twox_hash::XxHash3_64::oneshot(&bytes[..PAGE_SIZE - CHECKSUM_SIZE]);
        bytes[PAGE_SIZE - CHECKSUM_SIZE..PAGE_SIZE].copy_from_slice(&checksum.to_le_bytes());
    }
    std::fs::write(db_path, bytes).unwrap();
}

/// Switching to MVCC inside a transaction is refused, as in SQLite. Before,
/// the switch ran, and after COMMIT or ROLLBACK the database could not be
/// opened again ("Missing MVCC metadata table while logical log state exists").
#[test]
fn test_switch_to_mvcc_inside_transaction_is_rejected() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");
    {
        let db = open_db(&db_path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)")
            .unwrap();
        conn.execute("INSERT INTO posts (title) VALUES ('a')")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        let result = conn.pragma_update("journal_mode", "'mvcc'");
        assert!(
            matches!(result, Err(turso_core::LimboError::TxError(_))),
            "got: {result:?}"
        );
        conn.execute("COMMIT").unwrap();
    }
    let db = open_db(&db_path);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, title FROM posts");
    assert_eq!(rows, vec![(1, "a".to_string())]);
}

fn open_db(db_path: &Path) -> Arc<Database> {
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database")
}

/// Open database in readonly mode with limbo and check header versions
fn open_with_limbo_readonly_and_check(db_path: &Path, enable_mvcc: bool) -> (u8, u8) {
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database with limbo in readonly mode");

    // Connect to ensure initialization is complete
    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Try to enable MVCC if requested (will fail in readonly mode, which is expected)
    if enable_mvcc {
        let _ = conn.pragma_update("journal_mode", "'mvcc'");
    }

    // Do a simple query to ensure everything is initialized
    if let Some(mut stmt) = conn.query("SELECT 1").unwrap() {
        stmt.run_with_row_callback(|_| Ok(())).unwrap();
    }

    // Drop connection and database
    drop(conn);
    drop(db);

    // Read header versions after limbo has closed
    read_header_versions(db_path)
}

/// Test that readonly database cannot modify header when opening a Legacy mode database
#[test]
fn test_readonly_legacy_db_header_not_modified() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a Legacy mode database
    create_legacy_db(&db_path);

    // Get the original header versions
    let (orig_write_ver, orig_read_ver) = read_header_versions(&db_path);
    assert_eq!(orig_write_ver, 1, "Original should be Legacy mode");
    assert_eq!(orig_read_ver, 1, "Original should be Legacy mode");

    // Open with limbo in readonly mode (without MVCC)
    let (write_ver, read_ver) = open_with_limbo_readonly_and_check(&db_path, false);

    // Header should NOT be modified - should still be Legacy mode
    assert_eq!(
        write_ver, 1,
        "Readonly DB should NOT convert Legacy to WAL (write_version should stay 1), got {write_ver}"
    );
    assert_eq!(
        read_ver, 1,
        "Readonly DB should NOT convert Legacy to WAL (read_version should stay 1), got {read_ver}"
    );
}

/// Test that readonly database cannot modify header when MVCC is requested
#[test]
fn test_readonly_legacy_db_with_mvcc_header_not_modified() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a Legacy mode database
    create_legacy_db(&db_path);

    // Get the original header versions
    let (orig_write_ver, orig_read_ver) = read_header_versions(&db_path);
    assert_eq!(orig_write_ver, 1, "Original should be Legacy mode");
    assert_eq!(orig_read_ver, 1, "Original should be Legacy mode");

    // Open with limbo in readonly mode with MVCC enabled
    let (write_ver, read_ver) = open_with_limbo_readonly_and_check(&db_path, true);

    // Header should NOT be modified - should still be Legacy mode
    // even though MVCC was requested
    assert_eq!(
        write_ver, 1,
        "Readonly DB should NOT convert Legacy to MVCC (write_version should stay 1), got {write_ver}"
    );
    assert_eq!(
        read_ver, 1,
        "Readonly DB should NOT convert Legacy to MVCC (read_version should stay 1), got {read_ver}"
    );
}

/// Test that readonly WAL database cannot be converted to MVCC
#[test]
fn test_readonly_wal_db_with_mvcc_header_not_modified() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    // Get the original header versions
    let (orig_write_ver, orig_read_ver) = read_header_versions(&db_path);
    assert_eq!(orig_write_ver, 2, "Original should be WAL mode");
    assert_eq!(orig_read_ver, 2, "Original should be WAL mode");

    // Open with limbo in readonly mode with MVCC enabled
    let (write_ver, read_ver) = open_with_limbo_readonly_and_check(&db_path, true);

    // Header should NOT be modified - should still be WAL mode
    // even though MVCC was requested
    assert_eq!(
        write_ver, 2,
        "Readonly DB should NOT convert WAL to MVCC (write_version should stay 2), got {write_ver}"
    );
    assert_eq!(
        read_ver, 2,
        "Readonly DB should NOT convert WAL to MVCC (read_version should stay 2), got {read_ver}"
    );
}

/// Test that PRAGMA journal_mode cannot change mode on readonly database
#[test]
fn test_readonly_pragma_journal_mode_cannot_change() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database
    create_wal_db(&db_path);

    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open database in readonly mode");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Try to switch to MVCC mode via PRAGMA - this should return an error
    // because we cannot change mode on readonly databases
    let result = conn.pragma_update("journal_mode", "'mvcc'");

    // The result should be a ReadOnly error
    assert!(
        matches!(result, Err(turso_core::LimboError::ReadOnly)),
        "PRAGMA journal_mode should return ReadOnly error on readonly database, got: {result:?}"
    );

    drop(conn);
    drop(db);

    // Verify header was NOT modified
    assert_that!(read_header_versions(&db_path)).is_equal_to((2, 2));
}

/// Test that readonly MVCC database can still be opened and read
#[test]
fn test_readonly_mvcc_db_can_be_read() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create a WAL mode database and convert to MVCC
    create_wal_db(&db_path);

    // First, convert to MVCC by opening in read-write mode and using PRAGMA
    {
        let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = DatabaseOpts::new();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .expect("Failed to open database");

        let conn = db.connect(ConnectOpts::new()).unwrap();

        // Switch to MVCC mode via PRAGMA
        conn.pragma_update("journal_mode", "'mvcc'")
            .expect("PRAGMA journal_mode = 'mvcc' should work");

        // Insert some data in MVCC mode
        conn.execute("INSERT INTO t (val) VALUES ('mvcc_readonly_test')")
            .expect("INSERT should work in MVCC mode");

        drop(conn);
        drop(db);
    }

    // Verify it's now MVCC mode
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));

    // Now open in readonly mode - this should work and we should be able to read data
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .expect("Failed to open MVCC database in readonly mode");

    let conn = db.connect(ConnectOpts::new()).unwrap();

    // Verify we can read data
    let mut stmt = conn
        .prepare("SELECT val FROM t WHERE val = 'mvcc_readonly_test'")
        .unwrap();
    let mut found = false;
    stmt.run_with_row_callback(|_| {
        found = true;
        Ok(())
    })
    .unwrap();
    assert!(found, "Should be able to read MVCC data in readonly mode");

    drop(conn);
    drop(db);

    // Verify header was NOT modified
    assert_that!(read_header_versions(&db_path)).is_equal_to((255, 255));
}

#[test]
fn test_readonly_first_writer_activation_preserves_readers() {
    for legacy in [false, true] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("activation.db");
        if legacy {
            create_legacy_db(&path);
        } else {
            create_wal_db(&path);
        }
        let original = std::fs::read(&path).unwrap();
        let io = Arc::new(turso_core::PlatformIO::new().unwrap());
        let open = |flags| {
            Database::open_file_with_flags(
                io.clone(),
                path.to_str().unwrap(),
                flags,
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
        };
        let db = open(OpenFlags::ReadOnly).unwrap();
        let reader = db.connect(ConnectOpts::new().with_readonly(true)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), original);
        reader.execute("BEGIN").unwrap();
        let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows, vec![(1,)]);
        let writer_db = if legacy {
            assert!(matches!(
                open(OpenFlags::None),
                Err(turso_core::LimboError::Busy)
            ));
            assert_eq!(std::fs::read(&path).unwrap(), original);
            reader.execute("COMMIT").unwrap();
            open(OpenFlags::None).unwrap()
        } else {
            open(OpenFlags::None).unwrap()
        };
        assert!(Arc::ptr_eq(&db, &writer_db));
        let writer = writer_db
            .connect(ConnectOpts::new().with_readonly(false))
            .unwrap();
        writer
            .execute("INSERT INTO t(val) VALUES ('writer')")
            .unwrap();
        if !legacy {
            match writer.checkpoint(turso_core::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            }) {
                Ok(result) => assert_eq!(result.wal_total_backfilled, 0),
                Err(turso_core::LimboError::Busy) => {}
                Err(error) => panic!("unexpected checkpoint error: {error}"),
            }
            let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
            assert_eq!(rows, vec![(1,)]);
            reader.execute("COMMIT").unwrap();
        }
        let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows, vec![(2,)]);
        let checkpoint_rows: Vec<(i64, i64, i64)> =
            reader.exec_rows("PRAGMA wal_checkpoint(TRUNCATE)");
        assert_eq!(checkpoint_rows, vec![(0, -1, -1)]);
        assert!(matches!(
            reader.checkpoint(turso_core::CheckpointMode::Truncate {
                upper_bound_inclusive: None
            }),
            Err(turso_core::LimboError::ReadOnly)
        ));
        reader.close().unwrap();
        writer
            .execute("INSERT INTO t(val) VALUES ('survivor')")
            .unwrap();
        writer.close().unwrap();
        drop(reader);
        drop(writer);
        drop(writer_db);
        drop(db);
        assert_eq!(read_header_versions(&path), (2, 2));
    }
}

#[test]
fn test_readonly_orphan_wal_is_preserved_until_writer_open() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source.db");
    let sqlite = rusqlite::Connection::open(&source).unwrap();
    sqlite.pragma_update(None, "journal_mode", "wal").unwrap();
    sqlite
        .execute_batch("CREATE TABLE orphan(x); INSERT INTO orphan VALUES(99)")
        .unwrap();
    let orphan = std::fs::read(format!("{}-wal", source.display())).unwrap();
    assert!(orphan.len() > 32);
    let path = dir.path().join("empty.db");
    let wal_path = format!("{}-wal", path.display());
    std::fs::write(&path, []).unwrap();
    std::fs::write(&wal_path, &orphan).unwrap();
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());
    let open = |flags| {
        Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            flags,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
    };
    let db = open(OpenFlags::ReadOnly).unwrap();
    let reader = db.connect(ConnectOpts::new().with_readonly(true)).unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("PRAGMA schema_version");
    assert_eq!(rows, vec![(0,)]);
    assert_eq!(std::fs::read(&path).unwrap(), Vec::<u8>::new());
    assert_eq!(std::fs::read(&wal_path).unwrap(), orphan);
    let writable = open(OpenFlags::None).unwrap();
    assert!(Arc::ptr_eq(&db, &writable));
    let writer = writable
        .connect(ConnectOpts::new().with_readonly(false))
        .unwrap();
    writer
        .execute("CREATE TABLE fresh(x); INSERT INTO fresh VALUES(1)")
        .unwrap();
    assert!(writer.prepare("SELECT * FROM orphan").is_err());
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT x FROM fresh");
    assert_eq!(rows, vec![(1,)]);
    reader.close().unwrap();
    writer.close().unwrap();
}

fn read_text_encoding(db_path: &Path) -> u32 {
    let bytes = std::fs::read(db_path).expect("Failed to read database file");
    assert!(
        bytes.len() >= 60,
        "Database file too small for encoding check"
    );
    // Text encoding is at offset 56, 4-byte big-endian
    u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]])
}

fn create_utf16le_db(db_path: &Path) {
    {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.pragma_update(None, "encoding", "UTF-16le").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ())
            .unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('test')", ())
            .unwrap();
    }

    let encoding = read_text_encoding(db_path);
    assert_eq!(encoding, 2, "Expected UTF-16le encoding=2, got {encoding}");
}

#[test]
fn test_utf16_db_returns_unsupported_encoding_error() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    create_utf16le_db(&db_path);

    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let result = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    );

    assert!(
        matches!(result, Err(turso_core::LimboError::UnsupportedEncoding(_))),
        "Opening UTF-16 database should return UnsupportedEncoding error, got: {result:?}"
    );

    if let Err(turso_core::LimboError::UnsupportedEncoding(msg)) = result {
        assert!(
            msg.contains("UTF-16le"),
            "Error message should mention UTF-16le encoding, got: {msg}"
        );
    }
}

fn external_sqlite_command(path: &Path, action: &str) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    let module = module_path!().split_once("::").unwrap().1;
    command
        .arg("--exact")
        .arg(format!("{module}::test_readonly_external_sqlite_process"))
        .arg("--nocapture")
        .env("TURSO_READONLY_EXTERNAL_PATH", path)
        .env("TURSO_READONLY_EXTERNAL_ACTION", action);
    command
}

#[test]
fn test_readonly_external_sqlite_process() {
    let Some(path) = std::env::var_os("TURSO_READONLY_EXTERNAL_PATH") else {
        return;
    };
    let action = std::env::var("TURSO_READONLY_EXTERNAL_ACTION").unwrap();
    if action == "lock" {
        use turso_core::IO;
        let io = turso_core::PlatformIO::new().unwrap();
        let _file = io
            .open_file(Path::new(&path).to_str().unwrap(), OpenFlags::None, false)
            .unwrap();
        return;
    }
    let connection = rusqlite::Connection::open(path).unwrap();
    if action == "hold_read" {
        use std::io::Write;
        connection.execute_batch("BEGIN; SELECT * FROM t;").unwrap();
        println!("SQLITE_READER_READY");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        connection.execute_batch("ROLLBACK").unwrap();
    } else {
        connection
            .execute("INSERT INTO t(val) VALUES ('external')", ())
            .unwrap();
        let count: i64 = connection
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        println!("EXTERNAL_SQLITE_COMMITTED count={count}");
    }
}

fn run_external_sqlite(path: &Path, action: &str) {
    let output = external_sqlite_command(path, action).output().unwrap();
    assert!(
        output.status.success(),
        "external SQLite failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if action == "write" {
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("EXTERNAL_SQLITE_COMMITTED count="),
            "external helper did not commit: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

fn hold_external_sqlite_reader(path: &Path) -> std::process::Child {
    use std::io::BufRead;
    let mut child = external_sqlite_command(path, "hold_read")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(stdout.read_line(&mut line).unwrap(), 0);
        if line.contains("SQLITE_READER_READY") {
            break;
        }
    }
    child.stdout = Some(stdout.into_inner());
    child
}

fn release_external_sqlite_reader(mut child: std::process::Child) {
    use std::io::Write;
    child.stdin.take().unwrap().write_all(b"release\n").unwrap();
    assert!(child.wait().unwrap().success());
}

fn open_readonly_activation_database(
    path: &Path,
    flags: OpenFlags,
) -> turso_core::Result<Arc<Database>> {
    Database::open_file_with_flags(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        flags,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
}

fn readonly_storage_bytes(path: &Path) -> Vec<Option<Vec<u8>>> {
    [
        path.to_path_buf(),
        std::path::PathBuf::from(format!("{}-wal", path.display())),
        path.with_extension("db-log"),
    ]
    .iter()
    .map(|path| match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("{}: {error}", path.display()),
    })
    .collect()
}

#[test]
fn test_readonly_allows_external_writer_and_rejects_stale_activation() {
    for legacy in [false, true] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("external.db");
        if legacy {
            create_legacy_db(&path);
        } else {
            create_wal_db(&path);
        }
        let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
        let reader = database
            .connect(ConnectOpts::new().with_readonly(true))
            .unwrap();
        let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows, vec![(1,)]);
        let initial = readonly_storage_bytes(&path);
        run_external_sqlite(&path, "write");
        let before = readonly_storage_bytes(&path);
        assert_ne!(
            initial, before,
            "external commit did not change storage; legacy={legacy}"
        );
        let changes: Vec<_> = initial
            .iter()
            .zip(&before)
            .enumerate()
            .map(|(index, (initial, current))| {
                (
                    index,
                    initial.as_ref().map(Vec::len),
                    current.as_ref().map(Vec::len),
                    initial == current,
                    initial.as_ref().map(|bytes| &bytes[..bytes.len().min(100)])
                        == current.as_ref().map(|bytes| &bytes[..bytes.len().min(100)]),
                )
            })
            .collect();
        let result = open_readonly_activation_database(&path, OpenFlags::None);
        assert!(
            matches!(result, Err(turso_core::LimboError::InvalidArgument(ref message)) if message.contains("storage changed")),
            "legacy={legacy}: writable activation succeeded={}, error={:?}, files(index,before_len,after_len,all_equal,header_equal)={changes:?}",
            result.is_ok(),
            result.as_ref().err()
        );
        assert_eq!(readonly_storage_bytes(&path), before);
        run_external_sqlite(&path, "write");
        assert!(matches!(
            reader.execute("INSERT INTO t(val) VALUES ('forbidden')"),
            Err(turso_core::LimboError::ReadOnly)
        ));
    }
}

#[test]
fn test_readonly_falls_back_while_external_reader_is_open() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("shared.db");
    create_wal_db(&path);
    let child = hold_external_sqlite_reader(&path);
    let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
    let reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(1,)]);
    assert!(database.is_readonly());
    assert!(matches!(
        open_readonly_activation_database(&path, OpenFlags::None),
        Err(turso_core::LimboError::InvalidArgument(_))
    ));
    release_external_sqlite_reader(child);
}

#[test]
fn test_readonly_activation_lock_failure_is_retryable() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("retry.db");
    create_legacy_db(&path);
    let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
    let reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    let child = hold_external_sqlite_reader(&path);
    assert!(matches!(
        open_readonly_activation_database(&path, OpenFlags::None),
        Err(turso_core::LimboError::LockingError(_))
    ));
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(1,)]);
    release_external_sqlite_reader(child);
    let writer_db = open_readonly_activation_database(&path, OpenFlags::None).unwrap();
    assert!(Arc::ptr_eq(&database, &writer_db));
    let writer = writer_db
        .connect(ConnectOpts::new().with_readonly(false))
        .unwrap();
    writer
        .execute("INSERT INTO t(val) VALUES ('retry')")
        .unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);
}

#[test]
fn test_readonly_bootstrap_failure_releases_main_lock() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("invalid.db");
    std::fs::write(&path, [0xff; 4096]).unwrap();
    assert!(open_readonly_activation_database(&path, OpenFlags::ReadOnly).is_err());
    run_external_sqlite(&path, "lock");
}

#[test]
fn test_readonly_activation_rejects_replaced_wal() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("replaced.db");
    create_wal_db(&path);
    let wal_path = format!("{}-wal", path.display());
    std::fs::write(&wal_path, []).unwrap();
    let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
    let _reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    std::fs::remove_file(&wal_path).unwrap();
    std::fs::write(&wal_path, []).unwrap();
    assert!(
        matches!(open_readonly_activation_database(&path, OpenFlags::None), Err(turso_core::LimboError::InvalidArgument(ref message)) if message.contains("storage changed"))
    );
}

#[test]
fn test_readonly_mvcc_activation_preserves_snapshot() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("mvcc-activation.db");
    create_wal_db(&path);
    {
        let database = open_readonly_activation_database(&path, OpenFlags::Create).unwrap();
        let connection = database.connect(ConnectOpts::new()).unwrap();
        connection.pragma_update("journal_mode", "'mvcc'").unwrap();
    }
    let before = readonly_storage_bytes(&path);
    let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
    let reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    reader.execute("BEGIN").unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(1,)]);
    assert_eq!(readonly_storage_bytes(&path), before);
    let writer_db = open_readonly_activation_database(&path, OpenFlags::None).unwrap();
    assert!(Arc::ptr_eq(&database, &writer_db));
    let writer = writer_db
        .connect(ConnectOpts::new().with_readonly(false))
        .unwrap();
    assert!(!writer.is_readonly(turso_core::MAIN_DB_ID));
    writer
        .execute("INSERT INTO t(val) VALUES ('writer')")
        .unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(1,)]);
    reader.execute("COMMIT").unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);
    assert!(matches!(
        reader.execute("INSERT INTO t(val) VALUES ('forbidden')"),
        Err(turso_core::LimboError::ReadOnly)
    ));
    reader.execute("CREATE TEMP TABLE local(x)").unwrap();
    reader.execute("INSERT INTO local VALUES (1)").unwrap();
    drop(writer);
    drop(writer_db);
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);
}

#[test]
fn test_readonly_mvcc_activation_rejects_changed_log() {
    use std::io::Write;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("changed-log.db");
    create_wal_db(&path);
    {
        let database = open_readonly_activation_database(&path, OpenFlags::Create).unwrap();
        let connection = database.connect(ConnectOpts::new()).unwrap();
        connection.pragma_update("journal_mode", "'mvcc'").unwrap();
    }
    let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
    let _reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(path.with_extension("db-log"))
        .unwrap()
        .write_all(&[0])
        .unwrap();
    let before = readonly_storage_bytes(&path);
    assert!(
        matches!(open_readonly_activation_database(&path, OpenFlags::None), Err(turso_core::LimboError::InvalidArgument(ref message)) if message.contains("storage changed"))
    );
    assert_eq!(readonly_storage_bytes(&path), before);
}

#[cfg(unix)]
#[test]
fn test_readonly_mvcc_alias_activates_canonical_log() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("canonical.db");
    let alias = directory.path().join("alias.db");
    let decoy = alias.with_extension("db-log");
    create_wal_db(&path);
    {
        let database = open_readonly_activation_database(&path, OpenFlags::Create).unwrap();
        let connection = database.connect(ConnectOpts::new()).unwrap();
        connection.pragma_update("journal_mode", "'mvcc'").unwrap();
    }
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    std::fs::write(&decoy, b"unrelated alias log").unwrap();
    {
        let database = open_readonly_activation_database(&alias, OpenFlags::ReadOnly).unwrap();
        let reader = database
            .connect(ConnectOpts::new().with_readonly(true))
            .unwrap();
        let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows, vec![(1,)]);
        let writer_db = open_readonly_activation_database(&alias, OpenFlags::None).unwrap();
        let writer = writer_db
            .connect(ConnectOpts::new().with_readonly(false))
            .unwrap();
        writer
            .execute("INSERT INTO t(val) VALUES ('canonical writer')")
            .unwrap();
        assert_eq!(std::fs::read(&decoy).unwrap(), b"unrelated alias log");
    }
    let database = open_readonly_activation_database(&path, OpenFlags::ReadOnly).unwrap();
    let reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);
}

#[cfg(unix)]
#[test]
fn test_readonly_bootstrap_rejects_wal_alias_of_main() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("aliased-sidecar.db");
    create_wal_db(&path);
    let wal_path = format!("{}-wal", path.display());
    std::fs::hard_link(&path, &wal_path).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(
        matches!(open_readonly_activation_database(&path, OpenFlags::ReadOnly), Err(turso_core::LimboError::InvalidArgument(ref message)) if message.contains("storage changed"))
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    run_external_sqlite(&path, "lock");
}

#[test]
fn test_readonly_no_lock_does_not_activate_writer_authority() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("no-lock.db");
    create_wal_db(&path);
    let database =
        open_readonly_activation_database(&path, OpenFlags::ReadOnly | OpenFlags::NoLock).unwrap();
    let reader = database
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    assert!(database.is_readonly());
    let rows: Vec<(i64,)> = reader.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(1,)]);
    assert!(
        matches!(open_readonly_activation_database(&path, OpenFlags::None), Err(turso_core::LimboError::InvalidArgument(ref message)) if message.contains("cannot be activated"))
    );
    run_external_sqlite(&path, "write");
}
