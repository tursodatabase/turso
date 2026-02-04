use std::path::Path;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags};

use crate::common::ExecRows;

/// Read header version bytes (write_version at offset 18, read_version at offset 19) from database file
fn read_header_versions(db_path: &Path) -> (u8, u8) {
    let bytes = std::fs::read(db_path).expect("Failed to read database file");
    assert!(bytes.len() >= 20, "Database file too small");
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
    let (write_ver, read_ver) = read_header_versions(db_path);
    assert_eq!(
        write_ver, 1,
        "Expected Legacy write_version=1, got {write_ver}"
    );
    assert_eq!(
        read_ver, 1,
        "Expected Legacy read_version=1, got {read_ver}"
    );
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
    let (write_ver, read_ver) = read_header_versions(db_path);
    assert_eq!(
        write_ver, 2,
        "Expected WAL write_version=2, got {write_ver}"
    );
    assert_eq!(read_ver, 2, "Expected WAL read_version=2, got {read_ver}");
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
    )
    .expect("Failed to open database with limbo");

    // Connect to ensure initialization is complete
    let conn = db.connect().unwrap();

    // Enable MVCC if requested
    if enable_mvcc {
        conn.pragma_update("journal_mode", "'experimental_mvcc'")
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
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 255, "Should be MVCC after first open");
    assert_eq!(read_ver, 255, "Should be MVCC after first open");

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
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 255, "Should be MVCC after first open");
    assert_eq!(read_ver, 255, "Should be MVCC after first open");

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
    let (write_ver, read_ver) = read_header_versions(db_path);
    assert_eq!(
        write_ver, 2,
        "Expected WAL write_version=2, got {write_ver}"
    );
    assert_eq!(read_ver, 2, "Expected WAL read_version=2, got {read_ver}");
}

/// Test switching from WAL to MVCC mode via PRAGMA when there's a non-empty WAL.
/// This tests the scenario where:
/// 1. A database is opened in WAL mode with pending WAL data
/// 2. User runs `PRAGMA journal_mode = "experimental_mvcc"` to switch to MVCC
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
        )
        .expect("Failed to open database with limbo (non-MVCC)");

        let conn = db.connect().unwrap();

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
    )
    .expect("Failed to open database with limbo (MVCC enabled)");

    let conn = db.connect().unwrap();

    // Switch to MVCC mode via PRAGMA
    // This should work even with pending WAL data
    let result = conn
        .pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("PRAGMA journal_mode update should not fail");

    // Verify the journal mode was set
    assert!(!result.is_empty(), "PRAGMA should return a result");
    let mode = result[0][0].to_string();
    assert_eq!(
        mode, "experimental_mvcc",
        "Journal mode should be experimental_mvcc after PRAGMA, got {mode}"
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
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(
        write_ver, 255,
        "After PRAGMA journal_mode switch, write_version should be 255 (MVCC), got {write_ver}"
    );
    assert_eq!(
        read_ver, 255,
        "After PRAGMA journal_mode switch, read_version should be 255 (MVCC), got {read_ver}"
    );
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
        )
        .expect("Failed to open database with limbo");

        let conn = db.connect().unwrap();

        // Switch to MVCC mode via PRAGMA
        conn.pragma_update("journal_mode", "'experimental_mvcc'")
            .expect("PRAGMA journal_mode = 'experimental_mvcc' should work");

        // Insert some data in MVCC mode
        conn.execute("INSERT INTO t (val) VALUES ('mvcc_data')")
            .expect("INSERT should work in MVCC mode");

        drop(conn);
        drop(db);
    }

    // Verify it's now MVCC mode
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 255, "Should be MVCC after first open");
    assert_eq!(read_ver, 255, "Should be MVCC after first open");

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
    )
    .expect("Failed to open MVCC database with limbo");

    let conn = db.connect().unwrap();

    let result = conn
        .pragma_query("journal_mode")
        .expect("PRAGMA journal_mode update should not fail");

    assert!(!result.is_empty(), "PRAGMA should return a result");
    let mode = result[0][0].to_string();
    assert_eq!(
        mode, "experimental_mvcc",
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
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(
        write_ver, 2,
        "After PRAGMA journal_mode switch to WAL, write_version should be 2, got {write_ver}"
    );
    assert_eq!(
        read_ver, 2,
        "After PRAGMA journal_mode switch to WAL, read_version should be 2, got {read_ver}"
    );
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
    )
    .expect("Failed to open database");

    let conn = db.connect().unwrap();

    // Switch to MVCC
    let result = conn
        .pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("Switch to MVCC should work");
    assert_eq!(result[0][0].to_string(), "experimental_mvcc");

    // Verify header is MVCC (version 255)
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 255, "mode should be MVCC (write_version=255)");
    assert_eq!(read_ver, 255, "mode should be MVCC (read_version=255)");

    // Insert data in MVCC mode
    conn.execute("INSERT INTO t (val) VALUES ('after_mvcc_switch')")
        .expect("INSERT should work");

    // Switch back to WAL
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("Switch to WAL should work");
    assert_eq!(result[0][0].to_string(), "wal");

    // Verify header is MVCC (version 255)
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 2, "mode should be WAL (write_version=2)");
    assert_eq!(read_ver, 2, "mode should be WAL (read_version=2)");

    // Insert data in WAL mode
    conn.execute("INSERT INTO t (val) VALUES ('after_wal_switch')")
        .expect("INSERT should work");

    // Switch to MVCC again
    let result = conn
        .pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("Switch to MVCC again should work");
    assert_eq!(result[0][0].to_string(), "experimental_mvcc");

    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 255, "mode should be MVCC (write_version=255)");
    assert_eq!(read_ver, 255, "mode should be MVCC (read_version=255)");

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
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(
        write_ver, 255,
        "Final mode should be MVCC (write_version=255)"
    );
    assert_eq!(
        read_ver, 255,
        "Final mode should be MVCC (read_version=255)"
    );
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
    )
    .expect("Failed to open database");

    let conn = db.connect().unwrap();

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

    // Now open and switch to MVCC via PRAGMA, then verify query returns experimental_mvcc
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

    // Switch to MVCC mode via PRAGMA
    conn.pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("PRAGMA journal_mode = 'experimental_mvcc' should work");

    // Query current journal mode (should be experimental_mvcc after switching)
    if let Some(mut stmt) = conn.query("PRAGMA journal_mode").unwrap() {
        stmt.run_with_row_callback(|row| {
            let mode: String = row.get::<String>(0).unwrap();
            assert_eq!(
                mode, "experimental_mvcc",
                "Mode should be experimental_mvcc after PRAGMA switch, got {mode}"
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
        )
        .expect("Failed to open database");

        let conn = db.connect().unwrap();

        // Switch to MVCC
        conn.pragma_update("journal_mode", "'experimental_mvcc'")
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
        )
        .expect("Failed to reopen database");

        let conn = db.connect().unwrap();

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
    )
    .expect("Failed to open database with limbo in readonly mode");

    // Connect to ensure initialization is complete
    let conn = db.connect().unwrap();

    // Try to enable MVCC if requested (will fail in readonly mode, which is expected)
    if enable_mvcc {
        let _ = conn.pragma_update("journal_mode", "'experimental_mvcc'");
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
    )
    .expect("Failed to open database in readonly mode");

    let conn = db.connect().unwrap();

    // Try to switch to MVCC mode via PRAGMA - this should return an error
    // because we cannot change mode on readonly databases
    let result = conn.pragma_update("journal_mode", "'experimental_mvcc'");

    // The result should be a ReadOnly error
    assert!(
        matches!(result, Err(turso_core::LimboError::ReadOnly)),
        "PRAGMA journal_mode should return ReadOnly error on readonly database, got: {result:?}"
    );

    drop(conn);
    drop(db);

    // Verify header was NOT modified
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(
        write_ver, 2,
        "Readonly DB header should NOT be modified (write_version should stay 2), got {write_ver}"
    );
    assert_eq!(
        read_ver, 2,
        "Readonly DB header should NOT be modified (read_version should stay 2), got {read_ver}"
    );
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
        )
        .expect("Failed to open database");

        let conn = db.connect().unwrap();

        // Switch to MVCC mode via PRAGMA
        conn.pragma_update("journal_mode", "'experimental_mvcc'")
            .expect("PRAGMA journal_mode = 'experimental_mvcc' should work");

        // Insert some data in MVCC mode
        conn.execute("INSERT INTO t (val) VALUES ('mvcc_readonly_test')")
            .expect("INSERT should work in MVCC mode");

        drop(conn);
        drop(db);
    }

    // Verify it's now MVCC mode
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(write_ver, 255, "Should be MVCC after first open");
    assert_eq!(read_ver, 255, "Should be MVCC after first open");

    // Now open in readonly mode - this should work and we should be able to read data
    let io = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new();

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        opts,
        None,
    )
    .expect("Failed to open MVCC database in readonly mode");

    let conn = db.connect().unwrap();

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
    let (write_ver, read_ver) = read_header_versions(&db_path);
    assert_eq!(
        write_ver, 255,
        "Readonly MVCC DB header should NOT be modified (write_version should stay 255), got {write_ver}"
    );
    assert_eq!(
        read_ver, 255,
        "Readonly MVCC DB header should NOT be modified (read_version should stay 255), got {read_ver}"
    );
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
