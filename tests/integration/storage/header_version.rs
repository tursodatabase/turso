use std::path::Path;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags};

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
    let opts = DatabaseOpts::new().with_mvcc(enable_mvcc);

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

    // Do a simple query to ensure everything is initialized
    if let Some(mut stmt) = conn.query("SELECT 1").unwrap() {
        loop {
            let result = stmt.step().unwrap();
            match result {
                turso_core::StepResult::Row => {
                    let _row = stmt.row().unwrap();
                }
                turso_core::StepResult::IO => {
                    stmt.run_once().unwrap();
                    continue;
                }
                turso_core::StepResult::Done => break,
                r => panic!("unexpected result {r:?}: expecting single row"),
            }
        }
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
