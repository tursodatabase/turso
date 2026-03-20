use std::sync::Arc;
use turso_core::{Database, OpenFlags};

/// Regression test: DATABASE_MANAGER registry returns stale Database after
/// the underlying file is renamed.
///
/// Steps:
///   1. Open database "A.db", create a table and insert data
///   2. Close all connections and drop the Database
///   3. Rename A.db -> B.db on disk
///   4. Open a *new* database at path "A.db"
///   5. The new A.db should be a fresh, empty database
#[test]
fn test_database_rename_registry_stale() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path_a = tmp_dir.path().join("A.db");
    let path_b = tmp_dir.path().join("B.db");

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    // 1. Open database A and populate it.
    let db_a = Database::open_file_with_flags(
        io.clone(),
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    let conn_a = db_a.connect().unwrap();
    conn_a.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn_a.execute("INSERT INTO t VALUES (42)").unwrap();

    // 2. Close all connections and drop the Database.
    drop(conn_a);
    drop(db_a);

    // 3. Rename A.db -> B.db on disk.
    std::fs::rename(&path_a, &path_b).unwrap();
    let wal_a = tmp_dir.path().join("A.db-wal");
    let wal_b = tmp_dir.path().join("B.db-wal");
    if wal_a.exists() {
        std::fs::rename(&wal_a, &wal_b).unwrap();
    }
    let shm_a = tmp_dir.path().join("A.db-shm");
    let shm_b = tmp_dir.path().join("B.db-shm");
    if shm_a.exists() {
        std::fs::rename(&shm_a, &shm_b).unwrap();
    }

    // 4. Open a new database at the original path A.db.
    let db_a2 = Database::open_file_with_flags(
        io.clone(),
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    // 5. The new A.db should be empty — querying table 't' should fail.
    let conn_a2 = db_a2.connect().unwrap();
    let result = conn_a2.execute("SELECT x FROM t");
    assert!(
        result.is_err(),
        "New database at A.db should not have table 't' — \
         DATABASE_MANAGER returned stale Database after rename"
    );
}

/// Regression test: a leaked (un-finalized) Statement keeps the Database alive
/// in DATABASE_MANAGER via the Arc chain Statement -> Program -> Arc<Connection>
/// -> Arc<Database>.
///
/// This is the core-level scenario that the React Native (and JavaScript)
/// bindings must guard against: if Database.close() does not dispose outstanding
/// statements, reopening the same path after a rename returns the stale database.
///
/// Steps:
///   1. Open database "A.db", create a table and insert data
///   2. Prepare a statement but keep it alive (simulating un-GC'd JS Statement)
///   3. Drop the Connection and Database handles
///   4. Rename A.db -> B.db on disk
///   5. Open a new database at path "A.db"
///   6. The new A.db must be stale because the leaked statement kept the old
///      database alive — demonstrating the bug the bindings fix prevents
#[test]
fn test_leaked_statement_keeps_database_alive() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path_a = tmp_dir.path().join("A.db");
    let path_b = tmp_dir.path().join("B.db");

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    // 1. Open database A and populate it.
    let db_a = Database::open_file_with_flags(
        io.clone(),
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    let conn_a = db_a.connect().unwrap();
    conn_a.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn_a.execute("INSERT INTO t VALUES (42)").unwrap();

    // 2. Prepare a statement and keep it alive — this simulates an un-GC'd
    //    JS/RN Statement object that has not been finalized.
    let leaked_stmt = conn_a.prepare("SELECT x FROM t").unwrap();

    // 3. Drop the connection and database handles. The leaked statement still
    //    holds Arc<Connection> -> Arc<Database>, so DATABASE_MANAGER's Weak
    //    remains upgradeable.
    drop(conn_a);
    drop(db_a);

    // 4. Rename A.db -> B.db on disk.
    std::fs::rename(&path_a, &path_b).unwrap();
    let wal_a = tmp_dir.path().join("A.db-wal");
    let wal_b = tmp_dir.path().join("B.db-wal");
    if wal_a.exists() {
        std::fs::rename(&wal_a, &wal_b).unwrap();
    }
    let shm_a = tmp_dir.path().join("A.db-shm");
    let shm_b = tmp_dir.path().join("B.db-shm");
    if shm_a.exists() {
        std::fs::rename(&shm_a, &shm_b).unwrap();
    }

    // 5. Open a new database at the original path A.db.
    let db_a2 = Database::open_file_with_flags(
        io.clone(),
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    // 6. Because the leaked statement kept the old Database alive in the
    //    registry, DATABASE_MANAGER returns the stale instance — table 't'
    //    is visible even though the file was renamed away.  This demonstrates
    //    the bug that the bindings-level fix (disposing statements on close)
    //    prevents.
    let conn_a2 = db_a2.connect().unwrap();
    let result = conn_a2.execute("SELECT x FROM t");
    assert!(
        result.is_ok(),
        "Leaked statement should keep old Database alive in registry — \
         table 't' should still be visible via the stale Database"
    );

    // Cleanup: drop the leaked statement so the old database can be freed.
    drop(leaked_stmt);
}
