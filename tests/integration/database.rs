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
