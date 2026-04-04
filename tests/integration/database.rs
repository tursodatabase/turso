use std::sync::Arc;
use turso_core::{Database, OpenFlags};
use turso_sdk_kit::rsapi::{TursoDatabase, TursoDatabaseConfig, TursoStatusCode};

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

/// Regression test: TursoConnection.close() must finalize outstanding statements
/// so that the Statement → Arc<Connection> → Arc<Database> chain is broken.
///
/// Without the fix, un-finalized statements keep the Database alive in
/// DATABASE_MANAGER. After a file rename, reopening the same path returns
/// the stale Database.
///
/// This exercises the SDK-level fix: TursoConnection tracks all prepared
/// statements and close() drops them before closing the connection.
#[test]
fn test_sdk_close_finalizes_leaked_statements() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path_a = tmp_dir.path().join("A.db");
    let path_b = tmp_dir.path().join("B.db");

    // 1. Open database A via SDK and populate it.
    let db_a = TursoDatabase::new(TursoDatabaseConfig {
        path: path_a.to_str().unwrap().to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: None,
        io: None,
        db_file: None,
        flags: None,
    });
    let _ = db_a.open().unwrap();
    let conn_a = db_a.connect().unwrap();

    let mut create_stmt = conn_a.prepare_single("CREATE TABLE t(x INTEGER)").unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut insert_stmt = conn_a.prepare_single("INSERT INTO t VALUES (42)").unwrap();
    assert_eq!(
        insert_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    // 2. Prepare a statement but do NOT finalize or drop it — simulates an
    //    un-GC'd binding-level Statement object.
    let mut leaked_stmt = conn_a.prepare_single("SELECT x FROM t").unwrap();
    assert_eq!(leaked_stmt.step(None).unwrap(), TursoStatusCode::Row);

    // 3. close() finalizes all outstanding statements (the fix).
    //    This drops the inner turso_core::Statement, releasing the
    //    Arc<Connection> → Arc<Database> chain.
    conn_a.close().unwrap();

    // The leaked statement should now be finalized.
    assert!(leaked_stmt.step(None).is_err());

    // 4. Drop remaining handles.
    drop(leaked_stmt);
    drop(create_stmt);
    drop(insert_stmt);
    drop(conn_a);
    drop(db_a);

    // 5. Rename A.db → B.db on disk.
    std::fs::rename(&path_a, &path_b).unwrap();
    for ext in &["-wal", "-shm"] {
        let from = tmp_dir.path().join(format!("A.db{ext}"));
        let to = tmp_dir.path().join(format!("B.db{ext}"));
        if from.exists() {
            std::fs::rename(&from, &to).unwrap();
        }
    }

    // 6. Open a new database at the original path A.db.
    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db_a2 = Database::open_file_with_flags(
        io,
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    // 7. The new A.db should be empty — table 't' should NOT exist.
    let conn_a2 = db_a2.connect().unwrap();
    let result = conn_a2.execute("SELECT x FROM t");
    assert!(
        result.is_err(),
        "New database at A.db should not have table 't' — \
         close() should have finalized statements and released the stale Database"
    );
}
