use crate::common::limbo_exec_rows;
use rusqlite::types::Value;
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, LimboError, OpenFlags, SqliteDialect};

fn open(
    io: &Arc<dyn turso_core::IO>,
    path: &str,
    flags: OpenFlags,
) -> turso_core::Result<Arc<Database>> {
    Database::open_file_with_flags(
        io.clone(),
        path,
        flags,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
}

fn create_db_with_table(io: &Arc<dyn turso_core::IO>, path: &str) {
    let db = open(io, path, OpenFlags::Create).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
}

#[test]
fn readonly_open_while_file_is_open_read_write_gives_readonly_connection() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("rw_then_ro.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    create_db_with_table(&io, path);

    let db_rw = open(&io, path, OpenFlags::default()).unwrap();
    let conn_rw = db_rw.connect().unwrap();

    let db_ro = open(&io, path, OpenFlags::ReadOnly).unwrap();
    assert!(Arc::ptr_eq(&db_rw, &db_ro));
    let conn_ro = db_ro.connect_with_flags(OpenFlags::ReadOnly).unwrap();
    assert!(conn_ro.is_readonly(turso_core::MAIN_DB_ID));
    assert!(!conn_rw.is_readonly(turso_core::MAIN_DB_ID));

    let err = conn_ro
        .execute("INSERT INTO t VALUES (2)")
        .expect_err("read-only connection wrote to the database");
    assert!(matches!(err, LimboError::ReadOnly), "{err:?}");

    conn_rw.execute("INSERT INTO t VALUES (3)").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn_ro, "SELECT x FROM t ORDER BY x"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn closing_last_readonly_connection_does_not_checkpoint() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("ro_close.db");
    let path = path.to_str().unwrap();
    let wal_path = format!("{path}-wal");
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    create_db_with_table(&io, path);

    let db = open(&io, path, OpenFlags::default()).unwrap();
    let conn_rw = db.connect().unwrap();
    let conn_ro = db.connect_with_flags(OpenFlags::ReadOnly).unwrap();
    conn_rw.execute("INSERT INTO t VALUES (2)").unwrap();
    let wal_size_after_write = std::fs::metadata(&wal_path).unwrap().len();
    assert!(wal_size_after_write > 0);

    conn_rw.close().unwrap();
    conn_ro.close().unwrap();
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        wal_size_after_write,
        "closing the last connection checkpointed the WAL through a read-only connection"
    );
}

#[test]
fn readonly_connection_attaches_databases_readonly() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("main.db");
    let path = path.to_str().unwrap();
    let other_path = tmp_dir.path().join("other.db");
    let other_path = other_path.to_str().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    create_db_with_table(&io, path);
    create_db_with_table(&io, other_path);

    let db = Database::open_file_with_flags(
        io.clone(),
        path,
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn_ro = db.connect_with_flags(OpenFlags::ReadOnly).unwrap();
    conn_ro
        .execute(format!("ATTACH '{other_path}' AS other"))
        .unwrap();
    let err = conn_ro
        .execute("INSERT INTO other.t VALUES (2)")
        .expect_err("read-only connection wrote to an attached database");
    assert!(matches!(err, LimboError::ReadOnly), "{err:?}");
}

#[test]
fn read_write_open_fails_while_file_is_open_readonly() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("ro_then_rw.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    create_db_with_table(&io, path);

    let db_ro = open(&io, path, OpenFlags::ReadOnly).unwrap();
    let _conn_ro = db_ro.connect().unwrap();
    assert!(db_ro.is_readonly());

    let err = open(&io, path, OpenFlags::default())
        .expect_err("read-write open returned the cached read-only Database");
    assert!(matches!(err, LimboError::InvalidArgument(_)), "{err:?}");

    drop(_conn_ro);
    drop(db_ro);
    let db_rw = open(&io, path, OpenFlags::default()).unwrap();
    assert!(!db_rw.is_readonly());
    db_rw
        .connect()
        .unwrap()
        .execute("INSERT INTO t VALUES (2)")
        .unwrap();
}

#[test]
fn opens_with_the_same_mode_share_one_database() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("same_mode.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    create_db_with_table(&io, path);

    let first_ro = open(&io, path, OpenFlags::ReadOnly).unwrap();
    let second_ro = open(&io, path, OpenFlags::ReadOnly).unwrap();
    assert!(Arc::ptr_eq(&first_ro, &second_ro));
    drop(first_ro);
    drop(second_ro);

    let first_rw = open(&io, path, OpenFlags::default()).unwrap();
    let second_rw = open(&io, path, OpenFlags::default()).unwrap();
    assert!(Arc::ptr_eq(&first_rw, &second_rw));
}

#[test]
fn readonly_open_cannot_write_after_disabling_query_only() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("ro_query_only_off.db");
    let path = path.to_str().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    create_db_with_table(&io, path);

    let db_ro = open(&io, path, OpenFlags::ReadOnly).unwrap();
    let conn_ro = db_ro.connect().unwrap();
    conn_ro.execute("PRAGMA query_only = false").unwrap();
    assert!(!conn_ro.get_query_only());
    assert!(
        conn_ro.execute("INSERT INTO t VALUES (2)").is_err(),
        "read-only open allowed a write after PRAGMA query_only = false"
    );
}
