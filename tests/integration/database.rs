use asserting::prelude::*;
use std::sync::Arc;
use turso::IoBackend;
use turso_core::SqliteDialect;
use turso_core::{ConnectOpts, Database, OpenFlags};
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
        Arc::new(SqliteDialect),
    )
    .unwrap();

    let conn_a = db_a
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
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
        Arc::new(SqliteDialect),
    )
    .unwrap();

    // 5. The new A.db should be empty — querying table 't' should fail.
    let conn_a2 = db_a2
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
    // A new database at A.db has no table `t`. Before the fix,
    // DATABASE_MANAGER returned the stale Database after the rename.
    assert_that!(conn_a2.execute("SELECT x FROM t")).is_err();
}

fn open_plain_file(path: &std::path::Path) -> turso_core::Result<Arc<Database>> {
    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
}

/// SQLite must refuse the file as "file is not a database" (SQLITE_NOTADB).
/// Opening is lazy in SQLite, so run a query to make it read the header.
fn assert_sqlite_says_not_a_database(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let err = conn
        .query_row("SELECT count(*) FROM sqlite_schema", [], |_| Ok(()))
        .unwrap_err();
    let rusqlite::Error::SqliteFailure(failure, _) = &err else {
        panic!("expected SQLITE_NOTADB from sqlite, got {err:?}");
    };
    assert_that!(failure.code).is_equal_to(rusqlite::ErrorCode::NotADatabase);
}

fn assert_turso_says_not_a_database(path: &std::path::Path) {
    match open_plain_file(path) {
        Err(turso_core::LimboError::NotADB) => {}
        Err(other) => panic!("expected NotADB for {}, got {other:?}", path.display()),
        Ok(_) => panic!(
            "expected NotADB for {}, got a successful open",
            path.display()
        ),
    }
}

/// A file whose header is garbage must report NotADB ("file is not a
/// database"), like SQLite, and not a corruption error derived from a
/// garbage header field such as the page size. The corrupt8.test cascade
/// started with a garbage header being reported as "invalid page size",
/// which the sqlite3 compat layer then surfaced as "out of memory".
#[test]
fn test_open_garbage_header_reports_not_a_database() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("garbage.db");
    // 1050 zero bytes: no SQLite magic, page size field reads as 0.
    std::fs::write(&path, vec![0u8; 1050]).unwrap();

    assert_sqlite_says_not_a_database(&path);
    assert_turso_says_not_a_database(&path);
}

/// A non-empty file shorter than the 512-byte header can never be a valid
/// database. SQLite zero-fills the missing bytes and then rejects the header,
/// so it reports "file is not a database" whether or not the file starts
/// with the SQLite magic.
#[test]
fn test_open_short_file_reports_not_a_database() {
    let tmp_dir = tempfile::TempDir::new().unwrap();

    let garbage = tmp_dir.path().join("short-garbage.db");
    std::fs::write(&garbage, vec![0x42u8; 100]).unwrap();
    assert_sqlite_says_not_a_database(&garbage);
    assert_turso_says_not_a_database(&garbage);

    let truncated = tmp_dir.path().join("short-truncated.db");
    let mut bytes = b"SQLite format 3\0".to_vec();
    bytes.resize(100, 0);
    std::fs::write(&truncated, bytes).unwrap();
    assert_sqlite_says_not_a_database(&truncated);
    assert_turso_says_not_a_database(&truncated);
}

/// Turso has no fts5 module. Opening a file that has an fts5 table must fail:
/// with only part of the schema loaded, writes skip index updates and
/// corrupt the file.
#[test]
fn test_open_refuses_file_with_unknown_virtual_table_module() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("fts5.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE a(x); CREATE INDEX ax ON a(x);
             CREATE VIRTUAL TABLE f USING fts5(b);
             CREATE TABLE z(y);",
        )
        .unwrap();
    drop(sqlite);

    match open_plain_file(&path) {
        Err(turso_core::LimboError::ExtensionError(msg)) => {
            assert_that!(msg).contains("fts5");
        }
        Err(other) => panic!("expected missing fts5 module error, got {other:?}"),
        Ok(_) => panic!("expected missing fts5 module error, got a successful open"),
    }
}

/// The SQLite dialect must refuse a table written by the PostgreSQL frontend,
/// as sqlite3 does. Reading it as a plain SQLite table drops the PostgreSQL
/// column types and rowid alias, so writes store values tursopg cannot read.
#[test]
fn test_open_refuses_file_with_postgres_table() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("postgres.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE t(id serial PRIMARY KEY, n numeric(10,2));
             PRAGMA writable_schema = ON;
             UPDATE sqlite_schema SET sql = '/* turso_frontend:postgres */ ' || sql
              WHERE name = 't';",
        )
        .unwrap();
    drop(sqlite);

    let err = match open_plain_file(&path) {
        Err(err) => err,
        Ok(_) => panic!("expected malformed schema error, got a successful open"),
    };
    let turso_core::LimboError::Corrupt(msg) = err else {
        panic!("expected malformed schema error, got {err:?}");
    };
    assert_that!(msg).contains("malformed database schema");
}

/// The SQLite dialect has no roles, so a table named like the roles table of
/// the PostgreSQL frontend is an ordinary table, as it is to sqlite3.
#[test]
fn test_open_file_with_table_named_like_the_roles_table() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("roles.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE __turso_internal_roles(x);
             INSERT INTO __turso_internal_roles VALUES (1);
             CREATE TABLE t(a);
             INSERT INTO t VALUES (5);",
        )
        .unwrap();
    drop(sqlite);

    let db = open_plain_file(&path).unwrap();
    let conn = db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
    conn.force_reparse_schema_without_publish().unwrap();

    let mut stmt = conn.prepare("SELECT a FROM t").unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_that!(rows).is_equal_to(vec![vec![turso_core::Value::from_i64(5)]]);
}

/// A read error while a connection checks whether the schema changed must end
/// the read transaction the check started, or checkpoints stay busy and the
/// connection's next transaction panics.
#[test]
fn test_failed_schema_check_ends_its_read_transaction() {
    use crate::queued_io::{QueuedIo, QueuedIoOpKind};

    let io = Arc::new(QueuedIo::new());
    let db =
        Database::open_file(io.clone(), "schema-check-fault.db", Arc::new(SqliteDialect)).unwrap();
    let conn1 = db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
    conn1.execute("CREATE TABLE t(x)").unwrap();
    conn1.execute("INSERT INTO t VALUES (1)").unwrap();
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
    // The schema cookie is then read from the database file, where the
    // fault is.
    conn2.get_pager().clear_page_cache(false);

    io.fault_after("schema-check-fault.db", QueuedIoOpKind::Pread, 0);
    assert!(conn2.maybe_reparse_schema().is_err());
    io.clear_fault();

    conn1.execute("INSERT INTO t VALUES (2)").unwrap();
    let mut checkpoint = conn1.prepare("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let rows = checkpoint.run_collect_rows().unwrap();
    assert_that!(rows).is_equal_to(vec![vec![
        turso_core::Value::from_i64(0),
        turso_core::Value::from_i64(0),
        turso_core::Value::from_i64(0),
    ]]);
    // The read that failed may still fail the insert, as it does on main, but
    // the insert must not find a read transaction left open.
    conn2.execute("BEGIN").unwrap();
    let _ = conn2.execute("INSERT INTO t VALUES (3)");
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
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
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
        Arc::new(SqliteDialect),
    )
    .unwrap();

    // 7. The new A.db should be empty — table 't' should NOT exist.
    let conn_a2 = db_a2
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
    let result = conn_a2.execute("SELECT x FROM t");
    assert!(
        result.is_err(),
        "New database at A.db should not have table 't' — \
         close() should have finalized statements and released the stale Database"
    );
}

/// Database::open with OpenOptions: works with pre-opened storage, and — when
/// no storage is supplied — resolves the default file storage at `path`.
#[test]
fn test_database_open_with_options() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("opts.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    let db = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    // With no storage supplied, open resolves the default file storage at
    // `path` and returns the same registered instance.
    let db_default = Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::Create),
    )
    .unwrap();
    assert!(
        Arc::ptr_eq(&db, &db_default),
        "open without storage must resolve default storage and hit the registered instance"
    );
}

/// Database::open_async still requires storage: default-storage resolution is
/// synchronous (registry lookup, file probes, file open) and only runs in the
/// synchronous Database::open entry point.
#[test]
fn test_open_async_requires_storage() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("async-needs-storage.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let mut state = turso_core::OpenDbAsyncState::new();
    let err = Database::open_async(
        &mut state,
        io,
        path,
        &turso_core::OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .expect_err("open_async without storage must fail");
    assert!(
        matches!(*err, turso_core::LimboError::InvalidArgument(ref m) if m.contains("storage")),
        "expected InvalidArgument about missing storage, got {err:?}"
    );
}

/// The registry-aware Database::open rejects a custom WAL path: the process
/// registry keys on the default WAL, so a nonstandard WAL must go through the
/// raw do_open path instead.
#[test]
fn test_open_rejects_wal_path() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("reject-wal.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));

    let err = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create)
            .wal_path(format!("{path}-wal-override")),
    )
    .expect_err("Database::open must reject a custom wal_path");
    assert!(
        matches!(err, turso_core::LimboError::InvalidArgument(ref m) if m.contains("wal_path")),
        "expected InvalidArgument about wal_path, got {err:?}"
    );

    // Register a canonical instance, then repeat with no pre-opened storage:
    // the rejection must fire before default-storage resolution, so a registry
    // hit cannot silently return the cached default-WAL instance.
    let _registered = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::Create),
    )
    .unwrap();
    let err = Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::Create)
            .wal_path(format!("{path}-wal-override")),
    )
    .expect_err("Database::open must reject a custom wal_path even with a registry hit");
    assert!(
        matches!(err, turso_core::LimboError::InvalidArgument(ref m) if m.contains("wal_path")),
        "expected InvalidArgument about wal_path on the registry-hit path, got {err:?}"
    );
}

/// do_open skips the process-wide registry: opening the same path first
/// through the registry-aware open and then through do_open yields two
/// distinct Database instances, and do_open does not register itself (so a
/// later open() does not observe the do_open instance).
#[test]
fn test_do_open_skips_registry() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("do-open-distinct.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));

    // Register a canonical instance via the registry-aware open.
    let registered = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file.clone())
            .flags(OpenFlags::Create),
    )
    .unwrap();

    // do_open must NOT return the registered instance.
    let detached = Database::do_open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file.clone())
            .flags(OpenFlags::Create),
    )
    .unwrap();
    assert!(
        !Arc::ptr_eq(&registered, &detached),
        "do_open must open a fresh Database instance, not the registered one"
    );

    // A registry-aware open still returns the originally registered instance,
    // proving do_open did not register itself over it.
    let registered_again = Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create),
    )
    .unwrap();
    assert!(
        Arc::ptr_eq(&registered, &registered_again),
        "open() must still return the registered instance; do_open must not replace it"
    );
}

/// Division by zero is a dialect property: a dialect that overrides
/// `errors_on_zero_divisor()` to return `true` raises `LimboError::DivisionByZero`;
/// the default SQLite dialect returns NULL. A NULL dividend returns NULL in
/// both cases.
#[test]
fn test_errors_on_zero_divisor_dialect() {
    use crate::common::TempDatabase;
    use std::sync::Arc;
    use turso_core::{Dialect, LimboError, SqliteDialect};

    struct DivisionRaisesErrorDialect;
    impl Dialect for DivisionRaisesErrorDialect {
        fn name(&self) -> &'static str {
            SqliteDialect.name()
        }
        fn parse(&self, sql: &str) -> turso_core::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            SqliteDialect.parse(sql)
        }
        fn parse_table_sql(
            &self,
            sql: &str,
            root_page: i64,
        ) -> turso_core::Result<turso_core::schema::BTreeTable> {
            SqliteDialect.parse_table_sql(sql, root_page)
        }
        fn parse_table_sql_ast(&self, sql: &str) -> turso_core::Result<turso_parser::ast::Stmt> {
            SqliteDialect.parse_table_sql_ast(sql)
        }
        fn table_sql_for_replay(&self, sql: &str) -> turso_core::Result<String> {
            SqliteDialect.table_sql_for_replay(sql)
        }
        fn format_table_sql(
            &self,
            input: &str,
            tbl_name: &turso_parser::ast::QualifiedName,
            body: &turso_parser::ast::CreateTableBody,
        ) -> turso_core::Result<String> {
            SqliteDialect.format_table_sql(input, tbl_name, body)
        }
        fn register_catalog(
            &self,
            schema: &mut turso_core::schema::Schema,
            enable_custom_types: bool,
        ) -> turso_core::Result<()> {
            SqliteDialect.register_catalog(schema, enable_custom_types)
        }
        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> turso_core::Result<Option<turso_core::Func>> {
            SqliteDialect.resolve_function(name, arg_count)
        }
        fn errors_on_zero_divisor(&self) -> bool {
            true
        }
    }

    let raising = TempDatabase::builder()
        .with_dialect(Arc::new(DivisionRaisesErrorDialect))
        .build();
    let conn = raising.connect_limbo();
    for sql in ["SELECT 1/0", "SELECT 1 % 0", "SELECT 1/0.0"] {
        let err = conn.execute(sql).expect_err(sql);
        assert!(
            matches!(err, LimboError::DivisionByZero),
            "{sql}: expected DivisionByZero, got {err:?}"
        );
    }
    let mut stmt = conn.prepare("SELECT NULL/0").unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert!(matches!(rows[0][0], turso_core::Value::Null));

    let defaulted = TempDatabase::builder().build();
    let conn = defaulted.connect_limbo();
    for sql in ["SELECT 1/0", "SELECT 1 % 0", "SELECT 1/0.0"] {
        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert!(
            matches!(rows[0][0], turso_core::Value::Null),
            "{sql}: default must return NULL, got {:?}",
            rows[0][0]
        );
    }
}

/// SQLite lets a user create a table named like the namespaces table of a
/// database that supports namespaces. Under the SQLite dialect it is an
/// ordinary table, and loading the schema must not read it as one.
#[test]
fn test_a_sqlite_table_named_like_the_namespaces_table_is_an_ordinary_table() {
    use crate::common::{limbo_exec_rows, TempDatabase};
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("main.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE __turso_internal_namespaces(x); \
             CREATE TABLE t(a); INSERT INTO t VALUES(5);",
        )
        .unwrap();
    let db = TempDatabase::new_with_existent_with_opts(
        &path,
        turso_core::DatabaseOpts::new().with_vacuum(true),
    );
    let conn = db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT a FROM t"),
        vec![vec![rusqlite::types::Value::Integer(5)]]
    );
    // VACUUM reloads the schema afterwards.
    conn.execute("VACUUM").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT a FROM t"),
        vec![vec![rusqlite::types::Value::Integer(5)]]
    );
}
