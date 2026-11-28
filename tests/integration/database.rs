use crate::common::ExecRows;
use asserting::prelude::*;
use std::sync::Arc;
use turso::IoBackend;
use turso_core::sync::RwLock;
use turso_core::{
    ConnectOpts, Database, InternalVirtualTable, InternalVirtualTableCursor, OpenFlags,
    SessionRole, Value,
};
use turso_sdk_kit::rsapi::{TursoDatabase, TursoDatabaseConfig, TursoStatusCode};
use turso_sqlite::SqliteDialect;

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
        .connect(ConnectOpts::new(SessionRole::Superuser))
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
        .connect(ConnectOpts::new(SessionRole::Superuser))
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

/// A double-quoted argument may hold a single quote or a comma, as in the usual
/// fts5 `tokenize="unicode61 tokenchars '_'"` option. Opening such a file must
/// get as far as looking up the module, like any other fts5 table.
#[test]
fn test_open_file_with_double_quoted_virtual_table_argument() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("fts5_tokenize.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            r#"CREATE VIRTUAL TABLE f USING fts5(text, tokenize="unicode61 tokenchars '_'");
               CREATE VIRTUAL TABLE g USING fts5(text, tokenize="unicode61 separators ',.'");"#,
        )
        .unwrap();
    drop(sqlite);

    match open_plain_file(&path) {
        Err(turso_core::LimboError::ExtensionError(msg)) => {
            assert_that!(msg).contains("Virtual table module not found: fts5");
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

/// Earlier Turso versions accepted built-in function calls with an argument
/// count the function does not take, such as `lpad` with 4 arguments, in
/// generated columns and expression indexes. A database with such a schema must
/// still open; only statements that evaluate the call fail.
#[test]
fn test_open_file_with_wrong_argument_count_in_schema() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("wrong_arg_count.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE g(a, b AS (upper(a)));
             CREATE TABLE i(a);
             CREATE INDEX ia ON i(upper(a));
             INSERT INTO g(a) VALUES ('x');
             INSERT INTO i VALUES ('y');
             PRAGMA writable_schema = ON;
             UPDATE sqlite_schema SET sql = 'CREATE TABLE g(a, b AS (lpad(a, 3, ''x'', ''y'')))'
              WHERE name = 'g';
             UPDATE sqlite_schema SET sql = 'CREATE INDEX ia ON i(lpad(a, 3, ''x'', ''y''))'
              WHERE name = 'ia';",
        )
        .unwrap();
    drop(sqlite);

    let db = crate::common::TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT 1");
    assert_eq!(rows, vec![(1,)]);
    let rows: Vec<(String,)> = conn.exec_rows("SELECT a FROM g");
    assert_eq!(rows, vec![("x".to_string(),)]);
    let rows: Vec<(String,)> = conn.exec_rows("SELECT a FROM i");
    assert_eq!(rows, vec![("y".to_string(),)]);

    for sql in ["SELECT b FROM g", "INSERT INTO i VALUES ('z')"] {
        let err = conn.execute(sql).unwrap_err();
        assert_that!(err.to_string()).contains("wrong number of arguments to function lpad()");
    }
}

/// The type, name and tbl_name of each row of sqlite_schema, read by SQLite.
fn sqlite_schema_rows(path: &std::path::Path) -> Vec<(String, String, String)> {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    let mut stmt = sqlite
        .prepare("SELECT type, name, tbl_name FROM sqlite_schema ORDER BY rowid")
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn sqlite_integrity_check(path: &std::path::Path) -> String {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    sqlite
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap()
}

/// SQLite keeps the case that CREATE gave a name in the name and tbl_name
/// columns of sqlite_schema. The tbl_name of a trigger is the table name as
/// CREATE TRIGGER wrote it. Turso must find these rows when it drops or
/// renames the objects.
#[test]
fn test_drop_and_rename_objects_of_sqlite_file_with_mixed_case_names() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("mixed_case.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            r#"CREATE TABLE "MixedCase"(a UNIQUE, b);
               CREATE INDEX "MyIdx" ON mixedcase(b);
               CREATE VIEW "MyView" AS SELECT a FROM "MixedCase";
               CREATE TRIGGER "MyTrig" AFTER INSERT ON MIXEDCASE BEGIN SELECT 1; END;
               CREATE TABLE "Other"(x UNIQUE);
               CREATE TRIGGER "OtherTrig" AFTER INSERT ON OTHER BEGIN SELECT 1; END;
               CREATE TABLE "Kept"(y);
               CREATE TRIGGER "KeptTrig" AFTER INSERT ON "Kept" BEGIN SELECT 1; END;"#,
        )
        .unwrap();
    drop(sqlite);

    {
        let db = crate::common::TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("DROP INDEX myidx").unwrap();
        conn.execute("DROP VIEW myview").unwrap();
        conn.execute("DROP TRIGGER mytrig").unwrap();
        conn.execute("DROP TABLE OTHER").unwrap();
        conn.execute(r#"ALTER TABLE mixedcase RENAME TO "NewName""#)
            .unwrap();
        conn.execute("ALTER TABLE newname ADD COLUMN c").unwrap();
        conn.execute("DROP TRIGGER KEPTTRIG").unwrap();
        conn.execute("DROP TABLE kept").unwrap();
    }

    let s = |v: &str| v.to_string();
    assert_eq!(
        sqlite_schema_rows(&path),
        vec![
            (s("table"), s("NewName"), s("NewName")),
            (s("index"), s("sqlite_autoindex_NewName_1"), s("NewName")),
        ]
    );
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    let columns: i64 = sqlite
        .query_row(
            "SELECT count(*) FROM pragma_table_info('NewName')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(columns, 3);
    assert_eq!(sqlite_integrity_check(&path), "ok");
}

/// Older Turso versions wrote names in lowercase in the name and tbl_name
/// columns of sqlite_schema, while the SQL text keeps the case that CREATE
/// gave the name. Turso must find these rows by the names they have.
#[test]
fn test_drop_objects_of_file_with_lowercase_schema_names() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("lowercase_names.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            r#"CREATE TABLE "MixedCase"(a UNIQUE, b);
               CREATE INDEX "MyIdx" ON "MixedCase"(b);
               CREATE VIEW "MyView" AS SELECT a FROM "MixedCase";
               CREATE TRIGGER "MyTrig" AFTER INSERT ON "MixedCase" BEGIN SELECT 1; END;
               CREATE TABLE "Other"(x);
               CREATE TRIGGER "OtherTrig" AFTER INSERT ON "Other" BEGIN SELECT 1; END;
               PRAGMA writable_schema = ON;
               UPDATE sqlite_schema SET name = lower(name), tbl_name = lower(tbl_name);"#,
        )
        .unwrap();
    drop(sqlite);

    const TABLE_LIST_NAMES: &str = "SELECT name FROM pragma_table_list \
         WHERE schema = 'main' AND name NOT LIKE 'sqlite_%' ORDER BY 1";
    const INDEX_LIST_NAMES: &str = "SELECT name FROM pragma_index_list('MixedCase') ORDER BY 1";
    let s = |v: &str| v.to_string();
    // Like SQLite, the PRAGMAs give the names as the CREATE statements wrote
    // them, not the lowercase names of the rows.
    let sqlite_table_names = sqlite_rows(&path, TABLE_LIST_NAMES);
    let sqlite_index_names = sqlite_rows(&path, INDEX_LIST_NAMES);
    assert_eq!(sqlite_table_names, vec!["MixedCase", "MyView", "Other"]);
    assert_eq!(
        sqlite_index_names,
        vec!["MyIdx", "sqlite_autoindex_MixedCase_1"]
    );
    {
        let db = crate::common::TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        for (query, sqlite_names) in [
            (TABLE_LIST_NAMES, &sqlite_table_names),
            (INDEX_LIST_NAMES, &sqlite_index_names),
        ] {
            let rows: Vec<(String,)> = conn.exec_rows(query);
            let names: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
            assert_eq!(&names, sqlite_names);
        }
        conn.execute("CREATE INDEX NewIdx ON MIXEDCASE(a)").unwrap();
        conn.execute("DROP INDEX MYIDX").unwrap();
        conn.execute("DROP VIEW MyView").unwrap();
        conn.execute("DROP TRIGGER MyTrig").unwrap();
    }

    assert_eq!(
        sqlite_schema_rows(&path),
        vec![
            (s("table"), s("mixedcase"), s("mixedcase")),
            (
                s("index"),
                s("sqlite_autoindex_mixedcase_1"),
                s("mixedcase")
            ),
            (s("table"), s("other"), s("other")),
            (s("trigger"), s("othertrig"), s("other")),
            (s("index"), s("NewIdx"), s("MixedCase")),
        ]
    );

    {
        let db = crate::common::TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("DROP TABLE MixedCase").unwrap();
        conn.execute("DROP TABLE Other").unwrap();
    }
    assert_eq!(sqlite_schema_rows(&path), vec![]);
    assert_eq!(sqlite_integrity_check(&path), "ok");
}

/// The text rows of `query`, read by SQLite, with the columns joined by `|`.
fn sqlite_rows(path: &std::path::Path, query: &str) -> Vec<String> {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    let mut stmt = sqlite.prepare(query).unwrap();
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        let mut values = Vec::new();
        for column in 0..columns {
            let value: rusqlite::types::Value = row.get(column)?;
            values.push(match value {
                rusqlite::types::Value::Text(text) => text,
                rusqlite::types::Value::Integer(integer) => integer.to_string(),
                other => format!("{other:?}"),
            });
        }
        Ok(values.join("|"))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
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
        .connect(ConnectOpts::new(SessionRole::Superuser))
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
        .connect(ConnectOpts::new(SessionRole::Superuser))
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
    use turso_core::{Dialect, LimboError};
    use turso_sqlite::SqliteDialect;

    struct DivisionRaisesErrorDialect;
    impl Dialect for DivisionRaisesErrorDialect {
        fn name(&self) -> &'static str {
            SqliteDialect.name()
        }

        fn parse_internal(
            &self,
            sql: &str,
        ) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
            turso_sqlite::SqliteDialect.parse_internal(sql)
        }

        fn parse_internal_column_definition(
            &self,
            sql: &str,
        ) -> turso_core::Result<turso_core::ast::ColumnDefinition> {
            turso_sqlite::SqliteDialect.parse_internal_column_definition(sql)
        }

        fn parameter_marker_ranges(
            &self,
            sql: &str,
        ) -> turso_core::Result<Vec<std::ops::Range<usize>>> {
            turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
        }

        fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(SqliteDialect)
        }
        fn parse(&self, sql: &str) -> turso_core::Result<(Option<turso_core::ast::Cmd>, usize)> {
            SqliteDialect.parse(sql)
        }
        fn parse_table_sql(
            &self,
            sql: &str,
            root_page: i64,
        ) -> turso_core::Result<turso_core::schema::BTreeTable> {
            SqliteDialect.parse_table_sql(sql, root_page)
        }
        fn parse_table_sql_ast(&self, sql: &str) -> turso_core::Result<turso_core::ast::Stmt> {
            SqliteDialect.parse_table_sql_ast(sql)
        }
        fn table_sql_for_replay(&self, sql: &str) -> turso_core::Result<String> {
            SqliteDialect.table_sql_for_replay(sql)
        }
        fn format_table_sql(
            &self,
            input: &str,
            tbl_name: &turso_core::ast::QualifiedName,
            body: &turso_core::ast::CreateTableBody,
        ) -> turso_core::Result<String> {
            SqliteDialect.format_table_sql(input, tbl_name, body)
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

#[test]
fn registered_internal_vtab_is_visible_to_connections() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let name = db
        .register_internal_vtab(StaticTable {
            name: "external_metadata",
        })
        .unwrap();
    assert_eq!(name, "external_metadata");

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let mut stmt = conn
        .prepare("SELECT key, value FROM external_metadata")
        .unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    let mapped: Vec<(String, i64)> = rows
        .into_iter()
        .map(|cols| {
            let key = match &cols[0] {
                Value::Text(t) => t.as_str().to_string(),
                other => panic!("unexpected key type {other:?}"),
            };
            let value = match &cols[1] {
                Value::Numeric(turso_core::Numeric::Integer(i)) => *i,
                other => panic!("unexpected value type {other:?}"),
            };
            (key, value)
        })
        .collect();
    assert_eq!(
        mapped,
        vec![("alpha".to_string(), 1), ("beta".to_string(), 2)]
    );
}

#[test]
fn registered_internal_vtab_lookup_folds_ascii_only() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let name = db
        .register_internal_vtab(StaticTable {
            name: "External_ΔΥΣ",
        })
        .unwrap();
    assert_eq!(name, "External_ΔΥΣ");

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let mut stmt = conn.prepare("SELECT key, value FROM external_ΔΥΣ").unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 2);

    assert!(conn.prepare("SELECT key, value FROM external_δυσ").is_err());
}

/// Minimal `InternalVirtualTable` that exposes a fixed two-row table. Used
/// to verify that callers can register an arbitrary catalog table at
/// database open time and query it like any other table.
#[derive(Debug)]
struct StaticTable {
    name: &'static str,
}

impl InternalVirtualTable for StaticTable {
    fn name(&self) -> String {
        self.name.to_string()
    }
    fn columns(&self) -> Vec<turso_core::ast::ColumnDefinition> {
        vec![
            turso_core::vtab::column_definition("key", "TEXT"),
            turso_core::vtab::column_definition("value", "INTEGER"),
        ]
    }
    fn superuser_only(&self) -> bool {
        false
    }
    fn open(
        &self,
        _conn: Arc<turso_core::Connection>,
    ) -> turso_core::Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(StaticCursor {
            rows: vec![("alpha".to_string(), 1), ("beta".to_string(), 2)],
            position: -1,
        })))
    }
    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
        Ok(turso_ext::IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 2,
            constraint_usages: constraints
                .iter()
                .map(|_| turso_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}

struct StaticCursor {
    rows: Vec<(String, i64)>,
    position: i64,
}

impl InternalVirtualTableCursor for StaticCursor {
    fn next(&mut self) -> turso_core::Result<bool> {
        self.position += 1;
        Ok((self.position as usize) < self.rows.len())
    }
    fn rowid(&self) -> i64 {
        self.position
    }
    fn column(&self, column: usize) -> turso_core::Result<Value> {
        let (key, value) = &self.rows[self.position as usize];
        match column {
            0 => Ok(Value::build_text(key.clone())),
            1 => Ok(Value::from_i64(*value)),
            _ => Err(turso_core::LimboError::InternalError(format!(
                "column index {column} out of range"
            ))),
        }
    }
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> turso_core::Result<bool> {
        self.position = -1;
        self.next()
    }
}
