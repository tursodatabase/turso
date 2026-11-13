#[cfg(feature = "simulator")]
#[allow(unused_imports)]
use std::path::Path;
use tempfile::TempDir;
use turso_core::attach::AttachDatabaseState;
use turso_core::connection::*;
use turso_core::io::MemoryIO;
use turso_core::io::IO;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::ConnectOpts;
use turso_core::Database;
use turso_core::DatabaseOpts;
use turso_core::IOResult;
use turso_core::LimboError;
use turso_core::OpenFlags;
use turso_core::Pager;
use turso_core::Result;
use turso_core::Schema;
use turso_core::Value;
use turso_core::MAIN_DB_ID;
use turso_sqlite::SqliteDialect;

fn open_connection_with_opts(path: &std::path::Path, opts: DatabaseOpts) -> Arc<Connection> {
    let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    db.connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap()
}

fn open_connection(path: &std::path::Path) -> Arc<Connection> {
    open_connection_with_opts(path, DatabaseOpts::new())
}

#[test]
fn test_the_sqlite_dialect_does_not_store_namespaces() {
    let temp_dir = TempDir::new().unwrap();
    let conn = open_connection(&temp_dir.path().join("main.db"));
    assert!(conn.prepare("CREATE SCHEMA app").is_err());
    assert!(conn.create_namespace("app").is_err());
    assert_eq!(conn.qualifier("app"), None);
}

fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) -> Result<()> {
    let mut state = AttachDatabaseState::default();
    loop {
        match conn.attach_database(path, alias, &mut state)? {
            IOResult::Done(()) => return Ok(()),
            IOResult::IO(io) => io.wait(conn.db.io.as_ref())?,
        }
    }
}

fn drive_attach_with_config(
    conn: &Arc<Connection>,
    path: &str,
    alias: &str,
    reserved_space: Option<u8>,
) -> Result<()> {
    let mut state = AttachDatabaseState::default();
    loop {
        match conn.attach_database_with_config(path, alias, reserved_space, &mut state)? {
            IOResult::Done(()) => return Ok(()),
            IOResult::IO(io) => io.wait(conn.db.io.as_ref())?,
        }
    }
}

fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}

fn text_value(value: &Value) -> &str {
    match value {
        Value::Text(text) => text.as_str(),
        other => panic!("expected text value, got {other:?}"),
    }
}

// given a attached 'alias', return the Database and Pager for that attached database
fn attached_entry(conn: &Connection, alias: &str) -> (Arc<Database>, Arc<Pager>) {
    let index = conn.qualifiers.database_id(alias).unwrap();
    let catalog = conn.attached_databases.read();
    let entry = catalog.index_to_data.get(&index).unwrap();
    (entry.db.clone(), entry.pager.clone())
}

#[test]
fn test_named_memory_databases_on_same_io_are_distinct() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let draft_db =
        Database::open_file(io.clone(), ":memory:sync-draft", Arc::new(SqliteDialect)).unwrap();
    let synced_db =
        Database::open_file(io, ":memory:sync-synced", Arc::new(SqliteDialect)).unwrap();
    assert!(!Arc::ptr_eq(&draft_db, &synced_db));

    let draft = draft_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let synced = synced_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    for conn in [&draft, &synced] {
        assert_eq!(conn.get_database_canonical_path(), "");
        assert_eq!(
            conn.list_all_databases(),
            vec![(MAIN_DB_ID, "main".to_string(), String::new())]
        );
    }

    draft
        .execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(11)")
        .unwrap();
    synced
        .execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(22)")
        .unwrap();

    assert_eq!(query_single_i64(&draft, "SELECT x FROM t"), 11);
    assert_eq!(query_single_i64(&synced, "SELECT x FROM t"), 22);
}

#[test]
fn test_named_memory_database_reopened_on_same_io_sees_same_rows() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());

    let first_db =
        Database::open_file(io.clone(), ":memory:reopen", Arc::new(SqliteDialect)).unwrap();
    let first = first_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    first
        .execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(99)")
        .unwrap();

    let second_db = Database::open_file(io, ":memory:reopen", Arc::new(SqliteDialect)).unwrap();
    let second = second_db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(query_single_i64(&second, "SELECT x FROM t"), 99);
}

#[test]
fn test_attach_named_memory_database_reports_empty_path() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let conn = open_connection_with_opts(&main_path, DatabaseOpts::new().with_attach(true));

    conn.execute("ATTACH ':memory:aux' AS aux").unwrap();
    conn.execute("CREATE TABLE aux.t(x INTEGER); INSERT INTO aux.t VALUES(5)")
        .unwrap();

    assert_eq!(query_single_i64(&conn, "SELECT x FROM aux.t"), 5);
    let database_list = conn.pragma_query("database_list").unwrap();
    let aux = database_list
        .iter()
        .find(|row| text_value(&row[1]) == "aux")
        .expect("attached aux database must be listed");
    assert_eq!(text_value(&aux[2]), "");
}

#[test]
fn test_search_path_decides_where_names_are_looked_up_and_created() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let conn = open_connection_with_opts(&main_path, DatabaseOpts::new().with_attach(true));
    conn.execute("ATTACH ':memory:aux' AS aux").unwrap();
    conn.execute("CREATE TABLE aux.t(x INTEGER); INSERT INTO aux.t VALUES(5)")
        .unwrap();
    assert_eq!(query_single_i64(&conn, "SELECT x FROM t"), 5);

    conn.set_search_path(Some(vec!["main".to_string()]));
    assert!(conn.prepare("SELECT x FROM t").is_err());
    conn.execute("DROP TABLE IF EXISTS t").unwrap();
    assert_eq!(query_single_i64(&conn, "SELECT x FROM aux.t"), 5);

    conn.set_search_path(Some(vec!["missing".to_string(), "aux".to_string()]));
    assert_eq!(query_single_i64(&conn, "SELECT x FROM t"), 5);
    conn.execute("CREATE TABLE u(y INTEGER)").unwrap();
    assert_eq!(query_single_i64(&conn, "SELECT count(*) FROM aux.u"), 0);

    conn.set_search_path(Some(vec!["missing".to_string()]));
    let Err(LimboError::ParseError(message)) = conn.execute("CREATE TABLE v(z INTEGER)") else {
        panic!("CREATE without a namespace needs a database in the search path");
    };
    assert_eq!(message, "no schema has been selected to create in");
    conn.execute("DROP TABLE IF EXISTS t").unwrap();

    conn.set_search_path(None);
    assert_eq!(query_single_i64(&conn, "SELECT x FROM t"), 5);
    conn.execute("CREATE TABLE v(z INTEGER)").unwrap();
    assert_eq!(query_single_i64(&conn, "SELECT count(*) FROM main.v"), 0);
}

#[test]
fn test_a_database_that_does_not_store_namespaces_keeps_names_that_look_stored() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("main.db");
    let conn = open_connection(&path);
    conn.execute(
        "CREATE TABLE __turso_ns1_t(x INTEGER); INSERT INTO __turso_ns1_t VALUES(7); \
         CREATE TABLE p(v INTEGER); CREATE INDEX __turso_ns2_i ON p(v); INSERT INTO p VALUES(1); \
         CREATE VIEW __turso_ns3_v AS SELECT x FROM __turso_ns1_t",
    )
    .unwrap();
    assert!(conn.create_namespace("app").is_err());
    drop(conn);

    let conn = open_connection(&path);
    assert_eq!(query_single_i64(&conn, "SELECT x FROM __turso_ns1_t"), 7);
    assert_eq!(query_single_i64(&conn, "SELECT x FROM __turso_ns3_v"), 7);
    assert!(conn.execute("DROP INDEX i").is_err());
    conn.execute("DROP INDEX __turso_ns2_i").unwrap();
    conn.execute("ALTER TABLE p RENAME TO __turso_ns4_p")
        .unwrap();
    assert_eq!(query_single_i64(&conn, "SELECT v FROM __turso_ns4_p"), 1);
    let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::Row));
    assert_eq!(stmt.row().unwrap().get::<&str>(0).unwrap(), "ok");
}

#[test]
fn test_named_memory_parent_can_attach_real_file_database() {
    let temp_dir = TempDir::new().unwrap();
    let aux_path = temp_dir.path().join("aux.db");
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:named-main",
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.to_str().unwrap()))
        .unwrap();
    conn.execute("CREATE TABLE aux.t(x INTEGER); INSERT INTO aux.t VALUES(7)")
        .unwrap();
    conn.execute("DETACH aux").unwrap();

    let reopened = open_connection(&aux_path);
    assert_eq!(query_single_i64(&reopened, "SELECT x FROM t"), 7);
}

#[test]
fn test_attach_database_with_config_overrides_reserved_space_before_initialization() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    drive_attach_with_config(&conn, aux_path.to_str().unwrap(), "aux", Some(48)).unwrap();

    let (attached_db, pager) = attached_entry(&conn, "aux");
    assert!(!attached_db.initialized());
    assert!(!pager.db_initialized());
    assert_eq!(pager.get_reserved_space(), Some(48));
}

#[cfg(feature = "checksum")]
#[test]
fn test_attach_database_with_config_rejects_reserved_space_below_minimum() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    let err = drive_attach_with_config(&conn, aux_path.to_str().unwrap(), "aux", Some(0))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "cannot attach database 'aux': reserved space 0 is smaller than attached database minimum 8"
    );
}

#[test]
fn test_fresh_mvcc_attach_installs_wal_before_bootstrap() {
    // this is a test to check that mvcc db on attach with a fresh db, makes the
    // attached db also mvcc
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux").unwrap();

    let (attached_db, pager) = attached_entry(&conn, "aux");
    assert!(attached_db.get_mv_store().as_ref().is_some());
    assert!(pager.has_wal());

    conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();
    conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)").unwrap();
}

#[test]
fn test_fresh_mvcc_attach_reuses_database_shared_wal() {
    let temp_dir = TempDir::new().unwrap();
    let main_path = temp_dir.path().join("main.db");
    let aux_path = temp_dir.path().join("aux.db");
    let conn = open_connection(&main_path);

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux").unwrap();
    conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();

    let (attached_db, pager) = attached_entry(&conn, "aux");
    let pager_shared_ptr = pager
        .wal_shared_ptr()
        .expect("fresh MVCC attach must expose WAL shared state in tests");
    let db_shared_ptr = Arc::as_ptr(&attached_db.shared_wal) as usize;

    assert_eq!(pager_shared_ptr, db_shared_ptr);
}

#[test]
fn test_temp_tables_are_connection_local_and_shadow_main() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn1 = open_connection(&db_path);

    conn1.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn1.execute("INSERT INTO main.t VALUES(1)").unwrap();
    let conn2 = open_connection(&db_path);
    conn1.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
    conn1.execute("INSERT INTO temp.t VALUES(2)").unwrap();

    assert_eq!(query_single_i64(&conn1, "SELECT x FROM t"), 2);
    assert_eq!(query_single_i64(&conn1, "SELECT x FROM main.t"), 1);
    assert_eq!(query_single_i64(&conn2, "SELECT x FROM t"), 1);

    let err = conn2
        .prepare("SELECT x FROM temp.t")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("no such table"),
        "expected no such table error, got: {err}"
    );
}

#[test]
fn test_reprepare_after_temp_store_reset_does_not_panic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
    let mut stmt = conn.prepare("SELECT x FROM t").unwrap();

    conn.execute("PRAGMA temp_store = MEMORY").unwrap();

    let err = stmt.step().unwrap_err().to_string();
    assert!(
        err.contains("no such table"),
        "expected no such table after temp reset, got: {err}"
    );
}

#[test]
fn test_prepared_insert_checks_constraints_after_ignore_check_constraints_off() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TABLE t1(a CHECK (a > 0))").unwrap();
    conn.execute("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    let mut stmt = conn.prepare("INSERT INTO t1 VALUES (-1)").unwrap();
    conn.execute("PRAGMA ignore_check_constraints = OFF")
        .unwrap();

    let err = stmt.run_ignore_rows().unwrap_err().to_string();
    assert!(err.contains("CHECK constraint failed"), "got: {err}");
}

#[test]
fn test_prepared_statement_reprepares_after_automatic_index_pragma() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TABLE t1(a)").unwrap();
    let mut stmt = conn.prepare("SELECT a FROM t1").unwrap();

    conn.execute("PRAGMA automatic_index = OFF").unwrap();
    stmt.run_collect_rows().unwrap();

    let reprepares = stmt.stmt_status(turso_core::statement::StatementStatusCounter::Reprepare);
    assert_eq!(reprepares, 1);
}

#[test]
fn test_temp_trigger_abort_rolls_back_temp_writes_without_panicking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
    conn.execute("CREATE TEMP TABLE u(y INTEGER)").unwrap();
    conn.execute(
        "CREATE TRIGGER tr BEFORE INSERT ON temp.t BEGIN \
         INSERT INTO u VALUES (NEW.x); \
         SELECT RAISE(ABORT, 'boom'); \
         END;",
    )
    .unwrap();

    let err = conn.execute("INSERT INTO temp.t VALUES(1)").unwrap_err();
    assert!(
        err.to_string().contains("boom"),
        "expected trigger abort error, got: {err}"
    );
    assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.u"), 0);
    assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.t"), 0);
}

#[test]
fn test_temp_trigger_abort_rolls_back_main_and_temp_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TABLE m(x INTEGER)").unwrap();
    conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
    conn.execute("CREATE TEMP TABLE u(y INTEGER)").unwrap();
    conn.execute(
        "CREATE TRIGGER tr BEFORE INSERT ON temp.t BEGIN \
         INSERT INTO m VALUES (NEW.x); \
         INSERT INTO u VALUES (NEW.x); \
         SELECT RAISE(ABORT, 'boom'); \
         END;",
    )
    .unwrap();

    let err = conn.execute("INSERT INTO temp.t VALUES(1)").unwrap_err();
    assert!(
        err.to_string().contains("boom"),
        "expected trigger abort error, got: {err}"
    );
    assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM main.m"), 0);
    assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.u"), 0);
    assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.t"), 0);
}

#[test]
fn test_distinct_triggers_with_same_name_in_different_schemas_can_fire_nested() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("main.db");
    let conn = open_connection(&db_path);

    conn.execute("CREATE TABLE src(x INTEGER)").unwrap();
    conn.execute("CREATE TABLE dst(y INTEGER)").unwrap();
    conn.execute("CREATE TABLE audit(z INTEGER)").unwrap();
    conn.execute(
        "CREATE TRIGGER shared_name AFTER INSERT ON dst BEGIN \
         INSERT INTO audit VALUES (NEW.y); \
         END;",
    )
    .unwrap();
    conn.execute(
        "CREATE TEMP TRIGGER shared_name AFTER INSERT ON main.src BEGIN \
         INSERT INTO dst VALUES (NEW.x); \
         END;",
    )
    .unwrap();

    conn.execute("INSERT INTO src VALUES(7)").unwrap();

    assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM main.dst"), 1);
    assert_eq!(query_single_i64(&conn, "SELECT SUM(z) FROM main.audit"), 7);
}

/// A committed `setval(X, false)` stores an unconsumed sequence value.
/// After sequence initialization reloads persisted state, the in-memory
/// sequence must still represent that value as unconsumed, so the next
/// `nextval()` returns `X` rather than advancing past it.
/// Disk-only sequence design: setval(value, is_called=false) must be
/// observable as the next nextval() result. Previously this exercised
/// the in-memory-atomic reseeding path; that path no longer exists,
/// but the user-visible contract still holds because every nextval
/// reads the backing-table watermark and applies is_called semantics
/// in op_sequence_compute_next.
#[test]
fn test_setval_uncalled_emits_stored_value_as_next() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("seq_init.db");
    let conn = open_connection_with_opts(&path, DatabaseOpts::new());

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE SEQUENCE s START 1 INCREMENT 3")?;
    conn.execute("SELECT setval('s', 13, 0)")?;

    let next_val = query_single_i64(&conn, "SELECT nextval('s')");
    assert_eq!(
        next_val, 13,
        "setval(13, false) committed: next nextval must return 13"
    );
    Ok(())
}

/// The reparse of the schema installs a fresh schema before the
/// dialect's schema state is read, and a statement compiled meanwhile
/// must not see it empty.
#[test]
fn schema_installed_while_being_read_keeps_the_loaded_dialect_schema() {
    let db = Database::open_file(
        Arc::new(turso_core::MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.with_schema_mut(|schema| {
        schema.dialect_schema = Some(Arc::new(()));
    })
    .unwrap();
    let fresh = Schema::with_options(false, &SqliteDialect).unwrap();
    assert!(fresh.dialect_schema.is_none());

    conn.install_schema_keeping_dialect_schema(&fresh).unwrap();

    assert!(conn.schema.read().dialect_schema.is_some());
}
