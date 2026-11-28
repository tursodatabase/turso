//! The `pg_class` relation catalog: created by the
//! PostgreSQL frontend when it opens a database writable, maintained by
//! every dialect in the same transaction as `sqlite_schema`.

use std::sync::Arc;

use crate::common::TempDatabase;
use turso_core::{Connection, Numeric, SessionRole, Value};

/// User class rows as `(oid, relname, engine namespace, relkind, engine owner)`,
/// ordered by oid.
fn class_rows(conn: &Arc<Connection>) -> Vec<(i64, String, i64, String, i64)> {
    let rows = conn
        .prepare_engine_schema_statement("SELECT oid, relname, relenginenamespace, relkind, CASE WHEN relowner=10 THEN 0 ELSE relowner-16384 END FROM __turso_internal_classes WHERE oid >= 16384 ORDER BY oid")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows.into_iter()
        .map(|row| {
            let text = |value: &Value| match value {
                Value::Text(text) => text.as_str().to_string(),
                other => panic!("expected text, got {other:?}"),
            };
            (
                row[0].as_int().unwrap(),
                text(&row[1]),
                row[2].as_int().unwrap(),
                text(&row[3]),
                row[4].as_int().unwrap(),
            )
        })
        .collect()
}

fn class_row(conn: &Arc<Connection>, relname: &str) -> Option<(i64, String, i64, String, i64)> {
    class_rows(conn).into_iter().find(|row| row.1 == relname)
}

#[test]
fn postgres_async_open_initializes_the_postgres_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server-open.db");
    let db = open_like_server(path.to_str().unwrap());
    let pg = turso_pg::PgConnection::connect(&db).unwrap();
    pg.execute("CREATE TABLE a (id integer PRIMARY KEY, name text)")
        .unwrap();
    pg.execute("CREATE TABLE b (value text)").unwrap();
    assert_catalog_columns(&pg, "public");
    let rows = class_rows(pg.inner());
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(rows.iter().all(|row| row.0 >= 16384));
    assert_ne!(rows[0].0, rows[1].0);
}

/// Byte 4 of the MVCC logical log header is its format version. Version 2
/// (LML2) logs cannot hold portable logical-change frames, so a PostgreSQL
/// log that starts as LML2 must later rewrite its header in place. A copy of
/// the log built only from appended bytes never sees that rewrite.
#[test]
fn postgres_async_open_starts_the_logical_log_as_version_3() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server-open.db");
    let db = open_like_server(path.to_str().unwrap());
    let log_path = dir.path().join("server-open.db-log");
    let header = std::fs::read(&log_path).unwrap();
    assert_eq!(header[4], 3, "log header version after catalog init");
    drop(db);
}

#[cfg(feature = "io_memory_yield")]
#[test]
fn postgres_async_open_initializes_catalog_across_io_yields() {
    let db = open_like_server_with_io("yielded.db", Arc::new(turso_core::MemoryYieldIO::new()));
    let pg = turso_pg::PgConnection::connect(&db).unwrap();
    pg.execute("CREATE TABLE a (id integer PRIMARY KEY, name text)")
        .unwrap();
    pg.execute("CREATE TABLE b (value text)").unwrap();
    assert_catalog_columns(&pg, "public");
    assert_eq!(class_rows(pg.inner()).len(), 3);
}

#[cfg(feature = "io_memory_yield")]
#[test]
fn cancelled_catalog_initialization_can_be_reopened_at_every_io_boundary() {
    for cancel_after in 0..100 {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryYieldIO::new());
        let path = "cancelled-catalog.db";
        let file = io
            .open_file(path, turso_core::OpenFlags::default(), true)
            .unwrap();
        let options = turso_core::OpenOptions::new(Arc::new(turso_pg::PostgresDialect))
            .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
                file,
            )))
            .db_opts(turso_core::DatabaseOpts::new().with_mvcc(true));
        drop(turso_core::Database::open(io.clone(), path, options.clone()).unwrap());
        let mut state = turso_pg::OpenDatabaseState::default();
        let mut yields = 0;
        let completed = loop {
            match turso_pg::open_database_async(
                &mut state,
                io.clone(),
                path,
                &options,
                turso_core::ConnectOpts::new(SessionRole::Superuser),
            )
            .unwrap()
            {
                turso_core::IOResult::Done(_) => break true,
                turso_core::IOResult::IO(completions) => {
                    completions.wait(io.as_ref()).unwrap();
                    if yields == cancel_after {
                        break false;
                    }
                    yields += 1;
                }
            }
        };
        drop(state);
        let db = open_like_server_with_io(path, io);
        let pg = turso_pg::PgConnection::connect(&db).unwrap();
        assert!(
            class_rows(pg.inner()).is_empty(),
            "cancel_after={cancel_after}"
        );
        pg.execute("CREATE TABLE t (x integer)").unwrap();
        assert_eq!(class_row(pg.inner(), "t").unwrap().0, 16384);
        if completed {
            return;
        }
    }
    panic!("opening exceeded the cancellation test's I/O bound");
}

fn assert_catalog_columns(pg: &turso_pg::PgConnection, namespace: &str) {
    let rows = pg
        .query(format!(
            "SELECT c.relname, a.attnum, a.attname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
        WHERE n.nspname='{namespace}' AND c.relname IN ('a', 'b') AND a.attnum > 0
        ORDER BY c.relname, a.attnum"
        ))
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![
                Value::from_text("a"),
                Value::from_i64(1),
                Value::from_text("id")
            ],
            vec![
                Value::from_text("a"),
                Value::from_i64(2),
                Value::from_text("name")
            ],
            vec![
                Value::from_text("b"),
                Value::from_i64(1),
                Value::from_text("value")
            ],
        ]
    );
}

fn open_like_server(path: &str) -> Arc<turso_core::Database> {
    open_like_server_with_io(path, Arc::new(turso_core::PlatformIO::new().unwrap()))
}

fn open_like_server_with_io(path: &str, io: Arc<dyn turso_core::IO>) -> Arc<turso_core::Database> {
    let file = io
        .open_file(path, turso_core::OpenFlags::default(), true)
        .unwrap();
    let options = turso_core::OpenOptions::new(Arc::new(turso_pg::PostgresDialect))
        .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
            file,
        )))
        .db_opts(turso_core::DatabaseOpts::new().with_mvcc(true));
    let mut state = turso_pg::OpenDatabaseState::default();
    loop {
        match turso_pg::open_database_async(
            &mut state,
            io.clone(),
            path,
            &options,
            turso_core::ConnectOpts::new(SessionRole::Superuser),
        )
        .unwrap()
        {
            turso_core::IOResult::Done(db) => return db,
            turso_core::IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
        }
    }
}

#[test]
fn a_new_database_has_the_classes_table_and_ddl_maintains_it() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();

    assert_eq!(class_rows(&conn), vec![]);

    pg.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();
    let rows = class_rows(&conn);
    // The table, the index of its primary key, and the index of its UNIQUE
    // constraint, named as PostgreSQL names them.
    assert_eq!(rows.len(), 3, "{rows:?}");
    let (t_oid, _, t_namespace, t_kind, t_owner) = class_row(&conn, "t").unwrap();
    assert_eq!(t_oid, 16384);
    assert_eq!(t_namespace, 1, "public is namespace 1");
    assert_eq!(t_kind, "r");
    assert_eq!(t_owner, 0, "owned by the bootstrap superuser");
    assert_eq!(class_row(&conn, "t_pkey").unwrap().0, 16385);
    assert_eq!(class_row(&conn, "t_name_key").unwrap().0, 16386);

    pg.execute("CREATE INDEX t_name ON t (name)").unwrap();
    assert_eq!(
        class_row(&conn, "t_name").unwrap(),
        (16387, "t_name".to_string(), 1, "i".to_string(), 0)
    );

    pg.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    let (_, _, _, v_kind, _) = class_row(&conn, "v").unwrap();
    assert_eq!(v_kind, "v");

    pg.execute("CREATE SEQUENCE s").unwrap();
    let (_, _, _, s_kind, _) = class_row(&conn, "s").unwrap();
    assert_eq!(s_kind, "S");

    pg.execute("DROP VIEW v").unwrap();
    assert_eq!(class_row(&conn, "v"), None);
    pg.execute("DROP SEQUENCE s").unwrap();
    assert_eq!(class_row(&conn, "s"), None);
    pg.execute("DROP INDEX t_name").unwrap();
    assert_eq!(class_row(&conn, "t_name"), None);
    pg.execute("DROP TABLE t").unwrap();
    assert_eq!(
        class_rows(&conn),
        vec![],
        "indexes are dropped with their table"
    );
}

#[turso_macros::test(mvcc)]
fn the_index_of_a_constraint_is_named_after_the_constraint(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t (a TEXT PRIMARY KEY, b INT, c INT, UNIQUE (b, c), UNIQUE (c))")
        .unwrap();
    pg.execute("CREATE TABLE named (id INTEGER CONSTRAINT my_key PRIMARY KEY)")
        .unwrap();

    let names: Vec<String> = class_rows(&conn).into_iter().map(|row| row.1).collect();
    assert!(names.contains(&"t_pkey".to_string()), "{names:?}");
    assert!(names.contains(&"t_b_c_key".to_string()), "{names:?}");
    assert!(names.contains(&"t_c_key".to_string()), "{names:?}");
    assert!(names.contains(&"my_key".to_string()), "{names:?}");
    assert!(names
        .iter()
        .all(|name| !name.starts_with("sqlite_autoindex")));
}

#[turso_macros::test(mvcc)]
fn a_table_with_an_integer_primary_key_has_a_row_for_the_index_of_its_key(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INT)")
        .unwrap();
    let (table_oid, ..) = class_row(&conn, "t").unwrap();
    let (index_oid, _, _, kind, _) = class_row(&conn, "t_pkey").unwrap();
    assert_eq!(kind, "i");
    assert!(index_oid > table_oid);

    pg.execute("ALTER TABLE t RENAME TO u").unwrap();
    assert!(
        class_row(&conn, "t_pkey").is_some(),
        "{:?}",
        class_rows(&conn)
    );

    pg.execute("DROP TABLE u").unwrap();
    assert_eq!(class_rows(&conn), vec![]);
}

#[turso_macros::test(mvcc)]
fn a_constraint_index_takes_the_next_free_name_when_its_name_is_used(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t_pkey (x INT)").unwrap();
    pg.execute("CREATE INDEX t_pkey1 ON t_pkey (x)").unwrap();
    pg.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();

    assert!(
        class_row(&conn, "t_pkey2").is_some(),
        "{:?}",
        class_rows(&conn)
    );
    assert!(class_row(&conn, "t_pkey").unwrap().3 == "r");
}

#[test]
fn oids_are_never_reused_even_across_a_reopen() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();

    pg.execute("CREATE TABLE a (x INTEGER)").unwrap();
    pg.execute("CREATE TABLE b (x INTEGER)").unwrap();
    let (a_oid, ..) = class_row(&conn, "a").unwrap();
    let (b_oid, ..) = class_row(&conn, "b").unwrap();
    assert_eq!((a_oid, b_oid), (16384, 16385));

    // Drop the relation with the highest oid, so only the stored watermark
    // remembers it was used.
    pg.execute("DROP TABLE b").unwrap();
    pg.execute("CREATE TABLE c (x INTEGER)").unwrap();
    let (c_oid, ..) = class_row(&conn, "c").unwrap();
    assert_eq!(c_oid, 16386, "the dropped table's oid is not reused");

    drop(pg);
    drop(conn);
    let db = db.reopen();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("DROP TABLE c").unwrap();
    pg.execute("CREATE TABLE d (x INTEGER)").unwrap();
    let (d_oid, ..) = class_row(&conn, "d").unwrap();
    assert_eq!(d_oid, 16387, "oids survive a reopen");
}

#[test]
fn a_rename_keeps_the_oid_and_owner() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();

    pg.execute("CREATE TABLE t (id TEXT PRIMARY KEY)").unwrap();
    let (t_oid, ..) = class_row(&conn, "t").unwrap();
    let autoindex_before = class_rows(&conn)
        .into_iter()
        .find(|row| row.3 == "i")
        .unwrap();

    pg.execute("ALTER TABLE t RENAME TO u").unwrap();

    assert_eq!(class_row(&conn, "t"), None);
    let (u_oid, ..) = class_row(&conn, "u").unwrap();
    assert_eq!(u_oid, t_oid);
    // PostgreSQL keeps the name of the index of a constraint when its table
    // is renamed, and the index keeps working under the schema's new name.
    let autoindex_after = class_rows(&conn)
        .into_iter()
        .find(|row| row.3 == "i")
        .unwrap();
    assert_eq!(autoindex_after, autoindex_before);
    pg.execute("INSERT INTO u VALUES ('a')").unwrap();
    pg.execute("DROP TABLE u").unwrap();
    assert_eq!(class_rows(&conn), vec![]);
}

#[test]
fn opening_the_same_file_twice_does_not_create_the_table_twice_or_change_rows() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    let conn = db.connect_limbo();
    let rows_before = class_rows(&conn);
    drop(pg);
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_limbo();
    assert_eq!(class_rows(&conn), rows_before);
}

#[test]
fn owner_to_stores_the_owner_and_lets_the_owner_drop_and_alter() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE ROLE alice").unwrap();
    pg.execute("CREATE ROLE bob").unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();

    pg.execute("ALTER TABLE t OWNER TO alice").unwrap();
    let (_, _, _, _, owner) = class_row(&conn, "t").unwrap();
    let alice_id = owner;
    assert!(alice_id > 0, "alice's stored role id");

    // bob may not drop or alter alice's table.
    pg.execute("SET ROLE bob").unwrap();
    let error = pg.execute("DROP TABLE t").unwrap_err().to_string();
    assert!(error.contains("must be owner of table t"), "{error}");
    let error = pg
        .execute("ALTER TABLE t RENAME TO u")
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be owner of table t"), "{error}");
    // Nor may bob, or even alice, transfer ownership: that stays with
    // superusers for now.
    let error = pg
        .execute("ALTER TABLE t OWNER TO bob")
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be owner of table t"), "{error}");

    // alice owns the table, so she may drop it. Renaming also needs CREATE
    // on the schema, which only a superuser has until GRANT exists.
    pg.execute("SET ROLE postgres").unwrap();
    pg.execute("SET ROLE alice").unwrap();
    let error = pg
        .execute("ALTER TABLE t RENAME TO u")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("permission denied for schema public"),
        "{error}"
    );
    pg.execute("SET ROLE postgres").unwrap();
    pg.execute("ALTER TABLE t RENAME TO u").unwrap();
    let (_, _, _, _, owner) = class_row(&conn, "u").unwrap();
    assert_eq!(owner, alice_id, "rename keeps the owner");
    pg.execute("SET ROLE alice").unwrap();
    pg.execute("DROP TABLE u").unwrap();
    assert_eq!(class_row(&conn, "u"), None);
}

#[test]
fn owner_to_resolves_role_keywords_and_reports_missing_names() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    pg.execute("CREATE VIEW v AS SELECT x FROM t").unwrap();
    pg.execute("CREATE SEQUENCE s").unwrap();
    pg.execute("CREATE INDEX t_x ON t (x)").unwrap();
    pg.execute("CREATE ROLE alice").unwrap();

    pg.execute("ALTER TABLE t OWNER TO CURRENT_ROLE").unwrap();
    assert_eq!(class_row(&conn, "t").unwrap().4, 0);
    pg.execute("ALTER TABLE t OWNER TO SESSION_USER").unwrap();
    assert_eq!(class_row(&conn, "t").unwrap().4, 0);
    pg.execute("ALTER VIEW v OWNER TO alice").unwrap();
    pg.execute("ALTER SEQUENCE s OWNER TO alice").unwrap();
    let alice_id = class_row(&conn, "v").unwrap().4;
    assert!(alice_id > 0);
    assert_eq!(class_row(&conn, "s").unwrap().4, alice_id);
    // An index is owned by its table: ALTER INDEX changes nothing, and
    // ALTER TABLE moves the index with the table.
    pg.execute("ALTER INDEX t_x OWNER TO alice").unwrap();
    assert_eq!(class_row(&conn, "t_x").unwrap().4, 0);
    pg.execute("ALTER TABLE t OWNER TO alice").unwrap();
    assert_eq!(class_row(&conn, "t").unwrap().4, alice_id);
    assert_eq!(class_row(&conn, "t_x").unwrap().4, alice_id);
    pg.execute("ALTER TABLE t OWNER TO postgres").unwrap();
    assert_eq!(class_row(&conn, "t").unwrap().4, 0);
    assert_eq!(class_row(&conn, "t_x").unwrap().4, 0);

    let error = pg
        .execute("ALTER TABLE t OWNER TO nobody")
        .unwrap_err()
        .to_string();
    assert!(error.contains("role \"nobody\" does not exist"), "{error}");
    let error = pg
        .execute("ALTER TABLE missing OWNER TO alice")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("relation \"missing\" does not exist"),
        "{error}"
    );
    pg.execute("ALTER TABLE IF EXISTS missing OWNER TO alice")
        .unwrap();
}

/// Opens `path` with the PostgreSQL dialect directly, skipping the
/// frontend's catalog creation, the way files written before the catalog
/// existed were made.
fn open_without_catalog(path: &str) -> Arc<turso_core::Database> {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = io
        .open_file(path, turso_core::OpenFlags::default(), true)
        .unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(turso_pg::PostgresDialect))
            .storage(db_file)
            .flags(turso_core::OpenFlags::default())
            .db_opts(turso_core::DatabaseOpts::new().with_mvcc(true)),
    )
    .unwrap()
}

#[test]
fn a_database_with_relations_but_no_catalog_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pre-catalog.db");
    let path = path.to_str().unwrap();
    {
        let db = open_without_catalog(path);
        let conn = db
            .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.create_namespace("app").unwrap();
        conn.prepare("CREATE TABLE app.t (x int)")
            .unwrap()
            .run_ignore_rows()
            .unwrap();
    }

    for read_only in [false, true] {
        let mut flags = turso_core::OpenFlags::default();
        if read_only {
            flags |= turso_core::OpenFlags::ReadOnly;
        }
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let error =
            turso_pg::open_database_with_io(io, path, flags, turso_core::DatabaseOpts::new())
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("has relations but no relation catalog"),
            "read_only={read_only}: {error}"
        );
    }
}

/// The SQLite frontend makes such a file: it creates `sqlite_sequence` for
/// an AUTOINCREMENT table and keeps it when the table is dropped.
#[test]
fn a_database_that_already_has_the_sequence_table_opens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("has-sequence-table.db");
    let path = path.to_str().unwrap();
    {
        let db = open_without_catalog(path);
        let conn = db
            .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.prepare_engine_schema_statement("CREATE TABLE sqlite_sequence(name,seq)")
            .unwrap()
            .run_ignore_rows()
            .unwrap();
    }

    let db = open_like_server(path);
    let pg = turso_pg::PgConnection::connect(&db).unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    let (oid, ..) = class_row(pg.inner(), "t").unwrap();
    assert_eq!(oid, 16384);
}

#[test]
fn a_rolled_back_create_leaves_no_class_row_behind() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();

    pg.execute("BEGIN").unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    pg.execute("ROLLBACK").unwrap();
    assert_eq!(class_rows(&conn), vec![]);

    // The classes table and sqlite_schema agree, so the same name works.
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    let (_, _, _, kind, _) = class_row(&conn, "t").unwrap();
    assert_eq!(kind, "r");
}

#[test]
fn capture_data_changes_is_refused_under_the_postgres_dialect() {
    // CDC tables are created at run time, outside the DDL that maintains
    // the relation catalog, so they would be relations without class rows.
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let error = pg
        .pragma_update("unstable_capture_data_changes_conn", "'full'")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("capture_data_changes is not supported"),
        "{error}"
    );
}

#[test]
fn dropping_a_table_drops_the_sequences_it_owned() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE ROLE alice").unwrap();
    pg.execute("CREATE TABLE d (x serial)").unwrap();
    let error = pg
        .execute("ALTER SEQUENCE d_x_seq OWNER TO alice")
        .unwrap_err()
        .to_string();
    assert!(error.contains("cannot change owner of sequence"), "{error}");
    pg.execute("DROP TABLE d").unwrap();
    let error = pg
        .execute("ALTER SEQUENCE d_x_seq OWNER TO alice")
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not exist"), "{error}");
}

/// The stored access control list of `relname`, as `pg_class.relacl` shows
/// it, read through `conn`.
fn relacl(conn: &turso_pg::Connection, relname: &str) -> Option<String> {
    let rows = conn
        .query(format!(
            "SELECT relacl FROM pg_class WHERE relname = '{relname}'"
        ))
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1, "{relname} has one pg_class row: {rows:?}");
    match &rows[0][0] {
        Value::Null => None,
        Value::Text(text) => Some(text.as_str().to_string()),
        other => panic!("relacl is text or NULL, got {other:?}"),
    }
}

/// The stored access control list of `relname`, read from the catalog
/// table, where roles are oids: 10 is the superuser and the first created
/// role is 16385.
fn stored_relacl(conn: &Arc<Connection>, relname: &str) -> Option<String> {
    let rows = conn
        .prepare(format!(
            "SELECT relprivileges FROM __turso_internal_classes WHERE relname = '{relname}'"
        ))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1, "{relname} has one class row: {rows:?}");
    match &rows[0][0] {
        Value::Null => None,
        Value::Text(text) => Some(text.as_str().to_string()),
        other => panic!("relacl is text or NULL, got {other:?}"),
    }
}

/// The stored name of the column that owns sequence `relname`, read from
/// the catalog table.
fn stored_relownedbycolumn(conn: &Arc<Connection>, relname: &str) -> Option<String> {
    let rows = conn
        .prepare(format!(
            "SELECT relownedbycolumn FROM __turso_internal_classes WHERE relname = '{relname}'"
        ))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1, "{relname} has one class row: {rows:?}");
    match &rows[0][0] {
        Value::Null => None,
        Value::Text(text) => Some(text.as_str().to_string()),
        other => panic!("relownedbycolumn is text or NULL, got {other:?}"),
    }
}

/// The stored values of the columns of `relname`'s class row that do not
/// change when the relation is granted, renamed or given an owner, joined by
/// commas.
fn stored_pg_class_columns(conn: &Arc<Connection>, relname: &str) -> String {
    let columns = "reltype, reloftype, relam, relfilenode, reltablespace, relpages, reltuples, \
         relallvisible, reltoastrelid, relhasindex, relisshared, relpersistence, relkind, \
         relnatts, relchecks, relhasrules, relhastriggers, relhassubclass, relforcerowsecurity, \
         relispopulated, relreplident, relispartition, relrewrite, relfrozenxid, relminmxid, \
         reloptions, relpartbound";
    let rows = conn
        .prepare(format!(
            "SELECT {columns} FROM pg_class WHERE relname = '{relname}'"
        ))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1, "{relname} has one class row: {rows:?}");
    rows[0]
        .iter()
        .map(|value| match value {
            Value::Null => "NULL".to_string(),
            Value::Text(text) => text.as_str().to_string(),
            Value::Numeric(Numeric::Integer(number)) => number.to_string(),
            Value::Numeric(Numeric::Float(number)) => f64::from(*number).to_string(),
            other => panic!("unexpected column value {other:?}"),
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[turso_macros::test(mvcc)]
fn every_new_class_row_has_all_the_columns_of_pg_class(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t (id serial, x int)").unwrap();
    pg.execute("CREATE INDEX t_x ON t (x)").unwrap();
    pg.execute("CREATE VIEW v AS SELECT x FROM t").unwrap();

    let table = "0,0,2,0,0,0,-1,0,0,1,0,p,r,2,0,0,0,0,0,1,d,0,0,0,0,NULL,NULL";
    let index = "0,0,403,0,0,1,0,0,0,0,0,p,i,1,0,0,0,0,0,1,d,0,0,0,0,NULL,NULL";
    let sequence = "0,0,0,0,0,1,1,0,0,0,0,p,S,3,0,0,0,0,0,1,n,0,0,0,0,NULL,NULL";
    let view = "0,0,0,0,0,0,-1,0,0,0,0,p,v,1,0,1,0,0,0,1,n,0,0,0,0,NULL,NULL";
    assert_eq!(stored_pg_class_columns(&conn, "t"), table);
    assert_eq!(stored_pg_class_columns(&conn, "t_x"), index);
    assert_eq!(stored_pg_class_columns(&conn, "t_id_seq"), sequence);
    assert_eq!(stored_pg_class_columns(&conn, "v"), view);
}

#[turso_macros::test(mvcc)]
fn rewriting_a_class_row_keeps_the_columns_it_does_not_change(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE ROLE reader").unwrap();
    pg.execute("CREATE TABLE t (id serial, x int)").unwrap();
    let before = stored_pg_class_columns(&conn, "t");

    pg.execute("GRANT SELECT ON t TO reader").unwrap();
    pg.execute("ALTER TABLE t OWNER TO reader").unwrap();
    pg.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();
    pg.execute("CREATE POLICY p ON t USING (true)").unwrap();
    pg.execute("ALTER TABLE t RENAME TO u").unwrap();

    assert_eq!(stored_pg_class_columns(&conn, "u"), before);
}

#[turso_macros::test(mvcc)]
fn the_catalog_stores_the_column_that_owns_a_sequence(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t (id serial, other int)").unwrap();
    assert_eq!(
        stored_relownedbycolumn(&conn, "t_id_seq").as_deref(),
        Some("id")
    );

    pg.execute("ALTER TABLE t RENAME COLUMN id TO ident")
        .unwrap();
    assert_eq!(
        stored_relownedbycolumn(&conn, "t_id_seq").as_deref(),
        Some("ident")
    );

    pg.execute("ALTER SEQUENCE t_id_seq OWNED BY t.other")
        .unwrap();
    assert_eq!(
        stored_relownedbycolumn(&conn, "t_id_seq").as_deref(),
        Some("other")
    );

    pg.execute("ALTER TABLE t RENAME COLUMN ident TO id")
        .unwrap();
    assert_eq!(
        stored_relownedbycolumn(&conn, "t_id_seq").as_deref(),
        Some("other")
    );

    pg.execute("ALTER SEQUENCE t_id_seq OWNED BY NONE").unwrap();
    assert_eq!(stored_relownedbycolumn(&conn, "t_id_seq"), None);
}

#[turso_macros::test(mvcc)]
fn grant_select_stores_the_list_in_the_file_and_it_survives_a_reopen(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE ROLE reader").unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    pg.execute("CREATE VIEW v AS SELECT x FROM t").unwrap();
    assert_eq!(stored_relacl(&conn, "t"), None);

    pg.execute("GRANT SELECT ON t, v TO reader").unwrap();
    let stored = "{10=arwdDxtm/10,16385=r/10}";
    let list = "{postgres=arwdDxtm/postgres,reader=r/postgres}";
    assert_eq!(stored_relacl(&conn, "t").as_deref(), Some(stored));
    assert_eq!(stored_relacl(&conn, "v").as_deref(), Some(stored));
    assert_eq!(relacl(&pg, "t").as_deref(), Some(list));

    drop(pg);
    drop(conn);
    let db = db.reopen();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    assert_eq!(stored_relacl(&conn, "v").as_deref(), Some(stored));
    assert_eq!(relacl(&pg, "t").as_deref(), Some(list));
    // The reopened database keeps granting on the stored list.
    pg.execute("GRANT SELECT ON t TO PUBLIC").unwrap();
    assert_eq!(
        relacl(&pg, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres,reader=r/postgres,=r/postgres}")
    );
}

#[turso_macros::test(mvcc)]
fn a_grant_is_seen_by_another_connection(db: TempDatabase) {
    let granting = db.connect_postgres();
    let watching = db.connect_postgres();
    granting.execute("CREATE ROLE reader").unwrap();
    granting.execute("CREATE TABLE t (x INTEGER)").unwrap();
    assert_eq!(relacl(&watching, "t"), None);

    granting.execute("GRANT SELECT ON t TO reader").unwrap();
    assert_eq!(
        relacl(&watching, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres,reader=r/postgres}")
    );
}

#[turso_macros::test(mvcc)]
fn a_rolled_back_grant_leaves_the_list_as_it_was(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE ROLE reader").unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();

    pg.execute("BEGIN").unwrap();
    pg.execute("GRANT SELECT ON t TO reader").unwrap();
    assert_eq!(
        relacl(&pg, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres,reader=r/postgres}"),
        "the transaction sees its own grant"
    );
    pg.execute("ROLLBACK").unwrap();
    assert_eq!(stored_relacl(&conn, "t"), None);
    assert_eq!(
        relacl(&pg, "t"),
        None,
        "the in-memory catalog forgot it too"
    );
}

#[turso_macros::test(mvcc)]
fn a_revoke_is_stored_in_the_file_and_survives_a_reopen(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE ROLE reader").unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    pg.execute("CREATE VIEW v AS SELECT x FROM t").unwrap();
    pg.execute("GRANT SELECT, INSERT ON t, v TO reader")
        .unwrap();

    pg.execute("REVOKE INSERT ON t, v FROM reader").unwrap();
    let stored = "{10=arwdDxtm/10,16385=r/10}";
    let list = "{postgres=arwdDxtm/postgres,reader=r/postgres}";
    assert_eq!(stored_relacl(&conn, "t").as_deref(), Some(stored));
    assert_eq!(stored_relacl(&conn, "v").as_deref(), Some(stored));
    assert_eq!(relacl(&pg, "t").as_deref(), Some(list));

    drop(pg);
    drop(conn);
    let db = db.reopen();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    assert_eq!(stored_relacl(&conn, "v").as_deref(), Some(stored));
    assert_eq!(relacl(&pg, "t").as_deref(), Some(list));
    pg.execute("SET ROLE reader").unwrap();
    assert_eq!(
        pg.execute("INSERT INTO t VALUES (1)")
            .unwrap_err()
            .to_string(),
        "permission denied for table t"
    );
    pg.execute("SELECT x FROM v").unwrap();
    // The reopened database keeps revoking on the stored list.
    pg.execute("RESET ROLE").unwrap();
    pg.execute("REVOKE SELECT ON t FROM reader").unwrap();
    assert_eq!(
        relacl(&pg, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres}")
    );
}

#[turso_macros::test(mvcc)]
fn a_revoke_is_seen_by_another_connection(db: TempDatabase) {
    let revoking = db.connect_postgres();
    let watching = db.connect_postgres();
    revoking.execute("CREATE ROLE reader").unwrap();
    revoking.execute("CREATE TABLE t (x INTEGER)").unwrap();
    revoking.execute("GRANT SELECT ON t TO reader").unwrap();
    watching.execute("SET ROLE reader").unwrap();
    watching.execute("SELECT x FROM t").unwrap();

    revoking.execute("REVOKE SELECT ON t FROM reader").unwrap();

    assert_eq!(
        watching.execute("SELECT x FROM t").unwrap_err().to_string(),
        "permission denied for table t"
    );
    watching.execute("RESET ROLE").unwrap();
    assert_eq!(
        relacl(&watching, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres}")
    );
}

#[turso_macros::test(mvcc)]
fn a_statement_prepared_before_a_revoke_is_checked_against_the_new_list(db: TempDatabase) {
    let revoking = db.connect_postgres();
    let reading = db.connect_postgres();
    revoking.execute("CREATE ROLE reader").unwrap();
    revoking.execute("CREATE ROLE owner").unwrap();
    revoking.execute("CREATE TABLE t (x INTEGER)").unwrap();
    revoking.execute("ALTER TABLE t OWNER TO owner").unwrap();
    revoking.execute("GRANT SELECT ON t TO reader").unwrap();
    reading.execute("SET ROLE reader").unwrap();
    let mut select = reading.prepare("SELECT x FROM t").unwrap();
    select.run_ignore_rows().unwrap();
    // The owner revokes on the connection it prepared the statement on.
    revoking.execute("SET ROLE owner").unwrap();
    let mut select_as_owner = revoking.prepare("SELECT x FROM t").unwrap();
    select_as_owner.run_ignore_rows().unwrap();

    revoking
        .execute("REVOKE SELECT ON t FROM reader, owner")
        .unwrap();

    for mut stmt in [select, select_as_owner] {
        stmt.reset().unwrap();
        assert_eq!(
            stmt.run_ignore_rows().unwrap_err().to_string(),
            "permission denied for table t"
        );
    }
}

#[turso_macros::test(mvcc)]
fn a_rolled_back_revoke_leaves_the_list_as_it_was(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE ROLE reader").unwrap();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    pg.execute("GRANT SELECT ON t TO reader").unwrap();
    let stored = "{10=arwdDxtm/10,16385=r/10}";

    pg.execute("BEGIN").unwrap();
    pg.execute("REVOKE SELECT ON t FROM reader").unwrap();
    assert_eq!(
        relacl(&pg, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres}"),
        "the transaction sees its own revoke"
    );
    pg.execute("ROLLBACK").unwrap();
    assert_eq!(stored_relacl(&conn, "t").as_deref(), Some(stored));
    assert_eq!(
        relacl(&pg, "t").as_deref(),
        Some("{postgres=arwdDxtm/postgres,reader=r/postgres}"),
        "the in-memory catalog forgot it too"
    );
    pg.execute("SET ROLE reader").unwrap();
    pg.execute("SELECT x FROM t").unwrap();
}

#[test]
fn policies_take_oids_from_the_relation_oid_counter_and_keep_them_across_a_reopen() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE TABLE t (x INTEGER)").unwrap();
    pg.execute("CREATE POLICY first ON t USING (x = 1)")
        .unwrap();
    pg.execute("CREATE POLICY second ON t USING (x = 2)")
        .unwrap();
    pg.execute("CREATE TABLE u (x INTEGER)").unwrap();
    assert_eq!(
        stored_policy_oids(&conn, "t"),
        vec![("first".to_string(), 16385), ("second".to_string(), 16386)]
    );
    assert_eq!(class_row(&conn, "u").unwrap().0, 16387);

    // The dropped policy had the highest oid, which is not used again.
    pg.execute("DROP POLICY second ON t").unwrap();
    pg.execute("CREATE POLICY third ON t USING (x = 3)")
        .unwrap();
    assert_eq!(
        stored_policy_oids(&conn, "t"),
        vec![("first".to_string(), 16385), ("third".to_string(), 16388)]
    );

    drop(pg);
    drop(conn);
    let db = db.reopen();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    pg.execute("CREATE POLICY fourth ON t USING (x = 4)")
        .unwrap();
    assert_eq!(
        stored_policy_oids(&conn, "t"),
        vec![
            ("first".to_string(), 16385),
            ("third".to_string(), 16388),
            ("fourth".to_string(), 16389)
        ]
    );
}

/// The policies of `relname` as `(polname, oid)`, read from the catalog
/// table.
fn stored_policy_oids(conn: &Arc<Connection>, relname: &str) -> Vec<(String, i64)> {
    conn.prepare(format!(
        "SELECT json_extract(c.relpolicies, '$[' || p.i || '].polname'), \
                json_extract(c.relpolicies, '$[' || p.i || '].oid') \
         FROM __turso_internal_classes c, \
              generate_series(0, json_array_length(c.relpolicies) - 1) AS p(i) \
         WHERE c.relname = '{relname}' ORDER BY p.i"
    ))
    .unwrap()
    .run_collect_rows()
    .unwrap()
    .into_iter()
    .map(|row| (row[0].to_string(), row[1].as_int().unwrap()))
    .collect()
}

/// A table without a class row is one the engine made for itself. Nobody
/// granted a role anything on it.
#[test]
fn a_table_without_a_class_row_is_only_readable_by_superusers() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.inner()
        .prepare_engine_schema_statement("CREATE TABLE engine_state (x INTEGER)")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SELECT x FROM pg_catalog.engine_state")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();
    let error = conn
        .execute("SELECT x FROM pg_catalog.engine_state")
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "permission denied for table engine_state"
    );
}

/// Reading the schema again scans `sqlite_schema`, which has no class row.
/// The scan is the engine's, so the role that is set does not limit it.
#[test]
fn schema_is_read_again_while_a_role_is_set() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("GRANT SELECT ON t TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    conn.inner().force_reparse_schema_without_publish().unwrap();

    conn.execute("SELECT x FROM t").unwrap();
}

#[turso_macros::test(mvcc)]
fn rename_cannot_take_a_constraint_index_name(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE source (id integer PRIMARY KEY, x integer UNIQUE)")
        .unwrap();
    let before = class_rows(pg.inner());
    for name in ["source_pkey", "source_x_key"] {
        let error = pg
            .execute(format!("ALTER TABLE source RENAME TO {name}"))
            .unwrap_err();
        assert_eq!(error.sqlstate, "42P07", "{error:?}");
        assert_eq!(class_rows(pg.inner()), before);
        pg.inner().force_reparse_schema_without_publish().unwrap();
    }
}

#[turso_macros::test(mvcc)]
fn an_explicit_primary_key_name_is_not_renamed_on_collision(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE t_pkey(x int)").unwrap();
    for sql in [
        "CREATE TABLE t(id int CONSTRAINT t_pkey PRIMARY KEY)",
        "CREATE TABLE t(id text, CONSTRAINT t_pkey PRIMARY KEY(id))",
    ] {
        let error = pg.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate, "42P07", "{error:?}");
        assert_eq!(class_rows(pg.inner()).len(), 1);
    }
    pg.execute("CREATE TABLE t(id int PRIMARY KEY)").unwrap();
    assert!(class_row(pg.inner(), "t_pkey1").is_some());
}

#[turso_macros::test(mvcc)]
fn constraint_names_cannot_collide_with_relations_in_the_same_statement(db: TempDatabase) {
    let pg = db.connect_postgres();
    for sql in [
        "CREATE TABLE t (x int CONSTRAINT t PRIMARY KEY)",
        "CREATE TABLE t (x text CONSTRAINT t PRIMARY KEY)",
        "CREATE TABLE t (x int CONSTRAINT t UNIQUE)",
        "CREATE TABLE t (x int CONSTRAINT same UNIQUE, y int CONSTRAINT same UNIQUE)",
        "CREATE TABLE t (x int CONSTRAINT same PRIMARY KEY, y int CONSTRAINT same UNIQUE)",
    ] {
        let error = pg.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate, "42P07", "{sql}: {error:?}");
        assert!(class_rows(pg.inner()).is_empty(), "{sql}");
        pg.inner().force_reparse_schema_without_publish().unwrap();
    }
    pg.execute("CREATE TABLE t (x int PRIMARY KEY, y int UNIQUE)")
        .unwrap();
    assert_eq!(class_rows(pg.inner()).len(), 3);
}

#[turso_macros::test(mvcc)]
fn generated_constraint_index_names_fit_postgres_identifiers(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.inner().pragma_update("foreign_keys", "ON").unwrap();
    pg.execute("CREATE TABLE abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij (x int PRIMARY KEY)")
        .unwrap();
    pg.execute(
        "CREATE TABLE abcdefghijabcdefghijabcdefghij (abcdefghijabcdefghijabcdefghij int UNIQUE)",
    )
    .unwrap();
    for name in [
        "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefgh_pkey",
        "abcdefghijabcdefghijabcdefghi_abcdefghijabcdefghijabcdefghi_key",
    ] {
        assert!(class_row(pg.inner(), name).is_some(), "missing {name}");
    }
    for (table, expected) in [
        ("a".repeat(60), format!("{}_pkey", "a".repeat(58))),
        (
            format!("{}b", "a".repeat(59)),
            format!("{}_pkey1", "a".repeat(57)),
        ),
        ("é".repeat(30), format!("{}_pkey", "é".repeat(29))),
        (
            format!("{}b", "é".repeat(29)),
            format!("{}_pkey1", "é".repeat(28)),
        ),
    ] {
        pg.execute(format!("CREATE TABLE \"{table}\" (x text PRIMARY KEY)"))
            .unwrap();
        assert!(
            class_row(pg.inner(), &expected).is_some(),
            "missing {expected}"
        );
        assert!(expected.len() <= 63);
    }
    pg.execute(format!(
        "CREATE TABLE refs (a text REFERENCES \"{}\"(x), b text REFERENCES \"{}\"(x))",
        "a".repeat(60),
        "é".repeat(30)
    ))
    .unwrap();
    pg.inner().force_reparse_schema_without_publish().unwrap();
    let error = pg
        .execute("INSERT INTO refs VALUES ('missing', 'missing')")
        .unwrap_err();
    assert_eq!(error.sqlstate, "23503");
}

#[turso_macros::test(mvcc)]
fn quoted_relation_names_survive_catalog_updates(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE \"MiXeD\" (id int PRIMARY KEY, x text, \"CoDe\" text UNIQUE)")
        .unwrap();
    pg.execute("CREATE INDEX \"IxX\" ON \"MiXeD\"(x)").unwrap();
    pg.execute("CREATE VIEW \"ViEw\" AS SELECT x FROM \"MiXeD\"")
        .unwrap();
    let oid = class_row(pg.inner(), "MiXeD").unwrap().0;
    for name in ["MiXeD_pkey", "MiXeD_CoDe_key", "IxX", "ViEw"] {
        assert!(class_row(pg.inner(), name).is_some(), "missing {name}");
    }
    pg.execute("ALTER TABLE \"MiXeD\" RENAME TO \"NewName\"")
        .unwrap();
    assert_eq!(class_row(pg.inner(), "NewName").unwrap().0, oid);
    pg.inner().force_reparse_schema_without_publish().unwrap();
    assert_eq!(class_row(pg.inner(), "NewName").unwrap().0, oid);
}

#[turso_macros::test(mvcc)]
fn views_have_class_rows_and_return_rules(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE t (a int, b text)").unwrap();
    pg.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let rows = pg.prepare("SELECT c.relkind, c.relnatts, c.relhasrules, r.rulename, r.ev_type, r.ev_enabled, r.is_instead FROM pg_class c JOIN pg_rewrite r ON r.ev_class = c.oid WHERE c.relname = 'v'").unwrap().run_collect_rows().unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::build_text("v"),
            Value::from_i64(2),
            Value::from_i64(1),
            Value::build_text("_RETURN"),
            Value::build_text("1"),
            Value::build_text("O"),
            Value::from_i64(1)
        ]]
    );
    pg.execute("DROP VIEW v").unwrap();
    assert!(pg
        .prepare("SELECT * FROM pg_rewrite")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .is_empty());
}
#[turso_macros::test(mvcc)]
fn ddl_updates_stored_class_columns(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = pg.inner();
    pg.execute("CREATE TABLE counts (id int PRIMARY KEY, x int CHECK (x > 0))")
        .unwrap();
    let read = || {
        conn.prepare_engine_schema_statement(
            "SELECT relnatts, relhasindex, relchecks FROM pg_class WHERE relname = 'counts'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap()
    };
    assert_eq!(
        read(),
        vec![vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::from_i64(1)
        ]]
    );
    pg.execute("ALTER TABLE counts ADD COLUMN y int").unwrap();
    assert_eq!(
        read(),
        vec![vec![
            Value::from_i64(3),
            Value::from_i64(1),
            Value::from_i64(1)
        ]]
    );
    pg.execute("ALTER TABLE counts DROP COLUMN y").unwrap();
    assert_eq!(
        read(),
        vec![vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::from_i64(1)
        ]]
    );
    pg.execute("CREATE TABLE no_index (a int)").unwrap();
    pg.execute("CREATE INDEX ix ON no_index (a)").unwrap();
    pg.execute("DROP INDEX ix").unwrap();
    let rows = conn
        .prepare_engine_schema_statement(
            "SELECT relhasindex FROM pg_class WHERE relname='no_index'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(0)]]);
}

#[turso_macros::test(mvcc)]
fn catalog_class_rows_match_the_exposed_attributes(db: TempDatabase) {
    let pg = db.connect_postgres();
    let conn = pg.inner();
    let rows = conn
        .prepare_engine_schema_statement(
            "SELECT oid, relnamespace FROM pg_class WHERE relname='pg_class'",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(1259), Value::from_i64(11)]]);
    let rows = conn.prepare_engine_schema_statement("SELECT c.relname, c.relnatts, count(a.attnum) FROM main.pg_class c LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 WHERE c.oid<16384 GROUP BY c.oid, c.relname, c.relnatts HAVING c.relnatts<>count(a.attnum)").unwrap().run_collect_rows().unwrap();
    assert!(rows.is_empty(), "{rows:?}");
}

#[turso_macros::test(mvcc)]
fn pg_class_scans_the_stored_rows(db: TempDatabase) {
    let pg = db.connect_postgres();
    let rows = pg
        .prepare("SELECT oid, relnamespace FROM pg_class WHERE relname='pg_class'")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(1259), Value::from_i64(11)]]);
    pg.execute("CREATE TABLE visible (a int)").unwrap();
    pg.execute("BEGIN").unwrap();
    pg.execute("ALTER TABLE visible ADD COLUMN b text").unwrap();
    let read = || {
        pg.prepare("SELECT relnatts FROM pg_class WHERE relname='visible'")
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };
    assert_eq!(read(), vec![vec![Value::from_i64(2)]]);
    pg.execute("ROLLBACK").unwrap();
    assert_eq!(read(), vec![vec![Value::from_i64(1)]]);
}

#[turso_macros::test(mvcc)]
fn random_ddl_keeps_stored_classes_in_agreement_with_schema(db: TempDatabase) {
    use rand::{Rng, SeedableRng};
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(7919);
    let pg = db.connect_postgres();
    for step in 0..24 {
        pg.execute("BEGIN").unwrap();
        let primary = if rng.random_bool(0.5) {
            "PRIMARY KEY"
        } else {
            ""
        };
        pg.execute(format!(
            "CREATE TABLE t{step} (id int {primary}, x int UNIQUE CHECK (x > 0))"
        ))
        .unwrap();
        pg.execute(format!("CREATE VIEW v{step} AS SELECT id FROM t{step}"))
            .unwrap();
        pg.execute(format!("CREATE SEQUENCE s{step}")).unwrap();
        pg.execute(format!("CREATE INDEX ix{step} ON t{step} (x)"))
            .unwrap();
        if rng.random_bool(0.5) {
            pg.execute(format!("DROP INDEX ix{step}")).unwrap();
        }
        pg.execute(format!("ALTER TABLE t{step} ADD COLUMN added text"))
            .unwrap();
        if rng.random_bool(0.5) {
            pg.execute(format!("ALTER TABLE t{step} DROP COLUMN added"))
                .unwrap();
        }
        pg.execute(format!("ALTER TABLE t{step} RENAME TO renamed{step}"))
            .unwrap();
        let mismatches = pg.prepare("SELECT c.relname FROM pg_class c LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 GROUP BY c.oid,c.relname,c.relnatts HAVING count(a.attnum)<>c.relnatts").unwrap().run_collect_rows().unwrap();
        assert!(mismatches.is_empty(), "step {step}: {mismatches:?}");
        pg.execute(if rng.random_bool(0.5) {
            "COMMIT"
        } else {
            "ROLLBACK"
        })
        .unwrap();
        // Reload checks both identities and derived counts against Schema.
        pg.inner().force_reparse_schema_without_publish().unwrap();
    }
}

#[test]
fn a_stored_class_without_a_relation_fails_schema_load() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE orphan (x int)").unwrap();
    pg.inner()
        .prepare_engine_schema_statement("DROP TABLE public.orphan")
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    let error = pg
        .inner()
        .force_reparse_schema_without_publish()
        .unwrap_err();
    assert!(
        matches!(error, turso_core::LimboError::Corrupt(_)),
        "{error}"
    );
}

#[turso_macros::test(mvcc)]
fn changing_table_owner_updates_every_stored_index(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE ROLE new_owner").unwrap();
    pg.execute("CREATE TABLE owned (id integer PRIMARY KEY, x integer UNIQUE)")
        .unwrap();
    pg.execute("CREATE INDEX owned_ix ON owned(x)").unwrap();
    pg.execute("ALTER TABLE owned OWNER TO new_owner").unwrap();
    let rows = class_rows(pg.inner());
    let owner = class_row(pg.inner(), "owned").unwrap().4;
    assert_ne!(owner, 0);
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.4 == owner), "{rows:?}");
    pg.inner().force_reparse_schema_without_publish().unwrap();
    assert_eq!(class_rows(pg.inner()), rows);
}

#[test]
fn a_missing_rowid_primary_key_class_fails_schema_load() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE t (id int PRIMARY KEY, x int UNIQUE)")
        .unwrap();
    pg.inner()
        .prepare_engine_schema_statement(
            "DELETE FROM __turso_internal_classes WHERE relname='t_pkey'",
        )
        .unwrap()
        .run_ignore_rows()
        .unwrap();
    let error = pg
        .inner()
        .force_reparse_schema_without_publish()
        .unwrap_err();
    assert!(
        matches!(error, turso_core::LimboError::Corrupt(_)),
        "{error}"
    );
}

#[turso_macros::test(mvcc)]
fn user_tables_with_catalog_names_have_attributes(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE public.pg_class (id int PRIMARY KEY, x text)")
        .unwrap();
    let rows = pg
        .prepare(
            "SELECT c.relname, c.relnatts, count(a.attnum) FROM pg_catalog.pg_class c \
         LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 \
         WHERE c.relnamespace=2200 GROUP BY c.oid,c.relname,c.relnatts ORDER BY c.relname",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec![
                Value::build_text("pg_class"),
                Value::from_i64(2),
                Value::from_i64(2)
            ],
            vec![
                Value::build_text("pg_class_pkey"),
                Value::from_i64(1),
                Value::from_i64(1)
            ],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn pg_class_is_a_read_only_virtual_catalog(db: TempDatabase) {
    let pg = db.connect_postgres();
    assert!(pg
        .inner()
        .current_schema()
        .get_btree_table(turso_core::NamespaceId::ROOT, "pg_class")
        .is_none());
    assert_eq!(
        pg.prepare("SELECT * FROM pg_catalog.pg_class")
            .unwrap()
            .num_columns(),
        34
    );
    assert_eq!(
        pg.prepare("SELECT tableoid FROM pg_class WHERE oid=1259")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(1259)]]
    );
    for sql in [
        "DELETE FROM pg_class",
        "UPDATE pg_class SET relname='broken'",
        "DROP TABLE pg_catalog.pg_class",
        "ALTER TABLE pg_class ADD COLUMN broken int",
        "TRUNCATE pg_class",
    ] {
        assert!(pg.execute(sql).is_err(), "{sql} must fail");
    }
    pg.execute("CREATE ROLE catalog_reader").unwrap();
    pg.execute("SET ROLE catalog_reader").unwrap();
    assert_eq!(
        pg.prepare("SELECT relname FROM pg_catalog.pg_class WHERE oid=1259")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::build_text("pg_class")]]
    );
}

#[turso_macros::test(mvcc)]
fn changing_a_star_views_width_is_rejected_before_writing(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE source (a int)").unwrap();
    pg.execute("CREATE VIEW star_view AS SELECT * FROM source")
        .unwrap();
    let error = pg
        .execute("ALTER TABLE source ADD COLUMN b int")
        .unwrap_err();
    assert_eq!(error.sqlstate, "0A000");
    assert_eq!(
        error.message,
        "cannot alter table \"source\" because it would change the number of columns returned by view \"star_view\""
    );
    assert_eq!(
        error.hint.as_deref(),
        Some("Drop view \"star_view\", alter table \"source\", and then recreate the view.")
    );
    assert_eq!(
        pg.prepare("SELECT relnatts FROM pg_class WHERE relname IN ('source', 'star_view')")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(1)]]
    );
    pg.inner().force_reparse_schema_without_publish().unwrap();
    pg.execute("DROP VIEW star_view").unwrap();
    pg.execute("ALTER TABLE source ADD COLUMN b int").unwrap();
    pg.execute("CREATE VIEW star_view AS SELECT * FROM source")
        .unwrap();
    assert_eq!(
        pg.prepare("SELECT * FROM star_view").unwrap().num_columns(),
        2
    );
    pg.execute("CREATE VIEW dependent_view AS SELECT * FROM star_view")
        .unwrap();
    pg.inner().force_reparse_schema_without_publish().unwrap();
    drop(pg);
    let db = db.reopen();
    let pg = db.connect_postgres();
    assert_eq!(
        pg.prepare("SELECT a, b FROM dependent_view")
            .unwrap()
            .num_columns(),
        2
    );
    assert_eq!(
        pg.prepare(
            "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = 'dependent_view' AND a.attnum > 0 ORDER BY a.attnum"
        )
        .unwrap()
        .run_collect_rows()
        .unwrap(),
        vec![vec![Value::build_text("a")], vec![Value::build_text("b")]]
    );
}

#[turso_macros::test(mvcc)]
fn joined_view_columns_survive_schema_load(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE SCHEMA other").unwrap();
    pg.execute("CREATE TABLE t (a int, b int)").unwrap();
    pg.execute("CREATE TABLE other.u (a int, c int)").unwrap();
    pg.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    pg.execute("INSERT INTO other.u VALUES (1, 3)").unwrap();
    pg.execute("CREATE VIEW v AS SELECT * FROM t JOIN other.u USING (a)")
        .unwrap();
    pg.execute("CREATE VIEW dependent AS SELECT * FROM v")
        .unwrap();
    pg.inner().force_reparse_schema_without_publish().unwrap();
    drop(pg);
    let db = db.reopen();
    let pg = db.connect_postgres();
    assert_eq!(
        pg.prepare("SELECT * FROM dependent")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![
            Value::from_i64(1),
            Value::from_i64(2),
            Value::from_i64(3)
        ]]
    );
    assert_eq!(
        pg.prepare("SELECT relnatts FROM pg_class WHERE relname IN ('v', 'dependent')")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(3)], vec![Value::from_i64(3)]]
    );
}

#[turso_macros::test(mvcc)]
fn stored_system_catalogs_reject_policy_and_owner_changes(db: TempDatabase) {
    let pg = db.connect_postgres();
    for table in ["pg_class", "pg_description"] {
        for sql in [
            format!("CREATE POLICY p ON {table} USING (true)"),
            format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
            format!("ALTER TABLE {table} DISABLE ROW LEVEL SECURITY"),
            format!("ALTER TABLE {table} OWNER TO CURRENT_USER"),
        ] {
            let error = pg.execute(&sql).unwrap_err();
            assert_eq!(
                error.sqlstate,
                if table == "pg_class" && sql.starts_with("CREATE POLICY") {
                    "42809"
                } else {
                    "42501"
                },
                "{sql}: {error:?}"
            );
        }
        if table == "pg_class" {
            assert_eq!(
                pg.execute(format!("DROP POLICY IF EXISTS p ON {table}"))
                    .unwrap_err()
                    .sqlstate,
                "42809"
            );
        } else {
            pg.execute(format!("DROP POLICY IF EXISTS p ON {table}"))
                .unwrap();
        }
        pg.execute(format!("GRANT SELECT ON {table} TO PUBLIC"))
            .unwrap();
        for privilege in ["GRANT INSERT ON", "REVOKE SELECT ON"] {
            let sql = if privilege.starts_with("GRANT") {
                format!("{privilege} {table} TO PUBLIC")
            } else {
                format!("{privilege} {table} FROM PUBLIC")
            };
            let error = pg.execute(&sql).unwrap_err();
            assert_eq!(error.sqlstate, "0A000", "{sql}: {error:?}");
        }
    }
    pg.execute("CREATE TABLE public.pg_class (id int)").unwrap();
    pg.execute("CREATE POLICY p ON public.pg_class USING (true)")
        .unwrap();
    pg.execute("ALTER TABLE public.pg_class ENABLE ROW LEVEL SECURITY")
        .unwrap();
    pg.execute("DROP POLICY p ON public.pg_class").unwrap();
    pg.inner().force_reparse_schema_without_publish().unwrap();
}

#[turso_macros::test(mvcc)]
fn long_relation_names_survive_schema_reload(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.inner().pragma_update("foreign_keys", "ON").unwrap();
    for name in ["a".repeat(60), "é".repeat(30)] {
        pg.execute(format!("CREATE TABLE \"{name}\" (x text PRIMARY KEY)"))
            .unwrap();
        pg.execute(format!(
            "CREATE TABLE refs (x text REFERENCES \"{name}\"(x))"
        ))
        .unwrap();
        pg.inner().force_reparse_schema_without_publish().unwrap();
        let error = pg
            .execute("INSERT INTO refs VALUES ('missing')")
            .unwrap_err();
        assert_eq!(error.sqlstate, "23503");
        pg.execute(format!("INSERT INTO \"{name}\" VALUES ('present')"))
            .unwrap();
        pg.execute("INSERT INTO refs VALUES ('present')").unwrap();
        pg.execute("DROP TABLE refs").unwrap();
        pg.execute(format!("DROP TABLE \"{name}\"")).unwrap();
    }
}

#[turso_macros::test(mvcc)]
fn quoted_relation_names_keep_engine_lookup(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE \"MiXeD\" (id int, x text)")
        .unwrap();
    pg.execute("CREATE INDEX \"IxX\" ON \"MiXeD\"(x)").unwrap();
    pg.execute("CREATE VIEW \"ViEw\" AS SELECT x FROM \"MiXeD\"")
        .unwrap();
    for name in ["MiXeD", "IxX", "ViEw"] {
        assert!(class_row(pg.inner(), name).is_some(), "missing {name}");
    }
    let oid = class_row(pg.inner(), "MiXeD").unwrap().0;
    pg.execute("ALTER TABLE \"MiXeD\" RENAME TO \"NewName\"")
        .unwrap();
    pg.inner().force_reparse_schema_without_publish().unwrap();
    assert_eq!(class_row(pg.inner(), "NewName").unwrap().0, oid);
    pg.execute("INSERT INTO \"NewName\" VALUES (1, 'x')")
        .unwrap();
    pg.execute("DROP VIEW \"ViEw\"").unwrap();
    pg.execute("DROP TABLE \"NewName\"").unwrap();
    assert!(class_rows(pg.inner()).is_empty());
}

#[turso_macros::test(mvcc)]
fn virtual_catalog_lists_only_implemented_relations(db: TempDatabase) {
    let pg = db.connect_postgres();
    assert_implemented_catalog_rows(pg.inner());
    drop(pg);
    let db = db.reopen();
    let pg = db.connect_postgres();
    assert_implemented_catalog_rows(pg.inner());
}

fn assert_implemented_catalog_rows(conn: &Arc<Connection>) {
    let stored_builtins = conn
        .prepare_engine_schema_statement(
            "SELECT count(*) FROM __turso_internal_classes WHERE oid < 16384",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(stored_builtins, vec![vec![Value::from_i64(0)]]);
    let rows = conn
        .prepare_engine_schema_statement(
            "SELECT relname FROM pg_class WHERE oid < 16384 ORDER BY relname",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    let expected = [
        "pg_am",
        "pg_attrdef",
        "pg_attribute",
        "pg_auth_members",
        "pg_cast",
        "pg_class",
        "pg_constraint",
        "pg_database",
        "pg_depend",
        "pg_description",
        "pg_index",
        "pg_indexes",
        "pg_language",
        "pg_namespace",
        "pg_operator",
        "pg_policies",
        "pg_policy",
        "pg_proc",
        "pg_rewrite",
        "pg_roles",
        "pg_sequence",
        "pg_sequences",
        "pg_settings",
        "pg_tables",
        "pg_type",
    ];
    let expected: Vec<_> = expected
        .into_iter()
        .map(|name| vec![Value::from_text(name)])
        .collect();
    assert_eq!(rows, expected);
}

#[turso_macros::test(mvcc)]
fn column_ddl_does_not_rewrite_relation_identity(db: TempDatabase) {
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE derived (x int CHECK (x > 0))")
        .unwrap();
    let stored = || {
        pg.inner()
            .prepare_engine_schema_statement(
                "SELECT * FROM __turso_internal_classes WHERE relname = 'derived'",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };
    let before = stored();
    pg.execute("ALTER TABLE derived ADD COLUMN y int").unwrap();
    assert_eq!(
        stored(),
        before,
        "column metadata belongs to the schema, not the identity row"
    );
    let read = || {
        pg.prepare("SELECT relnatts, relchecks FROM pg_class WHERE relname = 'derived'")
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };
    assert_eq!(read(), vec![vec![Value::from_i64(2), Value::from_i64(1)]]);
    pg.inner().force_reparse_schema_without_publish().unwrap();
    assert_eq!(read(), vec![vec![Value::from_i64(2), Value::from_i64(1)]]);
}

#[test]
fn an_obsolete_relation_catalog_layout_is_refused_before_reading_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old-catalog.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        // Earlier layouts stored the engine namespace and ACL under these names.
        conn.execute_batch(
            "CREATE TABLE __turso_internal_classes (
            oid INTEGER PRIMARY KEY, relname TEXT, relnamespace INTEGER,
            relkind TEXT, relowner INTEGER, relownedby INTEGER, relacl TEXT,
            relrowsecurity INTEGER, relpolicies TEXT, relownedbycolumn TEXT)",
        )
        .unwrap();
    }
    let io = Arc::new(turso_core::PlatformIO::new().unwrap());
    let error = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("earlier version of tursopg"),
        "{error}"
    );
}
