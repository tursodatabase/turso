//! The `__turso_internal_classes` relation catalog: created by the
//! PostgreSQL frontend when it opens a database writable, maintained by
//! every dialect in the same transaction as `sqlite_schema`.

use std::sync::Arc;

use crate::common::TempDatabase;
use turso_core::{Connection, SessionRole, Value};

/// The class rows as `(oid, relname, relnamespace, relkind, relowner)`,
/// ordered by oid.
fn class_rows(conn: &Arc<Connection>) -> Vec<(i64, String, i64, String, i64)> {
    let rows = conn
        .prepare_engine_schema_statement("SELECT oid, relname, relnamespace, relkind, relowner FROM __turso_internal_classes ORDER BY oid")
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
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.0 >= 16384));
    assert_ne!(rows[0].0, rows[1].0);
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
    assert_eq!(class_rows(pg.inner()).len(), 2);
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
    // The table and the automatic index of its UNIQUE constraint; the rowid
    // alias primary key has no backing index.
    assert_eq!(rows.len(), 2, "{rows:?}");
    let (t_oid, _, t_namespace, t_kind, t_owner) = class_row(&conn, "t").unwrap();
    assert_eq!(t_oid, 16384);
    assert_eq!(t_namespace, 1, "public is namespace 1");
    assert_eq!(t_kind, "r");
    assert_eq!(t_owner, 0, "owned by the bootstrap superuser");
    let autoindex = rows.iter().find(|row| row.3 == "i").unwrap();
    assert!(autoindex.1.starts_with("sqlite_autoindex_"), "{rows:?}");
    assert_eq!(autoindex.0, 16385);

    pg.execute("CREATE INDEX t_name ON t (name)").unwrap();
    assert_eq!(
        class_row(&conn, "t_name").unwrap(),
        (16386, "t_name".to_string(), 1, "i".to_string(), 0)
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
    // The automatic index was renamed with its table, keeping its oid.
    let autoindex_after = class_rows(&conn)
        .into_iter()
        .find(|row| row.3 == "i")
        .unwrap();
    assert_eq!(autoindex_after.0, autoindex_before.0);
    assert!(autoindex_after.1.contains("_u_"), "{autoindex_after:?}");
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
            "SELECT relacl FROM __turso_internal_classes WHERE relname = '{relname}'"
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
        "SELECT json_extract(p.value, '$.polname'), json_extract(p.value, '$.oid') \
         FROM __turso_internal_classes c, json_each(c.relpolicies) p \
         WHERE c.relname = '{relname}' ORDER BY p.key"
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
