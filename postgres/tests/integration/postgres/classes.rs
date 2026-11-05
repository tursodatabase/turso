//! The `__turso_internal_classes` relation catalog: created by the
//! PostgreSQL frontend when it opens a database writable, maintained by
//! every dialect in the same transaction as `sqlite_schema`.

use std::sync::Arc;

use crate::common::TempDatabase;
use turso_core::{Connection, Value};

/// The class rows as `(oid, relname, relnamespace, relkind, relowner)`,
/// ordered by oid.
fn class_rows(conn: &Arc<Connection>) -> Vec<(i64, String, i64, String, i64)> {
    let rows = conn
        .prepare("SELECT oid, relname, relnamespace, relkind, relowner FROM __turso_internal_classes ORDER BY oid")
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
        let conn = db.connect(turso_core::ConnectOpts::new()).unwrap();
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
fn dropping_a_table_releases_the_sequences_it_owned() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE ROLE alice").unwrap();
    pg.execute("CREATE TABLE d (x serial)").unwrap();
    let error = pg
        .execute("ALTER SEQUENCE d_x_seq OWNER TO alice")
        .unwrap_err()
        .to_string();
    assert!(error.contains("cannot change owner of sequence"), "{error}");
    // Turso keeps the sequence of a dropped table, unlike PostgreSQL, so it
    // becomes a sequence of its own.
    pg.execute("DROP TABLE d").unwrap();
    pg.execute("ALTER SEQUENCE d_x_seq OWNER TO alice").unwrap();
}
