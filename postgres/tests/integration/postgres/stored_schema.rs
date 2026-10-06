use crate::common::TempDatabase;
use turso_core::Value;
use turso_pg::PgConnection;

fn rows(conn: &PgConnection, sql: &str) -> Vec<String> {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "NULL".to_string(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[turso_macros::test(mvcc)]
fn add_and_drop_column_keep_function_defaults_readable(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE s (id serial PRIMARY KEY, a text, b text, \
         ts timestamp DEFAULT now(), c text DEFAULT 'x'::text, \
         e text DEFAULT ('y' || 'z'))",
    )
    .unwrap();
    conn.execute("INSERT INTO s (a, b, ts) VALUES ('one', 'gone', '2024-01-01 10:00:00')")
        .unwrap();
    conn.execute("ALTER TABLE s DROP COLUMN b").unwrap();
    conn.execute("ALTER TABLE s ADD COLUMN d text DEFAULT 'q'")
        .unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO s (a, ts) VALUES ('two', '2024-01-02 10:00:00')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, a, ts, c, e, d FROM s ORDER BY id"),
        [
            "1|one|2024-01-01 10:00:00|x|yz|q",
            "2|two|2024-01-02 10:00:00|x|yz|q"
        ]
    );
}

fn open_with_sqlite_dialect(path: &std::path::Path) -> std::sync::Arc<turso_core::Connection> {
    let io: std::sync::Arc<dyn turso_core::IO + Send> =
        std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
        None,
        std::sync::Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    db.connect().unwrap()
}

fn sqlite_rows(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "NULL".to_string(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[turso_macros::test(mvcc)]
fn new_tables_store_sql_that_both_dialects_load(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
        .unwrap();
    conn.execute("CREATE DOMAIN posint AS integer CHECK (VALUE > 0)")
        .unwrap();
    conn.execute(
        "CREATE TABLE t (id serial PRIMARY KEY, a bigint UNIQUE, \
         n numeric(10,2) DEFAULT 1.5, ts timestamp, d text[] DEFAULT '{}', m mood, \
         p posint, v varchar(10) CHECK (length(v) > 1), b boolean DEFAULT true, \
         UNIQUE (a, n))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO t (a, n, ts, m, p, v) \
         VALUES (7, 2.5, '2024-01-01 10:00:00', 'ok', 3, 'vv')",
    )
    .unwrap();
    drop(conn);
    let path = db.path.clone();
    drop(db);

    let conn = open_with_sqlite_dialect(&path);
    assert_eq!(
        sqlite_rows(&conn, "SELECT sql FROM sqlite_schema WHERE name = 't'"),
        [
            "CREATE TABLE t (id INTEGER PRIMARY KEY DEFAULT (nextval ('t_id_seq')), \
          a bigint UNIQUE, n numeric (10, 2) DEFAULT 1.5, ts timestamp, d TEXT[] DEFAULT '{}', \
          m mood, p posint, v varchar (10) CHECK (length (v) > 1), b boolean DEFAULT 1, \
          UNIQUE (a, n)) STRICT, PGSTORAGE"
        ]
    );
    let expected = ["1|7|2.50|2024-01-01 10:00:00|{}|ok|3|vv|1"];
    assert_eq!(
        sqlite_rows(&conn, "SELECT id, a, n, ts, d, m, p, v, b FROM t"),
        expected
    );
    for bad in [
        "INSERT INTO t (id, m) VALUES (2, 'angry')",
        "INSERT INTO t (id, p) VALUES (2, -1)",
    ] {
        assert!(conn.execute(bad).is_err(), "{bad}");
    }
    conn.close().unwrap();
    drop(conn);

    let db = TempDatabase::builder().with_db_path(path).build();
    let conn = db.connect_postgres();
    assert_eq!(
        rows(&conn, "SELECT id, a, n, ts, d, m, p, v, b FROM t"),
        ["1|7|2.50|2024-01-01 10:00:00|{}|ok|3|vv|1"]
    );
}

#[turso_macros::test(mvcc)]
fn rename_keeps_the_file_readable(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE r (id bigint PRIMARY KEY, ts timestamp DEFAULT now(), \
         n numeric(10,2))",
    )
    .unwrap();
    conn.execute("INSERT INTO r VALUES (1, '2024-01-01 10:00:00', 2.5)")
        .unwrap();
    conn.execute("ALTER TABLE r RENAME COLUMN n TO amount")
        .unwrap();
    conn.execute("ALTER TABLE r RENAME TO r2").unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    conn.execute("INSERT INTO r2 (id, amount) VALUES (2, 3)")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, amount FROM r2 ORDER BY id"),
        ["1|2.50", "2|3.00"]
    );
}

#[turso_macros::test(mvcc)]
fn alter_column_type_is_refused_and_keeps_the_table(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE ac (id bigint PRIMARY KEY, i integer, ts text)")
        .unwrap();
    conn.execute("INSERT INTO ac VALUES (1, 7, 'hello')")
        .unwrap();
    let err = conn
        .execute("ALTER TABLE ac ALTER COLUMN i TYPE numeric(10,2)")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("ALTER TABLE ... ALTER COLUMN ... TYPE is not supported"),
        "{err}"
    );
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(rows(&conn, "SELECT id, i, ts FROM ac"), ["1|7|hello"]);
}

#[turso_macros::test]
fn pg_prefix_is_reserved_for_built_in_types(db: TempDatabase) {
    let conn = db.connect_postgres();
    for sql in [
        "CREATE TYPE pg_mood AS ENUM ('a', 'b')",
        "CREATE DOMAIN pg_posint AS integer CHECK (VALUE > 0)",
    ] {
        let err = conn.execute(sql).unwrap_err();
        assert!(
            err.to_string()
                .contains("names that start with \"pg_\" are reserved for built-in types"),
            "{sql}: {err}"
        );
    }
}

fn attach_schema_file(db: &TempDatabase, schema: &str) -> turso_core::Result<PgConnection> {
    let path = db
        .path
        .with_file_name(format!("turso-postgres-schema-{schema}.db"));
    let core = db.connect_limbo();
    core.execute(format!("ATTACH '{}' AS {schema}", path.display()))?;
    Ok(PgConnection::new(core))
}

/// A table in a schema file uses the types of the main database.
#[turso_macros::test]
fn schema_file_tables_resolve_types_through_the_main_database(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TYPE mood AS ENUM ('sad', 'ok')")
        .unwrap();
    conn.execute("CREATE SCHEMA s").unwrap();
    conn.execute("CREATE TABLE s.t (id integer PRIMARY KEY, m mood)")
        .unwrap();
    conn.execute("INSERT INTO s.t VALUES (1, 'ok')").unwrap();
    drop(conn);
    let schema_file = db.path.with_file_name("turso-postgres-schema-s.db");

    let db = db.reopen();
    let conn = attach_schema_file(&db, "s").unwrap();
    assert_eq!(rows(&conn, "SELECT id, m FROM s.t"), ["1|ok"]);
    assert!(conn.execute("INSERT INTO s.t VALUES (2, 'angry')").is_err());
    drop(conn);
    drop(db);

    let io: std::sync::Arc<dyn turso_core::IO + Send> =
        std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let Err(err) = turso_core::Database::open_file_with_flags(
        io,
        schema_file.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
        None,
        std::sync::Arc::new(turso_core::SqliteDialect),
    ) else {
        panic!("a schema file without the types of its main database must not open");
    };
    assert!(
        err.to_string()
            .contains("column t.m has type \"mood\", which this database does not define"),
        "{err}"
    );

    let other = TempDatabase::builder().build();
    for suffix in ["", "-wal"] {
        let file = format!("turso-postgres-schema-s.db{suffix}");
        std::fs::copy(
            schema_file.with_file_name(&file),
            other.path.with_file_name(&file),
        )
        .unwrap();
    }
    let Err(err) = attach_schema_file(&other, "s") else {
        panic!("a main database without the type must refuse the schema file");
    };
    assert!(
        err.to_string()
            .contains("column t.m has type \"mood\", which this database does not define"),
        "{err}"
    );
}

fn core_rows(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "NULL".to_string(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// VACUUM through the core API of a PostgreSQL connection: the replayed
/// table SQL must create the same tables, internal tables included.
#[turso_macros::test]
fn vacuum_keeps_tables_and_values(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TYPE mood AS ENUM ('sad', 'ok')")
        .unwrap();
    conn.execute("CREATE TABLE v (id serial PRIMARY KEY, n numeric(10,2), m mood, ts timestamp)")
        .unwrap();
    conn.execute("CREATE INDEX v_ts ON v (ts)").unwrap();
    conn.execute("INSERT INTO v (n, m, ts) VALUES (2.5, 'ok', '2024-01-01 10:00:00')")
        .unwrap();
    let core = db.connect_limbo();
    let schema_sql = "SELECT name, replace(sql, ' ', '') FROM sqlite_schema \
                      WHERE type = 'table' ORDER BY name";
    let before = core_rows(&core, schema_sql);
    assert_eq!(before.len(), 3);

    core.execute("VACUUM").unwrap();

    assert_eq!(core_rows(&core, schema_sql), before);
    assert_eq!(core_rows(&core, "PRAGMA integrity_check"), ["ok"]);
    conn.execute("INSERT INTO v (n, m) VALUES (3, 'sad')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, n, m, ts FROM v ORDER BY id"),
        ["1|2.50|ok|2024-01-01 10:00:00", "2|3.00|sad|NULL"]
    );
}

#[turso_macros::test]
fn catalog_shows_postgres_ddl_and_defaults_of_new_tables(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE c (id serial PRIMARY KEY, n numeric(10,2) NOT NULL, \
         ts timestamp DEFAULT now(), t text, r double precision, b bytea, k integer UNIQUE)",
    )
    .unwrap();
    let ddl: Vec<String> = rows(&conn, "SELECT table_name, ddl FROM pg_get_tabledef")
        .into_iter()
        .filter_map(|row| row.strip_prefix("c|").map(str::to_string))
        .collect();
    assert_eq!(
        ddl,
        [
            "CREATE TABLE c (id serial PRIMARY KEY, n numeric (10, 2) NOT NULL, \
          ts timestamp DEFAULT (now ()), t text, r double precision, b bytea, \
          k integer UNIQUE)"
        ]
    );
    assert_eq!(
        rows(&conn, "SELECT adnum, adbin FROM pg_attrdef ORDER BY adnum"),
        ["1|nextval ('c_id_seq')", "3|now ()"]
    );
}
