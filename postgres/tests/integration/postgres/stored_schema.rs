use crate::common::{core_rows, open_with_sqlite_dialect, rows, TempDatabase};
use turso_pg::PgConnection;

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

    let conn = open_with_sqlite_dialect(&path).unwrap().connect().unwrap();
    assert_eq!(
        core_rows(&conn, "SELECT sql FROM sqlite_schema WHERE name = 't'"),
        [
            "CREATE TABLE t (id pg_int4 PRIMARY KEY DEFAULT (nextval ('t_id_seq')), \
          a pg_int8 UNIQUE, n pg_numeric (10, 2) DEFAULT 1.5, ts pg_timestamp, d TEXT[] DEFAULT '{}', \
          m mood, p posint, v varchar (10) CHECK (length (v) > 1), b boolean DEFAULT 1, \
          UNIQUE (a, n)) STRICT, PGSTORAGE"
        ]
    );
    let expected = ["1|7|2.50|2024-01-01 10:00:00|{}|ok|3|vv|1"];
    assert_eq!(
        core_rows(&conn, "SELECT id, a, n, ts, d, m, p, v, b FROM t"),
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

    let Err(err) = open_with_sqlite_dialect(&schema_file) else {
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
    let copy = db.path.with_file_name("vacuum-into-copy.db");
    core.execute(format!("VACUUM INTO '{}'", copy.display()))
        .unwrap();

    assert_eq!(core_rows(&core, schema_sql), before);
    assert_eq!(core_rows(&core, "PRAGMA integrity_check"), ["ok"]);
    conn.execute("INSERT INTO v (n, m) VALUES (3, 'sad')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, n, m, ts FROM v ORDER BY id"),
        ["1|2.50|ok|2024-01-01 10:00:00", "2|3.00|sad|NULL"]
    );

    let copy = TempDatabase::builder().with_db_path(copy).build();
    let copy_conn = copy.connect_postgres();
    assert_eq!(core_rows(copy_conn.inner(), schema_sql), before);
    copy_conn
        .execute("INSERT INTO v (n, m) VALUES (4, 'ok')")
        .unwrap();
    assert!(copy_conn
        .execute("INSERT INTO v (n, m) VALUES (5, 'angry')")
        .is_err());
    assert_eq!(
        rows(&copy_conn, "SELECT id, n, m, ts FROM v ORDER BY id"),
        ["1|2.50|ok|2024-01-01 10:00:00", "2|4.00|ok|NULL"]
    );
}

#[turso_macros::test]
fn catalog_shows_postgres_ddl_and_defaults_of_new_tables(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE SEQUENCE f_c_seq").unwrap();
    let tables = [
        (
            "CREATE TABLE c (id serial PRIMARY KEY, n numeric(10,2) NOT NULL, \
             ts timestamp DEFAULT now(), t text, r double precision, b bytea, k integer UNIQUE)",
            "c|CREATE TABLE c (id serial PRIMARY KEY, n numeric (10, 2) NOT NULL, \
             ts timestamp DEFAULT (now ()), t text, r double precision, b bytea, \
             k integer UNIQUE)",
        ),
        (
            "CREATE TABLE f (a bigserial PRIMARY KEY, b smallserial, \
             c bigint DEFAULT nextval('f_c_seq'), d text DEFAULT nextval('f_c_seq'), \
             e boolean DEFAULT false, g boolean DEFAULT true, h integer[] DEFAULT ARRAY[1, 2], \
             i integer[][] DEFAULT ARRAY[ARRAY[1], ARRAY[2]])",
            "f|CREATE TABLE f (a bigserial PRIMARY KEY, b serial NOT NULL, \
             c bigint DEFAULT (nextval ('f_c_seq')), d text DEFAULT (nextval ('f_c_seq')), \
             e boolean DEFAULT FALSE, g boolean DEFAULT TRUE, h integer[] DEFAULT (ARRAY[1, 2]), \
             i integer[][] DEFAULT (ARRAY[ARRAY[1], ARRAY[2]]))",
        ),
        (
            "CREATE TABLE \"MixedT\" (id serial PRIMARY KEY, \"Value\" text)",
            "mixedt|CREATE TABLE \"MixedT\" (id serial PRIMARY KEY, \"Value\" text)",
        ),
        (
            "CREATE TABLE g (d date DEFAULT '2024-01-01'::date, tz timestamptz, tm time, \
             big bigint, t text CHECK (t::date > '2020-01-01' AND t::time < '23:00'), \
             CHECK (t::timestamptz > '2000-01-01'))",
            "g|CREATE TABLE g (d date DEFAULT (CAST ('2024-01-01' AS date)), tz timestamptz, \
             tm time, big bigint, t text CHECK (CAST (t AS date) > '2020-01-01' \
             AND CAST (t AS time) < '23:00'), CHECK (CAST (t AS timestamptz) > '2000-01-01'))",
        ),
    ];
    for (create, _) in tables {
        conn.execute(create).unwrap();
    }
    let ddl: Vec<String> = rows(
        &conn,
        "SELECT table_name, ddl FROM pg_get_tabledef \
         WHERE table_name IN ('c', 'f', 'g', 'mixedt') ORDER BY table_name",
    );
    let mut expected: Vec<&str> = tables.iter().map(|(_, ddl)| *ddl).collect();
    expected.sort();
    assert_eq!(ddl, expected);
    assert_eq!(
        rows(
            &conn,
            "SELECT adnum, adbin FROM pg_attrdef JOIN pg_class ON pg_class.oid = adrelid \
             WHERE relname = 'c' ORDER BY adnum"
        ),
        ["1|nextval ('c_id_seq')", "3|now ()"]
    );

    let other = TempDatabase::builder().build();
    let other_conn = other.connect_postgres();
    other_conn.execute("CREATE SEQUENCE f_c_seq").unwrap();
    for row in ddl {
        let (_, ddl) = row.split_once('|').unwrap();
        other_conn
            .execute(ddl)
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
}

/// A table in a schema file uses the types of the main database, so DROP
/// TYPE and DROP DOMAIN refuse a type that such a table uses.
#[turso_macros::test]
fn drop_type_refuses_a_type_that_a_schema_file_table_uses(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TYPE mood AS ENUM ('sad', 'ok')")
        .unwrap();
    conn.execute("CREATE DOMAIN posint AS integer CHECK (VALUE > 0)")
        .unwrap();
    conn.execute("CREATE SCHEMA s").unwrap();
    conn.execute("CREATE TABLE s.t (id integer PRIMARY KEY, m mood, p posint)")
        .unwrap();
    conn.execute("INSERT INTO s.t VALUES (1, 'ok', 2)").unwrap();
    for (sql, error) in [
        (
            "DROP TYPE mood",
            "cannot drop type mood: used by column m in table t",
        ),
        (
            "DROP DOMAIN posint",
            "cannot drop type posint: used by column p in table t",
        ),
    ] {
        let err = conn.execute(sql).unwrap_err();
        assert!(err.to_string().contains(error), "{sql}: {err}");
    }
    drop(conn);

    let db = db.reopen();
    let conn = attach_schema_file(&db, "s").unwrap();
    assert_eq!(rows(&conn, "SELECT id, m, p FROM s.t"), ["1|ok|2"]);
    assert!(conn
        .execute("INSERT INTO s.t VALUES (2, 'angry', 3)")
        .is_err());
}

/// The engine creates its own tables with SQLite SQL, also on a connection
/// of the PostgreSQL frontend. They are not tables of the PostgreSQL
/// frontend.
#[test]
fn engine_tables_are_not_postgres_tables() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let conn = db.connect_postgres();
    conn.inner()
        .execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t (id integer PRIMARY KEY, a text)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    assert_eq!(
        core_rows(
            conn.inner(),
            "SELECT name, sql FROM sqlite_schema \
             WHERE name IN ('__turso_internal_mvcc_meta', 'turso_cdc_version') ORDER BY name"
        ),
        [
            "__turso_internal_mvcc_meta|CREATE TABLE __turso_internal_mvcc_meta \
             (k TEXT, v INTEGER NOT NULL)",
            "turso_cdc_version|CREATE TABLE turso_cdc_version \
             (table_name TEXT PRIMARY KEY, version TEXT NOT NULL)",
        ]
    );
}

/// RENAME COLUMN of a parent column rewrites the stored SQL of the child
/// table.
#[turso_macros::test(mvcc)]
fn rename_column_of_a_parent_keeps_the_child_table(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE parent (id integer PRIMARY KEY, n numeric(10,2))")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id integer PRIMARY KEY, \
         pid integer REFERENCES parent (id), ts timestamp DEFAULT now())",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 2.5)").unwrap();
    conn.execute("INSERT INTO child (id, pid, ts) VALUES (1, 1, '2024-01-01 10:00:00')")
        .unwrap();
    conn.execute("ALTER TABLE parent RENAME COLUMN id TO pkey")
        .unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(
        core_rows(
            conn.inner(),
            "SELECT sql FROM sqlite_schema WHERE name = 'child'"
        ),
        [
            "CREATE TABLE child (id pg_int4 PRIMARY KEY, pid pg_int4 REFERENCES parent (pkey), \
          ts pg_timestamp DEFAULT (now ())) STRICT, PGSTORAGE"
        ]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT child.id, parent.n, child.ts FROM child JOIN parent ON parent.pkey = child.pid"
        ),
        ["1|2.50|2024-01-01 10:00:00"]
    );
}
