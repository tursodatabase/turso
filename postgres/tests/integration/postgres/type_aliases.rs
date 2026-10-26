use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

#[turso_macros::test]
fn test_int4_write_bounds_do_not_restrict_lookups_or_expose_checks(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE int4_meta(a int CHECK (a >= 0)); CREATE INDEX int4_meta_idx ON int4_meta(a); INSERT INTO int4_meta VALUES (1)").unwrap();
    for sql in [
        "SELECT COUNT(*) FROM int4_meta WHERE a = 3000000000",
        "SELECT COUNT(*) FROM int4_meta WHERE a > 3000000000",
    ] {
        let rows = conn.prepare(sql).unwrap().run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(0)]]);
    }
    let mut insert = conn.prepare("INSERT INTO int4_meta VALUES ($1)").unwrap();
    insert
        .bind_at(
            std::num::NonZero::new(1).unwrap(),
            Value::from_i64(3000000000),
        )
        .unwrap();
    assert_eq!(insert.run_ignore_rows().unwrap_err().sqlstate, "22003");
    let rows = conn.prepare("SELECT COUNT(*) FROM pg_constraint WHERE contype = 'c' AND conrelid IN (SELECT oid FROM pg_class WHERE relname = 'int4_meta')").unwrap().run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
}

#[turso_macros::test]
fn test_int4_write_bounds_survive_alter_and_reopen(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE int4_ddl(id bigint, a int DEFAULT 7); INSERT INTO int4_ddl(id) VALUES (3000000000)").unwrap();
    conn.execute("ALTER TABLE int4_ddl ADD COLUMN b int DEFAULT 8")
        .unwrap();
    assert_eq!(
        conn.execute("UPDATE int4_ddl SET b = 3000000000")
            .unwrap_err()
            .sqlstate,
        "22003"
    );
    conn.execute("ALTER TABLE int4_ddl RENAME COLUMN b TO renamed")
        .unwrap();
    assert_eq!(
        conn.execute("INSERT INTO int4_ddl(a) VALUES (9), (3000000000)")
            .unwrap_err()
            .sqlstate,
        "22003"
    );
    conn.execute("CREATE TABLE int4_default(id bigint, a int DEFAULT (random() * 0 + 3000000000))")
        .unwrap();
    let default_error = conn
        .execute("INSERT INTO int4_default(id) VALUES (1)")
        .unwrap_err();
    assert_eq!(default_error.sqlstate, "22003", "{default_error}");
    let path = db.path.clone();
    drop(conn);
    drop(db);
    let (_io, reopened) = turso_pg::open_database(
        path.to_str().unwrap(),
        None,
        turso_pg::OpenFlags::default(),
        turso_pg::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = turso_pg::Connection::connect(&reopened).unwrap();
    assert_eq!(
        conn.execute("UPDATE int4_ddl SET renamed = 3000000000")
            .unwrap_err()
            .sqlstate,
        "22003"
    );
    let rows = conn
        .prepare("SELECT id, a, renamed FROM int4_ddl")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::from_i64(3000000000),
            Value::from_i64(7),
            Value::from_i64(8)
        ]]
    );
}

/// serial2 is a PostgreSQL alias for smallserial (auto-incrementing smallint).
/// We test that the type is accepted and maps to INTEGER storage.
/// No mvcc variant: serial implies AUTOINCREMENT which is unsupported in MVCC mode.
#[turso_macros::test]
fn test_serial2_type(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t_serial2(id serial2, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t_serial2(id, name) VALUES (1, 'alice')")
        .unwrap();

    let mut rows = conn
        .query("SELECT id, name FROM t_serial2")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer id");
    };
    assert_eq!(*id, 1);
}

/// serial4 is a PostgreSQL alias for serial (auto-incrementing integer).
#[turso_macros::test]
fn test_serial4_type(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t_serial4(id serial4, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t_serial4(id, name) VALUES (1, 'alice')")
        .unwrap();

    let mut rows = conn.query("SELECT id FROM t_serial4").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer id");
    };
    assert_eq!(*id, 1);
}

/// serial8 is a PostgreSQL alias for bigserial (auto-incrementing bigint).
#[turso_macros::test]
fn test_serial8_type(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t_serial8(id serial8, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t_serial8(id, name) VALUES (1, 'alice')")
        .unwrap();

    let mut rows = conn.query("SELECT id FROM t_serial8").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer id");
    };
    assert_eq!(*id, 1);
}
