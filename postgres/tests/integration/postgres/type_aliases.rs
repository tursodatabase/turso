use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

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
