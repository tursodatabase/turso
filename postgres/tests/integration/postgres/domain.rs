use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_basic_integer(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN pos_int AS integer CHECK (VALUE > 0)")
        .unwrap();
    conn.execute("CREATE TABLE t (id pos_int)").unwrap();

    // Valid insert
    conn.execute("INSERT INTO t (id) VALUES (5)").unwrap();
    let mut rows = conn.query("SELECT id FROM t").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) else {
        panic!("expected integer");
    };
    assert_eq!(*v, 5);
    drop(rows);

    // Invalid insert should fail
    let result = conn.execute("INSERT INTO t (id) VALUES (-1)");
    assert!(result.is_err(), "negative value should violate CHECK");
}

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_text_with_check(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN email AS text CHECK (VALUE LIKE '%@%')")
        .unwrap();
    conn.execute("CREATE TABLE contacts (addr email)").unwrap();

    // Valid
    conn.execute("INSERT INTO contacts (addr) VALUES ('user@example.com')")
        .unwrap();
    let mut rows = conn.query("SELECT addr FROM contacts").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(v) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(v.value, "user@example.com");
    drop(rows);

    // Invalid
    let result = conn.execute("INSERT INTO contacts (addr) VALUES ('no-at-sign')");
    assert!(result.is_err(), "value without @ should violate CHECK");
}

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_not_null(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN nn_text AS text NOT NULL")
        .unwrap();
    conn.execute("CREATE TABLE t (val nn_text)").unwrap();

    // Valid
    conn.execute("INSERT INTO t (val) VALUES ('hello')")
        .unwrap();

    // NULL should fail
    let result = conn.execute("INSERT INTO t (val) VALUES (NULL)");
    assert!(result.is_err(), "NULL should violate NOT NULL domain");
}

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_default(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN d AS integer DEFAULT 42")
        .unwrap();
    conn.execute("CREATE TABLE t (val d)").unwrap();

    // Insert with explicit column using DEFAULT keyword
    conn.execute("INSERT INTO t (val) VALUES (DEFAULT)")
        .unwrap();
    let mut rows = conn.query("SELECT val FROM t").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) else {
        panic!("expected integer");
    };
    assert_eq!(*v, 42);
}

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_multiple_constraints(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN pos_nn AS integer NOT NULL CHECK (VALUE > 0)")
        .unwrap();
    conn.execute("CREATE TABLE t (val pos_nn)").unwrap();

    // Valid
    conn.execute("INSERT INTO t (val) VALUES (10)").unwrap();

    // NULL should fail
    let result = conn.execute("INSERT INTO t (val) VALUES (NULL)");
    assert!(result.is_err(), "NULL should violate NOT NULL");

    // Negative should fail
    let result = conn.execute("INSERT INTO t (val) VALUES (-5)");
    assert!(result.is_err(), "negative should violate CHECK");
}

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_named_constraint(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN pos_int AS integer CONSTRAINT mycheck CHECK (VALUE > 0)")
        .unwrap();
    conn.execute("CREATE TABLE t (val pos_int)").unwrap();

    conn.execute("INSERT INTO t (val) VALUES (1)").unwrap();
    let result = conn.execute("INSERT INTO t (val) VALUES (0)");
    assert!(result.is_err(), "zero should violate CHECK (VALUE > 0)");
}

#[turso_macros::test(mvcc)]
fn test_pg_create_domain_in_table(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN score AS integer CHECK (VALUE >= 0 AND VALUE <= 100)")
        .unwrap();
    conn.execute("CREATE TABLE students (name text, grade score)")
        .unwrap();

    conn.execute("INSERT INTO students (name, grade) VALUES ('Alice', 95)")
        .unwrap();
    conn.execute("INSERT INTO students (name, grade) VALUES ('Bob', 42)")
        .unwrap();

    let mut rows = conn
        .query("SELECT name, grade FROM students ORDER BY grade")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(name.value, "Bob");
    let Value::Numeric(Numeric::Integer(grade)) = row.get_value(1) else {
        panic!("expected integer");
    };
    assert_eq!(*grade, 42);

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(name.value, "Alice");
    drop(rows);

    // Out of range should fail
    let result = conn.execute("INSERT INTO students (name, grade) VALUES ('Eve', 101)");
    assert!(result.is_err(), "101 should violate CHECK");
}

#[turso_macros::test(mvcc)]
fn test_pg_drop_domain(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE DOMAIN d AS integer").unwrap();
    conn.execute("DROP DOMAIN d").unwrap();

    // IF EXISTS on already-dropped domain should succeed
    conn.execute("DROP DOMAIN IF EXISTS d").unwrap();
}

#[turso_macros::test(mvcc)]
fn test_pg_drop_domain_nonexistent(db: TempDatabase) {
    let conn = db.connect_postgres();

    let result = conn.execute("DROP DOMAIN nonexistent");
    assert!(result.is_err(), "DROP DOMAIN on nonexistent should error");

    // IF EXISTS should not error
    conn.execute("DROP DOMAIN IF EXISTS nonexistent").unwrap();
}
