use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};
use turso_pg::PgConnection;

fn pg_execute(conn: &PgConnection, sql: &str) {
    conn.execute(sql).unwrap();
}

fn pg_execute_err(conn: &PgConnection, sql: &str) -> String {
    match conn.execute(sql) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected error for: {sql}"),
    }
}

fn pg_query_int(conn: &PgConnection, sql: &str) -> i64 {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row for: {sql}");
    };
    let value = match rows.row().unwrap().get_value(0) {
        Value::Numeric(Numeric::Integer(v)) => *v,
        other => panic!("expected integer, got {other:?} for: {sql}"),
    };
    drain_to_done(&mut rows, sql);
    value
}

fn pg_query_text(conn: &PgConnection, sql: &str) -> String {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row for: {sql}");
    };
    let value = match rows.row().unwrap().get_value(0) {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("expected text, got {other:?} for: {sql}"),
    };
    drain_to_done(&mut rows, sql);
    value
}

fn drain_to_done(rows: &mut turso_core::Statement, sql: &str) {
    loop {
        match rows.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO => continue,
            StepResult::Row => panic!("expected single-row result for: {sql}"),
            other => panic!("unexpected step result while draining: {other:?} for: {sql}"),
        }
    }
}

#[turso_macros::test(mvcc)]
fn test_create_function_via_pg_frontend(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE FUNCTION myadd(a integer, b integer)\nRETURNS integer\nLANGUAGE starlark\nAS $$\nreturn a + b\n$$",
    );
    assert_eq!(pg_query_int(&conn, "SELECT myadd(2, 3)"), 5);
}

#[turso_macros::test(mvcc)]
fn test_create_or_replace_and_drop_function_via_pg_frontend(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE FUNCTION v() RETURNS integer LANGUAGE starlark AS $$\nreturn 1\n$$",
    );
    assert_eq!(pg_query_int(&conn, "SELECT v()"), 1);
    pg_execute(
        &conn,
        "CREATE OR REPLACE FUNCTION v() RETURNS integer LANGUAGE starlark AS $$\nreturn 2\n$$",
    );
    assert_eq!(pg_query_int(&conn, "SELECT v()"), 2);

    pg_execute(&conn, "DROP FUNCTION v");
    let err = pg_execute_err(&conn, "SELECT v()");
    assert!(
        err.contains("no such function: v"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_create_function_control_flow_via_pg_frontend(db: TempDatabase) {
    let conn = db.connect_postgres();

    pg_execute(
        &conn,
        "CREATE FUNCTION classify(x integer) RETURNS text LANGUAGE starlark AS $$\nif x > 0:\n    return 'pos'\nelif x < 0:\n    return 'neg'\nelse:\n    return 'zero'\n$$",
    );
    assert_eq!(pg_query_text(&conn, "SELECT classify(7)"), "pos");
    assert_eq!(pg_query_text(&conn, "SELECT classify(-7)"), "neg");
    assert_eq!(pg_query_text(&conn, "SELECT classify(0)"), "zero");
}

#[turso_macros::test(mvcc)]
fn test_create_procedure_rejected_via_pg_frontend(db: TempDatabase) {
    let conn = db.connect_postgres();

    let err = pg_execute_err(
        &conn,
        "CREATE PROCEDURE p() LANGUAGE starlark AS $$\nreturn 1\n$$",
    );
    assert!(
        err.contains("CREATE PROCEDURE is not supported"),
        "unexpected error: {err}"
    );
}
