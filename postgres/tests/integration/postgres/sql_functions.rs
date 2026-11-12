//! Functions that users define with `CREATE FUNCTION ... LANGUAGE sql`,
//! where the behavior depends on more than one connection, on the database
//! file, or on what tursopg does not support. The SQL behavior that
//! PostgreSQL shares is in `create-function.sqltest`.

use crate::common::TempDatabase;
use turso_core::Value;
use turso_pg::PgConnection;

fn query(conn: &PgConnection, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn sqlstate_of(conn: &PgConnection, sql: &str) -> String {
    let error = match conn.prepare(sql) {
        Ok(mut stmt) => stmt.run_collect_rows().expect_err(sql),
        Err(error) => error,
    };
    error.sqlstate.to_string()
}

#[test]
fn another_connection_sees_a_function_once_it_is_committed() {
    let db = TempDatabase::builder().build();
    let writer = db.connect_postgres();
    let reader = db.connect_postgres();

    writer.execute("BEGIN").unwrap();
    writer
        .execute("CREATE FUNCTION answer() RETURNS int LANGUAGE sql AS 'SELECT 42'")
        .unwrap();
    assert_eq!(query(&writer, "SELECT answer()"), [[Value::from_i64(42)]]);
    assert_eq!(sqlstate_of(&reader, "SELECT answer()"), "42883");
    writer.execute("COMMIT").unwrap();
    assert_eq!(query(&reader, "SELECT answer()"), [[Value::from_i64(42)]]);

    // A statement prepared before the function changed runs the new body.
    let mut prepared = reader.prepare("SELECT answer()").unwrap();
    writer
        .execute("CREATE OR REPLACE FUNCTION answer() RETURNS int LANGUAGE sql AS 'SELECT 43'")
        .unwrap();
    assert_eq!(
        prepared.run_collect_rows().unwrap(),
        [[Value::from_i64(43)]]
    );

    writer.execute("DROP FUNCTION answer()").unwrap();
    assert_eq!(sqlstate_of(&reader, "SELECT answer()"), "42883");
}

#[test]
fn functions_are_stored_in_the_database_file() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE SCHEMA app").unwrap();
    conn.execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    conn.execute(
        "CREATE FUNCTION app.label(id int, name text) RETURNS text LANGUAGE sql IMMUTABLE \
         AS $$ SELECT name || '''#''' || id::text $$",
    )
    .unwrap();
    conn.execute("CREATE VIEW labels AS SELECT app.label(id, 'row') AS label FROM t")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (7)").unwrap();
    let oid = query(&conn, "SELECT oid FROM pg_proc WHERE proname = 'label'");
    // The table took the first oid, and the view the one after the function's.
    assert_eq!(oid, [[Value::from_i64(16385)]]);
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(
        query(&conn, "SELECT label FROM labels"),
        [[Value::build_text("row'#'7")]]
    );
    assert_eq!(
        query(&conn, "SELECT oid FROM pg_proc WHERE proname = 'label'"),
        oid
    );
    // The oid of a function is not given to a relation made later.
    conn.execute("CREATE TABLE u (id int)").unwrap();
    assert_eq!(
        query(&conn, "SELECT oid FROM pg_class WHERE relname = 'u'"),
        [[Value::from_i64(16387)]]
    );
}

#[test]
fn a_database_made_before_functions_gets_the_functions_table_when_opened() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    drop(conn);
    db.connect_limbo()
        .prepare_engine_schema_statement("DROP TABLE __turso_internal_functions")
        .unwrap()
        .run_ignore_rows()
        .unwrap();

    let db = db.reopen();
    let conn = db.connect_postgres();
    conn.execute("CREATE FUNCTION answer() RETURNS int LANGUAGE sql AS 'SELECT 42'")
        .unwrap();
    assert_eq!(
        query(&conn, "SELECT answer() FROM t UNION ALL SELECT answer()"),
        [[Value::from_i64(42)]]
    );
}

#[test]
fn what_tursopg_cannot_define_is_refused_as_not_supported() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    for sql in [
        "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN 1; END'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql RETURN 1",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END",
        "CREATE FUNCTION f() RETURNS SETOF int LANGUAGE sql AS 'SELECT 1'",
        "CREATE FUNCTION f() RETURNS void LANGUAGE sql AS 'SELECT 1'",
        "CREATE FUNCTION f() RETURNS TABLE (a int) LANGUAGE sql AS 'SELECT 1'",
        "CREATE FUNCTION f(OUT a int) LANGUAGE sql AS 'SELECT 1'",
        "CREATE FUNCTION f(VARIADIC a int[]) RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "CREATE FUNCTION f(a int DEFAULT 1) RETURNS int LANGUAGE sql AS 'SELECT a'",
        "CREATE FUNCTION f(a anyelement) RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql STRICT AS 'SELECT a'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET search_path = public AS 'SELECT 1'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT id FROM t'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1, 2'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1; SELECT 2'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'INSERT INTO t VALUES (1) RETURNING id'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT count(*)'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT max(1)'",
        "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS 'SELECT (SELECT a)'",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT f()'",
        "CREATE PROCEDURE p() LANGUAGE sql AS 'SELECT 1'",
    ] {
        assert_eq!(sqlstate_of(&conn, sql), "0A000", "{sql}");
    }
    assert_eq!(
        query(&conn, "SELECT count(*) FROM pg_proc WHERE prolang = 14"),
        [[Value::from_i64(0)]]
    );

    // A function with the same name and number of arguments as another but
    // with other argument types would be a second function in PostgreSQL.
    conn.execute("CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS 'SELECT a'")
        .unwrap();
    for sql in [
        "CREATE FUNCTION f(a text) RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "CREATE OR REPLACE FUNCTION f(a text) RETURNS int LANGUAGE sql AS 'SELECT 1'",
    ] {
        assert_eq!(sqlstate_of(&conn, sql), "0A000", "{sql}");
    }
    assert_eq!(query(&conn, "SELECT f(5)"), [[Value::from_i64(5)]]);
}

#[test]
fn an_argument_that_changes_between_reads_is_refused_for_a_parameter_read_twice() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE FUNCTION double(x float8) RETURNS float8 LANGUAGE sql AS 'SELECT x + x'")
        .unwrap();
    conn.execute("CREATE FUNCTION once(x float8) RETURNS float8 LANGUAGE sql AS 'SELECT x + 1'")
        .unwrap();
    conn.execute(
        "CREATE FUNCTION dice() RETURNS float8 LANGUAGE sql AS 'SELECT floor(random() * 6)'",
    )
    .unwrap();
    for sql in [
        "SELECT double(random())",
        "SELECT double(dice())",
        "SELECT double((SELECT 1.5))",
    ] {
        assert_eq!(sqlstate_of(&conn, sql), "0A000", "{sql}");
    }
    assert_eq!(
        query(&conn, "SELECT double(once(1.5)), once(random()) >= 1"),
        [[Value::from_f64(5.0), Value::from_i64(1)]]
    );
}

#[test]
fn only_a_superuser_creates_and_drops_functions() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE FUNCTION answer() RETURNS int LANGUAGE sql AS 'SELECT 42'")
        .unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    for sql in [
        "CREATE FUNCTION other() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "CREATE OR REPLACE FUNCTION answer() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "DROP FUNCTION answer()",
    ] {
        assert_eq!(sqlstate_of(&conn, sql), "42501", "{sql}");
    }
    // Every role may call a function, as in PostgreSQL, where PUBLIC has
    // EXECUTE on a new function.
    assert_eq!(query(&conn, "SELECT answer()"), [[Value::from_i64(42)]]);
}

#[test]
fn functions_cannot_change_in_a_read_only_transaction() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE FUNCTION answer() RETURNS int LANGUAGE sql AS 'SELECT 42'")
        .unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    assert_eq!(
        sqlstate_of(
            &conn,
            "CREATE FUNCTION other() RETURNS int LANGUAGE sql AS 'SELECT 1'"
        ),
        "25006"
    );
    conn.execute("ROLLBACK").unwrap();
    conn.execute("BEGIN READ ONLY").unwrap();
    assert_eq!(sqlstate_of(&conn, "DROP FUNCTION answer()"), "25006");
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(query(&conn, "SELECT answer()"), [[Value::from_i64(42)]]);
}
