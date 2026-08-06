// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! Output column names. PostgreSQL derives a name for every unaliased result
//! column from the expression's *shape*, not from its text: a bare column
//! keeps its name, a function call takes the function's name, a cast takes the
//! type's name, and anything else is `?column?`. We used to send the
//! expression source instead, so `SELECT 1+1` came back as a column called
//! `1+1`. Every client that reads column names sees this — psql headers, a
//! driver building a dict per row, an ORM mapping rows to fields.

use super::wire::{exec, start_server};
use turso_pg_client::{BackendEvent, PgConn};

/// The column names a query reports, from its RowDescription.
fn column_names(conn: &mut PgConn, sql: &str) -> Vec<String> {
    conn.simple_query(sql)
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
        .into_iter()
        .find_map(|event| match event {
            BackendEvent::RowDescription(cols) => Some(cols.into_iter().map(|c| c.name).collect()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no RowDescription for: {sql}"))
}

fn assert_names(conn: &mut PgConn, sql: &str, expected: &[&str]) {
    assert_eq!(
        column_names(conn, sql),
        expected,
        "column names for `{sql}`"
    );
}

#[test]
fn an_expression_column_is_called_question_column() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    // Anything PostgreSQL has no better name for.
    for sql in [
        "SELECT 1 + 1",
        "SELECT 'a' || 'b'",
        "SELECT -1",
        "SELECT NULL",
        "SELECT 42",
        "SELECT 1 = 1",
        "SELECT NOT true",
        "SELECT 3 % 2",
        "SELECT (SELECT 1)",
    ] {
        assert_names(&mut conn, sql, &["?column?"]);
    }
}

#[test]
fn a_cast_column_takes_the_type_name() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    // PostgreSQL reports its internal type name, so `boolean` is `bool` and
    // `integer` is `int4`, whichever spelling was written.
    for (sql, name) in [
        ("SELECT 1::bool", "bool"),
        ("SELECT 1::boolean", "bool"),
        ("SELECT '1'::integer", "int4"),
        ("SELECT '1'::int", "int4"),
        ("SELECT '1'::bigint", "int8"),
        ("SELECT 1::text", "text"),
        ("SELECT '2024-01-15'::date", "date"),
        ("SELECT CAST(1 AS boolean)", "bool"),
    ] {
        assert_names(&mut conn, sql, &[name]);
    }
}

#[test]
fn names_that_come_from_the_expression_itself() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE cn (a int, b text)");

    // A column keeps its own name, qualified or not, and an alias always wins.
    assert_names(&mut conn, "SELECT a, b FROM cn", &["a", "b"]);
    assert_names(&mut conn, "SELECT cn.a FROM cn", &["a"]);
    assert_names(&mut conn, "SELECT a AS renamed FROM cn", &["renamed"]);
    assert_names(&mut conn, "SELECT 1 + 1 AS total", &["total"]);
    assert_names(&mut conn, "SELECT 1::bool AS flag", &["flag"]);

    // A function call reports the function's name.
    assert_names(&mut conn, "SELECT count(*) FROM cn", &["count"]);
    assert_names(&mut conn, "SELECT abs(-1)", &["abs"]);
    assert_names(&mut conn, "SELECT upper('x')", &["upper"]);

    // Constructs PostgreSQL names after the construct.
    assert_names(&mut conn, "SELECT CASE WHEN true THEN 1 END", &["case"]);
    assert_names(&mut conn, "SELECT coalesce(NULL, 1)", &["coalesce"]);
    assert_names(&mut conn, "SELECT greatest(1, 2)", &["greatest"]);
    assert_names(&mut conn, "SELECT least(1, 2)", &["least"]);
    assert_names(&mut conn, "SELECT EXISTS (SELECT 1)", &["exists"]);
    assert_names(&mut conn, "SELECT ARRAY[1, 2]", &["array"]);

    // A scalar subquery is named after the column it selects.
    assert_names(&mut conn, "SELECT (SELECT max(a) FROM cn)", &["max"]);
    assert_names(&mut conn, "SELECT (SELECT a FROM cn)", &["a"]);
    assert_names(
        &mut conn,
        "SELECT (SELECT 1 AS inner_name)",
        &["inner_name"],
    );
    // ...and still `?column?` when the inner column has no name either.
    assert_names(&mut conn, "SELECT (SELECT 1 + 1)", &["?column?"]);
}

#[test]
fn several_unnamed_columns_are_all_question_column() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    // PostgreSQL does not disambiguate them.
    assert_names(
        &mut conn,
        "SELECT 1 + 1, 2 + 2, 'x' || 'y'",
        &["?column?", "?column?", "?column?"],
    );
    // Mixed with ones that do have names.
    assert_names(
        &mut conn,
        "SELECT 1 + 1, abs(-2), 1::bool, 4 AS four",
        &["?column?", "abs", "bool", "four"],
    );
}
