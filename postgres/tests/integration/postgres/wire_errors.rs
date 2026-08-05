// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! SQLSTATE mapping over the wire. Every error used to arrive as the
//! catch-all XX000; drivers and ORMs branch on SQLSTATE (23505 to retry an
//! upsert, 42P01 to create a missing table, 40001 to retry a transaction),
//! so the codes are load-bearing far beyond transcript fidelity.

use super::wire::{exec, expect_error, start_server};
use turso_pg_client::{error_message, PgConn};

/// Asserts the SQLSTATE (field 'C') of a failing statement.
fn assert_code(conn: &mut PgConn, sql: &str, code: &str) {
    let fields = expect_error(conn, sql);
    assert_eq!(
        fields.get(&b'C').map(String::as_str),
        Some(code),
        "wrong SQLSTATE for `{sql}` (message: {})",
        error_message(&fields)
    );
}

/// Asserts SQLSTATE and exact message of a failing statement.
fn assert_error(conn: &mut PgConn, sql: &str, code: &str, message: &str) {
    let fields = expect_error(conn, sql);
    assert_eq!(
        fields.get(&b'C').map(String::as_str),
        Some(code),
        "wrong SQLSTATE for `{sql}`"
    );
    assert_eq!(
        fields.get(&b'M').map(String::as_str),
        Some(message),
        "wrong message for `{sql}`"
    );
}

#[test]
fn errors_carry_postgres_sqlstates() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(
        &mut conn,
        "CREATE TABLE sqlstate_t (a int PRIMARY KEY, b int NOT NULL, c int CHECK (c > 0))",
    );
    exec(&mut conn, "INSERT INTO sqlstate_t VALUES (1, 1, 1)");

    // undefined_table, with PostgreSQL's wording.
    assert_error(
        &mut conn,
        "SELECT * FROM no_such_tbl",
        "42P01",
        "relation \"no_such_tbl\" does not exist",
    );

    // undefined_column, with PostgreSQL's wording.
    assert_error(
        &mut conn,
        "SELECT zzz FROM sqlstate_t",
        "42703",
        "column \"zzz\" does not exist",
    );

    // undefined_function.
    assert_code(&mut conn, "SELECT no_such_func(1)", "42883");

    // unique_violation.
    assert_code(
        &mut conn,
        "INSERT INTO sqlstate_t VALUES (1, 2, 2)",
        "23505",
    );

    // not_null_violation, with PostgreSQL's wording.
    assert_error(
        &mut conn,
        "INSERT INTO sqlstate_t VALUES (2, NULL, 1)",
        "23502",
        "null value in column \"b\" of relation \"sqlstate_t\" violates not-null constraint",
    );

    // check_violation.
    assert_code(
        &mut conn,
        "INSERT INTO sqlstate_t VALUES (3, 1, -5)",
        "23514",
    );
}

#[test]
fn foreign_key_and_syntax_errors_carry_sqlstates() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE fk_parent (id int PRIMARY KEY)");
    exec(
        &mut conn,
        "CREATE TABLE fk_child (id int PRIMARY KEY, pid int REFERENCES fk_parent(id))",
    );

    // foreign_key_violation.
    assert_code(&mut conn, "INSERT INTO fk_child VALUES (1, 99)", "23503");

    // syntax_error.
    assert_code(&mut conn, "SELEC 1", "42601");

    // feature_not_supported for parsed-but-unimplemented statements.
    assert_code(&mut conn, "CREATE PUBLICATION pub1", "0A000");
}

/// Asserts SQLSTATE, exact message, and the error position field ('P'),
/// which clients render as a `LINE n:` marker with a caret.
fn assert_error_at(conn: &mut PgConn, sql: &str, code: &str, message: &str, position: &str) {
    let fields = expect_error(conn, sql);
    assert_eq!(
        fields.get(&b'C').map(String::as_str),
        Some(code),
        "wrong SQLSTATE for `{sql}`"
    );
    assert_eq!(
        fields.get(&b'M').map(String::as_str),
        Some(message),
        "wrong message for `{sql}`"
    );
    assert_eq!(
        fields.get(&b'P').map(String::as_str),
        Some(position),
        "wrong position for `{sql}`"
    );
}

#[test]
fn syntax_errors_carry_postgres_wording_and_positions() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    // The reported token occurs once, so the position is exact: PostgreSQL
    // points at the misspelled keyword and at the stray semicolon.
    assert_error_at(
        &mut conn,
        "SELEC 1",
        "42601",
        "syntax error at or near \"SELEC\"",
        "1",
    );
    // A truncated statement errors at end of input; the position points
    // one past the last character, like PostgreSQL.
    assert_error_at(
        &mut conn,
        "SELECT 1 +",
        "42601",
        "syntax error at end of input",
        "11",
    );

    // An ambiguous token (several occurrences) must not produce a guessed
    // position — a wrong caret is worse than none.
    let fields = expect_error(&mut conn, "SELECT 1 + + SELECT");
    assert_eq!(fields.get(&b'C').map(String::as_str), Some("42601"));
    assert_eq!(
        fields.get(&b'M').map(String::as_str),
        Some("syntax error at or near \"SELECT\""),
    );
}
