// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! SQLSTATE mapping over the wire. Every error used to arrive as the
//! catch-all XX000; drivers and ORMs branch on SQLSTATE (23505 to retry an
//! upsert, 42P01 to create a missing table, 40001 to retry a transaction),
//! so the codes are load-bearing far beyond transcript fidelity.

use super::wire::{exec, expect_error, query_int, start_server};
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

/// Clauses whose semantics the engine cannot honor must error instead of
/// being silently dropped: each of these statements previously executed
/// with different semantics than PostgreSQL (`> ANY` ran as `IN`,
/// `DISTINCT ON` degraded to `DISTINCT`, aggregate-internal `ORDER BY`
/// vanished, `DELETE USING` deleted the wrong row set, `SKIP LOCKED`
/// queues handed out duplicate jobs).
#[test]
fn unsupported_clauses_error_instead_of_silently_dropping() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE sd (x int, y int)");
    exec(&mut conn, "INSERT INTO sd VALUES (1, 2)");

    for sql in [
        "SELECT x FROM sd WHERE x > ANY (SELECT y FROM sd)",
        "SELECT DISTINCT ON (x) x, y FROM sd",
        "SELECT string_agg(x::text, ',' ORDER BY x) FROM sd",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM sd",
        "DELETE FROM sd USING sd other WHERE sd.x = other.y",
        "SELECT x FROM sd FOR UPDATE SKIP LOCKED",
        "SELECT x FROM sd FOR UPDATE NOWAIT",
    ] {
        assert_code(&mut conn, sql, "0A000");
    }

    // Plain FOR UPDATE stays accepted: the engine's single-writer
    // transactions already hold what it would lock.
    assert_eq!(query_int(&mut conn, "SELECT x FROM sd FOR UPDATE"), 1);

    // `= ANY (SELECT ...)` is IN and keeps working.
    assert_eq!(
        query_int(
            &mut conn,
            "SELECT count(*) FROM sd WHERE y = ANY (SELECT y FROM sd)"
        ),
        1
    );
}

/// DROP a, b and TRUNCATE a, b expand into per-object statements; the
/// expansion must be atomic (PostgreSQL treats the original as one
/// statement) and the option forms we cannot honor must error.
#[test]
fn multi_object_ddl_expands_atomically() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE atomic_a (x int)");
    // The second name does not exist, so the whole DROP must roll back.
    let fields = expect_error(&mut conn, "DROP TABLE atomic_a, no_such_zzz");
    assert_eq!(fields.get(&b'C').map(String::as_str), Some("42P01"));
    assert_eq!(
        query_int(
            &mut conn,
            "SELECT count(*) FROM pg_tables WHERE tablename = 'atomic_a'"
        ),
        1,
        "failed multi-DROP must not drop the first table"
    );

    exec(&mut conn, "CREATE TABLE ident_t (x int)");
    assert_code(&mut conn, "TRUNCATE ident_t RESTART IDENTITY", "0A000");
    assert_code(&mut conn, "TRUNCATE ident_t CASCADE", "0A000");
    assert_code(
        &mut conn,
        "ALTER TABLE ident_t ADD COLUMN y int, ADD COLUMN z int",
        "0A000",
    );
}

/// The remaining formerly-silent drops: each accepted form here executes
/// with PostgreSQL semantics, each rejected form errors instead of running
/// with different semantics.
#[test]
fn remaining_silent_drops_error_or_work() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE strag (x int)");
    exec(&mut conn, "INSERT INTO strag VALUES (1), (2)");

    for sql in [
        "SELECT x FROM strag ORDER BY x FETCH FIRST 1 ROW WITH TIES",
        "SELECT a.x FROM strag a, LATERAL (SELECT a.x) b",
        "SELECT t.a FROM strag AS t(a)",
        "CREATE INDEX strag_inc ON strag (x) INCLUDE (x)",
    ] {
        assert_code(&mut conn, sql, "0A000");
    }

    // CURRENT_USER agrees with the catalog's single role instead of ''.
    let row = conn
        .simple_query("SELECT CURRENT_USER")
        .unwrap()
        .into_iter()
        .find_map(|e| match e {
            turso_pg_client::BackendEvent::DataRow(r) => Some(r),
            _ => None,
        })
        .expect("no row");
    assert_eq!(row[0].as_deref(), Some("turso"));
}

/// Statements that used to panic the engine and take the whole server down.
/// The contract is survival: whatever each statement returns, the session
/// must still answer the next query.
#[test]
fn server_survives_formerly_panicking_statements() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(
        &mut conn,
        "CREATE TABLE panic_probe (col1 int, col2 int, col3 int)",
    );
    let killers = [
        // unreachable!() in the engine's numeric literal parser
        "SELECT 0b10000000000000000000000000000000",
        // i32::MIN - 1 overflow in date month arithmetic
        "SELECT make_date(-2147483648, 1, 1)",
        // empty VALUES row list after DEFAULT-entry translation
        "INSERT INTO panic_probe (col1, col2, col3) VALUES (DEFAULT, DEFAULT)",
    ];
    for sql in killers {
        // Any outcome is acceptable here; reaching ReadyForQuery is not.
        let _ = conn.simple_query(sql).unwrap();
        assert_eq!(
            query_int(&mut conn, "SELECT 41 + 1"),
            42,
            "server no longer answers after: {sql}"
        );
    }
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

/// PostgreSQL refuses a qualified reference to a relation the statement gave
/// an alias: the alias replaces the name, so the name reaches nothing. The
/// engine reported the relation as missing, which reads as though the table
/// were gone. The reply also carries the position of the offending reference
/// and a hint naming the alias, both of which the golden files compare.
#[test]
fn referring_to_an_aliased_relation_by_name_is_refused() {
    let (params, _dir) = start_server();
    let mut conn = PgConn::connect(&params, &[]).unwrap();

    exec(&mut conn, "CREATE TABLE alias_tbl (a int)");
    exec(&mut conn, "INSERT INTO alias_tbl VALUES (30)");

    let sql = "DELETE FROM alias_tbl dt WHERE alias_tbl.a > 25";
    let fields = expect_error(&mut conn, sql);
    assert_eq!(
        fields.get(&b'M').map(String::as_str),
        Some("invalid reference to FROM-clause entry for table \"alias_tbl\"")
    );
    assert_eq!(fields.get(&b'C').map(String::as_str), Some("42P01"));
    assert_eq!(
        fields.get(&b'H').map(String::as_str),
        Some("Perhaps you meant to reference the table alias \"dt\".")
    );
    // The caret points at the qualified reference, not at the FROM entry.
    let at: usize = sql.find("alias_tbl.a").unwrap() + 1;
    assert_eq!(
        fields.get(&b'P').map(String::as_str),
        Some(at.to_string().as_str()),
        "position should be the qualified reference"
    );

    // The row is untouched, and using the alias works.
    assert_eq!(query_int(&mut conn, "SELECT count(*) FROM alias_tbl"), 1);
    exec(&mut conn, "DELETE FROM alias_tbl dt WHERE dt.a > 25");
    assert_eq!(query_int(&mut conn, "SELECT count(*) FROM alias_tbl"), 0);

    // Without an alias the table's own name is still fine.
    exec(&mut conn, "INSERT INTO alias_tbl VALUES (30)");
    exec(&mut conn, "DELETE FROM alias_tbl WHERE alias_tbl.a > 25");
    assert_eq!(query_int(&mut conn, "SELECT count(*) FROM alias_tbl"), 0);
}
