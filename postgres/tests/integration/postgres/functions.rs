use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};
use turso_pg::PgConnection;

fn query_text(conn: &PgConnection, sql: &str) -> Vec<String> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                match row.get_value(0) {
                    Value::Text(v) => result.push(v.value.to_string()),
                    Value::Null => result.push("NULL".to_string()),
                    other => panic!("expected text, got {other:?}"),
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    result
}

fn query_integer(conn: &PgConnection, sql: &str) -> Vec<i64> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                match row.get_value(0) {
                    Value::Numeric(Numeric::Integer(v)) => result.push(*v),
                    other => panic!("expected integer, got {other:?}"),
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    result
}

#[turso_macros::test(mvcc)]
fn test_pg_version_is_client_parseable(db: TempDatabase) {
    let conn = db.connect_postgres();

    for sql in [
        "SELECT version()",
        "SELECT pg_catalog.version()",
        "SELECT * FROM version()",
    ] {
        let out = query_text(&conn, sql);
        assert_eq!(out.len(), 1, "{sql} should return one row");
        let version = &out[0];
        // drivers like knex and TypeORM regex-parse this output as /^PostgreSQL ([\d.]+)/,
        // so the string must lead with "PostgreSQL <numeric version>".
        let rest = version
            .strip_prefix("PostgreSQL ")
            .unwrap_or_else(|| panic!("version() must start with 'PostgreSQL ': {version}"));
        let number = rest.split(' ').next().unwrap();
        assert!(
            !number.is_empty() && number.chars().all(|c| c.is_ascii_digit() || c == '.'),
            "version() must lead with a numeric version: {version}"
        );
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_current_database_matches_pg_database(db: TempDatabase) {
    let conn = db.connect_postgres();

    let expected = db.path.file_stem().unwrap().to_str().unwrap().to_string();
    assert_eq!(
        query_text(&conn, "SELECT current_database()"),
        [expected.clone()]
    );
    assert_eq!(
        query_text(&conn, "SELECT * FROM current_database()"),
        [expected.clone()]
    );
    // The bare current_catalog keyword is the same function per PG docs.
    assert_eq!(
        query_text(&conn, "SELECT current_catalog"),
        [expected.clone()]
    );
    // Must agree with what the pg_database catalog reports as datname.
    assert_eq!(
        query_text(&conn, "SELECT datname FROM pg_catalog.pg_database"),
        [expected]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_current_schema_all_syntactic_forms(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Function call, bare SQLValueFunction keyword,
    // function-in-FROM-position form must all resolve.
    assert_eq!(query_text(&conn, "SELECT current_schema()"), ["public"]);
    assert_eq!(query_text(&conn, "SELECT current_schema"), ["public"]);
    assert_eq!(
        query_text(&conn, "SELECT * FROM current_schema()"),
        ["public"]
    );
}

#[turso_macros::test(mvcc)]
fn test_pg_derived_result_column_names(db: TempDatabase) {
    let conn = db.connect_postgres();

    // PostgreSQL names unaliased function-call and keyword columns after the
    // function
    for (sql, expected) in [
        ("SELECT version()", "version"),
        ("SELECT pg_catalog.version()", "version"),
        ("SELECT current_schema()", "current_schema"),
        ("SELECT current_schema", "current_schema"),
        ("SELECT current_catalog", "current_catalog"),
        ("SELECT current_user", "current_user"),
        ("SELECT session_user", "session_user"),
        ("SELECT * FROM current_schema()", "current_schema"),
        ("SELECT * FROM current_schema() AS cs", "cs"),
        ("SELECT count(*) FROM pg_catalog.pg_database", "count"),
        ("SELECT version() AS v", "v"),
    ] {
        let stmt = conn.prepare(sql).unwrap();
        assert_eq!(stmt.get_column_name(0), expected, "{sql}");
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_backend_pid_is_positive_integer(db: TempDatabase) {
    let conn = db.connect_postgres();

    let pids = query_integer(&conn, "SELECT pg_backend_pid()");
    assert_eq!(pids.len(), 1);
    assert!(pids[0] > 0, "pg_backend_pid() must be a positive pid");
}

#[turso_macros::test(mvcc)]
fn test_pg_quote_functions(db: TempDatabase) {
    let conn = db.connect_postgres();

    for (input, expected) in [
        ("SELECT quote_ident('abc')", "abc"),
        ("SELECT quote_ident('a_1')", "a_1"),
        ("SELECT quote_ident('Abc')", "\"Abc\""),
        ("SELECT quote_ident('a b')", "\"a b\""),
        ("SELECT quote_ident('a\"b')", "\"a\"\"b\""),
        // Reserved keywords must be quoted, unreserved ones must not.
        ("SELECT quote_ident('select')", "\"select\""),
        ("SELECT quote_ident('table')", "\"table\""),
        ("SELECT quote_ident('commit')", "commit"),
        ("SELECT quote_ident('')", "\"\""),
    ] {
        assert_eq!(query_text(&conn, input), [expected], "{input}");
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_quote_literal(db: TempDatabase) {
    let conn = db.connect_postgres();

    for (input, expected) in [
        ("SELECT quote_literal('abc')", "'abc'"),
        ("SELECT quote_literal('O''Reilly')", "'O''Reilly'"),
        ("SELECT quote_literal(42)", "'42'"),
        ("SELECT quote_literal('a\\b')", "E'a\\\\b'"),
    ] {
        assert_eq!(query_text(&conn, input), [expected], "{input}");
    }
}

#[turso_macros::test(mvcc)]
fn test_pg_description_stubs_return_null(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id integer PRIMARY KEY)")
        .unwrap();

    // Comments are not stored, so description lookups report none rather
    // than erroring.
    for sql in [
        "SELECT obj_description(16384, 'pg_class')",
        "SELECT obj_description(16384)",
        "SELECT col_description(16384, 1)",
    ] {
        assert_eq!(query_text(&conn, sql), ["NULL"], "{sql}");
    }
}
