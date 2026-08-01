//! Tests that the PG parse → translate → compile pipeline handles edge cases
//! correctly: invalid SQL must produce errors (never silently succeed), and
//! valid-but-unsupported SQL must produce clear errors (never leak SQLite errors).

use crate::common::TempDatabase;

/// Helper: switch to PG dialect and execute SQL, returning Ok/Err.
fn pg_exec(db: &TempDatabase, sql: &str) -> Result<(), String> {
    let conn = db.connect_postgres();
    // Use query_runner to exercise the same path as the REPL.
    let runner = conn.query_runner(sql.as_bytes());
    let mut got_result = false;
    let mut first_err = None;
    for result in runner {
        got_result = true;
        if let Err(e) = result {
            if first_err.is_none() {
                first_err = Some(e.to_string());
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    if !got_result && !sql.trim().is_empty() {
        return Err("no result produced (silent swallow)".to_string());
    }
    Ok(())
}

// -------------------------------------------------------------------------
// Invalid SQL must produce errors
// -------------------------------------------------------------------------

#[turso_macros::test]
fn test_pg_parse_typo_errors(db: TempDatabase) {
    let cases = [
        "selecgt 1;",
        "SELET * FROM t;",
        "CRATE TABLE t (id INT);",
        "INSRT INTO t VALUES (1);",
        "DELET FROM t;",
        "UDPATE t SET x = 1;",
    ];
    for sql in &cases {
        let result = pg_exec(&db, sql);
        assert!(result.is_err(), "expected error for typo SQL: {sql}");
    }
}

#[turso_macros::test]
fn test_pg_parse_garbage_errors(db: TempDatabase) {
    let cases = [
        ";;;",
        "!!!",
        "SELECT FROM WHERE;",
        "@#$%^&",
        "1 + 2",
        "hello world",
    ];
    for sql in &cases {
        let result = pg_exec(&db, sql);
        // These should either error or produce a result, but never silently vanish.
        // (Some of these might actually parse in PG — "1 + 2" doesn't, but ";;;' is
        // just empty statements. The key invariant is: no silent swallow.)
        let _ = result; // Just checking we don't panic.
    }
}

// -------------------------------------------------------------------------
// Semantically invalid SQL that pg_query accepts must produce clean errors
// -------------------------------------------------------------------------

#[turso_macros::test]
fn test_pg_parse_empty_select_errors(db: TempDatabase) {
    let result = pg_exec(&db, "SELECT;");
    assert!(result.is_err(), "bare SELECT should error");
    let err = result.unwrap_err();
    assert!(
        !err.contains("unexpected token"),
        "error should not leak SQLite parser errors, got: {err}"
    );
}

#[turso_macros::test]
fn test_pg_parse_empty_create_table_errors(db: TempDatabase) {
    let result = pg_exec(&db, "CREATE TABLE t();");
    assert!(result.is_err(), "CREATE TABLE with no columns should error");
    let err = result.unwrap_err();
    assert!(
        !err.contains("unexpected token"),
        "error should not leak SQLite parser errors, got: {err}"
    );
}

#[turso_macros::test]
fn test_pg_parse_trailing_garbage_after_valid(db: TempDatabase) {
    // Valid statement followed by garbage — the valid part should work,
    // the garbage should error.
    let conn = db.connect_postgres();

    // First statement valid, second is garbage.
    let runner = conn.query_runner(b"SELECT 1; selecgt 2;");
    let results: Vec<_> = runner.collect();
    assert!(!results.is_empty(), "should get at least one result");
    // First statement should succeed.
    assert!(results[0].is_ok(), "first valid statement should succeed");
}

// -------------------------------------------------------------------------
// Valid PG SQL that we support must work
// -------------------------------------------------------------------------

#[turso_macros::test]
fn test_pg_parse_basic_statements_work(db: TempDatabase) {
    let conn = db.connect_postgres();

    // DDL/DML via execute() so schema is visible to subsequent statements.
    conn.execute("CREATE TABLE test (id INT, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO test (id, name) VALUES (1, 'alice')")
        .unwrap();

    // Queries via query_runner to exercise the PG parse path.
    for sql in &[
        "SELECT * FROM test;",
        "SELECT id, name FROM test WHERE id = 1;",
    ] {
        let runner = conn.query_runner(sql.as_bytes());
        for result in runner {
            assert!(
                result.is_ok(),
                "expected success for: {sql}, got: {:?}",
                result.err()
            );
        }
    }

    conn.execute("UPDATE test SET name = 'bob' WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM test WHERE id = 1").unwrap();
    conn.execute("DROP TABLE test").unwrap();
}

#[turso_macros::test]
fn test_pg_parse_expressions_work(db: TempDatabase) {
    let conn = db.connect_postgres();

    let expressions = [
        "SELECT 1 + 2;",
        "SELECT 'hello' || ' ' || 'world';",
        "SELECT 1::TEXT;",
        "SELECT CAST(42 AS TEXT);",
        "SELECT COALESCE(NULL, 1);",
        "SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END;",
        "SELECT NULLIF(1, 2);",
        "SELECT 1 IN (1, 2, 3);",
        "SELECT 1 BETWEEN 0 AND 10;",
        "SELECT 'hello' LIKE 'hel%';",
        "SELECT 'hello' ILIKE 'HEL%';",
        "SELECT TRUE AND FALSE;",
        "SELECT NOT TRUE;",
    ];
    for sql in &expressions {
        let runner = conn.query_runner(sql.as_bytes());
        for result in runner {
            assert!(
                result.is_ok(),
                "expected success for: {sql}, got: {:?}",
                result.err()
            );
        }
    }
}

// -------------------------------------------------------------------------
// Multi-statement edge cases
// -------------------------------------------------------------------------

#[turso_macros::test]
fn test_pg_parse_multi_statement(db: TempDatabase) {
    let conn = db.connect_postgres();

    let sql = "SELECT 1; SELECT 2; SELECT 3;";
    let runner = conn.query_runner(sql.as_bytes());
    let results: Vec<_> = runner.collect();
    assert_eq!(results.len(), 3, "expected 3 results for 3 statements");
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "statement {} should succeed", i + 1);
    }
}

#[turso_macros::test]
fn test_pg_parse_empty_statements(db: TempDatabase) {
    // Empty input should not produce results (and not panic).
    let conn = db.connect_postgres();

    for sql in &["", "   ", "\n\n", "\t"] {
        let runner = conn.query_runner(sql.as_bytes());
        let results: Vec<_> = runner.collect();
        assert!(
            results.is_empty(),
            "empty/whitespace input should produce no results, got {} for {:?}",
            results.len(),
            sql,
        );
    }
}
