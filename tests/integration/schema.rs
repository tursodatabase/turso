use crate::common::TempDatabase;
use std::sync::Arc;

fn assert_schema_error_contains(conn: &Arc<turso_core::Connection>, sql: &str, expected: &str) {
    let err = conn
        .execute(sql)
        .expect_err("schema statement should have failed");
    let err = err.to_string();
    assert!(
        err.contains(expected),
        "expected error containing {expected:?}, got {err:?}"
    );
}

#[test]
fn test_generated_columns_reject_current_time_literals() {
    let opts = turso_core::DatabaseOpts::new().with_generated_columns(true);
    let db = TempDatabase::builder().with_opts(opts).build();
    let conn = db.connect_limbo();

    for literal in ["CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP"] {
        let sql = format!("CREATE TABLE t_{literal}(a, b AS ({literal}))");
        assert_schema_error_contains(&conn, &sql, "non-deterministic");

        let sql =
            format!("CREATE TABLE t_nested_{literal}(a, b AS (coalesce({literal}, 'fallback')))");
        assert_schema_error_contains(&conn, &sql, "non-deterministic");
    }
}

#[test]
fn test_partial_indexes_reject_current_time_literals() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a)").unwrap();

    for literal in ["CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP"] {
        let sql = format!("CREATE INDEX i_{literal} ON t(a) WHERE {literal} IS NOT NULL");
        assert_schema_error_contains(&conn, &sql, "non-deterministic");
    }
}
