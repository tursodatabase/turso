use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn query_plan(conn: &std::sync::Arc<turso_core::Connection>, query: &str) -> String {
    limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN {query}"))
        .iter()
        .filter_map(|row| match row.get(3) {
            Some(Value::Text(plan)) => Some(plan.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn test_composite_pk_plan_preferred_over_secondary_index() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t (a, b, c, PRIMARY KEY (a, b))");
    limbo_exec_rows(&conn, "CREATE INDEX t_a_c ON t (a, c)");
    limbo_exec_rows(&conn, "CREATE TABLE u (a, b)");
    limbo_exec_rows(&conn, "CREATE INDEX u_a ON u (a)");

    // Shape 1: join
    let plan1 = query_plan(
        &conn,
        "SELECT count(*) FROM u JOIN t ON t.a = u.a AND t.b = u.b WHERE u.a = 'x' AND t.c = 'y'",
    );
    assert!(
        plan1.contains("SEARCH t USING INDEX sqlite_autoindex_t_1 (a=? AND b=?)")
            || plan1.contains("SEARCH t USING COVERING INDEX sqlite_autoindex_t_1 (a=? AND b=?)"),
        "expected PK autoindex seek for t in join, got:\n{plan1}"
    );

    // Shape 2: correlated subquery
    let plan2 = query_plan(
        &conn,
        "SELECT count(*) FROM u WHERE u.a = 'x' AND (SELECT count(*) FROM t WHERE t.a = u.a AND t.b = u.b AND t.c = 'y') > 0",
    );
    assert!(
        plan2.contains("SEARCH t USING INDEX sqlite_autoindex_t_1 (a=? AND b=?)")
            || plan2.contains("SEARCH t USING COVERING INDEX sqlite_autoindex_t_1 (a=? AND b=?)"),
        "expected PK autoindex seek for t in correlated subquery, got:\n{plan2}"
    );
}
