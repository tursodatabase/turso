use std::sync::Arc;

use crate::common::{ExecRows, TempDatabase};

fn explain_plans(conn: &Arc<turso_core::Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(format!("EXPLAIN QUERY PLAN {sql}"))?;
    let mut plans = Vec::new();
    stmt.run_with_row_callback(|row| {
        plans.push(row.get::<String>(3)?);
        Ok(())
    })?;

    Ok(plans)
}

#[test]
fn expression_index_used_for_where() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a INT, b INT, c INT);");
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t VALUES (1, 2, 0)")?;
    conn.execute("INSERT INTO t VALUES (3, 4, 0)")?;
    conn.execute("INSERT INTO t VALUES (5, 6, 0)")?;

    conn.execute("CREATE INDEX idx_expr ON t(a + b)")?;

    let plans = explain_plans(&conn, "SELECT * FROM t WHERE a + b = 7")?;
    assert!(
        plans.iter().any(|p| p.contains("idx_expr")),
        "expected query plan to mention idx_expr, got {plans:?}"
    );

    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT a, b FROM t WHERE a + b = 7");
    assert_eq!(rows, vec![(3, 4)]);
    Ok(())
}

#[test]
fn expression_index_used_for_order_by() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a INT, b INT);");
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t VALUES (2, 2)")?;
    conn.execute("INSERT INTO t VALUES (1, 3)")?;
    conn.execute("INSERT INTO t VALUES (0, 5)")?;

    conn.execute("CREATE INDEX idx_expr_order ON t(a + b)")?;

    let plans = explain_plans(
        &conn,
        "SELECT a, b FROM t WHERE a + b > 0 ORDER BY a + b DESC LIMIT 1 OFFSET 0",
    )?;
    assert!(
        plans.iter().any(|p| p.contains("idx_expr_order")),
        "expected query plan to mention idx_expr_order, got {plans:?}"
    );

    let rows: Vec<(i64, i64)> =
        conn.exec_rows("SELECT a, b FROM t WHERE a + b > 0 ORDER BY a + b DESC LIMIT 1");
    assert_eq!(rows, vec![(0, 5)]);
    Ok(())
}

#[test]
fn expression_index_covering_scan() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a INT, b INT);");
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t VALUES (1, 2)")?;
    conn.execute("INSERT INTO t VALUES (3, 4)")?;
    conn.execute("INSERT INTO t VALUES (5, 6)")?;

    conn.execute("CREATE INDEX idx_expr_proj ON t(a + b)")?;

    let plans = explain_plans(&conn, "SELECT a + b FROM t")?;
    assert!(
        plans
            .iter()
            .any(|p| p.contains("USING COVERING INDEX idx_expr_proj")),
        "expected covering index usage, got {plans:?}"
    );

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a + b FROM t ORDER BY a + b");
    assert_eq!(rows, vec![(3,), (7,), (11,)]);
    Ok(())
}
