use crate::assertions::AssertQueryPlan;
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;

#[test]
fn expression_index_used_for_where() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a INT, b INT, c INT);");
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t VALUES (1, 2, 0)")?;
    conn.execute("INSERT INTO t VALUES (3, 4, 0)")?;
    conn.execute("INSERT INTO t VALUES (5, 6, 0)")?;

    conn.execute("CREATE INDEX idx_expr ON t(a + b)")?;

    assert_that!(limbo_exec_rows(
        &conn,
        "EXPLAIN QUERY PLAN SELECT * FROM t WHERE a + b = 7"
    ))
    .uses_index("idx_expr");

    assert_that!(limbo_exec_rows(&conn, "SELECT a, b FROM t WHERE a + b = 7"))
        .is_equal_to(vec![row![3, 4]]);
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

    assert_that!(limbo_exec_rows(
        &conn,
        "EXPLAIN QUERY PLAN SELECT a, b FROM t WHERE a + b > 0 ORDER BY a + b DESC LIMIT 1 OFFSET 0"
    ))
    .uses_index("idx_expr_order");

    assert_that!(limbo_exec_rows(
        &conn,
        "SELECT a, b FROM t WHERE a + b > 0 ORDER BY a + b DESC LIMIT 1"
    ))
    .is_equal_to(vec![row![0, 5]]);
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

    assert_that!(limbo_exec_rows(
        &conn,
        "EXPLAIN QUERY PLAN SELECT a + b FROM t"
    ))
    .has_step_containing("USING COVERING INDEX idx_expr_proj");

    assert_that!(limbo_exec_rows(&conn, "SELECT a + b FROM t ORDER BY a + b")).is_equal_to(vec![
        row![3],
        row![7],
        row![11],
    ]);
    Ok(())
}
