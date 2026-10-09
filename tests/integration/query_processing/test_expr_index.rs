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

/// Stored index expressions, partial-index predicates and CHECK constraints
/// are resolved to column positions when the schema loads from the file, so
/// statements never look the names up again.
#[test]
fn stored_expressions_resolve_on_reopen() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let temp_dir = tempfile::TempDir::new()?;
    let path = temp_dir.path().join("stored_expressions_reopen.db");

    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, CHECK (t.a < t.b))")?;
        conn.execute("CREATE UNIQUE INDEX t_sum ON t (t.a + t.b)")?;
        conn.execute("CREATE TABLE p (x INTEGER, y INTEGER)")?;
        conn.execute("CREATE UNIQUE INDEX p_x_late ON p (x) WHERE rowid > 1")?;
        conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)")?;
        conn.execute("INSERT INTO p VALUES (1, 1), (1, 2)")?;
        conn.close()?;
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();

    conn.execute("UPDATE t AS z SET b = b + 10 WHERE z.a = 1")?;
    assert!(conn.execute("INSERT INTO t AS z VALUES (6, 7)").is_err());
    assert!(conn.execute("UPDATE t SET b = 0 WHERE a = 3").is_err());
    assert_that!(limbo_exec_rows(&conn, "SELECT a, b FROM t ORDER BY a"))
        .is_equal_to(vec![row![1, 12], row![3, 4]]);

    conn.execute("INSERT INTO p VALUES (2, 3)")?;
    assert!(conn.execute("INSERT INTO p VALUES (1, 4)").is_err());
    assert_that!(limbo_exec_rows(
        &conn,
        "SELECT rowid, y FROM p WHERE rowid > 1 AND x = 1"
    ))
    .is_equal_to(vec![row![2, 2]]);
    assert_that!(limbo_exec_rows(&conn, "PRAGMA integrity_check")).is_equal_to(vec![row!["ok"]]);
    Ok(())
}
