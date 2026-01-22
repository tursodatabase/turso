use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value;

/// Extract table access order from EXPLAIN QUERY PLAN.
/// Returns table names in the order they appear in the plan (SCAN/SEARCH lines).
fn extract_table_order(eqp_rows: &[Vec<Value>]) -> Vec<String> {
    let mut tables = Vec::new();
    for row in eqp_rows {
        if let Value::Text(detail) = &row[3] {
            if let Some(rest) = detail.strip_prefix("SCAN ") {
                let table = rest.split_whitespace().next().unwrap();
                tables.push(table.to_string());
            } else if let Some(rest) = detail.strip_prefix("SEARCH ") {
                let table = rest.split_whitespace().next().unwrap();
                tables.push(table.to_string());
            }
        }
    }
    tables
}

#[test]
fn auto_analyze_disabled_by_default() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1), (2)")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);
    assert!(conn.auto_analyze_stats_snapshot().is_none());

    Ok(())
}

#[test]
fn auto_analyze_is_per_connection() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1.execute("CREATE TABLE t (id INTEGER)")?;
    conn1.execute("INSERT INTO t VALUES (1), (2), (3)")?;
    conn1.execute("PRAGMA autoanalyze = 1")?;

    let rows1: Vec<(i64,)> = conn1.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows1, vec![(3,)]);

    let stats1 = conn1.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats1.row_count("t"), Some(3));
    assert!(conn2.auto_analyze_stats_snapshot().is_none());

    conn2.execute("PRAGMA autoanalyze = 1")?;
    let stats2 = conn2.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats2.row_count("t"), None);

    let rows2: Vec<(i64,)> = conn2.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows2, vec![(3,)]);
    let stats2 = conn2.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats2.row_count("t"), Some(3));

    Ok(())
}

#[test]
fn auto_analyze_disable_clears_stats() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t (id INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);
    let stats = conn.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(3));

    conn.execute("PRAGMA autoanalyze = 0")?;
    assert!(conn.auto_analyze_stats_snapshot().is_none());

    conn.execute("PRAGMA autoanalyze = 1")?;
    let stats = conn.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), None);

    Ok(())
}

#[test]
fn auto_analyze_row_count_invalidated_on_write() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t (id INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);

    let stats = conn.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(3));

    conn.execute("INSERT INTO t VALUES (4)")?;
    let stats = conn.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), None);

    Ok(())
}

#[test]
fn auto_analyze_reorders_join_after_stats() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t_big (id INTEGER)")?;
    conn.execute("CREATE TABLE t_small (id INTEGER)")?;
    conn.execute("INSERT INTO t_big SELECT value FROM generate_series(1, 2000)")?;
    conn.execute("INSERT INTO t_small SELECT value FROM generate_series(1, 3)")?;

    let query = "SELECT * FROM t_big JOIN t_small ON t_big.id = t_small.id";
    let eqp_before = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    let order_before = extract_table_order(&eqp_before);
    assert_eq!(
        order_before.first().map(String::as_str),
        Some("t_big"),
        "expected planner to keep FROM order without stats, got {order_before:?}"
    );

    let big_count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t_big");
    assert_eq!(big_count, vec![(2000,)]);
    let small_count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t_small");
    assert_eq!(small_count, vec![(3,)]);

    let stats = conn.auto_analyze_stats_snapshot().expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t_big"), Some(2000));
    assert_eq!(stats.row_count("t_small"), Some(3));

    let eqp_after = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    let order_after = extract_table_order(&eqp_after);
    assert_eq!(
        order_after.first().map(String::as_str),
        Some("t_small"),
        "expected planner to start with t_small after auto stats, got {order_after:?}"
    );

    Ok(())
}
