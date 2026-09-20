use crate::assertions::{AssertColumn, Cell};
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;
#[test]
fn large_indexed_in_list_uses_seek_loop() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, v TEXT)",
    );
    limbo_exec_rows(&conn, "CREATE INDEX t_x ON t(x)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO t
        SELECT value, value, 'v' || value
        FROM generate_series(1, 10000)",
    );
    limbo_exec_rows(&conn, "ANALYZE");

    let values = (1..=5000)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("SELECT count(*) FROM t WHERE x IN ({values})");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}")
    ))
    .described_as("expected IN-list to drive indexed seeks, not a residual scan")
    .column(3)
    .contains(Cell::from("SEARCH t USING COVERING INDEX t_x (x=?)"));
}

#[test]
fn in_list_seek_over_a_changing_value_is_refilled_for_each_row() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    for stmt in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER DEFAULT 0)",
        "CREATE TABLE u(cid INTEGER, k TEXT)",
        "CREATE INDEX iu ON u(cid)",
        "INSERT INTO t(id) VALUES(1),(2),(3),(4)",
        "INSERT INTO u VALUES(1,'a'),(1,'a'),(2,'b'),(3,'a')",
    ] {
        limbo_exec_rows(&conn, stmt);
    }

    let once_count = |sql: &str| {
        limbo_exec_rows(&conn, &format!("EXPLAIN {sql}"))
            .iter()
            .filter(
                |row| matches!(row.get(1), Some(rusqlite::types::Value::Text(op)) if op == "Once"),
            )
            .count()
    };

    let correlated = "UPDATE t SET n = (SELECT count(*) FROM u WHERE u.cid IN (t.id))";
    // A list of constants is still filled once, so a zero below reports the list
    // contents and not the loss of the guard everywhere.
    assert!(
        once_count("UPDATE t SET n = (SELECT count(*) FROM u WHERE u.cid IN (1,2))") > 0,
        "a constant IN list should still be materialized once"
    );
    assert_eq!(
        once_count(correlated),
        0,
        "an IN list holding a column of the enclosing query cannot be materialized once"
    );

    limbo_exec_rows(&conn, correlated);
    // Counts read from /usr/bin/sqlite3 3.51.0 on this schema and these rows.
    assert_that!(limbo_exec_rows(&conn, "SELECT n FROM t ORDER BY id"))
        .described_as("each row must be counted against its own id")
        .column(0)
        .is_equal_to(vec![
            Cell::from(2i64),
            Cell::from(1i64),
            Cell::from(1i64),
            Cell::from(0i64),
        ]);

    // The same list read at one query level, with no subquery involved at all.
    limbo_exec_rows(&conn, "CREATE TABLE z(a INTEGER)");
    limbo_exec_rows(&conn, "INSERT INTO z VALUES(1),(2),(3),(4)");
    assert_that!(limbo_exec_rows(
        &conn,
        "SELECT count(*) FROM z JOIN u ON 1=1 WHERE u.cid IN (z.a) GROUP BY z.a ORDER BY z.a"
    ))
    .described_as("each group must be counted against its own key")
    .column(0)
    .is_equal_to(vec![Cell::from(2i64), Cell::from(1i64), Cell::from(1i64)]);
}
