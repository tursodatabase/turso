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
fn indexed_in_list_after_equality_prefix_uses_seek_loop() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, source TEXT, item_id INTEGER, v TEXT)",
    );
    limbo_exec_rows(&conn, "CREATE INDEX t_source_item ON t(source, item_id)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO t
        SELECT value, 'a', value, 'v' || value
        FROM generate_series(1, 20000)",
    );
    limbo_exec_rows(&conn, "ANALYZE");

    let values = (1..=500)
        .map(|i| (i * 7).to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("SELECT count(*) FROM t WHERE source = 'a' AND item_id IN ({values})");

    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}")
    ))
    .described_as("expected the IN list to seek (source, item_id), not scan the source prefix")
    .column(3)
    .contains(Cell::from(
        "SEARCH t USING COVERING INDEX t_source_item (source=? AND item_id=?)",
    ));
    assert_that!(limbo_exec_rows(&conn, &query))
        .described_as("every listed item_id is stored once")
        .column(0)
        .contains(Cell::from(500));

    let subquery = "SELECT count(*) FROM t
        WHERE source = 'a'
        AND item_id IN (SELECT value * 7 FROM generate_series(1, 500))";
    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {subquery}")
    ))
    .described_as("expected the IN subquery to seek (source, item_id)")
    .column(3)
    .contains(Cell::from(
        "SEARCH t USING COVERING INDEX t_source_item (source=? AND item_id=?)",
    ));
    assert_that!(limbo_exec_rows(&conn, subquery))
        .column(0)
        .contains(Cell::from(500));
}
