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
