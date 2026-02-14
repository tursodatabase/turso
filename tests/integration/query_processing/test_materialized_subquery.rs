use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

#[test]
// Regression test for incorrect handling of NullRow/null_flag in op_next/op_prev.
fn materialized_subquery_preserves_duplicate_rows() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t1(id INTEGER PRIMARY KEY, d INT);");
    limbo_exec_rows(
        &conn,
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);",
    );

    limbo_exec_rows(&conn, "INSERT INTO t1 VALUES (1, -5), (2, -5);");
    limbo_exec_rows(
        &conn,
        "INSERT INTO t2 VALUES \
         (10, 1, NULL, NULL, -5), \
         (11, 1, NULL, NULL, -5);",
    );

    let query = "SELECT sub_t1.id, sub_t2.a \
                 FROM (SELECT * FROM t1) AS sub_t1 \
                 JOIN (SELECT a, b, c, d, c + d AS cd FROM t2) AS sub_t2 \
                   ON sub_t1.d = sub_t2.d \
                 ORDER BY sub_t1.id, sub_t2.a";

    let rows = limbo_exec_rows(&conn, query);
    let expected = vec![
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(1)],
    ];

    assert_eq!(rows, expected, "unexpected join results: {rows:?}");
}
