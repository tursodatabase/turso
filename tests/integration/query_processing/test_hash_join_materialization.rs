use crate::common::{limbo_exec_rows, TempDatabase};
use core_tester::common::sqlite_exec_rows;
use rusqlite::types::Value;

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(v) => Some(*v),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(v) => Some(v.as_str()),
        _ => None,
    }
}

#[test]
// Regression test where hash join produced too many rows due to only preserving rowids of the build table
// instead of including the payloads of earlier tables in the join.
fn hash_join_materialization_preserves_left_join_correlation() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    let schema = [
        "CREATE TABLE test_table1(id INTEGER PRIMARY KEY, a, b, c, d)",
        "CREATE TABLE test_table2(id INTEGER PRIMARY KEY, a, b, c, d)",
        "CREATE TABLE test_table3(id INTEGER PRIMARY KEY, a, b, c, d)",
        "CREATE TABLE test_table4(id INTEGER PRIMARY KEY, a, b, c, d)",
    ];
    for stmt in &schema {
        limbo_exec_rows(&conn, stmt);
    }

    let inserts = [
        "INSERT INTO test_table1 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,10)",
        "INSERT INTO test_table2 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,10)",
        "INSERT INTO test_table3 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,10)",
        "INSERT INTO test_table4 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,10)",
    ];
    for stmt in &inserts {
        limbo_exec_rows(&conn, stmt);
    }

    let query = "SELECT sub.a, test_table2.id, test_table3.id, test_table4.id\nFROM (SELECT * FROM test_table1) sub\nLEFT JOIN test_table2 ON sub.c = test_table2.c\nJOIN test_table3 ON test_table2.a = test_table3.a\nJOIN test_table4 ON test_table3.b = test_table4.b AND test_table3.d = test_table4.d\nORDER BY 1,2,3,4";

    let rows = limbo_exec_rows(&conn, query);
    let expected: Vec<Vec<Value>> = vec![
        vec![
            Value::Integer(0),
            Value::Integer(10),
            Value::Integer(10),
            Value::Integer(10),
        ],
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
        ],
        vec![
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(2),
        ],
        vec![
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
        ],
        vec![
            Value::Integer(4),
            Value::Integer(4),
            Value::Integer(4),
            Value::Integer(4),
        ],
        vec![
            Value::Integer(5),
            Value::Integer(5),
            Value::Integer(5),
            Value::Integer(5),
        ],
        vec![
            Value::Integer(6),
            Value::Integer(6),
            Value::Integer(6),
            Value::Integer(6),
        ],
        vec![
            Value::Integer(7),
            Value::Integer(7),
            Value::Integer(7),
            Value::Integer(7),
        ],
        vec![
            Value::Integer(8),
            Value::Integer(8),
            Value::Integer(8),
            Value::Integer(8),
        ],
        vec![
            Value::Integer(9),
            Value::Integer(9),
            Value::Integer(9),
            Value::Integer(9),
        ],
    ];

    assert_eq!(rows, expected, "unexpected join results: {rows:?}");
}

#[test]
/// Regression test for hash join reading from an uninitialized cursor (#4882)
fn hash_join_materialization_does_not_read_unrewound_probe_cursor() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    let schema = [
        "CREATE TABLE test_table1(id INTEGER PRIMARY KEY, a, b, c, d)",
        "CREATE TABLE test_table2(id INTEGER PRIMARY KEY, a, b, c, d)",
        "CREATE TABLE test_table3(id INTEGER PRIMARY KEY, a, b, c, d)",
        "CREATE TABLE test_table4(id INTEGER PRIMARY KEY, a, b, c, d)",
    ];
    for stmt in &schema {
        limbo_exec_rows(&conn, stmt);
    }

    let inserts = [
        "INSERT INTO test_table1 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,200)",
        "INSERT INTO test_table2 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,200)",
        "INSERT INTO test_table3 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,200)",
        "INSERT INTO test_table4 SELECT value, value%10, value%20, value%30, value%40 FROM generate_series(1,200)",
    ];
    for stmt in &inserts {
        limbo_exec_rows(&conn, stmt);
    }

    let query = "SELECT sub.a, test_table2.id, test_table3.id, test_table4.id\nFROM (SELECT * FROM test_table1) sub\nLEFT JOIN test_table2 ON sub.c = test_table2.c\nJOIN test_table3 ON test_table2.a = test_table3.a\nJOIN test_table4 ON test_table3.b = test_table4.b AND test_table3.d = test_table4.d\nLIMIT 5";

    let explain_rows = limbo_exec_rows(&conn, &format!("EXPLAIN {query}"));

    let mut test_table3_cursor_id = None;
    for row in &explain_rows {
        let op = row.get(1).and_then(value_as_text).unwrap_or("");
        if op != "OpenRead" {
            continue;
        }
        let comment = row.get(7).and_then(value_as_text).unwrap_or("");
        if comment.contains("table=test_table3") {
            test_table3_cursor_id = row.get(2).and_then(value_as_i64);
            break;
        }
    }

    let test_table3_cursor_id =
        test_table3_cursor_id.expect("expected OpenRead for test_table3 in EXPLAIN output");
    let mut positioned = false;

    for row in &explain_rows {
        let op = row.get(1).and_then(value_as_text).unwrap_or("");
        let p1 = row.get(2).and_then(value_as_i64);
        let p2 = row.get(3).and_then(value_as_i64);

        match op {
            "Rewind" | "Last" | "SeekRowid" | "SeekGE" | "SeekGT" | "SeekLE" | "SeekLT"
            | "SeekEnd" => {
                if p1 == Some(test_table3_cursor_id) {
                    positioned = true;
                }
            }
            "DeferredSeek" => {
                if p2 == Some(test_table3_cursor_id) {
                    positioned = true;
                }
            }
            "Column" | "RowId" => {
                if p1 == Some(test_table3_cursor_id) {
                    assert!(
                        positioned,
                        "test_table3 cursor read before being positioned; EXPLAIN row: {row:?}"
                    );
                    break;
                }
            }
            _ => {}
        }
    }
}

#[test]
/// Regression: losing a JOIN predicate during hash-join materialization.
///
/// Before, the constraint t3.a = sub_t4.a was removed due to incorrect
/// table masks in build_materialized_build_input_plan, causing both the
/// hash join materialization subplan and the main query plan to mark the
/// constraint as consumed, so it was never evaluated anywhere, resulting
/// in extra rows.
fn hash_join_preserves_join_predicates_after_outer_join_conversion() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    let conn = tmp_db.connect_limbo();
    let schema = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT)",
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT)",
        "CREATE TABLE t3(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT)",
        "CREATE TABLE t4(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT)",
    ];
    for stmt in &schema {
        limbo_exec_rows(&conn, stmt);
        sqlite_conn.execute(stmt, []).unwrap();
    }

    sqlite_conn.execute("BEGIN", []).unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=200_i64 {
        let t1 = format!(
            "INSERT INTO t1(id,a,b,c,d) VALUES ({id}, {}, {}, {}, {})",
            id % 20,
            id % 10,
            id % 25,
            id % 5
        );
        let t2 = format!(
            "INSERT INTO t2(id,a,b,c,d) VALUES ({id}, {}, {}, {}, {})",
            id % 20,
            id % 7,
            id % 11,
            id % 5
        );
        let t3 = format!(
            "INSERT INTO t3(id,a,b,c,d) VALUES ({id}, {}, {}, {}, {})",
            id % 20,
            id % 9,
            id % 13,
            id % 5
        );
        let t4 = format!(
            "INSERT INTO t4(id,a,b,c,d) VALUES ({id}, {}, {}, {}, {})",
            id % 20,
            id % 4,
            id % 17,
            id % 6
        );
        conn.execute(&t1).unwrap();
        conn.execute(&t2).unwrap();
        conn.execute(&t3).unwrap();
        conn.execute(&t4).unwrap();
        sqlite_conn.execute(&t1, []).unwrap();
        sqlite_conn.execute(&t2, []).unwrap();
        sqlite_conn.execute(&t3, []).unwrap();
        sqlite_conn.execute(&t4, []).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    sqlite_conn.execute("COMMIT", []).unwrap();

    let query = "SELECT t1.id, t2.id, t3.id, sub_t4.a \
FROM t1 \
JOIN t2 ON t1.d = t2.d \
JOIN t3 ON t2.a = t3.a \
LEFT JOIN (SELECT a, sum(b) AS sum_b, max(c) AS max_c, count(*) AS cnt FROM t4 GROUP BY a) AS sub_t4 \
  ON t3.a = sub_t4.a \
WHERE t1.c IS NOT NULL AND sub_t4.a = 15 \
ORDER BY t1.id, t2.id, t3.id, sub_t4.a LIMIT 50";

    let explain_rows = limbo_exec_rows(&conn, &format!("EXPLAIN {query}"));
    let has_hash = explain_rows.iter().any(|row| {
        row.get(1)
            .and_then(value_as_text)
            .is_some_and(|op| op == "HashBuild" || op == "HashProbe")
    });
    assert!(has_hash, "expected hash join in EXPLAIN output");

    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
    let limbo_rows = limbo_exec_rows(&conn, query);
    assert_eq!(
        sqlite_rows, limbo_rows,
        "Mismatch after outer join conversion with hash join materialization"
    );
}
