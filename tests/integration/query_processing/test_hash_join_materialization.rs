use crate::common::{limbo_exec_rows, TempDatabase};
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
