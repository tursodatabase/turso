use crate::assertions::{AssertColumn, Cell};
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;
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

    assert_that!(limbo_exec_rows(&conn, &format!("EXPLAIN {query}")))
        .named("opcodes of the join")
        .column(1)
        .contains_any_of([Cell::from("HashBuild"), Cell::from("HashProbe")]);

    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
    let limbo_rows = limbo_exec_rows(&conn, query);
    assert_eq!(
        sqlite_rows, limbo_rows,
        "Mismatch after outer join conversion with hash join materialization"
    );
}

#[test]
/// Regression: a join predicate silently dropped from the null-extended rows of
/// a hash join.
///
/// `t3.d IS t4.d` belongs to the inner join with t4, so it must filter the rows
/// that the preceding LEFT JOIN null-extends. The unmatched-row scan skipped it,
/// because it only kept conditions whose tables were still cursor-positioned and
/// t4's value arrives in a hash payload register instead. Every operator that is
/// TRUE for a null-extended row exposes this — `=` and `<` hide it by evaluating
/// to NULL there.
fn hash_join_unmatched_rows_apply_payload_backed_predicates() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    let conn = tmp_db.connect_limbo();
    let schema = [
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, c INT, d INT)",
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, c INT, d INT)",
        "CREATE TABLE t3(id INTEGER PRIMARY KEY, c INT, d INT)",
        "CREATE TABLE t4(id INTEGER PRIMARY KEY, c INT, d INT)",
    ];
    for stmt in &schema {
        limbo_exec_rows(&conn, stmt);
        sqlite_conn.execute(stmt, []).unwrap();
    }

    sqlite_conn.execute("BEGIN", []).unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..30_i64 {
        let d = i % 5;
        let stmts = [
            format!("INSERT INTO t1 VALUES ({}, {}, {d})", i + 1, i % 3),
            format!("INSERT INTO t2 VALUES ({}, {}, {d})", i + 101, i % 3),
            // t3.c never matches t2.c, so the LEFT JOIN null-extends every row.
            format!("INSERT INTO t3 VALUES ({}, {}, {d})", i + 201, 900 + i),
            format!(
                "INSERT INTO t4 VALUES ({}, {}, {})",
                i + 301,
                i % 3,
                if i % 4 == 0 {
                    "NULL".to_string()
                } else {
                    d.to_string()
                }
            ),
        ];
        for stmt in &stmts {
            conn.execute(stmt).unwrap();
            sqlite_conn.execute(stmt, []).unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();
    sqlite_conn.execute("COMMIT", []).unwrap();
    limbo_exec_rows(&conn, "ANALYZE");

    for predicate in [
        "t3.d IS t4.d",
        "t3.d IS NOT t4.d",
        "ifnull(t3.d, -9) = ifnull(t4.d, -9)",
    ] {
        let query = format!(
            "SELECT count(*) FROM t1 \
JOIN t2 ON t1.d = t2.d \
LEFT JOIN t3 ON t2.c = t3.c \
JOIN t4 ON {predicate}"
        );

        assert_that!(limbo_exec_rows(&conn, &format!("EXPLAIN {query}")))
            .named(format!("opcodes of the join on `{predicate}`"))
            .column(1)
            .contains(Cell::from("HashScanUnmatched"));

        let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
        assert_that!(limbo_exec_rows(&conn, &query))
            .named(format!("rows for `{predicate}`"))
            .is_equal_to(sqlite_rows.clone());
        // Guard against the comparison passing because both sides return nothing.
        assert_that!(sqlite_rows)
            .named(format!("sqlite count for `{predicate}`"))
            .column(0)
            .single_element()
            .satisfies_with_message(
                "be more than zero",
                |count| matches!(count, Value::Integer(count) if *count > 0),
            );
    }
}

#[test]
// The hash build applies build-only WHERE terms while filling the hash table,
// so every row a probe can match has already passed them. Re-checking the same
// terms on every probe match is wasted work.
fn hash_join_build_filter_runs_once_not_per_probe_match() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &conn,
        "CREATE TABLE build_side(id INTEGER PRIMARY KEY, k INTEGER, name TEXT)",
    );
    limbo_exec_rows(&conn, "CREATE TABLE probe_side(k INTEGER, v INTEGER)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO build_side VALUES (1, 10, 'keep'), (2, 20, 'drop'), (3, 30, 'keep')",
    );
    limbo_exec_rows(
        &conn,
        "INSERT INTO build_side
         SELECT value + 3, value + 1000, 'drop' FROM generate_series(1, 97)",
    );
    limbo_exec_rows(
        &conn,
        "INSERT INTO probe_side VALUES (10, 100), (20, 200), (30, 300), (30, 301)",
    );
    limbo_exec_rows(
        &conn,
        "INSERT INTO probe_side
         SELECT value + 10000, value + 10000 FROM generate_series(1, 996)",
    );
    limbo_exec_rows(&conn, "ANALYZE");

    let query = "SELECT probe_side.v FROM build_side \
JOIN probe_side ON probe_side.k = build_side.k \
WHERE build_side.name = 'keep' ORDER BY probe_side.v";

    let explain_rows = limbo_exec_rows(&conn, &format!("EXPLAIN {query}"));
    let opcode_count = |opcode: &str| {
        explain_rows
            .iter()
            .filter(|row| {
                row.get(1)
                    .and_then(value_as_text)
                    .is_some_and(|op| op == opcode)
            })
            .count()
    };
    assert!(opcode_count("HashBuild") > 0, "expected a hash join");
    // The name filter compiles to a single Ne (jump when the names differ)
    // inside the hash build loop. A second Ne means the probe loop re-checks it.
    assert_eq!(
        opcode_count("Ne"),
        1,
        "build-side filter must be evaluated once, during the hash build"
    );

    let rows = limbo_exec_rows(&conn, query);
    let expected: Vec<Vec<Value>> = vec![
        vec![Value::Integer(100)],
        vec![Value::Integer(300)],
        vec![Value::Integer(301)],
    ];
    assert_eq!(rows, expected);
}
