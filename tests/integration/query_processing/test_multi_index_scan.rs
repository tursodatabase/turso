use crate::common::{limbo_exec_rows, TempDatabase};

/// `h.v = 5 OR h.w = 7` can never be TRUE when every column of `h` is NULL,
/// so the LEFT JOIN behaves like an inner join and must be rewritten to one —
/// which in turn lets the OR term drive a multi-index scan, like SQLite does.
#[test]
fn null_rejecting_or_term_converts_left_join_and_uses_multi_index_scan() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE g(id INTEGER PRIMARY KEY)");
    limbo_exec_rows(&conn, "CREATE TABLE h(id INTEGER, v INTEGER, w INTEGER)");
    limbo_exec_rows(&conn, "CREATE INDEX hv ON h(v)");
    limbo_exec_rows(&conn, "CREATE INDEX hw ON h(w)");

    let plan = {
        let rows = limbo_exec_rows(
            &conn,
            "EXPLAIN QUERY PLAN SELECT * FROM g LEFT JOIN h ON g.id = h.id WHERE h.v = 5 OR h.w = 7",
        );
        rows.iter()
            .filter_map(|row| match row.get(3) {
                Some(rusqlite::types::Value::Text(plan)) => Some(plan.as_str().to_string()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert!(
        plan.contains("MULTI-INDEX OR"),
        "expected the OR term to drive a multi-index scan after the join is \
         rewritten to inner, got:\n{plan}"
    );
}
use core_tester::common::sqlite_exec_rows;
use rusqlite::types::Value;

#[test]
/// Regression: a multi-index OR scan driving an outer join's right-hand table
/// silently dropped the predicate it was built from.
///
/// The scan *is* the predicate: rows failing it are never visited, and the term
/// is marked consumed so nothing checks it again. That only holds for rows the
/// scan actually produces. When nothing matches, the LEFT JOIN emits a
/// null-extended row by jumping straight past the scan, so `t3.b = t4.b OR
/// t3.c = t4.c` — which belongs to the *inner* join with t4 and must reject that
/// row — never ran, and the query returned every (t1, t4) pair.
fn multi_index_or_scan_not_used_for_outer_join_rhs() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_empty();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    let conn = tmp_db.connect_limbo();

    let mut ddl = vec![
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT, b INT, c INT)".to_string(),
        "CREATE TABLE t3(id INTEGER PRIMARY KEY, a INT, b INT, c INT)".to_string(),
        "CREATE TABLE t4(id INTEGER PRIMARY KEY, a INT, b INT, c INT)".to_string(),
    ];
    for table in ["t1", "t3", "t4"] {
        for col in ["a", "b", "c"] {
            ddl.push(format!("CREATE INDEX {table}_{col}_idx ON {table}({col})"));
        }
    }
    for stmt in &ddl {
        limbo_exec_rows(&conn, stmt);
        sqlite_conn.execute(stmt, []).unwrap();
    }

    sqlite_conn.execute("BEGIN", []).unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..30_i64 {
        let stmts = [
            format!(
                "INSERT INTO t1 VALUES ({}, {}, {}, {})",
                i + 1,
                i % 7,
                i % 5,
                i % 3
            ),
            // t3.a is always >= 900, so every ON condition below leaves t3
            // unmatched and the LEFT JOIN null-extends every row.
            format!(
                "INSERT INTO t3 VALUES ({}, {}, {}, {})",
                i + 201,
                900 + i,
                i % 5,
                i % 3
            ),
            format!(
                "INSERT INTO t4 VALUES ({}, {}, {}, {})",
                i + 301,
                i % 7,
                if i % 4 == 0 { 8 } else { i % 5 },
                i % 3
            ),
        ];
        for stmt in &stmts {
            conn.execute(stmt).unwrap();
            sqlite_conn.execute(stmt, []).unwrap();
        }
    }
    conn.execute("COMMIT").unwrap();
    sqlite_conn.execute("COMMIT", []).unwrap();

    // Each of these leaves t3 unmatched; `IS` reaches the plan through the
    // NULL-matching seek, the others through a plain range comparison.
    for on_condition in ["t1.a IS t3.a", "t1.a > t3.a", "t3.a < 0"] {
        let query = format!(
            "SELECT count(*) FROM t1 \
LEFT JOIN t3 ON {on_condition} \
JOIN t4 ON t3.b = t4.b OR t3.c = t4.c \
WHERE t4.b = 8"
        );
        let sqlite_rows = sqlite_exec_rows(&sqlite_conn, &query);
        let limbo_rows = limbo_exec_rows(&conn, &query);
        assert_eq!(
            sqlite_rows, limbo_rows,
            "null-extended rows were not filtered by the OR condition, ON `{on_condition}`"
        );
    }

    // The optimization itself still applies where it is sound: the same OR
    // condition over an inner join.
    let inner = "SELECT count(*) FROM t1 \
JOIN t3 ON t1.a = t3.a \
JOIN t4 ON t3.b = t4.b OR t3.c = t4.c";
    let plan = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {inner}"))
        .iter()
        .filter_map(|row| match row.get(3) {
            Some(Value::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("MULTI-INDEX OR t3"),
        "expected the multi-index OR scan to still be used for an inner join, got:\n{plan}"
    );
    assert_eq!(
        sqlite_exec_rows(&sqlite_conn, inner),
        limbo_exec_rows(&conn, inner)
    );
}
