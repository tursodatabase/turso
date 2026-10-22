use crate::assertions::{AssertColumn, AssertQueryPlan, Cell};
use crate::common::{limbo_exec_rows, TempDatabase};
use asserting::prelude::*;
use core_tester::common::sqlite_exec_rows;
use rusqlite::types::Value;

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

    assert_that!(limbo_exec_rows(
        &conn,
        "EXPLAIN QUERY PLAN SELECT * FROM g LEFT JOIN h ON g.id = h.id WHERE h.v = 5 OR h.w = 7",
    ))
    .has_step_containing("MULTI-INDEX OR");
}

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
        assert_that!(limbo_exec_rows(&conn, &query))
            .named(format!("rows for ON `{on_condition}`"))
            .is_equal_to(sqlite_exec_rows(&sqlite_conn, &query));
    }

    // The optimization itself still applies where it is sound: the same OR
    // condition over an inner join.
    let inner = "SELECT count(*) FROM t1 \
JOIN t3 ON t1.a = t3.a \
JOIN t4 ON t3.b = t4.b OR t3.c = t4.c";
    assert_that!(limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {inner}")
    ))
    .has_step_containing("MULTI-INDEX OR t3");
    assert_that!(limbo_exec_rows(&conn, inner)).is_equal_to(sqlite_exec_rows(&sqlite_conn, inner));
}

#[test]
fn debug_tracing_does_not_crash_multi_index_update() {
    let log = tempfile::NamedTempFile::new().expect("create temporary debug log");
    let log_file = log.reopen().expect("reopen temporary debug log");
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || {
            log_file
                .try_clone()
                .expect("clone temporary debug log handle")
        })
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();

        for statement in [
            "CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER)",
            "CREATE INDEX ia ON t(a)",
            "CREATE INDEX ib ON t(b)",
        ] {
            limbo_exec_rows(&conn, statement);
        }

        assert_that!(limbo_exec_rows(
            &conn,
            "EXPLAIN QUERY PLAN UPDATE t SET c = 8 WHERE a = 1 OR b = 2",
        ))
        .column(3)
        .contains(Cell::from("MULTI-INDEX OR t (ia, ib)"));

        limbo_exec_rows(&conn, "INSERT INTO t VALUES (1, 2, 0)");
        limbo_exec_rows(&conn, "UPDATE t SET c = 8 WHERE a = 1 OR b = 2");
        assert_that!(limbo_exec_rows(&conn, "SELECT c FROM t")).is_equal_to(vec![row![8]]);
    });

    assert_that!(std::fs::read_to_string(log.path()).expect("read temporary debug log"))
        .named("debug log")
        .contains("MULTI-INDEX OR t (ia, ib)");
}

/// HIR reads a table through one index seek per branch of an OR term, and
/// returns each row once, as legacy does.
#[test]
fn hir_uses_one_seek_per_or_branch() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE h(id INTEGER PRIMARY KEY, v INTEGER, w INTEGER)",
        "CREATE INDEX hv ON h(v)",
        "CREATE INDEX hw ON h(w)",
        "CREATE TABLE g(id INTEGER PRIMARY KEY, k INTEGER)",
        "INSERT INTO g VALUES (1, 5), (2, 3)",
    ] {
        limbo_exec_rows(&conn, sql);
        sqlite_conn.execute(sql, []).unwrap();
    }
    for id in 1..=200 {
        let sql = format!("INSERT INTO h VALUES ({id}, {}, {})", id % 10, id % 7);
        limbo_exec_rows(&conn, &sql);
        sqlite_conn.execute(&sql, []).unwrap();
    }
    let plan = |sql: &str| {
        limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"))
            .iter()
            .filter_map(|row| match row.get(3) {
                Some(Value::Text(text)) => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    for sql in [
        "SELECT count(*), sum(id) FROM h WHERE v = 5 OR w = 3",
        "SELECT count(*), sum(id) FROM h WHERE v = 5 OR w = 3 OR id = 7",
        "SELECT id FROM h WHERE v IN (1, 2) OR w = 6 ORDER BY id DESC LIMIT 4",
        "SELECT g.id, count(*) FROM g JOIN h ON h.v = g.k OR h.w = g.k GROUP BY g.id",
    ] {
        assert!(
            plan(sql).contains("MULTI-INDEX OR h"),
            "{sql}: {}",
            plan(sql)
        );
        assert_eq!(
            limbo_exec_rows(&conn, sql),
            sqlite_exec_rows(&sqlite_conn, sql),
            "{sql}"
        );
    }
}

/// Through HIR too, a WHERE term that rejects the NULL row of a LEFT JOIN
/// makes it an inner join, so the OR term can drive the right table's loop.
#[test]
fn hir_turns_a_null_rejected_left_join_into_an_inner_join() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE g(id INTEGER PRIMARY KEY)",
        "CREATE TABLE h(id INTEGER, v INTEGER, w INTEGER)",
        "CREATE INDEX hv ON h(v)",
        "CREATE INDEX hw ON h(w)",
        "INSERT INTO g VALUES (1), (2), (3)",
        "INSERT INTO h VALUES (1, 5, 0), (2, 0, 7), (9, 5, 7)",
    ] {
        limbo_exec_rows(&conn, sql);
        sqlite_conn.execute(sql, []).unwrap();
    }
    let query = "SELECT * FROM g LEFT JOIN h ON g.id = h.id WHERE h.v = 5 OR h.w = 7";
    let plan = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"))
        .iter()
        .filter_map(|row| match row.get(3) {
            Some(Value::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("MULTI-INDEX OR h"), "{plan}");
    for sql in [
        query,
        // IS NULL keeps the NULL row, so the join stays a LEFT JOIN.
        "SELECT g.id, h.id FROM g LEFT JOIN h ON g.id = h.id WHERE h.v IS NULL",
        "SELECT g.id, h.id FROM g LEFT JOIN h ON g.id = h.id WHERE coalesce(h.v, 1) = 1",
    ] {
        assert_eq!(
            limbo_exec_rows(&conn, sql),
            sqlite_exec_rows(&sqlite_conn, sql),
            "{sql}"
        );
    }
}
