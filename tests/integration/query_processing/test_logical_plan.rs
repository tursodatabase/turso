use std::sync::Arc;

use rusqlite::types::Value;

use crate::common::{limbo_exec_rows, limbo_stmt_get_column_names, TempDatabase};

const SCHEMA: &[&str] = &[
    "CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c)",
    "CREATE TABLE t2(a, b, c)",
    "CREATE TABLE t3(x, y)",
    "CREATE INDEX i1 ON t2(a)",
    "CREATE VIEW v1 AS SELECT a, b FROM t2",
    "INSERT INTO t1 VALUES (1, 10, 'x'), (2, 20, 'y'), (3, NULL, 'z'), (4, 40, NULL)",
    "INSERT INTO t2 VALUES (1, 100, 'p'), (1, 200, 'q'), (2, 300, 'r'), (NULL, 400, 's'), (4, NULL, 't')",
    "INSERT INTO t3 VALUES (1, 1), (1, 2), (2, 3), (NULL, 4)",
];

fn database() -> TempDatabase {
    let database = TempDatabase::new_empty();
    let connection = database.connect_limbo();
    for statement in SCHEMA {
        connection
            .execute(statement)
            .expect("schema statement runs");
    }
    database
}

fn connect(database: &TempDatabase, logical_plan: bool) -> Arc<turso_core::Connection> {
    let connection = database.connect_limbo();
    if logical_plan {
        connection
            .execute("PRAGMA unstable_logical_plan = 1")
            .expect("the pragma runs");
    }
    connection
}

/// Return the text rows from `EXPLAIN QUERY PLAN`.
fn explain(connection: &Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    let mut statement = connection
        .prepare(format!("EXPLAIN QUERY PLAN {sql}"))
        .expect("the statement prepares");
    let mut details = Vec::new();
    statement
        .run_with_row_callback(|row| {
            details.push(row.get::<String>(3)?);
            Ok(())
        })
        .expect("the statement runs");
    details
}

/// Run the same statements with the stage off and on. The rows must match.
fn assert_same_rows(database: &TempDatabase, queries: &[&str]) {
    let legacy = connect(database, false);
    let logical = connect(database, true);
    for query in queries {
        let expected: Vec<Vec<Value>> = limbo_exec_rows(&legacy, query);
        let actual: Vec<Vec<Value>> = limbo_exec_rows(&logical, query);
        assert_eq!(actual, expected, "rows differ for: {query}");
    }
}

#[test]
fn flattened_derived_table_gets_the_plan_of_the_flat_query() {
    let database = database();
    let legacy = connect(&database, false);
    let logical = connect(&database, true);
    for (nested, flat) in [
        (
            "SELECT * FROM (SELECT a, b FROM t2) s WHERE s.a = 1",
            "SELECT a, b FROM t2 WHERE a = 1",
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t2 WHERE b > 100) s WHERE s.a = 1",
            "SELECT a, b FROM t2 WHERE a = 1 AND b > 100",
        ),
        (
            "WITH c AS (SELECT a, b FROM t2) SELECT * FROM c WHERE a = 1",
            "SELECT a, b FROM t2 WHERE a = 1",
        ),
        (
            "SELECT * FROM v1 WHERE a = 1",
            "SELECT a, b FROM t2 WHERE a = 1",
        ),
        (
            "SELECT * FROM t1 JOIN (SELECT a, b FROM t2) s ON s.a = t1.a WHERE t1.a = 1",
            "SELECT * FROM t1 JOIN t2 ON t2.a = t1.a WHERE t1.a = 1",
        ),
    ] {
        let before = explain(&legacy, nested);
        assert!(
            before.iter().any(|detail| detail.starts_with("SCAN")),
            "expected a scan of the derived table without the stage, got {before:?}"
        );
        let after = explain(&logical, nested);
        let expected = explain(&legacy, flat);
        assert_eq!(after, expected, "plan of the flattened query: {nested}");
    }
    let after = explain(
        &logical,
        "SELECT * FROM (SELECT a, b FROM t2) s WHERE s.a = 1",
    );
    assert_eq!(after, vec!["SEARCH t2 USING INDEX i1 (a=?)"]);
}

#[test]
fn flattened_column_keeps_its_name() {
    let database = database();
    let logical = connect(&database, true);
    let names = limbo_stmt_get_column_names(
        &database,
        &logical,
        "SELECT s.x, s.a, s.x + 1 FROM (SELECT a + 1 AS x, a FROM t2) s",
    );
    assert_eq!(names, vec!["x", "a", "s.x + 1"]);
}

#[test]
fn unnested_subquery_keeps_its_column_name() {
    let database = database();
    let logical = connect(&database, true);
    let names = limbo_stmt_get_column_names(
        &database,
        &logical,
        "SELECT a, (SELECT max(b) FROM t2 WHERE t2.a = t1.a), (SELECT count(*) FROM t2 WHERE t2.a = t1.a) AS n FROM t1",
    );
    assert_eq!(
        names,
        vec!["a", "(SELECT max(b) FROM t2 WHERE t2.a = t1.a)", "n"]
    );
}

#[test]
fn flattened_queries_return_the_same_rows() {
    let database = database();
    assert_same_rows(
        &database,
        &[
            "SELECT * FROM (SELECT a, b FROM t2) s WHERE s.a = 1 ORDER BY b",
            "SELECT * FROM (SELECT a, b FROM t2 WHERE b > 100) s WHERE s.a = 1 ORDER BY b",
            "SELECT s.b, s.n FROM (SELECT b, a + 1 AS n FROM t2) s ORDER BY s.b",
            "WITH c AS (SELECT a, b FROM t2) SELECT * FROM c WHERE a = 1 ORDER BY b",
            "WITH c AS (SELECT a, b FROM t2) SELECT c1.a, c2.b FROM c c1 JOIN c c2 ON c1.a = c2.a ORDER BY 1, 2",
            "SELECT * FROM v1 WHERE a = 1 ORDER BY b",
            "SELECT t1.a, s.b FROM t1 JOIN (SELECT a, b FROM t2) s ON s.a = t1.a ORDER BY 1, 2",
            "SELECT t1.a, s.b FROM t1 LEFT JOIN (SELECT a, b FROM t2) s ON s.a = t1.a ORDER BY 1, 2",
            "SELECT s.a, t1.b FROM (SELECT a, b FROM t2) s LEFT JOIN t1 ON t1.a = s.a ORDER BY 1, 2",
            "SELECT * FROM (SELECT t2.a, t3.y FROM t2 LEFT JOIN t3 ON t3.x = t2.a) s WHERE s.a = 1 ORDER BY 1, 2",
            "SELECT * FROM (SELECT * FROM (SELECT a, b FROM t2) inner_s WHERE b > 100) s WHERE s.a = 1 ORDER BY 1, 2",
            "SELECT s.a, count(*) FROM (SELECT a, b FROM t2) s GROUP BY s.a ORDER BY 1",
            "SELECT DISTINCT s.a FROM (SELECT a, b FROM t2) s ORDER BY 1",
            "SELECT s.a FROM (SELECT a, b FROM t2 ORDER BY b LIMIT 2) s ORDER BY 1",
            "SELECT s.n FROM (SELECT a, count(*) AS n FROM t2 GROUP BY a) s ORDER BY 1",
            "SELECT s.a FROM (SELECT a FROM t2 UNION ALL SELECT a FROM t1) s WHERE s.a = 1",
            "SELECT * FROM (SELECT a AS x, b AS y FROM t2) s WHERE s.y > (SELECT min(b) FROM t2) ORDER BY 1, 2",
            "SELECT count(*) FROM (SELECT a FROM t2) s WHERE s.a IS NULL",
            "SELECT json_group_array(s.v) FROM (SELECT json_object('score', b) AS v FROM t2 WHERE b IS NOT NULL) s",
            "SELECT s.v FROM (SELECT json_object('score', b) AS v FROM t2 WHERE b IS NOT NULL) s ORDER BY s.v",
        ],
    );
}

#[test]
fn correlated_aggregate_subquery_becomes_a_grouped_join() {
    let database = database();
    let logical = connect(&database, true);
    let details = explain(
        &logical,
        "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a OR t2.b < t1.b) FROM t1",
    );
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("scalar_subquery")),
        "expected a grouped subquery table, got {details:?}"
    );
    assert!(
        details.iter().any(|detail| detail.contains("domain_")),
        "expected a domain table, got {details:?}"
    );
    assert!(
        details.iter().all(|detail| !detail.contains("CORRELATED")),
        "expected no subquery call for each outer row, got {details:?}"
    );
}

#[test]
fn unnested_subqueries_return_the_same_rows() {
    let database = database();
    assert_same_rows(
        &database,
        &[
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(b) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT sum(b) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT avg(b) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT max(b) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT total(b) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a IS t1.b) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a < t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a OR t2.b < t1.b) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a AND t2.b > t1.b) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a) + 1 FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a), (SELECT max(b) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a), (SELECT count(*) FROM t2 WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 JOIN t3 ON t3.x = t2.a WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(t3.y) FROM t2 LEFT JOIN t3 ON t3.x = t2.a WHERE t2.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a AND t2.b > 150) FROM t1 WHERE t1.b > 10 ORDER BY a",
            "SELECT a FROM t1 WHERE b < (SELECT avg(b) FROM t2 WHERE t2.a = t1.a) ORDER BY a",
            "SELECT a FROM t1 WHERE (SELECT count(*) FROM t2 WHERE t2.a = t1.a) = 0 ORDER BY a",
            "SELECT a FROM t1 WHERE (SELECT max(b) FROM t2 WHERE t2.a = t1.a) IS NULL ORDER BY a",
            "SELECT t1.a, t3.y, (SELECT count(*) FROM t2 WHERE t2.a = t3.x) FROM t1 LEFT JOIN t3 ON t3.x = t1.a ORDER BY 1, 2",
            "SELECT t1.a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a AND t2.b = t3.y) FROM t1 JOIN t3 ON t3.x = t1.a ORDER BY 1, 2",
            "SELECT sum((SELECT count(*) FROM t2 WHERE t2.a = t1.a)) FROM t1",
            "SELECT a FROM t1 ORDER BY (SELECT count(*) FROM t2 WHERE t2.a = t1.a) DESC, a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a) FROM t1 WHERE a IN (1, 3) ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a) FROM t1 WHERE random() IS NOT NULL ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a) FROM t1 WHERE a = (SELECT max(a) FROM t1)",
            "SELECT a, (SELECT count(*) FROM (SELECT a FROM t2 WHERE b > 100) s WHERE s.a = t1.a) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a LIMIT 1) FROM t1 ORDER BY a",
            "SELECT a, (SELECT count(*) FROM t2 WHERE t2.a = t1.a AND json_extract(CASE WHEN t2.a IS NULL THEN 'x' ELSE '{}' END, '$') IS NULL) FROM t1 ORDER BY a",
            "SELECT a, (SELECT sum(b) FROM t2 WHERE t2.a = t1.a) FROM t1 WHERE json_extract(CASE WHEN t1.a = 3 THEN 'x' ELSE '{}' END, '$') IS NULL AND a < 3 ORDER BY a",
        ],
    );
}
