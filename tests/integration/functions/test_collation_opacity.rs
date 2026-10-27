use rusqlite::types::Value;

use crate::common::{limbo_exec_rows, TempDatabase};

fn ids(rows: Vec<Vec<Value>>) -> Vec<Value> {
    rows.into_iter().map(|row| row[0].clone()).collect()
}

const SETUP_TABLE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, e TEXT COLLATE NOCASE)";
const SETUP_ROWS: &str = "INSERT INTO t VALUES(1,'A'),(2,'a')";

fn setup() -> (TempDatabase, std::sync::Arc<turso_core::Connection>) {
    let tmp = TempDatabase::new_with_rusqlite(SETUP_TABLE);
    let conn = tmp.connect_limbo();
    limbo_exec_rows(&conn, SETUP_ROWS);
    (tmp, conn)
}

#[test]
fn collation_opaque_expressions_compare_binary() {
    let (_tmp, conn) = setup();

    // A bare column comparison uses the column's declared NOCASE collation.
    let rows = limbo_exec_rows(&conn, "SELECT id FROM t WHERE e = 'a'");
    assert_eq!(ids(rows), vec![Value::Integer(1), Value::Integer(2)]);

    // Collation-opaque wrappers fall back to BINARY, so only 'a' matches.
    for expr in [
        "(e || '')",
        "trim(e)",
        "substr(e, 1, 1)",
        "coalesce(e, '')",
        "replace(e, 'x', '')",
        "CASE WHEN 1 THEN e END",
    ] {
        let query = format!("SELECT id FROM t WHERE {expr} = 'a'");
        let rows = limbo_exec_rows(&conn, &query);
        assert_eq!(
            ids(rows),
            vec![Value::Integer(2)],
            "{expr} should compare BINARY, got NOCASE"
        );
    }
}

#[test]
fn explicit_collate_still_hoists_through_opaque_expressions() {
    let (_tmp, conn) = setup();

    for query in [
        "SELECT id FROM t WHERE (e COLLATE NOCASE || '') = 'a'",
        "SELECT id FROM t WHERE ('' || e COLLATE NOCASE) = 'a'",
        "SELECT id FROM t WHERE trim(e COLLATE NOCASE) = 'a'",
        "SELECT id FROM t WHERE coalesce(e, e COLLATE NOCASE) = 'a'",
        "SELECT id FROM t WHERE CASE WHEN 1 THEN e COLLATE NOCASE END = 'a'",
        "SELECT id FROM t WHERE CASE WHEN 0 THEN e ELSE e COLLATE NOCASE END = 'a'",
    ] {
        let rows = limbo_exec_rows(&conn, query);
        assert_eq!(
            ids(rows),
            vec![Value::Integer(1), Value::Integer(2)],
            "{query} should keep the explicit NOCASE collation"
        );
    }
}

#[test]
fn delete_over_concat_does_not_remove_rows_it_should_keep() {
    // The reported failure mode: a DELETE whose predicate wraps a NOCASE
    // column in a collation-opaque expression must not match the
    // case-insensitive sibling.
    let (_tmp, conn) = setup();

    limbo_exec_rows(&conn, "DELETE FROM t WHERE (e || '') = 'a'");
    let rows = limbo_exec_rows(&conn, "SELECT id FROM t");
    assert_eq!(ids(rows), vec![Value::Integer(1)]);
}
