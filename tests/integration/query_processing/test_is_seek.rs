use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn query_plan(conn: &std::sync::Arc<turso_core::Connection>, query: &str) -> String {
    limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN {query}"))
        .iter()
        .filter_map(|row| match row.get(3) {
            Some(Value::Text(plan)) => Some(plan.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// `IS` is an index-usable equality in SQLite, not just a filter: it seeks the
/// index and additionally matches NULL. Without the seek, every NULL-safe
/// identity lookup (e.g. sync replay of a row whose composite primary key has a
/// NULL component) degrades to a full table scan.
#[test]
fn is_operator_uses_index_seek() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(a, b, c, PRIMARY KEY (a, b))");
    limbo_exec_rows(&conn, "CREATE TABLE s(k TEXT PRIMARY KEY, v)");

    for query in [
        "SELECT * FROM t WHERE a IS ? AND b IS ?",
        "DELETE FROM t WHERE a IS ? AND b IS ?",
        "UPDATE t SET c = ? WHERE a IS ? AND b IS ?",
    ] {
        let plan = query_plan(&conn, query);
        assert!(
            plan.contains("SEARCH t USING INDEX sqlite_autoindex_t_1 (a=? AND b=?)"),
            "expected a two-column index seek for `{query}`, got:\n{plan}"
        );
    }

    let plan = query_plan(&conn, "SELECT * FROM s WHERE k IS ?");
    assert!(
        plan.contains("SEARCH s USING INDEX sqlite_autoindex_s_1 (k=?)"),
        "expected an index seek for a single-column key, got:\n{plan}"
    );

    // Mixing the two: the `=` prefix and the NULL-matching component both seek.
    let plan = query_plan(&conn, "SELECT * FROM t WHERE a = ? AND b IS ?");
    assert!(
        plan.contains("SEARCH t USING INDEX sqlite_autoindex_t_1 (a=? AND b=?)"),
        "expected a two-column index seek for a mixed `=`/`IS` key, got:\n{plan}"
    );
}

#[test]
fn is_true_and_false_do_not_use_equality_seeks() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(x)");
    limbo_exec_rows(&conn, "CREATE INDEX ti ON t(x)");
    for literal in ["TRUE", "FALSE"] {
        let plan = query_plan(&conn, &format!("SELECT * FROM t WHERE x IS {literal}"));
        assert!(
            plan.contains("SCAN t"),
            "`IS {literal}` checks boolean value and cannot seek one key, got:\n{plan}"
        );
    }
    for query in [
        "SELECT * FROM t WHERE TRUE IS x",
        "SELECT * FROM t WHERE x IS +TRUE",
    ] {
        let plan = query_plan(&conn, query);
        assert!(
            plan.contains("SEARCH t USING INDEX ti (x=?)"),
            "expected an index seek for `{query}`, got:\n{plan}"
        );
    }
}

/// The seek must still find rows whose key component is NULL: `IS` compares
/// NULLs as equal, so the NULL travels into the seek key instead of ending the
/// loop the way an `=` seek does.
#[test]
fn is_operator_seek_matches_null_key_components() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(a, b, c, PRIMARY KEY (a, b))");
    limbo_exec_rows(
        &conn,
        "INSERT INTO t VALUES ('a1', NULL, 'c1'), ('a1', 'b1', 'c2'), (NULL, NULL, 'c3')",
    );

    let text = |rows: Vec<Vec<Value>>| -> Vec<String> {
        rows.iter()
            .map(|row| match row.first() {
                Some(Value::Text(value)) => value.clone(),
                other => panic!("unexpected row value: {other:?}"),
            })
            .collect()
    };

    assert_eq!(
        text(limbo_exec_rows(
            &conn,
            "SELECT c FROM t WHERE a IS 'a1' AND b IS NULL"
        )),
        vec!["c1".to_string()]
    );
    assert_eq!(
        text(limbo_exec_rows(
            &conn,
            "SELECT c FROM t WHERE a IS NULL AND b IS NULL"
        )),
        vec!["c3".to_string()]
    );
    assert_eq!(
        text(limbo_exec_rows(
            &conn,
            "SELECT c FROM t WHERE a = 'a1' AND b IS NULL"
        )),
        vec!["c1".to_string()]
    );
    // `=` keeps its semantics: NULL never matches.
    assert!(limbo_exec_rows(&conn, "SELECT c FROM t WHERE a = 'a1' AND b = NULL").is_empty());

    limbo_exec_rows(&conn, "DELETE FROM t WHERE a IS 'a1' AND b IS NULL");
    assert_eq!(
        text(limbo_exec_rows(&conn, "SELECT c FROM t ORDER BY c")),
        vec!["c2".to_string(), "c3".to_string()]
    );
}
