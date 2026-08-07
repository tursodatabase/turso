use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, TempDatabase};
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

/// A UNIQUE index stores every NULL key separately, so `WHERE a IS NULL` can
/// touch many rows even though the index is unique. The statement journal must
/// stay enabled for such an UPDATE: if the statement fails halfway, the rows it
/// already changed must be restored.
#[test]
fn failed_update_with_is_seek_on_unique_index_rolls_back_every_row() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(a, b NOT NULL)");
    limbo_exec_rows(&conn, "CREATE UNIQUE INDEX ta ON t(a)");
    limbo_exec_rows(&conn, "INSERT INTO t VALUES (NULL, 1), (NULL, 2)");

    limbo_exec_rows(&conn, "BEGIN");
    // Row 1 gets b = 5, then row 2 hits the NOT NULL constraint. The whole
    // statement must roll back, including the change to row 1 that was
    // already applied.
    let result = limbo_exec_rows_fallible(
        &tmp_db,
        &conn,
        "UPDATE t SET b = CASE WHEN b = 1 THEN 5 ELSE NULL END WHERE a IS NULL",
    );
    assert!(
        result.is_err(),
        "expected the UPDATE to fail on NOT NULL, got {result:?}"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT b FROM t ORDER BY rowid"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        "the failed UPDATE must not leave row 1 changed"
    );
    limbo_exec_rows(&conn, "COMMIT");
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT b FROM t ORDER BY rowid"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        "the change from the failed UPDATE must not survive COMMIT"
    );
}

/// Same as above, but the many-NULL-rows key component sits at the end of a
/// composite UNIQUE index behind an `IS <non-NULL>` component.
#[test]
fn failed_update_with_composite_is_seek_on_unique_index_rolls_back_every_row() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(k1, k2, b NOT NULL)");
    limbo_exec_rows(&conn, "CREATE UNIQUE INDEX tk ON t(k1, k2)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO t VALUES (1, NULL, 1), (1, NULL, 2), (2, 'x', 3)",
    );

    limbo_exec_rows(&conn, "BEGIN");
    let result = limbo_exec_rows_fallible(
        &tmp_db,
        &conn,
        "UPDATE t SET b = CASE WHEN b = 1 THEN 5 ELSE NULL END WHERE k1 IS 1 AND k2 IS NULL",
    );
    assert!(
        result.is_err(),
        "expected the UPDATE to fail on NOT NULL, got {result:?}"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT b FROM t ORDER BY rowid"),
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ],
        "the failed UPDATE must not leave row 1 changed"
    );
    limbo_exec_rows(&conn, "COMMIT");
}

/// Rows: `a` is NULL for 90% of the 2000 rows and holds a distinct value on
/// every 10th row. sqlite_stat1 then reports ~10 average rows per `a` key
/// (2000 rows / 201 distinct keys, NULLs counting as one key) even though the
/// NULL bucket alone holds 1800 rows. `b` cycles 0..100, so every `b` value
/// matches ~20 rows.
fn create_null_heavy_table(conn: &std::sync::Arc<turso_core::Connection>, a_index_ddl: &str) {
    limbo_exec_rows(conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b)");
    limbo_exec_rows(conn, a_index_ddl);
    limbo_exec_rows(conn, "CREATE INDEX t_b ON t(b)");
    let values = (0..2000)
        .map(|i| {
            let a = if i % 10 == 0 {
                i.to_string()
            } else {
                "NULL".to_string()
            };
            format!("({}, {}, {})", i + 1, a, i % 100)
        })
        .collect::<Vec<_>>()
        .join(", ");
    limbo_exec_rows(conn, &format!("INSERT INTO t VALUES {values}"));
    limbo_exec_rows(conn, "ANALYZE");
}

/// A UNIQUE index bounds each non-NULL key to one row, but puts no bound on
/// NULL keys. `a IS NULL` here matches 1800 of 2000 rows, while `b = 5`
/// matches 20 — the planner must not treat the unique index as a 1-row lookup
/// and must pick the `b` index.
#[test]
fn is_null_on_unique_index_is_not_costed_as_one_row() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    create_null_heavy_table(&conn, "CREATE UNIQUE INDEX t_a ON t(a)");

    let plan = query_plan(&conn, "SELECT count(*) FROM t WHERE a IS NULL AND b = 5");
    assert!(
        plan.contains("USING INDEX t_b"),
        "the b index matches 20 rows, a IS NULL matches 1800 — expected t_b, got:\n{plan}"
    );
}

/// sqlite_stat1 stores the average rows per distinct key (~10 here), but the
/// NULL bucket alone holds 1800 rows. The planner must not apply the per-key
/// average to a NULL seek key; the `b` index (20 rows) must win.
#[test]
fn is_null_seek_does_not_use_average_rows_per_key_from_analyze() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    create_null_heavy_table(&conn, "CREATE INDEX t_a ON t(a)");

    let plan = query_plan(&conn, "SELECT count(*) FROM t WHERE a IS NULL AND b = 5");
    assert!(
        plan.contains("USING INDEX t_b"),
        "the b index matches 20 rows, a IS NULL matches 1800 — expected t_b, got:\n{plan}"
    );
}

#[test]
fn is_operator_seek_matches_null_in_65th_key_column() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    const WIDTH: usize = 65;
    let columns = (1..=WIDTH)
        .map(|i| format!("c{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = (1..=WIDTH)
        .map(|i| if i == WIDTH { "NULL" } else { "1" })
        .collect::<Vec<_>>()
        .join(", ");
    for statement in [
        format!("CREATE TABLE t ({columns})"),
        format!("CREATE INDEX ti ON t ({columns})"),
        format!("INSERT INTO t VALUES ({values})"),
    ] {
        limbo_exec_rows(&conn, &statement);
    }

    // Column 65 is the first column that does not fit in one 64-bit word.
    let predicate = (1..=WIDTH)
        .map(|i| {
            if i == WIDTH {
                format!("c{i} IS NULL")
            } else {
                format!("c{i} = 1")
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let query = format!("SELECT count(*) FROM t WHERE {predicate}");

    assert_eq!(
        limbo_exec_rows(&conn, &query),
        vec![vec![Value::Integer(1)]]
    );
}
