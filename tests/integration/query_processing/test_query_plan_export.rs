//! The machine-readable query plan, as exposed by `Connection::query_plan`.
//!
//! Two things have to stay true, and both are easy to break by accident:
//!
//! 1. The plan describes the same tree `EXPLAIN QUERY PLAN` prints — same ids,
//!    same parents, same text — because both are rendered from the same nodes.
//! 2. The structured fields say what the text says, so a tool never has to
//!    read the text back.

use turso_core::explain_plan::{PlanOp, QueryPlan, TableKind};

use crate::common::{limbo_exec_rows, TempDatabase};

fn schema(conn: &std::sync::Arc<turso_core::Connection>) {
    limbo_exec_rows(
        conn,
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)",
    );
    limbo_exec_rows(
        conn,
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)",
    );
    limbo_exec_rows(conn, "CREATE INDEX idx_users_age ON users(age)");
    limbo_exec_rows(conn, "CREATE INDEX idx_orders_user ON orders(user_id)");
}

/// The `(id, parent, detail)` triples `EXPLAIN QUERY PLAN` returns as rows.
fn eqp_rows(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> Vec<(i64, i64, String)> {
    limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN {sql}"))
        .iter()
        .map(|row| {
            let int = |i: usize| match row.get(i) {
                Some(rusqlite::types::Value::Integer(v)) => *v,
                other => panic!("column {i} of an EQP row should be an integer, got {other:?}"),
            };
            let detail = match row.get(3) {
                Some(rusqlite::types::Value::Text(text)) => text.clone(),
                other => panic!("column 3 of an EQP row should be text, got {other:?}"),
            };
            (int(0), int(1), detail)
        })
        .collect()
}

fn triples(plan: &QueryPlan) -> Vec<(i64, i64, String)> {
    plan.nodes
        .iter()
        .map(|node| {
            (
                node.id as i64,
                node.parent_id.map_or(0, |id| id as i64),
                node.op.to_string(),
            )
        })
        .collect()
}

/// Every statement shape we annotate, so a new node kind that forgets to
/// render the same text as before gets caught here rather than in a snapshot.
const STATEMENTS: &[&str] = &[
    "SELECT 1",
    "SELECT * FROM users",
    "SELECT * FROM users u WHERE u.age > 30",
    "SELECT * FROM users WHERE id = 5",
    "SELECT age FROM users ORDER BY age",
    "SELECT DISTINCT city FROM users",
    "SELECT city, count(*) FROM users GROUP BY city ORDER BY 2 DESC",
    "SELECT count(DISTINCT city) FROM users",
    "SELECT * FROM users LEFT JOIN orders ON orders.user_id = users.id",
    "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id ORDER BY o.total",
    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    "SELECT name, (SELECT count(*) FROM orders o WHERE o.user_id = users.id) FROM users",
    "SELECT name FROM users UNION SELECT city FROM users",
    "SELECT name FROM users UNION ALL SELECT city FROM users",
    "SELECT name FROM users EXCEPT SELECT city FROM users",
    "SELECT name FROM users INTERSECT SELECT city FROM users",
    "SELECT * FROM (SELECT city, count(*) c FROM users GROUP BY city) t WHERE t.c > 2",
    "SELECT * FROM users WHERE age = 3 OR city = 'x'",
    "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT * FROM cnt",
    "DELETE FROM users WHERE age > 30",
    "UPDATE users SET name = 'x' WHERE id = 3",
];

#[test]
fn the_exported_plan_is_the_same_tree_explain_query_plan_prints() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    for sql in STATEMENTS {
        let plan = conn
            .query_plan(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(
            triples(&plan),
            eqp_rows(&conn, sql),
            "the exported plan and the EQP rows disagree for: {sql}"
        );
    }
}

#[test]
fn explain_query_plan_prefixes_are_accepted_and_ignored() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let plain = conn.query_plan("SELECT * FROM users").unwrap();
    for prefixed in [
        "EXPLAIN SELECT * FROM users",
        "EXPLAIN QUERY PLAN SELECT * FROM users",
    ] {
        assert_eq!(
            triples(&conn.query_plan(prefixed).unwrap()),
            triples(&plain)
        );
    }
}

#[test]
fn a_scan_reports_the_table_it_reads() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let plan = conn.query_plan("SELECT * FROM users u").unwrap();
    let [node] = &plan.nodes[..] else {
        panic!("expected one node, got {:?}", plan.nodes);
    };
    let PlanOp::Scan {
        access,
        index,
        left_join,
    } = &node.op
    else {
        panic!("expected a scan, got {}", node.op);
    };
    assert_eq!(access.name, "users");
    assert_eq!(access.identifier, "u");
    assert_eq!(access.kind, TableKind::Table);
    assert_eq!(*index, None);
    assert!(!left_join);
    assert_eq!(node.op.to_string(), "SCAN users AS u");
}

#[test]
fn a_seek_reports_its_index_and_the_key_parts_it_pins_down() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let plan = conn
        .query_plan("SELECT * FROM users WHERE age > 30")
        .unwrap();
    let search = plan
        .nodes
        .iter()
        .find(|node| matches!(node.op, PlanOp::Search { .. }))
        .unwrap_or_else(|| panic!("expected a search, got {:?}", plan.nodes));
    let PlanOp::Search {
        index, constraints, ..
    } = &search.op
    else {
        unreachable!("filtered for Search above");
    };
    assert_eq!(
        index.as_ref().map(|i| i.name.as_str()),
        Some("idx_users_age")
    );
    assert_eq!(constraints, &["age>?".to_string()]);
}

/// A rowid lookup has no index, and says so with a constraint rather than by
/// spelling `rowid=?` into the text and leaving nothing structured behind.
#[test]
fn a_rowid_lookup_reports_no_index() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let plan = conn.query_plan("SELECT * FROM users WHERE id = 5").unwrap();
    let PlanOp::Search {
        index, constraints, ..
    } = &plan.nodes[0].op
    else {
        panic!("expected a search, got {}", plan.nodes[0].op);
    };
    assert_eq!(*index, None);
    assert_eq!(constraints, &["rowid=?".to_string()]);
    assert_eq!(
        plan.nodes[0].op.to_string(),
        "SEARCH users USING INTEGER PRIMARY KEY (rowid=?)"
    );
}

/// The text of a SCAN line never says LEFT-JOIN, but the node still knows.
#[test]
fn a_left_joined_scan_is_flagged_even_though_the_text_is_silent() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&conn, "CREATE TABLE l(id INTEGER)");
    limbo_exec_rows(&conn, "CREATE TABLE r(id INTEGER)");

    // `<>` cannot drive a hash join or an index seek, so the right side is
    // scanned once per left row and the scan node is the one to inspect.
    let plan = conn
        .query_plan("SELECT * FROM l LEFT JOIN r ON r.id <> l.id")
        .unwrap();
    let right = plan
        .nodes
        .iter()
        .find(|node| {
            node.op
                .table_access()
                .is_some_and(|access| access.identifier == "r")
        })
        .unwrap_or_else(|| panic!("expected a node for r, got {:?}", plan.nodes));
    let PlanOp::Scan { left_join, .. } = &right.op else {
        panic!("expected a scan of r, got {}", right.op);
    };
    assert!(left_join, "r is the right side of a LEFT JOIN");
    assert_eq!(right.op.to_string(), "SCAN r");
}

#[test]
fn a_correlated_subquery_is_marked_correlated() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let plan = conn
        .query_plan(
            "SELECT name, (SELECT count(*) FROM orders o WHERE o.user_id = users.id) FROM users",
        )
        .unwrap();
    let subquery = plan
        .nodes
        .iter()
        .find_map(|node| match &node.op {
            PlanOp::Subquery {
                correlated, kind, ..
            } => Some((*correlated, *kind)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a subquery node, got {:?}", plan.nodes));
    assert!(subquery.0, "the subquery reads a column of the outer query");
    assert_eq!(subquery.1, turso_core::explain_plan::SubqueryKind::Scalar);
}

/// Children always come after their parent, which is what lets a consumer
/// build the tree in one pass.
#[test]
fn parents_always_come_before_their_children() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    for sql in STATEMENTS {
        let plan = conn.query_plan(sql).unwrap();
        let mut seen = std::collections::HashSet::new();
        for node in &plan.nodes {
            if let Some(parent) = node.parent_id {
                assert!(
                    seen.contains(&parent),
                    "node {} names parent {parent}, which has not been emitted yet, in: {sql}",
                    node.id
                );
            }
            seen.insert(node.id);
        }
    }
}

#[test]
fn the_json_carries_every_node_and_stays_parseable() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    for sql in STATEMENTS {
        let plan = conn.query_plan(sql).unwrap();
        let json: serde_json::Value = serde_json::from_str(&plan.to_json())
            .unwrap_or_else(|e| panic!("{sql} produced JSON that does not parse: {e}"));

        assert_eq!(json["sql"].as_str().unwrap(), plan.sql);
        let nodes = json["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), plan.nodes.len());
        for (value, node) in nodes.iter().zip(&plan.nodes) {
            assert_eq!(value["id"].as_u64().unwrap() as usize, node.id);
            assert_eq!(value["detail"].as_str().unwrap(), node.op.to_string());
            assert_eq!(value["op"].as_str().unwrap(), node.op.tag());
            match node.parent_id {
                Some(parent) => assert_eq!(value["parent_id"].as_u64().unwrap() as usize, parent),
                None => assert!(value["parent_id"].is_null()),
            }
        }
    }
}

/// A statement whose text carries a quote or a newline still yields JSON, and
/// the SQL comes back unchanged.
#[test]
fn json_survives_sql_that_needs_escaping() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let sql = "SELECT *\n  FROM users\n WHERE city = '\"quoted\"\tvalue'";
    let plan = conn.query_plan(sql).unwrap();
    let json: serde_json::Value = serde_json::from_str(&plan.to_json()).unwrap();
    assert_eq!(json["sql"].as_str().unwrap(), plan.sql);
    assert!(json["sql"].as_str().unwrap().contains("\"quoted\""));
}

/// The estimate comes from the optimizer, so it only appears on steps the
/// optimizer actually costed.
#[test]
fn table_access_carries_the_optimizers_row_estimate() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    let plan = conn.query_plan("SELECT * FROM users").unwrap();
    let estimate = plan.nodes[0]
        .op
        .table_access()
        .and_then(|access| access.estimated_rows)
        .expect("a scan chosen by the optimizer has a row estimate");
    assert!(
        estimate.is_finite() && estimate > 0.0,
        "row estimates are positive and finite, got {estimate}"
    );

    let sorter = conn
        .query_plan("SELECT age FROM users ORDER BY age + 1")
        .unwrap()
        .nodes
        .into_iter()
        .find(|node| matches!(node.op, PlanOp::Sort { .. }))
        .expect("ordering by an expression needs a sorter");
    assert!(sorter.op.table_access().is_none());
}

#[test]
fn a_statement_that_does_not_compile_reports_the_error() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    schema(&conn);

    assert!(conn.query_plan("SELECT * FROM nope").is_err());
    assert!(conn.query_plan("SELECT FROM").is_err());
    assert!(conn.query_plan("").is_err());
}
