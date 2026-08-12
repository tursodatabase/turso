use crate::common::{limbo_exec_rows, TempDatabase};

const SCHEMA: [&str; 4] = [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city_id INTEGER)",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
    "CREATE INDEX idx_users_age ON users(age)",
    "CREATE INDEX idx_orders_user ON orders(user_id)",
];

fn connect_with_schema(tmp_db: &TempDatabase) -> std::sync::Arc<turso_core::Connection> {
    let conn = tmp_db.connect_limbo();
    for ddl in SCHEMA {
        limbo_exec_rows(&conn, ddl);
    }
    conn
}

fn plan_json(tmp_db: &TempDatabase, sql: &str) -> serde_json::Value {
    let conn = connect_with_schema(tmp_db);
    let stmt = conn
        .prepare(format!("EXPLAIN QUERY PLAN {sql}"))
        .expect("prepare failed");
    let json = stmt
        .query_plan_json()
        .expect("EXPLAIN QUERY PLAN statement must produce a plan");
    serde_json::from_str(&json).expect("plan must be valid JSON")
}

fn nodes(plan: &serde_json::Value) -> &Vec<serde_json::Value> {
    plan["nodes"].as_array().expect("plan must have nodes")
}

fn find_node<'a>(
    plan: &'a serde_json::Value,
    op_type: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> {
    nodes(plan)
        .iter()
        .filter(move |n| n["op"]["type"] == op_type)
}

#[turso_macros::test]
fn plain_statement_has_no_plan_json(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let stmt = conn.prepare("SELECT * FROM users")?;
    assert!(stmt.query_plan_json().is_none());
    Ok(())
}

#[turso_macros::test]
fn search_nodes_report_index_constraints_and_left_join(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "SELECT u.name, o.amount FROM users u
         LEFT JOIN orders o ON o.user_id = u.id
         WHERE u.age > 21 ORDER BY o.amount",
    );

    let searches: Vec<_> = find_node(&plan, "search").collect();
    assert_eq!(searches.len(), 2, "expected two searches: {plan:#}");

    let users = &searches[0]["op"];
    assert_eq!(users["table"], "users");
    assert_eq!(users["alias"], "u");
    assert_eq!(users["index"]["name"], "idx_users_age");
    assert_eq!(users["constraints"][0], "age>?");
    assert!(users.get("join").is_none(), "first table has no join");

    let orders = &searches[1]["op"];
    assert_eq!(orders["table"], "orders");
    assert_eq!(orders["join"], "left");
    assert_eq!(orders["index"]["name"], "idx_orders_user");

    let order_by: Vec<_> = find_node(&plan, "order_by").collect();
    assert_eq!(order_by.len(), 1);
    assert_eq!(order_by[0]["op"]["method"], "sorter");

    assert_eq!(plan["result_columns"][0], "name");
    assert_eq!(plan["result_columns"][1], "amount");
    Ok(())
}

#[turso_macros::test]
fn exists_subquery_is_reported_as_semi_join(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    let semi_joins: Vec<_> = nodes(&plan)
        .iter()
        .filter(|n| n["op"]["join"] == "semi")
        .collect();
    assert_eq!(semi_joins.len(), 1, "expected one semi join: {plan:#}");
    assert_eq!(semi_joins[0]["op"]["table"], "orders");
    Ok(())
}

#[turso_macros::test]
fn shared_cte_links_readers_to_one_materialization(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "WITH spenders AS (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id)
         SELECT u.name, s1.total FROM users u
         JOIN spenders s1 ON s1.user_id = u.id
         JOIN spenders s2 ON s2.user_id = u.city_id",
    );

    // Both CTE readers carry the same cte_id and a reuse marker.
    let readers: Vec<_> = nodes(&plan)
        .iter()
        .filter(|n| n["op"]["subquery"]["execution"] == "materialized_reuse")
        .collect();
    assert_eq!(readers.len(), 2, "expected two CTE readers: {plan:#}");
    let cte_id = readers[0]["op"]["subquery"]["cte_id"].as_u64().unwrap();
    assert_eq!(
        readers[1]["op"]["subquery"]["cte_id"].as_u64().unwrap(),
        cte_id
    );

    // The materialization side channel names the CTE and points at real nodes.
    let ctes = plan["cte_materializations"].as_array().unwrap();
    assert_eq!(ctes.len(), 1);
    assert_eq!(ctes[0]["name"], "spenders");
    assert_eq!(ctes[0]["cte_id"].as_u64().unwrap(), cte_id);
    let node_ids: Vec<u64> = ctes[0]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert!(!node_ids.is_empty());
    for id in node_ids {
        assert!(
            nodes(&plan).iter().any(|n| n["id"].as_u64() == Some(id)),
            "cte materialization node {id} missing from plan: {plan:#}"
        );
    }
    Ok(())
}

#[turso_macros::test]
fn from_subquery_coroutine_body_nests_under_its_scan(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "SELECT city_id, avg(age) FROM (SELECT * FROM users WHERE age > 18)
         GROUP BY city_id ORDER BY avg(age)",
    );

    let scans: Vec<_> = find_node(&plan, "scan").collect();
    let subquery_scan = scans
        .iter()
        .find(|n| n["op"]["source"] == "subquery")
        .expect("subquery scan present");
    assert_eq!(subquery_scan["op"]["subquery"]["execution"], "coroutine");

    // The subquery body (search on users) is a child of the scan node.
    let scan_id = subquery_scan["id"].as_u64().unwrap();
    let body: Vec<_> = nodes(&plan)
        .iter()
        .filter(|n| n["parent"].as_u64() == Some(scan_id))
        .collect();
    assert!(!body.is_empty(), "coroutine body must nest under the scan");
    assert_eq!(body[0]["op"]["type"], "search");

    assert_eq!(find_node(&plan, "group_by").count(), 1);
    assert_eq!(find_node(&plan, "order_by").count(), 1);
    Ok(())
}

#[turso_macros::test]
fn compound_select_reports_arms(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "SELECT name FROM users WHERE age > 30 UNION SELECT name FROM users ORDER BY name",
    );

    let compound: Vec<_> = find_node(&plan, "compound").collect();
    assert_eq!(compound.len(), 1);
    let compound_id = compound[0]["id"].as_u64().unwrap();

    let arm_ops: Vec<&str> = nodes(&plan)
        .iter()
        .filter(|n| n["op"]["type"] == "compound_arm")
        .map(|n| n["op"]["op"].as_str().unwrap())
        .collect();
    assert_eq!(arm_ops, vec!["left_most", "union"], "plan: {plan:#}");

    // Arms hang off the compound node.
    for arm in nodes(&plan)
        .iter()
        .filter(|n| n["op"]["type"] == "compound_arm")
    {
        assert_eq!(arm["parent"].as_u64(), Some(compound_id));
    }
    Ok(())
}

#[turso_macros::test]
fn scalar_and_list_subqueries_report_correlation(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "SELECT u.name, (SELECT count(*) FROM orders o WHERE o.user_id = u.id) FROM users u
         WHERE u.age IN (SELECT age FROM users WHERE city_id = 3)",
    );

    let lists: Vec<_> = find_node(&plan, "list_subquery").collect();
    assert_eq!(lists.len(), 1);
    assert_eq!(lists[0]["op"]["correlated"], false);

    let scalars: Vec<_> = find_node(&plan, "scalar_subquery").collect();
    assert_eq!(scalars.len(), 1);
    assert_eq!(scalars[0]["op"]["correlated"], true);

    // Each subquery node has its body nested under it.
    for sub in lists.iter().chain(scalars.iter()) {
        let id = sub["id"].as_u64().unwrap();
        assert!(
            nodes(&plan)
                .iter()
                .any(|n| n["parent"].as_u64() == Some(id)),
            "subquery body must nest under node {id}: {plan:#}"
        );
    }
    Ok(())
}

#[turso_macros::test]
fn multi_index_or_reports_branch_indexes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(&tmp_db, "SELECT * FROM users WHERE id = 5 OR age = 30");
    let multi: Vec<_> = find_node(&plan, "multi_index").collect();
    assert_eq!(multi.len(), 1, "plan: {plan:#}");
    assert_eq!(multi[0]["op"]["set_op"], "or");
    let indexes = multi[0]["op"]["indexes"].as_array().unwrap();
    assert_eq!(indexes.len(), 2);
    assert!(indexes.contains(&serde_json::json!("PRIMARY KEY")));
    assert!(indexes.contains(&serde_json::json!("idx_users_age")));
    Ok(())
}

#[turso_macros::test]
fn recursive_cte_reports_setup_and_step(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let plan = plan_json(
        &tmp_db,
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10)
         SELECT x FROM cnt",
    );
    let outer_scan = find_node(&plan, "scan")
        .find(|n| n["op"]["subquery"]["recursive"] == true)
        .expect("recursive CTE scan present");
    let outer_id = outer_scan["id"].as_u64().unwrap();

    let setup: Vec<_> = find_node(&plan, "recursive_setup").collect();
    let step: Vec<_> = find_node(&plan, "recursive_step").collect();
    assert_eq!(setup.len(), 1);
    assert_eq!(step.len(), 1);
    assert_eq!(setup[0]["parent"].as_u64(), Some(outer_id));
    assert_eq!(step[0]["parent"].as_u64(), Some(outer_id));

    // The recursive part reads the CTE's own queue.
    let input_scan = find_node(&plan, "scan")
        .find(|n| n["op"]["source"] == "recursive_cte_input")
        .expect("recursive input scan present");
    assert_eq!(
        input_scan["parent"].as_u64(),
        Some(step[0]["id"].as_u64().unwrap())
    );
    Ok(())
}

/// The JSON must describe the same tree as the EXPLAIN QUERY PLAN rows:
/// same ids, same parents, same detail strings.
#[turso_macros::test]
fn json_matches_explain_query_plan_rows(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let sql = "WITH spenders AS (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id)
         SELECT u.name, s1.total FROM users u
         JOIN spenders s1 ON s1.user_id = u.id
         LEFT JOIN orders o ON o.user_id = u.id
         WHERE u.age IN (SELECT age FROM users WHERE city_id = 3)
         ORDER BY s1.total DESC";
    let plan = plan_json(&tmp_db, sql);

    let conn = tmp_db.connect_limbo();
    let rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"));
    assert_eq!(rows.len(), nodes(&plan).len(), "plan: {plan:#}");
    for (row, node) in rows.iter().zip(nodes(&plan)) {
        let id = match row.first() {
            Some(rusqlite::types::Value::Integer(id)) => *id as u64,
            other => panic!("unexpected id column: {other:?}"),
        };
        let parent = match row.get(1) {
            Some(rusqlite::types::Value::Integer(parent)) => *parent as u64,
            other => panic!("unexpected parent column: {other:?}"),
        };
        let detail = match row.get(3) {
            Some(rusqlite::types::Value::Text(detail)) => detail.clone(),
            other => panic!("unexpected detail column: {other:?}"),
        };
        assert_eq!(node["id"].as_u64(), Some(id));
        // The row output encodes "no parent" as 0; the JSON as null.
        assert_eq!(node["parent"].as_u64().unwrap_or(0), parent);
        assert_eq!(node["detail"].as_str(), Some(detail.as_str()));
    }
    Ok(())
}

#[turso_macros::test]
fn format_json_returns_the_plan_as_one_text_row(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let rows = limbo_exec_rows(
        &conn,
        "EXPLAIN QUERY PLAN FORMAT=JSON SELECT * FROM users WHERE age > 21",
    );
    assert_eq!(rows.len(), 1, "expected one row: {rows:?}");
    assert_eq!(rows[0].len(), 1, "expected one column: {rows:?}");
    let rusqlite::types::Value::Text(json) = &rows[0][0] else {
        panic!("plan_json column must be text: {rows:?}");
    };
    let plan: serde_json::Value = serde_json::from_str(json)?;
    assert_eq!(plan["version"], 1);
    let searches: Vec<_> = find_node(&plan, "search").collect();
    assert_eq!(searches.len(), 1, "{plan:#}");
    assert_eq!(searches[0]["op"]["index"]["name"], "idx_users_age");
    Ok(())
}

#[turso_macros::test]
fn format_json_row_equals_the_rust_api_output(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let sql = "EXPLAIN QUERY PLAN FORMAT=JSON
        SELECT u.name FROM users u JOIN orders o ON o.user_id = u.id";
    let rows = limbo_exec_rows(&conn, sql);
    let rusqlite::types::Value::Text(from_row) = &rows[0][0] else {
        panic!("plan_json column must be text: {rows:?}");
    };
    let stmt = conn.prepare(sql)?;
    let from_api = stmt
        .query_plan_json()
        .expect("FORMAT=JSON statement must expose query_plan_json");
    assert_eq!(from_row, &from_api);
    Ok(())
}

#[turso_macros::test]
fn format_json_statement_reports_one_text_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let stmt = conn.prepare("EXPLAIN QUERY PLAN FORMAT=JSON SELECT 1")?;
    assert_eq!(stmt.num_columns(), 1);
    assert_eq!(stmt.get_column_name(0), "plan_json");
    assert_eq!(stmt.get_column_decltype(0).as_deref(), Some("TEXT"));
    Ok(())
}

#[turso_macros::test]
fn format_text_matches_the_default_text_output(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let sql = "SELECT u.name, o.amount FROM users u
        LEFT JOIN orders o ON o.user_id = u.id WHERE u.age > 21";
    let default_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"));
    let text_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN FORMAT=TEXT {sql}"));
    assert_eq!(default_rows, text_rows);
    Ok(())
}

#[turso_macros::test]
fn unknown_format_is_a_parse_error(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let err = conn
        .prepare("EXPLAIN QUERY PLAN FORMAT=YAML SELECT 1")
        .expect_err("unknown format must not prepare");
    assert!(
        err.to_string()
            .contains("unknown EXPLAIN QUERY PLAN format"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[turso_macros::test]
fn format_json_prepares_writes_without_executing_them(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let rows = limbo_exec_rows(
        &conn,
        "EXPLAIN QUERY PLAN FORMAT=JSON INSERT INTO users(name) VALUES ('x')",
    );
    let rusqlite::types::Value::Text(json) = &rows[0][0] else {
        panic!("plan_json column must be text: {rows:?}");
    };
    assert!(json.contains("\"version\""), "not a plan document: {json}");
    let count = limbo_exec_rows(&conn, "SELECT count(*) FROM users");
    assert_eq!(count[0][0], rusqlite::types::Value::Integer(0));
    Ok(())
}
