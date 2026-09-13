use crate::common::{limbo_exec_rows, TempDatabase, TempDatabaseBuilder};
use rusqlite::types::Value;
use std::sync::Arc;
use turso_core::Connection;

const SCHEMA: [&str; 2] = [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    "CREATE INDEX idx_users_age ON users(age)",
];

fn connect_with_schema(tmp_db: &TempDatabase) -> Arc<Connection> {
    let conn = tmp_db.connect_limbo();
    for ddl in SCHEMA {
        limbo_exec_rows(&conn, ddl);
    }
    conn
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
fn join_access_nodes_report_cost_and_row_estimates(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
    );
    limbo_exec_rows(&conn, "CREATE INDEX idx_orders_user ON orders(user_id)");

    let plan = explain_query_plan(
        &conn,
        "SELECT users.name, orders.total
         FROM users JOIN orders ON orders.user_id = users.id
         WHERE users.id = 1",
    )?;
    let access_nodes: Vec<_> = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|node| matches!(node["op"]["type"].as_str(), Some("scan" | "search")))
        .collect();

    assert_eq!(access_nodes.len(), 2);
    assert_eq!(access_nodes[0]["op"]["estimate"]["input_rows"], 1.0);
    for node in access_nodes {
        let estimate = &node["op"]["estimate"];
        assert!(estimate["rows_per_input"].as_f64().unwrap() > 0.0);
        assert!(estimate["output_rows"].as_f64().unwrap() > 0.0);
        assert!(estimate["access_cost"].as_f64().unwrap() > 0.0);
        assert!(estimate["total_cost"].as_f64().unwrap() > 0.0);
    }
    Ok(())
}

#[turso_macros::test]
fn outer_join_null_test_estimates_unmatched_rows(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_parent_child_data(&conn, "CREATE INDEX child_parent ON child(parent_id)");

    let query = "SELECT parent.id
                 FROM parent
                 LEFT JOIN child ON child.parent_id = parent.id AND child.kind = 1
                 WHERE child.id IS NULL";
    assert_eq!(limbo_exec_rows(&conn, query).len(), 90);

    let plan = explain_query_plan(&conn, query)?;
    let estimated_rows = final_output_rows(&plan);
    assert!(
        (80.0..99.0).contains(&estimated_rows),
        "expected 80 to 99 rows, got {estimated_rows}"
    );
    Ok(())
}

#[turso_macros::test]
fn nullable_primary_key_null_test_keeps_matched_rows(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    limbo_exec_rows(
        &conn,
        "CREATE TABLE child (
            key TEXT PRIMARY KEY,
            nullable_value TEXT,
            parent_id INTEGER
        )",
    );
    limbo_exec_rows(&conn, "CREATE INDEX child_parent ON child(parent_id)");
    limbo_exec_rows(&conn, "INSERT INTO parent VALUES (1), (2)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO child VALUES (NULL, NULL, 1), ('two', 'two', 2)",
    );
    limbo_exec_rows(&conn, "ANALYZE");

    let primary_key_plan = explain_query_plan(
        &conn,
        "SELECT parent.id
         FROM parent LEFT JOIN child ON child.parent_id = parent.id
         WHERE child.key IS NULL",
    )?;
    let nullable_column_plan = explain_query_plan(
        &conn,
        "SELECT parent.id
         FROM parent LEFT JOIN child ON child.parent_id = parent.id
         WHERE child.nullable_value IS NULL",
    )?;

    assert_eq!(
        final_output_rows(&primary_key_plan),
        final_output_rows(&nullable_column_plan)
    );
    Ok(())
}

#[turso_macros::test]
fn compound_join_lookup_estimates_its_constant_filter(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_parent_child_data(
        &conn,
        "CREATE INDEX child_parent_kind ON child(parent_id, kind)",
    );

    let query = "SELECT parent.id
                 FROM parent CROSS JOIN child
                 WHERE child.parent_id = parent.id AND child.kind = 1";
    assert_eq!(limbo_exec_rows(&conn, query).len(), 10);

    let plan = explain_query_plan(&conn, query)?;
    let child_estimate = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["table"] == "child")
        .map(|node| &node["op"]["estimate"])
        .unwrap();
    assert_eq!(child_estimate["input_rows"], 100.0);
    let output_rows = child_estimate["output_rows"].as_f64().unwrap();
    assert!(
        (5.0..=20.0).contains(&output_rows),
        "expected 5 to 20 rows, got {output_rows}"
    );
    Ok(())
}

#[turso_macros::test]
fn unavailable_duplicate_does_not_stop_a_compound_lookup(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&conn, "CREATE TABLE first (a INTEGER, b INTEGER)");
    limbo_exec_rows(&conn, "CREATE TABLE middle (x INTEGER, y INTEGER)");
    limbo_exec_rows(&conn, "CREATE INDEX middle_xy ON middle(x, y)");
    limbo_exec_rows(&conn, "CREATE TABLE last (z INTEGER)");

    let plan = explain_query_plan(
        &conn,
        "SELECT *
         FROM first CROSS JOIN middle CROSS JOIN last
         WHERE middle.x = first.a
           AND middle.x = last.z
           AND middle.y = first.b",
    )?;
    let middle_constraints = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["table"] == "middle")
        .map(|node| node["op"]["constraints"].as_array().unwrap())
        .unwrap();
    assert_eq!(middle_constraints.len(), 2);
    assert_eq!(middle_constraints[0], "x=?");
    assert_eq!(middle_constraints[1], "y=?");
    Ok(())
}

#[turso_macros::test]
fn limit_reduces_correlated_filter_calls_only_when_input_can_stop(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_parent_child_data(&conn, "CREATE INDEX child_parent ON child(parent_id)");
    let filter = "SELECT parent.id FROM parent
        WHERE (SELECT count(*) FROM child WHERE child.parent_id > parent.id) > 0";
    for (suffix, consumes_input, sorted) in [
        (" ORDER BY parent.id % 10, parent.id LIMIT 3", true, true),
        (" GROUP BY parent.id % 10 LIMIT 3", true, false),
        (" LIMIT 3 OFFSET 97", true, false),
        (" ORDER BY parent.id LIMIT 3", false, false),
        (" LIMIT 3", false, false),
    ] {
        let plan = explain_query_plan(&conn, &format!("{filter}{suffix}"))?;
        let nodes = plan["nodes"].as_array().unwrap();
        let child = nodes
            .iter()
            .find(|node| node["op"]["table"] == "child")
            .unwrap();
        let calls = child["op"]["estimate"]["input_rows"].as_f64().unwrap();
        if sorted {
            assert!(nodes.iter().any(|node| node["op"]["type"] == "order_by"));
        }
        if consumes_input {
            assert_eq!(calls, 100.0, "{suffix}");
        } else {
            assert!(calls < 100.0, "{suffix}: {calls}");
        }
    }
    Ok(())
}

#[turso_macros::test]
fn a_correlated_limit_applies_once_per_outer_call(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_parent_child_data(&conn, "CREATE INDEX child_parent ON child(parent_id)");
    let query = "SELECT p.id FROM parent p WHERE (
        SELECT c.id FROM child c WHERE c.id = p.id AND (
            SELECT count(*) FROM parent g WHERE g.id > c.parent_id
        ) > 0 LIMIT 1
    ) > 0";
    let plan = explain_query_plan(&conn, query)?;
    let grandchild = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["alias"] == "g")
        .unwrap();
    assert_eq!(
        grandchild["op"]["estimate"]["input_rows"].as_f64(),
        Some(100.0),
        "{plan}"
    );
    assert_eq!(limbo_exec_rows(&conn, query).len(), 99);
    Ok(())
}

#[turso_macros::test]
fn exists_costs_only_the_scan_until_its_first_match(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&conn, "CREATE TABLE outer_rows(k INTEGER)");
    limbo_exec_rows(&conn, "CREATE TABLE inner_rows(k INTEGER)");
    let outer = (0..16)
        .map(|k| format!("({k})"))
        .collect::<Vec<_>>()
        .join(",");
    let inner = (0..256)
        .map(|k| format!("({})", k % 32))
        .collect::<Vec<_>>()
        .join(",");
    limbo_exec_rows(&conn, &format!("INSERT INTO outer_rows VALUES {outer}"));
    limbo_exec_rows(&conn, &format!("INSERT INTO inner_rows VALUES {inner}"));
    limbo_exec_rows(&conn, "ANALYZE");
    let query = "SELECT o.k FROM outer_rows o WHERE EXISTS (
        SELECT 1 FROM inner_rows i WHERE i.k = o.k) ORDER BY o.k";
    let plan = explain_query_plan(&conn, query)?;
    let inner = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["alias"] == "i")
        .unwrap();
    assert_eq!(inner["op"]["type"], "scan", "{plan}");
    assert_eq!(limbo_exec_rows(&conn, query).len(), 16);

    let mut costs = Vec::new();
    for suffix in [
        "LIMIT 1",
        "LIMIT 8",
        "ORDER BY i.k % 32 LIMIT 1",
        "GROUP BY i.k LIMIT 1",
        "LIMIT 1 OFFSET 255",
    ] {
        let query = format!(
            "SELECT o.k FROM outer_rows o WHERE (SELECT i.k FROM inner_rows i
            WHERE i.k = o.k {suffix}) IS NOT NULL"
        );
        let plan = explain_query_plan(&conn, &query)?;
        let inner = plan["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|node| node["op"]["alias"] == "i")
            .unwrap();
        assert_eq!(inner["op"]["type"], "scan", "{query}: {plan}");
        costs.push(inner["op"]["estimate"]["access_cost"].as_f64().unwrap());
    }
    assert_eq!(costs[0], costs[1]);
    assert!(costs[1] < costs[2], "{costs:?}");
    assert_eq!(&costs[2..], &[costs[2]; 3]);

    let additional = (16..1024)
        .map(|k| format!("({})", k % 32))
        .collect::<Vec<_>>()
        .join(",");
    limbo_exec_rows(
        &conn,
        &format!("INSERT INTO outer_rows VALUES {additional}"),
    );
    limbo_exec_rows(&conn, "ANALYZE");
    let plan = explain_query_plan(&conn, query)?;
    let inner = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["alias"] == "i")
        .unwrap();
    assert_eq!(inner["op"]["type"], "search", "{plan}");
    assert_eq!(inner["op"]["index"]["ephemeral"], true, "{plan}");
    assert_eq!(limbo_exec_rows(&conn, query).len(), 1024);
    Ok(())
}

#[turso_macros::test]
fn logical_json_preserves_columns_and_bound_parameters(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let query = "SELECT name AS display_name, id + ?7 AS adjusted FROM users WHERE age IS NULL";
    let stmt = conn.prepare(format!("EXPLAIN QUERY PLAN FORMAT=JSON_LOGICAL {query}"))?;
    assert_eq!(stmt.num_columns(), 1);
    assert_eq!(stmt.get_column_name(0), "plan_json");
    assert_eq!(stmt.get_column_decltype(0).as_deref(), Some("TEXT"));
    let plan = explain_logical_plan(&conn, query)?;
    assert_eq!(plan["logical"]["version"], 1);
    assert_eq!(
        plan["result_columns"],
        serde_json::json!(["display_name", "adjusted"])
    );
    let before = &plan["logical"]["scopes"][0]["before"];
    assert_eq!(before["status"], "bound");
    assert_eq!(before["root"]["type"], "project");
    assert_eq!(
        before["root"]["inputs"][0]["predicates"][0]["expression"]["type"],
        "binary"
    );
    assert_eq!(
        before["root"]["inputs"][0]["predicates"][0]["expression"]["operator"],
        "IS"
    );
    assert_eq!(stmt.parameters_count(), 7);
    assert_eq!(
        before["root"]["expressions"][1]["scalar"]["expression"]["children"][1]["slot"],
        7
    );
    assert_eq!(explain_logical_plan(&conn, query)?, plan);
    assert!(explain_query_plan(&conn, query)?.get("logical").is_none());
    Ok(())
}

#[turso_macros::test]
fn logical_json_unnests_non_equality_and_disjunction(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "CREATE TABLE orders (user_id INTEGER, alternate INTEGER)",
    );
    for (negation, kind) in [("", "semi"), ("NOT ", "anti")] {
        let query = format!(
            "SELECT name FROM users u WHERE {negation}EXISTS (
            SELECT 1 FROM orders o WHERE o.user_id > u.id OR o.alternate IS u.age)"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["dependent_joins"], 1);
        assert_eq!(scope["after"]["dependent_joins"], 0);
        assert_eq!(
            scope["before"]["root"]["inputs"][0]["unnesting_rules"][0]["applicable"],
            true
        );
        assert_eq!(
            count_logical_nodes(&scope["before"]["root"], "dependent_join"),
            1
        );
        assert_eq!(
            count_logical_nodes(&scope["after"]["root"], "dependent_join"),
            0
        );
        let join = &scope["after"]["root"]["inputs"][0];
        assert_eq!(join["type"], "join");
        assert_eq!(join["kind"], kind);
        assert_eq!(join["outer_references"], serde_json::json!([]));
        assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
        assert_eq!(scope["after"]["rewrites"]["budget_exhausted"], false);
        assert_eq!(scope["selected"]["status"], "bound");
        assert!(plan["nodes"]
            .as_array()
            .is_some_and(|nodes| !nodes.is_empty()));
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_selected_projection_snapshot(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let plan = explain_logical_plan(&conn, "SELECT 1 AS answer")?;
    assert_eq!(
        plan["logical"]["scopes"][0]["selected"],
        serde_json::json!({
            "status": "bound", "bindings": [], "outer_references": [],
            "retained_parameters": [], "shared_inputs": [],
            "dependent_joins": 0, "dependency_declines": {},
            "root": {
                "id": 0, "type": "project", "outer_references": [],
                "output_columns": [{"relation": 1, "column": 0}],
                "expressions": [{
                    "output": {
                        "id": {"relation": 1, "column": 0}, "name": "answer",
                        "nullable": false, "affinity": "", "collation": "Unset"
                    },
                    "scalar": {
                        "affinity": "", "collation": "Unset", "nullable": false,
                        "can_fail": false, "volatile": false,
                        "expression": {"type": "literal", "sql": "1", "children": []}
                    }
                }],
                "inputs": [{"id": 1, "type": "one_row", "output_columns": [], "outer_references": [], "inputs": []}]
            }
        })
    );
    Ok(())
}

#[turso_macros::test]
fn logical_json_runs_generated_normalization_and_decorrelation(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(&conn, "CREATE TABLE orders(user_id)");
    let plan = explain_logical_plan(
        &conn,
        "SELECT a.name, b.name FROM users a JOIN users b ON a.age = b.age
        WHERE a.age > 0 AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id > a.id)",
    )?;
    let after = &plan["logical"]["scopes"][0]["after"];
    assert_eq!(
        after["rewrites"]["applied_rules"]["MergeSelectInnerJoin"],
        1
    );
    assert_eq!(after["rewrites"]["applied_rules"]["PullDependentFilter"], 1);
    assert_eq!(count_logical_nodes(&after["root"], "dependent_join"), 0);
    assert_eq!(count_logical_nodes(&after["root"], "filter"), 0);
    Ok(())
}

#[turso_macros::test]
fn logical_json_keeps_effectful_predicates_dependent(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(&conn, "CREATE TABLE orders (user_id INTEGER)");
    for predicate in [
        "o.user_id > u.id AND random() > 0",
        "CASE WHEN o.user_id > u.id THEN abs(-9223372036854775808) ELSE 0 END",
    ] {
        let query = format!(
            "SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE {predicate})"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let after = &plan["logical"]["scopes"][0]["after"];
        assert_eq!(after["rewrites"]["pull_dependent_filter"], 0);
        assert_eq!(count_logical_nodes(&after["root"], "dependent_join"), 1);
        assert_eq!(after["dependent_joins"], 1);
        assert_eq!(
            after["root"]["inputs"][0]["unnesting_rules"][0]["decline_reason"],
            "predicate_effects"
        );
        assert_eq!(
            after["dependency_declines"]["PullDependentFilter"]["predicate_effects"],
            1
        );
    }
    Ok(())
}

#[test]
fn logical_json_keeps_failing_generated_columns_dependent() -> anyhow::Result<()> {
    let db = TempDatabaseBuilder::new()
        .with_opts(turso_core::DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE TABLE parent(id)",
        "INSERT INTO parent VALUES(2)",
        "CREATE TABLE child(id, x)",
        "INSERT INTO child VALUES(1, -9223372036854775808)",
        "ALTER TABLE child ADD COLUMN g AS (abs(x))",
    ] {
        limbo_exec_rows(&conn, sql);
    }
    let query = "SELECT id FROM parent WHERE EXISTS (
        SELECT 1 FROM child WHERE child.id > parent.id AND child.g > 0)";
    let plan = explain_logical_plan(&conn, query)?;
    let after = &plan["logical"]["scopes"][0]["after"];
    assert_eq!(after["rewrites"]["pull_dependent_filter"], 0);
    assert_eq!(count_logical_nodes(&after["root"], "dependent_join"), 1);
    assert!(limbo_exec_rows(&conn, query).is_empty());
    Ok(())
}

#[turso_macros::test]
fn logical_json_identifies_unnesting_preconditions(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (query, phase, rule, reason) in [
        (
            "SELECT u.id FROM users u WHERE EXISTS (
                SELECT 1 FROM users v WHERE v.id > u.id LIMIT 1 OFFSET 1)",
            "after",
            "PullDependentFilter",
            "right_input_shape",
        ),
        (
            "SELECT a.id FROM users a WHERE EXISTS (
                SELECT 1 FROM users b WHERE EXISTS (SELECT 1 FROM users c WHERE c.id > a.id))",
            "after",
            "PullDependentFilter",
            "unavailable_columns",
        ),
        (
            "SELECT u.id FROM users u WHERE NOT EXISTS (
                SELECT 1 FROM users v WHERE u.age > 0 AND v.id > u.id)",
            "after",
            "PullDependentFilter",
            "anti_predicate_placement",
        ),
        (
            "SELECT u.id FROM users u WHERE NOT EXISTS (
                SELECT 1 FROM users v JOIN users w ON w.id = v.id
                WHERE u.age > 0 AND v.id > u.id)",
            "before",
            "PullDependentFilterOverJoin",
            "anti_predicate_placement",
        ),
    ] {
        let plan = explain_logical_plan(&conn, query)?;
        let logical = &plan["logical"]["scopes"][0][phase];
        assert_eq!(
            logical["dependency_declines"][rule][reason], 1,
            "{query}: {logical}"
        );
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_keeps_parameters_from_an_ignored_exists_projection(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(&conn, "CREATE TABLE orders (user_id INTEGER)");
    let query =
        "SELECT name FROM users u WHERE EXISTS (SELECT ?7 FROM orders o WHERE o.user_id > u.id)";
    let stmt = conn.prepare(query)?;
    assert_eq!(stmt.parameters_count(), 7);
    assert_eq!(stmt.get_column_name(0), "name");
    assert_eq!(stmt.get_column_decltype(0).as_deref(), Some("TEXT"));
    let plan = explain_logical_plan(&conn, query)?;
    for phase in ["before", "after", "selected"] {
        assert_eq!(
            plan["logical"]["scopes"][0][phase]["retained_parameters"],
            serde_json::json!([7]),
            "{phase}"
        );
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_does_not_move_custom_collations(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    conn.register_external_collation("callback".to_owned(), 0, equal_collation, None);
    let query = "SELECT name FROM users u WHERE EXISTS (
        SELECT 1 FROM users v WHERE (v.id > u.id COLLATE binary) OR (v.name = u.name COLLATE callback))";
    let plan = explain_logical_plan(&conn, query)?;
    let after = &plan["logical"]["scopes"][0]["after"];
    assert_eq!(after["rewrites"]["pull_dependent_filter"], 0);
    assert_eq!(count_logical_nodes(&after["root"], "dependent_join"), 1);
    Ok(())
}

unsafe extern "C" fn equal_collation(
    _: usize,
    _: *const u8,
    _: usize,
    _: *const u8,
    _: usize,
) -> i32 {
    0
}

#[turso_macros::test]
fn logical_json_represents_aggregate_inputs_and_outputs(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (query, grouped, dependencies) in [
        ("SELECT count(*) AS n FROM users", false, 0),
        (
            "SELECT name, count(*) AS n FROM users u WHERE EXISTS (
                SELECT 1 FROM users v WHERE v.id > u.id
             ) GROUP BY name HAVING count(*) > 0",
            true,
            1,
        ),
    ] {
        let plan = explain_logical_plan(&conn, query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["status"], "bound", "{query}: {scope}");
        assert_eq!(scope["before"]["dependent_joins"], dependencies);
        assert_eq!(scope["after"]["dependent_joins"], 0);
        for phase in ["before", "after"] {
            let root = &scope[phase]["root"];
            assert_eq!(root["type"], "aggregate");
            assert_eq!(root["grouped"], grouped);
            assert_eq!(root["empty_input_row"], !grouped);
            assert_eq!(root["aggregates"].as_array().unwrap().len(), 1);
            assert_eq!(root["aggregates"][0]["function"], "count");
            assert_eq!(
                root["group_keys"].as_array().unwrap().len(),
                usize::from(grouped)
            );
            assert_eq!(
                root["having"].as_array().unwrap().len(),
                usize::from(grouped)
            );
            assert_eq!(
                root["expressions"].as_array().unwrap().len(),
                if grouped { 2 } else { 1 }
            );
        }
        assert!(plan["nodes"]
            .as_array()
            .is_some_and(|nodes| !nodes.is_empty()));
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_preserves_aggregate_modifiers_and_shared_outputs(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let query = "WITH shared AS MATERIALIZED (
        SELECT name, count(DISTINCT age) AS n,
               sum(age) FILTER (WHERE age > 10) AS total
        FROM users u WHERE EXISTS (SELECT ?7 FROM users v WHERE v.id > u.id)
        GROUP BY name
    ) SELECT a.name, a.n, a.total, b.n FROM shared a JOIN shared b ON a.name IS b.name";
    let statement = conn.prepare(query)?;
    assert_eq!(statement.parameters_count(), 7);
    assert_eq!(statement.get_column_name(0), "name");
    assert_eq!(statement.get_column_name(1), "n");
    assert_eq!(statement.get_column_decltype(0).as_deref(), Some("TEXT"));
    let plan = explain_logical_plan(&conn, query)?;
    let scope = plan["logical"]["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| {
            scope["before"]["shared_inputs"]
                .as_array()
                .is_some_and(|inputs| !inputs.is_empty())
        })
        .expect("the aggregate shared producer has a logical representation");
    let after = &scope["after"];
    assert_eq!(after["dependent_joins"], 0);
    assert_eq!(after["retained_parameters"], serde_json::json!([7]));
    assert_eq!(after["shared_inputs"].as_array().unwrap().len(), 1);
    let root = &after["shared_inputs"][0]["root"];
    assert_eq!(root["type"], "aggregate");
    assert_eq!(root["aggregates"][0]["function"], "count");
    assert_eq!(root["aggregates"][0]["distinct"], true);
    assert_eq!(root["aggregates"][1]["function"], "sum");
    assert!(root["aggregates"][1]["filter"].is_object());
    assert_eq!(root["expressions"].as_array().unwrap().len(), 3);
    assert_eq!(plan["cte_materializations"].as_array().unwrap().len(), 1);
    Ok(())
}

#[turso_macros::test]
fn logical_json_keeps_unmigrated_aggregate_evaluation_explicit(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (query, reason) in [
        (
            "SELECT count(*) FROM users GROUP BY name ORDER BY name",
            "aggregate ordering references an unprojected expression",
        ),
        (
            "SELECT abs(min(age)) FROM users LIMIT 1",
            "effectful projection across sort or limit",
        ),
    ] {
        let plan = explain_logical_plan(&conn, query)?;
        let before = &plan["logical"]["scopes"][0]["before"];
        assert_eq!(before["status"], "legacy", "{query}: {before}");
        assert_eq!(before["reason"], reason);
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_distinct_aggregate_precedes_order_and_limit(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (query, grouped, dependencies, limited) in [
        ("SELECT DISTINCT count(*) AS n FROM users", false, 0, false),
        (
            "SELECT DISTINCT count(*) AS n FROM users u
             WHERE EXISTS (SELECT ?7 FROM users v WHERE v.id > u.id)
             GROUP BY name ORDER BY n DESC LIMIT 1 OFFSET 1",
            true,
            1,
            true,
        ),
    ] {
        let plan = explain_logical_plan(&conn, query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["status"], "bound", "{query}: {scope}");
        assert_eq!(scope["before"]["dependent_joins"], dependencies);
        assert_eq!(scope["after"]["dependent_joins"], 0);
        for phase in ["before", "after"] {
            let mut root = &scope[phase]["root"];
            if limited {
                assert_eq!(root["type"], "limit");
                root = &root["inputs"][0];
                assert_eq!(root["type"], "sort");
                root = &root["inputs"][0];
            }
            assert_eq!(root["type"], "distinct");
            let aggregate = &root["inputs"][0];
            assert_eq!(aggregate["type"], "aggregate");
            assert_eq!(aggregate["grouped"], grouped);
            assert_eq!(aggregate["empty_input_row"], !grouped);
            assert_eq!(aggregate["aggregates"][0]["function"], "count");
        }
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_rewrites_filters_inside_dependent_aggregates(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for suffix in [
        "",
        " HAVING count(*) = 0",
        " GROUP BY name HAVING count(*) > 1",
        " LIMIT 1 OFFSET 1",
    ] {
        let query = format!(
            "SELECT u.id FROM users u WHERE EXISTS (
                SELECT count(*) FROM users v WHERE v.id > u.id
                AND EXISTS (SELECT ?7 FROM users w WHERE w.id > v.id){suffix}
             )"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["status"], "bound", "{query}: {scope}");
        assert_eq!(scope["before"]["dependent_joins"], 2);
        let after = &scope["after"];
        assert_eq!(after["dependent_joins"], 1);
        assert_eq!(after["retained_parameters"], serde_json::json!([7]));
        assert_eq!(count_logical_nodes(&after["root"], "aggregate"), 1);
        assert_eq!(after["rewrites"]["pull_dependent_filter"], 1);
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_compound_results_have_positional_outputs(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (operator, name) in [
        ("UNION ALL", "union_all"),
        ("UNION", "union"),
        ("INTERSECT", "intersect"),
        ("EXCEPT", "except"),
    ] {
        let query = format!(
            "SELECT name AS label FROM users u
             WHERE EXISTS (SELECT ?7 FROM users v WHERE v.id > u.id)
             {operator} SELECT name FROM users WHERE age > 10
             ORDER BY label DESC NULLS LAST LIMIT 2 OFFSET 1"
        );
        let statement = conn.prepare(&query)?;
        assert_eq!(statement.parameters_count(), 7);
        assert_eq!(statement.get_column_name(0), "label");
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["status"], "bound", "{query}: {scope}");
        assert_eq!(scope["before"]["dependent_joins"], 1);
        assert_eq!(scope["after"]["dependent_joins"], 0);
        for phase in ["before", "after"] {
            let limit = &scope[phase]["root"];
            assert_eq!(limit["type"], "limit");
            let sort = &limit["inputs"][0];
            assert_eq!(sort["type"], "sort");
            let set = &sort["inputs"][0];
            assert_eq!(set["type"], "set");
            assert_eq!(set["operation"], name);
            assert_eq!(set["output_columns"].as_array().unwrap().len(), 1);
            assert_eq!(set["inputs"].as_array().unwrap().len(), 2);
            for input in set["inputs"].as_array().unwrap() {
                assert_eq!(input["output_columns"].as_array().unwrap().len(), 1);
                assert_ne!(input["output_columns"], set["output_columns"]);
            }
        }
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_compound_comparisons_use_the_whole_query(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let query = "SELECT 'A' AS label INTERSECT SELECT 'a'
        UNION ALL SELECT 'B' COLLATE NOCASE ORDER BY 1 COLLATE BINARY";
    let plan = explain_logical_plan(&conn, query)?;
    for phase in ["before", "after"] {
        let sort = &plan["logical"]["scopes"][0][phase]["root"];
        assert_eq!(sort["type"], "sort");
        assert_eq!(sort["keys"][0]["scalar"]["collation"], "Binary");
        let set = &sort["inputs"][0];
        assert_eq!(set["operation"], "union_all");
        assert_eq!(set["comparison_collations"], serde_json::json!(["NoCase"]));
        assert_eq!(set["columns"][0]["collation"], "Unset");
        let left = &set["inputs"][0];
        assert_eq!(left["operation"], "intersect");
        assert_eq!(left["comparison_collations"], serde_json::json!(["NoCase"]));
    }
    assert_eq!(
        limbo_exec_rows(&conn, query),
        vec![
            vec![Value::Text("A".to_owned())],
            vec![Value::Text("B".to_owned())]
        ]
    );
    Ok(())
}

#[turso_macros::test]
fn logical_json_lowers_compound_shared_producers(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    );
    for (operator, expected) in [
        ("UNION ALL", vec![2, 3]),
        ("UNION", vec![2, 3]),
        ("INTERSECT", vec![2]),
        ("EXCEPT", vec![1]),
    ] {
        let query = format!(
            "WITH eligible AS MATERIALIZED (
                SELECT u.id AS value FROM users u
                WHERE EXISTS (SELECT ?7 FROM users v WHERE v.id > u.id)
                {operator} SELECT id FROM users WHERE id >= 2
                ORDER BY 1 DESC LIMIT 2 OFFSET 0
            ) SELECT u.id FROM users u WHERE EXISTS (
                SELECT 1 FROM eligible a JOIN eligible b ON a.value = b.value
                WHERE a.value = u.id
            ) ORDER BY u.id"
        );
        assert_eq!(conn.prepare(&query)?.parameters_count(), 7);
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["status"], "bound", "{operator}: {plan}");
        assert_eq!(scope["before"]["dependent_joins"], 2);
        assert_eq!(scope["after"]["dependent_joins"], 1);
        assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
        assert_eq!(scope["after"]["shared_inputs"].as_array().unwrap().len(), 1);
        let producer = &scope["after"]["shared_inputs"][0]["root"];
        assert_eq!(count_logical_nodes(producer, "set"), 1);
        assert_eq!(count_logical_nodes(producer, "dependent_join"), 0);
        assert_eq!(
            count_logical_nodes(&scope["after"]["root"], "shared_ref"),
            2
        );
        assert_eq!(plan["cte_materializations"].as_array().unwrap().len(), 1);
        assert_eq!(
            limbo_exec_rows(&conn, &query),
            expected
                .into_iter()
                .map(|id| vec![Value::Integer(id)])
                .collect::<Vec<_>>(),
            "{operator}"
        );
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_distinct_follows_projection_and_precedes_limit(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (suffix, sorted, limited) in [
        ("", false, false),
        (" LIMIT 2 OFFSET 1", false, true),
        (" ORDER BY name DESC", true, false),
        (" ORDER BY name DESC LIMIT 2 OFFSET 1", true, true),
    ] {
        let query = format!(
            "SELECT DISTINCT name AS display_name FROM users u
             WHERE EXISTS (SELECT ?7 FROM users v WHERE v.id > u.id){suffix}"
        );
        let statement = conn.prepare(&query)?;
        assert_eq!(statement.parameters_count(), 7);
        assert_eq!(statement.get_column_name(0), "display_name");
        assert_eq!(statement.get_column_decltype(0).as_deref(), Some("TEXT"));
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        for (phase, dependencies) in [("before", 1), ("after", 0)] {
            let logical = &scope[phase];
            assert_eq!(logical["status"], "bound", "{query}: {logical}");
            assert_eq!(logical["dependent_joins"], dependencies);
            assert_eq!(logical["retained_parameters"], serde_json::json!([7]));
            let mut distinct = &logical["root"];
            if limited {
                assert_eq!(logical["root"]["type"], "limit");
                distinct = &distinct["inputs"][0];
            }
            if sorted {
                assert_eq!(distinct["type"], "sort");
                distinct = &distinct["inputs"][0];
            }
            assert_eq!(distinct["type"], "distinct");
            assert_eq!(distinct["inputs"][0]["type"], "project");
            assert_eq!(
                distinct["output_columns"],
                distinct["inputs"][0]["output_columns"]
            );
        }
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_reports_distinct_ordering_that_still_needs_a_legacy_path(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (ordering, reason) in [
        (
            "name",
            "DISTINCT ordering references an unprojected expression",
        ),
        ("random()", "effectful DISTINCT ordering"),
    ] {
        let query = format!(
            "SELECT DISTINCT age FROM users u WHERE EXISTS (
                SELECT 1 FROM users v WHERE v.id > u.id
            ) ORDER BY {ordering}"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        assert_eq!(plan["logical"]["scopes"][0]["before"]["status"], "legacy");
        assert_eq!(plan["logical"]["scopes"][0]["before"]["reason"], reason);
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_rewrites_a_distinct_shared_producer(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'one', 10), (3, 'two', 20)",
    );
    let query = "WITH shared AS MATERIALIZED (
        SELECT DISTINCT name FROM users u
        WHERE EXISTS (SELECT 1 FROM users v WHERE v.id > u.id)
    ) SELECT a.name, b.name FROM shared a CROSS JOIN shared b";
    assert_eq!(
        limbo_exec_rows(&conn, query),
        vec![vec![
            Value::Text("one".to_owned()),
            Value::Text("one".to_owned())
        ]]
    );
    let plan = explain_logical_plan(&conn, query)?;
    let scope = plan["logical"]["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| {
            scope["before"]["shared_inputs"]
                .as_array()
                .is_some_and(|inputs| !inputs.is_empty())
        })
        .expect("the shared producer has a logical representation");
    assert_eq!(scope["after"]["dependent_joins"], 0);
    assert_eq!(scope["after"]["shared_inputs"].as_array().unwrap().len(), 1);
    assert_eq!(
        scope["after"]["shared_inputs"][0]["root"]["type"],
        "distinct"
    );
    assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
    assert_eq!(plan["cte_materializations"].as_array().unwrap().len(), 1);
    Ok(())
}

#[turso_macros::test]
fn logical_json_reuses_one_cte_producer_in_a_rewritten_filter(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    );
    let query = "WITH shared AS MATERIALIZED (SELECT id, name FROM users)
        SELECT a.name, b.name FROM shared a JOIN shared b ON a.id = b.id
        WHERE EXISTS (SELECT 1 FROM users u WHERE u.id > a.id) ORDER BY a.id";
    assert_eq!(
        limbo_exec_rows(&conn, query),
        vec![
            vec![Value::Text("one".to_owned()), Value::Text("one".to_owned())],
            vec![Value::Text("two".to_owned()), Value::Text("two".to_owned())],
        ]
    );
    let plan = explain_logical_plan(&conn, query)?;
    let scope = plan["logical"]["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| {
            scope["before"]["shared_inputs"]
                .as_array()
                .is_some_and(|inputs| !inputs.is_empty())
        })
        .expect("logical compilation includes the shared CTE");
    for phase in ["before", "after", "selected"] {
        assert_eq!(
            scope[phase]["shared_inputs"].as_array().unwrap().len(),
            1,
            "{phase}"
        );
        assert_eq!(
            count_logical_nodes(&scope[phase]["root"], "shared_ref"),
            2,
            "{phase}"
        );
    }
    assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
    assert_eq!(
        count_logical_nodes(&scope["after"]["root"], "dependent_join"),
        0
    );
    assert_eq!(plan["cte_materializations"].as_array().unwrap().len(), 1);
    Ok(())
}

#[turso_macros::test]
fn logical_json_rewrites_a_shared_producer_once(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let query = "WITH shared AS MATERIALIZED (
        SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM users v WHERE v.id > u.id)
    ) SELECT a.id FROM shared a JOIN shared b ON a.id = b.id";
    let plan = explain_logical_plan(&conn, query)?;
    let scope = plan["logical"]["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| {
            scope["before"]["shared_inputs"]
                .as_array()
                .is_some_and(|inputs| !inputs.is_empty())
        })
        .unwrap();
    for (phase, dependencies) in [("before", 1), ("after", 0)] {
        let logical = &scope[phase];
        assert_eq!(count_logical_nodes(&logical["root"], "shared_ref"), 2);
        assert_eq!(
            logical["dependent_joins"], dependencies,
            "{phase}: {logical}"
        );
        assert_eq!(
            count_logical_nodes(&logical["shared_inputs"][0]["root"], "dependent_join"),
            dependencies
        );
    }
    assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
    Ok(())
}

#[turso_macros::test]
fn logical_json_lowers_rewritten_shared_producers(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    );
    let query = "WITH eligible AS MATERIALIZED (
        SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM users v WHERE v.id > u.id)
    ) SELECT u.id FROM users u WHERE EXISTS (
        SELECT 1 FROM eligible a JOIN eligible b ON a.id = b.id WHERE a.id = u.id
    ) ORDER BY u.id";
    let plan = explain_logical_plan(&conn, query)?;
    let scope = plan["logical"]["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| {
            scope["before"]["shared_inputs"]
                .as_array()
                .is_some_and(|inputs| !inputs.is_empty())
                && scope["before"]["dependent_joins"] == 2
        })
        .expect("both dependencies share a logical compilation scope");
    assert_eq!(scope["after"]["dependent_joins"], 0, "{plan}");
    assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 2);
    assert_eq!(plan["cte_materializations"].as_array().unwrap().len(), 1);
    assert_eq!(
        limbo_exec_rows(&conn, query),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
    Ok(())
}

#[turso_macros::test]
fn logical_json_unnests_filters_over_independent_shared_inputs(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    );
    for (negated, expected) in [("", vec![1, 2]), ("NOT ", vec![3])] {
        let query = format!(
            "WITH shared AS MATERIALIZED (SELECT id FROM users)
             SELECT u.id FROM users u WHERE {negated}EXISTS (
                 SELECT 1 FROM shared s WHERE s.id > u.id
             ) ORDER BY u.id"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(scope["before"]["status"], "bound", "{plan}");
        assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
        assert_eq!(
            count_logical_nodes(&scope["after"]["root"], "dependent_join"),
            0
        );
        assert_eq!(
            count_logical_nodes(&scope["after"]["root"], "shared_ref"),
            1
        );
        assert_eq!(plan["cte_materializations"].as_array().unwrap().len(), 1);
        assert_eq!(
            limbo_exec_rows(&conn, &query),
            expected
                .into_iter()
                .map(|id| vec![Value::Integer(id)])
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_keeps_effectful_shared_inputs_dependent(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for expression in ["abs(age)", "random()"] {
        let query = format!(
            "WITH shared AS MATERIALIZED (SELECT {expression} AS id FROM users)
             SELECT u.id FROM users u
             WHERE EXISTS (SELECT 1 FROM shared s WHERE s.id > u.id)"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let after = &plan["logical"]["scopes"][0]["after"];
        assert_eq!(after["status"], "bound", "{expression}");
        assert_eq!(
            after["rewrites"]["pull_dependent_filter"], 0,
            "{expression}"
        );
        assert_eq!(count_logical_nodes(&after["root"], "dependent_join"), 1);
        assert_eq!(
            after["dependency_declines"]["PullDependentFilter"]["right_input_evaluation"], 1,
            "{expression}"
        );
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_rejects_a_shared_producer_that_uses_an_outer_row(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let query = "SELECT u.id FROM users u WHERE EXISTS (
        WITH shared AS MATERIALIZED (SELECT id FROM users WHERE id > u.id)
        SELECT 1 FROM shared
    )";
    let plan = explain_logical_plan(&conn, query)?;
    let before = &plan["logical"]["scopes"][0]["before"];
    assert_eq!(before["status"], "legacy");
    assert_eq!(before["reason"], "shared input in an outer query scope");
    Ok(())
}

#[turso_macros::test]
fn logical_json_unnests_a_joined_right_input(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    );
    limbo_exec_rows(&conn, "CREATE TABLE orders(user_id INTEGER)");
    limbo_exec_rows(&conn, "CREATE TABLE flags(user_id INTEGER)");
    limbo_exec_rows(&conn, "INSERT INTO orders VALUES (2), (3), (3), (NULL)");
    limbo_exec_rows(&conn, "INSERT INTO flags VALUES (3), (3), (NULL)");
    for (negated, expected) in [("", vec![1, 2]), ("NOT ", vec![3])] {
        let query = format!(
            "SELECT u.id FROM users u WHERE {negated}EXISTS (
            SELECT ?7 FROM orders o JOIN flags f ON f.user_id = o.user_id
            WHERE o.user_id > u.id
        ) ORDER BY u.id"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let scope = &plan["logical"]["scopes"][0];
        assert_eq!(
            count_logical_nodes(&scope["before"]["root"], "dependent_join"),
            1
        );
        assert_eq!(
            count_logical_nodes(&scope["after"]["root"], "dependent_join"),
            0,
            "{plan}"
        );
        assert_eq!(count_logical_nodes(&scope["after"]["root"], "subquery"), 1);
        assert_eq!(
            scope["after"]["rewrites"]["applied_rules"]["PullDependentFilterOverJoin"],
            1
        );
        assert_eq!(conn.prepare(&query)?.parameters_count(), 7);
        assert_eq!(
            limbo_exec_rows(&conn, &query),
            expected
                .into_iter()
                .map(|id| vec![Value::Integer(id)])
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_unnests_nested_filters(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for depth in [2, 4] {
        for (outer_negation, inner_negation) in
            [("", ""), ("", "NOT "), ("NOT ", ""), ("NOT ", "NOT ")]
        {
            let mut nested = None;
            for level in (1..=depth).rev() {
                let previous = if level == 1 {
                    "u".to_owned()
                } else {
                    format!("n{}", level - 1)
                };
                let tail = nested.map_or(String::new(), |query| format!(" AND {query}"));
                let negation = if level == 1 {
                    outer_negation
                } else {
                    inner_negation
                };
                nested = Some(format!(
                    "{negation}EXISTS (SELECT ?7 FROM users n{level} WHERE
                    (n{level}.age > {previous}.age OR n{level}.id = {previous}.id){tail})"
                ));
            }
            let query = format!("SELECT u.id FROM users u WHERE {}", nested.unwrap());
            assert_eq!(conn.prepare(&query)?.parameters_count(), 7);
            let plan = explain_logical_plan(&conn, &query)?;
            let scope = &plan["logical"]["scopes"][0];
            assert_eq!(scope["before"]["dependent_joins"], depth, "{query}");
            assert_eq!(scope["after"]["dependent_joins"], 0, "{query}: {scope}");
            assert_eq!(
                scope["after"]["rewrites"]["applied_rules"]["PullLeftFilter"],
                depth - 1
            );
            assert_eq!(
                scope["after"]["rewrites"]["applied_rules"]["PullDependentFilterOverJoin"],
                depth - 1
            );
        }
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_keeps_effectful_nested_filters_dependent(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for predicate in [
        "v.age > u.age AND random() > 0",
        "CASE WHEN v.age > u.age THEN abs(-9223372036854775808) ELSE 0 END",
    ] {
        let query = format!(
            "SELECT u.id FROM users u WHERE EXISTS (
                SELECT 1 FROM users v WHERE ({predicate})
                AND EXISTS (SELECT 1 FROM users w WHERE w.age > v.age))"
        );
        let plan = explain_logical_plan(&conn, &query)?;
        let after = &plan["logical"]["scopes"][0]["after"];
        assert_eq!(after["dependent_joins"], 2, "{predicate}: {after}");
        assert_eq!(after["rewrites"]["applied_rules"]["PullLeftFilter"], 0);
    }
    Ok(())
}

#[turso_macros::test]
fn logical_json_lowers_a_rewritten_derived_input(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "INSERT INTO users VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    );
    limbo_exec_rows(&conn, "CREATE TABLE orders(user_id)");
    limbo_exec_rows(&conn, "INSERT INTO orders VALUES (2), (3), (3)");
    let query = "SELECT d.id, d.display FROM (
        SELECT u.id, u.name AS display FROM users u
        WHERE EXISTS (SELECT ?7 FROM orders o WHERE o.user_id > u.id)
        ORDER BY u.id DESC LIMIT 1
    ) d WHERE EXISTS (SELECT 1 FROM orders p WHERE p.user_id > d.id)";
    let plan = explain_logical_plan(&conn, query)?;
    let scope = &plan["logical"]["scopes"][0];
    assert_eq!(scope["before"]["status"], "bound");
    assert_eq!(count_logical_nodes(&scope["before"]["root"], "subquery"), 1);
    assert_eq!(
        count_logical_nodes(&scope["before"]["root"], "dependent_join"),
        2
    );
    assert_eq!(
        count_logical_nodes(&scope["after"]["root"], "dependent_join"),
        1
    );
    assert_eq!(scope["after"]["rewrites"]["pull_dependent_filter"], 1);
    for phase in ["before", "after", "selected"] {
        let derived = &scope[phase]["root"]["inputs"][0]["inputs"][0];
        assert_eq!(derived["type"], "subquery", "{phase}");
        let limit = &derived["inputs"][0]["inputs"][0];
        assert_eq!(limit["type"], "limit", "{phase}");
        assert_eq!(limit["limit"]["expression"]["sql"], "1", "{phase}");
        assert!(scope[phase]["retained_parameters"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(7)));
    }
    let stmt = conn.prepare(query)?;
    assert_eq!(stmt.parameters_count(), 7);
    assert_eq!(stmt.get_column_name(1), "display");
    assert_eq!(stmt.get_column_decltype(1).as_deref(), Some("TEXT"));
    assert_eq!(
        limbo_exec_rows(&conn, query),
        vec![vec![Value::Integer(2), Value::Text("two".to_owned())]]
    );
    Ok(())
}

#[turso_macros::test]
fn logical_scalar_effects_cover_nested_calls_and_operators(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    for (expression, can_fail, volatile) in [
        ("id + age", false, false),
        ("name IS NULL", false, false),
        ("CASE WHEN age > 0 THEN id ELSE age END", false, false),
        ("CAST(age AS INTEGER)", true, false),
        ("name LIKE 'a%'", true, false),
        ("abs(age)", true, false),
        ("random()", true, true),
        (
            "CASE WHEN age > 0 THEN coalesce(age, random()) ELSE 0 END",
            true,
            true,
        ),
    ] {
        let plan = explain_logical_plan(&conn, &format!("SELECT {expression} FROM users"))?;
        let scalar = &plan["logical"]["scopes"][0]["before"]["root"]["expressions"][0]["scalar"];
        assert_eq!(scalar["can_fail"], can_fail, "{expression}");
        assert_eq!(scalar["volatile"], volatile, "{expression}");
    }
    Ok(())
}

fn explain_logical_plan(conn: &Arc<Connection>, query: &str) -> anyhow::Result<serde_json::Value> {
    let rows = limbo_exec_rows(
        conn,
        &format!("EXPLAIN QUERY PLAN FORMAT=JSON_LOGICAL {query}"),
    );
    let Value::Text(plan) = &rows[0][0] else {
        panic!("logical query plan must be text")
    };
    Ok(serde_json::from_str(plan)?)
}

fn count_logical_nodes(node: &serde_json::Value, kind: &str) -> usize {
    usize::from(node["type"] == kind)
        + node["inputs"].as_array().map_or(0, |inputs| {
            inputs
                .iter()
                .map(|input| count_logical_nodes(input, kind))
                .sum::<usize>()
        })
}

fn explain_query_plan(conn: &Arc<Connection>, query: &str) -> anyhow::Result<serde_json::Value> {
    let rows = limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN FORMAT=JSON {query}"));
    let Value::Text(plan) = &rows[0][0] else {
        panic!("query plan must be text");
    };
    Ok(serde_json::from_str(plan.as_str())?)
}

fn final_output_rows(plan: &serde_json::Value) -> f64 {
    plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|node| node["op"]["estimate"]["output_rows"].as_f64())
        .next_back()
        .unwrap()
}

fn create_parent_child_data(conn: &Arc<Connection>, index_sql: &str) {
    limbo_exec_rows(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    limbo_exec_rows(
        conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, kind INTEGER)",
    );
    limbo_exec_rows(conn, index_sql);

    let parent_values = (1..=100)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(",");
    let child_values = (1..=100)
        .map(|value| {
            let kind = if value <= 10 { 1 } else { 2 };
            format!("({value},{value},{kind})")
        })
        .collect::<Vec<_>>()
        .join(",");
    limbo_exec_rows(conn, &format!("INSERT INTO parent VALUES {parent_values}"));
    limbo_exec_rows(conn, &format!("INSERT INTO child VALUES {child_values}"));
    limbo_exec_rows(conn, "ANALYZE");
}
