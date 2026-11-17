use turso_core::namespace::NamespaceId;
use turso_core::schema::alter_table::{
    rewrite_select_table_refs, rewrite_view_sql_for_column_rename,
};
use turso_core::schema::view::ViewColumnSchema;
use turso_core::schema::BTreeTable;
use turso_core::schema::Schema;
use turso_parser::ast;

fn schema_with_tables(create_table_sqls: &[&str]) -> Schema {
    let mut schema = Schema::new();
    for (index, create_table_sql) in create_table_sqls.iter().enumerate() {
        let root_page = i64::try_from(index).expect("test table index should fit in i64") + 2;
        let table = BTreeTable::from_sql(create_table_sql, root_page)
            .expect("test CREATE TABLE should parse");
        schema
            .add_btree_table(NamespaceId::ROOT, std::sync::Arc::new(table))
            .expect("test table should be added to schema");
    }

    schema
}

fn schema_with_table(create_table_sql: &str) -> Schema {
    schema_with_tables(&[create_table_sql])
}

/// A schema with table `t (a)` in the root namespace and table
/// `t (b, c)` in namespace 1.
pub(crate) fn schema_with_a_table_t_in_two_namespaces() -> (Schema, NamespaceId) {
    let mut schema = schema_with_table("CREATE TABLE t (a)");
    let other = NamespaceId::new(1);
    schema.add_namespace(other, "other");
    let table =
        BTreeTable::from_sql("CREATE TABLE t (b, c)", 3).expect("test CREATE TABLE should parse");
    schema
        .add_btree_table(other, std::sync::Arc::new(table))
        .expect("test table should be added to schema");
    (schema, other)
}

pub(crate) fn select(sql: &str) -> ast::Select {
    let Some(ast::Cmd::Stmt(ast::Stmt::Select(select))) =
        turso_core::dialect::engine::parse_statement(sql).unwrap()
    else {
        panic!("expected SELECT: {sql}");
    };
    select
}

pub(crate) fn column_names(columns: &ViewColumnSchema) -> Vec<String> {
    columns
        .flat_columns()
        .unwrap()
        .iter()
        .map(|column| column.name.clone().unwrap())
        .collect()
}

#[test]
fn rename_column_rewrites_a_view_against_the_table_of_its_namespace() {
    let (schema, other) = schema_with_a_table_t_in_two_namespaces();
    let view_sql = "CREATE VIEW v AS SELECT b FROM t";
    let rewritten =
        rewrite_view_sql_for_column_rename(view_sql, &schema, other, "t", "main", "b", "x")
            .unwrap()
            .expect("view should be rewritten");
    assert!(
        rewritten.sql.contains("SELECT x FROM t"),
        "{}",
        rewritten.sql
    );
}

#[test]
fn test_rewrite_view_sql_select_table_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT s.x FROM (SELECT b AS x FROM t) AS s";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(rewritten.sql.contains("SELECT c AS x FROM t"));
}

#[test]
fn test_rewrite_view_sql_sub_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT s.b FROM (t) AS s";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(!rewritten.sql.contains("s.b"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_table_call_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT j.value FROM t JOIN json_each(json_array(t.b)) AS j";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(!rewritten.sql.contains("t.b"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_compound_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT b FROM t UNION ALL SELECT b FROM t ORDER BY b";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert_eq!(rewritten.sql.matches("SELECT c FROM t").count(), 2);
    assert!(!rewritten.sql.contains("ORDER BY b"), "{}", rewritten.sql);
    assert!(rewritten.sql.contains("ORDER BY c"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_cte_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS WITH cte AS (SELECT b FROM t) SELECT b FROM cte";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(
        rewritten.sql.contains("WITH cte AS (SELECT c FROM t)"),
        "{}",
        rewritten.sql
    );
    assert!(
        rewritten.sql.contains("SELECT c FROM cte"),
        "{}",
        rewritten.sql
    );
}

#[test]
fn test_rewrite_view_sql_cte_branch_with_explicit_columns() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS WITH cte(x) AS (SELECT b FROM t) SELECT x FROM cte";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(
        rewritten.sql.contains("WITH cte(x)") || rewritten.sql.contains("WITH cte (x)"),
        "{}",
        rewritten.sql
    );
    assert!(
        rewritten.sql.contains("AS (SELECT c FROM t)"),
        "{}",
        rewritten.sql
    );
    assert!(
        rewritten.sql.contains("SELECT x FROM cte"),
        "{}",
        rewritten.sql
    );
}

#[test]
fn test_rewrite_trigger_cmd_table_refs_cte_branch() {
    let sql = "CREATE TEMP TRIGGER trg AFTER INSERT ON temp.old BEGIN WITH cte AS (SELECT * FROM temp.old) SELECT * FROM cte; END";
    let cmd = turso_core::dialect::engine::parse_statement(sql)
        .expect("trigger SQL should parse")
        .expect("trigger SQL should produce a statement");
    let ast::Cmd::Stmt(ast::Stmt::CreateTrigger { commands, .. }) = cmd else {
        panic!("expected CREATE TRIGGER statement");
    };
    let mut commands = commands;
    let ast::TriggerCmd::Select(select) = &mut commands[0] else {
        panic!("expected SELECT trigger command");
    };

    rewrite_select_table_refs(select, "old", "new");

    let Some(with_clause) = &select.with else {
        panic!("expected WITH clause");
    };
    let ast::OneSelect::Select {
        from: Some(from), ..
    } = &with_clause.ctes[0].select.body.select
    else {
        panic!("expected CTE SELECT core");
    };
    let ast::SelectTable::Table(tbl_name, _, _) = from.select.as_ref() else {
        panic!("expected CTE SELECT FROM table");
    };

    assert_eq!(
        tbl_name.namespace.as_ref().map(ast::Name::as_str),
        Some("temp")
    );
    assert_eq!(tbl_name.name.as_str(), "new");
}

#[test]
fn test_rewrite_trigger_cmd_table_refs_expr_subquery_branch() {
    let sql = "CREATE TEMP TRIGGER trg AFTER INSERT ON temp.old BEGIN SELECT EXISTS(SELECT 1 FROM temp.old); END";
    let cmd = turso_core::dialect::engine::parse_statement(sql)
        .expect("trigger SQL should parse")
        .expect("trigger SQL should produce a statement");
    let ast::Cmd::Stmt(ast::Stmt::CreateTrigger { commands, .. }) = cmd else {
        panic!("expected CREATE TRIGGER statement");
    };
    let mut commands = commands;
    let ast::TriggerCmd::Select(select) = &mut commands[0] else {
        panic!("expected SELECT trigger command");
    };

    rewrite_select_table_refs(select, "old", "new");

    let ast::OneSelect::Select { columns, .. } = &select.body.select else {
        panic!("expected SELECT core");
    };
    let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
        panic!("expected expression result column");
    };
    let ast::Expr::Exists(subquery) = expr.as_ref() else {
        panic!("expected EXISTS expression");
    };
    let ast::OneSelect::Select {
        from: Some(from), ..
    } = &subquery.body.select
    else {
        panic!("expected EXISTS subquery FROM clause");
    };
    let ast::SelectTable::Table(tbl_name, _, _) = from.select.as_ref() else {
        panic!("expected EXISTS subquery FROM table");
    };

    assert_eq!(
        tbl_name.namespace.as_ref().map(ast::Name::as_str),
        Some("temp")
    );
    assert_eq!(tbl_name.name.as_str(), "new");
}

#[test]
fn test_rewrite_view_sql_join_on_branch() {
    let schema = schema_with_tables(&["CREATE TABLE t (a, b)", "CREATE TABLE u (b)"]);
    let view_sql = "CREATE VIEW v AS SELECT t.a FROM t JOIN u ON t.b = u.b";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(!rewritten.sql.contains("t.b"), "{}", rewritten.sql);
    assert!(rewritten.sql.contains("t.c = u.b"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_join_using_branch() {
    let schema = schema_with_tables(&["CREATE TABLE t (a, b)", "CREATE TABLE u (b)"]);
    let view_sql = "CREATE VIEW v AS SELECT t.a FROM t JOIN u USING (b)";

    // Renaming t.b breaks the USING join no matter what: u has no c, and
    // after the rename t has no b. SQLite refuses the ALTER, so the
    // rewrite must error rather than emit a view that can never be
    // queried again.
    let err = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("cannot join using column"),
        "{err}"
    );
}

#[test]
fn test_rewrite_view_sql_group_by_having_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT b FROM t GROUP BY b HAVING b > 0";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(
        rewritten
            .sql
            .contains("SELECT c FROM t GROUP BY c HAVING c > 0"),
        "{}",
        rewritten.sql
    );
}

#[test]
fn test_rewrite_view_sql_window_clause_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT sum(a) OVER (PARTITION BY b ORDER BY b) FROM t";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(
        !rewritten.sql.contains("PARTITION BY b"),
        "{}",
        rewritten.sql
    );
    assert!(!rewritten.sql.contains("ORDER BY b"), "{}", rewritten.sql);
    assert!(
        rewritten.sql.contains("PARTITION BY c"),
        "{}",
        rewritten.sql
    );
    assert!(rewritten.sql.contains("ORDER BY c"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_limit_offset_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT a FROM t LIMIT b OFFSET b";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(!rewritten.sql.contains("LIMIT b"), "{}", rewritten.sql);
    assert!(!rewritten.sql.contains("OFFSET b"), "{}", rewritten.sql);
    assert!(rewritten.sql.contains("LIMIT c"), "{}", rewritten.sql);
    assert!(rewritten.sql.contains("OFFSET c"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_values_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS VALUES ((SELECT b FROM t LIMIT 1))";

    let rewritten = rewrite_view_sql_for_column_rename(
        view_sql,
        &schema,
        NamespaceId::ROOT,
        "t",
        "main",
        "b",
        "c",
    )
    .unwrap()
    .expect("view should be rewritten");

    assert!(
        rewritten.sql.contains("VALUES ((SELECT c FROM t LIMIT 1))"),
        "{}",
        rewritten.sql
    );
}
