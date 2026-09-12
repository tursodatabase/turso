use super::*;
use crate::schema::{BTreeTable, Type as SchemaValueType};
use turso_parser::ast::{self, Expr, FunctionTail, Literal, Name, Operator::*, Type, Variable};
use turso_parser::parser::Parser;

#[test]
fn test_normalize_ident() {
    assert_eq!(normalize_ident("foo"), "foo");
    assert_eq!(normalize_ident("FOO"), "foo");
    // SQLite folds only ASCII; non-ASCII bytes pass through untouched.
    assert_eq!(normalize_ident("ὈΔΥΣΣΕΎΣ"), "ὈΔΥΣΣΕΎΣ");
    assert_eq!(normalize_ident("Foo_ΔΥΣ"), "foo_ΔΥΣ");
}

fn schema_with_tables(create_table_sqls: &[&str]) -> Schema {
    let mut schema = Schema::new();
    for (index, create_table_sql) in create_table_sqls.iter().enumerate() {
        let root_page = i64::try_from(index).expect("test table index should fit in i64") + 2;
        let table = BTreeTable::from_sql(create_table_sql, root_page)
            .expect("test CREATE TABLE should parse");
        schema
            .add_btree_table(std::sync::Arc::new(table))
            .expect("test table should be added to schema");
    }

    schema
}

fn schema_with_table(create_table_sql: &str) -> Schema {
    schema_with_tables(&[create_table_sql])
}

fn parse_select_from(sql: &str) -> Option<ast::FromClause> {
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser
        .next_cmd()
        .expect("test SQL should parse")
        .expect("test SQL should contain a statement");
    let ast::Cmd::Stmt(ast::Stmt::Select(select)) = cmd else {
        panic!("expected SELECT statement");
    };
    match select.body.select {
        ast::OneSelect::Select { from, .. } => from,
        _ => panic!("expected simple SELECT"),
    }
}

#[test]
fn test_rewrite_view_sql_select_table_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT s.x FROM (SELECT b AS x FROM t) AS s";

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
        .unwrap()
        .expect("view should be rewritten");

    assert!(rewritten.sql.contains("SELECT c AS x FROM t"));
}

#[test]
fn test_rewrite_view_sql_sub_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT s.b FROM (t) AS s";

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
        .unwrap()
        .expect("view should be rewritten");

    assert!(!rewritten.sql.contains("s.b"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_table_call_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT j.value FROM t JOIN json_each(json_array(t.b)) AS j";

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
        .unwrap()
        .expect("view should be rewritten");

    assert!(!rewritten.sql.contains("t.b"), "{}", rewritten.sql);
}

#[test]
fn test_rewrite_view_sql_compound_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT b FROM t UNION ALL SELECT b FROM t ORDER BY b";

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser
        .next_cmd()
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
        tbl_name.db_name.as_ref().map(ast::Name::as_str),
        Some("temp")
    );
    assert_eq!(tbl_name.name.as_str(), "new");
}

#[test]
fn test_from_clause_target_qualifiers_dedups_case_insensitively() {
    let from = parse_select_from("SELECT 1 FROM Target AS tgt, target AS TARGET");
    assert_eq!(
        from_clause_target_qualifiers(&from, "target"),
        vec!["target".to_string(), "tgt".to_string()]
    );
}

#[test]
fn test_extend_qualifiers_scoped_preserves_first_seen_order() {
    let mut qualifiers = vec!["target".to_string(), "outer_alias".to_string()];
    let local = vec![
        "outer_alias".to_string(),
        "local_alias".to_string(),
        "target".to_string(),
    ];

    let added = extend_qualifiers_scoped(&mut qualifiers, &local);
    assert_eq!(
        qualifiers,
        vec![
            "target".to_string(),
            "outer_alias".to_string(),
            "local_alias".to_string(),
        ]
    );
    assert_eq!(added, 1);
    qualifiers.truncate(qualifiers.len() - added);
    assert_eq!(
        qualifiers,
        vec!["target".to_string(), "outer_alias".to_string(),]
    );
}

#[test]
fn test_rewrite_trigger_cmd_table_refs_expr_subquery_branch() {
    let sql = "CREATE TEMP TRIGGER trg AFTER INSERT ON temp.old BEGIN SELECT EXISTS(SELECT 1 FROM temp.old); END";
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser
        .next_cmd()
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
        tbl_name.db_name.as_ref().map(ast::Name::as_str),
        Some("temp")
    );
    assert_eq!(tbl_name.name.as_str(), "new");
}

#[test]
fn test_rewrite_view_sql_join_on_branch() {
    let schema = schema_with_tables(&["CREATE TABLE t (a, b)", "CREATE TABLE u (b)"]);
    let view_sql = "CREATE VIEW v AS SELECT t.a FROM t JOIN u ON t.b = u.b";

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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
    let err =
        rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c").unwrap_err();
    assert!(
        err.to_string().contains("cannot join using column"),
        "{err}"
    );
}

#[test]
fn test_rewrite_view_sql_group_by_having_branch() {
    let schema = schema_with_table("CREATE TABLE t (a, b)");
    let view_sql = "CREATE VIEW v AS SELECT b FROM t GROUP BY b HAVING b > 0";

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
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

    let rewritten = rewrite_view_sql_for_column_rename(view_sql, &schema, "t", "main", "b", "c")
        .unwrap()
        .expect("view should be rewritten");

    assert!(
        rewritten.sql.contains("VALUES ((SELECT c FROM t LIMIT 1))"),
        "{}",
        rewritten.sql
    );
}

#[test]
fn test_indexed_variable_comparison() {
    let expr1 = Expr::Variable(Variable::indexed(1u32.try_into().unwrap()));
    let expr2 = Expr::Variable(Variable::indexed(1u32.try_into().unwrap()));
    assert!(exprs_are_equivalent(&expr1, &expr2));
}

#[test]
fn test_named_variable_comparison() {
    let expr1 = Expr::Variable(Variable::named(":a".to_string(), 1u32.try_into().unwrap()));
    let expr2 = Expr::Variable(Variable::named(":a".to_string(), 1u32.try_into().unwrap()));
    assert!(exprs_are_equivalent(&expr1, &expr2));

    let expr1 = Expr::Variable(Variable::named(":a".to_string(), 1u32.try_into().unwrap()));
    let expr2 = Expr::Variable(Variable::named(":b".to_string(), 2u32.try_into().unwrap()));
    assert!(!exprs_are_equivalent(&expr1, &expr2));
}

#[test]
fn test_basic_addition_exprs_are_equivalent() {
    let expr1 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
    );
    let expr2 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("389".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("826".to_string()))),
    );
    assert!(exprs_are_equivalent(&expr1, &expr2));
}

#[test]
fn test_addition_expressions_equivalent_normalized() {
    // Same types: 123.0 + 243.0 == 243.0 + 123.0 (commutative)
    let expr1 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
    );
    let expr2 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
    );
    assert!(exprs_are_equivalent(&expr1, &expr2));

    // Mixed types are NOT equivalent (different result types)
    let expr3 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("123.0".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("243".to_string()))),
    );
    let expr4 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("243.0".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
    );
    assert!(!exprs_are_equivalent(&expr3, &expr4));
}

#[test]
fn test_subtraction_expressions_not_equivalent() {
    let expr3 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
        Subtract,
        Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
    );
    let expr4 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
        Subtract,
        Box::new(Expr::Literal(Literal::Numeric("364".to_string()))),
    );
    assert!(!exprs_are_equivalent(&expr3, &expr4));
}

#[test]
fn test_subtraction_expressions_normalized() {
    // Same types: 66.0 - 22.0 == 66.0 - 22.0
    let expr3 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
        Subtract,
        Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
    );
    let expr4 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
        Subtract,
        Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
    );
    assert!(exprs_are_equivalent(&expr3, &expr4));

    // Mixed types are NOT equivalent
    let expr5 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("66.0".to_string()))),
        Subtract,
        Box::new(Expr::Literal(Literal::Numeric("22".to_string()))),
    );
    let expr6 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("66".to_string()))),
        Subtract,
        Box::new(Expr::Literal(Literal::Numeric("22.0".to_string()))),
    );
    assert!(!exprs_are_equivalent(&expr5, &expr6));
}

#[test]
fn test_expressions_equivalent_case_insensitive_functioncalls() {
    let func1 = Expr::FunctionCall {
        name: Name::exact("SUM".to_string()),
        distinctness: None,
        args: vec![Expr::Id(Name::exact("x".to_string())).into()],
        order_by: vec![],
        within_group: vec![],
        filter_over: FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    let func2 = Expr::FunctionCall {
        name: Name::exact("sum".to_string()),
        distinctness: None,
        args: vec![Expr::Id(Name::exact("x".to_string())).into()],
        order_by: vec![],
        within_group: vec![],
        filter_over: FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    assert!(exprs_are_equivalent(&func1, &func2));

    let func3 = Expr::FunctionCall {
        name: Name::exact("SUM".to_string()),
        distinctness: Some(ast::Distinctness::Distinct),
        args: vec![Expr::Id(Name::exact("x".to_string())).into()],
        order_by: vec![],
        within_group: vec![],
        filter_over: FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    assert!(!exprs_are_equivalent(&func1, &func3));
}

#[test]
fn test_expressions_equivalent_identical_fn_with_distinct() {
    let sum = Expr::FunctionCall {
        name: Name::exact("SUM".to_string()),
        distinctness: None,
        args: vec![Expr::Id(Name::exact("x".to_string())).into()],
        order_by: vec![],
        within_group: vec![],
        filter_over: FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    let sum_distinct = Expr::FunctionCall {
        name: Name::exact("SUM".to_string()),
        distinctness: Some(ast::Distinctness::Distinct),
        args: vec![Expr::Id(Name::exact("x".to_string())).into()],
        order_by: vec![],
        within_group: vec![],
        filter_over: FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    assert!(!exprs_are_equivalent(&sum, &sum_distinct));
}

#[test]
fn test_expressions_equivalent_multiplication() {
    // Same types: 42.0 * 38.0 == 38.0 * 42.0 (commutative)
    let expr1 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
        Multiply,
        Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
    );
    let expr2 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("38.0".to_string()))),
        Multiply,
        Box::new(Expr::Literal(Literal::Numeric("42.0".to_string()))),
    );
    assert!(exprs_are_equivalent(&expr1, &expr2));
}

#[test]
fn test_expressions_both_parenthesized_equivalent() {
    // Same types: (683 + 799) == 799 + 683 (commutative, integers only)
    let expr1 = Expr::Parenthesized(vec![Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("799".to_string()))),
    )
    .into()]);
    let expr2 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("799".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("683".to_string()))),
    );
    assert!(exprs_are_equivalent(&expr1, &expr2));
}
#[test]
fn test_expressions_parenthesized_equivalent() {
    let expr7 = Expr::Parenthesized(vec![Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
    )
    .into()]);
    let expr8 = Expr::Binary(
        Box::new(Expr::Literal(Literal::Numeric("6".to_string()))),
        Add,
        Box::new(Expr::Literal(Literal::Numeric("7".to_string()))),
    );
    assert!(exprs_are_equivalent(&expr7, &expr8));
}

#[test]
fn test_like_expressions_equivalent() {
    let expr1 = Expr::Like {
        lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
        not: false,
        op: ast::LikeOperator::Like,
        rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
        escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
    };
    let expr2 = Expr::Like {
        lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
        not: false,
        op: ast::LikeOperator::Like,
        rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
        escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
    };
    assert!(exprs_are_equivalent(&expr1, &expr2));
}

#[test]
fn test_expressions_equivalent_like_escaped() {
    let expr1 = Expr::Like {
        lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
        not: false,
        op: ast::LikeOperator::Like,
        rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
        escape: Some(Box::new(Expr::Literal(Literal::String("\\".to_string())))),
    };
    let expr2 = Expr::Like {
        lhs: Box::new(Expr::Id(Name::exact("name".to_string()))),
        not: false,
        op: ast::LikeOperator::Like,
        rhs: Box::new(Expr::Literal(Literal::String("%john%".to_string()))),
        escape: Some(Box::new(Expr::Literal(Literal::String("#".to_string())))),
    };
    assert!(!exprs_are_equivalent(&expr1, &expr2));
}
#[test]
fn test_expressions_equivalent_between() {
    let expr1 = Expr::Between {
        lhs: Box::new(Expr::Id(Name::exact("age".to_string()))),
        not: false,
        start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
        end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
    };
    let expr2 = Expr::Between {
        lhs: Box::new(Expr::Id(Name::exact("age".to_string()))),
        not: false,
        start: Box::new(Expr::Literal(Literal::Numeric("18".to_string()))),
        end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
    };
    assert!(exprs_are_equivalent(&expr1, &expr2));

    // differing BETWEEN bounds
    let expr3 = Expr::Between {
        lhs: Box::new(Expr::Id(Name::exact("age".to_string()))),
        not: false,
        start: Box::new(Expr::Literal(Literal::Numeric("20".to_string()))),
        end: Box::new(Expr::Literal(Literal::Numeric("65".to_string()))),
    };
    assert!(!exprs_are_equivalent(&expr1, &expr3));
}
#[test]
fn test_cast_exprs_equivalent() {
    let cast1 = Expr::Cast {
        expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
        type_name: Some(Type {
            name: "INTEGER".to_string(),
            size: None,
            array_dimensions: 0,
        }),
    };

    let cast2 = Expr::Cast {
        expr: Box::new(Expr::Literal(Literal::Numeric("123".to_string()))),
        type_name: Some(Type {
            name: "integer".to_string(),
            size: None,
            array_dimensions: 0,
        }),
    };
    assert!(exprs_are_equivalent(&cast1, &cast2));
}

#[test]
fn test_ident_equivalency() {
    assert!(check_ident_equivalency("\"foo\"", "foo"));
    assert!(check_ident_equivalency("[foo]", "foo"));
    assert!(check_ident_equivalency("`FOO`", "foo"));
    assert!(check_ident_equivalency("\"foo\"", "`FOO`"));
    assert!(!check_ident_equivalency("\"foo\"", "[bar]"));
    assert!(!check_ident_equivalency("foo", "\"bar\""));
}

#[test]
fn test_simple_uri() {
    let uri = "file:/home/user/db.sqlite";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.authority, None);
}

#[test]
fn test_uri_with_authority() {
    let uri = "file://localhost/home/user/db.sqlite";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.authority, Some("localhost"));
}

#[test]
fn test_uri_with_invalid_authority() {
    let uri = "file://example.com/home/user/db.sqlite";
    let result = OpenOptions::parse(uri);
    assert!(result.is_err());
}

#[test]
fn test_uri_with_query_params() {
    let uri = "file:/home/user/db.sqlite?vfs=unix&mode=ro&immutable=1";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.vfs, Some("unix".to_string()));
    assert_eq!(opts.mode, OpenMode::ReadOnly);
    assert!(opts.immutable);
}

#[test]
fn test_uri_with_fragment() {
    let uri = "file:/home/user/db.sqlite#section1";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
}

#[test]
fn test_uri_with_percent_encoding() {
    let uri = "file:/home/user/db%20with%20spaces.sqlite?vfs=unix";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db with spaces.sqlite");
    assert_eq!(opts.vfs, Some("unix".to_string()));
}

#[test]
fn test_uri_without_scheme() {
    let uri = "/home/user/db.sqlite";
    let result = OpenOptions::parse(uri);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().path, "/home/user/db.sqlite");
}

#[test]
fn test_uri_with_empty_query() {
    let uri = "file:/home/user/db.sqlite?";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.vfs, None);
}

#[test]
fn test_uri_with_partial_query() {
    let uri = "file:/home/user/db.sqlite?mode=rw";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.mode, OpenMode::ReadWrite);
    assert_eq!(opts.vfs, None);
}

#[test]
fn test_uri_windows_style_path() {
    let uri = "file:///C:/Users/test/db.sqlite";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/C:/Users/test/db.sqlite");
}

#[test]
fn test_uri_with_only_query_params() {
    let uri = "file:?mode=memory&cache=shared";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "");
    assert_eq!(opts.mode, OpenMode::Memory);
    assert_eq!(opts.cache, CacheMode::Shared);
}

#[test]
fn test_uri_with_only_fragment() {
    let uri = "file:#fragment";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "");
}

#[test]
fn test_uri_with_invalid_scheme() {
    let uri = "http:/home/user/db.sqlite";
    let result = OpenOptions::parse(uri);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().path, "http:/home/user/db.sqlite");
}

#[test]
fn test_uri_with_multiple_query_params() {
    let uri = "file:/home/user/db.sqlite?vfs=unix&mode=rw&cache=private&immutable=0";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.vfs, Some("unix".to_string()));
    assert_eq!(opts.mode, OpenMode::ReadWrite);
    assert_eq!(opts.cache, CacheMode::Private);
    assert!(!opts.immutable);
}

#[test]
fn test_uri_with_unknown_query_param() {
    let uri = "file:/home/user/db.sqlite?unknown=param";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.vfs, None);
}

#[test]
fn test_uri_with_multiple_equal_signs() {
    let uri = "file:/home/user/db.sqlite?vfs=unix=custom";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.vfs, Some("unix=custom".to_string()));
}

#[test]
fn test_uri_with_trailing_slash() {
    let uri = "file:/home/user/db.sqlite/";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite/");
}

#[test]
fn test_uri_with_encoded_characters_in_query() {
    let uri = "file:/home/user/db.sqlite?vfs=unix%20mode";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/user/db.sqlite");
    assert_eq!(opts.vfs, Some("unix mode".to_string()));
}

#[test]
fn test_uri_windows_network_path() {
    let uri = "file://server/share/db.sqlite";
    let result = OpenOptions::parse(uri);
    assert!(result.is_err()); // non-localhost authority should fail
}

#[test]
fn test_uri_windows_drive_letter_with_slash() {
    let uri = "file:///C:/database.sqlite";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/C:/database.sqlite");
}

#[test]
fn test_localhost_with_double_slash_and_no_path() {
    let uri = "file://localhost";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "");
    assert_eq!(opts.authority, Some("localhost"));
}

#[test]
fn test_uri_windows_drive_letter_without_slash() {
    let uri = "file:///C:/database.sqlite";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/C:/database.sqlite");
}

#[test]
fn test_improper_mode() {
    // any other mode but ro, rwc, rw, memory should fail per sqlite

    let uri = "file:data.db?mode=readonly";
    let res = OpenOptions::parse(uri);
    assert!(res.is_err());
    // including empty
    let uri = "file:/home/user/db.sqlite?vfs=&mode=";
    let res = OpenOptions::parse(uri);
    assert!(res.is_err());
}

// Some examples from https://www.sqlite.org/c3ref/open.html#urifilenameexamples
#[test]
fn test_simple_file_current_dir() {
    let uri = "file:data.db";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "data.db");
    assert_eq!(opts.authority, None);
    assert_eq!(opts.vfs, None);
    assert_eq!(opts.mode, OpenMode::ReadWriteCreate);
}

#[test]
fn test_simple_file_three_slash() {
    let uri = "file:///home/data/data.db";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/data/data.db");
    assert_eq!(opts.authority, None);
    assert_eq!(opts.vfs, None);
    assert_eq!(opts.mode, OpenMode::ReadWriteCreate);
}

#[test]
fn test_simple_file_two_slash_localhost() {
    let uri = "file://localhost/home/fred/data.db";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/home/fred/data.db");
    assert_eq!(opts.authority, Some("localhost"));
    assert_eq!(opts.vfs, None);
}

#[test]
fn test_windows_double_invalid() {
    let uri = "file://C:/home/fred/data.db?mode=ro";
    let opts = OpenOptions::parse(uri);
    assert!(opts.is_err());
}

#[test]
fn test_simple_file_two_slash() {
    let uri = "file:///C:/Documents%20and%20Settings/fred/Desktop/data.db";
    let opts = OpenOptions::parse(uri).unwrap();
    assert_eq!(opts.path, "/C:/Documents and Settings/fred/Desktop/data.db");
    assert_eq!(opts.vfs, None);
}

#[test]
fn test_decode_percent_basic() {
    assert_eq!(decode_percent("hello%20world"), "hello world");
    assert_eq!(decode_percent("file%3Adata.db"), "file:data.db");
    assert_eq!(decode_percent("path%2Fto%2Ffile"), "path/to/file");
}

#[test]
fn test_decode_percent_edge_cases() {
    assert_eq!(decode_percent(""), "");
    assert_eq!(decode_percent("plain_text"), "plain_text");
    assert_eq!(
        decode_percent("%2Fhome%2Fuser%2Fdb.sqlite"),
        "/home/user/db.sqlite"
    );
    // multiple percent-encoded characters in sequence
    assert_eq!(decode_percent("%41%42%43"), "ABC");
    assert_eq!(decode_percent("%61%62%63"), "abc");
}

#[test]
fn test_decode_percent_invalid_sequences() {
    // invalid percent encoding (single % without two hex digits)
    assert_eq!(decode_percent("hello%"), "hello%");
    // only one hex digit after %
    assert_eq!(decode_percent("file%2"), "file%2");
    // invalid hex digits (not 0-9, A-F, a-f)
    assert_eq!(decode_percent("file%2X.db"), "file%2X.db");

    // Incomplete sequence at the end, leave untouched
    assert_eq!(decode_percent("path%2Fto%2"), "path/to%2");
}

#[test]
fn test_decode_percent_mixed_valid_invalid() {
    assert_eq!(decode_percent("hello%20world%"), "hello world%");
    assert_eq!(decode_percent("%2Fpath%2Xto%2Ffile"), "/path%2Xto/file");
    assert_eq!(decode_percent("file%3Adata.db%2"), "file:data.db%2");
}

#[test]
fn test_decode_percent_special_characters() {
    assert_eq!(
        decode_percent("%21%40%23%24%25%5E%26%2A%28%29"),
        "!@#$%^&*()"
    );
    assert_eq!(decode_percent("%5B%5D%7B%7D%7C%5C%3A"), "[]{}|\\:");
}

#[test]
fn test_decode_percent_unmodified_valid_text() {
    // ensure already valid text remains unchanged
    assert_eq!(
        decode_percent("C:/Users/Example/Database.sqlite"),
        "C:/Users/Example/Database.sqlite"
    );
    assert_eq!(
        decode_percent("/home/user/db.sqlite"),
        "/home/user/db.sqlite"
    );
}

#[test]
fn test_text_to_integer() {
    assert_eq!(
        checked_cast_text_to_numeric("1", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1823400-00000", false).unwrap(),
        Value::from_i64(1823400)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-10000000", false).unwrap(),
        Value::from_i64(-10000000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("123xxx", false).unwrap(),
        Value::from_i64(123)
    );
    assert_eq!(
        checked_cast_text_to_numeric("9223372036854775807", false).unwrap(),
        Value::from_i64(i64::MAX)
    );
    // Overflow becomes Float (different from cast_text_to_integer which returned 0)
    assert_eq!(
        checked_cast_text_to_numeric("9223372036854775808", false).unwrap(),
        Value::from_f64(9.22337203685478e18)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-9223372036854775808", false).unwrap(),
        Value::from_i64(i64::MIN)
    );
    // Overflow becomes Float (different from cast_text_to_integer which returned 0)
    assert_eq!(
        checked_cast_text_to_numeric("-9223372036854775809", false).unwrap(),
        Value::from_f64(-9.22337203685478e18)
    );
    assert!(checked_cast_text_to_numeric("-", false).is_err());
}

#[test]
fn test_text_to_real() {
    assert_eq!(
        checked_cast_text_to_numeric("1", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.0", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.0", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1e10", false).unwrap(),
        Value::from_i64(10_000_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1e10", false).unwrap(),
        Value::from_i64(-10_000_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1e-10", false).unwrap(),
        Value::from_f64(1e-10)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1e-10", false).unwrap(),
        Value::from_f64(-1e-10)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.123e10", false).unwrap(),
        Value::from_i64(11_230_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.123e10", false).unwrap(),
        Value::from_i64(-11_230_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.123e-10", false).unwrap(),
        Value::from_f64(1.123e-10)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.123-e-10", false).unwrap(),
        Value::from_f64(-1.123)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1-282584294928", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.7976931348623157e309", false).unwrap(),
        Value::from_f64(f64::INFINITY),
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.7976931348623157e308", false).unwrap(),
        Value::from_f64(f64::MIN),
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.7976931348623157e309", false).unwrap(),
        Value::from_f64(f64::NEG_INFINITY),
    );
    assert_eq!(
        checked_cast_text_to_numeric("1E", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1EE", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1E", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.23E", false).unwrap(),
        Value::from_f64(1.23)
    );
    assert_eq!(
        checked_cast_text_to_numeric(".1.23E-", false).unwrap(),
        Value::from_f64(0.1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-0.0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("0.0", false).unwrap(),
        Value::from_i64(0)
    );
    assert!(checked_cast_text_to_numeric("-", false).is_err());
}

#[test]
fn test_text_to_numeric() {
    assert_eq!(
        checked_cast_text_to_numeric("1", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1823400-00000", false).unwrap(),
        Value::from_i64(1823400)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-10000000", false).unwrap(),
        Value::from_i64(-10000000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("123xxx", false).unwrap(),
        Value::from_i64(123)
    );
    // vertical tab (0x0B) is whitespace to SQLite, unlike Rust's
    // is_ascii_whitespace(). https://github.com/tursodatabase/turso/issues/8454
    assert_eq!(
        checked_cast_text_to_numeric("\x0b12", false).unwrap(),
        Value::from_i64(12)
    );
    assert_eq!(
        checked_cast_text_to_numeric("12\x0b", false).unwrap(),
        Value::from_i64(12)
    );
    assert_eq!(
        checked_cast_text_to_numeric("9223372036854775807", false).unwrap(),
        Value::from_i64(i64::MAX)
    );
    assert_eq!(
        checked_cast_text_to_numeric("9223372036854775808", false).unwrap(),
        Value::from_f64(9.22337203685478e18)
    ); // Exceeds i64, becomes float
    assert_eq!(
        checked_cast_text_to_numeric("-9223372036854775808", false).unwrap(),
        Value::from_i64(i64::MIN)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-9223372036854775809", false).unwrap(),
        Value::from_f64(-9.22337203685478e18)
    ); // Exceeds i64, becomes float

    assert_eq!(
        checked_cast_text_to_numeric("1.0", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.0", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1e10", false).unwrap(),
        Value::from_i64(10_000_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1e10", false).unwrap(),
        Value::from_i64(-10_000_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1e-10", false).unwrap(),
        Value::from_f64(1e-10)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1e-10", false).unwrap(),
        Value::from_f64(-1e-10)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.123e10", false).unwrap(),
        Value::from_i64(11_230_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.123e10", false).unwrap(),
        Value::from_i64(-11_230_000_000)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.123e-10", false).unwrap(),
        Value::from_f64(1.123e-10)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.123-e-10", false).unwrap(),
        Value::from_f64(-1.123)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1-282584294928", false).unwrap(),
        Value::from_i64(1)
    );
    assert!(checked_cast_text_to_numeric("xxx", false).is_err());
    assert_eq!(
        checked_cast_text_to_numeric("1.7976931348623157e309", false).unwrap(),
        Value::from_f64(f64::INFINITY)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.7976931348623157e308", false).unwrap(),
        Value::from_f64(f64::MIN)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.7976931348623157e309", false).unwrap(),
        Value::from_f64(f64::NEG_INFINITY)
    );

    assert_eq!(
        checked_cast_text_to_numeric("1E", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1EE", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1E", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.", false).unwrap(),
        Value::from_i64(1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-1.", false).unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.23E", false).unwrap(),
        Value::from_f64(1.23)
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.23E-", false).unwrap(),
        Value::from_f64(1.23)
    );

    assert_eq!(
        checked_cast_text_to_numeric("0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-0.0", false).unwrap(),
        Value::from_i64(0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("0.0", false).unwrap(),
        Value::from_i64(0)
    );
    assert!(checked_cast_text_to_numeric("-", false).is_err());
    assert_eq!(
        checked_cast_text_to_numeric("-e", false).unwrap(),
        Value::from_f64(0.0)
    );
    assert_eq!(
        checked_cast_text_to_numeric("-E", false).unwrap(),
        Value::from_f64(0.0)
    );
}

#[test]
fn test_parse_numeric_str_valid_integer() {
    assert_eq!(parse_numeric_str("123"), Ok((ValueType::Integer, "123")));
    assert_eq!(parse_numeric_str("-456"), Ok((ValueType::Integer, "-456")));
    assert_eq!(parse_numeric_str("+789"), Ok((ValueType::Integer, "+789")));
    assert_eq!(
        parse_numeric_str("000789"),
        Ok((ValueType::Integer, "000789"))
    );
}

#[test]
fn test_parse_numeric_str_valid_float() {
    assert_eq!(
        parse_numeric_str("123.456"),
        Ok((ValueType::Float, "123.456"))
    );
    assert_eq!(
        parse_numeric_str("-0.789"),
        Ok((ValueType::Float, "-0.789"))
    );
    assert_eq!(
        parse_numeric_str("+0.789"),
        Ok((ValueType::Float, "+0.789"))
    );
    assert_eq!(parse_numeric_str("1e10"), Ok((ValueType::Float, "1e10")));
    assert_eq!(parse_numeric_str("+1e10"), Ok((ValueType::Float, "+1e10")));
    assert_eq!(
        parse_numeric_str("-1.23e-4"),
        Ok((ValueType::Float, "-1.23e-4"))
    );
    assert_eq!(
        parse_numeric_str("1.23E+4"),
        Ok((ValueType::Float, "1.23E+4"))
    );
    assert_eq!(parse_numeric_str("1.2.3"), Ok((ValueType::Float, "1.2")))
}

#[test]
fn test_parse_numeric_str_edge_cases() {
    assert_eq!(parse_numeric_str("1e"), Ok((ValueType::Float, "1")));
    assert_eq!(parse_numeric_str("1e-"), Ok((ValueType::Float, "1")));
    assert_eq!(parse_numeric_str("1e+"), Ok((ValueType::Float, "1")));
    assert_eq!(parse_numeric_str("-1e"), Ok((ValueType::Float, "-1")));
    assert_eq!(parse_numeric_str("-1e-"), Ok((ValueType::Float, "-1")));
}

#[test]
fn test_parse_numeric_str_invalid() {
    assert_eq!(parse_numeric_str(""), Err(()));
    assert_eq!(parse_numeric_str("abc"), Err(()));
    assert_eq!(parse_numeric_str("-"), Err(()));
    assert_eq!(parse_numeric_str("+"), Err(()));
    assert_eq!(parse_numeric_str("e10"), Err(()));
    assert_eq!(parse_numeric_str(".e10"), Err(()));
}

#[test]
fn test_parse_numeric_str_with_whitespace() {
    assert_eq!(parse_numeric_str("   123"), Ok((ValueType::Integer, "123")));
    assert_eq!(
        parse_numeric_str("  -456.78  "),
        Ok((ValueType::Float, "-456.78"))
    );
    assert_eq!(
        parse_numeric_str("  1.23e4  "),
        Ok((ValueType::Float, "1.23e4"))
    );
}

#[test]
fn test_parse_numeric_str_leading_zeros() {
    assert_eq!(
        parse_numeric_str("000123"),
        Ok((ValueType::Integer, "000123"))
    );
    assert_eq!(
        parse_numeric_str("000.456"),
        Ok((ValueType::Float, "000.456"))
    );
    assert_eq!(
        parse_numeric_str("0001e3"),
        Ok((ValueType::Float, "0001e3"))
    );
}

#[test]
fn test_parse_numeric_str_trailing_characters() {
    assert_eq!(parse_numeric_str("123abc"), Ok((ValueType::Integer, "123")));
    assert_eq!(
        parse_numeric_str("456.78xyz"),
        Ok((ValueType::Float, "456.78"))
    );
    assert_eq!(
        parse_numeric_str("1.23e4extra"),
        Ok((ValueType::Float, "1.23e4"))
    );
}

#[test]
fn test_sql_is_create_virtual_table() {
    assert!(sql_is_create_virtual_table(
        "CREATE VIRTUAL TABLE x USING y;"
    ));
    assert!(sql_is_create_virtual_table(
        "create virtual table x using y"
    ));
    assert!(sql_is_create_virtual_table(
        "  \n\tCREATE  VIRTUAL TABLE x USING y"
    ));
    assert!(sql_is_create_virtual_table(
        "-- comment\nCREATE /* c */ VIRTUAL TABLE x USING y"
    ));
    assert!(sql_is_create_virtual_table(
        "CREATE VIRTUAL TABLE IF NOT EXISTS x USING y(a, b)"
    ));
    // Quoted identifiers must not confuse the classifier either way.
    assert!(sql_is_create_virtual_table(
        "CREATE VIRTUAL TABLE \"create table\" USING y"
    ));
    // Regular table whose SQL merely contains the text must not match.
    assert!(!sql_is_create_virtual_table(
        "CREATE TABLE t(x TEXT DEFAULT 'create virtual')"
    ));
    assert!(!sql_is_create_virtual_table(
        "CREATE TABLE \"create virtual\"(x)"
    ));
    assert!(!sql_is_create_virtual_table("CREATE TABLE t(x)"));
    assert!(!sql_is_create_virtual_table("CREATE VIRTUALX TABLE t(x)"));
    assert!(!sql_is_create_virtual_table("CREATE VIRTUAL"));
    assert!(!sql_is_create_virtual_table(""));
}

#[test]
fn test_module_name_basic() {
    let sql = "CREATE VIRTUAL TABLE x USING y;";
    assert_eq!(module_name_from_sql(sql).unwrap(), "y");
}

#[test]
fn test_module_name_with_args() {
    let sql = "CREATE VIRTUAL TABLE x USING modname('a', 'b');";
    assert_eq!(module_name_from_sql(sql).unwrap(), "modname");
}

#[test]
fn test_module_name_missing_using() {
    let sql = "CREATE VIRTUAL TABLE x (a, b);";
    assert!(module_name_from_sql(sql).is_err());
}

#[test]
fn test_module_name_no_semicolon() {
    let sql = "CREATE VIRTUAL TABLE x USING limbo(a, b)";
    assert_eq!(module_name_from_sql(sql).unwrap(), "limbo");
}

#[test]
fn test_module_name_no_semicolon_or_args() {
    let sql = "CREATE VIRTUAL TABLE x USING limbo";
    assert_eq!(module_name_from_sql(sql).unwrap(), "limbo");
}

#[test]
fn test_module_args_none() {
    let sql = "CREATE VIRTUAL TABLE x USING modname;";
    let args = module_args_from_sql(sql).unwrap();
    assert_eq!(args.len(), 0);
}

#[test]
fn test_module_args_basic() {
    let sql = "CREATE VIRTUAL TABLE x USING modname('arg1', 'arg2');";
    let args = module_args_from_sql(sql).unwrap();
    assert_eq!(args.len(), 2);
    assert_eq!("arg1", args[0].to_text().unwrap());
    assert_eq!("arg2", args[1].to_text().unwrap());
    for arg in args {
        unsafe { arg.__free_internal_type() }
    }
}

#[test]
fn test_module_args_with_escaped_quote() {
    let sql = "CREATE VIRTUAL TABLE x USING modname('a''b', 'c');";
    let args = module_args_from_sql(sql).unwrap();
    assert_eq!(args.len(), 2);
    assert_eq!(args[0].to_text().unwrap(), "a'b");
    assert_eq!(args[1].to_text().unwrap(), "c");
    for arg in args {
        unsafe { arg.__free_internal_type() }
    }
}

#[test]
fn test_module_args_unterminated_string() {
    let sql = "CREATE VIRTUAL TABLE x USING modname('arg1, 'arg2');";
    assert!(module_args_from_sql(sql).is_err());
}

#[test]
fn test_module_args_extra_garbage_after_quote() {
    let sql = "CREATE VIRTUAL TABLE x USING modname('arg1'x);";
    assert!(module_args_from_sql(sql).is_err());
}

#[test]
fn test_module_args_trailing_comma() {
    let sql = "CREATE VIRTUAL TABLE x USING modname('arg1',);";
    let args = module_args_from_sql(sql).unwrap();
    assert_eq!(args.len(), 1);
    assert_eq!("arg1", args[0].to_text().unwrap());
    for arg in args {
        unsafe { arg.__free_internal_type() }
    }
}

#[test]
fn test_parse_numeric_literal_hex() {
    assert_eq!(
        parse_numeric_literal("0x1234").unwrap(),
        Value::from_i64(4660)
    );
    assert_eq!(
        parse_numeric_literal("0xFFFFFFFF").unwrap(),
        Value::from_i64(4294967295)
    );
    assert_eq!(
        parse_numeric_literal("0x7FFFFFFF").unwrap(),
        Value::from_i64(2147483647)
    );
    assert_eq!(
        parse_numeric_literal("0x7FFFFFFFFFFFFFFF").unwrap(),
        Value::from_i64(9223372036854775807)
    );
    assert_eq!(
        parse_numeric_literal("0xFFFFFFFFFFFFFFFF").unwrap(),
        Value::from_i64(-1)
    );
    assert_eq!(
        parse_numeric_literal("0x8000000000000000").unwrap(),
        Value::from_i64(-9223372036854775808)
    );

    assert_eq!(
        parse_numeric_literal("-0x1234").unwrap(),
        Value::from_i64(-4660)
    );
    // too big hex: matches SQLite's prepare-time error
    assert!(parse_numeric_literal("-0x8000000000000000")
        .unwrap_err()
        .to_string()
        .contains("hex literal too big: -0x8000000000000000"));
}

#[test]
fn test_parse_numeric_literal_integer() {
    assert_eq!(parse_numeric_literal("123").unwrap(), Value::from_i64(123));
    assert_eq!(
        parse_numeric_literal("9_223_372_036_854_775_807").unwrap(),
        Value::from_i64(9223372036854775807)
    );
}

#[test]
fn test_parse_numeric_literal_float() {
    assert_eq!(
        parse_numeric_literal("123.456").unwrap(),
        Value::from_f64(123.456)
    );
    assert_eq!(
        parse_numeric_literal(".123").unwrap(),
        Value::from_f64(0.123)
    );
    assert_eq!(
        parse_numeric_literal("1.23e10").unwrap(),
        Value::from_f64(1.23e10)
    );
    assert_eq!(
        parse_numeric_literal("1e-10").unwrap(),
        Value::from_f64(1e-10)
    );
    assert_eq!(
        parse_numeric_literal("1.23E+10").unwrap(),
        Value::from_f64(1.23e10)
    );
    assert_eq!(
        parse_numeric_literal("1.1_1").unwrap(),
        Value::from_f64(1.11)
    );

    // > i64::MAX, convert to float
    assert_eq!(
        parse_numeric_literal("9223372036854775808").unwrap(),
        Value::from_f64(9.223_372_036_854_776e18)
    );
    // < i64::MIN, convert to float
    assert_eq!(
        parse_numeric_literal("-9223372036854775809").unwrap(),
        Value::from_f64(-9.223_372_036_854_776e18)
    );
}

#[test]
fn test_parse_pragma_bool() {
    assert!(parse_pragma_bool(&Expr::Literal(Literal::Numeric("1".into()))).unwrap(),);
    assert!(parse_pragma_bool(&Expr::Name(Name::exact("true".into()))).unwrap(),);
    assert!(parse_pragma_bool(&Expr::Name(Name::exact("on".into()))).unwrap(),);
    assert!(parse_pragma_bool(&Expr::Name(Name::exact("yes".into()))).unwrap(),);

    assert!(!parse_pragma_bool(&Expr::Literal(Literal::Numeric("0".into()))).unwrap(),);
    assert!(!parse_pragma_bool(&Expr::Name(Name::exact("false".into()))).unwrap(),);
    assert!(!parse_pragma_bool(&Expr::Name(Name::exact("off".into()))).unwrap(),);
    assert!(!parse_pragma_bool(&Expr::Name(Name::exact("no".into()))).unwrap(),);

    assert!(parse_pragma_bool(&Expr::Name(Name::exact("nono".into()))).is_err());
    assert!(parse_pragma_bool(&Expr::Name(Name::exact("10".into()))).is_err());
    assert!(parse_pragma_bool(&Expr::Name(Name::exact("-1".into()))).is_err());
}

#[test]
fn test_type_from_name() {
    let tc = vec![
        ("", (SchemaValueType::Blob, false)),
        ("INTEGER", (SchemaValueType::Integer, true)),
        ("INT", (SchemaValueType::Integer, false)),
        ("CHAR", (SchemaValueType::Text, false)),
        ("CLOB", (SchemaValueType::Text, false)),
        ("TEXT", (SchemaValueType::Text, false)),
        ("BLOB", (SchemaValueType::Blob, false)),
        ("REAL", (SchemaValueType::Real, false)),
        ("FLOAT", (SchemaValueType::Real, false)),
        ("DOUBLE", (SchemaValueType::Real, false)),
        ("U128", (SchemaValueType::Numeric, false)),
    ];

    for (input, expected) in tc {
        let result = type_from_name(input);
        assert_eq!(result, expected, "Failed for input: {input}");
    }
}

#[test]
fn test_checked_cast_text_to_numeric_lossless_property() {
    assert_eq!(checked_cast_text_to_numeric("1.xx", true), Err(()));
    assert_eq!(checked_cast_text_to_numeric("abc", true), Err(()));
    assert_eq!(checked_cast_text_to_numeric("--5", true), Err(()));
    assert_eq!(checked_cast_text_to_numeric("12.34.56", true), Err(()));
    assert_eq!(checked_cast_text_to_numeric("", true), Err(()));
    assert_eq!(checked_cast_text_to_numeric(" ", true), Err(()));
    assert_eq!(
        checked_cast_text_to_numeric("0", true),
        Ok(Value::from_i64(0))
    );
    assert_eq!(
        checked_cast_text_to_numeric("42", true),
        Ok(Value::from_i64(42))
    );
    assert_eq!(
        checked_cast_text_to_numeric("-42", true),
        Ok(Value::from_i64(-42))
    );
    assert_eq!(
        checked_cast_text_to_numeric("999999999999", true),
        Ok(Value::from_i64(999_999_999_999))
    );
    assert_eq!(
        checked_cast_text_to_numeric("1.0", true),
        Ok(Value::from_i64(1))
    );
    assert_eq!(
        checked_cast_text_to_numeric("-3.22", true),
        Ok(Value::from_f64(-3.22))
    );
    assert_eq!(
        checked_cast_text_to_numeric("0.001", true),
        Ok(Value::from_f64(0.001))
    );
    assert_eq!(
        checked_cast_text_to_numeric("2e3", true),
        Ok(Value::from_i64(2000))
    );
    assert_eq!(
        checked_cast_text_to_numeric("-5.5e-2", true),
        Ok(Value::from_f64(-0.055))
    );
    assert_eq!(
        checked_cast_text_to_numeric(" 123 ", true),
        Ok(Value::from_i64(123))
    );
    assert_eq!(
        checked_cast_text_to_numeric("\t-3.22\n", true),
        Ok(Value::from_f64(-3.22))
    );
    assert_eq!(
        checked_cast_text_to_numeric("\u{00A0}123\u{00A0}", true),
        Err(())
    );
}

#[test]
fn test_trim_ascii_whitespace_helper() {
    assert_eq!(trim_ascii_whitespace("  hello  "), "hello");
    assert_eq!(trim_ascii_whitespace("\t\nhello\r\n"), "hello");
    assert_eq!(trim_ascii_whitespace("hello"), "hello");
    assert_eq!(trim_ascii_whitespace("   "), "");
    assert_eq!(trim_ascii_whitespace(""), "");

    // vertical tab (0x0B) is whitespace to SQLite's ctype table, unlike
    // Rust's is_ascii_whitespace(). https://github.com/tursodatabase/turso/issues/8454
    assert_eq!(trim_ascii_whitespace("\x0bhello\x0b"), "hello");

    // non-breaking space should NOT be trimmed
    assert_eq!(
        trim_ascii_whitespace("\u{00A0}hello\u{00A0}"),
        "\u{00A0}hello\u{00A0}"
    );
    assert_eq!(
        trim_ascii_whitespace("  \u{00A0}hello\u{00A0}  "),
        "\u{00A0}hello\u{00A0}"
    );
}

#[test]
fn test_cast_real_to_integer_limits() {
    // Values that are exactly representable in f64 and strictly within i64 range
    let max_exact = ((1i64 << 51) - 1) as f64;
    assert_eq!(cast_real_to_integer(max_exact), Ok((1i64 << 51) - 1));
    assert_eq!(cast_real_to_integer(-max_exact), Ok(-((1i64 << 51) - 1)));

    // Values beyond 2^51 are valid if they round-trip correctly and are strictly within bounds
    assert_eq!(cast_real_to_integer((1i64 << 51) as f64), Ok(1i64 << 51));
    assert_eq!(cast_real_to_integer((1i64 << 52) as f64), Ok(1i64 << 52));

    // 2^62 round-trips correctly and is strictly between i64::MIN and i64::MAX
    assert_eq!(cast_real_to_integer((1i64 << 62) as f64), Ok(1i64 << 62));

    // The original bug's value: 426601719749026560 should work
    assert_eq!(
        cast_real_to_integer(426601719749026560.0),
        Ok(426601719749026560)
    );

    // SQLite rejects boundary values: i64::MIN and i64::MAX exactly
    // (ix > SMALLEST_INT64 && ix < LARGEST_INT64 requires STRICT inequality)
    assert_eq!(cast_real_to_integer(i64::MIN as f64), Err(()));
    assert_eq!(cast_real_to_integer(i64::MAX as f64), Err(()));

    // Values at or beyond i64::MAX + 1 (2^63) should fail
    assert_eq!(cast_real_to_integer(9223372036854775808.0), Err(()));

    // Values below i64::MIN should fail
    assert_eq!(cast_real_to_integer(-9223372036854777856.0), Err(()));

    // Non-whole numbers should fail
    assert_eq!(cast_real_to_integer(1.5), Err(()));
    assert_eq!(cast_real_to_integer(-1.5), Err(()));

    // Non-finite values should fail
    assert_eq!(cast_real_to_integer(f64::INFINITY), Err(()));
    assert_eq!(cast_real_to_integer(f64::NEG_INFINITY), Err(()));
    assert_eq!(cast_real_to_integer(f64::NAN), Err(()));
}

#[test]
fn capture_parameters_treats_numbered_markers_as_placeholders() {
    use turso_parser::ast::{Literal, Variable};
    // Index-method patterns use ?N to share one captured argument
    // across several call sites (fts_score/fts_match); a numbered
    // marker carries its "?N" spelling as its name and must still
    // capture. Named parameters are not placeholders.
    let query = Expr::Literal(Literal::String("'x'".to_string()));

    let numbered = Expr::Variable(Variable::numbered(1.try_into().unwrap()));
    let captured = try_capture_parameters(&numbered, &query).unwrap();
    assert_eq!(captured.get(&1), Some(&query));

    let anonymous = Expr::Variable(Variable::indexed(1.try_into().unwrap()));
    assert!(try_capture_parameters(&anonymous, &query)
        .unwrap()
        .contains_key(&1));

    let named = Expr::Variable(Variable::named(":x", 1.try_into().unwrap()));
    assert!(try_capture_parameters(&named, &query).is_none());
}
