use super::{where_term_is_null_rejecting_for_table, Optimizable};
use crate::translate::emitter::{DoubleQuotedDml, Resolver};
use crate::{schema::Schema, DatabaseCatalog, RwLock, SymbolTable};
use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, Expr, FunctionTail, Name, TableInternalId};

fn empty_resolver<'a>(
    schema: &'a Schema,
    database_schemas: &'a RwLock<HashMap<usize, crate::sync::Arc<Schema>>>,
    temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
    attached_databases: &'a RwLock<DatabaseCatalog>,
    syms: &'a SymbolTable,
) -> Resolver<'a> {
    Resolver::new(
        schema,
        database_schemas,
        temp_database,
        attached_databases,
        syms,
        true,
        DoubleQuotedDml::Enabled,
        crate::sync::Arc::new(crate::dialect::SqliteDialect),
        &None,
    )
}

fn no_tail() -> FunctionTail {
    FunctionTail {
        filter_clause: None,
        over_clause: None,
    }
}

fn fn_call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall {
        name: Name::exact(name.to_string()),
        distinctness: None,
        args: args.into_iter().map(Box::new).collect(),
        order_by: vec![],
        within_group: vec![],
        filter_over: no_tail(),
    }
}

#[test]
fn constant_classifier_for_coalesce_with_in_list() {
    let schema = Schema::new();
    let syms = SymbolTable::new();
    let database_schemas = RwLock::new(HashMap::default());
    let attached_databases = RwLock::new(DatabaseCatalog::new());
    let temp_database = RwLock::new(None);
    let resolver = empty_resolver(
        &schema,
        &database_schemas,
        &temp_database,
        &attached_databases,
        &syms,
    );

    let expr = fn_call(
        "coalesce",
        vec![
            fn_call(
                "length",
                vec![Expr::Literal(ast::Literal::String("a".into()))],
            ),
            Expr::InList {
                lhs: Box::new(fn_call(
                    "hex",
                    vec![Expr::Literal(ast::Literal::Blob("X'01'".into()))],
                )),
                not: false,
                rhs: vec![Box::new(Expr::Literal(ast::Literal::Blob("X'02'".into())))],
            },
        ],
    );

    assert!(expr.is_constant(&resolver));
}

#[test]
fn constant_classifier_for_quote_of_column() {
    let schema = Schema::new();
    let syms = SymbolTable::new();
    let database_schemas = RwLock::new(HashMap::default());
    let attached_databases = RwLock::new(DatabaseCatalog::new());
    let temp_database = RwLock::new(None);
    let resolver = empty_resolver(
        &schema,
        &database_schemas,
        &temp_database,
        &attached_databases,
        &syms,
    );

    let expr = fn_call(
        "quote",
        vec![Expr::Column {
            database: None,
            table: TableInternalId::default(),
            column: 0,
            is_rowid_alias: false,
        }],
    );

    assert!(!expr.is_constant(&resolver));
}

#[test]
fn null_rejection_detection_uses_function_resolution() {
    let table = TableInternalId::from(42);
    let expr = Expr::Binary(
        Box::new(fn_call(
            "IFNULL",
            vec![
                Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                },
                Expr::Literal(ast::Literal::Numeric("2147483647".into())),
            ],
        )),
        ast::Operator::GreaterEquals,
        Box::new(Expr::Literal(ast::Literal::Numeric("127".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_requires_target_table_reference() {
    let target_table = TableInternalId::from(7);
    let other_table = TableInternalId::from(8);
    // A term that never mentions the target table can be TRUE on the
    // join's null-extended rows, so it proves nothing about them.
    let expr = Expr::Binary(
        Box::new(fn_call(
            "coalesce",
            vec![
                Expr::Column {
                    database: None,
                    table: other_table,
                    column: 0,
                    is_rowid_alias: false,
                },
                Expr::Literal(ast::Literal::Numeric("0".into())),
            ],
        )),
        ast::Operator::Greater,
        Box::new(Expr::Literal(ast::Literal::Numeric("1".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, target_table));
}

#[test]
fn null_rejection_detection_handles_nested_null_masking_functions() {
    let table = TableInternalId::from(9);
    let expr = Expr::Binary(
        Box::new(fn_call(
            "coalesce",
            vec![
                fn_call(
                    "ifnull",
                    vec![
                        Expr::Column {
                            database: None,
                            table,
                            column: 1,
                            is_rowid_alias: false,
                        },
                        Expr::Literal(ast::Literal::Numeric("0".into())),
                    ],
                ),
                Expr::Literal(ast::Literal::Numeric("2".into())),
            ],
        )),
        ast::Operator::Equals,
        Box::new(Expr::Literal(ast::Literal::Numeric("2".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_treats_is_operator_as_non_rejecting() {
    let table = TableInternalId::from(11);
    let expr = Expr::Binary(
        Box::new(Expr::Column {
            database: None,
            table,
            column: 0,
            is_rowid_alias: false,
        }),
        ast::Operator::Is,
        Box::new(Expr::Literal(ast::Literal::Null)),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_treats_empty_not_in_as_non_rejecting() {
    let table = TableInternalId::from(15);
    let column = Expr::Column {
        database: None,
        table,
        column: 0,
        is_rowid_alias: false,
    };
    let not_in_empty = Expr::InList {
        lhs: Box::new(column.clone()),
        not: true,
        rhs: vec![],
    };
    let in_value = Expr::InList {
        lhs: Box::new(column),
        not: false,
        rhs: vec![Box::new(Expr::Literal(ast::Literal::Numeric("1".into())))],
    };

    assert!(!where_term_is_null_rejecting_for_table(
        &not_in_empty,
        table
    ));
    assert!(where_term_is_null_rejecting_for_table(&in_value, table));
}

#[test]
fn null_rejection_detection_treats_is_between_columns_as_non_rejecting() {
    let table = TableInternalId::from(12);
    let expr = Expr::Binary(
        Box::new(Expr::Column {
            database: None,
            table,
            column: 0,
            is_rowid_alias: false,
        }),
        ast::Operator::Is,
        Box::new(Expr::Column {
            database: None,
            table,
            column: 1,
            is_rowid_alias: false,
        }),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_treats_is_with_non_null_literal_as_non_rejecting() {
    let table = TableInternalId::from(13);
    let expr = Expr::Binary(
        Box::new(Expr::Column {
            database: None,
            table,
            column: 0,
            is_rowid_alias: false,
        }),
        ast::Operator::Is,
        Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_treats_is_not_with_non_null_literal_as_non_rejecting() {
    let table = TableInternalId::from(14);
    let expr = Expr::Binary(
        Box::new(Expr::Column {
            database: None,
            table,
            column: 0,
            is_rowid_alias: false,
        }),
        ast::Operator::IsNot,
        Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_case_with_is_null_check_not_rejecting() {
    let table = TableInternalId::from(15);
    // CASE WHEN t.col IS NULL THEN 1 ELSE t.col END > 0
    let expr = Expr::Binary(
        Box::new(Expr::Case {
            base: None,
            when_then_pairs: vec![(
                Box::new(Expr::IsNull(Box::new(Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                }))),
                Box::new(Expr::Literal(ast::Literal::Numeric("1".into()))),
            )],
            else_expr: Some(Box::new(Expr::Column {
                database: None,
                table,
                column: 0,
                is_rowid_alias: false,
            })),
        }),
        ast::Operator::Greater,
        Box::new(Expr::Literal(ast::Literal::Numeric("0".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_case_without_null_check_is_rejecting() {
    let table = TableInternalId::from(16);
    // CASE WHEN t.col > 5 THEN t.col ELSE 0 END > 0
    let expr = Expr::Binary(
        Box::new(Expr::Case {
            base: None,
            when_then_pairs: vec![(
                Box::new(Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table,
                        column: 0,
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Greater,
                    Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
                )),
                Box::new(Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                }),
            )],
            else_expr: Some(Box::new(Expr::Literal(ast::Literal::Numeric("0".into())))),
        }),
        ast::Operator::Greater,
        Box::new(Expr::Literal(ast::Literal::Numeric("0".into()))),
    );

    // Any CASE can turn NULL inputs into a non-NULL result (here the ELSE
    // arm yields 0 for a NULL t.col), so no CASE term proves anything
    // about null-extended rows. Same rule as SQLite's impliesNotNullRow.
    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_null_test_nested_in_comparison_not_rejecting() {
    let table = TableInternalId::from(18);
    // (t.col IS NULL) = 1 — TRUE on a null-extended row.
    let expr = Expr::Binary(
        Box::new(Expr::Parenthesized(vec![Box::new(Expr::IsNull(Box::new(
            Expr::Column {
                database: None,
                table,
                column: 0,
                is_rowid_alias: false,
            },
        )))])),
        ast::Operator::Equals,
        Box::new(Expr::Literal(ast::Literal::Numeric("1".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}

#[test]
fn null_rejection_detection_or_needs_both_arms() {
    let table = TableInternalId::from(19);
    let other_table = TableInternalId::from(20);
    let col = |t: TableInternalId, c: usize| Expr::Column {
        database: None,
        table: t,
        column: c,
        is_rowid_alias: false,
    };
    let eq_five = |t: TableInternalId, c: usize| {
        Expr::Binary(
            Box::new(col(t, c)),
            ast::Operator::Equals,
            Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
        )
    };

    // t.a = 5 OR t.b = 5: both arms are false when t's columns are NULL.
    let both_arms_on_table = Expr::Binary(
        Box::new(eq_five(table, 0)),
        ast::Operator::Or,
        Box::new(eq_five(table, 1)),
    );
    assert!(where_term_is_null_rejecting_for_table(
        &both_arms_on_table,
        table
    ));

    // t.a = 5 OR u.x = 5: the u arm can make the OR true on t's
    // null-extended rows.
    let one_arm_on_other_table = Expr::Binary(
        Box::new(eq_five(table, 0)),
        ast::Operator::Or,
        Box::new(eq_five(other_table, 0)),
    );
    assert!(!where_term_is_null_rejecting_for_table(
        &one_arm_on_other_table,
        table
    ));
}

#[test]
fn null_rejection_detection_iif_with_is_null_check_not_rejecting() {
    let table = TableInternalId::from(17);
    // IIF(t.col IS NULL, 1, t.col) > 0
    let expr = Expr::Binary(
        Box::new(fn_call(
            "iif",
            vec![
                Expr::IsNull(Box::new(Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                })),
                Expr::Literal(ast::Literal::Numeric("1".into())),
                Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                },
            ],
        )),
        ast::Operator::Greater,
        Box::new(Expr::Literal(ast::Literal::Numeric("0".into()))),
    );

    assert!(!where_term_is_null_rejecting_for_table(&expr, table));
}
