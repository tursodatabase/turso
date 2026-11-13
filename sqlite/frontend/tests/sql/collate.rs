use std::cmp::Ordering;
#[allow(unused_imports)]
use std::str::FromStr as _;
use turso_core::alloc::vec;
use turso_core::namespace::NamespaceRef;
use turso_core::translate::expr_context::ExprContext;
use turso_core::type_ordering::TypeOrdering;
use turso_core::{sync::Arc, MAIN_DB_ID};
use turso_parser::ast::Expr;
use turso_sqlite::SqliteDialect;

use turso_parser::ast::{Literal, Name, Operator, TableInternalId, UnaryOperator};

use turso_core::{
    schema::{BTreeCharacteristics, BTreeTable, ColDef, ColDefFlags, Column, Table, Type},
    translate::plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan},
    translate::{emitter::Resolver, expr_context::TableMetadata, plan::TableReferences},
};

use turso_core::translate::collate::*;

#[test]
fn test_locale_collation_names() {
    assert!(matches!(
        TextCollation::new("fr-FR").unwrap(),
        TextCollation::Locale(_)
    ));
    assert!(matches!(
        TextCollation::new("es-u-co-trad").unwrap(),
        TextCollation::Locale(_)
    ));
    assert!(matches!(
        TextCollation::new("en-u-kf-upper").unwrap(),
        TextCollation::Locale(_)
    ));
    assert!(matches!(
        TextCollation::new("en-US-u-kf-upper").unwrap(),
        TextCollation::Locale(_)
    ));
    assert!(TextCollation::new("compile_options").is_err());
}

#[test]
fn collation_bits_round_trip_and_unknown_bits_are_an_error() {
    let locale = CollationSeq::new("fr-FR").unwrap();
    for collation in [
        CollationSeq::BINARY,
        CollationSeq::NOCASE,
        CollationSeq::RTRIM,
        locale,
    ]
    .into_iter()
    .chain(TypeOrdering::ALL.map(CollationSeq::Type))
    {
        assert_eq!(
            CollationSeq::from_bits(collation.to_bits()).unwrap(),
            collation
        );
    }
    assert!(CollationSeq::from_bits(CollationSeq::NO_COLLATION_BITS).is_err());
    assert!(CollationSeq::from_bits(u16::MAX).is_err());
}

#[test]
fn test_locale_collation_compare() {
    let traditional_spanish = TextCollation::new("es-u-co-trad").unwrap();
    assert_eq!(
        traditional_spanish.compare_strings("pollo", "polvo"),
        Ordering::Greater
    );

    let upper_first = TextCollation::new("en-u-kf-upper").unwrap();
    assert_eq!(upper_first.compare_strings("A", "a"), Ordering::Less);

    let upper_first_us = TextCollation::new("en-US-u-kf-upper").unwrap();
    assert_eq!(upper_first_us.compare_strings("A", "a"), Ordering::Less);
}

#[test]
fn expr_collation_single_table_single_column() {
    // A plain column has its declared collation, BINARY when it declares none.
    for collation in [
        None,
        Some(TextCollation::Binary),
        Some(TextCollation::NoCase),
        Some(TextCollation::Rtrim),
    ] {
        let table_references =
            get_table_references_single_table_single_column_with_collation(collation);
        let expr = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
        )
        .unwrap();
        assert_eq!(collseq, Some(collation.unwrap_or(TextCollation::Binary)));
    }
}

#[test]
fn expr_collation_single_table_single_column_with_collate() {
    let table_references =
        get_table_references_single_table_single_column_with_collation(Some(TextCollation::Binary));
    // col COLLATE RTRIM, col COLLATE NOCASE, col COLLATE BINARY
    for collation in ["RTRIM", "NOCASE", "BINARY"] {
        let expected_collation = TextCollation::new(collation).unwrap();
        let expr = Expr::Collate(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            }),
            Name::exact(collation.to_string()),
        );
        let collseq = expr_collation_if_any(
            &expr,
            &TableMetadata::new(Some(&table_references)),
            &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
        )
        .unwrap();
        assert_eq!(collseq, Some(expected_collation));
    }
}

#[test]
fn expr_collation_multiple_collate_leftmost_wins() {
    let table_references =
        get_table_references_single_table_single_column_with_collation(Some(TextCollation::Binary));
    // (col COLLATE NOCASE) COLLATE RTRIM -- RTRIM wins as it is the leftmost AST node with a COLLATE
    let inner = Expr::Collate(
        Box::new(Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        }),
        Name::exact("NOCASE".to_string()),
    );
    let expr = Expr::Collate(
        Box::new(Expr::Parenthesized(std::vec![Box::new(inner)])),
        Name::exact("RTRIM".to_string()),
    );
    let collseq = expr_collation_if_any(
        &expr,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq, Some(TextCollation::Rtrim));
}

#[test]
fn expr_collation_unary_plus_and_cast_still_column() {
    let table_references =
        get_table_references_single_table_single_column_with_collation(Some(TextCollation::NoCase));
    // Unary plus on column
    let expr_plus = Expr::unary(
        UnaryOperator::Positive,
        Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        },
    );
    let collseq_plus = expr_collation_if_any(
        &expr_plus,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq_plus, Some(TextCollation::NoCase));

    // CAST(column AS TEXT)
    let cast_ty = Some(turso_parser::ast::Type {
        name: "TEXT".to_string(),
        size: None,
        array_dimensions: 0,
    });
    let expr_cast = Expr::cast(
        Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        },
        cast_ty,
    );
    let collseq_cast = expr_collation_if_any(
        &expr_cast,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq_cast, Some(TextCollation::NoCase));
}

#[test]
fn expr_collation_explicit_collate_anywhere_in_operand() {
    let table_references = get_table_references_two_tables_single_column_with_collations(
        Some(TextCollation::NoCase),
        None,
    );
    // RTRIM wins because it's an explicit COLLATE even though it appears on the right side of the expression
    let lhs = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let rhs = Expr::Parenthesized(std::vec![Box::new(Expr::Collate(
        Box::new(Expr::Literal(Literal::String("x".to_string()))),
        Name::exact("RTRIM".to_string()),
    ))]);
    let expr = Expr::binary(lhs, Operator::Add, rhs);
    let collseq = expr_collation_if_any(
        &expr,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq, Some(TextCollation::Rtrim));
}

#[test]
fn expr_collation_column_plus_column_has_none() {
    let table_references = get_table_references_two_tables_single_column_with_collations(
        Some(TextCollation::NoCase),
        Some(TextCollation::Rtrim),
    );
    // col1 + col2 has no collation: as in SQLite, an operator only passes on a
    // collation from an operand that contains an explicit COLLATE.
    let lhs = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let rhs = Expr::Column {
        database: None,
        table: TableInternalId::from(2),
        column: 0,
        is_rowid_alias: false,
    };
    let expr = Expr::binary(lhs, Operator::Add, rhs);
    let collseq = expr_collation_if_any(
        &expr,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq, None);
}

#[test]
fn expr_collation_collate_vs_collate_leftside_expr_wins() {
    let table_references = TableReferences::new_empty();
    // (x COLLATE NOCASE) + (y COLLATE RTRIM) -- NOCASE wins since it's on the left side
    let lhs = Expr::Collate(
        Box::new(Expr::Literal(Literal::String("x".to_string()))),
        Name::exact("NOCASE".to_string()),
    );
    let rhs = Expr::Collate(
        Box::new(Expr::Literal(Literal::String("y".to_string()))),
        Name::exact("RTRIM".to_string()),
    );
    let expr = Expr::binary(lhs, Operator::Add, rhs);
    let collseq = expr_collation_if_any(
        &expr,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq, Some(TextCollation::NoCase));
}

#[test]
fn expr_collation_default_binary_when_no_collate_or_column() {
    let table_references = TableReferences::new_empty();
    let expr = Expr::Literal(Literal::String("abc".to_string()));
    let collseq = expr_collation_if_any(
        &expr,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq, None);
}

#[test]
fn expr_collation_rowid_has_none_even_with_declared_collation() {
    let table_references =
        get_table_references_single_table_rowid_alias_with_collation(Some(TextCollation::NoCase));
    let expr = Expr::RowId {
        database: None,
        table: TableInternalId::from(1),
    };
    // As in SQLite, the rowid has no collation even when its INTEGER PRIMARY KEY
    // alias declares one.
    let collseq = expr_collation_if_any(
        &expr,
        &TableMetadata::new(Some(&table_references)),
        &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect)).catalog,
    )
    .unwrap();
    assert_eq!(collseq, None);
}

#[test]
fn comparison_collation_nocase_column_vs_binary_default() {
    // LHS has NOCASE column, RHS has no collation → NOCASE
    let table_refs = get_table_references_two_tables_single_column_with_collations(
        Some(TextCollation::NoCase),
        None,
    );
    let lhs = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let rhs = Expr::Column {
        database: None,
        table: TableInternalId::from(2),
        column: 0,
        is_rowid_alias: false,
    };
    assert_eq!(
        comparison_collation(
            &lhs,
            &rhs,
            &ExprContext::new(
                Some(&table_refs),
                &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect))
            )
        )
        .unwrap(),
        CollationSeq::NOCASE
    );
    // Swapped: the LHS column is still a column, so its default BINARY
    // collation wins over the NOCASE column on the RHS.
    assert_eq!(
        comparison_collation(
            &rhs,
            &lhs,
            &ExprContext::new(
                Some(&table_refs),
                &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect))
            )
        )
        .unwrap(),
        CollationSeq::BINARY
    );
}

#[test]
fn comparison_collation_function_hides_column_collation() {
    // A column inside a function call is not a column operand, so the
    // NOCASE column on the other side supplies the collation.
    let table_refs = get_table_references_two_tables_single_column_with_collations(
        Some(TextCollation::NoCase),
        None,
    );
    let nocase_column = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let function_of_binary_column = Expr::FunctionCall {
        name: Name::exact("lower".to_string()),
        distinctness: None,
        args: std::vec![Box::new(Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        })],
        order_by: std::vec![],
        within_group: std::vec![],
        filter_over: turso_parser::ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    assert_eq!(
        comparison_collation(
            &function_of_binary_column,
            &nocase_column,
            &ExprContext::new(
                Some(&table_refs),
                &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect))
            )
        )
        .unwrap(),
        CollationSeq::NOCASE
    );
}

#[test]
fn comparison_collation_uplus_and_parens_keep_column_collation() {
    // "+column" and "(column)" still count as columns, so the BINARY
    // column on the LHS wins over the NOCASE column on the RHS.
    let table_refs = get_table_references_two_tables_single_column_with_collations(
        Some(TextCollation::NoCase),
        None,
    );
    let nocase_column = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let wrapped_binary_column = Expr::Unary(
        UnaryOperator::Positive,
        Box::new(Expr::Parenthesized(std::vec![Box::new(Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        })])),
    );
    assert_eq!(
        comparison_collation(
            &wrapped_binary_column,
            &nocase_column,
            &ExprContext::new(
                Some(&table_refs),
                &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect))
            )
        )
        .unwrap(),
        CollationSeq::BINARY
    );
}

#[test]
fn comparison_collation_explicit_beats_column() {
    // LHS column is NOCASE, but RHS has explicit RTRIM → RTRIM wins
    let table_refs = get_table_references_two_tables_single_column_with_collations(
        Some(TextCollation::NoCase),
        None,
    );
    let lhs = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let rhs = Expr::Collate(
        Box::new(Expr::Column {
            database: None,
            table: TableInternalId::from(2),
            column: 0,
            is_rowid_alias: false,
        }),
        Name::exact("RTRIM".to_string()),
    );
    assert_eq!(
        comparison_collation(
            &lhs,
            &rhs,
            &ExprContext::new(
                Some(&table_refs),
                &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect))
            )
        )
        .unwrap(),
        CollationSeq::RTRIM
    );
}

#[test]
fn comparison_collation_both_default_is_binary() {
    let table_refs = get_table_references_two_tables_single_column_with_collations(None, None);
    let lhs = Expr::Column {
        database: None,
        table: TableInternalId::from(1),
        column: 0,
        is_rowid_alias: false,
    };
    let rhs = Expr::Column {
        database: None,
        table: TableInternalId::from(2),
        column: 0,
        is_rowid_alias: false,
    };
    assert_eq!(
        comparison_collation(
            &lhs,
            &rhs,
            &ExprContext::new(
                Some(&table_refs),
                &Resolver::empty_for_tests(std::sync::Arc::new(SqliteDialect))
            )
        )
        .unwrap(),
        CollationSeq::BINARY
    );
}

// Helpers //

fn get_table_references_single_table_single_column_with_collation(
    collation: Option<TextCollation>,
) -> TableReferences {
    let mut table_references = TableReferences::new_empty();
    let columns = vec![Column::new(
        Some("foo".to_string()),
        "text".to_string(),
        None,
        None,
        Type::Text,
        collation,
        ColDef::default(),
    )];
    let table = Table::BTree(Arc::new(BTreeTable::new(
        0,
        "foo".to_string(),
        vec![],
        columns,
        BTreeCharacteristics::empty(),
        vec![],
        vec![],
        vec![],
        None,
    )));
    table_references.add_joined_table(JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        unmatched_right_rows_plan: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        namespace: NamespaceRef::root_of(MAIN_DB_ID),
        identifier: "foo".to_string(),
        has_alias: false,
        internal_id: TableInternalId::from(1),
        join_info: None,
        table,
        plan_estimate: None,
        row_filter: None,
        indexed: None,
    });

    table_references
}

fn get_table_references_two_tables_single_column_with_collations(
    left: Option<TextCollation>,
    right: Option<TextCollation>,
) -> TableReferences {
    let mut table_references = TableReferences::new_empty();
    // Left table t1(id=1)
    let columns = vec![Column::new(
        Some("a".to_string()),
        "text".to_string(),
        None,
        None,
        Type::Text,
        left,
        ColDef::default(),
    )];
    table_references.add_joined_table(JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        unmatched_right_rows_plan: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        namespace: NamespaceRef::root_of(MAIN_DB_ID),
        identifier: "t1".to_string(),
        has_alias: false,
        internal_id: TableInternalId::from(1),
        join_info: None,
        plan_estimate: None,
        row_filter: None,
        table: Table::BTree(Arc::new(BTreeTable::new(
            0,
            "t1".to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        ))),
        indexed: None,
    });
    // Right table t2(id=2)
    let columns = vec![Column::new(
        Some("b".to_string()),
        "text".to_string(),
        None,
        None,
        Type::Text,
        right,
        ColDef::default(),
    )];
    table_references.add_joined_table(JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        unmatched_right_rows_plan: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        namespace: NamespaceRef::root_of(MAIN_DB_ID),
        identifier: "t2".to_string(),
        has_alias: false,
        internal_id: TableInternalId::from(2),
        join_info: None,
        plan_estimate: None,
        row_filter: None,
        table: Table::BTree(Arc::new(BTreeTable::new(
            0,
            "t2".to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        ))),
        indexed: None,
    });
    table_references
}

fn get_table_references_single_table_rowid_alias_with_collation(
    collation: Option<TextCollation>,
) -> TableReferences {
    use turso_parser::ast::SortOrder;
    let mut table_references = TableReferences::new_empty();
    let columns = vec![Column::new(
        Some("id".to_string()),
        "INTEGER".to_string(),
        None,
        None,
        Type::Integer,
        collation,
        ColDef {
            flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::Unique,
            ..Default::default()
        },
    )];
    table_references.add_joined_table(JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        unmatched_right_rows_plan: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        namespace: NamespaceRef::root_of(MAIN_DB_ID),
        identifier: "bar".to_string(),
        has_alias: false,
        internal_id: TableInternalId::from(1),
        join_info: None,
        plan_estimate: None,
        row_filter: None,
        indexed: None,
        table: Table::BTree(Arc::new(BTreeTable::new(
            0,
            "bar".to_string(),
            vec![("id".to_string(), SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        ))),
    });
    table_references
}
