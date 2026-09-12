use crate::alloc::vec;
use crate::{sync::Arc, MAIN_DB_ID};

use turso_parser::ast::{Literal, Name, Operator, TableInternalId, UnaryOperator};

use crate::{
    schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Table, Type},
    translate::plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan},
};

use super::*;

#[test]
fn test_locale_collation_names() {
    assert!(matches!(
        CollationSeq::new("fr-FR").unwrap(),
        CollationSeq::Locale(_)
    ));
    assert!(matches!(
        CollationSeq::new("es-u-co-trad").unwrap(),
        CollationSeq::Locale(_)
    ));
    assert!(matches!(
        CollationSeq::new("en-u-kf-upper").unwrap(),
        CollationSeq::Locale(_)
    ));
    assert!(matches!(
        CollationSeq::new("en-US-u-kf-upper").unwrap(),
        CollationSeq::Locale(_)
    ));
    assert!(CollationSeq::new("compile_options").is_err());
}

#[test]
fn test_locale_collation_compare() {
    let traditional_spanish = CollationSeq::new("es-u-co-trad").unwrap();
    assert_eq!(
        traditional_spanish.compare_strings("pollo", "polvo"),
        Ordering::Greater
    );

    let upper_first = CollationSeq::new("en-u-kf-upper").unwrap();
    assert_eq!(upper_first.compare_strings("A", "a"), Ordering::Less);

    let upper_first_us = CollationSeq::new("en-US-u-kf-upper").unwrap();
    assert_eq!(upper_first_us.compare_strings("A", "a"), Ordering::Less);
}

#[test]
fn test_get_collseq_from_expr_single_table_single_column() {
    // plain column
    for collation in [
        None,
        Some(CollationSeq::Binary),
        Some(CollationSeq::NoCase),
        Some(CollationSeq::Rtrim),
    ] {
        let table_references =
            get_table_references_single_table_single_column_with_collation(collation);
        let expr = Expr::Column {
            database: None,
            table: TableInternalId::from(1),
            column: 0,
            is_rowid_alias: false,
        };
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, collation);
    }
}

#[test]
fn test_get_collseq_from_expr_single_table_single_column_with_collate() {
    let table_references =
        get_table_references_single_table_single_column_with_collation(Some(CollationSeq::Binary));
    // col COLLATE RTRIM, col COLLATE NOCASE, col COLLATE BINARY
    for collation in ["RTRIM", "NOCASE", "BINARY"] {
        let expected_collation = CollationSeq::new(collation).unwrap();
        let expr = Expr::Collate(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::from(1),
                column: 0,
                is_rowid_alias: false,
            }),
            Name::exact(collation.to_string()),
        );
        let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
        assert_eq!(collseq, Some(expected_collation));
    }
}

#[test]
fn test_get_collseq_from_expr_multiple_collate_leftmost_wins() {
    let table_references =
        get_table_references_single_table_single_column_with_collation(Some(CollationSeq::Binary));
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
    let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
    assert_eq!(collseq, Some(CollationSeq::Rtrim));
}

#[test]
fn test_get_collseq_from_expr_unary_plus_and_cast_still_column() {
    let table_references =
        get_table_references_single_table_single_column_with_collation(Some(CollationSeq::NoCase));
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
    let collseq_plus = get_collseq_from_expr(&expr_plus, &table_references).unwrap();
    assert_eq!(collseq_plus, Some(CollationSeq::NoCase));

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
    let collseq_cast = get_collseq_from_expr(&expr_cast, &table_references).unwrap();
    assert_eq!(collseq_cast, Some(CollationSeq::NoCase));
}

#[test]
fn test_get_collseq_from_expr_explicit_collate_anywhere_in_operand() {
    let table_references = get_table_references_two_tables_single_column_with_collations(
        Some(CollationSeq::NoCase),
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
    let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
    assert_eq!(collseq, Some(CollationSeq::Rtrim));
}

#[test]
fn test_get_collseq_from_expr_column_plus_column_leftside_column_wins() {
    let table_references = get_table_references_two_tables_single_column_with_collations(
        Some(CollationSeq::NoCase),
        Some(CollationSeq::Rtrim),
    );
    // col1 + col2 -- col1's NOCASE collation wins since it's on the left side
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
    let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
    assert_eq!(collseq, Some(CollationSeq::NoCase));
}

#[test]
fn test_get_collseq_from_expr_collate_vs_collate_leftside_expr_wins() {
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
    let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
    assert_eq!(collseq, Some(CollationSeq::NoCase));
}

#[test]
fn test_get_collseq_from_expr_default_binary_when_no_collate_or_column() {
    let table_references = TableReferences::new_empty();
    let expr = Expr::Literal(Literal::String("abc".to_string()));
    let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
    assert_eq!(collseq, None);
}

#[test]
fn test_get_collseq_from_expr_rowid_uses_rowid_alias_collation() {
    let table_references =
        get_table_references_single_table_rowid_alias_with_collation(Some(CollationSeq::NoCase));
    let expr = Expr::RowId {
        database: None,
        table: TableInternalId::from(1),
    };
    let collseq = get_collseq_from_expr(&expr, &table_references).unwrap();
    assert_eq!(collseq, Some(CollationSeq::NoCase));
}

#[test]
fn test_resolve_comparison_collseq_nocase_column_vs_binary_default() {
    // LHS has NOCASE column, RHS has no collation → NOCASE
    let table_refs = get_table_references_two_tables_single_column_with_collations(
        Some(CollationSeq::NoCase),
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
        resolve_comparison_collseq(&lhs, &rhs, &table_refs).unwrap(),
        CollationSeq::NoCase
    );
    // Swapped: RHS has NOCASE, LHS has no collation → still NOCASE
    assert_eq!(
        resolve_comparison_collseq(&rhs, &lhs, &table_refs).unwrap(),
        CollationSeq::NoCase
    );
}

#[test]
fn test_resolve_comparison_collseq_explicit_beats_column() {
    // LHS column is NOCASE, but RHS has explicit RTRIM → RTRIM wins
    let table_refs = get_table_references_two_tables_single_column_with_collations(
        Some(CollationSeq::NoCase),
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
        resolve_comparison_collseq(&lhs, &rhs, &table_refs).unwrap(),
        CollationSeq::Rtrim
    );
}

#[test]
fn test_resolve_comparison_collseq_both_default_is_binary() {
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
        resolve_comparison_collseq(&lhs, &rhs, &table_refs).unwrap(),
        CollationSeq::Binary
    );
}

// Helpers //

fn get_table_references_single_table_single_column_with_collation(
    collation: Option<CollationSeq>,
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
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        identifier: "foo".to_string(),
        internal_id: TableInternalId::from(1),
        join_info: None,
        table,
        plan_estimate: None,
        indexed: None,
    });

    table_references
}

fn get_table_references_two_tables_single_column_with_collations(
    left: Option<CollationSeq>,
    right: Option<CollationSeq>,
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
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        identifier: "t1".to_string(),
        internal_id: TableInternalId::from(1),
        join_info: None,
        plan_estimate: None,
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
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        identifier: "t2".to_string(),
        internal_id: TableInternalId::from(2),
        join_info: None,
        plan_estimate: None,
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
    collation: Option<CollationSeq>,
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
            primary_key: true,
            rowid_alias: true,
            notnull: false,
            explicit_notnull: false,
            unique: true,
            hidden: false,
            notnull_conflict_clause: None,
        },
    )];
    table_references.add_joined_table(JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        identifier: "bar".to_string(),
        internal_id: TableInternalId::from(1),
        join_info: None,
        plan_estimate: None,
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
