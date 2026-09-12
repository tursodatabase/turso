use super::{
    consider_multi_index_intersection, consider_multi_index_union, AnalyzeStats,
    MultiIndexBranchParams,
};
use crate::alloc::TursoIteratorExt;
use crate::alloc::TursoSliceExt;
use crate::{
    schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table, Type,
    },
    translate::{
        optimizer::{
            access_method::AccessMethodParams,
            cost::{Cost, RowCountEstimate},
            cost_params::DEFAULT_PARAMS,
            AvailableIndexes,
        },
        plan::{
            ColumnUsedMask, JoinInfo, JoinType, JoinedTable, Operation, TableReferences, WhereTerm,
        },
        planner::TableMask,
    },
    vdbe::builder::TableRefIdCounter,
    MAIN_DB_ID,
};
use std::{collections::VecDeque, sync::Arc};
use turso_parser::ast::{self, Expr, Operator, TableInternalId};

struct TestColumn {
    name: String,
    ty: Type,
    is_rowid_alias: bool,
}

fn empty_schema() -> Schema {
    Schema::default()
}

fn create_column(c: &TestColumn) -> Column {
    Column::new(
        Some(c.name.clone()),
        c.ty.to_string(),
        None,
        None,
        c.ty,
        None,
        ColDef {
            primary_key: false,
            rowid_alias: c.is_rowid_alias,
            ..Default::default()
        },
    )
}

fn create_column_of_type(name: &str, ty: Type) -> Column {
    create_column(&TestColumn {
        name: name.to_string(),
        ty,
        is_rowid_alias: false,
    })
}

fn create_btree_table(name: &str, columns: Vec<Column>) -> Arc<BTreeTable> {
    Arc::new(BTreeTable::new(
        1,
        name.to_string(),
        crate::alloc::vec![],
        columns.try_to_vec().expect(crate::alloc::ALLOC_ERR_MSG),
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ))
}

fn create_table_reference(
    table: Arc<BTreeTable>,
    join_info: Option<JoinInfo>,
    internal_id: TableInternalId,
) -> JoinedTable {
    let name = table.name.clone();
    let table = Table::BTree(table);
    JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        identifier: name,
        internal_id,
        join_info,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        plan_estimate: None,
        indexed: None,
    }
}

fn create_column_expr(table: TableInternalId, column: usize, is_rowid_alias: bool) -> Expr {
    Expr::Column {
        database: None,
        table,
        column,
        is_rowid_alias,
    }
}

fn create_numeric_literal(value: &str) -> Expr {
    Expr::Literal(ast::Literal::Numeric(value.to_string()))
}

fn create_string_literal(value: &str) -> Expr {
    Expr::Literal(ast::Literal::String(value.to_string()))
}

fn assert_is_multi_index(
    access_method: &crate::translate::optimizer::access_method::AccessMethod,
) -> &Vec<MultiIndexBranchParams> {
    let AccessMethodParams::MultiIndexScan { branches, .. } = &access_method.params else {
        panic!("expected multi-index scan access method");
    };
    branches
}

#[test]
fn test_multi_index_union_rejects_residuals_on_future_tables() {
    let link = create_btree_table(
        "link",
        vec![
            create_column_of_type("src", Type::Integer),
            create_column_of_type("dst", Type::Integer),
        ],
    );
    let item = create_btree_table(
        "item",
        vec![
            create_column_of_type("id", Type::Integer),
            create_column_of_type("kind", Type::Text),
        ],
    );
    let meta = create_btree_table(
        "meta",
        vec![
            create_column_of_type("id", Type::Integer),
            create_column_of_type("kind", Type::Text),
        ],
    );

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        create_table_reference(link, None, table_id_counter.next()),
        create_table_reference(
            item,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
        create_table_reference(
            meta,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const LINK: usize = 0;
    const ITEM: usize = 1;
    const META: usize = 2;

    let mut available_indexes = AvailableIndexes::default();
    available_indexes.insert_for_table_name(
        &joined_tables,
        "item",
        VecDeque::from([Arc::new(Index {
            name: "idx_item_id".to_string(),
            table_name: "item".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["id"]),
            unique: false,
            ephemeral: false,
            root_page: 2,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        })]),
    );

    let lhs_link_src = Expr::Binary(
        Box::new(create_column_expr(
            joined_tables[LINK].internal_id,
            0,
            false,
        )),
        Operator::Equals,
        Box::new(create_numeric_literal("1")),
    );
    let lhs_link_dst_item_id = Expr::Binary(
        Box::new(create_column_expr(
            joined_tables[LINK].internal_id,
            1,
            false,
        )),
        Operator::Equals,
        Box::new(create_column_expr(
            joined_tables[ITEM].internal_id,
            0,
            false,
        )),
    );
    let rhs_link_dst = Expr::Binary(
        Box::new(create_column_expr(
            joined_tables[LINK].internal_id,
            1,
            false,
        )),
        Operator::Equals,
        Box::new(create_numeric_literal("1")),
    );
    let rhs_link_src_item_id = Expr::Binary(
        Box::new(create_column_expr(
            joined_tables[LINK].internal_id,
            0,
            false,
        )),
        Operator::Equals,
        Box::new(create_column_expr(
            joined_tables[ITEM].internal_id,
            0,
            false,
        )),
    );
    let future_meta_kind = Expr::Binary(
        Box::new(create_column_expr(
            joined_tables[META].internal_id,
            1,
            false,
        )),
        Operator::Equals,
        Box::new(create_string_literal("entity")),
    );

    let left_disjunct = Expr::Binary(
        Box::new(Expr::Binary(
            Box::new(lhs_link_src),
            Operator::And,
            Box::new(lhs_link_dst_item_id),
        )),
        Operator::And,
        Box::new(future_meta_kind.clone()),
    );
    let right_disjunct = Expr::Binary(
        Box::new(Expr::Binary(
            Box::new(rhs_link_dst),
            Operator::And,
            Box::new(rhs_link_src_item_id),
        )),
        Operator::And,
        Box::new(future_meta_kind),
    );
    let where_clause = vec![WhereTerm {
        expr: Expr::Binary(
            Box::new(left_disjunct),
            Operator::Or,
            Box::new(right_disjunct),
        ),
        from_outer_join: None,
        consumed: false,
    }];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);
    let lhs_mask: TableMask = [LINK].into_iter().try_collect().unwrap();

    let access_method = consider_multi_index_union(
        &table_references.joined_tables()[ITEM],
        &where_clause,
        &available_indexes,
        &table_references,
        &[],
        &empty_schema(),
        1.0,
        base_row_count,
        &DEFAULT_PARAMS,
        Cost(f64::INFINITY),
        &lhs_mask,
        &AnalyzeStats::default(),
    )
    .unwrap();

    assert!(
        access_method.is_none(),
        "future-table residuals must not produce a multi-index OR access method"
    );
}

#[test]
fn test_multi_index_intersection_supports_rowid_and_secondary_index_branches() {
    let item = create_btree_table(
        "item",
        vec![
            create_column(&TestColumn {
                name: "id".to_string(),
                ty: Type::Integer,
                is_rowid_alias: true,
            }),
            create_column_of_type("a", Type::Integer),
        ],
    );

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![create_table_reference(item, None, table_id_counter.next())];
    let item_id = joined_tables[0].internal_id;

    let mut available_indexes = AvailableIndexes::default();
    available_indexes.insert_for_table_name(
        &joined_tables,
        "item",
        VecDeque::from([Arc::new(Index {
            name: "idx_item_a".to_string(),
            table_name: "item".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("a", 1)],
            unique: false,
            ephemeral: false,
            root_page: 2,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        })]),
    );

    let where_clause = vec![
        WhereTerm {
            expr: Expr::Binary(
                Box::new(create_column_expr(item_id, 0, true)),
                Operator::Greater,
                Box::new(create_numeric_literal("10")),
            ),
            from_outer_join: None,
            consumed: false,
        },
        WhereTerm {
            expr: Expr::Binary(
                Box::new(create_column_expr(item_id, 1, false)),
                Operator::Equals,
                Box::new(create_numeric_literal("7")),
            ),
            from_outer_join: None,
            consumed: false,
        },
    ];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

    let access_method = consider_multi_index_intersection(
        &table_references.joined_tables()[0],
        &where_clause,
        &available_indexes,
        &table_references,
        &[],
        &empty_schema(),
        1.0,
        base_row_count,
        &DEFAULT_PARAMS,
        Cost(f64::INFINITY),
        &TableMask::default(),
        &AnalyzeStats::default(),
    )
    .unwrap()
    .expect("rowid and secondary-index terms should be eligible for intersection");

    let branches = assert_is_multi_index(&access_method);
    assert_eq!(branches.len(), 2);
    assert!(
        branches.iter().any(|branch| branch.index.is_none()),
        "expected one rowid branch"
    );
    assert!(
        branches
            .iter()
            .any(|branch| branch.index.as_ref().map(|idx| idx.name.as_str()) == Some("idx_item_a")),
        "expected one secondary-index branch"
    );
}

#[test]
fn test_multi_index_union_branch_reuses_compound_seek_analysis() {
    let link = create_btree_table(
        "link",
        vec![
            create_column_of_type("src", Type::Integer),
            create_column_of_type("dst", Type::Integer),
        ],
    );
    let item = create_btree_table(
        "item",
        vec![
            create_column_of_type("id", Type::Integer),
            create_column_of_type("kind", Type::Integer),
        ],
    );

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        create_table_reference(link, None, table_id_counter.next()),
        create_table_reference(
            item,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const LINK: usize = 0;
    const ITEM: usize = 1;

    let mut available_indexes = AvailableIndexes::default();
    available_indexes.insert_for_table_name(
        &joined_tables,
        "item",
        VecDeque::from([Arc::new(Index {
            name: "idx_item_id_kind".to_string(),
            table_name: "item".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["id", "kind"]),
            unique: false,
            ephemeral: false,
            root_page: 2,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        })]),
    );

    let left_disjunct = Expr::Binary(
        Box::new(Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[LINK].internal_id,
                    0,
                    false,
                )),
                Operator::Equals,
                Box::new(create_numeric_literal("1")),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[ITEM].internal_id,
                    0,
                    false,
                )),
                Operator::Equals,
                Box::new(create_column_expr(
                    joined_tables[LINK].internal_id,
                    1,
                    false,
                )),
            )),
        )),
        Operator::And,
        Box::new(Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[ITEM].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_numeric_literal("7")),
        )),
    );
    let right_disjunct = Expr::Binary(
        Box::new(Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[LINK].internal_id,
                    1,
                    false,
                )),
                Operator::Equals,
                Box::new(create_numeric_literal("1")),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[ITEM].internal_id,
                    0,
                    false,
                )),
                Operator::Equals,
                Box::new(create_column_expr(
                    joined_tables[LINK].internal_id,
                    0,
                    false,
                )),
            )),
        )),
        Operator::And,
        Box::new(Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[ITEM].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_numeric_literal("7")),
        )),
    );

    let where_clause = vec![WhereTerm {
        expr: Expr::Binary(
            Box::new(left_disjunct),
            Operator::Or,
            Box::new(right_disjunct),
        ),
        from_outer_join: None,
        consumed: false,
    }];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let lhs_mask = [LINK].into_iter().try_collect().unwrap();
    let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

    let access_method = consider_multi_index_union(
        &table_references.joined_tables()[ITEM],
        &where_clause,
        &available_indexes,
        &table_references,
        &[],
        &empty_schema(),
        1.0,
        base_row_count,
        &DEFAULT_PARAMS,
        Cost(f64::INFINITY),
        &lhs_mask,
        &AnalyzeStats::default(),
    )
    .unwrap()
    .expect("compound OR branches should produce a multi-index union");

    let branches = assert_is_multi_index(&access_method);
    assert_eq!(branches.len(), 2);
    for branch in branches {
        assert_eq!(
            branch.index.as_ref().map(|idx| idx.name.as_str()),
            Some("idx_item_id_kind")
        );
        let super::MultiIndexBranchAccessParams::Seek {
            constraint_refs, ..
        } = &branch.access
        else {
            panic!("compound OR test should choose ordinary seek branches");
        };
        assert_eq!(
            constraint_refs.len(),
            2,
            "branch should use both id and kind in the compound seek"
        );
    }
}

#[test]
fn test_multi_index_union_residual_selectivity_reduces_row_estimate() {
    let link = create_btree_table(
        "link",
        vec![
            create_column_of_type("src", Type::Integer),
            create_column_of_type("dst", Type::Integer),
        ],
    );
    let item = create_btree_table(
        "item",
        vec![
            create_column_of_type("id", Type::Integer),
            create_column_of_type("kind", Type::Integer),
        ],
    );

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        create_table_reference(link, None, table_id_counter.next()),
        create_table_reference(
            item,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const LINK: usize = 0;
    const ITEM: usize = 1;
    let link_id = joined_tables[LINK].internal_id;
    let item_id = joined_tables[ITEM].internal_id;

    let mut available_indexes = AvailableIndexes::default();
    available_indexes.insert_for_table_name(
        &joined_tables,
        "item",
        VecDeque::from([Arc::new(Index {
            name: "idx_item_id".to_string(),
            table_name: "item".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["id"]),
            unique: false,
            ephemeral: false,
            root_page: 2,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        })]),
    );

    let make_branch = |literal_col, join_col, item_kind: Option<&str>| {
        let branch = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(create_column_expr(link_id, literal_col, false)),
                Operator::Equals,
                Box::new(create_numeric_literal("1")),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(item_id, 0, false)),
                Operator::Equals,
                Box::new(create_column_expr(link_id, join_col, false)),
            )),
        );

        if let Some(kind) = item_kind {
            Expr::Binary(
                Box::new(branch),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(item_id, 1, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal(kind)),
                )),
            )
        } else {
            branch
        }
    };
    let make_join_expr = |item_kind: Option<&str>| {
        vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(make_branch(0, 1, item_kind)),
                Operator::Or,
                Box::new(make_branch(1, 0, item_kind)),
            ),
            from_outer_join: None,
            consumed: false,
        }]
    };

    let table_references = TableReferences::new(joined_tables, vec![]);
    let lhs_mask = [LINK].into_iter().try_collect().unwrap();
    let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

    let without_residual = consider_multi_index_union(
        &table_references.joined_tables()[ITEM],
        &make_join_expr(None),
        &available_indexes,
        &table_references,
        &[],
        &empty_schema(),
        1.0,
        base_row_count,
        &DEFAULT_PARAMS,
        Cost(f64::INFINITY),
        &lhs_mask,
        &AnalyzeStats::default(),
    )
    .unwrap()
    .expect("plain OR branches should produce a multi-index union");

    let with_residual = consider_multi_index_union(
        &table_references.joined_tables()[ITEM],
        &make_join_expr(Some("7")),
        &available_indexes,
        &table_references,
        &[],
        &empty_schema(),
        1.0,
        base_row_count,
        &DEFAULT_PARAMS,
        Cost(f64::INFINITY),
        &lhs_mask,
        &AnalyzeStats::default(),
    )
    .unwrap()
    .expect("residual-filtered OR branches should still produce a multi-index union");

    assert!(
        with_residual.estimated_rows_per_outer_row < without_residual.estimated_rows_per_outer_row,
        "branch-local residual filters must reduce the multi-index row estimate"
    );
}
