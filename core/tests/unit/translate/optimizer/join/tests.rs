use std::{collections::VecDeque, sync::Arc};

use turso_parser::ast::{self, Expr, Operator, TableInternalId};

use super::*;
use crate::alloc::TursoSliceExt;
use crate::{
    schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table, Type,
    },
    stats::AnalyzeStats,
    translate::{
        optimizer::{
            access_method::AccessMethodParams,
            constraints::{constraints_from_where_clause, BinaryExprSide, RangeConstraintRef},
            cost_params::DEFAULT_PARAMS,
        },
        plan::{
            ColumnUsedMask, IterationDirection, JoinInfo, JoinType, Operation, TableReferences,
            WhereTerm,
        },
    },
    vdbe::builder::TableRefIdCounter,
    MAIN_DB_ID,
};

fn default_base_rows(n: usize) -> Vec<RowCountEstimate> {
    vec![RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS); n]
}

fn empty_schema() -> Schema {
    Schema::default()
}

fn single_table_plan_cost(where_expr: Expr) -> Cost {
    let table = _create_btree_table("test_table", _create_column_list(&["value"], Type::Integer));
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![_create_table_reference(
        table,
        None,
        table_id_counter.next(),
    )];
    let table_references = TableReferences::new(joined_tables, vec![]);
    let available_indexes = AvailableIndexes::default();
    let mut where_clause = vec![WhereTerm::from(where_expr)];
    let constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();
    let mut access_methods = Vec::new();
    let base_rows = default_base_rows(1);
    let schema = empty_schema();
    compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &constraints,
        &base_rows,
        &mut access_methods,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap()
    .best_plan
    .cost
}

#[test]
fn automatic_index_puts_equalities_before_ranges() {
    let mut table_id_counter = TableRefIdCounter::new();
    let outer = _create_table_reference(
        _create_btree_table("outer_rows", _create_column_list(&["k"], Type::Integer)),
        None,
        table_id_counter.next(),
    );
    let inner = _create_table_reference(
        _create_btree_table(
            "inner_rows",
            _create_column_list(&["x", "k"], Type::Integer),
        ),
        Some(JoinInfo {
            join_type: JoinType::Inner,
            using: vec![],
            no_reorder: false,
        }),
        table_id_counter.next(),
    );
    let inner_id = inner.internal_id;
    let outer_id = outer.internal_id;
    let table_references = TableReferences::new(vec![outer, inner], vec![]);
    let where_clause = vec![
        _create_binary_expr(
            _create_column_expr(inner_id, 0, false),
            Operator::Greater,
            _create_numeric_literal("10"),
        ),
        _create_binary_expr(
            _create_column_expr(inner_id, 1, false),
            Operator::Equals,
            _create_column_expr(outer_id, 0, false),
        ),
    ];
    let constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &AvailableIndexes::default(),
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let operators: Vec<_> = constraints[1]
        .temporary_index_terms
        .iter()
        .map(|term| {
            constraints[1].constraints[term.constraint_vec_pos]
                .operator
                .as_ast_operator()
                .unwrap()
        })
        .collect();

    assert_eq!(operators, vec![Operator::Equals, Operator::Greater]);
}

/// `WHERE` work waits for every table it needs.
#[test]
fn where_work_runs_after_the_needed_tables() -> Result<()> {
    let mut table_id_counter = TableRefIdCounter::new();
    let first_id = table_id_counter.next();
    let second_id = table_id_counter.next();
    let joined_tables = vec![
        _create_table_reference(
            _create_btree_table("first", _create_column_list(&["value"], Type::Integer)),
            None,
            first_id,
        ),
        _create_table_reference(
            _create_btree_table("second", _create_column_list(&["value"], Type::Integer)),
            None,
            second_id,
        ),
    ];
    let table_references = TableReferences::new(joined_tables, vec![]);
    let check = |table_id| {
        _create_binary_expr(
            _create_column_expr(table_id, 0, false),
            Operator::Equals,
            _create_numeric_literal("1"),
        )
        .expr
    };
    let two_table_where = vec![WhereTerm::from(Expr::Binary(
        Box::new(check(first_id)),
        Operator::Or,
        Box::new(check(second_id)),
    ))];
    let where_terms = build_where_term_info(&two_table_where, &table_references, &[])?;

    let mut joined_mask = TableMask::default();
    joined_mask.set(0)?;
    assert!(ready_where_work(&two_table_where, &where_terms, &joined_mask, 0, first_id).is_empty());

    joined_mask.set(1)?;
    let ready = ready_where_work(&two_table_where, &where_terms, &joined_mask, 1, second_id);
    assert_eq!(ready.as_slice(), &[(0, where_terms[0].extra_steps)]);
    let mut term = WhereTerm::from(Expr::Binary(
        Box::new(check(first_id)),
        Operator::Or,
        Box::new(check(first_id)),
    ));
    term.from_outer_join = Some(second_id);
    let outer_join_where = vec![term];
    let where_terms = build_where_term_info(&outer_join_where, &table_references, &[])?;

    let mut joined_mask = TableMask::default();
    joined_mask.set(0)?;
    assert!(
        ready_where_work(&outer_join_where, &where_terms, &joined_mask, 0, first_id).is_empty()
    );

    joined_mask.set(1)?;
    let ready = ready_where_work(&outer_join_where, &where_terms, &joined_mask, 1, second_id);
    assert_eq!(ready.as_slice(), &[(0, where_terms[0].extra_steps)]);
    Ok(())
}

#[test]
fn connected_component_finishes_before_a_cross_join() -> Result<()> {
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = (0..4)
        .map(|index| {
            _create_table_reference(
                _create_btree_table(
                    &format!("table_{index}"),
                    _create_column_list(&["key"], Type::Integer),
                ),
                None,
                table_id_counter.next(),
            )
        })
        .collect::<Vec<_>>();
    let mut where_clause = vec![
        _create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, false),
            Operator::Equals,
            _create_column_expr(joined_tables[1].internal_id, 0, false),
        ),
        _create_binary_expr(
            _create_column_expr(joined_tables[2].internal_id, 0, false),
            Operator::Equals,
            _create_column_expr(joined_tables[3].internal_id, 0, false),
        ),
    ];
    let table_references = TableReferences::new(joined_tables, vec![]);
    let available_indexes = AvailableIndexes::default();
    let constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )?;
    let base_table_rows = [1.0, 1_000_000.0, 10.0, 10.0].map(RowCountEstimate::HardcodedFallback);
    let mut access_methods = Vec::new();
    let schema = empty_schema();
    let plan = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &constraints,
        &base_table_rows,
        &mut access_methods,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )?
    .unwrap()
    .best_plan;
    let order = plan.table_numbers().collect::<Vec<_>>();
    let component = |table| table / 2;

    assert_eq!(component(order[0]), component(order[1]), "order: {order:?}");
    assert_eq!(component(order[2]), component(order[3]), "order: {order:?}");
    assert_ne!(component(order[1]), component(order[2]), "order: {order:?}");
    Ok(())
}

#[test]
fn equality_class_connects_columns_through_a_third_table() -> Result<()> {
    let (table_references, table_ids) =
        equality_test_tables([Type::Integer, Type::Integer, Type::Integer]);
    let mut where_clause = vec![
        _create_binary_expr(
            _create_column_expr(table_ids[0], 0, false),
            Operator::Equals,
            _create_column_expr(table_ids[1], 0, false),
        ),
        _create_binary_expr(
            _create_column_expr(table_ids[1], 0, false),
            Operator::Equals,
            _create_column_expr(table_ids[2], 0, false),
        ),
    ];

    super::super::constraints::add_implied_column_equalities(&mut where_clause, &table_references)?;

    assert_eq!(where_clause.len(), 3);
    assert!(where_clause[2].consumed);
    assert_eq!(
        table_mask_from_expr(&where_clause[2].expr, &table_references, &[])?,
        TableMask::try_from(0b101_u128)?
    );
    Ok(())
}

#[test]
fn equality_class_does_not_cross_column_affinities() -> Result<()> {
    let (table_references, table_ids) =
        equality_test_tables([Type::Integer, Type::Integer, Type::Text]);
    let mut where_clause = vec![
        _create_binary_expr(
            _create_column_expr(table_ids[0], 0, false),
            Operator::Equals,
            _create_column_expr(table_ids[1], 0, false),
        ),
        _create_binary_expr(
            _create_column_expr(table_ids[1], 0, false),
            Operator::Equals,
            _create_column_expr(table_ids[2], 0, false),
        ),
    ];

    super::super::constraints::add_implied_column_equalities(&mut where_clause, &table_references)?;

    assert_eq!(where_clause.len(), 2);
    Ok(())
}

#[test]
fn equality_class_does_not_link_rowid_aliases() -> Result<()> {
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = (0..3)
        .map(|index| {
            _create_table_reference(
                _create_btree_table(
                    &format!("table_{index}"),
                    vec![_create_column_rowid_alias("id")],
                ),
                None,
                table_id_counter.next(),
            )
        })
        .collect::<Vec<_>>();
    let table_references = TableReferences::new(joined_tables, vec![]);
    let table_ids: [TableInternalId; 3] =
        std::array::from_fn(|index| table_references.joined_tables()[index].internal_id);
    let mut where_clause = vec![
        _create_binary_expr(
            _create_column_expr(table_ids[0], 0, true),
            Operator::Equals,
            _create_column_expr(table_ids[1], 0, true),
        ),
        _create_binary_expr(
            _create_column_expr(table_ids[1], 0, true),
            Operator::Equals,
            _create_column_expr(table_ids[2], 0, true),
        ),
    ];

    super::super::constraints::add_implied_column_equalities(&mut where_clause, &table_references)?;

    assert_eq!(where_clause.len(), 2);
    Ok(())
}

fn equality_test_tables(column_types: [Type; 3]) -> (TableReferences, [TableInternalId; 3]) {
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = column_types
        .into_iter()
        .enumerate()
        .map(|(index, column_type)| {
            _create_table_reference(
                _create_btree_table(
                    &format!("table_{index}"),
                    _create_column_list(&["key"], column_type),
                ),
                None,
                table_id_counter.next(),
            )
        })
        .collect::<Vec<_>>();
    let table_references = TableReferences::new(joined_tables, vec![]);
    let table_ids =
        std::array::from_fn(|index| table_references.joined_tables()[index].internal_id);
    (table_references, table_ids)
}

#[test]
fn test_generate_bitmasks() -> std::result::Result<(), TryReserveError> {
    let bitmasks = generate_join_bitmasks(4, 2).collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(bitmasks.contains(&TableMask::try_from(0b0011u128)?)); // {0,1}
    assert!(bitmasks.contains(&TableMask::try_from(0b0101u128)?)); // {0,2}
    assert!(bitmasks.contains(&TableMask::try_from(0b0110u128)?)); // {1,2}
    assert!(bitmasks.contains(&TableMask::try_from(0b1001u128)?)); // {0,3}
    assert!(bitmasks.contains(&TableMask::try_from(0b1010u128)?)); // {1,3}
    assert!(bitmasks.contains(&TableMask::try_from(0b1100u128)?)); // {2,3}
    Ok(())
}

#[test]
fn test_seek_score_accounts_for_composite_index_prefix() {
    let mut table_id_counter = TableRefIdCounter::new();
    let t1 = _create_btree_table("table1", _create_column_list(&["x"], Type::Integer));
    let t2 = _create_btree_table(
        "table2",
        _create_column_list(&["x", "y", "z"], Type::Integer),
    );
    let joined_tables = vec![
        _create_table_reference(t1, None, table_id_counter.next()),
        _create_table_reference(
            t2,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const TABLE1: usize = 0;
    const TABLE2: usize = 1;

    let where_clause = vec![
        _create_binary_expr(
            _create_column_expr(joined_tables[TABLE2].internal_id, 0, false),
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE1].internal_id, 0, false),
        ),
        _create_binary_expr(
            _create_column_expr(joined_tables[TABLE2].internal_id, 1, false),
            ast::Operator::Equals,
            _create_numeric_literal("1"),
        ),
        _create_binary_expr(
            _create_column_expr(joined_tables[TABLE2].internal_id, 2, false),
            ast::Operator::Equals,
            _create_numeric_literal("2"),
        ),
    ];

    let single_col_index = _create_index("idx_table2_x", "table2", &[("x", 0)], false);
    let composite_index = _create_index(
        "idx_table2_xyz",
        "table2",
        &[("x", 0), ("y", 1), ("z", 2)],
        false,
    );

    let single_col_score = seek_score_for_indexes(
        &joined_tables,
        &where_clause,
        VecDeque::from([single_col_index]),
    );
    let composite_score = seek_score_for_indexes(
        &joined_tables,
        &where_clause,
        VecDeque::from([composite_index]),
    );

    assert!(composite_score > single_col_score);
}

#[test]
fn plan_cost_counts_long_where_condition() {
    let table_id = TableInternalId::default();
    let check = |value| {
        Expr::Binary(
            Box::new(_create_column_expr(table_id, 0, false)),
            Operator::Equals,
            Box::new(_create_numeric_literal(value)),
        )
    };
    let simple_cost = single_table_plan_cost(check("1"));
    let long_cost = single_table_plan_cost(Expr::Binary(
        Box::new(check("1")),
        Operator::Or,
        Box::new(check("2")),
    ));

    assert!(
        long_cost > simple_cost,
        "simple cost: {simple_cost:?}, long cost: {long_cost:?}"
    );
}

#[test]
/// Test that [compute_best_join_order] returns None when there are no table references.
fn test_compute_best_join_order_empty() {
    let table_references = TableReferences::new(vec![], vec![]);
    let available_indexes = AvailableIndexes::default();
    let mut where_clause = vec![];

    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_none());
}

#[test]
/// Test that [compute_best_join_order] returns a table scan access method when the where clause is empty.
fn test_compute_best_join_order_single_table_no_indexes() {
    let t1 = _create_btree_table("test_table", _create_column_list(&["id"], Type::Integer));
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];
    let table_references = TableReferences::new(joined_tables, vec![]);
    let available_indexes = AvailableIndexes::default();
    let mut where_clause = vec![];

    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    // SELECT * from test_table
    // expecting best_best_plan() not to do any work due to empty where clause.
    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap();
    // Should just be a table scan access method
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, _, constraint_refs) = _as_btree(access_method);
    assert!(constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
}

#[test]
/// Test that [compute_best_join_order] returns a RowidEq access method when the where clause has an EQ constraint on the rowid alias.
fn test_compute_best_join_order_single_table_rowid_eq() {
    let t1 = _create_btree_table("test_table", vec![_create_column_rowid_alias("id")]);
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];

    let mut where_clause = vec![_create_binary_expr(
        _create_column_expr(joined_tables[0].internal_id, 0, true), // table 0, column 0 (rowid)
        ast::Operator::Equals,
        _create_numeric_literal("42"),
    )];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let available_indexes = AvailableIndexes::default();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    // SELECT * FROM test_table WHERE id = 42
    // expecting a RowidEq access method because id is a rowid alias.
    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_some());
    let BestJoinOrderResult { best_plan, .. } = result.unwrap();
    assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, _, constraint_refs) = _as_btree(access_method);
    assert!(!constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(constraint_refs.len() == 1);
    assert!(
        table_constraints[0].constraints[constraint_refs[0].eq.as_ref().unwrap().constraint_pos]
            .where_clause_pos
            == (0, BinaryExprSide::Rhs)
    );
}

#[test]
/// Test that [compute_best_join_order] returns an IndexScan access method when the where clause has an EQ constraint on a primary key.
fn test_compute_best_join_order_single_table_pk_eq() {
    let t1 = _create_btree_table(
        "test_table",
        vec![_create_column_of_type("id", Type::Integer)],
    );
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];

    let mut where_clause = vec![_create_binary_expr(
        _create_column_expr(joined_tables[0].internal_id, 0, false), // table 0, column 0 (id)
        ast::Operator::Equals,
        _create_numeric_literal("42"),
    )];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let mut available_indexes = AvailableIndexes::default();
    let index = Arc::new(Index {
        name: "sqlite_autoindex_test_table_1".to_string(),
        table_name: "test_table".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![IndexColumn::new("id", 0)],
        unique: true,
        ephemeral: false,
        root_page: 1,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });
    available_indexes.insert_for_table_name(
        table_references.joined_tables(),
        "test_table",
        VecDeque::from([index]),
    );

    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();
    // SELECT * FROM test_table WHERE id = 42
    // expecting an IndexScan access method because id is a primary key with an index
    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_some());
    let BestJoinOrderResult { best_plan, .. } = result.unwrap();
    assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(!constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.as_ref().unwrap().name == "sqlite_autoindex_test_table_1");
    assert!(constraint_refs.len() == 1);
    assert!(
        table_constraints[0].constraints[constraint_refs[0].eq.as_ref().unwrap().constraint_pos]
            .where_clause_pos
            == (0, BinaryExprSide::Rhs)
    );
}

#[test]
/// Test that [compute_best_join_order] moves the outer table to the inner position when an index can be used on it, but not the original inner table.
fn test_compute_best_join_order_two_tables() {
    let t1 = _create_btree_table("table1", _create_column_list(&["id"], Type::Integer));
    let t2 = _create_btree_table("table2", _create_column_list(&["id"], Type::Integer));

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        _create_table_reference(t1, None, table_id_counter.next()),
        _create_table_reference(
            t2,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const TABLE1: usize = 0;
    const TABLE2: usize = 1;

    let mut available_indexes = AvailableIndexes::default();
    // Index on the outer table (table1)
    let index1 = Arc::new(Index {
        name: "index1".to_string(),
        table_name: "table1".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![IndexColumn::new("id", 0)],
        unique: true,
        ephemeral: false,
        root_page: 1,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });
    available_indexes.insert_for_table_name(&joined_tables, "table1", VecDeque::from([index1]));

    // SELECT * FROM table1 JOIN table2 WHERE table1.id = table2.id
    // expecting table2 to be chosen first due to the index on table1.id
    let mut where_clause = vec![_create_binary_expr(
        _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // table1.id
        ast::Operator::Equals,
        _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // table2.id
    )];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_some());
    let BestJoinOrderResult { best_plan, .. } = result.unwrap();
    assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![1, 0]);
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, _, constraint_refs) = _as_btree(access_method);
    assert!(constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    let access_method = &access_methods_arena[best_plan.data[1].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(!constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.as_ref().unwrap().name == "index1");
    assert!(constraint_refs.len() == 1);
    assert!(
        table_constraints[TABLE1].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos]
            .where_clause_pos
            == (0, BinaryExprSide::Rhs)
    );
}

#[test]
/// Test that [compute_best_join_order] returns a sensible order and plan for three tables, each with indexes.
fn test_compute_best_join_order_three_tables_indexed() {
    let table_orders = _create_btree_table(
        "orders",
        vec![
            _create_column_of_type("id", Type::Integer),
            _create_column_of_type("customer_id", Type::Integer),
            _create_column_of_type("total", Type::Integer),
        ],
    );
    let table_customers = _create_btree_table(
        "customers",
        vec![
            _create_column_of_type("id", Type::Integer),
            _create_column_of_type("name", Type::Integer),
        ],
    );
    let table_order_items = _create_btree_table(
        "order_items",
        vec![
            _create_column_of_type("id", Type::Integer),
            _create_column_of_type("order_id", Type::Integer),
            _create_column_of_type("product_id", Type::Integer),
            _create_column_of_type("quantity", Type::Integer),
        ],
    );

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        _create_table_reference(table_orders, None, table_id_counter.next()),
        _create_table_reference(
            table_customers,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
        _create_table_reference(
            table_order_items,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const TABLE_NO_ORDERS: usize = 0;
    const TABLE_NO_CUSTOMERS: usize = 1;
    const TABLE_NO_ORDER_ITEMS: usize = 2;

    let mut available_indexes = AvailableIndexes::default();
    ["orders", "customers", "order_items"]
        .iter()
        .for_each(|table_name| {
            // add primary key index called sqlite_autoindex_<tablename>_1
            let index_name = format!("sqlite_autoindex_{table_name}_1");
            let index = Arc::new(Index {
                name: index_name,
                where_clause: None,
                table_name: table_name.to_string(),
                columns: crate::alloc::vec![IndexColumn::new("id", 0)],
                unique: true,
                ephemeral: false,
                root_page: 1,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            });
            available_indexes.insert_for_table_name(
                &joined_tables,
                table_name,
                VecDeque::from([index]),
            );
        });
    let customer_id_idx = Arc::new(Index {
        name: "orders_customer_id_idx".to_string(),
        table_name: "orders".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![IndexColumn::new("customer_id", 1)],
        unique: false,
        ephemeral: false,
        root_page: 1,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });
    let order_id_idx = Arc::new(Index {
        name: "order_items_order_id_idx".to_string(),
        table_name: "order_items".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![IndexColumn::new("order_id", 1)],
        unique: false,
        ephemeral: false,
        root_page: 1,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });

    available_indexes.push_front_for_table_name(&joined_tables, "orders", customer_id_idx);
    available_indexes.push_front_for_table_name(&joined_tables, "order_items", order_id_idx);

    // SELECT * FROM orders JOIN customers JOIN order_items
    // WHERE orders.customer_id = customers.id AND orders.id = order_items.order_id AND customers.id = 42
    // expecting customers to be chosen first due to the index on customers.id and it having a selective filter (=42)
    // then orders to be chosen next due to the index on orders.customer_id
    // then order_items to be chosen last due to the index on order_items.order_id
    let mut where_clause = vec![
        // orders.customer_id = customers.id
        _create_binary_expr(
            _create_column_expr(joined_tables[TABLE_NO_ORDERS].internal_id, 1, false), // orders.customer_id
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE_NO_CUSTOMERS].internal_id, 0, false), // customers.id
        ),
        // orders.id = order_items.order_id
        _create_binary_expr(
            _create_column_expr(joined_tables[TABLE_NO_ORDERS].internal_id, 0, false), // orders.id
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE_NO_ORDER_ITEMS].internal_id, 1, false), // order_items.order_id
        ),
        // customers.id = 42
        _create_binary_expr(
            _create_column_expr(joined_tables[TABLE_NO_CUSTOMERS].internal_id, 0, false), // customers.id
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        ),
    ];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_some());
    let BestJoinOrderResult { best_plan, .. } = result.unwrap();

    // Customers (due to =42 filter) -> Orders (due to index on customer_id) -> Order_items (due to index on order_id)
    assert_eq!(
        best_plan.table_numbers().collect::<Vec<_>>(),
        vec![TABLE_NO_CUSTOMERS, TABLE_NO_ORDERS, TABLE_NO_ORDER_ITEMS]
    );

    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.as_ref().unwrap().name == "sqlite_autoindex_customers_1");
    assert!(constraint_refs.len() == 1);
    let constraint = &table_constraints[TABLE_NO_CUSTOMERS].constraints
        [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
    assert!(constraint.lhs_mask.is_empty());

    let access_method = &access_methods_arena[best_plan.data[1].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.as_ref().unwrap().name == "orders_customer_id_idx");
    assert!(constraint_refs.len() == 1);
    let constraint = &table_constraints[TABLE_NO_ORDERS].constraints
        [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
    assert!(constraint.lhs_mask.get(TABLE_NO_CUSTOMERS));

    let access_method = &access_methods_arena[best_plan.data[2].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.as_ref().unwrap().name == "order_items_order_id_idx");
    assert!(constraint_refs.len() == 1);
    let constraint = &table_constraints[TABLE_NO_ORDER_ITEMS].constraints
        [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
    assert!(constraint.lhs_mask.get(TABLE_NO_ORDERS));
}

struct TestColumn {
    name: String,
    ty: Type,
    is_rowid_alias: bool,
}

impl Default for TestColumn {
    fn default() -> Self {
        Self {
            name: "a".to_string(),
            ty: Type::Integer,
            is_rowid_alias: false,
        }
    }
}

#[test]
fn test_join_order_three_tables_no_indexes() {
    let t1 = _create_btree_table("t1", _create_column_list(&["id", "foo"], Type::Integer));
    let t2 = _create_btree_table("t2", _create_column_list(&["id", "foo"], Type::Integer));
    let t3 = _create_btree_table("t3", _create_column_list(&["id", "foo"], Type::Integer));

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        _create_table_reference(t1, None, table_id_counter.next()),
        _create_table_reference(
            t2,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
        _create_table_reference(
            t3,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    let mut where_clause = vec![
        // t2.foo = 42 (equality filter, more selective)
        _create_binary_expr(
            _create_column_expr(joined_tables[1].internal_id, 1, false), // table 1, column 1 (foo)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        ),
        // t1.foo > 10 (inequality filter, less selective)
        _create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 1, false), // table 0, column 1 (foo)
            ast::Operator::Greater,
            _create_numeric_literal("10"),
        ),
    ];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let available_indexes = AvailableIndexes::default();
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap();

    // Put the table with no filter first. The two inner tables can then
    // build an automatic index once instead of scanning once per outer row.
    assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![2, 1, 0]);

    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.is_none());

    let access_method = &access_methods_arena[best_plan.data[1].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.is_none());
    assert!(matches!(
        access_method.params,
        AccessMethodParams::BTreeTable {
            build_index: true,
            ..
        }
    ));

    let access_method = &access_methods_arena[best_plan.data[2].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(constraint_refs.is_empty());
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.is_none());
    assert!(matches!(
        access_method.params,
        AccessMethodParams::BTreeTable {
            build_index: true,
            ..
        }
    ));
}

#[test]
/// Test that [compute_best_join_order] chooses a "fact table" as the outer table,
/// when it has a foreign key to all dimension tables.
fn test_compute_best_join_order_star_schema() {
    const NUM_DIM_TABLES: usize = 9;
    const FACT_TABLE_IDX: usize = 9;

    // Create fact table with foreign keys to all dimension tables
    let mut fact_columns = vec![_create_column_rowid_alias("id")];
    for i in 0..NUM_DIM_TABLES {
        fact_columns.push(_create_column_of_type(&format!("dim{i}_id"), Type::Integer));
    }
    let fact_table = _create_btree_table("fact", fact_columns);

    // Create dimension tables, each with an id and value column
    let dim_tables: Vec<_> = (0..NUM_DIM_TABLES)
        .map(|i| {
            _create_btree_table(
                &format!("dim{i}"),
                vec![
                    _create_column_rowid_alias("id"),
                    _create_column_of_type("value", Type::Integer),
                ],
            )
        })
        .collect();

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = {
        let mut refs = vec![_create_table_reference(
            dim_tables[0].clone(),
            None,
            table_id_counter.next(),
        )];
        refs.extend(dim_tables.iter().skip(1).map(|t| {
            _create_table_reference(
                t.clone(),
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            )
        }));
        refs.push(_create_table_reference(
            fact_table,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ));
        refs
    };

    let mut where_clause = vec![];

    // Add join conditions between fact and each dimension table
    for i in 0..NUM_DIM_TABLES {
        let internal_id_fact = joined_tables[FACT_TABLE_IDX].internal_id;
        let internal_id_other = joined_tables[i].internal_id;
        where_clause.push(_create_binary_expr(
            _create_column_expr(internal_id_fact, i + 1, false), // fact.dimX_id
            ast::Operator::Equals,
            _create_column_expr(internal_id_other, 0, true), // dimX.id
        ));
    }

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let available_indexes = AvailableIndexes::default();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_some());
    let BestJoinOrderResult { best_plan, .. } = result.unwrap();

    // Expected optimal order: fact table as outer, with rowid seeks in any order on each dimension table
    // Verify fact table is selected as the outer table as all the other tables can use SeekRowid
    assert_eq!(
        best_plan.table_numbers().next().unwrap(),
        FACT_TABLE_IDX,
        "First table should be fact (table {}) due to available index, got table {} instead",
        FACT_TABLE_IDX,
        best_plan.table_numbers().next().unwrap()
    );

    // Verify access methods
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.is_none());
    assert!(constraint_refs.is_empty());

    for (table_number, access_method_index) in best_plan.data.iter().skip(1) {
        let access_method = &access_methods_arena[*access_method_index];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[*table_number].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.lhs_mask.get(FACT_TABLE_IDX));
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
    }
}

#[test]
/// Test that [compute_best_join_order] figures out that the tables form a "linked list" pattern
/// where a column in each table points to an indexed column in the next table,
/// and chooses the best order based on that.
fn test_compute_best_join_order_linked_list() {
    const NUM_TABLES: usize = 5;

    // Create tables t1 -> t2 -> t3 -> t4 -> t5 where there is a foreign key from each table to the next
    let mut tables = Vec::with_capacity(NUM_TABLES);
    for i in 0..NUM_TABLES {
        let mut columns = vec![_create_column_rowid_alias("id")];
        if i < NUM_TABLES - 1 {
            columns.push(_create_column_of_type("next_id", Type::Integer));
        }
        tables.push(_create_btree_table(&format!("t{}", i + 1), columns));
    }

    let available_indexes = AvailableIndexes::default();

    let mut table_id_counter = TableRefIdCounter::new();
    // Create table references
    let joined_tables: Vec<_> = tables
        .iter()
        .map(|t| _create_table_reference(t.clone(), None, table_id_counter.next()))
        .collect();

    // Create where clause linking each table to the next
    let mut where_clause = Vec::new();
    for i in 0..NUM_TABLES - 1 {
        let internal_id_left = joined_tables[i].internal_id;
        let internal_id_right = joined_tables[i + 1].internal_id;
        where_clause.push(_create_binary_expr(
            _create_column_expr(internal_id_left, 1, false), // ti.next_id
            ast::Operator::Equals,
            _create_column_expr(internal_id_right, 0, true), // t(i+1).id
        ));
    }

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    // Run the optimizer
    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap();

    // Verify the join order is exactly t1 -> t2 -> t3 -> t4 -> t5
    for i in 0..NUM_TABLES {
        assert_eq!(
            best_plan.table_numbers().nth(i).unwrap(),
            i,
            "Expected table {} at position {}, got table {} instead",
            i,
            i,
            best_plan.table_numbers().nth(i).unwrap()
        );
    }

    // Verify access methods:
    // - First table should use Table scan
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (iter_dir, index, constraint_refs) = _as_btree(access_method);
    assert!(iter_dir == IterationDirection::Forwards);
    assert!(index.is_none());
    assert!(constraint_refs.is_empty());

    // all of the rest should use rowid equality
    for (i, table_constraints) in table_constraints
        .iter()
        .enumerate()
        .take(NUM_TABLES)
        .skip(1)
    {
        let access_method = &access_methods_arena[best_plan.data[i].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(constraint_refs.len() == 1);
        let constraint =
            &table_constraints.constraints[constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.lhs_mask.get(i - 1));
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
    }
}

#[test]
/// Test that [compute_best_join_order] figures out that the index can't be used when only the second column is referenced
fn test_index_second_column_only() {
    let mut joined_tables = Vec::new();

    let mut table_id_counter = TableRefIdCounter::new();

    // Create a table with two columns
    let table = _create_btree_table("t1", _create_column_list(&["x", "y"], Type::Integer));

    // Create a two-column index on (x,y)
    let index = Arc::new(Index {
        name: "idx_xy".to_string(),
        table_name: "t1".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![IndexColumn::new("x", 0), IndexColumn::new("y", 1),],
        unique: false,
        root_page: 2,
        ephemeral: false,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });

    let mut available_indexes = AvailableIndexes::default();

    let table = Table::BTree(table);
    joined_tables.push(JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        internal_id: table_id_counter.next(),
        identifier: "t1".to_string(),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        plan_estimate: None,
        indexed: None,
    });
    available_indexes.insert_for_table_name(&joined_tables, "t1", VecDeque::from([index]));

    // Create where clause that only references second column
    let mut where_clause = vec![WhereTerm {
        expr: Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: joined_tables[0].internal_id,
                column: 1,
                is_rowid_alias: false,
            }),
            ast::Operator::Equals,
            Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
        ),
        from_outer_join: None,
        consumed: false,
    }];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap();

    // Verify access method is a scan, not a seek, because the index can't be used when only the second column is referenced
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (_, _, constraint_refs) = _as_btree(access_method);
    assert!(constraint_refs.is_empty());
}

#[test]
/// Test that an index with a gap in referenced columns (e.g. index on (a,b,c), where clause on a and c)
/// only uses the prefix before the gap.
fn test_index_skips_middle_column() {
    let mut table_id_counter = TableRefIdCounter::new();
    let mut joined_tables = Vec::new();
    let mut available_indexes = AvailableIndexes::default();

    let columns = _create_column_list(&["c1", "c2", "c3"], Type::Integer);
    let table = _create_btree_table("t1", columns);
    let index = Arc::new(Index {
        name: "idx1".to_string(),
        table_name: "t1".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![
            IndexColumn::new("c1", 0),
            IndexColumn::new("c2", 1),
            IndexColumn::new("c3", 2),
        ],
        unique: false,
        root_page: 2,
        ephemeral: false,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });
    let table = Table::BTree(table);
    joined_tables.push(JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        internal_id: table_id_counter.next(),
        identifier: "t1".to_string(),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        plan_estimate: None,
        indexed: None,
    });
    available_indexes.insert_for_table_name(&joined_tables, "t1", VecDeque::from([index]));

    // Create where clause that references first and third columns
    let mut where_clause = vec![
        WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 0, // c1
                    is_rowid_alias: false,
                }),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        },
        WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 2, // c3
                    is_rowid_alias: false,
                }),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(7.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        },
    ];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap();

    // Verify access method is a seek, and only uses the first column of the index
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (_, index, constraint_refs) = _as_btree(access_method);
    assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
    assert!(constraint_refs.len() == 1);
    let constraint =
        &table_constraints[0].constraints[constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
    assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
    assert!(constraint.table_col_pos == Some(0)); // c1
}

#[test]
/// Test that an index seek stops after a range operator.
/// e.g. index on (a,b,c), where clause a=1, b>2, c=3. Only a and b should be used for seek.
fn test_index_stops_at_range_operator() {
    let mut table_id_counter = TableRefIdCounter::new();
    let mut joined_tables = Vec::new();
    let mut available_indexes = AvailableIndexes::default();

    let columns = _create_column_list(&["c1", "c2", "c3"], Type::Integer);
    let table = _create_btree_table("t1", columns);
    let index = Arc::new(Index {
        name: "idx1".to_string(),
        table_name: "t1".to_string(),
        where_clause: None,
        columns: IndexColumn::new_many(vec!["c1", "c2", "c3"]),
        root_page: 2,
        ephemeral: false,
        has_rowid: true,
        unique: false,
        index_method: None,
        on_conflict: None,
    });
    let table = Table::BTree(table);
    joined_tables.push(JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        internal_id: table_id_counter.next(),
        identifier: "t1".to_string(),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: MAIN_DB_ID,
        plan_estimate: None,
        indexed: None,
    });
    available_indexes.insert_for_table_name(&joined_tables, "t1", VecDeque::from([index]));

    // Create where clause: c1 = 5 AND c2 > 10 AND c3 = 7
    let mut where_clause = vec![
        WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 0, // c1
                    is_rowid_alias: false,
                }),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        },
        WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 1, // c2
                    is_rowid_alias: false,
                }),
                ast::Operator::Greater,
                Box::new(Expr::Literal(ast::Literal::Numeric(10.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        },
        WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 2, // c3
                    is_rowid_alias: false,
                }),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(7.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        },
    ];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap()
    .unwrap();

    // Verify access method is a seek, and uses the first two columns of the index.
    // The third column can't be used because the second is a range query.
    let access_method = &access_methods_arena[best_plan.data[0].1];
    let (_, index, constraint_refs) = _as_btree(access_method);
    assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
    assert!(constraint_refs.len() == 2);
    let constraint =
        &table_constraints[0].constraints[constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
    assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
    assert!(constraint.table_col_pos == Some(0)); // c1
    let constraint = &table_constraints[0].constraints[constraint_refs[1].lower_bound.unwrap()];
    assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Greater));
    assert!(constraint.table_col_pos == Some(1)); // c2
}

fn _create_column(c: &TestColumn) -> Column {
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
fn _create_column_of_type(name: &str, ty: Type) -> Column {
    _create_column(&TestColumn {
        name: name.to_string(),
        ty,
        is_rowid_alias: false,
    })
}

fn _create_column_list(names: &[&str], ty: Type) -> Vec<Column> {
    names
        .iter()
        .map(|name| _create_column_of_type(name, ty))
        .collect()
}

fn _create_column_rowid_alias(name: &str) -> Column {
    _create_column(&TestColumn {
        name: name.to_string(),
        ty: Type::Integer,
        is_rowid_alias: true,
    })
}

/// Creates a BTreeTable with the given name and columns
fn _create_btree_table(name: &str, columns: Vec<Column>) -> Arc<BTreeTable> {
    Arc::new(BTreeTable::new(
        1, // root_page, doesn't matter for tests
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

fn _create_index(
    name: &str,
    table_name: &str,
    columns: &[(&str, usize)],
    unique: bool,
) -> Arc<Index> {
    Arc::new(Index {
        name: name.to_string(),
        table_name: table_name.to_string(),
        where_clause: None,
        columns: columns
            .iter()
            .map(|(name, pos_in_table)| IndexColumn::new((*name).to_string(), *pos_in_table))
            .try_collect()
            .unwrap(),
        unique,
        ephemeral: false,
        root_page: 1,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    })
}

/// Creates a TableReference for a BTreeTable
fn _create_table_reference(
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

/// Creates a column expression
fn _create_column_expr(table: TableInternalId, column: usize, is_rowid_alias: bool) -> Expr {
    Expr::Column {
        database: None,
        table,
        column,
        is_rowid_alias,
    }
}

/// Creates a binary expression for a WHERE clause
fn _create_binary_expr(lhs: Expr, op: Operator, rhs: Expr) -> WhereTerm {
    WhereTerm {
        expr: Expr::Binary(Box::new(lhs), op, Box::new(rhs)),
        from_outer_join: None,
        consumed: false,
    }
}

/// Creates a numeric literal expression
fn _create_numeric_literal(value: &str) -> Expr {
    Expr::Literal(ast::Literal::Numeric(value.to_string()))
}

fn seek_score_for_indexes(
    joined_tables: &[JoinedTable],
    where_clause: &[WhereTerm],
    indexes: VecDeque<Arc<Index>>,
) -> f64 {
    let mut available_indexes = AvailableIndexes::default();
    available_indexes.insert_for_table_name(joined_tables, "table2", indexes);
    let table_references = TableReferences::new(joined_tables.to_vec(), vec![]);
    let constraints = constraints_from_where_clause(
        where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let mut lhs_mask = TableMask::default();
    lhs_mask.set(0).unwrap();
    get_best_seek_score(
        &constraints[1],
        &lhs_mask,
        1,
        &joined_tables[1],
        RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS),
        &AnalyzeStats::default(),
        &DEFAULT_PARAMS,
    )
}

fn _as_btree(
    access_method: &AccessMethod,
) -> (
    IterationDirection,
    Option<Arc<Index>>,
    &'_ [RangeConstraintRef],
) {
    match &access_method.params {
        AccessMethodParams::BTreeTable {
            iter_dir,
            index,
            constraint_refs,
            ..
        } => (*iter_dir, index.clone(), constraint_refs),
        _ => panic!("expected BTreeTable access method"),
    }
}

#[test]
/// Test that when an index is available on the join column, the optimizer prefers
/// index lookup over hash join.
fn test_prefer_index_lookup_over_hash_join() {
    // CREATE TABLE t1(a,b,c);
    // CREATE TABLE t2(a,b,c);
    // CREATE INDEX idx_t2_a ON t2(a);
    // SELECT * FROM t1 JOIN t2 ON t1.a = t2.a;
    // Expected: SCAN t1, SEARCH t2 USING INDEX idx_t2_a (a=?)
    // Not: HASH JOIN

    let t1 = _create_btree_table("t1", _create_column_list(&["a", "b", "c"], Type::Integer));
    let t2 = _create_btree_table("t2", _create_column_list(&["a", "b", "c"], Type::Integer));

    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        _create_table_reference(t1, None, table_id_counter.next()),
        _create_table_reference(
            t2,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];

    const TABLE1: usize = 0;
    const TABLE2: usize = 1;

    // Index on t2.a
    let mut available_indexes = AvailableIndexes::default();
    let index_t2_a = Arc::new(Index {
        name: "idx_t2_a".to_string(),
        table_name: "t2".to_string(),
        where_clause: None,
        columns: crate::alloc::vec![IndexColumn::new("a", 0)],
        unique: false, // Non-unique index
        ephemeral: false,
        root_page: 2,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    });
    available_indexes.insert_for_table_name(&joined_tables, "t2", VecDeque::from([index_t2_a]));

    // WHERE t1.a = t2.a
    let mut where_clause = vec![_create_binary_expr(
        _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // t1.a
        ast::Operator::Equals,
        _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // t2.a
    )];

    let table_references = TableReferences::new(joined_tables, vec![]);
    let mut access_methods_arena = Vec::new();
    let table_constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();

    let base_table_rows = default_base_rows(table_references.joined_tables().len());
    let schema = empty_schema();
    let result = compute_best_join_order(
        table_references.joined_tables(),
        1.0,
        None,
        &table_constraints,
        &base_table_rows,
        &mut access_methods_arena,
        &mut where_clause,
        &[],
        &[],
        &DEFAULT_PARAMS,
        &AnalyzeStats::default(),
        &available_indexes,
        &table_references,
        &schema,
    )
    .unwrap();
    assert!(result.is_some());
    let BestJoinOrderResult { best_plan, .. } = result.unwrap();

    // Expected: t1 first (scan), t2 second (index seek)
    assert_eq!(
        best_plan.table_numbers().collect::<Vec<_>>(),
        vec![TABLE1, TABLE2],
        "Expected join order [t1, t2] to use index on t2.a"
    );

    // t1 should use table scan (no constraints)
    let access_method_t1 = &access_methods_arena[best_plan.data[0].1];
    let (_, _, constraint_refs_t1) = _as_btree(access_method_t1);
    assert!(
        constraint_refs_t1.is_empty(),
        "t1 should use table scan with no constraints"
    );

    // t2 should use index seek, NOT hash join
    let access_method_t2 = &access_methods_arena[best_plan.data[1].1];
    match &access_method_t2.params {
        AccessMethodParams::BTreeTable {
            index,
            constraint_refs,
            ..
        } => {
            assert!(
                index.is_some(),
                "t2 should use index idx_t2_a, not a hash join"
            );
            assert_eq!(
                index.as_ref().unwrap().name,
                "idx_t2_a",
                "t2 should use index idx_t2_a"
            );
            assert!(
                !constraint_refs.is_empty(),
                "t2 should have constraints for index seek"
            );
        }
        AccessMethodParams::HashJoin { .. } => {
            panic!("Expected index lookup on t2, but got hash join instead");
        }
        _ => panic!("Unexpected access method for t2"),
    }
}

#[test]
fn hash_join_uses_estimated_matches_for_row_count() {
    let t1 = _create_btree_table("t1", _create_column_list(&["value"], Type::Integer));
    let mut t2 = _create_btree_table("t2", _create_column_list(&["value"], Type::Integer));
    Arc::get_mut(&mut t2).unwrap().root_page = 2;
    let mut table_id_counter = TableRefIdCounter::new();
    let joined_tables = vec![
        _create_table_reference(t1, None, table_id_counter.next()),
        _create_table_reference(
            t2,
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        ),
    ];
    let mut where_clause = vec![_create_binary_expr(
        _create_column_expr(joined_tables[0].internal_id, 0, false),
        Operator::Equals,
        _create_column_expr(joined_tables[1].internal_id, 0, false),
    )];
    let table_references = TableReferences::new(joined_tables, vec![]);
    let available_indexes = AvailableIndexes::default();
    let constraints = constraints_from_where_clause(
        &where_clause,
        &table_references,
        &available_indexes,
        &[],
        &empty_schema(),
        &DEFAULT_PARAMS,
    )
    .unwrap();
    let method = try_hash_join_access_method(
        &table_references.joined_tables()[0],
        &table_references.joined_tables()[1],
        0,
        1,
        &constraints[0],
        &constraints[1],
        &mut where_clause,
        std::iter::once((
            0,
            table_references.joined_tables()[0].internal_id,
            table_references.joined_tables()[1].internal_id,
        )),
        1_000.0,
        1_000.0,
        1.0,
        &[],
        &DEFAULT_PARAMS,
    )
    .unwrap()
    .unwrap();

    assert!(method.estimated_rows_per_outer_row < 1_000.0);
}
