//! Lower a `Block` tree back into a `SelectPlan`.
//!
//! The tree must have the block shape: `Limit(Sort(Distinct(Project(
//! Aggregate(Filter(joins))))))` with every operator optional except the
//! projection. Rules keep this shape, so a different shape is a bug.

use crate::schema::{FromClauseSubquery, Table};
use crate::sync::Arc;
use crate::translate::plan::{
    Distinctness, JoinInfo, JoinOrderMember, JoinedTable, Plan, SelectPlan, TableReferences,
};
use crate::{LimboError, Result};

use crate::translate::expr::{walk_expr_mut, WalkControl};
use crate::translate::plan::{NonFromClauseSubquery, SubqueryState};
use turso_parser::ast::{Expr, TableInternalId};

use super::{Block, DependentJoinKind, DerivedTable, LogicalPlan};

pub(crate) fn lower_block(block: Block) -> Result<SelectPlan> {
    let Block {
        mut root,
        mut subqueries,
        outer_query_refs,
        right_join_swapped,
        query_destination,
        input_cardinality_hint,
        phantom_params,
    } = block;
    restore_dependent_joins(&mut root, &mut subqueries)?;

    let (node, limit, offset) = match root {
        LogicalPlan::Limit(limit) => (*limit.input, limit.limit, limit.offset),
        node => (node, None, None),
    };
    let (node, order_by) = match node {
        LogicalPlan::Sort(sort) => (*sort.input, sort.keys),
        node => (node, Vec::new()),
    };
    let (node, distinctness) = match node {
        LogicalPlan::Distinct(distinct) => (*distinct.input, distinct.distinctness),
        node => (node, Distinctness::NonDistinct),
    };
    let (node, result_columns) = match node {
        LogicalPlan::Project(project) => (*project.input, project.columns),
        _ => return Err(shape_error("a block must start with a projection")),
    };
    let (node, group_by, aggregates) = match node {
        LogicalPlan::Aggregate(aggregate) => {
            (*aggregate.input, aggregate.group_by, aggregate.aggregates)
        }
        node => (node, None, Vec::new()),
    };
    let (node, filter_terms) = match node {
        LogicalPlan::Filter(filter) => (*filter.input, filter.terms),
        node => (node, Vec::new()),
    };

    let mut tables = Vec::new();
    linearize(node, None, &mut tables)?;
    let where_clause = filter_terms;
    if where_clause.iter().any(|term| {
        term.from_outer_join.is_some_and(|table_id| {
            !tables.iter().skip(1).any(|table| {
                table.internal_id == table_id
                    && table
                        .join_info
                        .as_ref()
                        .is_some_and(|info| info.is_outer() || info.is_semi_or_anti())
            })
        })
    }) {
        return Err(shape_error(
            "a join term names a table that is not the right side of an outer join",
        ));
    }
    if tables.len() > TableReferences::MAX_JOINED_TABLES {
        return Err(LimboError::ParseError(format!(
            "too many tables in the FROM clause after rewriting: {}",
            tables.len()
        )));
    }
    let join_order = tables
        .iter()
        .enumerate()
        .map(|(index, table)| JoinOrderMember {
            table_id: table.internal_id,
            original_idx: index,
            is_outer: table.join_info.as_ref().is_some_and(JoinInfo::is_outer),
        })
        .collect();
    let mut table_references = TableReferences::new(tables, outer_query_refs);
    if right_join_swapped {
        table_references.set_right_join_swapped();
    }

    Ok(SelectPlan {
        table_references,
        join_order,
        result_columns,
        where_clause,
        group_by,
        order_by,
        aggregates,
        limit,
        offset,
        contains_constant_false_condition: false,
        query_destination,
        distinctness,
        values: Vec::new(),
        window: None,
        non_from_clause_subqueries: subqueries,
        input_cardinality_hint,
        estimated_output_rows: None,
        estimated_cost: None,
        simple_aggregate: None,
        phantom_params,
    })
}

/// Put back every scalar subquery whose dependent join no rule removed.
fn restore_dependent_joins(
    root: &mut LogicalPlan,
    subqueries: &mut Vec<NonFromClauseSubquery>,
) -> Result<()> {
    let mut node: &mut LogicalPlan = &mut *root;
    loop {
        match node {
            LogicalPlan::Limit(next) => node = &mut next.input,
            LogicalPlan::Sort(next) => node = &mut next.input,
            LogicalPlan::Distinct(next) => node = &mut next.input,
            LogicalPlan::Project(next) => node = &mut next.input,
            LogicalPlan::Aggregate(next) => node = &mut next.input,
            LogicalPlan::Filter(next) => node = &mut next.input,
            _ => break,
        }
    }
    let mut restored: Vec<(usize, NonFromClauseSubquery)> = Vec::new();
    while let LogicalPlan::DependentJoin(_) = node {
        let LogicalPlan::DependentJoin(join) = std::mem::replace(node, LogicalPlan::OneRow) else {
            unreachable!("checked: the node is a dependent join")
        };
        let (left, right, kind) = (join.left, join.right, join.kind);
        let DependentJoinKind::Scalar {
            mut subquery,
            position,
            duplicates,
        } = kind
        else {
            return Err(shape_error("a domain join was not removed"));
        };
        let LogicalPlan::DerivedTable(derived) = *right else {
            return Err(shape_error(
                "a scalar dependent join needs a subquery on its right side",
            ));
        };
        let plan = lower_block(*derived.block)?;
        subquery.state = SubqueryState::Unevaluated {
            plan: Some(Box::new(Plan::Select(Box::new(plan)))),
        };
        restored.push((position, subquery));
        restored.extend(duplicates);
        *node = *left;
    }
    if restored.is_empty() {
        return Ok(());
    }
    let types: Vec<(TableInternalId, turso_parser::ast::SubqueryType)> = restored
        .iter()
        .map(|(_, subquery)| (subquery.internal_id, subquery.query_type.clone()))
        .collect();
    root.for_each_expr_mut(&mut |expr| {
        walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            if let Expr::Column {
                table, column: 0, ..
            } = expr
            {
                if let Some((_, query_type)) = types.iter().find(|(id, _)| id == table) {
                    *expr = Expr::SubqueryResult {
                        subquery_id: *table,
                        lhs: None,
                        not_in: false,
                        query_type: query_type.clone(),
                    };
                    return Ok(WalkControl::SkipChildren);
                }
            }
            Ok(WalkControl::Continue)
        })
        .map(|_| ())
    })?;
    restored.sort_by_key(|(position, _)| *position);
    for (position, subquery) in restored {
        let index = position.min(subqueries.len());
        subqueries.insert(index, subquery);
    }
    Ok(())
}

/// Turn a join tree into the flat table list of a `SelectPlan`.
///
/// `info` is the join that connects this subtree to the tables before it. It
/// goes to the leftmost leaf of the subtree.
fn linearize(
    node: LogicalPlan,
    info: Option<JoinInfo>,
    tables: &mut Vec<JoinedTable>,
) -> Result<()> {
    match node {
        LogicalPlan::OneRow => {
            if info.is_some() || !tables.is_empty() {
                return Err(shape_error("a one-row source cannot take part in a join"));
            }
            Ok(())
        }
        LogicalPlan::Scan(scan) => {
            let mut table = scan.table;
            table.join_info = info;
            tables.push(table);
            Ok(())
        }
        LogicalPlan::DerivedTable(derived) => {
            tables.push(lower_derived_table(derived, info)?);
            Ok(())
        }
        LogicalPlan::Join(join) => {
            linearize(*join.left, info, tables)?;
            let attaches_to_leaf = join.info.is_outer() || join.info.is_semi_or_anti();
            if attaches_to_leaf && join.right.leaf_id().is_none() {
                return Err(shape_error(
                    "an outer, semi, or anti join needs one table on its right side",
                ));
            }
            linearize(*join.right, Some(join.info), tables)
        }
        LogicalPlan::DependentJoin(_) => Err(shape_error("a dependent join was not removed")),
        LogicalPlan::Filter(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Project(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_) => Err(shape_error("an operator inside a join tree")),
    }
}

fn lower_derived_table(derived: DerivedTable, info: Option<JoinInfo>) -> Result<JoinedTable> {
    let DerivedTable {
        identifier,
        internal_id,
        shell,
        value_without_collation,
        block,
    } = derived;
    let plan = lower_block(*block)?;
    match shell {
        Some(mut table) => {
            let (name, columns, cte) = {
                let Table::FromClauseSubquery(subquery) = &table.table else {
                    unreachable!("a derived table shell holds a subquery")
                };
                (
                    subquery.name.clone(),
                    subquery.columns.clone(),
                    subquery.cte,
                )
            };
            table.table = Table::FromClauseSubquery(Arc::new(FromClauseSubquery {
                name,
                plan: Box::new(Plan::Select(Box::new(plan))),
                columns,
                result_columns_start_reg: None,
                materialized_cursor_id: None,
                cte,
            }));
            table.join_info = info;
            Ok(table)
        }
        None => {
            let mut table = JoinedTable::new_subquery(identifier, plan, info, internal_id)?;
            if value_without_collation {
                let Table::FromClauseSubquery(subquery) = &mut table.table else {
                    unreachable!("a new derived table holds a subquery")
                };
                Arc::get_mut(subquery)
                    .expect("a new derived table is not shared")
                    .columns[0]
                    .set_collation(None);
            }
            for column in 0..table.columns().len() {
                table.mark_column_used(column);
            }
            Ok(table)
        }
    }
}

fn shape_error(what: &str) -> LimboError {
    LimboError::InternalError(format!("logical plan cannot be lowered: {what}"))
}
