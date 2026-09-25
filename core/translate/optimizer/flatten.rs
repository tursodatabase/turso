//! Flattens a FROM-clause subquery, view, or CTE reference: moves it into the
//! query that reads it, so that the join optimizer can use the indexes of its
//! tables.
//!
//! ```sql
//! CREATE VIEW v AS SELECT id, a, b * 2 AS b2 FROM t;
//!
//! -- Before
//! SELECT * FROM v WHERE id = 5;
//! -- After
//! SELECT t.id, t.a, (t.b * 2) AS b2 FROM t WHERE t.id = 5;
//! ```
//!
//! Without flattening the view runs as its own query, which reads every row
//! of `t`, and the outer query filters its output. After flattening the outer
//! query seeks `t` by its primary key. This is the query flattener of SQLite
//! (`flattenSubquery` in `select.c`), also called view merging (Oracle) and
//! subquery pull-up (PostgreSQL).
//!
//! Flattening moves the subquery's tables and `WHERE` terms into the outer
//! query and replaces each reference to a subquery column with the column's
//! result expression. An expression that is not a plain column goes inside
//! [Expr::SubqueryColumnValue], which keeps the collation and subtype
//! behavior that the value had as a subquery column.
//!
//! A subquery is flattened only when the flattened query returns the same
//! rows:
//!
//! - The subquery is a plain `SELECT` with a `FROM` clause and no aggregate,
//!   `GROUP BY`, `DISTINCT`, window function, `LIMIT`, `OFFSET`, `FULL JOIN`,
//!   or subquery outside its `FROM` clause. A `LIMIT`, `DISTINCT`, or
//!   aggregate changes which rows the subquery returns, so its filter cannot
//!   run together with the filters of the outer query.
//! - It reads no column of an enclosing query.
//! - Its result columns call no nondeterministic function. The outer query
//!   can read a column more than once, and each copy of `random()` would then
//!   return a different value.
//! - It is not a `MATERIALIZED` CTE, and not a CTE that other reads share.
//! - The outer query has no window function and no `FULL JOIN`.
//! - No subquery in the outer `WHERE`, `SELECT` list, or other clauses reads
//!   a column of the subquery. That subquery was planned against the
//!   subquery's columns.
//! - An ordered subquery is not flattened into an aggregate query, because
//!   the order decides the result of `group_concat`. The order moves to the
//!   outer query when the subquery is its only table and the outer query has
//!   no order of its own. Otherwise it is dropped, as in SQLite, because a
//!   join or an outer `ORDER BY` decides the order of the result rows.
//! - A subquery on the right side of a `LEFT JOIN` reads one table, the
//!   outer query is not `DISTINCT`, and the outer query reads only plain
//!   columns of the subquery. For a row with no match, a column of the
//!   subquery table is NULL, but an expression such as `5` or `coalesce(a, 0)`
//!   would not be.

use turso_parser::ast::{Expr, Name, TableInternalId};

use crate::schema::{Column, Table};
use crate::sync::Arc;
use crate::translate::collate::get_collseq_from_expr;
use crate::translate::display::PlanContext;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, walk_expr, walk_expr_mut, WalkControl,
};
use crate::translate::plan::{
    JoinInfo, JoinType, NonFromClauseSubquery, Plan, SelectPlan, SubqueryColumnName, SubqueryState,
    TableReferences, WhereTerm,
};
use crate::Result;
use turso_parser::ast::fmt::ToTokens;

use super::default_join_order;

/// Flatten every FROM-clause subquery of `plan` that can be flattened. A
/// flattened subquery can bring its own FROM-clause subqueries, and those are
/// tried next.
pub(super) fn flatten_from_clause_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver<'_>,
) -> Result<()> {
    if plan.window.is_some() || plan.table_references.has_full_join() {
        return Ok(());
    }
    while let Some(position) = find_subquery_to_flatten(plan, resolver)? {
        flatten_subquery(plan, position)?;
    }
    Ok(())
}

fn find_subquery_to_flatten(plan: &SelectPlan, resolver: &Resolver<'_>) -> Result<Option<usize>> {
    for position in 0..plan.joined_tables().len() {
        if subquery_can_flatten(plan, position, resolver)? {
            return Ok(Some(position));
        }
    }
    Ok(None)
}

fn subquery_can_flatten(
    plan: &SelectPlan,
    position: usize,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let table = &plan.joined_tables()[position];
    let Table::FromClauseSubquery(subquery) = &table.table else {
        return Ok(false);
    };
    if subquery.requires_table_materialization() {
        return Ok(false);
    }
    let Plan::Select(inner) = subquery.plan.as_ref() else {
        return Ok(false);
    };
    if !inner_query_can_flatten(inner, resolver)? {
        return Ok(false);
    }
    let is_left_join = match table.join_info.as_ref().map(|info| info.join_type) {
        None | Some(JoinType::Inner) => false,
        Some(JoinType::LeftOuter) => true,
        Some(JoinType::FullOuter | JoinType::Semi | JoinType::Anti) => return Ok(false),
    };
    if plan.joined_tables().len() - 1 + inner.joined_tables().len()
        > TableReferences::MAX_JOINED_TABLES
    {
        return Ok(false);
    }
    if subqueries_read_table(&plan.non_from_clause_subqueries, table.internal_id) {
        return Ok(false);
    }
    if is_left_join
        && !left_join_subquery_can_flatten(plan, table.internal_id, inner, &subquery.columns)?
    {
        return Ok(false);
    }
    let inner_order_by_would_move = plan.order_by.is_empty() && plan.joined_tables().len() == 1;
    let inner_order_by_blocks_flattening = !inner.order_by.is_empty()
        && (plan.is_aggregate() || (inner_order_by_would_move && plan.distinctness.is_distinct()));
    Ok(!inner_order_by_blocks_flattening)
}

fn inner_query_can_flatten(inner: &SelectPlan, resolver: &Resolver<'_>) -> Result<bool> {
    if inner.joined_tables().is_empty()
        || inner.is_aggregate()
        || inner.distinctness.is_distinct()
        || inner.window.is_some()
        || inner.limit.is_some()
        || inner.offset.is_some()
        || !inner.non_from_clause_subqueries.is_empty()
        || inner.contains_constant_false_condition
        || inner.is_correlated()
        || inner.table_references.has_full_join()
    {
        return Ok(false);
    }
    for column in &inner.result_columns {
        if expr_contains_nondeterministic_scalar_function(&column.expr, resolver)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether a subquery outside the FROM clause reads a column of the table.
/// A read by a subquery nested deeper is also recorded in the outer
/// references of the subquery that contains it.
fn subqueries_read_table(subqueries: &[NonFromClauseSubquery], table_id: TableInternalId) -> bool {
    subqueries.iter().any(|subquery| match &subquery.state {
        SubqueryState::Unevaluated { plan: Some(plan) } => {
            plan.used_outer_query_ref_ids().contains(&table_id)
        }
        SubqueryState::Unevaluated { plan: None } => false,
        SubqueryState::Evaluated { outer_ref_ids, .. } => outer_ref_ids.contains(&table_id),
    })
}

fn left_join_subquery_can_flatten(
    plan: &SelectPlan,
    subquery_id: TableInternalId,
    inner: &SelectPlan,
    subquery_columns: &[Column],
) -> Result<bool> {
    if inner.joined_tables().len() != 1 || plan.distinctness.is_distinct() {
        return Ok(false);
    }
    for column in columns_read_from(plan, subquery_id) {
        if !column_is_copied_unchanged(inner, &subquery_columns[column], column)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn columns_read_from(plan: &SelectPlan, table_id: TableInternalId) -> Vec<usize> {
    let mut columns = Vec::new();
    for expr in plan.exprs() {
        walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
            if let Expr::Column { table, column, .. } = expr {
                if *table == table_id {
                    columns.push(*column);
                }
            }
            Ok(WalkControl::Continue)
        })
        .expect("collecting column references cannot fail");
    }
    columns
}

fn flatten_subquery(plan: &mut SelectPlan, position: usize) -> Result<()> {
    let inner_order_by_moves = plan.order_by.is_empty() && plan.joined_tables().len() == 1;
    save_subquery_column_names(plan, position);

    let subquery_table = plan.table_references.joined_tables_mut().remove(position);
    let Table::FromClauseSubquery(subquery) = subquery_table.table else {
        unreachable!("a checked subquery is a FROM-clause subquery")
    };
    let subquery = Arc::unwrap_or_clone(subquery);
    let Plan::Select(inner) = *subquery.plan else {
        unreachable!("a checked subquery is a SELECT")
    };
    let mut inner = *inner;

    let replacements = replacements_for_columns(&inner, &subquery.columns)?;

    let mut inner_tables = std::mem::take(inner.table_references.joined_tables_mut());
    let first_inner_table_id = inner_tables[0].internal_id;
    let is_left_join = subquery_table
        .join_info
        .as_ref()
        .is_some_and(JoinInfo::is_outer);
    inner_tables[0].join_info = subquery_table.join_info;
    let moved_table_ids: Vec<TableInternalId> =
        inner_tables.iter().map(|table| table.internal_id).collect();
    for table in &mut inner_tables {
        table.col_used_mask = Default::default();
        table.column_use_counts.clear();
    }
    plan.table_references
        .joined_tables_mut()
        .splice(position..position, inner_tables);

    let subquery_id = subquery_table.internal_id;
    for term in &mut plan.where_clause {
        if term.from_outer_join == Some(subquery_id) {
            term.from_outer_join = Some(first_inner_table_id);
        }
    }
    for expr in plan.exprs_mut() {
        replace_subquery_columns(expr, subquery_id, &replacements);
    }

    for term in inner.where_clause {
        plan.where_clause.push(WhereTerm {
            expr: term.expr,
            from_outer_join: if is_left_join {
                Some(first_inner_table_id)
            } else {
                term.from_outer_join
            },
            consumed: false,
        });
    }
    if inner_order_by_moves {
        plan.order_by = inner.order_by;
    }

    for table_id in moved_table_ids {
        for column in columns_read_from(plan, table_id) {
            plan.table_references.mark_column_used(table_id, column);
        }
    }
    plan.join_order = default_join_order(&plan.table_references);
    Ok(())
}

/// Save the names of each result column that reads a subquery column
/// directly, before its expression is replaced.
fn save_subquery_column_names(plan: &mut SelectPlan, position: usize) {
    let table = &plan.table_references.joined_tables()[position];
    let subquery_id = table.internal_id;
    let tables = [&plan.table_references];
    let context = PlanContext(&tables);
    let names: Vec<Option<(SubqueryColumnName, String)>> = plan
        .result_columns
        .iter()
        .map(|result_column| match &result_column.expr {
            Expr::Column {
                table: table_id, ..
            } if *table_id == subquery_id
                && result_column.alias.is_none()
                && result_column.subquery_column_name.is_none() =>
            {
                let column_name = result_column
                    .name(&plan.table_references)
                    .map(str::to_string);
                let name = SubqueryColumnName {
                    full_name: format!(
                        "{}.{}",
                        table.table.get_name(),
                        column_name.as_deref().unwrap_or("?")
                    ),
                    column_name,
                };
                Some((name, result_column.expr.displayer(&context).to_string()))
            }
            _ => None,
        })
        .collect();
    for (result_column, name) in plan.result_columns.iter_mut().zip(names) {
        if let Some((name, expr_text)) = name {
            result_column.subquery_column_name = Some(Box::new(name));
            result_column.implicit_column_name.get_or_insert(expr_text);
        }
    }
}

/// The expressions that replace references to the subquery's columns, one
/// for each column, taken from the result columns of `inner`.
pub(super) fn replacements_for_columns(
    inner: &SelectPlan,
    columns: &[Column],
) -> Result<Vec<Expr>> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| replacement_for_column(inner, column, index))
        .collect()
}

/// The expression that replaces a reference to subquery column `index`.
fn replacement_for_column(inner: &SelectPlan, column: &Column, index: usize) -> Result<Expr> {
    let expr = &inner.result_columns[index].expr;
    if column_is_copied_unchanged(inner, column, index)? {
        return Ok(expr.clone());
    }
    Ok(Expr::SubqueryColumnValue {
        expr: Box::new(expr.clone()),
        collation: Name::exact(column.collation().name()),
    })
}

/// Whether subquery column `index` is a plain column with the same
/// collation, so that its expression can replace it without a wrapper.
fn column_is_copied_unchanged(inner: &SelectPlan, column: &Column, index: usize) -> Result<bool> {
    let expr = &inner.result_columns[index].expr;
    Ok(matches!(expr, Expr::Column { .. } | Expr::RowId { .. })
        && get_collseq_from_expr(expr, &inner.table_references)?.unwrap_or_default()
            == column.collation())
}

/// Replace each reference to a column of the subquery with its replacement.
pub(super) fn replace_subquery_columns(
    expr: &mut Expr,
    subquery_id: TableInternalId,
    replacements: &[Expr],
) {
    walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
        if let Expr::Column { table, column, .. } = expr {
            if *table == subquery_id {
                *expr = replacements[*column].clone();
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    })
    .expect("replacing column references cannot fail");
}
