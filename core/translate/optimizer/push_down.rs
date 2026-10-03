//! Copies `WHERE` terms of a query into a FROM-clause subquery that cannot be
//! flattened, so that the subquery can use the indexes of its tables.
//!
//! ```sql
//! -- Before
//! SELECT * FROM (SELECT a, count(*) AS n FROM t GROUP BY a) WHERE a = 5;
//! -- After
//! SELECT * FROM (SELECT a, count(*) AS n FROM t WHERE a = 5 GROUP BY a) WHERE a = 5;
//! ```
//!
//! The outer query keeps its term, so a copy only has to keep every row that
//! the outer term keeps. This is SQLite's WHERE-clause push-down
//! (`pushDownWhereTerms` in `select.c`). A compound subquery gets a copy in
//! each of its `SELECT`s, which lets `SELECT * FROM a UNION ALL SELECT * FROM b`
//! seek both tables.
//!
//! A term is copied when:
//!
//! - it reads columns of the subquery and nothing else: no other table, no
//!   subquery, and no nondeterministic function;
//! - each column that it reads is deterministic in every `SELECT` of the
//!   subquery, so that `random() AS r` gives the copy and the outer term the
//!   same value, and has the affinity of the subquery column in every
//!   `SELECT`, so that the copy compares values the same way (only a compound
//!   subquery can differ);
//! - it comes from the `WHERE` clause, or from the `ON` clause of the
//!   `LEFT JOIN` whose right side is the subquery. A term of any other join
//!   decides which rows get NULLs, not which subquery rows exist;
//! - the outer query has no `FULL JOIN`, and the subquery is not the right
//!   side of a semi-join or anti-join;
//! - the subquery has no `LIMIT`, `OFFSET`, or window function, is not
//!   recursive, is not a `VALUES` list, and is not a `MATERIALIZED` or shared
//!   CTE; and
//! - an aggregate subquery has a `GROUP BY`. The copy goes into the
//!   subquery's `WHERE`, before the aggregate, so without a `GROUP BY` it
//!   would change the one row that the aggregate returns.
//!
//! `GROUP BY`, `DISTINCT`, `UNION`, `INTERSECT`, and `EXCEPT` merge rows that
//! compare equal into one row. A copy runs before the merge, so it must give
//! the same answer for all rows that merge. A term such as `typeof(a) = 'real'`
//! does not: `1` and `1.0` merge, and the copy would decide which of them the
//! merged row shows. SQLite copies such terms and can return a different row
//! than the query without the copy. Here, a subquery that merges rows gets
//! only a comparison of one column with a constant. The column must use the
//! BINARY collation, and in a `SELECT` with a `GROUP BY` it must be one of the
//! `GROUP BY` terms. Rows that merge on such a column hold the same value, so
//! the comparison gives them the same answer.

use turso_parser::ast::{self, Expr, TableInternalId};

use crate::schema::{Column, Table};
use crate::sync::Arc;
use crate::translate::collate::{get_collseq_from_expr, CollationSeq};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, get_expr_affinity, walk_expr, WalkControl,
};
use crate::translate::plan::{JoinType, Plan, SelectPlan, WhereTerm};
use crate::util::exprs_are_equivalent;
use crate::Result;

use super::flatten::{replace_subquery_columns, replacements_for_columns};
use super::Optimizable;

/// Copy the `WHERE` terms of `plan` that can be copied into its FROM-clause
/// subqueries.
pub(super) fn push_where_terms_into_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver<'_>,
) -> Result<()> {
    if plan.where_clause.is_empty() || plan.table_references.has_full_join() {
        return Ok(());
    }
    for position in 0..plan.joined_tables().len() {
        let terms = terms_to_push_into(plan, position, resolver)?;
        if terms.is_empty() {
            continue;
        }
        let table = &mut plan.table_references.joined_tables_mut()[position];
        let subquery_id = table.internal_id;
        let Table::FromClauseSubquery(subquery) = &mut table.table else {
            unreachable!("terms are only found for a FROM-clause subquery")
        };
        let subquery = Arc::make_mut(subquery);
        for select in subquery.plan.selects_mut() {
            let replacements = replacements_for_columns(select, &subquery.columns)?;
            for term in &terms {
                let mut copy = term.clone();
                replace_subquery_columns(&mut copy, subquery_id, &replacements);
                if !select
                    .where_clause
                    .iter()
                    .any(|existing| exprs_are_equivalent(&existing.expr, &copy))
                {
                    select.where_clause.push(WhereTerm::from(copy));
                }
            }
        }
    }
    Ok(())
}

/// The outer terms that can be copied into the subquery at `position`.
fn terms_to_push_into(
    plan: &SelectPlan,
    position: usize,
    resolver: &Resolver<'_>,
) -> Result<Vec<Expr>> {
    let table = &plan.joined_tables()[position];
    let Table::FromClauseSubquery(subquery) = &table.table else {
        return Ok(vec![]);
    };
    if subquery.requires_table_materialization() || !can_take_copied_terms(&subquery.plan) {
        return Ok(vec![]);
    }
    let from_outer_join = match table.join_info.as_ref().map(|info| info.join_type) {
        None | Some(JoinType::Inner) => None,
        Some(JoinType::LeftOuter) => Some(table.internal_id),
        Some(JoinType::Semi | JoinType::Anti) => return Ok(vec![]),
        Some(JoinType::FullOuter) => unreachable!("a plan with a FULL JOIN is skipped"),
    };
    let merges_rows = merges_equal_rows(&subquery.plan);
    let copyable = copyable_columns(&subquery.plan, &subquery.columns, merges_rows, resolver)?;
    let mut terms = Vec::new();
    for term in &plan.where_clause {
        if term.from_outer_join == from_outer_join
            && reads_only_copyable_columns(&term.expr, table.internal_id, &copyable)
            && (!merges_rows || compares_column_with_constant(&term.expr, resolver))
            && !expr_contains_nondeterministic_scalar_function(&term.expr, resolver)?
        {
            terms.push(term.expr.clone());
        }
    }
    Ok(terms)
}

fn can_take_copied_terms(plan: &Plan) -> bool {
    let compound_limits_rows = match plan {
        Plan::Select(_) => false,
        Plan::CompoundSelect { limit, offset, .. } => limit.is_some() || offset.is_some(),
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => return false,
    };
    !compound_limits_rows
        && plan.selects().iter().all(|select| {
            select.limit.is_none()
                && select.offset.is_none()
                && select.window.is_none()
                && !select.joined_tables().is_empty()
                && (select.aggregates.is_empty() || select.group_by.is_some())
        })
}

fn merges_equal_rows(plan: &Plan) -> bool {
    let merges_across_selects = match plan {
        Plan::CompoundSelect { left, .. } => left
            .iter()
            .any(|(_, operator)| *operator != ast::CompoundOperator::UnionAll),
        _ => false,
    };
    merges_across_selects
        || plan
            .selects()
            .iter()
            .any(|select| select.group_by.is_some() || select.distinctness.is_distinct())
}

/// For each subquery column, whether a copied term can read it: the column
/// is deterministic and has the subquery column's affinity in every
/// `SELECT`, and when the subquery merges rows, it uses the BINARY collation
/// and is a `GROUP BY` term of each grouped `SELECT`.
fn copyable_columns(
    plan: &Plan,
    columns: &[Column],
    merges_rows: bool,
    resolver: &Resolver<'_>,
) -> Result<Vec<bool>> {
    let mut copyable = vec![true; columns.len()];
    for select in plan.selects() {
        for (index, column) in columns.iter().enumerate() {
            let expr = &select.result_columns[index].expr;
            if expr_contains_nondeterministic_scalar_function(expr, resolver)?
                || get_expr_affinity(expr, Some(&select.table_references), None)
                    != column.affinity()
            {
                copyable[index] = false;
                continue;
            }
            if !merges_rows {
                continue;
            }
            let is_group_key = select.group_by.as_ref().is_none_or(|group_by| {
                group_by
                    .exprs
                    .iter()
                    .any(|key| exprs_are_equivalent(key, expr))
            });
            let collation = get_collseq_from_expr(expr, &select.table_references)?;
            if !is_group_key || collation.unwrap_or_default() != CollationSeq::Binary {
                copyable[index] = false;
            }
        }
    }
    Ok(copyable)
}

/// Whether the term reads at least one column of the table, only columns
/// marked copyable, and nothing else.
fn reads_only_copyable_columns(expr: &Expr, table_id: TableInternalId, copyable: &[bool]) -> bool {
    let mut reads_table = false;
    let mut reads_anything_else = false;
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, column, .. } if *table == table_id => {
                reads_table = true;
                reads_anything_else |= !copyable[*column];
            }
            Expr::Column { .. } | Expr::RowId { .. } | Expr::SubqueryResult { .. } => {
                reads_anything_else = true
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
    .expect("collecting table references cannot fail");
    reads_table && !reads_anything_else
}

/// Whether the term compares one column with a constant, as in `a = 5`,
/// `a > ?`, `a BETWEEN 1 AND 9`, `a IN (1, 2)`, or `a IS NULL`.
fn compares_column_with_constant(expr: &Expr, resolver: &Resolver<'_>) -> bool {
    let is_column = |expr: &Expr| matches!(expr, Expr::Column { .. });
    let is_constant = |expr: &Expr| expr.is_constant(resolver);
    match expr {
        Expr::Binary(lhs, operator, rhs) if operator.is_comparison() => {
            (is_column(lhs) && is_constant(rhs)) || (is_constant(lhs) && is_column(rhs))
        }
        Expr::Between {
            lhs, start, end, ..
        } => is_column(lhs) && is_constant(start) && is_constant(end),
        Expr::InList { lhs, rhs, .. } => is_column(lhs) && rhs.iter().all(|e| is_constant(e)),
        Expr::IsNull(operand) | Expr::NotNull(operand) => is_column(operand),
        _ => false,
    }
}
