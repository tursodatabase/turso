//! Changes a correlated `EXISTS` or `IN` subquery into a join when both forms
//! give the same rows.
//!
//! - `EXISTS` becomes a semi-join. It keeps an outer row when an inner row matches.
//! - `NOT EXISTS` becomes an anti-join. It keeps an outer row when no inner row matches.
//! - A direct positive `IN` becomes a semi-join with one extra equality test.
//!
//! The code only moves direct `=` checks between an inner and outer column.
//! Other forms stay as subqueries. `NOT IN` also stays as a subquery because
//! NULL values can change its result.
//!
//! References:
//! - SQLite subquery results: https://sqlite.org/lang_expr.html#subquery_expressions
//! - PostgreSQL subquery results: https://www.postgresql.org/docs/current/functions-subquery.html
//! - MySQL semi-joins: https://dev.mysql.com/doc/refman/8.4/en/semijoins-antijoins.html
//! - MariaDB semi-joins: https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/subquery-optimizations/semi-join-subquery-optimizations

use smallvec::SmallVec;
use turso_parser::ast::{self, Expr, TableInternalId, UnaryOperator};

use crate::translate::plan::Plan;

use crate::translate::{
    emitter::Resolver,
    expr::{expr_contains_nondeterministic_scalar_function, walk_expr, WalkControl},
    plan::{Distinctness, JoinInfo, JoinType, SelectPlan, SubqueryState, WhereTerm},
};
use crate::Result;

/// Try each supported rewrite and return whether the plan changed.
pub fn rewrite_correlated_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let mut changed = false;
    let mut subquery_index = 0;
    while subquery_index < plan.non_from_clause_subqueries.len() {
        let subquery = &plan.non_from_clause_subqueries[subquery_index];
        if !subquery.correlated || !matches!(subquery.query_type, ast::SubqueryType::Exists { .. })
        {
            subquery_index += 1;
            continue;
        }
        if try_rewrite_exists(plan, subquery_index, resolver)? {
            changed = true;
            continue;
        }
        subquery_index += 1;
    }

    let mut subquery_index = 0;
    while subquery_index < plan.non_from_clause_subqueries.len() {
        let subquery = &plan.non_from_clause_subqueries[subquery_index];
        if !subquery.correlated {
            subquery_index += 1;
            continue;
        }
        let rewritten = matches!(subquery.query_type, ast::SubqueryType::In { .. })
            && try_rewrite_in(plan, subquery_index, resolver)?;
        if rewritten {
            changed = true;
            continue;
        }
        subquery_index += 1;
    }
    Ok(changed)
}

/// Return the SELECT plan for a subquery that has not run yet.
fn select_subquery_plan(plan: &SelectPlan, subquery_index: usize) -> Option<Box<SelectPlan>> {
    let subquery = &plan.non_from_clause_subqueries[subquery_index];
    let SubqueryState::Unevaluated { plan: inner } = &subquery.state else {
        return None;
    };
    let Plan::Select(inner) = inner.as_ref()?.as_ref() else {
        return None;
    };
    Some(inner.clone())
}

/// Try to change one `EXISTS` or `NOT EXISTS` into a join.
fn try_rewrite_exists(
    plan: &mut SelectPlan,
    subquery_index: usize,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(false);
    };
    let subquery_id = plan.non_from_clause_subqueries[subquery_index].internal_id;

    let Some(term) = find_exists_in_where(&plan.where_clause, subquery_id) else {
        return Ok(false);
    };

    let join_type = if term.negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    rewrite_as_semi_or_anti_join(
        plan,
        subquery_index,
        inner_plan,
        term.index,
        join_type,
        resolver,
    )
}

/// Turn a direct `IN` test that is not `NOT IN` into a semi-join.
fn try_rewrite_in(
    plan: &mut SelectPlan,
    subquery_index: usize,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(false);
    };
    let subquery_id = plan.non_from_clause_subqueries[subquery_index].internal_id;
    let Some((where_term_index, left)) = find_in_term(&plan.where_clause, subquery_id) else {
        return Ok(false);
    };
    let Some(right) = inner_plan
        .result_columns
        .first()
        .map(|column| column.expr.clone())
    else {
        return Ok(false);
    };
    if inner_plan.result_columns.len() != 1
        || expr_contains_nondeterministic_scalar_function(&left, resolver)?
        || expr_contains_nondeterministic_scalar_function(&right, resolver)?
    {
        return Ok(false);
    }

    let mut inner_plan = inner_plan;
    inner_plan.where_clause.push(WhereTerm {
        expr: Expr::Binary(Box::new(left), ast::Operator::Equals, Box::new(right)),
        from_outer_join: None,
        consumed: false,
    });
    rewrite_as_semi_or_anti_join(
        plan,
        subquery_index,
        inner_plan,
        where_term_index,
        JoinType::Semi,
        resolver,
    )
}

/// Move one simple subquery into the outer query as a semi-join or anti-join.
fn rewrite_as_semi_or_anti_join(
    plan: &mut SelectPlan,
    subquery_index: usize,
    inner_plan: Box<SelectPlan>,
    where_term_index: usize,
    join_type: JoinType,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    if !can_rewrite_as_semi_join(&inner_plan, resolver)? {
        return Ok(false);
    }

    let outer_table_ids: Vec<TableInternalId> = inner_plan
        .table_references
        .outer_query_refs()
        .iter()
        .map(|reference| reference.internal_id)
        .collect();
    let inner_table_ids: Vec<TableInternalId> = inner_plan
        .table_references
        .joined_tables()
        .iter()
        .map(|table| table.internal_id)
        .collect();

    for term in &inner_plan.where_clause {
        if !can_move_join_term(&term.expr, &outer_table_ids, &inner_table_ids) {
            return Ok(false);
        }
    }

    // Every NOT EXISTS term must use an inner table. Moving a constant false
    // term into the outer query would reject a row that NOT EXISTS keeps.
    if join_type == JoinType::Anti {
        for term in &inner_plan.where_clause {
            let tables = collect_table_refs(&term.expr);
            if !tables.iter().any(|table| inner_table_ids.contains(table)) {
                return Ok(false);
            }
        }
    }

    // Keep a link to a table that an outer join may fill with NULL. Moving the
    // link into a later join could remove that row too early.
    let mut outer_table_ids_that_may_be_null: Vec<TableInternalId> = Vec::new();
    let outer_tables = plan.table_references.joined_tables();
    for (index, table) in outer_tables.iter().enumerate() {
        if let Some(join_info) = &table.join_info {
            if join_info.is_outer() {
                outer_table_ids_that_may_be_null.push(table.internal_id);
            }
            if join_info.is_full_outer() && index > 0 {
                outer_table_ids_that_may_be_null.push(outer_tables[index - 1].internal_id);
            }
        }
    }
    if !outer_table_ids_that_may_be_null.is_empty() {
        for term in &inner_plan.where_clause {
            let tables = collect_table_refs(&term.expr);
            if tables
                .iter()
                .any(|table| outer_table_ids_that_may_be_null.contains(table))
            {
                return Ok(false);
            }
        }
    }

    let mut inner_plan = inner_plan;
    let inner_tables = std::mem::take(inner_plan.table_references.joined_tables_mut());
    for (index, mut table) in inner_tables.into_iter().enumerate() {
        if index == 0 {
            table.join_info = Some(JoinInfo {
                join_type,
                using: vec![],
                no_reorder: false,
            });
        }
        plan.table_references.add_joined_table(table);
    }

    // The terms now run in the outer plan, so prior access choices do not apply.
    for mut term in inner_plan.where_clause {
        term.consumed = false;
        plan.where_clause.push(term);
    }

    for inner_subquery in inner_plan.non_from_clause_subqueries {
        plan.non_from_clause_subqueries.push(inner_subquery);
    }

    // EXISTS ignores its SELECT list. Keep any parameters from that list so
    // callers can still bind them.
    if matches!(
        plan.non_from_clause_subqueries[subquery_index].query_type,
        ast::SubqueryType::Exists { .. }
    ) {
        for result_column in &inner_plan.result_columns {
            walk_expr(
                &result_column.expr,
                &mut |expr: &Expr| -> Result<WalkControl> {
                    if let Expr::Variable(variable) = expr {
                        plan.phantom_params.push(variable.clone());
                    }
                    Ok(WalkControl::Continue)
                },
            )
            .expect("walking a result expression cannot fail");
        }
    }

    replace_subquery_term_with_true(&mut plan.where_clause, where_term_index);

    plan.non_from_clause_subqueries.remove(subquery_index);

    Ok(true)
}

/// Return whether a subquery is simple enough for a semi-join or anti-join.
fn can_rewrite_as_semi_join(plan: &SelectPlan, resolver: &Resolver<'_>) -> Result<bool> {
    // The current semi-join and anti-join loops stop after a match in one table.
    // A joined inner query needs a separate implementation.
    if plan.table_references.joined_tables().len() != 1 {
        return Ok(false);
    }
    if plan.limit.is_some()
        || plan.group_by.is_some()
        || !plan.order_by.is_empty()
        || !matches!(plan.distinctness, Distinctness::NonDistinct)
        || plan.window.is_some()
        || plan.offset.is_some()
        || !plan.values.is_empty()
    {
        return Ok(false);
    }

    // An aggregate can return a row when its input table is empty.
    if !plan.aggregates.is_empty() {
        return Ok(false);
    }

    if plan
        .non_from_clause_subqueries
        .iter()
        .any(|subquery| subquery.correlated)
    {
        return Ok(false);
    }
    if plan
        .table_references
        .joined_tables()
        .iter()
        .any(|table| match &table.table {
            crate::schema::Table::FromClauseSubquery(subquery) => {
                plan_is_correlated(&subquery.plan)
            }
            _ => false,
        })
    {
        return Ok(false);
    }

    for term in &plan.where_clause {
        if expr_contains_nondeterministic_scalar_function(&term.expr, resolver)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The top-level `WHERE` term that uses an `EXISTS` result.
struct ExistsTerm {
    /// The term's position in the `WHERE` list.
    index: usize,
    /// Whether the term is `NOT EXISTS`.
    negated: bool,
}

/// Find the WHERE term that references the EXISTS subquery with the given ID.
/// Returns `None` if the subquery appears where it cannot be moved into a join
/// (e.g., inside OR, or referenced multiple times).
fn find_exists_in_where(
    where_clause: &[WhereTerm],
    subquery_id: TableInternalId,
) -> Option<ExistsTerm> {
    for (index, term) in where_clause.iter().enumerate() {
        // An EXISTS inside an outer join condition must still allow the outer
        // row through when it is false.
        if term.from_outer_join.is_some() {
            continue;
        }
        if let Expr::SubqueryResult {
            subquery_id: id,
            query_type: ast::SubqueryType::Exists { .. },
            ..
        } = &term.expr
        {
            if *id == subquery_id {
                return Some(ExistsTerm {
                    index,
                    negated: false,
                });
            }
        }
        if let Expr::Unary(UnaryOperator::Not, inner) = &term.expr {
            if let Expr::SubqueryResult {
                subquery_id: id,
                query_type: ast::SubqueryType::Exists { .. },
                ..
            } = inner.as_ref()
            {
                if *id == subquery_id {
                    return Some(ExistsTerm {
                        index,
                        negated: true,
                    });
                }
            }
        }
    }
    None
}

/// Find a direct IN term. IN under OR and NOT IN stay as subqueries.
fn find_in_term(where_clause: &[WhereTerm], subquery_id: TableInternalId) -> Option<(usize, Expr)> {
    where_clause.iter().enumerate().find_map(|(index, term)| {
        if term.from_outer_join.is_some() {
            return None;
        }
        let Expr::SubqueryResult {
            subquery_id: id,
            lhs: Some(left),
            not_in: false,
            query_type: ast::SubqueryType::In { .. },
        } = &term.expr
        else {
            return None;
        };
        (*id == subquery_id).then(|| (index, left.as_ref().clone()))
    })
}

/// Return whether an inner `WHERE` term can move into the outer query.
fn can_move_join_term(
    expr: &Expr,
    outer_table_ids: &[TableInternalId],
    inner_table_ids: &[TableInternalId],
) -> bool {
    let mut has_outer_ref = false;
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        if let Expr::Column { table, .. } = expr {
            if outer_table_ids.contains(table) {
                has_outer_ref = true;
            }
        }
        Ok(WalkControl::Continue)
    })
    .expect("walking a WHERE expression cannot fail");

    if !has_outer_ref {
        return true;
    }

    is_inner_outer_equal_check(expr, outer_table_ids, inner_table_ids)
}

/// One side of an `=` check may use inner tables and the other may use outer tables.
fn is_inner_outer_equal_check(
    expr: &Expr,
    outer_table_ids: &[TableInternalId],
    inner_table_ids: &[TableInternalId],
) -> bool {
    if let Expr::Binary(left, ast::Operator::Equals, right) = expr {
        let left_tables = collect_table_refs(left);
        let right_tables = collect_table_refs(right);

        let left_is_outer = left_tables
            .iter()
            .all(|table| outer_table_ids.contains(table))
            && !left_tables.is_empty();
        let left_is_inner = left_tables
            .iter()
            .all(|table| inner_table_ids.contains(table))
            && !left_tables.is_empty();
        let right_is_outer = right_tables
            .iter()
            .all(|table| outer_table_ids.contains(table))
            && !right_tables.is_empty();
        let right_is_inner = right_tables
            .iter()
            .all(|table| inner_table_ids.contains(table))
            && !right_tables.is_empty();

        (left_is_outer && right_is_inner) || (left_is_inner && right_is_outer)
    } else {
        false
    }
}

/// Return each table used by an expression.
fn collect_table_refs(expr: &Expr) -> SmallVec<[TableInternalId; 2]> {
    let mut tables = SmallVec::new();
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        if let Expr::Column { table, .. } = expr {
            if !tables.contains(table) {
                tables.push(*table);
            }
        }
        Ok(WalkControl::Continue)
    })
    .expect("walking an expression cannot fail");
    tables
}

/// Replace a WHERE term after its join now performs the same test.
fn replace_subquery_term_with_true(where_clause: &mut [WhereTerm], index: usize) {
    where_clause[index].expr = Expr::Literal(ast::Literal::Numeric("1".to_string()));
}
