//! Changes a correlated subquery into a join when both forms give the same rows.
//!
//! - `EXISTS` becomes a semi-join. It keeps an outer row when an inner row matches.
//! - `NOT EXISTS` becomes an anti-join. It keeps an outer row when no inner row matches.
//! - A direct positive `IN` becomes a semi-join with one extra equality test.
//! - A subquery that returns one aggregate value is grouped by the columns that
//!   link it to the outer query.
//!
//! The last rewrite turns this query:
//!
//! ```sql
//! SELECT * FROM outer_table o
//! WHERE o.value < (SELECT avg(i.value) FROM inner_table i WHERE i.key = o.key)
//! ```
//!
//! into this form:
//!
//! ```sql
//! SELECT * FROM outer_table o
//! LEFT JOIN (
//!   SELECT avg(value) AS avg_value, key FROM inner_table GROUP BY key
//! ) i
//!   ON i.key = o.key
//! WHERE o.value < i.avg_value
//! ```
//!
//! The code only moves direct `=` checks between an inner and outer column. Other
//! forms stay as subqueries. `NOT IN` also stays as a subquery because NULL values
//! can change its result. A one-value subquery stays as it is unless its result for
//! an empty input is known.
//!
//! References:
//! - SQLite subquery results: https://sqlite.org/lang_expr.html#subquery_expressions
//! - PostgreSQL subquery results: https://www.postgresql.org/docs/current/functions-subquery.html
//! - MySQL semi-joins: https://dev.mysql.com/doc/refman/8.4/en/semijoins-antijoins.html
//! - MySQL scalar decorrelation: https://dev.mysql.com/doc/refman/8.4/en/correlated-subqueries.html
//! - MySQL optimizer switches: https://dev.mysql.com/doc/refman/8.0/en/switchable-optimizations.html
//! - MariaDB semi-joins: https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/subquery-optimizations/semi-join-subquery-optimizations
//! - MariaDB materialization: https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/subquery-optimizations/optimization-strategies/semi-join-materialization-strategy
//! - MariaDB subquery cache: https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/subquery-optimizations/subquery-cache
//! - Neumann and Kemper, Unnesting Arbitrary Queries: https://db.cs.tum.edu/teaching/ws2122/foundationsde/unnesting.pdf
//! - Neumann, A Formalization of Top-Down Unnesting: https://arxiv.org/abs/2412.04294

use smallvec::SmallVec;
use turso_parser::ast::{
    self, Expr, FunctionTail, Name, SortOrder, TableInternalId, UnaryOperator,
};

use crate::translate::plan::Plan;

use crate::function::AggFunc;
use crate::schema::Table;
use crate::sync::Arc;
use crate::translate::{
    collate::get_collseq_from_expr,
    emitter::Resolver,
    expr::{
        expr_contains_nondeterministic_scalar_function, expr_references_subquery_id,
        get_expr_affinity, walk_expr, walk_expr_mut, WalkControl,
    },
    plan::{
        plan_is_correlated, Distinctness, GroupBy, JoinInfo, JoinType, JoinedTable,
        QueryDestination, ResultSetColumn, SelectPlan, SubqueryState, TableReferences, WhereTerm,
    },
};
use crate::util::exprs_are_equivalent;
use crate::Result;

/// Try each supported rewrite and return whether the plan changed.
pub fn rewrite_correlated_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let has_full_join = plan.table_references.joined_tables().iter().any(|table| {
        table
            .join_info
            .as_ref()
            .is_some_and(JoinInfo::is_full_outer)
    });
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
        let rewritten = match subquery.query_type {
            ast::SubqueryType::In { .. } => try_rewrite_in(plan, subquery_index, resolver)?,
            ast::SubqueryType::RowValue { num_regs: 1, .. } if !has_full_join => {
                try_rewrite_single_value_aggregate(plan, subquery_index, resolver)?
            }
            _ => false,
        };
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
    // A semi join needs a left table. Keep the subquery when the outer SELECT
    // has no FROM clause.
    if plan.table_references.joined_tables().is_empty() {
        return Ok(false);
    }

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
    for table in outer_tables {
        if let Some(join_info) = &table.join_info {
            if join_info.is_outer() && !join_info.is_full_outer() {
                outer_table_ids_that_may_be_null.push(table.internal_id);
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

/// The value returned when no input row matches.
#[derive(Clone, Copy)]
enum EmptyInputValue {
    /// SQL NULL.
    Null,
    /// Integer zero, as returned by `count`.
    IntegerZero,
    /// Real zero, as returned by `total`.
    RealZero,
}

/// The inner and outer columns used by one `=` check.
struct ColumnPair {
    /// The column read by the inner query.
    inner: Expr,
    /// The column read from the outer query.
    outer: Expr,
    /// Whether the inner column was on the left side of the `=` check.
    inner_was_left: bool,
}

/// Group an aggregate by the columns that link it to the outer query.
///
/// Use a left join because the old subquery returns one value even when no
/// inner row matches.
fn try_rewrite_single_value_aggregate(
    plan: &mut SelectPlan,
    subquery_index: usize,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    // Window planning keeps copies of its expressions. Leave those plans alone
    // until all of those copies can be changed together.
    if plan.window.is_some() {
        return Ok(false);
    }

    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(false);
    };

    if !can_rewrite_single_value_aggregate(&inner_plan, resolver)? {
        return Ok(false);
    }
    let Some(empty_value) = result_on_empty_input(&inner_plan) else {
        return Ok(false);
    };
    let subquery_id = plan.non_from_clause_subqueries[subquery_index].internal_id;

    // A value used by an outer join condition must be ready before that join
    // decides whether to fill its right side with NULL values.
    if plan.where_clause.iter().any(|term| {
        term.from_outer_join.is_some() && expr_references_subquery_id(&term.expr, subquery_id)
    }) {
        return Ok(false);
    }

    let outer_tables = inner_plan.table_references.outer_query_refs();
    if outer_tables.iter().any(|outer_table| {
        outer_table.is_used()
            && (outer_table.scope_depth != 0
                || plan
                    .table_references
                    .find_joined_table_by_internal_id(outer_table.internal_id)
                    .is_none())
    }) {
        return Ok(false);
    }
    // A grouped table linked to more than one outer table can move before one
    // of those tables after its left join becomes an inner join.
    if outer_tables
        .iter()
        .filter(|outer_table| outer_table.is_used())
        .count()
        != 1
    {
        return Ok(false);
    }
    let outer_table_ids: Vec<_> = outer_tables.iter().map(|table| table.internal_id).collect();
    let inner_table_ids: Vec<_> = inner_plan
        .table_references
        .joined_tables()
        .iter()
        .map(|table| table.internal_id)
        .collect();

    let mut pairs = Vec::new();
    let mut inner_where = Vec::new();
    for term in &inner_plan.where_clause {
        let tables = collect_table_refs(&term.expr);
        if !tables.iter().any(|table| outer_table_ids.contains(table)) {
            let mut term = term.clone();
            term.consumed = false;
            inner_where.push(term);
            continue;
        }
        if term.from_outer_join.is_some() {
            return Ok(false);
        }
        let Some(pair) = read_column_pair(&term.expr, &outer_table_ids, &inner_table_ids) else {
            return Ok(false);
        };
        if !column_pair_compares_the_same(&pair, &inner_plan.table_references) {
            return Ok(false);
        }
        pairs.push(pair);
    }
    if pairs.is_empty() || uses_outer_tables_outside_where(&inner_plan, &outer_table_ids) {
        return Ok(false);
    }

    let mut inner_plan = *inner_plan;
    inner_plan.where_clause = inner_where;
    inner_plan.limit = None;
    inner_plan.query_destination = QueryDestination::placeholder_for_subquery();
    inner_plan.simple_aggregate = None;
    inner_plan.input_cardinality_hint = None;
    inner_plan.estimated_output_rows = None;
    inner_plan.estimated_cost = None;
    inner_plan.table_references.clear_outer_query_refs();

    let mut group_columns: Vec<Expr> = Vec::new();
    let mut result_positions = Vec::with_capacity(pairs.len());
    for pair in &pairs {
        let column_index = group_columns
            .iter()
            .position(|existing| exprs_are_equivalent(existing, &pair.inner))
            .unwrap_or_else(|| {
                group_columns.push(pair.inner.clone());
                group_columns.len() - 1
            });
        result_positions.push(column_index + 1);
    }

    for (index, column) in group_columns.iter().enumerate() {
        inner_plan.result_columns.push(ResultSetColumn {
            expr: column.clone(),
            alias: Some(format!("correlation_key_{index}")),
            implicit_column_name: None,
            contains_aggregates: false,
        });
    }
    let column_count = group_columns.len();
    inner_plan.group_by = Some(GroupBy {
        exprs: group_columns,
        sort_order: vec![SortOrder::Asc; column_count],
        nulls_order: vec![None; column_count],
        sort_elided: false,
        having: None,
    });

    let mut grouped_table = JoinedTable::new_subquery(
        format!("scalar_subquery_{subquery_id}"),
        inner_plan,
        Some(JoinInfo {
            join_type: JoinType::LeftOuter,
            using: vec![],
            no_reorder: false,
        }),
        subquery_id,
    )?;
    // A scalar subquery result has no text order. Do not give its replacement
    // the text order of the grouped table's first result column.
    let Table::FromClauseSubquery(grouped_subquery) = &mut grouped_table.table else {
        unreachable!("a grouped table must hold a subquery")
    };
    Arc::get_mut(grouped_subquery)
        .expect("a new grouped subquery is not shared")
        .columns[0]
        .set_collation(None);
    for column in 0..grouped_table.columns().len() {
        grouped_table.mark_column_used(column);
    }

    let result_column = Expr::Column {
        database: None,
        table: subquery_id,
        column: 0,
        is_rowid_alias: false,
    };
    let replacement = match empty_value {
        EmptyInputValue::Null => result_column,
        EmptyInputValue::IntegerZero => coalesce_with_zero(result_column, "0"),
        EmptyInputValue::RealZero => coalesce_with_zero(result_column, "0.0"),
    };
    if !replace_subquery_value(plan, subquery_id, &replacement)? {
        return Ok(false);
    }

    for (pair, column) in pairs.into_iter().zip(result_positions) {
        let grouped_column = Expr::Column {
            database: None,
            table: subquery_id,
            column,
            is_rowid_alias: false,
        };
        let (left, right) = if pair.inner_was_left {
            (grouped_column, pair.outer)
        } else {
            (pair.outer, grouped_column)
        };
        plan.where_clause.push(WhereTerm {
            expr: Expr::Binary(Box::new(left), ast::Operator::Equals, Box::new(right)),
            from_outer_join: Some(subquery_id),
            consumed: false,
        });
    }
    plan.table_references.add_joined_table(grouped_table);
    plan.non_from_clause_subqueries.remove(subquery_index);
    Ok(true)
}

/// Return whether a one-value aggregate is simple enough to move into a join.
fn can_rewrite_single_value_aggregate(plan: &SelectPlan, resolver: &Resolver<'_>) -> Result<bool> {
    if plan.result_columns.len() != 1
        || plan.aggregates.is_empty()
        || plan.table_references.joined_tables().is_empty()
        || plan.group_by.is_some()
        || !plan.order_by.is_empty()
        || plan.offset.is_some()
        || !plan.values.is_empty()
        || plan.window.is_some()
        || !matches!(plan.distinctness, Distinctness::NonDistinct)
        || !plan.non_from_clause_subqueries.is_empty()
    {
        return Ok(false);
    }
    if !matches!(
        plan.limit.as_deref(),
        Some(Expr::Literal(ast::Literal::Numeric(value))) if value.parse::<i64>() == Ok(1)
    ) {
        return Ok(false);
    }
    if plan
        .table_references
        .joined_tables()
        .iter()
        .any(|table| match &table.table {
            // A virtual table can store function arguments as hidden-column
            // checks. Moving those checks would change the function call.
            crate::schema::Table::Virtual(_) => true,
            crate::schema::Table::FromClauseSubquery(subquery) => {
                plan_is_correlated(&subquery.plan)
            }
            _ => false,
        })
    {
        return Ok(false);
    }
    for column in &plan.result_columns {
        if expr_contains_nondeterministic_scalar_function(&column.expr, resolver)? {
            return Ok(false);
        }
    }
    for term in &plan.where_clause {
        if expr_contains_nondeterministic_scalar_function(&term.expr, resolver)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Return the result for no input rows, if it is known.
fn result_on_empty_input(plan: &SelectPlan) -> Option<EmptyInputValue> {
    let expr = &plan.result_columns[0].expr;
    for aggregate in &plan.aggregates {
        if !exprs_are_equivalent(expr, &aggregate.original_expr) {
            continue;
        }
        return match aggregate.func {
            AggFunc::Count | AggFunc::Count0 => Some(EmptyInputValue::IntegerZero),
            AggFunc::Total => Some(EmptyInputValue::RealZero),
            AggFunc::Avg
            | AggFunc::GroupConcat
            | AggFunc::Max
            | AggFunc::Min
            | AggFunc::StringAgg
            | AggFunc::Sum => Some(EmptyInputValue::Null),
            _ => None,
        };
    }
    is_null_on_empty_input(expr, &plan.aggregates).then_some(EmptyInputValue::Null)
}

/// Return whether an expression must be NULL when its input is empty.
fn is_null_on_empty_input(expr: &Expr, aggregates: &[crate::translate::plan::Aggregate]) -> bool {
    if aggregates.iter().any(|aggregate| {
        exprs_are_equivalent(expr, &aggregate.original_expr)
            && matches!(
                aggregate.func,
                AggFunc::Avg
                    | AggFunc::GroupConcat
                    | AggFunc::Max
                    | AggFunc::Min
                    | AggFunc::StringAgg
                    | AggFunc::Sum
            )
    }) {
        return true;
    }
    match expr {
        Expr::Binary(
            left,
            ast::Operator::Add
            | ast::Operator::Subtract
            | ast::Operator::Multiply
            | ast::Operator::Divide
            | ast::Operator::Modulus
            | ast::Operator::BitwiseAnd
            | ast::Operator::BitwiseOr
            | ast::Operator::LeftShift
            | ast::Operator::RightShift
            | ast::Operator::Concat,
            right,
        ) => is_null_on_empty_input(left, aggregates) || is_null_on_empty_input(right, aggregates),
        Expr::Unary(_, inner) | Expr::Cast { expr: inner, .. } | Expr::Collate(inner, _) => {
            is_null_on_empty_input(inner, aggregates)
        }
        Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            is_null_on_empty_input(&exprs[0], aggregates)
        }
        _ => false,
    }
}

/// Read an `=` check between one inner column and one outer column.
fn read_column_pair(
    expr: &Expr,
    outer_table_ids: &[TableInternalId],
    inner_table_ids: &[TableInternalId],
) -> Option<ColumnPair> {
    let Expr::Binary(left, ast::Operator::Equals, right) = expr else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (
            inner @ Expr::Column {
                table: inner_table, ..
            },
            outer @ Expr::Column {
                table: outer_table, ..
            },
        ) if inner_table_ids.contains(inner_table) && outer_table_ids.contains(outer_table) => {
            Some(ColumnPair {
                inner: inner.clone(),
                outer: outer.clone(),
                inner_was_left: true,
            })
        }
        (
            outer @ Expr::Column {
                table: outer_table, ..
            },
            inner @ Expr::Column {
                table: inner_table, ..
            },
        ) if outer_table_ids.contains(outer_table) && inner_table_ids.contains(inner_table) => {
            Some(ColumnPair {
                inner: inner.clone(),
                outer: outer.clone(),
                inner_was_left: false,
            })
        }
        _ => None,
    }
}

/// Return whether grouping and joining compare this column pair the same way.
fn column_pair_compares_the_same(pair: &ColumnPair, tables: &TableReferences) -> bool {
    // GROUP BY and the join must treat these values the same way. With
    // different value types or text sort rules, two inner groups can both
    // equal one outer value and copy its row.
    if get_expr_affinity(&pair.inner, Some(tables), None)
        != get_expr_affinity(&pair.outer, Some(tables), None)
    {
        return false;
    }

    let inner_collation = get_collseq_from_expr(&pair.inner, tables)
        .ok()
        .flatten()
        .unwrap_or_default();
    let outer_collation = get_collseq_from_expr(&pair.outer, tables)
        .ok()
        .flatten()
        .unwrap_or_default();
    inner_collation == outer_collation
}

/// Return whether the plan uses an outer table outside its `WHERE` clause.
fn uses_outer_tables_outside_where(plan: &SelectPlan, outer_table_ids: &[TableInternalId]) -> bool {
    let uses_outer = |expr: &Expr| {
        collect_table_refs(expr)
            .iter()
            .any(|table| outer_table_ids.contains(table))
    };
    plan.result_columns
        .iter()
        .any(|column| uses_outer(&column.expr))
        || plan.order_by.iter().any(|(expr, _, _)| uses_outer(expr))
        || plan.limit.as_deref().is_some_and(uses_outer)
        || plan.offset.as_deref().is_some_and(uses_outer)
        || plan.group_by.as_ref().is_some_and(|group| {
            group.exprs.iter().any(&uses_outer)
                || group
                    .having
                    .as_ref()
                    .is_some_and(|having| having.iter().any(&uses_outer))
        })
}

/// Keep the zero that `count` and `total` return for an empty input.
fn coalesce_with_zero(value: Expr, zero: &str) -> Expr {
    Expr::FunctionCall {
        name: Name::exact("coalesce".to_string()),
        distinctness: None,
        args: vec![
            Box::new(value),
            Box::new(Expr::Literal(ast::Literal::Numeric(zero.to_string()))),
        ],
        order_by: vec![],
        within_group: vec![],
        filter_over: FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    }
}

/// Replace each use of one subquery value with a joined column.
fn replace_subquery_value(
    plan: &mut SelectPlan,
    subquery_id: TableInternalId,
    replacement: &Expr,
) -> Result<bool> {
    let mut found = false;
    let mut replace = |expr: &mut Expr| -> Result<WalkControl> {
        if matches!(
            expr,
            Expr::SubqueryResult {
                subquery_id: id,
                query_type: ast::SubqueryType::RowValue { num_regs: 1, .. },
                ..
            } if *id == subquery_id
        ) {
            *expr = replacement.clone();
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    };

    for column in &mut plan.result_columns {
        walk_expr_mut(&mut column.expr, &mut replace)?;
    }
    for term in &mut plan.where_clause {
        walk_expr_mut(&mut term.expr, &mut replace)?;
    }
    if let Some(group) = &mut plan.group_by {
        for expr in &mut group.exprs {
            walk_expr_mut(expr, &mut replace)?;
        }
        if let Some(having) = &mut group.having {
            for expr in having {
                walk_expr_mut(expr, &mut replace)?;
            }
        }
    }
    for (expr, _, _) in &mut plan.order_by {
        walk_expr_mut(expr, &mut replace)?;
    }
    if let Some(limit) = &mut plan.limit {
        walk_expr_mut(limit, &mut replace)?;
    }
    if let Some(offset) = &mut plan.offset {
        walk_expr_mut(offset, &mut replace)?;
    }
    for row in &mut plan.values {
        for expr in row {
            walk_expr_mut(expr, &mut replace)?;
        }
    }
    for aggregate in &mut plan.aggregates {
        walk_expr_mut(&mut aggregate.original_expr, &mut replace)?;
        for arg in &mut aggregate.args {
            walk_expr_mut(arg, &mut replace)?;
        }
        if let Some(filter) = &mut aggregate.filter_expr {
            walk_expr_mut(filter, &mut replace)?;
        }
    }
    Ok(found)
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
        if let Expr::Column { table, .. } | Expr::RowId { table, .. } = expr {
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
        if let Expr::Column { table, .. } | Expr::RowId { table, .. } = expr {
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
