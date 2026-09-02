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
//! This is the **group-first** form: it groups the whole inner table first and
//! then joins the groups to the outer rows. In this module, a "key" means one
//! distinct value of the column that links the two queries (`i.key` above).
//! Group-first:
//!
//! - computes the aggregate for each key once, even when many outer rows ask
//!   for the same key;
//! - also computes the aggregate for keys that no outer row asks for; and
//! - is used for `avg`, `count`, `min`, `max`, and `total` when their inputs,
//!   aggregate `FILTER` expressions, and inner `WHERE` expressions cannot fail.
//!
//! The second point is the dangerous one. The original subquery only reads the
//! keys that outer rows ask for. Group-first reads every key, so it can hit an
//! error that the original query never hits. For example:
//!
//! - `sum` can overflow for unused key 99 when its values are
//!   `9223372036854775807` and `1`, although no outer row asks for 99;
//! - `group_concat` or `string_agg` can build an unused string larger than the
//!   largest SQL value that Turso allows; and
//! - `avg(json_extract(value, '$'))` can read invalid JSON for an unused key.
//!
//! When group-first is unsafe, a direct `WHERE` comparison can instead use the
//! **join-first** form: it joins each outer row to its matching inner rows
//! first and then groups the joined rows back into one group per outer row.
//!
//! ```sql
//! -- Before
//! SELECT o.id
//! FROM outer_table o
//! WHERE o.limit > (
//!   SELECT sum(i.value) FROM inner_table i WHERE i.key = o.key
//! );
//!
//! -- After
//! SELECT o.id
//! FROM outer_table o
//! LEFT JOIN inner_table i ON i.key = o.key
//! GROUP BY o.rowid
//! HAVING o.limit > sum(i.value) FILTER (WHERE i.rowid IS NOT NULL)
//! ```
//!
//! The join only finds inner rows whose key some outer row asks for, so
//! join-first never computes an aggregate for an unused key. The `i.rowid`
//! filter keeps the NULL-filled row that a left join makes when no inner row
//! matches out of the aggregate. Without it, `sum(1)` would read that row and
//! return 1, while the original subquery returns NULL when nothing matches.
//!
//! Join-first is not used for every aggregate subquery:
//!
//! - Group-first does less work when several outer rows ask for the same key,
//!   because join-first computes the aggregate again for each of those rows.
//! - Join-first needs one outer B-tree table with a rowid so that
//!   `GROUP BY o.rowid` makes exactly one group for each outer row.
//! - It needs one inner B-tree table with a rowid, because `i.rowid IS NOT
//!   NULL` is how it recognizes the NULL-filled row made by a left join with
//!   no match.
//! - The current code moves only one direct `WHERE` comparison to `HAVING`. It
//!   does not handle the subquery value in a `SELECT` list, `ORDER BY`,
//!   `HAVING`, or inside a larger expression.
//! - The join makes each outer row appear once for each matching inner row,
//!   and the other `WHERE` terms then run once per copy instead of once per
//!   outer row. A term such as `random() % 2 = 0` could accept some copies and
//!   reject others, so the aggregate would see only some of the row's inner
//!   rows. Join-first is skipped when another `WHERE` term calls a
//!   nondeterministic function or reads a correlated subquery.
//! - Join-first must group by `o.rowid` to keep outer rows separate. An existing
//!   `GROUP BY o.key` may instead combine several outer rows. Combining these
//!   two groups needs another rewrite. Outer aggregates, `DISTINCT`, window
//!   functions, `ORDER BY`, `LIMIT`, and `OFFSET` also need separate rules
//!   around the new group. Those rules are not implemented.
//! - Extension aggregates stay as subqueries. With no matching inner row,
//!   `count` returns 0 and `sum` returns NULL. An extension aggregate may return
//!   something else, and this code does not know which value to use.
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

use rustc_hash::FxHashMap as HashMap;
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
        expr_contains_nondeterministic_scalar_function, expr_references_any_subquery,
        expr_references_subquery_id, expression_can_fail_on_input, get_expr_affinity, walk_expr,
        walk_expr_mut, WalkControl,
    },
    plan::{
        plan_is_correlated, Distinctness, GroupBy, JoinInfo, JoinOrigin, JoinType, JoinedTable,
        QueryDestination, ResultSetColumn, SelectPlan, SubqueryState, TableReferences, WhereTerm,
        WhereTermOrigin,
    },
};
use crate::util::exprs_are_equivalent;
use crate::Result;

/// Try each supported rewrite and return whether the plan changed.
pub fn rewrite_correlated_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let has_unmatched_right_rows = plan.table_references.joined_tables().iter().any(|table| {
        table
            .join_info
            .as_ref()
            .is_some_and(JoinInfo::keeps_right_rows)
    });
    if has_unmatched_right_rows {
        // SQLite keeps correlated subqueries inside the shared join body. A
        // later semi-join cannot take part in the unmatched-right scan.
        return Ok(false);
    }
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

    let mut aggregate_replacements = HashMap::default();
    let mut subquery_index = 0;
    while subquery_index < plan.non_from_clause_subqueries.len() {
        let subquery = &plan.non_from_clause_subqueries[subquery_index];
        if !subquery.correlated {
            subquery_index += 1;
            continue;
        }
        let query_type = subquery.query_type.clone();
        let subquery_id = subquery.internal_id;
        let same_query = subquery.same_query;
        match query_type {
            ast::SubqueryType::In { .. } => {
                if try_rewrite_in(plan, subquery_index, resolver)? {
                    changed = true;
                    continue;
                }
            }
            ast::SubqueryType::RowValue { num_regs: 1, .. } => {
                if let Some(replacement) = same_query
                    .and_then(|same_query| aggregate_replacements.get(&same_query))
                    .cloned()
                {
                    if replace_subquery_value(plan, subquery_id, &replacement)? {
                        plan.non_from_clause_subqueries.remove(subquery_index);
                        changed = true;
                        continue;
                    }
                }
                if let Some(rewrite) =
                    try_rewrite_single_value_aggregate(plan, subquery_index, resolver)?
                {
                    if let AggregateRewrite::GroupedTable(replacement) = rewrite {
                        aggregate_replacements.insert(subquery_id, replacement);
                    }
                    changed = true;
                    continue;
                }
            }
            _ => {}
        }
        subquery_index += 1;
    }
    Ok(changed)
}

/// Return the SELECT plan for a subquery that has not run yet.
fn select_subquery_plan(plan: &SelectPlan, subquery_index: usize) -> Option<&SelectPlan> {
    let subquery = &plan.non_from_clause_subqueries[subquery_index];
    let SubqueryState::Unevaluated { plan: inner } = &subquery.state else {
        return None;
    };
    let Plan::Select(inner) = inner.as_ref()?.as_ref() else {
        return None;
    };
    Some(inner)
}

/// Take a SELECT plan after all rewrite checks have passed.
fn take_select_subquery_plan(plan: &mut SelectPlan, subquery_index: usize) -> Box<SelectPlan> {
    let subquery = &mut plan.non_from_clause_subqueries[subquery_index];
    let SubqueryState::Unevaluated { plan: inner } = &mut subquery.state else {
        unreachable!("a checked subquery must not have run")
    };
    let Plan::Select(inner) = *inner.take().expect("a checked subquery must have a plan") else {
        unreachable!("a checked subquery must be a SELECT")
    };
    inner
}

/// Try to change one `EXISTS` or `NOT EXISTS` into a join.
fn try_rewrite_exists(
    plan: &mut SelectPlan,
    subquery_index: usize,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let subquery_id = plan.non_from_clause_subqueries[subquery_index].internal_id;

    let Some(term) = find_exists_in_where(&plan.where_clause, subquery_id) else {
        return Ok(false);
    };
    if plan.table_references.joined_tables().is_empty() {
        return Ok(false);
    }
    if select_subquery_plan(plan, subquery_index).is_none() {
        return Ok(false);
    }

    let join_type = if term.negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    rewrite_as_semi_or_anti_join(plan, subquery_index, term.index, join_type, None, resolver)
}

/// Turn a direct `IN` test that is not `NOT IN` into a semi-join.
fn try_rewrite_in(
    plan: &mut SelectPlan,
    subquery_index: usize,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    let subquery_id = plan.non_from_clause_subqueries[subquery_index].internal_id;
    let Some((where_term_index, left)) = find_in_term(&plan.where_clause, subquery_id) else {
        return Ok(false);
    };
    if plan.table_references.joined_tables().is_empty() {
        return Ok(false);
    }
    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(false);
    };
    let Some(right) = inner_plan
        .result_columns
        .first()
        .map(|column| column.expr.clone())
    else {
        return Ok(false);
    };
    // A semi-join changes which expressions run and how many times they run:
    //
    // - IN runs the inner query to the end. For example,
    //   `1 IN (SELECT json_extract(value, '$') FROM t)` must fail when any row
    //   of `t` holds invalid JSON, even a row after the match. A semi-join
    //   stops at the first match, so it would skip the invalid row and hide
    //   the error.
    // - IN evaluates its left side once per outer row. A semi-join uses the
    //   left side as a join condition, so it can evaluate the left side once
    //   for every inner row it scans.
    //
    // So: keep IN as a subquery when its left side, its result expression, or
    // any inner WHERE expression can fail. JSON is only one example; the same
    // rule applies to any function or operator that can return an error.
    if inner_plan.result_columns.len() != 1
        || expression_can_fail_on_input(&left)
        || expression_can_fail_on_input(&right)
        || inner_plan
            .where_clause
            .iter()
            .any(|term| expression_can_fail_on_input(&term.expr))
    {
        return Ok(false);
    }

    let extra_term = WhereTerm {
        expr: Expr::Binary(Box::new(left), ast::Operator::Equals, Box::new(right)),
        origin: WhereTermOrigin::Where,
        consumed: false,
    };
    rewrite_as_semi_or_anti_join(
        plan,
        subquery_index,
        where_term_index,
        JoinType::Semi,
        Some(extra_term),
        resolver,
    )
}

/// Move one simple subquery into the outer query as a semi-join or anti-join.
fn rewrite_as_semi_or_anti_join(
    plan: &mut SelectPlan,
    subquery_index: usize,
    where_term_index: usize,
    join_type: JoinType,
    extra_term: Option<WhereTerm>,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    // A semi join needs a left table. Keep the subquery when the outer SELECT
    // has no FROM clause.
    if plan.table_references.joined_tables().is_empty() {
        return Ok(false);
    }

    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(false);
    };
    if !can_rewrite_as_semi_join(inner_plan, resolver)? {
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

    for term in inner_plan.where_clause.iter().chain(extra_term.iter()) {
        if !can_move_join_term(&term.expr, &outer_table_ids, &inner_table_ids) {
            return Ok(false);
        }
    }

    // Every NOT EXISTS term must use an inner table. Moving a constant false
    // term into the outer query would reject a row that NOT EXISTS keeps.
    if join_type == JoinType::Anti {
        for term in inner_plan.where_clause.iter().chain(extra_term.iter()) {
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
        for term in inner_plan.where_clause.iter().chain(extra_term.iter()) {
            let tables = collect_table_refs(&term.expr);
            if tables
                .iter()
                .any(|table| outer_table_ids_that_may_be_null.contains(table))
            {
                return Ok(false);
            }
        }
    }

    let mut inner_plan = take_select_subquery_plan(plan, subquery_index);
    if let Some(extra_term) = extra_term {
        inner_plan.where_clause.push(extra_term);
    }
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

/// Where the aggregate result is stored after a rewrite.
#[expect(clippy::large_enum_variant)]
enum AggregateRewrite {
    /// The result is a column in a grouped table. Another reference to the same
    /// subquery can read that column instead of building another grouped table.
    GroupedTable(Expr),
    /// The aggregate moved into the outer query and is used by its `HAVING`
    /// clause. There is no result column for another subquery to reuse.
    Joined,
}

/// Rewrite one correlated aggregate subquery using group-first or join-first.
///
/// - Try group-first when computing the aggregate for unused keys cannot fail.
///   This form computes each key once, and a second identical subquery can
///   reuse its result.
/// - Otherwise, try join-first. This form never computes an aggregate for an
///   unused key, but it supports fewer query shapes.
/// - Both forms use a left join. The original subquery returns one value even
///   when no inner row matches, so the rewrite must keep such outer rows too.
fn try_rewrite_single_value_aggregate(
    plan: &mut SelectPlan,
    subquery_index: usize,
    resolver: &Resolver<'_>,
) -> Result<Option<AggregateRewrite>> {
    // A window plan stores another copy of its expressions. Rewriting only the
    // copy here would leave the other copy pointing at the removed subquery.
    if plan.window.is_some() {
        return Ok(None);
    }

    let subquery_id = plan.non_from_clause_subqueries[subquery_index].internal_id;

    // A value used by an outer join condition must be ready before that join
    // decides whether to fill its right side with NULL values.
    if plan.where_clause.iter().any(|term| {
        term.origin.join_origin().is_some_and(JoinOrigin::is_outer)
            && expr_references_subquery_id(&term.expr, subquery_id)
    }) {
        return Ok(None);
    }
    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(None);
    };

    if !can_rewrite_single_value_aggregate(inner_plan, resolver)? {
        return Ok(None);
    }
    let Some(empty_value) = result_on_empty_input(inner_plan) else {
        return Ok(None);
    };

    let outer_tables = inner_plan.table_references.outer_query_refs();
    if outer_tables.iter().any(|outer_table| {
        outer_table.is_used()
            && (outer_table.scope_depth != 0
                || plan
                    .table_references
                    .find_joined_table_by_internal_id(outer_table.internal_id)
                    .is_none())
    }) {
        return Ok(None);
    }
    // A grouped table linked to more than one outer table can move before one
    // of those tables after its left join becomes an inner join.
    if outer_tables
        .iter()
        .filter(|outer_table| outer_table.is_used())
        .count()
        != 1
    {
        return Ok(None);
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
        if term.origin.join_origin().is_some_and(JoinOrigin::is_outer) {
            return Ok(None);
        }
        let Some(pair) = read_column_pair(&term.expr, &outer_table_ids, &inner_table_ids) else {
            return Ok(None);
        };
        if !column_pair_compares_the_same(&pair, &inner_plan.table_references)? {
            return Ok(None);
        }
        pairs.push(pair);
    }
    if pairs.is_empty() || uses_outer_tables_outside_where(inner_plan, &outer_table_ids) {
        return Ok(None);
    }

    if !aggregate_can_run_for_unused_rows(inner_plan) {
        return rewrite_aggregate_as_join_then_group(plan, subquery_index, subquery_id, resolver);
    }

    let mut inner_plan = *take_select_subquery_plan(plan, subquery_index);
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
        return Ok(None);
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
            origin: WhereTermOrigin::Join(JoinOrigin::Outer(subquery_id)),
            consumed: false,
        });
    }
    plan.table_references.add_joined_table(grouped_table);
    plan.non_from_clause_subqueries.remove(subquery_index);
    Ok(Some(AggregateRewrite::GroupedTable(replacement)))
}

/// Join matching rows before computing an aggregate such as `sum`.
///
/// ```sql
/// -- Before
/// SELECT o.id FROM outer_rows o
/// WHERE o.limit > (SELECT sum(i.x) FROM inner_rows i WHERE i.key = o.key);
///
/// -- After
/// SELECT o.id FROM outer_rows o
/// LEFT JOIN inner_rows i ON i.key = o.key
/// GROUP BY o.rowid
/// HAVING o.limit > sum(i.x) FILTER (WHERE i.rowid IS NOT NULL);
/// ```
///
/// Use this form only when all of these rules hold:
///
/// - The outer query reads one B-tree table with a rowid. `GROUP BY o.rowid`
///   then makes one group for each original outer row.
/// - The inner query reads one B-tree table with a rowid. The rewrite uses
///   `i.rowid IS NOT NULL` to keep the NULL-filled row made by a left join with
///   no match out of the aggregate.
/// - The outer query has no aggregate, `GROUP BY`, `DISTINCT`, window function,
///   `ORDER BY`, `LIMIT`, `OFFSET`, or `VALUES`. The current rewrite does not
///   preserve these operations around its new grouping step.
/// - The subquery is one complete side of one top-level `WHERE` comparison.
///   That whole comparison can move to `HAVING` without leaving another use of
///   the removed subquery.
/// - Every other `WHERE` term keeps the same value each time it runs. After
///   the rewrite, an outer row appears once for each matching inner row, and
///   the other `WHERE` terms run once per copy instead of once per outer row.
///   A term such as `random() % 2 = 0` could then accept some copies and
///   reject others, and the aggregate would see only some of the row's inner
///   rows. So a term must not call a nondeterministic function and must not
///   read a correlated subquery, which runs again for each copy. Reading an
///   uncorrelated subquery is fine because it runs once and its stored result
///   is the same for every copy.
fn rewrite_aggregate_as_join_then_group(
    plan: &mut SelectPlan,
    subquery_index: usize,
    subquery_id: TableInternalId,
    resolver: &Resolver<'_>,
) -> Result<Option<AggregateRewrite>> {
    if plan.table_references.joined_tables().len() != 1
        || !plan.aggregates.is_empty()
        || plan.group_by.is_some()
        || !plan.order_by.is_empty()
        || plan.limit.is_some()
        || plan.offset.is_some()
        || !plan.values.is_empty()
        || plan.window.is_some()
        || plan.distinctness.is_distinct()
    {
        return Ok(None);
    }

    let outer_table = &plan.table_references.joined_tables()[0];
    if !matches!(&outer_table.table, Table::BTree(table) if table.has_rowid) {
        return Ok(None);
    }
    let outer_table_id = outer_table.internal_id;

    let Some(inner_plan) = select_subquery_plan(plan, subquery_index) else {
        return Ok(None);
    };
    if inner_plan.table_references.joined_tables().len() != 1
        || !matches!(
            &inner_plan.table_references.joined_tables()[0].table,
            Table::BTree(table) if table.has_rowid
        )
    {
        return Ok(None);
    }

    let result = inner_plan.result_columns[0].expr.clone();
    let Some((where_index, having)) =
        find_direct_aggregate_comparison(plan, subquery_id, &result, resolver)?
    else {
        return Ok(None);
    };

    let mut inner_plan = *take_select_subquery_plan(plan, subquery_index);
    let mut inner_tables = std::mem::take(inner_plan.table_references.joined_tables_mut());
    let mut inner_table = inner_tables
        .pop()
        .expect("a checked aggregate subquery must have one table");
    let inner_table_id = inner_table.internal_id;
    inner_table.join_info = Some(JoinInfo {
        join_type: JoinType::LeftOuter,
        using: vec![],
        no_reorder: false,
    });
    plan.table_references.add_joined_table(inner_table);

    for mut term in inner_plan.where_clause {
        term.origin = match term.origin {
            WhereTermOrigin::TableFunction(_) => {
                WhereTermOrigin::TableFunction(JoinOrigin::Outer(inner_table_id))
            }
            WhereTermOrigin::Where | WhereTermOrigin::Join(_) => {
                WhereTermOrigin::Join(JoinOrigin::Outer(inner_table_id))
            }
        };
        term.consumed = false;
        plan.where_clause.push(term);
    }

    let inner_row_exists = Expr::NotNull(Box::new(Expr::RowId {
        database: None,
        table: inner_table_id,
    }));
    for aggregate in &mut inner_plan.aggregates {
        aggregate.filter_expr = Some(match aggregate.filter_expr.take() {
            Some(filter) => Expr::Binary(
                Box::new(inner_row_exists.clone()),
                ast::Operator::And,
                Box::new(filter),
            ),
            None => inner_row_exists.clone(),
        });
    }
    plan.aggregates.extend(inner_plan.aggregates);
    plan.where_clause.remove(where_index);
    plan.group_by = Some(GroupBy {
        exprs: vec![Expr::RowId {
            database: None,
            table: outer_table_id,
        }],
        sort_order: vec![SortOrder::Asc],
        nulls_order: vec![None],
        sort_elided: false,
        having: Some(vec![having]),
    });
    plan.non_from_clause_subqueries.remove(subquery_index);
    Ok(Some(AggregateRewrite::Joined))
}

/// Find one comparison that can move from `WHERE` to `HAVING`.
///
/// This form is accepted:
///
/// ```sql
/// WHERE o.limit > (SELECT sum(i.x) FROM inner_rows i WHERE i.key = o.key)
/// ```
///
/// These forms are rejected:
///
/// - `SELECT (SELECT sum(...))`: the value is used outside `WHERE`.
/// - `WHERE o.limit > 1 + (SELECT sum(...))`: the subquery is inside a larger
///   expression.
/// - `WHERE (SELECT sum(...)) > (SELECT max(...))`: moving one subquery would
///   leave another subquery in the new `HAVING` clause.
/// - The same subquery is used by two `WHERE` terms.
/// - The comparison came from an outer join condition. Moving it to `HAVING`
///   would change which rows the outer join fills with NULL values.
/// - Another `WHERE` term calls a nondeterministic function or reads a
///   correlated subquery. After the rewrite these terms run once per joined
///   copy of an outer row, so their value must be the same for every copy.
///
/// The rewrite removes the subquery, so every use of its value must be handled
/// here. This function handles only one complete comparison.
fn find_direct_aggregate_comparison(
    plan: &SelectPlan,
    subquery_id: TableInternalId,
    replacement: &Expr,
    resolver: &Resolver<'_>,
) -> Result<Option<(usize, Expr)>> {
    if plan
        .result_columns
        .iter()
        .any(|column| expr_references_subquery_id(&column.expr, subquery_id))
    {
        return Ok(None);
    }

    let mut found = None;
    for (index, term) in plan.where_clause.iter().enumerate() {
        if !expr_references_subquery_id(&term.expr, subquery_id) {
            if expr_contains_nondeterministic_scalar_function(&term.expr, resolver)?
                || expr_references_correlated_subquery(&term.expr, plan)
            {
                return Ok(None);
            }
            continue;
        }
        if found.is_some() || term.origin.join_origin().is_some_and(JoinOrigin::is_outer) {
            return Ok(None);
        }
        let Expr::Binary(left, operator, right) = &term.expr else {
            return Ok(None);
        };
        if !operator.is_comparison() {
            return Ok(None);
        }

        let is_subquery = |expr: &Expr| {
            matches!(
                expr,
                Expr::SubqueryResult {
                    subquery_id: id,
                    query_type: ast::SubqueryType::RowValue { num_regs: 1, .. },
                    ..
                } if *id == subquery_id
            )
        };
        let expr = if is_subquery(left) && !expr_references_any_subquery(right) {
            Expr::Binary(Box::new(replacement.clone()), *operator, right.clone())
        } else if is_subquery(right) && !expr_references_any_subquery(left) {
            Expr::Binary(left.clone(), *operator, Box::new(replacement.clone()))
        } else {
            return Ok(None);
        };
        found = Some((index, expr));
    }
    Ok(found)
}

/// Return whether an expression reads the result of a correlated subquery.
fn expr_references_correlated_subquery(expr: &Expr, plan: &SelectPlan) -> bool {
    plan.non_from_clause_subqueries
        .iter()
        .filter(|subquery| subquery.correlated)
        .any(|subquery| expr_references_subquery_id(expr, subquery.internal_id))
}

/// Check whether an aggregate subquery uses a form that both rewrites support.
///
/// For example, this is supported:
///
/// ```sql
/// SELECT avg(i.value) FROM inner_rows i WHERE i.key = o.key
/// ```
///
/// The shared rules are:
///
/// - The subquery returns one expression, contains an aggregate, and reads at
///   least one table.
/// - It has no `GROUP BY`, `ORDER BY`, window function, `DISTINCT`, or `VALUES`.
///   The two rewrites change the grouping and the location of the inner tables.
///   Each of these operations needs its own rule to keep the same result, and
///   those rules are not implemented.
/// - It has no nested subquery. A nested subquery has a separate plan, and this
///   rewrite does not move that plan with the inner tables.
/// - Its only limit is `LIMIT 1`. Planning a scalar subquery adds this limit.
///   `LIMIT 0` removes the result row, and `OFFSET 1` skips it.
/// - It reads no virtual table. For example, the hidden columns of
///   `generate_series(o.first, o.last)` are function arguments. They cannot be
///   moved like normal `WHERE` checks.
/// - It reads no correlated subquery in `FROM`. Such a subquery still needs
///   values from the current outer row and needs a separate rewrite.
/// - Its result and `WHERE` clause contain no function such as `random()` whose
///   result can change between calls. A rewrite can change the number of calls.
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

/// Return whether group-first may compute the aggregate for every key,
/// including keys that no outer row asks for.
///
/// Suppose the outer query asks only for key 1, while the inner table also has
/// key 99. Group-first computes the aggregate for both keys. This function
/// returns true only when:
///
/// - every aggregate is `avg`, `count`, `min`, `max`, or `total`; and
/// - no aggregate input, aggregate `FILTER`, or inner `WHERE` expression can
///   fail for data stored under unused key 99.
///
/// `sum` can overflow. String aggregates can grow too large. Any aggregate not
/// listed above also returns false because this code has not proved that it is
/// safe for unused keys. The caller then tries join-first, which computes only
/// keys requested by outer rows. If join-first does not support this kind of
/// query, the correlated subquery stays unchanged.
fn aggregate_can_run_for_unused_rows(plan: &SelectPlan) -> bool {
    if !plan.aggregates.iter().all(|aggregate| {
        matches!(
            aggregate.func,
            AggFunc::Avg
                | AggFunc::Count
                | AggFunc::Count0
                | AggFunc::Max
                | AggFunc::Min
                | AggFunc::Total
        )
    }) {
        return false;
    }

    let aggregate_expressions = plan
        .aggregates
        .iter()
        .flat_map(|aggregate| aggregate.args.iter().chain(aggregate.filter_expr.iter()));
    let filter_expressions = plan.where_clause.iter().map(|term| &term.expr);
    !aggregate_expressions
        .chain(filter_expressions)
        .any(expression_can_fail_on_input)
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

/// Return whether grouping and joining use the same comparison rules.
///
/// For example, an inner `BINARY` key can put `A` and `a` in two groups, while
/// an outer `NOCASE` key can join to both groups. An inner key can also put the
/// number `1` and the text `'1'` in two groups, while a numeric outer key joins
/// to both. Either case would return the outer row twice. Use the grouped form
/// only when both columns use the same number/text conversion rule and the same
/// text order.
fn column_pair_compares_the_same(pair: &ColumnPair, tables: &TableReferences) -> Result<bool> {
    if get_expr_affinity(&pair.inner, Some(tables), None)
        != get_expr_affinity(&pair.outer, Some(tables), None)
    {
        return Ok(false);
    }

    let inner_collation = get_collseq_from_expr(&pair.inner, tables)?.unwrap_or_default();
    let outer_collation = get_collseq_from_expr(&pair.outer, tables)?.unwrap_or_default();
    Ok(inner_collation == outer_collation)
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
        if term.origin.join_origin().is_some_and(JoinOrigin::is_outer) {
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
        if term.origin.join_origin().is_some_and(JoinOrigin::is_outer) {
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
