//! Changes a correlated subquery into a join of the query block that uses it,
//! when both forms give the same rows.
//!
//! - `EXISTS` becomes a semi-join. It keeps an outer row when an inner row
//!   matches.
//! - `NOT EXISTS` becomes an anti-join. It keeps an outer row when no inner row
//!   matches.
//! - A direct positive `IN` becomes a semi-join with one extra equality test.
//! - A subquery that returns one aggregate value becomes a FROM subquery
//!   grouped by the columns that link it to the outer row, LEFT JOINed on
//!   those columns (the group-first form). When computing the aggregate
//!   for keys no outer row asks for could fail, a direct WHERE comparison
//!   may instead LEFT JOIN the inner table and group by the outer rowid
//!   (the join-first form).
//!
//! The rewrite works on HIR only: the subquery's source moves into the outer
//! block's FROM clause with its identity, so every column reference to it
//! stays valid, and its WHERE terms move into the outer block's WHERE clause.
//! The planner plans both forms and keeps the rewritten one when it costs no
//! more than the original.

use turso_parser::ast::{Distinctness, Operator, UnaryOperator};

use super::{
    optimizer::CostModelParams,
    plan::PredicateExpr,
    semantic::hir::{self, HirDocument, QueryBlockId, QueryId, SourceId, SourceOwner},
    semantic_to_plan::{hir_expr_can_fail, HirPlan},
};
use crate::{function::Deterministic, schema::Schema, sync::Arc, Result};
use rustc_hash::FxHashSet as HashSet;

/// Rewrite the correlated subqueries of every query block whose rewritten
/// form costs no more than its original form, and return the document to
/// plan. Blocks are decided from the innermost query out, so a block is
/// costed with the forms already chosen for the queries it runs.
/// Also return the parameters of the parts the kept rewrites dropped, such
/// as the SELECT list of an EXISTS, which callers still bind.
pub(crate) fn unnest_correlated_subqueries(
    document: Arc<HirDocument>,
    schema: &Schema,
    params: &CostModelParams,
    mode: crate::SubqueryUnnestingMode,
) -> Result<(Arc<HirDocument>, Vec<hir::Parameter>)> {
    if mode == crate::SubqueryUnnestingMode::Disabled {
        return Ok((document, Vec::new()));
    }
    let candidates = queries_innermost_first(&document)
        .into_iter()
        .flat_map(|query| {
            let blocks = document.query(query).map_or(0, |query| query.blocks.len());
            (0..blocks).map(move |index| QueryBlockId::new(query, index))
        })
        .filter(|block| has_candidate(&document, *block))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok((document, Vec::new()));
    }
    let mut document = document;
    let mut dropped_parameters = Vec::new();
    let mut original_plan: Option<HirPlan> = None;
    for block in candidates {
        let mut rewritten = (*document).clone();
        let mut dropped = Vec::new();
        if !rewrite_block(&mut rewritten, block, &mut dropped)? {
            continue;
        }
        #[cfg(debug_assertions)]
        rewritten.validate().map_err(|error| {
            crate::LimboError::InternalError(format!(
                "unnesting a subquery produced invalid HIR: {error}"
            ))
        })?;
        let rewritten = Arc::new(rewritten);
        let original = match &original_plan {
            Some(plan) => plan,
            None => original_plan.insert(HirPlan::build_without_unnesting(
                document.clone(),
                schema,
                params,
            )?),
        };
        // A form the planner cannot plan is not chosen.
        let Ok(rewritten_plan) =
            HirPlan::build_without_unnesting(rewritten.clone(), schema, params)
        else {
            continue;
        };
        let original_cost = query_cost(original, block.query, params);
        let rewritten_cost = query_cost(&rewritten_plan, block.query, params);
        // Equal work is better without one subquery call per outer row,
        // and a block with a FULL join whose correlated
        // subqueries all became joins always keeps that form.
        if mode == crate::SubqueryUnnestingMode::Forced
            || complete_full_join_rewrite(&rewritten, block)
            || matches!(
                (original_cost, rewritten_cost),
                (Some(original), Some(rewritten)) if rewritten <= original
            )
        {
            document = rewritten;
            original_plan = Some(rewritten_plan);
            dropped_parameters.extend(dropped);
        }
    }
    Ok((document, dropped_parameters))
}

/// Whether `block` has a FULL join and no correlated subquery left.
fn complete_full_join_rewrite(document: &HirDocument, block: QueryBlockId) -> bool {
    document
        .query_block(block)
        .and_then(|block| block.from.as_ref())
        .is_some_and(|from| {
            from.joins
                .iter()
                .any(|join| join.kind == hir::JoinKind::Full)
        })
        && !block_subqueries(document, block)
            .into_iter()
            .any(|query| document.reads_outer_sources(query))
}

/// Every query of the document, each after the queries it runs.
fn queries_innermost_first(document: &HirDocument) -> Vec<QueryId> {
    let mut order = Vec::with_capacity(document.queries.len());
    let mut seen = HashSet::default();
    for query in &document.queries {
        // (query, whether its dependencies are already listed)
        let mut pending = vec![(query.id, false)];
        while let Some((query, expanded)) = pending.pop() {
            if expanded {
                order.push(query);
                continue;
            }
            if !seen.insert(query) {
                continue;
            }
            pending.push((query, true));
            for dependency in document.direct_query_dependencies(query).into_iter().rev() {
                if !seen.contains(&dependency) {
                    pending.push((dependency, false));
                }
            }
        }
    }
    order
}

/// The work of one run of `query`, plus the work of every subquery in its
/// expressions for the number of times the plan runs it, plus the work of
/// starting each call.
fn query_cost(plan: &HirPlan, query: QueryId, params: &CostModelParams) -> Option<f64> {
    let planned = plan.planned_query(query)?;
    let mut cost = planned.cost.0;
    for subquery in plan.document.expression_subqueries(query) {
        let calls = if plan.document.reads_outer_sources(subquery) {
            planned
                .blocks
                .iter()
                .flat_map(|block| block.subquery_calls.iter())
                .find_map(|(called, calls)| (*called == subquery).then_some(*calls))
                .unwrap_or(1.0)
        } else {
            1.0
        };
        let calls = calls.max(1.0);
        cost += query_cost(plan, subquery, params)? * calls + calls * params.cpu_cost_per_seek;
    }
    Some(cost)
}

fn block_filter(document: &HirDocument, block: QueryBlockId) -> Option<&hir::Expr> {
    match &document.query_block(block)?.body {
        hir::QueryBlockBody::Select { filter, .. } => filter.as_ref(),
        hir::QueryBlockBody::Values { .. } => None,
    }
}

fn filter_conjuncts(document: &HirDocument, block: QueryBlockId) -> Vec<hir::Expr> {
    block_filter(document, block)
        .map(|filter| filter.conjuncts().cloned().collect())
        .unwrap_or_default()
}

/// One subquery test that may become a join.
enum Candidate<'a> {
    Exists {
        query: QueryId,
        negated: bool,
    },
    In {
        lhs: &'a hir::Expr,
        query: QueryId,
        comparison: &'a hir::ComparisonSemantics,
    },
}

fn candidate(conjunct: &hir::Expr) -> Option<Candidate<'_>> {
    match conjunct {
        hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)) => Some(Candidate::Exists {
            query: *query,
            negated: false,
        }),
        hir::Expr::Unary {
            operator: UnaryOperator::Not,
            expr,
        } => match expr.as_ref() {
            hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)) => Some(Candidate::Exists {
                query: *query,
                negated: true,
            }),
            _ => None,
        },
        // NOT IN stays a subquery: a NULL in its list changes its result.
        hir::Expr::Subquery(hir::SubqueryExpr::In {
            lhs,
            query,
            negated: false,
            comparison,
        }) => Some(Candidate::In {
            lhs,
            query: *query,
            comparison,
        }),
        _ => None,
    }
}

fn candidate_query(candidate: &Candidate<'_>) -> QueryId {
    match candidate {
        Candidate::Exists { query, .. } | Candidate::In { query, .. } => *query,
    }
}

/// Whether `block` runs a correlated subquery, which may become a join.
fn has_candidate(document: &HirDocument, block: QueryBlockId) -> bool {
    block_subqueries(document, block)
        .into_iter()
        .any(|query| document.reads_outer_sources(query))
}

/// Rewrite every subquery of `block` that may become a join: `EXISTS` and
/// `NOT EXISTS` first, then `IN` and one-value subqueries in clause order:
/// WHERE, GROUP BY, the result columns, then ORDER BY. Return whether
/// anything changed.
fn rewrite_block(
    document: &mut HirDocument,
    block: QueryBlockId,
    dropped: &mut Vec<hir::Parameter>,
) -> Result<bool> {
    let mut changed = false;
    let mut position = 0;
    loop {
        let conjuncts = filter_conjuncts(document, block);
        let Some(conjunct) = conjuncts.get(position) else {
            break;
        };
        let rewritten = match candidate(conjunct) {
            Some(candidate @ Candidate::Exists { query, .. })
                if document.reads_outer_sources(query) =>
            {
                try_semi_or_anti_join(document, block, &conjuncts, position, &candidate, dropped)
            }
            _ => false,
        };
        if rewritten {
            // The conjunct is gone; the next one is now at `position`.
            changed = true;
        } else {
            position += 1;
        }
    }

    let mut tried = HashSet::default();
    // Each one-value subquery rewritten into a grouped table: the clause it
    // was in, its query as it was written, and what replaced it.
    let mut grouped: Vec<(Clause, hir::Query, hir::Expr)> = Vec::new();
    while let Some(query) = block_subqueries(document, block)
        .into_iter()
        .find(|query| !tried.contains(query) && document.reads_outer_sources(*query))
    {
        tried.insert(query);
        // A later copy of a rewritten subquery in the same clause reads the
        // same grouped table.
        let clause = subquery_clause(document, block, query);
        if let Some(replacement) = grouped
            .iter()
            .find_map(|(found_clause, found, replacement)| {
                (*found_clause == clause && queries_match(document, found, query))
                    .then(|| replacement.clone())
            })
        {
            if replace_scalar_subquery(document, block, query, &replacement) {
                detach_query(document, query);
                changed = true;
            }
            continue;
        }
        let conjuncts = filter_conjuncts(document, block);
        let in_conjunct = conjuncts.iter().position(|conjunct| {
            matches!(
                candidate(conjunct),
                Some(Candidate::In { query: found, .. }) if found == query
            )
        });
        if let Some(position) = in_conjunct {
            let candidate = candidate(&conjuncts[position]).expect("found above");
            changed |=
                try_semi_or_anti_join(document, block, &conjuncts, position, &candidate, dropped);
            continue;
        }
        let written = document.query(query).cloned();
        if let Some(rewrite) = try_aggregate_rewrite(document, block, query) {
            changed = true;
            if let (AggregateRewrite::GroupedTable(replacement), Some(written)) = (rewrite, written)
            {
                grouped.push((clause, written, replacement));
            }
        }
    }
    Ok(changed)
}

/// The clause of a block a subquery is first used in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Clause {
    Where,
    GroupBy,
    Outputs,
    OrderBy,
}

fn subquery_clause(document: &HirDocument, block_id: QueryBlockId, query: QueryId) -> Clause {
    let Some(block) = document.query_block(block_id) else {
        return Clause::Where;
    };
    if let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &block.body
    {
        if filter
            .as_ref()
            .is_some_and(|filter| runs_query(filter, query))
        {
            return Clause::Where;
        }
        if grouping.as_ref().is_some_and(|grouping| {
            grouping.keys.iter().any(|key| runs_query(key, query))
                || grouping
                    .having
                    .as_ref()
                    .is_some_and(|having| runs_query(having, query))
        }) {
            return Clause::GroupBy;
        }
    }
    if block
        .outputs
        .iter()
        .any(|output| runs_query(&output.expr, query))
    {
        return Clause::Outputs;
    }
    Clause::OrderBy
}

/// Whether `query` is written exactly like `written`, a query of the same
/// document before any rewrite: the same tables read the same way, and the
/// same expressions once each source of `query` is named like the matching
/// source of `written`.
fn queries_match(document: &HirDocument, written: &hir::Query, query: QueryId) -> bool {
    let Some(query) = document.query(query) else {
        return false;
    };
    let ([written_block], [block]) = (written.blocks.as_slice(), query.blocks.as_slice()) else {
        return false;
    };
    let (Some(written_from), Some(from)) = (&written_block.from, &block.from) else {
        return false;
    };
    if !written_from.joins.is_empty() || !from.joins.is_empty() {
        return false;
    }
    let (Some(written_source), Some(source)) = (
        document.source(written_from.first),
        document.source(from.first),
    ) else {
        return false;
    };
    if !matches!(source.kind, hir::SourceKind::Table(_))
        || format!("{:?}", written_source.kind) != format!("{:?}", source.kind)
        || written_source.name != source.name
        || written_source.alias != source.alias
        || format!("{:?}", written_source.index_hint) != format!("{:?}", source.index_hint)
    {
        return false;
    }
    let (from_source, to_source) = (source.id, written_source.id);
    let (from_block, to_block) = (block.id, written_block.id);
    // The debug form of an expression names every part of it.
    let rename = |expression: &hir::Expr| {
        let mut expression = expression.clone();
        replace_expression(&mut expression, &|expression| match expression {
            hir::Expr::Column(column) if column.source == from_source => {
                Some(hir::Expr::Column(hir::ColumnRef {
                    source: to_source,
                    column: column.column,
                }))
            }
            hir::Expr::RowId(source) if *source == from_source => Some(hir::Expr::RowId(to_source)),
            _ => None,
        });
        replace_expression(&mut expression, &|expression| {
            let hir::Expr::Function(call) = expression else {
                return None;
            };
            let hir::FunctionEvaluation::Aggregate { id, filter } = &call.evaluation else {
                return None;
            };
            if id.block != from_block {
                return None;
            }
            let mut call = call.clone();
            call.evaluation = hir::FunctionEvaluation::Aggregate {
                id: hir::AggregateId::new(to_block, id.index),
                filter: filter.clone(),
            };
            Some(hir::Expr::Function(call))
        });
        format!("{expression:?}")
    };
    let debug = |expression: &hir::Expr| format!("{expression:?}");
    let body = |block: &hir::QueryBlock, show: &dyn Fn(&hir::Expr) -> String| match &block.body {
        hir::QueryBlockBody::Select {
            distinctness,
            filter,
            grouping: None,
        } => Some((format!("{distinctness:?}"), filter.as_ref().map(show))),
        _ => None,
    };
    written.compounds.is_empty()
        && query.compounds.is_empty()
        && written.order_by.is_empty()
        && query.order_by.is_empty()
        && written.limit.is_none()
        && query.limit.is_none()
        && written_block.aggregate_count == block.aggregate_count
        && written_block.window_function_count == 0
        && block.window_function_count == 0
        && written_block.outputs.len() == block.outputs.len()
        && written_block
            .outputs
            .iter()
            .zip(&block.outputs)
            .all(|(written, output)| {
                written.name == output.name && debug(&written.expr) == rename(&output.expr)
            })
        && body(written_block, &debug).is_some()
        && body(written_block, &debug) == body(block, &rename)
}

/// Replace every use of the value of `query` in `block` with `replacement`.
fn replace_scalar_subquery(
    document: &mut HirDocument,
    block: QueryBlockId,
    query: QueryId,
    replacement: &hir::Expr,
) -> bool {
    replace_in_block(document, block, &|expression| {
        matches!(
            expression,
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query: found, output: 0 })
                if *found == query
        )
        .then(|| replacement.clone())
    })
}

/// Where an aggregate subquery's value is after a rewrite.
enum AggregateRewrite {
    /// A column of a grouped table, which a copy of the same subquery can
    /// read too.
    GroupedTable(hir::Expr),
    /// The aggregate moved into the outer block's HAVING clause.
    Joined,
}

/// The subqueries in the expressions of `block`, in the order they are
/// planned: WHERE, GROUP BY and HAVING, the result columns, then the query's
/// ORDER BY and LIMIT when the block is the whole query.
fn block_subqueries(document: &HirDocument, block_id: QueryBlockId) -> Vec<QueryId> {
    let mut subqueries = Vec::new();
    for_each_block_expression(document, block_id, &mut |expression| {
        expression.for_each(&mut |expression| {
            if let hir::Expr::Subquery(subquery) = expression {
                let query = subquery_query(subquery);
                if !subqueries.contains(&query) {
                    subqueries.push(query);
                }
            }
        });
    });
    subqueries
}

/// Visit the expressions of `block` whose subqueries a rewrite may replace,
/// in clause order. Join constraints are not visited.
fn for_each_block_expression(
    document: &HirDocument,
    block_id: QueryBlockId,
    visit: &mut impl FnMut(&hir::Expr),
) {
    let (Some(query), Some(block)) = (
        document.query(block_id.query),
        document.query_block(block_id),
    ) else {
        return;
    };
    if let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &block.body
    {
        if let Some(filter) = filter {
            visit(filter);
        }
        if let Some(grouping) = grouping {
            for key in &grouping.keys {
                visit(key);
            }
            if let Some(having) = &grouping.having {
                visit(having);
            }
        }
    }
    for output in &block.outputs {
        visit(&output.expr);
    }
    if query.blocks.len() == 1 {
        for term in &query.order_by {
            visit(&term.expr);
        }
        if let Some(limit) = &query.limit {
            visit(&limit.limit);
            if let Some(offset) = &limit.offset {
                visit(offset);
            }
        }
    }
}

/// Replace, in place, every expression of `block` that `replace` gives a
/// replacement for. Return whether anything was replaced.
fn replace_in_block(
    document: &mut HirDocument,
    block_id: QueryBlockId,
    replace: &impl Fn(&hir::Expr) -> Option<hir::Expr>,
) -> bool {
    let mut replaced = false;
    let query = &mut document.queries[block_id.query.index()];
    let single_block = query.blocks.len() == 1;
    let block = &mut query.blocks[block_id.index];
    if let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &mut block.body
    {
        if let Some(filter) = filter {
            replaced |= replace_expression(filter, replace);
        }
        if let Some(grouping) = grouping {
            for key in &mut grouping.keys {
                replaced |= replace_expression(key, replace);
            }
            if let Some(having) = &mut grouping.having {
                replaced |= replace_expression(having, replace);
            }
        }
    }
    for output in &mut block.outputs {
        replaced |= replace_expression(&mut output.expr, replace);
    }
    if single_block {
        for term in &mut query.order_by {
            replaced |= replace_expression(&mut term.expr, replace);
        }
        if let Some(limit) = &mut query.limit {
            replaced |= replace_expression(&mut limit.limit, replace);
            if let Some(offset) = &mut limit.offset {
                replaced |= replace_expression(offset, replace);
            }
        }
    }
    replaced
}

/// Replace `expression`, or each of its parts, that `replace` gives a
/// replacement for. Parts of a replacement are not visited again.
fn replace_expression(
    expression: &mut hir::Expr,
    replace: &impl Fn(&hir::Expr) -> Option<hir::Expr>,
) -> bool {
    if let Some(replacement) = replace(expression) {
        *expression = replacement;
        return true;
    }
    let mut replaced = false;
    let mut visit = |child: &mut hir::Expr| replaced |= replace_expression(child, replace);
    match expression {
        hir::Expr::Literal(_)
        | hir::Expr::Parameter(_)
        | hir::Expr::Column(_)
        | hir::Expr::RowId(_)
        | hir::Expr::Output(_)
        | hir::Expr::Subquery(
            hir::SubqueryExpr::Scalar { .. }
            | hir::SubqueryExpr::Row { .. }
            | hir::SubqueryExpr::Exists(_),
        ) => {}
        hir::Expr::MergedColumn(column) => {
            visit(&mut column.left);
            visit(&mut column.right);
        }
        hir::Expr::Unary { expr, .. }
        | hir::Expr::IsNull(expr)
        | hir::Expr::NotNull(expr)
        | hir::Expr::TruthTest { expr, .. }
        | hir::Expr::Collate { expr, .. }
        | hir::Expr::Cast { expr, .. } => visit(expr),
        hir::Expr::Binary { lhs, rhs, .. } => {
            visit(lhs);
            visit(rhs);
        }
        hir::Expr::Between {
            expr, start, end, ..
        } => {
            visit(expr);
            visit(start);
            visit(end);
        }
        hir::Expr::Case {
            base,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(base) = base {
                visit(base);
            }
            for (when, then) in when_then {
                visit(when);
                visit(then);
            }
            if let Some(else_expr) = else_expr {
                visit(else_expr);
            }
        }
        hir::Expr::Function(call) => {
            match &mut call.arguments {
                hir::FunctionArguments::Star => {}
                hir::FunctionArguments::Expressions {
                    values, order_by, ..
                } => {
                    for value in values {
                        visit(value);
                    }
                    for term in order_by {
                        visit(&mut term.expr);
                    }
                }
                hir::FunctionArguments::OrderedSet { direct, order_by } => {
                    for value in direct {
                        visit(value);
                    }
                    visit(&mut order_by.expr);
                }
            }
            match &mut call.evaluation {
                hir::FunctionEvaluation::Scalar => {}
                hir::FunctionEvaluation::Aggregate { filter, .. }
                | hir::FunctionEvaluation::Window { filter, .. } => {
                    if let Some(filter) = filter {
                        visit(filter);
                    }
                }
            }
        }
        hir::Expr::InList { lhs, values, .. } => {
            visit(lhs);
            for value in values {
                visit(value);
            }
        }
        hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => visit(lhs),
        hir::Expr::Like {
            lhs, rhs, escape, ..
        } => {
            visit(lhs);
            visit(rhs);
            if let Some(escape) = escape {
                visit(escape);
            }
        }
        hir::Expr::Row(expressions) | hir::Expr::Array(expressions) => {
            for expression in expressions {
                visit(expression);
            }
        }
        hir::Expr::Subscript { base, index } => {
            visit(base);
            visit(index);
        }
        hir::Expr::FieldAccess(access) => visit(&mut access.base),
        hir::Expr::Raise { message, .. } => {
            if let Some(message) = message {
                visit(message);
            }
        }
    }
    replaced
}

/// Every source of an expression's own columns and rowids, without looking
/// into subqueries.
fn expression_sources(expression: &hir::Expr) -> Vec<SourceId> {
    let mut sources = Vec::new();
    expression.for_each(&mut |expression| {
        let source = match expression {
            hir::Expr::Column(column) => column.source,
            hir::Expr::RowId(source) => *source,
            _ => return,
        };
        if !sources.contains(&source) {
            sources.push(source);
        }
    });
    sources
}

/// Whether evaluating `expression`, or a subquery it runs, may call a
/// function whose result can change between calls, such as `random()`.
fn calls_nondeterministic_function(document: &HirDocument, expression: &hir::Expr) -> bool {
    let mut found = false;
    let mut pending_queries = Vec::new();
    let mut visit = |expression: &hir::Expr, pending_queries: &mut Vec<QueryId>| {
        expression.for_each(&mut |expression| match expression {
            hir::Expr::Function(call) => {
                let function = call.function.value();
                found |= !matches!(
                    function,
                    crate::function::Func::Agg(_) | crate::function::Func::Window(_)
                ) && !matches!(
                    function,
                    crate::function::Func::External(external)
                        if matches!(external.func, crate::function::ExtFunc::Aggregate { .. })
                ) && !function.is_deterministic();
            }
            hir::Expr::Subquery(subquery) => pending_queries.push(subquery_query(subquery)),
            _ => {}
        });
    };
    visit(expression, &mut pending_queries);
    let mut seen = HashSet::default();
    while let Some(query) = pending_queries.pop() {
        if !seen.insert(query) {
            continue;
        }
        let mut nested = Vec::new();
        document.for_each_query_expression(query, &mut |expression| {
            visit(expression, &mut nested);
        });
        pending_queries.extend(nested);
        pending_queries.extend(document.direct_query_dependencies(query));
    }
    found
}

fn subquery_query(subquery: &hir::SubqueryExpr) -> QueryId {
    match subquery {
        hir::SubqueryExpr::Scalar { query, .. }
        | hir::SubqueryExpr::Row { query }
        | hir::SubqueryExpr::In { query, .. }
        | hir::SubqueryExpr::Exists(query) => *query,
    }
}

fn and(lhs: hir::Expr, rhs: hir::Expr) -> hir::Expr {
    hir::Expr::Binary {
        lhs: Box::new(lhs),
        operator: Operator::And,
        rhs: Box::new(rhs),
        array_concat: false,
        custom: None,
        comparison: None,
    }
}

/// Move one subquery of `block` into it as a semi-join or anti-join, when
/// every check below passes. Return whether it moved.
fn try_semi_or_anti_join(
    document: &mut HirDocument,
    block_id: QueryBlockId,
    conjuncts: &[hir::Expr],
    position: usize,
    candidate: &Candidate<'_>,
    dropped: &mut Vec<hir::Parameter>,
) -> bool {
    let subquery_id = candidate_query(candidate);
    let Some(block) = document.query_block(block_id) else {
        return false;
    };
    let Some(from) = &block.from else {
        // A semi-join needs a left table.
        return false;
    };
    let Some(subquery) = document.query(subquery_id) else {
        return false;
    };
    // The current semi-join and anti-join loops stop after a match in one
    // table, so the subquery reads one source and does nothing but filter
    // its rows.
    let [inner_block] = subquery.blocks.as_slice() else {
        return false;
    };
    if !subquery.compounds.is_empty()
        || !subquery.order_by.is_empty()
        || subquery.limit.is_some()
        || inner_block.aggregate_count != 0
        || inner_block.window_function_count != 0
    {
        return false;
    }
    let hir::QueryBlockBody::Select {
        distinctness,
        filter: inner_filter,
        grouping: None,
    } = &inner_block.body
    else {
        return false;
    };
    if matches!(distinctness, Some(Distinctness::Distinct)) {
        return false;
    }
    let Some(inner_from) = &inner_block.from else {
        return false;
    };
    if !inner_from.joins.is_empty() {
        return false;
    }
    let inner_source = inner_from.first;
    let Some(inner_definition) = document.source(inner_source) else {
        return false;
    };
    // The argument checks of a table function stay with its source, so they
    // move with it and must pass the same checks as the WHERE terms.
    let mut source_terms: &[hir::Expr] = &[];
    match &inner_definition.kind {
        hir::SourceKind::Table(_) => {}
        hir::SourceKind::TableFunction {
            argument_predicates,
            ..
        } => source_terms = argument_predicates,
        // A FROM subquery that reads the outer row still needs that row,
        // which needs a separate rewrite.
        hir::SourceKind::Derived(query) if !document.reads_outer_sources(*query) => {}
        // The CTE must be visible from the outer query too.
        hir::SourceKind::Cte(cte) => {
            let Some(outer_query) = document.query(block_id.query) else {
                return false;
            };
            if !outer_query.reachable_ctes.contains(cte) {
                return false;
            }
        }
        _ => return false,
    }

    // Every source the subquery reads from outside is a table of this
    // block's own FROM clause, and none of them is NULL-filled by a LEFT
    // JOIN (its right side) or a RIGHT JOIN (every source on its left side):
    // a LEFT JOIN whose NULL row a WHERE term rejects becomes an inner join,
    // and the link of a NOT EXISTS must not count as such a term. FULL JOIN
    // sides are allowed.
    let outer_sources = document.outer_sources(subquery_id);
    if outer_sources.is_empty()
        || outer_sources.iter().any(|source| {
            let Some(position) = from.source_position(*source) else {
                return true;
            };
            from.joins
                .iter()
                .enumerate()
                .any(|(index, join)| match join.kind {
                    hir::JoinKind::Left => join.right == *source,
                    hir::JoinKind::Right => position <= index,
                    _ => false,
                })
        })
    {
        return false;
    }

    // A subquery in the subquery's own clauses must not read any row from
    // outside itself. One in the WHERE clause moves with its term
    // and stays a subquery of the outer block; one in the SELECT list of an
    // EXISTS is never run, and is dropped with that list.
    let nested = document.expression_subqueries(subquery_id);
    if nested
        .iter()
        .any(|query| document.reads_outer_sources(*query))
    {
        return false;
    }
    let mut moved_nested = Vec::new();
    if let Some(filter) = inner_filter {
        filter.for_each(&mut |expression| {
            if let hir::Expr::Subquery(subquery) = expression {
                let query = subquery_query(subquery);
                if !moved_nested.contains(&query) {
                    moved_nested.push(query);
                }
            }
        });
    }
    let dropped_nested = nested
        .iter()
        .copied()
        .filter(|query| !moved_nested.contains(query))
        .collect::<Vec<_>>();

    let mut terms = inner_filter
        .as_ref()
        .map(|filter| filter.conjuncts().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    match candidate {
        Candidate::Exists { .. } => {
            // EXISTS ignores its SELECT list, which disappears with the
            // subquery, together with the subqueries it holds.
            if !dropped_nested
                .iter()
                .all(|query| can_drop_query_tree(document, *query))
            {
                return false;
            }
            // A join can skip an inner row the subquery tests, and so hide
            // an error that row's WHERE terms would raise.
            if terms.iter().any(|term| hir_expr_can_fail(document, term)) {
                return false;
            }
        }
        Candidate::In {
            lhs, comparison, ..
        } => {
            let [output] = inner_block.outputs.as_slice() else {
                return false;
            };
            // A semi-join changes which expressions run and how many
            // times: IN runs the inner query to the end, while a
            // semi-join stops at its first match, and the left side runs once
            // for every inner row instead of once. So nothing involved may
            // fail.
            if hir_expr_can_fail(document, lhs)
                || hir_expr_can_fail(document, &output.expr)
                || terms.iter().any(|term| hir_expr_can_fail(document, term))
            {
                return false;
            }
            terms.push(hir::Expr::Binary {
                lhs: Box::new((*lhs).clone()),
                operator: Operator::Equals,
                rhs: Box::new(output.expr.clone()),
                array_concat: false,
                custom: None,
                comparison: Some((*comparison).clone()),
            });
        }
    }
    let negated = matches!(candidate, Candidate::Exists { negated: true, .. });

    for term in terms.iter().chain(source_terms) {
        // A function such as random() could run a different number of times.
        if calls_nondeterministic_function(document, term) {
            return false;
        }
        let sources = expression_sources(term);
        let reads_outer = sources.iter().any(|source| *source != inner_source);
        // A term that reads the outer row must be a comparison between
        // the inner source and outer sources.
        if reads_outer && !is_inner_outer_comparison(term, inner_source) {
            return false;
        }
        // Every NOT EXISTS term must read the inner source. Moving a
        // constant false term into the outer query would reject a row that
        // NOT EXISTS keeps.
        if negated && !sources.contains(&inner_source) {
            return false;
        }
    }

    // Every check passed. Move the source and its terms. The parameters of
    // the dropped SELECT list are kept so callers can still bind them.
    let outer_query = block_id.query;
    for query in moved_nested {
        document.queries[query.index()].parent = Some(outer_query);
    }
    if matches!(candidate, Candidate::Exists { .. }) {
        for output in &document.queries[subquery_id.index()].blocks[0].outputs {
            collect_parameters(&output.expr, dropped);
        }
        for query in dropped_nested {
            drop_query_tree(document, query, dropped);
        }
    }
    if let hir::SourceKind::Derived(query) = document.sources[inner_source.index()].kind {
        document.queries[query.index()].parent = Some(outer_query);
    }
    document.sources[inner_source.index()].owner = SourceOwner::QueryBlock(block_id);

    let mut remaining = conjuncts.to_vec();
    remaining.remove(position);
    remaining.extend(terms);
    let block = &mut document.queries[block_id.query.index()].blocks[block_id.index];
    let from = block.from.as_mut().expect("checked above");
    from.joins.push(hir::Join {
        right: inner_source,
        kind: if negated {
            hir::JoinKind::Anti
        } else {
            hir::JoinKind::Semi
        },
        constraint: hir::JoinConstraint::None,
    });
    let hir::QueryBlockBody::Select { filter, .. } = &mut block.body else {
        unreachable!("a block with a WHERE clause is a SELECT");
    };
    *filter = remaining.into_iter().reduce(and);

    detach_query(document, subquery_id);
    true
}

/// Add the LEFT JOIN a rewritten subquery becomes. A semi-join or anti-join
/// from a WHERE EXISTS or IN rewritten earlier must stay the last join: it
/// only filters the rows before it, and the loops are built in this order.
fn push_join_before_semi_anti(from: &mut hir::From, join: hir::Join) {
    let first_semi_anti = from
        .joins
        .iter()
        .position(|join| matches!(join.kind, hir::JoinKind::Semi | hir::JoinKind::Anti))
        .unwrap_or(from.joins.len());
    from.joins.insert(first_semi_anti, join);
}

/// Whether one side of a comparison reads only the inner source and the
/// other only outer sources.
fn is_inner_outer_comparison(expression: &hir::Expr, inner_source: SourceId) -> bool {
    let hir::Expr::Binary {
        lhs,
        operator,
        rhs,
        custom: None,
        ..
    } = expression
    else {
        return false;
    };
    if !operator.is_comparison() {
        return false;
    }
    let side = |expression: &hir::Expr| {
        let sources = expression_sources(expression);
        if sources.is_empty() {
            return None;
        }
        if sources.iter().all(|source| *source == inner_source) {
            Some(true)
        } else if sources.iter().all(|source| *source != inner_source) {
            Some(false)
        } else {
            None
        }
    };
    matches!(
        (side(lhs), side(rhs)),
        (Some(true), Some(false)) | (Some(false), Some(true))
    )
}

fn collect_parameters(expression: &hir::Expr, parameters: &mut Vec<hir::Parameter>) {
    expression.for_each(&mut |expression| {
        if let hir::Expr::Parameter(parameter) = expression {
            parameters.push(parameter.clone());
        }
    });
}

/// `query` and every query it runs through its expressions and FROM
/// subqueries.
fn query_tree(document: &HirDocument, query: QueryId) -> Vec<QueryId> {
    let mut tree = vec![query];
    let mut position = 0;
    while let Some(query) = tree.get(position).copied() {
        position += 1;
        for dependency in document.direct_query_dependencies(query) {
            if !tree.contains(&dependency) {
                tree.push(dependency);
            }
        }
    }
    tree
}

/// Whether a query that no expression will run can be dropped with every
/// query it runs: none of them reads a CTE, whose body other queries may
/// share.
fn can_drop_query_tree(document: &HirDocument, query: QueryId) -> bool {
    query_tree(document, query).into_iter().all(|query| {
        document.query(query).is_some_and(|definition| {
            definition.blocks.iter().all(|block| {
                block.from.as_ref().is_none_or(|from| {
                    from_sources(document, from).into_iter().all(|source| {
                        !matches!(
                            document.source(source).map(|source| &source.kind),
                            Some(hir::SourceKind::Cte(_) | hir::SourceKind::RecursiveInput(_))
                        )
                    })
                })
            })
        })
    })
}

/// Detach `query` and every query it runs, keeping their parameters.
fn drop_query_tree(document: &mut HirDocument, query: QueryId, dropped: &mut Vec<hir::Parameter>) {
    for query in query_tree(document, query) {
        document.for_each_query_expression(query, &mut |expression| {
            collect_parameters(expression, dropped);
        });
        detach_query(document, query);
    }
}

/// Leave a query that no expression runs anymore empty, so that nothing
/// plans or reads its old sources and expressions.
fn detach_query(document: &mut HirDocument, query: QueryId) {
    let query = &mut document.queries[query.index()];
    query.parent = None;
    query.captures.clear();
    query.compounds.clear();
    query.order_by.clear();
    query.limit = None;
    query.output.clear();
    query.blocks.truncate(1);
    let block = &mut query.blocks[0];
    block.from = None;
    block.outputs.clear();
    block.aggregate_count = 0;
    block.window_function_count = 0;
    block.windows.clear();
    block.body = hir::QueryBlockBody::Select {
        distinctness: None,
        filter: None,
        grouping: None,
    };
}

/// The value an aggregate subquery returns when no input row matches.
#[derive(Clone, Copy)]
enum EmptyInputValue {
    Null,
    /// Integer zero, as `count` returns.
    IntegerZero,
    /// Real zero, as `total` returns.
    RealZero,
}

fn aggregate_function(expression: &hir::Expr) -> Option<&crate::function::AggFunc> {
    let hir::Expr::Function(call) = expression else {
        return None;
    };
    if !matches!(call.evaluation, hir::FunctionEvaluation::Aggregate { .. }) {
        return None;
    }
    match call.function.value() {
        crate::function::Func::Agg(function) => Some(function),
        _ => None,
    }
}

/// The result of a one-value aggregate subquery for no input rows, if it is
/// known.
fn result_on_empty_input(expression: &hir::Expr) -> Option<EmptyInputValue> {
    use crate::function::AggFunc;
    if let Some(function) = aggregate_function(expression) {
        return match function {
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
    is_null_on_empty_input(expression).then_some(EmptyInputValue::Null)
}

/// Whether an expression must be NULL when its aggregates read no row.
fn is_null_on_empty_input(expression: &hir::Expr) -> bool {
    use crate::function::AggFunc;
    if aggregate_function(expression).is_some_and(|function| {
        matches!(
            function,
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
    match expression {
        hir::Expr::Binary {
            lhs,
            operator:
                Operator::Add
                | Operator::Subtract
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulus
                | Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::LeftShift
                | Operator::RightShift
                | Operator::Concat,
            rhs,
            custom: None,
            array_concat: false,
            ..
        } => is_null_on_empty_input(lhs) || is_null_on_empty_input(rhs),
        hir::Expr::Unary { expr, .. }
        | hir::Expr::Cast { expr, .. }
        | hir::Expr::Collate { expr, .. } => is_null_on_empty_input(expr),
        _ => false,
    }
}

/// The inner and outer columns of one `=` link of an aggregate subquery.
struct ColumnPair {
    inner: hir::ColumnRef,
    outer: hir::ColumnRef,
    /// The `=` term, which keeps its comparison rules.
    term: hir::Expr,
}

/// An `=` between one column of the subquery's own sources and one column
/// of `outer`.
fn read_column_pair(
    term: &hir::Expr,
    inner_sources: &[SourceId],
    outer: SourceId,
) -> Option<ColumnPair> {
    let hir::Expr::Binary {
        lhs,
        operator: Operator::Equals,
        rhs,
        custom: None,
        ..
    } = term
    else {
        return None;
    };
    let (hir::Expr::Column(left), hir::Expr::Column(right)) = (lhs.as_ref(), rhs.as_ref()) else {
        return None;
    };
    let (inner, outer_column) = if inner_sources.contains(&left.source) && right.source == outer {
        (*left, *right)
    } else if left.source == outer && inner_sources.contains(&right.source) {
        (*right, *left)
    } else {
        return None;
    };
    Some(ColumnPair {
        inner,
        outer: outer_column,
        term: term.clone(),
    })
}

fn source_column(
    document: &HirDocument,
    column: hir::ColumnRef,
) -> Option<hir::SourceColumnRef<'_>> {
    document.source(column.source)?.columns.get(column.column)
}

/// Grouping by the inner column and joining on the `=` must use the same
/// number/text conversion and the same text order, or an outer row could
/// match two groups.
fn column_pair_compares_the_same(document: &HirDocument, pair: &ColumnPair) -> bool {
    use crate::translate::collate::CollationSeq;
    let (Some(inner), Some(outer)) = (
        source_column(document, pair.inner),
        source_column(document, pair.outer),
    ) else {
        return false;
    };
    let collation = |column: hir::SourceColumnRef<'_>| match column
        .collation()
        .map(|collation| *collation.value())
    {
        None | Some(CollationSeq::Unset) => CollationSeq::Binary,
        Some(collation) => collation,
    };
    let affinity = |column: hir::SourceColumnRef<'_>| {
        if column.has_affinity() {
            column.affinity()
        } else {
            crate::vdbe::affinity::Affinity::Blob
        }
    };
    affinity(inner) == affinity(outer) && collation(inner) == collation(outer)
}

/// Whether computing the aggregate for keys no outer row asks for cannot
/// fail.
fn aggregate_can_run_for_unused_rows(
    document: &HirDocument,
    block: &hir::QueryBlock,
    inner_where: &[hir::Expr],
) -> bool {
    use crate::function::AggFunc;
    let mut safe = true;
    for output in &block.outputs {
        output.expr.for_each(&mut |expression| {
            let Some(function) = aggregate_function(expression) else {
                return;
            };
            safe &= matches!(
                function,
                AggFunc::Avg
                    | AggFunc::Count
                    | AggFunc::Count0
                    | AggFunc::Max
                    | AggFunc::Min
                    | AggFunc::Total
            );
            let hir::Expr::Function(call) = expression else {
                unreachable!("an aggregate is a function call");
            };
            safe &= !call
                .arguments
                .expressions()
                .iter()
                .chain(call.evaluation.filter())
                .any(|argument| hir_expr_can_fail(document, argument));
        });
    }
    safe && !inner_where
        .iter()
        .any(|term| hir_expr_can_fail(document, term))
}

/// Every source of a FROM clause, including those in parenthesized groups.
fn from_sources(document: &HirDocument, from: &hir::From) -> Vec<SourceId> {
    let mut sources = Vec::new();
    let mut pending = vec![from];
    while let Some(from) = pending.pop() {
        for source in core::iter::once(from.first).chain(from.joins.iter().map(|join| join.right)) {
            sources.push(source);
            if let Some(hir::SourceKind::FromGroup(group)) =
                document.source(source).map(|source| &source.kind)
            {
                pending.push(&group.from);
            }
        }
    }
    sources
}

/// Whether a join constraint of `from` reads one of `sources`, or runs
/// `query`.
fn join_constraints_read(
    document: &HirDocument,
    from: &hir::From,
    sources: &[SourceId],
    query: Option<QueryId>,
) -> bool {
    let mut reads = false;
    let mut pending = vec![from];
    while let Some(from) = pending.pop() {
        for join in &from.joins {
            if let Some(hir::SourceKind::FromGroup(group)) =
                document.source(join.right).map(|source| &source.kind)
            {
                pending.push(&group.from);
            }
            let mut visit = |expression: &hir::Expr| {
                expression.for_each(&mut |expression| match expression {
                    hir::Expr::Column(column) => reads |= sources.contains(&column.source),
                    hir::Expr::RowId(source) => reads |= sources.contains(source),
                    hir::Expr::Subquery(subquery) => {
                        reads |= Some(subquery_query(subquery)) == query
                    }
                    _ => {}
                });
            };
            match &join.constraint {
                hir::JoinConstraint::None => {}
                hir::JoinConstraint::On(expression) => visit(expression),
                hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                    for column in columns {
                        visit(&column.left);
                        visit(&hir::Expr::Column(column.right));
                    }
                }
            }
        }
    }
    reads
}

/// Turn one correlated aggregate subquery of `block` into a grouped FROM
/// subquery, the group-first form:
///
/// ```sql
/// SELECT * FROM o WHERE o.v < (SELECT avg(i.v) FROM i WHERE i.k = o.k)
/// -- becomes
/// SELECT * FROM o
/// LEFT JOIN (SELECT avg(v), k FROM i GROUP BY k) g ON g.k = o.k
/// WHERE o.v < g.avg
/// ```
///
/// Return whether it changed.
fn try_aggregate_rewrite(
    document: &mut HirDocument,
    block_id: QueryBlockId,
    query_id: QueryId,
) -> Option<AggregateRewrite> {
    let block = document.query_block(block_id)?;
    // A window keeps another copy of its expressions.
    if block.window_function_count != 0 || !block.windows.is_empty() {
        return None;
    }
    let Some(from) = &block.from else {
        return None;
    };
    // A block with a FULL join keeps its aggregate subqueries.
    if from
        .joins
        .iter()
        .any(|join| join.kind == hir::JoinKind::Full)
    {
        return None;
    }
    // A value an outer join's ON clause reads must be ready before that join
    // decides whether to fill its right side with NULL values.
    if join_constraints_read(document, from, &[], Some(query_id)) {
        return None;
    }
    // Only a one-value use reads the grouped table: IN and EXISTS stay.
    let mut used_only_as_value = true;
    for_each_block_expression(document, block_id, &mut |expression| {
        expression.for_each(&mut |expression| {
            if let hir::Expr::Subquery(subquery) = expression {
                used_only_as_value &= subquery_query(subquery) != query_id
                    || matches!(subquery, hir::SubqueryExpr::Scalar { output: 0, .. });
            }
        });
    });
    if !used_only_as_value {
        return None;
    }
    let query = document.query(query_id)?;
    // The subquery must be one plain block with one aggregate output.
    let [inner_block] = query.blocks.as_slice() else {
        return None;
    };
    let [output] = inner_block.outputs.as_slice() else {
        return None;
    };
    // Planning a scalar subquery adds LIMIT 1; LIMIT 0 removes its row and
    // OFFSET 1 skips it.
    let limit_is_one = query.limit.as_ref().is_none_or(|limit| {
        limit.offset.is_none()
            && matches!(
                &limit.limit,
                hir::Expr::Literal(turso_parser::ast::Literal::Numeric(value))
                    if value.parse::<i64>() == Ok(1)
            )
    });
    if !query.compounds.is_empty()
        || !query.order_by.is_empty()
        || !limit_is_one
        || inner_block.aggregate_count == 0
        || inner_block.window_function_count != 0
        || !document.expression_subqueries(query_id).is_empty()
    {
        return None;
    }
    let hir::QueryBlockBody::Select {
        distinctness,
        filter: inner_filter,
        grouping: None,
    } = &inner_block.body
    else {
        return None;
    };
    if matches!(distinctness, Some(Distinctness::Distinct)) {
        return None;
    }
    let Some(inner_from) = &inner_block.from else {
        return None;
    };
    let inner_sources = from_sources(document, inner_from);
    for source in &inner_sources {
        let definition = document.source(*source)?;
        let movable = match &definition.kind {
            hir::SourceKind::Table(table) => {
                !matches!(table.value(), crate::schema::Table::Virtual(_))
            }
            hir::SourceKind::Derived(query) => !document.reads_outer_sources(*query),
            hir::SourceKind::Cte(_) | hir::SourceKind::FromGroup(_) => true,
            _ => false,
        };
        if !movable {
            return None;
        }
    }
    if calls_nondeterministic_function(document, &output.expr)
        || inner_filter
            .as_ref()
            .is_some_and(|filter| calls_nondeterministic_function(document, filter))
    {
        return None;
    }
    let empty_value = result_on_empty_input(&output.expr)?;
    // A grouped table linked to more than one outer table can move before
    // one of those tables after its left join becomes an inner join.
    let outer_sources = document.outer_sources(query_id);
    let [outer] = outer_sources.as_slice() else {
        return None;
    };
    let outer = *outer;
    from.source_position(outer)?;
    // An ON clause of the subquery that reads the outer row is not moved.
    if join_constraints_read(document, inner_from, &[outer], None) {
        return None;
    }
    let mut pairs = Vec::new();
    let mut inner_where = Vec::new();
    for term in inner_filter
        .as_ref()
        .map(|filter| filter.conjuncts().cloned().collect::<Vec<_>>())
        .unwrap_or_default()
    {
        if !expression_sources(&term).contains(&outer) {
            inner_where.push(term);
            continue;
        }
        let pair = read_column_pair(&term, &inner_sources, outer)?;
        if !column_pair_compares_the_same(document, &pair) {
            return None;
        }
        pairs.push(pair);
    }
    // The outer row may only be read by the links.
    if pairs.is_empty() || expression_sources(&output.expr).contains(&outer) {
        return None;
    }
    if !aggregate_can_run_for_unused_rows(document, inner_block, &inner_where) {
        return try_join_first(document, block_id, query_id).then_some(AggregateRewrite::Joined);
    }

    // Every check passed. Group the subquery by its link columns.
    let mut keys: Vec<hir::ColumnRef> = Vec::new();
    let positions = pairs
        .iter()
        .map(|pair| {
            1 + keys
                .iter()
                .position(|key| *key == pair.inner)
                .unwrap_or_else(|| {
                    keys.push(pair.inner);
                    keys.len() - 1
                })
        })
        .collect::<Vec<_>>();
    let key_columns = keys
        .iter()
        .map(|key| {
            source_column(document, *key)
                .map(hir::SourceColumnRef::to_owned)
                .expect("a link column exists")
        })
        .collect::<Vec<_>>();
    let result_name = output.name.clone();
    let result_column = hir::SourceColumn {
        name: result_name.into(),
        type_fact: output.type_fact.clone(),
        affinity: output.schema_affinity,
        has_affinity: output.has_affinity,
        // A scalar subquery result has no text order.
        collation: None,
        hidden: false,
        rowid_alias: false,
    };
    let inner_block_id = inner_block.id;

    let query = &mut document.queries[query_id.index()];
    query.captures.clear();
    query.limit = None;
    let inner_block = &mut query.blocks[0];
    for (index, (key, column)) in keys.iter().zip(&key_columns).enumerate() {
        let id = hir::OutputId::query(inner_block_id, inner_block.outputs.len());
        inner_block.outputs.push(hir::Output {
            id,
            name: format!("correlation_key_{index}"),
            expr: hir::Expr::Column(*key),
            type_fact: column.type_fact.clone(),
            affinity: column.affinity,
            schema_affinity: column.affinity,
            has_affinity: column.has_affinity,
            collation: column.collation.clone(),
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::ExplicitAlias,
        });
        query.output.push(id);
    }
    let inner_block = &mut query.blocks[0];
    let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &mut inner_block.body
    else {
        unreachable!("checked above");
    };
    *filter = inner_where.into_iter().reduce(and);
    *grouping = Some(hir::Grouping {
        keys: keys.iter().map(|key| hir::Expr::Column(*key)).collect(),
        key_type_facts: key_columns
            .iter()
            .map(|column| column.type_fact.clone())
            .collect(),
        key_collations: key_columns
            .iter()
            .map(|column| column.collation.clone())
            .collect(),
        having: None,
    });

    let grouped = SourceId::new(document.sources.len());
    let mut columns = vec![result_column];
    columns.extend(
        key_columns
            .iter()
            .enumerate()
            .map(|(index, column)| hir::SourceColumn {
                name: format!("correlation_key_{index}").into(),
                type_fact: column.type_fact.clone(),
                affinity: column.affinity,
                has_affinity: column.has_affinity,
                collation: column.collation.clone(),
                hidden: false,
                rowid_alias: false,
            }),
    );
    let width = columns.len();
    document.sources.push(hir::Source {
        id: grouped,
        owner: SourceOwner::QueryBlock(block_id),
        database: None,
        name: format!("scalar_subquery_{}", query_id.index()),
        alias: None,
        kind: hir::SourceKind::Derived(query_id),
        columns: columns.into(),
        generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
        default_expressions: vec![hir::ColumnReadExpression::Absent; width],
        column_type_programs: vec![None; width],
        check_constraints: None,
        rowid_available: false,
        index_hint: hir::IndexHint::None,
        index_expressions: Vec::new(),
        index_coverage: hir::IndexCoverage::Selective,
        index_method_patterns: Vec::new(),
    });

    let link = pairs
        .into_iter()
        .zip(positions)
        .map(|(pair, position)| {
            let mut term = pair.term;
            let grouped_column = hir::ColumnRef {
                source: grouped,
                column: position,
            };
            replace_expression(&mut term, &|expression| {
                matches!(expression, hir::Expr::Column(column) if *column == pair.inner)
                    .then_some(hir::Expr::Column(grouped_column))
            });
            term
        })
        .reduce(and)
        .expect("a grouped table has a link");
    let block = &mut document.queries[block_id.query.index()].blocks[block_id.index];
    push_join_before_semi_anti(
        block.from.as_mut().expect("checked above"),
        hir::Join {
            right: grouped,
            kind: hir::JoinKind::Left,
            constraint: hir::JoinConstraint::On(link),
        },
    );

    let result = hir::Expr::Column(hir::ColumnRef {
        source: grouped,
        column: 0,
    });
    // Keep the zero that count and total return for no row.
    let replacement = match empty_value {
        EmptyInputValue::Null => result,
        EmptyInputValue::IntegerZero | EmptyInputValue::RealZero => hir::Expr::Case {
            base: None,
            when_then: vec![(
                hir::Expr::IsNull(Box::new(result.clone())),
                hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                    if matches!(empty_value, EmptyInputValue::IntegerZero) {
                        "0"
                    } else {
                        "0.0"
                    }
                    .to_string(),
                )),
            )],
            else_expr: Some(Box::new(result)),
            base_comparisons: Vec::new(),
        },
    };
    let replaced = replace_scalar_subquery(document, block_id, query_id, &replacement);
    assert!(replaced, "a block subquery is used by the block");
    Some(AggregateRewrite::GroupedTable(replacement))
}

/// Whether `expression` runs `query`.
fn runs_query(expression: &hir::Expr, query: QueryId) -> bool {
    let mut runs = false;
    expression.for_each(&mut |expression| {
        if let hir::Expr::Subquery(subquery) = expression {
            runs |= subquery_query(subquery) == query;
        }
    });
    runs
}

fn runs_any_query(expression: &hir::Expr) -> bool {
    let mut runs = false;
    expression.for_each(&mut |expression| {
        runs |= matches!(expression, hir::Expr::Subquery(_));
    });
    runs
}

/// Whether `source` reads a B-tree table that has a rowid.
fn reads_rowid_table(document: &HirDocument, source: SourceId) -> bool {
    matches!(
        document.source(source).map(|source| &source.kind),
        Some(hir::SourceKind::Table(table))
            if table.value().btree().is_some_and(|table| table.has_rowid)
    )
}

/// Join the inner table of one correlated aggregate subquery to `block`
/// before computing the aggregate, the join-first form:
///
/// ```sql
/// SELECT o.id FROM o WHERE o.limit > (SELECT sum(i.x) FROM i WHERE i.key = o.key)
/// -- becomes
/// SELECT o.id FROM o LEFT JOIN i ON i.key = o.key
/// GROUP BY o.rowid
/// HAVING o.limit > sum(i.x) FILTER (WHERE i.rowid IS NOT NULL)
/// ```
///
/// The join only reads inner rows some outer row asks for, so it never
/// computes an aggregate that could fail for an unused key. The outer block
/// must read one rowid table and do nothing else that the new
/// grouping would change, the subquery reads one rowid table, and its value
/// is one whole side of one WHERE comparison. Return whether it changed.
fn try_join_first(document: &mut HirDocument, block_id: QueryBlockId, query_id: QueryId) -> bool {
    let (Some(query), Some(block)) = (
        document.query(block_id.query),
        document.query_block(block_id),
    ) else {
        return false;
    };
    let hir::QueryBlockBody::Select {
        distinctness,
        filter: Some(filter),
        grouping: None,
    } = &block.body
    else {
        return false;
    };
    if block.aggregate_count != 0
        || block.window_function_count != 0
        || matches!(distinctness, Some(Distinctness::Distinct))
        || (query.blocks.len() == 1 && (!query.order_by.is_empty() || query.limit.is_some()))
    {
        return false;
    }
    let Some(from) = &block.from else {
        return false;
    };
    if !from.joins.is_empty() || !reads_rowid_table(document, from.first) {
        return false;
    }
    let outer = from.first;
    let Some(subquery) = document.query(query_id) else {
        return false;
    };
    let [inner_block] = subquery.blocks.as_slice() else {
        return false;
    };
    let Some(inner_from) = &inner_block.from else {
        return false;
    };
    if !inner_from.joins.is_empty() || !reads_rowid_table(document, inner_from.first) {
        return false;
    }
    let inner = inner_from.first;
    let result = inner_block.outputs[0].expr.clone();

    // The subquery must be one whole side of one WHERE comparison, and every
    // other WHERE term must give the same value each time it runs, since it
    // runs once for every joined copy of an outer row.
    if block
        .outputs
        .iter()
        .any(|output| runs_query(&output.expr, query_id))
    {
        return false;
    }
    let conjuncts = filter.conjuncts().cloned().collect::<Vec<_>>();
    let mut found = None;
    for (position, term) in conjuncts.iter().enumerate() {
        if !runs_query(term, query_id) {
            let mut reads_correlated_subquery = false;
            term.for_each(&mut |expression| {
                if let hir::Expr::Subquery(subquery) = expression {
                    reads_correlated_subquery |=
                        document.reads_outer_sources(subquery_query(subquery));
                }
            });
            if reads_correlated_subquery || calls_nondeterministic_function(document, term) {
                return false;
            }
            continue;
        }
        if found.is_some() {
            return false;
        }
        let hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            array_concat,
            custom,
            comparison,
        } = term
        else {
            return false;
        };
        if !operator.is_comparison() {
            return false;
        }
        let is_subquery = |expression: &hir::Expr| {
            matches!(
                expression,
                hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output: 0 })
                    if *query == query_id
            )
        };
        let (lhs, rhs) = if is_subquery(lhs) && !runs_any_query(rhs) {
            (result.clone(), rhs.as_ref().clone())
        } else if is_subquery(rhs) && !runs_any_query(lhs) {
            (lhs.as_ref().clone(), result.clone())
        } else {
            return false;
        };
        found = Some((
            position,
            hir::Expr::Binary {
                lhs: Box::new(lhs),
                operator: *operator,
                rhs: Box::new(rhs),
                array_concat: *array_concat,
                custom: custom.clone(),
                comparison: comparison.clone(),
            },
        ));
    }
    let Some((position, mut having)) = found else {
        return false;
    };
    let hir::QueryBlockBody::Select {
        filter: inner_filter,
        ..
    } = &inner_block.body
    else {
        return false;
    };
    let inner_filter = inner_filter.clone();
    let inner_block_id = inner_block.id;
    let inner_aggregates = inner_block.aggregate_count;
    let outer_aggregates = block.aggregate_count;

    // Every check passed. The aggregates move into the outer block, and the
    // NULL row the LEFT JOIN makes when nothing matches stays out of them.
    let inner_row_exists = hir::Expr::NotNull(Box::new(hir::Expr::RowId(inner)));
    replace_expression(&mut having, &|expression| {
        let hir::Expr::Function(call) = expression else {
            return None;
        };
        let hir::FunctionEvaluation::Aggregate { id, filter } = &call.evaluation else {
            return None;
        };
        if id.block != inner_block_id {
            return None;
        }
        let mut call = call.clone();
        call.evaluation = hir::FunctionEvaluation::Aggregate {
            id: hir::AggregateId::new(block_id, outer_aggregates + id.index),
            filter: Some(Box::new(match filter {
                Some(filter) => and(inner_row_exists.clone(), filter.as_ref().clone()),
                None => inner_row_exists.clone(),
            })),
        };
        Some(hir::Expr::Function(call))
    });

    document.sources[inner.index()].owner = SourceOwner::QueryBlock(block_id);
    let mut remaining = conjuncts;
    remaining.remove(position);
    let block = &mut document.queries[block_id.query.index()].blocks[block_id.index];
    push_join_before_semi_anti(
        block.from.as_mut().expect("checked above"),
        hir::Join {
            right: inner,
            kind: hir::JoinKind::Left,
            constraint: match inner_filter {
                Some(filter) => hir::JoinConstraint::On(filter),
                None => hir::JoinConstraint::None,
            },
        },
    );
    block.aggregate_count += inner_aggregates;
    let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &mut block.body
    else {
        unreachable!("checked above");
    };
    *filter = remaining.into_iter().reduce(and);
    *grouping = Some(hir::Grouping {
        keys: vec![hir::Expr::RowId(outer)],
        key_type_facts: vec![hir::TypeFact::known(crate::schema::Type::Integer)],
        key_collations: vec![None],
        having: Some(having),
    });
    detach_query(document, query_id);
    true
}
