use crate::sync::Arc;
use crate::turso_assert;

use super::plan::NamedWindowBound;
use super::{
    expr::{walk_expr, walk_expr_mut},
    plan::{
        Aggregate, Distinctness, EvalAt, JoinOrderMember, JoinedTable, Operation,
        OuterQueryReference, Plan, QueryDestination, TableReferences, WhereTerm,
    },
    select::prepare_select_plan,
};
use crate::translate::plan::BitSet;
use crate::translate::{
    emitter::Resolver,
    expr::{
        expr_contains_nondeterministic_scalar_function, expr_vector_size, unwrap_parens,
        BindingBehavior, WalkControl,
    },
    plan::{NonFromClauseSubquery, SubqueryState},
};
use crate::{
    ast::Limit,
    function::Func,
    schema::Table,
    util::{exprs_are_equivalent, normalize_ident},
    Result,
};
use crate::{
    function::{AccumulatorFunc, AggFunc, ExtFunc},
    translate::expr::bind_and_rewrite_expr,
};
use crate::{
    translate::plan::{Window, WindowFunction},
    vdbe::builder::ProgramBuilder,
};
use turso_parser::ast::Literal::Null;
use turso_parser::ast::{self, Expr, JoinType, Over, Select, TableInternalId, With};

/// Self-reference facts about a CTE body, for the binder: whether the body
/// references its own name anywhere (making the CTE recursive, whether or not
/// the RECURSIVE keyword was written), and whether the first arm itself
/// contains such a reference (a circular reference — the initial query of a
/// recursive CTE must not see the table).
pub(crate) fn cte_self_reference_info(cte_name: &str, select: &Select) -> (bool, bool) {
    let counter = RecursiveRefCounter { cte_name };
    let references_itself = counter.count_select(select, &mut RecursiveRefScope::new()) > 0;
    if !references_itself {
        return (false, false);
    }
    let mut scope = RecursiveRefScope::new();
    counter.push_nested_ctes(select.with.as_ref(), &mut scope);
    let (_, first_arm_count) = counter.count_arm(&select.body.select, &mut scope);
    (true, first_arm_count > 0)
}

/// Collect all table names referenced in a SELECT's FROM clause.
/// Used to determine which earlier CTEs a CTE directly depends on.
pub(crate) fn collect_from_clause_table_refs(select: &Select, out: &mut Vec<String>) {
    collect_from_select_body(&select.body, out);
    collect_subquery_table_refs_in_select_exprs(select, out);
}

fn collect_from_select_body(body: &ast::SelectBody, out: &mut Vec<String>) {
    collect_from_one_select(&body.select, out);
    for compound in &body.compounds {
        collect_from_one_select(&compound.select, out);
    }
}

fn collect_from_one_select(one: &ast::OneSelect, out: &mut Vec<String>) {
    match one {
        ast::OneSelect::Select { from, .. } => {
            if let Some(from_clause) = from {
                collect_from_select_table(&from_clause.select, out);
                for join in &from_clause.joins {
                    collect_from_select_table(&join.table, out);
                }
            }
        }
        ast::OneSelect::Values(_) => {}
    }
}

/// Counts references to a recursive CTE's own name the way SQLite's name
/// resolution does:
/// - a nested `WITH` that redefines the name shadows it for that subtree, and
/// - references inside a nested CTE body only count when that CTE is itself
///   referenced (an unused nested CTE that mentions the recursive table does
///   not make the query recursive), weighted by how many references its body
///   contains.
struct RecursiveRefCounter<'a> {
    cte_name: &'a str,
}

/// Names visible at the current point, innermost last. Each entry carries the
/// number of recursive references that using the name implies: 0 for a name
/// that shadows the recursive table, and the body's own reference count for
/// any other nested CTE.
type RecursiveRefScope = Vec<(String, usize)>;

impl RecursiveRefCounter<'_> {
    /// The number of recursive references implied by referring to `name`.
    fn name_weight(&self, name: &str, scope: &RecursiveRefScope) -> usize {
        for (scope_name, weight) in scope.iter().rev() {
            if scope_name == name {
                return *weight;
            }
        }
        usize::from(name == self.cte_name)
    }

    /// Brings the CTEs of a nested `WITH` into scope with their reference
    /// weights. The caller is responsible for truncating `scope` afterwards.
    fn push_nested_ctes(&self, with: Option<&With>, scope: &mut RecursiveRefScope) {
        let Some(with) = with else {
            return;
        };
        for cte in &with.ctes {
            let name = normalize_ident(cte.tbl_name.as_str());
            // The CTE's own name is visible inside its body, where it refers
            // to the nested CTE itself rather than the recursive table.
            scope.push((name, 0));
            let weight = self.count_select(&cte.select, scope);
            scope.last_mut().expect("scope entry pushed above").1 = weight;
        }
    }

    fn count_select(&self, select: &Select, scope: &mut RecursiveRefScope) -> usize {
        let scope_base = scope.len();
        self.push_nested_ctes(select.with.as_ref(), scope);
        let mut count = self.count_one_select(&select.body.select, scope);
        for compound in &select.body.compounds {
            count += self.count_one_select(&compound.select, scope);
        }
        for sorted in &select.order_by {
            count += self.count_expr(&sorted.expr, scope);
        }
        if let Some(limit) = &select.limit {
            count += self.count_expr(&limit.expr, scope);
            if let Some(offset) = &limit.offset {
                count += self.count_expr(offset, scope);
            }
        }
        scope.truncate(scope_base);
        count
    }

    fn count_one_select(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> usize {
        match one {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                let mut count = 0;
                if let Some(from) = from {
                    count += self.count_from_table(&from.select, scope);
                    for join in &from.joins {
                        count += self.count_from_table(&join.table, scope);
                        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                            count += self.count_expr(expr, scope);
                        }
                    }
                }
                for column in columns {
                    if let ast::ResultColumn::Expr(expr, _) = column {
                        count += self.count_expr(expr, scope);
                    }
                }
                if let Some(expr) = where_clause {
                    count += self.count_expr(expr, scope);
                }
                if let Some(group_by) = group_by {
                    for expr in &group_by.exprs {
                        count += self.count_expr(expr, scope);
                    }
                    if let Some(having) = &group_by.having {
                        count += self.count_expr(having, scope);
                    }
                }
                for window_def in window_clause {
                    count += self.count_window(&window_def.window, scope);
                }
                count
            }
            ast::OneSelect::Values(rows) => rows
                .iter()
                .flatten()
                .map(|expr| self.count_expr(expr, scope))
                .sum(),
        }
    }

    fn count_from_table(&self, table: &ast::SelectTable, scope: &mut RecursiveRefScope) -> usize {
        match table {
            ast::SelectTable::Table(name, _, _) => {
                if name.db_name.is_none() {
                    self.name_weight(&normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                }
            }
            ast::SelectTable::TableCall(name, args, _) => {
                let mut count = if name.db_name.is_none() {
                    self.name_weight(&normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                };
                for arg in args {
                    count += self.count_expr(arg, scope);
                }
                count
            }
            ast::SelectTable::Select(subselect, _) => self.count_select(subselect, scope),
            ast::SelectTable::Sub(from, _) => {
                let mut count = self.count_from_table(&from.select, scope);
                for join in &from.joins {
                    count += self.count_from_table(&join.table, scope);
                    if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                        count += self.count_expr(expr, scope);
                    }
                }
                count
            }
        }
    }

    fn count_window(&self, window: &ast::Window, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        for expr in &window.partition_by {
            count += self.count_expr(expr, scope);
        }
        for sorted in &window.order_by {
            count += self.count_expr(&sorted.expr, scope);
        }
        if let Some(frame_clause) = &window.frame_clause {
            for bound in std::iter::once(&frame_clause.start).chain(frame_clause.end.as_ref()) {
                if let ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) = bound {
                    count += self.count_expr(expr, scope);
                }
            }
        }
        count
    }

    fn count_expr(&self, expr: &Expr, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        let _ = walk_expr(expr, &mut |node: &Expr| -> Result<WalkControl> {
            match node {
                Expr::Exists(select) | Expr::Subquery(select) => {
                    count += self.count_select(select, scope);
                    Ok(WalkControl::SkipChildren)
                }
                Expr::InSelect { rhs, .. } => {
                    count += self.count_select(rhs, scope);
                    // The walker does not descend into the subquery, only the
                    // left-hand side expression.
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        });
        count
    }

    /// Returns `(top_level_from_count, total_count)` for one arm of a
    /// recursive CTE body: direct references to the recursive table in the
    /// arm's FROM clause, and all references reachable from the arm.
    fn count_arm(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> (usize, usize) {
        fn count_direct_in_from_table(
            counter: &RecursiveRefCounter,
            table: &ast::SelectTable,
            scope: &RecursiveRefScope,
        ) -> usize {
            match table {
                ast::SelectTable::Table(name, _, _) | ast::SelectTable::TableCall(name, _, _) => {
                    if name.db_name.is_some() {
                        return 0;
                    }
                    let name = normalize_ident(name.name.as_str());
                    // A direct reference only counts when nothing shadows the
                    // recursive table's name.
                    usize::from(
                        name == counter.cte_name
                            && !scope.iter().any(|(scope_name, _)| *scope_name == name),
                    )
                }
                ast::SelectTable::Select(_, _) => 0,
                ast::SelectTable::Sub(from, _) => {
                    count_direct_in_from_table(counter, &from.select, scope)
                        + from
                            .joins
                            .iter()
                            .map(|join| count_direct_in_from_table(counter, &join.table, scope))
                            .sum::<usize>()
                }
            }
        }

        let top_level_from_count = if let ast::OneSelect::Select {
            from: Some(from), ..
        } = one
        {
            count_direct_in_from_table(self, &from.select, scope)
                + from
                    .joins
                    .iter()
                    .map(|join| count_direct_in_from_table(self, &join.table, scope))
                    .sum::<usize>()
        } else {
            0
        };
        let total_count = self.count_one_select(one, scope);
        (top_level_from_count, total_count)
    }
}

fn collect_from_select_table(table: &ast::SelectTable, out: &mut Vec<String>) {
    match table {
        ast::SelectTable::Table(qualified_name, _, _) => {
            if qualified_name.db_name.is_none() {
                out.push(normalize_ident(qualified_name.name.as_str()));
            }
        }
        ast::SelectTable::TableCall(qualified_name, args, _) => {
            if qualified_name.db_name.is_none() {
                out.push(normalize_ident(qualified_name.name.as_str()));
            }
            for arg in args {
                collect_subquery_table_refs_in_expr(arg, out);
            }
        }
        ast::SelectTable::Select(subselect, _) => {
            collect_from_clause_table_refs(subselect, out);
        }
        ast::SelectTable::Sub(from_clause, _) => {
            collect_from_select_table(&from_clause.select, out);
            for join in &from_clause.joins {
                collect_from_select_table(&join.table, out);
                if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
            }
        }
    }
}

/// Collect table references from subqueries embedded in expressions.
fn collect_subquery_table_refs_in_select_exprs(select: &Select, out: &mut Vec<String>) {
    collect_subquery_table_refs_in_one_select(&select.body.select, out);
    for compound in &select.body.compounds {
        collect_subquery_table_refs_in_one_select(&compound.select, out);
    }

    for sorted in &select.order_by {
        collect_subquery_table_refs_in_expr(&sorted.expr, out);
    }

    if let Some(limit) = &select.limit {
        collect_subquery_table_refs_in_expr(&limit.expr, out);
        if let Some(offset) = &limit.offset {
            collect_subquery_table_refs_in_expr(offset, out);
        }
    }
}

fn collect_subquery_table_refs_in_one_select(one: &ast::OneSelect, out: &mut Vec<String>) {
    match one {
        ast::OneSelect::Select {
            columns,
            where_clause,
            group_by,
            ..
        } => {
            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
            }
            if let Some(expr) = where_clause {
                collect_subquery_table_refs_in_expr(expr, out);
            }
            if let Some(group_by) = group_by {
                for expr in &group_by.exprs {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
                if let Some(having) = &group_by.having {
                    collect_subquery_table_refs_in_expr(having, out);
                }
            }
            if let ast::OneSelect::Select {
                from: Some(from), ..
            } = one
            {
                for join in &from.joins {
                    if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                        collect_subquery_table_refs_in_expr(expr, out);
                    }
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
            }
        }
    }
}

fn collect_subquery_table_refs_in_expr(expr: &Expr, out: &mut Vec<String>) {
    let _ = walk_expr(expr, &mut |node: &Expr| -> Result<WalkControl> {
        match node {
            Expr::Exists(select) | Expr::Subquery(select) => {
                collect_from_clause_table_refs(select, out);
                Ok(WalkControl::SkipChildren)
            }
            Expr::InSelect { rhs, .. } => {
                collect_from_clause_table_refs(rhs, out);
                Ok(WalkControl::SkipChildren)
            }
            _ => Ok(WalkControl::Continue),
        }
    });
}

/// Valid ways to refer to the rowid of a btree table.
pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"];

/// This function walks the expression tree and identifies aggregate
/// and window functions.
///
/// # Window functions
/// - If `windows` is `Some`, window functions will be resolved against the
///   provided set of windows or added to it if not present.
/// - If `windows` is `None`, any encountered window function is treated
///   as a misuse and results in a parse error.
///
/// # Aggregates
/// Aggregate functions are always allowed. They are collected in `aggs`.
///
/// # Returns
/// - `Ok(true)` if at least one aggregate function was found.
/// - `Ok(false)` if no aggregates were found.
/// - `Err(..)` if an invalid function usage is detected (e.g., window
///   function encountered while `windows` is `None`).
pub fn resolve_window_and_aggregate_functions(
    top_level_expr: &Expr,
    resolver: &Resolver,
    aggs: &mut Vec<Aggregate>,
    mut windows: Option<&mut Vec<Window>>,
    named_windows: &mut [crate::translate::plan::NamedWindowDef],
) -> Result<bool> {
    let mut contains_aggregates = false;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinctness,
                filter_over,
                order_by,
                within_group,
            } => {
                let ordered_set_func = ordered_set_agg_func(&normalize_ident(name.as_str()));

                if !within_group.is_empty() {
                    let new_agg = build_ordered_set_aggregate(
                        name,
                        args,
                        distinctness.as_ref(),
                        order_by,
                        within_group,
                        filter_over,
                        ordered_set_func,
                    )?;
                    add_aggregate_if_not_exists(
                        aggs,
                        expr,
                        &new_agg.args,
                        Distinctness::NonDistinct,
                        new_agg.func,
                        filter_over.filter_clause.as_deref().cloned(),
                    )?;
                    contains_aggregates = true;
                    return Ok(WalkControl::SkipChildren);
                }

                // Ordered-set aggregates are only meaningful with WITHIN GROUP. mode() in
                // particular has no plain-aggregate form, so reject it early with a clear error.
                if matches!(ordered_set_func, Some(AggFunc::Mode)) {
                    crate::bail_parse_error!(
                        "mode() requires a WITHIN GROUP (ORDER BY ...) clause"
                    );
                }

                if !order_by.is_empty() {
                    crate::bail_parse_error!(
                        "ORDER BY clause is not supported yet in aggregate functions"
                    );
                }
                let args_count = args.len();
                let distinctness = Distinctness::from_ast(distinctness.as_ref());

                match Func::resolve_function(name.as_str(), args_count)? {
                    Some(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Agg(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                distinctness,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(
                                aggs,
                                expr,
                                args,
                                distinctness,
                                f,
                                filter_over.filter_clause.as_deref().cloned(),
                            )?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Some(Func::Window(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Window(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                distinctness,
                            )?;
                        } else {
                            crate::bail_parse_error!("misuse of window function: {}()", f);
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    None => {
                        if let Some(f) = resolver
                            .symbol_table
                            .resolve_function(name.as_str(), args_count)
                        {
                            let func = AggFunc::External(f.func.clone().into());
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                if let Some(over_clause) = filter_over.over_clause.as_ref() {
                                    link_with_window(
                                        windows.as_deref_mut(),
                                        named_windows,
                                        resolver,
                                        expr,
                                        AccumulatorFunc::Agg(func),
                                        over_clause,
                                        filter_over.filter_clause.as_deref(),
                                        distinctness,
                                    )?;
                                } else {
                                    add_aggregate_if_not_exists(
                                        aggs,
                                        expr,
                                        args,
                                        distinctness,
                                        func,
                                        filter_over.filter_clause.as_deref().cloned(),
                                    )?;
                                    contains_aggregates = true;
                                }
                                return Ok(WalkControl::SkipChildren);
                            }
                        }
                    }
                    _ => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }
                    }
                }
            }
            Expr::FunctionCallStar { name, filter_over } => {
                match Func::resolve_function(name.as_str(), 0)? {
                    Some(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Agg(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                Distinctness::NonDistinct,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(
                                aggs,
                                expr,
                                &[],
                                Distinctness::NonDistinct,
                                f,
                                filter_over.filter_clause.as_deref().cloned(),
                            )?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Some(Func::Window(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Window(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                Distinctness::NonDistinct,
                            )?;
                        } else {
                            crate::bail_parse_error!("misuse of window function: {}()", f);
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Some(func) => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }

                        // Check if the function supports (*) syntax using centralized logic
                        if func.supports_star_syntax() {
                            return Ok(WalkControl::Continue);
                        } else {
                            crate::bail_parse_error!(
                                "wrong number of arguments to function {}()",
                                name.as_str()
                            );
                        }
                    }
                    None => {
                        if let Some(f) = resolver.symbol_table.resolve_function(name.as_str(), 0) {
                            let func = AggFunc::External(f.func.clone().into());
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                if let Some(over_clause) = filter_over.over_clause.as_ref() {
                                    link_with_window(
                                        windows.as_deref_mut(),
                                        named_windows,
                                        resolver,
                                        expr,
                                        AccumulatorFunc::Agg(func),
                                        over_clause,
                                        filter_over.filter_clause.as_deref(),
                                        Distinctness::NonDistinct,
                                    )?;
                                } else {
                                    add_aggregate_if_not_exists(
                                        aggs,
                                        expr,
                                        &[],
                                        Distinctness::NonDistinct,
                                        func,
                                        filter_over.filter_clause.as_deref().cloned(),
                                    )?;
                                    contains_aggregates = true;
                                }
                                return Ok(WalkControl::SkipChildren);
                            }
                        } else {
                            crate::bail_parse_error!("no such function: {}", name.as_str());
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(WalkControl::Continue)
    })?;

    Ok(contains_aggregates)
}

#[allow(clippy::too_many_arguments)]
fn link_with_window(
    windows: Option<&mut Vec<Window>>,
    named_windows: &mut [crate::translate::plan::NamedWindowDef],
    resolver: &Resolver,
    expr: &Expr,
    func: AccumulatorFunc,
    over_clause: &Over,
    filter_clause: Option<&Expr>,
    distinctness: Distinctness,
) -> Result<()> {
    if distinctness.is_distinct() {
        crate::bail_parse_error!("DISTINCT is not supported for window functions");
    }
    // FILTER decides which input rows contribute to a running aggregate, so
    // it is only meaningful for aggregating window functions. Non-aggregate
    // ones (`row_number`/`lag`/`lead`) have nothing to filter.
    if matches!(func, AccumulatorFunc::Window(_)) && filter_clause.is_some() {
        crate::bail_parse_error!("FILTER clause may only be used with aggregate window functions");
    }
    expr_vector_size(expr)?;
    if let Some(windows) = windows {
        // Every function carries a coerced frame (`WindowFunc::coerced_frame`
        // for built-in window funcs, the default `RANGE UNBOUNDED PRECEDING
        // TO CURRENT ROW` for aggregate window funcs). Functions whose
        // coerced frames disagree cannot share a single ephemeral-table
        // pass — see SQLite's invariant at window.c:1679 — so the planner
        // groups them into separate `Window` entries.
        let coerced_frame = match &func {
            AccumulatorFunc::Window(w) => w.coerced_frame(),
            AccumulatorFunc::Agg(_) => None,
        }
        .unwrap_or_default();
        let window = resolve_window(windows, named_windows, over_clause, coerced_frame)?;
        // Two equivalent window expressions can share one `WindowFunction`
        // entry unless they contain nondeterministic calls like `random()`,
        // which SQLite evaluates separately at each SQL occurrence.
        let deduplicate = !expr_contains_nondeterministic_scalar_function(expr, resolver)?;
        if deduplicate
            && window
                .functions
                .iter()
                .any(|f| exprs_are_equivalent(&f.original_expr, expr))
        {
            return Ok(());
        }
        window.functions.push(WindowFunction {
            func,
            original_expr: expr.clone(),
            rewritten: None,
        });
    } else {
        let func_name = match &func {
            AccumulatorFunc::Agg(f) => f.as_str().to_string(),
            AccumulatorFunc::Window(f) => f.to_string(),
        };
        crate::bail_parse_error!("misuse of window function: {}()", func_name);
    }
    Ok(())
}

/// Resolve the `Window` a function call should be attached to, given the
/// function's coerced frame. Two functions can share a `Window` only when
/// their OVER clauses are equivalent AND their coerced frames match —
/// functions with the same OVER but conflicting frames get separate
/// `Window` entries so each compiles to its own ephemeral-table pass.
fn resolve_window<'a>(
    windows: &'a mut Vec<Window>,
    named_windows: &mut [crate::translate::plan::NamedWindowDef],
    over_clause: &Over,
    frame: crate::translate::plan::Frame,
) -> Result<&'a mut Window> {
    match over_clause {
        Over::Window(window) if window.base.is_none() => {
            if let Some(idx) = windows.iter().position(|w| w.is_equivalent(window, &frame)) {
                return Ok(&mut windows[idx]);
            }
            windows.push(Window::new_unnamed(window, frame)?);
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
        Over::Window(window) => {
            if !Window::is_default_frame_spec(&window.frame_clause) {
                crate::bail_parse_error!("Custom frame specifications are not supported yet");
            }
            let base_name = normalize_ident(
                window
                    .base
                    .as_ref()
                    .expect("guarded by the preceding match arm")
                    .as_str(),
            );
            let def = named_windows
                .iter()
                .rfind(|definition| definition.name == base_name)
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such window: {base_name}"))
                })?;
            if !window.partition_by.is_empty() {
                crate::bail_parse_error!("cannot override PARTITION clause of window: {base_name}");
            }
            if def.has_frame_clause {
                crate::bail_parse_error!(
                    "cannot override frame specification of window: {base_name}"
                );
            }
            let mut bound = clone_named_window_bound(windows, def);
            if !bound.order_by.is_empty() && !window.order_by.is_empty() {
                crate::bail_parse_error!("cannot override ORDER BY clause of window: {base_name}");
            }
            if bound.order_by.is_empty() {
                bound.order_by = window
                    .order_by
                    .iter()
                    .map(|column| {
                        (
                            *column.expr.clone(),
                            column.order.unwrap_or(ast::SortOrder::Asc),
                            column.nulls,
                        )
                    })
                    .collect();
            }
            if let Some(idx) = windows
                .iter()
                .position(|candidate| candidate.is_equivalent_to_bound(&bound, &frame))
            {
                return Ok(&mut windows[idx]);
            }
            windows.push(Window::from_unnamed_bound(bound, frame));
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
        Over::Name(name) => {
            let window_name = normalize_ident(name.as_str());
            // Reuse an existing resolved entry with the same name AND
            // frame so functions sharing one coerced frame fold into one
            // ephemeral-table pass. SQLite uses the most recent
            // definition when names collide, so iterate in reverse.
            if let Some(idx) = windows
                .iter()
                .rposition(|w| w.name.as_ref() == Some(&window_name) && w.frame == frame)
            {
                return Ok(&mut windows[idx]);
            }
            // Need a new resolved entry. Verify the name exists.
            let def = named_windows
                .iter_mut()
                .rfind(|d| d.name == window_name)
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such window: {window_name}"))
                })?;
            // First attachment under this name takes ownership of the
            // bound exprs. Subsequent distinct-frame
            // attachments deep-clone from a sister resolved Window —
            // the first attachment guarantees one exists.
            let bound = match def.bound.take() {
                Some(bound) => bound,
                None => {
                    let sister = windows
                        .iter()
                        .rfind(|w| w.name.as_ref() == Some(&window_name))
                        .expect("sister Window must exist after the named def was taken");
                    NamedWindowBound {
                        partition_by: sister.partition_by.clone(),
                        order_by: sister.order_by.clone(),
                    }
                }
            };
            windows.push(Window::from_named_bound(window_name, bound, frame));
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
    }
}

fn clone_named_window_bound(
    windows: &[Window],
    def: &crate::translate::plan::NamedWindowDef,
) -> NamedWindowBound {
    match def.bound.as_ref() {
        Some(bound) => bound.clone(),
        None => {
            let sister = windows
                .iter()
                .rfind(|window| window.name.as_ref() == Some(&def.name))
                .expect("sister Window must exist after the named def was taken");
            NamedWindowBound {
                partition_by: sister.partition_by.clone(),
                order_by: sister.order_by.clone(),
            }
        }
    }
}

fn add_aggregate_if_not_exists(
    aggs: &mut Vec<Aggregate>,
    expr: &Expr,
    args: &[Box<Expr>],
    distinctness: Distinctness,
    func: AggFunc,
    filter_expr: Option<ast::Expr>,
) -> Result<()> {
    if distinctness.is_distinct() && args.len() != 1 {
        crate::bail_parse_error!("DISTINCT aggregate functions must have exactly one argument");
    }
    if aggs
        .iter()
        .all(|a| !exprs_are_equivalent(&a.original_expr, expr))
    {
        aggs.push(Aggregate::new(func, args, expr, distinctness, filter_expr));
    }
    Ok(())
}

/// Maps a normalized function name to the ordered-set [`AggFunc`] it implements, if any.
/// Ordered-set aggregates are written `f(direct_args) WITHIN GROUP (ORDER BY x)`.
fn ordered_set_agg_func(normalized_name: &str) -> Option<AggFunc> {
    match normalized_name {
        "mode" => Some(AggFunc::Mode),
        "percentile_cont" => Some(AggFunc::PercentileCont),
        "percentile_disc" => Some(AggFunc::PercentileDisc),
        _ => None,
    }
}

struct OrderedSetAggregate {
    func: AggFunc,
    // Matches `add_aggregate_if_not_exists`, which consumes `&[Box<Expr>]`.
    #[allow(clippy::vec_box)]
    args: Vec<Box<Expr>>,
}

/// Validates a `WITHIN GROUP (ORDER BY ...)` ordered-set aggregate and rewrites it into a
/// uniform argument list for the rest of the pipeline: `[value]` for `mode`, or
/// `[value, fraction]` for the percentile functions. `value` is the single ORDER BY
/// expression; `fraction` is the direct argument.
fn build_ordered_set_aggregate(
    name: &ast::Name,
    args: &[Box<Expr>],
    distinctness: Option<&ast::Distinctness>,
    order_by: &[ast::SortedColumn],
    within_group: &[ast::SortedColumn],
    filter_over: &ast::FunctionTail,
    ordered_set_func: Option<AggFunc>,
) -> Result<OrderedSetAggregate> {
    let Some(func) = ordered_set_func else {
        crate::bail_parse_error!(
            "WITHIN GROUP is not supported for function {}()",
            name.as_str()
        );
    };
    if filter_over.over_clause.is_some() {
        crate::bail_parse_error!(
            "ordered-set aggregate {}() may not be used as a window function",
            name.as_str()
        );
    }
    if distinctness.is_some() {
        crate::bail_parse_error!(
            "DISTINCT is not supported for ordered-set aggregate {}()",
            name.as_str()
        );
    }
    if !order_by.is_empty() {
        crate::bail_parse_error!(
            "{}() does not accept an argument ORDER BY together with WITHIN GROUP",
            name.as_str()
        );
    }
    if within_group.len() != 1 {
        crate::bail_parse_error!(
            "WITHIN GROUP for {}() must specify exactly one ORDER BY expression",
            name.as_str()
        );
    }
    let sort_col = &within_group[0];
    if matches!(sort_col.order, Some(ast::SortOrder::Desc)) || sort_col.nulls.is_some() {
        crate::bail_parse_error!(
            "DESC and NULLS ordering inside WITHIN GROUP are not supported yet"
        );
    }
    let expected_direct_args = match func {
        AggFunc::Mode => 0,
        _ => 1, // percentile_cont / percentile_disc take the fraction
    };
    if args.len() != expected_direct_args {
        crate::bail_parse_error!("wrong number of arguments to function {}()", name.as_str());
    }
    let mut new_args: Vec<Box<Expr>> = Vec::with_capacity(args.len() + 1);
    new_args.push(sort_col.expr.clone());
    new_args.extend(args.iter().cloned());
    Ok(OrderedSetAggregate {
        func,
        args: new_args,
    })
}

/// Validate the arm structure of a recursive CTE body and return the index of
/// the first recursive arm. Errors mirror SQLite: a self-reference in the
/// initial query or outside the recursive arm's top-level FROM is a circular
/// reference; more than one reference per recursive arm is rejected.
pub(crate) fn validate_recursive_cte_structure(cte_name: &str, select: &Select) -> Result<usize> {
    let mut first_recursive_query_index = None;
    let ref_counter = RecursiveRefCounter { cte_name };
    // Nested CTEs defined at the body level are visible in every arm; bring
    // them into scope so shadowing and use-through-nested-CTE references are
    // counted the way name resolution will see them.
    let mut ref_scope = RecursiveRefScope::new();
    ref_counter.push_nested_ctes(select.with.as_ref(), &mut ref_scope);
    for (query_index, query) in std::iter::once(&select.body.select)
        .chain(
            select
                .body
                .compounds
                .iter()
                .map(|compound| &compound.select),
        )
        .enumerate()
    {
        let (top_level_from_count, total_count) = ref_counter.count_arm(query, &mut ref_scope);
        if first_recursive_query_index.is_none() && total_count == 0 {
            continue;
        }
        if query_index == 0 {
            crate::bail_parse_error!("circular reference: {}", cte_name);
        }
        first_recursive_query_index.get_or_insert(query_index);
        if top_level_from_count == 0 {
            crate::bail_parse_error!("circular reference: {}", cte_name);
        }
        if top_level_from_count > 1 {
            crate::bail_parse_error!("multiple references to recursive table: {}", cte_name);
        }
        if total_count > top_level_from_count {
            crate::bail_parse_error!("multiple recursive references: {}", cte_name);
        }
    }
    first_recursive_query_index.ok_or_else(|| {
        crate::LimboError::InternalError(format!("recursive CTE {cte_name} has no recursive query"))
    })
}

fn reject_aggregates_and_windows_in_recursive_query(query: &Plan) -> Result<()> {
    match query {
        Plan::Select(select) => {
            if !select.aggregates.is_empty() || select.group_by.is_some() {
                crate::bail_parse_error!("recursive aggregate queries not supported");
            }
            if select.window.is_some() {
                crate::bail_parse_error!("cannot use window functions in recursive queries");
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            if left
                .iter()
                .any(|(select, _)| !select.aggregates.is_empty() || select.group_by.is_some())
                || !right_most.aggregates.is_empty()
                || right_most.group_by.is_some()
            {
                crate::bail_parse_error!("recursive aggregate queries not supported");
            }
            if left.iter().any(|(select, _)| select.window.is_some()) || right_most.window.is_some()
            {
                crate::bail_parse_error!("cannot use window functions in recursive queries");
            }
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
            return Err(crate::LimboError::InternalError(
                "recursive CTE query is not a SELECT".to_string(),
            ));
        }
    }
    Ok(())
}

fn transform_args_into_where_terms(
    args: &[Box<Expr>],
    internal_id: TableInternalId,
    predicates: &mut Vec<Expr>,
    table: &Table,
) -> Result<()> {
    let mut args_iter = args.iter();
    let mut hidden_count = 0;
    for (i, col) in table.columns().iter().enumerate() {
        if !col.hidden() {
            continue;
        }
        hidden_count += 1;

        if let Some(arg_expr) = args_iter.next() {
            let column_expr = Expr::Column {
                database: None,
                table: internal_id,
                column: i,
                is_rowid_alias: col.is_rowid_alias(),
            };
            let expr = match arg_expr.as_ref() {
                Expr::Literal(Null) => Expr::IsNull(Box::new(column_expr)),
                other => Expr::Binary(
                    column_expr.into(),
                    ast::Operator::Equals,
                    other.clone().into(),
                ),
            };
            predicates.push(expr);
        }
    }

    if args_iter.next().is_some() {
        return Err(crate::LimboError::ParseError(format!(
            "Too many arguments for {}: expected at most {}, got {}",
            table.get_name(),
            hidden_count,
            hidden_count + 1 + args_iter.count()
        )));
    }

    Ok(())
}
// ── Bound planning: consume the output of the binding phase ──────────────
//
// These functions are the planner-side counterpart of [super::bind::BindContext].
// The binder resolves all names and produces `Bound*` structures; the functions
// below turn pre-bound CTE definitions and derived tables into planned
// `JoinedTable`s, and fold already-bound JOIN constraints / vtab arguments into
// WHERE terms. They perform no name resolution themselves.

/// Plan all CTE definitions produced by the binder, in definition order.
///
/// Explicit column-count validation is deferred until the CTE is actually
/// referenced (matching SQLite, where unreferenced CTEs with mismatched
/// column counts don't error) — see [plan_one_bound_cte].
pub fn plan_bound_ctes(
    cte_definitions: Vec<(String, super::bind::CteEntry)>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<rustc_hash::FxHashMap<String, JoinedTable>> {
    plan_bound_ctes_with_outer_refs(
        cte_definitions,
        resolver,
        program,
        connection,
        &[],
        &Default::default(),
    )
}

/// [plan_bound_ctes] with context from the enclosing scope: correlation refs
/// (for recursive CTE bodies, which are planned raw) and already-planned CTEs
/// from enclosing WITH clauses. The inherited CTEs seed the planned map so a
/// nested CTE body that references a parent or sibling CTE can find it; a CTE
/// defined here shadows an inherited one with the same name.
pub fn plan_bound_ctes_with_outer_refs(
    mut cte_definitions: Vec<(String, super::bind::CteEntry)>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    outer_query_refs: &[OuterQueryReference],
    inherited_ctes: &rustc_hash::FxHashMap<String, JoinedTable>,
) -> Result<rustc_hash::FxHashMap<String, JoinedTable>> {
    let mut planned = inherited_ctes.clone();
    // Track planning per definition index, not by map membership: an
    // inherited entry with the same name must not stop the shadowing
    // definition from being planned.
    let mut done = vec![false; cte_definitions.len()];
    for idx in 0..cte_definitions.len() {
        plan_one_bound_cte(
            idx,
            &mut cte_definitions,
            resolver,
            program,
            connection,
            &mut planned,
            &mut done,
            outer_query_refs,
        )?;
    }
    Ok(planned)
}

/// Plan derived tables (FROM-clause subqueries) from binder-provided bindings.
///
/// Each derived table's inner select is already bound. This function plans them
/// and returns a map of `internal_id` → `JoinedTable` for use in
/// [super::bind::BoundSelect::into_table_references_with_outer_refs]. Outer refs from the
/// enclosing scope are propagated so correlated references inside a derived
/// table stay visible.
pub fn plan_derived_tables_with_outer_refs(
    derived_bindings: rustc_hash::FxHashMap<TableInternalId, super::bind::BoundSubquery>,
    planned_ctes: &mut rustc_hash::FxHashMap<String, JoinedTable>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    outer_query_refs: Vec<OuterQueryReference>,
) -> Result<rustc_hash::FxHashMap<TableInternalId, JoinedTable>> {
    let mut planned: rustc_hash::FxHashMap<TableInternalId, JoinedTable> = Default::default();

    for (internal_id, bound_sq) in derived_bindings {
        let subplan = plan_bound_subquery(
            bound_sq,
            resolver,
            program,
            connection,
            outer_query_refs.clone(),
            planned_ctes,
            QueryDestination::placeholder_for_subquery(),
        )?;

        let jt = JoinedTable::new_subquery_from_plan(
            String::new(), // identifier set later by scope_to_table_references
            subplan,
            None, // join_info set later
            internal_id,
            None,  // no explicit columns
            None,  // not a CTE
            false, // no materialize hint
        )?;
        planned.insert(internal_id, jt);
    }

    Ok(planned)
}

/// Add every planned CTE as a definition-only outer query reference on each
/// `TableReferences`, skipping names already present. This lets subqueries
/// inside the query reference CTEs from an enclosing WITH clause by name.
pub fn add_planned_ctes_as_outer_refs(
    table_refs: &mut [TableReferences],
    planned_ctes: &rustc_hash::FxHashMap<String, JoinedTable>,
) {
    for tr in table_refs {
        for (name, jt) in planned_ctes {
            if tr.outer_query_refs().iter().any(|r| r.identifier == *name) {
                continue;
            }
            tr.add_outer_query_reference(OuterQueryReference::cte_definition_only(
                name.clone(),
                jt.internal_id,
                jt.table.clone(),
            ));
        }
    }
}

/// Turn a [super::bind::BoundSelect] into ready-to-use [TableReferences],
/// one per SELECT core (main first, then compounds).
///
/// This is the single place where bound output becomes table references:
/// 1. plan the WITH-clause CTEs
/// 2. make CTEs planned by enclosing scopes available by name
/// 3. plan the FROM-clause subqueries (derived tables)
/// 4. convert the bound scopes into `TableReferences` (with the caller's
///    outer refs attached for correlation)
/// 5. attach every planned CTE as a definition-only outer ref so subqueries
///    can still reference them by name
///
/// Also returns the pre-bound expression subqueries, keyed by the id in the
/// corresponding [ast::Expr::SubqueryResult].
pub fn plan_bound_select_refs(
    mut bound: super::bind::BoundSelect,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    outer_query_refs: Vec<OuterQueryReference>,
    inherited_ctes: &rustc_hash::FxHashMap<String, JoinedTable>,
) -> Result<(
    Vec<TableReferences>,
    rustc_hash::FxHashMap<TableInternalId, super::bind::BoundSubquery>,
)> {
    let cte_definitions = std::mem::take(&mut bound.cte_definitions);
    let subquery_bindings = std::mem::take(&mut bound.subquery_bindings);
    let derived_bindings = std::mem::take(&mut bound.derived_bindings);

    let mut planned_ctes = plan_bound_ctes_with_outer_refs(
        cte_definitions,
        resolver,
        program,
        connection,
        &outer_query_refs,
        inherited_ctes,
    )?;

    let mut planned_derived = plan_derived_tables_with_outer_refs(
        derived_bindings,
        &mut planned_ctes,
        resolver,
        program,
        connection,
        outer_query_refs.clone(),
    )?;

    let mut table_refs = bound.into_table_references_with_outer_refs(
        &mut planned_ctes,
        &mut planned_derived,
        outer_query_refs,
    )?;

    add_planned_ctes_as_outer_refs(&mut table_refs, &planned_ctes);

    Ok((table_refs, subquery_bindings))
}

/// Plan a pre-bound subquery ([super::bind::BoundSubquery]) into a [Plan]
/// with no name resolution: [plan_bound_select_refs] followed by
/// [prepare_select_plan] in bound mode.
pub fn plan_bound_subquery(
    bound_sq: super::bind::BoundSubquery,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    outer_query_refs: Vec<OuterQueryReference>,
    inherited_ctes: &rustc_hash::FxHashMap<String, JoinedTable>,
    query_destination: QueryDestination,
) -> Result<Plan> {
    let (table_refs, bound_subqueries) = plan_bound_select_refs(
        bound_sq.inner_bound,
        resolver,
        program,
        connection,
        outer_query_refs,
        inherited_ctes,
    )?;
    prepare_select_plan(
        bound_sq.select,
        resolver,
        program,
        crate::translate::select::SelectBinding {
            table_refs: table_refs.into_iter(),
            bound_subqueries,
        },
        query_destination,
        connection,
    )
}

/// Plan a recursive CTE from its pre-bound arms ([super::bind::RecursiveCteBinding]).
///
/// The binder resolved all names, including the self-reference: every
/// recursive arm's reference to the CTE was bound to `binding.input_id`.
/// Making that id resolve to the recursive input table only requires seeding
/// the arms' planned-CTE map with the input table under the CTE's own name.
/// Compound-operator validation, ORDER BY (queue order), and LIMIT still read
/// the raw body in `select`, mirroring [prepare_recursive_cte_plan].
#[allow(clippy::too_many_arguments)]
fn prepare_bound_recursive_cte_plan(
    name: &str,
    select: &Select,
    binding: super::bind::RecursiveCteBinding,
    explicit_columns: &[String],
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    outer_query_refs: &[OuterQueryReference],
    inherited_ctes: &rustc_hash::FxHashMap<String, JoinedTable>,
) -> Result<Plan> {
    let first_recursive_query_index = binding.first_recursive_arm_index;

    let recursive_compound_operator =
        select.body.compounds[first_recursive_query_index - 1].operator;
    let union_all = match recursive_compound_operator {
        ast::CompoundOperator::UnionAll => true,
        ast::CompoundOperator::Union => false,
        ast::CompoundOperator::Except | ast::CompoundOperator::Intersect => {
            crate::bail_parse_error!(
                "recursive CTEs must use UNION ALL or UNION between the initial and recursive queries"
            );
        }
    };
    for compound in select
        .body
        .compounds
        .iter()
        .skip(first_recursive_query_index)
    {
        if compound.operator != recursive_compound_operator {
            crate::bail_parse_error!("recursive CTE queries must use the same UNION operator");
        }
    }

    let initial_query = plan_bound_subquery(
        binding.initial,
        resolver,
        program,
        connection,
        outer_query_refs.to_vec(),
        inherited_ctes,
        QueryDestination::placeholder_for_subquery(),
    )?;

    let explicit_columns = (!explicit_columns.is_empty()).then_some(explicit_columns);
    if let Some(columns) = explicit_columns {
        let result_column_count = initial_query.select_result_columns().len();
        if columns.len() != result_column_count {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                name,
                result_column_count,
                columns.len()
            );
        }
    }

    let input_table = JoinedTable::new_recursive_cte_input(
        name.to_string(),
        &initial_query,
        binding.input_id,
        explicit_columns,
    )?;
    let input_table_id = input_table.internal_id;

    // The self-reference in each arm is a scope table with
    // ScopeTableSource::Cte under the CTE's own name; seeding the planned map
    // with the input table makes scope conversion find it. All arms share
    // input_table_id, so they all read the same recursive input cursor.
    let mut arm_ctes = inherited_ctes.clone();
    arm_ctes.insert(name.to_string(), input_table);

    let mut arm_plans = Vec::with_capacity(binding.recursive_arms.len());
    for arm in binding.recursive_arms {
        let arm_plan = plan_bound_subquery(
            arm,
            resolver,
            program,
            connection,
            outer_query_refs.to_vec(),
            &arm_ctes,
            QueryDestination::placeholder_for_subquery(),
        )?;
        let Plan::Select(arm_plan) = arm_plan else {
            unreachable!("a single-arm SELECT plans to Plan::Select");
        };
        arm_plans.push(arm_plan);
    }

    let last_arm = arm_plans.pop().expect("at least one recursive arm");
    for arm_plan in &arm_plans {
        if arm_plan.result_columns.len() != last_arm.result_columns.len() {
            crate::bail_parse_error!(
                "SELECTs to the left and right of {} do not have the same number of result columns",
                ast::CompoundOperator::UnionAll
            );
        }
    }
    let recursive_query = if arm_plans.is_empty() {
        Plan::Select(last_arm)
    } else {
        Plan::CompoundSelect {
            left: arm_plans
                .into_iter()
                .map(|plan| (*plan, ast::CompoundOperator::UnionAll))
                .collect(),
            right_most: last_arm,
            limit: None,
            offset: None,
            order_by: None,
        }
    };

    if initial_query.select_result_columns().len() != recursive_query.select_result_columns().len()
    {
        crate::bail_parse_error!(
            "SELECTs to the left and right of {} do not have the same number of result columns",
            recursive_compound_operator
        );
    }
    reject_aggregates_and_windows_in_recursive_query(&recursive_query)?;

    let queue_order = super::select::resolve_recursive_cte_queue_order(
        &select.order_by,
        &initial_query,
        &recursive_query,
    )?;
    let (limit, offset) = select
        .limit
        .clone()
        .map_or(Ok((None, None)), |limit| parse_limit(limit, resolver))?;

    Ok(Plan::RecursiveCte(Box::new(
        super::plan::RecursiveCtePlan {
            name: name.to_string(),
            initial_query: Box::new(initial_query),
            recursive_query: Box::new(recursive_query),
            input_table_id,
            union_all,
            limit,
            offset,
            queue_order,
            query_destination: QueryDestination::placeholder_for_subquery(),
        },
    )))
}

/// Plan a single CTE using its pre-bound data from the binder.
///
/// The binder already resolved all names and column references in the CTE body.
/// This function takes the pre-bound `inner_bound` and converts it into a plan
/// without re-binding. Referenced sibling CTEs are planned recursively first
/// to avoid exponential blowup on transitive dependencies.
#[allow(clippy::too_many_arguments)]
fn plan_one_bound_cte(
    cte_idx: usize,
    cte_definitions: &mut [(String, super::bind::CteEntry)],
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    planned: &mut rustc_hash::FxHashMap<String, JoinedTable>,
    done: &mut [bool],
    outer_query_refs: &[OuterQueryReference],
) -> Result<()> {
    if done[cte_idx] {
        return Ok(());
    }
    done[cte_idx] = true;
    // A poisoned entry (deferred binding error) was never referenced — the
    // binder surfaces its error at any reference — so there is nothing to
    // plan, matching SQLite's lazy resolution of unused CTE bodies.
    if cte_definitions[cte_idx].1.bind_error.is_some() {
        return Ok(());
    }
    // Copy metadata needed before mutably borrowing the entry.
    let name = cte_definitions[cte_idx].0.clone();
    let referenced_indices = cte_definitions[cte_idx].1.referenced_cte_indices.clone();

    // Recursively plan referenced sibling CTEs first.
    for &ref_idx in &referenced_indices {
        plan_one_bound_cte(
            ref_idx,
            cte_definitions,
            resolver,
            program,
            connection,
            planned,
            done,
            outer_query_refs,
        )?;
    }

    if cte_definitions[cte_idx].1.recursive {
        let entry = &mut cte_definitions[cte_idx].1;
        let binding = entry
            .recursive_binding
            .take()
            .expect("recursive CTE binding should be present");
        let select = entry.select.clone();
        let explicit_columns = entry.explicit_columns.clone();
        let cte_id = entry.cte_id;
        let materialize_hint = entry.materialize_hint;

        let plan = prepare_bound_recursive_cte_plan(
            &name,
            &select,
            binding,
            &explicit_columns,
            resolver,
            program,
            connection,
            outer_query_refs,
            planned,
        )?;
        let explicit_cols = if explicit_columns.is_empty() {
            None
        } else {
            Some(explicit_columns.as_slice())
        };
        let cte_table = JoinedTable::new_subquery_from_plan(
            name.clone(),
            plan,
            None,
            program.table_reference_counter.next(),
            explicit_cols,
            Some(cte_id),
            materialize_hint,
        )?;
        planned.insert(name, cte_table);
        return Ok(());
    }

    let entry = &mut cte_definitions[cte_idx].1;

    // Take the pre-bound data produced by the binder. Already-planned sibling
    // CTEs are passed as inherited: CTEs can be referenced not only from the
    // FROM clause (tracked by referenced_cte_indices) but also from correlated
    // subqueries within the CTE body.
    let bound_sq = super::bind::BoundSubquery {
        select: entry.select.clone(),
        inner_bound: entry
            .inner_bound
            .take()
            .expect("CTE inner binding should be present"),
    };

    // Block circular references during planning.
    program.push_cte_being_defined(name.clone());
    let plan_result = plan_bound_subquery(
        bound_sq,
        resolver,
        program,
        connection,
        outer_query_refs.to_vec(),
        planned,
        QueryDestination::placeholder_for_subquery(),
    );
    program.pop_cte_being_defined();
    let cte_plan = plan_result?;

    let entry = &cte_definitions[cte_idx].1;
    let explicit_cols = if entry.explicit_columns.is_empty() {
        None
    } else {
        // SQLite defers explicit column-count validation until the CTE is
        // actually referenced; scope_to_table_references performs the check.
        Some(entry.explicit_columns.as_slice())
    };

    let cte_table = JoinedTable::new_subquery_from_plan(
        name.clone(),
        cte_plan,
        None,
        program.table_reference_counter.next(),
        explicit_cols,
        Some(entry.cte_id),
        entry.materialize_hint,
    )?;

    planned.insert(name, cte_table);
    Ok(())
}

/// Break a pre-bound WHERE clause into [WhereTerm]s.
///
/// Bound-path counterpart of [parse_where]: the binder already resolved all
/// identifiers (and rewrote BETWEEN into AND-connected comparisons), so this
/// only needs to split at AND boundaries.
pub fn parse_where_bound(
    where_clause: Option<&Expr>,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        break_predicate_at_and_boundaries(where_expr, out_where_clause);
    }
    Ok(())
}

/// Fold pre-bound JOIN ON/USING constraints into [WhereTerm]s.
///
/// Bound-path counterpart of the constraint handling in [parse_join]: table
/// resolution is already done and ON expressions are bound to `Expr::Column`.
/// NATURAL joins were already transformed to USING by the binder.
pub fn fold_join_constraints(
    from: &ast::FromClause,
    table_references: &mut TableReferences,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    for (join_idx, join) in from.joins.iter().enumerate() {
        // The first table is from.select (index 0 in joined_tables),
        // joins start at index 1. This holds under right_join_swapped too:
        // the binder swapped the tables so index 0 is the originally-right
        // table and index 1 is the originally-left one, which carries the
        // LeftOuter join_info — i.e. the outer table the ON/USING constraint
        // must be tagged with is still at index 1.
        let actual_table_idx = join_idx + 1;

        let outer = table_references.joined_tables()[actual_table_idx]
            .join_info
            .as_ref()
            .is_some_and(|j| j.is_outer());
        let outer_table_id = table_references.joined_tables()[actual_table_idx].internal_id;

        // NATURAL joins were rewritten to USING by the binder, but the
        // operator still carries the flag. SQLite does not use HIDDEN columns
        // for NATURAL joins, so natural-derived USING lookups must skip them
        // on the left side (explicit USING may match hidden columns).
        let natural = matches!(
            &join.operator,
            ast::JoinOperator::TypedJoin(Some(jt)) if jt.contains(JoinType::NATURAL)
        );

        match &join.constraint {
            Some(ast::JoinConstraint::On(expr)) => {
                let start_idx = out_where_clause.len();
                break_predicate_at_and_boundaries(expr, out_where_clause);
                for predicate in out_where_clause[start_idx..].iter_mut() {
                    predicate.from_outer_join = if outer { Some(outer_table_id) } else { None };
                }
            }
            Some(ast::JoinConstraint::Using(cols)) => {
                // USING join is replaced with a list of equality predicates.
                let right_table_idx = actual_table_idx;
                turso_assert!(right_table_idx > 0);

                for col_name in cols.iter() {
                    let name_normalized = normalize_ident(col_name.as_str());

                    // Scope the immutable borrows so mark_column_used below can
                    // borrow table_references mutably.
                    let (
                        left_table_idx,
                        left_table_id,
                        left_col_idx,
                        left_is_rowid_alias,
                        right_col_idx,
                        right_is_rowid_alias,
                        right_table_internal_id,
                    ) = {
                        let tables = table_references.joined_tables();
                        let left_tables = &tables[..right_table_idx];
                        let right_table = &tables[right_table_idx];

                        // Find column in left tables
                        let mut left_col = None;
                        for (left_table_offset, left_table) in left_tables.iter().enumerate() {
                            left_col = left_table
                                .columns()
                                .iter()
                                .enumerate()
                                .filter(|(_, col)| !natural || !col.hidden())
                                .find(|(_, col)| {
                                    col.name.as_deref().is_some_and(|name| {
                                        name.eq_ignore_ascii_case(&name_normalized)
                                    })
                                })
                                .map(|(idx, col)| {
                                    (
                                        left_table_offset,
                                        left_table.internal_id,
                                        idx,
                                        col.is_rowid_alias(),
                                    )
                                });
                            if left_col.is_some() {
                                break;
                            }
                        }
                        let Some((
                            left_table_idx,
                            left_table_id,
                            left_col_idx,
                            left_is_rowid_alias,
                        )) = left_col
                        else {
                            crate::bail_parse_error!(
                                "cannot join using column {} - column not present in both tables",
                                col_name.as_str()
                            );
                        };

                        // Find column in right table
                        let right_col = right_table
                            .columns()
                            .iter()
                            .enumerate()
                            .find(|(_, col)| {
                                col.name
                                    .as_deref()
                                    .is_some_and(|name| name.eq_ignore_ascii_case(&name_normalized))
                            })
                            .map(|(idx, col)| (idx, col.is_rowid_alias()));
                        let Some((right_col_idx, right_is_rowid_alias)) = right_col else {
                            crate::bail_parse_error!(
                                "cannot join using column {} - column not present in both tables",
                                col_name.as_str()
                            );
                        };

                        (
                            left_table_idx,
                            left_table_id,
                            left_col_idx,
                            left_is_rowid_alias,
                            right_col_idx,
                            right_is_rowid_alias,
                            right_table.internal_id,
                        )
                    };

                    let expr = Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table: left_table_id,
                            column: left_col_idx,
                            is_rowid_alias: left_is_rowid_alias,
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: right_table_internal_id,
                            column: right_col_idx,
                            is_rowid_alias: right_is_rowid_alias,
                        }),
                    );

                    let left_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(left_table_idx)
                        .unwrap();
                    left_table.mark_column_used(left_col_idx);
                    let right_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(right_table_idx)
                        .unwrap();
                    right_table.mark_column_used(right_col_idx);

                    out_where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: if outer {
                            Some(right_table_internal_id)
                        } else {
                            None
                        },
                        consumed: false,
                    });
                }
            }
            None => {}
        }
    }
    Ok(())
}

/// Walk the FROM clause AST and generate virtual-table argument predicates.
///
/// Bound-path counterpart of the vtab argument handling in [parse_table]:
/// `TableReferences` already contains the resolved `JoinedTable`s. For each
/// `TableCall` node we find the matching joined table by identifier and call
/// [transform_args_into_where_terms].
pub fn collect_vtab_predicates(
    from: &ast::FromClause,
    table_references: &TableReferences,
    vtab_predicates: &mut Vec<Expr>,
) -> Result<()> {
    collect_vtab_predicates_for_table(&from.select, table_references, vtab_predicates)?;
    for join in &from.joins {
        collect_vtab_predicates_for_table(&join.table, table_references, vtab_predicates)?;
    }
    Ok(())
}

fn collect_vtab_predicates_for_table(
    select_table: &ast::SelectTable,
    table_references: &TableReferences,
    vtab_predicates: &mut Vec<Expr>,
) -> Result<()> {
    if let ast::SelectTable::TableCall(qualified_name, args, maybe_alias) = select_table {
        if args.is_empty() {
            return Ok(());
        }
        let table_name = normalize_ident(qualified_name.name.as_str());
        let identifier = maybe_alias
            .as_ref()
            .map(|a| normalize_ident(a.name().as_str()))
            .unwrap_or(table_name);

        // Find the matching JoinedTable by identifier
        let joined_table = table_references
            .joined_tables()
            .iter()
            .find(|jt| jt.identifier == identifier);
        if let Some(jt) = joined_table {
            transform_args_into_where_terms(args, jt.internal_id, vtab_predicates, &jt.table)?;
        }
    }
    Ok(())
}

pub fn rewrite_between_exprs(expr: &mut Expr) -> Result<()> {
    walk_expr_mut(expr, &mut |e: &mut Expr| -> Result<WalkControl> {
        if let Expr::Between {
            lhs,
            not,
            start,
            end,
        } = e
        {
            let lhs_expr = std::mem::take(lhs.as_mut());
            let start_expr = std::mem::take(start.as_mut());
            let end_expr = std::mem::take(end.as_mut());

            let (lower, upper, combine_op) = if *not {
                (
                    Expr::Binary(
                        Box::new(lhs_expr.clone()),
                        ast::Operator::Less,
                        Box::new(start_expr),
                    ),
                    Expr::Binary(
                        Box::new(lhs_expr),
                        ast::Operator::Greater,
                        Box::new(end_expr),
                    ),
                    ast::Operator::Or,
                )
            } else {
                (
                    Expr::Binary(
                        Box::new(lhs_expr.clone()),
                        ast::Operator::GreaterEquals,
                        Box::new(start_expr),
                    ),
                    Expr::Binary(
                        Box::new(lhs_expr),
                        ast::Operator::LessEquals,
                        Box::new(end_expr),
                    ),
                    ast::Operator::And,
                )
            };
            *e = Expr::Binary(Box::new(lower), combine_op, Box::new(upper));
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/**
  Returns the earliest point at which a WHERE term can be evaluated.
  For expressions referencing tables, this is the innermost loop that contains a row for each
  table referenced in the expression.
  For expressions not referencing any tables (e.g. constants), this is before the main loop is
  opened, because they do not need any table data.
*/
pub fn determine_where_to_eval_term(
    term: &WhereTerm,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_id == table_id)
                .unwrap_or(usize::MAX),
        ));
    }

    determine_where_to_eval_expr(&term.expr, join_order, subqueries, table_references)
}

/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their index in [SelectPlan::joined_tables].
/// In the bitmask, the first bit is unused so that a mask with all zeros
/// can represent "no tables".
///
/// E.g. table 0 is represented by bit index 1, table 1 by bit index 2, etc.
///
/// Usage in Join Optimization
///
/// In join optimization, [TableMask] is used to:
/// - Generate subsets of tables for dynamic programming in join optimization
/// - Ensure tables are joined in valid orders (e.g., respecting LEFT JOIN order)
///
/// Usage with constraints (WHERE clause)
///
/// [TableMask] helps determine:
/// - Which tables are referenced in a constraint
/// - When a constraint can be applied as a join condition (all referenced tables must be on the left side of the table being joined)
///
/// Note that although [TableReference]s contain an internal ID as well, in join order optimization
/// the [TableMask] refers to the index of the table in the original join order, not the internal ID.
/// This is simply because we want to represent the tables as a contiguous set of bits, and the internal ID
/// might not be contiguous after e.g. subquery unnesting or other transformations.
pub type TableMask = BitSet;

/// Returns a [TableMask] representing the tables referenced in the given expression.
///
/// This includes outer references from subqueries, even if the subquery plan has
/// already been consumed, by relying on the cached outer reference ids.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &Expr,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<TableMask> {
    let mut mask = TableMask::default();
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(table_idx) = table_references
                    .joined_tables()
                    .iter()
                    .position(|t| t.internal_id == *table)
                {
                    mask.set(table_idx)?;
                } else if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_none()
                {
                    // Tables from outer query scopes are guaranteed to be 'in scope' for this query,
                    // so they don't need to be added to the table mask. However, if the table is not found
                    // in the outer scope either, then it's an invalid reference.
                    crate::bail_parse_error!("table not found in joined_tables");
                }
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            // Hence, the tables referenced in subqueries must be added to the table mask.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Unevaluated { plan } => {
                        let outer_ref_ids = plan.as_ref().unwrap().used_outer_query_ref_ids();
                        for outer_ref_id in &outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.set(table_idx)?;
                            }
                        }
                    }
                    SubqueryState::Evaluated { outer_ref_ids, .. } => {
                        // Now hash-join plans can now translate some correlated subqueries early, we
                        // still revisit those predicates even though the plan has already been consumed.
                        // Without this cache we'd panic or lose the knowledge that an outer table was required.
                        //
                        // Example: `SELECT t.a FROM t WHERE t.a = (SELECT MAX(x.a) FROM x WHERE x.b = t.b)`.
                        // The outer expression `x.b = t.b` is visited after the subquery is translated,
                        // so we need cached `outer_ref_ids` to realize that `t` must already be in scope.
                        for outer_ref_id in outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.set(table_idx)?;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(mask)
}

/// Determines the earliest loop where an expression can be safely evaluated.
///
/// When a referenced table is not found in `join_order`, we check if it's a hash-join
/// build table and map the condition to the probe loop where its rows are produced.
/// Subquery references are also respected, even after their plans are consumed.
pub fn determine_where_to_eval_expr(
    top_level_expr: &Expr,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    // If the expression references no tables, it can be evaluated before any table loops are opened.
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let Some(join_idx) = join_order.iter().position(|t| t.table_id == *table) else {
                    // Table not found in join_order. Check if it's a hash join build table.
                    // If so, we need to evaluate the condition at the probe table's loop position.
                    if let Some(tables) = table_references {
                        for (probe_idx, member) in join_order.iter().enumerate() {
                            let probe_table = &tables.joined_tables()[member.original_idx];
                            if let Operation::HashJoin(ref hj) = probe_table.op {
                                let build_table = &tables.joined_tables()[hj.build_table_idx];
                                if build_table.internal_id == *table {
                                    // This table is the build side of a hash join.
                                    // Evaluate the condition at the probe table's loop position.
                                    eval_at = eval_at.max(EvalAt::Loop(probe_idx));
                                    return Ok(WalkControl::Continue);
                                }
                            }
                        }
                    }
                    // Must be an outer query reference; in that case, the table is already in scope.
                    return Ok(WalkControl::Continue);
                };
                eval_at = eval_at.max(EvalAt::Loop(join_idx));
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Evaluated { evaluated_at, .. } => {
                        eval_at = eval_at.max(*evaluated_at);
                    }
                    SubqueryState::Unevaluated { plan } => {
                        let outer_ref_ids = plan.as_ref().unwrap().used_outer_query_ref_ids();
                        for outer_ref_id in &outer_ref_ids {
                            let join_idx = join_order
                                .iter()
                                .position(|t| t.table_id == *outer_ref_id)
                                .or_else(|| {
                                    let tables = table_references?;
                                    for (probe_idx, member) in join_order.iter().enumerate() {
                                        let probe_table =
                                            &tables.joined_tables()[member.original_idx];
                                        if let Operation::HashJoin(ref hj) = probe_table.op {
                                            let build_table =
                                                &tables.joined_tables()[hj.build_table_idx];
                                            if build_table.internal_id == *outer_ref_id {
                                                return Some(probe_idx);
                                            }
                                        }
                                    }
                                    None
                                });
                            if let Some(join_idx) = join_idx {
                                eval_at = eval_at.max(EvalAt::Loop(join_idx));
                            }
                        }
                        return Ok(WalkControl::Continue);
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(eval_at)
}

pub(crate) fn append_vtab_predicates_to_where_clause(
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    // The argument expressions were already resolved by the binder when the
    // TableCall arguments were bound.
    for expr in vtab_predicates.drain(..) {
        // Virtual table argument predicates (e.g. the 't2' in pragma_table_info('t2'))
        // must be associated with the virtual table's outer join context if the table is
        // the RHS of a LEFT JOIN. Otherwise the optimizer may incorrectly simplify the
        // LEFT JOIN into an INNER JOIN, breaking NULL row emission for unmatched rows.
        let from_outer_join = vtab_predicate_table_id(&expr).and_then(|table_id| {
            table_references
                .find_joined_table_by_internal_id(table_id)
                .and_then(|table_ref| {
                    table_ref
                        .join_info
                        .as_ref()
                        .and_then(|join_info| join_info.is_outer().then_some(table_id))
                })
        });
        out_where_clause.push(WhereTerm {
            expr,
            from_outer_join,
            consumed: false,
        });
    }
    Ok(())
}

/// Extract the table internal_id from a virtual table argument predicate.
/// These are always of the form `Column { table, .. } = literal` or `IsNull(Column { table, .. })`.
fn vtab_predicate_table_id(expr: &Expr) -> Option<TableInternalId> {
    match expr {
        Expr::Binary(lhs, _, _) | Expr::IsNull(lhs) => match lhs.as_ref() {
            Expr::Column { table, .. } => Some(*table),
            _ => None,
        },
        _ => None,
    }
}
pub fn break_predicate_at_and_boundaries<T: From<Expr>>(
    predicate: &Expr,
    out_predicates: &mut Vec<T>,
) {
    // Unwrap single-element parenthesized expressions recursively: ((expr)) -> expr.
    // This is semantically equivalent since single-element Parenthesized is purely
    // syntactic grouping. Multi-element Parenthesized (row values like (x, y)) are
    // left as-is by unwrap_parens.
    let predicate = unwrap_parens(predicate).unwrap_or(predicate);
    match predicate {
        Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(left, out_predicates);
            break_predicate_at_and_boundaries(right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate.clone().into());
        }
    }
}

pub fn parse_row_id<F>(
    column_name: &str,
    table_id: TableInternalId,
    fn_check: F,
) -> Result<Option<Expr>>
where
    F: FnOnce() -> bool,
{
    if ROWID_STRS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(column_name))
    {
        if fn_check() {
            crate::bail_parse_error!("ROWID is ambiguous");
        }

        return Ok(Some(Expr::RowId {
            database: None, // TODO: support different databases
            table: table_id,
        }));
    }
    Ok(None)
}

#[allow(clippy::type_complexity)]
#[turso_macros::trace_stack]
pub fn parse_limit(
    mut limit: Limit,
    resolver: &Resolver,
) -> Result<(Option<Box<Expr>>, Option<Box<Expr>>)> {
    bind_and_rewrite_expr(
        &mut limit.expr,
        None,
        None,
        resolver,
        BindingBehavior::TryResultColumnsFirst,
    )?;
    if let Some(ref mut off_expr) = limit.offset {
        bind_and_rewrite_expr(
            off_expr,
            None,
            None,
            resolver,
            BindingBehavior::TryResultColumnsFirst,
        )?;
    }
    Ok((Some(limit.expr), limit.offset))
}
