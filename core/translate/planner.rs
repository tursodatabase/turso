use super::{
    bind::CteEntry,
    expr::walk_expr,
    plan::{
        Aggregate, ColumnUsedMask, Distinctness, EvalAt, JoinOrderMember, JoinedTable, Operation,
        OuterQueryReference, Plan, QueryDestination, TableReferences, WhereTerm,
    },
    select::bind_prepare_select_plan,
};
use crate::function::{AggFunc, ExtFunc};
use crate::sync::Arc;
use crate::translate::{
    emitter::Resolver,
    expr::{expr_vector_size, unwrap_parens, WalkControl},
    plan::{NonFromClauseSubquery, SubqueryState},
};
use crate::turso_assert_less_than;
use crate::{
    function::Func,
    schema::Table,
    util::{exprs_are_equivalent, normalize_ident, validate_aggregate_function_tail},
    Result,
};
use crate::{
    translate::plan::{Window, WindowFunction},
    vdbe::builder::ProgramBuilder,
};
use rustc_hash::FxHashMap;
use std::cmp::PartialEq;
use turso_parser::ast::Literal::Null;
use turso_parser::ast::{self, Expr, Materialized, Over, Select, TableInternalId, With};

/// Collect all table names referenced in a SELECT's FROM clause.
/// Used to determine which earlier CTEs a CTE directly depends on.
pub fn collect_from_clause_table_refs(select: &Select, out: &mut Vec<String>) {
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

fn collect_from_select_table(table: &ast::SelectTable, out: &mut Vec<String>) {
    match table {
        ast::SelectTable::Table(qualified_name, _, _)
        | ast::SelectTable::TableCall(qualified_name, _, _) => {
            out.push(normalize_ident(qualified_name.name.as_str()));
        }
        ast::SelectTable::Select(subselect, _) => {
            collect_from_clause_table_refs(subselect, out);
        }
        ast::SelectTable::Sub(from_clause, _) => {
            collect_from_select_table(&from_clause.select, out);
            for join in &from_clause.joins {
                collect_from_select_table(&join.table, out);
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
            } => {
                validate_aggregate_function_tail(filter_over, order_by)?;
                let args_count = args.len();
                let distinctness = Distinctness::from_ast(distinctness.as_ref());

                match Func::resolve_function(name.as_str(), args_count) {
                    Ok(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                expr,
                                f,
                                over_clause,
                                distinctness,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(aggs, expr, args, distinctness, f)?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Err(e) => {
                        if let Some(f) = resolver
                            .symbol_table
                            .resolve_function(name.as_str(), args_count)
                        {
                            let func = AggFunc::External(f.func.clone().into());
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                if let Some(over_clause) = filter_over.over_clause.as_ref() {
                                    link_with_window(
                                        windows.as_deref_mut(),
                                        expr,
                                        func,
                                        over_clause,
                                        distinctness,
                                    )?;
                                } else {
                                    add_aggregate_if_not_exists(
                                        aggs,
                                        expr,
                                        args,
                                        distinctness,
                                        func,
                                    )?;
                                    contains_aggregates = true;
                                }
                                return Ok(WalkControl::SkipChildren);
                            }
                        } else {
                            return Err(e);
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
                validate_aggregate_function_tail(filter_over, &[])?;
                match Func::resolve_function(name.as_str(), 0) {
                    Ok(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                expr,
                                f,
                                over_clause,
                                Distinctness::NonDistinct,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(
                                aggs,
                                expr,
                                &[],
                                Distinctness::NonDistinct,
                                f,
                            )?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Ok(_) => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }

                        // Check if the function supports (*) syntax using centralized logic
                        match crate::function::Func::resolve_function(name.as_str(), 0) {
                            Ok(func) => {
                                if func.supports_star_syntax() {
                                    return Ok(WalkControl::Continue);
                                } else {
                                    crate::bail_parse_error!(
                                        "wrong number of arguments to function {}()",
                                        name.as_str()
                                    );
                                }
                            }
                            Err(_) => {
                                crate::bail_parse_error!(
                                    "wrong number of arguments to function {}()",
                                    name.as_str()
                                );
                            }
                        }
                    }
                    Err(e) => match e {
                        crate::LimboError::ParseError(e) => {
                            crate::bail_parse_error!("{}", e);
                        }
                        _ => {
                            crate::bail_parse_error!(
                                "Invalid aggregate function: {}",
                                name.as_str()
                            );
                        }
                    },
                }
            }
            _ => {}
        }

        Ok(WalkControl::Continue)
    })?;

    Ok(contains_aggregates)
}

fn link_with_window(
    windows: Option<&mut Vec<Window>>,
    expr: &Expr,
    func: AggFunc,
    over_clause: &Over,
    distinctness: Distinctness,
) -> Result<()> {
    if distinctness.is_distinct() {
        crate::bail_parse_error!("DISTINCT is not supported for window functions");
    }
    expr_vector_size(expr)?;
    if let Some(windows) = windows {
        let window = resolve_window(windows, over_clause)?;
        window.functions.push(WindowFunction {
            func,
            original_expr: expr.clone(),
        });
    } else {
        crate::bail_parse_error!("misuse of window function: {}()", func.as_str());
    }
    Ok(())
}

fn resolve_window<'a>(windows: &'a mut Vec<Window>, over_clause: &Over) -> Result<&'a mut Window> {
    match over_clause {
        Over::Window(window) => {
            if let Some(idx) = windows.iter().position(|w| w.is_equivalent(window)) {
                return Ok(&mut windows[idx]);
            }

            windows.push(Window::new(None, window)?);
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
        Over::Name(name) => {
            let window_name = normalize_ident(name.as_str());
            // When multiple windows share the same name, SQLite uses the most recent
            // definition. Iterate in reverse so we find the last definition first.
            for window in windows.iter_mut().rev() {
                if window.name.as_ref() == Some(&window_name) {
                    return Ok(window);
                }
            }
            crate::bail_parse_error!("no such window: {}", window_name);
        }
    }
}

fn add_aggregate_if_not_exists(
    aggs: &mut Vec<Aggregate>,
    expr: &Expr,
    args: &[Box<Expr>],
    distinctness: Distinctness,
    func: AggFunc,
) -> Result<()> {
    if distinctness.is_distinct() && args.len() != 1 {
        crate::bail_parse_error!("DISTINCT aggregate functions must have exactly one argument");
    }
    if aggs
        .iter()
        .all(|a| !exprs_are_equivalent(&a.original_expr, expr))
    {
        aggs.push(Aggregate::new(func, args, expr, distinctness));
    }
    Ok(())
}

/// Plan CTEs from binder-provided definitions.
///
/// Uses the same recursive approach as `plan_cte` to avoid exponential re-planning:
/// only directly referenced CTEs are planned as dependencies.
///
/// Returns a map of CTE name → planned `JoinedTable` for use in
/// `BoundSelect::into_table_references`.
pub fn plan_bound_ctes(
    mut cte_definitions: Vec<(String, CteEntry)>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<FxHashMap<String, JoinedTable>> {
    let mut planned: FxHashMap<String, JoinedTable> = FxHashMap::default();
    for idx in 0..cte_definitions.len() {
        if !planned.contains_key(&cte_definitions[idx].0) {
            // count_reference = false: validation of explicit column count is
            // deferred until the CTE is actually referenced (matching SQLite
            // behavior where unreferenced CTEs with mismatched column counts
            // don't error).
            plan_one_bound_cte(
                idx,
                &mut cte_definitions,
                resolver,
                program,
                connection,
                false,
                &mut planned,
            )?;
        }
    }
    Ok(planned)
}

/// Plan derived tables (FROM-clause subqueries) from binder-provided bindings.
///
/// Each derived table's inner select is already bound. This function plans them
/// and returns a map of `internal_id` → `JoinedTable` for use in
/// `BoundSelect::into_table_references`.
pub fn plan_derived_tables(
    derived_bindings: FxHashMap<TableInternalId, super::bind::BoundSubquery>,
    planned_ctes: &mut FxHashMap<String, JoinedTable>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<FxHashMap<TableInternalId, JoinedTable>> {
    plan_derived_tables_with_outer_refs(
        derived_bindings,
        planned_ctes,
        resolver,
        program,
        connection,
        Vec::new(),
    )
}

pub fn plan_derived_tables_with_outer_refs(
    derived_bindings: FxHashMap<TableInternalId, super::bind::BoundSubquery>,
    planned_ctes: &mut FxHashMap<String, JoinedTable>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    outer_query_refs: Vec<super::plan::OuterQueryReference>,
) -> Result<FxHashMap<TableInternalId, JoinedTable>> {
    let mut planned: FxHashMap<TableInternalId, JoinedTable> = FxHashMap::default();

    for (internal_id, bound_sq) in derived_bindings {
        let mut inner_bound = bound_sq.inner_bound;

        // Extract nested CTE definitions and subquery bindings.
        let inner_cte_defs = std::mem::take(&mut inner_bound.cte_definitions);
        let inner_subquery_bindings = std::mem::take(&mut inner_bound.subquery_bindings);
        let inner_derived_bindings = std::mem::take(&mut inner_bound.derived_bindings);

        // Plan any inner CTEs.
        let mut inner_planned_ctes =
            plan_bound_ctes(inner_cte_defs, resolver, program, connection)?;

        // Make parent CTEs available for inner references.
        for (name, jt) in planned_ctes.iter() {
            if !inner_planned_ctes.contains_key(name) {
                inner_planned_ctes.insert(name.clone(), jt.clone());
            }
        }

        // Plan any nested derived tables.
        let mut inner_planned_derived = plan_derived_tables_with_outer_refs(
            inner_derived_bindings,
            &mut inner_planned_ctes,
            resolver,
            program,
            connection,
            outer_query_refs.clone(),
        )?;

        let all_table_refs = inner_bound.into_table_references_with_outer_refs(
            &mut inner_planned_ctes,
            &mut inner_planned_derived,
            outer_query_refs.clone(),
        )?;

        let subplan = super::select::prepare_select_plan(
            bound_sq.select,
            resolver,
            program,
            QueryDestination::placeholder_for_subquery(),
            connection,
            all_table_refs.into_iter(),
            inner_subquery_bindings,
        )?;

        match &subplan {
            Plan::Select(_) | Plan::CompoundSelect { .. } => {}
            Plan::Delete(_) | Plan::Update(_) => {
                crate::bail_parse_error!(
                    "DELETE/UPDATE queries are not supported in FROM clause subqueries"
                );
            }
        }

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

/// Plan a single CTE using its pre-bound data from the binder.
///
/// The binder already resolved all names and column references in the CTE body.
/// This function takes the pre-bound `inner_bound` and converts it into a plan
/// without re-binding. Referenced sibling CTEs are planned recursively first
/// (with `count_reference = false`) to avoid exponential blowup.
fn plan_one_bound_cte(
    cte_idx: usize,
    cte_definitions: &mut [(String, CteEntry)],
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    count_reference: bool,
    planned: &mut FxHashMap<String, JoinedTable>,
) -> Result<JoinedTable> {
    // Copy metadata needed before mutably borrowing the entry.
    let name = cte_definitions[cte_idx].0.clone();
    let referenced_indices = cte_definitions[cte_idx].1.referenced_cte_indices.clone();

    // Recursively plan referenced sibling CTEs first.
    for &ref_idx in &referenced_indices {
        if !planned.contains_key(&cte_definitions[ref_idx].0) {
            plan_one_bound_cte(
                ref_idx,
                cte_definitions,
                resolver,
                program,
                connection,
                false,
                planned,
            )?;
        }
    }

    let entry = &mut cte_definitions[cte_idx].1;

    // Take the pre-bound data produced by the binder.
    let mut inner_bound = entry
        .inner_bound
        .take()
        .expect("CTE inner binding should be present");
    let cte_select = entry.select.clone();

    // Extract nested CTE definitions, subquery bindings, and derived bindings.
    let inner_cte_defs = std::mem::take(&mut inner_bound.cte_definitions);
    let inner_subquery_bindings = std::mem::take(&mut inner_bound.subquery_bindings);
    let inner_derived_bindings = std::mem::take(&mut inner_bound.derived_bindings);

    // Block circular references during planning.
    program.push_cte_being_defined(name.clone());

    // Plan any nested CTEs from inner WITH clauses.
    let mut inner_planned = plan_bound_ctes(inner_cte_defs, resolver, program, connection)?;

    // Make all already-planned sibling CTEs available. CTEs can be
    // referenced not only from the FROM clause (tracked by referenced_indices)
    // but also from correlated subqueries within the CTE body.
    for (ref_name, ref_table) in planned.iter() {
        if !inner_planned.contains_key(ref_name) {
            inner_planned.insert(ref_name.clone(), ref_table.clone());
        }
    }

    // Plan any derived tables (FROM subqueries).
    let mut inner_planned_derived = plan_derived_tables(
        inner_derived_bindings,
        &mut inner_planned,
        resolver,
        program,
        connection,
    )?;

    // Count CTE references in scope tables for materialization decisions.
    for scope_table in inner_bound.main_scope.tables.iter().chain(
        inner_bound
            .compound_scopes
            .iter()
            .flat_map(|s| s.tables.iter()),
    ) {
        if let super::bind::ScopeTableSource::Cte { cte_id, .. } = &scope_table.source {
            program.increment_cte_reference(*cte_id);
        }
    }

    let mut all_table_refs =
        inner_bound.into_table_references(&mut inner_planned, &mut inner_planned_derived)?;

    // Add sibling CTEs as outer query refs so correlated subqueries within
    // this CTE body can reference them (e.g. previous_points referencing
    // ordered_points via a scalar subquery).
    for tr in &mut all_table_refs {
        for (ref_name, ref_table) in planned.iter() {
            if !tr
                .outer_query_refs()
                .iter()
                .any(|r| r.identifier == *ref_name)
            {
                tr.add_outer_query_reference(OuterQueryReference {
                    identifier: ref_name.clone(),
                    internal_id: ref_table.internal_id,
                    table: ref_table.table.clone(),
                    col_used_mask: ColumnUsedMask::default(),
                    cte_select: None,
                    cte_explicit_columns: vec![],
                    cte_id: None,
                    cte_definition_only: true,
                    rowid_referenced: false,
                });
            }
        }
    }

    let cte_plan = super::select::prepare_select_plan(
        cte_select,
        resolver,
        program,
        QueryDestination::placeholder_for_subquery(),
        connection,
        all_table_refs.into_iter(),
        inner_subquery_bindings,
    );
    program.pop_cte_being_defined();
    let cte_plan = cte_plan?;

    let entry = &cte_definitions[cte_idx].1;
    let explicit_cols = if entry.explicit_columns.is_empty() {
        None
    } else {
        Some(entry.explicit_columns.as_slice())
    };

    if count_reference {
        program.increment_cte_reference(entry.cte_id);

        if let Some(cols) = explicit_cols {
            let result_col_count = cte_plan
                .select_result_columns()
                .expect("should be a select plan")
                .len();
            if cols.len() != result_col_count {
                crate::bail_parse_error!(
                    "table {} has {} columns but {} column names were provided",
                    name,
                    result_col_count,
                    cols.len()
                );
            }
        }
    }

    let cte_table = match cte_plan {
        Plan::Select(_) | Plan::CompoundSelect { .. } => JoinedTable::new_subquery_from_plan(
            name.clone(),
            cte_plan,
            None,
            program.table_reference_counter.next(),
            explicit_cols,
            Some(entry.cte_id),
            entry.materialize_hint,
        )?,
        Plan::Delete(_) | Plan::Update(_) => {
            crate::bail_parse_error!("DELETE/UPDATE queries are not supported in CTEs")
        }
    };

    planned.insert(name, cte_table.clone());
    Ok(cte_table)
}

/// Plan CTEs from a WITH clause and add them as outer query references.
/// This is used by DML statements (DELETE, UPDATE) to make CTEs available
/// for subqueries in WHERE and SET clauses.
pub fn plan_ctes_as_outer_refs(
    with: Option<With>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let Some(with) = with else {
        return Ok(());
    };

    if with.recursive {
        crate::bail_parse_error!("Recursive CTEs are not yet supported");
    }

    for cte in with.ctes {
        // Normalize explicit column names
        let explicit_columns: Vec<String> = cte
            .columns
            .iter()
            .map(|c| normalize_ident(c.col_name.as_str()))
            .collect();

        let cte_name = normalize_ident(cte.tbl_name.as_str());

        // Check for duplicate CTE names
        if table_references
            .outer_query_refs()
            .iter()
            .any(|r| r.identifier == cte_name)
        {
            crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
        }

        // Clone the CTE select AST before planning, so we can store it for re-planning
        let cte_select_ast = cte.select.clone();
        // AS MATERIALIZED forces materialization
        let materialize_hint = cte.materialized == Materialized::Yes;

        // Block the CTE's own name from resolving to a schema object during
        // planning of its body (see push_cte_being_defined).
        program.push_cte_being_defined(cte_name.clone());

        // Plan the CTE SELECT
        let cte_plan = bind_prepare_select_plan(
            cte.select,
            resolver,
            program,
            table_references.outer_query_refs(),
            QueryDestination::placeholder_for_subquery(),
            connection,
        );
        program.pop_cte_being_defined();
        let cte_plan = cte_plan?;

        // Convert plan to JoinedTable to extract column info
        let explicit_cols = if explicit_columns.is_empty() {
            None
        } else {
            Some(explicit_columns.as_slice())
        };
        let joined_table = match cte_plan {
            Plan::Select(_) | Plan::CompoundSelect { .. } => JoinedTable::new_subquery_from_plan(
                cte_name.clone(),
                cte_plan,
                None,
                program.table_reference_counter.next(),
                explicit_cols,
                None, // CTEs in DML don't share materialized data (TODO: implement if needed)
                materialize_hint,
            )?,
            Plan::Delete(_) | Plan::Update(_) => {
                crate::bail_parse_error!("Only SELECT queries are supported in CTEs")
            }
        };

        // Add CTE as outer query reference so it's available to subqueries.
        // cte_definition_only = true: the CTE is only for subquery FROM lookup
        // (e.g. UPDATE t SET b = (SELECT v FROM c)), not for direct column
        // resolution (e.g. UPDATE t SET b = c.v which SQLite rejects as
        // "no such column").
        table_references.add_outer_query_reference(OuterQueryReference {
            identifier: cte_name,
            internal_id: joined_table.internal_id,
            table: joined_table.table,
            col_used_mask: ColumnUsedMask::default(),
            cte_select: Some(cte_select_ast),
            cte_explicit_columns: explicit_columns,
            cte_id: None, // DML CTEs don't track CTE sharing (TODO: implement if needed)
            cte_definition_only: true,
            rowid_referenced: false,
        });
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

/// Walk the FROM clause AST and generate virtual-table argument predicates.
///
/// Called after binding — `TableReferences` already contains the resolved
/// `JoinedTable`s. For each `TableCall` node in the AST we find the matching
/// joined table by identifier and call `transform_args_into_where_terms`,
/// which is the same transform that `parse_table` used to do inline.
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
            .map(|a| match a {
                ast::As::As(id) | ast::As::Elided(id) => normalize_ident(id.as_str()),
            })
            .unwrap_or_else(|| table_name.clone());

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

pub fn parse_where(
    where_clause: Option<&Expr>,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        let start_idx = out_where_clause.len();
        break_predicate_at_and_boundaries(where_expr, out_where_clause);
        // BETWEEN is rewritten to (lhs >= start) AND (lhs <= end) by the binder.
        // Re-break any ANDs that were created so they become separate WhereTerms for
        // constraint extraction.
        let mut i = start_idx;
        while i < out_where_clause.len() {
            if matches!(
                &out_where_clause[i].expr,
                Expr::Binary(_, ast::Operator::And, _)
            ) {
                let term = out_where_clause.remove(i);
                let mut new_terms: Vec<WhereTerm> = Vec::new();
                break_predicate_at_and_boundaries(&term.expr, &mut new_terms);
                // Preserve from_outer_join from the original term
                for new_term in new_terms.iter_mut() {
                    new_term.from_outer_join = term.from_outer_join;
                }
                let count = new_terms.len();
                for (j, new_term) in new_terms.into_iter().enumerate() {
                    out_where_clause.insert(i + j, new_term);
                }
                i += count;
            } else {
                i += 1;
            }
        }
        Ok(())
    } else {
        Ok(())
    }
}

/// Fold JOIN ON/USING constraints into WhereTerms.
///
/// Called after binding — table resolution is already done, ON expressions are
/// already bound to `Expr::Column`. This function:
/// - Breaks ON expressions at AND boundaries and tags them with `from_outer_join`
/// - Synthesizes equality predicates for USING columns
/// - NATURAL is already transformed to USING by the binder
pub fn fold_join_constraints(
    from: &ast::FromClause,
    table_references: &TableReferences,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    for (join_idx, join) in from.joins.iter().enumerate() {
        // The first table is from.select (index 0 in joined_tables),
        // joins start at index 1.
        let table_idx = join_idx + 1;
        // For right_join_swapped, the binder swapped table positions so
        // index 0 is the originally-right table (no join_info) and index 1
        // is the originally-left table (with LeftOuter join_info).
        // The ON/USING constraint should be tagged with the outer table's id.
        let actual_table_idx = if table_references.right_join_swapped() && table_idx == 1 {
            // After swap: the originally-left table (now at idx 0) is the outer one
            0
        } else {
            table_idx
        };

        let outer = table_references.joined_tables()[actual_table_idx]
            .join_info
            .as_ref()
            .is_some_and(|j| j.is_outer());
        let outer_table_id = table_references.joined_tables()[actual_table_idx].internal_id;

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
                // NATURAL joins have already been transformed to USING by the binder.
                // Column usage is already tracked by the binder.
                let right_table_idx = if table_references.right_join_swapped() && table_idx == 1 {
                    0
                } else {
                    table_idx
                };
                let left_range_end = right_table_idx;
                let tables = table_references.joined_tables();
                let left_tables = &tables[..left_range_end];
                let right_table = &tables[right_table_idx];

                for col_name in cols.iter() {
                    let name_normalized = normalize_ident(col_name.as_str());

                    // Find column in left tables
                    let mut left_col = None;
                    for left_table in left_tables.iter() {
                        left_col = left_table
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(_, col)| !col.hidden())
                            .find(|(_, col)| {
                                col.name
                                    .as_ref()
                                    .is_some_and(|name| *name == name_normalized)
                            })
                            .map(|(idx, col)| (left_table.internal_id, idx, col.is_rowid_alias()));
                        if left_col.is_some() {
                            break;
                        }
                    }
                    let Some((left_table_id, left_col_idx, left_is_rowid_alias)) = left_col else {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            col_name.as_str()
                        );
                    };

                    // Find column in right table
                    let right_col = right_table.columns().iter().enumerate().find(|(_, col)| {
                        col.name
                            .as_ref()
                            .is_some_and(|name| *name == name_normalized)
                    });
                    let Some((right_col_idx, right_col)) = right_col else {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            col_name.as_str()
                        );
                    };

                    out_where_clause.push(WhereTerm {
                        expr: Expr::Binary(
                            Box::new(Expr::Column {
                                database: None,
                                table: left_table_id,
                                column: left_col_idx,
                                is_rowid_alias: left_is_rowid_alias,
                            }),
                            ast::Operator::Equals,
                            Box::new(Expr::Column {
                                database: None,
                                table: right_table.internal_id,
                                column: right_col_idx,
                                is_rowid_alias: right_col.is_rowid_alias(),
                            }),
                        ),
                        from_outer_join: if outer { Some(outer_table_id) } else { None },
                        consumed: false,
                    });
                }
            }
            None => {}
        }
    }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableMask(pub u128);

impl std::ops::BitOrAssign for TableMask {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl TableMask {
    /// Creates a new empty table mask.
    ///
    /// The initial mask represents an empty set of tables.
    pub fn new() -> Self {
        Self(0)
    }

    /// Returns true if the mask represents an empty set of tables.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Creates a new mask that is the same as this one but without the specified table.
    pub fn without_table(&self, table_no: usize) -> Self {
        turso_assert_less_than!(table_no, 127, "table_no must be less than 127");
        Self(self.0 ^ (1 << (table_no + 1)))
    }

    /// Creates a table mask from raw bits.
    ///
    /// The bits are shifted left by 1 to maintain the convention that table 0 is at bit 1.
    pub fn from_bits(bits: u128) -> Self {
        Self(bits << 1)
    }

    /// Creates a table mask from an iterator of table numbers.
    pub fn from_table_number_iter(iter: impl Iterator<Item = usize>) -> Self {
        iter.fold(Self::new(), |mut mask, table_no| {
            turso_assert_less_than!(table_no, 127, "table_no must be less than 127");
            mask.add_table(table_no);
            mask
        })
    }

    /// Adds a table to the mask.
    pub fn add_table(&mut self, table_no: usize) {
        turso_assert_less_than!(table_no, 127, "table_no must be less than 127");
        self.0 |= 1 << (table_no + 1);
    }

    /// Returns true if the mask contains the specified table.
    pub fn contains_table(&self, table_no: usize) -> bool {
        turso_assert_less_than!(table_no, 127, "table_no must be less than 127");
        self.0 & (1 << (table_no + 1)) != 0
    }

    /// Returns true if this mask contains all tables in the other mask.
    pub fn contains_all(&self, other: &TableMask) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the number of tables in the mask.
    pub fn table_count(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns true if this mask shares any tables with the other mask.
    pub fn intersects(&self, other: &TableMask) -> bool {
        self.0 & other.0 != 0
    }

    /// Iterate the table indices present in this mask.
    pub fn tables_iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..127).filter(move |table_no| self.contains_table(*table_no))
    }
}

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
    let mut mask = TableMask::new();
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(table_idx) = table_references
                    .joined_tables()
                    .iter()
                    .position(|t| t.internal_id == *table)
                {
                    mask.add_table(table_idx);
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
                        let used_outer_query_refs = plan
                            .as_ref()
                            .unwrap()
                            .table_references
                            .outer_query_refs()
                            .iter()
                            .filter(|t| t.is_used());
                        for outer_query_ref in used_outer_query_refs {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == outer_query_ref.internal_id)
                            {
                                mask.add_table(table_idx);
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
                                mask.add_table(table_idx);
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
                        let used_outer_refs = plan
                            .as_ref()
                            .unwrap()
                            .table_references
                            .outer_query_refs()
                            .iter()
                            .filter(|t| t.is_used());
                        for outer_ref in used_outer_refs {
                            let join_idx = join_order
                                .iter()
                                .position(|t| t.table_id == outer_ref.internal_id)
                                .or_else(|| {
                                    let tables = table_references?;
                                    for (probe_idx, member) in join_order.iter().enumerate() {
                                        let probe_table =
                                            &tables.joined_tables()[member.original_idx];
                                        if let Operation::HashJoin(ref hj) = probe_table.op {
                                            let build_table =
                                                &tables.joined_tables()[hj.build_table_idx];
                                            if build_table.internal_id == outer_ref.internal_id {
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
