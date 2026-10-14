use crate::alloc::TursoSliceExt;
use crate::function::{AccumulatorFunc, AggFunc, WindowFunc};
use crate::schema::{BTreeCharacteristics, BTreeTable, Index, IndexColumn, Table};
use crate::sync::Arc;
use crate::translate::aggregation::{
    agg_arg_collation, translate_prepared_aggregation_step, PreparedAggregateArguments,
};
use crate::translate::collate::{get_collseq_from_expr, CollationSeq};
use crate::translate::emitter::{Resolver, TranslateCtx};
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, translate_expr, translate_expr_no_constant_opt,
    walk_expr, walk_expr_mut, NoConstantOptReason, WalkControl,
};
use crate::translate::order_by::{custom_type_comparator, EmitOrderBy};
use crate::translate::plan::{
    Aggregate, Distinctness, JoinOrderMember, JoinedTable, QueryDestination, ResultSetColumn,
    RewrittenWindowCall, SelectPlan, TableReferences, Window, WindowFunction,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::result_row::emit_select_result;
use crate::translate::subquery::plan_subqueries_from_select_plan;
use crate::types::KeyInfo;
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, AggStepData, {IdxInsertFlags, InsertFlags, Insn},
};
use crate::vdbe::{BranchOffset, CursorID};
use crate::Connection;
use crate::Result;
use crate::{turso_assert, turso_assert_eq};
use std::mem;
use turso_parser::ast::Name;
use turso_parser::ast::{Expr, FrameMode, Literal, Over, SortOrder, TableInternalId};

const SUBQUERY_DATABASE_ID: usize = 0;

struct WindowSubqueryContext<'a> {
    resolver: &'a Resolver<'a>,
    subquery_order_by: &'a mut Vec<(Box<Expr>, SortOrder, Option<turso_parser::ast::NullsOrder>)>,
    subquery_result_columns: &'a mut Vec<ResultSetColumn>,
    subquery_id: &'a TableInternalId,
}

/// Rewrite a `SELECT` plan for window function processing.
///
/// A `SELECT` may reference multiple window definitions, but internally, each `SELECT` plan
/// operates on **exactly one** window. Multiple window functions may reference the same window.
///
/// The original plan is rewritten into a series of nested subqueries, each  bound to a single
/// window definition. Each subquery produces rows in the order determined by its parent window
/// definition. The innermost subquery does not have any window assigned to it; instead,
/// the FROM, WHERE, GROUP BY, and HAVING clauses from the original query are pushed down to it.
/// The outermost query retains ORDER BY, LIMIT, and OFFSET.
///
/// # Examples
/// ```sql
/// -- Example 1: Query with one window
/// SELECT
///     a+1,
///     max(b) OVER (PARTITION BY c ORDER BY d),
///     min(c) OVER (PARTITION BY c ORDER BY d)
/// FROM t1
/// ORDER BY e;
///
/// -- Rewritten form
/// SELECT
///     a+1,
///     max(b) OVER (PARTITION BY c ORDER BY d),
///     min(c) OVER (PARTITION BY c ORDER BY d)
/// FROM (SELECT a, b, c, d, e FROM t1 ORDER BY c, d)
/// ORDER BY e;
///
/// -- Example 2: Query with multiple windows
/// SELECT
///     a,
///     max(b) OVER (PARTITION BY c ORDER BY d),
///     min(c) OVER (PARTITION BY e ORDER BY f)
/// FROM t1;
///
/// -- Rewritten form
/// SELECT
///     a,
///     max(b) OVER (PARTITION BY c ORDER BY d) AS w1,
///     w2
/// FROM (
///     SELECT
///         a,
///         b,
///         c,
///         d,
///         min(c) OVER (PARTITION BY e ORDER BY f) AS w2
///     FROM (SELECT a, b, c, d, e, f FROM t1 ORDER BY e, f)
///     ORDER BY c, d
/// );
/// ```
#[turso_macros::trace_stack]
pub fn plan_windows(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    windows: &mut Vec<Window>,
) -> crate::Result<()> {
    // Remove named windows that are not referenced by any function, as they can be ignored.
    windows.retain(|w| !w.functions.is_empty());

    if !windows.is_empty() {
        // Sanity check: this should never happen because the syntax disallows combining VALUES with windows
        turso_assert!(
            plan.values.is_empty(),
            "VALUES clause with windows is not supported"
        );
    }

    prepare_window_subquery(program, plan, resolver, connection, windows, 0)
}

fn prepare_window_subquery(
    program: &mut ProgramBuilder,
    outer_plan: &mut SelectPlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    windows: &mut Vec<Window>,
    processed_window_count: usize,
) -> crate::Result<()> {
    if windows.is_empty() {
        // The innermost plan holds the original FROM/WHERE/GROUP BY plus any
        // raw subquery expressions pushed down from outer window layers.
        // Plan them now so they become SubqueryResult nodes with entries in
        // non_from_clause_subqueries.
        plan_subqueries_from_select_plan(program, outer_plan, resolver, connection)?;
        return Ok(());
    }

    // Layer windows in their declaration order: the first-declared window
    // becomes the outermost subquery (its PARTITION/ORDER drives the
    // user-visible row order), and later-declared windows nest deeper. Each
    // window function evaluates against rows in its own layer's order, so
    // the relative position of unordered windows like `OVER ()` matters.
    let mut current_window = windows.remove(0);
    let mut subquery_result_columns = Vec::new();
    let mut subquery_order_by = Vec::new();
    let subquery_id = program.table_reference_counter.next();

    if current_window.name.is_none() {
        // This is part of normalizing the window definition. The remaining logic lives in
        // `rewrite_expr_referencing_current_window`, which replaces inline window definitions
        // with references by name.
        //
        // The goal is to always work with named windows instead of a mix of named and
        // inline ones. This way, we don’t need to rewrite expressions embedded in inline
        // definitions (there might be many equivalent definitions per subquery). Instead,
        // we rewrite the named definition once, and all associated window functions
        // require no additional processing.
        //
        // At this stage, window definitions and window functions are already bound,
        // so this normalization is purely to keep the plan valid.
        //
        // If the generated name is not unique across the entire query, that’s acceptable —
        // the final plan always associates exactly one window with one subquery.
        current_window.name = Some(format!("window_{processed_window_count}"));
    }

    let mut ctx = WindowSubqueryContext {
        resolver,
        subquery_order_by: &mut subquery_order_by,
        subquery_result_columns: &mut subquery_result_columns,
        subquery_id: &subquery_id,
    };

    // Build the ORDER BY clause for the subquery by concatenating the window’s PARTITION BY
    // columns with its ORDER BY columns.This ensures that rows in the subquery are returned
    // in the correct order for partitioning and window function evaluation.
    for expr in current_window.partition_by.iter_mut() {
        append_order_by(outer_plan, expr, &SortOrder::Asc, None, &mut ctx)?;
        current_window.deduplicated_partition_by_len = Some(ctx.subquery_result_columns.len())
    }
    for (expr, order, nulls) in current_window.order_by.iter_mut() {
        append_order_by(outer_plan, expr, order, *nulls, &mut ctx)?;
    }

    // Rewrite expressions from the outer query’s result columns and ORDER BY clause so that
    // they reference the subquery instead. The original expressions are included in the
    // subquery’s result columns.
    for col in outer_plan.result_columns.iter_mut() {
        rewrite_terminal_expr(
            &mut outer_plan.aggregates,
            &mut col.expr,
            &mut current_window,
            &mut ctx,
        )?;
    }
    for (expr, _, _) in outer_plan.order_by.iter_mut() {
        rewrite_terminal_expr(
            &mut outer_plan.aggregates,
            expr,
            &mut current_window,
            &mut ctx,
        )?;
    }

    // When there is no ORDER BY or PARTITION BY clause, the window function takes zero arguments,
    // and no other columns are selected (e.g., "SELECT count() OVER () FROM products"),
    // `subquery_result_columns` may be empty. Add a constant expression to keep the query valid.
    if subquery_result_columns.is_empty() {
        subquery_result_columns.push(ResultSetColumn {
            expr: Expr::Literal(Literal::Numeric("0".to_string())),
            alias: None,
            implicit_column_name: None,
            contains_aggregates: false,
        });
    }

    let new_join_order = vec![JoinOrderMember {
        table_id: subquery_id,
        original_idx: 0,
        is_outer: false,
    }];
    let new_table_references = TableReferences::new(
        vec![],
        outer_plan.table_references.outer_query_refs().to_vec(),
    );

    let mut inner_plan = SelectPlan {
        join_order: mem::replace(&mut outer_plan.join_order, new_join_order),
        table_references: mem::replace(&mut outer_plan.table_references, new_table_references),
        result_columns: subquery_result_columns,
        where_clause: mem::take(&mut outer_plan.where_clause),
        group_by: mem::take(&mut outer_plan.group_by),
        order_by: subquery_order_by,
        aggregates: mem::take(&mut outer_plan.aggregates),
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination: QueryDestination::placeholder_for_subquery(),
        distinctness: Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: vec![],
        input_cardinality_hint: None,
        estimated_output_rows: None,
        estimated_cost: None,
        simple_aggregate: None,
        phantom_params: vec![],
    };

    prepare_window_subquery(
        program,
        &mut inner_plan,
        resolver,
        connection,
        windows,
        processed_window_count + 1,
    )?;

    let subquery = JoinedTable::new_subquery(
        format!("window_subquery_{processed_window_count}"),
        inner_plan,
        None,
        subquery_id,
    )?;

    // Verify that the subquery has the expected database ID.
    // This is required to ensure that assumptions in `rewrite_terminal_expr` are valid.
    turso_assert_eq!(
        subquery.database_id,
        SUBQUERY_DATABASE_ID,
        "subquery database id must be SUBQUERY_DATABASE_ID",
        {"SUBQUERY_DATABASE_ID": SUBQUERY_DATABASE_ID}
    );

    outer_plan.window = Some(current_window);
    outer_plan.table_references.add_joined_table(subquery);

    Ok(())
}

fn append_order_by(
    plan: &mut SelectPlan,
    expr: &mut Expr,
    sort_order: &SortOrder,
    nulls_order: Option<turso_parser::ast::NullsOrder>,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<()> {
    // Deduplicate: if an equivalent expression already exists in the subquery ORDER BY,
    // skip adding it again. This can happen when the same column appears in both
    // PARTITION BY and ORDER BY (e.g. OVER (PARTITION BY a ORDER BY a)), and prevents
    // the optimizer assertion group_by.exprs.len() >= order_by.len() from being violated.
    let already_exists = ctx
        .subquery_order_by
        .iter()
        .any(|(existing, _, _)| exprs_are_equivalent(existing, expr));
    if !already_exists {
        ctx.subquery_order_by
            .push((Box::new(expr.clone()), *sort_order, nulls_order));
    }

    let contains_aggregates = resolve_window_and_aggregate_functions(
        expr,
        ctx.resolver,
        &mut plan.aggregates,
        None,
        &mut [],
    )?;
    rewrite_expr_as_subquery_column(expr, ctx, contains_aggregates);
    Ok(())
}

fn rewrite_terminal_expr(
    aggregates: &mut Vec<Aggregate>,
    top_level_expr: &mut Expr,
    current_window: &mut Window,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<WalkControl> {
    walk_expr_mut(
        top_level_expr,
        &mut |expr: &mut Expr| -> crate::Result<WalkControl> {
            match expr {
                Expr::FunctionCall { filter_over, .. }
                | Expr::FunctionCallStar { filter_over, .. } => {
                    if filter_over.over_clause.is_none() {
                        // If the expression is a standard aggregate (non-window), push it down
                        // to the subquery.
                        if aggregates
                            .iter()
                            .any(|a| exprs_are_equivalent(&a.original_expr, expr))
                        {
                            rewrite_expr_as_subquery_column(expr, ctx, true);
                        }
                    } else if let Some(window_function) =
                        find_window_function_entry(&mut current_window.functions, expr)
                    {
                        // Window function tied to the current window: rewrite its
                        // children to reference the subquery, not the call itself.
                        if let Some(rewritten) = &window_function.rewritten {
                            *expr = rewritten.expr.clone();
                        } else {
                            let window_name = current_window
                                .name
                                .clone()
                                .expect("current_window must always have a name here");
                            let func = window_function.func.clone();
                            window_function.rewritten =
                                Some(rewrite_expr_referencing_current_window(
                                    aggregates,
                                    window_name,
                                    ctx,
                                    expr,
                                    &func,
                                )?);
                        }
                        return Ok(WalkControl::SkipChildren);
                    } else {
                        // Window function referencing a different window. Push the
                        // whole expression to the subquery; it will be rewritten later.
                        rewrite_expr_as_subquery_column(expr, ctx, false);
                    }
                }
                Expr::RowId { .. } | Expr::Column { .. } => {
                    rewrite_expr_as_subquery_column(expr, ctx, false);
                }
                Expr::SubqueryResult { .. }
                | Expr::Exists(..)
                | Expr::InSelect { .. }
                | Expr::Subquery(..) => {
                    rewrite_expr_as_subquery_column(expr, ctx, false);
                    return Ok(WalkControl::SkipChildren);
                }
                _ => {}
            }

            Ok(WalkControl::Continue)
        },
    )
}

/// Find the `WindowFunction` entry that this expression corresponds to.
/// Returns an entry that has not been rewritten yet when one exists.
fn find_window_function_entry<'a>(
    functions: &'a mut [WindowFunction],
    expr: &Expr,
) -> Option<&'a mut WindowFunction> {
    let mut fallback = None;
    let mut chosen = None;
    for (i, f) in functions.iter().enumerate() {
        if !exprs_are_equivalent(&f.original_expr, expr) {
            continue;
        }
        if f.rewritten.is_none() {
            chosen = Some(i);
            break;
        }
        fallback.get_or_insert(i);
    }
    functions.get_mut(chosen.or(fallback)?)
}

/// Add `expr` as an output column of the source subquery (the one being built
/// in `ctx`) and replace `*expr` with a reference to that column. Reuses an
/// existing equivalent column when `expr` is deterministic; nondeterministic
/// calls (e.g. `random()`) get a fresh column on every occurrence.
fn push_into_source_subquery(
    expr: &mut Expr,
    aggregates: &mut Vec<Aggregate>,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<()> {
    let contains_aggregates =
        resolve_window_and_aggregate_functions(expr, ctx.resolver, aggregates, None, &mut [])?;
    if expr_contains_nondeterministic_scalar_function(expr, ctx.resolver)? {
        push_new_subquery_column(expr, ctx, contains_aggregates);
    } else {
        rewrite_expr_as_subquery_column(expr, ctx, contains_aggregates);
    }
    Ok(())
}

/// Rewrite a window function call `expr` so its arguments and FILTER predicate
/// reference output columns of the source subquery (the one being built in
/// `ctx`). Returns the rewritten form, ready to be stored on the matching
/// `WindowFunction`.
fn rewrite_expr_referencing_current_window(
    aggregates: &mut Vec<Aggregate>,
    window_name: String,
    ctx: &mut WindowSubqueryContext,
    expr: &mut Expr,
    func: &AccumulatorFunc,
) -> crate::Result<RewrittenWindowCall> {
    let filter_over = match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group: _,
            filter_over,
            ..
        } => {
            let evaluate_args_after_buffer = window_function_uses_subtypes(func);
            for arg in args.iter_mut() {
                if evaluate_args_after_buffer {
                    rewrite_late_window_arg(arg, aggregates, ctx)?;
                } else {
                    push_into_source_subquery(arg, aggregates, ctx)?;
                }
            }
            turso_assert!(
                order_by.is_empty(),
                "ORDER BY in window functions is not supported"
            );
            filter_over
        }
        Expr::FunctionCallStar { filter_over, .. } => filter_over,
        _ => unreachable!("only functions can reference windows"),
    };

    if let Some(filter_expr) = filter_over.filter_clause.as_deref_mut() {
        push_into_source_subquery(filter_expr, aggregates, ctx)?;
    }
    let filter_expr = filter_over.filter_clause.as_deref().cloned();
    filter_over.over_clause = Some(Over::Name(Name::exact(window_name)));
    Ok(RewrittenWindowCall {
        expr: expr.clone(),
        filter_expr,
    })
}

/// JSON aggregates inspect their arguments' runtime subtypes. SQLite records
/// do not carry subtypes, so every argument expression must run after its row
/// is read back from the window buffer.
pub(crate) fn window_function_uses_subtypes(window_func: &AccumulatorFunc) -> bool {
    #[cfg(feature = "json")]
    {
        matches!(
            window_func,
            AccumulatorFunc::Agg(
                AggFunc::JsonGroupArray
                    | AggFunc::JsonbGroupArray
                    | AggFunc::JsonGroupObject
                    | AggFunc::JsonbGroupObject
            )
        )
    }
    #[cfg(not(feature = "json"))]
    {
        let _ = window_func;
        false
    }
}

/// Keep a window argument in the window layer while moving the values it reads
/// into the source subquery. This mirrors SQLite's subtype-aware aggregate
/// path, including evaluating scalar calls separately for step and inverse.
fn rewrite_late_window_arg(
    arg: &mut Expr,
    aggregates: &mut [Aggregate],
    ctx: &mut WindowSubqueryContext,
) -> Result<()> {
    walk_expr_mut(arg, &mut |node| {
        if matches!(
            node,
            Expr::FunctionCall { .. } | Expr::FunctionCallStar { .. }
        ) && aggregates
            .iter()
            .any(|aggregate| exprs_are_equivalent(&aggregate.original_expr, node))
        {
            rewrite_expr_as_subquery_column(node, ctx, true);
            return Ok(WalkControl::SkipChildren);
        }
        match node {
            Expr::RowId { .. } | Expr::Column { .. } => {
                rewrite_expr_as_subquery_column(node, ctx, false);
                return Ok(WalkControl::SkipChildren);
            }
            Expr::SubqueryResult { .. }
            | Expr::Exists(..)
            | Expr::InSelect { .. }
            | Expr::Subquery(..) => {
                rewrite_expr_as_subquery_column(node, ctx, false);
                return Ok(WalkControl::SkipChildren);
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Rewrites an expression into a reference to a subquery column. If an
/// equivalent expression was already pushed down, reuses its column index.
fn rewrite_expr_as_subquery_column(
    expr: &mut Expr,
    ctx: &mut WindowSubqueryContext,
    contains_aggregates: bool,
) {
    if let Some(pos) = ctx
        .subquery_result_columns
        .iter()
        .position(|col| exprs_are_equivalent(&col.expr, expr))
    {
        *expr = Expr::Column {
            database: Some(SUBQUERY_DATABASE_ID),
            table: *ctx.subquery_id,
            column: pos,
            is_rowid_alias: false,
        };
    } else {
        push_new_subquery_column(expr, ctx, contains_aggregates);
    }
}

/// Pushes `expr` as a fresh subquery column even if an equivalent column
/// already exists. Use this for expressions containing nondeterministic calls
/// like `random()`, which SQLite evaluates separately at each SQL occurrence.
fn push_new_subquery_column(
    expr: &mut Expr,
    ctx: &mut WindowSubqueryContext,
    contains_aggregates: bool,
) {
    let column_idx = ctx.subquery_result_columns.len();
    let subquery_ref = Expr::Column {
        database: Some(SUBQUERY_DATABASE_ID),
        table: *ctx.subquery_id,
        column: column_idx,
        is_rowid_alias: false,
    };
    let subquery_expr = mem::replace(expr, subquery_ref);
    ctx.subquery_result_columns.push(ResultSetColumn {
        expr: subquery_expr,
        alias: None,
        implicit_column_name: None,
        contains_aggregates,
    });
}

#[derive(Debug)]
pub struct WindowMetadata<'a> {
    pub labels: WindowLabels,
    pub registers: WindowRegisters,
    pub cursors: WindowCursors,
    pub(crate) frame: WindowFrameShape,
    /// Number of input columns in the source subquery.
    pub src_column_count: usize,
    /// Maps expressions in the current query that reference subquery columns
    /// to their corresponding column indexes in the subquery’s result.
    pub expressions_referencing_subquery: Vec<(&'a Expr, usize)>,
    pub buffer_table_name: String,
    pub functions: Vec<WindowFunctionRuntime<&'a Expr>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BufferedWindowValue<E> {
    Column(usize),
    Recompute(E),
}

#[derive(Debug, Clone)]
pub(crate) struct WindowFunctionRuntime<E> {
    function: AccumulatorFunc,
    arguments: Vec<BufferedWindowValue<E>>,
    filter: Option<BufferedWindowValue<E>>,
    argument_collations: Vec<CollationSeq>,
    argument_comparators: Vec<Option<crate::vdbe::insn::SortComparatorType>>,
    result_register: usize,
    /// Sorted index used by min/max when rows can leave the frame. Most
    /// aggregates can undo one row directly, but min/max must retain every
    /// in-frame `(value, sequence)` entry so removing the current answer
    /// still leaves the next answer available.
    minmax: Option<WindowMinMax>,
}

/// Resolved inputs needed to create window function state.
///
/// Legacy and HIR adapters resolve their own expression metadata before this
/// representation-neutral boundary.
pub(crate) struct WindowFunctionRuntimeSpec<E> {
    pub(crate) function: AccumulatorFunc,
    pub(crate) arguments: Vec<BufferedWindowValue<E>>,
    pub(crate) filter: Option<BufferedWindowValue<E>>,
    pub(crate) argument_collations: Vec<CollationSeq>,
    pub(crate) argument_comparators: Vec<Option<crate::vdbe::insn::SortComparatorType>>,
    pub(crate) result_register: usize,
    pub(crate) moving_start: bool,
    pub(crate) frame_excludes_rows: bool,
    pub(crate) minmax_collation: Option<CollationSeq>,
}

impl<E> WindowFunctionRuntime<E> {
    pub(crate) fn prepare(
        program: &mut ProgramBuilder,
        ordinal: usize,
        spec: WindowFunctionRuntimeSpec<E>,
    ) -> Self {
        let minmax_function = match &spec.function {
            AccumulatorFunc::Agg(agg @ (AggFunc::Min | AggFunc::Max))
                if spec.moving_start && !spec.frame_excludes_rows =>
            {
                Some(agg)
            }
            _ => None,
        };
        let minmax = minmax_function.map(|aggregate| {
            let index = Arc::new(Index {
                name: format!(
                    "window_minmax_{}_{}",
                    program.offset().as_offset_int(),
                    ordinal
                ),
                table_name: String::new(),
                root_page: 0,
                columns: crate::alloc::vec![
                    IndexColumn {
                        name: "0".to_string(),
                        order: if matches!(aggregate, AggFunc::Min) {
                            SortOrder::Desc
                        } else {
                            SortOrder::Asc
                        },
                        nulls_order: None,
                        pos_in_table: 0,
                        collation: spec.minmax_collation,
                        default: None,
                        expr: None,
                    },
                    IndexColumn::new("1", 1),
                ],
                unique: false,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index));
            let registers = program.alloc_registers(3);
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: cursor,
                is_table: false,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: registers + 1,
            });
            WindowMinMax { cursor, registers }
        });
        Self {
            function: spec.function,
            arguments: spec.arguments,
            filter: spec.filter,
            argument_collations: spec.argument_collations,
            argument_comparators: spec.argument_comparators,
            result_register: spec.result_register,
            minmax,
        }
    }

    #[cfg(test)]
    pub(crate) fn argument_collations(&self) -> &[CollationSeq] {
        &self.argument_collations
    }

    #[cfg(test)]
    pub(crate) fn result_register(&self) -> usize {
        self.result_register
    }

    #[cfg(test)]
    pub(crate) fn has_minmax_state(&self) -> bool {
        self.minmax.is_some()
    }
}

pub(crate) fn emit_window_partition_reset<E>(
    program: &mut ProgramBuilder,
    accumulator_start: usize,
    functions: &[WindowFunctionRuntime<E>],
    frame_tracking: WindowFrameTracking,
) {
    assert!(
        !functions.is_empty(),
        "window partition reset requires at least one function"
    );
    program.add_comment(program.offset(), "reset accumulator registers");
    program.emit_insn(Insn::Null {
        dest: accumulator_start,
        dest_end: Some(accumulator_start + functions.len() - 1),
    });

    // Reset every min/max index and its insertion sequence at a partition
    // boundary. Mirrors windowInitAccum (window.c:2000-2009).
    for function in functions {
        if let Some(state) = function.minmax {
            program.emit_insn(Insn::ResetSorter {
                cursor_id: state.cursor,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: state.registers + 1,
            });
        }
    }

    // Zero the frame-index counters for the new partition. Mirrors
    // `windowInitAccum`'s regApp reset (window.c:2010-2013).
    if let WindowFrameTracking::Positional { counters } = frame_tracking {
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: counters,
        });
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: counters + 1,
        });
    }
}

/// Reads one prepared window value from the current buffered row.
pub(crate) trait WindowValueEmitter<E> {
    fn emit_buffered_value(
        &mut self,
        program: &mut ProgramBuilder,
        cursor: CursorID,
        value: &BufferedWindowValue<E>,
        target: usize,
    ) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct WindowMinMax {
    pub cursor: CursorID,
    /// The first of three registers in a row, used to build one index
    /// entry: `[0]` the function argument's value for this row, `[1]` the
    /// sequence counter (bumped once per row so equal values stay
    /// distinct), `[2]` those two packed into the record that is inserted
    /// into the index.
    pub registers: usize,
}

/// Prepare the values and runtime state used while stepping each function.
///
/// This also creates the sorted index that a `min()` / `max()` window
/// function uses when the frame's start can move. The
/// index sorts on two columns: the argument value, then an
/// always-increasing sequence number so equal values still get separate
/// entries. For `max()` the value sorts ascending and for `min()` it sorts
/// descending, so in both cases the current answer is just the last entry
/// in the index — one code path serves both. The index is ephemeral: it
/// exists only for the duration of this query.
fn prepare_window_functions<'a>(
    program: &mut ProgramBuilder,
    window: &'a Window,
    table_references: &TableReferences,
    resolver: &Resolver,
    result_registers_start: usize,
) -> Result<Vec<WindowFunctionRuntime<&'a Expr>>> {
    let moving_start = !matches!(
        window.frame.start,
        crate::translate::plan::FrameBoundary::UnboundedPreceding
    );
    let mut functions = Vec::with_capacity(window.functions.len());

    for (i, func) in window.functions.iter().enumerate() {
        let arguments: Vec<&Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|arg| arg.as_ref()).collect(),
            Expr::FunctionCallStar { .. } => Vec::new(),
            _ => unreachable!("window functions are FunctionCall or FunctionCallStar expressions"),
        };
        let minmax_collation = arguments
            .first()
            .map(|argument| get_collseq_from_expr(argument, table_references))
            .transpose()?
            .flatten();
        let argument_collations = arguments
            .iter()
            .map(|argument| agg_arg_collation(table_references, argument, resolver))
            .collect();
        let argument_comparators = arguments
            .iter()
            .map(|argument| custom_type_comparator(argument, table_references, resolver.schema()))
            .collect();
        let arguments = arguments
            .into_iter()
            .map(|argument| match argument {
                Expr::Column { column, .. } => BufferedWindowValue::Column(*column),
                _ => BufferedWindowValue::Recompute(argument),
            })
            .collect();
        let filter = func
            .rewritten
            .as_ref()
            .and_then(|rewritten| rewritten.filter_expr.as_ref())
            .map(|filter| match filter {
                Expr::Column { column, .. } => BufferedWindowValue::Column(*column),
                _ => BufferedWindowValue::Recompute(filter),
            });

        functions.push(WindowFunctionRuntime::prepare(
            program,
            i,
            WindowFunctionRuntimeSpec {
                function: func.func.clone(),
                arguments,
                filter,
                argument_collations,
                argument_comparators,
                result_register: result_registers_start + i,
                moving_start,
                frame_excludes_rows: window.frame.exclude.is_some(),
                minmax_collation,
            },
        ));
    }
    Ok(functions)
}

#[derive(Debug, Clone, Copy)]
pub struct WindowLabels {
    /// Address of the flush subroutine — the code that, at the end of each
    /// partition, finishes off the rows still sitting in the buffer.
    pub flush_buffer: BranchOffset,
    /// Address of a small shared subroutine that hands one finished row to
    /// the rest of the query (into the SELECT output, or into the ORDER BY
    /// sorter). Every place that emits a row (RETURN_ROW) calls this same
    /// subroutine, so the per-row output logic — for example the duplicate
    /// check for SELECT DISTINCT — is written once here instead of being
    /// repeated at every emit site. Mirrors SQLite's `addrGosub`
    /// (window.c:1988).
    pub row_output: BranchOffset,
    /// Address of the instruction just past all window processing; control
    /// jumps here once the input rows have run out.
    pub window_processing_end: BranchOffset,
}

/// One of the three cursors that walk the buffered rows of a partition.
/// Each main-loop operation drives exactly one of them: AGGSTEP → `End`,
/// RETURN_ROW → `Current`, AGGINVERSE → `Start`. Used to index per-cursor
/// state by role so a cursor and its bookkeeping can't be mixed up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameCursor {
    Start = 0,
    Current = 1,
    End = 2,
}

/// The REPLAY side of peer tracking: for each frame cursor, the start
/// register of a block holding the ORDER BY values of the peer group (a
/// run of rows with equal ORDER BY values) that cursor is currently
/// sitting on. As a cursor walks forward through the
/// buffered rows, `emit_window_op` compares the next row against its block
/// to tell when the cursor has crossed into a new group. Three separate
/// blocks because under RANGE/GROUPS the cursors can each be on a
/// different group at the same time (needed for percent_rank /
/// cume_dist).
///
/// An entry is `None` when that cursor isn't peer-tracked: always under
/// ROWS (rows are never compared for peer-equality), and `Start` also
/// when the frame start can't move (there is no `csr_start`). Indexed by
/// [`FrameCursor`]. Mirrors SQLite's `s.{start,current,end}.reg`
/// (window.c:2897-2899). See `source_peer_values` for the INPUT-side
/// counterpart.
#[derive(Debug, Clone, Copy, Default)]
pub struct CursorPeerValues([Option<usize>; 3]);

impl core::ops::Index<FrameCursor> for CursorPeerValues {
    type Output = Option<usize>;
    fn index(&self, cursor: FrameCursor) -> &Self::Output {
        &self.0[cursor as usize]
    }
}

impl CursorPeerValues {
    /// The register of each cursor that is peer-tracked, skipping the ones
    /// that aren't (`None`).
    pub(crate) fn allocated(&self) -> impl Iterator<Item = usize> {
        self.0.into_iter().flatten()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WindowPeerState {
    pub(crate) key_count: usize,
    pub(crate) current: Option<usize>,
    pub(crate) previous_input: Option<usize>,
    pub(crate) cursors: CursorPeerValues,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WindowPeerComparison {
    AllRows,
    Keys {
        current: usize,
        previous: usize,
        count: usize,
    },
}

impl WindowPeerComparison {
    pub(crate) fn from_registers(
        count: usize,
        current: Option<usize>,
        previous: Option<usize>,
    ) -> Self {
        if count == 0 {
            Self::AllRows
        } else {
            Self::Keys {
                current: current.expect("current peer keys must exist when ORDER BY is present"),
                previous: previous.expect("previous peer keys must exist when ORDER BY is present"),
                count,
            }
        }
    }
}

pub(crate) fn prepare_window_peer_state(
    program: &mut ProgramBuilder,
    key_count: usize,
    track_peers: bool,
    moving_start: bool,
) -> WindowPeerState {
    WindowPeerState {
        key_count,
        current: alloc_optional_registers(program, key_count),
        previous_input: track_peers
            .then(|| alloc_optional_registers(program, key_count))
            .flatten(),
        // Order matters: this allocates registers, so keep it
        // Start / Current / End to match `FrameCursor`.
        cursors: CursorPeerValues([
            // Start: only when the frame start can move — that is the
            // only case with a `csr_start` to track.
            (track_peers && moving_start)
                .then(|| alloc_optional_registers(program, key_count))
                .flatten(),
            track_peers
                .then(|| alloc_optional_registers(program, key_count))
                .flatten(),
            track_peers
                .then(|| alloc_optional_registers(program, key_count))
                .flatten(),
        ]),
    }
}

pub(crate) fn emit_window_peer_seed(program: &mut ProgramBuilder, state: WindowPeerState) {
    let (Some(current), Some(previous_input)) = (state.current, state.previous_input) else {
        return;
    };

    // Seed the source-row peer reference and every per-cursor
    // peer reference with the first row's ORDER BY values.
    // Mirrors SQLite's init at `window.c:2972-2976` (regNewPeer
    // → regPeer → s.start.reg / current.reg / end.reg).
    program.add_comment(
        program.offset(),
        "initialize peer-reference registers for new partition",
    );
    let extra_amount = state.key_count - 1;
    program.emit_insn(Insn::Copy {
        src_reg: current,
        dst_reg: previous_input,
        extra_amount,
    });
    for cursor in state.cursors.allocated() {
        program.emit_insn(Insn::Copy {
            src_reg: previous_input,
            dst_reg: cursor,
            extra_amount,
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowFrameEdge {
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowFrameShape {
    pub(crate) mode: FrameMode,
    pub(crate) start: WindowFrameEdge,
    pub(crate) end: WindowFrameEdge,
    /// Any explicit EXCLUDE clause selects the full-frame scan path,
    /// including `EXCLUDE NO OTHERS`.
    pub(crate) has_exclude: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowFrameOrderCheck {
    /// A PRECEDING/PRECEDING frame is non-empty when its end offset is no
    /// greater than its start offset.
    EndAtOrBeforeStart { start: usize, end: usize },
    /// A FOLLOWING/FOLLOWING frame is non-empty when its end offset is no
    /// less than its start offset.
    EndAtOrAfterStart { start: usize, end: usize },
}

pub(crate) fn window_frame_order_check(
    frame: WindowFrameShape,
    offsets: WindowFrameOffsets,
) -> Option<WindowFrameOrderCheck> {
    if frame.mode == FrameMode::Range {
        return None;
    }
    let direction = match (frame.start, frame.end) {
        (WindowFrameEdge::Preceding, WindowFrameEdge::Preceding) => WindowFrameEdge::Preceding,
        (WindowFrameEdge::Following, WindowFrameEdge::Following) => WindowFrameEdge::Following,
        _ => return None,
    };
    let WindowFrameOffsets::Both { start, end } = offsets else {
        unreachable!("same-kind bounded frames carry both offset registers")
    };
    Some(match direction {
        WindowFrameEdge::Preceding => WindowFrameOrderCheck::EndAtOrBeforeStart { start, end },
        WindowFrameEdge::Following => WindowFrameOrderCheck::EndAtOrAfterStart { start, end },
        WindowFrameEdge::UnboundedPreceding
        | WindowFrameEdge::CurrentRow
        | WindowFrameEdge::UnboundedFollowing => unreachable!("direction was checked above"),
    })
}

pub(crate) fn emit_window_nonempty_branch(
    program: &mut ProgramBuilder,
    check: WindowFrameOrderCheck,
    target: BranchOffset,
) {
    let flags = crate::vdbe::insn::CmpInsFlags::default();
    match check {
        WindowFrameOrderCheck::EndAtOrBeforeStart { start, end } => {
            program.emit_insn(Insn::Le {
                lhs: end,
                rhs: start,
                target_pc: target,
                flags,
                collation: None,
            });
        }
        WindowFrameOrderCheck::EndAtOrAfterStart { start, end } => {
            program.emit_insn(Insn::Ge {
                lhs: end,
                rhs: start,
                target_pc: target,
                flags,
                collation: None,
            });
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WindowEmptyFrameState {
    pub(crate) current_cursor: CursorID,
    pub(crate) rowid: usize,
    pub(crate) step_end: BranchOffset,
}

#[derive(Clone, Copy)]
pub(crate) enum WindowEmptyFrameOutput {
    Aggregate,
    Row,
}

/// Handle a non-RANGE frame that is empty for every row because its same-kind
/// bounded edges cross: M > N for `N PRECEDING AND M PRECEDING`, or M < N for
/// `N FOLLOWING AND M FOLLOWING`. Each row emits the result an empty frame
/// gives: an aggregate that never saw a row (`sum` is NULL, `count` is zero),
/// while `first_value` and `nth_value` yield NULL because their frame counters
/// stay zero.
///
/// The aggregate result must be emitted before the current cursor is rewound;
/// the row result must be emitted after it. The buffer is then cleared and
/// `rowid` is set back to NULL so every subsequent row re-enters this first-row
/// path. SQLite gets that re-entry for free because its first-row test is
/// `rowid == 1` and `ResetSorter` restarts the rowids.
pub(crate) fn emit_window_empty_frame_guard(
    program: &mut ProgramBuilder,
    check: WindowFrameOrderCheck,
    state: WindowEmptyFrameState,
    mut emit_output: impl FnMut(&mut ProgramBuilder, WindowEmptyFrameOutput) -> Result<()>,
) -> Result<()> {
    let frame_valid = program.allocate_label();
    program.add_comment(program.offset(), "empty-frame check");
    // FOLLOWING pair: valid iff end >= start. PRECEDING pair:
    // valid iff end <= start. Mirrors the Ge/Le pick at
    // window.c:2951.
    emit_window_nonempty_branch(program, check, frame_valid);
    emit_output(program, WindowEmptyFrameOutput::Aggregate)?;

    // The row was just inserted, so the empty branch of this
    // Rewind is unreachable — the label lands on the next
    // instruction either way.
    let unreachable_empty = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: state.current_cursor,
        pc_if_empty: unreachable_empty,
    });
    program.preassign_label_to_next_insn(unreachable_empty);
    emit_output(program, WindowEmptyFrameOutput::Row)?;

    program.emit_insn(Insn::ResetSorter {
        cursor_id: state.current_cursor,
    });
    program.emit_insn(Insn::Null {
        dest: state.rowid,
        dest_end: None,
    });
    program.emit_insn(Insn::Goto {
        target_pc: state.step_end,
    });
    program.preassign_label_to_next_insn(frame_valid);
    Ok(())
}

/// First-row control flow needs only prepared frame and buffer state.
pub(crate) struct WindowFirstRowFrame<'a> {
    pub frame: WindowFrameShape,
    pub offsets: WindowFrameOffsets,
    pub cursors: WindowCursors,
    pub input: WindowBufferInput,
    pub rowid: usize,
    pub table_name: &'a str,
    pub step_end: BranchOffset,
}

/// Keep first-row insertion and frame positioning identical for HIR and legacy callers.
pub(crate) fn emit_window_first_row_frame(
    program: &mut ProgramBuilder,
    state: WindowFirstRowFrame<'_>,
    emit_output: impl FnMut(&mut ProgramBuilder, WindowEmptyFrameOutput) -> Result<()>,
) -> Result<()> {
    emit_window_buffer_insert(
        program,
        &state.cursors,
        state.input,
        state.rowid,
        state.table_name,
    );
    // Empty-frame check (window.c:2950-2961): for non-RANGE frames
    // bounded on both sides by the same kind, crossing bounds make the
    // frame empty for every row.
    if let Some(check) = window_frame_order_check(state.frame, state.offsets) {
        emit_window_empty_frame_guard(
            program,
            check,
            WindowEmptyFrameState {
                current_cursor: state.cursors.csr_current,
                rowid: state.rowid,
                step_end: state.step_end,
            },
            emit_output,
        )?;
    }
    // `N FOLLOWING AND M FOLLOWING`: dropping rows off the start
    // (AGGINVERSE) must lag adding them (AGGSTEP) by M - N rows,
    // rather than lag emitting the result (RETURN_ROW) by M. So the
    // start count is set to the difference of the two offsets
    // (window.c:2962-2965).
    emit_window_following_start_delay(program, state.frame, state.offsets);
    // Position each frame cursor at the just-inserted first row.
    // Mirrors `window.c:2967-2971` — `csr_start` is rewound only when
    // the frame start isn't UNBOUNDED PRECEDING (otherwise the
    // cursor wasn't allocated and AggInverse is a no-op).
    emit_window_frame_cursor_rewind(program, state.cursors);
    // The first row is not added to the totals here. The flush
    // subroutine adds it once at the end of the partition (AGGSTEP)
    // and emits it there (RETURN_ROW).
    program.emit_insn(Insn::Goto {
        target_pc: state.step_end,
    });
    Ok(())
}

/// Buffer a later partition row before deciding whether its peer group changed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_subsequent_row(
    program: &mut ProgramBuilder,
    cursors: WindowCursors,
    input: WindowBufferInput,
    rowid: usize,
    table_name: &str,
    mode: FrameMode,
    peers: WindowPeerState,
    key_info: impl IntoIterator<Item = Result<KeyInfo>>,
    step_end: BranchOffset,
) -> Result<()> {
    emit_window_buffer_insert(program, &cursors, input, rowid, table_name);

    // Under RANGE / GROUPS, a row with the same ORDER BY values as the
    // previous one (its peer) is only buffered for now: adding rows to
    // the totals and emitting results both wait until the next group
    // begins, and then handle the whole buffered group in one go. With
    // no ORDER BY the whole partition is a single group, so every row
    // waits and the end-of-partition flush does the work. Mirrors
    // SQLite's `windowIfNewPeer` call at window.c:2984-2986 (an
    // unconditional jump when there's no ORDER BY, window.c:2076).
    if mode != FrameMode::Rows {
        program.add_comment(
            program.offset(),
            "peer of previous row: buffer only, handle at group end",
        );
        emit_window_peer_change(
            program,
            WindowPeerComparison::from_registers(
                peers.key_count,
                peers.current,
                peers.previous_input,
            ),
            key_info,
            step_end,
        )?;
    }
    Ok(())
}

pub(crate) fn emit_window_following_start_delay(
    program: &mut ProgramBuilder,
    frame: WindowFrameShape,
    offsets: WindowFrameOffsets,
) {
    if frame.mode == FrameMode::Range || frame.start != WindowFrameEdge::Following {
        return;
    }
    let WindowFrameOffsets::Both { start, end } = offsets else {
        return;
    };
    program.emit_insn(Insn::Subtract {
        lhs: end,
        rhs: start,
        dest: start,
    });
}

fn legacy_window_frame_edge(boundary: &crate::translate::plan::FrameBoundary) -> WindowFrameEdge {
    match boundary {
        crate::translate::plan::FrameBoundary::UnboundedPreceding => {
            WindowFrameEdge::UnboundedPreceding
        }
        crate::translate::plan::FrameBoundary::Preceding(_) => WindowFrameEdge::Preceding,
        crate::translate::plan::FrameBoundary::CurrentRow => WindowFrameEdge::CurrentRow,
        crate::translate::plan::FrameBoundary::Following(_) => WindowFrameEdge::Following,
        crate::translate::plan::FrameBoundary::UnboundedFollowing => {
            WindowFrameEdge::UnboundedFollowing
        }
    }
}

fn legacy_window_frame_shape(frame: &crate::translate::plan::Frame) -> WindowFrameShape {
    WindowFrameShape {
        mode: frame.mode,
        start: legacy_window_frame_edge(&frame.start),
        end: legacy_window_frame_edge(&frame.end),
        has_exclude: frame.exclude.is_some(),
    }
}

/// Runtime countdown registers for bounded frame edges.
///
/// A start boundary "N rows away" means one piece of work has to wait until
/// we have moved N rows along. `OP_IfPos` skips that work and ticks the start
/// counter down until it reaches zero:
/// - `ROWS BETWEEN N PRECEDING AND CURRENT ROW` delays AGGINVERSE, so the
///   frame first grows to N+1 rows before it starts sliding forward.
/// - cume_dist's implicit `1 FOLLOWING` start delays RETURN_ROW.
///
/// An end boundary "N rows away" delays work until we have moved N rows
/// further along:
/// - `N FOLLOWING` delays RETURN_ROW and AGGINVERSE.
/// - `N PRECEDING` delays AGGSTEP.
///
/// The expressions are evaluated at the start of every partition because
/// these registers are decremented while that partition runs. Mirrors
/// SQLite's `regStart` and `regEnd` (window.c:2883-2887).
#[derive(Clone, Copy, Debug)]
pub(crate) enum WindowFrameOffsets {
    None,
    /// The offset N from a frame whose start is `N PRECEDING` or
    /// `N FOLLOWING`, worked out once per partition. This is a countdown:
    /// `OP_IfPos` skips work and ticks it down until it reaches zero.
    Start {
        start: usize,
    },
    /// The offset N from a frame whose end is `N PRECEDING` or
    /// `N FOLLOWING`, worked out once per partition. This is a countdown
    /// for work that must wait until the end boundary is reached.
    End {
        end: usize,
    },
    /// Independent countdowns for bounded start and end expressions.
    Both {
        start: usize,
        end: usize,
    },
}

impl WindowFrameOffsets {
    pub(crate) const fn start(self) -> Option<usize> {
        match self {
            Self::Start { start } | Self::Both { start, .. } => Some(start),
            Self::None | Self::End { .. } => None,
        }
    }

    pub(crate) const fn end(self) -> Option<usize> {
        match self {
            Self::End { end } | Self::Both { end, .. } => Some(end),
            Self::None | Self::Start { .. } => None,
        }
    }
}

pub(crate) fn prepare_window_frame_offsets(
    program: &mut ProgramBuilder,
    has_start: bool,
    has_end: bool,
) -> WindowFrameOffsets {
    match (has_start, has_end) {
        (false, false) => WindowFrameOffsets::None,
        (true, false) => WindowFrameOffsets::Start {
            start: program.alloc_register(),
        },
        (false, true) => WindowFrameOffsets::End {
            end: program.alloc_register(),
        },
        (true, true) => WindowFrameOffsets::Both {
            start: program.alloc_register(),
            end: program.alloc_register(),
        },
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum WindowFrameTracking {
    None,
    /// Two counters tracking where the frame's edges are, so `first_value`
    /// / `nth_value` can jump straight to a particular row of the frame by
    /// its position:
    ///
    /// * `+0` — rows that have left the frame (one per AGGINVERSE), i.e.
    ///   the frame start's buffer index minus one.
    /// * `+1` — rows that have entered the frame (one per AGGSTEP), i.e.
    ///   the frame end's buffer index.
    ///
    /// Buffer rowids run 1, 2, 3, ... within each partition, so these
    /// indexes are also `SeekRowid` targets. One pair per window (not per
    /// function like SQLite's `pWin->regApp`) since every function in a
    /// window shares the same frame. Mirrors SQLite's `regApp` pair
    /// (window.c:1457-1459, 1726, 2010-2013).
    Positional {
        counters: usize,
    },
    /// The first and last buffer rowid of the frame as it stands right now.
    /// Only used when the window has an EXCLUDE clause. EXCLUDE means we
    /// cannot keep a running total as rows come and go, because which rows
    /// count keeps changing; instead we re-add up the whole frame for every
    /// output row, and to do that we need to know exactly which rows the
    /// frame currently spans. AGGSTEP moves the last-rowid forward as rows
    /// join the frame, AGGINVERSE moves the first-rowid forward as rows
    /// drop off it. They start at first=1, last=0: with the first rowid
    /// past the last, the range covers no rows, so the frame starts empty.
    Excluded {
        start_rowid: usize,
        end_rowid: usize,
    },
}

pub(crate) fn prepare_window_frame_tracking(
    program: &mut ProgramBuilder,
    excluded: bool,
    positional: bool,
) -> WindowFrameTracking {
    if excluded {
        let start_rowid = program.alloc_register();
        let end_rowid = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: start_rowid,
        });
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: end_rowid,
        });
        WindowFrameTracking::Excluded {
            start_rowid,
            end_rowid,
        }
    } else if positional {
        WindowFrameTracking::Positional {
            counters: program.alloc_registers(2),
        }
    } else {
        WindowFrameTracking::None
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WindowRegisters {
    /// The rowid of the last row we inserted into the buffer table. It is
    /// set to NULL at the start of each partition, so a NULL value here
    /// means we haven't yet inserted this partition's first row — which is
    /// how the main loop tells "first row of a new partition" apart from
    /// every later row.
    pub rowid: usize,
    /// Start of a block of registers holding the PARTITION BY column values
    /// of the current partition. Comparing the next row's PARTITION BY
    /// values against these is how we notice that a new partition has
    /// begun.
    pub partition_start: Option<usize>,
    /// Start of a block of registers, one per window function, holding that
    /// function's running total. AGGSTEP folds a row into these as it joins
    /// the frame; AGGINVERSE takes one back out as it leaves.
    pub acc_start: usize,
    /// The return address the flush subroutine jumps back to once it has
    /// finished emptying the buffer.
    pub flush_buffer_return_offset: usize,
    /// Start of consecutive registers holding the current row's column
    /// values, as read from the source subquery.
    pub src_columns_start: usize,
    /// Start of a block of registers holding the column values that have to
    /// be carried through from the subquery out to the parent query.
    pub result_columns_start: usize,
    /// Start of the register array holding ORDER BY column values for the current row.
    /// These registers are used to detect whether the current row is a "peer"
    /// (i.e., has identical ORDER BY values to the previous row).
    pub new_order_by_columns_start: Option<usize>,
    /// The INPUT side of peer tracking: the ORDER BY values of the peer
    /// group that the source rows coming in are currently in. (A peer group
    /// is a run of rows with equal ORDER BY values.) The main loop
    /// (`emit_window_step`) compares each new source row's values
    /// (`new_order_by_columns_start`) against this, once per row:
    /// - Equal: the row is in the same group. Under RANGE/GROUPS a whole
    ///   group is handled at once, so the row is only buffered for now —
    ///   nothing is added to the totals and no result is emitted yet.
    /// - Different: a new group has started, so this is overwritten with
    ///   the new row's values.
    ///
    /// Just one register block, because the input is read in one pass.
    /// Contrast `cursor_peer_values` below, the REPLAY side. Mirrors
    /// SQLite's `regPeer` (window.c:2896, updated at window.c:2984-2986).
    pub source_peer_values: Option<usize>,
    /// The REPLAY side of peer tracking, one entry per frame cursor. See
    /// [`CursorPeerValues`].
    pub cursor_peer_values: CursorPeerValues,
    /// Runtime countdowns for bounded frame edges. Mirrors SQLite's
    /// `regStart` and `regEnd` (window.c:2883-2887).
    pub(crate) frame_offsets: WindowFrameOffsets,
    pub(crate) frame_tracking: WindowFrameTracking,
    /// The register holding the return address for calls to the
    /// `row_output` subroutine (see `WindowLabels::row_output`): the
    /// subroutine jumps back to whatever address is stored here when it
    /// finishes. Mirrors SQLite's `regGosub` (window.c:2793).
    pub row_output_return: usize,
}

/// Cursors over the "buffer": a temporary B-tree table, created just for
/// this query, that holds the rows of the current partition so we can read
/// them more than once. The four cursor roles mirror SQLite's
/// `sqlite3WindowCodeStep` allocation (`window.c:2834-2837`).
#[derive(Debug, Clone, Copy)]
pub struct WindowCursors {
    /// The cursor used to append each incoming source row to the buffer.
    /// We never seek it; it simply advances as we `NewRowid` + `Insert` the
    /// next row.
    pub csr_write: CursorID,
    /// The row currently being sent to the outer query. Each function's
    /// result for this row is computed just before the cursor moves on,
    /// and the row's output columns are read from here.
    pub csr_current: CursorID,
    /// Points at the next row about to join the frame. `AggStep` reads
    /// that row, adds it to the running totals, then moves the cursor on.
    /// Under GROUPS/RANGE frames one advance steps over every row with
    /// equal ORDER BY values (a "peer group") at once.
    pub csr_end: CursorID,
    /// Points at the first row still inside the frame — the one
    /// `AggInverse` drops next as the frame's start moves forward. Only
    /// allocated when the frame start can move off the first row of the
    /// partition (ntile, percent_rank, cume_dist); an UNBOUNDED PRECEDING
    /// start never moves, so there is nothing to track (`None`). Under
    /// GROUPS/RANGE frames one advance steps over a whole peer group (rows
    /// with equal ORDER BY values). Mirrors SQLite's `s.start.csr`
    /// (`window.c:2836`).
    pub csr_start: Option<CursorID>,
    /// A separate cursor for functions that read one specific row rather
    /// than a running total — first_value, nth_value, lag, lead. At output
    /// time it jumps straight to the wanted row by its rowid (`SeekRowid`).
    /// It is a duplicate of `csr_current` (`OpenDup`) and only ever moves
    /// on those jumps, so it never disturbs the three frame cursors.
    /// Allocated only when some function needs it (`None` otherwise).
    /// Mirrors SQLite's `pWin->csrApp` (`window.c`).
    pub csr_app: Option<CursorID>,
}

/// Position every cursor that walks the window frame at the first buffered
/// row. The buffer has just received that row, so the empty target is only a
/// common landing point for the `Rewind` instructions.
pub(crate) fn emit_window_frame_cursor_rewind(
    program: &mut ProgramBuilder,
    cursors: WindowCursors,
) {
    let label_unreachable_empty = program.allocate_label();
    for cursor_id in cursors
        .csr_start
        .into_iter()
        .chain([cursors.csr_current, cursors.csr_end])
    {
        program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: label_unreachable_empty,
        });
    }
    program.preassign_label_to_next_insn(label_unreachable_empty);
}

/// Open cursors that share one ephemeral window buffer.
///
/// Legacy and HIR lowering decide independently which rows and expressions
/// the table stores. The cursor roles and their opening order are shared.
#[derive(Debug)]
pub(crate) struct WindowBufferRuntime {
    pub(crate) table: Arc<BTreeTable>,
    pub(crate) cursors: WindowCursors,
}

/// Consecutive registers holding one row in window-buffer column order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowBufferInput {
    pub(crate) start: usize,
    pub(crate) count: usize,
}

/// Copy selected input slots into one consecutive key-register block.
pub(crate) fn emit_window_key_gather(
    program: &mut ProgramBuilder,
    input: WindowBufferInput,
    slots: impl IntoIterator<Item = usize>,
    output_start: usize,
) -> Result<()> {
    for (output, slot) in slots.into_iter().enumerate() {
        turso_assert!(slot < input.count, "window key slot outside input row");
        program.emit_insn(Insn::Copy {
            src_reg: input.start + slot,
            dst_reg: output_start + output,
            extra_amount: 0,
        });
    }
    Ok(())
}

/// Read selected window-buffer columns into one consecutive key-register block.
pub(crate) fn emit_window_cursor_keys(
    program: &mut ProgramBuilder,
    cursor: CursorID,
    columns: impl IntoIterator<Item = usize>,
    output_start: usize,
) -> usize {
    let mut count = 0;
    for (offset, column) in columns.into_iter().enumerate() {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column,
            dest: output_start + offset,
            default: None,
        });
        count = offset + 1;
    }
    count
}

pub(crate) fn open_window_buffer(
    program: &mut ProgramBuilder,
    table: Arc<BTreeTable>,
    needs_start_cursor: bool,
    needs_app_cursor: bool,
) -> WindowBufferRuntime {
    // `csr_current` owns the ephemeral table. The other cursors share its
    // B-tree but keep independent positions for their frame roles.
    let csr_current = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let csr_write = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let csr_end = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let csr_start =
        needs_start_cursor.then(|| program.alloc_cursor_id(CursorType::BTreeTable(table.clone())));
    let csr_app =
        needs_app_cursor.then(|| program.alloc_cursor_id(CursorType::BTreeTable(table.clone())));

    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: csr_current,
        is_table: true,
    });
    for cursor in [Some(csr_write), Some(csr_end), csr_start, csr_app]
        .into_iter()
        .flatten()
    {
        program.emit_insn(Insn::OpenDup {
            original_cursor_id: csr_current,
            new_cursor_id: cursor,
        });
    }

    WindowBufferRuntime {
        table,
        cursors: WindowCursors {
            csr_write,
            csr_current,
            csr_end,
            csr_start,
            csr_app,
        },
    }
}

pub(crate) fn emit_window_buffer_insert(
    program: &mut ProgramBuilder,
    cursors: &WindowCursors,
    input: WindowBufferInput,
    rowid: usize,
    table_name: &str,
) {
    let record = program.alloc_register();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(input.start),
        count: to_u32(input.count),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: cursors.csr_write,
        rowid_reg: rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.csr_write,
        key_reg: rowid,
        record_reg: record,
        flag: InsertFlags::new().require_seek(),
        table_name: table_name.to_string(),
    });
}

/// Builds `KeyInfo` entries for the window's ORDER BY columns, populating
/// each entry's collation from the expression. Used everywhere two rows
/// are compared to see whether they are peers (have equal ORDER BY
/// values): once on each incoming source row, and once per cursor while
/// stepping over a group in `emit_window_op`. `sort_order` and
/// `nulls_order` are left at their `Insn::Compare` defaults — this check
/// only asks whether the values are equal, not which one sorts first.
fn order_by_key_info<'a>(
    window: &'a Window,
    table_references: &'a crate::translate::plan::TableReferences,
) -> impl Iterator<Item = Result<KeyInfo>> + 'a {
    window.order_by.iter().map(|(expr, _, _)| {
        let collation = get_collseq_from_expr(expr, table_references)?.unwrap_or_default();
        Ok(KeyInfo {
            sort_order: SortOrder::Asc,
            collation,
            nulls_order: None,
        })
    })
}

fn window_order_by_columns(window: &Window) -> impl Iterator<Item = usize> + '_ {
    window.order_by.iter().map(|(expression, _, _)| {
        let Expr::Column { column, .. } = expression else {
            unreachable!("window ORDER BY expressions are buffer columns after rewrite");
        };
        *column
    })
}

/// Compare two consecutive register blocks with the supplied key metadata.
/// Key metadata stays lazy until `Insn::Compare` needs its owned vector.
pub(crate) fn emit_window_key_compare(
    program: &mut ProgramBuilder,
    left: usize,
    right: usize,
    key_info: impl IntoIterator<Item = Result<KeyInfo>>,
    if_equal: BranchOffset,
    if_different: BranchOffset,
) -> Result<usize> {
    let key_info = key_info.into_iter().collect::<Result<Vec<_>>>()?;
    let count = key_info.len();
    turso_assert!(count > 0, "window key comparison requires at least one key");
    // `Insn::Compare` requires `start_reg_a < start_reg_b`; unequal
    // targets are symmetric, so operand order does not change semantics.
    let (left, right) = (left.min(right), left.max(right));
    program.emit_insn(Insn::Compare {
        start_reg_a: left,
        start_reg_b: right,
        count,
        key_info,
    });
    program.emit_insn(Insn::Jump {
        target_pc_lt: if_different,
        target_pc_eq: if_equal,
        target_pc_gt: if_different,
    });
    Ok(count)
}

/// Compare the current ORDER BY values against the remembered ones: jump to
/// `target_if_peer` when they're equal (the two rows are peers), otherwise
/// copy the current values into the remembered registers and fall through.
/// With no ORDER BY every row counts as a peer, so this becomes an
/// unconditional jump. Mirrors SQLite's `windowIfNewPeer`
/// (window.c:2057-2078).
pub(crate) fn emit_window_peer_change(
    program: &mut ProgramBuilder,
    comparison: WindowPeerComparison,
    key_info: impl IntoIterator<Item = Result<KeyInfo>>,
    target_if_peer: BranchOffset,
) -> Result<()> {
    match comparison {
        WindowPeerComparison::AllRows => {
            program.emit_insn(Insn::Goto {
                target_pc: target_if_peer,
            });
        }
        WindowPeerComparison::Keys {
            current,
            previous,
            count,
        } => {
            let changed = program.allocate_label();
            let compared = emit_window_key_compare(
                program,
                current,
                previous,
                key_info,
                target_if_peer,
                changed,
            )?;
            turso_assert!(
                compared == count,
                "window peer register width must match key metadata"
            );
            program.preassign_label_to_next_insn(changed);
            program.emit_insn(Insn::Copy {
                src_reg: current,
                dst_reg: previous,
                extra_amount: compared - 1,
            });
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct WindowInputState {
    pub(crate) rowid: usize,
    pub(crate) previous_partition: Option<usize>,
    pub(crate) flush_return: usize,
}

pub(crate) fn prepare_window_input_state(
    program: &mut ProgramBuilder,
    partition_key_count: usize,
) -> WindowInputState {
    WindowInputState {
        rowid: program.alloc_registers_and_init_w_null(1),
        previous_partition: (partition_key_count > 0)
            .then(|| program.alloc_registers_and_init_w_null(partition_key_count)),
        flush_return: program.alloc_register(),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WindowPartitionState {
    pub(crate) previous_keys: usize,
    pub(crate) rowid: usize,
    pub(crate) flush_return: usize,
}

/// Flush the previous partition when the current row has different keys.
pub(crate) fn emit_window_partition_change(
    program: &mut ProgramBuilder,
    current_keys: usize,
    state: WindowPartitionState,
    flush_buffer: BranchOffset,
    key_info: impl IntoIterator<Item = Result<KeyInfo>>,
) -> Result<()> {
    let same_partition_label = program.allocate_label();
    let new_partition_label = program.allocate_label();

    program.add_comment(
        program.offset(),
        "compare partition keys to detect new partition",
    );
    let key_count = emit_window_key_compare(
        program,
        current_keys,
        state.previous_keys,
        key_info,
        same_partition_label,
        new_partition_label,
    )?;

    program.preassign_label_to_next_insn(new_partition_label);
    program.add_comment(program.offset(), "detected new partition");
    program.emit_insn(Insn::Gosub {
        target_pc: flush_buffer,
        return_reg: state.flush_return,
    });
    // Reset rowid to signal the start of processing a new partition.
    program.emit_insn(Insn::Null {
        dest: state.rowid,
        dest_end: None,
    });
    program.emit_insn(Insn::Copy {
        src_reg: current_keys,
        dst_reg: state.previous_keys,
        extra_amount: key_count - 1,
    });

    program.preassign_label_to_next_insn(same_partition_label);
    Ok(())
}

/// Which side of the frame an offset belongs to. Selects the error
/// message wording ("starting" vs "ending").
#[derive(Clone, Copy)]
pub(crate) enum WindowFrameBoundSide {
    Start,
    End,
}

/// Evaluate a frame-offset expression, then emit the runtime
/// non-negative check. A non-constant offset (a per-row column
/// reference, say) is loaded as NULL rather than evaluated, mirroring
/// SQLite's substitution at `window.c:1166-1171`: the check then rejects
/// it, but only once a partition is actually processed — a statement
/// over an empty table never evaluates the offset and never errors.
pub(crate) fn emit_window_frame_offset(
    program: &mut ProgramBuilder,
    register: usize,
    mode: turso_parser::ast::FrameMode,
    side: WindowFrameBoundSide,
    constant: bool,
    emit_value: impl FnOnce(&mut ProgramBuilder, usize) -> Result<()>,
) -> Result<()> {
    if constant {
        emit_value(program, register)?;
    } else {
        program.emit_insn(Insn::Null {
            dest: register,
            dest_end: None,
        });
    }
    emit_window_check_offset(program, register, mode, side);
    Ok(())
}

fn emit_legacy_window_frame_offset(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    resolver: &Resolver,
    expr: &Expr,
    register: usize,
    mode: turso_parser::ast::FrameMode,
    side: WindowFrameBoundSide,
) -> Result<()> {
    emit_window_frame_offset(
        program,
        register,
        mode,
        side,
        is_constant_frame_offset(expr, resolver)?,
        |program, register| {
            translate_expr_no_constant_opt(
                program,
                Some(&plan.table_references),
                expr,
                register,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )
            .map(|_| ())
        },
    )
}

/// SQLite uses a stricter constant-expression rule for frame offsets than it
/// uses for normal constant folding. In particular, function calls and the
/// current-time keywords are rejected even when they are deterministic or sit
/// in a CASE branch that cannot run.
fn is_constant_frame_offset(expr: &Expr, resolver: &Resolver<'_>) -> Result<bool> {
    use crate::translate::optimizer::Optimizable;

    if !expr.is_constant(resolver) {
        return Ok(false);
    }

    let mut allowed = true;
    walk_expr(expr, &mut |node| {
        if matches!(
            node,
            Expr::FunctionCall { .. }
                | Expr::FunctionCallStar { .. }
                | Expr::Literal(
                    Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp
                )
        ) {
            allowed = false;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(allowed)
}

/// Emit a runtime check that the frame-offset register holds a
/// non-negative integer, halting the program with the standard SQLite
/// error message on failure. Mirrors `windowCheckValue` at
/// `window.c:1482-1523`.
fn emit_window_check_offset(
    program: &mut ProgramBuilder,
    offset_reg: usize,
    mode: turso_parser::ast::FrameMode,
    side: WindowFrameBoundSide,
) {
    use turso_parser::ast::FrameMode;
    let label_halt = program.allocate_label();
    let label_ok = program.allocate_label();
    let reg_zero = program.alloc_register();
    if mode == FrameMode::Range {
        // A RANGE offset has to be a number. This checks that indirectly:
        // comparing with NUMERIC affinity turns a number (or numeric-looking
        // text) into a number, and in SQLite's type ordering numbers sort
        // below every string, including the empty string. So a valid
        // numeric offset is `< ""` and falls through, while real text or a
        // blob is `>= ""` and jumps to the error; jump_if_null sends NULL
        // there too.
        let reg_empty = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: String::new(),
            dest: reg_empty,
        });
        program.emit_insn(Insn::Ge {
            lhs: offset_reg,
            rhs: reg_empty,
            target_pc: label_halt,
            flags: crate::vdbe::insn::CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(crate::vdbe::affinity::Affinity::Numeric),
            collation: None,
        });
    } else {
        // A ROWS / GROUPS offset has to be an integer. MustBeInt converts
        // it, or jumps to our own Halt if it can't — so the user gets
        // SQLite's specific frame-offset error rather than a generic
        // "datatype mismatch".
        program.emit_insn(Insn::MustBeInt {
            reg: offset_reg,
            target_pc: Some(label_halt),
        });
    }
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_zero,
    });
    program.emit_insn(Insn::Ge {
        lhs: offset_reg,
        rhs: reg_zero,
        target_pc: label_ok,
        flags: crate::vdbe::insn::CmpInsFlags::default()
            .with_affinity(crate::vdbe::affinity::Affinity::Numeric),
        collation: None,
    });
    program.preassign_label_to_next_insn(label_halt);
    let msg = match (mode, side) {
        (FrameMode::Range, WindowFrameBoundSide::Start) => {
            "frame starting offset must be a non-negative number"
        }
        (FrameMode::Range, WindowFrameBoundSide::End) => {
            "frame ending offset must be a non-negative number"
        }
        (_, WindowFrameBoundSide::Start) => "frame starting offset must be a non-negative integer",
        (_, WindowFrameBoundSide::End) => "frame ending offset must be a non-negative integer",
    };
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: msg.to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(label_ok);
}

pub struct EmitWindow;
impl EmitWindow {
    pub fn init<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        window: &'a Window,
        plan: &SelectPlan,
        result_columns: &'a [ResultSetColumn],
        order_by: &'a [(Box<Expr>, SortOrder, Option<turso_parser::ast::NullsOrder>)],
    ) -> crate::Result<()> {
        let joined_tables = &plan.joined_tables();
        turso_assert_eq!(joined_tables.len(), 1, "expected only one joined table");

        let src_table = &joined_tables[0];
        let reg_src_columns_start =
            if let Table::FromClauseSubquery(from_clause_subquery) = &src_table.table {
                from_clause_subquery
                    .result_columns_start_reg
                    .expect("Subquery result_columns_start_reg must be set")
            } else {
                panic!(
                    "expected source table to be a FromClauseSubquery, but got: {:?}",
                    src_table.table
                );
            };
        let src_columns = src_table.columns().try_to_vec()?;
        let src_column_count = src_columns.len();
        let window_name = window.name.clone().expect("window name is missing");
        let partition_by_len = window
            .deduplicated_partition_by_len
            .unwrap_or(window.partition_by.len());
        let order_by_len = window.order_by.len();
        let window_function_count = window.functions.len();

        // An ephemeral table used to buffer rows for the current frame
        let buffer_table = Arc::new(BTreeTable::new(
            0,
            // TODO: Generating the name this way may cause collisions with real tables in the
            //  attached database. Other ephemeral tables are created similarly, so it's left
            //  as-is for now. Ideally, there should be a way to mark tables as ephemeral so
            //  they can be handled differently from regular tables.
            format!("buffer_table_{window_name}"),
            crate::alloc::vec![],
            src_columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        // `csr_start` follows the start of the frame. Only needed when the
        // frame start can move off the first row of the partition (ntile,
        // percent_rank, cume_dist); AggInverse reads from this cursor as
        // rows drop off the start of the frame.
        let has_moving_start = !matches!(
            window.frame.start,
            crate::translate::plan::FrameBoundary::UnboundedPreceding
        );
        // `csr_app` either seeks positional values or scans the inclusive
        // frame bounds for an explicit EXCLUDE clause.
        let needs_csr_app = window.frame.exclude.is_some()
            || window.functions.iter().any(|f| {
                matches!(
                    &f.func,
                    AccumulatorFunc::Window(
                        WindowFunc::FirstValue
                            | WindowFunc::NthValue
                            | WindowFunc::Lag
                            | WindowFunc::Lead
                    ),
                )
            });
        // Frame-index counters for the positional lookups; see
        // `WindowFrameTracking::Positional`. Only first_value / nth_value
        // consult them — lag / lead seek relative to the emitted row's own
        // rowid, not the frame bounds. Zeroed per partition alongside the
        // accumulator reset, so no init is needed here.
        let positional = window.functions.iter().any(|f| {
            matches!(
                &f.func,
                AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue),
            )
        });
        let frame_tracking =
            prepare_window_frame_tracking(program, window.frame.exclude.is_some(), positional);
        let buffer = open_window_buffer(program, buffer_table, has_moving_start, needs_csr_app);
        // Window function processing is similar to aggregation processing in how results are mapped
        // to registers. Each function expression is stored in `expr_to_reg_cache` along with its
        // result register. Later, when bytecode generation encounters the expression, the value is
        // copied from the result register instead of generating code to evaluate the expression.
        let reg_acc_start = program.alloc_registers(window_function_count);
        let reg_acc_result_start = program.alloc_registers(window_function_count);
        let functions = prepare_window_functions(
            program,
            window,
            &plan.table_references,
            &t_ctx.resolver,
            reg_acc_result_start,
        )?;
        for (func, runtime) in window.functions.iter().zip(&functions) {
            // Cache by the rewritten form (when available) so lookups against the
            // result-column / ORDER-BY expressions — which were rewritten to
            // reference this window's subquery — find the cached register.
            t_ctx.resolver.cache_expr_reg(
                std::borrow::Cow::Borrowed(func.current_expr()),
                runtime.result_register,
                false,
                None,
            );
        }

        // The same approach applies to expressions referencing the subquery (columns).
        // Instead of reading directly from the subquery, we redirect them to the corresponding
        // result registers. This is necessary because rows are buffered in an ephemeral table and
        // returned according to the rules of the window definition.
        let expressions_referencing_subquery = collect_expressions_referencing_subquery(
            result_columns,
            order_by,
            &src_table.internal_id,
        )?;
        let reg_col_start = program.alloc_registers(expressions_referencing_subquery.len());
        for (i, (expr, _)) in expressions_referencing_subquery.iter().enumerate() {
            t_ctx.resolver.cache_scalar_expr_reg(
                std::borrow::Cow::Borrowed(expr),
                reg_col_start + i,
                false,
                &plan.table_references,
            )?;
        }

        let input_state = prepare_window_input_state(program, partition_by_len);
        let peer_state = prepare_window_peer_state(
            program,
            order_by_len,
            window.frame.mode != turso_parser::ast::FrameMode::Rows,
            has_moving_start,
        );

        t_ctx.meta_window = Some(WindowMetadata {
            labels: WindowLabels {
                flush_buffer: program.allocate_label(),
                row_output: program.allocate_label(),
                window_processing_end: program.allocate_label(),
            },
            frame: legacy_window_frame_shape(&window.frame),
            registers: WindowRegisters {
                rowid: input_state.rowid,
                partition_start: input_state.previous_partition,
                acc_start: reg_acc_start,
                flush_buffer_return_offset: input_state.flush_return,
                src_columns_start: reg_src_columns_start,
                result_columns_start: reg_col_start,
                new_order_by_columns_start: peer_state.current,
                // Peer tracking only runs under RANGE / GROUPS — under
                // ROWS a row is never compared against its neighbours to
                // see if they are peers, so all of these stay `None`.
                // Mirrors SQLite's `regPeer` allocation gate at
                // window.c:2892-2896.
                source_peer_values: peer_state.previous_input,
                cursor_peer_values: peer_state.cursors,
                frame_offsets: prepare_window_frame_offsets(
                    program,
                    matches!(
                        window.frame.start,
                        crate::translate::plan::FrameBoundary::Preceding(_)
                            | crate::translate::plan::FrameBoundary::Following(_)
                    ),
                    matches!(
                        window.frame.end,
                        crate::translate::plan::FrameBoundary::Preceding(_)
                            | crate::translate::plan::FrameBoundary::Following(_)
                    ),
                ),
                frame_tracking,
                row_output_return: program.alloc_register(),
            },
            cursors: buffer.cursors,
            src_column_count,
            expressions_referencing_subquery,
            buffer_table_name: buffer.table.name.clone(),
            functions,
        });

        Ok(())
    }
    /// Emits the per-source-row body for window processing.
    ///
    /// This is the Rust port of the main loop in SQLite's
    /// `sqlite3WindowCodeStep` (`window.c:2786-3037`). The three
    /// operations that move the frame forward — adding a row to the
    /// running totals, emitting a row's result, and removing a row from
    /// the totals — are emitted in one of three orders, chosen by the
    /// frame's bounds: a `FOLLOWING` start, a `PRECEDING` end, or anything
    /// else. This matches SQLite's three cases at window.c:2987-3037.
    ///
    /// Pseudocode for the most common shape (`UNBOUNDED PRECEDING TO
    /// CURRENT ROW`):
    ///
    /// ```text
    ///   load ORDER BY columns into newPeer regs
    ///   if partition_by_cols changed:
    ///       Gosub flush_partition
    ///       Null rowid_reg                 ; marks "first row of new partition"
    ///       Copy partition_by_cols → prev_partition_regs
    ///
    ///   if rowid_reg is null:                       ; FIRST ROW OF PARTITION
    ///       Copy newPeer → prevPeer
    ///       Null accumulators
    ///       Insert row into ephemeral via csr_write
    ///       Rewind csr_current, csr_end, csr_start  ; position at row 1
    ///       Goto loop_end
    ///   else:                                       ; SUBSEQUENT ROW
    ///       Insert row into ephemeral via csr_write
    ///       if RANGE/GROUPS and newPeer == prevPeer:
    ///           Goto loop_end                       ; same group: just buffer, process later
    ///       emit_window_op AGGSTEP        ; advance csr_end through peer rows; AggStep per row
    ///       emit_window_op RETURN_ROW     ; advance csr_current; emit each row
    ///       (emit_window_op AGGINVERSE is a no-op for UNBOUNDED start)
    ///       Copy newPeer → prevPeer                 ; RANGE/GROUPS only
    ///   loop_end:
    /// ```
    ///
    /// Example — `row_number() OVER (ORDER BY salary DESC)` over 3 rows:
    /// the source coroutine yields each row, this body inserts it, advances
    /// `csr_end` (AggStep increments row_number's counter) and `csr_current`
    /// (RETURN_ROW emits the previous row with its accumulated counter).
    /// The very first row's emission is deferred to the flush subroutine
    /// because `csr_current` was just rewound and there's no preceding row
    /// to emit yet.
    pub fn emit_window_step(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx,
        plan: &SelectPlan,
    ) -> crate::Result<()> {
        let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
        let window = plan.window.as_ref().expect("missing window");

        let labels = meta.labels;
        let registers = meta.registers;
        let cursors = meta.cursors;
        let frame = meta.frame;
        let src_column_count = meta.src_column_count;
        let input = WindowBufferInput {
            start: registers.src_columns_start,
            count: src_column_count,
        };
        let buffer_table_name = meta.buffer_table_name.clone();

        emit_window_buffer_step(
            program,
            frame,
            cursors,
            registers.frame_offsets,
            registers.rowid,
            |program, phase| match phase {
                WindowStreamPhase::PrepareInput => {
                    emit_load_order_by_columns(program, window, &registers, input)?;
                    emit_flush_buffer_if_new_partition(program, &labels, &registers, window, plan)
                }
                WindowStreamPhase::FirstRow {
                    step_end: label_step_end,
                } => {
                    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
                    // --- FIRST ROW OF PARTITION ---
                    emit_window_peer_seed(
                        program,
                        WindowPeerState {
                            key_count: window.order_by.len(),
                            current: registers.new_order_by_columns_start,
                            previous_input: registers.source_peer_values,
                            cursors: registers.cursor_peer_values,
                        },
                    );
                    emit_window_partition_reset(
                        program,
                        registers.acc_start,
                        &meta.functions,
                        registers.frame_tracking,
                    );
                    // The same register holds the offset for every partition, and the
                    // IfPos delays decrement it as they run. Re-evaluate it here at the
                    // start of each partition: setting it just once up front would
                    // leave the decremented (wrong) value in place from the second
                    // partition on. Mirrors SQLite's `regStart` init at `window.c:2942`.
                    if let Some(start_offset_reg) = registers.frame_offsets.start() {
                        let offset_expr = match &window.frame.start {
                            crate::translate::plan::FrameBoundary::Preceding(expr)
                            | crate::translate::plan::FrameBoundary::Following(expr) => expr,
                            _ => unreachable!(
                    "start_offset_reg is only allocated when frame.start is Preceding/Following"
                ),
                        };
                        emit_legacy_window_frame_offset(
                            program,
                            plan,
                            &t_ctx.resolver,
                            offset_expr,
                            start_offset_reg,
                            frame.mode,
                            WindowFrameBoundSide::Start,
                        )?;
                    }
                    if let Some(end_offset_reg) = registers.frame_offsets.end() {
                        let offset_expr = match &window.frame.end {
                            crate::translate::plan::FrameBoundary::Preceding(expr)
                            | crate::translate::plan::FrameBoundary::Following(expr) => expr,
                            _ => unreachable!(
                    "end_offset_reg is only allocated when frame.end is Preceding/Following"
                ),
                        };
                        emit_legacy_window_frame_offset(
                            program,
                            plan,
                            &t_ctx.resolver,
                            offset_expr,
                            end_offset_reg,
                            frame.mode,
                            WindowFrameBoundSide::End,
                        )?;
                    }
                    emit_window_first_row_frame(
                        program,
                        WindowFirstRowFrame {
                            frame,
                            offsets: registers.frame_offsets,
                            cursors,
                            input,
                            rowid: registers.rowid,
                            table_name: &buffer_table_name,
                            step_end: label_step_end,
                        },
                        |program, output| match output {
                            WindowEmptyFrameOutput::Aggregate => {
                                if !frame.has_exclude {
                                    let meta = t_ctx
                                        .meta_window
                                        .as_ref()
                                        .expect("missing window metadata");
                                    emit_window_aggregate_results(
                                        program,
                                        registers.acc_start,
                                        &meta.functions,
                                        WindowAggregateResultMode::Value,
                                    );
                                }
                                Ok(())
                            }
                            WindowEmptyFrameOutput::Row => {
                                emit_return_one_row(program, t_ctx, plan)
                            }
                        },
                    )?;

                    Ok(())
                }
                WindowStreamPhase::SubsequentRow {
                    step_end: label_step_end,
                } => {
                    emit_window_subsequent_row(
                        program,
                        cursors,
                        input,
                        registers.rowid,
                        &buffer_table_name,
                        frame.mode,
                        WindowPeerState {
                            key_count: window.order_by.len(),
                            current: registers.new_order_by_columns_start,
                            previous_input: registers.source_peer_values,
                            cursors: registers.cursor_peer_values,
                        },
                        order_by_key_info(window, &plan.table_references),
                        label_step_end,
                    )?;

                    Ok(())
                }
                WindowStreamPhase::Operation { op, countdown } => {
                    emit_window_op(program, t_ctx, plan, op, countdown, None, false)
                }
            },
            |program, comparison, first, offset, second, target| {
                emit_legacy_window_range_test(
                    program, plan, comparison, first, offset, second, target,
                )
            },
        )
    }
}

/// Representation-specific work at each point in the streaming loop.
pub(crate) enum WindowStreamPhase {
    PrepareInput,
    FirstRow {
        step_end: BranchOffset,
    },
    SubsequentRow {
        step_end: BranchOffset,
    },
    Operation {
        op: WindowOp,
        countdown: Option<usize>,
    },
}

/// Buffer one input row and run frame operations in legacy order.
pub(crate) fn emit_window_buffer_step(
    program: &mut ProgramBuilder,
    frame: WindowFrameShape,
    cursors: WindowCursors,
    offsets: WindowFrameOffsets,
    rowid: usize,
    mut emit_phase: impl FnMut(&mut ProgramBuilder, WindowStreamPhase) -> Result<()>,
    mut emit_range: impl FnMut(
        &mut ProgramBuilder,
        RangeCmp,
        CursorID,
        usize,
        CursorID,
        BranchOffset,
    ) -> Result<()>,
) -> Result<()> {
    emit_phase(program, WindowStreamPhase::PrepareInput)?;
    let label_subsequent = program.allocate_label();
    let label_step_end = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: rowid,
        target_pc: label_subsequent,
    });
    emit_phase(
        program,
        WindowStreamPhase::FirstRow {
            step_end: label_step_end,
        },
    )?;
    program.preassign_label_to_next_insn(label_subsequent);
    emit_phase(
        program,
        WindowStreamPhase::SubsequentRow {
            step_end: label_step_end,
        },
    )?;
    // Pick the order to run the three operations in, based on the
    // frame's bounds — one of SQLite's three `sqlite3WindowCodeStep`
    // cases (window.c:2987-3037).
    let is_range = frame.mode == FrameMode::Range;
    let end_is_unbounded = frame.end == WindowFrameEdge::UnboundedFollowing;
    if frame.start == WindowFrameEdge::Following {
        // Pattern A — the frame starts after the current row (`<expr>
        // FOLLOWING` start), e.g. `ROWS BETWEEN 1 FOLLOWING AND 3
        // FOLLOWING` (window.c:2987-3002). Every row is added to the
        // totals as soon as we reach it (AGGSTEP, no delay). Producing
        // a row's result waits until we have read M more rows past it
        // (M = the end offset); dropping rows that have fallen off the
        // start waits a further N rows (N = the start offset).
        emit_phase(
            program,
            WindowStreamPhase::Operation {
                op: WindowOp::AggStep,
                countdown: None,
            },
        )?;
        if !end_is_unbounded {
            if is_range {
                let label_done = program.allocate_label();
                let label_loop = program.allocate_label();
                program.preassign_label_to_next_insn(label_loop);
                emit_range(
                    program,
                    RangeCmp::Ge,
                    cursors.csr_current,
                    offsets
                        .end()
                        .expect("bounded RANGE end has an offset register"),
                    cursors.csr_end,
                    label_done,
                )?;
                emit_phase(
                    program,
                    WindowStreamPhase::Operation {
                        op: WindowOp::AggInverse,
                        countdown: offsets.start(),
                    },
                )?;
                emit_phase(
                    program,
                    WindowStreamPhase::Operation {
                        op: WindowOp::ReturnRow,
                        countdown: None,
                    },
                )?;
                program.emit_insn(Insn::Goto {
                    target_pc: label_loop,
                });
                program.preassign_label_to_next_insn(label_done);
            } else {
                emit_phase(
                    program,
                    WindowStreamPhase::Operation {
                        op: WindowOp::ReturnRow,
                        countdown: offsets.end(),
                    },
                )?;
                emit_phase(
                    program,
                    WindowStreamPhase::Operation {
                        op: WindowOp::AggInverse,
                        countdown: offsets.start(),
                    },
                )?;
            }
        }
    } else if frame.end == WindowFrameEdge::Preceding {
        // Pattern B — the frame ends before the current row (`<expr>
        // PRECEDING` end), e.g. `ROWS BETWEEN UNBOUNDED PRECEDING AND
        // 2 PRECEDING` (window.c:3004-3009). A row's result can be
        // produced right away (RETURN_ROW, no delay). Adding rows to
        // the totals waits M rows (M = the end offset), because the
        // last row of the frame sits M rows behind the current one.
        emit_phase(
            program,
            WindowStreamPhase::Operation {
                op: WindowOp::AggStep,
                countdown: offsets.end(),
            },
        )?;
        // When a RANGE frame has both bounds PRECEDING, drop rows off
        // the start (AGGINVERSE) before producing the result
        // (RETURN_ROW). Both cursors already sit behind the current
        // row; this order stops the start cursor from moving past the
        // end cursor when the two offsets differ. SQLite's `bRPS` at
        // window.c:3005.
        let inverse_before_return = is_range && frame.start == WindowFrameEdge::Preceding;
        if inverse_before_return {
            emit_phase(
                program,
                WindowStreamPhase::Operation {
                    op: WindowOp::AggInverse,
                    countdown: offsets.start(),
                },
            )?;
        }
        emit_phase(
            program,
            WindowStreamPhase::Operation {
                op: WindowOp::ReturnRow,
                countdown: None,
            },
        )?;
        if !inverse_before_return {
            emit_phase(
                program,
                WindowStreamPhase::Operation {
                    op: WindowOp::AggInverse,
                    countdown: offsets.start(),
                },
            )?;
        }
    } else {
        // Pattern C — everything else (window.c:3010-3037).
        emit_phase(
            program,
            WindowStreamPhase::Operation {
                op: WindowOp::AggStep,
                countdown: None,
            },
        )?;
        if !end_is_unbounded {
            let range_loop = (is_range && offsets.end().is_some()).then(|| {
                let label = program.allocate_label();
                program.preassign_label_to_next_insn(label);
                label
            });
            let range_done = range_loop.map(|_| program.allocate_label());
            if let Some(label_done) = range_done {
                emit_range(
                    program,
                    RangeCmp::Ge,
                    cursors.csr_current,
                    offsets.end().expect("checked above"),
                    cursors.csr_end,
                    label_done,
                )?;
            }
            // A `<expr> FOLLOWING` end delays both producing the
            // result (RETURN_ROW) and dropping rows off the start
            // (AGGINVERSE) by M rows. The per-op delays inside
            // `emit_window_op` hold back one op at a time; this one
            // skips both together. SQLite emits it inline at
            // window.c:3028-3034.
            let label_skip_pair =
                (!is_range)
                    .then_some(offsets.end())
                    .flatten()
                    .map(|end_offset_reg| {
                        let label = program.allocate_label();
                        program.emit_insn(Insn::IfPos {
                            reg: end_offset_reg,
                            target_pc: label,
                            decrement_by: 1,
                        });
                        label
                    });
            emit_phase(
                program,
                WindowStreamPhase::Operation {
                    op: WindowOp::ReturnRow,
                    countdown: None,
                },
            )?;
            // AGGINVERSE does nothing for an UNBOUNDED PRECEDING start
            // — no row ever leaves the frame, so `emit_window_op`
            // early-returns (window.c:2252-2257). For an `N PRECEDING`
            // start it holds off until the frame has grown to N+1 rows,
            // then drops one row for each new row that joins
            // (window.c:3032-3033).
            emit_phase(
                program,
                WindowStreamPhase::Operation {
                    op: WindowOp::AggInverse,
                    countdown: offsets.start(),
                },
            )?;
            if let Some(range_loop) = range_loop {
                program.emit_insn(Insn::Goto {
                    target_pc: range_loop,
                });
                program.preassign_label_to_next_insn(
                    range_done.expect("range loop and done labels are paired"),
                );
            }
            if let Some(label) = label_skip_pair {
                program.preassign_label_to_next_insn(label);
            }
        }
    }

    program.preassign_label_to_next_insn(label_step_end);

    Ok(())
}

fn alloc_optional_registers(program: &mut ProgramBuilder, count: usize) -> Option<usize> {
    if count > 0 {
        Some(program.alloc_registers(count))
    } else {
        None
    }
}

fn collect_expressions_referencing_subquery<'a>(
    result_columns: &'a [ResultSetColumn],
    order_by: &'a [(Box<Expr>, SortOrder, Option<turso_parser::ast::NullsOrder>)],
    subquery_id: &TableInternalId,
) -> crate::Result<Vec<(&'a Expr, usize)>> {
    let mut expressions_referencing_subquery: Vec<(&'a Expr, usize)> = Vec::new();

    for root_expr in result_columns
        .iter()
        .map(|col| &col.expr)
        .chain(order_by.iter().map(|(e, _, _)| e.as_ref()))
    {
        walk_expr(
            root_expr,
            &mut |expr: &Expr| -> crate::Result<WalkControl> {
                match expr {
                    Expr::FunctionCall { filter_over, .. }
                    | Expr::FunctionCallStar { filter_over, .. } => {
                        if filter_over.over_clause.is_some() {
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    Expr::Column { column, table, .. } => {
                        turso_assert_eq!(
                            table,
                            subquery_id,
                            "only subquery columns can be referenced"
                        );
                        if expressions_referencing_subquery
                            .iter()
                            .all(|(_, existing_column)| column != existing_column)
                        {
                            expressions_referencing_subquery.push((expr, *column));
                        }
                    }
                    _ => {}
                };
                Ok(WalkControl::Continue)
            },
        )?;
    }

    Ok(expressions_referencing_subquery)
}

fn emit_flush_buffer_if_new_partition(
    program: &mut ProgramBuilder,
    labels: &WindowLabels,
    registers: &WindowRegisters,
    window: &Window,
    plan: &SelectPlan,
) -> Result<()> {
    if let Some(reg_partition_start) = registers.partition_start {
        // Compare the first `deduplicated_partition_by_len` source columns with the saved
        // partition keys. If they differ, this row starts a new partition and we flush the buffer.
        let partition_by_len = window
            .deduplicated_partition_by_len
            .expect("deduplicated_partition_by_len must exist");

        let partition_key_info = (0..partition_by_len).map(|i| {
            // After rewriting, partition_by entries are Expr::Column references to the
            // subquery. Duplicates reference the same column index, so we find the entry
            // that references column i (the i-th unique partition column) to get the
            // correct collation.
            let expr = window
                .partition_by
                .iter()
                .find(|e| matches!(e, Expr::Column { column, .. } if *column == i))
                .unwrap_or(&window.partition_by[i]);
            Ok(KeyInfo {
                sort_order: SortOrder::Asc,
                collation: get_collseq_from_expr(expr, &plan.table_references)?.unwrap_or_default(),
                nulls_order: None,
            })
        });
        emit_window_partition_change(
            program,
            registers.src_columns_start,
            WindowPartitionState {
                previous_keys: reg_partition_start,
                rowid: registers.rowid,
                flush_return: registers.flush_buffer_return_offset,
            },
            labels.flush_buffer,
            partition_key_info,
        )?;
    }

    Ok(())
}

fn emit_load_order_by_columns(
    program: &mut ProgramBuilder,
    window: &Window,
    registers: &WindowRegisters,
    input: WindowBufferInput,
) -> Result<()> {
    if let Some(reg_new_order_by_columns_start) = registers.new_order_by_columns_start {
        // Source columns are deduplicated and may appear in a different order than
        // the ORDER BY terms. Therefore, we must restore the original ORDER BY layout
        // here by copying the values into an array of registers.
        let slots = window.order_by.iter().map(|(expr, _, _)| {
            let Expr::Column { column, .. } = expr else {
                unreachable!("expected Column, got {:?}", expr)
            };
            *column
        });
        emit_window_key_gather(program, input, slots, reg_new_order_by_columns_start)?;
    }
    Ok(())
}

/// The three operations that move the frame forward, one step at a time.
/// Each reads the row a cursor points at, updates every window function
/// with it, then moves that cursor on. Mirror SQLite's `WINDOW_AGGSTEP`,
/// `WINDOW_RETURN_ROW`, and `WINDOW_AGGINVERSE` (`window.c:1765-1773`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowOp {
    /// A new row has joined the frame. Read it from `csr_end`, add it to
    /// every function's running total (xStep), then move `csr_end` on.
    AggStep,
    /// The frame for the current row is complete. Compute every function's
    /// result for it (xValue) and send the row at `csr_current` to the
    /// outer query, then move `csr_current` on.
    ReturnRow,
    /// A row has dropped out of the frame. Read it from `csr_start`,
    /// subtract it from every function's running total (xInverse), then
    /// move `csr_start` on. Never happens when the frame always starts at
    /// the first row of the partition (UNBOUNDED PRECEDING): no row ever
    /// leaves, so there is nothing to subtract.
    AggInverse,
}

/// Once a row has been used by a given op it can be deleted from the
/// buffer to free memory sooner. This says after which op that is safe;
/// `None` means keep every row until flush. Deleting early never changes
/// results. Mirrors SQLite's `eDelete` (window.c:2845-2869), applied at
/// the `OP_Delete` in `emit_window_op`:
///
/// - FOLLOWING start, offset provably > 0, non-RANGE → after RETURN_ROW.
///   The frame starts after the current row, so once a row has been
///   output it can never fall inside any later row's frame.
/// - UNBOUNDED PRECEDING start:
///   - a `csr_app` function needs every row (`window_cache_frame`) → never.
///   - N PRECEDING end, offset provably > 0, non-RANGE → after AGGSTEP.
///   - otherwise → after RETURN_ROW.
/// - CURRENT ROW / N PRECEDING start → after AGGINVERSE. Safe with
///   first_value / nth_value: only rows that have already left the frame
///   are deleted, and those functions only ever look up rows still in it.
fn window_delete_op(window: &Window) -> Option<WindowOp> {
    use crate::translate::plan::FrameBoundary;
    window_delete_op_for_frame(
        legacy_window_frame_shape(&window.frame),
        window_cache_frame(window),
        matches!(&window.frame.start, FrameBoundary::Following(expr) if window_expr_gt_zero(expr)),
        matches!(&window.frame.end, FrameBoundary::Preceding(expr) if window_expr_gt_zero(expr)),
    )
}

/// Choose when a buffered row is no longer needed, using resolved frame facts.
pub(crate) fn window_delete_op_for_frame(
    frame: WindowFrameShape,
    must_keep_rows: bool,
    start_gt_zero: bool,
    end_gt_zero: bool,
) -> Option<WindowOp> {
    let is_range = frame.mode == FrameMode::Range;
    match frame.start {
        WindowFrameEdge::Following => (!is_range && start_gt_zero).then_some(WindowOp::ReturnRow),
        WindowFrameEdge::UnboundedPreceding => {
            if must_keep_rows {
                None
            } else if frame.end == WindowFrameEdge::Preceding {
                (!is_range && end_gt_zero).then_some(WindowOp::AggStep)
            } else {
                Some(WindowOp::ReturnRow)
            }
        }
        WindowFrameEdge::CurrentRow | WindowFrameEdge::Preceding => Some(WindowOp::AggInverse),
        WindowFrameEdge::UnboundedFollowing => {
            unreachable!("UNBOUNDED FOLLOWING can never be a frame start")
        }
    }
}

/// Whether we must keep every row of the partition in the buffer until the
/// end, rather than deleting rows as the frame moves past them. This is
/// needed when a function might reach out to any row at output time —
/// first_value, nth_value, lag and lead all look up a row by position — or
/// when an EXCLUDE clause forces us to re-scan the frame. Mirrors SQLite's
/// `windowCacheFrame` (window.c:2031-2045).
fn window_cache_frame(window: &Window) -> bool {
    window.frame.exclude.is_some()
        || window.functions.iter().any(|f| {
            matches!(
                &f.func,
                AccumulatorFunc::Window(
                    WindowFunc::FirstValue
                        | WindowFunc::NthValue
                        | WindowFunc::Lag
                        | WindowFunc::Lead
                ),
            )
        })
}

/// Whether a frame offset is a literal provably greater than zero. Used
/// only to decide whether `window_delete_op` may delete rows early: a
/// `N FOLLOWING` (N > 0) start can delete rows once they are output, a
/// `0 FOLLOWING` start can't. Returning `false` is always correct — it
/// just keeps rows until flush — so this only frees memory sooner, never
/// changes results. Mirrors SQLite's `windowExprGtZero`
/// (window.c:2439-2449).
fn window_expr_gt_zero(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(turso_parser::ast::Literal::Numeric(s)) if window_numeric_offset_gt_zero(s))
}

/// Match legacy integer truncation when deciding whether an offset permits deletion.
pub(crate) fn window_numeric_offset_gt_zero(value: &str) -> bool {
    if let Ok(n) = value.parse::<i64>() {
        return n > 0;
    }
    if let Ok(f) = value.parse::<f64>() {
        return f as i64 > 0;
    }
    false
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowAggregateResultMode {
    Value,
    Finalize,
}

/// Turn each function's running total into the value to output for the
/// current row, writing it into that function's result register, just
/// before the row is emitted (RETURN_ROW). `Finalize` consumes and clears
/// the running total once at the very end; `Value` reads it without
/// disturbing it as the frame slides. first_value / nth_value / lag / lead
/// are the exception: they have no running total to read, so their result is
/// filled in separately — by looking up a specific row — inside
/// `emit_return_one_row`. Mirrors SQLite's `windowAggFinal(p, 0)`
/// (window.c:1777-1808).
pub(crate) fn emit_window_aggregate_results<E>(
    program: &mut ProgramBuilder,
    accumulator_start: usize,
    functions: &[WindowFunctionRuntime<E>],
    mode: WindowAggregateResultMode,
) {
    for (ordinal, function) in functions.iter().enumerate() {
        let positional = matches!(
            &function.function,
            AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue)
        );
        let always_lookup = matches!(
            &function.function,
            AccumulatorFunc::Window(WindowFunc::Lag | WindowFunc::Lead)
        );
        if always_lookup || (positional && mode == WindowAggregateResultMode::Value) {
            continue;
        }
        let acc_reg = accumulator_start + ordinal;
        let result_reg = function.result_register;
        if let Some(state) = function.minmax {
            debug_assert!(
                mode == WindowAggregateResultMode::Value,
                "EXCLUDE frames do not use min/max indexes"
            );
            let label_empty = program.allocate_label();
            program.emit_insn(Insn::Null {
                dest: result_reg,
                dest_end: None,
            });
            program.emit_insn(Insn::Last {
                cursor_id: state.cursor,
                pc_if_empty: label_empty,
            });
            program.emit_insn(Insn::Column {
                cursor_id: state.cursor,
                column: 0,
                dest: result_reg,
                default: None,
            });
            program.preassign_label_to_next_insn(label_empty);
            continue;
        }
        match mode {
            WindowAggregateResultMode::Finalize => {
                program.emit_insn(Insn::AggFinal {
                    register: acc_reg,
                    func: function.function.clone(),
                });
                program.emit_insn(Insn::Copy {
                    src_reg: acc_reg,
                    dst_reg: result_reg,
                    extra_amount: 0,
                });
                program.emit_insn(Insn::Null {
                    dest: acc_reg,
                    dest_end: None,
                });
            }
            WindowAggregateResultMode::Value => {
                program.emit_insn(Insn::AggValue {
                    acc_reg,
                    dest_reg: result_reg,
                    func: function.function.clone(),
                });
            }
        }
    }
}

/// Recompute every function from scratch over the current frame. Used when
/// the window has an EXCLUDE clause: because some rows inside the frame are
/// excluded, we can't keep a simple running total, so for each output row
/// we re-add the rows the frame currently spans (the
/// tracked start-to-end rowid range), skipping the excluded
/// ones. Re-adding from scratch also sidesteps needing a subtract-a-row
/// step for aggregates that don't have one, such as min / max and
/// group_concat.
fn emit_window_full_scan(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let exclude = window
        .frame
        .exclude
        .as_ref()
        .expect("full frame scan requires an explicit EXCLUDE clause");
    let function_count = window.functions.len();
    emit_window_full_scan_runtime(
        program,
        cursors,
        registers.frame_tracking,
        registers.acc_start,
        function_count,
        exclude,
        window.order_by.len(),
        |program, cursor, target| {
            emit_window_cursor_keys(program, cursor, window_order_by_columns(window), target)
        },
        |program, left, right, equal, different| {
            emit_window_key_compare(
                program,
                left,
                right,
                order_by_key_info(window, &plan.table_references),
                equal,
                different,
            )
            .map(|_| ())
        },
        |program, phase| match phase {
            WindowScanPhase::Step(cursor) => emit_function_step(program, t_ctx, plan, cursor),
            WindowScanPhase::Finalize => {
                let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
                emit_window_aggregate_results(
                    program,
                    registers.acc_start,
                    &meta.functions,
                    WindowAggregateResultMode::Finalize,
                );
                Ok(())
            }
        },
    )
}

#[derive(Clone, Copy)]
pub(crate) enum WindowScanPhase {
    Step(CursorID),
    Finalize,
}

/// Recompute one output row's aggregates over the included frame rows.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_full_scan_runtime(
    program: &mut ProgramBuilder,
    cursors: WindowCursors,
    tracking: WindowFrameTracking,
    accumulator_start: usize,
    function_count: usize,
    exclude: &turso_parser::ast::FrameExclude,
    order_by_len: usize,
    mut emit_keys: impl FnMut(&mut ProgramBuilder, CursorID, usize) -> usize,
    emit_compare: impl FnOnce(
        &mut ProgramBuilder,
        usize,
        usize,
        BranchOffset,
        BranchOffset,
    ) -> Result<()>,
    mut emit_phase: impl FnMut(&mut ProgramBuilder, WindowScanPhase) -> Result<()>,
) -> Result<()> {
    let WindowFrameTracking::Excluded {
        start_rowid: frame_start_rowid,
        end_rowid: frame_end_rowid,
    } = tracking
    else {
        panic!("EXCLUDE frame requires rowid trackers");
    };
    let scan_cursor = cursors
        .csr_app
        .expect("EXCLUDE frame requires a full-scan cursor");

    let current_rowid = program.alloc_register();
    let scan_rowid = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: cursors.csr_current,
        dest: current_rowid,
    });

    let compare_peers = matches!(
        exclude,
        turso_parser::ast::FrameExclude::Group | turso_parser::ast::FrameExclude::Ties
    );
    let current_peer =
        (compare_peers && order_by_len > 0).then(|| program.alloc_registers(order_by_len));
    let scan_peer =
        (compare_peers && order_by_len > 0).then(|| program.alloc_registers(order_by_len));
    if let Some(current_peer) = current_peer {
        let count = emit_keys(program, cursors.csr_current, current_peer);
        turso_assert!(count == order_by_len, "window peer key width changed");
    }

    program.emit_insn(Insn::Null {
        dest: accumulator_start,
        dest_end: Some(accumulator_start + function_count - 1),
    });

    let label_break = program.allocate_label();
    let label_loop = program.allocate_label();
    let label_next = program.allocate_label();
    let label_step = program.allocate_label();
    program.emit_insn(Insn::SeekGE {
        is_index: false,
        cursor_id: scan_cursor,
        start_reg: frame_start_rowid,
        num_regs: 1,
        target_pc: label_break,
        eq_only: false,
        null_matching_mask: Default::default(),
    });
    program.preassign_label_to_next_insn(label_loop);
    program.emit_insn(Insn::RowId {
        cursor_id: scan_cursor,
        dest: scan_rowid,
    });
    program.emit_insn(Insn::Gt {
        lhs: scan_rowid,
        rhs: frame_end_rowid,
        target_pc: label_break,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });

    match exclude {
        turso_parser::ast::FrameExclude::NoOthers => {}
        turso_parser::ast::FrameExclude::CurrentRow => {
            program.emit_insn(Insn::Eq {
                lhs: current_rowid,
                rhs: scan_rowid,
                target_pc: label_next,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: None,
            });
        }
        turso_parser::ast::FrameExclude::Group | turso_parser::ast::FrameExclude::Ties => {
            if matches!(exclude, turso_parser::ast::FrameExclude::Ties) {
                program.emit_insn(Insn::Eq {
                    lhs: current_rowid,
                    rhs: scan_rowid,
                    target_pc: label_step,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            if order_by_len == 0 {
                program.emit_insn(Insn::Goto {
                    target_pc: label_next,
                });
            } else {
                let current_peer = current_peer.expect("allocated above");
                let scan_peer = scan_peer.expect("allocated above");
                let count = emit_keys(program, scan_cursor, scan_peer);
                turso_assert!(count == order_by_len, "window peer key width changed");
                emit_compare(program, current_peer, scan_peer, label_next, label_step)?;
            }
        }
    }

    program.preassign_label_to_next_insn(label_step);
    emit_phase(program, WindowScanPhase::Step(scan_cursor))?;
    program.preassign_label_to_next_insn(label_next);
    program.emit_insn(Insn::Next {
        cursor_id: scan_cursor,
        pc_if_next: label_loop,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(label_break);
    emit_phase(program, WindowScanPhase::Finalize)?;
    Ok(())
}

/// A comparison operator held as plain data. The RANGE boundary test
/// needs to reason about the operator before emitting it — flipping it
/// for DESC order (`>=` becomes `<=`, and so on) and branching on its
/// direction for the NULL cases — none of which is possible once it's a
/// built `Insn::Ge { .. }`. `emit_range_cmp` turns it into the real
/// instruction at the end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeCmp {
    Lt,
    Le,
    Gt,
    Ge,
}

fn emit_range_cmp(
    program: &mut ProgramBuilder,
    op: RangeCmp,
    lhs: usize,
    rhs: usize,
    target_pc: BranchOffset,
    flags: crate::vdbe::insn::CmpInsFlags,
    collation: Option<CollationSeq>,
) {
    let insn = match op {
        RangeCmp::Lt => Insn::Lt {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
        RangeCmp::Le => Insn::Le {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
        RangeCmp::Gt => Insn::Gt {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
        RangeCmp::Ge => Insn::Ge {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
    };
    program.emit_insn(insn);
}

/// Resolved buffer column and ordering rules for a bounded RANGE comparison.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowRangeKey {
    pub(crate) column: usize,
    pub(crate) sort_order: SortOrder,
    pub(crate) nulls_order: Option<turso_parser::ast::NullsOrder>,
    pub(crate) collation: CollationSeq,
}

fn emit_legacy_window_range_test(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    op: RangeCmp,
    csr1: CursorID,
    offset_reg: usize,
    csr2: CursorID,
    target_pc: BranchOffset,
) -> Result<()> {
    let window = plan.window.as_ref().expect("missing window");
    let [(order_expr, sort_order, nulls_order)] = window.order_by.as_slice() else {
        unreachable!("RANGE offsets require exactly one ORDER BY expression");
    };
    let Expr::Column { column, .. } = order_expr else {
        unreachable!("window ORDER BY expressions are buffer columns after rewrite");
    };
    let key = WindowRangeKey {
        column: *column,
        sort_order: *sort_order,
        nulls_order: *nulls_order,
        collation: get_collseq_from_expr(order_expr, &plan.table_references)?.unwrap_or_default(),
    };
    emit_window_range_test(program, key, op, csr1, offset_reg, csr2, target_pc)
}

/// Emit the value comparison a numeric RANGE offset turns on: jump to
/// `target_pc` when the two cursors' ORDER BY values, with `offset`
/// applied, satisfy `op`. Ascending order compares `csr1.value + offset`
/// against `csr2.value` — for `RANGE BETWEEN 10 PRECEDING`, this is what
/// decides whether a trailing row is still within 10 of the current one.
/// Descending order subtracts the offset and flips `op` instead.
/// Non-numeric values skip the arithmetic, and an explicit NULLS
/// FIRST/LAST is handled first. Mirrors SQLite's `windowCodeRangeTest`.
pub(crate) fn emit_window_range_test(
    program: &mut ProgramBuilder,
    key: WindowRangeKey,
    op: RangeCmp,
    csr1: CursorID,
    offset_reg: usize,
    csr2: CursorID,
    target_pc: BranchOffset,
) -> Result<()> {
    let reg1 = program.alloc_register();
    let reg2 = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: csr1,
        column: key.column,
        dest: reg1,
        default: None,
    });
    program.emit_insn(Insn::Column {
        cursor_id: csr2,
        column: key.column,
        dest: reg2,
        default: None,
    });

    let (op, subtract) = if key.sort_order == SortOrder::Desc {
        (
            match op {
                RangeCmp::Ge => RangeCmp::Le,
                RangeCmp::Gt => RangeCmp::Lt,
                RangeCmp::Le => RangeCmp::Ge,
                RangeCmp::Lt => RangeCmp::Gt,
            },
            true,
        )
    } else {
        (op, false)
    };
    let collation = key.collation;
    let big_null = matches!(
        (key.sort_order, key.nulls_order),
        (SortOrder::Asc, Some(turso_parser::ast::NullsOrder::Last))
            | (SortOrder::Desc, Some(turso_parser::ast::NullsOrder::First))
    );
    let label_done = program.allocate_label();

    // NULLS sort at the high end here (NULLS LAST ascending, NULLS FIRST
    // descending), so a NULL value compares as greater than any real one.
    // Handle the NULL operands directly, since the arithmetic below can't.
    if big_null {
        let label_reg1_not_null = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: reg1,
            target_pc: label_reg1_not_null,
        });
        match op {
            RangeCmp::Ge => program.emit_insn(Insn::Goto { target_pc }),
            RangeCmp::Gt => program.emit_insn(Insn::NotNull {
                reg: reg2,
                target_pc,
            }),
            RangeCmp::Le => program.emit_insn(Insn::IsNull {
                reg: reg2,
                target_pc,
            }),
            RangeCmp::Lt => {}
        }
        program.emit_insn(Insn::Goto {
            target_pc: label_done,
        });
        program.preassign_label_to_next_insn(label_reg1_not_null);
        program.emit_insn(Insn::IsNull {
            reg: reg2,
            target_pc: if matches!(op, RangeCmp::Gt | RangeCmp::Ge) {
                label_done
            } else {
                target_pc
            },
        });
    }

    // Text and blobs sort at or above the empty string, so only numeric
    // values fall through to Add/Subtract. NULL arithmetic remains NULL.
    let reg_empty = program.alloc_register();
    let label_after_arithmetic = program.allocate_label();
    program.emit_insn(Insn::String8 {
        value: String::new(),
        dest: reg_empty,
    });
    program.emit_insn(Insn::Ge {
        lhs: reg1,
        rhs: reg_empty,
        target_pc: label_after_arithmetic,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });

    // SQLite performs this comparison before arithmetic in the two cases
    // where overflow to +/-infinity cannot change a successful result.
    if (op == RangeCmp::Ge && !subtract) || (op == RangeCmp::Le && subtract) {
        emit_range_cmp(
            program,
            op,
            reg1,
            reg2,
            target_pc,
            crate::vdbe::insn::CmpInsFlags::default(),
            Some(collation),
        );
    }
    if subtract {
        program.emit_insn(Insn::Subtract {
            lhs: reg1,
            rhs: offset_reg,
            dest: reg1,
        });
    } else {
        program.emit_insn(Insn::Add {
            lhs: reg1,
            rhs: offset_reg,
            dest: reg1,
        });
    }
    program.preassign_label_to_next_insn(label_after_arithmetic);
    emit_range_cmp(
        program,
        op,
        reg1,
        reg2,
        target_pc,
        crate::vdbe::insn::CmpInsFlags::default().null_eq(),
        Some(collation),
    );
    program.preassign_label_to_next_insn(label_done);
    Ok(())
}

/// Update frame bounds around one aggregate step or inverse.
/// EXCLUDE defers aggregation until output; positional counters advance after aggregation.
pub(crate) fn emit_window_tracked_aggregate(
    program: &mut ProgramBuilder,
    tracking: WindowFrameTracking,
    op: WindowOp,
    emit_aggregate: impl FnOnce(&mut ProgramBuilder) -> Result<()>,
) -> Result<()> {
    let inverse = match op {
        WindowOp::AggStep => false,
        WindowOp::AggInverse => true,
        WindowOp::ReturnRow => unreachable!("RETURN_ROW does not update frame tracking"),
    };
    match tracking {
        WindowFrameTracking::Excluded {
            start_rowid,
            end_rowid,
        } => {
            program.emit_insn(Insn::AddImm {
                register: if inverse { start_rowid } else { end_rowid },
                value: 1,
            });
        }
        WindowFrameTracking::Positional { counters } => {
            emit_aggregate(program)?;
            // Start is the buffer index minus one; end is the buffer index.
            // Mirrors SQLite windowAggStep's regApp+1-bInverse increment.
            program.emit_insn(Insn::AddImm {
                register: counters + usize::from(!inverse),
                value: 1,
            });
        }
        WindowFrameTracking::None => emit_aggregate(program)?,
    }
    Ok(())
}

/// Keep bounded RANGE cursors behind the frame end and newest input row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_range_cursor_guards(
    program: &mut ProgramBuilder,
    frame: WindowFrameShape,
    op: WindowOp,
    countdown_reg: Option<usize>,
    cursor_for_op: CursorID,
    end_cursor: CursorID,
    newest_rowid: usize,
    in_flush: bool,
    label_done: BranchOffset,
) {
    // For same-kind RANGE offsets, keep the frame-start cursor at or before
    // the frame-end cursor. While the source is still producing rows, also
    // keep csr_end at or before the newest buffered row. During flush SQLite
    // clears its source-rowid register, disabling the latter guard.
    let same_kind_range_offsets = frame.mode == FrameMode::Range
        && countdown_reg.is_some()
        && matches!(
            (frame.start, frame.end),
            (WindowFrameEdge::Preceding, WindowFrameEdge::Preceding)
                | (WindowFrameEdge::Following, WindowFrameEdge::Following)
        );
    if same_kind_range_offsets {
        match op {
            WindowOp::AggInverse => {
                let start_rowid = program.alloc_register();
                let end_rowid = program.alloc_register();
                program.emit_insn(Insn::RowId {
                    cursor_id: cursor_for_op,
                    dest: start_rowid,
                });
                program.emit_insn(Insn::RowId {
                    cursor_id: end_cursor,
                    dest: end_rowid,
                });
                program.emit_insn(Insn::Ge {
                    lhs: start_rowid,
                    rhs: end_rowid,
                    target_pc: label_done,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            WindowOp::AggStep if !in_flush => {
                let end_rowid = program.alloc_register();
                program.emit_insn(Insn::RowId {
                    cursor_id: cursor_for_op,
                    dest: end_rowid,
                });
                program.emit_insn(Insn::Ge {
                    lhs: end_rowid,
                    rhs: newest_rowid,
                    target_pc: label_done,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            WindowOp::AggStep | WindowOp::ReturnRow => {}
        }
    }
}

/// Gate one frame operation with its countdown or RANGE value boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_offset_gate(
    program: &mut ProgramBuilder,
    frame: WindowFrameShape,
    op: WindowOp,
    countdown_reg: Option<usize>,
    current_cursor: CursorID,
    cursor_for_op: CursorID,
    label_done: BranchOffset,
    emit_range: impl FnOnce(
        &mut ProgramBuilder,
        RangeCmp,
        CursorID,
        usize,
        CursorID,
        BranchOffset,
    ) -> Result<()>,
) -> Result<Option<BranchOffset>> {
    // ROWS/GROUPS offsets use a countdown. RANGE offsets instead compare
    // ORDER BY values and loop back after each peer group until the cursor
    // reaches the value boundary.
    let range_loop_start = if let Some(reg) = countdown_reg {
        if frame.mode == FrameMode::Range {
            let label = program.allocate_label();
            program.preassign_label_to_next_insn(label);
            match op {
                WindowOp::AggInverse => {
                    if frame.start == WindowFrameEdge::Following {
                        emit_range(
                            program,
                            RangeCmp::Le,
                            current_cursor,
                            reg,
                            cursor_for_op,
                            label_done,
                        )?;
                    } else {
                        emit_range(
                            program,
                            RangeCmp::Ge,
                            cursor_for_op,
                            reg,
                            current_cursor,
                            label_done,
                        )?;
                    }
                }
                WindowOp::AggStep => emit_range(
                    program,
                    RangeCmp::Gt,
                    cursor_for_op,
                    reg,
                    current_cursor,
                    label_done,
                )?,
                WindowOp::ReturnRow => {
                    unreachable!("RANGE offsets never gate RETURN_ROW directly")
                }
            }
            Some(label)
        } else {
            program.emit_insn(Insn::IfPos {
                reg,
                target_pc: label_done,
                decrement_by: 1,
            });
            None
        }
    } else {
        None
    };

    Ok(range_loop_start)
}

/// Emit one of the three operations that move the frame forward — add a
/// row to the totals (AGGSTEP), emit a row's result (RETURN_ROW), or
/// remove a row from the totals (AGGINVERSE). Mirrors SQLite's
/// `windowCodeOp` (window.c:2229-2376).
///
/// `countdown_reg`: a register holding a count. While it is positive the
/// op is skipped and the count is decremented (`OP_IfPos`); the op only
/// starts firing once the count reaches zero. This delays an op for the
/// first N rows — cume_dist delays RETURN_ROW, and `ROWS BETWEEN N
/// PRECEDING` delays AGGINVERSE until the frame has grown to N+1 rows.
///
/// `break_on_eof`: only set at flush time, to exit a RETURN_ROW loop on
/// EOF instead of falling through.
///
/// Under RANGE / GROUPS one call handles a whole peer group at once
/// (AGGSTEP adds every row in it, RETURN_ROW emits every row in it, all
/// sharing the same totals); under ROWS one call handles exactly one row.
fn emit_window_op(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    op: WindowOp,
    countdown_reg: Option<usize>,
    break_on_eof: Option<BranchOffset>,
    in_flush: bool,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let frame = meta.frame;
    let buffer_table_name = meta.buffer_table_name.clone();
    emit_window_operation(
        program,
        frame,
        cursors,
        registers.cursor_peer_values,
        registers.frame_tracking,
        registers.rowid,
        buffer_table_name,
        window_delete_op(window),
        op,
        countdown_reg,
        break_on_eof,
        in_flush,
        |program, phase| match phase {
            WindowOperationPhase::ReadAggregateResults => {
                let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
                emit_window_aggregate_results(
                    program,
                    registers.acc_start,
                    &meta.functions,
                    WindowAggregateResultMode::Value,
                );
                Ok(())
            }
            WindowOperationPhase::Apply(WindowOp::AggStep) => {
                emit_function_step(program, t_ctx, plan, cursors.csr_end)
            }
            WindowOperationPhase::Apply(WindowOp::AggInverse) => {
                emit_function_inverse(program, t_ctx, plan)
            }
            WindowOperationPhase::Apply(WindowOp::ReturnRow) => {
                emit_return_one_row(program, t_ctx, plan)
            }
        },
        |program, comparison, first, offset, second, target| {
            emit_legacy_window_range_test(program, plan, comparison, first, offset, second, target)
        },
        |program, cursor, previous, repeat| {
            let order_by_len = window.order_by.len();
            let temp_start = (order_by_len > 0).then(|| {
                let temp_start = program.alloc_registers(order_by_len);
                let count = emit_window_cursor_keys(
                    program,
                    cursor,
                    window_order_by_columns(window),
                    temp_start,
                );
                turso_assert!(count == order_by_len, "window peer key width changed");
                temp_start
            });
            emit_window_peer_change(
                program,
                WindowPeerComparison::from_registers(order_by_len, temp_start, previous),
                order_by_key_info(window, &plan.table_references),
                repeat,
            )
        },
    )
}

/// Work supplied by the legacy or HIR adapter at each operation phase.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WindowOperationPhase {
    ReadAggregateResults,
    Apply(WindowOp),
}

/// Shared frame-operation branches, cursor movement, and peer-group loops.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_operation(
    program: &mut ProgramBuilder,
    frame: WindowFrameShape,
    cursors: WindowCursors,
    peer_values: CursorPeerValues,
    tracking: WindowFrameTracking,
    newest_rowid: usize,
    buffer_table_name: String,
    delete_after: Option<WindowOp>,
    op: WindowOp,
    countdown_reg: Option<usize>,
    break_on_eof: Option<BranchOffset>,
    in_flush: bool,
    mut emit_phase: impl FnMut(&mut ProgramBuilder, WindowOperationPhase) -> Result<()>,
    emit_range: impl FnOnce(
        &mut ProgramBuilder,
        RangeCmp,
        CursorID,
        usize,
        CursorID,
        BranchOffset,
    ) -> Result<()>,
    emit_peers: impl FnOnce(&mut ProgramBuilder, CursorID, Option<usize>, BranchOffset) -> Result<()>,
) -> Result<()> {
    // Under RANGE / GROUPS frames one advance steps over every row with
    // equal ORDER BY values (a peer group); with no ORDER BY every row
    // counts as equal, so it runs to the end of the partition. Mirrors
    // SQLite's `WindowCodeArg.eFrmType != TK_ROWS` test (window.c:2247).
    let b_peer = frame.mode != turso_parser::ast::FrameMode::Rows;

    // AGGINVERSE does nothing when the frame starts at UNBOUNDED
    // PRECEDING: no row ever leaves the frame. Mirrors SQLite's early
    // return at window.c:2252-2257, which lets every caller emit all three
    // operations without first checking for this case.
    if matches!(op, WindowOp::AggInverse) && frame.start == WindowFrameEdge::UnboundedPreceding {
        assert!(
            countdown_reg.is_none() && break_on_eof.is_none(),
            "an AGGINVERSE no-op cannot carry a countdown or an EOF break"
        );
        return Ok(());
    }

    let label_done = program.allocate_label();
    // Cursor + per-cursor peer reference register for each op. Mirrors
    // SQLite's switch at `window.c:2315-2344` plus the reg pickup at
    // `window.c:2317-2336`.
    let (cursor_for_op, peer_ref_reg) = match op {
        WindowOp::AggStep => (cursors.csr_end, peer_values[FrameCursor::End]),
        WindowOp::ReturnRow => (cursors.csr_current, peer_values[FrameCursor::Current]),
        WindowOp::AggInverse => (
            cursors
                .csr_start
                .expect("AggInverse can only be emitted when the window has a moving frame start"),
            peer_values[FrameCursor::Start],
        ),
    };

    let range_loop_start = emit_window_offset_gate(
        program,
        frame,
        op,
        countdown_reg,
        cursors.csr_current,
        cursor_for_op,
        label_done,
        emit_range,
    )?;

    // RETURN_ROW finalizes accumulators before emitting (SQLite's
    // windowAggFinal at window.c:2284).
    if matches!(op, WindowOp::ReturnRow) && !frame.has_exclude {
        emit_phase(program, WindowOperationPhase::ReadAggregateResults)?;
    }

    let label_continue = program.allocate_label();
    program.preassign_label_to_next_insn(label_continue);

    emit_window_range_cursor_guards(
        program,
        frame,
        op,
        countdown_reg,
        cursor_for_op,
        cursors.csr_end,
        newest_rowid,
        in_flush,
        label_done,
    );

    match op {
        WindowOp::AggStep | WindowOp::AggInverse => {
            emit_window_tracked_aggregate(program, tracking, op, |program| {
                emit_phase(program, WindowOperationPhase::Apply(op))
            })?;
        }
        WindowOp::ReturnRow => emit_phase(program, WindowOperationPhase::Apply(op))?,
    }

    // Delete the row we are done with (SQLite window.c:2346,
    // `if( op==p->eDelete ) OP_Delete`), before advancing the cursor past it.
    if delete_after == Some(op) {
        program.emit_insn(Insn::Delete {
            cursor_id: cursor_for_op,
            table_name: buffer_table_name,
            is_part_of_update: false,
        });
    }

    // Advance the cursor. Three control-flow shapes mirroring SQLite's
    // `windowCodeOp` tail at window.c:2351-2361:
    //   - break_on_eof=Some: Next; on EOF fall through to a Goto that
    //     jumps to the caller's break label.
    //   - bPeer=true: Next; on EOF fall through to Goto label_done; else
    //     fall through to the peer-group check below (is the next row
    //     still in the same group?).
    //   - bPeer=false, no break_on_eof: Next; on EOF fall through;
    //     no peer check; label_done is the next instruction.
    let label_after_next = program.allocate_label();
    program.emit_insn(Insn::Next {
        cursor_id: cursor_for_op,
        pc_if_next: label_after_next,
        fullscan: false,
    });
    if let Some(break_target) = break_on_eof {
        program.emit_insn(Insn::Goto {
            target_pc: break_target,
        });
    } else if b_peer {
        program.emit_insn(Insn::Goto {
            target_pc: label_done,
        });
    }
    program.preassign_label_to_next_insn(label_after_next);

    if b_peer {
        // Keep repeating this operation as long as the row we just moved
        // to is a peer of the cursor's current group (same ORDER BY
        // values), so a whole group is handled in one call. When the group
        // changes, record the new group's values in this cursor's entry of
        // `cursor_peer_values`. There is one entry per cursor, so the
        // start / current / end cursors can each be on a different group
        // during flush — needed for percent_rank / cume_dist. Mirrors
        // SQLite's `windowReadPeerValues` +
        // `windowIfNewPeer` tail (window.c:2363-2369); with no ORDER BY the
        // whole partition is one group, so this just loops to EOF.
        emit_peers(program, cursor_for_op, peer_ref_reg, label_continue)?;
    }

    if let Some(range_loop_start) = range_loop_start {
        program.emit_insn(Insn::Goto {
            target_pc: range_loop_start,
        });
    }
    program.preassign_label_to_next_insn(label_done);

    Ok(())
}

/// Emit the per-row FILTER predicate read used by both `AggStep` (on
/// `csr_end`, the row entering the frame) and `AggInverse` (on
/// `csr_start`, the row leaving). Caller is responsible for pinning
/// the returned label to the instruction the filtered emit should
/// skip past.
fn emit_filter_skip<E>(
    program: &mut ProgramBuilder,
    emitter: &mut impl WindowValueEmitter<E>,
    cursor: CursorID,
    filter: &BufferedWindowValue<E>,
) -> Result<BranchOffset> {
    let label = program.allocate_label();
    let filter_reg = program.alloc_register();
    emitter.emit_buffered_value(program, cursor, filter, filter_reg)?;
    program.emit_insn(Insn::IfNot {
        reg: filter_reg,
        target_pc: label,
        jump_if_null: true,
    });
    Ok(label)
}

struct LegacyWindowValueEmitter<'a, 'resolver> {
    resolver: &'a mut Resolver<'resolver>,
    table_references: &'a TableReferences,
}

impl WindowValueEmitter<&Expr> for LegacyWindowValueEmitter<'_, '_> {
    fn emit_buffered_value(
        &mut self,
        program: &mut ProgramBuilder,
        cursor: CursorID,
        value: &BufferedWindowValue<&Expr>,
        target: usize,
    ) -> Result<()> {
        match value {
            BufferedWindowValue::Column(column) => program.emit_insn(Insn::Column {
                cursor_id: cursor,
                column: *column,
                dest: target,
                default: None,
            }),
            BufferedWindowValue::Recompute(expression) => {
                walk_expr(expression, &mut |node| {
                    if let Expr::Column { column, .. } = node {
                        let value_reg = program.alloc_register();
                        program.emit_insn(Insn::Column {
                            cursor_id: cursor,
                            column: *column,
                            dest: value_reg,
                            default: None,
                        });
                        self.resolver.cache_expr_reg(
                            std::borrow::Cow::Owned(node.clone()),
                            value_reg,
                            false,
                            None,
                        );
                        return Ok(WalkControl::SkipChildren);
                    }
                    Ok(WalkControl::Continue)
                })?;
                self.resolver.expr_to_reg_cache_enabled = true;
                translate_expr(
                    program,
                    Some(self.table_references),
                    expression,
                    target,
                    self.resolver,
                )?;
            }
        }
        Ok(())
    }
}

/// Runtime locations and flags needed to add one row to a window frame.
#[derive(Clone, Copy)]
pub(crate) struct WindowStepContext {
    pub(crate) accumulator_registers_start: usize,
    pub(crate) read_cursor: CursorID,
    pub(crate) current_cursor: CursorID,
    pub(crate) has_exclude: bool,
    pub(crate) custom_types_enabled: bool,
}

/// Add one buffered row to every prepared window function.
pub(crate) fn emit_function_step_runtime<E>(
    program: &mut ProgramBuilder,
    functions: &[WindowFunctionRuntime<E>],
    emitter: &mut impl WindowValueEmitter<E>,
    context: WindowStepContext,
) -> Result<()> {
    for (i, function) in functions.iter().enumerate() {
        let positional = matches!(
            &function.function,
            AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue)
        );
        let always_lookup = matches!(
            &function.function,
            AccumulatorFunc::Window(WindowFunc::Lag | WindowFunc::Lead)
        );
        if always_lookup || (positional && !context.has_exclude) {
            continue;
        }
        let acc_reg = context.accumulator_registers_start + i;
        let arg_load_start = (!function.arguments.is_empty())
            .then(|| program.alloc_registers(function.arguments.len()));
        if let Some(base) = arg_load_start {
            for (j, argument) in function.arguments.iter().enumerate() {
                // SQLite's slow nth_value() path reads the value from each
                // included scan row, but keeps N fixed to the current output
                // row (window.c:1679-1683).
                let argument_cursor = if context.has_exclude
                    && j == 1
                    && matches!(
                        &function.function,
                        AccumulatorFunc::Window(WindowFunc::NthValue)
                    ) {
                    context.current_cursor
                } else {
                    context.read_cursor
                };
                emitter.emit_buffered_value(program, argument_cursor, argument, base + j)?;
            }
        }

        match &function.function {
            AccumulatorFunc::Agg(agg_func) => {
                let filter_skip_label = function
                    .filter
                    .as_ref()
                    .map(|filter| emit_filter_skip(program, emitter, context.read_cursor, filter))
                    .transpose()?;

                if let Some(state) = function.minmax {
                    let label_skip = filter_skip_label.unwrap_or_else(|| program.allocate_label());
                    let argument = arg_load_start.expect("min/max has one argument");
                    program.emit_insn(Insn::Copy {
                        src_reg: argument,
                        dst_reg: state.registers,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: state.registers,
                        target_pc: label_skip,
                    });
                    program.emit_insn(Insn::AddImm {
                        register: state.registers + 1,
                        value: 1,
                    });
                    program.emit_insn(Insn::MakeRecord {
                        start_reg: to_u32(state.registers),
                        count: 2,
                        dest_reg: to_u32(state.registers + 2),
                        index_name: None,
                        affinity_str: None,
                    });
                    program.emit_insn(Insn::IdxInsert {
                        cursor_id: state.cursor,
                        record_reg: state.registers + 2,
                        unpacked_start: Some(state.registers),
                        unpacked_count: Some(2),
                        flags: IdxInsertFlags::new(),
                    });
                    program.preassign_label_to_next_insn(label_skip);
                } else {
                    translate_prepared_aggregation_step(
                        program,
                        PreparedAggregateArguments {
                            function: agg_func,
                            registers_start: arg_load_start.unwrap_or(0),
                            count: function.arguments.len(),
                            collations: &function.argument_collations,
                            comparators: &function.argument_comparators,
                            custom_types_enabled: context.custom_types_enabled,
                        },
                        acc_reg,
                    )?;

                    if let Some(label) = filter_skip_label {
                        program.preassign_label_to_next_insn(label);
                    }
                }
            }
            AccumulatorFunc::Window(win_func) => {
                program.emit_insn(Insn::AggStep {
                    data: Box::new(AggStepData {
                        acc_reg,
                        col: arg_load_start.unwrap_or(0),
                        delimiter: 0,
                        func: AccumulatorFunc::Window(win_func.clone()),
                        comparator: None,
                        collation: None,
                    }),
                });
            }
        }
    }
    Ok(())
}

/// Emit the code that adds one row into each function's running total (an
/// AGGSTEP step), reading that row's argument values from `read_csr` — the
/// cursor sitting on the row that just joined the frame.
///
/// Mirrors SQLite's `windowAggStep` (`window.c:1658-1762`). For each
/// function, its argument values are loaded from the cursor's current row
/// into their own small block of registers, and the aggregate step is
/// emitted against that block. Those load registers are kept deliberately
/// apart from `src_columns_start` (where the source loop puts the next
/// row's columns), so stepping the previous partition here can't overwrite
/// the first row of the next one.
///
/// `WindowFunctionRuntime` keeps this loop independent of the expression
/// representation. The legacy adapter either reads one buffered column or
/// recomputes an expression after loading its buffered inputs; HIR can supply
/// another adapter without changing the stepping rules below.
fn emit_function_step(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    read_csr: CursorID,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let acc_start = meta.registers.acc_start;
    let functions = &meta.functions;
    let csr_current = meta.cursors.csr_current;
    let has_exclude = plan
        .window
        .as_ref()
        .expect("missing window")
        .frame
        .exclude
        .is_some();

    // Save cache state so the per-arg overrides we push below don't leak
    // out to other parts of the emit pipeline (e.g. emit_return_one_row).
    let initial_cache_len = t_ctx.resolver.expr_to_reg_cache.len();
    let cache_was_enabled = t_ctx.resolver.expr_to_reg_cache_enabled;
    let custom_types_enabled = t_ctx.resolver.custom_types_enabled();
    let mut emitter = LegacyWindowValueEmitter {
        resolver: &mut t_ctx.resolver,
        table_references: &plan.table_references,
    };

    emit_function_step_runtime(
        program,
        functions,
        &mut emitter,
        WindowStepContext {
            accumulator_registers_start: acc_start,
            read_cursor: read_csr,
            current_cursor: csr_current,
            has_exclude,
            custom_types_enabled,
        },
    )?;

    // Restore expr-cache state so the per-function arg overrides don't
    // leak into later emit calls (e.g. emit_return_one_row's outer-query
    // result emission, which expects its own result_columns_start mapping).
    emitter
        .resolver
        .expr_to_reg_cache
        .truncate(initial_cache_len);
    emitter.resolver.expr_to_reg_cache_enabled = cache_was_enabled;

    Ok(())
}

/// Emit the code that takes one row back out of each function's running
/// total (an AGGINVERSE step), for the row leaving the frame at its start.
/// This is the mirror image of `emit_function_step`, which adds a row at
/// the frame's end. Mirrors the inverse path inside SQLite's
/// `windowAggStep(... bInverse=1, ...)` (window.c:2329).
///
/// ntile / percent_rank / cume_dist only track counts, so removing a row
/// just adjusts an internal counter — there is no argument value to load.
/// For aggregates over a moving start (`sum`, `count(arg)`, `avg`) we first
/// load the leaving row's argument value from `csr_start`, so the runtime
/// knows what to subtract back out — the same way `emit_function_step`
/// loads from `csr_end` when adding a row.
fn emit_function_inverse(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let acc_start = meta.registers.acc_start;
    let functions = &meta.functions;
    let csr_start = meta.cursors.csr_start.expect(
        "emit_function_inverse: csr_start must be allocated when any AGGINVERSE is emitted",
    );
    let initial_cache_len = t_ctx.resolver.expr_to_reg_cache.len();
    let cache_was_enabled = t_ctx.resolver.expr_to_reg_cache_enabled;
    let mut emitter = LegacyWindowValueEmitter {
        resolver: &mut t_ctx.resolver,
        table_references: &plan.table_references,
    };

    emit_function_inverse_runtime(program, functions, &mut emitter, acc_start, csr_start)?;

    emitter
        .resolver
        .expr_to_reg_cache
        .truncate(initial_cache_len);
    emitter.resolver.expr_to_reg_cache_enabled = cache_was_enabled;
    Ok(())
}

/// Remove one buffered row from every prepared window function.
pub(crate) fn emit_function_inverse_runtime<E>(
    program: &mut ProgramBuilder,
    functions: &[WindowFunctionRuntime<E>],
    emitter: &mut impl WindowValueEmitter<E>,
    accumulator_registers_start: usize,
    start_cursor: CursorID,
) -> Result<()> {
    for (i, function) in functions.iter().enumerate() {
        let acc_reg = accumulator_registers_start + i;

        let arg_load_start = (!function.arguments.is_empty())
            .then(|| program.alloc_registers(function.arguments.len()));
        if let Some(base) = arg_load_start {
            for (j, argument) in function.arguments.iter().enumerate() {
                emitter.emit_buffered_value(program, start_cursor, argument, base + j)?;
            }
        }

        // A row skipped by FILTER when it entered was never added to the
        // totals (AggStep), so it must not be subtracted (AggInverse)
        // either — otherwise the running totals go wrong and `count(*)`
        // would go negative.
        let filter_skip_label = function
            .filter
            .as_ref()
            .map(|filter| emit_filter_skip(program, emitter, start_cursor, filter))
            .transpose()?;

        if let Some(state) = function.minmax {
            let label_skip = filter_skip_label.unwrap_or_else(|| program.allocate_label());
            let arg_reg = arg_load_start.expect("min/max has one argument");
            program.emit_insn(Insn::Copy {
                src_reg: arg_reg,
                dst_reg: state.registers,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IsNull {
                reg: state.registers,
                target_pc: label_skip,
            });
            program.emit_insn(Insn::SeekGE {
                is_index: true,
                cursor_id: state.cursor,
                start_reg: state.registers,
                num_regs: 1,
                target_pc: label_skip,
                eq_only: false,
                null_matching_mask: Default::default(),
            });
            program.emit_insn(Insn::Delete {
                cursor_id: state.cursor,
                table_name: String::new(),
                is_part_of_update: false,
            });
            program.preassign_label_to_next_insn(label_skip);
        } else {
            program.emit_insn(Insn::AggInverse {
                acc_reg,
                col: arg_load_start.unwrap_or(0),
                delimiter: 0,
                func: function.function.clone(),
                comparator: None,
            });

            if let Some(label) = filter_skip_label {
                program.preassign_label_to_next_insn(label);
            }
        }
    }
    Ok(())
}

/// Emit the per-output-row lookup for every `lag` / `lead` function in the
/// current window. Mirrors SQLite's lag/lead arm in `windowReturnOneRow`
/// (`window.c:1958`):
///
/// ```text
///   reg_result := arg[2] from csr_current   (default; NULL if nArg < 3)
///   tmp := rowid(csr_current)
///   tmp := tmp ± offset                     (offset = 1 if nArg < 2,
///                                             else arg[1] from csr_current)
///   if SeekRowid(csr_app, tmp) succeeds:
///       reg_result := Column(csr_app, arg_col[0])
///   lbl_miss:
/// ```
///
/// `csr_app` is a separate cursor (a duplicate of `csr_current`) used only
/// for these lookups, so moving it doesn't disturb the three frame
/// cursors. The result goes into the function's result register
/// stored in its prepared runtime; later, when the SELECT output is built, the
/// expression cache points the function's value at that register.
fn emit_lag_lead_lookup(program: &mut ProgramBuilder, t_ctx: &TranslateCtx) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    emit_lag_lead_lookup_runtime(program, &meta.functions, meta.cursors)
}

/// Positional lookup arguments are materialized once in the window buffer.
fn window_lookup_column<E>(argument: &BufferedWindowValue<E>) -> usize {
    match argument {
        BufferedWindowValue::Column(column) => *column,
        BufferedWindowValue::Recompute(_) => {
            unreachable!("positional lookup requires a buffered column")
        }
    }
}

/// Read lag/lead values by row position, retaining defaults when the seek misses.
pub(crate) fn emit_lag_lead_lookup_runtime<E>(
    program: &mut ProgramBuilder,
    functions: &[WindowFunctionRuntime<E>],
    cursors: WindowCursors,
) -> Result<()> {
    let csr_current = cursors.csr_current;

    for func in functions {
        let is_lead = match &func.function {
            AccumulatorFunc::Window(WindowFunc::Lag) => false,
            AccumulatorFunc::Window(WindowFunc::Lead) => true,
            _ => continue,
        };
        let csr_app = cursors
            .csr_app
            .expect("csr_app must exist when a window contains lag / lead");
        let result_reg = func.result_register;

        let args = &func.arguments;
        let arg0_col = window_lookup_column(args.first().expect("lookup needs a value"));

        // Default value first: NULL when nArg < 3, else read arg[2] from csr_current.
        if args.len() < 3 {
            program.emit_insn(Insn::Null {
                dest: result_reg,
                dest_end: None,
            });
        } else {
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: window_lookup_column(&args[2]),
                dest: result_reg,
                default: None,
            });
        }

        // tmp = rowid(csr_current) ± offset.
        let tmp_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: csr_current,
            dest: tmp_reg,
        });
        if args.len() < 2 {
            // Default offset = 1.
            program.emit_insn(Insn::AddImm {
                register: tmp_reg,
                value: if is_lead { 1 } else { -1 },
            });
        } else {
            let offset_col = window_lookup_column(&args[1]);
            let offset_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: offset_col,
                dest: offset_reg,
                default: None,
            });
            // Insn::Subtract is `dest = lhs - rhs`; Insn::Add is
            // `dest = lhs + rhs`. We want `tmp = tmp ± offset`, so tmp
            // is the lhs in both cases.
            if is_lead {
                program.emit_insn(Insn::Add {
                    lhs: tmp_reg,
                    rhs: offset_reg,
                    dest: tmp_reg,
                });
            } else {
                program.emit_insn(Insn::Subtract {
                    lhs: tmp_reg,
                    rhs: offset_reg,
                    dest: tmp_reg,
                });
            }
        }

        // SeekRowid on csr_app — on hit, overwrite result with the
        // column from the target row; on miss, fall through to lbl_miss
        // and the default register value (set above) stays put.
        let lbl_miss = program.allocate_label();
        program.emit_insn(Insn::SeekRowid {
            cursor_id: csr_app,
            src_reg: tmp_reg,
            target_pc: lbl_miss,
        });
        program.emit_insn(Insn::Column {
            cursor_id: csr_app,
            column: arg0_col,
            dest: result_reg,
            default: None,
        });
        program.preassign_label_to_next_insn(lbl_miss);
    }

    Ok(())
}

/// Emit the per-output-row lookup for every `first_value` / `nth_value` in
/// the current window: jump straight to the wanted row of the frame and
/// read its value. Mirrors SQLite's first_value / nth_value arms in
/// `windowReturnOneRow` (`window.c:1940-1965`):
///
/// ```text
///   reg_result := NULL
///   tmp := 1                                 (first_value, N=1)
///     -- or --
///   tmp := arg[1] from csr_current           (nth_value, per-row N)
///   MustBeInt tmp; if tmp <= 0: error
///   tmp := tmp + frame_counters[0]           (rows that left the frame)
///   if tmp > frame_counters[1]: goto lbl     (target past frame end → NULL)
///   SeekRowid(csr_app, tmp)
///   reg_result := Column(csr_app, arg[0])
///   lbl:
/// ```
fn emit_first_value_nth_value_lookup(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    emit_first_value_nth_value_lookup_runtime(
        program,
        &meta.functions,
        meta.cursors,
        meta.registers.frame_tracking,
    )
}

/// Read first/nth values within the tracked frame, preserving bounds and N checks.
pub(crate) fn emit_first_value_nth_value_lookup_runtime<E>(
    program: &mut ProgramBuilder,
    functions: &[WindowFunctionRuntime<E>],
    cursors: WindowCursors,
    tracking: WindowFrameTracking,
) -> Result<()> {
    let csr_current = cursors.csr_current;

    for func in functions {
        let is_nth = match &func.function {
            AccumulatorFunc::Window(WindowFunc::FirstValue) => false,
            AccumulatorFunc::Window(WindowFunc::NthValue) => true,
            _ => continue,
        };
        let csr_app = cursors
            .csr_app
            .expect("csr_app must exist when a window contains first_value or nth_value");
        let result_reg = func.result_register;

        let args = &func.arguments;
        let arg_value_col = window_lookup_column(args.first().expect("lookup needs a value"));

        // Default result: NULL (stays NULL on seek miss or N past frame end).
        program.emit_insn(Insn::Null {
            dest: result_reg,
            dest_end: None,
        });

        // Compute target rowid. target = N + frame_start_rowid - 1.
        // For UB-CR frames frame_start_rowid = 1 → target = N. For
        // ROWS BETWEEN N PRECEDING AND CURRENT ROW it tracks csr_start.
        let target_reg = program.alloc_register();
        if is_nth {
            let n_col = window_lookup_column(args.get(1).expect("nth_value needs N"));
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: n_col,
                dest: target_reg,
                default: None,
            });
            // Validate N: must be a positive integer. Matches SQLite's
            // `windowCheckValue(eCond=2)` at `window.c:1490-1506` — the
            // error message is the verbatim SQLite string.
            let lbl_n_ok = program.allocate_label();
            let lbl_n_err = program.allocate_label();
            program.emit_insn(Insn::MustBeInt {
                reg: target_reg,
                target_pc: Some(lbl_n_err),
            });
            let reg_zero = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: reg_zero,
            });
            program.emit_insn(Insn::Gt {
                lhs: target_reg,
                rhs: reg_zero,
                target_pc: lbl_n_ok,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: None,
            });
            program.preassign_label_to_next_insn(lbl_n_err);
            program.emit_insn(Insn::Halt {
                err_code: crate::error::SQLITE_ERROR,
                description: "second argument to nth_value must be a positive integer".to_string(),
                on_error: None,
                description_reg: None,
            });
            program.preassign_label_to_next_insn(lbl_n_ok);
        } else {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_reg,
            });
        }

        // Apply the frame-start offset: nth_value counts N from the frame
        // start, whose buffer index is `frame_counters + 1` rows in
        // (`frame_counters` holds the count of rows that have left the
        // frame — 0 for an UNBOUNDED PRECEDING start). Buffer rowids are
        // dense from 1 per partition, so `N + frame_counters` is the
        // target's rowid directly. Mirrors SQLite's
        // `OP_Add tmpReg, regApp` at window.c:1949.
        let WindowFrameTracking::Positional {
            counters: frame_counters,
        } = tracking
        else {
            panic!("positional frame tracking must exist for first_value/nth_value");
        };
        program.emit_insn(Insn::Add {
            lhs: target_reg,
            rhs: frame_counters,
            dest: target_reg,
        });

        // Two distinct "no value" outcomes, kept separate:
        //
        // * target past the frame end — a legitimate NULL (the Nth row lies
        //   outside this row's frame). `frame_counters + 1` is the index of
        //   the last row stepped into the frame, NOT the emitted row's own
        //   rowid: under RANGE the frame runs to the current row's last
        //   peer, so an early row of a peer group must still see its whole
        //   group. Mirrors SQLite's `OP_Gt regApp+1, lbl, tmpReg` at
        //   window.c:1950.
        //
        // * target within the frame but `SeekRowid` misses — impossible:
        //   buffered rows are only ever deleted after leaving the frame
        //   (behind `frame_counters` rows in), and the target lies at or
        //   past the frame start. Treat a miss as an invariant violation
        //   and Halt rather than silently returning NULL. Mirrors SQLite
        //   finding its row via csrApp unconditionally.
        let lbl_past_frame_end = program.allocate_label();
        let lbl_missing_row = program.allocate_label();
        let lbl_done = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: target_reg,
            rhs: frame_counters + 1,
            target_pc: lbl_past_frame_end,
            flags: crate::vdbe::insn::CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: csr_app,
            src_reg: target_reg,
            target_pc: lbl_missing_row,
        });
        program.emit_insn(Insn::Column {
            cursor_id: csr_app,
            column: arg_value_col,
            dest: result_reg,
            default: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: lbl_done,
        });
        program.preassign_label_to_next_insn(lbl_missing_row);
        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_ERROR,
            description: "first_value/nth_value could not find a saved row within its frame"
                .to_string(),
            on_error: None,
            description_reg: None,
        });
        // Past frame end falls through here with `result_reg` still NULL.
        program.preassign_label_to_next_insn(lbl_past_frame_end);
        program.preassign_label_to_next_insn(lbl_done);
    }

    Ok(())
}

/// Emit one output row's worth of work: populate `result_columns_start` from
/// `csr_current`, run the positional lookups for functions computed at
/// output time, then Gosub to the shared row-output subroutine. Mirrors
/// SQLite's `windowReturnOneRow` (`window.c:1816-1990`), restricted to the
/// streaming code path.
fn emit_return_one_row(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let labels = meta.labels;
    let registers = meta.registers;
    let cursors = meta.cursors;
    let expressions_referencing_subquery = meta.expressions_referencing_subquery.clone();

    let has_exclude = meta.frame.has_exclude;
    emit_window_row_results(
        program,
        cursors.csr_current,
        expressions_referencing_subquery
            .iter()
            .map(|(_, column)| *column),
        registers.result_columns_start,
        has_exclude,
        labels.row_output,
        registers.row_output_return,
        |program, phase| match phase {
            WindowRowResultPhase::FullScan => emit_window_full_scan(program, t_ctx, plan),
            WindowRowResultPhase::FirstNth => emit_first_value_nth_value_lookup(program, t_ctx),
            WindowRowResultPhase::LagLead => emit_lag_lead_lookup(program, t_ctx),
        },
    )
}

#[derive(Clone, Copy)]
pub(crate) enum WindowRowResultPhase {
    FullScan,
    FirstNth,
    LagLead,
}

/// Fill buffered outputs and function results before calling the output subroutine.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_row_results(
    program: &mut ProgramBuilder,
    current_cursor: CursorID,
    columns: impl IntoIterator<Item = usize>,
    result_start: usize,
    has_exclude: bool,
    output_label: BranchOffset,
    output_return: usize,
    mut emit_results: impl FnMut(&mut ProgramBuilder, WindowRowResultPhase) -> Result<()>,
) -> Result<()> {
    for (i, col_idx) in columns.into_iter().enumerate() {
        let reg_result = result_start + i;
        program.emit_column_or_rowid(current_cursor, col_idx, reg_result);
    }

    if has_exclude {
        emit_results(program, WindowRowResultPhase::FullScan)?;
    } else {
        // Per-row lookups for functions whose value is computed at output
        // time (first_value / nth_value / lag / lead). Each writes the
        // function's value register before the outer query reads it via
        // `emit_select_result`'s expression-cache lookup.
        emit_results(program, WindowRowResultPhase::FirstNth)?;
        emit_results(program, WindowRowResultPhase::LagLead)?;
    }

    // The select-result / sorter-insert code is a shared subroutine —
    // RETURN_ROW is emitted at several sites (streaming step + flush
    // loops), and jump targets inside the row-output code (SELECT
    // DISTINCT's on-conflict label in particular) must resolve to exactly
    // one address. Mirrors SQLite's `OP_Gosub regGosub, addrGosub` at the
    // end of `windowReturnOneRow` (window.c:1988).
    program.emit_insn(Insn::Gosub {
        target_pc: output_label,
        return_reg: output_return,
    });

    Ok(())
}

/// Emit the single row-output subroutine that every RETURN_ROW site calls:
/// send the filled-in result registers on to the outer query (or into the
/// ORDER BY sorter), applying OFFSET, LIMIT and SELECT DISTINCT
/// de-duplication. This is the window-loop half of what SQLite emits once
/// as the `addrGosub` target in `select.c`.
fn emit_row_output_subroutine(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let labels = meta.labels;
    let registers = meta.registers;

    program.preassign_label_to_next_insn(labels.row_output);

    let label_skip_returning_row = program.allocate_label();
    t_ctx.resolver.enable_expr_to_reg_cache();

    if plan.order_by.is_empty() {
        emit_select_result(
            program,
            &t_ctx.resolver,
            plan,
            Some(labels.window_processing_end),
            Some(label_skip_returning_row),
            t_ctx.reg_nonagg_emit_once_flag,
            t_ctx.reg_offset,
            t_ctx.reg_result_cols_start.unwrap(),
            t_ctx.limit_ctx,
        )?;
    } else {
        EmitOrderBy::sorter_insert(program, t_ctx, plan)?;
    }

    program.preassign_label_to_next_insn(label_skip_returning_row);

    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }

    program.emit_insn(Insn::Return {
        return_reg: registers.row_output_return,
        can_fallthrough: false,
    });

    Ok(())
}

/// Emit the flush subroutine for window processing — the code that runs
/// at the end of a partition to finish off any rows the frame end hasn't
/// reached yet and emit every row not yet sent to the outer query.
///
/// This is the Rust port of SQLite's flush block at `window.c:3043-3105`.
/// The loop shape is one of three patterns, chosen by the frame's bounds
/// the same way the main loop chooses. The most common shape
/// (`UNBOUNDED PRECEDING TO CURRENT ROW`, window.c:3085-3094):
///
/// ```text
///   Rewind csr_write → if empty, jump to label_empty
///   emit_window_op AGGSTEP, break_on_eof=None
///       ↑ adds the last buffered row, whose AggStep was left until now
///   addr_loop_start:
///   emit_window_op RETURN_ROW, break_on_eof=label_break
///   emit_window_op AGGINVERSE (no-op for UNBOUNDED start)
///   Goto addr_loop_start
///   label_break:
///   label_empty:
///   ResetSorter csr_current
///   Return flush_buffer_return_offset
/// ```
///
/// Entered two ways, just like SQLite's flush block:
/// - **Fallthrough** (end of source): the inline code immediately preceding
///   sets `flush_buffer_return_offset` to NULL so the `Return` falls through.
/// - **Subroutine** (partition boundary): the main loop body Gosubs to
///   `labels.flush_buffer`; `Return` jumps back to the address stored in
///   `flush_buffer_return_offset`.
pub fn emit_window_flush(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> crate::Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let labels = meta.labels;
    let registers = meta.registers;
    let cursors = meta.cursors;
    let frame = meta.frame;

    emit_window_partition_flush(
        program,
        frame,
        cursors,
        registers.frame_offsets,
        registers.frame_tracking,
        registers.flush_buffer_return_offset,
        labels.flush_buffer,
        labels.window_processing_end,
        |program, phase| match phase {
            WindowFlushPhase::Operation {
                op,
                countdown,
                break_on_eof,
            } => emit_window_op(program, t_ctx, plan, op, countdown, break_on_eof, true),
            WindowFlushPhase::OutputSubroutine => emit_row_output_subroutine(program, t_ctx, plan),
        },
    )
}

/// Work supplied by the legacy or HIR adapter during partition flush.
pub(crate) enum WindowFlushPhase {
    Operation {
        op: WindowOp,
        countdown: Option<usize>,
        break_on_eof: Option<BranchOffset>,
    },
    OutputSubroutine,
}

/// Finish pending frame operations, reset the buffer, and return from a partition flush.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_window_partition_flush(
    program: &mut ProgramBuilder,
    frame: WindowFrameShape,
    cursors: WindowCursors,
    offsets: WindowFrameOffsets,
    tracking: WindowFrameTracking,
    flush_return: usize,
    flush_entry: BranchOffset,
    processing_end: BranchOffset,
    mut emit_phase: impl FnMut(&mut ProgramBuilder, WindowFlushPhase) -> Result<()>,
) -> Result<()> {
    let label_empty = program.allocate_label();
    let label_break = program.allocate_label();

    // Fallthrough entry: the source loop just ended. Null the return
    // register so the trailing Return falls through past this subroutine.
    program.add_comment(program.offset(), "return remaining buffered rows");
    program.emit_insn(Insn::Null {
        dest: flush_return,
        dest_end: None,
    });

    // Subroutine entry: partition boundary Gosub lands here.
    program.preassign_label_to_next_insn(flush_entry);

    // Detect empty partition. Rewind csr_write sets its position to the
    // first row and jumps to label_empty when the table is empty; csr_write
    // isn't used past flush so resetting its position is harmless.
    program.emit_insn(Insn::Rewind {
        cursor_id: cursors.csr_write,
        pc_if_empty: label_empty,
    });

    // The flush uses the same three patterns, chosen by the frame's
    // bounds, as the main loop. Mirrors SQLite's three flush paths at
    // window.c:3052-3094. Each
    // branch opens with a final AGGSTEP: the last row inserted in the main
    // loop was never added to the totals (under ROWS), or its peer group
    // might still have been incomplete (under RANGE / GROUPS).
    if frame.end == WindowFrameEdge::Preceding {
        // Pattern B flush (window.c:3052-3056): the main loop emitted a
        // row on every iteration, so exactly one row is still waiting to
        // be output — a single RETURN_ROW with no delay and no loop. The
        // final AGGSTEP keeps its delay count: any rows the frame end
        // never reached must stay out of the totals.
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::AggStep,
                countdown: offsets.end(),
                break_on_eof: None,
            },
        )?;
        let inverse_before_return =
            frame.mode == FrameMode::Range && frame.start == WindowFrameEdge::Preceding;
        if inverse_before_return {
            emit_phase(
                program,
                WindowFlushPhase::Operation {
                    op: WindowOp::AggInverse,
                    countdown: offsets.start(),
                    break_on_eof: None,
                },
            )?;
        }
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::ReturnRow,
                countdown: None,
                break_on_eof: None,
            },
        )?;
    } else if frame.start == WindowFrameEdge::Following {
        // Pattern A flush (window.c:3057-3084): two-stage loop.
        //
        //   loop_1:
        //     RETURN_ROW (delayed by IfPos → skips the first few calls),
        //                 break_on_eof = label_break
        //     AGGINVERSE,  break_on_eof = label_after_inverse_eof
        //     Goto loop_1
        //   label_after_inverse_eof:
        //   loop_2:
        //     RETURN_ROW,  break_on_eof = label_break
        //     Goto loop_2
        //   label_break:
        //
        // The stage-one delays depend on the frame end
        // (window.c:3068-3077). With an UNBOUNDED FOLLOWING end the
        // RETURN_ROW delay is the start offset — it holds back the first
        // few emits so rows drop off the start (AGGINVERSE) before their
        // result is read (cume_dist's `1 FOLLOWING` start), while
        // AGGINVERSE itself runs with no delay. With a bounded `M
        // FOLLOWING` end the RETURN_ROW delay continues the end offset and
        // AGGINVERSE continues the (rewritten, M - N) start offset — both
        // pick up whatever the main loop left unfinished. Once AGGINVERSE
        // runs out of rows (csr_start hits EOF), the second loop emits the
        // rows that remain, with the totals now final.
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::AggStep,
                countdown: None,
                break_on_eof: None,
            },
        )?;
        let (return_row_countdown, agg_inverse_countdown) = if frame.mode == FrameMode::Range {
            (None, offsets.start())
        } else if frame.end == WindowFrameEdge::UnboundedFollowing {
            (offsets.start(), None)
        } else {
            (offsets.end(), offsets.start())
        };
        let label_after_inverse_eof = program.allocate_label();
        let label_loop_1 = program.allocate_label();
        program.preassign_label_to_next_insn(label_loop_1);
        if frame.mode == FrameMode::Range {
            emit_phase(
                program,
                WindowFlushPhase::Operation {
                    op: WindowOp::AggInverse,
                    countdown: agg_inverse_countdown,
                    break_on_eof: Some(label_after_inverse_eof),
                },
            )?;
            emit_phase(
                program,
                WindowFlushPhase::Operation {
                    op: WindowOp::ReturnRow,
                    countdown: return_row_countdown,
                    break_on_eof: Some(label_break),
                },
            )?;
        } else {
            emit_phase(
                program,
                WindowFlushPhase::Operation {
                    op: WindowOp::ReturnRow,
                    countdown: return_row_countdown,
                    break_on_eof: Some(label_break),
                },
            )?;
            emit_phase(
                program,
                WindowFlushPhase::Operation {
                    op: WindowOp::AggInverse,
                    countdown: agg_inverse_countdown,
                    break_on_eof: Some(label_after_inverse_eof),
                },
            )?;
        }
        program.emit_insn(Insn::Goto {
            target_pc: label_loop_1,
        });

        program.preassign_label_to_next_insn(label_after_inverse_eof);
        let label_loop_2 = program.allocate_label();
        program.preassign_label_to_next_insn(label_loop_2);
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::ReturnRow,
                countdown: None,
                break_on_eof: Some(label_break),
            },
        )?;
        program.emit_insn(Insn::Goto {
            target_pc: label_loop_2,
        });
    } else {
        // Pattern C flush (window.c:3085-3094): paired RETURN_ROW +
        // AGGINVERSE in a single loop.
        //
        //   loop:
        //     RETURN_ROW (break_on_eof = label_break)
        //     AGGINVERSE (countdown = start_offset_reg)
        //     Goto loop
        //
        // One branch serves every non-FOLLOWING start, exactly as
        // SQLite's else-arm does: for UNBOUNDED PRECEDING the AGGINVERSE
        // does nothing (window.c:2252-2257); for CURRENT ROW there is no
        // delay count and csr_start moves forward one row for each row
        // emitted; for `N PRECEDING` the delay count lets the frame grow
        // to N+1 rows before csr_start starts moving.
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::AggStep,
                countdown: None,
                break_on_eof: None,
            },
        )?;
        let label_loop_start = program.allocate_label();
        program.preassign_label_to_next_insn(label_loop_start);
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::ReturnRow,
                countdown: None,
                break_on_eof: Some(label_break),
            },
        )?;
        emit_phase(
            program,
            WindowFlushPhase::Operation {
                op: WindowOp::AggInverse,
                countdown: offsets.start(),
                break_on_eof: None,
            },
        )?;
        program.emit_insn(Insn::Goto {
            target_pc: label_loop_start,
        });
    }

    program.preassign_label_to_next_insn(label_break);
    program.preassign_label_to_next_insn(label_empty);

    program.emit_insn(Insn::ResetSorter {
        cursor_id: cursors.csr_current,
    });
    if let WindowFrameTracking::Excluded {
        start_rowid: frame_start_rowid,
        end_rowid: frame_end_rowid,
    } = tracking
    {
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: frame_start_rowid,
        });
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: frame_end_rowid,
        });
    }
    program.emit_insn(Insn::Return {
        return_reg: flush_return,
        can_fallthrough: true,
    });

    // The fallthrough entry (end of source) continues past the Return;
    // jump over the row-output subroutine body that follows.
    program.emit_insn(Insn::Goto {
        target_pc: processing_end,
    });

    emit_phase(program, WindowFlushPhase::OutputSubroutine)?;

    program.preassign_label_to_next_insn(processing_end);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};

    fn program() -> ProgramBuilder {
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn buffer_table() -> Arc<BTreeTable> {
        Arc::new(BTreeTable::new(
            0,
            "window-buffer".to_string(),
            crate::alloc::vec![],
            crate::alloc::vec![],
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ))
    }

    fn assert_buffer_cursors(needs_start_cursor: bool, needs_app_cursor: bool) {
        let mut program = program();
        let table = buffer_table();

        let runtime = open_window_buffer(
            &mut program,
            table.clone(),
            needs_start_cursor,
            needs_app_cursor,
        );

        assert!(Arc::ptr_eq(&runtime.table, &table));
        assert_eq!(runtime.cursors.csr_start.is_some(), needs_start_cursor);
        assert_eq!(runtime.cursors.csr_app.is_some(), needs_app_cursor);
        let expected_duplicates =
            2 + usize::from(needs_start_cursor) + usize::from(needs_app_cursor);
        assert_eq!(program.insns.len(), 1 + expected_duplicates);
        assert!(matches!(
            program.insns.first(),
            Some((
                Insn::OpenEphemeral {
                    cursor_id,
                    is_table: true,
                },
                _
            )) if *cursor_id == runtime.cursors.csr_current
        ));
        let expected = [
            Some(runtime.cursors.csr_write),
            Some(runtime.cursors.csr_end),
            runtime.cursors.csr_start,
            runtime.cursors.csr_app,
        ];
        for ((instruction, _), cursor) in program.insns[1..]
            .iter()
            .zip(expected.into_iter().flatten())
        {
            assert!(matches!(
                instruction,
                Insn::OpenDup {
                    original_cursor_id,
                    new_cursor_id,
                } if *original_cursor_id == runtime.cursors.csr_current && *new_cursor_id == cursor
            ));
        }
    }

    #[test]
    fn window_buffer_opens_only_requested_duplicate_cursors() {
        assert_buffer_cursors(false, false);
        assert_buffer_cursors(true, false);
        assert_buffer_cursors(false, true);
        assert_buffer_cursors(true, true);
    }
}
