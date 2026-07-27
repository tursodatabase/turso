use crate::alloc::TursoSliceExt;
use crate::function::{AccumulatorFunc, WindowFunc};
use crate::schema::{BTreeCharacteristics, BTreeTable, Table};
use crate::sync::Arc;
use crate::translate::aggregation::{translate_aggregation_step, AggArgumentSource};
use crate::translate::collate::{get_collseq_from_expr, CollationSeq};
use crate::translate::emitter::{Resolver, TranslateCtx};
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, translate_expr, walk_expr, walk_expr_mut,
    WalkControl,
};
use crate::translate::order_by::EmitOrderBy;
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
    to_u16, {InsertFlags, Insn},
};
use crate::vdbe::{BranchOffset, CursorID};
use crate::Connection;
use crate::Result;
use crate::{turso_assert, turso_assert_eq};
use std::mem;
use turso_parser::ast::Name;
use turso_parser::ast::{Expr, Literal, Over, SortOrder, TableInternalId};

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
                            window_function.rewritten =
                                Some(rewrite_expr_referencing_current_window(
                                    aggregates,
                                    window_name,
                                    ctx,
                                    expr,
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
) -> crate::Result<RewrittenWindowCall> {
    let filter_over = match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group: _,
            filter_over,
            ..
        } => {
            for arg in args.iter_mut() {
                push_into_source_subquery(arg, aggregates, ctx)?;
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
    /// Number of input columns in the source subquery.
    pub src_column_count: usize,
    /// Maps expressions in the current query that reference subquery columns
    /// to their corresponding column indexes in the subquery’s result.
    pub expressions_referencing_subquery: Vec<(&'a Expr, usize)>,
    pub buffer_table_name: String,
}

#[derive(Debug, Clone, Copy)]
pub struct WindowLabels {
    /// Address of the subroutine for flushing buffered rows
    pub flush_buffer: BranchOffset,
    /// Address of the subroutine that sends the populated result registers
    /// to the outer query (select-result / order-by sorter insert). Mirrors
    /// SQLite's `addrGosub` (window.c:1988): every RETURN_ROW site Gosubs
    /// here, so per-row bookkeeping with a single jump target — SELECT
    /// DISTINCT's on-conflict label in particular — is emitted exactly once.
    pub row_output: BranchOffset,
    /// Address of the end of window processing
    pub window_processing_end: BranchOffset,
}

#[derive(Debug, Clone, Copy)]
pub struct WindowRegisters {
    /// Stores the ROWID of the last row inserted into the buffer table.
    /// If NULL, we are before inserting the first row of a new partition.
    pub rowid: usize,
    /// Start of the register array storing partition key values for the current partition.
    pub partition_start: Option<usize>,
    /// Start of the register array storing per-function state for window functions.
    /// Aggregates use `AggStep` to populate their state.
    pub acc_start: usize,
    /// Start of the register array storing per-function outputs. Aggregate windows
    /// populate these via `AggValue`; window-only functions like ROW_NUMBER()
    /// keep their running state here.
    pub acc_result_start: usize,
    /// Stores the address to which control returns after all buffered rows are flushed.
    pub flush_buffer_return_offset: usize,
    /// Start of consecutive registers containing column values for the current row
    /// read from the subquery.
    pub src_columns_start: usize,
    /// Start of the register array storing column values that need to be propagated
    /// from the subquery to the parent query.
    pub result_columns_start: usize,
    /// Start of the register array holding ORDER BY column values for the current row.
    /// These registers are used to detect whether the current row is a "peer"
    /// (i.e., has identical ORDER BY values to the previous row).
    pub new_order_by_columns_start: Option<usize>,
    /// Per-cursor "peer reference" registers — each holds the ORDER BY
    /// values of the first row of the peer group that cursor is currently
    /// processing. The peer-loop inside `emit_window_op` compares the
    /// next-advanced row's values against the cursor's reg and either
    /// loops (peer) or falls through (new group, also updates the reg).
    ///
    /// `peer_end_reg` doubles as the "previous source row" tracker —
    /// it's also used by `emit_window_step`'s pre-check that skips
    /// AggStep / RETURN_ROW when consecutive source rows are peers,
    /// because under our supported frames AGGSTEP's peer-loop on
    /// csr_end and the source-row peer check share the same reference.
    ///
    /// `peer_start_reg` is `Some` only when the window has a moving
    /// frame start (ntile, percent_rank, cume_dist); other windows
    /// don't allocate `csr_start` so the start peer-loop never runs.
    ///
    /// Mirrors SQLite's `s.start.reg` / `s.current.reg` / `s.end.reg`
    /// (window.c:2897-2899).
    pub peer_start_reg: Option<usize>,
    pub peer_current_reg: Option<usize>,
    pub peer_end_reg: Option<usize>,
    /// Return-address register for the `labels.row_output` Gosub. Mirrors
    /// SQLite's `regGosub` (window.c:2793).
    pub row_output_return: usize,
    /// Rowid of the last row stepped into the frame (`AggStep` on `csr_end`).
    /// This is the frame-end bound `first_value` / `nth_value` seek against:
    /// under the default `RANGE UNBOUNDED PRECEDING TO CURRENT ROW` frame the
    /// frame extends to the current row's last peer, so the *emitted* row's
    /// own rowid would undercount it (a peer group's earlier rows would wrongly
    /// see a truncated frame). `AggStep` peer-loops `csr_end` over the whole
    /// group before the group's rows are returned, so capturing `csr_end`'s
    /// rowid there yields the true frame end. Mirrors SQLite's `regApp+1`
    /// (`window.c:1424`, maintained in `windowAggStep`). `None` unless the
    /// window contains `first_value` / `nth_value`.
    pub frame_end_rowid: Option<usize>,
}

/// Cursors on the ephemeral "buffer" B-tree that holds rows of the current
/// partition. The four cursor roles mirror SQLite's `sqlite3WindowCodeStep`
/// allocation (`window.c:2834-2837`).
#[derive(Debug, Clone, Copy)]
pub struct WindowCursors {
    /// Used to `Insert` newly-buffered source rows. Position is implicit
    /// (advances on each `NewRowid` + `Insert`).
    pub csr_write: CursorID,
    /// The row currently being returned to the outer query. `AggValue` fires
    /// when this advances; result-column propagation reads from here.
    pub csr_current: CursorID,
    /// Tracks the frame-end boundary. `AggStep` reads from this cursor's
    /// current row, then advances it. Under RANGE/GROUPS frames the advance
    /// loops through peer-equal rows in one go.
    pub csr_end: CursorID,
    /// Tracks the frame-start boundary for windows whose coerced frame
    /// doesn't start at UNBOUNDED PRECEDING (ntile, percent_rank,
    /// cume_dist). `AggInverse` reads from this cursor's current row,
    /// then advances it (peer-loop under GROUPS/RANGE mode). `None` when
    /// the frame start is UNBOUNDED PRECEDING — the cursor would never
    /// move so allocation would be wasted. Mirrors SQLite's
    /// `s.start.csr` (`window.c:2836`).
    pub csr_start: Option<CursorID>,
    /// Per-row lookup cursor used at output time by functions whose
    /// value is computed via `SeekRowid` rather than running aggregator
    /// state — first_value, nth_value, lag, lead. `OpenDup`'d off
    /// `csr_current` and only moved by `SeekRowid` at output time, so
    /// it's independent of the frame-cursor positions. Allocated
    /// lazily — `None` when no function in the window needs positional
    /// lookup. Mirrors SQLite's `pWin->csrApp` (`window.c`).
    pub csr_app: Option<CursorID>,
}

/// Builds `KeyInfo` entries for the window's ORDER BY columns, populating
/// each entry's collation from the expression. Used by every peer-equality
/// `Insn::Compare` site (source-row pre-check + per-cursor peer-loop in
/// `emit_window_op`); `sort_order` and `nulls_order` are left at their
/// `Insn::Compare` defaults — the peer check only inspects equality, not
/// ordering direction.
fn build_order_by_key_info(
    window: &Window,
    table_references: &crate::translate::plan::TableReferences,
) -> crate::Result<Vec<KeyInfo>> {
    window
        .order_by
        .iter()
        .map(|(expr, _, _)| {
            let collation = get_collseq_from_expr(expr, table_references)?.unwrap_or_default();
            Ok(KeyInfo {
                sort_order: SortOrder::Asc,
                collation,
                nulls_order: None,
            })
        })
        .collect()
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
        // `csr_current` is the primary cursor on the ephemeral buffer;
        // the others are OpenDup'd duplicates that share the same B-tree
        // with independent positions.
        let cursor_csr_current =
            program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        let cursor_csr_write =
            program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        let cursor_csr_end = program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        // `csr_start` tracks the frame-start cursor for functions whose
        // coerced frame doesn't start at UNBOUNDED PRECEDING (ntile,
        // percent_rank, cume_dist). AggInverse fires on this cursor as
        // the frame shrinks from the left.
        let has_moving_start = !matches!(
            window.frame.start,
            crate::translate::plan::FrameBoundary::UnboundedPreceding
        );
        let cursor_csr_start = if has_moving_start {
            Some(program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone())))
        } else {
            None
        };
        // `csr_app` is the per-row lookup cursor used at output time by
        // first_value / nth_value / lag / lead (positional seek). Mirrors
        // SQLite's `csrApp` allocation (`window.c:1422-1426`). Allocate
        // only when needed so unaffected windows don't pay the extra
        // `OpenDup`.
        let needs_csr_app = window.functions.iter().any(|f| {
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
        let cursor_csr_app = if needs_csr_app {
            Some(program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone())))
        } else {
            None
        };
        // Frame-end bound for the positional lookups; see
        // `WindowRegisters::frame_end_rowid`. Only first_value / nth_value
        // consult it — lag / lead seek relative to the emitted row's own
        // rowid, not the frame end.
        let frame_end_rowid = window
            .functions
            .iter()
            .any(|f| {
                matches!(
                    &f.func,
                    AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue),
                )
            })
            .then(|| program.alloc_registers_and_init_w_null(1));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor_csr_current,
            is_table: true,
        });
        program.emit_insn(Insn::OpenDup {
            original_cursor_id: cursor_csr_current,
            new_cursor_id: cursor_csr_write,
        });
        program.emit_insn(Insn::OpenDup {
            original_cursor_id: cursor_csr_current,
            new_cursor_id: cursor_csr_end,
        });
        if let Some(csr_start) = cursor_csr_start {
            program.emit_insn(Insn::OpenDup {
                original_cursor_id: cursor_csr_current,
                new_cursor_id: csr_start,
            });
        }
        if let Some(csr_app) = cursor_csr_app {
            program.emit_insn(Insn::OpenDup {
                original_cursor_id: cursor_csr_current,
                new_cursor_id: csr_app,
            });
        }

        // Window function processing is similar to aggregation processing in how results are mapped
        // to registers. Each function expression is stored in `expr_to_reg_cache` along with its
        // result register. Later, when bytecode generation encounters the expression, the value is
        // copied from the result register instead of generating code to evaluate the expression.
        let reg_acc_start = program.alloc_registers(window_function_count);
        let reg_acc_result_start = program.alloc_registers(window_function_count);
        for (i, func) in window.functions.iter().enumerate() {
            // Cache by the rewritten form (when available) so lookups against the
            // result-column / ORDER-BY expressions — which were rewritten to
            // reference this window's subquery — find the cached register.
            t_ctx.resolver.cache_expr_reg(
                std::borrow::Cow::Borrowed(func.current_expr()),
                reg_acc_result_start + i,
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

        t_ctx.meta_window = Some(WindowMetadata {
            labels: WindowLabels {
                flush_buffer: program.allocate_label(),
                row_output: program.allocate_label(),
                window_processing_end: program.allocate_label(),
            },
            registers: WindowRegisters {
                rowid: program.alloc_registers_and_init_w_null(1),
                partition_start: if partition_by_len > 0 {
                    Some(program.alloc_registers_and_init_w_null(partition_by_len))
                } else {
                    None
                },
                acc_start: reg_acc_start,
                acc_result_start: reg_acc_result_start,
                flush_buffer_return_offset: program.alloc_register(),
                src_columns_start: reg_src_columns_start,
                result_columns_start: reg_col_start,
                new_order_by_columns_start: alloc_optional_registers(program, order_by_len),
                peer_start_reg: if has_moving_start {
                    alloc_optional_registers(program, order_by_len)
                } else {
                    None
                },
                // `peer_current_reg` / `peer_end_reg` drive the AGGSTEP /
                // RETURN_ROW peer-loops, which only run under RANGE / GROUPS
                // (`b_peer` in `emit_window_op`). Under ROWS they're written
                // by the seeding step but never read; skip the allocation.
                peer_current_reg: if window.frame.mode != turso_parser::ast::FrameMode::Rows {
                    alloc_optional_registers(program, order_by_len)
                } else {
                    None
                },
                peer_end_reg: if window.frame.mode != turso_parser::ast::FrameMode::Rows {
                    alloc_optional_registers(program, order_by_len)
                } else {
                    None
                },
                row_output_return: program.alloc_register(),
                frame_end_rowid,
            },
            cursors: WindowCursors {
                csr_write: cursor_csr_write,
                csr_current: cursor_csr_current,
                csr_end: cursor_csr_end,
                csr_start: cursor_csr_start,
                csr_app: cursor_csr_app,
            },
            src_column_count,
            expressions_referencing_subquery,
            buffer_table_name: buffer_table.name.clone(),
        });

        Ok(())
    }
    /// Emits the per-source-row body for window processing.
    ///
    /// This is the Rust port of the main loop in SQLite's
    /// `sqlite3WindowCodeStep` (`window.c:2786-3037`), restricted to the
    /// frames our planner coerces to: ROWS / RANGE / GROUPS with
    /// `UNBOUNDED PRECEDING` start and `CURRENT ROW` end (and the moving
    /// variants for the as-yet-unimplemented ntile / percent_rank /
    /// cume_dist).
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
    ///           Goto loop_end                       ; defer step until peer break
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
        let src_column_count = meta.src_column_count;
        let buffer_table_name = meta.buffer_table_name.clone();

        emit_load_order_by_columns(program, window, &registers);
        emit_flush_buffer_if_new_partition(program, &labels, &registers, window, plan)?;

        // `rowid_reg` was NULL'd at partition entry; it stays NULL until the
        // first Insert of this partition. We use that to dispatch between
        // the first-row branch (cold-start setup) and the subsequent-row
        // branch (the steady-state AGGSTEP + RETURN_ROW pair).
        let label_subsequent = program.allocate_label();
        let label_step_end = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: registers.rowid,
            target_pc: label_subsequent,
        });

        // --- FIRST ROW OF PARTITION ---
        if let Some(new_ob) = registers.new_order_by_columns_start {
            // Seed every per-cursor peer reference with the first row's
            // ORDER BY values. Mirrors SQLite's three-cursor init at
            // `window.c:2973-2976` (regNewPeer → s.start.reg / current.reg
            // / end.reg). Doubling as the source-row peer-pre-check
            // reference (`regPeer` in SQLite), `peer_end_reg` is set
            // here and updated by AGGSTEP's peer-loop fall-through —
            // we don't need a separate `regPeer` because the values
            // they would hold are always identical at any program point.
            program.add_comment(
                program.offset(),
                "initialize per-cursor peer-reference registers for new partition",
            );
            let n = window.order_by.len() - 1;
            for &peer_reg in [
                registers.peer_start_reg,
                registers.peer_current_reg,
                registers.peer_end_reg,
            ]
            .iter()
            .flatten()
            {
                program.emit_insn(Insn::Copy {
                    src_reg: new_ob,
                    dst_reg: peer_reg,
                    extra_amount: n,
                });
            }
        }
        program.add_comment(program.offset(), "reset accumulator registers");
        program.emit_insn(Insn::Null {
            dest: registers.acc_start,
            dest_end: Some(registers.acc_start + window.functions.len() - 1),
        });
        emit_insert_row_into_buffer(
            program,
            &registers,
            &cursors,
            &src_column_count,
            &buffer_table_name,
        );
        // Position each frame cursor at the just-inserted first row.
        // Mirrors `window.c:2967-2971` — `csr_start` is rewound only when
        // the frame start isn't UNBOUNDED PRECEDING (otherwise the
        // cursor wasn't allocated and AggInverse is a no-op).
        let label_unreachable_empty = program.allocate_label();
        if let Some(csr_start) = cursors.csr_start {
            program.emit_insn(Insn::Rewind {
                cursor_id: csr_start,
                pc_if_empty: label_unreachable_empty,
            });
        }
        program.emit_insn(Insn::Rewind {
            cursor_id: cursors.csr_current,
            pc_if_empty: label_unreachable_empty,
        });
        program.emit_insn(Insn::Rewind {
            cursor_id: cursors.csr_end,
            pc_if_empty: label_unreachable_empty,
        });
        program.preassign_label_to_next_insn(label_unreachable_empty);
        // First row contributes nothing to xStep here — the drain AGGSTEP
        // in the flush subroutine processes it once at end-of-partition,
        // and RETURN_ROW there emits it.
        program.emit_insn(Insn::Goto {
            target_pc: label_step_end,
        });

        // --- SUBSEQUENT ROW ---
        program.preassign_label_to_next_insn(label_subsequent);
        emit_insert_row_into_buffer(
            program,
            &registers,
            &cursors,
            &src_column_count,
            &buffer_table_name,
        );

        let frame_mode = window.frame.mode;
        let order_by_len = window.order_by.len();
        let is_range_or_groups = frame_mode != turso_parser::ast::FrameMode::Rows;
        if is_range_or_groups && order_by_len == 0 {
            // RANGE/GROUPS with no ORDER BY: the entire partition is one
            // peer group. The main body emits nothing — every step gets
            // deferred to the flush subroutine, which drains all rows
            // under one peer-equal loop and emits each with the same
            // accumulated value.
            program.emit_insn(Insn::Goto {
                target_pc: label_step_end,
            });
        } else if is_range_or_groups {
            // RANGE/GROUPS: if the new row is peer-equal with prevPeer, the
            // frame doesn't advance yet — buffer the row and skip the step.
            // The next non-peer row will trigger AGGSTEP, which loops
            // through all peer-equal buffered rows in `emit_window_op`.
            let label_step_body = program.allocate_label();
            program.add_comment(program.offset(), "compare ORDER BY columns to detect peer");
            let compare_key_info = build_order_by_key_info(window, &plan.table_references)?;
            // Source-row peer pre-check — skip AGGSTEP / RETURN_ROW when
            // the new source row is in the same peer group as the
            // previous one. Uses `peer_end_reg` as the reference (which
            // tracks the last AGGSTEP'd group); AGGSTEP's own peer-loop
            // updates `peer_end_reg` so subsequent source rows see the
            // correct comparison value.
            //
            // `Insn::Compare` requires `start_reg_a < start_reg_b`, so
            // we put the earlier-allocated `new_order_by_columns_start`
            // first. The Jump targets are symmetric on lt/gt
            // (both go to step_body) so the operand order doesn't
            // change the semantics.
            program.emit_insn(Insn::Compare {
                start_reg_a: registers
                    .new_order_by_columns_start
                    .expect("new_order_by_columns_start must exist"),
                start_reg_b: registers
                    .peer_end_reg
                    .expect("peer_end_reg must exist under RANGE/GROUPS with ORDER BY"),
                count: window.order_by.len(),
                key_info: compare_key_info,
            });
            program.emit_insn(Insn::Jump {
                target_pc_lt: label_step_body,
                target_pc_eq: label_step_end,
                target_pc_gt: label_step_body,
            });
            program.preassign_label_to_next_insn(label_step_body);
        }

        emit_window_op(program, t_ctx, plan, WindowOp::AggStep, None)?;
        // Frames whose end is UNBOUNDED FOLLOWING (lead's and lag's
        // coerced frames) cache the entire partition before any RETURN_ROW
        // fires — every output row's frame depends on rows yet to be
        // inserted, so we can only emit at flush time. SQLite's
        // `windowCacheFrame` (`window.c:2031`) selects the same control
        // flow when a lead/lag function is present.
        let cache_frame = matches!(
            window.frame.end,
            crate::translate::plan::FrameBoundary::UnboundedFollowing
        );
        if !cache_frame {
            emit_window_op(program, t_ctx, plan, WindowOp::ReturnRow, None)?;
        }

        // No explicit peer-tracker update needed here: AGGSTEP's
        // peer-loop fall-through copies `new` into `peer_end_reg`, and
        // the (non-cached) RETURN_ROW peer-loop fall-through copies
        // `new` into `peer_current_reg`. Mirrors SQLite's
        // `windowIfNewPeer` Copy-on-fall-through (window.c:2074).

        program.preassign_label_to_next_insn(label_step_end);

        Ok(())
    }
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
        let same_partition_label = program.allocate_label();
        let new_partition_label = program.allocate_label();

        // Compare the first `deduplicated_partition_by_len` source columns with the saved
        // partition keys. If they differ, this row starts a new partition and we flush the buffer.
        let partition_by_len = window
            .deduplicated_partition_by_len
            .expect("deduplicated_partition_by_len must exist");

        program.add_comment(
            program.offset(),
            "compare partition keys to detect new partition",
        );
        let mut compare_key_info = (0..partition_by_len)
            .map(|_| KeyInfo {
                sort_order: SortOrder::Asc,
                collation: CollationSeq::default(),
                nulls_order: None,
            })
            .collect::<Vec<_>>();
        for (i, c) in compare_key_info
            .iter_mut()
            .enumerate()
            .take(partition_by_len)
        {
            // After rewriting, partition_by entries are Expr::Column references to the
            // subquery. Duplicates reference the same column index, so we find the entry
            // that references column i (the i-th unique partition column) to get the
            // correct collation.
            let expr = window
                .partition_by
                .iter()
                .find(|e| matches!(e, Expr::Column { column, .. } if *column == i))
                .unwrap_or(&window.partition_by[i]);
            let maybe_collation = get_collseq_from_expr(expr, &plan.table_references)?;
            c.collation = maybe_collation.unwrap_or_default();
        }
        program.emit_insn(Insn::Compare {
            start_reg_a: registers.src_columns_start,
            start_reg_b: reg_partition_start,
            count: partition_by_len,
            key_info: compare_key_info,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: new_partition_label,
            target_pc_eq: same_partition_label,
            target_pc_gt: new_partition_label,
        });

        program.preassign_label_to_next_insn(new_partition_label);
        program.add_comment(program.offset(), "detected new partition");
        program.emit_insn(Insn::Gosub {
            target_pc: labels.flush_buffer,
            return_reg: registers.flush_buffer_return_offset,
        });
        // Reset rowid to signal the start of processing a new partition.
        program.emit_insn(Insn::Null {
            dest: registers.rowid,
            dest_end: None,
        });
        program.emit_insn(Insn::Copy {
            src_reg: registers.src_columns_start,
            dst_reg: reg_partition_start,
            extra_amount: partition_by_len - 1,
        });

        program.preassign_label_to_next_insn(same_partition_label);
    }

    Ok(())
}

fn emit_load_order_by_columns(
    program: &mut ProgramBuilder,
    window: &Window,
    registers: &WindowRegisters,
) {
    if let Some(reg_new_order_by_columns_start) = registers.new_order_by_columns_start {
        // Source columns are deduplicated and may appear in a different order than
        // the ORDER BY terms. Therefore, we must restore the original ORDER BY layout
        // here by copying the values into an array of registers.
        for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
            match expr {
                Expr::Column { column, .. } => {
                    program.emit_insn(Insn::Copy {
                        src_reg: registers.src_columns_start + column,
                        dst_reg: reg_new_order_by_columns_start + i,
                        extra_amount: 0,
                    });
                }
                _ => unreachable!("expected Column, got {:?}", expr),
            }
        }
    }
}

fn emit_insert_row_into_buffer(
    program: &mut ProgramBuilder,
    registers: &WindowRegisters,
    cursors: &WindowCursors,
    input_column_count: &usize,
    table_name: &str,
) {
    let reg_record = program.alloc_register();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(registers.src_columns_start),
        count: to_u16(*input_column_count),
        dest_reg: to_u16(reg_record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: cursors.csr_write,
        rowid_reg: registers.rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.csr_write,
        key_reg: registers.rowid,
        record_reg: reg_record,
        flag: InsertFlags::new().require_seek(),
        table_name: table_name.to_string(),
    });
}

/// The three frame-cursor operations. Mirrors SQLite's `WINDOW_AGGSTEP`,
/// `WINDOW_RETURN_ROW`, and `WINDOW_AGGINVERSE` constants in
/// `window.c:1765-1773`. Each operation reads from one cursor, fires the
/// per-function callback for it, then advances that cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowOp {
    /// A row entered the frame at its right edge: read `csr_end`, fire
    /// each function's xStep, advance `csr_end`.
    AggStep,
    /// Emit the row at `csr_current` to the outer query, then advance
    /// `csr_current`. xValue is computed for each function before the
    /// emit.
    ReturnRow,
    /// A row left the frame at its left edge: read `csr_start`, fire
    /// each function's xInverse, advance `csr_start`. No-op (and never
    /// emitted) for windows whose frame start is UNBOUNDED PRECEDING.
    AggInverse,
}

/// The window op after which a buffered row becomes unreachable and can be
/// deleted from the ephemeral table. Mirrors SQLite's `s.eDelete`
/// (`window.c:2839-2869`): the emitter deletes the row at
/// `window.c:2346` (`if( op==p->eDelete ) OP_Delete`). `None` is SQLite's
/// `eDelete == 0` — never delete, keep the whole partition buffered.
///
/// Frame-caching functions (`first_value` / `nth_value` / `lag` / `lead`)
/// seek arbitrary rows through `csr_app` at output time, so their rows must
/// survive until the partition is flushed. SQLite's `windowCacheFrame` forces
/// `eDelete == 0` for exactly that set; we detect it via `has_csr_app`.
///
/// Only frames with an UNBOUNDED PRECEDING start (delete at `RETURN_ROW`) or a
/// sliding start (delete at `AGGINVERSE`) are reachable today — explicit frame
/// clauses, and therefore SQLite's offset-bearing arms (`ROWS ... N PRECEDING`
/// end → `AGGSTEP`, `ROWS N FOLLOWING` start → `RETURN_ROW`), are rejected
/// before emit. The start-boundary split here matches the one the flush loop
/// uses to decide whether to fire `AGGINVERSE` at all; a future `FOLLOWING`
/// frame start needs both sites updated together.
fn window_delete_op(window: &Window, has_csr_app: bool) -> Option<WindowOp> {
    if has_csr_app {
        return None;
    }
    match window.frame.start {
        crate::translate::plan::FrameBoundary::UnboundedPreceding => Some(WindowOp::ReturnRow),
        _ => Some(WindowOp::AggInverse),
    }
}

/// Emit one of the three frame-cursor operations, mirroring SQLite's
/// `windowCodeOp` helper (`window.c:2229-2376`).
///
/// `break_on_eof` is `Some(label)` only at flush time, when the caller wants
/// the `RETURN_ROW` loop to exit cleanly on EOF instead of falling through.
///
/// Under RANGE/GROUPS frames (`bPeer == true`), the cursor advances loop
/// through every peer-equal row before returning — one `AggStep` call
/// xSteps the whole previous peer group, one `RETURN_ROW` call emits the
/// whole previous peer group with the same accumulated state. Under ROWS
/// frames (`bPeer == false`), each call advances by exactly one row.
fn emit_window_op(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    op: WindowOp,
    break_on_eof: Option<BranchOffset>,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let buffer_table_name = meta.buffer_table_name.clone();
    let order_by_len = window.order_by.len();
    let frame_mode = window.frame.mode;
    // bPeer mirrors SQLite's `WindowCodeArg.eFrmType != TK_ROWS` test
    // (window.c:2247). Under RANGE / GROUPS frames, advances loop through
    // peer-equal rows; with no ORDER BY every row is peer-equal so the
    // loop runs to EOF.
    let b_peer = frame_mode != turso_parser::ast::FrameMode::Rows;

    let label_done = program.allocate_label();
    // Cursor + per-cursor peer reference register for each op. Mirrors
    // SQLite's switch at `window.c:2315-2344` plus the reg pickup at
    // `window.c:2317-2336`.
    let (cursor_for_op, peer_ref_reg) = match op {
        WindowOp::AggStep => (cursors.csr_end, registers.peer_end_reg),
        WindowOp::ReturnRow => (cursors.csr_current, registers.peer_current_reg),
        WindowOp::AggInverse => (
            cursors
                .csr_start
                .expect("AggInverse can only be emitted when the window has a moving frame start"),
            registers.peer_start_reg,
        ),
    };

    // RETURN_ROW finalizes accumulators before emitting (SQLite's
    // windowAggFinal at window.c:2284). first_value / nth_value / lag /
    // lead have no accumulator value to read — their result registers
    // are written directly inside `emit_return_one_row` via positional
    // lookup helpers.
    if matches!(op, WindowOp::ReturnRow) {
        for (i, func) in window.functions.iter().enumerate() {
            if matches!(
                &func.func,
                AccumulatorFunc::Window(
                    WindowFunc::FirstValue
                        | WindowFunc::NthValue
                        | WindowFunc::Lag
                        | WindowFunc::Lead
                ),
            ) {
                continue;
            }
            program.emit_insn(Insn::AggValue {
                acc_reg: registers.acc_start + i,
                dest_reg: registers.acc_result_start + i,
                func: func.func.clone(),
            });
        }
    }

    let label_continue = program.allocate_label();
    program.preassign_label_to_next_insn(label_continue);

    match op {
        WindowOp::AggStep => {
            emit_function_step(program, t_ctx, plan, cursors.csr_end)?;
            // Record the rowid of the row just stepped into the frame. This
            // runs once per row in the peer-loop, so after AGGSTEP has walked
            // the whole peer group `frame_end_rowid` holds the group's last
            // rowid — the frame end that first_value / nth_value seek against.
            if let Some(frame_end_rowid) = registers.frame_end_rowid {
                program.emit_insn(Insn::RowId {
                    cursor_id: cursors.csr_end,
                    dest: frame_end_rowid,
                });
            }
        }
        WindowOp::ReturnRow => {
            emit_return_one_row(program, t_ctx, plan)?;
        }
        WindowOp::AggInverse => {
            emit_function_inverse(program, t_ctx, plan)?;
        }
    }

    // Delete the row that just left the reachable set (SQLite window.c:2346,
    // `if( op==p->eDelete ) OP_Delete`), before advancing the cursor past it.
    if window_delete_op(window, cursors.csr_app.is_some()) == Some(op) {
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
    //     fall through to the peer-loop check below.
    //   - bPeer=false, no break_on_eof: Next; on EOF fall through;
    //     no peer check; label_done is the next instruction.
    let label_after_next = program.allocate_label();
    program.emit_insn(Insn::Next {
        cursor_id: cursor_for_op,
        pc_if_next: label_after_next,
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

    if b_peer && order_by_len > 0 {
        // Peer-loop check, mirroring SQLite's `windowIfNewPeer` (which
        // bundles the Compare + Jump-if-equal + Copy-on-fall-through
        // at `window.c:2057-2078`). The peer reference register is
        // per-cursor (`peer_start_reg` / `peer_current_reg` /
        // `peer_end_reg`) so the start / current / end cursors can be
        // at different peer groups during flush — required for
        // percent_rank / cume_dist.
        let temp_start = program.alloc_registers(order_by_len);
        for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
            if let Expr::Column { column, .. } = expr {
                program.emit_insn(Insn::Column {
                    cursor_id: cursor_for_op,
                    column: *column,
                    dest: temp_start + i,
                    default: None,
                });
            } else {
                unreachable!("expected Column in window.order_by, got {:?}", expr);
            }
        }
        let key_info = build_order_by_key_info(window, &plan.table_references)?;
        let peer_ref = peer_ref_reg.expect(
            "per-cursor peer reference register must exist under RANGE/GROUPS with ORDER BY",
        );
        let label_update_peer = program.allocate_label();
        program.emit_insn(Insn::Compare {
            start_reg_a: peer_ref,
            start_reg_b: temp_start,
            count: order_by_len,
            key_info,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: label_update_peer,
            target_pc_eq: label_continue,
            target_pc_gt: label_update_peer,
        });
        // Fall-through path (new peer group): copy the new row's ORDER
        // BY values into the cursor's peer reference register so the
        // next peer-loop iteration compares against this group.
        program.preassign_label_to_next_insn(label_update_peer);
        program.emit_insn(Insn::Copy {
            src_reg: temp_start,
            dst_reg: peer_ref,
            extra_amount: order_by_len - 1,
        });
    } else if b_peer {
        // RANGE/GROUPS with no ORDER BY: every row in the partition is
        // peer-equal, so the loop always continues until EOF (handled by
        // the Next/Goto pair above).
        program.emit_insn(Insn::Goto {
            target_pc: label_continue,
        });
    }

    program.preassign_label_to_next_insn(label_done);

    Ok(())
}

/// Emit per-function xStep / xInverse calls reading from `read_csr`.
///
/// Mirrors SQLite's `windowAggStep` (`window.c:1658-1762`): for each
/// function, each argument is loaded from the cursor's current row into a
/// fresh register block (SQLite's `p->regArg`), then `AggStep` /
/// `AggInverse` is emitted referencing that block. Crucially, the load
/// destinations are *separate* from `src_columns_start` so the source
/// coroutine's register block (which holds the new-partition row being
/// processed) isn't trampled while we drain the previous partition.
///
/// Argument expressions are pushed into the resolver's expression cache
/// during the call so `translate_aggregation_step`'s recursive
/// `translate_expr` resolves each `Expr::Column` to the arg-load register
/// we just populated, instead of to `src_columns_start`. The cache is
/// restored on the way out.
fn emit_function_step(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    read_csr: CursorID,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let acc_start = meta.registers.acc_start;

    // Save cache state so the per-arg overrides we push below don't leak
    // out to other parts of the emit pipeline (e.g. emit_return_one_row).
    let initial_cache_len = t_ctx.resolver.expr_to_reg_cache.len();
    let cache_was_enabled = t_ctx.resolver.expr_to_reg_cache_enabled;

    for (i, func) in window.functions.iter().enumerate() {
        // first_value / nth_value / lag / lead are WINDOWFUNCNOOP in
        // SQLite (window.c:591, 1727-1730): no step function — the value
        // is computed at output time via `SeekRowid` on `csr_app`. See
        // `emit_first_value_nth_value_lookup` and `emit_lag_lead_lookup`.
        if matches!(
            &func.func,
            AccumulatorFunc::Window(
                WindowFunc::FirstValue | WindowFunc::NthValue | WindowFunc::Lag | WindowFunc::Lead
            )
        ) {
            continue;
        }
        let acc_reg = acc_start + i;
        let args: Vec<Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| (**a).clone()).collect(),
            Expr::FunctionCallStar { .. } => vec![],
            _ => unreachable!("window functions are FunctionCall or FunctionCallStar expressions"),
        };

        // Load each Expr::Column arg from the frame cursor into a fresh
        // reg, then push a cache entry so translate_expr on that same Expr
        // resolves to the freshly-loaded reg. Args that aren't simple
        // column refs (rare after the planner rewrite) fall through to
        // translate_expr's default path.
        let arg_load_start = (!args.is_empty()).then(|| program.alloc_registers(args.len()));
        if let Some(base) = arg_load_start {
            for (j, arg) in args.iter().enumerate() {
                if let Expr::Column { column, .. } = arg {
                    program.emit_insn(Insn::Column {
                        cursor_id: read_csr,
                        column: *column,
                        dest: base + j,
                        default: None,
                    });
                    t_ctx.resolver.cache_expr_reg(
                        std::borrow::Cow::Owned(arg.clone()),
                        base + j,
                        false,
                        None,
                    );
                }
            }
        }
        t_ctx.resolver.expr_to_reg_cache_enabled = true;

        match &func.func {
            AccumulatorFunc::Agg(agg_func) => {
                // FILTER predicate also loads from the frame cursor.
                let filter_skip_label = if let Some(filter_expr) =
                    func.rewritten.as_ref().and_then(|r| r.filter_expr.as_ref())
                {
                    let label = program.allocate_label();
                    let filter_reg = program.alloc_register();
                    if let Expr::Column { column, .. } = filter_expr {
                        program.emit_insn(Insn::Column {
                            cursor_id: read_csr,
                            column: *column,
                            dest: filter_reg,
                            default: None,
                        });
                    } else {
                        translate_expr(
                            program,
                            Some(&plan.table_references),
                            filter_expr,
                            filter_reg,
                            &t_ctx.resolver,
                        )?;
                    }
                    program.emit_insn(Insn::IfNot {
                        reg: filter_reg,
                        target_pc: label,
                        jump_if_null: true,
                    });
                    Some(label)
                } else {
                    None
                };

                translate_aggregation_step(
                    program,
                    &plan.table_references,
                    AggArgumentSource::new_from_expression(
                        agg_func,
                        &args,
                        &Distinctness::NonDistinct,
                    ),
                    acc_reg,
                    &t_ctx.resolver,
                    None,
                )?;

                if let Some(label) = filter_skip_label {
                    program.preassign_label_to_next_insn(label);
                }
            }
            AccumulatorFunc::Window(win_func) => {
                // 0-ary window funcs (row_number) ignore `col`; the runtime
                // only reads `state.registers[col + i]` for i in 0..arity.
                program.emit_insn(Insn::AggStep {
                    acc_reg,
                    col: arg_load_start.unwrap_or(0),
                    delimiter: 0,
                    func: AccumulatorFunc::Window(win_func.clone()),
                    comparator: None,
                    collation: None,
                });
            }
        }
    }

    // Restore expr-cache state so the per-function arg overrides don't
    // leak into later emit calls (e.g. emit_return_one_row's outer-query
    // result emission, which expects its own result_columns_start mapping).
    t_ctx.resolver.expr_to_reg_cache.truncate(initial_cache_len);
    t_ctx.resolver.expr_to_reg_cache_enabled = cache_was_enabled;

    Ok(())
}

/// Emit per-function `AggInverse` calls — the counterpart of
/// `emit_function_step` for the frame-start side. Mirrors the xInverse
/// dispatch inside SQLite's `windowAggStep(... bInverse=1, ...)`
/// (window.c:2329).
///
/// For our supported moving-start frames (ntile, percent_rank,
/// cume_dist) xInverse is purely state-mutating: it increments a
/// counter the function's xValue reads. We pass `col = 0` because none
/// of these read leaving-row columns. Aggregate inverses (sum / count
/// / avg under a sliding frame) would need the leaving row's args loaded
/// from `csr_start` first — that path lands when we accept user-specified
/// custom frames.
fn emit_function_inverse(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let acc_start = meta.registers.acc_start;
    for (i, func) in window.functions.iter().enumerate() {
        program.emit_insn(Insn::AggInverse {
            acc_reg: acc_start + i,
            col: 0,
            delimiter: 0,
            func: func.func.clone(),
            comparator: None,
        });
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
/// `csr_app` is OpenDup'd off `csr_current` and only seeks at output time, so
/// it doesn't disturb the frame-cursor positions. The function's result is
/// written to its cached `acc_result_start + i` register, which the
/// expression-cache then resolves to when `emit_select_result` reads the
/// function expression.
fn emit_lag_lead_lookup(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let acc_result_start = registers.acc_result_start;
    let csr_current = cursors.csr_current;

    for (i, func) in window.functions.iter().enumerate() {
        let is_lead = match &func.func {
            AccumulatorFunc::Window(WindowFunc::Lag) => false,
            AccumulatorFunc::Window(WindowFunc::Lead) => true,
            _ => continue,
        };
        let csr_app = cursors
            .csr_app
            .expect("csr_app must exist when a window contains lag / lead");
        let result_reg = acc_result_start + i;

        let args: Vec<&Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| a.as_ref()).collect(),
            _ => unreachable!("lag / lead are always Expr::FunctionCall"),
        };

        let arg0_col = match args.first() {
            Some(Expr::Column { column, .. }) => *column,
            other => unreachable!(
                "lag/lead arg[0] must be a buffer column reference after planner rewrite, got {other:?}"
            ),
        };

        // Default value first: NULL when nArg < 3, else read arg[2] from csr_current.
        if args.len() < 3 {
            program.emit_insn(Insn::Null {
                dest: result_reg,
                dest_end: None,
            });
        } else if let Expr::Column { column, .. } = args[2] {
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: *column,
                dest: result_reg,
                default: None,
            });
        } else {
            unreachable!(
                "lag/lead arg[2] must be a buffer column reference after planner rewrite, got {:?}",
                args[2]
            );
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
            let offset_col = match args[1] {
                Expr::Column { column, .. } => *column,
                other => unreachable!(
                    "lag/lead arg[1] must be a buffer column reference after planner rewrite, got {other:?}"
                ),
            };
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

/// Emit one output row's worth of work: populate `result_columns_start` from
/// `csr_current`, then either send the row to the outer query (no ORDER BY)
/// or insert it into the order-by sorter. Mirrors SQLite's
/// `windowReturnOneRow` (`window.c:1816-1990`), restricted to the
/// streaming code path.
/// Emit per-output-row positional lookup for every `first_value` /
/// `nth_value` in the current window. Mirrors SQLite's first_value /
/// nth_value arms in `windowReturnOneRow` (`window.c:1940-1965`):
///
/// ```text
///   reg_result := NULL
///   tmp := 1                                 (first_value, N=1)
///     -- or --
///   tmp := arg[1] from csr_current           (nth_value, per-row N)
///   MustBeInt tmp; if tmp <= 0: error
///   tmp := tmp + frame_start_rowid - 1       (frame_start_rowid = 1 for UB-CR)
///   if tmp > frame_end_rowid: goto lbl       (target past frame end → NULL)
///   SeekRowid(csr_app, tmp)
///   reg_result := Column(csr_app, arg[0])
///   lbl:
/// ```
///
/// The frames the planner currently lets first_value/nth_value through
/// with all have `frame_start_rowid = 1` (RANGE UNBOUNDED PRECEDING TO
/// CURRENT ROW). Sliding `ROWS BETWEEN N PRECEDING AND CURRENT ROW` for
/// these functions is rejected at parse — it needs a stable
/// per-partition N register (parallel to SQLite's `pWin->regApp`) to
/// compute `frame_start_rowid` at emit time.
fn emit_first_value_nth_value_lookup(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let acc_result_start = registers.acc_result_start;
    let csr_current = cursors.csr_current;

    for (i, func) in window.functions.iter().enumerate() {
        let is_nth = match &func.func {
            AccumulatorFunc::Window(WindowFunc::FirstValue) => false,
            AccumulatorFunc::Window(WindowFunc::NthValue) => true,
            _ => continue,
        };
        let csr_app = cursors
            .csr_app
            .expect("csr_app must exist when a window contains first_value or nth_value");
        let result_reg = acc_result_start + i;

        let args: Vec<&Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| a.as_ref()).collect(),
            _ => unreachable!("first_value / nth_value are always Expr::FunctionCall"),
        };
        let arg_value_col = match args.first() {
            Some(Expr::Column { column, .. }) => *column,
            other => unreachable!(
                "first_value/nth_value arg[0] must be a subquery column ref \
                 after planner rewrite, got {other:?}"
            ),
        };

        // Default result: NULL (stays NULL on seek miss or N past frame end).
        program.emit_insn(Insn::Null {
            dest: result_reg,
            dest_end: None,
        });

        // Compute target rowid. For UB-CR frames, frame_start_rowid = 1,
        // so target = N (read fresh per output row for nth_value, 1 for
        // first_value).
        let target_reg = program.alloc_register();
        if is_nth {
            let n_col = match args.get(1) {
                Some(Expr::Column { column, .. }) => *column,
                other => unreachable!(
                    "nth_value arg[1] must be a subquery column ref \
                     after planner rewrite, got {other:?}"
                ),
            };
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

        // Two distinct "no value" outcomes, kept separate:
        //
        // * target past the frame end — a legitimate NULL (the Nth row lies
        //   outside this row's frame). `frame_end_rowid` is the last rowid
        //   stepped into the frame, NOT the emitted row's own rowid: under
        //   RANGE the frame runs to the current row's last peer, so an early
        //   row of a peer group must still see its whole group.
        //
        // * target within the frame but `SeekRowid` misses — impossible:
        //   frame rows carry dense rowids 1..frame_end and are all buffered
        //   by emit time. Treat it as an invariant violation and Halt rather
        //   than silently returning NULL. Mirrors SQLite finding its row via
        //   csrApp unconditionally.
        let frame_end_rowid = registers
            .frame_end_rowid
            .expect("frame_end_rowid must exist when a window contains first_value/nth_value");
        let lbl_past_frame_end = program.allocate_label();
        let lbl_missing_row = program.allocate_label();
        let lbl_done = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: target_reg,
            rhs: frame_end_rowid,
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

    for (i, (_, col_idx)) in expressions_referencing_subquery.iter().enumerate() {
        let reg_result = registers.result_columns_start + i;
        program.emit_column_or_rowid(cursors.csr_current, *col_idx, reg_result);
    }

    // Per-row lookups for functions whose value is computed at output
    // time (first_value / nth_value / lag / lead). Each writes the
    // function's value register before the outer query reads it via
    // `emit_select_result`'s expression-cache lookup.
    emit_first_value_nth_value_lookup(program, t_ctx, plan)?;
    emit_lag_lead_lookup(program, t_ctx, plan)?;

    // The select-result / sorter-insert code is a shared subroutine —
    // RETURN_ROW is emitted at several sites (streaming step + flush
    // loops), and jump targets inside the row-output code (SELECT
    // DISTINCT's on-conflict label in particular) must resolve to exactly
    // one address. Mirrors SQLite's `OP_Gosub regGosub, addrGosub` at the
    // end of `windowReturnOneRow` (window.c:1988).
    program.emit_insn(Insn::Gosub {
        target_pc: labels.row_output,
        return_reg: registers.row_output_return,
    });

    Ok(())
}

/// Emit the single row-output subroutine that every RETURN_ROW site Gosubs
/// to: send the populated result registers to the outer query (or the
/// order-by sorter), applying OFFSET, LIMIT and SELECT DISTINCT
/// deduplication. This is the window-loop half of what SQLite emits once
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

/// Emit the flush subroutine for window processing — the partition-end /
/// end-of-source code that drains any remaining rows still ahead of the
/// frame-end cursor and emits all rows still ahead of the current cursor.
///
/// This is the Rust port of SQLite's flush block at `window.c:3043-3105`,
/// restricted to the `UNBOUNDED PRECEDING TO CURRENT ROW` path
/// (`window.c:3085-3094`):
///
/// ```text
///   Rewind csr_write → if empty, jump to label_empty
///   emit_window_op AGGSTEP, break_on_eof=None
///       ↑ drains the last buffered row whose AggStep was deferred
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

    let label_empty = program.allocate_label();
    let label_break = program.allocate_label();

    // Fallthrough entry: the source loop just ended. Null the return
    // register so the trailing Return falls through past this subroutine.
    program.add_comment(program.offset(), "return remaining buffered rows");
    program.emit_insn(Insn::Null {
        dest: registers.flush_buffer_return_offset,
        dest_end: None,
    });

    // Subroutine entry: partition boundary Gosub lands here.
    program.preassign_label_to_next_insn(labels.flush_buffer);

    // Detect empty partition. Rewind csr_write sets its position to the
    // first row and jumps to label_empty when the table is empty; csr_write
    // isn't used past flush so resetting its position is harmless.
    program.emit_insn(Insn::Rewind {
        cursor_id: cursors.csr_write,
        pc_if_empty: label_empty,
    });

    // Drain step: the very last row inserted in the main loop hasn't had
    // AggStep called against it (under ROWS) or its peer group might still
    // be incomplete (under RANGE). One AGGSTEP advances csr_end through the
    // remaining rows.
    emit_window_op(program, t_ctx, plan, WindowOp::AggStep, None)?;

    let label_loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(label_loop_start);
    emit_window_op(program, t_ctx, plan, WindowOp::ReturnRow, Some(label_break))?;
    // Functions with a moving frame start (ntile, percent_rank,
    // cume_dist) fire AggInverse between output rows so xValue's next
    // read sees the advanced state. The window is split per-coerced-frame
    // at plan time, so every function in this window has the same start
    // boundary — one window-level gate. Mirrors SQLite's flush block at
    // window.c:3091 (`windowCodeOp(..., WINDOW_AGGINVERSE, regStart, 0)`).
    let window = plan.window.as_ref().expect("missing window");
    if !matches!(
        window.frame.start,
        crate::translate::plan::FrameBoundary::UnboundedPreceding
    ) {
        emit_window_op(program, t_ctx, plan, WindowOp::AggInverse, None)?;
    }
    program.emit_insn(Insn::Goto {
        target_pc: label_loop_start,
    });

    program.preassign_label_to_next_insn(label_break);
    program.preassign_label_to_next_insn(label_empty);

    program.emit_insn(Insn::ResetSorter {
        cursor_id: cursors.csr_current,
    });
    program.emit_insn(Insn::Return {
        return_reg: registers.flush_buffer_return_offset,
        can_fallthrough: true,
    });

    // The fallthrough entry (end of source) continues past the Return;
    // jump over the row-output subroutine body that follows.
    program.emit_insn(Insn::Goto {
        target_pc: labels.window_processing_end,
    });

    emit_row_output_subroutine(program, t_ctx, plan)?;

    program.preassign_label_to_next_insn(labels.window_processing_end);

    Ok(())
}
