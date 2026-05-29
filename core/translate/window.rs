use crate::alloc::TursoSliceExt;
use crate::function::AccumulatorFunc;
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
    Aggregate, Distinctness, FrameBoundary, JoinOrderMember, JoinedTable, QueryDestination,
    ResultSetColumn, RewrittenWindowCall, SelectPlan, TableReferences, Window, WindowFunction,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::result_row::emit_select_result;
use crate::translate::subquery::plan_subqueries_from_select_plan;
use crate::types::KeyInfo;
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u16, {CmpInsFlags, InsertFlags, Insn},
};
use crate::vdbe::{BranchOffset, CursorID};
use crate::Connection;
use crate::Result;
use crate::{turso_assert, turso_assert_eq};
use std::mem;
use turso_parser::ast::Name;
use turso_parser::ast::{Expr, Literal, Over, ResolveType, SortOrder, TableInternalId};

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
    buffer_mode: WindowBufferMode,
    pub nth_value: Option<NthValueMetadata>,
    /// Number of input columns in the source subquery.
    pub src_column_count: usize,
    /// Maps expressions in the current query that reference subquery columns
    /// to their corresponding column indexes in the subquery’s result.
    pub expressions_referencing_subquery: Vec<(&'a Expr, usize)>,
    pub buffer_table_name: String,
}

/// Controls whether a window may discard a saved row after returning it.
/// Keeping a whole partition is more expensive, so it is only used when a
/// later result may need to read an earlier row again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowBufferMode {
    DeleteReturnedRows,
    KeepPartitionRows { rowid_one_register: usize },
}

impl WindowBufferMode {
    fn for_window(window: &Window, program: &mut ProgramBuilder) -> Self {
        let keeps_partition_rows = window.functions.iter().any(|function| {
            matches!(
                &function.func,
                AccumulatorFunc::Window(window_function)
                    if window_function.needs_rows_after_returning_them()
            )
        });
        if keeps_partition_rows {
            let rowid_one_register = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: rowid_one_register,
            });
            Self::KeepPartitionRows { rowid_one_register }
        } else {
            Self::DeleteReturnedRows
        }
    }

    fn keeps_partition_rows(self) -> bool {
        matches!(self, Self::KeepPartitionRows { .. })
    }
}

/// State used to find an `nth_value` result in the saved rows. Saved rows get
/// rowids 1, 2, 3, and so on in window order within each partition. Custom
/// frames are not supported, so every frame starts at rowid 1 and the position
/// argument is also the rowid to read.
#[derive(Debug)]
pub struct NthValueMetadata {
    /// Finds a saved row by its ordered position without moving the cursor
    /// that returns rows.
    pub row_by_position_cursor: CursorID,
    /// Holds the rowid of the last row in the current window frame. The first
    /// row of the next sort group is saved before the current group is
    /// returned, so the lookup must not read beyond this rowid.
    pub frame_end_rowid_register: usize,
}

#[derive(Debug)]
pub struct WindowLabels {
    /// Address of the subroutine for flushing buffered rows
    pub flush_buffer: BranchOffset,
    /// Address of the end of window processing
    pub window_processing_end: BranchOffset,
}

#[derive(Debug)]
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
    /// Start of the register array holding ORDER BY column values from the previous row.
    /// These are used to compare against the current row to determine peer relationships.
    pub prev_order_by_columns_start: Option<usize>,
}

/// Cursors that can move independently over the saved rows. Separate cursors
/// are needed because inserting a row must not move the row being returned.
#[derive(Debug)]
pub struct WindowCursors {
    /// Used to `Insert` newly-buffered source rows. Position is implicit
    /// (advances on each `NewRowid` + `Insert`).
    pub csr_write: CursorID,
    /// The row currently being returned to the outer query; result-column
    /// propagation reads from here.
    pub csr_current: CursorID,
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

        // Save rows because a window result may depend on rows that arrived
        // earlier. `nth_value` keeps every row already seen in the current
        // partition so it can find one by its ordered position.
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
        // The "current" cursor is the primary cursor on the ephemeral
        // buffer; the write cursor is an OpenDup'd duplicate that shares
        // the same underlying B-tree but tracks an independent position.
        let cursor_csr_current =
            program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        let cursor_csr_write =
            program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor_csr_current,
            is_table: true,
        });
        program.emit_insn(Insn::OpenDup {
            original_cursor_id: cursor_csr_current,
            new_cursor_id: cursor_csr_write,
        });
        let buffer_mode = WindowBufferMode::for_window(window, program);
        let has_nth_value = window.functions.iter().any(|func| {
            matches!(
                &func.func,
                AccumulatorFunc::Window(crate::function::WindowFunc::NthValue)
            )
        });
        let nth_value = if has_nth_value {
            turso_assert!(
                matches!(&window.frame.start, FrameBoundary::UnboundedPreceding),
                "nth_value row lookup requires a frame that starts at the first partition row"
            );
            let row_by_position_cursor =
                program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
            program.emit_insn(Insn::OpenDup {
                original_cursor_id: cursor_csr_current,
                new_cursor_id: row_by_position_cursor,
            });
            let frame_end_rowid_register = program.alloc_registers_and_init_w_null(1);
            Some(NthValueMetadata {
                row_by_position_cursor,
                frame_end_rowid_register,
            })
        } else {
            None
        };

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
                prev_order_by_columns_start: alloc_optional_registers(program, order_by_len),
                new_order_by_columns_start: alloc_optional_registers(program, order_by_len),
            },
            cursors: WindowCursors {
                csr_write: cursor_csr_write,
                csr_current: cursor_csr_current,
            },
            buffer_mode,
            nth_value,
            src_column_count,
            expressions_referencing_subquery,
            buffer_table_name: buffer_table.name.clone(),
        });

        Ok(())
    }
    /// Emits bytecode to process a single row of the window’s input (always a subquery).
    ///
    /// Note:
    /// The **buffer table** mentioned below saves rows because a window result may
    /// depend on rows that arrived earlier. Most rows are removed after they are
    /// returned. `nth_value` keeps them because later results may refer back to them.
    ///
    /// High-level overview:
    /// - Each row from the subquery is read, and its ORDER BY columns are loaded into
    ///   dedicated registers for comparison and partitioning purposes.
    /// - If the row starts a new partition (based on PARTITION BY columns), the buffer
    ///   and accumulators are flushed or reset as needed.
    /// - Rows are compared against the previous row to determine if they are "peers"
    ///   (i.e., have the same ORDER BY values). Non-peer rows may trigger flushing
    ///   of intermediate results.
    /// - The row is then inserted into the window’s buffer table.
    /// - Aggregate steps for any window functions are executed.
    pub fn emit_window_loop_source(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx,
        plan: &SelectPlan,
    ) -> crate::Result<()> {
        let WindowMetadata {
            labels,
            registers,
            cursors,
            buffer_mode,
            nth_value,
            src_column_count: input_column_count,
            buffer_table_name,
            ..
        } = t_ctx.meta_window.as_ref().expect("missing window metadata");
        let window = plan.window.as_ref().expect("missing window");

        emit_load_order_by_columns(program, window, registers);
        emit_flush_buffer_if_new_partition(
            program,
            labels,
            registers,
            cursors,
            *buffer_mode,
            window,
            plan,
        )?;
        emit_reset_state_if_new_partition(program, registers, nth_value.as_ref(), window);
        if let WindowBufferMode::KeepPartitionRows { rowid_one_register } = *buffer_mode {
            // Keep the return cursor on the first row of the next sort group,
            // as SQLite does. That row must be inserted before the completed
            // group is returned so the cursor does not run past the table.
            emit_insert_row_into_buffer(
                program,
                registers,
                cursors,
                input_column_count,
                buffer_table_name,
            );
            emit_rewind_return_cursor_for_first_partition_row(
                program,
                cursors.csr_current,
                registers.rowid,
                rowid_one_register,
            );
            emit_flush_buffer_if_not_peer(program, labels, registers, window, plan)?;
        } else {
            emit_flush_buffer_if_not_peer(program, labels, registers, window, plan)?;
            emit_insert_row_into_buffer(
                program,
                registers,
                cursors,
                input_column_count,
                buffer_table_name,
            );
        }
        emit_aggregation_step(program, window, &t_ctx.resolver, plan, registers)?;
        if let Some(nth_value) = nth_value {
            program.emit_insn(Insn::Copy {
                src_reg: registers.rowid,
                dst_reg: nth_value.frame_end_rowid_register,
                extra_amount: 0,
            });
        }

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
    cursors: &WindowCursors,
    buffer_mode: WindowBufferMode,
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
        if buffer_mode.keeps_partition_rows() {
            program.emit_insn(Insn::ResetSorter {
                cursor_id: cursors.csr_current,
            });
        }
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

fn emit_reset_state_if_new_partition(
    program: &mut ProgramBuilder,
    registers: &WindowRegisters,
    nth_value: Option<&NthValueMetadata>,
    window: &Window,
) {
    let label_skip_reset_state = program.allocate_label();

    // If rowid is null, it means we are starting a new partition. It was either set by the code
    // initializing window processing or by code detecting the start of a new partition.
    program.emit_insn(Insn::NotNull {
        reg: registers.rowid,
        target_pc: label_skip_reset_state,
    });
    if let Some(dst_reg_start) = registers.new_order_by_columns_start {
        // Initialize previous ORDER BY values for the new partition. The first row of the
        // partition is compared to itself, not to the row from the previous partition.
        program.add_comment(
            program.offset(),
            "initialize previous peer register for new partition",
        );
        program.emit_insn(Insn::Copy {
            src_reg: dst_reg_start,
            dst_reg: registers
                .prev_order_by_columns_start
                .expect("prev_order_by_columns_start must exist"),
            extra_amount: window.order_by.len() - 1,
        });
    }
    // Since this is a new partition, we must reset accumulator registers.
    program.add_comment(program.offset(), "reset accumulator registers");
    program.emit_insn(Insn::Null {
        dest: registers.acc_start,
        dest_end: Some(registers.acc_start + window.functions.len() - 1),
    });
    if let Some(nth_value) = nth_value {
        program.emit_insn(Insn::Null {
            dest: nth_value.frame_end_rowid_register,
            dest_end: None,
        });
    }

    program.preassign_label_to_next_insn(label_skip_reset_state);
}

fn emit_flush_buffer_if_not_peer(
    program: &mut ProgramBuilder,
    labels: &WindowLabels,
    registers: &WindowRegisters,
    window: &Window,
    plan: &SelectPlan,
) -> Result<()> {
    if let Some(reg_new_order_by_columns_start) = registers.new_order_by_columns_start {
        let label_peer = program.allocate_label();
        let label_not_peer = program.allocate_label();
        let order_by_len = window.order_by.len();
        let reg_prev_order_by_columns_start = registers
            .prev_order_by_columns_start
            .expect("prev_order_by_columns_start must exist");

        program.add_comment(program.offset(), "compare ORDER BY columns to detect peer");
        program.emit_insn(Insn::Compare {
            start_reg_a: reg_prev_order_by_columns_start,
            start_reg_b: reg_new_order_by_columns_start,
            count: order_by_len,
            key_info: window_order_by_key_info(window, plan)?,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: label_not_peer,
            target_pc_eq: label_peer,
            target_pc_gt: label_not_peer,
        });

        program.preassign_label_to_next_insn(label_not_peer);
        program.add_comment(program.offset(), "detected non-peer row");
        program.emit_insn(Insn::Gosub {
            target_pc: labels.flush_buffer,
            return_reg: registers.flush_buffer_return_offset,
        });
        program.emit_insn(Insn::Copy {
            src_reg: reg_new_order_by_columns_start,
            dst_reg: reg_prev_order_by_columns_start,
            extra_amount: order_by_len - 1,
        });

        program.preassign_label_to_next_insn(label_peer);
    }

    Ok(())
}

fn window_order_by_key_info(window: &Window, plan: &SelectPlan) -> Result<Vec<KeyInfo>> {
    window
        .order_by
        .iter()
        .map(|(expr, _, _)| {
            Ok(KeyInfo {
                sort_order: SortOrder::Asc,
                collation: get_collseq_from_expr(expr, &plan.table_references)?.unwrap_or_default(),
                nulls_order: None,
            })
        })
        .collect()
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
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });
}

fn emit_rewind_return_cursor_for_first_partition_row(
    program: &mut ProgramBuilder,
    return_cursor: CursorID,
    inserted_rowid_register: usize,
    rowid_one_register: usize,
) {
    let cursor_ready = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: inserted_rowid_register,
        rhs: rowid_one_register,
        target_pc: cursor_ready,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Rewind {
        cursor_id: return_cursor,
        pc_if_empty: cursor_ready,
    });
    program.preassign_label_to_next_insn(cursor_ready);
}

fn emit_aggregation_step(
    program: &mut ProgramBuilder,
    window: &Window,
    resolver: &Resolver,
    plan: &SelectPlan,
    registers: &WindowRegisters,
) -> crate::Result<()> {
    for (i, func) in window
        .functions
        .iter()
        .enumerate()
        .filter(|(_, func)| match &func.func {
            // Aggregates and these window functions build their answers while
            // input rows arrive. Functions that wait for a result row are
            // handled later, when that row is returned.
            AccumulatorFunc::Agg(_) => true,
            AccumulatorFunc::Window(window_func) => {
                !window_func.calculates_answer_when_row_is_returned()
            }
        })
    {
        let reg_acc_start = registers.acc_start + i;
        match &func.func {
            AccumulatorFunc::Agg(agg_func) => {
                // The aggregation step is performed incrementally as each row from the subquery is
                // processed. Therefore, we don’t need to access the buffer table and can obtain
                // argument values directly by evaluating the expressions that reference the
                // subquery result columns. Use the rewritten form when available so the args
                // reference the subquery rather than the (no-longer-visible) original tables.
                let args = match func.current_expr() {
                    Expr::FunctionCall { args, .. } => {
                        args.iter().map(|a| (**a).clone()).collect()
                    }
                    Expr::FunctionCallStar { .. } => vec![],
                    _ => unreachable!(
                        "All window functions should be either FunctionCall or FunctionCallStar expressions"
                    ),
                };

                // FILTER controls whether the current input row contributes to the
                // running aggregate; it does not suppress the output row itself.
                let filter_skip_label = if let Some(filter_expr) =
                    func.rewritten.as_ref().and_then(|r| r.filter_expr.as_ref())
                {
                    let label = program.allocate_label();
                    let filter_reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        filter_expr,
                        filter_reg,
                        resolver,
                    )?;
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
                    reg_acc_start,
                    resolver,
                    None,
                )?;
                if let Some(label) = filter_skip_label {
                    program.preassign_label_to_next_insn(label);
                }
            }
            AccumulatorFunc::Window(win_func) => {
                // These functions need at most their first argument while
                // input rows arrive. Arguments that may differ for each result
                // row are read later from that saved row.
                let first_arg = match func.current_expr() {
                    Expr::FunctionCall { args, .. } => args.first(),
                    Expr::FunctionCallStar { .. } => None,
                    _ => unreachable!("window function must be a function call"),
                };
                let arg_reg = if let Some(arg) = first_arg {
                    let reg = program.alloc_register();
                    translate_expr(program, Some(&plan.table_references), arg, reg, resolver)?;
                    reg
                } else {
                    // TODO: Make `AggStep::col` optional. It currently requires a
                    // register number, so reserved register 0 means no argument.
                    0
                };
                program.emit_insn(Insn::AggStep {
                    acc_reg: reg_acc_start,
                    col: arg_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Window(win_func.clone()),
                    comparator: None,
                });
            }
        }
    }

    Ok(())
}

/// Emits bytecode to output all buffered rows produced by window processing.
///
/// The generated code has two possible entry points:
/// * **Fallthrough mode** (normal flow): After all source rows have been processed,
///   this code executes inline to flush any remaining buffered rows, then continues execution.
/// * **Subroutine mode** (jump into `labels.flush_buffer`): In this case the code
///   returns control to the address stored in `registers.flush_buffer_return_offset`
///   once all buffered rows are processed.
pub fn emit_window_results(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> crate::Result<()> {
    let WindowMetadata {
        labels,
        registers,
        cursors,
        buffer_mode,
        ..
    } = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");

    let label_empty = program.allocate_label();
    let label_window_processing_end = labels.window_processing_end;
    let label_flush_buffer = labels.flush_buffer;
    let reg_flush_buffer_return_offset = registers.flush_buffer_return_offset;
    let cursor_csr_current = cursors.csr_current;
    let keeps_partition_rows = buffer_mode.keeps_partition_rows();

    // All source rows have already been processed at this point.
    // In fallthrough mode, we are not returning to a caller — we just flush
    // the buffered rows and continue execution.
    program.add_comment(program.offset(), "return remaining buffered rows");
    program.emit_insn(Insn::Null {
        dest: reg_flush_buffer_return_offset,
        dest_end: None,
    });

    // If control jumps here (labels.flush_buffer), we are in subroutine mode.
    // In that case, after flushing the buffer, execution will return to the
    // address stored in `flush_buffer_return_offset`.
    program.preassign_label_to_next_insn(label_flush_buffer);

    if keeps_partition_rows {
        program.emit_insn(Insn::IsNull {
            reg: registers.rowid,
            target_pc: label_empty,
        });
    } else {
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor_csr_current,
            pc_if_empty: label_empty,
        });
    }

    emit_peer_group_flush(program, window, t_ctx, plan)?;

    program.preassign_label_to_next_insn(label_empty);

    if !keeps_partition_rows {
        program.emit_insn(Insn::ResetSorter {
            cursor_id: cursor_csr_current,
        });
    }
    program.emit_insn(Insn::Return {
        return_reg: reg_flush_buffer_return_offset,
        can_fallthrough: true,
    });

    program.preassign_label_to_next_insn(label_window_processing_end);

    Ok(())
}

fn emit_peer_group_flush(
    program: &mut ProgramBuilder,
    window: &Window,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> crate::Result<()> {
    let WindowMetadata {
        labels,
        registers,
        cursors,
        buffer_mode,
        nth_value,
        expressions_referencing_subquery,
        ..
    } = t_ctx.meta_window.as_ref().expect("missing window metadata");

    // Calculate shared answers before returning rows with equal sort values,
    // so every such row reuses the same answer. `nth_value` is calculated
    // below because its second argument is read separately from each row.
    for (i, func) in window.functions.iter().enumerate() {
        let answer_is_shared = match &func.func {
            AccumulatorFunc::Agg(_) => true,
            AccumulatorFunc::Window(window_func) => {
                !window_func.calculates_answer_when_row_is_returned()
            }
        };
        if answer_is_shared {
            program.emit_insn(Insn::AggValue {
                acc_reg: registers.acc_start + i,
                dest_reg: registers.acc_result_start + i,
                func: func.func.clone(),
            });
        }
    }

    let label_skip_returning_row = program.allocate_label();
    let label_loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(label_loop_start);

    // Propagate subquery result column values to the outer query (if any) or directly to
    // the final output that will be returned to the user, by copying them from the buffer table
    // into the dedicated registers.
    for (i, (_, col_idx)) in expressions_referencing_subquery.iter().enumerate() {
        let reg_result = registers.result_columns_start + i;
        program.emit_column_or_rowid(cursors.csr_current, *col_idx, reg_result);
    }
    // Calculate these answers while returning each row because rows with equal
    // sort values may still have different answers.
    for (i, func) in window.functions.iter().enumerate() {
        if let AccumulatorFunc::Window(win_func) = &func.func {
            if !win_func.calculates_answer_when_row_is_returned() {
                continue;
            }
            let dest_reg = registers.acc_result_start + i;
            if matches!(win_func, crate::function::WindowFunc::NthValue) {
                let Expr::FunctionCall { args, .. } = func.current_expr() else {
                    unreachable!("nth_value must be a function call");
                };
                let Expr::Column {
                    column: value_column,
                    ..
                } = args[0].as_ref()
                else {
                    unreachable!("rewritten nth_value value must be a subquery column");
                };
                let Expr::Column {
                    column: position_column,
                    ..
                } = args[1].as_ref()
                else {
                    unreachable!("rewritten nth_value position argument must be a subquery column");
                };
                let nth_value = nth_value
                    .as_ref()
                    .expect("nth_value requires a partition lookup cursor");
                let position_lookup_done = program.allocate_label();
                let missing_saved_row = program.allocate_label();
                let invalid_position = program.allocate_label();
                let valid_position = program.allocate_label();
                let position_register = program.alloc_register();
                let zero_register = program.alloc_register();
                program.emit_insn(Insn::Null {
                    dest: dest_reg,
                    dest_end: None,
                });
                program.emit_column_or_rowid(
                    cursors.csr_current,
                    *position_column,
                    position_register,
                );
                program.emit_insn(Insn::MustBeInt {
                    reg: position_register,
                    target_pc: Some(invalid_position),
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: zero_register,
                });
                program.emit_insn(Insn::Gt {
                    lhs: position_register,
                    rhs: zero_register,
                    target_pc: valid_position,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
                program.preassign_label_to_next_insn(invalid_position);
                program.emit_insn(Insn::Halt {
                    err_code: crate::error::SQLITE_ERROR,
                    description: "second argument to nth_value must be a positive integer"
                        .to_string(),
                    on_error: Some(ResolveType::Abort),
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(valid_position);
                program.emit_insn(Insn::Gt {
                    lhs: position_register,
                    rhs: nth_value.frame_end_rowid_register,
                    target_pc: position_lookup_done,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
                program.emit_insn(Insn::SeekRowid {
                    cursor_id: nth_value.row_by_position_cursor,
                    src_reg: position_register,
                    target_pc: missing_saved_row,
                });
                program.emit_column_or_rowid(
                    nth_value.row_by_position_cursor,
                    *value_column,
                    dest_reg,
                );
                program.emit_insn(Insn::Goto {
                    target_pc: position_lookup_done,
                });
                program.preassign_label_to_next_insn(missing_saved_row);
                program.emit_insn(Insn::Halt {
                    err_code: crate::error::SQLITE_ERROR,
                    description: "nth_value could not find a saved row within its frame"
                        .to_string(),
                    on_error: None,
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(position_lookup_done);
                continue;
            }

            let acc_reg = registers.acc_start + i;
            // TODO: Make `AggStep::col` optional. It currently requires a
            // register number, so reserved register 0 means no argument.
            program.emit_insn(Insn::AggStep {
                acc_reg,
                col: 0,
                delimiter: 0,
                func: AccumulatorFunc::Window(win_func.clone()),
                comparator: None,
            });
            program.emit_insn(Insn::AggValue {
                acc_reg,
                dest_reg,
                func: AccumulatorFunc::Window(win_func.clone()),
            });
        }
    }
    t_ctx.resolver.enable_expr_to_reg_cache();

    match plan.order_by.is_empty() {
        true => {
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
        }
        false => {
            EmitOrderBy::sorter_insert(program, t_ctx, plan)?;
        }
    }

    program.preassign_label_to_next_insn(label_skip_returning_row);

    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }

    let must_stop_return_cursor_at_next_peer_group =
        buffer_mode.keeps_partition_rows() && !window.order_by.is_empty();
    if must_stop_return_cursor_at_next_peer_group {
        // The first row of the next sort group was inserted before this group
        // was returned. Stop on that row so this cursor can continue from it
        // when the next group is ready.
        let next_row_found = program.allocate_label();
        let finished_returning_group = program.allocate_label();
        program.emit_insn(Insn::Next {
            cursor_id: cursors.csr_current,
            pc_if_next: next_row_found,
        });
        program.emit_insn(Insn::Goto {
            target_pc: finished_returning_group,
        });

        program.preassign_label_to_next_insn(next_row_found);
        let current_order_by_start = program.alloc_registers(window.order_by.len());
        for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
            let Expr::Column { column, .. } = expr else {
                unreachable!("rewritten window ORDER BY term must be a subquery column");
            };
            program.emit_column_or_rowid(cursors.csr_current, *column, current_order_by_start + i);
        }
        program.emit_insn(Insn::Compare {
            start_reg_a: registers
                .prev_order_by_columns_start
                .expect("window ORDER BY values must be saved"),
            start_reg_b: current_order_by_start,
            count: window.order_by.len(),
            key_info: window_order_by_key_info(window, plan)?,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: finished_returning_group,
            target_pc_eq: label_loop_start,
            target_pc_gt: finished_returning_group,
        });
        program.preassign_label_to_next_insn(finished_returning_group);
    } else {
        program.emit_insn(Insn::Next {
            cursor_id: cursors.csr_current,
            pc_if_next: label_loop_start,
        });
    }

    Ok(())
}
