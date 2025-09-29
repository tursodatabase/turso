use crate::schema::{BTreeTable, Table};
use crate::translate::aggregation::{translate_aggregation_step, AggArgumentSource};
use crate::translate::emitter::{Resolver, TranslateCtx};
use crate::translate::expr::{walk_expr, walk_expr_mut, WalkControl};
use crate::translate::order_by::order_by_sorter_insert;
use crate::translate::plan::{
    Aggregate, Distinctness, JoinOrderMember, JoinedTable, QueryDestination, ResultSetColumn,
    SelectPlan, TableReferences, Window,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::result_row::emit_select_result;
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorType, ProgramBuilder, TableRefIdCounter};
use crate::vdbe::insn::{InsertFlags, Insn};
use crate::vdbe::{BranchOffset, CursorID};
use std::mem;
use std::sync::Arc;
use turso_parser::ast::Name;
use turso_parser::ast::{Expr, FunctionTail, Literal, Over, SortOrder, TableInternalId};

const SUBQUERY_DATABASE_ID: usize = 0;

struct WindowSubqueryContext<'a> {
    resolver: &'a Resolver<'a>,
    subquery_order_by: &'a mut Vec<(Box<Expr>, SortOrder)>,
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
pub fn plan_windows(
    plan: &mut SelectPlan,
    resolver: &Resolver,
    table_ref_counter: &mut TableRefIdCounter,
    windows: &mut Vec<Window>,
) -> crate::Result<()> {
    // Remove named windows that are not referenced by any function, as they can be ignored.
    windows.retain(|w| !w.functions.is_empty());

    if !windows.is_empty() {
        // Sanity check: this should never happen because the syntax disallows combining VALUES with windows
        assert!(
            plan.values.is_empty(),
            "VALUES clause with windows is not supported"
        );
    }

    prepare_window_subquery(plan, resolver, table_ref_counter, windows, 0)
}

fn prepare_window_subquery(
    outer_plan: &mut SelectPlan,
    resolver: &Resolver,
    table_ref_counter: &mut TableRefIdCounter,
    windows: &mut Vec<Window>,
    processed_window_count: usize,
) -> crate::Result<()> {
    if windows.is_empty() {
        return Ok(());
    }

    let mut current_window = windows.swap_remove(0);
    let mut subquery_result_columns = Vec::new();
    let mut subquery_order_by = Vec::new();
    let subquery_id = table_ref_counter.next();

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
        append_order_by(outer_plan, expr, &SortOrder::Asc, &mut ctx)?;
        current_window.deduplicated_partition_by_len = Some(ctx.subquery_result_columns.len())
    }
    for (expr, order) in current_window.order_by.iter_mut() {
        append_order_by(outer_plan, expr, order, &mut ctx)?;
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
    for (expr, _) in outer_plan.order_by.iter_mut() {
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
            contains_aggregates: false,
        });
    }

    let new_join_order = vec![JoinOrderMember {
        table_id: subquery_id,
        original_idx: 0,
        is_outer: false,
    }];
    let new_table_references = TableReferences::new(vec![], vec![]);

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
    };

    prepare_window_subquery(
        &mut inner_plan,
        resolver,
        table_ref_counter,
        windows,
        processed_window_count + 1,
    )?;

    let subquery = JoinedTable::new_subquery(
        format!("window_subquery_{processed_window_count}"),
        inner_plan,
        None,
        subquery_id,
    );

    // Verify that the subquery has the expected database ID.
    // This is required to ensure that assumptions in `rewrite_terminal_expr` are valid.
    assert_eq!(
        subquery.database_id, SUBQUERY_DATABASE_ID,
        "expected subquery database id to be {SUBQUERY_DATABASE_ID}"
    );

    outer_plan.window = Some(current_window);
    outer_plan.table_references.add_joined_table(subquery);

    Ok(())
}

fn append_order_by(
    plan: &mut SelectPlan,
    expr: &mut Expr,
    sort_order: &SortOrder,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<()> {
    ctx.subquery_order_by
        .push((Box::new(expr.clone()), *sort_order));

    let contains_aggregates =
        resolve_window_and_aggregate_functions(expr, ctx.resolver, &mut plan.aggregates, None)?;
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
                    } else if let Some(window_function) = current_window
                        .functions
                        .iter_mut()
                        .find(|f| exprs_are_equivalent(&f.original_expr, expr))
                    {
                        // If the expression is a window function tied to the current window,
                        // do not push it to the subquery. Instead, rewrite it so its child
                        // expressions reference the subquery where needed.
                        rewrite_expr_referencing_current_window(
                            aggregates,
                            current_window
                                .name
                                .clone()
                                .expect("current_window must always have a name here"),
                            ctx,
                            expr,
                        )?;
                        window_function.original_expr = expr.clone();

                        // At this point, the expression and all its children now reference the subquery,
                        // so further traversal is unnecessary.
                        return Ok(WalkControl::SkipChildren);
                    } else {
                        // This is a window function referencing a different window (not the current one).
                        // Push the entire expression to the subquery; it will be rewritten later.
                        rewrite_expr_as_subquery_column(expr, ctx, false);
                    }
                }
                Expr::RowId { .. } | Expr::Column { .. } => {
                    rewrite_expr_as_subquery_column(expr, ctx, false);
                }
                _ => {}
            }

            Ok(WalkControl::Continue)
        },
    )
}

fn rewrite_expr_referencing_current_window(
    aggregates: &mut Vec<Aggregate>,
    window_name: String,
    ctx: &mut WindowSubqueryContext,
    expr: &mut Expr,
) -> crate::Result<()> {
    fn normalize_over_clause(filter_over: &mut FunctionTail, window_name: &str) {
        // FILTER clause is not supported yet. Proper checks elsewhere return appropriate
        // error messages, and this ensures that nothing slips through unnoticed.
        assert!(
            filter_over.filter_clause.is_none(),
            "FILTER in window functions is not supported"
        );

        // Replace inline OVER clause with a reference to the named window.
        // The window name may be user-provided or planner-generated.
        *filter_over = FunctionTail {
            filter_clause: None,
            over_clause: Some(Over::Name(Name::exact(window_name.to_string()))),
        };
    }

    match expr {
        Expr::FunctionCall {
            name: _,
            distinctness: _,
            args,
            order_by,
            filter_over,
        } => {
            for arg in args.iter_mut() {
                let contains_aggregates =
                    resolve_window_and_aggregate_functions(arg, ctx.resolver, aggregates, None)?;
                rewrite_expr_as_subquery_column(arg, ctx, contains_aggregates);
            }
            assert!(
                order_by.is_empty(),
                "ORDER BY in window functions is not supported"
            );
            normalize_over_clause(filter_over, &window_name);
        }
        Expr::FunctionCallStar {
            filter_over,
            name: _,
        } => {
            normalize_over_clause(filter_over, &window_name);
        }
        _ => unreachable!("only functions can reference windows"),
    }
    Ok(())
}

/// Rewrites an expression into a reference to a subquery column.
/// If the expression was already pushed down, reuses the existing column index.
/// Otherwise, adds it as a new column in the subquery's result set.
fn rewrite_expr_as_subquery_column(
    expr: &mut Expr,
    ctx: &mut WindowSubqueryContext,
    contains_aggregates: bool,
) {
    let (column_idx, existing) = match ctx
        .subquery_result_columns
        .iter()
        .position(|col| exprs_are_equivalent(&col.expr, expr))
    {
        Some(pos) => (pos, true),
        None => (ctx.subquery_result_columns.len(), false),
    };

    let subquery_ref = Expr::Column {
        database: Some(SUBQUERY_DATABASE_ID),
        table: *ctx.subquery_id,
        column: column_idx,
        is_rowid_alias: false,
    };

    if existing {
        *expr = subquery_ref;
    } else {
        let subquery_expr = mem::replace(expr, subquery_ref);
        ctx.subquery_result_columns.push(ResultSetColumn {
            expr: subquery_expr,
            alias: None,
            contains_aggregates,
        });
    }
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
    /// Start of the register array storing accumulator states for each window function
    /// (populated by `AggStep` during aggregation).
    pub acc_start: usize,
    /// Start of the register array storing current accumulator results for each window function
    /// (populated by `AggValue` when computing results without clearing accumulators).
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

#[derive(Debug)]
pub struct WindowCursors {
    /// Cursor used to read from the ephemeral buffer table
    pub buffer_read: CursorID,
    /// Cursor used to write to the ephemeral buffer table
    pub buffer_write: CursorID,
}

pub fn init_window<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    window: &'a Window,
    plan: &SelectPlan,
    result_columns: &'a [ResultSetColumn],
    order_by: &'a [(Box<Expr>, SortOrder)],
) -> crate::Result<()> {
    let joined_tables = &plan.joined_tables();
    assert_eq!(joined_tables.len(), 1, "expected only one joined table");

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

    let src_columns = src_table.columns().to_vec();
    let src_column_count = src_columns.len();
    let window_name = window.name.clone().expect("window name is missing");
    let partition_by_len = window.partition_by.len();
    let order_by_len = window.order_by.len();
    let window_function_count = window.functions.len();

    // An ephemeral table used to buffer rows for the current frame
    let buffer_table = Arc::new(BTreeTable {
        root_page: 0,
        // TODO: Generating the name this way may cause collisions with real tables in the
        //  attached database. Other ephemeral tables are created similarly, so it’s left
        //  as-is for now. Ideally, there should be a way to mark tables as ephemeral so
        //  they can be handled differently from regular tables.
        name: format!("buffer_table_{window_name}"),
        has_rowid: true,
        primary_key_columns: vec![],
        columns: src_columns,
        is_strict: false,
        unique_sets: vec![],
        has_autoincrement: false,
    });
    let cursor_buffer_read = program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
    let cursor_buffer_write = program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor_buffer_read,
        is_table: true,
    });
    program.emit_insn(Insn::OpenDup {
        original_cursor_id: cursor_buffer_read,
        new_cursor_id: cursor_buffer_write,
    });

    // Window function processing is similar to aggregation processing in how results are mapped
    // to registers. Each function expression is stored in `expr_to_reg_cache` along with its
    // result register. Later, when bytecode generation encounters the expression, the value is
    // copied from the result register instead of generating code to evaluate the expression.
    let reg_acc_start = program.alloc_registers(window_function_count);
    let reg_acc_result_start = program.alloc_registers(window_function_count);
    for (i, func) in window.functions.iter().enumerate() {
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((&func.original_expr, reg_acc_result_start + i));
    }

    // The same approach applies to expressions referencing the subquery (columns).
    // Instead of reading directly from the subquery, we redirect them to the corresponding
    // result registers. This is necessary because rows are buffered in an ephemeral table and
    // returned according to the rules of the window definition.
    let expressions_referencing_subquery =
        collect_expressions_referencing_subquery(result_columns, order_by, &src_table.internal_id)?;
    let reg_col_start = program.alloc_registers(expressions_referencing_subquery.len());
    for (i, (expr, _)) in expressions_referencing_subquery.iter().enumerate() {
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((expr, reg_col_start + i));
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
            buffer_read: cursor_buffer_read,
            buffer_write: cursor_buffer_write,
        },
        src_column_count,
        expressions_referencing_subquery,
        buffer_table_name: buffer_table.name.clone(),
    });

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
    order_by: &'a [(Box<Expr>, SortOrder)],
    subquery_id: &TableInternalId,
) -> crate::Result<Vec<(&'a Expr, usize)>> {
    let mut expressions_referencing_subquery: Vec<(&'a Expr, usize)> = Vec::new();

    for root_expr in result_columns
        .iter()
        .map(|col| &col.expr)
        .chain(order_by.iter().map(|(e, _)| e.as_ref()))
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
                        assert_eq!(
                            table, subquery_id,
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

/// Emits bytecode to process a single row of the window’s input (always a subquery).
///
/// Note:
/// The **buffer table** mentioned below is an ephemeral B-tree that temporarily
/// stores rows for the current window frame.
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
        src_column_count: input_column_count,
        buffer_table_name,
        ..
    } = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");

    emit_load_order_by_columns(program, window, registers);
    emit_flush_buffer_if_new_partition(program, labels, registers, window);
    emit_reset_state_if_new_partition(program, registers, window);
    emit_flush_buffer_if_not_peer(program, labels, registers, window);
    emit_insert_row_into_buffer(
        program,
        registers,
        cursors,
        input_column_count,
        buffer_table_name,
    );
    emit_aggregation_step(program, window, &t_ctx.resolver, plan, registers)?;

    Ok(())
}

fn emit_flush_buffer_if_new_partition(
    program: &mut ProgramBuilder,
    labels: &WindowLabels,
    registers: &WindowRegisters,
    window: &Window,
) {
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
        program.emit_insn(Insn::Compare {
            start_reg_a: registers.src_columns_start,
            start_reg_b: reg_partition_start,
            count: partition_by_len,
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: new_partition_label,
            target_pc_eq: same_partition_label,
            target_pc_gt: new_partition_label,
        });

        program.resolve_label(new_partition_label, program.offset());
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

        program.resolve_label(same_partition_label, program.offset());
    }
}

fn emit_reset_state_if_new_partition(
    program: &mut ProgramBuilder,
    registers: &WindowRegisters,
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

    program.preassign_label_to_next_insn(label_skip_reset_state);
}

fn emit_flush_buffer_if_not_peer(
    program: &mut ProgramBuilder,
    labels: &WindowLabels,
    registers: &WindowRegisters,
    window: &Window,
) {
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
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: label_not_peer,
            target_pc_eq: label_peer,
            target_pc_gt: label_not_peer,
        });

        program.resolve_label(label_not_peer, program.offset());
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

        program.resolve_label(label_peer, program.offset());
    }
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
        for (i, (expr, _)) in window.order_by.iter().enumerate() {
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
        start_reg: registers.src_columns_start,
        count: *input_column_count,
        dest_reg: reg_record,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: cursors.buffer_write,
        rowid_reg: registers.rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.buffer_write,
        key_reg: registers.rowid,
        record_reg: reg_record,
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });
}

fn emit_aggregation_step(
    program: &mut ProgramBuilder,
    window: &Window,
    resolver: &Resolver,
    plan: &SelectPlan,
    registers: &WindowRegisters,
) -> crate::Result<()> {
    for (i, func) in window.functions.iter().enumerate() {
        // The aggregation step is performed incrementally as each row from the subquery is
        // processed. Therefore, we don’t need to access the buffer table and can obtain argument
        // values directly by evaluating the expressions that reference the subquery result columns.
        let args = match &func.original_expr {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| (**a).clone()).collect(),
            Expr::FunctionCallStar { .. } => vec![],
            _ => unreachable!("All window functions should be either FunctionCall or FunctionCallStar expressions"),
        };

        let reg_acc_start = registers.acc_start + i;
        translate_aggregation_step(
            program,
            &plan.table_references,
            AggArgumentSource::new_from_expression(&func.func, &args, &Distinctness::NonDistinct),
            reg_acc_start,
            resolver,
        )?;
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
        ..
    } = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");

    let label_empty = program.allocate_label();
    let label_window_processing_end = labels.window_processing_end;
    let reg_flush_buffer_return_offset = registers.flush_buffer_return_offset;
    let cursor_buffer_read = cursors.buffer_read;

    // All source rows have already been processed at this point.
    // In fallthrough mode, we are not returning to a caller — we just flush
    // the buffered rows and continue execution.
    program.add_comment(program.offset(), "return remaining buffered rows");
    program.emit_insn(Insn::Null {
        dest: registers.flush_buffer_return_offset,
        dest_end: None,
    });

    // If control jumps here (labels.flush_buffer), we are in subroutine mode.
    // In that case, after flushing the buffer, execution will return to the
    // address stored in `flush_buffer_return_offset`.
    program.preassign_label_to_next_insn(labels.flush_buffer);

    program.emit_insn(Insn::Rewind {
        cursor_id: cursor_buffer_read,
        pc_if_empty: label_empty,
    });

    emit_return_buffered_rows(program, window, t_ctx, plan)?;

    program.resolve_label(label_empty, program.offset());

    program.emit_insn(Insn::ResetSorter {
        cursor_id: cursor_buffer_read,
    });
    program.emit_insn(Insn::Return {
        return_reg: reg_flush_buffer_return_offset,
        can_fallthrough: true,
    });

    program.preassign_label_to_next_insn(label_window_processing_end);

    Ok(())
}

fn emit_return_buffered_rows(
    program: &mut ProgramBuilder,
    window: &Window,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> crate::Result<()> {
    let WindowMetadata {
        labels,
        registers,
        cursors,
        expressions_referencing_subquery,
        ..
    } = t_ctx.meta_window.as_ref().expect("missing window metadata");

    for (i, func) in window.functions.iter().enumerate() {
        program.emit_insn(Insn::AggValue {
            acc_reg: registers.acc_start + i,
            dest_reg: registers.acc_result_start + i,
            func: func.func.clone(),
        });
    }

    let label_skip_returning_row = program.allocate_label();
    let label_loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(label_loop_start);

    // Propagate subquery result column values to the outer query (if any) or directly to
    // the final output that will be returned to the user, by copying them from the buffer table
    // into the dedicated registers.
    for (i, (_, col_idx)) in expressions_referencing_subquery.iter().enumerate() {
        let reg_result = registers.result_columns_start + i;
        program.emit_column_or_rowid(cursors.buffer_read, *col_idx, reg_result);
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
            order_by_sorter_insert(program, t_ctx, plan)?;
        }
    }

    program.resolve_label(label_skip_returning_row, program.offset());

    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }

    program.emit_insn(Insn::Next {
        cursor_id: cursors.buffer_read,
        pc_if_next: label_loop_start,
    });

    Ok(())
}
