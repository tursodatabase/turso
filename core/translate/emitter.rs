// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::num::NonZeroUsize;
use std::sync::Arc;

use tracing::{instrument, Level};
use turso_parser::ast::{self, Expr};

use super::aggregation::emit_ungrouped_aggregation;
use super::expr::translate_expr;
use super::group_by::{
    group_by_agg_phase, group_by_emit_row_phase, init_group_by, GroupByMetadata, GroupByRowSource,
};
use super::main_loop::{
    close_loop, emit_loop, init_distinct, init_loop, open_loop, LeftJoinMetadata, LoopLabels,
};
use super::order_by::{emit_order_by, init_order_by, SortMetadata};
use super::plan::{
    Distinctness, JoinOrderMember, Operation, Scan, SelectPlan, TableReferences, UpdatePlan,
};
use super::select::emit_simple_count;
use super::subquery::emit_subqueries;
use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::function::Func;
use crate::schema::{BTreeTable, Column, Schema, Table, ROWID_SENTINEL};
use crate::translate::compound_select::emit_program_for_compound_select;
use crate::translate::expr::{
    emit_returning_results, translate_expr_no_constant_opt, walk_expr_mut, NoConstantOptReason,
    ReturningValueRegisters, WalkControl,
};
use crate::translate::plan::{DeletePlan, JoinedTable, Plan, QueryDestination, Search};
use crate::translate::planner::ROWID_STRS;
use crate::translate::result_row::try_fold_expr_to_i64;
use crate::translate::values::emit_values;
use crate::translate::window::{emit_window_results, init_window, WindowMetadata};
use crate::util::{exprs_are_equivalent, normalize_ident};
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, RegisterOrLiteral};
use crate::vdbe::CursorID;
use crate::vdbe::{insn::Insn, BranchOffset};
use crate::Connection;
use crate::{bail_parse_error, Result, SymbolTable};

pub struct Resolver<'a> {
    pub schema: &'a Schema,
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache_enabled: bool,
    pub expr_to_reg_cache: Vec<(&'a ast::Expr, usize)>,
}

impl<'a> Resolver<'a> {
    pub fn new(schema: &'a Schema, symbol_table: &'a SymbolTable) -> Self {
        Self {
            schema,
            symbol_table,
            expr_to_reg_cache_enabled: false,
            expr_to_reg_cache: Vec::new(),
        }
    }

    pub fn resolve_function(&self, func_name: &str, arg_count: usize) -> Option<Func> {
        match Func::resolve_function(func_name, arg_count).ok() {
            Some(func) => Some(func),
            None => self
                .symbol_table
                .resolve_function(func_name, arg_count)
                .map(|arg| Func::External(arg.clone())),
        }
    }

    pub(crate) fn enable_expr_to_reg_cache(&mut self) {
        self.expr_to_reg_cache_enabled = true;
    }

    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<usize> {
        if self.expr_to_reg_cache_enabled {
            self.expr_to_reg_cache
                .iter()
                .find(|(e, _)| exprs_are_equivalent(expr, e))
                .map(|(_, reg)| *reg)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LimitCtx {
    /// Register holding the LIMIT value (e.g. LIMIT 5)
    pub reg_limit: usize,
    /// Whether to initialize the LIMIT counter to the LIMIT value;
    /// There are cases like compound SELECTs where all the sub-selects
    /// utilize the same limit register, but it is initialized only once.
    pub initialize_counter: bool,
}

impl LimitCtx {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            reg_limit: program.alloc_register(),
            initialize_counter: true,
        }
    }

    pub fn new_shared(reg_limit: usize) -> Self {
        Self {
            reg_limit,
            initialize_counter: false,
        }
    }
}

/// The TranslateCtx struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
pub struct TranslateCtx<'a> {
    // A typical query plan is a nested loop. Each loop has its own LoopLabels (see the definition of LoopLabels for more details)
    pub labels_main_loop: Vec<LoopLabels>,
    // label for the instruction that jumps to the next phase of the query after the main loop
    // we don't know ahead of time what that is (GROUP BY, ORDER BY, etc.)
    pub label_main_loop_end: Option<BranchOffset>,
    // First register of the aggregation results
    pub reg_agg_start: Option<usize>,
    // In non-group-by statements with aggregations (e.g. SELECT foo, bar, sum(baz) FROM t),
    // we want to emit the non-aggregate columns (foo and bar) only once.
    // This register is a flag that tracks whether we have already done that.
    pub reg_nonagg_emit_once_flag: Option<usize>,
    // First register of the result columns of the query
    pub reg_result_cols_start: Option<usize>,
    pub limit_ctx: Option<LimitCtx>,
    // The register holding the offset value, if any.
    pub reg_offset: Option<usize>,
    // The register holding the limit+offset value, if any.
    pub reg_limit_offset_sum: Option<usize>,
    // metadata for the group by operator
    pub meta_group_by: Option<GroupByMetadata>,
    // metadata for the order by operator
    pub meta_sort: Option<SortMetadata>,
    /// mapping between table loop index and associated metadata (for left joins only)
    /// this metadata exists for the right table in a given left join
    pub meta_left_joins: Vec<Option<LeftJoinMetadata>>,
    pub resolver: Resolver<'a>,
    /// A list of expressions that are not aggregates, along with a flag indicating
    /// whether the expression should be included in the output for each group.
    ///
    /// Each entry is a tuple:
    /// - `&'ast Expr`: the expression itself
    /// - `bool`: `true` if the expression should be included in the output for each group, `false` otherwise.
    ///
    /// The order of expressions is **significant**:
    /// - First: all `GROUP BY` expressions, in the order they appear in the `GROUP BY` clause.
    /// - Then: remaining non-aggregate expressions that are not part of `GROUP BY`.
    pub non_aggregate_expressions: Vec<(&'a Expr, bool)>,
    /// Cursor id for cdc table (if capture_data_changes PRAGMA is set and query can modify the data)
    pub cdc_cursor_id: Option<usize>,
    pub meta_window: Option<WindowMetadata<'a>>,
}

impl<'a> TranslateCtx<'a> {
    pub fn new(
        program: &mut ProgramBuilder,
        schema: &'a Schema,
        syms: &'a SymbolTable,
        table_count: usize,
    ) -> Self {
        TranslateCtx {
            labels_main_loop: (0..table_count).map(|_| LoopLabels::new(program)).collect(),
            label_main_loop_end: None,
            reg_agg_start: None,
            reg_nonagg_emit_once_flag: None,
            limit_ctx: None,
            reg_offset: None,
            reg_limit_offset_sum: None,
            reg_result_cols_start: None,
            meta_group_by: None,
            meta_left_joins: (0..table_count).map(|_| None).collect(),
            meta_sort: None,
            resolver: Resolver::new(schema, syms),
            non_aggregate_expressions: Vec::new(),
            cdc_cursor_id: None,
            meta_window: None,
        }
    }
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Sqlite always considers Read transactions implicit
pub enum TransactionMode {
    None,
    Read,
    Write,
    Concurrent,
}

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    plan: Plan,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, resolver, plan),
        Plan::Delete(plan) => emit_program_for_delete(connection, resolver, program, plan),
        Plan::Update(plan) => emit_program_for_update(connection, resolver, program, plan, after),
        Plan::CompoundSelect { .. } => emit_program_for_compound_select(program, resolver, plan),
    }
}

#[instrument(skip_all, level = Level::DEBUG)]
fn emit_program_for_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: SelectPlan,
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.schema,
        resolver.symbol_table,
        plan.table_references.joined_tables().len(),
    );

    // Emit main parts of query
    emit_query(program, &mut plan, &mut t_ctx)?;

    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_query<'a>(
    program: &mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    init_limit(program, t_ctx, &plan.limit, &plan.offset);

    if !plan.values.is_empty() {
        let reg_result_cols_start = emit_values(program, plan, t_ctx)?;
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(reg_result_cols_start);
    }

    // Emit subqueries first so the results can be read in the main query loop.
    emit_subqueries(program, t_ctx, &mut plan.table_references)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    // For non-grouped aggregation queries that also have non-aggregate columns,
    // we need to ensure non-aggregate columns are only emitted once.
    // This flag helps track whether we've already emitted these columns.
    if !plan.aggregates.is_empty()
        && plan.group_by.is_none()
        && plan.result_columns.iter().any(|c| !c.contains_aggregates)
    {
        let flag = program.alloc_register();
        program.emit_int(0, flag); // Initialize flag to 0 (not yet emitted)
        t_ctx.reg_nonagg_emit_once_flag = Some(flag);
    }

    // Allocate registers for result columns
    if t_ctx.reg_result_cols_start.is_none() {
        t_ctx.reg_result_cols_start = Some(program.alloc_registers(plan.result_columns.len()));
    }

    // Initialize cursors and other resources needed for query execution
    if !plan.order_by.is_empty() {
        init_order_by(
            program,
            t_ctx,
            &plan.result_columns,
            &plan.order_by,
            &plan.table_references,
            plan.group_by.is_some(),
            &plan.aggregates,
        )?;
    }

    if let Some(ref group_by) = plan.group_by {
        init_group_by(
            program,
            t_ctx,
            group_by,
            plan,
            &plan.result_columns,
            &plan.order_by,
        )?;
    } else if !plan.aggregates.is_empty() {
        // Aggregate registers need to be NULLed at the start because the same registers might be reused on another invocation of a subquery,
        // and if they are not NULLed, the 2nd invocation of the same subquery will have values left over from the first invocation.
        t_ctx.reg_agg_start = Some(program.alloc_registers_and_init_w_null(plan.aggregates.len()));
    } else if let Some(window) = &plan.window {
        init_window(
            program,
            t_ctx,
            window,
            plan,
            &plan.result_columns,
            &plan.order_by,
        )?;
    }

    let distinct_ctx = if let Distinctness::Distinct { .. } = &plan.distinctness {
        Some(init_distinct(program, plan))
    } else {
        None
    };
    if let Distinctness::Distinct { ctx } = &mut plan.distinctness {
        *ctx = distinct_ctx
    }
    init_loop(
        program,
        t_ctx,
        &plan.table_references,
        &mut plan.aggregates,
        plan.group_by.as_ref(),
        OperationMode::SELECT,
        &plan.where_clause,
    )?;

    if plan.is_simple_count() {
        emit_simple_count(program, t_ctx, plan)?;
        return Ok(t_ctx.reg_result_cols_start.unwrap());
    }

    // Set up main query execution loop
    open_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        &plan.where_clause,
        None,
    )?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    close_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        None,
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    let mut order_by_necessary =
        !plan.order_by.is_empty() && !plan.contains_constant_false_condition;
    let order_by = &plan.order_by;

    // Handle GROUP BY and aggregation processing
    if plan.group_by.is_some() {
        let row_source = &t_ctx
            .meta_group_by
            .as_ref()
            .expect("group by metadata not found")
            .row_source;
        if matches!(row_source, GroupByRowSource::Sorter { .. }) {
            group_by_agg_phase(program, t_ctx, plan)?;
        }
        group_by_emit_row_phase(program, t_ctx, plan)?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY
        emit_ungrouped_aggregation(program, t_ctx, plan)?;
        // Single row result for aggregates without GROUP BY, so ORDER BY not needed
        order_by_necessary = false;
    } else if plan.window.is_some() {
        emit_window_results(program, t_ctx, plan)?;
    }

    // Process ORDER BY results if needed
    if !order_by.is_empty() && order_by_necessary {
        emit_order_by(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

#[instrument(skip_all, level = Level::DEBUG)]
fn emit_program_for_delete(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    mut plan: DeletePlan,
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.schema,
        resolver.symbol_table,
        plan.table_references.joined_tables().len(),
    );

    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    init_limit(program, &mut t_ctx, &plan.limit, &None);

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    // Initialize cursors and other resources needed for query execution
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        None,
        OperationMode::DELETE,
        &plan.where_clause,
    )?;

    // Set up main query execution loop
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        &plan.where_clause,
        None,
    )?;

    emit_delete_insns(
        connection,
        program,
        &mut t_ctx,
        &mut plan.table_references,
        &plan.result_columns,
    )?;

    // Clean up and close the main execution loop
    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        None,
    )?;
    program.preassign_label_to_next_insn(after_main_loop_label);

    // Finalize program
    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

fn emit_delete_insns(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    result_columns: &[super::plan::ResultSetColumn],
) -> Result<()> {
    // we can either use this obviously safe raw pointer or we can clone it
    let table_reference: *const JoinedTable = table_references.joined_tables().first().unwrap();
    if unsafe { &*table_reference }
        .virtual_table()
        .is_some_and(|t| t.readonly())
    {
        return Err(crate::LimboError::ReadOnly);
    }
    let internal_id = unsafe { (*table_reference).internal_id };

    let table_name = unsafe { &*table_reference }.table.get_name();
    let cursor_id = match unsafe { &(*table_reference).op } {
        Operation::Scan { .. } => program.resolve_cursor_id(&CursorKey::table(internal_id)),
        Operation::Search(search) => match search {
            Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                program.resolve_cursor_id(&CursorKey::table(internal_id))
            }
            Search::Seek {
                index: Some(index), ..
            } => program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
        },
    };
    let main_table_cursor_id = program.resolve_cursor_id(&CursorKey::table(internal_id));

    // Emit the instructions to delete the row
    let key_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: main_table_cursor_id,
        dest: key_reg,
    });

    if unsafe { &*table_reference }.virtual_table().is_some() {
        let conflict_action = 0u16;
        let start_reg = key_reg;

        let new_rowid_reg = program.alloc_register();
        program.emit_insn(Insn::Null {
            dest: new_rowid_reg,
            dest_end: None,
        });
        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count: 2,
            start_reg,
            conflict_action,
        });
    } else {
        // Delete from all indexes before deleting from the main table.
        let indexes = t_ctx.resolver.schema.indexes.get(table_name);

        // Get the index that is being used to iterate the deletion loop, if there is one.
        let iteration_index = unsafe { &*table_reference }.op.index();
        // Get all indexes that are not the iteration index.
        let other_indexes = indexes
            .map(|indexes| {
                indexes
                    .iter()
                    .filter(|index| {
                        iteration_index
                            .as_ref()
                            .is_none_or(|it_idx| !Arc::ptr_eq(it_idx, index))
                    })
                    .map(|index| {
                        (
                            index.clone(),
                            program
                                .resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        for (index, index_cursor_id) in other_indexes {
            let skip_delete_label = if index.where_clause.is_some() {
                let where_copy = index
                    .bind_where_expr(Some(table_references), connection)
                    .expect("where clause to exist");
                let skip_label = program.allocate_label();
                let reg = program.alloc_register();
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    &where_copy,
                    reg,
                    &t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                program.emit_insn(Insn::IfNot {
                    reg,
                    jump_if_null: true,
                    target_pc: skip_label,
                });
                Some(skip_label)
            } else {
                None
            };
            let num_regs = index.columns.len() + 1;
            let start_reg = program.alloc_registers(num_regs);
            // Emit columns that are part of the index
            index
                .columns
                .iter()
                .enumerate()
                .for_each(|(reg_offset, column_index)| {
                    program.emit_column_or_rowid(
                        main_table_cursor_id,
                        column_index.pos_in_table,
                        start_reg + reg_offset,
                    );
                });
            program.emit_insn(Insn::RowId {
                cursor_id: main_table_cursor_id,
                dest: start_reg + num_regs - 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg,
                num_regs,
                cursor_id: index_cursor_id,
                raise_error_if_no_matching_entry: index.where_clause.is_none(),
            });
            if let Some(label) = skip_delete_label {
                program.resolve_label(label, program.offset());
            }
        }

        // Emit update in the CDC table if necessary (before DELETE updated the table)
        if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: main_table_cursor_id,
                dest: rowid_reg,
            });
            let cdc_has_before = program.capture_data_changes_mode().has_before();
            let before_record_reg = if cdc_has_before {
                Some(emit_cdc_full_record(
                    program,
                    unsafe { &*table_reference }.table.columns(),
                    main_table_cursor_id,
                    rowid_reg,
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                &t_ctx.resolver,
                OperationMode::DELETE,
                cdc_cursor_id,
                rowid_reg,
                before_record_reg,
                None,
                None,
                table_name,
            )?;
        }

        // Emit RETURNING results if specified (must be before DELETE)
        if !result_columns.is_empty() {
            // Get rowid for RETURNING
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: main_table_cursor_id,
                dest: rowid_reg,
            });
            let cols_len = unsafe { &*table_reference }.columns().len();

            // Allocate registers for column values
            let columns_start_reg = program.alloc_registers(cols_len);

            // Read all column values from the row to be deleted
            for (i, _column) in unsafe { &*table_reference }.columns().iter().enumerate() {
                program.emit_column_or_rowid(main_table_cursor_id, i, columns_start_reg + i);
            }

            // Emit RETURNING results using the values we just read
            let value_registers = ReturningValueRegisters {
                rowid_register: rowid_reg,
                columns_start_register: columns_start_reg,
                num_columns: cols_len,
            };

            emit_returning_results(program, result_columns, &value_registers)?;
        }

        program.emit_insn(Insn::Delete {
            cursor_id: main_table_cursor_id,
            table_name: table_name.to_string(),
        });

        if let Some(index) = iteration_index {
            let iteration_index_cursor =
                program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone()));
            program.emit_insn(Insn::Delete {
                cursor_id: iteration_index_cursor,
                table_name: index.name.clone(),
            });
        }
    }
    if let Some(limit_ctx) = t_ctx.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }

    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
fn emit_program_for_update(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    mut plan: UpdatePlan,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.schema,
        resolver.symbol_table,
        plan.table_references.joined_tables().len(),
    );

    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    init_limit(program, &mut t_ctx, &plan.limit, &plan.offset);

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    let ephemeral_plan = plan.ephemeral_plan.take();
    let temp_cursor_id = ephemeral_plan.as_ref().map(|plan| {
        let QueryDestination::EphemeralTable { cursor_id, .. } = &plan.query_destination else {
            unreachable!()
        };
        *cursor_id
    });
    if let Some(ephemeral_plan) = ephemeral_plan {
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: temp_cursor_id.unwrap(),
            is_table: true,
        });
        program.incr_nesting();
        emit_program_for_select(program, resolver, ephemeral_plan)?;
        program.decr_nesting();
    }

    // Initialize the main loop
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        None,
        OperationMode::UPDATE,
        &plan.where_clause,
    )?;

    // Prepare index cursors
    let mut index_cursors = Vec::with_capacity(plan.indexes_to_update.len());
    for index in &plan.indexes_to_update {
        let index_cursor = if let Some(cursor) = program.resolve_cursor_id_safe(&CursorKey::index(
            plan.table_references
                .joined_tables()
                .first()
                .unwrap()
                .internal_id,
            index.clone(),
        )) {
            cursor
        } else {
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(index.root_page),
                db: 0,
            });
            cursor
        };
        let record_reg = program.alloc_register();
        index_cursors.push((index_cursor, record_reg));
    }

    // Open the main loop
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        &plan.where_clause,
        temp_cursor_id,
    )?;

    // Emit update instructions
    emit_update_insns(
        connection,
        &mut plan,
        &t_ctx,
        program,
        index_cursors,
        temp_cursor_id,
    )?;

    // Close the main loop
    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        temp_cursor_id,
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    after(program);

    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
fn emit_update_insns(
    connection: &Arc<Connection>,
    plan: &mut UpdatePlan,
    t_ctx: &TranslateCtx,
    program: &mut ProgramBuilder,
    index_cursors: Vec<(usize, usize)>,
    temp_cursor_id: Option<CursorID>,
) -> crate::Result<()> {
    // we can either use this obviously safe raw pointer or we can clone it
    let table_ref: *const JoinedTable = plan.table_references.joined_tables().first().unwrap();
    let internal_id = unsafe { (*table_ref).internal_id };
    let loop_labels = t_ctx.labels_main_loop.first().unwrap();
    let cursor_id = program.resolve_cursor_id(&CursorKey::table(internal_id));
    let (index, is_virtual) = match &unsafe { &*table_ref }.op {
        Operation::Scan(Scan::BTreeTable { index, .. }) => (
            index.as_ref().map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            }),
            false,
        ),
        Operation::Scan(_) => (None, unsafe { &*table_ref }.virtual_table().is_some()),
        Operation::Search(search) => match search {
            &Search::RowidEq { .. } | Search::Seek { index: None, .. } => (None, false),
            Search::Seek {
                index: Some(index), ..
            } => (
                Some((
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )),
                false,
            ),
        },
    };

    let beg = program.alloc_registers(
        unsafe { &*table_ref }.table.columns().len()
            + if is_virtual {
                2 // two args before the relevant columns for VUpdate
            } else {
                1 // rowid reg
            },
    );
    program.emit_insn(Insn::RowId {
        cursor_id: temp_cursor_id.unwrap_or(cursor_id),
        dest: beg,
    });

    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)
    let rowid_alias_index = unsafe { &*table_ref }
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias);

    let has_direct_rowid_update = plan
        .set_clauses
        .iter()
        .any(|(idx, _)| *idx == ROWID_SENTINEL);

    let has_user_provided_rowid = if let Some(index) = rowid_alias_index {
        plan.set_clauses.iter().any(|(idx, _)| *idx == index)
    } else {
        has_direct_rowid_update
    };

    let rowid_set_clause_reg = if has_user_provided_rowid {
        Some(program.alloc_register())
    } else {
        None
    };

    let check_rowid_not_exists_label = if has_user_provided_rowid {
        Some(program.allocate_label())
    } else {
        None
    };

    if has_user_provided_rowid {
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg: beg,
            target_pc: check_rowid_not_exists_label.unwrap(),
        });
    } else {
        // if no rowid, we're done
        program.emit_insn(Insn::IsNull {
            reg: beg,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        });
    }

    if is_virtual {
        program.emit_insn(Insn::Copy {
            src_reg: beg,
            dst_reg: beg + 1,
            extra_amount: 0,
        })
    }

    if let Some(offset) = t_ctx.reg_offset {
        program.emit_insn(Insn::IfPos {
            reg: offset,
            target_pc: loop_labels.next,
            decrement_by: 1,
        });
    }
    let col_len = unsafe { &*table_ref }.columns().len();

    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.

    // we allocate 2C registers for "updates" as the structure of this column for CDC table is following:
    // [C boolean values where true set for changed columns] [C values with updates where NULL is set for not-changed columns]
    let cdc_updates_register = if program.capture_data_changes_mode().has_updates() {
        Some(program.alloc_registers(2 * col_len))
    } else {
        None
    };
    let table_name = unsafe { &*table_ref }.table.get_name();

    let start = if is_virtual { beg + 2 } else { beg + 1 };

    if has_direct_rowid_update {
        if let Some((_, expr)) = plan.set_clauses.iter().find(|(i, _)| *i == ROWID_SENTINEL) {
            let rowid_set_clause_reg = rowid_set_clause_reg.unwrap();
            translate_expr(
                program,
                Some(&plan.table_references),
                expr,
                rowid_set_clause_reg,
                &t_ctx.resolver,
            )?;
            program.emit_insn(Insn::MustBeInt {
                reg: rowid_set_clause_reg,
            });
        }
    }
    for (idx, table_column) in unsafe { &*table_ref }.columns().iter().enumerate() {
        let target_reg = start + idx;
        if let Some((col_idx, expr)) = plan.set_clauses.iter().find(|(i, _)| *i == idx) {
            // Skip if this is the sentinel value
            if *col_idx == ROWID_SENTINEL {
                continue;
            }
            if has_user_provided_rowid
                && (table_column.primary_key || table_column.is_rowid_alias)
                && !is_virtual
            {
                let rowid_set_clause_reg = rowid_set_clause_reg.unwrap();
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    rowid_set_clause_reg,
                    &t_ctx.resolver,
                )?;

                program.emit_insn(Insn::MustBeInt {
                    reg: rowid_set_clause_reg,
                });

                program.emit_null(target_reg, None);
            } else {
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    target_reg,
                    &t_ctx.resolver,
                )?;
                if table_column.notnull {
                    use crate::error::SQLITE_CONSTRAINT_NOTNULL;
                    program.emit_insn(Insn::HaltIfNull {
                        target_reg,
                        err_code: SQLITE_CONSTRAINT_NOTNULL,
                        description: format!(
                            "{}.{}",
                            table_name,
                            table_column
                                .name
                                .as_ref()
                                .expect("Column name must be present")
                        ),
                    });
                }
            }

            if let Some(cdc_updates_register) = cdc_updates_register {
                let change_reg = cdc_updates_register + idx;
                let value_reg = cdc_updates_register + col_len + idx;
                program.emit_bool(true, change_reg);
                program.mark_last_insn_constant();
                let mut updated = false;
                if let Some(ddl_query_for_cdc_update) = &plan.cdc_update_alter_statement {
                    if table_column.name.as_deref() == Some("sql") {
                        program.emit_string8(ddl_query_for_cdc_update.clone(), value_reg);
                        updated = true;
                    }
                }
                if !updated {
                    program.emit_insn(Insn::Copy {
                        src_reg: target_reg,
                        dst_reg: value_reg,
                        extra_amount: 0,
                    });
                }
            }
        } else {
            let column_idx_in_index = index.as_ref().and_then(|(idx, _)| {
                idx.columns
                    .iter()
                    .position(|c| Some(&c.name) == table_column.name.as_ref())
            });

            // don't emit null for pkey of virtual tables. they require first two args
            // before the 'record' to be explicitly non-null
            if table_column.is_rowid_alias && !is_virtual {
                program.emit_null(target_reg, None);
            } else if is_virtual {
                program.emit_insn(Insn::VColumn {
                    cursor_id,
                    column: idx,
                    dest: target_reg,
                });
            } else {
                let cursor_id = *index
                    .as_ref()
                    .and_then(|(_, id)| {
                        if column_idx_in_index.is_some() {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(&cursor_id);
                program.emit_column_or_rowid(
                    cursor_id,
                    column_idx_in_index.unwrap_or(idx),
                    target_reg,
                );
            }

            if let Some(cdc_updates_register) = cdc_updates_register {
                let change_bit_reg = cdc_updates_register + idx;
                let value_reg = cdc_updates_register + col_len + idx;
                program.emit_bool(false, change_bit_reg);
                program.mark_last_insn_constant();
                program.emit_null(value_reg, None);
                program.mark_last_insn_constant();
            }
        }
    }

    for (index, (idx_cursor_id, record_reg)) in plan.indexes_to_update.iter().zip(&index_cursors) {
        // We need to know whether or not the OLD values satisfied the predicate on the
        // partial index, so we can know whether or not to delete the old index entry,
        // as well as whether or not the NEW values satisfy the predicate, to determine whether
        // or not to insert a new index entry for a partial index
        let (old_satisfies_where, new_satisfies_where) = if index.where_clause.is_some() {
            // This means that we need to bind the column references to a copy of the index Expr,
            // so we can emit Insn::Column instructions and refer to the old values.
            let where_clause = index
                .bind_where_expr(Some(&mut plan.table_references), connection)
                .expect("where clause to exist");
            let old_satisfied_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                Some(&plan.table_references),
                &where_clause,
                old_satisfied_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            // grab a new copy of the original where clause from the index
            let mut new_where = index
                .where_clause
                .as_ref()
                .expect("checked where clause to exist")
                .clone();
            // Now we need to rewrite the Expr::Id and Expr::Qualified/Expr::RowID (from a copy of the original, un-bound `where` expr),
            // to refer to the new values, which are already loaded into registers starting at `start`.
            rewrite_where_for_update_registers(
                &mut new_where,
                unsafe { &*table_ref }.columns(),
                start,
                rowid_set_clause_reg.unwrap_or(beg),
            )?;

            let new_satisfied_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                None,
                &new_where,
                new_satisfied_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            // now we have two registers that tell us whether or not the old and new values satisfy
            // the partial index predicate, and we can use those to decide whether or not to
            // delete/insert a new index entry for this partial index.
            (Some(old_satisfied_reg), Some(new_satisfied_reg))
        } else {
            (None, None)
        };

        let mut skip_delete_label = None;
        let mut skip_insert_label = None;

        // Handle deletion for partial indexes
        if let Some(old_satisfied) = old_satisfies_where {
            skip_delete_label = Some(program.allocate_label());
            // If the old values don't satisfy the WHERE clause, skip the delete
            program.emit_insn(Insn::IfNot {
                reg: old_satisfied,
                target_pc: skip_delete_label.unwrap(),
                jump_if_null: true,
            });
        }

        // Delete old index entry
        let num_regs = index.columns.len() + 1;
        let delete_start_reg = program.alloc_registers(num_regs);
        for (reg_offset, column_index) in index.columns.iter().enumerate() {
            program.emit_column_or_rowid(
                cursor_id,
                column_index.pos_in_table,
                delete_start_reg + reg_offset,
            );
        }
        program.emit_insn(Insn::RowId {
            cursor_id,
            dest: delete_start_reg + num_regs - 1,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: delete_start_reg,
            num_regs,
            cursor_id: *idx_cursor_id,
            raise_error_if_no_matching_entry: true,
        });

        // Resolve delete skip label if it exists
        if let Some(label) = skip_delete_label {
            program.resolve_label(label, program.offset());
        }

        // Check if we should insert into partial index
        if let Some(new_satisfied) = new_satisfies_where {
            skip_insert_label = Some(program.allocate_label());
            // If the new values don't satisfy the WHERE clause, skip the idx insert
            program.emit_insn(Insn::IfNot {
                reg: new_satisfied,
                target_pc: skip_insert_label.unwrap(),
                jump_if_null: true,
            });
        }

        // Build new index entry
        let num_cols = index.columns.len();
        let idx_start_reg = program.alloc_registers(num_cols + 1);
        let rowid_reg = rowid_set_clause_reg.unwrap_or(beg);

        for (i, col) in index.columns.iter().enumerate() {
            let col_in_table = unsafe { &*table_ref }
                .columns()
                .get(col.pos_in_table)
                .expect("column index out of bounds");
            program.emit_insn(Insn::Copy {
                src_reg: if col_in_table.is_rowid_alias {
                    rowid_reg
                } else {
                    start + col.pos_in_table
                },
                dst_reg: idx_start_reg + i,
                extra_amount: 0,
            });
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        program.emit_insn(Insn::MakeRecord {
            start_reg: idx_start_reg,
            count: num_cols + 1,
            dest_reg: *record_reg,
            index_name: Some(index.name.clone()),
            affinity_str: None,
        });

        // Handle unique constraint
        if index.unique {
            let aff = index
                .columns
                .iter()
                .map(|ic| {
                    unsafe { &*table_ref }.columns()[ic.pos_in_table]
                        .affinity()
                        .aff_mask()
                })
                .collect::<String>();
            program.emit_insn(Insn::Affinity {
                start_reg: idx_start_reg,
                count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
                affinities: aff,
            });
            let constraint_check = program.allocate_label();
            // check if the record already exists in the index for unique indexes and abort if so
            program.emit_insn(Insn::NoConflict {
                cursor_id: *idx_cursor_id,
                target_pc: constraint_check,
                record_reg: idx_start_reg,
                num_regs: num_cols,
            });

            let idx_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::IdxRowId {
                cursor_id: *idx_cursor_id,
                dest: idx_rowid_reg,
            });

            // Skip over the UNIQUE constraint failure if the existing row is the one that we are currently changing
            program.emit_insn(Insn::Eq {
                lhs: beg,
                rhs: idx_rowid_reg,
                target_pc: constraint_check,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });

            let column_names = index.columns.iter().enumerate().fold(
                String::with_capacity(50),
                |mut accum, (idx, col)| {
                    if idx > 0 {
                        accum.push_str(", ");
                    }
                    accum.push_str(table_name);
                    accum.push('.');
                    accum.push_str(&col.name);
                    accum
                },
            );

            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description: column_names,
            });

            program.preassign_label_to_next_insn(constraint_check);
        }

        // Insert the index entry
        program.emit_insn(Insn::IdxInsert {
            cursor_id: *idx_cursor_id,
            record_reg: *record_reg,
            unpacked_start: Some(idx_start_reg),
            unpacked_count: Some((num_cols + 1) as u16),
            flags: IdxInsertFlags::new().nchange(true),
        });

        // Resolve insert skip label if it exists
        if let Some(label) = skip_insert_label {
            program.resolve_label(label, program.offset());
        }
    }

    if let Some(btree_table) = unsafe { &*table_ref }.btree() {
        if btree_table.is_strict {
            program.emit_insn(Insn::TypeCheck {
                start_reg: start,
                count: col_len,
                check_generated: true,
                table_reference: Arc::clone(&btree_table),
            });
        }

        if has_user_provided_rowid {
            let record_label = program.allocate_label();
            let target_reg = rowid_set_clause_reg.unwrap();

            program.emit_insn(Insn::Eq {
                lhs: target_reg,
                rhs: beg,
                target_pc: record_label,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });

            program.emit_insn(Insn::NotExists {
                cursor: cursor_id,
                rowid_reg: target_reg,
                target_pc: record_label,
            });

            let description = if let Some(idx) = rowid_alias_index {
                String::from(table_name)
                    + "."
                    + unsafe { &*table_ref }
                        .columns()
                        .get(idx)
                        .unwrap()
                        .name
                        .as_ref()
                        .map_or("", |v| v)
            } else {
                String::from(table_name) + ".rowid"
            };

            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description,
            });

            program.preassign_label_to_next_insn(record_label);
        }

        let record_reg = program.alloc_register();

        let affinity_str = unsafe { &*table_ref }
            .columns()
            .iter()
            .map(|col| col.affinity().aff_mask())
            .collect::<String>();

        program.emit_insn(Insn::MakeRecord {
            start_reg: start,
            count: col_len,
            dest_reg: record_reg,
            index_name: None,
            affinity_str: Some(affinity_str),
        });

        if has_user_provided_rowid {
            program.emit_insn(Insn::NotExists {
                cursor: cursor_id,
                rowid_reg: beg,
                target_pc: check_rowid_not_exists_label.unwrap(),
            });
        }

        // create alias for CDC rowid after the change (will differ from cdc_rowid_before_reg only in case of UPDATE with change in rowid alias)
        let cdc_rowid_after_reg = rowid_set_clause_reg.unwrap_or(beg);

        // create separate register with rowid before UPDATE for CDC
        let cdc_rowid_before_reg = if t_ctx.cdc_cursor_id.is_some() {
            let cdc_rowid_before_reg = program.alloc_register();
            if has_user_provided_rowid {
                program.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: cdc_rowid_before_reg,
                });
                Some(cdc_rowid_before_reg)
            } else {
                Some(cdc_rowid_after_reg)
            }
        } else {
            None
        };

        // create full CDC record before update if necessary
        let cdc_before_reg = if program.capture_data_changes_mode().has_before() {
            Some(emit_cdc_full_record(
                program,
                unsafe { &*table_ref }.table.columns(),
                cursor_id,
                cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set"),
            ))
        } else {
            None
        };

        // If we are updating the rowid, we cannot rely on overwrite on the
        // Insert instruction to update the cell. We need to first delete the current cell
        // and later insert the updated record
        if has_user_provided_rowid {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: table_name.to_string(),
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid_set_clause_reg.unwrap_or(beg),
            record_reg,
            flag: if has_user_provided_rowid {
                // The previous Insn::NotExists and Insn::Delete seek to the old rowid,
                // so to insert a new user-provided rowid, we need to seek to the correct place.
                InsertFlags::new().require_seek().update_rowid_change()
            } else {
                InsertFlags::new()
            },
            table_name: unsafe { &*table_ref }.identifier.clone(),
        });

        // Emit RETURNING results if specified
        if let Some(returning_columns) = &plan.returning {
            if !returning_columns.is_empty() {
                let value_registers = ReturningValueRegisters {
                    rowid_register: rowid_set_clause_reg.unwrap_or(beg),
                    columns_start_register: start,
                    num_columns: col_len,
                };

                emit_returning_results(program, returning_columns, &value_registers)?;
            }
        }

        // create full CDC record after update if necessary
        let cdc_after_reg = if program.capture_data_changes_mode().has_after() {
            Some(emit_cdc_patch_record(
                program,
                &unsafe { &*table_ref }.table,
                start,
                record_reg,
                cdc_rowid_after_reg,
            ))
        } else {
            None
        };

        let cdc_updates_record = if let Some(cdc_updates_register) = cdc_updates_register {
            let record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: cdc_updates_register,
                count: 2 * col_len,
                dest_reg: record_reg,
                index_name: None,
                affinity_str: None,
            });
            Some(record_reg)
        } else {
            None
        };

        // emit actual CDC instructions for write to the CDC table
        if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
            let cdc_rowid_before_reg =
                cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set");
            if has_user_provided_rowid {
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::DELETE,
                    cdc_cursor_id,
                    cdc_rowid_before_reg,
                    cdc_before_reg,
                    None,
                    None,
                    table_name,
                )?;
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::INSERT,
                    cdc_cursor_id,
                    cdc_rowid_after_reg,
                    cdc_after_reg,
                    None,
                    None,
                    table_name,
                )?;
            } else {
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::UPDATE,
                    cdc_cursor_id,
                    cdc_rowid_before_reg,
                    cdc_before_reg,
                    cdc_after_reg,
                    cdc_updates_record,
                    table_name,
                )?;
            }
        }
    } else if unsafe { &*table_ref }.virtual_table().is_some() {
        let arg_count = col_len + 2;
        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count,
            start_reg: beg,
            conflict_action: 0u16,
        });
    }

    if let Some(limit_ctx) = t_ctx.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }
    // TODO(pthorpe): handle RETURNING clause

    if let Some(label) = check_rowid_not_exists_label {
        program.preassign_label_to_next_insn(label);
    }

    Ok(())
}

pub fn prepare_cdc_if_necessary(
    program: &mut ProgramBuilder,
    schema: &Schema,
    changed_table_name: &str,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_mode();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    if changed_table_name == cdc_table {
        return Ok(None);
    }
    let Some(turso_cdc_table) = schema.get_table(cdc_table) else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let Some(cdc_btree) = turso_cdc_table.btree().clone() else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: cdc_btree.root_page.into(),
        db: 0, // todo(sivukhin): fix DB number when write will be supported for ATTACH
    });
    Ok(Some((cursor_id, cdc_btree)))
}

pub fn emit_cdc_patch_record(
    program: &mut ProgramBuilder,
    table: &Table,
    columns_reg: usize,
    record_reg: usize,
    rowid_reg: usize,
) -> usize {
    let columns = table.columns();
    let rowid_alias_position = columns.iter().position(|x| x.is_rowid_alias);
    if let Some(rowid_alias_position) = rowid_alias_position {
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: columns_reg + rowid_alias_position,
            extra_amount: 0,
        });
        let affinity_str = table
            .columns()
            .iter()
            .map(|col| col.affinity().aff_mask())
            .collect::<String>();

        program.emit_insn(Insn::MakeRecord {
            start_reg: columns_reg,
            count: table.columns().len(),
            dest_reg: record_reg,
            index_name: None,
            affinity_str: Some(affinity_str),
        });
        record_reg
    } else {
        record_reg
    }
}

pub fn emit_cdc_full_record(
    program: &mut ProgramBuilder,
    columns: &[Column],
    table_cursor_id: usize,
    rowid_reg: usize,
) -> usize {
    let columns_reg = program.alloc_registers(columns.len() + 1);
    for (i, column) in columns.iter().enumerate() {
        if column.is_rowid_alias {
            program.emit_insn(Insn::Copy {
                src_reg: rowid_reg,
                dst_reg: columns_reg + 1 + i,
                extra_amount: 0,
            });
        } else {
            program.emit_column_or_rowid(table_cursor_id, i, columns_reg + 1 + i);
        }
    }
    let affinity_str = columns
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: columns_reg + 1,
        count: columns.len(),
        dest_reg: columns_reg,
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    columns_reg
}

#[allow(clippy::too_many_arguments)]
pub fn emit_cdc_insns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)
    let turso_cdc_registers = program.alloc_registers(8);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    let Some(unixepoch_fn) = resolver.resolve_function("unixepoch", 0) else {
        bail_parse_error!("no function {}", "unixepoch");
    };
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: unixepoch_fn,
        arg_count: 0,
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 2);
    program.mark_last_insn_constant();

    program.emit_string8(table_name.to_string(), turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 4,
        extra_amount: 0,
    });

    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 5,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 5, None);
        program.mark_last_insn_constant();
    }

    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0, // todo(sivukhin): properly set value here from sqlite_sequence table when AUTOINCREMENT will be properly implemented in Turso
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: turso_cdc_registers,
        count: 8,
        dest_reg: record_reg,
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: "".to_string(),
    });
    Ok(())
}
/// Initialize the limit/offset counters and registers.
/// In case of compound SELECTs, the limit counter is initialized only once,
/// hence [LimitCtx::initialize_counter] being false in those cases.
fn init_limit(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
) {
    if t_ctx.limit_ctx.is_none() && limit.is_some() {
        t_ctx.limit_ctx = Some(LimitCtx::new(program));
    }
    let Some(limit_ctx) = &t_ctx.limit_ctx else {
        return;
    };

    if limit_ctx.initialize_counter {
        if let Some(expr) = limit {
            if let Some(value) = try_fold_expr_to_i64(expr) {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: limit_ctx.reg_limit,
                });
            } else {
                let r = limit_ctx.reg_limit;
                program.add_comment(program.offset(), "OFFSET expr");
                _ = translate_expr(program, None, expr, r, &t_ctx.resolver);
                program.emit_insn(Insn::MustBeInt { reg: r });
            }
        }
    }

    if t_ctx.reg_offset.is_none() {
        if let Some(expr) = offset {
            if let Some(value) = try_fold_expr_to_i64(expr) {
                if value != 0 {
                    let reg = program.alloc_register();
                    t_ctx.reg_offset = Some(reg);
                    program.emit_insn(Insn::Integer { value, dest: reg });
                    let combined_reg = program.alloc_register();
                    t_ctx.reg_limit_offset_sum = Some(combined_reg);
                    program.emit_insn(Insn::OffsetLimit {
                        limit_reg: limit_ctx.reg_limit,
                        offset_reg: reg,
                        combined_reg,
                    });
                }
            } else {
                let reg = program.alloc_register();
                t_ctx.reg_offset = Some(reg);
                let r = reg;

                program.add_comment(program.offset(), "OFFSET expr");
                _ = translate_expr(program, None, expr, r, &t_ctx.resolver);
                program.emit_insn(Insn::MustBeInt { reg: r });

                let combined_reg = program.alloc_register();
                t_ctx.reg_limit_offset_sum = Some(combined_reg);
                program.emit_insn(Insn::OffsetLimit {
                    limit_reg: limit_ctx.reg_limit,
                    offset_reg: reg,
                    combined_reg,
                });
            }
        }
    }

    // exit early if LIMIT 0
    let main_loop_end = t_ctx
        .label_main_loop_end
        .expect("label_main_loop_end must be set before init_limit");
    program.emit_insn(Insn::IfNot {
        reg: limit_ctx.reg_limit,
        target_pc: main_loop_end,
        jump_if_null: false,
    });
}

/// We have `Expr`s which have *not* had column references bound to them,
/// so they are in the state of Expr::Id/Expr::Qualified, etc, and instead of binding Expr::Column
/// we need to bind Expr::Register, as we have already loaded the *new* column values from the
/// UPDATE statement into registers starting at `columns_start_reg`, which we want to reference.
fn rewrite_where_for_update_registers(
    expr: &mut Expr,
    columns: &[Column],
    columns_start_reg: usize,
    rowid_reg: usize,
) -> Result<WalkControl> {
    walk_expr_mut(expr, &mut |e: &mut Expr| -> Result<WalkControl> {
        match e {
            Expr::Qualified(_, col) | Expr::DoublyQualified(_, _, col) => {
                let normalized = normalize_ident(col.as_str());
                if let Some((idx, c)) = columns.iter().enumerate().find(|(_, c)| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&normalized))
                }) {
                    if c.is_rowid_alias {
                        *e = Expr::Register(rowid_reg);
                    } else {
                        *e = Expr::Register(columns_start_reg + idx);
                    }
                }
            }
            Expr::Id(name) => {
                let normalized = normalize_ident(name.as_str());
                if ROWID_STRS
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(&normalized))
                {
                    *e = Expr::Register(rowid_reg);
                } else if let Some((idx, c)) = columns.iter().enumerate().find(|(_, c)| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&normalized))
                }) {
                    if c.is_rowid_alias {
                        *e = Expr::Register(rowid_reg);
                    } else {
                        *e = Expr::Register(columns_start_reg + idx);
                    }
                }
            }
            Expr::RowId { .. } => {
                *e = Expr::Register(rowid_reg);
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
}
