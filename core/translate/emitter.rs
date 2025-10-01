// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::collections::HashSet;
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
use crate::schema::{BTreeTable, Column, ResolvedFkRef, Schema, Table, ROWID_SENTINEL};
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
        program.reg_result_cols_start = t_ctx.reg_result_cols_start
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
        Some(init_distinct(program, plan)?)
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

    let has_parent_fks = connection.foreign_keys_enabled() && {
        let table_name = plan
            .table_references
            .joined_tables()
            .first()
            .unwrap()
            .table
            .get_name();
        resolver.schema.any_resolved_fks_referencing(table_name)
    };
    // Open FK scope for the whole statement
    if has_parent_fks {
        program.emit_insn(Insn::FkCounter {
            increment_value: 1,
            check_abort: false,
            is_scope: true,
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
    if has_parent_fks {
        program.emit_insn(Insn::FkCounter {
            increment_value: -1,
            check_abort: true,
            is_scope: true,
        });
    }
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

    if connection.foreign_keys_enabled()
        && unsafe { &*table_reference }.btree().is_some()
        && t_ctx
            .resolver
            .schema
            .any_resolved_fks_referencing(table_name)
    {
        emit_fk_parent_existence_checks(
            program,
            &t_ctx.resolver,
            table_name,
            main_table_cursor_id,
            key_reg,
        )?;
    }

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

/// Emit parent-side FK counter maintenance for UPDATE on a table with a composite PK.
///
/// For every child FK that targets `parent_table_name`:
///  1. Pass 1: If any child row currently references the OLD parent key,
///     increment the global FK counter (deferred violation potential).
///     We try an index probe on child(child_cols...) if available, else do a table scan.
///  2. Pass 2: If any child row references the NEW parent key, decrement the counter
///     (because the reference would be “retargeted” by the update).
pub fn emit_fk_parent_pk_change_counters(
    program: &mut ProgramBuilder,
    incoming: &[ResolvedFkRef],
    resolver: &Resolver,
    old_pk_start: usize,
    new_pk_start: usize,
    n_cols: usize,
) -> crate::Result<()> {
    if incoming.is_empty() {
        return Ok(());
    }
    for fk_ref in incoming.iter() {
        let child_tbl = &fk_ref.child_table;
        let child_cols = &fk_ref.fk.child_columns;
        // Prefer exact-prefix index on child
        let idx = resolver.schema.get_indices(&child_tbl.name).find(|ix| {
            ix.columns.len() == child_cols.len()
                && ix
                    .columns
                    .iter()
                    .zip(child_cols.iter())
                    .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
        });

        if let Some(ix) = idx {
            let icur = program.alloc_cursor_id(CursorType::BTreeIndex(ix.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: icur,
                root_page: ix.root_page,
                db: 0,
            });

            // Build child-probe key from OLD parent PK (1:1 map ensured by the column-name equality above)
            // We just copy the OLD PK registers, apply index affinities before the probe.
            let probe_start = old_pk_start;

            // Apply affinities for composite comparison
            let aff: String = ix
                .columns
                .iter()
                .map(|ic| {
                    let (_, col) = child_tbl
                        .get_column(&ic.name)
                        .expect("indexed child column not found");
                    col.affinity().aff_mask()
                })
                .collect();
            if let Some(count) = NonZeroUsize::new(n_cols) {
                program.emit_insn(Insn::Affinity {
                    start_reg: probe_start,
                    count,
                    affinities: aff,
                });
            }

            let found = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: found,
                record_reg: probe_start,
                num_regs: n_cols,
            });

            // Not found => no increment
            program.emit_insn(Insn::Close { cursor_id: icur });
            let skip = program.allocate_label();
            program.emit_insn(Insn::Goto { target_pc: skip });

            // Found => increment
            program.preassign_label_to_next_insn(found);
            program.emit_insn(Insn::Close { cursor_id: icur });
            program.emit_insn(Insn::FkCounter {
                increment_value: 1,
                check_abort: false,
                is_scope: false,
            });
            program.preassign_label_to_next_insn(skip);
        } else {
            // Table-scan fallback with per-column checks (jump-if-NULL semantics)
            let ccur = program.alloc_cursor_id(CursorType::BTreeTable(child_tbl.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: ccur,
                root_page: child_tbl.root_page,
                db: 0,
            });

            let done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: ccur,
                pc_if_empty: done,
            });

            let loop_top = program.allocate_label();
            let next_row = program.allocate_label();
            program.preassign_label_to_next_insn(loop_top);

            for (i, child_name) in child_cols.iter().enumerate() {
                let (pos, _) = child_tbl.get_column(child_name).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("child col {child_name} missing"))
                })?;
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: ccur,
                    column: pos,
                    dest: tmp,
                    default: None,
                });

                // Treat NULL as non-match: jump away immediately
                program.emit_insn(Insn::IsNull {
                    reg: tmp,
                    target_pc: next_row,
                });

                // Eq(tmp, old_pk[i]) with Binary collation, jump-if-NULL enabled
                let cont = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: old_pk_start + i,
                    target_pc: cont,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: Some(super::collate::CollationSeq::Binary),
                });
                program.emit_insn(Insn::Goto {
                    target_pc: next_row,
                });
                program.preassign_label_to_next_insn(cont);
            }

            // All columns matched OLD -> increment
            program.emit_insn(Insn::FkCounter {
                increment_value: 1,
                check_abort: false,
                is_scope: false,
            });

            program.preassign_label_to_next_insn(next_row);
            program.emit_insn(Insn::Next {
                cursor_id: ccur,
                pc_if_next: loop_top,
            });
            program.preassign_label_to_next_insn(done);
            program.emit_insn(Insn::Close { cursor_id: ccur });
        }
    }

    // PASS 2: count children of NEW key
    for fk_ref in incoming.iter() {
        let child_tbl = &fk_ref.child_table;
        let child_cols = &fk_ref.fk.child_columns;

        let idx = resolver.schema.get_indices(&child_tbl.name).find(|ix| {
            ix.columns.len() == child_cols.len()
                && ix
                    .columns
                    .iter()
                    .zip(child_cols.iter())
                    .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
        });

        if let Some(ix) = idx {
            let icur = program.alloc_cursor_id(CursorType::BTreeIndex(ix.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: icur,
                root_page: ix.root_page,
                db: 0,
            });

            // Build probe from NEW PK registers; apply affinities
            let probe_start = new_pk_start;
            let aff: String = ix
                .columns
                .iter()
                .map(|ic| {
                    let (_, col) = child_tbl
                        .get_column(&ic.name)
                        .expect("indexed child column not found");
                    col.affinity().aff_mask()
                })
                .collect();
            if let Some(count) = NonZeroUsize::new(n_cols) {
                program.emit_insn(Insn::Affinity {
                    start_reg: probe_start,
                    count,
                    affinities: aff,
                });
            }

            let found = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: found,
                record_reg: probe_start,
                num_regs: n_cols,
            });

            // Not found => no decrement
            program.emit_insn(Insn::Close { cursor_id: icur });
            let skip = program.allocate_label();
            program.emit_insn(Insn::Goto { target_pc: skip });

            // Found => decrement
            program.preassign_label_to_next_insn(found);
            program.emit_insn(Insn::Close { cursor_id: icur });
            program.emit_insn(Insn::FkCounter {
                increment_value: -1,
                check_abort: false,
                is_scope: false,
            });
            program.preassign_label_to_next_insn(skip);
        } else {
            // Table-scan fallback on NEW key
            let ccur = program.alloc_cursor_id(CursorType::BTreeTable(child_tbl.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: ccur,
                root_page: child_tbl.root_page,
                db: 0,
            });

            let done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: ccur,
                pc_if_empty: done,
            });

            let loop_top = program.allocate_label();
            let next_row = program.allocate_label();
            program.preassign_label_to_next_insn(loop_top);

            for (i, child_name) in child_cols.iter().enumerate() {
                let (pos, _) = child_tbl.get_column(child_name).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("child col {child_name} missing"))
                })?;
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: ccur,
                    column: pos,
                    dest: tmp,
                    default: None,
                });

                program.emit_insn(Insn::IsNull {
                    reg: tmp,
                    target_pc: next_row,
                });

                let cont = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: new_pk_start + i,
                    target_pc: cont,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: Some(super::collate::CollationSeq::Binary),
                });
                program.emit_insn(Insn::Goto {
                    target_pc: next_row,
                });
                program.preassign_label_to_next_insn(cont);
            }

            // All columns matched NEW: decrement
            program.emit_insn(Insn::FkCounter {
                increment_value: -1,
                check_abort: false,
                is_scope: false,
            });

            program.preassign_label_to_next_insn(next_row);
            program.emit_insn(Insn::Next {
                cursor_id: ccur,
                pc_if_next: loop_top,
            });
            program.preassign_label_to_next_insn(done);
            program.emit_insn(Insn::Close { cursor_id: ccur });
        }
    }
    Ok(())
}

/// Emit checks that prevent updating/deleting a parent row that is still referenced by a child.
///
/// If the global deferred-FK counter is zero, we skip all checks (fast path for no outstanding refs).
/// For each incoming FK:
///   Build the parent key (in FK parent-column order) from the current row.
///   Probe the child table for any row whose FK columns equal that key.
///     - If an exact child index exists on the FK columns, use `NotFound` against that index.
///     - Otherwise, scan the child table and compare each FK column (NULL short-circuits to “no match”).
/// If a referencing child is found:
///     - Deferred FK: increment counter (violation will be raised at COMMIT).
///     - Immediate FK: raise `SQLITE_CONSTRAINT_FOREIGNKEY` now.
pub fn emit_fk_parent_existence_checks(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    parent_table_name: &str,
    parent_cursor_id: usize,
    parent_rowid_reg: usize,
) -> Result<()> {
    let parent_bt = resolver
        .schema
        .get_btree_table(parent_table_name)
        .ok_or_else(|| crate::LimboError::InternalError("parent not btree".into()))?;

    for fk_ref in resolver.schema.resolved_fks_referencing(parent_table_name) {
        // Resolve parent key columns
        let parent_cols: Vec<String> = if fk_ref.fk.parent_columns.is_empty() {
            parent_bt
                .primary_key_columns
                .iter()
                .map(|(n, _)| n.clone())
                .collect()
        } else {
            fk_ref.fk.parent_columns.clone()
        };

        // Load parent key values for THIS row into regs, in parent_cols order
        let parent_cols_len = parent_cols.len();
        let parent_key_start = program.alloc_registers(parent_cols_len);
        for (i, pcol) in parent_cols.iter().enumerate() {
            let src = if pcol.eq_ignore_ascii_case("rowid") {
                parent_rowid_reg
            } else {
                let (pos, col) = parent_bt
                    .get_column(&normalize_ident(pcol))
                    .ok_or_else(|| {
                        crate::LimboError::InternalError(format!("col {pcol} missing"))
                    })?;
                if col.is_rowid_alias {
                    parent_rowid_reg
                } else {
                    // read current cell's column value
                    program.emit_insn(Insn::Column {
                        cursor_id: parent_cursor_id,
                        column: pos,
                        dest: parent_key_start + i,
                        default: None,
                    });
                    continue;
                }
            };
            program.emit_insn(Insn::Copy {
                src_reg: src,
                dst_reg: parent_key_start + i,
                extra_amount: 0,
            });
        }

        // Build child-side probe key in child_columns order, from parent_key_start
        //
        // Map parent_col to child_col position 1:1
        let child_cols = &fk_ref.fk.child_columns;
        // Try to find an index on child(child_cols...) to do an existance check
        let child_idx = resolver
            .schema
            .get_indices(&fk_ref.child_table.name)
            .find(|idx| {
                idx.columns.len() == child_cols.len()
                    && idx
                        .columns
                        .iter()
                        .zip(child_cols.iter())
                        .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
            });

        if let Some(idx) = child_idx {
            // Index existence probe: Found -> violation
            let icur = program.alloc_cursor_id(CursorType::BTreeIndex(idx.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: icur,
                root_page: idx.root_page,
                db: 0,
            });

            // Pack the child key regs from the parent key regs in fk order.
            // Same order because we matched columns 1:1 above
            let probe_start = program.alloc_registers(parent_cols_len);
            for i in 0..parent_cols_len {
                program.emit_insn(Insn::Copy {
                    src_reg: parent_key_start + i,
                    dst_reg: probe_start + i,
                    extra_amount: 0,
                });
            }
            if let Some(count) = NonZeroUsize::new(parent_cols_len) {
                // Apply index affinities for composite comparison
                let aff: String = idx
                    .columns
                    .iter()
                    .map(|ic| {
                        let (_, col) = fk_ref
                            .child_table
                            .get_column(&ic.name)
                            .expect("indexed child column not found");
                        col.affinity().aff_mask()
                    })
                    .collect();
                program.emit_insn(Insn::Affinity {
                    start_reg: probe_start,
                    count,
                    affinities: aff,
                });
            }

            let ok = program.allocate_label();
            program.emit_insn(Insn::NotFound {
                cursor_id: icur,
                target_pc: ok,
                record_reg: probe_start,
                num_regs: parent_cols_len,
            });

            // found referencing child row = violation path
            program.emit_insn(Insn::Close { cursor_id: icur });
            if fk_ref.fk.deferred {
                program.emit_insn(Insn::FkCounter {
                    increment_value: 1,
                    check_abort: false,
                    is_scope: false,
                });
            } else {
                program.emit_insn(Insn::Halt {
                    err_code: crate::error::SQLITE_CONSTRAINT_FOREIGNKEY,
                    description: "FOREIGN KEY constraint failed".to_string(),
                });
            }
            program.preassign_label_to_next_insn(ok);
            program.emit_insn(Insn::Close { cursor_id: icur });
        } else {
            // Fallback: table-scan the child table
            let ccur = program.alloc_cursor_id(CursorType::BTreeTable(fk_ref.child_table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: ccur,
                root_page: fk_ref.child_table.root_page,
                db: 0,
            });

            let done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: ccur,
                pc_if_empty: done,
            });

            // Loop labels local to this scan
            let loop_top = program.allocate_label();
            let next_row = program.allocate_label();

            program.preassign_label_to_next_insn(loop_top);

            // For each FK column: require a match, if NULL or mismatch -> next_row
            for (i, child_col) in child_cols.iter().enumerate() {
                let (pos, _) = fk_ref
                    .child_table
                    .get_column(&normalize_ident(child_col))
                    .ok_or_else(|| {
                        crate::LimboError::InternalError(format!("child col {child_col} missing"))
                    })?;

                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: ccur,
                    column: pos,
                    dest: tmp,
                    default: None,
                });

                // NULL FK value => this child row cannot reference the parent, skip row
                program.emit_insn(Insn::IsNull {
                    reg: tmp,
                    target_pc: next_row,
                });

                // Equal? continue to check next column; else jump to next_row
                let cont_i = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: parent_key_start + i,
                    target_pc: cont_i,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: program.curr_collation(),
                });
                // Not equal -> skip this child row
                program.emit_insn(Insn::Goto {
                    target_pc: next_row,
                });

                // Equal path resumes here, then we check the next column
                program.preassign_label_to_next_insn(cont_i);
            }

            // If we reached here, all FK columns matched, violation
            if fk_ref.fk.deferred {
                program.emit_insn(Insn::FkCounter {
                    increment_value: 1,
                    check_abort: false,
                    is_scope: false,
                });
            } else {
                program.emit_insn(Insn::Halt {
                    err_code: crate::error::SQLITE_CONSTRAINT_FOREIGNKEY,
                    description: "FOREIGN KEY constraint failed".to_string(),
                });
            }

            // Advance to next child row and loop
            program.preassign_label_to_next_insn(next_row);
            program.emit_insn(Insn::Next {
                cursor_id: ccur,
                pc_if_next: loop_top,
            });

            program.preassign_label_to_next_insn(done);
            program.emit_insn(Insn::Close { cursor_id: ccur });
        }
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

    let fk_enabled = connection.foreign_keys_enabled();
    let table_name = plan
        .table_references
        .joined_tables()
        .first()
        .unwrap()
        .table
        .get_name();
    let has_child_fks = fk_enabled && resolver.schema.has_child_fks(table_name);
    let has_parent_fks = fk_enabled && resolver.schema.any_resolved_fks_referencing(table_name);
    // statement-level FK scope open
    if has_child_fks || has_parent_fks {
        program.emit_insn(Insn::FkCounter {
            increment_value: 1,
            check_abort: false,
            is_scope: true,
        });
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

    if has_child_fks || has_parent_fks {
        program.emit_insn(Insn::FkCounter {
            increment_value: -1,
            check_abort: true,
            is_scope: true,
        });
    }
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

    if connection.foreign_keys_enabled() {
        let rowid_new_reg = rowid_set_clause_reg.unwrap_or(beg);
        if let Some(table_btree) = unsafe { &*table_ref }.btree() {
            //first, stablize the image of the NEW row in the registers
            if !table_btree.primary_key_columns.is_empty() {
                let set_cols: std::collections::HashSet<usize> = plan
                    .set_clauses
                    .iter()
                    .filter_map(|(i, _)| if *i == ROWID_SENTINEL { None } else { Some(*i) })
                    .collect();
                for (pk_name, _) in &table_btree.primary_key_columns {
                    let (pos, col) = table_btree.get_column(pk_name).unwrap();
                    if !set_cols.contains(&pos) {
                        if col.is_rowid_alias {
                            program.emit_insn(Insn::Copy {
                                src_reg: rowid_new_reg,
                                dst_reg: start + pos,
                                extra_amount: 0,
                            });
                        } else {
                            program.emit_insn(Insn::Column {
                                cursor_id,
                                column: pos,
                                dest: start + pos,
                                default: None,
                            });
                        }
                    }
                }
            }
            if t_ctx.resolver.schema.has_child_fks(table_name) {
                // Child-side checks:
                // this ensures updated row still satisfies child FKs that point OUT from this table
                emit_fk_child_existence_checks(
                    program,
                    &t_ctx.resolver,
                    &table_btree,
                    table_name,
                    start,
                    rowid_new_reg,
                    &plan
                        .set_clauses
                        .iter()
                        .map(|(i, _)| *i)
                        .collect::<HashSet<_>>(),
                )?;
            }
            // Parent-side checks:
            // We only need to do work if the referenced key (the parent key) might change.
            // we detect that by comparing OLD vs NEW primary key representation
            // then run parent FK checks only when it actually changes.
            if t_ctx
                .resolver
                .schema
                .any_resolved_fks_referencing(table_name)
            {
                let updated_parent_positions: HashSet<usize> =
                    plan.set_clauses.iter().map(|(i, _)| *i).collect();

                // If no incoming FK’s parent key can be affected by these updates, skip the whole parent-FK block.
                let incoming = t_ctx.resolver.schema.resolved_fks_referencing(table_name);
                let parent_tbl = &table_btree;
                let maybe_affects_parent_key = incoming
                    .iter()
                    .any(|r| r.parent_key_may_change(&updated_parent_positions, parent_tbl));
                if maybe_affects_parent_key {
                    let pk_len = table_btree.primary_key_columns.len();
                    match pk_len {
                        0 => {
                            // Rowid table: the implicit PK is rowid.
                            // If rowid is unchanged then we skip, else check that no child row still references the OLD key.
                            let skip_parent_fk = program.allocate_label();
                            let old_rowid_reg = beg;
                            let new_rowid_reg = rowid_set_clause_reg.unwrap_or(beg);

                            program.emit_insn(Insn::Eq {
                                lhs: new_rowid_reg,
                                rhs: old_rowid_reg,
                                target_pc: skip_parent_fk,
                                flags: CmpInsFlags::default(),
                                collation: program.curr_collation(),
                            });
                            // Rowid changed: check incoming FKs (children) that reference this parent row
                            emit_fk_parent_existence_checks(
                                program,
                                &t_ctx.resolver,
                                table_name,
                                cursor_id,
                                old_rowid_reg,
                            )?;
                            program.preassign_label_to_next_insn(skip_parent_fk);
                        }
                        1 => {
                            // Single-column declared PK, may be a rowid alias or a real column.
                            // If PK value unchanged then skip, else verify no child still references OLD key.
                            let (pk_name, _) = &table_btree.primary_key_columns[0];
                            let (pos, col) = table_btree.get_column(pk_name).unwrap();

                            let old_reg = program.alloc_register();
                            if col.is_rowid_alias {
                                program.emit_insn(Insn::RowId {
                                    cursor_id,
                                    dest: old_reg,
                                });
                            } else {
                                program.emit_insn(Insn::Column {
                                    cursor_id,
                                    column: pos,
                                    dest: old_reg,
                                    default: None,
                                });
                            }
                            let new_reg = if col.is_rowid_alias {
                                rowid_new_reg
                            } else {
                                start + pos
                            };

                            let skip_parent_fk = program.allocate_label();
                            program.emit_insn(Insn::Eq {
                                lhs: old_reg,
                                rhs: new_reg,
                                target_pc: skip_parent_fk,
                                flags: CmpInsFlags::default(),
                                collation: program.curr_collation(),
                            });
                            emit_fk_parent_existence_checks(
                                program,
                                &t_ctx.resolver,
                                table_name,
                                cursor_id,
                                beg,
                            )?;
                            program.preassign_label_to_next_insn(skip_parent_fk);
                        }
                        _ => {
                            // Composite PK:
                            // 1. Materialize OLD PK vector from current row.
                            // 2. Materialize NEW PK vector from updated registers.
                            // 3. If any component differs, the PK changes -> run composite parent-FK update flow.
                            let old_pk_start = program.alloc_registers(pk_len);
                            for (i, (pk_name, _)) in
                                table_btree.primary_key_columns.iter().enumerate()
                            {
                                let (pos, col) = table_btree.get_column(pk_name).unwrap();
                                if col.is_rowid_alias {
                                    program.emit_insn(Insn::Copy {
                                        src_reg: beg,
                                        dst_reg: old_pk_start + i,
                                        extra_amount: 0,
                                    });
                                } else {
                                    program.emit_insn(Insn::Column {
                                        cursor_id,
                                        column: pos,
                                        dest: old_pk_start + i,
                                        default: None,
                                    });
                                }
                            }

                            // Build NEW PK values from the updated registers
                            let new_pk_start = program.alloc_registers(pk_len);
                            for (i, (pk_name, _)) in
                                table_btree.primary_key_columns.iter().enumerate()
                            {
                                let (pos, col) = table_btree.get_column(pk_name).unwrap();
                                let src = if col.is_rowid_alias {
                                    rowid_new_reg
                                } else {
                                    start + pos // Updated value from SET clause
                                };
                                program.emit_insn(Insn::Copy {
                                    src_reg: src,
                                    dst_reg: new_pk_start + i,
                                    extra_amount: 0,
                                });
                            }

                            // Compare OLD vs NEW to see if PK is changing
                            let skip_parent_fk = program.allocate_label();
                            let pk_changed = program.allocate_label();

                            for i in 0..pk_len {
                                if i == pk_len - 1 {
                                    // Last comparison, if equal, all are equal
                                    program.emit_insn(Insn::Eq {
                                        lhs: old_pk_start + i,
                                        rhs: new_pk_start + i,
                                        target_pc: skip_parent_fk,
                                        flags: CmpInsFlags::default(),
                                        collation: program.curr_collation(),
                                    });
                                    // Not equal - PK is changing
                                    program.emit_insn(Insn::Goto {
                                        target_pc: pk_changed,
                                    });
                                } else {
                                    // Not last comparison
                                    let next_check = program.allocate_label();
                                    program.emit_insn(Insn::Eq {
                                        lhs: old_pk_start + i,
                                        rhs: new_pk_start + i,
                                        target_pc: next_check, // Equal, check next component
                                        flags: CmpInsFlags::default(),
                                        collation: program.curr_collation(),
                                    });
                                    // Not equal - PK is changing
                                    program.emit_insn(Insn::Goto {
                                        target_pc: pk_changed,
                                    });
                                    program.preassign_label_to_next_insn(next_check);
                                }
                            }
                            program.preassign_label_to_next_insn(pk_changed);
                            // PK changed: maintain the deferred FK counter in two passes
                            emit_fk_parent_pk_change_counters(
                                program,
                                &incoming,
                                &t_ctx.resolver,
                                old_pk_start,
                                new_pk_start,
                                pk_len,
                            )?;
                            program.preassign_label_to_next_insn(skip_parent_fk);
                        }
                    }
                }
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

pub fn emit_fk_child_existence_checks(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &BTreeTable,
    table_name: &str,
    start_reg: usize,
    rowid_reg: usize,
    updated_cols: &HashSet<usize>,
) -> Result<()> {
    for fk_ref in resolver.schema.resolved_fks_for_child(table_name) {
        // Skip when the child key is untouched (including rowid-alias special case)
        if !fk_ref.child_key_changed(updated_cols, table) {
            continue;
        }

        let fk_ok = program.allocate_label();

        // look for NULLs in any child FK column
        for child_name in &fk_ref.child_cols {
            let (i, col) = table.get_column(child_name).unwrap();
            let src = if col.is_rowid_alias {
                rowid_reg
            } else {
                start_reg + i
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: fk_ok,
            });
        }

        if fk_ref.parent_uses_rowid {
            // Fast rowid probe on the parent table
            let parent_tbl = resolver
                .schema
                .get_btree_table(&fk_ref.fk.parent_table)
                .expect("Parent must be btree");

            let pcur = program.alloc_cursor_id(CursorType::BTreeTable(parent_tbl.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: pcur,
                root_page: parent_tbl.root_page,
                db: 0,
            });

            let (i_child, col_child) = table.get_column(&fk_ref.child_cols[0]).unwrap();
            let val_reg = if col_child.is_rowid_alias {
                rowid_reg
            } else {
                start_reg + i_child
            };
            let tmp = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val_reg,
                dst_reg: tmp,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: tmp });
            let violation = program.allocate_label();
            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmp,
                target_pc: violation,
            });
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: fk_ok });

            program.preassign_label_to_next_insn(violation);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            if fk_ref.fk.deferred {
                program.emit_insn(Insn::FkCounter {
                    increment_value: 1,
                    check_abort: false,
                    is_scope: false,
                });
            } else {
                program.emit_insn(Insn::Halt {
                    err_code: crate::error::SQLITE_CONSTRAINT_FOREIGNKEY,
                    description: "FOREIGN KEY constraint failed".to_string(),
                });
            }
        } else {
            // Unique-index probe on the parent (already resolved)
            let parent_idx = fk_ref
                .parent_unique_index
                .as_ref()
                .expect("parent unique index required");
            let icur = program.alloc_cursor_id(CursorType::BTreeIndex(parent_idx.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: icur,
                root_page: parent_idx.root_page,
                db: 0,
            });

            // Build probe key from NEW child values in fk order
            let n = fk_ref.child_cols.len();
            let probe_start = program.alloc_registers(n);
            for (k, child_name) in fk_ref.child_cols.iter().enumerate() {
                let (i, col) = table.get_column(child_name).unwrap();
                program.emit_insn(Insn::Copy {
                    src_reg: if col.is_rowid_alias {
                        rowid_reg
                    } else {
                        start_reg + i
                    },
                    dst_reg: probe_start + k,
                    extra_amount: 0,
                });
            }

            let aff: String = parent_idx
                .columns
                .iter()
                .map(|ic| table.columns[ic.pos_in_table].affinity().aff_mask())
                .collect();
            program.emit_insn(Insn::Affinity {
                start_reg: probe_start,
                count: NonZeroUsize::new(n).unwrap(),
                affinities: aff,
            });
            let found = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: found,
                record_reg: probe_start,
                num_regs: n,
            });

            // Not found => violation
            program.emit_insn(Insn::Close { cursor_id: icur });
            if fk_ref.fk.deferred {
                program.emit_insn(Insn::FkCounter {
                    increment_value: 1,
                    check_abort: false,
                    is_scope: false,
                });
            } else {
                program.emit_insn(Insn::Halt {
                    err_code: crate::error::SQLITE_CONSTRAINT_FOREIGNKEY,
                    description: "FOREIGN KEY constraint failed".to_string(),
                });
            }
            program.emit_insn(Insn::Goto { target_pc: fk_ok });

            // Found => OK
            program.preassign_label_to_next_insn(found);
            program.emit_insn(Insn::Close { cursor_id: icur });
        }

        program.preassign_label_to_next_insn(fk_ok);
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
