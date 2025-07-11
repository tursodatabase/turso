// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::rc::Rc;

use tracing::{instrument, Level};
use turso_sqlite3_parser::ast::{self, Expr};

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
    Distinctness, JoinOrderMember, Operation, SelectPlan, TableReferences, UpdatePlan,
};
use super::select::emit_simple_count;
use super::subquery::emit_subqueries;
use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::function::Func;
use crate::schema::{Schema, Table};
use crate::translate::compound_select::emit_program_for_compound_select;
use crate::translate::plan::{DeletePlan, Plan, QueryDestination, Search};
use crate::translate::values::emit_values;
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, RegisterOrLiteral};
use crate::vdbe::CursorID;
use crate::vdbe::{insn::Insn, BranchOffset};
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
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    pub result_column_indexes_in_orderby_sorter: Vec<usize>,
    // We might skip adding a SELECT result column into the ORDER BY sorter if it is an exact match in the ORDER BY keys.
    // This vector holds the indexes of the result columns that we need to skip.
    pub result_columns_to_skip_in_orderby_sorter: Option<Vec<usize>>,
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
}

impl<'a> TranslateCtx<'a> {
    pub fn new(
        program: &mut ProgramBuilder,
        schema: &'a Schema,
        syms: &'a SymbolTable,
        table_count: usize,
        result_column_count: usize,
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
            result_column_indexes_in_orderby_sorter: (0..result_column_count).collect(),
            result_columns_to_skip_in_orderby_sorter: None,
            resolver: Resolver::new(schema, syms),
            non_aggregate_expressions: Vec::new(),
            cdc_cursor_id: None,
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

#[derive(Clone, Copy, Debug)]
pub enum TransactionMode {
    None,
    Read,
    Write,
}

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
#[instrument(skip_all, level = Level::INFO)]
pub fn emit_program(
    program: &mut ProgramBuilder,
    plan: Plan,
    schema: &Schema,
    syms: &SymbolTable,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, plan, schema, syms),
        Plan::Delete(plan) => emit_program_for_delete(program, plan, schema, syms),
        Plan::Update(plan) => emit_program_for_update(program, plan, schema, syms, after),
        Plan::CompoundSelect { .. } => {
            emit_program_for_compound_select(program, plan, schema, syms)
        }
    }
}

#[instrument(skip_all, level = Level::INFO)]
fn emit_program_for_select(
    program: &mut ProgramBuilder,
    mut plan: SelectPlan,
    schema: &Schema,
    syms: &SymbolTable,
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        schema,
        syms,
        plan.table_references.joined_tables().len(),
        plan.result_columns.len(),
    );

    // Trivial exit on LIMIT 0
    if let Some(limit) = plan.limit {
        if limit == 0 {
            program.epilogue(TransactionMode::Read);
            program.result_columns = plan.result_columns;
            program.table_references.extend(plan.table_references);
            return Ok(());
        }
    }
    // Emit main parts of query
    emit_query(program, &mut plan, &mut t_ctx)?;

    // Finalize program
    if plan.table_references.joined_tables().is_empty() {
        program.epilogue(TransactionMode::None);
    } else {
        program.epilogue(TransactionMode::Read);
    }

    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[instrument(skip_all, level = Level::INFO)]
pub fn emit_query<'a>(
    program: &mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    if !plan.values.is_empty() {
        let reg_result_cols_start = emit_values(program, plan, &t_ctx.resolver)?;
        return Ok(reg_result_cols_start);
    }

    // Emit subqueries first so the results can be read in the main query loop.
    emit_subqueries(program, t_ctx, &mut plan.table_references)?;

    init_limit(program, t_ctx, plan.limit, plan.offset);

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
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
    if let Some(ref mut order_by) = plan.order_by {
        init_order_by(program, t_ctx, order_by, &plan.table_references)?;
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

    let mut order_by_necessary = plan.order_by.is_some() && !plan.contains_constant_false_condition;
    let order_by = plan.order_by.as_ref();

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
    }

    // Process ORDER BY results if needed
    if order_by.is_some() && order_by_necessary {
        emit_order_by(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

#[instrument(skip_all, level = Level::INFO)]
fn emit_program_for_delete(
    program: &mut ProgramBuilder,
    plan: DeletePlan,
    schema: &Schema,
    syms: &SymbolTable,
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        schema,
        syms,
        plan.table_references.joined_tables().len(),
        plan.result_columns.len(),
    );

    // exit early if LIMIT 0
    if let Some(0) = plan.limit {
        program.epilogue(TransactionMode::Write);
        program.result_columns = plan.result_columns;
        program.table_references.extend(plan.table_references);
        return Ok(());
    }

    init_limit(program, &mut t_ctx, plan.limit, None);

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
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

    emit_delete_insns(program, &mut t_ctx, &plan.table_references)?;

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
    program.epilogue(TransactionMode::Write);
    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

fn emit_delete_insns(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
) -> Result<()> {
    let table_reference = table_references.joined_tables().first().unwrap();
    let cursor_id = match &table_reference.op {
        Operation::Scan { .. } => {
            program.resolve_cursor_id(&CursorKey::table(table_reference.internal_id))
        }
        Operation::Search(search) => match search {
            Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                program.resolve_cursor_id(&CursorKey::table(table_reference.internal_id))
            }
            Search::Seek {
                index: Some(index), ..
            } => program.resolve_cursor_id(&CursorKey::index(
                table_reference.internal_id,
                index.clone(),
            )),
        },
    };
    let main_table_cursor_id =
        program.resolve_cursor_id(&CursorKey::table(table_reference.internal_id));

    // Emit the instructions to delete the row
    let key_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: main_table_cursor_id,
        dest: key_reg,
    });

    if table_reference.virtual_table().is_some() {
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
        let indexes = t_ctx
            .resolver
            .schema
            .indexes
            .get(table_reference.table.get_name());
        let index_refs_opt = indexes.map(|indexes| {
            indexes
                .iter()
                .map(|index| {
                    (
                        index.clone(),
                        program.resolve_cursor_id(&CursorKey::index(
                            table_reference.internal_id,
                            index.clone(),
                        )),
                    )
                })
                .collect::<Vec<_>>()
        });

        if let Some(index_refs) = index_refs_opt {
            for (index, index_cursor_id) in index_refs {
                let num_regs = index.columns.len() + 1;
                let start_reg = program.alloc_registers(num_regs);
                // Emit columns that are part of the index
                index
                    .columns
                    .iter()
                    .enumerate()
                    .for_each(|(reg_offset, column_index)| {
                        program.emit_column(
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
                    raise_error_if_no_matching_entry: true,
                });
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
                    &table_reference.table,
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
                table_reference.table.get_name(),
            )?;
        }

        program.emit_insn(Insn::Delete {
            cursor_id: main_table_cursor_id,
        });
    }
    if let Some(limit_ctx) = t_ctx.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }

    Ok(())
}

#[instrument(skip_all, level = Level::INFO)]
fn emit_program_for_update(
    program: &mut ProgramBuilder,
    mut plan: UpdatePlan,
    schema: &Schema,
    syms: &SymbolTable,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    let mut t_ctx = TranslateCtx::new(
        program,
        schema,
        syms,
        plan.table_references.joined_tables().len(),
        plan.returning.as_ref().map_or(0, |r| r.len()),
    );

    // Exit on LIMIT 0
    if let Some(0) = plan.limit {
        program.epilogue(TransactionMode::None);
        program.result_columns = plan.returning.unwrap_or_default();
        program.table_references.extend(plan.table_references);
        return Ok(());
    }

    init_limit(program, &mut t_ctx, plan.limit, plan.offset);
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
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
        emit_program_for_select(program, ephemeral_plan, schema, syms)?;
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
                name: index.name.clone(),
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
    emit_update_insns(&plan, &t_ctx, program, index_cursors, temp_cursor_id)?;

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

    // Finalize program
    program.epilogue(TransactionMode::Write);
    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[instrument(skip_all, level = Level::INFO)]
fn emit_update_insns(
    plan: &UpdatePlan,
    t_ctx: &TranslateCtx,
    program: &mut ProgramBuilder,
    index_cursors: Vec<(usize, usize)>,
    temp_cursor_id: Option<CursorID>,
) -> crate::Result<()> {
    let table_ref = plan.table_references.joined_tables().first().unwrap();
    let loop_labels = t_ctx.labels_main_loop.first().unwrap();
    let cursor_id = program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id));
    let (index, is_virtual) = match &table_ref.op {
        Operation::Scan { index, .. } => (
            index.as_ref().map(|index| {
                (
                    index.clone(),
                    program
                        .resolve_cursor_id(&CursorKey::index(table_ref.internal_id, index.clone())),
                )
            }),
            table_ref.virtual_table().is_some(),
        ),
        Operation::Search(search) => match search {
            &Search::RowidEq { .. } | Search::Seek { index: None, .. } => (None, false),
            Search::Seek {
                index: Some(index), ..
            } => (
                Some((
                    index.clone(),
                    program
                        .resolve_cursor_id(&CursorKey::index(table_ref.internal_id, index.clone())),
                )),
                false,
            ),
        },
    };

    let beg = program.alloc_registers(
        table_ref.table.columns().len()
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

    let rowid_alias_index = table_ref.columns().iter().position(|c| c.is_rowid_alias);

    let has_user_provided_rowid = if let Some(index) = rowid_alias_index {
        plan.set_clauses.iter().position(|(idx, _)| *idx == index)
    } else {
        None
    }
    .is_some();

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
            amount: 0,
        })
    }

    if let Some(offset) = t_ctx.reg_offset {
        program.emit_insn(Insn::IfPos {
            reg: offset,
            target_pc: loop_labels.next,
            decrement_by: 1,
        });
    }

    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.
    let start = if is_virtual { beg + 2 } else { beg + 1 };
    for (idx, table_column) in table_ref.columns().iter().enumerate() {
        let target_reg = start + idx;
        if let Some((_, expr)) = plan.set_clauses.iter().find(|(i, _)| *i == idx) {
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
                            table_ref.table.get_name(),
                            table_column
                                .name
                                .as_ref()
                                .expect("Column name must be present")
                        ),
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
                program.emit_column(cursor_id, column_idx_in_index.unwrap_or(idx), target_reg);
            }
        }
    }

    for (index, (idx_cursor_id, record_reg)) in plan.indexes_to_update.iter().zip(&index_cursors) {
        let num_cols = index.columns.len();
        // allocate scratch registers for the index columns plus rowid
        let idx_start_reg = program.alloc_registers(num_cols + 1);

        // Use the new rowid value (if the UPDATE statement sets the rowid alias),
        // otherwise keep using the original rowid. This guarantees that any
        // newly inserted/updated index entries point at the correct row after
        // the primary key change.
        let rowid_reg = if has_user_provided_rowid {
            // Safe to unwrap because `has_user_provided_rowid` implies the register was allocated.
            rowid_set_clause_reg.expect("rowid register must be set when updating rowid alias")
        } else {
            beg
        };
        let idx_cols_start_reg = beg + 1;

        // copy each index column from the table's column registers into these scratch regs
        for (i, col) in index.columns.iter().enumerate() {
            // copy from the table's column register over to the index's scratch register

            program.emit_insn(Insn::Copy {
                src_reg: idx_cols_start_reg + col.pos_in_table,
                dst_reg: idx_start_reg + i,
                amount: 0,
            });
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: idx_start_reg + num_cols,
            amount: 0,
        });

        // this record will be inserted into the index later
        program.emit_insn(Insn::MakeRecord {
            start_reg: idx_start_reg,
            count: num_cols + 1,
            dest_reg: *record_reg,
            index_name: Some(index.name.clone()),
        });

        if !index.unique {
            continue;
        }

        // check if the record already exists in the index for unique indexes and abort if so
        let constraint_check = program.allocate_label();
        program.emit_insn(Insn::NoConflict {
            cursor_id: *idx_cursor_id,
            target_pc: constraint_check,
            record_reg: idx_start_reg,
            num_regs: num_cols,
        });

        let column_names = index.columns.iter().enumerate().fold(
            String::with_capacity(50),
            |mut accum, (idx, col)| {
                if idx > 0 {
                    accum.push_str(", ");
                }
                accum.push_str(table_ref.table.get_name());
                accum.push('.');
                accum.push_str(&col.name);

                accum
            },
        );

        let idx_rowid_reg = program.alloc_register();
        program.emit_insn(Insn::IdxRowId {
            cursor_id: *idx_cursor_id,
            dest: idx_rowid_reg,
        });

        program.emit_insn(Insn::Eq {
            lhs: rowid_reg,
            rhs: idx_rowid_reg,
            target_pc: constraint_check,
            flags: CmpInsFlags::default(), // TODO: not sure what type of comparison flag is needed
            collation: program.curr_collation(),
        });

        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY, // TODO: distinct between primary key and unique index for error code
            description: column_names,
        });

        program.preassign_label_to_next_insn(constraint_check);
    }

    if let Some(btree_table) = table_ref.btree() {
        if btree_table.is_strict {
            program.emit_insn(Insn::TypeCheck {
                start_reg: start,
                count: table_ref.columns().len(),
                check_generated: true,
                table_reference: Rc::clone(&btree_table),
            });
        }

        if has_user_provided_rowid {
            let record_label = program.allocate_label();
            let idx = rowid_alias_index.unwrap();
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

            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description: format!(
                    "{}.{}",
                    table_ref.table.get_name(),
                    &table_ref
                        .columns()
                        .get(idx)
                        .unwrap()
                        .name
                        .as_ref()
                        .map_or("", |v| v)
                ),
            });

            program.preassign_label_to_next_insn(record_label);
        }

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: start,
            count: table_ref.columns().len(),
            dest_reg: record_reg,
            index_name: None,
        });

        if has_user_provided_rowid {
            program.emit_insn(Insn::NotExists {
                cursor: cursor_id,
                rowid_reg: beg,
                target_pc: check_rowid_not_exists_label.unwrap(),
            });
        }

        // For each index -> insert
        for (index, (idx_cursor_id, record_reg)) in plan.indexes_to_update.iter().zip(index_cursors)
        {
            let num_regs = index.columns.len() + 1;
            let start_reg = program.alloc_registers(num_regs);

            // Delete existing index key
            index
                .columns
                .iter()
                .enumerate()
                .for_each(|(reg_offset, column_index)| {
                    program.emit_column(
                        cursor_id,
                        column_index.pos_in_table,
                        start_reg + reg_offset,
                    );
                });

            program.emit_insn(Insn::RowId {
                cursor_id,
                dest: start_reg + num_regs - 1,
            });

            program.emit_insn(Insn::IdxDelete {
                start_reg,
                num_regs,
                cursor_id: idx_cursor_id,
                raise_error_if_no_matching_entry: true,
            });

            // Insert new index key (filled further above with values from set_clauses)
            program.emit_insn(Insn::IdxInsert {
                cursor_id: idx_cursor_id,
                record_reg,
                unpacked_start: Some(start),
                unpacked_count: Some((index.columns.len() + 1) as u16),
                flags: IdxInsertFlags::new(),
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
                &table_ref.table,
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
            program.emit_insn(Insn::Delete { cursor_id });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid_set_clause_reg.unwrap_or(beg),
            record_reg,
            flag: InsertFlags::new().update(true),
            table_name: table_ref.identifier.clone(),
        });

        // create full CDC record after update if necessary
        let cdc_after_reg = if program.capture_data_changes_mode().has_after() {
            Some(emit_cdc_patch_record(
                program,
                &table_ref.table,
                start,
                record_reg,
                cdc_rowid_after_reg,
            ))
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
                    table_ref.table.get_name(),
                )?;
                emit_cdc_insns(
                    program,
                    &t_ctx.resolver,
                    OperationMode::INSERT,
                    cdc_cursor_id,
                    cdc_rowid_after_reg,
                    cdc_after_reg,
                    None,
                    table_ref.table.get_name(),
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
                    table_ref.table.get_name(),
                )?;
            }
        }
    } else if table_ref.virtual_table().is_some() {
        let arg_count = table_ref.columns().len() + 2;
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
            amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: columns_reg,
            count: table.columns().len(),
            dest_reg: record_reg,
            index_name: None,
        });
        record_reg
    } else {
        record_reg
    }
}

pub fn emit_cdc_full_record(
    program: &mut ProgramBuilder,
    table: &Table,
    table_cursor_id: usize,
    rowid_reg: usize,
) -> usize {
    let columns = table.columns();
    let columns_reg = program.alloc_registers(columns.len() + 1);
    for (i, column) in columns.iter().enumerate() {
        if column.is_rowid_alias {
            program.emit_insn(Insn::Copy {
                src_reg: rowid_reg,
                dst_reg: columns_reg + 1 + i,
                amount: 0,
            });
        } else {
            program.emit_column(table_cursor_id, i, columns_reg + 1 + i);
        }
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: columns_reg + 1,
        count: columns.len(),
        dest_reg: columns_reg,
        index_name: None,
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
    table_name: &str,
) -> Result<()> {
    // (operation_id INTEGER PRIMARY KEY AUTOINCREMENT, operation_time INTEGER, operation_type INTEGER, table_name TEXT, id, before BLOB, after BLOB)
    let turso_cdc_registers = program.alloc_registers(7);
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

    let operation_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(operation_type, turso_cdc_registers + 2);
    program.mark_last_insn_constant();

    program.emit_string8(table_name.to_string(), turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 4,
        amount: 0,
    });

    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 5,
            amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 5, None);
        program.mark_last_insn_constant();
    }

    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 6,
            amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
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
        count: 7,
        dest_reg: record_reg,
        index_name: None,
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
    limit: Option<isize>,
    offset: Option<isize>,
) {
    if t_ctx.limit_ctx.is_none() {
        t_ctx.limit_ctx = limit.map(|_| LimitCtx::new(program));
    }
    let Some(limit_ctx) = t_ctx.limit_ctx else {
        return;
    };
    if limit_ctx.initialize_counter {
        program.emit_insn(Insn::Integer {
            value: limit.expect("limit must be Some if limit_ctx is Some") as i64,
            dest: limit_ctx.reg_limit,
        });
    }
    if t_ctx.reg_offset.is_none() && offset.is_some_and(|n| n.ne(&0)) {
        let reg = program.alloc_register();
        t_ctx.reg_offset = Some(reg);
        program.emit_insn(Insn::Integer {
            value: offset.unwrap() as i64,
            dest: reg,
        });
        let combined_reg = program.alloc_register();
        t_ctx.reg_limit_offset_sum = Some(combined_reg);
        program.emit_insn(Insn::OffsetLimit {
            limit_reg: t_ctx.limit_ctx.unwrap().reg_limit,
            offset_reg: reg,
            combined_reg,
        });
    }
}
