// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::rc::Rc;
use std::sync::Arc;

use limbo_sqlite3_parser::ast::{self, SortOrder};
use tracing::{instrument, Level};

use super::aggregation::emit_ungrouped_aggregation;
use super::expr::{translate_condition_expr, translate_expr, ConditionMetadata};
use super::group_by::{
    group_by_agg_phase, group_by_emit_row_phase, init_group_by, GroupByMetadata, GroupByRowSource,
};
use super::main_loop::{
    close_loop, emit_loop, init_distinct, init_loop, open_loop, LeftJoinMetadata, LoopLabels,
};
use super::order_by::{emit_order_by, init_order_by, SortMetadata};
use super::plan::{
    JoinOrderMember, Operation, QueryDestination, SelectPlan, TableReferences, UpdatePlan,
};
use super::schema::ParseSchema;
use super::select::emit_simple_count;
use super::subquery::emit_subqueries;
use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::function::Func;
use crate::schema::{Index, IndexColumn, Schema};
use crate::translate::plan::{DeletePlan, Plan, Search};
use crate::translate::values::emit_values;
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, RegisterOrLiteral};
use crate::vdbe::{insn::Insn, BranchOffset};
use crate::{Result, SymbolTable};

pub struct Resolver<'a> {
    pub schema: &'a Schema,
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache: Vec<(&'a ast::Expr, usize)>,
}

impl<'a> Resolver<'a> {
    pub fn new(schema: &'a Schema, symbol_table: &'a SymbolTable) -> Self {
        Self {
            schema,
            symbol_table,
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

    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<usize> {
        self.expr_to_reg_cache
            .iter()
            .find(|(e, _)| exprs_are_equivalent(expr, e))
            .map(|(_, reg)| *reg)
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
#[instrument(skip_all, level = Level::TRACE)]
pub fn emit_program(
    program: &mut ProgramBuilder,
    plan: Plan,
    schema: &Schema,
    syms: &SymbolTable,
) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, plan, schema, syms),
        Plan::Delete(plan) => emit_program_for_delete(program, plan, schema, syms),
        Plan::Update(plan) => emit_program_for_update(program, plan, schema, syms),
        Plan::CompoundSelect { .. } => {
            emit_program_for_compound_select(program, plan, schema, syms)
        }
    }
}

#[instrument(skip_all, level = Level::TRACE)]
fn emit_program_for_compound_select(
    program: &mut ProgramBuilder,
    plan: Plan,
    schema: &Schema,
    syms: &SymbolTable,
) -> Result<()> {
    let Plan::CompoundSelect {
        mut first,
        mut rest,
        limit,
        ..
    } = plan
    else {
        crate::bail_parse_error!("expected compound select plan");
    };

    // Trivial exit on LIMIT 0
    if let Some(limit) = limit {
        if limit == 0 {
            program.epilogue(TransactionMode::Read);
            program.result_columns = first.result_columns;
            program.table_references.extend(first.table_references);
            return Ok(());
        }
    }

    // Each subselect gets their own TranslateCtx, but they share the same limit_ctx
    // because the LIMIT applies to the entire compound select, not just a single subselect.
    // The way LIMIT works with compound selects is:
    // - If a given subselect appears BEFORE any UNION, then do NOT count those rows towards the LIMIT,
    //   because the rows from those subselects need to be deduplicated before they start being counted.
    // - If a given subselect appears AFTER the last UNION, then count those rows towards the LIMIT immediately.
    let limit_ctx = limit.map(|limit| {
        let reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: reg,
        });
        LimitCtx::new_shared(reg)
    });

    // Each subselect gets their own TranslateCtx.
    let mut t_ctx_list = Vec::with_capacity(rest.len() + 1);
    t_ctx_list.push(TranslateCtx::new(
        program,
        schema,
        syms,
        first.table_references.joined_tables().len(),
        first.result_columns.len(),
    ));
    rest.iter().for_each(|(select, _)| {
        let t_ctx = TranslateCtx::new(
            program,
            schema,
            syms,
            select.table_references.joined_tables().len(),
            select.result_columns.len(),
        );
        t_ctx_list.push(t_ctx);
    });

    // Compound select operators have the same precedence and are left-associative.
    // If there is any remaining UNION operator on the right side of a given sub-SELECT,
    // all of the rows from the preceding UNION arms need to be deduplicated.
    // This is done by creating an ephemeral index and inserting all the rows from the left side of
    // the last UNION arm into it.
    // Then, as soon as there are no more UNION operators left, all the deduplicated rows from the
    // ephemeral index are emitted, and lastly the rows from the remaining sub-SELECTS are emitted
    // as is, as they don't require deduplication.
    let mut first_t_ctx = t_ctx_list.remove(0);
    let requires_union_deduplication = rest
        .iter()
        .any(|(_, operator)| operator == &ast::CompoundOperator::Union);
    if requires_union_deduplication {
        // appears BEFORE a UNION operator, so do not count those rows towards the LIMIT.
        first.limit = None;
    } else {
        // appears AFTER the last UNION operator, so count those rows towards the LIMIT.
        first_t_ctx.limit_ctx = limit_ctx;
    }

    let mut registers_subqery = None;
    let yield_reg = match first.query_destination {
        QueryDestination::CoroutineYield { yield_reg, .. } => {
            registers_subqery = Some(program.alloc_registers(first.result_columns.len()));
            first_t_ctx.reg_result_cols_start = registers_subqery.clone();
            Some(yield_reg)
        }
        _ => None,
    };

    let mut union_dedupe_index = if requires_union_deduplication {
        let dedupe_index = get_union_dedupe_index(program, &first);
        first.query_destination = QueryDestination::EphemeralIndex {
            cursor_id: dedupe_index.0,
            index: dedupe_index.1.clone(),
        };
        Some(dedupe_index)
    } else {
        None
    };

    // Emit the first SELECT
    emit_query(program, &mut first, &mut first_t_ctx)?;

    // Emit the remaining SELECTs. Any selects on the left side of a UNION must deduplicate their
    // results with the ephemeral index created above.
    while !t_ctx_list.is_empty() {
        let label_next_select = program.allocate_label();
        // If the LIMIT is reached in any subselect, jump to either:
        // a) the IfNot of the next subselect, or
        // b) the end of the program
        if let Some(limit_ctx) = limit_ctx {
            program.emit_insn(Insn::IfNot {
                reg: limit_ctx.reg_limit,
                target_pc: label_next_select,
                jump_if_null: true,
            });
        }
        let mut t_ctx = t_ctx_list.remove(0);
        let requires_union_deduplication = rest
            .iter()
            .any(|(_, operator)| operator == &ast::CompoundOperator::Union);
        let (mut select, operator) = rest.remove(0);
        if operator != ast::CompoundOperator::UnionAll && operator != ast::CompoundOperator::Union {
            crate::bail_parse_error!("unimplemented compound select operator: {:?}", operator);
        }

        if requires_union_deduplication {
            // Again: appears BEFORE a UNION operator, so do not count those rows towards the LIMIT.
            select.limit = None;
        } else {
            // appears AFTER the last UNION operator, so count those rows towards the LIMIT.
            t_ctx.limit_ctx = limit_ctx;
        }

        if requires_union_deduplication {
            select.query_destination = QueryDestination::EphemeralIndex {
                cursor_id: union_dedupe_index.as_ref().unwrap().0,
                index: union_dedupe_index.as_ref().unwrap().1.clone(),
            };
        } else if let Some((dedupe_cursor_id, dedupe_index)) = union_dedupe_index.take() {
            // When there are no more UNION operators left, all the deduplicated rows from the preceding union arms need to be emitted
            // as result rows.
            read_deduplicated_union_rows(
                program,
                dedupe_cursor_id,
                dedupe_index.as_ref(),
                limit_ctx,
                label_next_select,
                yield_reg.clone(),
            );
        }
        if matches!(
            select.query_destination,
            crate::translate::plan::QueryDestination::CoroutineYield { .. }
        ) {
            // Need to reuse the same registers when you are yielding
            t_ctx.reg_result_cols_start = registers_subqery.clone();
        }
        emit_query(program, &mut select, &mut t_ctx)?;
        program.preassign_label_to_next_insn(label_next_select);
    }

    if let Some((dedupe_cursor_id, dedupe_index)) = union_dedupe_index {
        let label_jump_over_dedupe = program.allocate_label();
        read_deduplicated_union_rows(
            program,
            dedupe_cursor_id,
            dedupe_index.as_ref(),
            limit_ctx,
            label_jump_over_dedupe,
            yield_reg,
        );
        program.preassign_label_to_next_insn(label_jump_over_dedupe);
    }

    program.epilogue(TransactionMode::Read);
    program.result_columns = first.result_columns;
    program.table_references.extend(first.table_references);

    Ok(())
}

/// Creates an ephemeral index that will be used to deduplicate the results of any sub-selects
/// that appear before the last UNION operator.
fn get_union_dedupe_index(
    program: &mut ProgramBuilder,
    first_select_in_compound: &SelectPlan,
) -> (usize, Arc<Index>) {
    let dedupe_index = Arc::new(Index {
        columns: first_select_in_compound
            .result_columns
            .iter()
            .map(|c| IndexColumn {
                name: c
                    .name(&first_select_in_compound.table_references)
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                collation: None, // FIXME: this should be inferred
            })
            .collect(),
        name: "union_dedupe".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: true,
        has_rowid: false,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(dedupe_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });
    (cursor_id, dedupe_index.clone())
}

/// Emits the bytecode for reading deduplicated rows from the ephemeral index created for UNION operators.
fn read_deduplicated_union_rows(
    program: &mut ProgramBuilder,
    dedupe_cursor_id: usize,
    dedupe_index: &Index,
    limit_ctx: Option<LimitCtx>,
    label_limit_reached: BranchOffset,
    yield_reg: Option<usize>,
) {
    let label_dedupe_next = program.allocate_label();
    let label_dedupe_loop_start = program.allocate_label();
    let dedupe_cols_start_reg = program.alloc_registers(dedupe_index.columns.len());
    program.emit_insn(Insn::Rewind {
        cursor_id: dedupe_cursor_id,
        pc_if_empty: label_dedupe_next,
    });
    program.preassign_label_to_next_insn(label_dedupe_loop_start);
    for col_idx in 0..dedupe_index.columns.len() {
        let start_reg = if let Some(yield_reg) = yield_reg {
            // Need to reuse the yield_reg for the column being emitted
            yield_reg + 1
        } else {
            dedupe_cols_start_reg
        };
        program.emit_insn(Insn::Column {
            cursor_id: dedupe_cursor_id,
            column: col_idx,
            dest: start_reg + col_idx,
        });
    }
    if let Some(yield_reg) = yield_reg {
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: BranchOffset::Offset(0),
        });
    } else {
        program.emit_insn(Insn::ResultRow {
            start_reg: dedupe_cols_start_reg,
            count: dedupe_index.columns.len(),
        });
    }

    if let Some(limit_ctx) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_limit_reached,
        })
    }
    program.preassign_label_to_next_insn(label_dedupe_next);
    program.emit_insn(Insn::Next {
        cursor_id: dedupe_cursor_id,
        pc_if_next: label_dedupe_loop_start,
    });
}

#[instrument(skip_all, level = Level::TRACE)]
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

#[instrument(skip_all, level = Level::TRACE)]
pub fn emit_query<'a>(
    program: &'a mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &'a mut TranslateCtx<'a>,
) -> Result<usize> {
    if !plan.values.is_empty() {
        let reg_result_cols_start = emit_values(program, &plan, &t_ctx.resolver)?;
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
        init_group_by(program, t_ctx, group_by, &plan)?;
    } else if !plan.aggregates.is_empty() {
        // Aggregate registers need to be NULLed at the start because the same registers might be reused on another invocation of a subquery,
        // and if they are not NULLed, the 2nd invocation of the same subquery will have values left over from the first invocation.
        t_ctx.reg_agg_start = Some(program.alloc_registers_and_init_w_null(plan.aggregates.len()));
    }

    init_distinct(program, plan);
    init_loop(
        program,
        t_ctx,
        &plan.table_references,
        &mut plan.aggregates,
        plan.group_by.as_ref(),
        OperationMode::SELECT,
    )?;

    if plan.is_simple_count() {
        emit_simple_count(program, t_ctx, plan)?;
        return Ok(t_ctx.reg_result_cols_start.unwrap());
    }

    for where_term in plan
        .where_clause
        .iter()
        .filter(|wt| wt.should_eval_before_loop(&plan.join_order))
    {
        let jump_target_when_true = program.allocate_label();
        let condition_metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_false: after_main_loop_label,
            jump_target_when_true,
        };
        translate_condition_expr(
            program,
            &plan.table_references,
            &where_term.expr,
            condition_metadata,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target_when_true);
    }

    // Set up main query execution loop
    open_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        &mut plan.where_clause,
    )?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    close_loop(program, t_ctx, &plan.table_references, &plan.join_order)?;

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

#[instrument(skip_all, level = Level::TRACE)]
fn emit_program_for_delete(
    program: &mut ProgramBuilder,
    mut plan: DeletePlan,
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
    )?;

    // Set up main query execution loop
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        &mut plan.where_clause,
    )?;

    emit_delete_insns(program, &mut t_ctx, &plan.table_references)?;

    // Clean up and close the main execution loop
    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
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

    if let Some(_) = table_reference.virtual_table() {
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
                        program.emit_insn(Insn::Column {
                            cursor_id: main_table_cursor_id,
                            column: column_index.pos_in_table,
                            dest: start_reg + reg_offset,
                        });
                    });
                program.emit_insn(Insn::RowId {
                    cursor_id: main_table_cursor_id,
                    dest: start_reg + num_regs - 1,
                });
                program.emit_insn(Insn::IdxDelete {
                    start_reg,
                    num_regs,
                    cursor_id: index_cursor_id,
                });
            }
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

#[instrument(skip_all, level = Level::TRACE)]
fn emit_program_for_update(
    program: &mut ProgramBuilder,
    mut plan: UpdatePlan,
    schema: &Schema,
    syms: &SymbolTable,
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

    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        None,
        OperationMode::UPDATE,
    )?;
    // Open indexes for update.
    let mut index_cursors = Vec::with_capacity(plan.indexes_to_update.len());
    for index in &plan.indexes_to_update {
        if let Some(index_cursor) = program.resolve_cursor_id_safe(&CursorKey::index(
            plan.table_references
                .joined_tables()
                .first()
                .unwrap()
                .internal_id,
            index.clone(),
        )) {
            // Don't reopen index if it was already opened as the iteration cursor for this update plan.
            let record_reg = program.alloc_register();
            index_cursors.push((index_cursor, record_reg));
            continue;
        }
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            name: index.name.clone(),
        });
        let record_reg = program.alloc_register();
        index_cursors.push((index_cursor, record_reg));
    }
    open_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
        &mut plan.where_clause,
    )?;
    emit_update_insns(&plan, &t_ctx, program, index_cursors)?;

    match plan.parse_schema {
        ParseSchema::None => {}
        ParseSchema::Reload => {
            program.emit_insn(crate::vdbe::insn::Insn::ParseSchema {
                db: usize::MAX, // TODO: This value is unused, change when we do something with it
                where_clause: None,
            });
        }
    }

    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &[JoinOrderMember::default()],
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    // Finalize program
    program.epilogue(TransactionMode::Write);
    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[instrument(skip_all, level = Level::TRACE)]
fn emit_update_insns(
    plan: &UpdatePlan,
    t_ctx: &TranslateCtx,
    program: &mut ProgramBuilder,
    index_cursors: Vec<(usize, usize)>,
) -> crate::Result<()> {
    let table_ref = plan.table_references.joined_tables().first().unwrap();
    let loop_labels = t_ctx.labels_main_loop.first().unwrap();
    let (cursor_id, index, is_virtual) = match &table_ref.op {
        Operation::Scan { index, .. } => (
            program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id)),
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
            &Search::RowidEq { .. } | Search::Seek { index: None, .. } => (
                program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id)),
                None,
                false,
            ),
            Search::Seek {
                index: Some(index), ..
            } => (
                program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id)),
                Some((
                    index.clone(),
                    program
                        .resolve_cursor_id(&CursorKey::index(table_ref.internal_id, index.clone())),
                )),
                false,
            ),
        },
    };

    for cond in plan
        .where_clause
        .iter()
        .filter(|c| c.should_eval_before_loop(&[JoinOrderMember::default()]))
    {
        let jump_target = program.allocate_label();
        let meta = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: jump_target,
            jump_target_when_false: t_ctx.label_main_loop_end.unwrap(),
        };
        translate_condition_expr(
            program,
            &plan.table_references,
            &cond.expr,
            meta,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target);
    }
    let beg = program.alloc_registers(
        table_ref.table.columns().len()
            + if is_virtual {
                2 // two args before the relevant columns for VUpdate
            } else {
                1 // rowid reg
            },
    );
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: beg,
    });

    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)

    let rowid_alias_index = {
        let rowid_alias_index = table_ref.columns().iter().position(|c| c.is_rowid_alias);
        if let Some(index) = rowid_alias_index {
            plan.set_clauses.iter().position(|(idx, _)| *idx == index)
        } else {
            None
        }
    };
    let rowid_set_clause_reg = if rowid_alias_index.is_some() {
        Some(program.alloc_register())
    } else {
        None
    };
    let has_user_provided_rowid = rowid_alias_index.is_some();

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

    for cond in plan
        .where_clause
        .iter()
        .filter(|c| c.should_eval_before_loop(&[JoinOrderMember::default()]))
    {
        let jump_target = program.allocate_label();
        let meta = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: jump_target,
            jump_target_when_false: loop_labels.next,
        };
        translate_condition_expr(
            program,
            &plan.table_references,
            &cond.expr,
            meta,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target);
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
                /*
                if let Some(check_constraint) = &table_column.check_constraint {
                    let mut expr = check_constraint.clone();
                    let description = expr.to_string();
                    rewrite_check_constraint(table_ref, &mut expr);
                    use crate::error::SQLITE_CONSTRAINT_CHECK;
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        &expr,
                        target_reg,
                        &t_ctx.resolver,
                    )?;
                    let label = program.allocate_label();
                    program.emit_insn(Insn::If {
                        reg: target_reg,
                        target_pc: label,
                        jump_if_null: true,
                    });
                    program.emit_halt_err(SQLITE_CONSTRAINT_CHECK, description);
                    program.preassign_label_to_next_insn(label);
                }
                */
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    target_reg,
                    &t_ctx.resolver,
                )?;
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
                program.emit_insn(Insn::Column {
                    cursor_id: *index
                        .as_ref()
                        .and_then(|(_, id)| {
                            if column_idx_in_index.is_some() {
                                Some(id)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(&cursor_id),
                    column: column_idx_in_index.unwrap_or(idx),
                    dest: target_reg,
                });
            }
        }
    }

    for (index, (idx_cursor_id, record_reg)) in plan.indexes_to_update.iter().zip(&index_cursors) {
        let num_cols = index.columns.len();
        // allocate scratch registers for the index columns plus rowid
        let idx_start_reg = program.alloc_registers(num_cols + 1);

        let rowid_reg = beg;
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
                accum.push_str(&table_ref.table.get_name());
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
                    program.emit_insn(Insn::Column {
                        cursor_id,
                        column: column_index.pos_in_table,
                        dest: start_reg + reg_offset,
                    });
                });

            program.emit_insn(Insn::RowId {
                cursor_id,
                dest: start_reg + num_regs - 1,
            });

            program.emit_insn(Insn::IdxDelete {
                start_reg,
                num_regs,
                cursor_id: idx_cursor_id,
            });

            // Insert new index key (filled further above with values from set_clauses)
            program.emit_insn(Insn::IdxInsert {
                cursor_id: idx_cursor_id,
                record_reg: record_reg,
                unpacked_start: Some(start),
                unpacked_count: Some((index.columns.len() + 1) as u16),
                flags: IdxInsertFlags::new(),
            });
        }

        program.emit_insn(Insn::Delete { cursor_id });

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid_set_clause_reg.unwrap_or(beg),
            record_reg,
            flag: 0,
            table_name: table_ref.identifier.clone(),
        });
    } else if let Some(_) = table_ref.virtual_table() {
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

use crate::ast::Expr;
use crate::translate::plan::JoinedTable;
fn rewrite_check_constraint(table: &JoinedTable, check_constraint_expr: &mut Expr) {
    use crate::translate::expr::walk_expr_mut;
    let _ = walk_expr_mut(
        check_constraint_expr,
        &mut |expr: &mut Expr| -> Result<()> {
            match expr {
                ast::Expr::Id(id) => {
                    let res = table
                        .columns()
                        .iter()
                        .enumerate()
                        .find(|(_i, col)| col.name.as_ref().map_or(false, |c| *c == id.0));
                    if let Some((i, col)) = res {
                        *expr = Expr::Column {
                            database: None, // TODO: support different databases
                            table: table.internal_id,
                            column: i,
                            is_rowid_alias: col.is_rowid_alias,
                        };
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        },
    );
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
