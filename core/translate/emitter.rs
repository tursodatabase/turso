// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tracing::{instrument, Level};
use turso_parser::ast::{self, Expr, Literal, TriggerEvent, TriggerTime};

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
use super::subquery::emit_from_clause_subqueries;
use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::function::Func;
use crate::schema::{BTreeTable, Column, Index, IndexColumn, Schema, Table, ROWID_SENTINEL};
use crate::translate::compound_select::emit_program_for_compound_select;
use crate::translate::expr::{
    bind_and_rewrite_expr, emit_returning_results, translate_expr_no_constant_opt, walk_expr_mut,
    BindingBehavior, NoConstantOptReason, WalkControl,
};
use crate::translate::fkeys::{
    build_index_affinity_string, emit_fk_child_update_counters,
    emit_fk_delete_parent_existence_checks, emit_guarded_fk_decrement,
    emit_parent_key_change_checks, open_read_index, open_read_table, stabilize_new_row_for_fk,
};
use crate::translate::plan::{
    DeletePlan, EvalAt, JoinedTable, Plan, QueryDestination, ResultSetColumn, Search,
};
use crate::translate::planner::ROWID_STRS;
use crate::translate::subquery::emit_non_from_clause_subquery;
use crate::translate::trigger_exec::{
    fire_trigger, get_relevant_triggers_type_and_time, has_relevant_triggers_type_only,
    TriggerContext,
};
use crate::translate::values::emit_values;
use crate::translate::window::{emit_window_results, init_window, WindowMetadata};
use crate::util::{exprs_are_equivalent, normalize_ident};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, RegisterOrLiteral};
use crate::vdbe::{insn::Insn, BranchOffset};
use crate::Connection;
use crate::{bail_parse_error, Result, SymbolTable};

pub struct Resolver<'a> {
    pub schema: &'a Schema,
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache_enabled: bool,
    pub expr_to_reg_cache: Vec<(std::borrow::Cow<'a, ast::Expr>, usize)>,
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
#[derive(Debug, Clone)]
pub struct HashCtx {
    pub match_reg: usize,
    pub hash_table_reg: usize,
    /// Label for hash join match processing (points to just after HashProbe instruction)
    /// Used by HashNext to jump back to process additional matches without re-probing
    pub match_found_label: BranchOffset,
    /// Label for advancing to the next hash match (points to HashNext instruction).
    /// When conditions fail within a hash join, they should jump here to try the next
    /// hash match, rather than jumping to the outer loop's next label.
    pub hash_next_label: BranchOffset,
    /// Starting register where payload columns are stored after HashProbe/HashNext.
    /// None if payload optimization is not used for this hash join.
    pub payload_start_reg: Option<usize>,
    /// Column indices from the build table that are stored in payload, in order.
    /// payload_start_reg + i contains the value for column payload_columns[i].
    pub payload_columns: Vec<usize>,
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
    /// Hash table contexts for hash joins, keyed by build table index.
    /// Supports multiple hash joins in chain patterns.
    pub hash_table_contexts: HashMap<usize, HashCtx>,
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
            hash_table_contexts: HashMap::new(),
            resolver: Resolver::new(schema, syms),
            non_aggregate_expressions: Vec::new(),
            cdc_cursor_id: None,
            meta_window: None,
        }
    }
}

#[derive(Debug, Clone)]
/// Update row source for UPDATE statements
/// `Normal` is the default mode, it will iterate either the table itself or an index on the table.
/// `PrebuiltEphemeralTable` is used when an ephemeral table containing the target rowids to update has
/// been built and it is being used for iteration.
pub enum UpdateRowSource {
    /// Iterate over the table itself or an index on the table
    Normal,
    /// Iterate over an ephemeral table containing the target rowids to update
    PrebuiltEphemeralTable {
        /// The cursor id of the ephemeral table that is being used to iterate the target rowids to update.
        ephemeral_table_cursor_id: usize,
        /// The table that is being updated.
        target_table: Arc<JoinedTable>,
    },
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE(UpdateRowSource),
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
pub fn emit_program_for_select(
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

    if !plan.values.is_empty() {
        let reg_result_cols_start = emit_values(program, plan, t_ctx)?;
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(reg_result_cols_start);
    }

    // Evaluate uncorrelated subqueries as early as possible, because even LIMIT can reference a subquery.
    for subquery in plan
        .non_from_clause_subqueries
        .iter_mut()
        .filter(|s| !s.has_been_evaluated())
    {
        let eval_at = subquery.get_eval_at(&plan.join_order)?;
        if eval_at != EvalAt::BeforeLoop {
            continue;
        }
        let plan = subquery.consume_plan(EvalAt::BeforeLoop);

        emit_non_from_clause_subquery(
            program,
            t_ctx,
            *plan,
            &subquery.query_type,
            subquery.correlated,
        )?;
    }

    // Emit FROM clause subqueries first so the results can be read in the main query loop.
    emit_from_clause_subqueries(program, t_ctx, &mut plan.table_references)?;

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
            plan.distinctness != Distinctness::NonDistinct,
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

    init_limit(program, t_ctx, &plan.limit, &plan.offset)?;

    init_loop(
        program,
        t_ctx,
        &plan.table_references,
        &mut plan.aggregates,
        plan.group_by.as_ref(),
        OperationMode::SELECT,
        &plan.where_clause,
        &plan.join_order,
        &mut plan.non_from_clause_subqueries,
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
        OperationMode::SELECT,
        &mut plan.non_from_clause_subqueries,
    )?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    close_loop(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        OperationMode::SELECT,
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

    init_limit(program, &mut t_ctx, &plan.limit, &None)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    let join_order = plan
        .table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: false,
        })
        .collect::<Vec<_>>();

    // Initialize cursors and other resources needed for query execution
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        None,
        OperationMode::DELETE,
        &plan.where_clause,
        &join_order,
        &mut [],
    )?;

    // If there's a rowset_plan, materialize rowids into a RowSet first and then iterate the RowSet
    // to delete the rows.
    if let Some(rowset_plan) = plan.rowset_plan.take() {
        let rowset_reg = plan
            .rowset_reg
            .expect("rowset_reg must be Some if rowset_plan is Some");

        // Initialize the RowSet register with NULL (RowSet will be created on first RowSetAdd)
        program.emit_insn(Insn::Null {
            dest: rowset_reg,
            dest_end: None,
        });

        // Execute the rowset SELECT plan to populate the rowset.
        program.incr_nesting();
        emit_program_for_select(program, resolver, rowset_plan)?;
        program.decr_nesting();

        // Close the read cursor(s) opened by the rowset plan before opening for writing
        let table_ref = plan.table_references.joined_tables().first().unwrap();
        let table_cursor_id_read =
            program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id));
        program.emit_insn(Insn::Close {
            cursor_id: table_cursor_id_read,
        });

        // Open the table cursor for writing
        let table_cursor_id = table_cursor_id_read;

        if let Some(btree_table) = table_ref.table.btree() {
            program.emit_insn(Insn::OpenWrite {
                cursor_id: table_cursor_id,
                root_page: RegisterOrLiteral::Literal(btree_table.root_page),
                db: table_ref.database_id,
            });

            // Open all indexes for writing (needed for DELETE)
            for index in resolver.schema.get_indices(table_ref.table.get_name()) {
                let index_cursor_id = program.alloc_cursor_index(
                    Some(CursorKey::index(table_ref.internal_id, index.clone())),
                    index,
                )?;
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: index_cursor_id,
                    root_page: RegisterOrLiteral::Literal(index.root_page),
                    db: table_ref.database_id,
                });
            }
        }

        // Now iterate over the RowSet and delete each rowid
        let rowset_loop_start = program.allocate_label();
        let rowset_loop_end = program.allocate_label();
        let rowid_reg = program.alloc_register();
        if table_ref.table.virtual_table().is_some() {
            // VUpdate requires a NULL second argument ("new rowid") for deletion
            let new_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: new_rowid_reg,
                dest_end: None,
            });
        }

        program.preassign_label_to_next_insn(rowset_loop_start);

        // Read next rowid from RowSet
        // Note: rowset_loop_end will be resolved later when we assign it
        program.emit_insn(Insn::RowSetRead {
            rowset_reg,
            pc_if_empty: rowset_loop_end,
            dest_reg: rowid_reg,
        });

        emit_delete_insns_when_triggers_present(
            connection,
            program,
            &mut t_ctx,
            &mut plan.table_references,
            &plan.result_columns,
            rowid_reg,
            table_cursor_id,
        )?;

        // Continue loop
        program.emit_insn(Insn::Goto {
            target_pc: rowset_loop_start,
        });

        // Assign the end label here, after all loop body code
        program.preassign_label_to_next_insn(rowset_loop_end);
    } else {
        // Normal DELETE path without RowSet

        // Set up main query execution loop
        open_loop(
            program,
            &mut t_ctx,
            &plan.table_references,
            &join_order,
            &plan.where_clause,
            None,
            OperationMode::DELETE,
            &mut [],
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
            &join_order,
            OperationMode::DELETE,
        )?;
    }
    program.preassign_label_to_next_insn(after_main_loop_label);
    // Finalize program
    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

pub fn emit_fk_child_decrement_on_delete(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    child_tbl: &BTreeTable,
    child_table_name: &str,
    child_cursor_id: usize,
    child_rowid_reg: usize,
) -> crate::Result<()> {
    for fk_ref in resolver.schema.resolved_fks_for_child(child_table_name)? {
        if !fk_ref.fk.deferred {
            continue;
        }
        // Fast path: if any FK column is NULL can't be a violation
        let null_skip = program.allocate_label();
        for cname in &fk_ref.child_cols {
            let (pos, col) = child_tbl.get_column(cname).unwrap();
            let src = if col.is_rowid_alias() {
                child_rowid_reg
            } else {
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: child_cursor_id,
                    column: pos,
                    dest: tmp,
                    default: None,
                });
                tmp
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: null_skip,
            });
        }

        if fk_ref.parent_uses_rowid {
            // Probe parent table by rowid
            let parent_tbl = resolver
                .schema
                .get_btree_table(&fk_ref.fk.parent_table)
                .expect("parent btree");
            let pcur = open_read_table(program, &parent_tbl);

            let (pos, col) = child_tbl.get_column(&fk_ref.child_cols[0]).unwrap();
            let val = if col.is_rowid_alias() {
                child_rowid_reg
            } else {
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: child_cursor_id,
                    column: pos,
                    dest: tmp,
                    default: None,
                });
                tmp
            };
            let tmpi = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val,
                dst_reg: tmpi,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: tmpi });

            // NotExists jumps when the parent key is missing, so we decrement there
            let missing = program.allocate_label();
            let done = program.allocate_label();

            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmpi,
                target_pc: missing,
            });

            // Parent FOUND, no decrement
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: done });

            // Parent MISSING, decrement is guarded by FkIfZero to avoid underflow
            program.preassign_label_to_next_insn(missing);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            emit_guarded_fk_decrement(program, done, true);
            program.preassign_label_to_next_insn(done);
        } else {
            // Probe parent unique index
            let parent_tbl = resolver
                .schema
                .get_btree_table(&fk_ref.fk.parent_table)
                .expect("parent btree");
            let idx = fk_ref.parent_unique_index.as_ref().expect("unique index");
            let icur = open_read_index(program, idx);

            // Build probe from current child row
            let n = fk_ref.child_cols.len();
            let probe = program.alloc_registers(n);
            for (i, cname) in fk_ref.child_cols.iter().enumerate() {
                let (pos, col) = child_tbl.get_column(cname).unwrap();
                let src = if col.is_rowid_alias() {
                    child_rowid_reg
                } else {
                    let r = program.alloc_register();
                    program.emit_insn(Insn::Column {
                        cursor_id: child_cursor_id,
                        column: pos,
                        dest: r,
                        default: None,
                    });
                    r
                };
                program.emit_insn(Insn::Copy {
                    src_reg: src,
                    dst_reg: probe + i,
                    extra_amount: 0,
                });
            }
            program.emit_insn(Insn::Affinity {
                start_reg: probe,
                count: std::num::NonZeroUsize::new(n).unwrap(),
                affinities: build_index_affinity_string(idx, &parent_tbl),
            });

            let ok = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: ok,
                record_reg: probe,
                num_regs: n,
            });
            program.emit_insn(Insn::Close { cursor_id: icur });
            emit_guarded_fk_decrement(program, ok, true);
            program.preassign_label_to_next_insn(ok);
            program.emit_insn(Insn::Close { cursor_id: icur });
        }
        program.preassign_label_to_next_insn(null_skip);
    }
    Ok(())
}

fn emit_delete_insns<'a>(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    table_references: &mut TableReferences,
    result_columns: &'a [super::plan::ResultSetColumn],
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
        Operation::IndexMethodQuery(_) => {
            panic!("access through IndexMethod is not supported for delete statements")
        }
        Operation::HashJoin(_) => {
            unreachable!("access through HashJoin is not supported for delete statements")
        }
    };
    let btree_table = unsafe { &*table_reference }.btree();
    let main_table_cursor_id = program.resolve_cursor_id(&CursorKey::table(internal_id));
    let has_returning = !result_columns.is_empty();
    let has_delete_triggers = if let Some(btree_table) = btree_table {
        has_relevant_triggers_type_only(
            t_ctx.resolver.schema,
            TriggerEvent::Delete,
            None,
            &btree_table,
        )
    } else {
        false
    };
    let cols_len = unsafe { &*table_reference }.columns().len();
    let (columns_start_reg, rowid_reg): (Option<usize>, usize) = {
        // Get rowid for RETURNING
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: main_table_cursor_id,
            dest: rowid_reg,
        });
        if unsafe { &*table_reference }.virtual_table().is_some() {
            // VUpdate requires a NULL second argument ("new rowid") for deletion
            let new_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: new_rowid_reg,
                dest_end: None,
            });
        }

        if !has_returning && !has_delete_triggers {
            (None, rowid_reg)
        } else {
            // Allocate registers for column values
            let columns_start_reg = program.alloc_registers(cols_len);

            // Read all column values from the row to be deleted
            for (i, _column) in unsafe { &*table_reference }.columns().iter().enumerate() {
                program.emit_column_or_rowid(main_table_cursor_id, i, columns_start_reg + i);
            }

            (Some(columns_start_reg), rowid_reg)
        }
    };

    // Get the index that is being used to iterate the deletion loop, if there is one.
    let iteration_index = unsafe { &*table_reference }.op.index();

    emit_delete_row_common(
        connection,
        program,
        t_ctx,
        table_references,
        result_columns,
        table_reference,
        rowid_reg,
        columns_start_reg,
        main_table_cursor_id,
        iteration_index,
        Some(cursor_id), // Use the cursor_id from the operation for virtual tables
    )?;

    // Delete from the iteration index after deleting from the main table
    if let Some(index) = iteration_index {
        let iteration_index_cursor =
            program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone()));
        program.emit_insn(Insn::Delete {
            cursor_id: iteration_index_cursor,
            table_name: index.name.clone(),
            is_part_of_update: false,
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

/// Common deletion logic shared between normal DELETE and RowSet-based DELETE.
///
/// Parameters:
/// - `rowid_reg`: Register containing the rowid of the row to delete
/// - `columns_start_reg`: Start register containing column values (already read)
/// - `skip_iteration_index`: If Some(index), skip deleting from this index (used when iterating over an index)
/// - `virtual_table_cursor_id`: If Some, use this cursor for virtual table deletion
#[allow(clippy::too_many_arguments)]
fn emit_delete_row_common(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    result_columns: &[super::plan::ResultSetColumn],
    table_reference: *const JoinedTable,
    rowid_reg: usize,
    columns_start_reg: Option<usize>, // must be provided when there are triggers or RETURNING
    main_table_cursor_id: usize,
    skip_iteration_index: Option<&Arc<crate::schema::Index>>,
    virtual_table_cursor_id: Option<usize>,
) -> Result<()> {
    let internal_id = unsafe { (*table_reference).internal_id };
    let table_name = unsafe { &*table_reference }.table.get_name();

    if connection.foreign_keys_enabled() {
        if let Some(table) = unsafe { &*table_reference }.btree() {
            if t_ctx
                .resolver
                .schema
                .any_resolved_fks_referencing(table_name)
            {
                emit_fk_delete_parent_existence_checks(
                    program,
                    &t_ctx.resolver,
                    table_name,
                    main_table_cursor_id,
                    rowid_reg,
                )?;
            }
            if t_ctx.resolver.schema.has_child_fks(table_name) {
                emit_fk_child_decrement_on_delete(
                    program,
                    &t_ctx.resolver,
                    &table,
                    table_name,
                    main_table_cursor_id,
                    rowid_reg,
                )?;
            }
        }
    }

    if unsafe { &*table_reference }.virtual_table().is_some() {
        let conflict_action = 0u16;
        let cursor_id = virtual_table_cursor_id.unwrap_or(main_table_cursor_id);

        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count: 2,
            start_reg: rowid_reg,
            conflict_action,
        });
    } else {
        // Delete from all indexes before deleting from the main table.
        let indexes = t_ctx.resolver.schema.get_indices(table_name);

        // Get indexes to delete from (skip the iteration index if specified)
        let indexes_to_delete = indexes
            .filter(|index| {
                skip_iteration_index
                    .as_ref()
                    .is_none_or(|skip_idx| !Arc::ptr_eq(skip_idx, index))
            })
            .map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            })
            .collect::<Vec<_>>();

        for (index, index_cursor_id) in indexes_to_delete {
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
            for (reg_offset, column_index) in index.columns.iter().enumerate() {
                emit_index_column_value_old_image(
                    program,
                    &t_ctx.resolver,
                    table_references,
                    connection,
                    main_table_cursor_id,
                    column_index,
                    start_reg + reg_offset,
                )?;
            }
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
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            // Emit RETURNING results using the values we just read
            emit_returning_results(
                program,
                table_references,
                result_columns,
                columns_start_reg,
                rowid_reg,
                &mut t_ctx.resolver,
            )?;
        }

        program.emit_insn(Insn::Delete {
            cursor_id: main_table_cursor_id,
            table_name: table_name.to_string(),
            is_part_of_update: false,
        });
    }

    Ok(())
}

/// Helper function to delete a row when we've already seeked to it (e.g., from a RowSet).
/// This is similar to emit_delete_insns but assumes the cursor is already positioned at the row.
fn emit_delete_insns_when_triggers_present(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    result_columns: &[super::plan::ResultSetColumn],
    rowid_reg: usize,
    main_table_cursor_id: usize,
) -> Result<()> {
    // Seek to the rowid and delete it
    let skip_not_found_label = program.allocate_label();

    // Skip if row with rowid pulled from the rowset does not exist in the table.
    program.emit_insn(Insn::NotExists {
        cursor: main_table_cursor_id,
        rowid_reg,
        target_pc: skip_not_found_label,
    });

    let table_reference: *const JoinedTable = table_references.joined_tables().first().unwrap();
    if unsafe { &*table_reference }
        .virtual_table()
        .is_some_and(|t| t.readonly())
    {
        return Err(crate::LimboError::ReadOnly);
    }
    let btree_table = unsafe { &*table_reference }.btree();
    let has_returning = !result_columns.is_empty();
    let has_delete_triggers = if let Some(btree_table) = btree_table {
        has_relevant_triggers_type_only(
            t_ctx.resolver.schema,
            TriggerEvent::Delete,
            None,
            &btree_table,
        )
    } else {
        false
    };
    let cols_len = unsafe { &*table_reference }.columns().len();

    let columns_start_reg = if !has_returning && !has_delete_triggers {
        None
    } else {
        let columns_start_reg = program.alloc_registers(cols_len);
        for (i, _column) in unsafe { &*table_reference }.columns().iter().enumerate() {
            program.emit_column_or_rowid(main_table_cursor_id, i, columns_start_reg + i);
        }
        Some(columns_start_reg)
    };

    let cols_len = unsafe { &*table_reference }.columns().len();

    // Fire BEFORE DELETE triggers
    if let Some(btree_table) = unsafe { &*table_reference }.btree() {
        let relevant_triggers = get_relevant_triggers_type_and_time(
            t_ctx.resolver.schema,
            TriggerEvent::Delete,
            TriggerTime::Before,
            None,
            &btree_table,
        );
        let has_relevant_triggers = relevant_triggers.clone().count() > 0;
        if has_relevant_triggers {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            let old_registers = (0..cols_len)
                .map(|i| columns_start_reg + i)
                .chain(std::iter::once(rowid_reg))
                .collect::<Vec<_>>();
            let trigger_ctx = TriggerContext::new(
                btree_table.clone(),
                None, // No NEW for DELETE
                Some(old_registers),
            );

            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx,
                    connection,
                )?;
            }
        }
    }

    // BEFORE DELETE Triggers may have altered the btree so we need to seek again.
    program.emit_insn(Insn::NotExists {
        cursor: main_table_cursor_id,
        rowid_reg,
        target_pc: skip_not_found_label,
    });

    emit_delete_row_common(
        connection,
        program,
        t_ctx,
        table_references,
        result_columns,
        table_reference,
        rowid_reg,
        columns_start_reg,
        main_table_cursor_id,
        None, // Don't skip any indexes when deleting from RowSet
        None, // Use main_table_cursor_id for virtual tables
    )?;

    // Fire AFTER DELETE triggers
    if let Some(btree_table) = unsafe { &*table_reference }.btree() {
        let relevant_triggers = get_relevant_triggers_type_and_time(
            t_ctx.resolver.schema,
            TriggerEvent::Delete,
            TriggerTime::After,
            None,
            &btree_table,
        );
        let has_relevant_triggers = relevant_triggers.clone().count() > 0;
        if has_relevant_triggers {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            let old_registers = (0..cols_len)
                .map(|i| columns_start_reg + i)
                .chain(std::iter::once(rowid_reg))
                .collect::<Vec<_>>();
            let trigger_ctx_after = TriggerContext::new(
                btree_table.clone(),
                None, // No NEW for DELETE
                Some(old_registers),
            );

            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx_after,
                    connection,
                )?;
            }
        }
    }

    program.preassign_label_to_next_insn(skip_not_found_label);

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

    init_limit(program, &mut t_ctx, &plan.limit, &plan.offset)?;

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
    let has_ephemeral_table = ephemeral_plan.is_some();

    let target_table = if let Some(ephemeral_plan) = ephemeral_plan {
        let table = ephemeral_plan
            .table_references
            .joined_tables()
            .first()
            .unwrap()
            .clone();
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: temp_cursor_id.unwrap(),
            is_table: true,
        });
        program.incr_nesting();
        emit_program_for_select(program, resolver, ephemeral_plan)?;
        program.decr_nesting();
        Arc::new(table)
    } else {
        Arc::new(
            plan.table_references
                .joined_tables()
                .first()
                .unwrap()
                .clone(),
        )
    };

    let mode = OperationMode::UPDATE(if has_ephemeral_table {
        UpdateRowSource::PrebuiltEphemeralTable {
            ephemeral_table_cursor_id: temp_cursor_id.expect(
                "ephemeral table cursor id is always allocated if has_ephemeral_table is true",
            ),
            target_table: target_table.clone(),
        }
    } else {
        UpdateRowSource::Normal
    });

    let join_order = plan
        .table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: false,
        })
        .collect::<Vec<_>>();

    // Initialize the main loop
    init_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        None,
        mode.clone(),
        &plan.where_clause,
        &join_order,
        &mut [],
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
            let cursor = program.alloc_cursor_index(None, index)?;
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
        &join_order,
        &plan.where_clause,
        temp_cursor_id,
        mode.clone(),
        &mut [],
    )?;

    let target_table_cursor_id =
        program.resolve_cursor_id(&CursorKey::table(target_table.internal_id));

    let iteration_cursor_id = if has_ephemeral_table {
        temp_cursor_id.unwrap()
    } else {
        target_table_cursor_id
    };

    // Emit update instructions
    emit_update_insns(
        connection,
        &mut plan.table_references,
        &plan.set_clauses,
        plan.cdc_update_alter_statement.as_deref(),
        &plan.indexes_to_update,
        plan.returning.as_ref(),
        plan.ephemeral_plan.as_ref(),
        &mut t_ctx,
        program,
        index_cursors,
        iteration_cursor_id,
        target_table_cursor_id,
        target_table,
    )?;

    // Close the main loop
    close_loop(
        program,
        &mut t_ctx,
        &plan.table_references,
        &join_order,
        mode.clone(),
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);
    after(program);

    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references.extend(plan.table_references);
    Ok(())
}

/// Helper function to evaluate SET expressions and read column values for UPDATE.
/// This is invoked once for every UPDATE, but will be invoked again if there are
/// any BEFORE UPDATE triggers that fired, because the triggers may have modified the row,
/// in which case the previously read values are stale.
#[allow(clippy::too_many_arguments)]
fn emit_update_column_values<'a>(
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    set_clauses: &[(usize, Box<ast::Expr>)],
    cdc_update_alter_statement: Option<&str>,
    target_table: &Arc<JoinedTable>,
    target_table_cursor_id: usize,
    start: usize,
    col_len: usize,
    table_name: &str,
    has_direct_rowid_update: bool,
    has_user_provided_rowid: bool,
    rowid_set_clause_reg: Option<usize>,
    is_virtual: bool,
    index: &Option<(Arc<Index>, usize)>,
    cdc_updates_register: Option<usize>,
    t_ctx: &mut TranslateCtx<'a>,
    skip_set_clauses: bool,
) -> crate::Result<()> {
    if has_direct_rowid_update {
        if let Some((_, expr)) = set_clauses.iter().find(|(i, _)| *i == ROWID_SENTINEL) {
            if !skip_set_clauses {
                let rowid_set_clause_reg = rowid_set_clause_reg.unwrap();
                translate_expr(
                    program,
                    Some(table_references),
                    expr,
                    rowid_set_clause_reg,
                    &t_ctx.resolver,
                )?;
                program.emit_insn(Insn::MustBeInt {
                    reg: rowid_set_clause_reg,
                });
            }
        }
    }
    for (idx, table_column) in target_table.table.columns().iter().enumerate() {
        let target_reg = start + idx;
        if let Some((col_idx, expr)) = set_clauses.iter().find(|(i, _)| *i == idx) {
            if !skip_set_clauses {
                // Skip if this is the sentinel value
                if *col_idx == ROWID_SENTINEL {
                    continue;
                }
                if has_user_provided_rowid
                    && (table_column.primary_key() || table_column.is_rowid_alias())
                    && !is_virtual
                {
                    let rowid_set_clause_reg = rowid_set_clause_reg.unwrap();
                    translate_expr(
                        program,
                        Some(table_references),
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
                        Some(table_references),
                        expr,
                        target_reg,
                        &t_ctx.resolver,
                    )?;
                    if table_column.notnull() {
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
                    if let Some(ddl_query_for_cdc_update) = cdc_update_alter_statement {
                        if table_column.name.as_deref() == Some("sql") {
                            program.emit_string8(ddl_query_for_cdc_update.to_string(), value_reg);
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
            }
        } else {
            // Column is not being updated, read it from the table
            let column_idx_in_index = index.as_ref().and_then(|(idx, _)| {
                idx.columns
                    .iter()
                    .position(|c| Some(&c.name) == table_column.name.as_ref())
            });

            // don't emit null for pkey of virtual tables. they require first two args
            // before the 'record' to be explicitly non-null
            if table_column.is_rowid_alias() && !is_virtual {
                program.emit_null(target_reg, None);
            } else if is_virtual {
                program.emit_insn(Insn::VColumn {
                    cursor_id: target_table_cursor_id,
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
                    .unwrap_or(&target_table_cursor_id);
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
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
#[allow(clippy::too_many_arguments)]
/// Emits the instructions for the UPDATE loop.
///
/// `iteration_cursor_id` is the cursor id of the table that is being iterated over. This can be either the table itself, an index, or an ephemeral table (see [crate::translate::plan::UpdatePlan]).
///
/// `target_table_cursor_id` is the cursor id of the table that is being updated.
///
/// `target_table` is the table that is being updated.
#[allow(clippy::too_many_arguments)]
fn emit_update_insns<'a>(
    connection: &Arc<Connection>,
    table_references: &mut TableReferences,
    set_clauses: &[(usize, Box<ast::Expr>)],
    cdc_update_alter_statement: Option<&str>,
    indexes_to_update: &[Arc<Index>],
    returning: Option<&'a Vec<ResultSetColumn>>,
    ephemeral_plan: Option<&SelectPlan>,
    t_ctx: &mut TranslateCtx<'a>,
    program: &mut ProgramBuilder,
    index_cursors: Vec<(usize, usize)>,
    iteration_cursor_id: usize,
    target_table_cursor_id: usize,
    target_table: Arc<JoinedTable>,
) -> crate::Result<()> {
    let internal_id = target_table.internal_id;
    let loop_labels = t_ctx.labels_main_loop.first().unwrap();
    let source_table = table_references.joined_tables().first().unwrap();
    let (index, is_virtual) = match &source_table.op {
        Operation::Scan(Scan::BTreeTable { index, .. }) => (
            index.as_ref().map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            }),
            false,
        ),
        Operation::Scan(_) => (None, target_table.virtual_table().is_some()),
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
        Operation::IndexMethodQuery(_) => {
            panic!("access through IndexMethod is not supported for update operations")
        }
        Operation::HashJoin(_) => {
            unreachable!("access through HashJoin is not supported for update operations")
        }
    };

    let beg = program.alloc_registers(
        target_table.table.columns().len()
            + if is_virtual {
                2 // two args before the relevant columns for VUpdate
            } else {
                1 // rowid reg
            },
    );
    program.emit_insn(Insn::RowId {
        cursor_id: iteration_cursor_id,
        dest: beg,
    });

    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)
    let rowid_alias_index = target_table
        .table
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias());

    let has_direct_rowid_update = set_clauses.iter().any(|(idx, _)| *idx == ROWID_SENTINEL);

    let has_user_provided_rowid = if let Some(index) = rowid_alias_index {
        set_clauses.iter().any(|(idx, _)| *idx == index)
    } else {
        has_direct_rowid_update
    };

    let rowid_set_clause_reg = if has_user_provided_rowid {
        Some(program.alloc_register())
    } else {
        None
    };

    let not_exists_check_required =
        has_user_provided_rowid || iteration_cursor_id != target_table_cursor_id;

    let check_rowid_not_exists_label = if not_exists_check_required {
        Some(program.allocate_label())
    } else {
        None
    };

    if not_exists_check_required {
        program.emit_insn(Insn::NotExists {
            cursor: target_table_cursor_id,
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
    let col_len = target_table.table.columns().len();

    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.

    // we allocate 2C registers for "updates" as the structure of this column for CDC table is following:
    // [C boolean values where true set for changed columns] [C values with updates where NULL is set for not-changed columns]
    let cdc_updates_register = if program.capture_data_changes_mode().has_updates() {
        Some(program.alloc_registers(2 * col_len))
    } else {
        None
    };
    let table_name = target_table.table.get_name();

    let start = if is_virtual { beg + 2 } else { beg + 1 };

    let skip_set_clauses = false;

    emit_update_column_values(
        program,
        table_references,
        set_clauses,
        cdc_update_alter_statement,
        &target_table,
        target_table_cursor_id,
        start,
        col_len,
        table_name,
        has_direct_rowid_update,
        has_user_provided_rowid,
        rowid_set_clause_reg,
        is_virtual,
        &index,
        cdc_updates_register,
        t_ctx,
        skip_set_clauses,
    )?;

    // Fire BEFORE UPDATE triggers and preserve old_registers for AFTER triggers
    let preserved_old_registers: Option<Vec<usize>> =
        if let Some(btree_table) = target_table.table.btree() {
            let updated_column_indices: std::collections::HashSet<usize> =
                set_clauses.iter().map(|(col_idx, _)| *col_idx).collect();
            let relevant_before_update_triggers = get_relevant_triggers_type_and_time(
                t_ctx.resolver.schema,
                TriggerEvent::Update,
                TriggerTime::Before,
                Some(updated_column_indices.clone()),
                &btree_table,
            );
            // Read OLD row values for trigger context
            let old_registers: Vec<usize> = (0..col_len)
                .map(|i| {
                    let reg = program.alloc_register();
                    program.emit_column_or_rowid(target_table_cursor_id, i, reg);
                    reg
                })
                .chain(std::iter::once(beg))
                .collect();
            let has_relevant_triggers = relevant_before_update_triggers.clone().count() > 0;
            if !has_relevant_triggers {
                Some(old_registers)
            } else {
                // NEW row values are already in 'start' registers
                let new_registers = (0..col_len)
                    .map(|i| start + i)
                    .chain(std::iter::once(beg))
                    .collect();

                let trigger_ctx = TriggerContext::new(
                    btree_table.clone(),
                    Some(new_registers),
                    Some(old_registers.clone()), // Clone for AFTER trigger
                );

                for trigger in relevant_before_update_triggers {
                    fire_trigger(
                        program,
                        &mut t_ctx.resolver,
                        trigger,
                        &trigger_ctx,
                        connection,
                    )?;
                }

                // BEFORE UPDATE Triggers may have altered the btree so we need to seek again.
                program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: beg,
                target_pc: check_rowid_not_exists_label.expect(
                    "check_rowid_not_exists_label must be set if there are BEFORE UPDATE triggers",
                ),
            });

                let has_relevant_after_triggers = get_relevant_triggers_type_and_time(
                    t_ctx.resolver.schema,
                    TriggerEvent::Update,
                    TriggerTime::After,
                    Some(updated_column_indices),
                    &btree_table,
                )
                .clone()
                .count()
                    > 0;
                if has_relevant_after_triggers {
                    // Preserve pseudo-row 'OLD' for AFTER triggers by copying to new registers
                    // (since registers might be overwritten during trigger execution)
                    let preserved: Vec<usize> = old_registers
                        .iter()
                        .map(|old_reg| {
                            let preserved_reg = program.alloc_register();
                            program.emit_insn(Insn::Copy {
                                src_reg: *old_reg,
                                dst_reg: preserved_reg,
                                extra_amount: 0,
                            });
                            preserved_reg
                        })
                        .collect();
                    Some(preserved)
                } else {
                    Some(old_registers)
                }
            }
        } else {
            None
        };

    // If BEFORE UPDATE triggers fired, they may have modified the row being updated.
    // According to the SQLite documentation, the behavior in these cases is undefined:
    // https://sqlite.org/lang_createtrigger.html
    // However, based on fuzz testing and observations, the logic seems to be:
    // The values that are NOT referred to in SET clauses will be evaluated again,
    // and values in SET clauses are evaluated using the old values.
    // sqlite> create table t(c0,c1,c2);
    // sqlite> create trigger tu before update on t begin update t set c1=666, c2=666; end;
    // sqlite> insert into t values (1,1,1);
    // sqlite> update t set c0 = c1+1;
    // sqlite> select * from t;
    // 2|666|666
    if target_table.table.btree().is_some() {
        let before_update_triggers_fired = preserved_old_registers.is_some();
        let skip_set_clauses = true;
        if before_update_triggers_fired {
            emit_update_column_values(
                program,
                table_references,
                set_clauses,
                cdc_update_alter_statement,
                &target_table,
                target_table_cursor_id,
                start,
                col_len,
                table_name,
                has_direct_rowid_update,
                has_user_provided_rowid,
                rowid_set_clause_reg,
                is_virtual,
                &index,
                cdc_updates_register,
                t_ctx,
                skip_set_clauses,
            )?;
        }
    }

    if connection.foreign_keys_enabled() {
        let rowid_new_reg = rowid_set_clause_reg.unwrap_or(beg);
        if let Some(table_btree) = target_table.table.btree() {
            stabilize_new_row_for_fk(
                program,
                &table_btree,
                set_clauses,
                target_table_cursor_id,
                start,
                rowid_new_reg,
            )?;
            if t_ctx.resolver.schema.has_child_fks(table_name) {
                // Child-side checks:
                // this ensures updated row still satisfies child FKs that point OUT from this table
                emit_fk_child_update_counters(
                    program,
                    &t_ctx.resolver,
                    &table_btree,
                    table_name,
                    target_table_cursor_id,
                    start,
                    rowid_new_reg,
                    &set_clauses
                        .iter()
                        .map(|(i, _)| *i)
                        .collect::<std::collections::HashSet<_>>(),
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
                emit_parent_key_change_checks(
                    program,
                    &t_ctx.resolver,
                    &table_btree,
                    indexes_to_update.iter(),
                    target_table_cursor_id,
                    beg,
                    start,
                    rowid_new_reg,
                    rowid_set_clause_reg,
                    set_clauses,
                )?;
            }
        }
    }

    for (index, (idx_cursor_id, record_reg)) in indexes_to_update.iter().zip(&index_cursors) {
        // We need to know whether or not the OLD values satisfied the predicate on the
        // partial index, so we can know whether or not to delete the old index entry,
        // as well as whether or not the NEW values satisfy the predicate, to determine whether
        // or not to insert a new index entry for a partial index
        let (old_satisfies_where, new_satisfies_where) = if index.where_clause.is_some() {
            // This means that we need to bind the column references to a copy of the index Expr,
            // so we can emit Insn::Column instructions and refer to the old values.
            let where_clause = index
                .bind_where_expr(Some(table_references), connection)
                .expect("where clause to exist");
            let old_satisfied_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                Some(table_references),
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
                target_table.table.columns(),
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
            emit_index_column_value_old_image(
                program,
                &t_ctx.resolver,
                table_references,
                connection,
                target_table_cursor_id,
                column_index,
                delete_start_reg + reg_offset,
            )?;
        }
        program.emit_insn(Insn::RowId {
            cursor_id: target_table_cursor_id,
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
            emit_index_column_value_new_image(
                program,
                &t_ctx.resolver,
                target_table.table.columns(),
                start,
                rowid_reg,
                col,
                idx_start_reg + i,
            )?;
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
                    if ic.expr.is_some() {
                        Affinity::Blob.aff_mask()
                    } else {
                        target_table.table.columns()[ic.pos_in_table]
                            .affinity()
                            .aff_mask()
                    }
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

    if let Some(btree_table) = target_table.table.btree() {
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
                cursor: target_table_cursor_id,
                rowid_reg: target_reg,
                target_pc: record_label,
            });

            let description = if let Some(idx) = rowid_alias_index {
                String::from(table_name)
                    + "."
                    + target_table
                        .table
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

        let affinity_str = target_table
            .table
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

        if not_exists_check_required {
            program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
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
                    cursor_id: target_table_cursor_id,
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
                target_table.table.columns(),
                target_table_cursor_id,
                cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set"),
            ))
        } else {
            None
        };

        // If we are updating the rowid, we cannot rely on overwrite on the
        // Insert instruction to update the cell. We need to first delete the current cell
        // and later insert the updated record
        if not_exists_check_required {
            program.emit_insn(Insn::Delete {
                cursor_id: target_table_cursor_id,
                table_name: table_name.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: target_table_cursor_id,
            key_reg: rowid_set_clause_reg.unwrap_or(beg),
            record_reg,
            flag: if not_exists_check_required {
                // The previous Insn::NotExists and Insn::Delete seek to the old rowid,
                // so to insert a new user-provided rowid, we need to seek to the correct place.
                InsertFlags::new().require_seek().update_rowid_change()
            } else {
                InsertFlags::new()
            },
            table_name: target_table.identifier.clone(),
        });

        // Fire AFTER UPDATE triggers
        if let Some(btree_table) = target_table.table.btree() {
            let updated_column_indices: std::collections::HashSet<usize> =
                set_clauses.iter().map(|(col_idx, _)| *col_idx).collect();
            let relevant_triggers = get_relevant_triggers_type_and_time(
                t_ctx.resolver.schema,
                TriggerEvent::Update,
                TriggerTime::After,
                Some(updated_column_indices),
                &btree_table,
            );
            let has_relevant_triggers = relevant_triggers.clone().count() > 0;
            if has_relevant_triggers {
                let new_rowid_reg = rowid_set_clause_reg.unwrap_or(beg);
                let new_registers_after = (0..col_len)
                    .map(|i| start + i)
                    .chain(std::iter::once(new_rowid_reg))
                    .collect();

                // Use preserved OLD registers from BEFORE trigger
                let old_registers_after = preserved_old_registers;

                let trigger_ctx_after = TriggerContext::new(
                    btree_table.clone(),
                    Some(new_registers_after),
                    old_registers_after, // OLD values preserved from BEFORE trigger
                );

                for trigger in relevant_triggers {
                    fire_trigger(
                        program,
                        &mut t_ctx.resolver,
                        trigger,
                        &trigger_ctx_after,
                        connection,
                    )?;
                }
            }
        }

        // Emit RETURNING results if specified
        if let Some(returning_columns) = &returning {
            if !returning_columns.is_empty() {
                emit_returning_results(
                    program,
                    table_references,
                    returning_columns,
                    start,
                    rowid_set_clause_reg.unwrap_or(beg),
                    &mut t_ctx.resolver,
                )?;
            }
        }

        // create full CDC record after update if necessary
        let cdc_after_reg = if program.capture_data_changes_mode().has_after() {
            Some(emit_cdc_patch_record(
                program,
                &target_table.table,
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
                    OperationMode::UPDATE(if ephemeral_plan.is_some() {
                        UpdateRowSource::PrebuiltEphemeralTable {
                            ephemeral_table_cursor_id: iteration_cursor_id,
                            target_table: target_table.clone(),
                        }
                    } else {
                        UpdateRowSource::Normal
                    }),
                    cdc_cursor_id,
                    cdc_rowid_before_reg,
                    cdc_before_reg,
                    cdc_after_reg,
                    cdc_updates_record,
                    table_name,
                )?;
            }
        }
    } else if target_table.virtual_table().is_some() {
        let arg_count = col_len + 2;
        program.emit_insn(Insn::VUpdate {
            cursor_id: target_table_cursor_id,
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
    let rowid_alias_position = columns.iter().position(|x| x.is_rowid_alias());
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
        if column.is_rowid_alias() {
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
        OperationMode::UPDATE { .. } | OperationMode::SELECT => 0,
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
) -> Result<()> {
    if t_ctx.limit_ctx.is_none() && limit.is_some() {
        t_ctx.limit_ctx = Some(LimitCtx::new(program));
    }
    let Some(limit_ctx) = &t_ctx.limit_ctx else {
        return Ok(());
    };

    if limit_ctx.initialize_counter {
        if let Some(expr) = limit {
            match expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => {
                    if let Ok(value) = n.parse::<i64>() {
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::Integer {
                            value,
                            dest: limit_ctx.reg_limit,
                        });
                    } else {
                        program.emit_insn(Insn::Real {
                            value: n.parse::<f64>().unwrap(),
                            dest: limit_ctx.reg_limit,
                        });
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::MustBeInt {
                            reg: limit_ctx.reg_limit,
                        });
                    }
                }
                _ => {
                    let r = limit_ctx.reg_limit;

                    _ = translate_expr(program, None, expr, r, &t_ctx.resolver)?;
                    program.emit_insn(Insn::MustBeInt { reg: r });
                }
            }
        }
    }

    if t_ctx.reg_offset.is_none() {
        if let Some(expr) = offset {
            let offset_reg = program.alloc_register();
            t_ctx.reg_offset = Some(offset_reg);
            match expr.as_ref() {
                Expr::Literal(Literal::Numeric(n)) => {
                    if let Ok(value) = n.parse::<i64>() {
                        program.emit_insn(Insn::Integer {
                            value,
                            dest: offset_reg,
                        });
                    } else {
                        let value = n.parse::<f64>()?;
                        program.emit_insn(Insn::Real {
                            value,
                            dest: limit_ctx.reg_limit,
                        });
                        program.emit_insn(Insn::MustBeInt {
                            reg: limit_ctx.reg_limit,
                        });
                    }
                }
                _ => {
                    _ = translate_expr(program, None, expr, offset_reg, &t_ctx.resolver)?;
                }
            }
            program.add_comment(program.offset(), "OFFSET counter");
            program.emit_insn(Insn::MustBeInt { reg: offset_reg });

            let combined_reg = program.alloc_register();
            t_ctx.reg_limit_offset_sum = Some(combined_reg);
            program.add_comment(program.offset(), "OFFSET + LIMIT");
            program.emit_insn(Insn::OffsetLimit {
                limit_reg: limit_ctx.reg_limit,
                offset_reg,
                combined_reg,
            });
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

    Ok(())
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
                    if c.is_rowid_alias() {
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
                    if c.is_rowid_alias() {
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

/// Emit code to load the value of an IndexColumn from the OLD image of the row being updated.
/// Handling expression indexes and regular columns
fn emit_index_column_value_old_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &mut TableReferences,
    connection: &Arc<Connection>,
    table_cursor_id: usize,
    idx_col: &IndexColumn,
    dest_reg: usize,
) -> Result<()> {
    if let Some(expr) = &idx_col.expr {
        let mut expr = expr.as_ref().clone();
        bind_and_rewrite_expr(
            &mut expr,
            Some(table_references),
            None,
            connection,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        translate_expr_no_constant_opt(
            program,
            Some(table_references),
            &expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    } else {
        program.emit_column_or_rowid(table_cursor_id, idx_col.pos_in_table, dest_reg);
    }
    Ok(())
}

/// Emit code to load the value of an IndexColumn from the NEW image of the row being updated.
/// Handling expression indexes and regular columns
fn emit_index_column_value_new_image(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    columns_start_reg: usize,
    rowid_reg: usize,
    idx_col: &IndexColumn,
    dest_reg: usize,
) -> Result<()> {
    if let Some(expr) = &idx_col.expr {
        let mut expr = expr.as_ref().clone();
        rewrite_where_for_update_registers(&mut expr, columns, columns_start_reg, rowid_reg)?;
        translate_expr_no_constant_opt(
            program,
            None,
            &expr,
            dest_reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    } else {
        let col_in_table = columns
            .get(idx_col.pos_in_table)
            .expect("column index out of bounds");
        let src_reg = if col_in_table.is_rowid_alias() {
            rowid_reg
        } else {
            columns_start_reg + idx_col.pos_in_table
        };
        program.emit_insn(Insn::Copy {
            src_reg,
            dst_reg: dest_reg,
            extra_amount: 0,
        });
    }
    Ok(())
}
