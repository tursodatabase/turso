//! Runtime destination preparation for resolved HIR queries.

use std::ops::ControlFlow;
use turso_parser::ast::{CompoundOperator, Distinctness, Literal, SortOrder};

use crate::translate::collate::CollationSeq;
use crate::{
    emit_explain,
    function::{AccumulatorFunc, AggFunc},
    schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, PseudoCursorType,
        Table, Type,
    },
    sync::Arc,
    translate::{
        aggregation::{
            hir_aggregate_function, translate_hir_aggregation_step, HirAggregateDistinct,
        },
        eqp::{EqpCompoundOp, EqpDetail, EqpSortMethod},
        expr::ConditionMetadata,
        main_loop::{
            emit_autoindex, emit_in_seek_advance, emit_in_seek_start, open_in_seek_values_cursor,
            AutoIndexBuild, InSeekLoop, LeftJoinMetadata, SeekEmitter, SeekExpressionLowering,
        },
        optimizer::{HirBtreeOperation, HirInSeekSource, HirVirtualTableOperation},
        order_by::{custom_type_comparator_from_type_fact, sorter_insert},
        plan::{
            self, EphemeralRowidMode, HirCteMaterialization, HirSeekDef, IterationDirection,
            QueryDestination,
        },
        recursive_cte::{
            emit_recursive_runtime, RecursivePhase, RecursiveRuntimeLimit, RecursiveRuntimeOrder,
            RecursiveRuntimeSpec,
        },
        result_row::emit_columns_to_destination,
        semantic::hir::{self, HirDocument, QueryId, SourceId, SubqueryExpr},
        semantic_to_plan::{
            BtreeTableLookup, HirPlan, HirPlannedLoop, HirQueryBlockPlan, HirSourceAccess,
        },
    },
    types::KeyInfo,
    util::parse_numeric_literal,
    vdbe::{
        affinity::Affinity,
        builder::{
            CursorType, MaterializedCteInfo, ProgramBuilder, SourceBinding, SubqueryBinding,
        },
        insn::{HashDistinctData, Insn, SorterOpenData},
        BranchOffset, CursorID,
    },
    LimboError, Numeric, Result, Value,
};

/// Physical destination and execution facts for one expression subquery.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct PreparedSubquery {
    pub(crate) query: QueryId,
    pub(crate) destination: QueryDestination,
    pub(crate) correlated: bool,
}

struct DestinationBinding {
    destination: QueryDestination,
    binding: SubqueryBinding,
}

#[derive(Clone, Copy)]
struct QueryLimitRegisters {
    limit: usize,
    offset: Option<usize>,
}

struct HirDistinctOutput {
    hash_table: usize,
    collations: Vec<CollationSeq>,
}

#[derive(Clone, Copy)]
enum HirSortKeySource {
    Output(usize),
    EncodedOutput(usize),
    Expression,
}

struct HirSorterOutput<'a> {
    cursor: CursorID,
    record: usize,
    row: usize,
    column_count: usize,
    order_by: &'a [hir::OrderTerm],
    key_sources: Vec<HirSortKeySource>,
    output_columns: Vec<usize>,
}

enum HirRowTarget<'a> {
    Direct {
        destination: &'a QueryDestination,
        limit: Option<QueryLimitRegisters>,
        done: BranchOffset,
    },
    Sorter(&'a HirSorterOutput<'a>),
}

struct HirRowOutput<'a> {
    target: HirRowTarget<'a>,
    distinct: Option<&'a HirDistinctOutput>,
}

struct HirAggregateRuntime<'a> {
    call: &'a hir::FunctionCall,
    accumulator: usize,
    distinct: Option<HirAggregateDistinct>,
    percentile_fraction: Option<usize>,
}

struct HirAggregateState<'a> {
    aggregates: Vec<HirAggregateRuntime<'a>>,
    bare_columns: Vec<hir::ColumnRef>,
    bare_rowids: Vec<SourceId>,
}

enum HirLoopBody<'a> {
    Rows(&'a HirRowOutput<'a>),
    UngroupedAggregate(&'a HirAggregateState<'a>),
    Group(&'a HirGroupRuntime<'a>),
}

struct HirAggregateCapture {
    source: SourceId,
    columns_start: usize,
    columns: Vec<usize>,
    rowid: Option<usize>,
}

struct HirGroupRuntime<'a> {
    grouping: &'a hir::Grouping,
    aggregates: HirAggregateState<'a>,
    rows: HirGroupRows,
    inputs: Vec<HirGroupInput>,
    sources: Vec<HirGroupSource>,
    state: HirGroupState,
}

enum HirGroupRows {
    Sorter(HirGroupSorter),
    InputOrdered,
}

struct HirGroupSorter {
    cursor: CursorID,
    pseudo_cursor: CursorID,
    record: usize,
    inputs_start: usize,
}

struct HirGroupState {
    output_group: BranchOffset,
    output_done: BranchOffset,
    finalize_group: BranchOffset,
    clear_group: BranchOffset,
    group_end: BranchOffset,
    output_return: usize,
    clear_return: usize,
    data_in_group: usize,
    previous_keys: usize,
    current_keys: usize,
}

enum HirGroupInput {
    Key(usize),
    Column {
        reference: hir::ColumnRef,
        row_register: usize,
        group_register: usize,
    },
    RowId {
        source: SourceId,
        row_register: usize,
        group_register: usize,
    },
}

struct HirGroupSource {
    source: SourceId,
    row_start: usize,
    group_start: usize,
    rowid: Option<HirGroupRowId>,
}

struct HirGroupRowId {
    row_register: usize,
    group_register: usize,
}

#[derive(Clone, Copy)]
enum HirGroupSourceValues {
    Row,
    Group,
}

impl HirRowOutput<'_> {
    fn needs_values(&self) -> bool {
        self.distinct.is_some()
            || match self.target {
                HirRowTarget::Direct { destination, .. } => {
                    !matches!(destination, QueryDestination::ExistsSubqueryResult { .. })
                }
                HirRowTarget::Sorter(_) => true,
            }
    }
}

fn open_hir_in_seek_source_cursor(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    index: Option<&Arc<Index>>,
    source: &HirInSeekSource,
) -> Result<CursorID> {
    match source {
        HirInSeekSource::Values { values, affinity } => open_in_seek_values_cursor(
            program,
            index,
            values,
            *affinity,
            |program, expression, target| {
                super::expr::translate_expr_no_constant_opt(program, document, expression, target)
                    .map(|_| ())
            },
        ),
        HirInSeekSource::Query { query } => {
            let Some(SubqueryBinding::InIndex { cursor }) = program.subquery_binding(*query) else {
                return Err(LimboError::InternalError(format!(
                    "HIR IN seek query {query} has no index runtime binding"
                )));
            };
            Ok(cursor)
        }
    }
}

struct HirSeekExpressionLowering<'a> {
    document: &'a HirDocument,
    source: &'a hir::Source,
}

impl SeekExpressionLowering<hir::Expr> for HirSeekExpressionLowering<'_> {
    fn emit_expression(
        &mut self,
        program: &mut ProgramBuilder,
        expression: &hir::Expr,
        target: usize,
    ) -> Result<()> {
        super::expr::translate_expr_no_constant_opt(program, self.document, expression, target)?;
        Ok(())
    }

    fn is_nonnull(&self, expression: &hir::Expr) -> bool {
        matches!(
            expression,
            hir::Expr::Literal(
                Literal::Numeric(_)
                    | Literal::String(_)
                    | Literal::Blob(_)
                    | Literal::True
                    | Literal::False
            )
        )
    }

    fn is_null_matching(
        &self,
        operator: turso_parser::ast::Operator,
        expression: &hir::Expr,
    ) -> bool {
        operator == turso_parser::ast::Operator::Is && !self.is_nonnull(expression)
    }

    fn affinity(&self, affinity: Affinity, _expression: &hir::Expr) -> Affinity {
        // HIR constraint planning already removes affinity work that cannot
        // change the resolved key expression.
        affinity
    }

    fn encode_index_keys(
        &mut self,
        program: &mut ProgramBuilder,
        index: &Arc<Index>,
        start_reg: usize,
        num_keys: usize,
        index_column_offset: usize,
    ) -> Result<()> {
        for position in 0..num_keys {
            let Some(index_column) = index.columns.get(index_column_offset + position) else {
                break;
            };
            let Some(programs) = self
                .source
                .column_type_programs
                .get(index_column.pos_in_table)
                .and_then(Option::as_ref)
            else {
                continue;
            };
            if programs.encode.is_empty() {
                continue;
            }
            let register = start_reg + position;
            let encoded = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg: register,
                target_pc: encoded,
            });
            super::expr::translate_schema_calls_in_place(
                program,
                self.document,
                &programs.encode,
                register,
            )?;
            program.preassign_label_to_next_insn(encoded);
        }
        Ok(())
    }

    fn rowid_affinity(&self) -> Affinity {
        self.source
            .columns
            .iter()
            .find(|column| column.rowid_alias)
            .map_or(Affinity::Numeric, |column| column.affinity)
    }
}

/// Allocate the legacy query destination from facts already frozen in HIR.
///
/// The matching expression binding is installed at the same time so query
/// emission and expression lowering cannot choose different storage.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn prepare_subquery(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &SubqueryExpr,
) -> Result<PreparedSubquery> {
    let query_id = match expression {
        SubqueryExpr::Scalar { query, .. }
        | SubqueryExpr::Row { query }
        | SubqueryExpr::In { query, .. }
        | SubqueryExpr::Exists(query) => *query,
    };
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR subquery destination references missing query {query_id}"
        ))
    })?;
    let width = query.output.len();
    if width == 0 {
        return Err(LimboError::InternalError(format!(
            "HIR subquery {query_id} has no outputs"
        )));
    }

    let prepared = match expression {
        SubqueryExpr::Scalar { output, .. } => {
            if *output >= width {
                return Err(LimboError::InternalError(format!(
                    "HIR scalar subquery {query_id} output {output} exceeds query width {width}"
                )));
            }
            row_value_destination(program, width)
        }
        SubqueryExpr::Row { .. } => row_value_destination(program, width),
        SubqueryExpr::Exists(_) => {
            let register = program.alloc_register();
            DestinationBinding {
                destination: QueryDestination::ExistsSubqueryResult {
                    result_reg: register,
                },
                binding: SubqueryBinding::Exists { register },
            }
        }
        SubqueryExpr::In { comparison, .. } => {
            if comparison.components.len() != width {
                return Err(LimboError::InternalError(format!(
                    "HIR IN subquery {query_id} comparison width {} does not match query width {width}",
                    comparison.components.len()
                )));
            }
            let columns = query
                .output
                .iter()
                .zip(&comparison.components)
                .enumerate()
                .map(|(position, (output, component))| {
                    let output = document.output(*output).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR IN subquery {query_id} references missing output {output:?}"
                        ))
                    })?;
                    Ok(IndexColumn {
                        name: output.name.clone(),
                        order: SortOrder::Asc,
                        nulls_order: None,
                        pos_in_table: position,
                        collation: component
                            .collation
                            .as_ref()
                            .map(|collation| *collation.value()),
                        default: None,
                        expr: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let affinity_str = comparison
                .components
                .iter()
                .map(|component| component.affinity.aff_mask())
                .collect::<String>();
            let index = Arc::new(Index {
                columns,
                name: format!("ephemeral_index_hir_sub_{}", query_id.index()),
                table_name: String::new(),
                root_page: 0,
                unique: false,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            DestinationBinding {
                destination: QueryDestination::EphemeralIndex {
                    cursor_id: cursor,
                    index,
                    affinity_str: Some(Arc::new(affinity_str)),
                    is_delete: false,
                },
                binding: SubqueryBinding::InIndex { cursor },
            }
        }
    };

    program.bind_subquery(query_id, prepared.binding);
    Ok(PreparedSubquery {
        query: query_id,
        destination: prepared.destination,
        correlated: !query.captures.is_empty(),
    })
}

/// Emit the execution shell shared by all non-FROM expression subqueries.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_prepared_subquery(
    program: &mut ProgramBuilder,
    prepared: &PreparedSubquery,
    emit_body: impl FnOnce(&mut ProgramBuilder, QueryId, &QueryDestination) -> Result<()>,
) -> Result<()> {
    program.nested(|program| {
        let explain_id = program.next_subquery_eqp_id();
        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { .. } => {}
            QueryDestination::RowValueSubqueryResult { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ScalarSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            QueryDestination::EphemeralIndex { .. } => {
                crate::emit_explain!(
                    program,
                    true,
                    EqpDetail::ListSubquery {
                        id: explain_id,
                        correlated: prepared.correlated,
                    }
                );
            }
            destination => {
                return Err(LimboError::InternalError(format!(
                    "HIR expression subquery has invalid destination {destination:?}"
                )));
            }
        }

        let once_done = if prepared.correlated {
            None
        } else {
            let done = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: done,
            });
            Some(done)
        };

        match &prepared.destination {
            QueryDestination::ExistsSubqueryResult { result_reg } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::RowValueSubqueryResult {
                result_reg_start,
                num_regs,
            } => {
                let return_register = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: return_register,
                    dest_end: None,
                });
                for register in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: register,
                        dest_end: None,
                    });
                }
                emit_body(program, prepared.query, &prepared.destination)?;
                program.emit_insn(Insn::Return {
                    return_reg: return_register,
                    can_fallthrough: true,
                });
            }
            QueryDestination::EphemeralIndex { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_body(program, prepared.query, &prepared.destination)?;
            }
            _ => unreachable!("expression subquery destination was checked"),
        }

        if !matches!(
            prepared.destination,
            QueryDestination::ExistsSubqueryResult { .. }
        ) {
            program.pop_current_parent_explain();
        }
        if let Some(once_done) = once_done {
            program.preassign_label_to_next_insn(once_done);
        }
        Ok(())
    })
}

/// Emit SELECT and VALUES bodies that do not read a row source.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_query_body(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query_id: QueryId,
    destination: &QueryDestination,
) -> Result<()> {
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR lowering references missing query {query_id}"))
    })?;
    if query.blocks.len() != 1 || !query.compounds.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query {query_id} is not a supported non-FROM query body"
        )));
    }
    let block = &query.blocks[0];
    if block.from.is_some()
        || block.aggregate_count != 0
        || block.window_function_count != 0
        || !block.windows.is_empty()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} is not a supported non-FROM query body",
            block.id
        )));
    }

    let done = program.allocate_label();
    let distinct = initialize_hir_distinct(program, block)?;
    let sorter = (!query.order_by.is_empty())
        .then(|| initialize_hir_sorter(program, query, block))
        .transpose()?;
    let limit = initialize_limit(program, document, query.limit.as_ref(), done)?;
    let output = if let Some(sorter) = sorter.as_ref() {
        HirRowOutput {
            target: HirRowTarget::Sorter(sorter),
            distinct: distinct.as_ref(),
        }
    } else {
        HirRowOutput {
            target: HirRowTarget::Direct {
                destination,
                limit,
                done,
            },
            distinct: distinct.as_ref(),
        }
    };
    emit_query_block_without_from(program, document, block, &output, done)?;
    if let Some(sorter) = sorter.as_ref() {
        emit_hir_sorted_rows(program, block, sorter, destination, limit, done)?;
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn emit_query_block_without_from(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    match &block.body {
        hir::QueryBlockBody::Select {
            filter,
            grouping: None,
            ..
        } => {
            let start = query_output_registers(program, &block.outputs, output.needs_values())?;
            if let Some(filter) = filter {
                let emit_row = program.allocate_label();
                super::expr::translate_condition_expr(
                    program,
                    document,
                    filter,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: emit_row,
                        jump_target_when_false: block_done,
                        jump_target_when_null: block_done,
                    },
                )?;
                program.preassign_label_to_next_insn(emit_row);
            }
            emit_query_row(
                program,
                document,
                output,
                &block.outputs,
                block.outputs.iter().map(|output| &output.expr),
                start,
                block_done,
            )?;
            Ok(())
        }
        hir::QueryBlockBody::Values { rows } => {
            let start = query_output_registers(program, &block.outputs, output.needs_values())?;
            for row in rows {
                let next_row = program.allocate_label();
                emit_query_row(
                    program,
                    document,
                    output,
                    &block.outputs,
                    row.iter(),
                    start,
                    next_row,
                )?;
                program.preassign_label_to_next_insn(next_row);
                if let HirRowTarget::Direct {
                    destination, limit, ..
                } = &output.target
                {
                    let stop_after_first = destination_stops_after_first_row(destination);
                    let needs_runtime_stop =
                        stop_after_first && limit.and_then(|limit| limit.offset).is_some();
                    if stop_after_first && !needs_runtime_stop {
                        break;
                    }
                }
            }
            Ok(())
        }
        _ => Err(LimboError::InternalError(format!(
            "HIR query block {:?} has unsupported non-FROM clauses",
            block.id
        ))),
    }
}

/// Emit one planned HIR query whose source loop is a full B-tree scan.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn emit_planned_query_body(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query_id: QueryId,
    destination: &QueryDestination,
) -> Result<()> {
    let document = &plan.document;
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR lowering references missing query {query_id}"))
    })?;
    let planned_query = plan.planned_query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR lowering references unplanned query {query_id}"
        ))
    })?;
    if query.blocks.len() != planned_query.blocks.len()
        || query.compounds.len() + 1 != query.blocks.len()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query {query_id} has inconsistent compound access plans"
        )));
    }
    if !query.compounds.is_empty() {
        return emit_hir_compound_query(program, plan, query, destination);
    }

    let block = &query.blocks[0];
    let done = program.allocate_label();
    let distinct = initialize_hir_distinct(program, block)?;
    let sorter = (!query.order_by.is_empty())
        .then(|| initialize_hir_sorter(program, query, block))
        .transpose()?;
    let limit = initialize_limit(program, document, query.limit.as_ref(), done)?;
    let output = if let Some(sorter) = sorter.as_ref() {
        HirRowOutput {
            target: HirRowTarget::Sorter(sorter),
            distinct: distinct.as_ref(),
        }
    } else {
        HirRowOutput {
            target: HirRowTarget::Direct {
                destination,
                limit,
                done,
            },
            distinct: distinct.as_ref(),
        }
    };
    emit_planned_query_block_with_output(program, plan, query, 0, &output, done)?;
    if let Some(sorter) = sorter.as_ref() {
        emit_hir_sorted_rows(program, block, sorter, destination, limit, done)?;
    }
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn emit_planned_query_block(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block_index: usize,
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    let block = query.blocks.get(block_index).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR query {} references missing block {block_index}",
            query.id
        ))
    })?;
    let distinct = initialize_hir_distinct(program, block)?;
    let output = HirRowOutput {
        target: HirRowTarget::Direct {
            destination,
            limit,
            done,
        },
        distinct: distinct.as_ref(),
    };
    emit_planned_query_block_with_output(program, plan, query, block_index, &output, done)
}

fn emit_planned_query_block_with_output(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block_index: usize,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    let block = query.blocks.get(block_index).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR query {} references missing block {block_index}",
            query.id
        ))
    })?;
    let planned_query = plan.planned_query(query.id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR lowering references unplanned query {}",
            query.id
        ))
    })?;
    let block_plan = planned_query.blocks.get(block_index).ok_or_else(|| {
        LimboError::InternalError(format!("HIR query block {:?} has no access plan", block.id))
    })?;
    if block_plan.block != block.id {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has mismatched access plan {:?}",
            block.id, block_plan.block
        )));
    }
    if block.from.is_none() {
        if block.window_function_count != 0 || !block.windows.is_empty() {
            return Err(LimboError::InternalError(format!(
                "HIR query block {:?} is not a supported non-FROM query body",
                block.id
            )));
        }
        if is_hir_ungrouped_aggregate(block) {
            return emit_hir_ungrouped_without_from(
                program,
                &plan.document,
                query,
                block,
                output,
                block_done,
            );
        }
        return emit_query_block_without_from(program, &plan.document, block, output, block_done);
    }
    if is_hir_ungrouped_aggregate(block) {
        emit_hir_ungrouped_aggregate(program, plan, query, block, block_plan, output, block_done)
    } else if hir_grouping(block).is_some() {
        emit_hir_grouped_aggregate(program, plan, query, block, block_plan, output)
    } else {
        emit_btree_loops(program, plan, block, block_plan, HirLoopBody::Rows(output))
    }
}

fn emit_hir_ungrouped_without_from(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: &hir::Query,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    let state = initialize_hir_aggregates(program, document, query, block)?;
    let scan_done = program.allocate_label();
    let hir::QueryBlockBody::Select { filter, .. } = &block.body else {
        return Err(LimboError::InternalError(format!(
            "HIR aggregate block {:?} is not a SELECT",
            block.id
        )));
    };
    if let Some(filter) = filter {
        let emit_step = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            filter,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: emit_step,
                jump_target_when_false: scan_done,
                jump_target_when_null: scan_done,
            },
        )?;
        program.preassign_label_to_next_insn(emit_step);
    }
    emit_hir_aggregate_steps(program, document, &state)?;
    program.preassign_label_to_next_insn(scan_done);
    emit_hir_aggregate_result(program, document, block, output, block_done, &state)
}

fn emit_hir_ungrouped_aggregate(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
) -> Result<()> {
    let state = initialize_hir_aggregates(program, &plan.document, query, block)?;
    emit_btree_loops(
        program,
        plan,
        block,
        block_plan,
        HirLoopBody::UngroupedAggregate(&state),
    )?;
    emit_hir_aggregate_result(program, &plan.document, block, output, block_done, &state)
}

fn emit_hir_grouped_aggregate<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    output: &HirRowOutput<'_>,
) -> Result<()> {
    let runtime = initialize_hir_group_runtime(program, plan, query, block, block_plan)?;
    emit_btree_loops(
        program,
        plan,
        block,
        block_plan,
        HirLoopBody::Group(&runtime),
    )?;
    if matches!(runtime.rows, HirGroupRows::Sorter(_)) {
        emit_hir_group_sorter_rows(program, &plan.document, &runtime)?;
    }
    emit_hir_group_result(program, &plan.document, block, output, &runtime)
}

fn initialize_hir_group_runtime<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
) -> Result<HirGroupRuntime<'a>> {
    let grouping = hir_grouping(block).expect("grouped lowering has non-empty keys");
    if grouping.keys.len() != grouping.key_type_facts.len()
        || grouping.keys.len() != grouping.key_collations.len()
    {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has inconsistent grouping facts",
            block.id
        )));
    }
    let aggregates = initialize_hir_aggregates(program, &plan.document, query, block)?;
    let usage = query.direct_column_usage(|source| plan.document.source(source));
    let rowids = hir_group_rowids(query, block);
    let mut inputs = (0..grouping.keys.len())
        .map(HirGroupInput::Key)
        .collect::<Vec<_>>();
    let mut sources = Vec::with_capacity(block_plan.loops.len());
    for planned_loop in &block_plan.loops {
        if sources
            .iter()
            .any(|source: &HirGroupSource| source.source == planned_loop.source)
        {
            return Err(LimboError::InternalError(format!(
                "HIR grouped query has more than one loop for source {}",
                planned_loop.source
            )));
        }
        let source = plan.document.source(planned_loop.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR grouped query references missing source {}",
                planned_loop.source
            ))
        })?;
        let row_start = if source.columns.is_empty() {
            0
        } else {
            program.alloc_registers(source.columns.len())
        };
        let group_start = if source.columns.is_empty() {
            0
        } else {
            program.alloc_registers(source.columns.len())
        };
        for column in usage
            .iter()
            .filter(|usage| usage.reference.source == source.id)
            .map(|usage| usage.reference.column)
        {
            inputs.push(HirGroupInput::Column {
                reference: hir::ColumnRef {
                    source: source.id,
                    column,
                },
                row_register: row_start + column,
                group_register: group_start + column,
            });
        }
        let rowid = rowids.contains(&source.id).then(|| {
            let row_register = program.alloc_register();
            let group_register = program.alloc_register();
            inputs.push(HirGroupInput::RowId {
                source: source.id,
                row_register,
                group_register,
            });
            HirGroupRowId {
                row_register,
                group_register,
            }
        });
        sources.push(HirGroupSource {
            source: source.id,
            row_start,
            group_start,
            rowid,
        });
    }
    let rows = if block_plan.group_sort_elided {
        HirGroupRows::InputOrdered
    } else {
        let order_collations_nulls = grouping
            .key_collations
            .iter()
            .map(|collation| {
                (
                    SortOrder::Asc,
                    collation.as_ref().map(|collation| *collation.value()),
                    None,
                )
            })
            .collect();
        let comparators = grouping
            .key_type_facts
            .iter()
            .map(custom_type_comparator_from_type_fact)
            .collect();
        let cursor = program.alloc_cursor_id(CursorType::Sorter);
        program.emit_insn(Insn::SorterOpen {
            data: Box::new(SorterOpenData {
                cursor_id: cursor,
                columns: inputs.len(),
                order_collations_nulls,
                comparators,
            }),
        });
        emit_explain!(
            program,
            false,
            EqpDetail::GroupBy {
                method: EqpSortMethod::Sorter,
            }
        );
        HirGroupRows::Sorter(HirGroupSorter {
            cursor,
            pseudo_cursor: program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
                column_count: inputs.len(),
            })),
            record: program.alloc_register(),
            inputs_start: program.alloc_registers(inputs.len()),
        })
    };
    let state = HirGroupState {
        output_group: program.allocate_label(),
        output_done: program.allocate_label(),
        finalize_group: program.allocate_label(),
        clear_group: program.allocate_label(),
        group_end: program.allocate_label(),
        output_return: program.alloc_register(),
        clear_return: program.alloc_register(),
        data_in_group: program.alloc_register(),
        previous_keys: program.alloc_registers(grouping.keys.len()),
        current_keys: program.alloc_registers(grouping.keys.len()),
    };
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: state.data_in_group,
    });
    program.emit_insn(Insn::Null {
        dest: state.previous_keys,
        dest_end: (grouping.keys.len() > 1)
            .then_some(state.previous_keys + grouping.keys.len() - 1),
    });
    program.emit_insn(Insn::Gosub {
        target_pc: state.clear_group,
        return_reg: state.clear_return,
    });
    Ok(HirGroupRuntime {
        grouping,
        aggregates,
        rows,
        inputs,
        sources,
        state,
    })
}

fn hir_group_rowids(query: &hir::Query, block: &hir::QueryBlock) -> Vec<SourceId> {
    let mut rowids = Vec::new();
    let mut collect = |expression: &hir::Expr| {
        expression.for_each(&mut |expression| {
            if let hir::Expr::RowId(source) = expression {
                if !rowids.contains(source) {
                    rowids.push(*source);
                }
            }
        });
    };
    for output in &block.outputs {
        collect(&output.expr);
    }
    if let hir::QueryBlockBody::Select { grouping, .. } = &block.body {
        if let Some(grouping) = grouping {
            for key in &grouping.keys {
                collect(key);
            }
            if let Some(having) = &grouping.having {
                collect(having);
            }
        }
    }
    for term in &query.order_by {
        collect(&term.expr);
    }
    rowids.sort_unstable();
    rowids
}

fn emit_hir_group_input(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    let HirGroupRows::Sorter(sorter) = &runtime.rows else {
        for input in &runtime.inputs {
            match input {
                HirGroupInput::Key(index) => super::expr::translate_expr(
                    program,
                    document,
                    &runtime.grouping.keys[*index],
                    runtime.state.current_keys + index,
                )?,
                HirGroupInput::Column {
                    reference,
                    row_register,
                    ..
                } => super::expr::translate_expr(
                    program,
                    document,
                    &hir::Expr::Column(*reference),
                    *row_register,
                )?,
                HirGroupInput::RowId {
                    source,
                    row_register,
                    ..
                } => super::expr::translate_expr(
                    program,
                    document,
                    &hir::Expr::RowId(*source),
                    *row_register,
                )?,
            };
        }
        return emit_hir_group_row(program, document, runtime);
    };
    for (position, input) in runtime.inputs.iter().enumerate() {
        let target = sorter.inputs_start + position;
        let expression = match input {
            HirGroupInput::Key(index) => &runtime.grouping.keys[*index],
            HirGroupInput::Column { reference, .. } => {
                super::expr::translate_expr(
                    program,
                    document,
                    &hir::Expr::Column(*reference),
                    target,
                )?;
                continue;
            }
            HirGroupInput::RowId { source, .. } => {
                super::expr::translate_expr(
                    program,
                    document,
                    &hir::Expr::RowId(*source),
                    target,
                )?;
                continue;
            }
        };
        super::expr::translate_expr(program, document, expression, target)?;
    }
    sorter_insert(
        program,
        sorter.inputs_start,
        runtime.inputs.len(),
        sorter.cursor,
        sorter.record,
    );
    Ok(())
}

fn bind_hir_group_sources(
    program: &mut ProgramBuilder,
    sources: &[HirGroupSource],
    values: HirGroupSourceValues,
) {
    for source in sources {
        let (start, rowid) = match values {
            HirGroupSourceValues::Row => (
                source.row_start,
                source.rowid.as_ref().map(|rowid| rowid.row_register),
            ),
            HirGroupSourceValues::Group => (
                source.group_start,
                source.rowid.as_ref().map(|rowid| rowid.group_register),
            ),
        };
        program.bind_source(source.source, SourceBinding::Registers { start, rowid });
    }
}

fn reset_hir_group_aggregates(program: &mut ProgramBuilder, state: &HirAggregateState<'_>) {
    for aggregate in &state.aggregates {
        program.emit_insn(Insn::Null {
            dest: aggregate.accumulator,
            dest_end: None,
        });
        if let Some(distinct) = &aggregate.distinct {
            program.emit_insn(Insn::HashClear {
                hash_table_id: distinct.hash_table,
            });
        }
    }
}

fn emit_hir_group_sorter_rows(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    let HirGroupRows::Sorter(sorter) = &runtime.rows else {
        unreachable!("sorter drain requires grouped sorter rows")
    };
    let sort_loop = program.allocate_label();
    let sort_end = program.allocate_label();
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: sorter.pseudo_cursor,
        content_reg: sorter.record,
        num_fields: runtime.inputs.len(),
    });
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter.cursor,
        pc_if_empty: sort_end,
    });
    program.preassign_label_to_next_insn(sort_loop);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter.cursor,
        dest_reg: sorter.record,
        pseudo_cursor: sorter.pseudo_cursor,
    });
    for (position, input) in runtime.inputs.iter().enumerate() {
        let target = match input {
            HirGroupInput::Key(index) => runtime.state.current_keys + index,
            HirGroupInput::Column { row_register, .. }
            | HirGroupInput::RowId { row_register, .. } => *row_register,
        };
        program.emit_column_or_rowid(sorter.pseudo_cursor, position, target);
    }
    emit_hir_group_row(program, document, runtime)?;
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter.cursor,
        pc_if_next: sort_loop,
    });
    program.preassign_label_to_next_insn(sort_end);
    Ok(())
}

fn emit_hir_group_row(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    let new_group = program.allocate_label();
    let aggregate_step = program.allocate_label();
    let captured = program.allocate_label();
    let key_info = runtime
        .grouping
        .key_collations
        .iter()
        .map(|collation| KeyInfo {
            sort_order: SortOrder::Asc,
            collation: collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default(),
            nulls_order: None,
        })
        .collect();
    program.emit_insn(Insn::Compare {
        start_reg_a: runtime.state.previous_keys,
        start_reg_b: runtime.state.current_keys,
        count: runtime.grouping.keys.len(),
        key_info,
    });
    program.emit_insn(Insn::Jump {
        target_pc_lt: new_group,
        target_pc_eq: aggregate_step,
        target_pc_gt: new_group,
    });
    program.preassign_label_to_next_insn(new_group);
    program.emit_insn(Insn::Gosub {
        target_pc: runtime.state.output_group,
        return_reg: runtime.state.output_return,
    });
    program.emit_insn(Insn::Move {
        source_reg: runtime.state.current_keys,
        dest_reg: runtime.state.previous_keys,
        count: runtime.grouping.keys.len(),
    });
    program.emit_insn(Insn::Gosub {
        target_pc: runtime.state.clear_group,
        return_reg: runtime.state.clear_return,
    });

    program.preassign_label_to_next_insn(aggregate_step);
    bind_hir_group_sources(program, &runtime.sources, HirGroupSourceValues::Row);
    emit_hir_aggregate_steps(program, document, &runtime.aggregates)?;
    program.emit_insn(Insn::If {
        reg: runtime.state.data_in_group,
        target_pc: captured,
        jump_if_null: false,
    });
    for input in &runtime.inputs {
        match input {
            HirGroupInput::Key(_) => {}
            HirGroupInput::Column {
                row_register,
                group_register,
                ..
            }
            | HirGroupInput::RowId {
                row_register,
                group_register,
                ..
            } => program.emit_insn(Insn::Copy {
                src_reg: *row_register,
                dst_reg: *group_register,
                extra_amount: 0,
            }),
        }
    }
    program.preassign_label_to_next_insn(captured);
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: runtime.state.data_in_group,
    });
    Ok(())
}

fn emit_hir_group_result(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    runtime: &HirGroupRuntime<'_>,
) -> Result<()> {
    program.emit_insn(Insn::Gosub {
        target_pc: runtime.state.output_group,
        return_reg: runtime.state.output_return,
    });
    program.emit_insn(Insn::Goto {
        target_pc: runtime.state.group_end,
    });

    program.preassign_label_to_next_insn(runtime.state.output_group);
    program.emit_insn(Insn::IfPos {
        reg: runtime.state.data_in_group,
        target_pc: runtime.state.finalize_group,
        decrement_by: 0,
    });
    program.preassign_label_to_next_insn(runtime.state.output_done);
    program.emit_insn(Insn::Return {
        return_reg: runtime.state.output_return,
        can_fallthrough: false,
    });

    program.preassign_label_to_next_insn(runtime.state.finalize_group);
    finalize_hir_aggregates(program, &runtime.aggregates)?;
    bind_hir_group_sources(program, &runtime.sources, HirGroupSourceValues::Group);
    if let Some(having) = &runtime.grouping.having {
        let emit_row = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            having,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: emit_row,
                jump_target_when_false: runtime.state.output_done,
                jump_target_when_null: runtime.state.output_done,
            },
        )?;
        program.preassign_label_to_next_insn(emit_row);
    }
    let start = query_output_registers(program, &block.outputs, output.needs_values())?;
    emit_query_row(
        program,
        document,
        output,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
        runtime.state.output_done,
    )?;
    program.emit_insn(Insn::Return {
        return_reg: runtime.state.output_return,
        can_fallthrough: false,
    });

    program.preassign_label_to_next_insn(runtime.state.clear_group);
    reset_hir_group_aggregates(program, &runtime.aggregates);
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: runtime.state.data_in_group,
    });
    program.emit_insn(Insn::Return {
        return_reg: runtime.state.clear_return,
        can_fallthrough: false,
    });
    program.preassign_label_to_next_insn(runtime.state.group_end);
    Ok(())
}

fn emit_hir_aggregate_result(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block: &hir::QueryBlock,
    output: &HirRowOutput<'_>,
    block_done: BranchOffset,
    state: &HirAggregateState<'_>,
) -> Result<()> {
    finalize_hir_aggregates(program, state)?;
    let hir::QueryBlockBody::Select { grouping, .. } = &block.body else {
        unreachable!("aggregate block is a SELECT")
    };
    if let Some(having) = grouping
        .as_ref()
        .and_then(|grouping| grouping.having.as_ref())
    {
        let emit_row = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            having,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: emit_row,
                jump_target_when_false: block_done,
                jump_target_when_null: block_done,
            },
        )?;
        program.preassign_label_to_next_insn(emit_row);
    }
    let start = query_output_registers(program, &block.outputs, output.needs_values())?;
    emit_query_row(
        program,
        document,
        output,
        &block.outputs,
        block.outputs.iter().map(|output| &output.expr),
        start,
        block_done,
    )
}

fn emit_hir_compound_query(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    destination: &QueryDestination,
) -> Result<()> {
    emit_explain!(program, true, EqpDetail::Compound);
    let result = if query.order_by.is_empty() {
        let done = program.allocate_label();
        let limit = initialize_limit(program, &plan.document, query.limit.as_ref(), done)?;
        emit_hir_compound_prefix(
            program,
            plan,
            query,
            query.blocks.len() - 1,
            destination,
            limit,
            done,
        )?;
        program.preassign_label_to_next_insn(done);
        Ok(())
    } else {
        let (collection_cursor, collection_index) = open_hir_compound_index(
            program,
            query,
            0,
            query.blocks.len() - 1,
            "compound_collection",
            true,
        )?;
        let collection_destination = QueryDestination::EphemeralIndex {
            cursor_id: collection_cursor,
            index: collection_index.clone(),
            affinity_str: None,
            is_delete: false,
        };
        let collection_done = program.allocate_label();
        emit_hir_compound_prefix(
            program,
            plan,
            query,
            query.blocks.len() - 1,
            &collection_destination,
            None,
            collection_done,
        )?;
        program.preassign_label_to_next_insn(collection_done);
        emit_hir_compound_order_by(
            program,
            &plan.document,
            query,
            collection_cursor,
            destination,
        )
    };
    program.pop_current_parent_explain();
    result
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_compound_prefix(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    query: &hir::Query,
    last_block: usize,
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    if last_block == 0 {
        emit_explain!(
            program,
            true,
            EqpDetail::CompoundArm {
                op: EqpCompoundOp::LeftMost,
                temp_btree: false,
            }
        );
        let result = emit_planned_query_block(program, plan, query, 0, destination, limit, done);
        program.pop_current_parent_explain();
        return result;
    }

    let operator = query.compounds[last_block - 1].operator;
    match operator {
        CompoundOperator::UnionAll => {
            emit_hir_compound_prefix(
                program,
                plan,
                query,
                last_block - 1,
                destination,
                limit,
                done,
            )?;
            emit_hir_compound_arm_explain(program, operator, false, |program| {
                emit_planned_query_block(program, plan, query, last_block, destination, limit, done)
            })
        }
        CompoundOperator::Union | CompoundOperator::Except => {
            let (cursor, index) = open_hir_compound_index(
                program,
                query,
                last_block - 1,
                last_block,
                "compound_dedupe",
                false,
            )?;
            let insert_destination = QueryDestination::EphemeralIndex {
                cursor_id: cursor,
                index: index.clone(),
                affinity_str: None,
                is_delete: false,
            };
            emit_hir_compound_prefix(
                program,
                plan,
                query,
                last_block - 1,
                &insert_destination,
                None,
                done,
            )?;
            let arm_destination = QueryDestination::EphemeralIndex {
                cursor_id: cursor,
                index: index.clone(),
                affinity_str: None,
                is_delete: operator == CompoundOperator::Except,
            };
            emit_hir_compound_arm_explain(program, operator, true, |program| {
                emit_planned_query_block(
                    program,
                    plan,
                    query,
                    last_block,
                    &arm_destination,
                    None,
                    done,
                )
            })?;
            emit_hir_deduplicated_rows(program, cursor, &index, limit, destination, done)
        }
        CompoundOperator::Intersect => {
            let (left_cursor, left_index) = open_hir_compound_index(
                program,
                query,
                last_block - 1,
                last_block,
                "compound_intersect_left",
                false,
            )?;
            let left_destination = QueryDestination::EphemeralIndex {
                cursor_id: left_cursor,
                index: left_index.clone(),
                affinity_str: None,
                is_delete: false,
            };
            emit_hir_compound_prefix(
                program,
                plan,
                query,
                last_block - 1,
                &left_destination,
                None,
                done,
            )?;

            let (right_cursor, right_index) = open_hir_compound_index(
                program,
                query,
                last_block - 1,
                last_block,
                "compound_intersect_right",
                false,
            )?;
            let right_destination = QueryDestination::EphemeralIndex {
                cursor_id: right_cursor,
                index: right_index,
                affinity_str: None,
                is_delete: false,
            };
            emit_hir_compound_arm_explain(program, operator, true, |program| {
                emit_planned_query_block(
                    program,
                    plan,
                    query,
                    last_block,
                    &right_destination,
                    None,
                    done,
                )
            })?;
            emit_hir_intersect_rows(
                program,
                left_cursor,
                &left_index,
                right_cursor,
                limit,
                destination,
                done,
            )
        }
    }
}

fn emit_hir_compound_arm_explain(
    program: &mut ProgramBuilder,
    operator: CompoundOperator,
    temp_btree: bool,
    emit: impl FnOnce(&mut ProgramBuilder) -> Result<()>,
) -> Result<()> {
    let op = match operator {
        CompoundOperator::Union => EqpCompoundOp::Union,
        CompoundOperator::UnionAll => EqpCompoundOp::UnionAll,
        CompoundOperator::Except => EqpCompoundOp::Except,
        CompoundOperator::Intersect => EqpCompoundOp::Intersect,
    };
    emit_explain!(program, true, EqpDetail::CompoundArm { op, temp_btree });
    let result = emit(program);
    program.pop_current_parent_explain();
    result
}

fn open_hir_compound_index(
    program: &mut ProgramBuilder,
    query: &hir::Query,
    left_block: usize,
    right_block: usize,
    name: &str,
    has_rowid: bool,
) -> Result<(CursorID, Arc<Index>)> {
    let left = query.blocks.get(left_block).ok_or_else(|| {
        LimboError::InternalError(format!("HIR compound query has no block {left_block}"))
    })?;
    let right = query.blocks.get(right_block).ok_or_else(|| {
        LimboError::InternalError(format!("HIR compound query has no block {right_block}"))
    })?;
    if left.outputs.len() != right.outputs.len() || left.outputs.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR compound blocks {:?} and {:?} have inconsistent widths",
            left.id, right.id
        )));
    }
    let mut columns = Vec::with_capacity(left.outputs.len());
    for (position, (left, right)) in left.outputs.iter().zip(&right.outputs).enumerate() {
        columns.push(IndexColumn {
            name: left.name.clone(),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table: position,
            collation: left
                .collation
                .as_ref()
                .or(right.collation.as_ref())
                .map(|collation| *collation.value()),
            default: None,
            expr: None,
        });
    }
    let index = Arc::new(Index {
        name: name.to_string(),
        table_name: String::new(),
        root_page: 0,
        columns,
        unique: false,
        ephemeral: true,
        has_rowid,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: false,
    });
    Ok((cursor, index))
}

fn emit_hir_deduplicated_rows(
    program: &mut ProgramBuilder,
    cursor: CursorID,
    index: &Index,
    limit: Option<QueryLimitRegisters>,
    destination: &QueryDestination,
    done: BranchOffset,
) -> Result<()> {
    let close = program.allocate_label();
    let next = program.allocate_label();
    let loop_start = program.allocate_label();
    let columns = program.alloc_registers(index.columns.len());
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: close,
    });
    program.preassign_label_to_next_insn(loop_start);
    emit_before_row(program, limit, next);
    for column in 0..index.columns.len() {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column,
            dest: columns + column,
            default: None,
        });
    }
    emit_columns_to_destination(program, destination, columns, index.columns.len())?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(close);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_intersect_rows(
    program: &mut ProgramBuilder,
    left_cursor: CursorID,
    index: &Index,
    right_cursor: CursorID,
    limit: Option<QueryLimitRegisters>,
    destination: &QueryDestination,
    done: BranchOffset,
) -> Result<()> {
    let close = program.allocate_label();
    let next = program.allocate_label();
    let loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: left_cursor,
        pc_if_empty: close,
    });
    program.preassign_label_to_next_insn(loop_start);
    let record = program.alloc_register();
    program.emit_insn(Insn::RowData {
        cursor_id: left_cursor,
        dest: record,
    });
    program.emit_insn(Insn::NotFound {
        cursor_id: right_cursor,
        target_pc: next,
        record_reg: record,
        num_regs: 0,
    });
    emit_before_row(program, limit, next);
    let columns = program.alloc_registers(index.columns.len());
    for column in 0..index.columns.len() {
        program.emit_insn(Insn::Column {
            cursor_id: left_cursor,
            column,
            dest: columns + column,
            default: None,
        });
    }
    emit_columns_to_destination(program, destination, columns, index.columns.len())?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: left_cursor,
        pc_if_next: loop_start,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(close);
    program.emit_insn(Insn::Close {
        cursor_id: right_cursor,
    });
    program.emit_insn(Insn::Close {
        cursor_id: left_cursor,
    });
    Ok(())
}

fn emit_hir_compound_order_by(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    query: &hir::Query,
    collection_cursor: CursorID,
    destination: &QueryDestination,
) -> Result<()> {
    let output_count = query.blocks[0].outputs.len();
    let mut order_positions = Vec::with_capacity(query.order_by.len());
    let mut order_collations_nulls = Vec::with_capacity(query.order_by.len() + 1);
    let mut comparators = Vec::with_capacity(query.order_by.len() + 1);
    for term in &query.order_by {
        let position = hir_compound_order_position(query, &term.expr)?;
        order_positions.push(position);
        order_collations_nulls.push((
            term.order,
            term.collation.as_ref().map(|collation| *collation.value()),
            term.nulls,
        ));
        comparators.push(custom_type_comparator_from_type_fact(&term.type_fact));
    }
    order_collations_nulls.push((SortOrder::Asc, None, None));
    comparators.push(None);

    let sequence_slot = order_positions.len();
    let data_start = sequence_slot + 1;
    let mut data_columns = 0;
    let mut remappings = Vec::with_capacity(output_count);
    for output in 0..output_count {
        if let Some(sort_key) = order_positions
            .iter()
            .position(|position| *position == output)
        {
            remappings.push((sort_key, true));
        } else {
            remappings.push((data_start + data_columns, false));
            data_columns += 1;
        }
    }
    let sorter_column_count = data_start + data_columns;
    let sort_cursor = program.alloc_cursor_id(CursorType::Sorter);
    program.emit_insn(Insn::SorterOpen {
        data: Box::new(SorterOpenData {
            cursor_id: sort_cursor,
            columns: order_collations_nulls.len(),
            order_collations_nulls,
            comparators,
        }),
    });

    let collection_done = program.allocate_label();
    let collection_loop = program.allocate_label();
    let read_registers = program.alloc_registers(output_count + 1);
    let sequence_register = read_registers + output_count;
    program.emit_insn(Insn::Rewind {
        cursor_id: collection_cursor,
        pc_if_empty: collection_done,
    });
    program.preassign_label_to_next_insn(collection_loop);
    for output in 0..output_count {
        program.emit_insn(Insn::Column {
            cursor_id: collection_cursor,
            column: output,
            dest: read_registers + output,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: collection_cursor,
        column: output_count,
        dest: sequence_register,
        default: None,
    });

    let sorter_registers = program.alloc_registers(sorter_column_count);
    for (sort_key, output) in order_positions.iter().enumerate() {
        program.emit_insn(Insn::Copy {
            src_reg: read_registers + output,
            dst_reg: sorter_registers + sort_key,
            extra_amount: 0,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: sequence_register,
        dst_reg: sorter_registers + sequence_slot,
        extra_amount: 0,
    });
    let mut data_column = data_start;
    for (output, (_, deduplicated)) in remappings.iter().enumerate() {
        if !deduplicated {
            program.emit_insn(Insn::Copy {
                src_reg: read_registers + output,
                dst_reg: sorter_registers + data_column,
                extra_amount: 0,
            });
            data_column += 1;
        }
    }
    let sorter_record = program.alloc_register();
    sorter_insert(
        program,
        sorter_registers,
        sorter_column_count,
        sort_cursor,
        sorter_record,
    );
    program.emit_insn(Insn::Next {
        cursor_id: collection_cursor,
        pc_if_next: collection_loop,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(collection_done);
    program.emit_insn(Insn::Close {
        cursor_id: collection_cursor,
    });

    let sort_done = program.allocate_label();
    let limit = initialize_limit(program, document, query.limit.as_ref(), sort_done)?;
    let output_columns = remappings
        .iter()
        .map(|(sorter_column, _)| *sorter_column)
        .collect::<Vec<_>>();
    emit_hir_sorter_rows(
        program,
        &query.blocks[0].outputs,
        sort_cursor,
        sorter_record,
        sorter_column_count,
        &output_columns,
        destination,
        limit,
        sort_done,
    )?;
    program.preassign_label_to_next_insn(sort_done);
    Ok(())
}

fn hir_compound_order_position(query: &hir::Query, expression: &hir::Expr) -> Result<usize> {
    let mut expression = expression;
    while let hir::Expr::Collate { expr, .. } = expression {
        expression = expr;
    }
    let hir::Expr::Output(output) = expression else {
        return Err(LimboError::InternalError(format!(
            "HIR compound ORDER BY expression is not an output reference: {expression:?}"
        )));
    };
    query.blocks[0]
        .outputs
        .iter()
        .position(|candidate| candidate.id == *output)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR compound ORDER BY references missing output {output:?}"
            ))
        })
}

enum HirLoopAdvance {
    None,
    Cursor {
        direction: IterationDirection,
        fullscan: bool,
    },
    InSeek {
        state: InSeekLoop,
        index_backed: bool,
    },
    Virtual,
}

struct PreparedHirBtreeLoop<'a> {
    source: &'a hir::Source,
    operation: &'a HirBtreeOperation,
    index: Option<&'a Arc<Index>>,
    cursor: CursorID,
    table_cursor: Option<CursorID>,
    autoindex_source_cursor: Option<CursorID>,
    table_has_rowid: bool,
}

struct PreparedHirMaterializedLoop<'a> {
    source: &'a hir::Source,
    cursor: CursorID,
    table: Arc<BTreeTable>,
}

struct PreparedHirVirtualLoop<'a> {
    source: &'a hir::Source,
    operation: &'a HirVirtualTableOperation,
    cursor: CursorID,
}

struct PreparedHirRegistersLoop<'a> {
    source: &'a hir::Source,
}

#[derive(Clone, Copy)]
enum HirMaterializedQuery {
    Derived(QueryId),
    Cte { cte: hir::CteId, query: QueryId },
}

impl HirMaterializedQuery {
    fn query(self) -> QueryId {
        match self {
            Self::Derived(query) | Self::Cte { query, .. } => query,
        }
    }
}

enum PreparedHirLoop<'a> {
    BTree(PreparedHirBtreeLoop<'a>),
    Virtual(PreparedHirVirtualLoop<'a>),
    Materialized(PreparedHirMaterializedLoop<'a>),
    Registers(PreparedHirRegistersLoop<'a>),
}

impl PreparedHirLoop<'_> {
    fn cursor(&self) -> Option<CursorID> {
        match self {
            Self::BTree(prepared) => Some(prepared.cursor),
            Self::Virtual(prepared) => Some(prepared.cursor),
            Self::Materialized(prepared) => Some(prepared.cursor),
            Self::Registers(_) => None,
        }
    }

    fn table_cursor(&self) -> Option<CursorID> {
        match self {
            Self::BTree(prepared) => prepared.table_cursor,
            Self::Virtual(_) => None,
            Self::Materialized(_) => None,
            Self::Registers(_) => None,
        }
    }
}

struct HirBtreeLoop {
    cursor: Option<CursorID>,
    loop_start: BranchOffset,
    /// Reject the current row, then run this loop's advance operation.
    next: BranchOffset,
    /// This access produced no row, so skip its advance operation.
    exhausted: BranchOffset,
    advance: HirLoopAdvance,
    left_join: Option<usize>,
}

struct HirLeftJoin {
    owner: SourceId,
    first_loop: usize,
    last_loop: usize,
    metadata: LeftJoinMetadata,
    null_cursors: Vec<CursorID>,
}

#[derive(Clone, Copy)]
enum HirPredicateKind {
    Join(SourceId),
    Where,
}

fn is_hir_ungrouped_aggregate(block: &hir::QueryBlock) -> bool {
    match &block.body {
        hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } => grouping.keys.is_empty(),
        hir::QueryBlockBody::Select { grouping: None, .. } => block.aggregate_count != 0,
        _ => false,
    }
}

fn hir_grouping(block: &hir::QueryBlock) -> Option<&hir::Grouping> {
    let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    else {
        return None;
    };
    (!grouping.keys.is_empty()).then_some(grouping)
}

struct HirAggregateCollector<'a> {
    block: hir::QueryBlockId,
    aggregates: Vec<Option<&'a hir::FunctionCall>>,
}

struct HirBareAggregateColumnCollector<'a> {
    document: &'a HirDocument,
    columns: Vec<hir::ColumnRef>,
    rowids: Vec<SourceId>,
}

impl<'expr> hir::ExprVisitor<'expr> for HirBareAggregateColumnCollector<'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn child(&mut self, expression: &'expr hir::Expr, index: usize) -> Option<&'expr hir::Expr> {
        if let hir::Expr::Output(output) = expression {
            return (index == 0).then(|| {
                &self
                    .document
                    .output(*output)
                    .expect("validated HIR output reference exists")
                    .expr
            });
        }
        expression.child(index)
    }

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _child_index: usize,
        child: &'expr hir::Expr,
    ) -> Result<ControlFlow<(), ()>> {
        if matches!(
            child,
            hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Aggregate { .. },
                ..
            })
        ) {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        match expression {
            hir::Expr::Column(reference) if !self.columns.contains(reference) => {
                self.columns.push(*reference);
            }
            hir::Expr::RowId(source) if !self.rowids.contains(source) => {
                self.rowids.push(*source);
            }
            _ => {}
        }
        Ok(())
    }
}

impl<'expr> HirBareAggregateColumnCollector<'expr> {
    fn collect(&mut self, expression: &'expr hir::Expr) -> Result<()> {
        if matches!(
            expression,
            hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Aggregate { .. },
                ..
            })
        ) {
            return Ok(());
        }
        expression.walk((), self)
    }
}

impl<'expr> hir::ExprVisitor<'expr> for HirAggregateCollector<'expr> {
    type Context = ();
    type Output = ();
    type Error = LimboError;

    fn pre_order(
        &mut self,
        _parent: &'expr hir::Expr,
        _context: &mut (),
        _child_index: usize,
        _child: &'expr hir::Expr,
    ) -> Result<ControlFlow<(), ()>> {
        Ok(ControlFlow::Continue(()))
    }

    fn post_order(
        &mut self,
        expression: &'expr hir::Expr,
        _context: (),
        _children: &[()],
    ) -> Result<()> {
        let hir::Expr::Function(call) = expression else {
            return Ok(());
        };
        let hir::FunctionEvaluation::Aggregate { id, .. } = call.evaluation else {
            return Ok(());
        };
        if id.block != self.block || id.index >= self.aggregates.len() {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate {id:?} is outside query block {:?}",
                self.block
            )));
        }
        if self.aggregates[id.index].replace(call).is_some() {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate {id:?} has more than one definition"
            )));
        }
        Ok(())
    }
}

fn collect_hir_aggregates<'a>(
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
) -> Result<Vec<&'a hir::FunctionCall>> {
    let mut collector = HirAggregateCollector {
        block: block.id,
        aggregates: (0..block.aggregate_count).map(|_| None).collect(),
    };
    let mut collect = |expression: &'a hir::Expr| expression.walk((), &mut collector);
    for output in &block.outputs {
        collect(&output.expr)?;
    }
    if let hir::QueryBlockBody::Select {
        filter, grouping, ..
    } = &block.body
    {
        if let Some(filter) = filter {
            collect(filter)?;
        }
        if let Some(grouping) = grouping {
            for key in &grouping.keys {
                collect(key)?;
            }
            if let Some(having) = &grouping.having {
                collect(having)?;
            }
        }
    }
    for term in &query.order_by {
        collect(&term.expr)?;
    }
    collector
        .aggregates
        .into_iter()
        .enumerate()
        .map(|(index, aggregate)| {
            aggregate.ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR query block {:?} has no definition for aggregate {index}",
                    block.id
                ))
            })
        })
        .collect()
}

fn initialize_hir_aggregates<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    query: &'a hir::Query,
    block: &'a hir::QueryBlock,
) -> Result<HirAggregateState<'a>> {
    let calls = collect_hir_aggregates(query, block)?;
    let mut aggregates = Vec::with_capacity(calls.len());
    for (index, call) in calls.into_iter().enumerate() {
        let hir::FunctionEvaluation::Aggregate { id, .. } = call.evaluation else {
            unreachable!("aggregate collection only returns aggregate calls")
        };
        if id != hir::AggregateId::new(block.id, index) {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate {id:?} does not match block aggregate {index}"
            )));
        }
        let func = hir_aggregate_function(call)?;
        let percentile_fraction =
            if matches!(func, AggFunc::PercentileCont | AggFunc::PercentileDisc) {
                let hir::FunctionArguments::OrderedSet { direct, .. } = &call.arguments else {
                    return Err(LimboError::InternalError(format!(
                        "HIR percentile aggregate {id:?} has non-ordered-set arguments"
                    )));
                };
                let [fraction] = direct.as_slice() else {
                    return Err(LimboError::InternalError(format!(
                        "HIR percentile aggregate {id:?} does not have one direct argument"
                    )));
                };
                validate_hir_percentile_fraction(document, block, fraction, &func)?;
                let register = program.alloc_register();
                super::expr::translate_expr(program, document, fraction, register)?;
                crate::translate::aggregation::emit_percentile_fraction_range_check(
                    program, register,
                );
                Some(register)
            } else {
                None
            };
        let accumulator = program.alloc_register();
        program.emit_insn(Insn::Null {
            dest: accumulator,
            dest_end: None,
        });
        program.bind_aggregate_result(id, accumulator);
        let distinct = if matches!(call.arguments.distinctness(), Some(Distinctness::Distinct)) {
            let hir::FunctionArguments::Expressions { values, facts, .. } = &call.arguments else {
                return Err(LimboError::InternalError(format!(
                    "HIR DISTINCT aggregate {id:?} has non-expression arguments"
                )));
            };
            if values.len() != 1 || facts.len() != 1 {
                return Err(LimboError::InternalError(format!(
                    "HIR DISTINCT aggregate {id:?} does not have one argument"
                )));
            }
            let hash_table = program.alloc_hash_table_id();
            program.emit_insn(Insn::HashClear {
                hash_table_id: hash_table,
            });
            Some(HirAggregateDistinct {
                hash_table,
                collation: facts[0]
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary),
                on_conflict: program.allocate_label(),
            })
        } else {
            None
        };
        aggregates.push(HirAggregateRuntime {
            call,
            accumulator,
            distinct,
            percentile_fraction,
        });
    }
    let mut bare = HirBareAggregateColumnCollector {
        document,
        columns: Vec::new(),
        rowids: Vec::new(),
    };
    for output in &block.outputs {
        bare.collect(&output.expr)?;
    }
    if let hir::QueryBlockBody::Select {
        grouping: Some(grouping),
        ..
    } = &block.body
    {
        if let Some(having) = &grouping.having {
            bare.collect(having)?;
        }
    }
    for term in &query.order_by {
        bare.collect(&term.expr)?;
    }
    bare.columns
        .sort_unstable_by_key(|reference| (reference.source.index(), reference.column));
    bare.rowids.sort_unstable();
    Ok(HirAggregateState {
        aggregates,
        bare_columns: bare.columns,
        bare_rowids: bare.rowids,
    })
}

fn validate_hir_percentile_fraction(
    document: &HirDocument,
    block: &hir::QueryBlock,
    fraction: &hir::Expr,
    function: &AggFunc,
) -> Result<()> {
    let mut invalid = false;
    fraction.for_each(&mut |expression| {
        invalid |= matches!(expression, hir::Expr::Subquery(_));
    });
    document.visit_expr_sources(fraction, &mut |source| {
        invalid |= matches!(
            document.source(source).map(|source| source.owner),
            Some(hir::SourceOwner::QueryBlock(owner)) if owner == block.id
        );
    });
    if invalid {
        crate::bail_parse_error!(
            "the fraction argument of {}() must be a constant expression that does not depend on the aggregated rows",
            function
        );
    }
    Ok(())
}

fn emit_hir_aggregate_steps(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    state: &HirAggregateState<'_>,
) -> Result<()> {
    for aggregate in &state.aggregates {
        let filter = aggregate.call.evaluation.filter();
        let skip = aggregate
            .distinct
            .as_ref()
            .map(|distinct| distinct.on_conflict)
            .or_else(|| filter.map(|_| program.allocate_label()));
        if let Some(filter) = filter {
            let register = program.alloc_register();
            super::expr::translate_expr(program, document, filter, register)?;
            program.emit_insn(Insn::IfNot {
                reg: register,
                target_pc: skip.expect("aggregate filter has a skip label"),
                jump_if_null: true,
            });
        }
        translate_hir_aggregation_step(
            program,
            document,
            aggregate.call,
            aggregate.accumulator,
            aggregate.distinct.as_ref(),
            aggregate.percentile_fraction,
        )?;
        if let Some(skip) = skip {
            program.preassign_label_to_next_insn(skip);
        }
    }
    Ok(())
}

fn finalize_hir_aggregates(
    program: &mut ProgramBuilder,
    state: &HirAggregateState<'_>,
) -> Result<()> {
    for aggregate in &state.aggregates {
        program.emit_insn(Insn::AggFinal {
            register: aggregate.accumulator,
            func: AccumulatorFunc::Agg(hir_aggregate_function(aggregate.call)?),
        });
    }
    Ok(())
}

fn initialize_hir_aggregate_captures(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block_plan: &HirQueryBlockPlan,
    state: &HirAggregateState<'_>,
) -> Result<(usize, Vec<HirAggregateCapture>)> {
    let first_row = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_row,
    });
    let mut captures = Vec::with_capacity(block_plan.loops.len());
    for planned_loop in &block_plan.loops {
        let mut columns = state
            .bare_columns
            .iter()
            .filter(|reference| reference.source == planned_loop.source)
            .map(|reference| reference.column)
            .collect::<Vec<_>>();
        columns.sort_unstable();
        columns.dedup();
        let needs_rowid = state.bare_rowids.contains(&planned_loop.source);
        if columns.is_empty() && !needs_rowid {
            continue;
        }
        if captures
            .iter()
            .any(|capture: &HirAggregateCapture| capture.source == planned_loop.source)
        {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate query has more than one loop for source {}",
                planned_loop.source
            )));
        }
        let source = document.source(planned_loop.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR aggregate capture references missing source {}",
                planned_loop.source
            ))
        })?;
        let columns_start = if source.columns.is_empty() {
            0
        } else {
            let start = program.alloc_registers(source.columns.len());
            program.emit_insn(Insn::Null {
                dest: start,
                dest_end: (source.columns.len() > 1).then_some(start + source.columns.len() - 1),
            });
            start
        };
        let rowid = needs_rowid.then(|| {
            let register = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: register,
                dest_end: None,
            });
            register
        });
        captures.push(HirAggregateCapture {
            source: source.id,
            columns_start,
            columns,
            rowid,
        });
    }
    Ok((first_row, captures))
}

fn emit_hir_aggregate_capture(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    first_row: usize,
    captures: &[HirAggregateCapture],
) -> Result<()> {
    let captured = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: first_row,
        target_pc: captured,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: first_row,
    });
    for capture in captures {
        let source = document.source(capture.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR aggregate capture references missing source {}",
                capture.source
            ))
        })?;
        for column in &capture.columns {
            super::expr::translate_expr(
                program,
                document,
                &hir::Expr::column(source.id, *column),
                capture.columns_start + column,
            )?;
        }
        if let Some(rowid) = capture.rowid {
            super::expr::translate_expr(program, document, &hir::Expr::rowid(source.id), rowid)?;
        }
    }
    program.preassign_label_to_next_insn(captured);
    Ok(())
}

fn bind_hir_aggregate_captures(program: &mut ProgramBuilder, captures: &[HirAggregateCapture]) {
    for capture in captures {
        program.bind_source(
            capture.source,
            SourceBinding::Registers {
                start: capture.columns_start,
                rowid: capture.rowid,
            },
        );
    }
}

fn emit_btree_loops(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    block: &hir::QueryBlock,
    block_plan: &HirQueryBlockPlan,
    body: HirLoopBody<'_>,
) -> Result<()> {
    let document = &plan.document;
    if block.window_function_count != 0 || !block.windows.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} is not a supported B-tree loop",
            block.id
        )));
    }
    let hir::QueryBlockBody::Select { grouping, .. } = &block.body else {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} has unsupported B-tree loop clauses",
            block.id
        )));
    };
    match (&body, grouping) {
        (HirLoopBody::Rows(_), None) if block.aggregate_count == 0 => {}
        (HirLoopBody::UngroupedAggregate(_), None) if block.aggregate_count != 0 => {}
        (HirLoopBody::UngroupedAggregate(_), Some(grouping)) if grouping.keys.is_empty() => {}
        (HirLoopBody::Group(_), Some(grouping)) if !grouping.keys.is_empty() => {}
        _ => {
            return Err(LimboError::InternalError(format!(
                "HIR query block {:?} has a mismatched loop body",
                block.id
            )))
        }
    }
    if block_plan.loops.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} does not have a source loop",
            block.id
        )));
    }
    let from = block.from.as_ref().expect("FROM query block has a source");
    let mut left_join_owners = Vec::new();
    if !supported_btree_from(document, from, &mut left_join_owners) {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} contains an unsupported join",
            block.id
        )));
    }
    if block_plan.predicates.iter().any(|predicate| {
        predicate
            .from_outer_join
            .is_some_and(|source| !left_join_owners.contains(&source))
    }) {
        return Err(LimboError::InternalError(format!(
            "HIR query block {:?} contains an unsupported outer-join predicate owner",
            block.id
        )));
    }

    let block_done = program.allocate_label();
    let prepared_loops = block_plan
        .loops
        .iter()
        .map(|planned_loop| prepare_hir_loop(program, plan, planned_loop))
        .collect::<Result<Vec<_>>>()?;
    let aggregate_captures = matches!(&body, HirLoopBody::UngroupedAggregate(_))
        .then(|| {
            let HirLoopBody::UngroupedAggregate(state) = &body else {
                unreachable!()
            };
            initialize_hir_aggregate_captures(program, document, block_plan, state)
        })
        .transpose()?;
    let left_joins =
        prepare_hir_left_joins(program, block_plan, &prepared_loops, &left_join_owners)?;

    emit_ready_predicates(
        program,
        document,
        block_plan,
        None,
        block_done,
        HirPredicateKind::Where,
        &left_joins,
    )?;
    let mut loops = Vec::with_capacity(prepared_loops.len());
    for (loop_index, prepared_loop) in prepared_loops.iter().enumerate() {
        let mut starting_left_joins = left_joins
            .iter()
            .enumerate()
            .filter(|(_, left_join)| left_join.first_loop == loop_index);
        let left_join = starting_left_joins.next().map(|(index, left_join)| {
            left_join.metadata.reset(program);
            index
        });
        if starting_left_joins.next().is_some() {
            return Err(LimboError::InternalError(format!(
                "HIR loop {loop_index} starts overlapping LEFT JOIN boundaries"
            )));
        }
        let hir_loop = start_hir_loop(program, document, prepared_loop, left_join)?;
        for left_join in left_joins
            .iter()
            .filter(|left_join| left_join.last_loop == loop_index)
        {
            emit_ready_predicates(
                program,
                document,
                block_plan,
                Some(loop_index),
                hir_loop.next,
                HirPredicateKind::Join(left_join.owner),
                &left_joins,
            )?;
            left_join.metadata.mark_matched(program);
        }
        emit_ready_predicates(
            program,
            document,
            block_plan,
            Some(loop_index),
            hir_loop.next,
            HirPredicateKind::Where,
            &left_joins,
        )?;
        loops.push(hir_loop);
    }

    let innermost_next = loops.last().expect("loop list is non-empty").next;
    match body {
        HirLoopBody::Rows(output) => {
            let start = query_output_registers(program, &block.outputs, output.needs_values())?;
            emit_query_row(
                program,
                document,
                output,
                &block.outputs,
                block.outputs.iter().map(|output| &output.expr),
                start,
                innermost_next,
            )?;
        }
        HirLoopBody::UngroupedAggregate(state) => {
            let (first_row, captures) = aggregate_captures
                .as_ref()
                .expect("aggregate loop initializes first-row captures");
            emit_hir_aggregate_capture(program, document, *first_row, captures)?;
            emit_hir_aggregate_steps(program, document, state)?;
        }
        HirLoopBody::Group(runtime) => {
            emit_hir_group_input(program, document, runtime)?;
        }
    }

    for hir_loop in loops.iter().rev() {
        close_hir_btree_loop(program, hir_loop, &left_joins);
    }
    program.preassign_label_to_next_insn(block_done);
    if let Some((_, captures)) = aggregate_captures {
        bind_hir_aggregate_captures(program, &captures);
    }
    Ok(())
}

fn supported_btree_from(
    document: &HirDocument,
    from: &hir::From,
    left_join_owners: &mut Vec<SourceId>,
) -> bool {
    let Some(first) = document.source(from.first) else {
        return false;
    };
    if let hir::SourceKind::FromGroup(group) = &first.kind {
        if !supported_btree_from(document, &group.from, left_join_owners) {
            return false;
        }
    }
    for join in &from.joins {
        let Some(source) = document.source(join.right) else {
            return false;
        };
        match join.kind {
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross => {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    if !supported_btree_from(document, &group.from, left_join_owners) {
                        return false;
                    }
                }
            }
            hir::JoinKind::Left => {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    if !ordinary_btree_from(document, &group.from) {
                        return false;
                    }
                }
                left_join_owners.push(join.right);
            }
            hir::JoinKind::Right | hir::JoinKind::Full => return false,
        }
    }
    true
}

fn ordinary_btree_from(document: &HirDocument, from: &hir::From) -> bool {
    from.joins.iter().all(|join| {
        matches!(
            join.kind,
            hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross
        )
    }) && std::iter::once(from.first)
        .chain(from.joins.iter().map(|join| join.right))
        .all(|source| {
            document.source(source).is_some_and(|source| {
                if let hir::SourceKind::FromGroup(group) = &source.kind {
                    ordinary_btree_from(document, &group.from)
                } else {
                    true
                }
            })
        })
}

fn prepare_hir_left_joins(
    program: &mut ProgramBuilder,
    block_plan: &HirQueryBlockPlan,
    prepared_loops: &[PreparedHirLoop<'_>],
    owners: &[SourceId],
) -> Result<Vec<HirLeftJoin>> {
    owners
        .iter()
        .map(|owner| {
            let loop_indices = if let Some(group) = block_plan
                .groups
                .iter()
                .find(|group| group.source == *owner)
            {
                block_plan
                    .loops
                    .iter()
                    .enumerate()
                    .filter_map(|(loop_index, planned_loop)| {
                        group
                            .source_range
                            .contains(&planned_loop.source_position)
                            .then_some(loop_index)
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![block_plan
                    .loops
                    .iter()
                    .position(|planned_loop| planned_loop.source == *owner)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR LEFT JOIN owner {owner} has no physical loop"
                        ))
                    })?]
            };
            let Some((&first_loop, &last_loop)) = loop_indices.first().zip(loop_indices.last())
            else {
                return Err(LimboError::InternalError(format!(
                    "HIR LEFT JOIN group {owner} has no physical loops"
                )));
            };
            if loop_indices.len() != last_loop - first_loop + 1 {
                return Err(LimboError::InternalError(format!(
                    "HIR LEFT JOIN group {owner} is not contiguous in physical loop order"
                )));
            }
            let mut null_cursors = Vec::new();
            for prepared in &prepared_loops[first_loop..=last_loop] {
                if let Some(cursor) = prepared.cursor() {
                    null_cursors.push(cursor);
                }
                if let Some(table_cursor) = prepared.table_cursor() {
                    null_cursors.push(table_cursor);
                }
            }
            Ok(HirLeftJoin {
                owner: *owner,
                first_loop,
                last_loop,
                metadata: LeftJoinMetadata::new(program),
                null_cursors,
            })
        })
        .collect()
}

fn prepare_hir_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirLoop<'a>> {
    match &planned_loop.access {
        HirSourceAccess::BTree { .. } => Ok(PreparedHirLoop::BTree(prepare_hir_btree_loop(
            program,
            &plan.document,
            planned_loop,
        )?)),
        HirSourceAccess::Virtual(operation) => Ok(PreparedHirLoop::Virtual(
            prepare_hir_virtual_loop(program, &plan.document, planned_loop.source, operation)?,
        )),
        HirSourceAccess::Derived { query } => Ok(PreparedHirLoop::Materialized(
            prepare_hir_materialized_loop(
                program,
                plan,
                planned_loop.source,
                HirMaterializedQuery::Derived(*query),
            )?,
        )),
        HirSourceAccess::Cte {
            cte,
            query,
            materialization: HirCteMaterialization::PerReference,
        } => Ok(PreparedHirLoop::Materialized(
            prepare_hir_materialized_loop(
                program,
                plan,
                planned_loop.source,
                HirMaterializedQuery::Cte {
                    cte: *cte,
                    query: *query,
                },
            )?,
        )),
        HirSourceAccess::Cte {
            cte,
            query,
            materialization: HirCteMaterialization::Shared | HirCteMaterialization::Explicit,
        } => Ok(PreparedHirLoop::Materialized(prepare_hir_shared_cte_loop(
            program,
            plan,
            planned_loop.source,
            *cte,
            *query,
        )?)),
        HirSourceAccess::RecursiveCte { cte } => Ok(PreparedHirLoop::Materialized(
            prepare_hir_recursive_cte_loop(program, plan, planned_loop.source, *cte)?,
        )),
        HirSourceAccess::RecursiveInput { cte } => Ok(PreparedHirLoop::Registers(
            prepare_hir_recursive_input_loop(program, &plan.document, planned_loop.source, *cte)?,
        )),
    }
}

fn prepare_hir_recursive_input_loop<'a>(
    program: &ProgramBuilder,
    document: &'a HirDocument,
    source_id: SourceId,
    cte: hir::CteId,
) -> Result<PreparedHirRegistersLoop<'a>> {
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR recursive input references missing source {source_id}"
        ))
    })?;
    if !matches!(source.kind, hir::SourceKind::RecursiveInput(source_cte) if source_cte == cte) {
        return Err(LimboError::InternalError(format!(
            "HIR recursive input source {source_id} does not match CTE {cte}"
        )));
    }
    if !matches!(
        program.source_binding(source_id),
        Some(SourceBinding::Registers { .. })
    ) {
        return Err(LimboError::InternalError(format!(
            "HIR recursive input source {source_id} has no row binding"
        )));
    }
    Ok(PreparedHirRegistersLoop { source })
}

fn prepare_hir_virtual_loop<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    source_id: SourceId,
    operation: &'a HirVirtualTableOperation,
) -> Result<PreparedHirVirtualLoop<'a>> {
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR virtual-table loop references missing source {source_id}"
        ))
    })?;
    let table = match &source.kind {
        hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => table,
        _ => {
            return Err(LimboError::InternalError(format!(
                "HIR virtual-table loop source {} is not a table",
                source.id
            )));
        }
    };
    let Table::Virtual(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR virtual-table loop source {} is not a virtual table",
            source.id
        )));
    };
    let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table.clone()));
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    program.bind_source(source.id, SourceBinding::Virtual { cursor });
    Ok(PreparedHirVirtualLoop {
        source,
        operation,
        cursor,
    })
}

fn prepare_hir_shared_cte_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    cte: hir::CteId,
    query: QueryId,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let materialized_query = HirMaterializedQuery::Cte { cte, query };
    let source = validate_hir_materialized_query(plan, source_id, materialized_query)?;
    if let Some(shared) = program.hir_materialized_cte(cte).cloned() {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(shared.table.clone()));
        program.emit_insn(Insn::OpenDup {
            new_cursor_id: cursor,
            original_cursor_id: shared.cursor_id,
        });
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        return Ok(PreparedHirMaterializedLoop {
            source,
            cursor,
            table: shared.table,
        });
    }

    let prepared = prepare_hir_materialized_loop(program, plan, source_id, materialized_query)?;
    program.register_hir_materialized_cte(
        cte,
        MaterializedCteInfo {
            cursor_id: prepared.cursor,
            table: prepared.table.clone(),
            num_columns: prepared.source.columns.len(),
        },
    );
    Ok(prepared)
}

fn prepare_hir_recursive_cte_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    cte_id: hir::CteId,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let document = &plan.document;
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR recursive CTE loop references missing source {source_id}"
        ))
    })?;
    if !matches!(source.kind, hir::SourceKind::Cte(source_cte) if source_cte == cte_id) {
        return Err(LimboError::InternalError(format!(
            "HIR recursive CTE source {source_id} does not match CTE {cte_id}"
        )));
    }
    let cte = document.cte(cte_id).ok_or_else(|| {
        LimboError::InternalError(format!("HIR references missing recursive CTE {cte_id}"))
    })?;
    let hir::CteBody::Recursive(recursive) = &cte.body else {
        return Err(LimboError::InternalError(format!(
            "HIR CTE {cte_id} is not recursive"
        )));
    };
    if source.columns.len() != cte.columns.len() {
        return Err(LimboError::InternalError(format!(
            "HIR recursive CTE source {source_id} width does not match CTE {cte_id}"
        )));
    }
    if let Some(shared) = program.hir_materialized_cte(cte_id).cloned() {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(shared.table.clone()));
        program.emit_insn(Insn::OpenDup {
            new_cursor_id: cursor,
            original_cursor_id: shared.cursor_id,
        });
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        return Ok(PreparedHirMaterializedLoop {
            source,
            cursor,
            table: shared.table,
        });
    }

    let table = hir_materialized_table(source);
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let materialized = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: materialized,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_hir_recursive_cte(
        program,
        plan,
        cte,
        recursive,
        &QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        },
    )?;
    program.preassign_label_to_next_insn(materialized);
    program.bind_source(
        source_id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    program.register_hir_materialized_cte(
        cte_id,
        MaterializedCteInfo {
            cursor_id: cursor,
            table: table.clone(),
            num_columns: source.columns.len(),
        },
    );
    Ok(PreparedHirMaterializedLoop {
        source,
        cursor,
        table,
    })
}

fn emit_hir_recursive_cte(
    program: &mut ProgramBuilder,
    plan: &HirPlan,
    cte: &hir::Cte,
    recursive: &hir::RecursiveCte,
    destination: &QueryDestination,
) -> Result<usize> {
    let union_all = match recursive.arms.first().map(|arm| arm.operator) {
        Some(CompoundOperator::UnionAll) => true,
        Some(CompoundOperator::Union) => false,
        _ => {
            return Err(LimboError::InternalError(format!(
                "recursive CTE {} does not use UNION or UNION ALL",
                cte.id
            )));
        }
    };
    if recursive.arms.iter().any(|arm| {
        arm.operator
            != if union_all {
                CompoundOperator::UnionAll
            } else {
                CompoundOperator::Union
            }
    }) {
        return Err(LimboError::InternalError(format!(
            "recursive CTE {} mixes recursive operators",
            cte.id
        )));
    }
    let query_is_uncorrelated = |query_id| {
        plan.document
            .query(query_id)
            .is_some_and(|query| query.captures.is_empty())
    };
    if !query_is_uncorrelated(recursive.seed)
        || recursive
            .arms
            .iter()
            .any(|arm| !query_is_uncorrelated(arm.query))
    {
        return Err(LimboError::InternalError(format!(
            "HIR recursive CTE {} is correlated",
            cte.id
        )));
    }

    let comparison_collations = recursive
        .comparison_collations
        .iter()
        .map(|collation| collation.as_ref().map(|collation| *collation.value()))
        .collect::<Vec<_>>();
    let queue_order = recursive
        .queue_order
        .iter()
        .map(|term| RecursiveRuntimeOrder {
            result_column: term.output,
            order: term.order,
            nulls: term.nulls,
            explicit_collation: term
                .explicit_collation
                .as_ref()
                .map(|collation| *collation.value()),
        })
        .collect::<Vec<_>>();
    let spec = RecursiveRuntimeSpec {
        name: &cte.name,
        result_columns: cte.columns.len(),
        union_all,
        comparison_collations: &comparison_collations,
        queue_order: &queue_order,
    };
    emit_recursive_runtime(
        program,
        &spec,
        destination,
        |program, done| {
            initialize_limit(program, &plan.document, recursive.limit.as_ref(), done).map(|limit| {
                RecursiveRuntimeLimit {
                    limit: limit.map(|limit| limit.limit),
                    offset: limit.and_then(|limit| limit.offset),
                }
            })
        },
        |program, input_start| {
            for source in &recursive.input_sources {
                program.bind_source(
                    *source,
                    SourceBinding::Registers {
                        start: input_start,
                        rowid: None,
                    },
                );
            }
            Ok(())
        },
        |program, phase, queue_destination| match phase {
            RecursivePhase::Seed => {
                emit_planned_query_body(program, plan, recursive.seed, queue_destination)
            }
            RecursivePhase::Step => {
                for arm in &recursive.arms {
                    emit_planned_query_body(program, plan, arm.query, queue_destination)?;
                }
                Ok(())
            }
        },
    )
}

fn validate_hir_materialized_query<'a>(
    plan: &'a HirPlan,
    source_id: SourceId,
    materialized_query: HirMaterializedQuery,
) -> Result<&'a hir::Source> {
    let document = &plan.document;
    let query_id = materialized_query.query();
    let source = document.source(source_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR materialized loop references missing source {source_id}"
        ))
    })?;
    let source_matches = match materialized_query {
        HirMaterializedQuery::Derived(query) => {
            matches!(source.kind, hir::SourceKind::Derived(source_query) if source_query == query)
        }
        HirMaterializedQuery::Cte { cte, .. } => {
            matches!(source.kind, hir::SourceKind::Cte(source_cte) if source_cte == cte)
        }
    };
    if !source_matches {
        return Err(LimboError::InternalError(format!(
            "HIR materialized loop source {source_id} does not match query {query_id}"
        )));
    }
    let query = document.query(query_id).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR materialized source {source_id} references missing query {query_id}"
        ))
    })?;
    if !query.captures.is_empty() {
        return Err(LimboError::InternalError(format!(
            "HIR materialized source {source_id} is correlated"
        )));
    }
    if source.columns.len() != query.output.len() {
        return Err(LimboError::InternalError(format!(
            "HIR materialized source {source_id} width {} does not match query {query_id} width {}",
            source.columns.len(),
            query.output.len()
        )));
    }
    Ok(source)
}

fn prepare_hir_materialized_loop<'a>(
    program: &mut ProgramBuilder,
    plan: &'a HirPlan,
    source_id: SourceId,
    materialized_query: HirMaterializedQuery,
) -> Result<PreparedHirMaterializedLoop<'a>> {
    let query_id = materialized_query.query();
    let source = validate_hir_materialized_query(plan, source_id, materialized_query)?;

    let table = hir_materialized_table(source);
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    let materialized = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: materialized,
    });
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_planned_query_body(
        program,
        plan,
        query_id,
        &QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        },
    )?;
    program.preassign_label_to_next_insn(materialized);
    program.bind_source(
        source_id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor: None,
        },
    );
    Ok(PreparedHirMaterializedLoop {
        source,
        cursor,
        table,
    })
}

fn hir_materialized_table(source: &hir::Source) -> Arc<BTreeTable> {
    let columns = source
        .columns
        .iter()
        .map(|column| {
            let storage = column.type_fact.storage.unwrap_or(Type::Null);
            let ty_str = match column.affinity {
                Affinity::Blob => "BLOB",
                Affinity::Text => "TEXT",
                Affinity::Numeric => "NUMERIC",
                Affinity::Integer => "INTEGER",
                Affinity::Real => "REAL",
                Affinity::None => "",
            };
            Column::new(
                Some(column.name.clone()),
                ty_str.to_string(),
                None,
                None,
                storage,
                column
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value()),
                ColDef {
                    hidden: column.hidden,
                    ..ColDef::default()
                },
            )
        })
        .collect();
    Arc::new(BTreeTable::new(
        0,
        source.name.clone(),
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    ))
}

fn prepare_hir_btree_loop<'a>(
    program: &mut ProgramBuilder,
    document: &'a HirDocument,
    planned_loop: &'a HirPlannedLoop,
) -> Result<PreparedHirBtreeLoop<'a>> {
    let HirSourceAccess::BTree {
        operation,
        table_lookup,
    } = &planned_loop.access
    else {
        return Err(LimboError::InternalError(format!(
            "HIR source {} does not use a B-tree loop",
            planned_loop.source
        )));
    };
    let index = match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_ref(),
        HirBtreeOperation::RowidEq { .. } => None,
    };
    if index.is_some_and(|index| index.ephemeral)
        && !matches!(operation, HirBtreeOperation::Seek { .. })
    {
        return Err(LimboError::InternalError(
            "HIR automatic indexes require a seek operation".to_string(),
        ));
    }
    let source = document.source(planned_loop.source).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop references missing source {}",
            planned_loop.source
        ))
    })?;
    let hir::SourceKind::Table(table) = &source.kind else {
        return Err(LimboError::InternalError(format!(
            "HIR B-tree loop source {} is not a table",
            source.id
        )));
    };
    let Table::BTree(table) = table.value() else {
        return Err(LimboError::InternalError(format!(
            "HIR B-tree loop source {} is not a B-tree table",
            source.id
        )));
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop source {} has no database",
            source.id
        ))
    })?;
    let database_snapshot = document.database(database).ok_or_else(|| {
        LimboError::InternalError(format!(
            "HIR B-tree loop source {} references missing database {database:?}",
            source.id
        ))
    })?;
    program.begin_read_on_database(database.index(), database_snapshot.schema_version)?;
    let (cursor, table_cursor, autoindex_source_cursor) = if let Some(index) = index {
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        let table_cursor = match table_lookup {
            BtreeTableLookup::ScanOnly => None,
            BtreeTableLookup::TableRequired => {
                Some(program.alloc_cursor_id(CursorType::BTreeTable(table.clone())))
            }
        };
        let autoindex_source_cursor =
            if index.ephemeral {
                Some(table_cursor.unwrap_or_else(|| {
                    program.alloc_cursor_id(CursorType::BTreeTable(table.clone()))
                }))
            } else {
                None
            };
        (index_cursor, table_cursor, autoindex_source_cursor)
    } else {
        if *table_lookup != BtreeTableLookup::ScanOnly {
            return Err(LimboError::InternalError(format!(
                "HIR table scan source {} unexpectedly requires a second table cursor",
                source.id
            )));
        }
        (
            program.alloc_cursor_id(CursorType::BTreeTable(table.clone())),
            None,
            None,
        )
    };
    program.bind_source(
        source.id,
        SourceBinding::BTree {
            scan_cursor: cursor,
            table_cursor,
        },
    );
    if index.is_none_or(|index| !index.ephemeral) {
        program.emit_insn(Insn::OpenRead {
            cursor_id: cursor,
            root_page: index.map_or(table.root_page, |index| index.root_page),
            db: database.index(),
        });
    }
    if let Some(source_cursor) = autoindex_source_cursor.or(table_cursor) {
        program.emit_insn(Insn::OpenRead {
            cursor_id: source_cursor,
            root_page: table.root_page,
            db: database.index(),
        });
    }

    Ok(PreparedHirBtreeLoop {
        source,
        operation,
        index,
        cursor,
        table_cursor,
        autoindex_source_cursor,
        table_has_rowid: table.has_rowid,
    })
}

fn build_hir_autoindex(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirBtreeLoop<'_>,
    seek_def: &HirSeekDef,
) -> Result<bool> {
    let Some(index) = prepared.index.filter(|index| index.ephemeral) else {
        return Ok(false);
    };
    let table_cursor = prepared
        .autoindex_source_cursor
        .expect("automatic index has a source table cursor");
    let num_seek_keys = seek_def.size(&seek_def.start);
    let lowering = HirSeekExpressionLowering {
        document,
        source: prepared.source,
    };
    let has_null_matching_key = seek_def.prefix.iter().take(num_seek_keys).any(|component| {
        component
            .eq
            .as_ref()
            .is_some_and(|(operator, expression, _)| {
                lowering.is_null_matching(*operator, expression)
            })
    });
    let affinity_str = plan::synthesized_seek_affinity_str(index, seek_def);

    // Generated expressions must read the base-table row while the automatic
    // index is being populated. Restore the planned index binding before the seek.
    program.bind_source(
        prepared.source.id,
        SourceBinding::BTree {
            scan_cursor: table_cursor,
            table_cursor: None,
        },
    );
    let result = emit_autoindex(
        program,
        AutoIndexBuild {
            index,
            table_cursor_id: table_cursor,
            index_cursor_id: prepared.cursor,
            table_has_rowid: prepared.table_has_rowid,
            num_seek_keys,
            has_null_matching_key,
            seek_def,
            affinity_str: affinity_str.as_ref(),
        },
        |program, column, target| {
            let Some(expression) = prepared.source.generated_expressions.get(column) else {
                return Err(LimboError::InternalError(format!(
                    "automatic index column {column} is outside source {}",
                    prepared.source.id
                )));
            };
            match expression {
                hir::ColumnReadExpression::Absent => {
                    program.emit_column_or_rowid(table_cursor, column, target);
                    Ok(())
                }
                hir::ColumnReadExpression::Planned(expression) => {
                    super::expr::translate_expr_no_constant_opt(
                        program, document, expression, target,
                    )?;
                    Ok(())
                }
                hir::ColumnReadExpression::NotRequired => Err(LimboError::InternalError(format!(
                    "automatic index requires unplanned generated column {column} from source {}",
                    prepared.source.id
                ))),
            }
        },
    );
    program.bind_source(
        prepared.source.id,
        SourceBinding::BTree {
            scan_cursor: prepared.cursor,
            table_cursor: prepared.table_cursor,
        },
    );
    Ok(result?.use_bloom_filter)
}

fn start_hir_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    match prepared {
        PreparedHirLoop::BTree(prepared) => {
            start_hir_btree_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::Virtual(prepared) => {
            start_hir_virtual_loop(program, document, prepared, left_join)
        }
        PreparedHirLoop::Registers(prepared) => {
            if !matches!(
                program.source_binding(prepared.source.id),
                Some(SourceBinding::Registers { .. })
            ) {
                return Err(LimboError::InternalError(format!(
                    "HIR register source {} lost its row binding",
                    prepared.source.id
                )));
            }
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            program.preassign_label_to_next_insn(loop_start);
            Ok(HirBtreeLoop {
                cursor: None,
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::None,
                left_join,
            })
        }
        PreparedHirLoop::Materialized(prepared) => {
            if !matches!(
                program.source_binding(prepared.source.id),
                Some(SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                }) if *scan_cursor == prepared.cursor
            ) {
                return Err(LimboError::InternalError(format!(
                    "HIR materialized source {} lost its cursor binding",
                    prepared.source.id
                )));
            }
            let loop_start = program.allocate_label();
            let next = program.allocate_label();
            let exhausted = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: prepared.cursor,
                pc_if_empty: exhausted,
            });
            program.preassign_label_to_next_insn(loop_start);
            Ok(HirBtreeLoop {
                cursor: Some(prepared.cursor),
                loop_start,
                next,
                exhausted,
                advance: HirLoopAdvance::Cursor {
                    direction: IterationDirection::Forwards,
                    fullscan: true,
                },
                left_join,
            })
        }
    }
}

fn start_hir_virtual_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirVirtualLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    if !matches!(
        program.source_binding(prepared.source.id),
        Some(SourceBinding::Virtual { cursor }) if *cursor == prepared.cursor
    ) {
        return Err(LimboError::InternalError(format!(
            "HIR virtual-table source {} lost its cursor binding",
            prepared.source.id
        )));
    }
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let args_reg = program.alloc_registers(prepared.operation.arguments.len());
    for (index, argument) in prepared.operation.arguments.iter().enumerate() {
        super::expr::translate_expr(program, document, argument, args_reg + index)?;
    }
    let idx_str = prepared.operation.idx_str.as_ref().map(|value| {
        let register = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: register,
            value: value.clone(),
        });
        register
    });
    program.emit_insn(Insn::VFilter {
        cursor_id: prepared.cursor,
        pc_if_empty: exhausted,
        arg_count: prepared.operation.arguments.len(),
        args_reg,
        idx_str,
        idx_num: prepared.operation.idx_num as usize,
    });
    program.preassign_label_to_next_insn(loop_start);
    Ok(HirBtreeLoop {
        cursor: Some(prepared.cursor),
        loop_start,
        next,
        exhausted,
        advance: HirLoopAdvance::Virtual,
        left_join,
    })
}

fn start_hir_btree_loop(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    prepared: &PreparedHirBtreeLoop<'_>,
    left_join: Option<usize>,
) -> Result<HirBtreeLoop> {
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let exhausted = program.allocate_label();
    let advance = match prepared.operation {
        HirBtreeOperation::Scan { iter_dir, .. } => {
            match iter_dir {
                IterationDirection::Forwards => program.emit_insn(Insn::Rewind {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
                IterationDirection::Backwards => program.emit_insn(Insn::Last {
                    cursor_id: prepared.cursor,
                    pc_if_empty: exhausted,
                }),
            }
            program.preassign_label_to_next_insn(loop_start);
            HirLoopAdvance::Cursor {
                direction: *iter_dir,
                fullscan: true,
            }
        }
        HirBtreeOperation::RowidEq { cmp_expr } => {
            let key = program.alloc_register();
            super::expr::translate_expr(program, document, cmp_expr, key)?;
            program.emit_insn(Insn::SeekRowid {
                cursor_id: prepared.cursor,
                src_reg: key,
                target_pc: exhausted,
            });
            HirLoopAdvance::None
        }
        HirBtreeOperation::Seek { seek_def, .. } => {
            let use_bloom_filter = build_hir_autoindex(program, document, prepared, seek_def)?;
            let key_registers = seek_def
                .size(&seek_def.start)
                .max(seek_def.size(&seek_def.end));
            let start_register = program.alloc_registers(key_registers);
            SeekEmitter::with_lowering(
                program,
                seek_def,
                HirSeekExpressionLowering {
                    document,
                    source: prepared.source,
                },
                prepared.cursor,
                start_register,
                exhausted,
                prepared.index,
            )
            .emit(loop_start, use_bloom_filter)?;
            HirLoopAdvance::Cursor {
                direction: seek_def.iter_dir,
                fullscan: false,
            }
        }
        HirBtreeOperation::InSeek { source, .. } => {
            let source_cursor =
                open_hir_in_seek_source_cursor(program, document, prepared.index, source)?;
            let state = emit_in_seek_start(
                program,
                source_cursor,
                prepared.cursor,
                prepared.table_cursor,
                prepared.index.is_some(),
                loop_start,
                exhausted,
            );
            HirLoopAdvance::InSeek {
                state,
                index_backed: prepared.index.is_some(),
            }
        }
    };
    if !matches!(prepared.operation, HirBtreeOperation::InSeek { .. }) {
        if let Some(table_cursor) = prepared.table_cursor {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id: prepared.cursor,
                table_cursor_id: table_cursor,
            });
        }
    }
    Ok(HirBtreeLoop {
        cursor: Some(prepared.cursor),
        loop_start,
        next,
        exhausted,
        advance,
        left_join,
    })
}

fn emit_ready_predicates(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    block_plan: &HirQueryBlockPlan,
    loop_index: Option<usize>,
    fail: BranchOffset,
    kind: HirPredicateKind,
    left_joins: &[HirLeftJoin],
) -> Result<()> {
    for predicate in block_plan.predicates.iter().filter(|term| !term.consumed) {
        let selected = match kind {
            HirPredicateKind::Join(source) => predicate.from_outer_join == Some(source),
            HirPredicateKind::Where => predicate.from_outer_join.is_none(),
        };
        if !selected {
            continue;
        }
        let references_ready_at =
            predicate_loop(program, document, &predicate.expr, &block_plan.loops)?;
        let evaluate_at = match kind {
            HirPredicateKind::Join(source) => {
                let owner = left_joins
                    .iter()
                    .find(|left_join| left_join.owner == source)
                    .map(|left_join| left_join.last_loop)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR outer-join predicate owner {source} has no LEFT JOIN boundary"
                        ))
                    })?;
                if references_ready_at.is_some_and(|ready| ready > owner) {
                    return Err(LimboError::InternalError(format!(
                        "HIR outer-join predicate owned by {source} references a later loop"
                    )));
                }
                Some(owner)
            }
            HirPredicateKind::Where => references_ready_at.map(|ready| {
                left_joins.iter().fold(ready, |ready, left_join| {
                    if left_join.first_loop <= ready && ready <= left_join.last_loop {
                        left_join.last_loop
                    } else {
                        ready
                    }
                })
            }),
        };
        if evaluate_at != loop_index {
            continue;
        }
        let passed = program.allocate_label();
        super::expr::translate_condition_expr(
            program,
            document,
            &predicate.expr,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: passed,
                jump_target_when_false: fail,
                jump_target_when_null: fail,
            },
        )?;
        program.preassign_label_to_next_insn(passed);
    }
    Ok(())
}

fn predicate_loop(
    program: &ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    loops: &[HirPlannedLoop],
) -> Result<Option<usize>> {
    let mut eval_at = None;
    let mut missing = None;
    let mut missing_query = None;
    let mut record_source = |source| {
        if let Some(position) = loops.iter().position(|hir_loop| hir_loop.source == source) {
            eval_at = Some(eval_at.map_or(position, |current: usize| current.max(position)));
        } else if program.source_binding(source).is_none() {
            missing = Some(source);
        }
    };
    expression.for_each(&mut |expression| match expression {
        hir::Expr::Column(reference) => record_source(reference.source),
        hir::Expr::RowId(source) => record_source(*source),
        hir::Expr::Subquery(subquery) => {
            let query = match subquery {
                SubqueryExpr::Scalar { query, .. }
                | SubqueryExpr::Row { query }
                | SubqueryExpr::In { query, .. }
                | SubqueryExpr::Exists(query) => *query,
            };
            if let Some(query) = document.query(query) {
                for source in &query.captures {
                    record_source(*source);
                }
            } else {
                missing_query = Some(query);
            }
        }
        _ => {}
    });
    if let Some(query) = missing_query {
        return Err(LimboError::InternalError(format!(
            "HIR predicate references missing subquery {query}"
        )));
    }
    if let Some(source) = missing {
        return Err(LimboError::InternalError(format!(
            "HIR predicate references source {source} outside its loop plan"
        )));
    }
    Ok(eval_at)
}

fn close_hir_btree_loop(
    program: &mut ProgramBuilder,
    hir_loop: &HirBtreeLoop,
    left_joins: &[HirLeftJoin],
) {
    program.preassign_label_to_next_insn(hir_loop.next);
    match &hir_loop.advance {
        HirLoopAdvance::None => {}
        HirLoopAdvance::Cursor {
            direction,
            fullscan,
        } => {
            let cursor = hir_loop
                .cursor
                .expect("cursor advance has a physical cursor");
            match direction {
                IterationDirection::Forwards => program.emit_insn(Insn::Next {
                    cursor_id: cursor,
                    pc_if_next: hir_loop.loop_start,
                    fullscan: *fullscan,
                }),
                IterationDirection::Backwards => program.emit_insn(Insn::Prev {
                    cursor_id: cursor,
                    pc_if_prev: hir_loop.loop_start,
                    fullscan: *fullscan,
                }),
            }
        }
        HirLoopAdvance::InSeek {
            state,
            index_backed,
        } => emit_in_seek_advance(
            program,
            state,
            hir_loop
                .cursor
                .expect("IN seek advance has a physical cursor"),
            *index_backed,
            hir_loop.loop_start,
        ),
        HirLoopAdvance::Virtual => program.emit_insn(Insn::VNext {
            cursor_id: hir_loop
                .cursor
                .expect("virtual advance has a physical cursor"),
            pc_if_next: hir_loop.loop_start,
        }),
    }
    program.preassign_label_to_next_insn(hir_loop.exhausted);
    if let Some(left_join) = hir_loop.left_join.map(|index| &left_joins[index]) {
        let finished = left_join.metadata.begin_unmatched_row(program);
        for cursor in &left_join.null_cursors {
            program.emit_insn(Insn::NullRow { cursor_id: *cursor });
        }
        left_join.metadata.finish_unmatched_row(program, finished);
    }
}

fn initialize_hir_distinct(
    program: &mut ProgramBuilder,
    block: &hir::QueryBlock,
) -> Result<Option<HirDistinctOutput>> {
    let hir::QueryBlockBody::Select { distinctness, .. } = &block.body else {
        return Ok(None);
    };
    if !matches!(distinctness, Some(Distinctness::Distinct)) {
        return Ok(None);
    }
    if block.outputs.is_empty() {
        return Err(LimboError::InternalError(format!(
            "DISTINCT HIR query block {:?} has no outputs",
            block.id
        )));
    }
    let hash_table = program.alloc_hash_table_id();
    program.emit_insn(Insn::HashClear {
        hash_table_id: hash_table,
    });
    emit_explain!(program, false, EqpDetail::Distinct);
    Ok(Some(HirDistinctOutput {
        hash_table,
        collations: block
            .outputs
            .iter()
            .map(|output| {
                output
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary)
            })
            .collect(),
    }))
}

fn initialize_hir_sorter<'a>(
    program: &mut ProgramBuilder,
    query: &'a hir::Query,
    block: &hir::QueryBlock,
) -> Result<HirSorterOutput<'a>> {
    if query.order_by.is_empty() || block.outputs.is_empty() {
        return Err(LimboError::InternalError(format!(
            "ordered HIR query block {:?} has no ordering or outputs",
            block.id
        )));
    }

    let mut key_sources = Vec::with_capacity(query.order_by.len());
    let mut order_collations_nulls = Vec::with_capacity(query.order_by.len());
    let mut comparators = Vec::with_capacity(query.order_by.len());
    for term in &query.order_by {
        let output = hir_order_output_position(block, &term.expr);
        let custom = term
            .type_fact
            .declared
            .as_ref()
            .and_then(|declared| declared.custom())
            .filter(|custom| custom.value().decode().is_some());
        if let Some(custom) = custom {
            if !custom
                .value()
                .operators()
                .iter()
                .any(|operator| operator.op == "<")
            {
                if let Some(output) = output {
                    crate::bail_parse_error!(
                        "cannot ORDER BY column '{}' of type '{}': type does not declare OPERATOR '<'",
                        block.outputs[output].name,
                        custom.value().name
                    );
                }
                crate::bail_parse_error!(
                    "cannot ORDER BY a custom type column that does not declare OPERATOR '<'"
                );
            }
        }
        key_sources.push(match (output, custom.is_some()) {
            (Some(output), false) => HirSortKeySource::Output(output),
            (Some(output), true) => HirSortKeySource::EncodedOutput(output),
            (None, _) => HirSortKeySource::Expression,
        });
        order_collations_nulls.push((
            term.order,
            term.collation.as_ref().map(|collation| *collation.value()),
            term.nulls,
        ));
        comparators.push(custom_type_comparator_from_type_fact(&term.type_fact));
    }

    let mut next_output_column = query.order_by.len();
    let output_columns = (0..block.outputs.len())
        .map(|output| {
            if let Some(key) = key_sources.iter().position(
                |source| matches!(source, HirSortKeySource::Output(found) if *found == output),
            ) {
                key
            } else {
                let column = next_output_column;
                next_output_column += 1;
                column
            }
        })
        .collect::<Vec<_>>();
    let column_count = next_output_column;
    let cursor = program.alloc_cursor_id(CursorType::Sorter);
    program.emit_insn(Insn::SorterOpen {
        data: Box::new(SorterOpenData {
            cursor_id: cursor,
            columns: query.order_by.len(),
            order_collations_nulls,
            comparators,
        }),
    });
    Ok(HirSorterOutput {
        cursor,
        record: program.alloc_register(),
        row: program.alloc_registers(column_count),
        column_count,
        order_by: &query.order_by,
        key_sources,
        output_columns,
    })
}

fn hir_order_output_position(block: &hir::QueryBlock, expression: &hir::Expr) -> Option<usize> {
    let mut expression = expression;
    while let hir::Expr::Collate { expr, .. } = expression {
        expression = expr;
    }
    if let hir::Expr::Output(output) = expression {
        return block
            .outputs
            .iter()
            .position(|candidate| candidate.id == *output);
    }
    block
        .outputs
        .iter()
        .position(|output| expression.equivalent(&output.expr))
}

fn emit_hir_sorter_insert(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    outputs: &[hir::Output],
    output_start: usize,
    sorter: &HirSorterOutput<'_>,
) -> Result<()> {
    for (key, (term, source)) in sorter.order_by.iter().zip(&sorter.key_sources).enumerate() {
        let target = sorter.row + key;
        match source {
            HirSortKeySource::Output(output) => program.emit_insn(Insn::Copy {
                src_reg: output_start + output,
                dst_reg: target,
                extra_amount: 0,
            }),
            HirSortKeySource::EncodedOutput(output) => emit_hir_sort_key(
                program,
                document,
                &outputs[*output].expr,
                &term.type_fact,
                target,
            )?,
            HirSortKeySource::Expression => {
                emit_hir_sort_key(program, document, &term.expr, &term.type_fact, target)?
            }
        }
    }
    for (output, column) in sorter.output_columns.iter().enumerate() {
        if *column < sorter.order_by.len() {
            continue;
        }
        program.emit_insn(Insn::Copy {
            src_reg: output_start + output,
            dst_reg: sorter.row + column,
            extra_amount: 0,
        });
    }
    sorter_insert(
        program,
        sorter.row,
        sorter.column_count,
        sorter.cursor,
        sorter.record,
    );
    Ok(())
}

fn emit_hir_sort_key(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    type_fact: &hir::TypeFact,
    target: usize,
) -> Result<()> {
    let suppress_decode = type_fact
        .declared
        .as_ref()
        .and_then(|declared| declared.custom())
        .is_some_and(|custom| custom.value().decode().is_some());
    if suppress_decode {
        program.flags.set_suppress_custom_type_decode(true);
    }
    let result = super::expr::translate_expr_no_constant_opt(program, document, expression, target);
    if suppress_decode {
        program.flags.set_suppress_custom_type_decode(false);
    }
    result.map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_sorted_rows(
    program: &mut ProgramBuilder,
    block: &hir::QueryBlock,
    sorter: &HirSorterOutput<'_>,
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    emit_hir_sorter_rows(
        program,
        &block.outputs,
        sorter.cursor,
        sorter.record,
        sorter.column_count,
        &sorter.output_columns,
        destination,
        limit,
        done,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_hir_sorter_rows(
    program: &mut ProgramBuilder,
    output_definitions: &[hir::Output],
    sorter_cursor: CursorID,
    sorter_record: usize,
    sorter_column_count: usize,
    output_columns: &[usize],
    destination: &QueryDestination,
    limit: Option<QueryLimitRegisters>,
    done: BranchOffset,
) -> Result<()> {
    if output_definitions.len() != output_columns.len() {
        return Err(LimboError::InternalError(format!(
            "HIR sorter has {} outputs but {} output columns",
            output_definitions.len(),
            output_columns.len()
        )));
    }
    emit_explain!(
        program,
        false,
        EqpDetail::OrderBy {
            method: EqpSortMethod::Sorter,
        }
    );
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter_column_count,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sorter_record,
        num_fields: sorter_column_count,
    });
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter_cursor,
        pc_if_empty: done,
    });
    program.preassign_label_to_next_insn(loop_start);
    emit_before_row(program, limit, next);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter_cursor,
        dest_reg: sorter_record,
        pseudo_cursor,
    });
    let outputs = program.alloc_registers(output_definitions.len());
    for (output, column) in output_columns.iter().enumerate() {
        program.emit_column_or_rowid(pseudo_cursor, *column, outputs + output);
    }
    emit_array_results(program, output_definitions, outputs);
    emit_columns_to_destination(program, destination, outputs, output_definitions.len())?;
    emit_after_row(
        program,
        limit,
        destination_stops_after_first_row(destination),
        done,
    );
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter_cursor,
        pc_if_next: loop_start,
    });
    Ok(())
}

fn initialize_limit(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    limit: Option<&hir::Limit>,
    done: BranchOffset,
) -> Result<Option<QueryLimitRegisters>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let limit_register = program.alloc_register();
    emit_limit_value(program, document, &limit.limit, limit_register)?;

    let offset = if let Some(offset) = &limit.offset {
        let offset_register = program.alloc_register();
        emit_offset_value(program, document, offset, offset_register)?;
        program.emit_insn(Insn::MustBeInt {
            reg: offset_register,
            target_pc: None,
        });
        let combined_register = program.alloc_register();
        program.emit_insn(Insn::OffsetLimit {
            limit_reg: limit_register,
            offset_reg: offset_register,
            combined_reg: combined_register,
        });
        Some(offset_register)
    } else {
        None
    };
    program.emit_insn(Insn::IfNot {
        reg: limit_register,
        target_pc: done,
        jump_if_null: false,
    });
    Ok(Some(QueryLimitRegisters {
        limit: limit_register,
        offset,
    }))
}

fn emit_limit_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    match expression {
        hir::Expr::Literal(Literal::Numeric(value)) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(value)) => {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            Value::Numeric(Numeric::Float(value)) => {
                program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: target,
                    target_pc: None,
                });
            }
            _ => unreachable!("numeric parser returns a numeric value"),
        },
        _ => {
            super::expr::translate_expr(program, document, expression, target)?;
            program.emit_insn(Insn::MustBeInt {
                reg: target,
                target_pc: None,
            });
        }
    }
    Ok(())
}

fn emit_offset_value(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<()> {
    match expression {
        hir::Expr::Literal(Literal::Numeric(value)) => match parse_numeric_literal(value)? {
            Value::Numeric(Numeric::Integer(value)) => {
                program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            Value::Numeric(Numeric::Float(value)) => {
                program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: target,
                    target_pc: None,
                });
            }
            _ => unreachable!("numeric parser returns a numeric value"),
        },
        _ => {
            super::expr::translate_expr(program, document, expression, target)?;
        }
    }
    Ok(())
}

fn emit_before_row(
    program: &mut ProgramBuilder,
    limit: Option<QueryLimitRegisters>,
    skip_row: BranchOffset,
) {
    let Some(offset) = limit.and_then(|limit| limit.offset) else {
        return;
    };
    program.emit_insn(Insn::IfPos {
        reg: offset,
        target_pc: skip_row,
        decrement_by: 1,
    });
}

fn emit_after_row(
    program: &mut ProgramBuilder,
    limit: Option<QueryLimitRegisters>,
    stop_after_first: bool,
    done: BranchOffset,
) {
    if let Some(limit) = limit {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit.limit,
            target_pc: done,
        });
    }
    if stop_after_first {
        program.emit_insn(Insn::Goto { target_pc: done });
    }
}

fn query_output_registers(
    program: &mut ProgramBuilder,
    outputs: &[hir::Output],
    needs_values: bool,
) -> Result<usize> {
    if outputs.is_empty() {
        return Err(LimboError::InternalError(
            "HIR query has no outputs".to_string(),
        ));
    }
    Ok(if needs_values {
        program.alloc_registers(outputs.len())
    } else {
        0
    })
}

fn destination_stops_after_first_row(destination: &QueryDestination) -> bool {
    matches!(
        destination,
        QueryDestination::ExistsSubqueryResult { .. }
            | QueryDestination::RowValueSubqueryResult { .. }
    )
}

fn emit_query_row<'expr>(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    output: &HirRowOutput<'_>,
    outputs: &[hir::Output],
    expressions: impl ExactSizeIterator<Item = &'expr hir::Expr>,
    start: usize,
    skip_row: BranchOffset,
) -> Result<()> {
    if expressions.len() != outputs.len() {
        return Err(LimboError::InternalError(format!(
            "HIR row width {} does not match output width {}",
            expressions.len(),
            outputs.len()
        )));
    }
    if !output.needs_values() {
        for expression in expressions {
            super::expr::register_parameters(program, expression);
        }
    } else {
        for (position, expression) in expressions.enumerate() {
            super::expr::translate_expr_no_constant_opt(
                program,
                document,
                expression,
                start + position,
            )?;
            program.bind_output(outputs[position].id, start + position);
        }
    }

    if output.needs_values() && matches!(&output.target, HirRowTarget::Direct { .. }) {
        emit_array_results(program, outputs, start);
    }

    if let Some(distinct) = output.distinct {
        program.emit_insn(Insn::HashDistinct {
            data: Box::new(HashDistinctData {
                hash_table_id: distinct.hash_table,
                key_start_reg: start,
                num_keys: outputs.len(),
                collations: distinct.collations.clone(),
                target_pc: skip_row,
            }),
        });
    }

    match output.target {
        HirRowTarget::Direct {
            destination,
            limit,
            done,
        } => {
            emit_before_row(program, limit, skip_row);
            emit_columns_to_destination(program, destination, start, outputs.len())?;
            emit_after_row(
                program,
                limit,
                destination_stops_after_first_row(destination),
                done,
            );
        }
        HirRowTarget::Sorter(sorter) => {
            emit_hir_sorter_insert(program, document, outputs, start, sorter)?;
        }
    }
    Ok(())
}

fn emit_array_results(program: &mut ProgramBuilder, outputs: &[hir::Output], start: usize) {
    for (position, output) in outputs.iter().enumerate() {
        if !output.type_fact.is_array() {
            continue;
        }
        let register = start + position;
        let skip = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: register,
            target_pc: skip,
        });
        program.emit_insn(Insn::ArrayDecode { reg: register });
        program.preassign_label_to_next_insn(skip);
    }
}

fn row_value_destination(program: &mut ProgramBuilder, width: usize) -> DestinationBinding {
    let start = program.alloc_registers(width);
    DestinationBinding {
        destination: QueryDestination::RowValueSubqueryResult {
            result_reg_start: start,
            num_regs: width,
        },
        binding: SubqueryBinding::RowValue {
            start,
            count: width,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::SqliteDialect,
        schema::{BTreeTable, Schema},
        translate::{
            optimizer::CostModelParams,
            semantic::{
                catalog::{SemanticCatalog, SemanticCatalogDatabase},
                context::DoubleQuotedDml,
                hir::{self, Expr, HirRoot},
                SemanticOptions, SemanticRootInput,
            },
            semantic_to_plan::HirPlan,
        },
        vdbe::builder::{ProgramBuilderOpts, QueryMode},
        SymbolTable, MAIN_DB_ID,
    };
    use turso_parser::parser::Parser;

    fn program() -> ProgramBuilder {
        program_with_mode(QueryMode::Normal)
    }

    fn program_with_mode(mode: QueryMode) -> ProgramBuilder {
        ProgramBuilder::new(mode, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn analyze_sql_with_schema(mut schema: Schema, sql: &str) -> (HirDocument, Arc<Schema>) {
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom affinities resolve");
        let schema = Arc::new(schema);
        let catalog = SemanticCatalog {
            databases: vec![SemanticCatalogDatabase {
                id: hir::DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: schema.clone(),
            }],
            unqualified_database_search_path: vec![hir::DatabaseId::new(MAIN_DB_ID)],
        };
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a statement");
        let turso_parser::ast::Cmd::Stmt(statement) = command else {
            panic!("SQL contains a statement command");
        };
        let document = crate::translate::semantic::analyze_root(
            &catalog,
            &SymbolTable::new(),
            SemanticOptions {
                dialect: Arc::new(SqliteDialect),
                custom_types_enabled: true,
                dqs_dml: DoubleQuotedDml::Enabled,
            },
            SemanticRootInput::Statement(&statement),
        )
        .map(|document| {
            document.validate().expect("HIR validates");
            document
        })
        .expect("SQL analyzes");
        (document, schema)
    }

    fn analyze_sql(schema: Schema, sql: &str) -> HirDocument {
        analyze_sql_with_schema(schema, sql).0
    }

    fn analyze_plan(schema: Schema, sql: &str) -> HirPlan {
        let (document, schema) = analyze_sql_with_schema(schema, sql);
        HirPlan::build(Arc::new(document), &schema, &CostModelParams::default())
            .expect("HIR query plans")
    }

    fn indexed_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value INTEGER, extra TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        schema
            .add_index(Arc::new(Index {
                name: "items_value".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .expect("fixed index name is unique");
        schema
    }

    fn rowid_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        schema
    }

    fn ordered_custom_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE amount(value INTEGER, factor INTEGER) BASE INTEGER \
                 ENCODE value * factor DECODE value / factor OPERATOR '<' numeric_lt",
            )
            .expect("custom ordered type parses");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value amount(4), extra TEXT) STRICT", 2)
                    .expect("custom ordered table parses"),
            ))
            .expect("custom ordered table name is unique");
        schema
    }

    fn joined_items_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE right_items(value INTEGER, extra TEXT)", 3)
                    .expect("right table schema parses"),
            ))
            .expect("right table name is unique");
        schema
            .add_index(Arc::new(Index {
                name: "right_items_value".to_string(),
                table_name: "right_items".to_string(),
                root_page: 4,
                columns: vec![IndexColumn {
                    name: "value".to_string(),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: 0,
                    collation: None,
                    default: None,
                    expr: None,
                }],
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }))
            .expect("right index name is unique");
        schema
    }

    fn automatic_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE right_items(value INTEGER, extra TEXT)", 3)
                    .expect("right table schema parses"),
            ))
            .expect("right table name is unique");
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        schema
    }

    fn automatic_generated_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE left_items(value INTEGER)", 2)
                    .expect("left table schema parses"),
            ))
            .expect("left table name is unique");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE right_items(\
                         value INTEGER, \
                         generated INTEGER AS (value + 1) VIRTUAL, \
                         extra TEXT\
                     )",
                    3,
                )
                .expect("generated table schema parses"),
            ))
            .expect("right table name is unique");
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        schema
    }

    fn left_join_with_tail_schema() -> Schema {
        let mut schema = automatic_join_schema();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE tail_items(value INTEGER, extra TEXT)", 4)
                    .expect("tail table schema parses"),
            ))
            .expect("tail table name is unique");
        schema
    }

    fn root_expression(document: &HirDocument) -> &Expr {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        &document.queries[root.query.index()].blocks[0].outputs[0].expr
    }

    fn root_query(document: &HirDocument) -> QueryId {
        let HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        root.query
    }

    fn root_subquery(document: &HirDocument) -> &SubqueryExpr {
        match root_expression(document) {
            Expr::Subquery(subquery) => subquery,
            Expr::Binary { rhs, .. } => {
                let Expr::Subquery(subquery) = rhs.as_ref() else {
                    panic!("binary RHS is a subquery");
                };
                subquery
            }
            _ => panic!("root expression contains the expected subquery"),
        }
    }

    fn root_filter_subquery(document: &HirDocument) -> &SubqueryExpr {
        let query = document
            .query(root_query(document))
            .expect("root query exists");
        let hir::QueryBlockBody::Select {
            filter: Some(Expr::Subquery(subquery)),
            ..
        } = &query.blocks[0].body
        else {
            panic!("root filter is a subquery expression");
        };
        subquery
    }

    #[test]
    fn planned_ungrouped_aggregates_use_hir_ids_and_shared_steps() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT sum(value), count(*) FROM items WHERE value > 0",
        );
        let query_id = root_query(&plan.document);
        let query = plan.document.query(query_id).expect("root query exists");
        let block = &query.blocks[0];
        assert_eq!(block.aggregate_count, 2);

        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query_id, &QueryDestination::ResultRows)
            .expect("ungrouped aggregate HIR query emits");
        program.resolve_labels().expect("aggregate labels resolve");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::AggFinal { .. }))
                .count(),
            2
        );
        for index in 0..block.aggregate_count {
            assert!(program
                .aggregate_result_register(hir::AggregateId::new(block.id, index))
                .is_some());
        }

        let last_step = program
            .insns
            .iter()
            .rposition(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
            .expect("aggregate step emits");
        let scan_advance = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Next { .. }))
            .expect("table scan advances");
        let first_final = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggFinal { .. }))
            .expect("aggregate final emits");
        assert!(last_step < scan_advance && scan_advance < first_final);
    }

    #[test]
    fn planned_filtered_distinct_aggregate_uses_hir_argument_collation() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT count(DISTINCT value COLLATE nocase) FILTER (WHERE id > 0) FROM items",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("filtered DISTINCT aggregate HIR query emits");
        program
            .resolve_labels()
            .expect("filtered DISTINCT labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashClear { .. })));
        let filter = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: true,
                        ..
                    }
                )
            })
            .expect("aggregate FILTER emits");
        let distinct = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::HashDistinct { data } => data.collations == [CollationSeq::NoCase],
                _ => false,
            })
            .expect("DISTINCT uses resolved argument collation");
        let step = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
            .expect("aggregate step emits");
        assert!(filter < distinct && distinct < step);
    }

    #[test]
    fn planned_percentile_checks_fraction_once_before_the_scan() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY value) FROM items",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("percentile HIR query emits");
        program.resolve_labels().expect("percentile labels resolve");

        let (check, fraction) = program
            .insns
            .iter()
            .enumerate()
            .find_map(|(index, (insn, _))| match insn {
                Insn::IsNull { reg, .. } => Some((index, *reg)),
                _ => None,
            })
            .expect("percentile fraction range check emits");
        let scan = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Rewind { .. }))
            .expect("table scan emits");
        let (step, delimiter) = program
            .insns
            .iter()
            .enumerate()
            .find_map(|(index, (insn, _))| match insn {
                Insn::AggStep { data }
                    if matches!(
                        &data.func,
                        AccumulatorFunc::Agg(AggFunc::PercentileCont)
                    ) =>
                {
                    Some((index, data.delimiter))
                }
                _ => None,
            })
            .expect("percentile step emits");
        assert_eq!(delimiter, fraction);
        assert!(check < scan && scan < step);
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Halt { description, .. }
                if description == "percentile value is not between 0 and 1"
        )));
    }

    #[test]
    fn planned_percentile_rejects_fraction_that_depends_on_aggregated_rows() {
        for sql in [
            "SELECT percentile_cont(value) WITHIN GROUP (ORDER BY extra) FROM items",
            "SELECT percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY value) FROM items",
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let error = emit_planned_query_body(
                &mut program(),
                &plan,
                query,
                &QueryDestination::ResultRows,
            )
            .expect_err("row-dependent percentile fraction fails");
            assert!(error.to_string().contains(
                "must be a constant expression that does not depend on the aggregated rows"
            ));
        }
    }

    #[test]
    fn planned_ungrouped_aggregate_captures_bare_columns_from_first_row() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT rowid, extra, sum(value) FROM items",
        );
        let query = root_query(&plan.document);
        let source = plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0]
            .source;
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("bare-column aggregate HIR query emits");

        let Some(SourceBinding::Registers { start, rowid }) = program.source_binding(source) else {
            panic!("aggregate source is rebound to captured registers");
        };
        assert!(rowid.is_some());
        assert!(program.insns.iter().any(|(insn, _)| {
            matches!(
                insn,
                Insn::Column {
                    column: 1,
                    dest,
                    ..
                } if *dest == start + 1
            )
        }));
    }

    #[test]
    fn planned_grouped_aggregate_sorts_then_uses_shared_aggregate_steps() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT extra COLLATE nocase, sum(value) FROM items \
             GROUP BY extra COLLATE nocase HAVING extra <> ''",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("grouped aggregate HIR query emits");
        program.resolve_labels().expect("GROUP BY labels resolve");

        let sorter_open = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::SorterOpen { data } => {
                    data.order_collations_nulls.first()
                        == Some(&(SortOrder::Asc, Some(CollationSeq::NoCase), None))
                }
                _ => false,
            })
            .expect("GROUP BY sorter uses resolved key collation");
        let sorter_insert = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
            .expect("scan rows enter GROUP BY sorter");
        let sorter_sort = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterSort { .. }))
            .expect("GROUP BY sorter drains");
        let aggregate_step = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggStep { .. }))
            .expect("group uses shared aggregate step");
        let aggregate_final = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::AggFinal { .. }))
            .expect("group uses shared aggregate final");
        assert!(sorter_open < sorter_insert && sorter_insert < sorter_sort);
        assert!(sorter_sort < aggregate_step && aggregate_step < aggregate_final);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Compare { .. })));
    }

    #[test]
    fn planned_grouped_aggregate_uses_index_order_without_a_sorter() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value, count(*) FROM items GROUP BY value",
        );
        let query = root_query(&plan.document);
        assert!(
            plan.planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .group_sort_elided
        );

        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("index-ordered GROUP BY HIR query emits");
        program
            .resolve_labels()
            .expect("index-ordered GROUP BY labels resolve");

        assert!(!program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SorterOpen { .. } | Insn::SorterInsert { .. } | Insn::SorterSort { .. }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Compare { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::AggStep { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ResultRow { .. })));
    }

    #[test]
    fn planned_grouped_aggregate_keeps_sorter_when_index_collation_differs() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value COLLATE nocase, count(*) FROM items GROUP BY value COLLATE nocase",
        );
        let query = root_query(&plan.document);
        assert!(
            !plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .group_sort_elided
        );

        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("collation-mismatched GROUP BY HIR query emits");
        assert!(program.insns.iter().any(|(insn, _)| match insn {
            Insn::SorterOpen { data } => {
                data.order_collations_nulls.first()
                    == Some(&(SortOrder::Asc, Some(CollationSeq::NoCase), None))
            }
            _ => false,
        }));
    }

    #[test]
    fn planned_group_by_without_aggregates_still_emits_one_row_per_group() {
        let plan = analyze_plan(indexed_items_schema(), "SELECT extra FROM items GROUP BY extra");
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("aggregate-free GROUP BY HIR query emits");
        program
            .resolve_labels()
            .expect("aggregate-free GROUP BY labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterInsert { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Compare { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ResultRow { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::AggStep { .. } | Insn::AggFinal { .. })));
    }

    #[test]
    fn planned_grouped_distinct_aggregate_clears_each_group_state() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT extra, count(DISTINCT value) FROM items GROUP BY extra",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("grouped DISTINCT aggregate HIR query emits");
        program
            .resolve_labels()
            .expect("grouped DISTINCT aggregate labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashDistinct { .. })));
        assert!(program
            .insns
            .iter()
            .filter(|(insn, _)| matches!(insn, Insn::HashClear { .. }))
            .count()
            >= 2);
    }

    #[test]
    fn function_argument_facts_are_resolved_and_validated() {
        let mut document = analyze_sql(
            rowid_items_schema(),
            "SELECT min(value COLLATE nocase) FROM items",
        );
        let Expr::Function(call) = root_expression(&document) else {
            panic!("root expression is aggregate call");
        };
        let hir::FunctionArguments::Expressions { values, facts, .. } = &call.arguments else {
            panic!("aggregate has expression arguments");
        };
        assert_eq!(values.len(), 1);
        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].collation.as_ref().map(|value| *value.value()),
            Some(CollationSeq::NoCase)
        );

        let Expr::Function(call) = &mut document.queries[0].blocks[0].outputs[0].expr else {
            panic!("root expression is aggregate call");
        };
        let hir::FunctionArguments::Expressions { facts, .. } = &mut call.arguments else {
            panic!("aggregate has expression arguments");
        };
        facts.clear();
        assert_eq!(
            document
                .validate()
                .expect_err("argument/fact width mismatch is invalid")
                .message(),
            "function argument facts do not match argument count"
        );
    }

    #[test]
    fn planned_distinct_uses_hir_collations_before_offset() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT DISTINCT value COLLATE nocase FROM items LIMIT 1 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("DISTINCT HIR query emits");
        program.resolve_labels().expect("DISTINCT labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::HashClear { .. })));
        let distinct = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::HashDistinct { data } => data.collations == [CollationSeq::NoCase],
                _ => false,
            })
            .expect("DISTINCT uses the resolved output collation");
        let offset = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfPos {
                        decrement_by: 1,
                        ..
                    }
                )
            })
            .expect("OFFSET emits");
        assert!(distinct < offset, "OFFSET counts unique rows");
    }

    #[test]
    fn planned_order_by_uses_hir_terms_and_sorts_before_limit() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value, extra FROM items \
             ORDER BY extra COLLATE nocase DESC NULLS FIRST LIMIT 2 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("ordered HIR query emits");
        program.resolve_labels().expect("ORDER BY labels resolve");

        let sorter_open = program
            .insns
            .iter()
            .position(|(insn, _)| match insn {
                Insn::SorterOpen { data } => {
                    data.order_collations_nulls
                        == [(
                            SortOrder::Desc,
                            Some(CollationSeq::NoCase),
                            Some(turso_parser::ast::NullsOrder::First),
                        )]
                }
                _ => false,
            })
            .expect("sorter uses resolved direction, collation, and NULL order");
        let sorter_insert = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
            .expect("rows enter sorter");
        let sorter_sort = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterSort { .. }))
            .expect("sorter drains");
        let offset = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfPos {
                        decrement_by: 1,
                        ..
                    }
                )
            })
            .expect("OFFSET emits");
        assert!(sorter_open < sorter_insert && sorter_insert < sorter_sort);
        assert!(sorter_sort < offset, "LIMIT/OFFSET applies after sorting");
    }

    #[test]
    fn planned_distinct_order_by_deduplicates_before_sorting() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT DISTINCT value FROM items ORDER BY value DESC",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("DISTINCT ordered HIR query emits");
        program
            .resolve_labels()
            .expect("DISTINCT ORDER BY labels resolve");

        let distinct = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::HashDistinct { .. }))
            .expect("DISTINCT check emits");
        let sorter_insert = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::SorterInsert { .. }))
            .expect("unique rows enter sorter");
        assert!(distinct < sorter_insert);
    }

    #[test]
    fn planned_order_by_uses_custom_comparator_from_hir_type_fact() {
        let plan = analyze_plan(
            ordered_custom_items_schema(),
            "SELECT value FROM items ORDER BY value",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("custom ordered HIR query emits");
        program
            .resolve_labels()
            .expect("custom ORDER BY labels resolve");

        assert!(program.insns.iter().any(|(insn, _)| match insn {
            Insn::SorterOpen { data } => {
                data.comparators == [Some(crate::vdbe::insn::SortComparatorType::NumericLt)]
            }
            _ => false,
        }));
    }

    #[test]
    fn planned_compound_operators_use_hir_blocks_and_existing_index_operations() {
        for (sql, expected) in [
            ("SELECT 1 UNION SELECT 1", "union"),
            ("SELECT 1 EXCEPT SELECT 2", "except"),
            ("SELECT 1 INTERSECT SELECT 1", "intersect"),
        ] {
            let plan = analyze_plan(Schema::new(), sql);
            let query = root_query(&plan.document);
            let mut program = program();
            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("compound HIR query emits");
            program.resolve_labels().expect("compound labels resolve");

            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::OpenEphemeral {
                    is_table: false,
                    ..
                }
            )));
            match expected {
                "union" => assert!(program
                    .insns
                    .iter()
                    .any(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))),
                "except" => assert!(program
                    .insns
                    .iter()
                    .any(|(insn, _)| matches!(insn, Insn::IdxDelete { .. }))),
                "intersect" => assert!(program
                    .insns
                    .iter()
                    .any(|(insn, _)| matches!(insn, Insn::NotFound { .. }))),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn planned_union_all_shares_limit_and_offset_across_arms() {
        let plan = analyze_plan(
            Schema::new(),
            "SELECT 1 UNION ALL SELECT 2 LIMIT 1 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("UNION ALL HIR query emits");
        program.resolve_labels().expect("compound labels resolve");

        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenEphemeral { .. })));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
                .count(),
            2
        );
        let limit_registers = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::DecrJumpZero { reg, .. } => Some(*reg),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(limit_registers.len(), 2);
        assert!(limit_registers
            .iter()
            .all(|register| *register == limit_registers[0]));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn planned_compound_order_by_sorts_before_shared_limit_and_offset() {
        let plan = analyze_plan(
            Schema::new(),
            "SELECT 2 AS value UNION ALL SELECT 1 ORDER BY value DESC LIMIT 1 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("ordered compound HIR query emits");
        program.resolve_labels().expect("compound labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterOpen { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SorterSort { .. })));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn planned_recursive_cte_accepts_compound_seed_query() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE seq(x) AS (\
                 SELECT 1 UNION ALL SELECT 10 \
                 UNION ALL SELECT x + 1 FROM seq WHERE x < 3\
             ) SELECT x FROM seq",
        );
        let query = root_query(&plan.document);
        let mut program = program();
        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("recursive CTE with compound seed emits from HIR");
        program
            .resolve_labels()
            .expect("recursive compound labels resolve");

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxDelete { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenPseudo { .. })));
    }

    #[test]
    fn scalar_row_and_exists_destinations_install_matching_bindings() {
        for sql in [
            "SELECT (SELECT 1)",
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");

            match (&prepared.destination, subquery) {
                (
                    QueryDestination::RowValueSubqueryResult {
                        result_reg_start,
                        num_regs,
                    },
                    SubqueryExpr::Scalar { .. } | SubqueryExpr::Row { .. },
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::RowValue {
                        start: *result_reg_start,
                        count: *num_regs,
                    })
                ),
                (
                    QueryDestination::ExistsSubqueryResult { result_reg },
                    SubqueryExpr::Exists(_),
                ) => assert_eq!(
                    program.subquery_binding(prepared.query),
                    Some(SubqueryBinding::Exists {
                        register: *result_reg,
                    })
                ),
                _ => panic!("subquery kind has its matching destination"),
            }
            assert!(!prepared.correlated);
        }
    }

    #[test]
    fn in_destination_uses_resolved_comparison_metadata() {
        let document = analyze_sql(
            Schema::new(),
            "SELECT ('a' COLLATE nocase, 2) IN (SELECT 'A', 3)",
        );
        let subquery = root_subquery(&document);
        let SubqueryExpr::In {
            comparison, query, ..
        } = subquery
        else {
            panic!("root is an IN subquery");
        };
        let expected_affinity = comparison
            .components
            .iter()
            .map(|component| component.affinity.aff_mask())
            .collect::<String>();
        let mut program = program();
        let prepared =
            prepare_subquery(&mut program, &document, subquery).expect("IN destination prepares");

        let QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str: Some(affinity_str),
            is_delete: false,
        } = &prepared.destination
        else {
            panic!("IN uses an ephemeral index");
        };
        assert_eq!(affinity_str.as_str(), expected_affinity);
        assert_eq!(index.columns.len(), comparison.components.len());
        for (column, component) in index.columns.iter().zip(&comparison.components) {
            assert_eq!(
                column.collation,
                component
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
            );
        }
        assert_eq!(
            program.subquery_binding(*query),
            Some(SubqueryBinding::InIndex { cursor: *cursor_id })
        );
    }

    #[test]
    fn correlation_comes_from_the_resolved_query_capture_set() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let subquery = root_subquery(&document);
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, subquery)
            .expect("correlated subquery destination prepares");

        assert!(prepared.correlated);
        assert!(!document
            .query(prepared.query)
            .expect("subquery exists")
            .captures
            .is_empty());
    }

    #[test]
    fn subquery_wrapper_keeps_legacy_initialization_and_body_order() {
        for sql in [
            "SELECT (1, 2) = (SELECT 3, 4)",
            "SELECT EXISTS (SELECT 1)",
            "SELECT 1 IN (SELECT 2)",
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let subquery = root_subquery(&document);
            let mut program = program();
            let prepared = prepare_subquery(&mut program, &document, subquery)
                .expect("subquery destination prepares");
            let marker = program.alloc_register();
            emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
                assert_eq!(query, prepared.query);
                assert!(std::ptr::eq(destination, &prepared.destination));
                program.emit_insn(Insn::Integer {
                    value: 99,
                    dest: marker,
                });
                Ok(())
            })
            .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((Insn::Once { .. }, _))
            ));
            let marker_position = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Integer { value: 99, dest } if *dest == marker)
                })
                .expect("body marker was emitted");
            match &prepared.destination {
                QueryDestination::RowValueSubqueryResult {
                    result_reg_start,
                    num_regs,
                } => {
                    assert!(matches!(
                        program.insns.get(1),
                        Some((Insn::BeginSubrtn { .. }, _))
                    ));
                    assert_eq!(marker_position, 2 + num_regs);
                    for (offset, register) in
                        (*result_reg_start..*result_reg_start + *num_regs).enumerate()
                    {
                        assert!(matches!(
                            program.insns.get(2 + offset),
                            Some((Insn::Null { dest, dest_end: None }, _)) if *dest == register
                        ));
                    }
                    assert!(matches!(
                        program.insns.get(marker_position + 1),
                        Some((Insn::Return { .. }, _))
                    ));
                }
                QueryDestination::ExistsSubqueryResult { result_reg } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::BeginSubrtn { .. }, _),
                            (Insn::Integer { value: 0, dest }, _),
                            (Insn::Integer { value: 99, .. }, _),
                            (Insn::Return { .. }, _),
                        ] if *dest == *result_reg
                    ));
                }
                QueryDestination::EphemeralIndex { cursor_id, .. } => {
                    assert!(matches!(
                        program.insns.as_slice(),
                        [
                            (Insn::Once { .. }, _),
                            (Insn::OpenEphemeral { cursor_id: opened, is_table: false }, _),
                            (Insn::Integer { value: 99, .. }, _),
                        ] if *opened == *cursor_id
                    ));
                }
                _ => panic!("expression subquery has a supported destination"),
            }
        }
    }

    #[test]
    fn correlated_subquery_wrapper_does_not_emit_once() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value TEXT)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let document = analyze_sql(schema, "SELECT (SELECT value) FROM items");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("correlated subquery destination prepares");

        emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
            .expect("correlated wrapper emits");

        assert!(prepared.correlated);
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Once { .. })));
    }

    #[test]
    fn subquery_wrapper_preserves_scalar_and_list_eqp_nodes() {
        for (sql, list) in [
            ("SELECT (SELECT 1)", false),
            ("SELECT 1 IN (SELECT 2)", true),
        ] {
            let document = analyze_sql(Schema::new(), sql);
            let mut program = program_with_mode(QueryMode::ExplainQueryPlan {
                format: turso_parser::ast::EqpFormat::Text,
            });
            let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
                .expect("subquery destination prepares");
            emit_prepared_subquery(&mut program, &prepared, |_, _, _| Ok(()))
                .expect("subquery wrapper emits");

            assert!(matches!(
                program.insns.first(),
                Some((
                    Insn::Explain { detail, .. },
                    _
                )) if matches!(
                    detail.as_ref(),
                    EqpDetail::ListSubquery { correlated: false, .. } if list
                ) || matches!(
                    detail.as_ref(),
                    EqpDetail::ScalarSubquery { correlated: false, .. } if !list
                )
            ));
        }
    }

    #[test]
    fn constant_select_binds_outputs_and_emits_one_result_row() {
        let document = analyze_sql(Schema::new(), "SELECT 1, 2");
        let query_id = root_query(&document);
        let query = document.query(query_id).expect("root query exists");
        let outputs = &query.blocks[0].outputs;
        let mut program = program();

        emit_query_body(
            &mut program,
            &document,
            query_id,
            &QueryDestination::ResultRows,
        )
        .expect("constant SELECT emits");

        let start = match program.insns.as_slice() {
            [(
                Insn::Integer {
                    value: 1,
                    dest: first,
                },
                _,
            ), (
                Insn::Integer {
                    value: 2,
                    dest: second,
                },
                _,
            ), (
                Insn::ResultRow {
                    start_reg,
                    count: 2,
                },
                _,
            )] if *second == *first + 1 && *start_reg == *first => *first,
            _ => panic!("constant SELECT keeps consecutive output registers"),
        };
        for (position, output) in outputs.iter().enumerate() {
            assert_eq!(
                program
                    .output_binding(output.id)
                    .expect("output register is bound")
                    .register,
                start + position
            );
        }
    }

    #[test]
    fn values_emits_every_row_without_hoisting_constants() {
        let document = analyze_sql(Schema::new(), "VALUES (1, 2), (3, 4)");
        let mut program = program();

        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("VALUES emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { count: 2, .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter_map(|(insn, _)| match insn {
                    Insn::Integer { value, .. } => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn scalar_and_in_subqueries_emit_the_legacy_number_of_rows() {
        let scalar = analyze_sql(Schema::new(), "SELECT (VALUES (1), (2))");
        let mut scalar_program = program();
        let scalar_prepared =
            prepare_subquery(&mut scalar_program, &scalar, root_subquery(&scalar))
                .expect("scalar destination prepares");
        emit_prepared_subquery(
            &mut scalar_program,
            &scalar_prepared,
            |program, query, destination| emit_query_body(program, &scalar, query, destination),
        )
        .expect("scalar query emits");
        assert!(scalar_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Integer { value: 1, .. })));
        assert!(!scalar_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Integer { value: 2, .. })));
        assert_eq!(
            scalar_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            1
        );

        let membership = analyze_sql(Schema::new(), "SELECT 2 IN (VALUES (1), (2))");
        let mut membership_program = program();
        let membership_prepared = prepare_subquery(
            &mut membership_program,
            &membership,
            root_subquery(&membership),
        )
        .expect("IN destination prepares");
        emit_prepared_subquery(
            &mut membership_program,
            &membership_prepared,
            |program, query, destination| emit_query_body(program, &membership, query, destination),
        )
        .expect("IN query emits");
        assert_eq!(
            membership_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn exists_skips_output_evaluation_and_arrays_are_decoded() {
        let exists = analyze_sql(Schema::new(), "SELECT EXISTS (SELECT random())");
        let mut exists_program = program();
        let prepared = prepare_subquery(&mut exists_program, &exists, root_subquery(&exists))
            .expect("EXISTS destination prepares");
        emit_prepared_subquery(
            &mut exists_program,
            &prepared,
            |program, query, destination| emit_query_body(program, &exists, query, destination),
        )
        .expect("EXISTS query emits");
        assert!(!exists_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Function { .. })));
        let QueryDestination::ExistsSubqueryResult { result_reg } = prepared.destination else {
            panic!("EXISTS uses its result destination");
        };
        assert!(exists_program.insns.iter().any(|(insn, _)| {
            matches!(insn, Insn::Integer { value: 1, dest } if *dest == result_reg)
        }));

        let array = analyze_sql(Schema::new(), "SELECT ARRAY[1, 2]");
        let mut array_program = program();
        emit_query_body(
            &mut array_program,
            &array,
            root_query(&array),
            &QueryDestination::ResultRows,
        )
        .expect("array SELECT emits");
        assert!(array_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::ArrayDecode { .. })));
    }

    #[test]
    fn no_from_filter_branches_around_result_emission() {
        let document = analyze_sql(Schema::new(), "SELECT 7 WHERE 0");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("filtered SELECT emits");

        let filter_jump = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: true,
                        ..
                    }
                )
            })
            .expect("filter emits a false-or-null jump");
        let output = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 7, .. }))
            .expect("result expression is emitted");
        let result = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
            .expect("result destination is emitted");
        assert!(filter_jump < output && output < result);
    }

    #[test]
    fn no_from_limit_and_offset_keep_row_counter_opcodes() {
        let document = analyze_sql(Schema::new(), "SELECT 1 LIMIT 1 OFFSET 1");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("limited SELECT emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: false,
                        ..
                    }
                ))
                .count(),
            1
        );
        let offset_limit = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
            .expect("offset and limit are combined");
        let limit_exit = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(
                    insn,
                    Insn::IfNot {
                        jump_if_null: false,
                        ..
                    }
                )
            })
            .expect("limit exits before rows when its counter is false");
        assert!(offset_limit < limit_exit);
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::ResultRow { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn scalar_select_offset_skips_its_only_candidate_row() {
        let document = analyze_sql(Schema::new(), "SELECT (SELECT 1 LIMIT 1 OFFSET 1)");
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &document, root_subquery(&document))
            .expect("scalar destination prepares");
        emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
            emit_query_body(program, &document, query, destination)
        })
        .expect("offset scalar query emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IfPos { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            1
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Goto { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn floating_limit_and_offset_keep_integer_checks() {
        let document = analyze_sql(Schema::new(), "SELECT 1 LIMIT 1.0 OFFSET 0.0");
        let mut program = program();
        emit_query_body(
            &mut program,
            &document,
            root_query(&document),
            &QueryDestination::ResultRows,
        )
        .expect("floating counters emit");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::MustBeInt { .. }))
                .count(),
            3
        );
    }

    #[test]
    fn planned_full_scan_binds_its_source_and_emits_residual_filter() {
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql("CREATE TABLE items(value INTEGER)", 2)
                    .expect("fixed table schema parses"),
            ))
            .expect("fixed table name is unique");
        let plan = analyze_plan(
            schema,
            "SELECT value FROM items WHERE value LIMIT 2 OFFSET 1",
        );
        let query = root_query(&plan.document);
        let source = plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0]
            .source;
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned full scan emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(source)
            .copied()
            .expect("source has a physical binding")
        else {
            panic!("full table scan binds one B-tree cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let open = position(&|insn| {
            matches!(
                insn,
                Insn::OpenRead {
                    cursor_id,
                    root_page: 2,
                    db: MAIN_DB_ID,
                } if *cursor_id == scan_cursor
            )
        });
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let filter = position(&|insn| {
            matches!(
                insn,
                Insn::IfNot {
                    jump_if_null: true,
                    ..
                }
            )
        });
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));
        let next = position(&|insn| {
            matches!(
                insn,
                Insn::Next {
                    cursor_id,
                    fullscan: true,
                    ..
                } if *cursor_id == scan_cursor
            )
        });
        assert!(open < rewind && rewind < filter && filter < result && result < next);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IfPos { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. })));
    }

    #[test]
    fn planned_derived_table_materializes_once_and_scans_its_hir_query() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT d.value \
             FROM (SELECT value FROM items NOT INDEXED WHERE value > 1) AS d \
             WHERE d.value < 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let [planned_loop] = block_plan.loops.as_slice() else {
            panic!("derived table has one physical loop");
        };
        let HirSourceAccess::Derived {
            query: derived_query,
        } = planned_loop.access
        else {
            panic!("derived source keeps its prepared HIR query");
        };
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("derived-table query emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("derived source is bound")
        else {
            panic!("materialized derived source uses one table cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let once = position(&|insn| matches!(insn, Insn::Once { .. }));
        let open = position(
            &|insn| matches!(insn, Insn::OpenEphemeral { cursor_id, is_table: true } if *cursor_id == scan_cursor),
        );
        let insert =
            position(&|insn| matches!(insn, Insn::Insert { cursor, .. } if *cursor == scan_cursor));
        let rewind = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor)
                    .then_some(position)
            })
            .last()
            .expect("materialized table is scanned");
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));
        let next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == scan_cursor),
        );

        assert_ne!(derived_query, query);
        assert!(once < open && open < insert && insert < rewind);
        assert!(rewind < result && result < next);
    }

    #[test]
    fn planned_single_reference_cte_materializes_its_hir_query() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS (\
                 SELECT value FROM items NOT INDEXED WHERE value > 1\
             ) \
             SELECT c.value FROM chosen AS c WHERE c.value < 5",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let [planned_loop] = block_plan.loops.as_slice() else {
            panic!("CTE reference has one physical loop");
        };
        let HirSourceAccess::Cte {
            query: cte_query,
            materialization,
            ..
        } = &planned_loop.access
        else {
            panic!("CTE source keeps its prepared HIR query");
        };
        assert_eq!(*materialization, HirCteMaterialization::PerReference);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("single-reference CTE query emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: None,
        } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("CTE source is bound")
        else {
            panic!("materialized CTE source uses one table cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let once = position(&|insn| matches!(insn, Insn::Once { .. }));
        let open = position(
            &|insn| matches!(insn, Insn::OpenEphemeral { cursor_id, is_table: true } if *cursor_id == scan_cursor),
        );
        let insert =
            position(&|insn| matches!(insn, Insn::Insert { cursor, .. } if *cursor == scan_cursor));
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 1, .. }));

        assert_ne!(*cte_query, query);
        assert!(once < open && open < insert && insert < rewind && rewind < result);
    }

    #[test]
    fn planned_shared_cte_materializes_once_and_opens_one_cursor_per_reference() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS (\
                 SELECT value FROM items NOT INDEXED WHERE value > 1\
             ) \
             SELECT a.value, b.value \
             FROM chosen AS a JOIN chosen AS b ON b.value = a.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 2);
        let ctes = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let HirSourceAccess::Cte {
                    cte,
                    materialization,
                    ..
                } = &planned_loop.access
                else {
                    panic!("both sources use the shared CTE");
                };
                assert_eq!(*materialization, HirCteMaterialization::Shared);
                *cte
            })
            .collect::<Vec<_>>();
        assert_eq!(ctes[0], ctes[1]);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("shared CTE query emits");

        let cursors = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                } = program
                    .source_binding(planned_loop.source)
                    .copied()
                    .expect("shared CTE source is bound")
                else {
                    panic!("each CTE reference uses one table cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        assert_ne!(cursors[0], cursors[1]);
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. }))
                .count(),
            1
        );
        let duplicates = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::OpenDup {
                    new_cursor_id,
                    original_cursor_id,
                } => Some((*new_cursor_id, *original_cursor_id)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let [duplicate] = duplicates.as_slice() else {
            panic!("second CTE reference opens one duplicate cursor");
        };
        assert!(cursors.contains(&duplicate.0));
        assert!(cursors.contains(&duplicate.1));
        assert!(cursors.iter().all(|cursor| program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::Rewind { cursor_id, .. } if cursor_id == cursor)
        )));
        assert!(program.hir_materialized_cte(ctes[0]).is_some());
    }

    #[test]
    fn planned_explicit_cte_uses_registered_materialized_storage() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "WITH chosen AS MATERIALIZED (\
                 SELECT value FROM items NOT INDEXED\
             ) \
             SELECT value FROM chosen",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::Cte {
            cte,
            materialization,
            ..
        } = &planned_loop.access
        else {
            panic!("explicit CTE keeps its CTE access");
        };
        assert_eq!(*materialization, HirCteMaterialization::Explicit);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("explicit CTE query emits");

        assert!(program.hir_materialized_cte(*cte).is_some());
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::OpenEphemeral { is_table: true, .. }))
                .count(),
            1
        );
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenDup { .. })));
    }

    #[test]
    fn planned_recursive_cte_uses_queue_and_register_bound_input() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE numbers(value) AS (\
                 VALUES (1) \
                 UNION ALL \
                 SELECT value + 1 FROM numbers WHERE value < 3\
             ) \
             SELECT value FROM numbers",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::RecursiveCte { cte } = planned_loop.access else {
            panic!("recursive CTE keeps recursive access");
        };
        let hir::CteBody::Recursive(recursive) = &plan.document.cte(cte).expect("CTE exists").body
        else {
            panic!("CTE body is recursive");
        };
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("recursive CTE query emits");

        assert!(recursive.input_sources.iter().all(|source| matches!(
            program.source_binding(*source),
            Some(SourceBinding::Registers { .. })
        )));
        assert!(program.hir_materialized_cte(cte).is_some());
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::OpenEphemeral {
                        is_table: false,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxDelete { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Goto { .. })));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OpenPseudo { .. })));
    }

    #[test]
    fn planned_recursive_union_uses_seen_rows_index() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE numbers(value) AS (\
                 VALUES (1) \
                 UNION \
                 SELECT value + 1 FROM numbers WHERE value < 3\
             ) \
             SELECT value FROM numbers",
        );
        let query = root_query(&plan.document);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("recursive UNION query emits");

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(
                    insn,
                    Insn::OpenEphemeral {
                        is_table: false,
                        ..
                    }
                ))
                .count(),
            2
        );
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Found { .. })));
    }

    #[test]
    fn planned_recursive_queue_keeps_order_limit_and_offset() {
        let plan = analyze_plan(
            Schema::new(),
            "WITH RECURSIVE numbers(value) AS (\
                 VALUES (1) \
                 UNION ALL \
                 SELECT value + 1 FROM numbers WHERE value < 5 \
                 ORDER BY 1 DESC LIMIT 3 OFFSET 1\
             ) \
             SELECT value FROM numbers",
        );
        let query = root_query(&plan.document);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("ordered recursive query emits");

        let queue_cursor = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::OpenEphemeral {
                    cursor_id,
                    is_table: false,
                } => Some(*cursor_id),
                _ => None,
            })
            .expect("recursive queue opens");
        let Some(CursorType::BTreeIndex(queue)) = program.get_cursor_type(queue_cursor) else {
            panic!("recursive queue uses an ephemeral index");
        };
        assert_eq!(queue.columns[0].order, SortOrder::Desc);
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::OffsetLimit { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IfPos { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::DecrJumpZero { .. })));
    }

    #[cfg(feature = "json")]
    #[test]
    fn planned_virtual_table_uses_resolved_arguments_and_virtual_cursor() {
        let plan = analyze_plan(Schema::new(), "SELECT value FROM json_each('[1]')");
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        let HirSourceAccess::Virtual(operation) = &planned_loop.access else {
            panic!("table function uses virtual-table access");
        };
        assert!(matches!(
            operation.arguments.as_slice(),
            [Expr::Literal(Literal::String(_))]
        ));
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("virtual-table query emits");

        let SourceBinding::Virtual { cursor } = program
            .source_binding(planned_loop.source)
            .copied()
            .expect("virtual source is bound")
        else {
            panic!("virtual source uses one virtual cursor");
        };
        assert!(matches!(
            program.get_cursor_type(cursor),
            Some(CursorType::VirtualTable(_))
        ));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::VOpen { cursor_id } if *cursor_id == cursor)));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VFilter {
                cursor_id,
                arg_count: 1,
                idx_num,
                ..
            } if *cursor_id == cursor && *idx_num == operation.idx_num as usize
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VColumn { cursor_id, .. } if *cursor_id == cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::VNext { cursor_id, .. } if *cursor_id == cursor
        )));
    }

    #[test]
    fn planned_cross_join_nests_loops_and_places_predicates_when_ready() {
        let plan = analyze_plan(
            joined_items_schema(),
            "SELECT l.value, r.value \
             FROM left_items AS l NOT INDEXED CROSS JOIN right_items AS r NOT INDEXED \
             WHERE l.value AND r.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 2);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned cross join emits");

        let cursors = block_plan
            .loops
            .iter()
            .map(|planned_loop| {
                let SourceBinding::BTree {
                    scan_cursor,
                    table_cursor: None,
                } = program
                    .source_binding(planned_loop.source)
                    .copied()
                    .expect("join source is bound")
                else {
                    panic!("table scan uses one cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let outer_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == cursors[0]),
        );
        let outer_column = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursors[0]),
        );
        let inner_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == cursors[1]),
        );
        let inner_column = position(
            &|insn| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursors[1]),
        );
        let result = position(&|insn| matches!(insn, Insn::ResultRow { count: 2, .. }));
        let advances = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Next {
                    cursor_id,
                    fullscan: true,
                    ..
                } => Some(*cursor_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let empty_targets = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::Rewind {
                    cursor_id,
                    pc_if_empty,
                } if cursors.contains(cursor_id) => Some(*pc_if_empty),
                _ => None,
            })
            .collect::<Vec<_>>();
        let predicate_failures = program
            .insns
            .iter()
            .filter_map(|(insn, _)| match insn {
                Insn::IfNot {
                    target_pc,
                    jump_if_null: true,
                    ..
                } => Some(*target_pc),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(outer_rewind < outer_column && outer_column < inner_rewind);
        assert!(inner_rewind < inner_column && inner_column < result);
        assert_eq!(advances, vec![cursors[1], cursors[0]]);
        assert_eq!(empty_targets.len(), 2);
        assert_eq!(predicate_failures.len(), 2);
        for (empty, predicate_failure) in empty_targets.iter().zip(&predicate_failures) {
            assert_ne!(empty, predicate_failure);
        }
    }

    #[test]
    fn planned_inner_join_seek_reads_its_outer_hir_source() {
        for (output, table_required) in [("r.value", false), ("r.extra", true)] {
            let sql = format!(
                "SELECT l.value, {output} \
                 FROM left_items AS l NOT INDEXED \
                 JOIN right_items AS r INDEXED BY right_items_value \
                 ON r.value = l.value"
            );
            let plan = analyze_plan(joined_items_schema(), &sql);
            let query = root_query(&plan.document);
            let block_plan = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0];
            assert_eq!(block_plan.loops.len(), 2);
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Seek {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &block_plan.loops[1].access
            else {
                panic!("right join source uses its requested index seek");
            };
            assert_eq!(index.name, "right_items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned inner seek emits");

            let outer_cursor = match program
                .source_binding(block_plan.loops[0].source)
                .copied()
                .expect("outer source is bound")
            {
                SourceBinding::BTree { scan_cursor, .. } => scan_cursor,
                _ => panic!("outer source is a B-tree"),
            };
            let outer_key = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == outer_cursor)
                })
                .expect("seek reads its outer HIR column");
            let seek = program
                .insns
                .iter()
                .position(|(insn, _)| {
                    matches!(
                        insn,
                        Insn::SeekGE { is_index: true, .. }
                            | Insn::SeekGT { is_index: true, .. }
                            | Insn::SeekLE { is_index: true, .. }
                            | Insn::SeekLT { is_index: true, .. }
                    )
                })
                .expect("inner source emits an index seek");
            assert!(outer_key < seek);
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
        }
    }

    #[test]
    fn planned_inner_join_builds_and_probes_automatic_index() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right join source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("automatic-index join emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor,
        } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let open = position(
            &|insn| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor),
        );
        let insert = position(
            &|insn| matches!(insn, Insn::IdxInsert { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let filter = position(
            &|insn| matches!(insn, Insn::Filter { cursor_id, .. } if *cursor_id == scan_cursor),
        );
        let seek = position(&|insn| {
            matches!(
                insn,
                Insn::SeekGE {
                    cursor_id,
                    is_index: true,
                    ..
                } if *cursor_id == scan_cursor
            )
        });

        assert!(open < insert && insert < filter && filter < seek);
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::FilterAdd { cursor_id, .. } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenRead { cursor_id, .. } if *cursor_id == scan_cursor)
        ));
        assert!(
            table_cursor.is_none(),
            "payload makes the automatic index covering"
        );
    }

    #[test]
    fn null_matching_automatic_index_does_not_use_bloom_filter() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.value IS l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right join source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("NULL-matching automatic-index join emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(!program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::FilterAdd { cursor_id, .. } | Insn::Filter { cursor_id, .. }
                if *cursor_id == scan_cursor
        )));
    }

    #[test]
    fn automatic_index_build_lowers_virtual_generated_column_from_base_table() {
        let plan = analyze_plan(
            automatic_generated_join_schema(),
            "SELECT r.extra \
             FROM left_items AS l NOT INDEXED \
             CROSS JOIN right_items AS r \
             WHERE r.generated = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("generated join key uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        assert_eq!(index.columns[0].pos_in_table, 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("generated automatic-index join emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("automatic-index source is bound")
        else {
            panic!("automatic-index source has a B-tree binding");
        };
        let add = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Add { .. }))
            .expect("generated expression is lowered while building the index");
        let insert = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::IdxInsert { cursor_id, .. } if *cursor_id == scan_cursor)
            })
            .expect("automatic index is populated");
        assert!(add < insert);
    }

    #[test]
    fn left_join_checks_on_before_marking_match_and_where_afterward() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON r.value = l.value \
             WHERE r.extra IS NULL",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let right_loop = &block_plan.loops[1];
        assert_eq!(block_plan.predicates.len(), 2);
        assert_eq!(
            block_plan.predicates[0].from_outer_join,
            Some(right_loop.source)
        );
        assert_eq!(block_plan.predicates[1].from_outer_join, None);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN emits");

        let SourceBinding::BTree {
            scan_cursor: right_cursor,
            table_cursor: None,
        } = program
            .source_binding(right_loop.source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("NOT INDEXED right source uses one B-tree cursor");
        };
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let reset = position(&|insn| matches!(insn, Insn::Integer { value: 0, .. }));
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == right_cursor),
        );
        let matched = position(
            &|insn| matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register),
        );
        let failures = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(insn, Insn::IfNot { .. }).then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 2);
        let [on_failure, where_failure] = failures.as_slice() else {
            unreachable!()
        };
        let result = position(&|insn| matches!(insn, Insn::ResultRow { .. }));
        let next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == right_cursor),
        );
        let check =
            position(&|insn| matches!(insn, Insn::IfPos { reg, .. } if *reg == match_register));
        let null_row = position(
            &|insn| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == right_cursor),
        );
        let retry = program
            .insns
            .iter()
            .enumerate()
            .skip(null_row + 1)
            .find_map(|(position, (insn, _))| matches!(insn, Insn::Goto { .. }).then_some(position))
            .expect("NULL row re-enters after ON predicates");

        assert!(reset < rewind && rewind < *on_failure && *on_failure < matched);
        assert!(matched < *where_failure && *where_failure < result && result < next);
        assert!(next < check && check < null_row && null_row < retry);
    }

    #[test]
    fn left_join_constant_on_runs_at_the_right_loop_boundary() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON 0",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(
            block_plan.predicates[0].from_outer_join,
            Some(block_plan.loops[1].source)
        );
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("constant-ON LEFT JOIN emits");

        let reset = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 0, .. }))
            .expect("match flag is reset");
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let on_failure = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::IfNot { .. }))
            .expect("constant ON predicate is checked");
        let matched = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register)
            })
            .expect("match flag is set after ON");
        assert!(reset < on_failure && on_failure < matched);
    }

    #[test]
    fn left_join_uses_automatic_index_and_null_extends_its_scan_cursor() {
        let plan = analyze_plan(
            automatic_join_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r ON r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let HirSourceAccess::BTree {
            operation:
                HirBtreeOperation::Seek {
                    index: Some(index), ..
                },
            ..
        } = &block_plan.loops[1].access
        else {
            panic!("right source uses an automatic-index seek");
        };
        assert!(index.ephemeral);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("automatic-index LEFT JOIN emits");

        let SourceBinding::BTree { scan_cursor, .. } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("right source has a B-tree binding");
        };
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::OpenAutoindex { cursor_id } if *cursor_id == scan_cursor)
        ));
        assert!(program.insns.iter().any(
            |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == scan_cursor)
        ));
    }

    #[test]
    fn left_join_null_extends_index_and_table_cursors() {
        let plan = analyze_plan(
            joined_items_schema(),
            "SELECT l.value, r.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r INDEXED BY right_items_value \
                 ON r.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("indexed LEFT JOIN emits");

        let SourceBinding::BTree {
            scan_cursor,
            table_cursor: Some(table_cursor),
        } = program
            .source_binding(block_plan.loops[1].source)
            .copied()
            .expect("right source is bound")
        else {
            panic!("uncovered output uses index and table cursors");
        };
        for cursor in [scan_cursor, table_cursor] {
            assert!(program.insns.iter().any(
                |(insn, _)| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == cursor)
            ));
        }
    }

    #[test]
    fn unmatched_left_join_row_restarts_downstream_loop() {
        let plan = analyze_plan(
            left_join_with_tail_schema(),
            "SELECT l.value, r.extra, t.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN right_items AS r NOT INDEXED ON r.value = l.value \
             CROSS JOIN tail_items AS t NOT INDEXED \
             WHERE t.value = l.value",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 3);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("LEFT JOIN with downstream loop emits");

        let cursor = |source| match program
            .source_binding(source)
            .copied()
            .expect("source is bound")
        {
            SourceBinding::BTree { scan_cursor, .. } => scan_cursor,
            _ => panic!("source uses a B-tree cursor"),
        };
        let right_cursor = cursor(block_plan.loops[1].source);
        let tail_cursor = cursor(block_plan.loops[2].source);
        let position = |matches: &dyn Fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(insn, _)| matches(insn))
                .expect("expected instruction is emitted")
        };
        let matched = position(&|insn| matches!(insn, Insn::Integer { value: 1, .. }));
        let tail_rewind = position(
            &|insn| matches!(insn, Insn::Rewind { cursor_id, .. } if *cursor_id == tail_cursor),
        );
        let tail_next = position(
            &|insn| matches!(insn, Insn::Next { cursor_id, .. } if *cursor_id == tail_cursor),
        );
        let null_row = position(
            &|insn| matches!(insn, Insn::NullRow { cursor_id } if *cursor_id == right_cursor),
        );
        let retry = program
            .insns
            .iter()
            .enumerate()
            .skip(null_row + 1)
            .find_map(|(position, (insn, _))| matches!(insn, Insn::Goto { .. }).then_some(position))
            .expect("NULL row re-enters the downstream loop");

        assert!(matched < tail_rewind && tail_rewind < tail_next);
        assert!(tail_next < null_row && null_row < retry);
    }

    #[test]
    fn parenthesized_left_join_marks_and_null_extends_the_whole_group() {
        let plan = analyze_plan(
            left_join_with_tail_schema(),
            "SELECT l.value, r.extra, t.extra \
             FROM left_items AS l NOT INDEXED \
             LEFT JOIN (\
                 right_items AS r NOT INDEXED \
                 JOIN tail_items AS t NOT INDEXED ON t.value = r.value\
             ) ON r.value = l.value \
             WHERE r.extra IS NULL",
        );
        let query = root_query(&plan.document);
        let block_plan = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0];
        assert_eq!(block_plan.loops.len(), 3);
        let [group] = block_plan.groups.as_slice() else {
            panic!("one parenthesized FROM group is planned");
        };
        let group_loop_indices = block_plan
            .loops
            .iter()
            .enumerate()
            .filter_map(|(index, planned)| {
                group
                    .source_range
                    .contains(&planned.source_position)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(group_loop_indices.len(), 2);
        assert_eq!(group_loop_indices[1], group_loop_indices[0] + 1);
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("parenthesized LEFT JOIN emits");

        let group_cursors = group_loop_indices
            .iter()
            .map(|&index| {
                let source = block_plan.loops[index].source;
                let SourceBinding::BTree { scan_cursor, .. } = program
                    .source_binding(source)
                    .copied()
                    .expect("group source is bound")
                else {
                    panic!("group source uses a B-tree cursor");
                };
                scan_cursor
            })
            .collect::<Vec<_>>();
        let reset = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Integer { value: 0, .. }))
            .expect("group match flag is reset");
        let match_register = match program.insns[reset].0 {
            Insn::Integer { dest, .. } => dest,
            _ => unreachable!(),
        };
        let matched = program
            .insns
            .iter()
            .position(|(insn, _)| {
                matches!(insn, Insn::Integer { value: 1, dest } if *dest == match_register)
            })
            .expect("group match flag is set");
        let failures = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| {
                matches!(insn, Insn::IfNot { .. }).then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 3);
        assert!(failures[..2].iter().all(|failure| *failure < matched));
        assert!(matched < failures[2]);

        let last_group_next = program
            .insns
            .iter()
            .enumerate()
            .filter_map(|(position, (insn, _))| match insn {
                Insn::Next { cursor_id, .. } if group_cursors.contains(cursor_id) => Some(position),
                _ => None,
            })
            .max()
            .expect("both group loops advance");
        let null_rows = group_cursors
            .iter()
            .map(|cursor| {
                program
                    .insns
                    .iter()
                    .position(|(insn, _)| {
                        matches!(insn, Insn::NullRow { cursor_id } if cursor_id == cursor)
                    })
                    .expect("each group cursor is null-extended")
            })
            .collect::<Vec<_>>();
        assert!(null_rows.iter().all(|null_row| last_group_next < *null_row));
    }

    #[test]
    fn planned_index_scan_opens_table_only_for_uncovered_columns() {
        for (sql, table_required) in [
            ("SELECT value FROM items INDEXED BY items_value", false),
            ("SELECT extra FROM items INDEXED BY items_value", true),
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Scan {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &planned_loop.access
            else {
                let HirSourceAccess::BTree { operation, .. } = &planned_loop.access else {
                    panic!("INDEXED BY selects B-tree access for {sql}");
                };
                panic!("INDEXED BY selects an index scan for {sql}: {operation:?}");
            };
            assert_eq!(index.name, "items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let source = planned_loop.source;
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned index scan emits");

            let SourceBinding::BTree {
                scan_cursor,
                table_cursor,
            } = program
                .source_binding(source)
                .copied()
                .expect("source has a physical binding")
            else {
                panic!("index scan binds B-tree cursors");
            };
            assert!(matches!(
                program.get_cursor_type(scan_cursor),
                Some(CursorType::BTreeIndex(index)) if index.name == "items_value"
            ));
            assert_eq!(table_cursor.is_some(), table_required);
            assert!(program.insns.iter().any(|(insn, _)| {
                matches!(
                    insn,
                    Insn::OpenRead {
                        cursor_id,
                        root_page: 3,
                        db: MAIN_DB_ID,
                    } if *cursor_id == scan_cursor
                )
            }));

            match table_cursor {
                Some(table_cursor) => {
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::DeferredSeek {
                                index_cursor_id,
                                table_cursor_id,
                            } if *index_cursor_id == scan_cursor
                                && *table_cursor_id == table_cursor
                        )
                    }));
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::Column {
                                cursor_id,
                                column: 1,
                                ..
                            } if *cursor_id == table_cursor
                        )
                    }));
                }
                None => {
                    assert!(!program
                        .insns
                        .iter()
                        .any(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. })));
                    assert!(program.insns.iter().any(|(insn, _)| {
                        matches!(
                            insn,
                            Insn::Column {
                                cursor_id,
                                column: 0,
                                ..
                            } if *cursor_id == scan_cursor
                        )
                    }));
                }
            }
        }
    }

    #[test]
    fn not_indexed_keeps_the_hir_plan_on_the_table() {
        let plan = analyze_plan(
            indexed_items_schema(),
            "SELECT value FROM items NOT INDEXED",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];

        assert!(matches!(
            &planned_loop.access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Scan { index: None, .. },
                table_lookup: BtreeTableLookup::ScanOnly,
            }
        ));
    }

    #[test]
    fn planned_rowid_equality_emits_one_point_lookup() {
        for sql in [
            "SELECT value FROM items WHERE rowid = 2",
            "SELECT value FROM items WHERE id = 2",
        ] {
            let plan = analyze_plan(rowid_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            assert!(matches!(
                &planned_loop.access,
                HirSourceAccess::BTree {
                    operation: HirBtreeOperation::RowidEq { .. },
                    table_lookup: BtreeTableLookup::ScanOnly,
                }
            ));
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned rowid equality emits");

            let SourceBinding::BTree {
                scan_cursor,
                table_cursor: None,
            } = program
                .source_binding(planned_loop.source)
                .copied()
                .expect("source has a physical binding")
            else {
                panic!("rowid equality binds one table cursor");
            };
            assert!(matches!(
                program.get_cursor_type(scan_cursor),
                Some(CursorType::BTreeTable(table)) if table.name == "items"
            ));
            assert!(program.insns.iter().any(|(insn, _)| {
                matches!(
                    insn,
                    Insn::SeekRowid { cursor_id, .. } if *cursor_id == scan_cursor
                )
            }));
            assert!(!program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::Next { .. } | Insn::Prev { .. })));
        }
    }

    #[test]
    fn planned_hir_ranges_use_the_shared_seek_emitter() {
        for (sql, table_required) in [
            (
                "SELECT value FROM items INDEXED BY items_value WHERE value >= 2 AND value < 5",
                false,
            ),
            (
                "SELECT extra FROM items INDEXED BY items_value WHERE value >= 2 AND value < 5",
                true,
            ),
        ] {
            let plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation:
                    HirBtreeOperation::Seek {
                        index: Some(index), ..
                    },
                table_lookup,
            } = &planned_loop.access
            else {
                panic!("indexed range selects an index seek: {sql}");
            };
            assert_eq!(index.name, "items_value");
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned index range emits");

            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::SeekGE { is_index: true, .. }
                    | Insn::SeekGT { is_index: true, .. }
                    | Insn::SeekLE { is_index: true, .. }
                    | Insn::SeekLT { is_index: true, .. }
            )));
            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::IdxGE { .. } | Insn::IdxGT { .. } | Insn::IdxLE { .. } | Insn::IdxLT { .. }
            )));
            assert!(program.insns.iter().any(|(insn, _)| matches!(
                insn,
                Insn::Next {
                    fullscan: false,
                    ..
                } | Insn::Prev {
                    fullscan: false,
                    ..
                }
            )));
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
        }

        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid >= 2 AND rowid < 5",
        );
        let query = root_query(&plan.document);
        let planned_loop = &plan
            .planned_query(query)
            .expect("root query is planned")
            .blocks[0]
            .loops[0];
        assert!(matches!(
            &planned_loop.access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Seek { index: None, .. },
                table_lookup: BtreeTableLookup::ScanOnly,
            }
        ));
        let mut program = program();

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned rowid range emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SeekGE {
                is_index: false,
                ..
            } | Insn::SeekGT {
                is_index: false,
                ..
            } | Insn::SeekLE {
                is_index: false,
                ..
            } | Insn::SeekLT {
                is_index: false,
                ..
            }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::RowId { .. })));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Ge { .. } | Insn::Gt { .. } | Insn::Le { .. } | Insn::Lt { .. }
        )));
    }

    #[test]
    fn planned_hir_in_lists_use_the_shared_two_level_loop() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid IN (1, 2, 2)",
        );
        let query = root_query(&plan.document);
        assert!(matches!(
            &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0]
                .access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::InSeek {
                    index: None,
                    source: HirInSeekSource::Values { values, .. },
                },
                ..
            } if values.len() == 3
        ));
        let mut rowid_program = program();

        emit_planned_query_body(
            &mut rowid_program,
            &plan,
            query,
            &QueryDestination::ResultRows,
        )
        .expect("planned rowid IN seek emits");

        assert_eq!(
            rowid_program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::IdxInsert { .. }))
                .count(),
            3
        );
        assert!(rowid_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SeekRowid { .. })));
        assert!(!rowid_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IdxGT { .. })));

        for (sql, table_required) in [
            (
                "SELECT value FROM items INDEXED BY items_value WHERE value IN (1, 2, 2)",
                false,
            ),
            (
                "SELECT extra FROM items INDEXED BY items_value WHERE value IN (1, 2, 2)",
                true,
            ),
        ] {
            let mut plan = analyze_plan(indexed_items_schema(), sql);
            let query = root_query(&plan.document);
            let values = match &plan
                .document
                .query(query)
                .expect("root query exists")
                .blocks[0]
                .body
            {
                hir::QueryBlockBody::Select {
                    filter: Some(Expr::InList { values, .. }),
                    ..
                } => values.clone(),
                _ => panic!("indexed test has an IN-list filter"),
            };
            let planned_query = plan
                .queries
                .iter_mut()
                .find(|planned| planned.query == query)
                .expect("root query is planned");
            let HirSourceAccess::BTree { operation, .. } =
                &mut planned_query.blocks[0].loops[0].access
            else {
                panic!("indexed IN uses a B-tree plan");
            };
            let HirBtreeOperation::Scan {
                index: Some(index), ..
            } = operation
            else {
                panic!("cost model starts from the requested index");
            };
            *operation = HirBtreeOperation::InSeek {
                index: Some(index.clone()),
                source: HirInSeekSource::Values {
                    values,
                    affinity: Affinity::Integer,
                },
            };
            let planned_loop = &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0];
            let HirSourceAccess::BTree {
                operation,
                table_lookup,
            } = &planned_loop.access
            else {
                panic!("indexed IN uses a B-tree plan");
            };
            assert!(
                matches!(
                    operation,
                    HirBtreeOperation::InSeek {
                        index: Some(index),
                        source: HirInSeekSource::Values { .. },
                    } if index.name == "items_value"
                ),
                "unexpected indexed IN operation for {sql}: {operation:?}"
            );
            assert_eq!(
                *table_lookup,
                if table_required {
                    BtreeTableLookup::TableRequired
                } else {
                    BtreeTableLookup::ScanOnly
                }
            );
            let mut program = program();

            emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
                .expect("planned indexed IN seek emits");

            assert!(program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::SeekGE { is_index: true, .. })));
            assert!(program
                .insns
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::IdxGT { .. })));
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(insn, Insn::DeferredSeek { .. }))
                    .count(),
                usize::from(table_required)
            );
            assert_eq!(
                program
                    .insns
                    .iter()
                    .filter(|(insn, _)| matches!(
                        insn,
                        Insn::Next {
                            fullscan: false,
                            ..
                        }
                    ))
                    .count(),
                2
            );
        }
    }

    #[test]
    fn planned_hir_in_subquery_reuses_its_prepared_index() {
        let plan = analyze_plan(
            rowid_items_schema(),
            "SELECT value FROM items WHERE rowid IN (VALUES (1), (2))",
        );
        let query = root_query(&plan.document);
        let subquery = root_filter_subquery(&plan.document);
        let SubqueryExpr::In {
            query: source_query,
            ..
        } = subquery
        else {
            panic!("root filter is an IN subquery");
        };
        assert!(matches!(
            &plan
                .planned_query(query)
                .expect("root query is planned")
                .blocks[0]
                .loops[0]
                .access,
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::InSeek {
                    source: HirInSeekSource::Query { query },
                    ..
                },
                ..
            } if query == source_query
        ));
        let mut program = program();
        let prepared = prepare_subquery(&mut program, &plan.document, subquery)
            .expect("IN subquery destination prepares");
        let Some(SubqueryBinding::InIndex {
            cursor: source_cursor,
        }) = program.subquery_binding(*source_query)
        else {
            panic!("IN subquery has an index binding");
        };
        emit_prepared_subquery(&mut program, &prepared, |program, query, destination| {
            emit_planned_query_body(program, &plan, query, destination)
        })
        .expect("IN subquery emits");

        emit_planned_query_body(&mut program, &plan, query, &QueryDestination::ResultRows)
            .expect("planned query IN seek emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Rewind { cursor_id, .. } if *cursor_id == source_cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Column {
                cursor_id,
                column: 0,
                ..
            } if *cursor_id == source_cursor
        )));
    }

    #[test]
    fn shared_hir_seek_emitter_keeps_backward_bounds() {
        use crate::{
            translate::plan::{SeekDef, SeekKey, SeekKeyComponent},
            types::SeekOp,
        };

        let document = analyze_sql(rowid_items_schema(), "SELECT value FROM items");
        let source = &document.sources[0];
        let seek_def = SeekDef {
            prefix: Vec::new(),
            start: SeekKey {
                last_component: SeekKeyComponent::Expr(Expr::Literal(Literal::Numeric(
                    "5".to_string(),
                ))),
                op: SeekOp::LE { eq_only: false },
                affinity: Affinity::Integer,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::Expr(Expr::Literal(Literal::Numeric(
                    "2".to_string(),
                ))),
                op: SeekOp::LE { eq_only: false },
                affinity: Affinity::Integer,
            },
            iter_dir: IterationDirection::Backwards,
        };
        let mut program = program();
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(match &source.kind {
            hir::SourceKind::Table(table) => match table.value() {
                Table::BTree(table) => table.clone(),
                _ => panic!("test source is a B-tree table"),
            },
            _ => panic!("test source is a table"),
        }));
        let start_register = program.alloc_register();
        let loop_start = program.allocate_label();
        let done = program.allocate_label();

        SeekEmitter::with_lowering(
            &mut program,
            &seek_def,
            HirSeekExpressionLowering {
                document: &document,
                source,
            },
            cursor,
            start_register,
            done,
            None,
        )
        .emit(loop_start, false)
        .expect("backward HIR seek emits");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::SeekLE {
                is_index: false,
                cursor_id,
                ..
            } if *cursor_id == cursor
        )));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Le { .. })));
    }
}
