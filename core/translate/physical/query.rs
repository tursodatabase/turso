//! Scan-first query emission from a closed physical HIR plan.
//!
//! This is deliberately a small executable boundary, not a compatibility
//! bridge to the parser-AST planner. A supported query is emitted directly;
//! every other shape returns an explicit error.

use std::fmt;

use rustc_hash::{FxHashMap, FxHashSet};
use turso_ext::{ConstraintInfo, ConstraintOp};
use turso_parser::ast::{CompoundOperator, Distinctness, Literal, SortOrder};

use crate::{
    function::{AccumulatorFunc, AggFunc, Func},
    schema::{
        BTreeCharacteristics, BTreeTable, Column, Index, IndexColumn, PseudoCursorType, Table,
    },
    sync::Arc,
    translate::collate::CollationSeq,
    translate::semantic::hir::{
        Assignment, CteBody, CteId, Expr, From as HirFrom, Grouping, Join, JoinConstraint,
        JoinKind, OrderTerm, QueryBlockBody, QueryBlockId, QueryId, ResolvedTable, SourceId,
        SubqueryExpr, TypeFact,
    },
    types::KeyInfo,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, HashDistinctData, IdxInsertFlags, InsertFlags, Insn, SortComparatorType},
    },
};

use super::{
    AggregateRuntime, ExpressionEmitter, ExpressionResult, OutputRuntime, PhysicalAggregate,
    PhysicalExpressionError, PhysicalPlan, PhysicalRoot, PhysicalSource, PhysicalSourceKind,
    PhysicalSubqueryEmitter, QueryRuntime, RegisterId, RegisterRange, RootRuntimeInputs,
    RuntimeBindingError, RuntimeBindings, SourceRuntime, TableAccess,
};

#[derive(Debug)]
pub(crate) enum PhysicalQueryError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical query: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "physical query is not emitted yet: {message}")
            }
        }
    }
}

impl std::error::Error for PhysicalQueryError {}

impl From<RuntimeBindingError> for PhysicalQueryError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalQueryError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

type QueryResult<T> = std::result::Result<T, PhysicalQueryError>;

#[derive(Clone, Copy)]
enum ScanCursor {
    BTree(usize),
    Virtual(usize),
}

struct VirtualFilter<'hir> {
    arguments: Vec<&'hir Expr>,
    idx_str: Option<usize>,
    idx_num: usize,
}

struct OpenedScan<'hir> {
    cursor: ScanCursor,
    runtime_cursor: usize,
    deferred_table: Option<usize>,
    virtual_filter: Option<VirtualFilter<'hir>>,
    owned: bool,
}

#[derive(Clone, Copy)]
enum ScanRowAction<'hir, 'destination> {
    Project {
        outputs: &'hir [crate::translate::semantic::hir::Output],
        result: RegisterRange,
        destination: QueryDestination<'destination>,
        limit: Option<LimitRuntime>,
        distinct: Option<&'destination DistinctRuntime>,
    },
    Aggregate {
        aggregates: &'destination [PhysicalAggregate<'hir>],
    },
    GroupSortInsert {
        sorter: &'destination GroupSorter<'hir>,
    },
    UpdateCandidate {
        target: SourceId,
        assignments: &'hir [Assignment],
        cursor: usize,
        table: &'destination BTreeTable,
    },
}

impl ScanRowAction<'_, '_> {
    fn cleanup_label(self) -> Option<crate::vdbe::BranchOffset> {
        match self {
            Self::Project {
                destination, limit, ..
            } => row_cleanup_label(destination, limit),
            Self::Aggregate { .. }
            | Self::GroupSortInsert { .. }
            | Self::UpdateCandidate { .. } => None,
        }
    }
}

struct GroupSourceLayout {
    source: SourceId,
    width: usize,
    rowid_available: bool,
    record_offset: usize,
}

struct GroupSorter<'hir> {
    cursor_id: usize,
    record: RegisterId,
    field_count: usize,
    grouping: &'hir Grouping,
    sources: Vec<GroupSourceLayout>,
}

#[derive(Clone, Copy)]
enum QueryDestination<'table> {
    ResultRows,
    EphemeralTable {
        cursor_id: usize,
        table: &'table BTreeTable,
    },
    CompoundIndex {
        cursor_id: usize,
        index: &'table Index,
        delete: bool,
    },
    Scalar {
        registers: RegisterRange,
        done: crate::vdbe::BranchOffset,
    },
    Exists {
        register: RegisterId,
        done: crate::vdbe::BranchOffset,
    },
    Sorter {
        cursor_id: usize,
        record: RegisterId,
        order_by: &'table [OrderTerm],
        first_block: QueryBlockId,
    },
}

impl QueryDestination<'_> {
    const fn early_exit_label(self) -> Option<crate::vdbe::BranchOffset> {
        match self {
            Self::Scalar { done, .. } | Self::Exists { done, .. } => Some(done),
            Self::ResultRows
            | Self::EphemeralTable { .. }
            | Self::CompoundIndex { .. }
            | Self::Sorter { .. } => None,
        }
    }
}

#[derive(Clone, Copy)]
struct OpenedSorter<'hir> {
    cursor_id: usize,
    record: RegisterId,
    order_by: &'hir [OrderTerm],
    first_block: QueryBlockId,
    width: usize,
}

#[derive(Clone, Copy)]
struct LimitRuntime {
    limit: RegisterId,
    offset: Option<RegisterId>,
    done: crate::vdbe::BranchOffset,
    stopped: Option<RegisterId>,
}

struct DistinctRuntime {
    hash_table_id: usize,
    collations: Vec<CollationSeq>,
}

impl<'hir> OpenedSorter<'hir> {
    const fn destination(self) -> QueryDestination<'hir> {
        QueryDestination::Sorter {
            cursor_id: self.cursor_id,
            record: self.record,
            order_by: self.order_by,
            first_block: self.first_block,
        }
    }
}

struct MaterializedCte {
    cursor_id: usize,
    table: Arc<BTreeTable>,
    width: usize,
}

#[derive(Default)]
struct MaterializedCtes {
    by_id: FxHashMap<CteId, MaterializedCte>,
    visiting: FxHashSet<CteId>,
    temporary_cursors: Vec<usize>,
}

pub(crate) struct MaterializedQuery {
    pub(crate) cursor: usize,
    cleanup_cursors: Vec<usize>,
}

pub(crate) struct MaterializedUpdateRows {
    pub(crate) cursor: usize,
    pub(crate) width: usize,
}

impl MaterializedUpdateRows {
    pub(crate) fn close(self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Close {
            cursor_id: self.cursor,
        });
    }
}

impl MaterializedQuery {
    pub(crate) fn close(self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Close {
            cursor_id: self.cursor,
        });
        for cursor_id in self.cleanup_cursors.into_iter().rev() {
            program.emit_insn(Insn::Close { cursor_id });
        }
    }
}

struct QuerySubqueryEmitter<'plan, 'document, 'ctes> {
    plan: &'plan PhysicalPlan<'document>,
    ctes: &'ctes mut MaterializedCtes,
}

impl<'document> PhysicalSubqueryEmitter<'document> for QuerySubqueryEmitter<'_, 'document, '_> {
    fn emit_subquery(
        &mut self,
        program: &mut ProgramBuilder,
        bindings: &mut RuntimeBindings<'document>,
        subquery: &SubqueryExpr,
    ) -> ExpressionResult<QueryRuntime> {
        let query_id = match subquery {
            SubqueryExpr::Scalar { query, .. }
            | SubqueryExpr::Exists(query)
            | SubqueryExpr::In { query, .. } => *query,
        };
        let query = self
            .plan
            .query(query_id)
            .ok_or(PhysicalExpressionError::Subquery(
                "query is missing from the physical plan".to_string(),
            ))?;
        match subquery {
            SubqueryExpr::Scalar { .. } => {
                let width = query.hir.output.len();
                if width == 0 {
                    return Err(PhysicalExpressionError::Subquery(
                        "scalar query has no outputs".to_string(),
                    ));
                }
                let first = program.alloc_registers(width);
                let registers = RegisterRange::new(first, width);
                program.emit_insn(Insn::Null {
                    dest: first,
                    dest_end: (width > 1).then_some(first + width - 1),
                });
                let runtime = QueryRuntime::Registers(registers);
                bindings.bind_query(query_id, runtime)?;
                let done = program.allocate_label();
                emit_query(
                    self.plan,
                    program,
                    bindings,
                    self.ctes,
                    query_id,
                    QueryDestination::Scalar { registers, done },
                )
                .map_err(|error| PhysicalExpressionError::Subquery(error.to_string()))?;
                Ok(runtime)
            }
            SubqueryExpr::Exists(_) => {
                let register = RegisterId(program.alloc_register());
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: register.0,
                });
                let runtime = QueryRuntime::Exists(register);
                bindings.bind_query(query_id, runtime)?;
                let done = program.allocate_label();
                emit_query(
                    self.plan,
                    program,
                    bindings,
                    self.ctes,
                    query_id,
                    QueryDestination::Exists { register, done },
                )
                .map_err(|error| PhysicalExpressionError::Subquery(error.to_string()))?;
                Ok(runtime)
            }
            SubqueryExpr::In { comparison, .. } => {
                let width = comparison.components.len();
                if width == 0 || query.hir.output.len() != width {
                    return Err(PhysicalExpressionError::Subquery(
                        "IN query width does not match its comparison facts".to_string(),
                    ));
                }
                let table = ephemeral_table(format!("in_subquery_{}", query_id.index()), width);
                let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: cursor,
                    is_table: true,
                });
                let runtime = QueryRuntime::RowSet(super::CursorId(cursor));
                bindings.bind_query(query_id, runtime)?;
                emit_query(
                    self.plan,
                    program,
                    bindings,
                    self.ctes,
                    query_id,
                    QueryDestination::EphemeralTable {
                        cursor_id: cursor,
                        table: &table,
                    },
                )
                .map_err(|error| PhysicalExpressionError::Subquery(error.to_string()))?;
                self.ctes.temporary_cursors.push(cursor);
                Ok(runtime)
            }
        }
    }
}

impl ScanCursor {
    const fn id(self) -> usize {
        match self {
            Self::BTree(cursor) | Self::Virtual(cursor) => cursor,
        }
    }
}

/// Emit a root query using only resolved HIR and runtime identities.
///
/// The caller owns statement setup, result metadata, transaction setup, and
/// final program construction. This function owns the root query's runtime
/// scope and emits only its executable row production.
pub(crate) fn emit_root_query(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
) -> QueryResult<()> {
    emit_root_query_with_inputs(plan, program, &RootRuntimeInputs::default())
}

pub(crate) fn emit_root_query_with_inputs(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
) -> QueryResult<()> {
    let query_id = match &plan.root {
        PhysicalRoot::Query(query) => *query,
        PhysicalRoot::Insert(_)
        | PhysicalRoot::Update(_)
        | PhysicalRoot::Delete(_)
        | PhysicalRoot::TriggerPredicate(_) => {
            return Err(PhysicalQueryError::Unsupported("non-query HIR root"));
        }
    };
    let query = plan
        .query(query_id)
        .ok_or(PhysicalQueryError::Invalid("root query is missing"))?;
    if query.hir.parent.is_some() {
        return Err(PhysicalQueryError::Invalid(
            "root query has a lexical parent",
        ));
    }
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    let mut ctes = MaterializedCtes::default();
    for cte in query_tree_ctes(plan, query_id)? {
        materialize_cte(plan, program, &mut bindings, &mut ctes, cte)?;
    }
    let result = emit_query(
        plan,
        program,
        &mut bindings,
        &mut ctes,
        query_id,
        QueryDestination::ResultRows,
    );
    if result.is_ok() {
        for cursor_id in ctes.temporary_cursors.iter().rev() {
            program.emit_insn(Insn::Close {
                cursor_id: *cursor_id,
            });
        }
        for cte in ctes.by_id.values() {
            program.emit_insn(Insn::Close {
                cursor_id: cte.cursor_id,
            });
        }
    }
    result
}

/// Materialize a HIR query for a DML consumer. This keeps query production
/// independent from the target write loop and is safe when source and target
/// name the same table.
pub(crate) fn emit_query_for_dml<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    query_id: QueryId,
) -> QueryResult<MaterializedQuery> {
    let query = plan
        .query(query_id)
        .ok_or(PhysicalQueryError::Invalid("DML query is missing"))?;
    if query.hir.parent.is_some() {
        return Err(PhysicalQueryError::Invalid(
            "DML query has a lexical parent",
        ));
    }
    let mut ctes = MaterializedCtes::default();
    for cte in query_tree_ctes(plan, query_id)? {
        materialize_cte(plan, program, bindings, &mut ctes, cte)?;
    }
    let table = ephemeral_table(
        format!("dml_query_{}", query_id.index()),
        query.hir.output.len(),
    );
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_query(
        plan,
        program,
        bindings,
        &mut ctes,
        query_id,
        QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: &table,
        },
    )?;
    let mut cleanup_cursors = ctes.temporary_cursors;
    cleanup_cursors.extend(ctes.by_id.into_values().map(|cte| cte.cursor_id));
    Ok(MaterializedQuery {
        cursor,
        cleanup_cursors,
    })
}

/// Materialize one stable UPDATE FROM candidate per target rowid. Assignment
/// values are evaluated while every FROM SourceId is still bound; inserting
/// by target rowid makes a later match replace an earlier match, matching
/// SQLite's unspecified single-match choice without carrying source cursors
/// into the write phase.
pub(crate) fn emit_update_from_rows<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    target: SourceId,
    target_cursor: usize,
    from: &HirFrom,
    filter: Option<&Expr>,
    assignments: &'document [Assignment],
) -> QueryResult<MaterializedUpdateRows> {
    let width = assignments
        .iter()
        .map(|assignment| assignment.columns.len())
        .sum::<usize>();
    if width == 0 {
        return Err(PhysicalQueryError::Invalid(
            "UPDATE FROM has no assignment values",
        ));
    }
    let table = ephemeral_table(format!("update_from_{}", target.index()), width);
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });

    let source_ids = std::iter::once(from.first)
        .chain(from.joins.iter().map(|join| join.right))
        .collect::<Vec<_>>();
    let mut ctes = MaterializedCtes::default();
    for source_id in &source_ids {
        let source = plan
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("UPDATE FROM source is missing"))?;
        match source.kind {
            PhysicalSourceKind::Cte(cte) => {
                materialize_cte(plan, program, bindings, &mut ctes, cte)?;
            }
            PhysicalSourceKind::Derived(query) => {
                for cte in query_tree_ctes(plan, query)? {
                    materialize_cte(plan, program, bindings, &mut ctes, cte)?;
                }
            }
            _ => {}
        }
    }

    let mut scans = Vec::with_capacity(source_ids.len() + 1);
    scans.push(OpenedScan {
        cursor: ScanCursor::BTree(target_cursor),
        runtime_cursor: target_cursor,
        deferred_table: None,
        virtual_filter: None,
        owned: false,
    });
    for source_id in &source_ids {
        let source = plan
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("UPDATE FROM source is missing"))?;
        let scan = open_source(plan, program, bindings, &mut ctes, source)?;
        bindings.bind_source(
            source.id,
            SourceRuntime::Cursor(super::CursorId(scan.runtime_cursor)),
        )?;
        scans.push(scan);
    }
    let mut joins = Vec::with_capacity(from.joins.len() + 1);
    joins.push(Join {
        right: from.first,
        kind: JoinKind::Comma,
        constraint: JoinConstraint::None,
    });
    joins.extend(from.joins.iter().cloned());
    emit_nested_scan(
        plan,
        program,
        bindings,
        &mut ctes,
        &scans,
        0,
        &joins,
        filter,
        ScanRowAction::UpdateCandidate {
            target,
            assignments,
            cursor,
            table: &table,
        },
    )?;
    for scan in scans.iter().rev().filter(|scan| scan.owned) {
        program.emit_insn(Insn::Close {
            cursor_id: scan.cursor.id(),
        });
        if let Some(table_cursor) = scan.deferred_table {
            program.emit_insn(Insn::Close {
                cursor_id: table_cursor,
            });
        }
    }
    for cursor_id in ctes.temporary_cursors.iter().rev() {
        program.emit_insn(Insn::Close {
            cursor_id: *cursor_id,
        });
    }
    for cte in ctes.by_id.values() {
        program.emit_insn(Insn::Close {
            cursor_id: cte.cursor_id,
        });
    }
    Ok(MaterializedUpdateRows { cursor, width })
}

fn emit_query<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    query_id: crate::translate::semantic::hir::QueryId,
    destination: QueryDestination<'_>,
) -> QueryResult<()> {
    let query = plan
        .query(query_id)
        .ok_or(PhysicalQueryError::Invalid("query is missing"))?;
    if query.blocks.len() != query.hir.compounds.len() + 1 {
        return Err(PhysicalQueryError::Invalid(
            "query block count does not match its compound arms",
        ));
    }
    if query.blocks[0].id != query.hir.first {
        return Err(PhysicalQueryError::Invalid(
            "query does not start with its first block",
        ));
    }
    if query.blocks.iter().any(|block| block.outputs.is_empty()) {
        return Err(PhysicalQueryError::Invalid("query has no outputs"));
    }
    for (index, arm) in query.hir.compounds.iter().enumerate() {
        if query.blocks[index + 1].id != arm.block {
            return Err(PhysicalQueryError::Invalid(
                "compound arm does not name its physical query block",
            ));
        }
    }
    if query
        .hir
        .compounds
        .iter()
        .any(|arm| arm.operator != CompoundOperator::UnionAll)
    {
        return emit_set_compound_query(plan, program, bindings, ctes, query, destination);
    }
    if !query.hir.compounds.is_empty() && destination.early_exit_label().is_some() {
        return Err(PhysicalQueryError::Unsupported(
            "compound scalar or EXISTS subquery",
        ));
    }
    bindings.enter_query(query_id)?;
    let sorter = if query.hir.order_by.is_empty()
        || matches!(destination, QueryDestination::Exists { .. })
    {
        None
    } else {
        Some(open_sorter(program, query)?)
    };
    let mut limit = open_limit(
        plan,
        program,
        bindings,
        ctes,
        query.hir.limit.as_ref(),
        destination,
    )?;
    let row_destination = sorter.map_or(destination, OpenedSorter::destination);
    let streaming_compound = sorter.is_none() && !query.hir.compounds.is_empty();
    if streaming_compound {
        if let Some(limit) = limit.as_mut() {
            let stopped = RegisterId(program.alloc_register());
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: stopped.0,
            });
            limit.stopped = Some(stopped);
        }
    }
    let row_limit = if sorter.is_some() { None } else { limit };
    let mut result = Ok(());
    for block in &query.blocks {
        let block_limit = if streaming_compound {
            row_limit.map(|limit| LimitRuntime {
                done: program.allocate_label(),
                ..limit
            })
        } else {
            row_limit
        };
        result = emit_query_block(
            plan,
            program,
            bindings,
            ctes,
            block,
            row_destination,
            block_limit,
        );
        if result.is_err() {
            break;
        }
        let manages_group_cleanup = matches!(
            &block.hir.body,
            QueryBlockBody::Select {
                grouping: Some(grouping),
                ..
            } if !grouping.keys.is_empty()
        );
        if block.source_order.is_empty() && !manages_group_cleanup {
            if let Some(done) = row_cleanup_label(row_destination, block_limit) {
                program.preassign_label_to_next_insn(done);
            }
        }
        if streaming_compound {
            if let Some(limit) = row_limit {
                let stopped = limit.stopped.ok_or(PhysicalQueryError::Invalid(
                    "compound LIMIT has no stop register",
                ))?;
                program.emit_insn(Insn::If {
                    reg: stopped.0,
                    target_pc: limit.done,
                    jump_if_null: false,
                });
            }
        }
    }
    if result.is_ok() {
        if let Some(sorter) = sorter {
            result = emit_sorted_rows(program, sorter, destination, limit);
        } else if streaming_compound {
            if let Some(limit) = limit {
                program.preassign_label_to_next_insn(limit.done);
            }
        }
    }
    let leave_result = bindings.leave_query();
    match result {
        Err(error) => Err(error),
        Ok(()) => {
            let left = leave_result?;
            if left != query_id {
                return Err(PhysicalQueryError::Invalid(
                    "left a different runtime query scope",
                ));
            }
            Ok(())
        }
    }
}

fn emit_set_compound_query<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    query: &super::PhysicalQuery<'document>,
    destination: QueryDestination<'_>,
) -> QueryResult<()> {
    let [arm] = query.hir.compounds.as_slice() else {
        return Err(PhysicalQueryError::Unsupported(
            "multi-arm compounds containing UNION, INTERSECT, or EXCEPT",
        ));
    };
    let [left, right] = query.blocks.as_slice() else {
        return Err(PhysicalQueryError::Invalid(
            "binary set compound does not have two blocks",
        ));
    };
    if arm.block != right.id || arm.operator == CompoundOperator::UnionAll {
        return Err(PhysicalQueryError::Invalid(
            "set compound arm does not match its right block",
        ));
    }

    bindings.enter_query(query.id)?;
    let emission = (|| -> QueryResult<()> {
        let sorter = if query.hir.order_by.is_empty()
            || matches!(destination, QueryDestination::Exists { .. })
        {
            None
        } else {
            Some(open_sorter(program, query)?)
        };
        let limit = open_limit(
            plan,
            program,
            bindings,
            ctes,
            query.hir.limit.as_ref(),
            destination,
        )?;
        let row_destination = sorter.map_or(destination, OpenedSorter::destination);
        let row_limit = if sorter.is_some() { None } else { limit };

        let (left_cursor, left_index) = open_compound_index(program, left.outputs)?;
        emit_query_block(
            plan,
            program,
            bindings,
            ctes,
            left,
            QueryDestination::CompoundIndex {
                cursor_id: left_cursor,
                index: &left_index,
                delete: false,
            },
            None,
        )?;

        match arm.operator {
            CompoundOperator::Union => {
                emit_query_block(
                    plan,
                    program,
                    bindings,
                    ctes,
                    right,
                    QueryDestination::CompoundIndex {
                        cursor_id: left_cursor,
                        index: &left_index,
                        delete: false,
                    },
                    None,
                )?;
                emit_compound_index_rows(
                    plan,
                    program,
                    bindings,
                    ctes,
                    left_cursor,
                    left_index.columns.len(),
                    None,
                    row_destination,
                    row_limit,
                )?;
            }
            CompoundOperator::Except => {
                emit_query_block(
                    plan,
                    program,
                    bindings,
                    ctes,
                    right,
                    QueryDestination::CompoundIndex {
                        cursor_id: left_cursor,
                        index: &left_index,
                        delete: true,
                    },
                    None,
                )?;
                emit_compound_index_rows(
                    plan,
                    program,
                    bindings,
                    ctes,
                    left_cursor,
                    left_index.columns.len(),
                    None,
                    row_destination,
                    row_limit,
                )?;
            }
            CompoundOperator::Intersect => {
                let (right_cursor, right_index) = open_compound_index(program, left.outputs)?;
                emit_query_block(
                    plan,
                    program,
                    bindings,
                    ctes,
                    right,
                    QueryDestination::CompoundIndex {
                        cursor_id: right_cursor,
                        index: &right_index,
                        delete: false,
                    },
                    None,
                )?;
                emit_compound_index_rows(
                    plan,
                    program,
                    bindings,
                    ctes,
                    left_cursor,
                    left_index.columns.len(),
                    Some(right_cursor),
                    row_destination,
                    row_limit,
                )?;
                program.emit_insn(Insn::Close {
                    cursor_id: right_cursor,
                });
            }
            CompoundOperator::UnionAll => unreachable!(),
        }
        program.emit_insn(Insn::Close {
            cursor_id: left_cursor,
        });

        if let Some(sorter) = sorter {
            emit_sorted_rows(program, sorter, destination, limit)?;
        }
        Ok(())
    })();
    let leave = bindings.leave_query();
    match emission {
        Err(error) => Err(error),
        Ok(()) => {
            if leave? != query.id {
                return Err(PhysicalQueryError::Invalid(
                    "left a different set-compound query scope",
                ));
            }
            Ok(())
        }
    }
}

fn open_compound_index(
    program: &mut ProgramBuilder,
    outputs: &[crate::translate::semantic::hir::Output],
) -> QueryResult<(usize, Arc<Index>)> {
    if outputs
        .iter()
        .any(|output| output.type_fact.is_array() || sort_comparator(&output.type_fact).is_some())
    {
        return Err(PhysicalQueryError::Unsupported(
            "set compound with a custom comparison",
        ));
    }
    let columns = outputs
        .iter()
        .enumerate()
        .map(|(position, output)| {
            let mut column = IndexColumn::new(&output.name, position);
            column.collation = output
                .collation
                .as_ref()
                .map(|collation| *collation.value());
            column
        })
        .collect();
    let index = Arc::new(Index {
        name: "hir_compound_set".to_string(),
        table_name: String::new(),
        root_page: 0,
        columns,
        unique: false,
        ephemeral: true,
        has_rowid: false,
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

#[allow(clippy::too_many_arguments)]
fn emit_compound_index_rows<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    cursor: usize,
    width: usize,
    required_in: Option<usize>,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
) -> QueryResult<()> {
    let loop_start = program.allocate_label();
    let next = program.allocate_label();
    let cleanup = row_cleanup_label(destination, limit).unwrap_or_else(|| program.allocate_label());
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: cleanup,
    });
    program.preassign_label_to_next_insn(loop_start);
    if let Some(required_in) = required_in {
        let record = program.alloc_register();
        program.emit_insn(Insn::RowData {
            cursor_id: cursor,
            dest: record,
        });
        program.emit_insn(Insn::NotFound {
            cursor_id: required_in,
            target_pc: next,
            record_reg: record,
            num_regs: 0,
        });
    }
    let first = program.alloc_registers(width);
    for position in 0..width {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column: position,
            dest: first + position,
            default: None,
        });
    }
    emit_row_destination(
        plan,
        program,
        bindings,
        ctes,
        RegisterRange::new(first, width),
        destination,
        limit,
        None,
    )?;
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(cleanup);
    Ok(())
}

fn emit_query_block<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
) -> QueryResult<()> {
    let result_start = program.alloc_registers(block.outputs.len());
    let result = RegisterRange::new(result_start, block.outputs.len());
    for (position, output) in block.outputs.iter().enumerate() {
        bindings.bind_output(
            output.id,
            OutputRuntime {
                register: RegisterId(result_start + position),
            },
        )?;
    }

    let distinct = match &block.hir.body {
        QueryBlockBody::Select { distinctness, .. }
            if matches!(distinctness, Some(Distinctness::Distinct)) =>
        {
            let distinct = DistinctRuntime {
                hash_table_id: program.alloc_hash_table_id(),
                collations: block
                    .outputs
                    .iter()
                    .map(|output| {
                        output
                            .collation
                            .as_ref()
                            .map_or(CollationSeq::Binary, |collation| *collation.value())
                    })
                    .collect(),
            };
            program.emit_insn(Insn::HashClear {
                hash_table_id: distinct.hash_table_id,
            });
            Some(distinct)
        }
        QueryBlockBody::Select { .. } | QueryBlockBody::Values { .. } => None,
    };

    let block_result = match &block.hir.body {
        QueryBlockBody::Values { rows } => {
            if block.hir.from.is_some() || !block.source_order.is_empty() {
                return Err(PhysicalQueryError::Invalid("VALUES has a FROM source"));
            }
            for row in rows {
                if row.len() != result.width {
                    return Err(PhysicalQueryError::Invalid(
                        "VALUES row width does not match query output width",
                    ));
                }
                if !matches!(destination, QueryDestination::Exists { .. }) {
                    emit_expressions(plan, program, bindings, ctes, row, result)?;
                }
                emit_row_destination(
                    plan,
                    program,
                    bindings,
                    ctes,
                    result,
                    destination,
                    limit,
                    distinct.as_ref(),
                )?;
            }
            Ok(())
        }
        QueryBlockBody::Select {
            distinctness: _,
            filter,
            grouping,
            windows,
        } => {
            if !windows.is_empty() || !block.window_functions.is_empty() {
                return Err(PhysicalQueryError::Unsupported("window query"));
            }
            if let Some(grouping) = grouping
                .as_ref()
                .filter(|grouping| !grouping.keys.is_empty())
            {
                emit_grouped_aggregate(
                    plan,
                    program,
                    bindings,
                    ctes,
                    block,
                    filter.as_ref(),
                    grouping,
                    result,
                    destination,
                    limit,
                    distinct.as_ref(),
                )
            } else if !block.aggregates.is_empty() || grouping.is_some() {
                emit_ungrouped_aggregate(
                    plan,
                    program,
                    bindings,
                    ctes,
                    block,
                    filter.as_ref(),
                    grouping
                        .as_ref()
                        .and_then(|grouping| grouping.having.as_ref()),
                    result,
                    destination,
                    limit,
                    distinct.as_ref(),
                )
            } else {
                match block.source_order.as_slice() {
                    [] => {
                        if block.hir.from.is_some() {
                            return Err(PhysicalQueryError::Invalid(
                                "FROM clause has no physical source",
                            ));
                        }
                        emit_single_row(
                            plan,
                            program,
                            bindings,
                            ctes,
                            filter.as_ref(),
                            block.outputs,
                            result,
                            destination,
                            limit,
                            distinct.as_ref(),
                        )
                    }
                    sources => emit_table_scans(
                        plan,
                        program,
                        bindings,
                        ctes,
                        sources,
                        filter.as_ref(),
                        block.hir.from.as_ref(),
                        ScanRowAction::Project {
                            outputs: block.outputs,
                            result,
                            destination,
                            limit,
                            distinct: distinct.as_ref(),
                        },
                    ),
                }
            }
        }
    };
    if block_result.is_ok() {
        if let Some(distinct) = distinct {
            program.emit_insn(Insn::HashClose {
                hash_table_id: distinct.hash_table_id,
            });
        }
    }
    block_result
}

fn emit_single_row<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    filter: Option<&crate::translate::semantic::hir::Expr>,
    outputs: &[crate::translate::semantic::hir::Output],
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
) -> QueryResult<()> {
    let skip = program.allocate_label();
    if let Some(filter) = filter {
        emit_filter(plan, program, bindings, ctes, filter, skip)?;
    }
    emit_output_row(
        plan,
        program,
        bindings,
        ctes,
        outputs,
        result,
        destination,
        limit,
        distinct,
    )?;
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

fn open_group_sorter<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    block: &super::PhysicalQueryBlock<'document>,
    grouping: &'document Grouping,
) -> QueryResult<GroupSorter<'document>> {
    let cursor_id = program.alloc_cursor_id(CursorType::Sorter);
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
        .map(sort_comparator)
        .collect();
    program.emit_insn(Insn::SorterOpen {
        cursor_id,
        columns: grouping.keys.len(),
        order_collations_nulls,
        comparators,
    });
    let mut record_offset = grouping.keys.len();
    let mut sources = Vec::with_capacity(block.source_order.len());
    for source_id in &block.source_order {
        let source = plan
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("group source is missing"))?;
        let definition = plan
            .document
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("group HIR source is missing"))?;
        sources.push(GroupSourceLayout {
            source: *source_id,
            width: source.width,
            rowid_available: definition.rowid_available,
            record_offset,
        });
        // Keep one fixed rowid slot per source. Sources without rowid store
        // NULL, which keeps the record layout simple and deterministic.
        record_offset += source.width + 1;
    }
    Ok(GroupSorter {
        cursor_id,
        record: RegisterId(program.alloc_register()),
        field_count: record_offset,
        grouping,
        sources,
    })
}

fn emit_group_sort_insert<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    sorter: &GroupSorter<'document>,
) -> QueryResult<()> {
    let fields = program.alloc_registers(sorter.field_count);
    for (position, key) in sorter.grouping.keys.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(key, RegisterRange::new(fields + position, 1))?;
    }
    for source in &sorter.sources {
        let source_start = fields + source.record_offset;
        for column in 0..source.width {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                &Expr::column(source.source, column),
                RegisterRange::new(source_start + column, 1),
            )?;
        }
        let rowid = source_start + source.width;
        if source.rowid_available {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_into(&Expr::rowid(source.source), RegisterRange::new(rowid, 1))?;
        } else {
            program.emit_insn(Insn::Null {
                dest: rowid,
                dest_end: None,
            });
        }
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(fields),
        count: to_u32(sorter.field_count),
        dest_reg: to_u32(sorter.record.0),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sorter.cursor_id,
        record_reg: sorter.record.0,
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_grouped_aggregate<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    filter: Option<&Expr>,
    grouping: &'document Grouping,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
) -> QueryResult<()> {
    let sorter = open_group_sorter(plan, program, block, grouping)?;
    match block.source_order.as_slice() {
        [] => {
            if block.hir.from.is_some() {
                return Err(PhysicalQueryError::Invalid(
                    "FROM clause has no physical source",
                ));
            }
            let skip = program.allocate_label();
            if let Some(filter) = filter {
                emit_filter(plan, program, bindings, ctes, filter, skip)?;
            }
            emit_group_sort_insert(plan, program, bindings, ctes, &sorter)?;
            program.preassign_label_to_next_insn(skip);
        }
        sources => emit_table_scans(
            plan,
            program,
            bindings,
            ctes,
            sources,
            filter,
            block.hir.from.as_ref(),
            ScanRowAction::GroupSortInsert { sorter: &sorter },
        )?,
    }

    let accumulator_start =
        (!block.aggregates.is_empty()).then(|| program.alloc_registers(block.aggregates.len()));
    if let Some(accumulator_start) = accumulator_start {
        program.emit_insn(Insn::Null {
            dest: accumulator_start,
            dest_end: (block.aggregates.len() > 1)
                .then_some(accumulator_start + block.aggregates.len() - 1),
        });
        for (position, aggregate) in block.aggregates.iter().enumerate() {
            bindings.bind_aggregate(
                aggregate.id,
                AggregateRuntime {
                    register: RegisterId(accumulator_start + position),
                },
            )?;
        }
    }

    let data = program.alloc_register();
    let pseudo = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter.field_count,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo,
        content_reg: data,
        num_fields: sorter.field_count,
    });

    let mut restored_sources = Vec::with_capacity(sorter.sources.len());
    for source in &sorter.sources {
        let row_start = program.alloc_registers(source.width + 1);
        let original = bindings.replace_source(
            source.source,
            SourceRuntime::Registers {
                columns: RegisterRange::new(row_start, source.width),
                rowid: source
                    .rowid_available
                    .then_some(RegisterId(row_start + source.width)),
            },
        )?;
        restored_sources.push((source.source, original));
    }

    let emission = (|| -> QueryResult<()> {
        let previous_keys = program.alloc_registers(grouping.keys.len());
        let current_keys = program.alloc_registers(grouping.keys.len());
        let has_group = program.alloc_register();
        let return_register = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: has_group,
        });

        let loop_start = program.allocate_label();
        let different_group = program.allocate_label();
        let start_group = program.allocate_label();
        let step_group = program.allocate_label();
        let output_group = program.allocate_label();
        let skip_output = program.allocate_label();
        let cleanup =
            row_cleanup_label(destination, limit).unwrap_or_else(|| program.allocate_label());

        program.emit_insn(Insn::SorterSort {
            cursor_id: sorter.cursor_id,
            pc_if_empty: cleanup,
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_insn(Insn::SorterData {
            cursor_id: sorter.cursor_id,
            dest_reg: data,
            pseudo_cursor: pseudo,
        });
        for position in 0..grouping.keys.len() {
            program.emit_insn(Insn::Column {
                cursor_id: pseudo,
                column: position,
                dest: current_keys + position,
                default: None,
            });
        }
        program.emit_insn(Insn::IfNot {
            reg: has_group,
            target_pc: start_group,
            jump_if_null: true,
        });
        let key_info = grouping
            .key_collations
            .iter()
            .map(|collation| KeyInfo {
                sort_order: SortOrder::Asc,
                collation: collation
                    .as_ref()
                    .map_or(CollationSeq::Binary, |collation| *collation.value()),
                nulls_order: None,
            })
            .collect();
        program.emit_insn(Insn::Compare {
            start_reg_a: previous_keys,
            start_reg_b: current_keys,
            count: grouping.keys.len(),
            key_info,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: different_group,
            target_pc_eq: step_group,
            target_pc_gt: different_group,
        });

        program.preassign_label_to_next_insn(different_group);
        program.emit_insn(Insn::Gosub {
            target_pc: output_group,
            return_reg: return_register,
        });
        if let Some(accumulator_start) = accumulator_start {
            program.emit_insn(Insn::Null {
                dest: accumulator_start,
                dest_end: (block.aggregates.len() > 1)
                    .then_some(accumulator_start + block.aggregates.len() - 1),
            });
        }

        program.preassign_label_to_next_insn(start_group);
        program.emit_insn(Insn::Copy {
            src_reg: current_keys,
            dst_reg: previous_keys,
            extra_amount: grouping.keys.len() - 1,
        });
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: has_group,
        });

        program.preassign_label_to_next_insn(step_group);
        for source in &sorter.sources {
            let SourceRuntime::Registers { columns, rowid } = bindings.source(source.source)?
            else {
                return Err(PhysicalQueryError::Invalid(
                    "group source is not backed by registers",
                ));
            };
            for column in 0..source.width {
                program.emit_insn(Insn::Column {
                    cursor_id: pseudo,
                    column: source.record_offset + column,
                    dest: columns.first.0 + column,
                    default: None,
                });
            }
            if let Some(rowid) = rowid {
                program.emit_insn(Insn::Column {
                    cursor_id: pseudo,
                    column: source.record_offset + source.width,
                    dest: rowid.0,
                    default: None,
                });
            }
        }
        emit_aggregate_steps(plan, program, bindings, ctes, &block.aggregates)?;
        program.emit_insn(Insn::SorterNext {
            cursor_id: sorter.cursor_id,
            pc_if_next: loop_start,
        });
        program.emit_insn(Insn::Gosub {
            target_pc: output_group,
            return_reg: return_register,
        });
        program.emit_insn(Insn::Goto { target_pc: cleanup });

        program.preassign_label_to_next_insn(output_group);
        for aggregate in &block.aggregates {
            let Func::Agg(function) = aggregate.call.function.value() else {
                return Err(PhysicalQueryError::Unsupported(
                    "external aggregate function",
                ));
            };
            let register = bindings.aggregate(aggregate.id)?.register;
            program.emit_insn(Insn::AggFinal {
                register: register.0,
                func: AccumulatorFunc::Agg(function.clone()),
            });
        }
        if let Some(having) = &grouping.having {
            emit_filter(plan, program, bindings, ctes, having, skip_output)?;
        }
        emit_output_row(
            plan,
            program,
            bindings,
            ctes,
            block.outputs,
            result,
            destination,
            limit,
            distinct,
        )?;
        program.preassign_label_to_next_insn(skip_output);
        program.emit_insn(Insn::Return {
            return_reg: return_register,
            can_fallthrough: false,
        });

        program.preassign_label_to_next_insn(cleanup);
        program.emit_insn(Insn::Close { cursor_id: pseudo });
        program.emit_insn(Insn::Close {
            cursor_id: sorter.cursor_id,
        });
        Ok(())
    })();

    for (source, runtime) in restored_sources.into_iter().rev() {
        bindings.replace_source(source, runtime)?;
    }
    emission
}

#[allow(clippy::too_many_arguments)]
fn emit_ungrouped_aggregate<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    filter: Option<&Expr>,
    having: Option<&Expr>,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
) -> QueryResult<()> {
    if block.aggregates.is_empty() {
        return Err(PhysicalQueryError::Unsupported(
            "HAVING without an aggregate function",
        ));
    }
    let accumulator_start = program.alloc_registers(block.aggregates.len());
    program.emit_insn(Insn::Null {
        dest: accumulator_start,
        dest_end: (block.aggregates.len() > 1)
            .then_some(accumulator_start + block.aggregates.len() - 1),
    });
    for (position, aggregate) in block.aggregates.iter().enumerate() {
        bindings.bind_aggregate(
            aggregate.id,
            AggregateRuntime {
                register: RegisterId(accumulator_start + position),
            },
        )?;
    }

    match block.source_order.as_slice() {
        [] => {
            if block.hir.from.is_some() {
                return Err(PhysicalQueryError::Invalid(
                    "FROM clause has no physical source",
                ));
            }
            let skip = program.allocate_label();
            if let Some(filter) = filter {
                emit_filter(plan, program, bindings, ctes, filter, skip)?;
            }
            emit_aggregate_steps(plan, program, bindings, ctes, &block.aggregates)?;
            program.preassign_label_to_next_insn(skip);
        }
        sources => emit_table_scans(
            plan,
            program,
            bindings,
            ctes,
            sources,
            filter,
            block.hir.from.as_ref(),
            ScanRowAction::Aggregate {
                aggregates: &block.aggregates,
            },
        )?,
    }

    for aggregate in &block.aggregates {
        let Func::Agg(function) = aggregate.call.function.value() else {
            return Err(PhysicalQueryError::Unsupported(
                "external aggregate function",
            ));
        };
        let register = bindings.aggregate(aggregate.id)?.register;
        program.emit_insn(Insn::AggFinal {
            register: register.0,
            func: AccumulatorFunc::Agg(function.clone()),
        });
    }

    let skip = program.allocate_label();
    if let Some(having) = having {
        emit_filter(plan, program, bindings, ctes, having, skip)?;
    }
    emit_output_row(
        plan,
        program,
        bindings,
        ctes,
        block.outputs,
        result,
        destination,
        limit,
        distinct,
    )?;
    program.preassign_label_to_next_insn(skip);
    Ok(())
}

fn emit_aggregate_steps<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    aggregates: &[PhysicalAggregate<'document>],
) -> QueryResult<()> {
    for aggregate in aggregates {
        let call = aggregate.call;
        if call.distinctness.is_some()
            || !call.argument_order.is_empty()
            || !call.within_group.is_empty()
        {
            return Err(PhysicalQueryError::Unsupported(
                "ordered or DISTINCT aggregate",
            ));
        }
        let Func::Agg(function) = call.function.value() else {
            return Err(PhysicalQueryError::Unsupported(
                "external aggregate function",
            ));
        };
        let skip = call.filter.as_ref().map(|_| program.allocate_label());
        if let (Some(filter), Some(skip)) = (call.filter.as_deref(), skip) {
            emit_filter(plan, program, bindings, ctes, filter, skip)?;
        }
        let (column, delimiter) = match function {
            AggFunc::Count0 => {
                if !call.arguments.is_empty() {
                    return Err(PhysicalQueryError::Invalid(
                        "count(*) has aggregate arguments",
                    ));
                }
                let one = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: one,
                });
                (one, 0)
            }
            AggFunc::Avg | AggFunc::Count | AggFunc::Sum | AggFunc::Total => {
                let [argument] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "aggregate has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(argument)?;
                if value.width != 1 {
                    return Err(PhysicalQueryError::Invalid(
                        "aggregate argument is not scalar",
                    ));
                }
                (value.first.0, 0)
            }
            _ => {
                return Err(PhysicalQueryError::Unsupported(
                    "aggregate function implementation",
                ));
            }
        };
        let accumulator = bindings.aggregate(aggregate.id)?.register;
        program.emit_insn(Insn::AggStep {
            acc_reg: accumulator.0,
            col: column,
            delimiter,
            func: AccumulatorFunc::Agg(function.clone()),
            comparator: None,
            collation: None,
        });
        if let Some(skip) = skip {
            program.preassign_label_to_next_insn(skip);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_table_scans<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    source_ids: &[crate::translate::semantic::hir::SourceId],
    filter: Option<&crate::translate::semantic::hir::Expr>,
    from: Option<&crate::translate::semantic::hir::From>,
    action: ScanRowAction<'document, '_>,
) -> QueryResult<()> {
    let from = from.ok_or(PhysicalQueryError::Invalid(
        "physical sources exist without a FROM clause",
    ))?;
    if source_ids.first().copied() != Some(from.first) || source_ids.len() != from.joins.len() + 1 {
        return Err(PhysicalQueryError::Invalid(
            "physical source order does not match FROM",
        ));
    }
    for join in &from.joins {
        if !matches!(
            join.kind,
            JoinKind::Comma | JoinKind::Inner | JoinKind::Cross | JoinKind::Left
        ) {
            return Err(PhysicalQueryError::Unsupported("RIGHT or FULL OUTER JOIN"));
        }
    }

    let mut scans = Vec::with_capacity(source_ids.len());
    for source_id in source_ids {
        let source = plan
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("query source is missing"))?;
        let scan = open_source(plan, program, bindings, ctes, source)?;
        bindings.bind_source(
            source.id,
            SourceRuntime::Cursor(super::CursorId(scan.runtime_cursor)),
        )?;
        scans.push(scan);
    }

    emit_nested_scan(
        plan,
        program,
        bindings,
        ctes,
        &scans,
        0,
        &from.joins,
        filter,
        action,
    )?;
    if let Some(done) = action.cleanup_label() {
        program.preassign_label_to_next_insn(done);
    }
    for scan in scans.iter().rev() {
        if !scan.owned {
            continue;
        }
        program.emit_insn(Insn::Close {
            cursor_id: scan.cursor.id(),
        });
        if let Some(table_cursor) = scan.deferred_table {
            program.emit_insn(Insn::Close {
                cursor_id: table_cursor,
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_nested_scan<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    scans: &[OpenedScan<'document>],
    level: usize,
    joins: &[crate::translate::semantic::hir::Join],
    filter: Option<&Expr>,
    action: ScanRowAction<'document, '_>,
) -> QueryResult<()> {
    let scan = scans
        .get(level)
        .ok_or(PhysicalQueryError::Invalid("nested scan level is missing"))?;

    let loop_start = program.allocate_label();
    let loop_next = program.allocate_label();
    let loop_end = program.allocate_label();
    let join = level
        .checked_sub(1)
        .and_then(|position| joins.get(position));
    let left_join = join.is_some_and(|join| join.kind == JoinKind::Left);
    let unmatched = left_join.then(|| program.allocate_label());
    let matched = left_join.then(|| program.alloc_register());
    if let Some(matched) = matched {
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: matched,
        });
    }
    let empty = unmatched.unwrap_or(loop_end);
    match scan.cursor {
        ScanCursor::BTree(cursor_id) => program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: empty,
        }),
        ScanCursor::Virtual(cursor_id) => {
            let (arg_count, args_reg, idx_str, idx_num) =
                emit_virtual_filter_arguments(plan, program, bindings, ctes, scan)?;
            program.emit_insn(Insn::VFilter {
                cursor_id,
                pc_if_empty: empty,
                arg_count,
                args_reg,
                idx_str,
                idx_num,
            });
        }
    }
    program.preassign_label_to_next_insn(loop_start);
    if let Some(table_cursor_id) = scan.deferred_table {
        program.emit_insn(Insn::DeferredSeek {
            index_cursor_id: scan.cursor.id(),
            table_cursor_id,
        });
    }

    if let Some(join) = join {
        emit_join_constraint(plan, program, bindings, ctes, &join.constraint, loop_next)?;
    }
    if let Some(matched) = matched {
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: matched,
        });
    }

    if level + 1 < scans.len() {
        emit_nested_scan(
            plan,
            program,
            bindings,
            ctes,
            scans,
            level + 1,
            joins,
            filter,
            action,
        )?;
    } else {
        if let Some(filter) = filter {
            emit_filter(plan, program, bindings, ctes, filter, loop_next)?;
        }
        emit_scan_action(plan, program, bindings, ctes, action)?;
    }
    program.preassign_label_to_next_insn(loop_next);
    match scan.cursor {
        ScanCursor::BTree(cursor_id) => program.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loop_start,
        }),
        ScanCursor::Virtual(cursor_id) => program.emit_insn(Insn::VNext {
            cursor_id,
            pc_if_next: loop_start,
        }),
    }

    if let (Some(unmatched), Some(matched)) = (unmatched, matched) {
        program.preassign_label_to_next_insn(unmatched);
        program.emit_insn(Insn::IfPos {
            reg: matched,
            target_pc: loop_end,
            decrement_by: 0,
        });
        program.emit_insn(Insn::NullRow {
            cursor_id: scan.cursor.id(),
        });
        if let Some(table_cursor) = scan.deferred_table {
            program.emit_insn(Insn::NullRow {
                cursor_id: table_cursor,
            });
        }
        if level + 1 < scans.len() {
            emit_nested_scan(
                plan,
                program,
                bindings,
                ctes,
                scans,
                level + 1,
                joins,
                filter,
                action,
            )?;
        } else {
            if let Some(filter) = filter {
                emit_filter(plan, program, bindings, ctes, filter, loop_end)?;
            }
            emit_scan_action(plan, program, bindings, ctes, action)?;
        }
    }
    program.preassign_label_to_next_insn(loop_end);
    Ok(())
}

fn emit_virtual_filter_arguments<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    scan: &OpenedScan<'document>,
) -> QueryResult<(usize, usize, Option<usize>, usize)> {
    let Some(filter) = &scan.virtual_filter else {
        return Ok((0, 0, None, 0));
    };
    if filter.arguments.is_empty() {
        return Ok((0, 0, filter.idx_str, filter.idx_num));
    }
    let arguments = program.alloc_registers(filter.arguments.len());
    for (position, argument) in filter.arguments.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(argument, RegisterRange::new(arguments + position, 1))?;
    }
    Ok((
        filter.arguments.len(),
        arguments,
        filter.idx_str,
        filter.idx_num,
    ))
}

fn emit_join_constraint<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    constraint: &JoinConstraint,
    skip: crate::vdbe::BranchOffset,
) -> QueryResult<()> {
    match constraint {
        JoinConstraint::None => return Ok(()),
        JoinConstraint::On(expression) => {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            let condition = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_new(expression)?;
            if condition.width != 1 {
                return Err(PhysicalQueryError::Invalid("join predicate is not scalar"));
            }
            program.emit_insn(Insn::IfNot {
                reg: condition.first.0,
                target_pc: skip,
                jump_if_null: true,
            });
        }
        JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
            for column in columns {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let condition =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_using_equality(column)?;
                if condition.width != 1 {
                    return Err(PhysicalQueryError::Invalid("join predicate is not scalar"));
                }
                program.emit_insn(Insn::IfNot {
                    reg: condition.first.0,
                    target_pc: skip,
                    jump_if_null: true,
                });
            }
        }
    }
    Ok(())
}

fn emit_scan_action<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    action: ScanRowAction<'document, '_>,
) -> QueryResult<()> {
    match action {
        ScanRowAction::Project {
            outputs,
            result,
            destination,
            limit,
            distinct,
        } => emit_output_row(
            plan,
            program,
            bindings,
            ctes,
            outputs,
            result,
            destination,
            limit,
            distinct,
        ),
        ScanRowAction::Aggregate { aggregates } => {
            emit_aggregate_steps(plan, program, bindings, ctes, aggregates)
        }
        ScanRowAction::GroupSortInsert { sorter } => {
            emit_group_sort_insert(plan, program, bindings, ctes, sorter)
        }
        ScanRowAction::UpdateCandidate {
            target,
            assignments,
            cursor,
            table,
        } => {
            let SourceRuntime::Cursor(target_cursor) = bindings.source(target)? else {
                return Err(PhysicalQueryError::Invalid(
                    "UPDATE FROM target is not cursor-backed",
                ));
            };
            let rowid = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: target_cursor.0,
                dest: rowid,
            });
            let width = assignments
                .iter()
                .map(|assignment| assignment.columns.len())
                .sum::<usize>();
            let values = program.alloc_registers(width);
            let mut offset = 0;
            for assignment in assignments {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                    &assignment.value,
                    RegisterRange::new(values + offset, assignment.columns.len()),
                )?;
                offset += assignment.columns.len();
            }
            let record = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(values),
                count: to_u32(width),
                dest_reg: to_u32(record),
                index_name: Some(table.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::Insert {
                cursor,
                key_reg: rowid,
                record_reg: record,
                flag: InsertFlags::new(),
                table_name: table.name.clone(),
            });
            Ok(())
        }
    }
}

fn open_source<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    source: &PhysicalSource<'document>,
) -> QueryResult<OpenedScan<'document>> {
    let PhysicalSourceKind::CatalogTable { table, access } = &source.kind else {
        return match &source.kind {
            PhysicalSourceKind::Derived(query) => {
                open_derived_source(plan, program, bindings, ctes, source, *query)
            }
            PhysicalSourceKind::TableFunction { table, arguments } => {
                open_table_function(program, table, arguments)
            }
            PhysicalSourceKind::RecursiveInput(_)
            | PhysicalSourceKind::Pseudo { .. }
            | PhysicalSourceKind::SchemaExpression => {
                Err(PhysicalQueryError::Unsupported("non-table FROM source"))
            }
            PhysicalSourceKind::Cte(cte) => open_cte_source(program, ctes, source, *cte),
            PhysicalSourceKind::CatalogTable { .. } => unreachable!(),
        };
    };
    let database_id = table.database().ok_or(PhysicalQueryError::Invalid(
        "catalog table has no database identity",
    ))?;
    let database = database_id.index();
    if let TableAccess::ForcedIndex(index) = access {
        let Table::BTree(table) = table.value() else {
            return Err(PhysicalQueryError::Unsupported(
                "forced index on a non-B-tree table",
            ));
        };
        if index.database() != Some(database_id)
            || index.value().index_method.is_some()
            || index.value().where_clause.is_some()
            || !index.value().has_rowid
        {
            return Err(PhysicalQueryError::Unsupported(
                "partial, custom, or rowid-free forced index",
            ));
        }
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.handle()));
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor,
            root_page: table.root_page,
            db: database,
        });
        program.emit_insn(Insn::OpenRead {
            cursor_id: index_cursor,
            root_page: index.value().root_page,
            db: database,
        });
        return Ok(OpenedScan {
            cursor: ScanCursor::BTree(index_cursor),
            runtime_cursor: table_cursor,
            deferred_table: Some(table_cursor),
            virtual_filter: None,
            owned: true,
        });
    }

    match table.value() {
        Table::BTree(table) => {
            let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: cursor,
                root_page: table.root_page,
                db: database,
            });
            Ok(OpenedScan {
                cursor: ScanCursor::BTree(cursor),
                runtime_cursor: cursor,
                deferred_table: None,
                virtual_filter: None,
                owned: true,
            })
        }
        Table::Virtual(table) => {
            let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table.clone()));
            program.emit_insn(Insn::VOpen { cursor_id: cursor });
            Ok(OpenedScan {
                cursor: ScanCursor::Virtual(cursor),
                runtime_cursor: cursor,
                deferred_table: None,
                virtual_filter: None,
                owned: true,
            })
        }
        Table::FromClauseSubquery(_) | Table::RecursiveCteInput(_) => {
            Err(PhysicalQueryError::Unsupported("non-catalog table cursor"))
        }
    }
}

fn open_table_function<'document>(
    program: &mut ProgramBuilder,
    resolved: &'document ResolvedTable,
    arguments: &'document [Expr],
) -> QueryResult<OpenedScan<'document>> {
    let Table::Virtual(table) = resolved.value() else {
        return Err(PhysicalQueryError::Invalid(
            "table function did not resolve to a virtual table",
        ));
    };
    let hidden_columns = table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(position, column)| column.hidden().then_some(position))
        .collect::<Vec<_>>();
    if arguments.len() > hidden_columns.len() {
        return Err(PhysicalQueryError::Invalid(
            "table function has more arguments than hidden columns",
        ));
    }
    let constraints = arguments
        .iter()
        .zip(hidden_columns)
        .enumerate()
        .map(|(position, (argument, column_index))| ConstraintInfo {
            column_index: column_index as u32,
            op: if matches!(argument, Expr::Literal(Literal::Null)) {
                ConstraintOp::IsNull
            } else {
                ConstraintOp::Eq
            },
            usable: true,
            index: position,
        })
        .collect::<Vec<_>>();
    let index = table
        .best_index(&constraints, &[])
        .map_err(|_| PhysicalQueryError::Unsupported("table-function argument constraints"))?;
    if index.constraint_usages.len() != constraints.len() {
        return Err(PhysicalQueryError::Invalid(
            "table function returned the wrong constraint count",
        ));
    }
    let mut ordered_arguments = vec![None; arguments.len()];
    for (position, usage) in index.constraint_usages.iter().enumerate() {
        let Some(argv_index) = usage.argv_index else {
            return Err(PhysicalQueryError::Unsupported(
                "table-function argument was not accepted",
            ));
        };
        if !usage.omit {
            return Err(PhysicalQueryError::Unsupported(
                "table-function residual argument constraint",
            ));
        }
        let argv_index = argv_index as usize;
        if argv_index == 0 || argv_index > ordered_arguments.len() {
            return Err(PhysicalQueryError::Invalid(
                "table function returned an invalid argument position",
            ));
        }
        let slot = &mut ordered_arguments[argv_index - 1];
        if slot.replace(&arguments[position]).is_some() {
            return Err(PhysicalQueryError::Invalid(
                "table function returned a duplicate argument position",
            ));
        }
    }
    let ordered_arguments = ordered_arguments
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(PhysicalQueryError::Invalid(
            "table function returned a gapped argument order",
        ))?;
    let idx_str = index.idx_str.map(|value| {
        let register = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value,
            dest: register,
        });
        register
    });
    let cursor = program.alloc_cursor_id(CursorType::VirtualTable(table.clone()));
    program.emit_insn(Insn::VOpen { cursor_id: cursor });
    Ok(OpenedScan {
        cursor: ScanCursor::Virtual(cursor),
        runtime_cursor: cursor,
        deferred_table: None,
        virtual_filter: Some(VirtualFilter {
            arguments: ordered_arguments,
            idx_str,
            idx_num: index.idx_num as usize,
        }),
        owned: true,
    })
}

fn open_derived_source<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    source: &PhysicalSource<'document>,
    query: crate::translate::semantic::hir::QueryId,
) -> QueryResult<OpenedScan<'document>> {
    let query_plan = plan
        .query(query)
        .ok_or(PhysicalQueryError::Invalid("derived query is missing"))?;
    if !query_plan.hir.captures.is_empty() {
        return Err(PhysicalQueryError::Unsupported(
            "correlated FROM subquery materialization",
        ));
    }
    if query_plan.hir.output.len() != source.width {
        return Err(PhysicalQueryError::Invalid(
            "derived query width does not match its source",
        ));
    }

    let columns = (0..source.width)
        .map(|position| {
            Column::new_default_text(Some(format!("column_{position}")), "BLOB".to_string(), None)
        })
        .collect();
    let table = Arc::new(BTreeTable::new(
        0,
        format!("derived_{}", source.id.index()),
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    ));
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: cursor,
        is_table: true,
    });
    emit_query(
        plan,
        program,
        bindings,
        ctes,
        query,
        QueryDestination::EphemeralTable {
            cursor_id: cursor,
            table: &table,
        },
    )?;
    Ok(OpenedScan {
        cursor: ScanCursor::BTree(cursor),
        runtime_cursor: cursor,
        deferred_table: None,
        virtual_filter: None,
        owned: true,
    })
}

fn materialize_cte<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    cte_id: CteId,
) -> QueryResult<()> {
    if ctes.by_id.contains_key(&cte_id) {
        return Ok(());
    }
    if !ctes.visiting.insert(cte_id) {
        return Err(PhysicalQueryError::Invalid(
            "non-recursive CTE dependency cycle",
        ));
    }

    let cte = plan
        .document
        .cte(cte_id)
        .ok_or(PhysicalQueryError::Invalid("reachable CTE is missing"))?;
    let query_id = match cte.body {
        CteBody::Query(query) => query,
        CteBody::Recursive(_) => {
            ctes.visiting.remove(&cte_id);
            return Err(PhysicalQueryError::Unsupported("recursive CTE"));
        }
    };
    let query = plan
        .query(query_id)
        .ok_or(PhysicalQueryError::Invalid("CTE body query is missing"))?;
    if query.hir.parent.is_some() || !query.hir.captures.is_empty() {
        ctes.visiting.remove(&cte_id);
        return Err(PhysicalQueryError::Invalid(
            "ordinary CTE query has an outer query dependency",
        ));
    }
    let dependencies = query_tree_ctes(plan, query_id)?;
    let width = cte.columns.len();
    if query.hir.output.len() != width {
        ctes.visiting.remove(&cte_id);
        return Err(PhysicalQueryError::Invalid(
            "CTE query width does not match its columns",
        ));
    }
    let name = cte.name.clone();
    for dependency in dependencies {
        materialize_cte(plan, program, bindings, ctes, dependency)?;
    }

    let table = ephemeral_table(format!("cte_{name}"), width);
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });
    let result = emit_query(
        plan,
        program,
        bindings,
        ctes,
        query_id,
        QueryDestination::EphemeralTable {
            cursor_id,
            table: &table,
        },
    );
    ctes.visiting.remove(&cte_id);
    result?;
    ctes.by_id.insert(
        cte_id,
        MaterializedCte {
            cursor_id,
            table,
            width,
        },
    );
    Ok(())
}

fn open_cte_source<'document>(
    program: &mut ProgramBuilder,
    ctes: &MaterializedCtes,
    source: &PhysicalSource<'document>,
    cte_id: CteId,
) -> QueryResult<OpenedScan<'document>> {
    let materialized = ctes.by_id.get(&cte_id).ok_or(PhysicalQueryError::Invalid(
        "CTE source was not materialized before its query",
    ))?;
    if source.width != materialized.width {
        return Err(PhysicalQueryError::Invalid(
            "CTE source width does not match its materialization",
        ));
    }
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(materialized.table.clone()));
    program.emit_insn(Insn::OpenDup {
        new_cursor_id: cursor,
        original_cursor_id: materialized.cursor_id,
    });
    Ok(OpenedScan {
        cursor: ScanCursor::BTree(cursor),
        runtime_cursor: cursor,
        deferred_table: None,
        virtual_filter: None,
        owned: true,
    })
}

fn ephemeral_table(name: String, width: usize) -> Arc<BTreeTable> {
    let columns = (0..width)
        .map(|position| {
            Column::new_default_text(Some(format!("column_{position}")), "BLOB".to_string(), None)
        })
        .collect();
    Arc::new(BTreeTable::new(
        0,
        name,
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    ))
}

fn query_tree_ctes(plan: &PhysicalPlan<'_>, root: QueryId) -> QueryResult<Vec<CteId>> {
    let mut queries = vec![root];
    let mut ctes = Vec::new();
    let mut position = 0;
    while position < queries.len() {
        let query_id = queries[position];
        position += 1;
        let query = plan
            .query(query_id)
            .ok_or(PhysicalQueryError::Invalid("reachable query is missing"))?;
        for cte in &query.hir.reachable_ctes {
            if !ctes.contains(cte) {
                ctes.push(*cte);
            }
        }
        for child in &plan.queries {
            if child.hir.parent == Some(query_id) && !queries.contains(&child.id) {
                queries.push(child.id);
            }
        }
    }
    Ok(ctes)
}

fn emit_filter<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    filter: &crate::translate::semantic::hir::Expr,
    skip: crate::vdbe::BranchOffset,
) -> QueryResult<()> {
    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
    let condition =
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_new(filter)?;
    if condition.width != 1 {
        return Err(PhysicalQueryError::Invalid("WHERE result is not scalar"));
    }
    program.emit_insn(Insn::IfNot {
        reg: condition.first.0,
        target_pc: skip,
        jump_if_null: true,
    });
    Ok(())
}

fn emit_output_row<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    outputs: &[crate::translate::semantic::hir::Output],
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
) -> QueryResult<()> {
    if matches!(destination, QueryDestination::Exists { .. }) {
        return emit_row_destination(
            plan,
            program,
            bindings,
            ctes,
            result,
            destination,
            limit,
            None,
        );
    }
    for (position, output) in outputs.iter().enumerate() {
        let target = result
            .register(position)
            .ok_or(PhysicalQueryError::Invalid("output register is missing"))?;
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(&output.expr, RegisterRange::new(target.0, 1))?;
    }
    emit_row_destination(
        plan,
        program,
        bindings,
        ctes,
        result,
        destination,
        limit,
        distinct,
    )
}

fn emit_row_destination<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
) -> QueryResult<()> {
    let duplicate = distinct.map(|distinct| {
        let duplicate = program.allocate_label();
        program.emit_insn(Insn::HashDistinct {
            data: Box::new(HashDistinctData {
                hash_table_id: distinct.hash_table_id,
                key_start_reg: result.first.0,
                num_keys: result.width,
                collations: distinct.collations.clone(),
                target_pc: duplicate,
            }),
        });
        duplicate
    });
    let skip = limit.and_then(|limit| {
        limit.offset.map(|offset| {
            let skip = program.allocate_label();
            program.emit_insn(Insn::IfPos {
                reg: offset.0,
                target_pc: skip,
                decrement_by: 1,
            });
            skip
        })
    });

    match destination {
        QueryDestination::Sorter {
            cursor_id,
            record,
            order_by,
            first_block,
        } => {
            let row_start = program.alloc_registers(order_by.len() + result.width);
            for (position, term) in order_by.iter().enumerate() {
                let target = row_start + position;
                if let Some(output) = order_output_position(&term.expr, first_block) {
                    let source = result.register(output).ok_or(PhysicalQueryError::Invalid(
                        "ORDER BY output position is outside the query row",
                    ))?;
                    if source.0 != target {
                        program.emit_insn(Insn::Copy {
                            src_reg: source.0,
                            dst_reg: target,
                            extra_amount: 0,
                        });
                    }
                } else {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(&term.expr, RegisterRange::new(target, 1))?;
                }
            }
            let result_start = row_start + order_by.len();
            if result.width > 0 {
                program.emit_insn(Insn::Copy {
                    src_reg: result.first.0,
                    dst_reg: result_start,
                    extra_amount: result.width - 1,
                });
            }
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(row_start),
                count: to_u32(order_by.len() + result.width),
                dest_reg: to_u32(record.0),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::SorterInsert {
                cursor_id,
                record_reg: record.0,
            });
        }
        destination => emit_result_destination(program, result, destination)?,
    }

    if let Some(limit) = limit {
        emit_limit_decrement(program, limit);
    }
    if let Some(skip) = skip {
        program.preassign_label_to_next_insn(skip);
    }
    if let Some(duplicate) = duplicate {
        program.preassign_label_to_next_insn(duplicate);
    }
    Ok(())
}

const fn row_cleanup_label(
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
) -> Option<crate::vdbe::BranchOffset> {
    match limit {
        Some(limit) => Some(limit.done),
        None => destination.early_exit_label(),
    }
}

fn order_output_position(expression: &Expr, first_block: QueryBlockId) -> Option<usize> {
    match expression {
        Expr::Output(output)
            if output.owner
                == crate::translate::semantic::hir::OutputOwner::QueryBlock(first_block) =>
        {
            Some(output.index)
        }
        Expr::Collate { expr, .. } => order_output_position(expr, first_block),
        _ => None,
    }
}

fn open_sorter<'hir>(
    program: &mut ProgramBuilder,
    query: &super::PhysicalQuery<'hir>,
) -> QueryResult<OpenedSorter<'hir>> {
    let width = query
        .blocks
        .first()
        .ok_or(PhysicalQueryError::Invalid("query has no physical blocks"))?
        .outputs
        .len();
    let cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    let order_collations_nulls = query
        .hir
        .order_by
        .iter()
        .map(|term| {
            (
                term.order,
                term.collation.as_ref().map(|collation| *collation.value()),
                term.nulls,
            )
        })
        .collect();
    let comparators = query
        .hir
        .order_by
        .iter()
        .map(|term| sort_comparator(&term.type_fact))
        .collect();
    program.emit_insn(Insn::SorterOpen {
        cursor_id,
        columns: query.hir.order_by.len(),
        order_collations_nulls,
        comparators,
    });
    Ok(OpenedSorter {
        cursor_id,
        record: RegisterId(program.alloc_register()),
        order_by: &query.hir.order_by,
        first_block: query.hir.first,
        width,
    })
}

fn open_limit<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    limit: Option<&crate::translate::semantic::hir::Limit>,
    destination: QueryDestination<'_>,
) -> QueryResult<Option<LimitRuntime>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let done = destination
        .early_exit_label()
        .unwrap_or_else(|| program.allocate_label());
    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
    let limit_result = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
        .emit_new(&limit.limit)?;
    if limit_result.width != 1 {
        return Err(PhysicalQueryError::Invalid("LIMIT is not scalar"));
    }
    program.emit_insn(Insn::MustBeInt {
        reg: limit_result.first.0,
        target_pc: None,
    });
    let offset = if let Some(offset) = &limit.offset {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        let offset_result = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_new(offset)?;
        if offset_result.width != 1 {
            return Err(PhysicalQueryError::Invalid("OFFSET is not scalar"));
        }
        program.emit_insn(Insn::MustBeInt {
            reg: offset_result.first.0,
            target_pc: None,
        });
        Some(offset_result.first)
    } else {
        None
    };
    program.emit_insn(Insn::IfNot {
        reg: limit_result.first.0,
        target_pc: done,
        jump_if_null: false,
    });
    Ok(Some(LimitRuntime {
        limit: limit_result.first,
        offset,
        done,
        stopped: None,
    }))
}

fn sort_comparator(type_fact: &TypeFact) -> Option<SortComparatorType> {
    if type_fact.is_array() {
        return Some(SortComparatorType::ArrayLt);
    }
    let declared = type_fact.declared.as_ref()?;
    let custom = declared.custom()?;
    let function = custom
        .value()
        .operators()
        .iter()
        .find(|operator| operator.op == "<")?
        .func_name
        .as_deref()?;
    match function {
        "numeric_lt" => Some(SortComparatorType::NumericLt),
        "test_uint_lt" => Some(SortComparatorType::TestUintLt),
        "string_reverse" => Some(SortComparatorType::StringReverse),
        "array_lt" => Some(SortComparatorType::ArrayLt),
        _ => None,
    }
}

fn emit_sorted_rows(
    program: &mut ProgramBuilder,
    sorter: OpenedSorter<'_>,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
) -> QueryResult<()> {
    let data = program.alloc_register();
    let pseudo = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter.order_by.len() + sorter.width,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo,
        content_reg: data,
        num_fields: sorter.order_by.len() + sorter.width,
    });
    let loop_start = program.allocate_label();
    let cleanup = row_cleanup_label(destination, limit).unwrap_or_else(|| program.allocate_label());
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter.cursor_id,
        pc_if_empty: cleanup,
    });
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter.cursor_id,
        dest_reg: data,
        pseudo_cursor: pseudo,
    });
    let result_start = program.alloc_registers(sorter.width);
    for position in 0..sorter.width {
        program.emit_insn(Insn::Column {
            cursor_id: pseudo,
            column: sorter.order_by.len() + position,
            dest: result_start + position,
            default: None,
        });
    }
    emit_row_destination_without_context(
        program,
        RegisterRange::new(result_start, sorter.width),
        destination,
        limit,
    )?;
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter.cursor_id,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(cleanup);
    program.emit_insn(Insn::Close { cursor_id: pseudo });
    program.emit_insn(Insn::Close {
        cursor_id: sorter.cursor_id,
    });
    Ok(())
}

fn emit_row_destination_without_context(
    program: &mut ProgramBuilder,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
) -> QueryResult<()> {
    let skip = limit.and_then(|limit| {
        limit.offset.map(|offset| {
            let skip = program.allocate_label();
            program.emit_insn(Insn::IfPos {
                reg: offset.0,
                target_pc: skip,
                decrement_by: 1,
            });
            skip
        })
    });
    emit_result_destination(program, result, destination)?;
    if let Some(limit) = limit {
        emit_limit_decrement(program, limit);
    }
    if let Some(skip) = skip {
        program.preassign_label_to_next_insn(skip);
    }
    Ok(())
}

fn emit_limit_decrement(program: &mut ProgramBuilder, limit: LimitRuntime) {
    let Some(stopped) = limit.stopped else {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit.limit.0,
            target_pc: limit.done,
        });
        return;
    };

    let exhausted = program.allocate_label();
    let resume = program.allocate_label();
    program.emit_insn(Insn::DecrJumpZero {
        reg: limit.limit.0,
        target_pc: exhausted,
    });
    program.emit_insn(Insn::Goto { target_pc: resume });
    program.preassign_label_to_next_insn(exhausted);
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: stopped.0,
    });
    program.emit_insn(Insn::Goto {
        target_pc: limit.done,
    });
    program.preassign_label_to_next_insn(resume);
}

fn emit_result_destination(
    program: &mut ProgramBuilder,
    result: RegisterRange,
    destination: QueryDestination<'_>,
) -> QueryResult<()> {
    match destination {
        QueryDestination::ResultRows => {
            program.emit_insn(Insn::ResultRow {
                start_reg: result.first.0,
                count: result.width,
            });
        }
        QueryDestination::EphemeralTable { cursor_id, table } => {
            let record = program.alloc_register();
            let rowid = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(result.first.0),
                count: to_u32(result.width),
                dest_reg: to_u32(record),
                index_name: Some(table.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::NewRowid {
                cursor: cursor_id,
                rowid_reg: rowid,
                prev_largest_reg: 0,
            });
            program.emit_insn(Insn::Insert {
                cursor: cursor_id,
                key_reg: rowid,
                record_reg: record,
                flag: InsertFlags::new().is_ephemeral_table_insert(),
                table_name: table.name.clone(),
            });
        }
        QueryDestination::CompoundIndex {
            cursor_id,
            index,
            delete,
        } => {
            if delete {
                program.emit_insn(Insn::IdxDelete {
                    start_reg: result.first.0,
                    num_regs: result.width,
                    cursor_id,
                    raise_error_if_no_matching_entry: false,
                });
            } else {
                let record = program.alloc_register();
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(result.first.0),
                    count: to_u32(result.width),
                    dest_reg: to_u32(record),
                    index_name: Some(index.name.clone()),
                    affinity_str: None,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id,
                    record_reg: record,
                    unpacked_start: None,
                    unpacked_count: None,
                    flags: IdxInsertFlags::new().no_op_duplicate(),
                });
            }
        }
        QueryDestination::Scalar { registers, done } => {
            if registers.width != result.width {
                return Err(PhysicalQueryError::Invalid(
                    "scalar destination width does not match query output",
                ));
            }
            if registers.first != result.first {
                program.emit_insn(Insn::Copy {
                    src_reg: result.first.0,
                    dst_reg: registers.first.0,
                    extra_amount: result.width - 1,
                });
            }
            program.emit_insn(Insn::Goto { target_pc: done });
        }
        QueryDestination::Exists { register, done } => {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: register.0,
            });
            program.emit_insn(Insn::Goto { target_pc: done });
        }
        QueryDestination::Sorter { .. } => {
            return Err(PhysicalQueryError::Invalid(
                "sorter destination was not handled with row context",
            ));
        }
    }
    Ok(())
}

fn emit_expressions<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    expressions: &[crate::translate::semantic::hir::Expr],
    result: RegisterRange,
) -> QueryResult<()> {
    for (position, expression) in expressions.iter().enumerate() {
        let target = result
            .register(position)
            .ok_or(PhysicalQueryError::Invalid("VALUES register is missing"))?;
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(expression, RegisterRange::new(target.0, 1))?;
    }
    Ok(())
}
