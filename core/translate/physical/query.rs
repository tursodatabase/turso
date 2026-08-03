//! Scan-first query emission from a closed physical HIR plan.
//!
//! This is deliberately a small executable boundary, not a compatibility
//! bridge to the parser-AST planner. A supported query is emitted directly;
//! every other shape returns an explicit error.

use std::fmt;

use rustc_hash::{FxHashMap, FxHashSet};
use turso_ext::{ConstraintInfo, ConstraintOp};
use turso_parser::ast::{CompoundOperator, Distinctness, Literal, NullsOrder, SortOrder};

use crate::{
    alloc::{TursoFromIterator, TursoIteratorExt, TursoVecExt},
    emit_explain,
    function::{AccumulatorFunc, AggFunc, Func, WindowFunc},
    schema::{
        BTreeCharacteristics, BTreeTable, Column, Index, IndexColumn, PseudoCursorType, Table,
    },
    sync::Arc,
    translate::collate::CollationSeq,
    translate::semantic::hir::{
        AggregateId, Assignment, CteBody, CteId, Expr, From as HirFrom, FunctionCall, Grouping,
        Join, JoinConstraint, JoinKind, OrderTerm, QueryBlockBody, QueryBlockId, QueryId,
        RecursiveCte, RecursiveOrderTerm, ResolvedTable, SourceId, SubqueryExpr, TypeFact,
    },
    types::KeyInfo,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, HashDistinctData, IdxInsertFlags, InsertFlags, Insn, SortComparatorType},
    },
};

use super::{
    plan::query_tree_has_outer_dependency, AggregateRuntime, ExpressionEmitter, ExpressionResult,
    OutputRuntime, PhysicalAggregate, PhysicalExpressionError, PhysicalPlan, PhysicalRoot,
    PhysicalSource, PhysicalSourceKind, PhysicalSubqueryEmitter, QueryRuntime, RegisterId,
    RegisterRange, RootRuntimeInputs, RuntimeBindingError, RuntimeBindings, SourceRuntime,
    TableAccess,
};

#[derive(Debug)]
pub(crate) enum PhysicalQueryError {
    Allocation(crate::alloc::TryReserveError),
    Engine(crate::LimboError),
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allocation(error) => error.fmt(formatter),
            Self::Engine(error) => error.fmt(formatter),
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

impl From<crate::alloc::TryReserveError> for PhysicalQueryError {
    fn from(error: crate::alloc::TryReserveError) -> Self {
        Self::Allocation(error)
    }
}

impl From<crate::LimboError> for PhysicalQueryError {
    fn from(error: crate::LimboError) -> Self {
        Self::Engine(error)
    }
}

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
    Single(usize),
}

struct VirtualFilter<'hir> {
    arguments: Vec<&'hir Expr>,
    idx_str: Option<usize>,
    idx_num: usize,
}

struct IndexMethodFilter<'hir> {
    database: usize,
    pattern: usize,
    arguments: Vec<&'hir Expr>,
}

struct OpenedScan<'hir> {
    cursor: ScanCursor,
    runtime_cursor: usize,
    deferred_table: Option<usize>,
    virtual_filter: Option<VirtualFilter<'hir>>,
    index_method_filter: Option<IndexMethodFilter<'hir>>,
    index_method_outputs: Vec<(usize, usize)>,
    owned: bool,
}

/// Read-side cursor used to freeze a DML target before its write phase.
///
/// The table cursor remains the runtime binding for column reads. When HIR
/// carries `INDEXED BY`, `cursor` walks that exact index and defers each table
/// seek until the predicate or rowid needs it.
#[derive(Clone, Copy)]
pub(crate) struct DmlTargetScan {
    cursor: usize,
    table_cursor: usize,
    indexed: bool,
}

impl DmlTargetScan {
    fn opened(self) -> OpenedScan<'static> {
        OpenedScan {
            cursor: ScanCursor::BTree(self.cursor),
            runtime_cursor: self.table_cursor,
            deferred_table: self.indexed.then_some(self.table_cursor),
            virtual_filter: None,
            index_method_filter: None,
            index_method_outputs: Vec::new(),
            owned: false,
        }
    }

    pub(crate) fn rewind(self, program: &mut ProgramBuilder, empty: crate::vdbe::BranchOffset) {
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor,
            pc_if_empty: empty,
        });
    }

    pub(crate) fn prepare_row(self, program: &mut ProgramBuilder) {
        if self.indexed {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id: self.cursor,
                table_cursor_id: self.table_cursor,
            });
        }
    }

    pub(crate) fn rowid(self, program: &mut ProgramBuilder, dest: usize) {
        if self.indexed {
            program.emit_insn(Insn::IdxRowId {
                cursor_id: self.cursor,
                dest,
            });
        } else {
            program.emit_insn(Insn::RowId {
                cursor_id: self.cursor,
                dest,
            });
        }
    }

    pub(crate) fn next(self, program: &mut ProgramBuilder, target: crate::vdbe::BranchOffset) {
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor,
            pc_if_next: target,
        });
    }

    pub(crate) fn close(self, program: &mut ProgramBuilder) {
        if self.indexed {
            program.emit_insn(Insn::Close {
                cursor_id: self.cursor,
            });
        }
    }
}

pub(crate) fn open_dml_target_scan(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    target: SourceId,
    table_cursor: usize,
) -> QueryResult<DmlTargetScan> {
    let source = plan
        .source(target)
        .ok_or(PhysicalQueryError::Invalid("DML target source is missing"))?;
    let PhysicalSourceKind::CatalogTable { table, access } = &source.kind else {
        return Err(PhysicalQueryError::Invalid(
            "DML target is not a catalog table",
        ));
    };
    match access {
        TableAccess::Scan => Ok(DmlTargetScan {
            cursor: table_cursor,
            table_cursor,
            indexed: false,
        }),
        TableAccess::ForcedIndex(index) => {
            let database = table.database().ok_or(PhysicalQueryError::Invalid(
                "DML target has no database identity",
            ))?;
            if index.database() != Some(database)
                || index.value().index_method.is_some()
                || !index.value().has_rowid
            {
                return Err(PhysicalQueryError::Unsupported(
                    "custom or rowid-free forced DML index",
                ));
            }
            let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.handle()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: cursor,
                root_page: index.value().root_page,
                db: database.index(),
            });
            Ok(DmlTargetScan {
                cursor,
                table_cursor,
                indexed: true,
            })
        }
        TableAccess::IndexMethod(_) => Err(PhysicalQueryError::Unsupported(
            "custom index method for a DML target scan",
        )),
    }
}

#[derive(Clone, Copy)]
enum ScanRowAction<'hir, 'destination> {
    Project {
        outputs: &'hir [crate::translate::semantic::hir::Output],
        covered_outputs: &'destination [Option<crate::translate::semantic::hir::OutputId>],
        result: RegisterRange,
        destination: QueryDestination<'destination>,
        limit: Option<LimitRuntime>,
        distinct: Option<&'destination DistinctRuntime>,
    },
    Aggregate {
        aggregates: &'destination [PhysicalAggregate<'hir>],
        saved_sources: &'destination [SavedSourceLayout],
        first_row_seen: RegisterId,
    },
    GroupSortInsert {
        sorter: &'destination GroupSorter<'hir>,
    },
    UpdateCandidate {
        target: SourceId,
        assignments: &'hir [Assignment],
        order_by: &'hir [OrderTerm],
        cursor: usize,
        table: &'destination BTreeTable,
    },
    WindowMaterialize {
        rows: &'destination WindowRows,
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
            | Self::UpdateCandidate { .. }
            | Self::WindowMaterialize { .. } => None,
        }
    }
}

struct GroupSourceLayout {
    source: SourceId,
    width: usize,
    rowid_available: bool,
    record_offset: usize,
}

struct SavedSourceLayout {
    source: SourceId,
    columns: RegisterRange,
    rowid: Option<RegisterId>,
}

struct GroupSorter<'hir> {
    cursor_id: usize,
    record: RegisterId,
    field_count: usize,
    grouping: &'hir Grouping,
    sources: Vec<GroupSourceLayout>,
}

struct WindowSorter<'hir> {
    cursor_id: usize,
    record: RegisterId,
    field_count: usize,
    key_count: usize,
    spec: &'hir crate::translate::semantic::hir::WindowSpec,
}

struct WindowRows {
    cursor_id: usize,
    table: Arc<BTreeTable>,
    sources: Vec<WindowSourceLayout>,
    aggregates: Vec<WindowAggregateLayout>,
    width: usize,
}

struct WindowSourceLayout {
    source: SourceId,
    width: usize,
    offset: usize,
}

struct WindowAggregateLayout {
    aggregate: AggregateId,
    offset: usize,
}

struct BoundWindowRow {
    rowid: RegisterId,
    previous_sources: Vec<(SourceId, SourceRuntime)>,
    previous_aggregates: Vec<(AggregateId, AggregateRuntime)>,
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
    RecursiveQueue {
        cursor_id: usize,
        index: &'table Index,
        order: &'table [RecursiveOrderTerm],
        seen: Option<(usize, &'table Index)>,
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
        tie_breaker: Option<SortOrder>,
        grouping_ties: Option<(
            &'table Grouping,
            &'table [crate::translate::semantic::hir::Output],
        )>,
    },
}

impl QueryDestination<'_> {
    const fn early_exit_label(self) -> Option<crate::vdbe::BranchOffset> {
        match self {
            Self::Scalar { done, .. } | Self::Exists { done, .. } => Some(done),
            Self::ResultRows
            | Self::EphemeralTable { .. }
            | Self::CompoundIndex { .. }
            | Self::RecursiveQueue { .. }
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
    tie_breaker: Option<SortOrder>,
    grouping_ties: Option<(
        &'hir Grouping,
        &'hir [crate::translate::semantic::hir::Output],
    )>,
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
            tie_breaker: self.tie_breaker,
            grouping_ties: self.grouping_ties,
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
    recursive_inputs: FxHashMap<CteId, (usize, usize)>,
    temporary_cursors: Vec<usize>,
}

pub(crate) struct MaterializedQuery {
    pub(crate) cursor: usize,
    cleanup_cursors: Vec<usize>,
}

pub(crate) struct MaterializedUpdateRows {
    pub(crate) cursor: usize,
    pub(crate) width: usize,
    pub(crate) rowid_column: Option<usize>,
    pub(crate) assignment_offset: usize,
}

pub(crate) struct MaterializedDmlRowids {
    pub(crate) cursor: usize,
}

impl MaterializedDmlRowids {
    pub(crate) fn close(self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Close {
            cursor_id: self.cursor,
        });
    }
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
        let has_outer_dependency = query_tree_has_outer_dependency(self.plan.document, query_id);
        match subquery {
            SubqueryExpr::Scalar { .. } => {
                let once_done = (!has_outer_dependency).then(|| {
                    let done = program.allocate_label();
                    program.emit_insn(Insn::Once {
                        target_pc_when_reentered: done,
                    });
                    done
                });
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
                if !query.hir.compounds.is_empty() {
                    let table =
                        ephemeral_table(format!("scalar_compound_{}", query_id.index()), width)
                            .map_err(|error| {
                                PhysicalExpressionError::Subquery(error.to_string())
                            })?;
                    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
                    program.emit_insn(Insn::OpenEphemeral {
                        cursor_id: cursor,
                        is_table: true,
                    });
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
                    let done = program.allocate_label();
                    program.emit_insn(Insn::Rewind {
                        cursor_id: cursor,
                        pc_if_empty: done,
                    });
                    for position in 0..width {
                        program.emit_insn(Insn::Column {
                            cursor_id: cursor,
                            column: position,
                            dest: first + position,
                            default: None,
                        });
                    }
                    program.preassign_label_to_next_insn(done);
                    program.emit_insn(Insn::Close { cursor_id: cursor });
                    if let Some(once_done) = once_done {
                        program.preassign_label_to_next_insn(once_done);
                    }
                    return Ok(runtime);
                }
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
                // Both an empty scan and the first produced row leave the
                // scalar subquery here. Anchor the shared destination after
                // every child-query shape, including ungrouped aggregates.
                program.preassign_label_to_next_insn_if_unassigned(done);
                if let Some(once_done) = once_done {
                    program.preassign_label_to_next_insn(once_done);
                }
                Ok(runtime)
            }
            SubqueryExpr::Exists(_) => {
                let once_done = (!has_outer_dependency).then(|| {
                    let done = program.allocate_label();
                    program.emit_insn(Insn::Once {
                        target_pc_when_reentered: done,
                    });
                    done
                });
                let register = RegisterId(program.alloc_register());
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: register.0,
                });
                let runtime = QueryRuntime::Exists(register);
                bindings.bind_query(query_id, runtime)?;
                if !query.hir.compounds.is_empty() {
                    let table = ephemeral_table(
                        format!("exists_compound_{}", query_id.index()),
                        query.hir.output.len(),
                    )
                    .map_err(|error| PhysicalExpressionError::Subquery(error.to_string()))?;
                    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
                    program.emit_insn(Insn::OpenEphemeral {
                        cursor_id: cursor,
                        is_table: true,
                    });
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
                    let done = program.allocate_label();
                    program.emit_insn(Insn::Rewind {
                        cursor_id: cursor,
                        pc_if_empty: done,
                    });
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: register.0,
                    });
                    program.preassign_label_to_next_insn(done);
                    program.emit_insn(Insn::Close { cursor_id: cursor });
                    if let Some(once_done) = once_done {
                        program.preassign_label_to_next_insn(once_done);
                    }
                    return Ok(runtime);
                }
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
                program.preassign_label_to_next_insn_if_unassigned(done);
                if let Some(once_done) = once_done {
                    program.preassign_label_to_next_insn(once_done);
                }
                Ok(runtime)
            }
            SubqueryExpr::In { comparison, .. } => {
                let width = comparison.components.len();
                if width == 0 || query.hir.output.len() != width {
                    return Err(PhysicalExpressionError::Subquery(
                        "IN query width does not match its comparison facts".to_string(),
                    ));
                }
                let table = ephemeral_table(format!("in_subquery_{}", query_id.index()), width)
                    .map_err(|error| PhysicalExpressionError::Subquery(error.to_string()))?;
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
            Self::BTree(cursor) | Self::Virtual(cursor) | Self::Single(cursor) => cursor,
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
    emit_root_query_to_destination(plan, program, inputs, QueryDestination::ResultRows)
}

pub(crate) fn emit_root_query_into_ephemeral(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    cursor_id: usize,
    table: &BTreeTable,
) -> QueryResult<()> {
    emit_root_query_to_destination(
        plan,
        program,
        &RootRuntimeInputs::default(),
        QueryDestination::EphemeralTable { cursor_id, table },
    )
}

fn emit_root_query_to_destination(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    destination: QueryDestination<'_>,
) -> QueryResult<()> {
    let query_id = match &plan.root {
        PhysicalRoot::Query(query) => *query,
        PhysicalRoot::Insert(_)
        | PhysicalRoot::Update(_)
        | PhysicalRoot::Delete(_)
        | PhysicalRoot::TriggerPredicate(_)
        | PhysicalRoot::SchemaExpressions(_) => {
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
    materialize_ctes_owned_by_current_query(plan, program, &mut bindings, &mut ctes, query_id)?;
    let result = emit_query(
        plan,
        program,
        &mut bindings,
        &mut ctes,
        query_id,
        destination,
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
    materialize_ctes_owned_by_current_query(plan, program, bindings, &mut ctes, query_id)?;
    let table = ephemeral_table(
        format!("dml_query_{}", query_id.index()),
        query.hir.output.len(),
    )?;
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

/// Emit one DML expression with the same subquery and CTE machinery used by
/// query blocks. The caller only supplies the already-bound row sources; all
/// QueryId destinations stay private to the physical query layer.
pub(crate) fn emit_expression_for_dml<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    expression: &Expr,
) -> QueryResult<RegisterRange> {
    let mut query_ids = Vec::new();
    expression.walk(&mut |expression| {
        let Expr::Subquery(subquery) = expression else {
            return;
        };
        let query_id = match subquery {
            SubqueryExpr::Scalar { query, .. }
            | SubqueryExpr::Exists(query)
            | SubqueryExpr::In { query, .. } => *query,
        };
        if !query_ids.contains(&query_id) {
            query_ids.push(query_id);
        }
    });

    let mut ctes = MaterializedCtes::default();
    for query_id in query_ids {
        materialize_ctes_owned_by_current_query(plan, program, bindings, &mut ctes, query_id)?;
    }
    let mut subqueries = QuerySubqueryEmitter {
        plan,
        ctes: &mut ctes,
    };
    let result =
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_new(expression);
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
    result.map_err(Into::into)
}

/// Freeze the target rowids selected by a DML ORDER BY/LIMIT before any row is
/// changed. ORDER BY terms are evaluated against the live target SourceId and
/// LIMIT/OFFSET are applied while draining the sorter into a private table.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_ordered_dml_rowids<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    target_scan: DmlTargetScan,
    predicate: Option<&Expr>,
    order_by: &'document [OrderTerm],
    limit: Option<&'document crate::translate::semantic::hir::Limit>,
) -> QueryResult<MaterializedDmlRowids> {
    let table = ephemeral_table("ordered_dml_rowids".to_string(), 1)?;
    let output_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: output_cursor,
        is_table: true,
    });
    let destination = QueryDestination::EphemeralTable {
        cursor_id: output_cursor,
        table: &table,
    };
    let mut ctes = MaterializedCtes::default();
    let runtime_limit = open_limit(plan, program, bindings, &mut ctes, limit, destination)?;

    let sorter = if order_by.is_empty() {
        None
    } else {
        let cursor_id = program.alloc_cursor_id(CursorType::Sorter);
        program.emit_insn(Insn::SorterOpen {
            cursor_id,
            columns: order_by.len(),
            order_collations_nulls: order_by
                .iter()
                .map(|term| {
                    (
                        term.order,
                        term.collation.as_ref().map(|collation| *collation.value()),
                        term.nulls,
                    )
                })
                .try_collect()?,
            comparators: order_by
                .iter()
                .map(|term| sort_comparator(&term.type_fact))
                .try_collect()?,
        });
        Some(OpenedSorter {
            cursor_id,
            record: RegisterId(program.alloc_register()),
            order_by,
            first_block: QueryBlockId::new(QueryId::new(0), 0),
            width: 1,
            tie_breaker: None,
            grouping_ties: None,
        })
    };

    let scan_start = program.allocate_label();
    let scan_next = program.allocate_label();
    let scan_done = program.allocate_label();
    target_scan.rewind(program, scan_done);
    program.preassign_label_to_next_insn(scan_start);
    target_scan.prepare_row(program);
    if let Some(predicate) = predicate {
        let condition = emit_expression_for_dml(plan, program, bindings, predicate)?;
        if condition.width != 1 {
            return Err(PhysicalQueryError::Invalid("DML predicate is not scalar"));
        }
        program.emit_insn(Insn::IfNot {
            reg: condition.first.0,
            target_pc: scan_next,
            jump_if_null: true,
        });
    }
    let rowid = RegisterId(program.alloc_register());
    target_scan.rowid(program, rowid.0);
    if let Some(sorter) = sorter {
        let fields = program.alloc_registers(order_by.len() + 1);
        for (position, term) in order_by.iter().enumerate() {
            let value = emit_expression_for_dml(plan, program, bindings, &term.expr)?;
            if value.width != 1 {
                return Err(PhysicalQueryError::Invalid(
                    "DML ORDER BY term is not scalar",
                ));
            }
            program.emit_insn(Insn::Copy {
                src_reg: value.first.0,
                dst_reg: fields + position,
                extra_amount: 0,
            });
        }
        program.emit_insn(Insn::Copy {
            src_reg: rowid.0,
            dst_reg: fields + order_by.len(),
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(fields),
            count: to_u32(order_by.len() + 1),
            dest_reg: to_u32(sorter.record.0),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::SorterInsert {
            cursor_id: sorter.cursor_id,
            record_reg: sorter.record.0,
        });
    } else {
        emit_row_destination_without_context(
            program,
            RegisterRange::new(rowid.0, 1),
            destination,
            runtime_limit,
        )?;
    }
    program.preassign_label_to_next_insn(scan_next);
    target_scan.next(program, scan_start);
    program.preassign_label_to_next_insn(scan_done);

    if let Some(sorter) = sorter {
        emit_sorted_rows(program, sorter, destination, runtime_limit)?;
    } else if let Some(limit) = runtime_limit {
        program.preassign_label_to_next_insn(limit.done);
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
    Ok(MaterializedDmlRowids {
        cursor: output_cursor,
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
    target_scan: DmlTargetScan,
    from: &HirFrom,
    filter: Option<&Expr>,
    assignments: &'document [Assignment],
    order_by: &'document [OrderTerm],
    limit: Option<&'document crate::translate::semantic::hir::Limit>,
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
    let candidate_width = width + order_by.len();
    let table = ephemeral_table(format!("update_from_{}", target.index()), candidate_width)?;
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
    scans.push(target_scan.opened());
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
        None,
        false,
        filter,
        ScanRowAction::UpdateCandidate {
            target,
            assignments,
            order_by,
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
    let selected = if order_by.is_empty() && limit.is_none() {
        MaterializedUpdateRows {
            cursor,
            width,
            rowid_column: None,
            assignment_offset: 0,
        }
    } else {
        let selected_table = ephemeral_table(
            format!("selected_update_from_{}", target.index()),
            width + 1,
        )?;
        let selected_cursor =
            program.alloc_cursor_id(CursorType::BTreeTable(selected_table.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: selected_cursor,
            is_table: true,
        });
        let destination = QueryDestination::EphemeralTable {
            cursor_id: selected_cursor,
            table: &selected_table,
        };
        let runtime_limit = open_limit(plan, program, bindings, &mut ctes, limit, destination)?;
        let sorter = if order_by.is_empty() {
            None
        } else {
            let sorter_cursor = program.alloc_cursor_id(CursorType::Sorter);
            program.emit_insn(Insn::SorterOpen {
                cursor_id: sorter_cursor,
                columns: order_by.len(),
                order_collations_nulls: order_by
                    .iter()
                    .map(|term| {
                        (
                            term.order,
                            term.collation.as_ref().map(|collation| *collation.value()),
                            term.nulls,
                        )
                    })
                    .try_collect()?,
                comparators: order_by
                    .iter()
                    .map(|term| sort_comparator(&term.type_fact))
                    .try_collect()?,
            });
            Some(OpenedSorter {
                cursor_id: sorter_cursor,
                record: RegisterId(program.alloc_register()),
                order_by,
                first_block: QueryBlockId::new(QueryId::new(0), 0),
                width: width + 1,
                tie_breaker: None,
                grouping_ties: None,
            })
        };
        let scan_start = program.allocate_label();
        let scan_done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: scan_done,
        });
        program.preassign_label_to_next_insn(scan_start);
        let target_rowid = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: cursor,
            dest: target_rowid,
        });
        let row = program.alloc_registers(width + 1);
        program.emit_insn(Insn::Copy {
            src_reg: target_rowid,
            dst_reg: row,
            extra_amount: 0,
        });
        for position in 0..width {
            program.emit_insn(Insn::Column {
                cursor_id: cursor,
                column: position,
                dest: row + 1 + position,
                default: None,
            });
        }
        if let Some(sorter) = sorter {
            let fields = program.alloc_registers(order_by.len() + width + 1);
            for position in 0..order_by.len() {
                program.emit_insn(Insn::Column {
                    cursor_id: cursor,
                    column: width + position,
                    dest: fields + position,
                    default: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: row,
                dst_reg: fields + order_by.len(),
                extra_amount: width,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(fields),
                count: to_u32(order_by.len() + width + 1),
                dest_reg: to_u32(sorter.record.0),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::SorterInsert {
                cursor_id: sorter.cursor_id,
                record_reg: sorter.record.0,
            });
        } else {
            emit_row_destination_without_context(
                program,
                RegisterRange::new(row, width + 1),
                destination,
                runtime_limit,
            )?;
        }
        program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: scan_start,
        });
        program.preassign_label_to_next_insn(scan_done);
        if let Some(sorter) = sorter {
            emit_sorted_rows(program, sorter, destination, runtime_limit)?;
        } else if let Some(limit) = runtime_limit {
            program.preassign_label_to_next_insn(limit.done);
        }
        program.emit_insn(Insn::Close { cursor_id: cursor });
        MaterializedUpdateRows {
            cursor: selected_cursor,
            width,
            rowid_column: Some(0),
            assignment_offset: 1,
        }
    };
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
    Ok(selected)
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
    materialize_ctes_owned_by_current_query(plan, program, bindings, ctes, query_id)?;
    prepare_uncorrelated_subqueries(plan, program, bindings, ctes, query)?;
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

/// Emit query-independent subqueries before any row-production branch can use
/// their runtime result. FULL joins emit the same filter for matched and
/// unmatched rows, and either branch may run first. Keeping this setup at the
/// query boundary matches the old planner's `BeforeLoop` phase without
/// copying HIR expressions into each scan branch.
fn prepare_uncorrelated_subqueries<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    query: &super::PhysicalQuery<'document>,
) -> QueryResult<()> {
    let mut seen = FxHashSet::default();
    let mut subqueries = Vec::new();
    let mut collect = |expression: &'document Expr| {
        expression.walk(&mut |expression| {
            let Expr::Subquery(subquery) = expression else {
                return;
            };
            let query_id = match subquery {
                SubqueryExpr::Scalar { query, .. }
                | SubqueryExpr::Exists(query)
                | SubqueryExpr::In { query, .. } => *query,
            };
            if seen.insert(query_id) {
                subqueries.push(subquery);
            }
        });
    };

    for block in &query.blocks {
        if let Some(from) = &block.hir.from {
            for source_id in
                std::iter::once(from.first).chain(from.joins.iter().map(|join| join.right))
            {
                if let Some(PhysicalSource {
                    kind: PhysicalSourceKind::TableFunction { arguments, .. },
                    ..
                }) = plan.source(source_id)
                {
                    for argument in *arguments {
                        collect(argument);
                    }
                }
            }
            for join in &from.joins {
                match &join.constraint {
                    JoinConstraint::On(expression) => collect(expression),
                    JoinConstraint::Using(columns) | JoinConstraint::Natural(columns) => {
                        for column in columns {
                            collect(&column.left);
                        }
                    }
                    JoinConstraint::None => {}
                }
            }
        }
        for output in block.outputs {
            collect(&output.expr);
        }
        match &block.hir.body {
            QueryBlockBody::Select {
                filter,
                grouping,
                windows,
                ..
            } => {
                if let Some(filter) = filter {
                    collect(filter);
                }
                if let Some(grouping) = grouping {
                    for key in &grouping.keys {
                        collect(key);
                    }
                    if let Some(having) = &grouping.having {
                        collect(having);
                    }
                }
                for window in windows {
                    for expression in &window.spec.partition_by {
                        collect(expression);
                    }
                    for term in &window.spec.order_by {
                        collect(&term.expr);
                    }
                    if let Some(frame) = &window.spec.frame {
                        for bound in std::iter::once(&frame.start).chain(frame.end.iter()) {
                            match bound {
                                crate::translate::semantic::hir::WindowFrameBound::Following(
                                    expression,
                                )
                                | crate::translate::semantic::hir::WindowFrameBound::Preceding(
                                    expression,
                                ) => collect(expression),
                                crate::translate::semantic::hir::WindowFrameBound::CurrentRow
                                | crate::translate::semantic::hir::WindowFrameBound::UnboundedFollowing
                                | crate::translate::semantic::hir::WindowFrameBound::UnboundedPreceding => {}
                            }
                        }
                    }
                }
            }
            QueryBlockBody::Values { rows } => {
                for expression in rows.iter().flatten() {
                    collect(expression);
                }
            }
        }
    }
    for term in &query.hir.order_by {
        collect(&term.expr);
    }
    if let Some(limit) = &query.hir.limit {
        collect(&limit.limit);
        if let Some(offset) = &limit.offset {
            collect(offset);
        }
    }

    for subquery in subqueries {
        let query_id = match subquery {
            SubqueryExpr::Scalar { query, .. }
            | SubqueryExpr::Exists(query)
            | SubqueryExpr::In { query, .. } => *query,
        };
        let child = plan.query(query_id).ok_or(PhysicalQueryError::Invalid(
            "subquery is missing from the physical plan",
        ))?;
        if query_tree_has_outer_dependency(plan.document, child.id) {
            continue;
        }
        let mut emitter = QuerySubqueryEmitter { plan, ctes };
        emitter.emit_subquery(program, bindings, subquery)?;
    }
    Ok(())
}

fn emit_set_compound_query<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    query: &super::PhysicalQuery<'document>,
    destination: QueryDestination<'_>,
) -> QueryResult<()> {
    let last_set_arm = query
        .hir
        .compounds
        .iter()
        .rposition(|arm| arm.operator != CompoundOperator::UnionAll)
        .ok_or(PhysicalQueryError::Invalid(
            "set-compound emission has no set operator",
        ))?;
    let first = query.blocks.first().ok_or(PhysicalQueryError::Invalid(
        "set compound has no first block",
    ))?;

    bindings.enter_query(query.id)?;
    let emission = (|| -> QueryResult<()> {
        materialize_ctes_owned_by_current_query(plan, program, bindings, ctes, query.id)?;
        prepare_uncorrelated_subqueries(plan, program, bindings, ctes, query)?;
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
        let trailing_union_all = last_set_arm + 1 < query.hir.compounds.len();
        let mut row_limit = if sorter.is_some() { None } else { limit };
        if trailing_union_all {
            if let Some(limit) = row_limit.as_mut() {
                let stopped = RegisterId(program.alloc_register());
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: stopped.0,
                });
                limit.stopped = Some(stopped);
            }
        }

        let (mut set_cursor, mut set_index) = open_compound_index(program, first.outputs)?;
        emit_query_block(
            plan,
            program,
            bindings,
            ctes,
            first,
            QueryDestination::CompoundIndex {
                cursor_id: set_cursor,
                index: &set_index,
                delete: false,
            },
            None,
        )?;
        let mut final_intersection = None;

        for (arm_index, arm) in query
            .hir
            .compounds
            .iter()
            .enumerate()
            .take(last_set_arm + 1)
        {
            let right = &query.blocks[arm_index + 1];
            match arm.operator {
                CompoundOperator::Union | CompoundOperator::UnionAll => emit_query_block(
                    plan,
                    program,
                    bindings,
                    ctes,
                    right,
                    QueryDestination::CompoundIndex {
                        cursor_id: set_cursor,
                        index: &set_index,
                        delete: false,
                    },
                    None,
                )?,
                CompoundOperator::Except => emit_query_block(
                    plan,
                    program,
                    bindings,
                    ctes,
                    right,
                    QueryDestination::CompoundIndex {
                        cursor_id: set_cursor,
                        index: &set_index,
                        delete: true,
                    },
                    None,
                )?,
                CompoundOperator::Intersect => {
                    let (right_cursor, right_index) = open_compound_index(program, first.outputs)?;
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
                    if arm_index == last_set_arm {
                        final_intersection = Some(right_cursor);
                        continue;
                    }
                    let (next_cursor, next_index) = open_compound_index(program, first.outputs)?;
                    emit_compound_index_rows(
                        plan,
                        program,
                        bindings,
                        ctes,
                        set_cursor,
                        set_index.columns.len(),
                        Some(right_cursor),
                        QueryDestination::CompoundIndex {
                            cursor_id: next_cursor,
                            index: &next_index,
                            delete: false,
                        },
                        None,
                    )?;
                    program.emit_insn(Insn::Close {
                        cursor_id: set_cursor,
                    });
                    program.emit_insn(Insn::Close {
                        cursor_id: right_cursor,
                    });
                    set_cursor = next_cursor;
                    set_index = next_index;
                }
            }
        }

        let set_limit = if trailing_union_all {
            row_limit.map(|limit| LimitRuntime {
                done: program.allocate_label(),
                ..limit
            })
        } else {
            row_limit
        };
        emit_compound_index_rows(
            plan,
            program,
            bindings,
            ctes,
            set_cursor,
            set_index.columns.len(),
            final_intersection,
            row_destination,
            set_limit,
        )?;
        program.emit_insn(Insn::Close {
            cursor_id: set_cursor,
        });
        if let Some(cursor_id) = final_intersection {
            program.emit_insn(Insn::Close { cursor_id });
        }

        if trailing_union_all {
            if let Some(limit) = row_limit {
                let stopped = limit.stopped.ok_or(PhysicalQueryError::Invalid(
                    "mixed compound LIMIT has no stop register",
                ))?;
                program.emit_insn(Insn::If {
                    reg: stopped.0,
                    target_pc: limit.done,
                    jump_if_null: false,
                });
            }
            for block in query.blocks.iter().skip(last_set_arm + 2) {
                let block_limit = row_limit.map(|limit| LimitRuntime {
                    done: program.allocate_label(),
                    ..limit
                });
                emit_query_block(
                    plan,
                    program,
                    bindings,
                    ctes,
                    block,
                    row_destination,
                    block_limit,
                )?;
                if block.source_order.is_empty() {
                    if let Some(done) = row_cleanup_label(row_destination, block_limit) {
                        program.preassign_label_to_next_insn(done);
                    }
                }
                if let Some(limit) = row_limit {
                    let stopped = limit.stopped.ok_or(PhysicalQueryError::Invalid(
                        "mixed compound LIMIT has no stop register",
                    ))?;
                    program.emit_insn(Insn::If {
                        reg: stopped.0,
                        target_pc: limit.done,
                        jump_if_null: false,
                    });
                }
            }
            if let Some(limit) = row_limit {
                program.preassign_label_to_next_insn(limit.done);
            }
        }

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
        .try_collect()?;
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
                emit_ranking_window_query(
                    plan,
                    program,
                    bindings,
                    ctes,
                    block,
                    filter.as_ref(),
                    result,
                    destination,
                    limit,
                    distinct.as_ref(),
                )
            } else if let Some(grouping) = grouping
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
                    None,
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
                        block.filter.as_deref(),
                        block.hir.from.as_ref(),
                        ScanRowAction::Project {
                            outputs: block.outputs,
                            covered_outputs: &block.covered_outputs,
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
        None,
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
        .try_collect()?;
    let comparators = grouping
        .key_type_facts
        .iter()
        .map(sort_comparator)
        .try_collect()?;
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

fn open_window_sorter<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    rows: &WindowRows,
    spec: &'document crate::translate::semantic::hir::WindowSpec,
) -> QueryResult<WindowSorter<'document>> {
    let cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    // The sorter key is the window's partition and order, followed by rowid
    // so peers keep their input order. Source values follow as payload and
    // become the bound outer row while the sorted stream is consumed.
    let key_count = spec.partition_by.len() + spec.order_by.len() + 1;
    let mut order_collations_nulls = spec
        .partition_by
        .iter()
        .map(|expression| {
            (
                SortOrder::Asc,
                Some(expression_collation(plan, expression)),
                None,
            )
        })
        .try_collect::<crate::alloc::Vec<_>>()?;
    order_collations_nulls.try_extend(spec.order_by.iter().map(|term| {
        (
            term.order,
            term.collation.as_ref().map(|collation| *collation.value()),
            term.nulls,
        )
    }))?;
    order_collations_nulls.try_push((SortOrder::Asc, Some(CollationSeq::Binary), None))?;

    let mut comparators = spec
        .partition_by
        .iter()
        .map(|expression| {
            expression_type_fact(plan, expression)
                .as_ref()
                .and_then(sort_comparator)
        })
        .try_collect::<crate::alloc::Vec<_>>()?;
    comparators.try_extend(
        spec.order_by
            .iter()
            .map(|term| sort_comparator(&term.type_fact)),
    )?;
    comparators.try_push(None)?;
    program.emit_insn(Insn::SorterOpen {
        cursor_id,
        columns: key_count,
        order_collations_nulls,
        comparators,
    });
    Ok(WindowSorter {
        cursor_id,
        record: RegisterId(program.alloc_register()),
        field_count: key_count + rows.width + 1,
        key_count,
        spec,
    })
}

fn open_window_rows<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    source_ids: &[SourceId],
    aggregates: &[PhysicalAggregate<'document>],
) -> QueryResult<WindowRows> {
    let mut width = 0;
    let mut sources = Vec::with_capacity(source_ids.len());
    for source_id in source_ids {
        let source = plan
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("window source is missing"))?;
        sources.push(WindowSourceLayout {
            source: *source_id,
            width: source.width,
            offset: width,
        });
        width += source.width + 1;
    }
    let aggregates = aggregates
        .iter()
        .map(|aggregate| {
            let layout = WindowAggregateLayout {
                aggregate: aggregate.id,
                offset: width,
            };
            width += 1;
            layout
        })
        .collect();
    let table = ephemeral_table("window_rows".to_string(), width)?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });
    Ok(WindowRows {
        cursor_id,
        table,
        sources,
        aggregates,
        width,
    })
}

fn duplicate_window_rows(program: &mut ProgramBuilder, rows: &WindowRows) -> usize {
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(rows.table.clone()));
    program.emit_insn(Insn::OpenDup {
        new_cursor_id: cursor_id,
        original_cursor_id: rows.cursor_id,
    });
    cursor_id
}

fn bind_window_row<'document>(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    rows: &WindowRows,
    cursor_id: usize,
) -> QueryResult<BoundWindowRow> {
    let mut previous_sources = Vec::with_capacity(rows.sources.len());
    for source in &rows.sources {
        let columns = RegisterRange::new(program.alloc_registers(source.width), source.width);
        for position in 0..source.width {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: source.offset + position,
                dest: columns.first.0 + position,
                default: None,
            });
        }
        let source_rowid = RegisterId(program.alloc_register());
        program.emit_insn(Insn::Column {
            cursor_id,
            column: source.offset + source.width,
            dest: source_rowid.0,
            default: None,
        });
        previous_sources.push((
            source.source,
            bindings.replace_source(
                source.source,
                SourceRuntime::Registers {
                    columns,
                    rowid: Some(source_rowid),
                },
            )?,
        ));
    }
    let mut previous_aggregates = Vec::with_capacity(rows.aggregates.len());
    for aggregate in &rows.aggregates {
        let register = RegisterId(program.alloc_register());
        program.emit_insn(Insn::Column {
            cursor_id,
            column: aggregate.offset,
            dest: register.0,
            default: None,
        });
        previous_aggregates.push((
            aggregate.aggregate,
            bindings.replace_aggregate(
                aggregate.aggregate,
                AggregateRuntime {
                    register,
                    distinct_hash_table: None,
                    ordered_sorter: None,
                },
            )?,
        ));
    }
    let rowid = RegisterId(program.alloc_register());
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: rowid.0,
    });
    Ok(BoundWindowRow {
        rowid,
        previous_sources,
        previous_aggregates,
    })
}

fn restore_window_row<'document>(
    bindings: &mut RuntimeBindings<'document>,
    bound: BoundWindowRow,
) -> QueryResult<()> {
    for (aggregate, runtime) in bound.previous_aggregates.into_iter().rev() {
        bindings.replace_aggregate(aggregate, runtime)?;
    }
    for (source, runtime) in bound.previous_sources.into_iter().rev() {
        bindings.replace_source(source, runtime)?;
    }
    Ok(())
}

fn emit_window_row_insert<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    rows: &WindowRows,
) -> QueryResult<()> {
    let fields = program.alloc_registers(rows.width);
    for source in &rows.sources {
        for column in 0..source.width {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                &Expr::column(source.source, column),
                RegisterRange::new(fields + source.offset + column, 1),
            )?;
        }
        let rowid_destination = fields + source.offset + source.width;
        match bindings.source(source.source)? {
            SourceRuntime::Registers {
                rowid: Some(rowid), ..
            } => program.emit_insn(Insn::Copy {
                src_reg: rowid.0,
                dst_reg: rowid_destination,
                extra_amount: 0,
            }),
            SourceRuntime::Registers { rowid: None, .. } => program.emit_insn(Insn::Null {
                dest: rowid_destination,
                dest_end: None,
            }),
            SourceRuntime::Cursor { .. } => {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                    &Expr::rowid(source.source),
                    RegisterRange::new(rowid_destination, 1),
                )?;
            }
        }
    }
    for aggregate in &rows.aggregates {
        let runtime = bindings.aggregate(aggregate.aggregate)?;
        program.emit_insn(Insn::Copy {
            src_reg: runtime.register.0,
            dst_reg: fields + aggregate.offset,
            extra_amount: 0,
        });
    }
    let rowid = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: rows.cursor_id,
        rowid_reg: rowid,
        prev_largest_reg: 0,
    });
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(fields),
        count: to_u32(rows.width),
        dest_reg: to_u32(record),
        index_name: Some(rows.table.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: rows.cursor_id,
        key_reg: rowid,
        record_reg: record,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: rows.table.name.clone(),
    });
    Ok(())
}

fn emit_window_sort_insert<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    sorter: &WindowSorter<'document>,
    rows: &WindowRows,
    rowid: RegisterId,
) -> QueryResult<()> {
    let fields = program.alloc_registers(sorter.field_count);
    let mut position = 0;
    for expression in &sorter.spec.partition_by {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(expression, RegisterRange::new(fields + position, 1))?;
        position += 1;
    }
    for term in &sorter.spec.order_by {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(&term.expr, RegisterRange::new(fields + position, 1))?;
        position += 1;
    }
    program.emit_insn(Insn::Copy {
        src_reg: rowid.0,
        dst_reg: fields + position,
        extra_amount: 0,
    });
    position += 1;
    for source in &rows.sources {
        for column in 0..source.width {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                &Expr::column(source.source, column),
                RegisterRange::new(fields + position, 1),
            )?;
            position += 1;
        }
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
            &Expr::rowid(source.source),
            RegisterRange::new(fields + position, 1),
        )?;
        position += 1;
    }
    for aggregate in &rows.aggregates {
        let runtime = bindings.aggregate(aggregate.aggregate)?;
        program.emit_insn(Insn::Copy {
            src_reg: runtime.register.0,
            dst_reg: fields + position,
            extra_amount: 0,
        });
        position += 1;
    }
    program.emit_insn(Insn::Copy {
        src_reg: rowid.0,
        dst_reg: fields + position,
        extra_amount: 0,
    });
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

fn emit_window_sorter_rows<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    sorter: &WindowSorter<'document>,
    rows: &WindowRows,
) -> QueryResult<()> {
    let cursor = duplicate_window_rows(program, rows);
    let loop_start = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: done,
    });
    program.preassign_label_to_next_insn(loop_start);
    let bound = bind_window_row(program, bindings, rows, cursor)?;
    emit_window_sort_insert(plan, program, bindings, ctes, sorter, rows, bound.rowid)?;
    restore_window_row(bindings, bound)?;
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(done);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_sorted_window_rows<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    filter: Option<&'document Expr>,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
    sorter: &WindowSorter<'document>,
    rows: &WindowRows,
) -> QueryResult<()> {
    let content = program.alloc_register();
    let pseudo = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter.field_count,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo,
        content_reg: content,
        num_fields: sorter.field_count,
    });
    let outer_ordinal = RegisterId(program.alloc_register());
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: outer_ordinal.0,
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
        dest_reg: content,
        pseudo_cursor: pseudo,
    });
    program.emit_insn(Insn::AddImm {
        register: outer_ordinal.0,
        value: 1,
    });
    let mut previous_sources = Vec::with_capacity(rows.sources.len());
    for source in &rows.sources {
        let columns = RegisterRange::new(program.alloc_registers(source.width), source.width);
        for position in 0..source.width {
            program.emit_insn(Insn::Column {
                cursor_id: pseudo,
                column: sorter.key_count + source.offset + position,
                dest: columns.first.0 + position,
                default: None,
            });
        }
        let source_rowid = RegisterId(program.alloc_register());
        program.emit_insn(Insn::Column {
            cursor_id: pseudo,
            column: sorter.key_count + source.offset + source.width,
            dest: source_rowid.0,
            default: None,
        });
        previous_sources.push((
            source.source,
            bindings.replace_source(
                source.source,
                SourceRuntime::Registers {
                    columns,
                    rowid: Some(source_rowid),
                },
            )?,
        ));
    }
    let mut previous_aggregates = Vec::with_capacity(rows.aggregates.len());
    for aggregate in &rows.aggregates {
        let register = RegisterId(program.alloc_register());
        program.emit_insn(Insn::Column {
            cursor_id: pseudo,
            column: sorter.key_count + aggregate.offset,
            dest: register.0,
            default: None,
        });
        previous_aggregates.push((
            aggregate.aggregate,
            bindings.replace_aggregate(
                aggregate.aggregate,
                AggregateRuntime {
                    register,
                    distinct_hash_table: None,
                    ordered_sorter: None,
                },
            )?,
        ));
    }
    let rowid = RegisterId(program.alloc_register());
    program.emit_insn(Insn::Column {
        cursor_id: pseudo,
        column: sorter.key_count + rows.width,
        dest: rowid.0,
        default: None,
    });
    let bound = BoundWindowRow {
        rowid,
        previous_sources,
        previous_aggregates,
    };
    let emission = emit_ranking_window_row(
        plan,
        program,
        bindings,
        ctes,
        block,
        filter,
        result,
        destination,
        limit,
        distinct,
        rows,
        bound.rowid,
        outer_ordinal,
    );
    restore_window_row(bindings, bound)?;
    emission?;
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter.cursor_id,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(cleanup);
    program.emit_insn(Insn::Close { cursor_id: pseudo });
    program.emit_insn(Insn::Close {
        cursor_id: sorter.cursor_id,
    });
    program.emit_insn(Insn::Close {
        cursor_id: rows.cursor_id,
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_materialized_window_rows<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
    rows: &WindowRows,
) -> QueryResult<()> {
    let cursor = duplicate_window_rows(program, rows);
    let loop_start = program.allocate_label();
    let cleanup = row_cleanup_label(destination, limit).unwrap_or_else(|| program.allocate_label());
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: cleanup,
    });
    program.preassign_label_to_next_insn(loop_start);
    let bound = bind_window_row(program, bindings, rows, cursor)?;
    let emission = emit_ranking_window_row(
        plan,
        program,
        bindings,
        ctes,
        block,
        None,
        result,
        destination,
        limit,
        distinct,
        rows,
        bound.rowid,
        bound.rowid,
    );
    restore_window_row(bindings, bound)?;
    emission?;
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(cleanup);
    program.emit_insn(Insn::Close { cursor_id: cursor });
    program.emit_insn(Insn::Close {
        cursor_id: rows.cursor_id,
    });
    Ok(())
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

fn load_group_sources(
    program: &mut ProgramBuilder,
    bindings: &RuntimeBindings<'_>,
    sorter: &GroupSorter<'_>,
    pseudo: usize,
) -> QueryResult<()> {
    for source in &sorter.sources {
        let SourceRuntime::Registers { columns, rowid } = bindings.source(source.source)? else {
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
    window_rows: Option<&WindowRows>,
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
            let distinct_hash_table = aggregate
                .call
                .distinctness
                .is_some()
                .then(|| program.alloc_hash_table_id());
            if let Some(hash_table_id) = distinct_hash_table {
                program.emit_insn(Insn::HashClear { hash_table_id });
            }
            bindings.bind_aggregate(
                aggregate.id,
                AggregateRuntime {
                    register: RegisterId(accumulator_start + position),
                    distinct_hash_table,
                    ordered_sorter: (!aggregate.call.argument_order.is_empty()).then(|| {
                        super::OrderedAggregateRuntime {
                            cursor: program.alloc_cursor_id(CursorType::Sorter),
                            record: RegisterId(program.alloc_register()),
                        }
                    }),
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
    let mut representative_sources = Vec::with_capacity(sorter.sources.len());
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
        representative_sources.push(SavedSourceLayout {
            source: source.source,
            columns: RegisterRange::new(program.alloc_registers(source.width), source.width),
            rowid: source
                .rowid_available
                .then(|| RegisterId(program.alloc_register())),
        });
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
        let cleanup = if window_rows.is_some() {
            program.allocate_label()
        } else {
            row_cleanup_label(destination, limit).unwrap_or_else(|| program.allocate_label())
        };

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
            clear_aggregate_distinct_sets(program, bindings, &block.aggregates)?;
        }

        program.preassign_label_to_next_insn(start_group);
        open_ordered_aggregate_sorters(program, bindings, &block.aggregates)?;
        program.emit_insn(Insn::Copy {
            src_reg: current_keys,
            dst_reg: previous_keys,
            extra_amount: grouping.keys.len() - 1,
        });
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: has_group,
        });
        load_group_sources(program, bindings, &sorter, pseudo)?;
        for representative in &representative_sources {
            let SourceRuntime::Registers { columns, rowid } =
                bindings.source(representative.source)?
            else {
                return Err(PhysicalQueryError::Invalid(
                    "group source is not backed by registers",
                ));
            };
            program.emit_insn(Insn::Copy {
                src_reg: columns.first.0,
                dst_reg: representative.columns.first.0,
                extra_amount: columns.width.saturating_sub(1),
            });
            if let (Some(source), Some(destination)) = (rowid, representative.rowid) {
                program.emit_insn(Insn::Copy {
                    src_reg: source.0,
                    dst_reg: destination.0,
                    extra_amount: 0,
                });
            }
        }

        let step_aggregates = program.allocate_label();
        program.emit_insn(Insn::Goto {
            target_pc: step_aggregates,
        });

        program.preassign_label_to_next_insn(step_group);
        load_group_sources(program, bindings, &sorter, pseudo)?;

        program.preassign_label_to_next_insn(step_aggregates);
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
        drain_ordered_aggregate_sorters(plan, program, bindings, &block.aggregates)?;
        for aggregate in &block.aggregates {
            let function = runtime_aggregate_function(aggregate.call)?;
            let register = bindings.aggregate(aggregate.id)?.register;
            program.emit_insn(Insn::AggFinal {
                register: register.0,
                func: AccumulatorFunc::Agg(function),
            });
        }
        let mut current_sources = Vec::with_capacity(representative_sources.len());
        for representative in &representative_sources {
            let current = bindings.replace_source(
                representative.source,
                SourceRuntime::Registers {
                    columns: representative.columns,
                    rowid: representative.rowid,
                },
            )?;
            current_sources.push((representative.source, current));
        }
        if let Some(rows) = window_rows {
            if let Some(having) = &grouping.having {
                emit_filter(plan, program, bindings, ctes, having, skip_output)?;
            }
            emit_window_row_insert(plan, program, bindings, ctes, rows)?;
        } else {
            emit_output_expressions(plan, program, bindings, ctes, block.outputs, None, result)?;
            if let Some(having) = &grouping.having {
                emit_filter(plan, program, bindings, ctes, having, skip_output)?;
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
            )?;
        }
        for (source, runtime) in current_sources.into_iter().rev() {
            bindings.replace_source(source, runtime)?;
        }
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
        close_aggregate_distinct_sets(program, bindings, &block.aggregates)?;
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
        let distinct_hash_table = aggregate
            .call
            .distinctness
            .is_some()
            .then(|| program.alloc_hash_table_id());
        if let Some(hash_table_id) = distinct_hash_table {
            program.emit_insn(Insn::HashClear { hash_table_id });
        }
        bindings.bind_aggregate(
            aggregate.id,
            AggregateRuntime {
                register: RegisterId(accumulator_start + position),
                distinct_hash_table,
                ordered_sorter: (!aggregate.call.argument_order.is_empty()).then(|| {
                    super::OrderedAggregateRuntime {
                        cursor: program.alloc_cursor_id(CursorType::Sorter),
                        record: RegisterId(program.alloc_register()),
                    }
                }),
            },
        )?;
    }
    open_ordered_aggregate_sorters(program, bindings, &block.aggregates)?;

    let mut saved_sources = Vec::with_capacity(block.source_order.len());
    for source_id in &block.source_order {
        let source = plan
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid("aggregate source is missing"))?;
        let definition = plan
            .document
            .source(*source_id)
            .ok_or(PhysicalQueryError::Invalid(
                "aggregate HIR source is missing",
            ))?;
        let columns = RegisterRange::new(program.alloc_registers(source.width), source.width);
        let rowid = definition
            .rowid_available
            .then(|| RegisterId(program.alloc_register()));
        saved_sources.push(SavedSourceLayout {
            source: *source_id,
            columns,
            rowid,
        });
    }
    let first_row_seen = RegisterId(program.alloc_register());
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_row_seen.0,
    });

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
                saved_sources: &saved_sources,
                first_row_seen,
            },
        )?,
    }

    let mut original_sources = Vec::with_capacity(saved_sources.len());
    for source in &saved_sources {
        let original = bindings.replace_source(
            source.source,
            SourceRuntime::Registers {
                columns: source.columns,
                rowid: source.rowid,
            },
        )?;
        original_sources.push((source.source, original));
    }

    let emission = (|| -> QueryResult<()> {
        drain_ordered_aggregate_sorters(plan, program, bindings, &block.aggregates)?;
        for aggregate in &block.aggregates {
            let function = runtime_aggregate_function(aggregate.call)?;
            let register = bindings.aggregate(aggregate.id)?.register;
            program.emit_insn(Insn::AggFinal {
                register: register.0,
                func: AccumulatorFunc::Agg(function),
            });
        }

        let skip = program.allocate_label();
        emit_output_expressions(plan, program, bindings, ctes, block.outputs, None, result)?;
        if let Some(having) = having {
            emit_filter(plan, program, bindings, ctes, having, skip)?;
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
        )?;
        program.preassign_label_to_next_insn(skip);
        close_aggregate_distinct_sets(program, bindings, &block.aggregates)?;
        if let Some(done) = row_cleanup_label(destination, limit) {
            program.preassign_label_to_next_insn(done);
        }
        Ok(())
    })();
    for (source, runtime) in original_sources.into_iter().rev() {
        bindings.replace_source(source, runtime)?;
    }
    emission
}

#[allow(clippy::too_many_arguments)]
fn emit_ranking_window_query<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    filter: Option<&'document Expr>,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
) -> QueryResult<()> {
    if block.source_order.is_empty() {
        return Err(PhysicalQueryError::Unsupported(
            "ranking window without a source",
        ));
    }
    for function in &block.window_functions {
        match function.call.function.value() {
            Func::Agg(_) => {}
            Func::Window(
                WindowFunc::RowNumber
                | WindowFunc::Rank
                | WindowFunc::DenseRank
                | WindowFunc::PercentRank
                | WindowFunc::CumeDist
                | WindowFunc::Ntile
                | WindowFunc::Lag
                | WindowFunc::Lead
                | WindowFunc::FirstValue
                | WindowFunc::LastValue
                | WindowFunc::NthValue,
            ) => {}
            _ => {
                return Err(PhysicalQueryError::Unsupported(
                    "window function outside the HIR subset",
                ));
            }
        }
        bindings.bind_window_function(
            function.id,
            super::WindowFunctionRuntime {
                register: RegisterId(program.alloc_register()),
            },
        )?;
    }
    let rows = open_window_rows(plan, program, &block.source_order, &block.aggregates)?;
    let sorter = block
        .window_functions
        .iter()
        .filter_map(|function| function.call.window.as_ref())
        .find(|spec| !spec.partition_by.is_empty() || !spec.order_by.is_empty())
        .or_else(|| {
            block
                .window_functions
                .first()
                .and_then(|function| function.call.window.as_ref())
        })
        .map(|spec| open_window_sorter(plan, program, &rows, spec))
        .transpose()?;
    if block.aggregates.is_empty() {
        emit_table_scans(
            plan,
            program,
            bindings,
            ctes,
            &block.source_order,
            filter,
            block.hir.from.as_ref(),
            ScanRowAction::WindowMaterialize { rows: &rows },
        )?;
    } else {
        let QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } = &block.hir.body
        else {
            return Err(PhysicalQueryError::Unsupported(
                "ungrouped aggregate and window functions in one query block",
            ));
        };
        if grouping.keys.is_empty() {
            return Err(PhysicalQueryError::Unsupported(
                "ungrouped aggregate and window functions in one query block",
            ));
        }
        emit_grouped_aggregate(
            plan,
            program,
            bindings,
            ctes,
            block,
            filter,
            grouping,
            result,
            destination,
            None,
            None,
            Some(&rows),
        )?;
    }
    if let Some(sorter) = sorter.as_ref() {
        emit_window_sorter_rows(plan, program, bindings, ctes, sorter, &rows)?;
        return emit_sorted_window_rows(
            plan,
            program,
            bindings,
            ctes,
            block,
            None,
            result,
            destination,
            limit,
            distinct,
            sorter,
            &rows,
        );
    }
    emit_materialized_window_rows(
        plan,
        program,
        bindings,
        ctes,
        block,
        result,
        destination,
        limit,
        distinct,
        &rows,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_ranking_window_row<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    block: &super::PhysicalQueryBlock<'document>,
    filter: Option<&'document Expr>,
    result: RegisterRange,
    destination: QueryDestination<'_>,
    limit: Option<LimitRuntime>,
    distinct: Option<&DistinctRuntime>,
    rows: &WindowRows,
    outer_rowid: RegisterId,
    outer_ordinal: RegisterId,
) -> QueryResult<()> {
    (|| -> QueryResult<()> {
        for function in &block.window_functions {
            if matches!(function.call.function.value(), Func::Agg(_)) {
                emit_default_aggregate_window(
                    plan, program, bindings, ctes, function, filter, rows,
                )?;
                continue;
            }
            let Func::Window(
                kind @ (WindowFunc::RowNumber
                | WindowFunc::Rank
                | WindowFunc::DenseRank
                | WindowFunc::PercentRank
                | WindowFunc::CumeDist
                | WindowFunc::Ntile
                | WindowFunc::Lag
                | WindowFunc::Lead
                | WindowFunc::FirstValue
                | WindowFunc::LastValue
                | WindowFunc::NthValue),
            ) = function.call.function.value()
            else {
                return Err(PhysicalQueryError::Unsupported(
                    "window function outside the ranking subset",
                ));
            };
            let spec = function
                .call
                .window
                .as_ref()
                .ok_or(PhysicalQueryError::Invalid(
                    "window call has no specification",
                ))?;
            if *kind == WindowFunc::RowNumber
                && spec.partition_by.is_empty()
                && spec.order_by.is_empty()
            {
                let value = bindings.window_function(function.id)?.register;
                program.emit_insn(Insn::Copy {
                    src_reg: outer_ordinal.0,
                    dst_reg: value.0,
                    extra_amount: 0,
                });
                continue;
            }
            if matches!(
                kind,
                WindowFunc::Lag
                    | WindowFunc::Lead
                    | WindowFunc::FirstValue
                    | WindowFunc::LastValue
                    | WindowFunc::NthValue
            ) {
                emit_positional_window(
                    plan,
                    program,
                    bindings,
                    ctes,
                    function,
                    kind,
                    spec,
                    filter,
                    outer_rowid,
                    rows,
                )?;
                continue;
            }
            let outer_partition = program.alloc_registers(spec.partition_by.len());
            for (position, expression) in spec.partition_by.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                    expression,
                    RegisterRange::new(outer_partition + position, 1),
                )?;
            }
            let outer_order = program.alloc_registers(spec.order_by.len());
            for (position, term) in spec.order_by.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_into(&term.expr, RegisterRange::new(outer_order + position, 1))?;
            }
            let ntile_argument = if matches!(kind, WindowFunc::Ntile) {
                Some(emit_ntile_bucket_count(
                    plan,
                    program,
                    bindings,
                    ctes,
                    function,
                    spec,
                    filter,
                    outer_partition,
                    rows,
                )?)
            } else {
                None
            };
            let inner_cursor = duplicate_window_rows(program, rows);
            let value = bindings.window_function(function.id)?.register;
            program.emit_insn(Insn::Integer {
                value: i64::from(matches!(kind, WindowFunc::Rank | WindowFunc::DenseRank)),
                dest: value.0,
            });
            let one = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: one,
            });
            let partition_size = matches!(
                kind,
                WindowFunc::PercentRank | WindowFunc::CumeDist | WindowFunc::Ntile
            )
            .then(|| {
                let register = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: register,
                });
                register
            });
            let distinct_orders =
                matches!(kind, WindowFunc::DenseRank).then(|| program.alloc_hash_table_id());
            if let Some(hash_table_id) = distinct_orders {
                program.emit_insn(Insn::HashClear { hash_table_id });
            }
            let loop_start = program.allocate_label();
            let loop_next = program.allocate_label();
            let less = program.allocate_label();
            let equal = program.allocate_label();
            let increment = program.allocate_label();
            let done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: inner_cursor,
                pc_if_empty: done,
            });
            program.preassign_label_to_next_insn(loop_start);
            let bound = bind_window_row(program, bindings, rows, inner_cursor)?;
            if let Some(filter) = filter {
                emit_filter(plan, program, bindings, ctes, filter, loop_next)?;
            }
            if !spec.partition_by.is_empty() {
                let inner_partition = program.alloc_registers(spec.partition_by.len());
                for (position, expression) in spec.partition_by.iter().enumerate() {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(
                            expression,
                            RegisterRange::new(inner_partition + position, 1),
                        )?;
                }
                program.emit_insn(Insn::Compare {
                    start_reg_a: outer_partition,
                    start_reg_b: inner_partition,
                    count: spec.partition_by.len(),
                    key_info: spec
                        .partition_by
                        .iter()
                        .map(|expression| KeyInfo {
                            sort_order: SortOrder::Asc,
                            collation: expression_collation(plan, expression),
                            nulls_order: None,
                        })
                        .collect(),
                });
                let same_partition = program.allocate_label();
                program.emit_insn(Insn::Jump {
                    target_pc_lt: loop_next,
                    target_pc_eq: same_partition,
                    target_pc_gt: loop_next,
                });
                program.preassign_label_to_next_insn(same_partition);
            }
            if let Some(partition_size) = partition_size {
                program.emit_insn(Insn::Add {
                    lhs: partition_size,
                    rhs: one,
                    dest: partition_size,
                });
            }
            if spec.order_by.is_empty() {
                if matches!(kind, WindowFunc::RowNumber | WindowFunc::Ntile) {
                    let inner_rowid = program.alloc_register();
                    program.emit_insn(Insn::RowId {
                        cursor_id: inner_cursor,
                        dest: inner_rowid,
                    });
                    program.emit_insn(Insn::Compare {
                        start_reg_a: outer_rowid.0,
                        start_reg_b: inner_rowid,
                        count: 1,
                        key_info: vec![KeyInfo {
                            sort_order: SortOrder::Asc,
                            collation: CollationSeq::Binary,
                            nulls_order: None,
                        }],
                    });
                    program.emit_insn(Insn::Jump {
                        target_pc_lt: loop_next,
                        target_pc_eq: increment,
                        target_pc_gt: increment,
                    });
                } else if matches!(kind, WindowFunc::CumeDist) {
                    program.emit_insn(Insn::Goto {
                        target_pc: increment,
                    });
                } else {
                    program.emit_insn(Insn::Goto {
                        target_pc: loop_next,
                    });
                }
            } else {
                let inner_order = program.alloc_registers(spec.order_by.len());
                for (position, term) in spec.order_by.iter().enumerate() {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(&term.expr, RegisterRange::new(inner_order + position, 1))?;
                }
                program.emit_insn(Insn::Compare {
                    start_reg_a: outer_order,
                    start_reg_b: inner_order,
                    count: spec.order_by.len(),
                    key_info: spec
                        .order_by
                        .iter()
                        .map(|term| KeyInfo {
                            sort_order: term.order,
                            collation: term
                                .collation
                                .as_ref()
                                .map_or(CollationSeq::Binary, |collation| *collation.value()),
                            nulls_order: term.nulls,
                        })
                        .collect(),
                });
                program.emit_insn(Insn::Jump {
                    target_pc_lt: loop_next,
                    target_pc_eq: equal,
                    target_pc_gt: less,
                });
                program.preassign_label_to_next_insn(less);
                if let Some(hash_table_id) = distinct_orders {
                    program.emit_insn(Insn::HashDistinct {
                        data: Box::new(HashDistinctData {
                            hash_table_id,
                            key_start_reg: inner_order,
                            num_keys: spec.order_by.len(),
                            collations: spec
                                .order_by
                                .iter()
                                .map(|term| {
                                    term.collation
                                        .as_ref()
                                        .map_or(CollationSeq::Binary, |collation| {
                                            *collation.value()
                                        })
                                })
                                .collect(),
                            target_pc: loop_next,
                        }),
                    });
                }
                program.emit_insn(Insn::Goto {
                    target_pc: increment,
                });
                program.preassign_label_to_next_insn(equal);
                if matches!(kind, WindowFunc::RowNumber | WindowFunc::Ntile) {
                    let inner_rowid = program.alloc_register();
                    program.emit_insn(Insn::RowId {
                        cursor_id: inner_cursor,
                        dest: inner_rowid,
                    });
                    program.emit_insn(Insn::Compare {
                        start_reg_a: outer_rowid.0,
                        start_reg_b: inner_rowid,
                        count: 1,
                        key_info: vec![KeyInfo {
                            sort_order: SortOrder::Asc,
                            collation: CollationSeq::Binary,
                            nulls_order: None,
                        }],
                    });
                    program.emit_insn(Insn::Jump {
                        target_pc_lt: loop_next,
                        target_pc_eq: increment,
                        target_pc_gt: increment,
                    });
                } else if matches!(kind, WindowFunc::CumeDist) {
                    program.emit_insn(Insn::Goto {
                        target_pc: increment,
                    });
                } else {
                    program.emit_insn(Insn::Goto {
                        target_pc: loop_next,
                    });
                }
            }
            program.preassign_label_to_next_insn(increment);
            program.emit_insn(Insn::Add {
                lhs: value.0,
                rhs: one,
                dest: value.0,
            });
            program.preassign_label_to_next_insn(loop_next);
            program.emit_insn(Insn::Next {
                cursor_id: inner_cursor,
                pc_if_next: loop_start,
            });
            program.preassign_label_to_next_insn(done);
            match kind {
                WindowFunc::PercentRank => {
                    let partition_size = partition_size.expect("percent_rank counts its partition");
                    let denominator = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: partition_size,
                        dst_reg: denominator,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::AddImm {
                        register: denominator,
                        value: -1,
                    });
                    let divide = program.allocate_label();
                    let calculated = program.allocate_label();
                    program.emit_insn(Insn::RealAffinity { register: value.0 });
                    program.emit_insn(Insn::If {
                        reg: denominator,
                        target_pc: divide,
                        jump_if_null: false,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: calculated,
                    });
                    program.preassign_label_to_next_insn(divide);
                    program.emit_insn(Insn::Divide {
                        lhs: value.0,
                        rhs: denominator,
                        dest: value.0,
                    });
                    program.preassign_label_to_next_insn(calculated);
                }
                WindowFunc::CumeDist => {
                    let partition_size = partition_size.expect("cume_dist counts its partition");
                    program.emit_insn(Insn::RealAffinity { register: value.0 });
                    program.emit_insn(Insn::Divide {
                        lhs: value.0,
                        rhs: partition_size,
                        dest: value.0,
                    });
                }
                WindowFunc::Ntile => {
                    let partition_size = partition_size.expect("ntile counts its partition");
                    let bucket_count = ntile_argument.expect("ntile validates its argument");
                    let row_index = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: value.0,
                        dst_reg: row_index,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::AddImm {
                        register: row_index,
                        value: -1,
                    });
                    let bucket_size = program.alloc_register();
                    program.emit_insn(Insn::Divide {
                        lhs: partition_size,
                        rhs: bucket_count,
                        dest: bucket_size,
                    });
                    let regular_buckets = program.allocate_label();
                    let small_bucket = program.allocate_label();
                    let bucket_done = program.allocate_label();
                    program.emit_insn(Insn::If {
                        reg: bucket_size,
                        target_pc: regular_buckets,
                        jump_if_null: false,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: bucket_done,
                    });
                    program.preassign_label_to_next_insn(regular_buckets);
                    let large_bucket_count = program.alloc_register();
                    let bucketed_rows = program.alloc_register();
                    program.emit_insn(Insn::Multiply {
                        lhs: bucket_count,
                        rhs: bucket_size,
                        dest: bucketed_rows,
                    });
                    program.emit_insn(Insn::Subtract {
                        lhs: partition_size,
                        rhs: bucketed_rows,
                        dest: large_bucket_count,
                    });
                    let large_bucket_size = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: bucket_size,
                        dst_reg: large_bucket_size,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::AddImm {
                        register: large_bucket_size,
                        value: 1,
                    });
                    let rows_in_large_buckets = program.alloc_register();
                    program.emit_insn(Insn::Multiply {
                        lhs: large_bucket_count,
                        rhs: large_bucket_size,
                        dest: rows_in_large_buckets,
                    });
                    program.emit_insn(Insn::Lt {
                        lhs: row_index,
                        rhs: rows_in_large_buckets,
                        target_pc: small_bucket,
                        flags: crate::vdbe::insn::CmpInsFlags::default(),
                        collation: None,
                    });
                    program.emit_insn(Insn::Subtract {
                        lhs: row_index,
                        rhs: rows_in_large_buckets,
                        dest: value.0,
                    });
                    program.emit_insn(Insn::Divide {
                        lhs: value.0,
                        rhs: bucket_size,
                        dest: value.0,
                    });
                    program.emit_insn(Insn::Add {
                        lhs: value.0,
                        rhs: large_bucket_count,
                        dest: value.0,
                    });
                    program.emit_insn(Insn::Add {
                        lhs: value.0,
                        rhs: one,
                        dest: value.0,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: bucket_done,
                    });
                    program.preassign_label_to_next_insn(small_bucket);
                    program.emit_insn(Insn::Divide {
                        lhs: row_index,
                        rhs: large_bucket_size,
                        dest: value.0,
                    });
                    program.emit_insn(Insn::Add {
                        lhs: value.0,
                        rhs: one,
                        dest: value.0,
                    });
                    program.preassign_label_to_next_insn(bucket_done);
                }
                _ => {}
            }
            if let Some(hash_table_id) = distinct_orders {
                program.emit_insn(Insn::HashClose { hash_table_id });
            }
            program.emit_insn(Insn::Close {
                cursor_id: inner_cursor,
            });
            restore_window_row(bindings, bound)?;
        }
        emit_output_row(
            plan,
            program,
            bindings,
            ctes,
            block.outputs,
            None,
            result,
            destination,
            limit,
            distinct,
        )
    })()
}

#[allow(clippy::too_many_arguments)]
fn emit_ntile_bucket_count<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    function: &super::PhysicalWindowFunction<'document>,
    spec: &'document crate::translate::semantic::hir::WindowSpec,
    filter: Option<&'document Expr>,
    outer_partition: usize,
    rows: &WindowRows,
) -> QueryResult<usize> {
    let argument = function
        .call
        .arguments
        .first()
        .ok_or(PhysicalQueryError::Invalid("ntile has no bucket argument"))?;
    let bucket_count = program.alloc_register();
    let initialized = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: initialized,
    });

    let key_count = spec.order_by.len() + 1;
    let first_key = program.alloc_registers(key_count);
    let current_key = program.alloc_registers(key_count);
    let inner_cursor = duplicate_window_rows(program, rows);
    let mut outer_runtime = None;
    let emission = (|| -> QueryResult<()> {
        let loop_start = program.allocate_label();
        let loop_next = program.allocate_label();
        let choose = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: inner_cursor,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        outer_runtime = Some(bind_window_row(program, bindings, rows, inner_cursor)?);
        if let Some(filter) = filter {
            emit_filter(plan, program, bindings, ctes, filter, loop_next)?;
        }
        if !spec.partition_by.is_empty() {
            let inner_partition = program.alloc_registers(spec.partition_by.len());
            for (position, expression) in spec.partition_by.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                    expression,
                    RegisterRange::new(inner_partition + position, 1),
                )?;
            }
            program.emit_insn(Insn::Compare {
                start_reg_a: outer_partition,
                start_reg_b: inner_partition,
                count: spec.partition_by.len(),
                key_info: spec
                    .partition_by
                    .iter()
                    .map(|expression| KeyInfo {
                        sort_order: SortOrder::Asc,
                        collation: expression_collation(plan, expression),
                        nulls_order: None,
                    })
                    .collect(),
            });
            let same_partition = program.allocate_label();
            program.emit_insn(Insn::Jump {
                target_pc_lt: loop_next,
                target_pc_eq: same_partition,
                target_pc_gt: loop_next,
            });
            program.preassign_label_to_next_insn(same_partition);
        }

        for (position, term) in spec.order_by.iter().enumerate() {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_into(&term.expr, RegisterRange::new(current_key + position, 1))?;
        }
        program.emit_insn(Insn::RowId {
            cursor_id: inner_cursor,
            dest: current_key + spec.order_by.len(),
        });
        program.emit_insn(Insn::IfNot {
            reg: initialized,
            target_pc: choose,
            jump_if_null: true,
        });
        let mut key_info = spec
            .order_by
            .iter()
            .map(|term| KeyInfo {
                sort_order: term.order,
                collation: term
                    .collation
                    .as_ref()
                    .map_or(CollationSeq::Binary, |collation| *collation.value()),
                nulls_order: term.nulls,
            })
            .try_collect::<crate::alloc::Vec<_>>()?;
        key_info.try_push(KeyInfo {
            sort_order: SortOrder::Asc,
            collation: CollationSeq::Binary,
            nulls_order: None,
        })?;
        program.emit_insn(Insn::Compare {
            start_reg_a: first_key,
            start_reg_b: current_key,
            count: key_count,
            key_info,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: loop_next,
            target_pc_eq: loop_next,
            target_pc_gt: choose,
        });
        program.preassign_label_to_next_insn(choose);
        program.emit_insn(Insn::Copy {
            src_reg: current_key,
            dst_reg: first_key,
            extra_amount: key_count - 1,
        });
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(argument, RegisterRange::new(bucket_count, 1))?;
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: initialized,
        });
        program.preassign_label_to_next_insn(loop_next);
        program.emit_insn(Insn::Next {
            cursor_id: inner_cursor,
            pc_if_next: loop_start,
        });
        program.preassign_label_to_next_insn(done);
        program.emit_insn(Insn::Close {
            cursor_id: inner_cursor,
        });
        Ok(())
    })();
    if let Some(bound) = outer_runtime {
        restore_window_row(bindings, bound)?;
    }
    emission?;

    program.emit_insn(Insn::Cast {
        reg: bucket_count,
        affinity: crate::vdbe::affinity::Affinity::Integer,
    });
    let valid = program.allocate_label();
    let zero = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: zero,
    });
    program.emit_insn(Insn::Gt {
        lhs: bucket_count,
        rhs: zero,
        target_pc: valid,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: "argument of ntile must be a positive integer".to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(valid);
    Ok(bucket_count)
}

#[allow(clippy::too_many_arguments)]
fn emit_default_aggregate_window<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    window: &super::PhysicalWindowFunction<'document>,
    filter: Option<&'document Expr>,
    rows: &WindowRows,
) -> QueryResult<()> {
    let call = window.call;
    let function = runtime_aggregate_function(call)?;
    let spec = call.window.as_ref().ok_or(PhysicalQueryError::Invalid(
        "aggregate window has no specification",
    ))?;
    if spec.frame.is_some() {
        return Err(PhysicalQueryError::Unsupported(
            "explicit frame for an aggregate window",
        ));
    }
    if !call.argument_order.is_empty() {
        return Err(PhysicalQueryError::Unsupported(
            "argument ORDER BY inside an aggregate window",
        ));
    }
    let value = bindings.window_function(window.id)?.register;
    program.emit_insn(Insn::Null {
        dest: value.0,
        dest_end: None,
    });
    let distinct = call.distinctness.is_some().then(|| {
        let hash_table_id = program.alloc_hash_table_id();
        program.emit_insn(Insn::HashClear { hash_table_id });
        hash_table_id
    });
    let outer_partition = program.alloc_registers(spec.partition_by.len());
    for (position, expression) in spec.partition_by.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
            expression,
            RegisterRange::new(outer_partition + position, 1),
        )?;
    }
    let outer_order = program.alloc_registers(spec.order_by.len());
    for (position, term) in spec.order_by.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(&term.expr, RegisterRange::new(outer_order + position, 1))?;
    }

    let inner_cursor = duplicate_window_rows(program, rows);
    let mut outer_runtime = None;
    let emission = (|| -> QueryResult<()> {
        let loop_start = program.allocate_label();
        let loop_next = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: inner_cursor,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        outer_runtime = Some(bind_window_row(program, bindings, rows, inner_cursor)?);
        if let Some(filter) = filter {
            emit_filter(plan, program, bindings, ctes, filter, loop_next)?;
        }
        if !spec.partition_by.is_empty() {
            let inner_partition = program.alloc_registers(spec.partition_by.len());
            for (position, expression) in spec.partition_by.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                    expression,
                    RegisterRange::new(inner_partition + position, 1),
                )?;
            }
            program.emit_insn(Insn::Compare {
                start_reg_a: outer_partition,
                start_reg_b: inner_partition,
                count: spec.partition_by.len(),
                key_info: spec
                    .partition_by
                    .iter()
                    .map(|expression| KeyInfo {
                        sort_order: SortOrder::Asc,
                        collation: expression_collation(plan, expression),
                        nulls_order: None,
                    })
                    .collect(),
            });
            let same_partition = program.allocate_label();
            program.emit_insn(Insn::Jump {
                target_pc_lt: loop_next,
                target_pc_eq: same_partition,
                target_pc_gt: loop_next,
            });
            program.preassign_label_to_next_insn(same_partition);
        }
        if !spec.order_by.is_empty() {
            let inner_order = program.alloc_registers(spec.order_by.len());
            for (position, term) in spec.order_by.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_into(&term.expr, RegisterRange::new(inner_order + position, 1))?;
            }
            program.emit_insn(Insn::Compare {
                start_reg_a: outer_order,
                start_reg_b: inner_order,
                count: spec.order_by.len(),
                key_info: spec
                    .order_by
                    .iter()
                    .map(|term| KeyInfo {
                        sort_order: term.order,
                        collation: term
                            .collation
                            .as_ref()
                            .map_or(CollationSeq::Binary, |collation| *collation.value()),
                        nulls_order: term.nulls,
                    })
                    .collect(),
            });
            let inside_frame = program.allocate_label();
            program.emit_insn(Insn::Jump {
                target_pc_lt: loop_next,
                target_pc_eq: inside_frame,
                target_pc_gt: inside_frame,
            });
            program.preassign_label_to_next_insn(inside_frame);
        }
        if let Some(aggregate_filter) = call.filter.as_deref() {
            emit_filter(plan, program, bindings, ctes, aggregate_filter, loop_next)?;
        }
        let (column, delimiter, comparator, collation) = match &function {
            AggFunc::Count0 => {
                let one = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: one,
                });
                (one, 0, None, None)
            }
            AggFunc::Avg
            | AggFunc::Count
            | AggFunc::Max
            | AggFunc::Min
            | AggFunc::Sum
            | AggFunc::Total
            | AggFunc::ArrayAgg => {
                let [argument] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "aggregate window has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let argument_value =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(argument)?;
                let comparison = matches!(&function, AggFunc::Min | AggFunc::Max);
                (
                    argument_value.first.0,
                    0,
                    comparison
                        .then(|| expression_type_fact(plan, argument))
                        .flatten()
                        .as_ref()
                        .and_then(sort_comparator),
                    comparison.then(|| expression_collation(plan, argument)),
                )
            }
            AggFunc::GroupConcat | AggFunc::StringAgg => {
                let ([argument] | [argument, _]) = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "string aggregate window has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let argument_value =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(argument)?;
                let delimiter = if let Some(delimiter) = call.arguments.get(1) {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(delimiter)?
                        .first
                        .0
                } else {
                    let delimiter = program.alloc_register();
                    program.emit_insn(Insn::String8 {
                        value: ",".to_string(),
                        dest: delimiter,
                    });
                    delimiter
                };
                (argument_value.first.0, delimiter, None, None)
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                let [key, object_value] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "JSON object window has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let key = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(key)?;
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let object_value =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(object_value)?;
                (key.first.0, object_value.first.0, None, None)
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
                let [argument] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "JSON array window has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let argument_value =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(argument)?;
                (argument_value.first.0, 0, None, None)
            }
            AggFunc::Mode | AggFunc::PercentileCont | AggFunc::PercentileDisc => {
                return Err(PhysicalQueryError::Unsupported(
                    "ordered-set aggregate window",
                ));
            }
            AggFunc::External(external) => {
                let first = if call.arguments.is_empty() {
                    0
                } else {
                    let arguments = program.alloc_registers(call.arguments.len());
                    for (position, argument) in call.arguments.iter().enumerate() {
                        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                            .emit_into(argument, RegisterRange::new(arguments + position, 1))?;
                    }
                    arguments
                };
                if external.agg_args().is_err() {
                    return Err(PhysicalQueryError::Invalid(
                        "resolved external aggregate window has no aggregate implementation",
                    ));
                }
                (first, 0, None, None)
            }
        };
        if let Some(hash_table_id) = distinct {
            let duplicate = program.allocate_label();
            program.emit_insn(Insn::HashDistinct {
                data: Box::new(HashDistinctData {
                    hash_table_id,
                    key_start_reg: column,
                    num_keys: 1,
                    collations: vec![call
                        .arguments
                        .first()
                        .map_or(CollationSeq::Binary, |argument| {
                            expression_collation(plan, argument)
                        })],
                    target_pc: duplicate,
                }),
            });
            program.emit_insn(Insn::AggStep {
                acc_reg: value.0,
                col: column,
                delimiter,
                func: AccumulatorFunc::Agg(function.clone()),
                comparator,
                collation,
            });
            program.preassign_label_to_next_insn(duplicate);
        } else {
            program.emit_insn(Insn::AggStep {
                acc_reg: value.0,
                col: column,
                delimiter,
                func: AccumulatorFunc::Agg(function.clone()),
                comparator,
                collation,
            });
        }
        program.preassign_label_to_next_insn(loop_next);
        program.emit_insn(Insn::Next {
            cursor_id: inner_cursor,
            pc_if_next: loop_start,
        });
        program.preassign_label_to_next_insn(done);
        program.emit_insn(Insn::AggFinal {
            register: value.0,
            func: AccumulatorFunc::Agg(function.clone()),
        });
        if let Some(hash_table_id) = distinct {
            program.emit_insn(Insn::HashClose { hash_table_id });
        }
        program.emit_insn(Insn::Close {
            cursor_id: inner_cursor,
        });
        Ok(())
    })();
    if let Some(bound) = outer_runtime {
        restore_window_row(bindings, bound)?;
    }
    emission
}

#[allow(clippy::too_many_arguments)]
fn emit_positional_window<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    function: &super::PhysicalWindowFunction<'document>,
    kind: &WindowFunc,
    spec: &'document crate::translate::semantic::hir::WindowSpec,
    filter: Option<&'document Expr>,
    outer_rowid: RegisterId,
    rows: &WindowRows,
) -> QueryResult<()> {
    let value = bindings.window_function(function.id)?.register;
    if matches!(kind, WindowFunc::Lag | WindowFunc::Lead) {
        if let Some(default) = function.call.arguments.get(2) {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_into(default, RegisterRange::new(value.0, 1))?;
        } else {
            program.emit_insn(Insn::Null {
                dest: value.0,
                dest_end: None,
            });
        }
    } else {
        program.emit_insn(Insn::Null {
            dest: value.0,
            dest_end: None,
        });
    }
    if matches!(
        kind,
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue
    ) && function
        .call
        .window
        .as_ref()
        .is_some_and(|spec| spec.frame.is_some())
    {
        return Err(PhysicalQueryError::Unsupported(
            "explicit frame for a positional value window",
        ));
    }
    let offset = if matches!(kind, WindowFunc::Lag | WindowFunc::Lead) {
        if let Some(offset) = function.call.arguments.get(1) {
            let register = program.alloc_register();
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_into(offset, RegisterRange::new(register, 1))?;
            Some(register)
        } else {
            let register = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: register,
            });
            Some(register)
        }
    } else {
        None
    };
    let nth = if matches!(kind, WindowFunc::NthValue) {
        let nth = function
            .call
            .arguments
            .get(1)
            .ok_or(PhysicalQueryError::Invalid(
                "nth_value has no position argument",
            ))?;
        let register = program.alloc_register();
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(nth, RegisterRange::new(register, 1))?;
        let invalid = program.allocate_label();
        let valid = program.allocate_label();
        program.emit_insn(Insn::MustBeInt {
            reg: register,
            target_pc: Some(invalid),
        });
        let zero = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: zero,
        });
        program.emit_insn(Insn::Gt {
            lhs: register,
            rhs: zero,
            target_pc: valid,
            flags: crate::vdbe::insn::CmpInsFlags::default(),
            collation: None,
        });
        program.preassign_label_to_next_insn(invalid);
        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_ERROR,
            description: "second argument to nth_value must be a positive integer".to_string(),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(valid);
        Some(register)
    } else {
        None
    };
    let outer_partition = program.alloc_registers(spec.partition_by.len());
    for (position, expression) in spec.partition_by.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
            expression,
            RegisterRange::new(outer_partition + position, 1),
        )?;
    }
    let outer_order = program.alloc_registers(spec.order_by.len());
    for (position, term) in spec.order_by.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(&term.expr, RegisterRange::new(outer_order + position, 1))?;
    }

    let key_count = spec.order_by.len() + 1;
    let sorter = program.alloc_cursor_id(CursorType::Sorter);
    let mut order_collations_nulls = spec
        .order_by
        .iter()
        .map(|term| {
            (
                term.order,
                term.collation.as_ref().map(|collation| *collation.value()),
                term.nulls,
            )
        })
        .try_collect::<crate::alloc::Vec<_>>()?;
    order_collations_nulls.try_push((SortOrder::Asc, Some(CollationSeq::Binary), None))?;
    let mut comparators = spec
        .order_by
        .iter()
        .map(|term| sort_comparator(&term.type_fact))
        .try_collect::<crate::alloc::Vec<_>>()?;
    comparators.try_push(None)?;
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sorter,
        columns: key_count,
        order_collations_nulls,
        comparators,
    });

    let inner_cursor = duplicate_window_rows(program, rows);
    let scan_start = program.allocate_label();
    let scan_next = program.allocate_label();
    let scan_done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: inner_cursor,
        pc_if_empty: scan_done,
    });
    program.preassign_label_to_next_insn(scan_start);
    let outer_runtime = bind_window_row(program, bindings, rows, inner_cursor)?;
    if let Some(filter) = filter {
        emit_filter(plan, program, bindings, ctes, filter, scan_next)?;
    }
    if !spec.partition_by.is_empty() {
        let inner_partition = program.alloc_registers(spec.partition_by.len());
        for (position, expression) in spec.partition_by.iter().enumerate() {
            let mut subqueries = QuerySubqueryEmitter { plan, ctes };
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                expression,
                RegisterRange::new(inner_partition + position, 1),
            )?;
        }
        program.emit_insn(Insn::Compare {
            start_reg_a: outer_partition,
            start_reg_b: inner_partition,
            count: spec.partition_by.len(),
            key_info: spec
                .partition_by
                .iter()
                .map(|expression| KeyInfo {
                    sort_order: SortOrder::Asc,
                    collation: expression_collation(plan, expression),
                    nulls_order: None,
                })
                .collect(),
        });
        let same_partition = program.allocate_label();
        program.emit_insn(Insn::Jump {
            target_pc_lt: scan_next,
            target_pc_eq: same_partition,
            target_pc_gt: scan_next,
        });
        program.preassign_label_to_next_insn(same_partition);
    }
    let fields = program.alloc_registers(key_count + 1);
    for (position, term) in spec.order_by.iter().enumerate() {
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
            .emit_into(&term.expr, RegisterRange::new(fields + position, 1))?;
    }
    program.emit_insn(Insn::RowId {
        cursor_id: inner_cursor,
        dest: fields + spec.order_by.len(),
    });
    let argument = function
        .call
        .arguments
        .first()
        .ok_or(PhysicalQueryError::Invalid(
            "navigation window has no value argument",
        ))?;
    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
        .emit_into(argument, RegisterRange::new(fields + key_count, 1))?;
    let record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(fields),
        count: to_u32(key_count + 1),
        dest_reg: to_u32(record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sorter,
        record_reg: record,
    });
    program.preassign_label_to_next_insn(scan_next);
    program.emit_insn(Insn::Next {
        cursor_id: inner_cursor,
        pc_if_next: scan_start,
    });
    program.preassign_label_to_next_insn(scan_done);
    restore_window_row(bindings, outer_runtime)?;
    program.emit_insn(Insn::Close {
        cursor_id: inner_cursor,
    });

    let positions = ephemeral_table("window_navigation".to_string(), 1)?;
    let positions_cursor = program.alloc_cursor_id(CursorType::BTreeTable(positions.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: positions_cursor,
        is_table: true,
    });
    let sorted_record = program.alloc_register();
    let pseudo = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: key_count + 1,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo,
        content_reg: sorted_record,
        num_fields: key_count + 1,
    });
    let outer_position = program.alloc_register();
    let peer_end = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: outer_position,
        dest_end: None,
    });
    program.emit_insn(Insn::Null {
        dest: peer_end,
        dest_end: None,
    });
    let sort_start = program.allocate_label();
    let sort_next = program.allocate_label();
    let sort_done = program.allocate_label();
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter,
        pc_if_empty: sort_done,
    });
    program.preassign_label_to_next_insn(sort_start);
    program.emit_insn(Insn::SorterData {
        cursor_id: sorter,
        dest_reg: sorted_record,
        pseudo_cursor: pseudo,
    });
    let ordinal = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: positions_cursor,
        rowid_reg: ordinal,
        prev_largest_reg: 0,
    });
    let after_peer = program.allocate_label();
    if spec.order_by.is_empty() {
        program.emit_insn(Insn::Copy {
            src_reg: ordinal,
            dst_reg: peer_end,
            extra_amount: 0,
        });
    } else {
        let sorted_order = program.alloc_registers(spec.order_by.len());
        for position in 0..spec.order_by.len() {
            program.emit_insn(Insn::Column {
                cursor_id: pseudo,
                column: position,
                dest: sorted_order + position,
                default: None,
            });
        }
        program.emit_insn(Insn::Compare {
            start_reg_a: outer_order,
            start_reg_b: sorted_order,
            count: spec.order_by.len(),
            key_info: spec
                .order_by
                .iter()
                .map(|term| KeyInfo {
                    sort_order: term.order,
                    collation: term
                        .collation
                        .as_ref()
                        .map_or(CollationSeq::Binary, |collation| *collation.value()),
                    nulls_order: term.nulls,
                })
                .try_collect()?,
        });
        let update_peer = program.allocate_label();
        program.emit_insn(Insn::Jump {
            target_pc_lt: after_peer,
            target_pc_eq: update_peer,
            target_pc_gt: after_peer,
        });
        program.preassign_label_to_next_insn(update_peer);
        program.emit_insn(Insn::Copy {
            src_reg: ordinal,
            dst_reg: peer_end,
            extra_amount: 0,
        });
    }
    program.preassign_label_to_next_insn(after_peer);
    let sorted_rowid = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: pseudo,
        column: key_count - 1,
        dest: sorted_rowid,
        default: None,
    });
    program.emit_insn(Insn::Ne {
        lhs: sorted_rowid,
        rhs: outer_rowid.0,
        target_pc: sort_next,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Copy {
        src_reg: ordinal,
        dst_reg: outer_position,
        extra_amount: 0,
    });
    program.preassign_label_to_next_insn(sort_next);
    let sorted_value = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: pseudo,
        column: key_count,
        dest: sorted_value,
        default: None,
    });
    let value_record = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(sorted_value),
        count: 1,
        dest_reg: to_u32(value_record),
        index_name: Some(positions.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: positions_cursor,
        key_reg: ordinal,
        record_reg: value_record,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: positions.name.clone(),
    });
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter,
        pc_if_next: sort_start,
    });
    program.preassign_label_to_next_insn(sort_done);

    let target = program.alloc_register();
    let no_value = program.allocate_label();
    let missing_saved_row = matches!(
        kind,
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue
    )
    .then(|| program.allocate_label());
    if *kind == WindowFunc::Lag {
        let last_buffered_lookahead = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: -1,
            dest: last_buffered_lookahead,
        });
        program.emit_insn(Insn::Lt {
            lhs: offset.expect("lag has an offset"),
            rhs: last_buffered_lookahead,
            target_pc: no_value,
            flags: crate::vdbe::insn::CmpInsFlags::default(),
            collation: None,
        });
    }
    let target_instruction = match kind {
        WindowFunc::Lag => Insn::Subtract {
            lhs: outer_position,
            rhs: offset.expect("lag has an offset"),
            dest: target,
        },
        WindowFunc::Lead => Insn::Add {
            lhs: outer_position,
            rhs: offset.expect("lead has an offset"),
            dest: target,
        },
        WindowFunc::FirstValue => Insn::Integer {
            value: 1,
            dest: target,
        },
        WindowFunc::LastValue => Insn::Copy {
            src_reg: peer_end,
            dst_reg: target,
            extra_amount: 0,
        },
        WindowFunc::NthValue => Insn::Copy {
            src_reg: nth.expect("nth_value validates its position"),
            dst_reg: target,
            extra_amount: 0,
        },
        _ => {
            return Err(PhysicalQueryError::Invalid(
                "non-positional function reached positional emission",
            ));
        }
    };
    program.emit_insn(target_instruction);
    if matches!(kind, WindowFunc::NthValue) {
        program.emit_insn(Insn::Gt {
            lhs: target,
            rhs: peer_end,
            target_pc: no_value,
            flags: crate::vdbe::insn::CmpInsFlags::default(),
            collation: None,
        });
    }
    program.emit_insn(Insn::SeekRowid {
        cursor_id: positions_cursor,
        src_reg: target,
        target_pc: missing_saved_row.unwrap_or(no_value),
    });
    program.emit_insn(Insn::Column {
        cursor_id: positions_cursor,
        column: 0,
        dest: value.0,
        default: None,
    });
    if let Some(missing_saved_row) = missing_saved_row {
        let lookup_done = program.allocate_label();
        program.emit_insn(Insn::Goto {
            target_pc: lookup_done,
        });
        program.preassign_label_to_next_insn(missing_saved_row);
        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_ERROR,
            description: "positional window could not find a saved row within its frame"
                .to_string(),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(lookup_done);
    }
    program.preassign_label_to_next_insn(no_value);
    program.emit_insn(Insn::Close { cursor_id: pseudo });
    program.emit_insn(Insn::Close { cursor_id: sorter });
    program.emit_insn(Insn::Close {
        cursor_id: positions_cursor,
    });
    Ok(())
}

fn open_ordered_aggregate_sorters<'document>(
    program: &mut ProgramBuilder,
    bindings: &RuntimeBindings<'document>,
    aggregates: &[PhysicalAggregate<'document>],
) -> QueryResult<()> {
    for aggregate in aggregates {
        let Some(sorter) = bindings.aggregate(aggregate.id)?.ordered_sorter else {
            continue;
        };
        let order_by = &aggregate.call.argument_order;
        program.emit_insn(Insn::SorterOpen {
            cursor_id: sorter.cursor,
            columns: order_by.len(),
            order_collations_nulls: order_by
                .iter()
                .map(|term| {
                    (
                        term.order,
                        term.collation.as_ref().map(|collation| *collation.value()),
                        term.nulls,
                    )
                })
                .try_collect()?,
            comparators: order_by
                .iter()
                .map(|term| sort_comparator(&term.type_fact))
                .try_collect()?,
        });
    }
    Ok(())
}

fn drain_ordered_aggregate_sorters<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &RuntimeBindings<'document>,
    aggregates: &[PhysicalAggregate<'document>],
) -> QueryResult<()> {
    for aggregate in aggregates {
        let call = aggregate.call;
        let Some(sorter) = bindings.aggregate(aggregate.id)?.ordered_sorter else {
            continue;
        };
        let function = runtime_aggregate_function(call)?;
        let field_count = call.argument_order.len() + call.arguments.len();
        let data = program.alloc_register();
        let pseudo = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
            column_count: field_count,
        }));
        program.emit_insn(Insn::OpenPseudo {
            cursor_id: pseudo,
            content_reg: data,
            num_fields: field_count,
        });
        let loop_start = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::SorterSort {
            cursor_id: sorter.cursor,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_insn(Insn::SorterData {
            cursor_id: sorter.cursor,
            dest_reg: data,
            pseudo_cursor: pseudo,
        });
        let arguments = program.alloc_registers(call.arguments.len());
        for position in 0..call.arguments.len() {
            program.emit_insn(Insn::Column {
                cursor_id: pseudo,
                column: call.argument_order.len() + position,
                dest: arguments + position,
                default: None,
            });
        }
        let (column, delimiter, comparator, collation) = match &function {
            AggFunc::Count0 => {
                return Err(PhysicalQueryError::Invalid(
                    "count(*) cannot have argument ORDER BY",
                ));
            }
            AggFunc::Avg
            | AggFunc::Count
            | AggFunc::Max
            | AggFunc::Min
            | AggFunc::Sum
            | AggFunc::Total
            | AggFunc::ArrayAgg => {
                if call.arguments.len() != 1 {
                    return Err(PhysicalQueryError::Invalid(
                        "ordered aggregate has the wrong argument count",
                    ));
                }
                let comparison = matches!(&function, AggFunc::Min | AggFunc::Max);
                (
                    arguments,
                    0,
                    comparison
                        .then(|| expression_type_fact(plan, &call.arguments[0]))
                        .flatten()
                        .as_ref()
                        .and_then(sort_comparator),
                    comparison.then(|| expression_collation(plan, &call.arguments[0])),
                )
            }
            AggFunc::GroupConcat => {
                if !(1..=2).contains(&call.arguments.len()) {
                    return Err(PhysicalQueryError::Invalid(
                        "ordered group_concat has the wrong argument count",
                    ));
                }
                let delimiter = if call.arguments.len() == 2 {
                    arguments + 1
                } else {
                    let delimiter = program.alloc_register();
                    program.emit_insn(Insn::String8 {
                        value: ",".to_string(),
                        dest: delimiter,
                    });
                    delimiter
                };
                (arguments, delimiter, None, None)
            }
            AggFunc::StringAgg => {
                if call.arguments.len() != 2 {
                    return Err(PhysicalQueryError::Invalid(
                        "ordered string_agg has the wrong argument count",
                    ));
                }
                (arguments, arguments + 1, None, None)
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                if call.arguments.len() != 2 {
                    return Err(PhysicalQueryError::Invalid(
                        "ordered JSON object aggregate has the wrong argument count",
                    ));
                }
                (arguments, arguments + 1, None, None)
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
                if call.arguments.len() != 1 {
                    return Err(PhysicalQueryError::Invalid(
                        "ordered JSON array aggregate has the wrong argument count",
                    ));
                }
                (arguments, 0, None, None)
            }
            AggFunc::External(_) => (arguments, 0, None, None),
            AggFunc::Mode | AggFunc::PercentileCont | AggFunc::PercentileDisc => {
                return Err(PhysicalQueryError::Invalid(
                    "WITHIN GROUP aggregate also has argument ORDER BY",
                ));
            }
        };
        let runtime = bindings.aggregate(aggregate.id)?;
        let duplicate = call
            .distinctness
            .is_some()
            .then(|| program.allocate_label());
        if let Some(duplicate) = duplicate {
            let hash_table_id = runtime
                .distinct_hash_table
                .ok_or(PhysicalQueryError::Invalid(
                    "DISTINCT aggregate has no duplicate set",
                ))?;
            program.emit_insn(Insn::HashDistinct {
                data: Box::new(HashDistinctData {
                    hash_table_id,
                    key_start_reg: column,
                    num_keys: 1,
                    collations: vec![expression_collation(plan, &call.arguments[0])],
                    target_pc: duplicate,
                }),
            });
        }
        program.emit_insn(Insn::AggStep {
            acc_reg: runtime.register.0,
            col: column,
            delimiter,
            func: AccumulatorFunc::Agg(function.clone()),
            comparator,
            collation,
        });
        if let Some(duplicate) = duplicate {
            program.preassign_label_to_next_insn(duplicate);
        }
        program.emit_insn(Insn::SorterNext {
            cursor_id: sorter.cursor,
            pc_if_next: loop_start,
        });
        program.preassign_label_to_next_insn(done);
        program.emit_insn(Insn::Close { cursor_id: pseudo });
        program.emit_insn(Insn::Close {
            cursor_id: sorter.cursor,
        });
    }
    Ok(())
}

fn runtime_aggregate_function(call: &FunctionCall) -> QueryResult<AggFunc> {
    let Func::Agg(function) = call.function.value() else {
        return Err(PhysicalQueryError::Invalid(
            "non-aggregate reached aggregate emission",
        ));
    };
    let AggFunc::External(external) = function else {
        return Ok(function.clone());
    };
    let registered_arguments = external.agg_args().map_err(|_| {
        PhysicalQueryError::Invalid("resolved external function is not an aggregate")
    })?;
    if registered_arguments >= 0 && registered_arguments as usize != call.arguments.len() {
        return Err(PhysicalQueryError::Invalid(
            "external aggregate has the wrong argument count",
        ));
    }
    let external = if registered_arguments < 0 {
        Arc::new(external.with_aggregate_arg_count(call.arguments.len()))
    } else {
        external.clone()
    };
    Ok(AggFunc::External(external))
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
        if let Some(sorter) = bindings.aggregate(aggregate.id)?.ordered_sorter {
            let skip = call.filter.as_ref().map(|_| program.allocate_label());
            if let (Some(filter), Some(skip)) = (call.filter.as_deref(), skip) {
                emit_filter(plan, program, bindings, ctes, filter, skip)?;
            }
            if call.arguments.is_empty() {
                return Err(PhysicalQueryError::Invalid(
                    "ordered aggregate has no arguments",
                ));
            }
            let fields = program.alloc_registers(call.argument_order.len() + call.arguments.len());
            for (position, term) in call.argument_order.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_into(&term.expr, RegisterRange::new(fields + position, 1))?;
            }
            for (position, argument) in call.arguments.iter().enumerate() {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries).emit_into(
                    argument,
                    RegisterRange::new(fields + call.argument_order.len() + position, 1),
                )?;
            }
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(fields),
                count: to_u32(call.argument_order.len() + call.arguments.len()),
                dest_reg: to_u32(sorter.record.0),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::SorterInsert {
                cursor_id: sorter.cursor,
                record_reg: sorter.record.0,
            });
            if let Some(skip) = skip {
                program.preassign_label_to_next_insn(skip);
            }
            continue;
        }
        let function = runtime_aggregate_function(call)?;
        let skip = call.filter.as_ref().map(|_| program.allocate_label());
        if let (Some(filter), Some(skip)) = (call.filter.as_deref(), skip) {
            emit_filter(plan, program, bindings, ctes, filter, skip)?;
        }
        let (column, delimiter, comparator, collation) = match &function {
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
                (one, 0, None, None)
            }
            AggFunc::Avg
            | AggFunc::Count
            | AggFunc::Max
            | AggFunc::Min
            | AggFunc::Sum
            | AggFunc::Total
            | AggFunc::ArrayAgg => {
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
                let comparison = matches!(&function, AggFunc::Min | AggFunc::Max);
                (
                    value.first.0,
                    0,
                    comparison
                        .then(|| expression_type_fact(plan, argument))
                        .flatten()
                        .as_ref()
                        .and_then(sort_comparator),
                    comparison.then(|| expression_collation(plan, argument)),
                )
            }
            AggFunc::GroupConcat => {
                let ([argument] | [argument, _]) = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "group_concat has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(argument)?;
                let delimiter = if let Some(delimiter) = call.arguments.get(1) {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(delimiter)?
                        .first
                        .0
                } else {
                    let delimiter = program.alloc_register();
                    program.emit_insn(Insn::String8 {
                        value: ",".to_string(),
                        dest: delimiter,
                    });
                    delimiter
                };
                (value.first.0, delimiter, None, None)
            }
            AggFunc::StringAgg => {
                let [argument, delimiter] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "string_agg has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(argument)?;
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let delimiter =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(delimiter)?;
                (value.first.0, delimiter.first.0, None, None)
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                let [key, value] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "json_group_object has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let key = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(key)?;
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(value)?;
                (key.first.0, value.first.0, None, None)
            }
            #[cfg(feature = "json")]
            AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
                let [argument] = call.arguments.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "json_group_array has the wrong argument count",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(argument)?;
                (value.first.0, 0, None, None)
            }
            AggFunc::Mode => {
                let [term] = call.within_group.as_slice() else {
                    return Err(PhysicalQueryError::Invalid(
                        "mode has the wrong WITHIN GROUP shape",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(&term.expr)?;
                (
                    value.first.0,
                    0,
                    sort_comparator(&term.type_fact),
                    Some(
                        term.collation
                            .as_ref()
                            .map_or(CollationSeq::Binary, |value| *value.value()),
                    ),
                )
            }
            AggFunc::PercentileCont | AggFunc::PercentileDisc => {
                let ([fraction], [term]) =
                    (call.arguments.as_slice(), call.within_group.as_slice())
                else {
                    return Err(PhysicalQueryError::Invalid(
                        "percentile has the wrong WITHIN GROUP shape",
                    ));
                };
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let value = ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_new(&term.expr)?;
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                let fraction =
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_new(fraction)?;
                (
                    value.first.0,
                    fraction.first.0,
                    sort_comparator(&term.type_fact),
                    Some(
                        term.collation
                            .as_ref()
                            .map_or(CollationSeq::Binary, |value| *value.value()),
                    ),
                )
            }
            AggFunc::External(function) => {
                let registered = function.agg_args().map_err(|_| {
                    PhysicalQueryError::Invalid("resolved external function is not aggregate")
                })?;
                if registered >= 0 && registered as usize != call.arguments.len() {
                    return Err(PhysicalQueryError::Invalid(
                        "external aggregate has the wrong argument count",
                    ));
                }
                let first = if call.arguments.is_empty() {
                    0
                } else {
                    let arguments = program.alloc_registers(call.arguments.len());
                    for (position, argument) in call.arguments.iter().enumerate() {
                        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                        ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                            .emit_into(argument, RegisterRange::new(arguments + position, 1))?;
                    }
                    arguments
                };
                (first, 0, None, None)
            }
        };
        let runtime = bindings.aggregate(aggregate.id)?;
        if call.distinctness.is_some() {
            if call.arguments.len() != 1 {
                return Err(PhysicalQueryError::Invalid(
                    "DISTINCT aggregate does not have exactly one argument",
                ));
            }
            let duplicate = program.allocate_label();
            let hash_table_id = runtime
                .distinct_hash_table
                .ok_or(PhysicalQueryError::Invalid(
                    "DISTINCT aggregate has no duplicate set",
                ))?;
            program.emit_insn(Insn::HashDistinct {
                data: Box::new(HashDistinctData {
                    hash_table_id,
                    key_start_reg: column,
                    num_keys: 1,
                    collations: vec![expression_collation(plan, &call.arguments[0])],
                    target_pc: duplicate,
                }),
            });
            program.emit_insn(Insn::AggStep {
                acc_reg: runtime.register.0,
                col: column,
                delimiter,
                func: AccumulatorFunc::Agg(function.clone()),
                comparator,
                collation,
            });
            program.preassign_label_to_next_insn(duplicate);
        } else {
            program.emit_insn(Insn::AggStep {
                acc_reg: runtime.register.0,
                col: column,
                delimiter,
                func: AccumulatorFunc::Agg(function.clone()),
                comparator,
                collation,
            });
        }
        if let Some(skip) = skip {
            program.preassign_label_to_next_insn(skip);
        }
    }
    Ok(())
}

fn clear_aggregate_distinct_sets<'document>(
    program: &mut ProgramBuilder,
    bindings: &RuntimeBindings<'document>,
    aggregates: &[PhysicalAggregate<'document>],
) -> QueryResult<()> {
    for aggregate in aggregates {
        if let Some(hash_table_id) = bindings.aggregate(aggregate.id)?.distinct_hash_table {
            program.emit_insn(Insn::HashClear { hash_table_id });
        }
    }
    Ok(())
}

fn close_aggregate_distinct_sets<'document>(
    program: &mut ProgramBuilder,
    bindings: &RuntimeBindings<'document>,
    aggregates: &[PhysicalAggregate<'document>],
) -> QueryResult<()> {
    for aggregate in aggregates {
        if let Some(hash_table_id) = bindings.aggregate(aggregate.id)?.distinct_hash_table {
            program.emit_insn(Insn::HashClose { hash_table_id });
        }
    }
    Ok(())
}

fn expression_type_fact<'document>(
    plan: &PhysicalPlan<'document>,
    expression: &Expr,
) -> Option<TypeFact> {
    match expression {
        Expr::Parameter(parameter) => Some(parameter.type_fact.clone()),
        Expr::Column(column) => plan
            .document
            .source(column.source)
            .and_then(|source| source.columns.get(column.column))
            .map(|column| column.type_fact.clone()),
        Expr::MergedColumn(column) => Some(column.type_fact.clone()),
        Expr::Cast { target, .. } => Some(target.type_fact.clone()),
        Expr::Function(call) => Some(call.result_type.clone()),
        Expr::Collate { expr, .. } => expression_type_fact(plan, expr),
        _ => None,
    }
}

fn expression_collation(plan: &PhysicalPlan<'_>, expression: &Expr) -> CollationSeq {
    match expression {
        Expr::Collate { collation, .. } => *collation.value(),
        Expr::Column(column) => plan
            .document
            .source(column.source)
            .and_then(|source| source.columns.get(column.column))
            .and_then(|column| column.collation.as_ref())
            .map_or(CollationSeq::Binary, |collation| *collation.value()),
        Expr::MergedColumn(column) => column
            .collation
            .as_ref()
            .map_or(CollationSeq::Binary, |collation| *collation.value()),
        _ => CollationSeq::Binary,
    }
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
    let mut full_join_position = None;
    for (position, join) in from.joins.iter().enumerate() {
        if join.kind == JoinKind::Right && position != 0 {
            return Err(PhysicalQueryError::Unsupported(
                "RIGHT JOIN following another join is not yet supported",
            ));
        }
        if join.kind != JoinKind::Full {
            continue;
        }
        if full_join_position.is_some()
            || from.joins[..position].iter().any(|prefix| {
                !matches!(
                    prefix.kind,
                    JoinKind::Comma | JoinKind::Inner | JoinKind::Cross
                )
            })
        {
            return Err(PhysicalQueryError::Unsupported(
                "FULL OUTER JOIN chaining is not yet supported",
            ));
        }
        full_join_position = Some(position);
    }

    let execution_order = inner_scan_order(plan, source_ids, from)?;
    let reordered = execution_order != source_ids;
    let mut scans = Vec::with_capacity(execution_order.len());
    for source_id in &execution_order {
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

    match (from.joins.first(), full_join_position) {
        (Some(join), None) if join.kind == JoinKind::Right => {
            scans.swap(0, 1);
            emit_nested_scan(
                plan,
                program,
                bindings,
                ctes,
                &scans,
                0,
                &from.joins,
                Some((0, JoinKind::Left)),
                false,
                filter,
                action,
            )?;
        }
        (_, Some(position)) => {
            let join = &from.joins[position];
            emit_nested_scan(
                plan,
                program,
                bindings,
                ctes,
                &scans,
                0,
                &from.joins,
                Some((position, JoinKind::Left)),
                false,
                filter,
                action,
            )?;
            emit_full_join_unmatched_right(
                plan,
                program,
                bindings,
                ctes,
                &scans,
                &from.joins,
                position,
                &join.constraint,
                filter,
                action,
            )?;
        }
        _ => emit_nested_scan(
            plan,
            program,
            bindings,
            ctes,
            &scans,
            0,
            &from.joins,
            None,
            reordered,
            filter,
            action,
        )?,
    }
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

fn inner_scan_order<'document>(
    plan: &PhysicalPlan<'document>,
    source_ids: &[SourceId],
    from: &HirFrom,
) -> QueryResult<Vec<SourceId>> {
    if from.joins.iter().any(|join| {
        !matches!(
            join.kind,
            JoinKind::Comma | JoinKind::Inner | JoinKind::Cross
        )
    }) {
        return Ok(source_ids.to_vec());
    }

    let local_sources = source_ids.iter().copied().collect::<FxHashSet<_>>();
    let mut ordered = Vec::with_capacity(source_ids.len());
    let mut remaining = source_ids.to_vec();
    while !remaining.is_empty() {
        let Some(position) = remaining.iter().position(|source_id| {
            table_function_dependencies(plan, *source_id)
                .iter()
                .all(|dependency| {
                    !local_sources.contains(dependency) || ordered.contains(dependency)
                })
        }) else {
            return Err(PhysicalQueryError::Unsupported(
                "cyclic table-function source dependencies",
            ));
        };
        ordered.push(remaining.remove(position));
    }
    Ok(ordered)
}

fn table_function_dependencies(
    plan: &PhysicalPlan<'_>,
    source_id: SourceId,
) -> FxHashSet<SourceId> {
    let mut dependencies = FxHashSet::default();
    let Some(PhysicalSource {
        kind: PhysicalSourceKind::TableFunction { arguments, .. },
        ..
    }) = plan.source(source_id)
    else {
        return dependencies;
    };
    for argument in *arguments {
        argument.walk(&mut |expression| match expression {
            Expr::Column(reference) => {
                dependencies.insert(reference.source);
            }
            Expr::RowId(source) => {
                dependencies.insert(*source);
            }
            Expr::MergedColumn(column) => {
                dependencies.insert(column.right.source);
            }
            _ => {}
        });
    }
    dependencies
}

#[allow(clippy::too_many_arguments)]
fn emit_full_join_unmatched_right<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    scans: &[OpenedScan<'document>],
    joins: &[crate::translate::semantic::hir::Join],
    full_join_position: usize,
    constraint: &JoinConstraint,
    filter: Option<&Expr>,
    action: ScanRowAction<'document, '_>,
) -> QueryResult<()> {
    let prefix = scans
        .get(..=full_join_position)
        .ok_or(PhysicalQueryError::Invalid(
            "FULL OUTER JOIN prefix is missing",
        ))?;
    let right = scans
        .get(full_join_position + 1)
        .ok_or(PhysicalQueryError::Invalid(
            "FULL OUTER JOIN right side is missing",
        ))?;
    let right_start = program.allocate_label();
    let right_next = program.allocate_label();
    let done = program.allocate_label();
    emit_scan_rewind(plan, program, bindings, ctes, right, done)?;
    program.preassign_label_to_next_insn(right_start);
    emit_scan_prepare_row(program, right);
    let matched = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: matched,
    });

    emit_full_join_prefix_matches(
        plan, program, bindings, ctes, prefix, joins, 0, constraint, matched,
    )?;
    program.emit_insn(Insn::IfPos {
        reg: matched,
        target_pc: right_next,
        decrement_by: 0,
    });
    for scan in prefix {
        emit_null_scan(program, scan);
    }
    let tail_level = full_join_position + 2;
    if tail_level < scans.len() {
        emit_nested_scan(
            plan, program, bindings, ctes, scans, tail_level, joins, None, false, filter, action,
        )?;
    } else {
        if let Some(filter) = filter {
            emit_filter(plan, program, bindings, ctes, filter, right_next)?;
        }
        emit_scan_action(plan, program, bindings, ctes, action)?;
    }
    program.preassign_label_to_next_insn(right_next);
    emit_scan_next(program, right, right_start);
    program.preassign_label_to_next_insn(done);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_full_join_prefix_matches<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    prefix: &[OpenedScan<'document>],
    joins: &[crate::translate::semantic::hir::Join],
    level: usize,
    full_constraint: &JoinConstraint,
    matched: usize,
) -> QueryResult<()> {
    let scan = prefix.get(level).ok_or(PhysicalQueryError::Invalid(
        "FULL OUTER JOIN prefix level is missing",
    ))?;
    let loop_start = program.allocate_label();
    let loop_next = program.allocate_label();
    let loop_end = program.allocate_label();
    emit_scan_rewind(plan, program, bindings, ctes, scan, loop_end)?;
    program.preassign_label_to_next_insn(loop_start);
    emit_scan_prepare_row(program, scan);
    if let Some(join) = level
        .checked_sub(1)
        .and_then(|position| joins.get(position))
    {
        emit_join_constraint(plan, program, bindings, ctes, &join.constraint, loop_next)?;
    }
    if level + 1 < prefix.len() {
        emit_full_join_prefix_matches(
            plan,
            program,
            bindings,
            ctes,
            prefix,
            joins,
            level + 1,
            full_constraint,
            matched,
        )?;
    } else {
        emit_join_constraint(plan, program, bindings, ctes, full_constraint, loop_next)?;
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: matched,
        });
    }
    program.preassign_label_to_next_insn(loop_next);
    emit_scan_next(program, scan, loop_start);
    program.preassign_label_to_next_insn(loop_end);
    Ok(())
}

fn emit_null_scan(program: &mut ProgramBuilder, scan: &OpenedScan<'_>) {
    program.emit_insn(Insn::NullRow {
        cursor_id: scan.cursor.id(),
    });
    if let Some(table_cursor) = scan.deferred_table {
        program.emit_insn(Insn::NullRow {
            cursor_id: table_cursor,
        });
    }
    for (register, _) in &scan.index_method_outputs {
        program.emit_insn(Insn::Null {
            dest: *register,
            dest_end: None,
        });
    }
}

fn emit_scan_rewind<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    scan: &OpenedScan<'document>,
    empty: crate::vdbe::BranchOffset,
) -> QueryResult<()> {
    match scan.cursor {
        ScanCursor::BTree(cursor_id) => {
            if let Some(filter) = &scan.index_method_filter {
                let start = program.alloc_registers(filter.arguments.len() + 1);
                program.emit_int(filter.pattern as i64, start);
                for (position, argument) in filter.arguments.iter().enumerate() {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(argument, RegisterRange::new(start + position + 1, 1))?;
                }
                program.emit_insn(Insn::IndexMethodQuery {
                    db: filter.database,
                    cursor_id,
                    start_reg: start,
                    count_reg: filter.arguments.len() + 1,
                    pc_if_empty: empty,
                });
            } else {
                program.emit_insn(Insn::Rewind {
                    cursor_id,
                    pc_if_empty: empty,
                });
            }
        }
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
        ScanCursor::Single(_) => {}
    }
    Ok(())
}

fn emit_scan_prepare_row(program: &mut ProgramBuilder, scan: &OpenedScan<'_>) {
    if let Some(table_cursor_id) = scan.deferred_table {
        program.emit_insn(Insn::DeferredSeek {
            index_cursor_id: scan.cursor.id(),
            table_cursor_id,
        });
    }
    for (register, column) in &scan.index_method_outputs {
        program.emit_insn(Insn::Column {
            cursor_id: scan.cursor.id(),
            column: *column,
            dest: *register,
            default: None,
        });
    }
}

fn emit_scan_next(
    program: &mut ProgramBuilder,
    scan: &OpenedScan<'_>,
    target: crate::vdbe::BranchOffset,
) {
    match scan.cursor {
        ScanCursor::BTree(cursor_id) => program.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: target,
        }),
        ScanCursor::Virtual(cursor_id) => program.emit_insn(Insn::VNext {
            cursor_id,
            pc_if_next: target,
        }),
        ScanCursor::Single(_) => {}
    }
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
    join_override: Option<(usize, JoinKind)>,
    defer_inner_constraints: bool,
    filter: Option<&Expr>,
    action: ScanRowAction<'document, '_>,
) -> QueryResult<()> {
    let scan = scans
        .get(level)
        .ok_or(PhysicalQueryError::Invalid("nested scan level is missing"))?;

    let loop_start = program.allocate_label();
    let loop_next = program.allocate_label();
    let loop_end = program.allocate_label();
    let join_position = level.checked_sub(1);
    let join = (!defer_inner_constraints)
        .then(|| join_position.and_then(|position| joins.get(position)))
        .flatten();
    let join_kind = join_position
        .zip(join)
        .map(|(position, join)| match join_override {
            Some((override_position, kind)) if position == override_position => kind,
            _ => join.kind,
        });
    let left_join = join_kind == Some(JoinKind::Left);
    let unmatched = left_join.then(|| program.allocate_label());
    let matched = left_join.then(|| program.alloc_register());
    if let Some(matched) = matched {
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: matched,
        });
    }
    let empty = unmatched.unwrap_or(loop_end);
    emit_scan_rewind(plan, program, bindings, ctes, scan, empty)?;
    program.preassign_label_to_next_insn(loop_start);
    emit_scan_prepare_row(program, scan);

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
            join_override,
            defer_inner_constraints,
            filter,
            action,
        )?;
    } else {
        if defer_inner_constraints {
            for join in joins {
                emit_join_constraint(plan, program, bindings, ctes, &join.constraint, loop_next)?;
            }
        }
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
        ScanCursor::Single(_) => {}
    }

    if let (Some(unmatched), Some(matched)) = (unmatched, matched) {
        program.preassign_label_to_next_insn(unmatched);
        program.emit_insn(Insn::IfPos {
            reg: matched,
            target_pc: loop_end,
            decrement_by: 0,
        });
        emit_null_scan(program, scan);
        if level + 1 < scans.len() {
            emit_nested_scan(
                plan,
                program,
                bindings,
                ctes,
                scans,
                level + 1,
                joins,
                join_override,
                defer_inner_constraints,
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
            covered_outputs,
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
            Some(covered_outputs),
            result,
            destination,
            limit,
            distinct,
        ),
        ScanRowAction::Aggregate {
            aggregates,
            saved_sources,
            first_row_seen,
        } => {
            let already_saved = program.allocate_label();
            program.emit_insn(Insn::IfPos {
                reg: first_row_seen.0,
                target_pc: already_saved,
                decrement_by: 0,
            });
            for source in saved_sources {
                for position in 0..source.columns.width {
                    let target =
                        source
                            .columns
                            .register(position)
                            .ok_or(PhysicalQueryError::Invalid(
                                "saved aggregate column is missing",
                            ))?;
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(
                            &Expr::column(source.source, position),
                            RegisterRange::new(target.0, 1),
                        )?;
                }
                if let Some(rowid) = source.rowid {
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(&Expr::rowid(source.source), RegisterRange::new(rowid.0, 1))?;
                }
            }
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: first_row_seen.0,
            });
            program.preassign_label_to_next_insn(already_saved);
            emit_aggregate_steps(plan, program, bindings, ctes, aggregates)
        }
        ScanRowAction::GroupSortInsert { sorter } => {
            emit_group_sort_insert(plan, program, bindings, ctes, sorter)
        }
        ScanRowAction::UpdateCandidate {
            target,
            assignments,
            order_by,
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
            let assignment_width = assignments
                .iter()
                .map(|assignment| assignment.columns.len())
                .sum::<usize>();
            let width = assignment_width + order_by.len();
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
            for term in order_by {
                let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                    .emit_into(&term.expr, RegisterRange::new(values + offset, 1))?;
                offset += 1;
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
        ScanRowAction::WindowMaterialize { rows } => {
            emit_window_row_insert(plan, program, bindings, ctes, rows)
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
    emit_source_explain(plan, program, source)?;
    let PhysicalSourceKind::CatalogTable { table, access } = &source.kind else {
        return match &source.kind {
            PhysicalSourceKind::Derived(query) => {
                open_derived_source(plan, program, bindings, ctes, source, *query)
            }
            PhysicalSourceKind::TableFunction { table, arguments } => {
                open_table_function(program, table, arguments)
            }
            PhysicalSourceKind::RecursiveInput(cte) => {
                let (cursor, width) = ctes
                    .recursive_inputs
                    .get(cte)
                    .copied()
                    .ok_or(PhysicalQueryError::Invalid("recursive input is not active"))?;
                if width != source.width {
                    return Err(PhysicalQueryError::Invalid(
                        "recursive input width does not match its source",
                    ));
                }
                Ok(OpenedScan {
                    cursor: ScanCursor::Single(cursor),
                    runtime_cursor: cursor,
                    deferred_table: None,
                    virtual_filter: None,
                    index_method_filter: None,
                    index_method_outputs: Vec::new(),
                    owned: false,
                })
            }
            PhysicalSourceKind::Pseudo { .. } | PhysicalSourceKind::SchemaExpression => {
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
    if let TableAccess::IndexMethod(access) = access {
        let Table::BTree(table) = table.value() else {
            return Err(PhysicalQueryError::Unsupported(
                "custom index method on a non-B-tree table",
            ));
        };
        if access.pattern.index.database() != Some(database_id) {
            return Err(PhysicalQueryError::Invalid(
                "custom index belongs to a different database",
            ));
        }
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        let index = access.pattern.index.handle();
        let index_cursor = program.alloc_cursor_index(None, &index)?;
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor,
            root_page: table.root_page,
            db: database,
        });
        program.emit_insn(Insn::OpenRead {
            cursor_id: index_cursor,
            root_page: index.root_page,
            db: database,
        });
        let mut index_method_outputs = Vec::new();
        for (column, output) in access.pattern.outputs.iter().enumerate() {
            if matches!(output.expr, Expr::Column(_) | Expr::RowId(_)) {
                continue;
            }
            let register = program.alloc_register();
            bindings.bind_output(
                output.id,
                OutputRuntime {
                    register: RegisterId(register),
                },
            )?;
            index_method_outputs.push((register, column));
        }
        return Ok(OpenedScan {
            cursor: ScanCursor::BTree(index_cursor),
            runtime_cursor: table_cursor,
            deferred_table: Some(table_cursor),
            virtual_filter: None,
            index_method_filter: Some(IndexMethodFilter {
                database,
                pattern: access.pattern.id.pattern,
                arguments: access.arguments.clone(),
            }),
            index_method_outputs,
            owned: true,
        });
    }
    if let TableAccess::ForcedIndex(index) = access {
        let Table::BTree(table) = table.value() else {
            return Err(PhysicalQueryError::Unsupported(
                "forced index on a non-B-tree table",
            ));
        };
        if index.database() != Some(database_id)
            || index.value().index_method.is_some()
            || !index.value().has_rowid
        {
            return Err(PhysicalQueryError::Unsupported(
                "custom or rowid-free forced index",
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
            index_method_filter: None,
            index_method_outputs: Vec::new(),
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
                index_method_filter: None,
                index_method_outputs: Vec::new(),
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
                index_method_filter: None,
                index_method_outputs: Vec::new(),
                owned: true,
            })
        }
    }
}

fn emit_source_explain(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    source: &PhysicalSource<'_>,
) -> QueryResult<()> {
    let hir_source = plan
        .document
        .source(source.id)
        .ok_or(PhysicalQueryError::Invalid(
            "physical source has no HIR source",
        ))?;
    match &source.kind {
        PhysicalSourceKind::CatalogTable { access, .. } => match access {
            TableAccess::IndexMethod(access) => {
                let method = access.pattern.index.value().index_method.as_ref().ok_or(
                    PhysicalQueryError::Invalid("custom index access has no index method"),
                )?;
                emit_explain!(
                    program,
                    false,
                    format!("QUERY INDEX METHOD {}", method.definition().method_name)
                );
            }
            TableAccess::ForcedIndex(index) => emit_explain!(
                program,
                false,
                format!(
                    "SCAN {} USING INDEX {}",
                    explain_source_name(hir_source),
                    index.value().name
                )
            ),
            TableAccess::Scan => emit_explain!(
                program,
                false,
                format!("SCAN {}", explain_source_name(hir_source))
            ),
        },
        PhysicalSourceKind::TableFunction { .. }
        | PhysicalSourceKind::Cte(_)
        | PhysicalSourceKind::Derived(_)
        | PhysicalSourceKind::RecursiveInput(_) => {
            emit_explain!(
                program,
                false,
                format!("SCAN {}", explain_source_name(hir_source))
            );
        }
        PhysicalSourceKind::Pseudo { .. } | PhysicalSourceKind::SchemaExpression => {}
    }
    Ok(())
}

fn explain_source_name(source: &crate::translate::semantic::hir::Source) -> String {
    match source.alias.as_deref() {
        None => source.name.clone(),
        Some(alias) if alias == source.name => alias.to_string(),
        Some(alias) => format!("{} AS {alias}", source.name),
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
        index_method_filter: None,
        index_method_outputs: Vec::new(),
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
    if query_plan.hir.output.len() != source.width {
        return Err(PhysicalQueryError::Invalid(
            "derived query width does not match its source",
        ));
    }

    let columns = (0..source.width)
        .map(|position| {
            Column::new_default_text(Some(format!("column_{position}")), "BLOB".to_string(), None)
        })
        .try_collect()?;
    let table = Arc::new(BTreeTable::new(
        0,
        format!("derived_{}", source.id.index()),
        crate::alloc::vec![],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
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
        index_method_filter: None,
        index_method_outputs: Vec::new(),
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
    if let CteBody::Recursive(recursive) = &cte.body {
        let result = materialize_recursive_cte(
            plan,
            program,
            bindings,
            ctes,
            cte_id,
            &cte.name,
            cte.columns.len(),
            recursive,
        );
        ctes.visiting.remove(&cte_id);
        return result;
    }
    let CteBody::Query(query_id) = cte.body else {
        unreachable!();
    };
    let query = plan
        .query(query_id)
        .ok_or(PhysicalQueryError::Invalid("CTE body query is missing"))?;
    if !query.hir.captures.is_empty() {
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

    let table = ephemeral_table(format!("cte_{name}"), width)?;
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

#[allow(clippy::too_many_arguments)]
fn materialize_recursive_cte<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    cte_id: CteId,
    name: &str,
    width: usize,
    recursive: &RecursiveCte,
) -> QueryResult<()> {
    let seed = plan
        .query(recursive.seed)
        .ok_or(PhysicalQueryError::Invalid("recursive CTE seed is missing"))?;
    if seed.hir.output.len() != width || recursive.arms.is_empty() {
        return Err(PhysicalQueryError::Invalid(
            "recursive CTE shape does not match its columns",
        ));
    }
    for arm in &recursive.arms {
        let query = plan
            .query(arm.query)
            .ok_or(PhysicalQueryError::Invalid("recursive CTE arm is missing"))?;
        if query.hir.output.len() != width {
            return Err(PhysicalQueryError::Invalid(
                "recursive CTE arm width does not match its columns",
            ));
        }
    }

    let mut dependencies = query_tree_ctes(plan, recursive.seed)?;
    for arm in &recursive.arms {
        dependencies.extend(query_tree_ctes(plan, arm.query)?);
    }
    dependencies.sort_by_key(|dependency| dependency.index());
    dependencies.dedup();
    for dependency in dependencies {
        if dependency != cte_id {
            materialize_cte(plan, program, bindings, ctes, dependency)?;
        }
    }

    let result_table = ephemeral_table(format!("cte_{name}"), width)?;
    let result_cursor = program.alloc_cursor_id(CursorType::BTreeTable(result_table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: result_cursor,
        is_table: true,
    });

    let input_record = program.alloc_register();
    let input_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: width,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: input_cursor,
        content_reg: input_record,
        num_fields: width,
    });
    if ctes
        .recursive_inputs
        .insert(cte_id, (input_cursor, width))
        .is_some()
    {
        return Err(PhysicalQueryError::Invalid(
            "recursive CTE input is already active",
        ));
    }

    let (queue_cursor, queue_index, sort_width) =
        open_recursive_queue(program, name, width, recursive)?;
    let distinct = recursive
        .arms
        .iter()
        .any(|arm| arm.operator != CompoundOperator::UnionAll);
    let seen = if distinct {
        Some(open_recursive_seen(program, name, width, recursive)?)
    } else {
        None
    };
    let seen_destination = seen
        .as_ref()
        .map(|(cursor, index)| (*cursor, index.as_ref()));
    let queue_destination = QueryDestination::RecursiveQueue {
        cursor_id: queue_cursor,
        index: &queue_index,
        order: &recursive.queue_order,
        seen: seen_destination,
    };
    emit_query(
        plan,
        program,
        bindings,
        ctes,
        recursive.seed,
        queue_destination,
    )?;

    let result_destination = QueryDestination::EphemeralTable {
        cursor_id: result_cursor,
        table: &result_table,
    };
    let limit = open_limit(
        plan,
        program,
        bindings,
        ctes,
        recursive.limit.as_ref(),
        result_destination,
    )?;
    let finished = limit
        .map(|limit| limit.done)
        .unwrap_or_else(|| program.allocate_label());
    let dequeue = program.allocate_label();
    program.preassign_label_to_next_insn(dequeue);
    program.emit_insn(Insn::Rewind {
        cursor_id: queue_cursor,
        pc_if_empty: finished,
    });
    let queue_width = sort_width + 1 + width;
    let queue_row = program.alloc_registers(queue_width);
    for column in 0..queue_width {
        program.emit_insn(Insn::Column {
            cursor_id: queue_cursor,
            column,
            dest: queue_row + column,
            default: None,
        });
    }
    let result = RegisterRange::new(queue_row + sort_width + 1, width);
    program.emit_insn(Insn::IdxDelete {
        start_reg: queue_row,
        num_regs: queue_width,
        cursor_id: queue_cursor,
        raise_error_if_no_matching_entry: false,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(result.first.0),
        count: to_u32(width),
        dest_reg: to_u32(input_record),
        index_name: None,
        affinity_str: None,
    });
    emit_row_destination(
        plan,
        program,
        bindings,
        ctes,
        result,
        result_destination,
        limit,
        None,
    )?;
    for arm in &recursive.arms {
        emit_query(plan, program, bindings, ctes, arm.query, queue_destination)?;
    }
    program.emit_insn(Insn::Goto { target_pc: dequeue });
    program.preassign_label_to_next_insn(finished);
    program.emit_insn(Insn::Close {
        cursor_id: queue_cursor,
    });
    if let Some((cursor_id, _)) = seen {
        program.emit_insn(Insn::Close { cursor_id });
    }
    program.emit_insn(Insn::Close {
        cursor_id: input_cursor,
    });
    ctes.recursive_inputs.remove(&cte_id);
    ctes.by_id.insert(
        cte_id,
        MaterializedCte {
            cursor_id: result_cursor,
            table: result_table,
            width,
        },
    );
    Ok(())
}

fn open_recursive_queue(
    program: &mut ProgramBuilder,
    name: &str,
    width: usize,
    recursive: &RecursiveCte,
) -> QueryResult<(usize, Arc<Index>, usize)> {
    let mut columns = crate::alloc::vec![];
    for (position, term) in recursive.queue_order.iter().enumerate() {
        let default = match term.order {
            SortOrder::Asc => NullsOrder::First,
            SortOrder::Desc => NullsOrder::Last,
        };
        if term.nulls.is_some_and(|nulls| nulls != default) {
            columns.try_push(IndexColumn::new(
                format!("null-rank-{position}"),
                columns.len(),
            ))?;
        }
        let mut column = IndexColumn::new(format!("priority-{position}"), columns.len());
        column.order = term.order;
        column.collation = term
            .explicit_collation
            .as_ref()
            .map(|collation| *collation.value())
            .or_else(|| {
                recursive
                    .comparison_collations
                    .get(term.output)
                    .and_then(|collation| collation.as_ref())
                    .map(|collation| *collation.value())
            });
        columns.try_push(column)?;
    }
    let sort_width = columns.len();
    columns.try_push(IndexColumn::new("sequence", columns.len()))?;
    for output in 0..width {
        columns.try_push(IndexColumn::new(format!("result-{output}"), columns.len()))?;
    }
    let index = Arc::new(Index {
        name: format!("hir_recursive_queue_{name}"),
        table_name: String::new(),
        root_page: 0,
        columns,
        unique: true,
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
    Ok((cursor, index, sort_width))
}

fn open_recursive_seen(
    program: &mut ProgramBuilder,
    name: &str,
    width: usize,
    recursive: &RecursiveCte,
) -> QueryResult<(usize, Arc<Index>)> {
    if recursive.comparison_collations.len() != width {
        return Err(PhysicalQueryError::Invalid(
            "recursive comparison width does not match its columns",
        ));
    }
    let columns = recursive
        .comparison_collations
        .iter()
        .enumerate()
        .map(|(position, collation)| {
            let mut column = IndexColumn::new(format!("distinct-{position}"), position);
            column.collation = collation.as_ref().map(|collation| *collation.value());
            column
        })
        .try_collect()?;
    let index = Arc::new(Index {
        name: format!("hir_recursive_seen_{name}"),
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
        index_method_filter: None,
        index_method_outputs: Vec::new(),
        owned: true,
    })
}

pub(crate) fn ephemeral_table(
    name: String,
    width: usize,
) -> Result<Arc<BTreeTable>, crate::alloc::TryReserveError> {
    let columns = (0..width)
        .map(|position| {
            Column::new_default_text(Some(format!("column_{position}")), "BLOB".to_string(), None)
        })
        .try_collect()?;
    Ok(Arc::new(BTreeTable::new(
        0,
        name,
        crate::alloc::vec![],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    )))
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

fn materialize_ctes_owned_by_current_query<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    root: QueryId,
) -> QueryResult<()> {
    let current = bindings.current_query();
    for cte_id in query_tree_ctes(plan, root)? {
        let cte = plan
            .document
            .cte(cte_id)
            .ok_or(PhysicalQueryError::Invalid("reachable CTE is missing"))?;
        let body = match &cte.body {
            CteBody::Query(query) => *query,
            CteBody::Recursive(recursive) => recursive.seed,
        };
        let parent = plan
            .query(body)
            .ok_or(PhysicalQueryError::Invalid("CTE body query is missing"))?
            .hir
            .parent;
        if parent == current {
            materialize_cte(plan, program, bindings, ctes, cte_id)?;
        }
    }
    Ok(())
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
    covered_outputs: Option<&[Option<crate::translate::semantic::hir::OutputId>]>,
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
    emit_output_expressions(
        plan,
        program,
        bindings,
        ctes,
        outputs,
        covered_outputs,
        result,
    )?;
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

fn emit_output_expressions<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    ctes: &mut MaterializedCtes,
    outputs: &[crate::translate::semantic::hir::Output],
    covered_outputs: Option<&[Option<crate::translate::semantic::hir::OutputId>]>,
    result: RegisterRange,
) -> QueryResult<()> {
    for (position, output) in outputs.iter().enumerate() {
        let target = result
            .register(position)
            .ok_or(PhysicalQueryError::Invalid("output register is missing"))?;
        let mut subqueries = QuerySubqueryEmitter { plan, ctes };
        let covered = covered_outputs
            .and_then(|covered| covered.get(position))
            .and_then(|output| *output);
        if let Some(covered) = covered {
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_into(&Expr::Output(covered), RegisterRange::new(target.0, 1))?;
        } else {
            ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                .emit_into(&output.expr, RegisterRange::new(target.0, 1))?;
        }
    }
    Ok(())
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
            tie_breaker,
            grouping_ties,
        } => {
            let grouping_tie_count = grouping_ties.map_or(0, |(grouping, outputs)| {
                grouping
                    .keys
                    .iter()
                    .filter(|key| !grouping_key_is_ordered(key, order_by, outputs))
                    .count()
            });
            let key_width =
                order_by.len() + grouping_tie_count + usize::from(tie_breaker.is_some());
            let row_start = program.alloc_registers(key_width + result.width);
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
            let mut next_key = row_start + order_by.len();
            if let Some((grouping, outputs)) = grouping_ties {
                for key in &grouping.keys {
                    if grouping_key_is_ordered(key, order_by, outputs) {
                        continue;
                    }
                    let mut subqueries = QuerySubqueryEmitter { plan, ctes };
                    ExpressionEmitter::with_subqueries(program, bindings, &mut subqueries)
                        .emit_into(key, RegisterRange::new(next_key, 1))?;
                    next_key += 1;
                }
            }
            if tie_breaker.is_some() {
                program.emit_insn(Insn::Sequence {
                    cursor_id,
                    target_reg: next_key,
                });
            }
            let result_start = row_start + key_width;
            if result.width > 0 {
                program.emit_insn(Insn::Copy {
                    src_reg: result.first.0,
                    dst_reg: result_start,
                    extra_amount: result.width - 1,
                });
            }
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(row_start),
                count: to_u32(key_width + result.width),
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

fn grouping_key_matches_order(
    key: &Expr,
    order: &Expr,
    outputs: &[crate::translate::semantic::hir::Output],
) -> bool {
    let order = match order {
        Expr::Output(id) => outputs
            .iter()
            .find(|output| output.id == *id)
            .map_or(order, |output| &output.expr),
        _ => order,
    };
    crate::translate::semantic::schema_expr::equivalent(key, order)
}

fn grouping_key_is_ordered(
    key: &Expr,
    order_by: &[OrderTerm],
    outputs: &[crate::translate::semantic::hir::Output],
) -> bool {
    order_by
        .iter()
        .any(|term| grouping_key_matches_order(key, &term.expr, outputs))
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
    let first_block = query
        .blocks
        .first()
        .ok_or(PhysicalQueryError::Invalid("query has no physical blocks"))?;
    let grouping_ties = match &first_block.hir.body {
        QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } if !grouping.keys.is_empty()
            && query.hir.order_by.iter().all(|term| {
                grouping
                    .keys
                    .iter()
                    .any(|key| grouping_key_matches_order(key, &term.expr, first_block.outputs))
            }) =>
        {
            Some((grouping, first_block.outputs))
        }
        _ => None,
    };
    let cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    let tie_breaker = grouping_ties
        .is_none()
        .then(|| query.hir.order_by.last().map(|term| term.order))
        .flatten();
    let mut order_collations_nulls = query
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
        .try_collect::<crate::alloc::Vec<_>>()?;
    if let Some(order) = tie_breaker {
        order_collations_nulls.try_push((order, Some(CollationSeq::Binary), None))?;
    }
    if let Some((grouping, outputs)) = grouping_ties {
        order_collations_nulls.try_extend(grouping.keys.iter().enumerate().filter_map(
            |(position, key)| {
                (!grouping_key_is_ordered(key, &query.hir.order_by, outputs)).then(|| {
                    (
                        SortOrder::Asc,
                        grouping.key_collations[position]
                            .as_ref()
                            .map(|collation| *collation.value()),
                        None,
                    )
                })
            },
        ))?;
    }
    let mut comparators = query
        .hir
        .order_by
        .iter()
        .map(|term| sort_comparator(&term.type_fact))
        .try_collect::<crate::alloc::Vec<_>>()?;
    if tie_breaker.is_some() {
        comparators.try_push(None)?;
    }
    if let Some((grouping, outputs)) = grouping_ties {
        comparators.try_extend(grouping.keys.iter().enumerate().filter_map(
            |(position, key)| {
                (!grouping_key_is_ordered(key, &query.hir.order_by, outputs))
                    .then(|| sort_comparator(&grouping.key_type_facts[position]))
            },
        ))?;
    }
    let grouping_tie_count = grouping_ties.map_or(0, |(grouping, outputs)| {
        grouping
            .keys
            .iter()
            .filter(|key| !grouping_key_is_ordered(key, &query.hir.order_by, outputs))
            .count()
    });
    program.emit_insn(Insn::SorterOpen {
        cursor_id,
        columns: query.hir.order_by.len() + grouping_tie_count + usize::from(tie_breaker.is_some()),
        order_collations_nulls,
        comparators,
    });
    Ok(OpenedSorter {
        cursor_id,
        record: RegisterId(program.alloc_register()),
        order_by: &query.hir.order_by,
        first_block: query.hir.first,
        width,
        tie_breaker,
        grouping_ties,
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
    let grouping_tie_count = sorter.grouping_ties.map_or(0, |(grouping, outputs)| {
        grouping
            .keys
            .iter()
            .filter(|key| !grouping_key_is_ordered(key, sorter.order_by, outputs))
            .count()
    });
    let key_width =
        sorter.order_by.len() + grouping_tie_count + usize::from(sorter.tie_breaker.is_some());
    let pseudo = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: key_width + sorter.width,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo,
        content_reg: data,
        num_fields: key_width + sorter.width,
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
            column: key_width + position,
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
        QueryDestination::RecursiveQueue {
            cursor_id,
            index,
            order,
            seen,
        } => {
            let skip_seen = seen.map(|(seen_cursor, seen_index)| {
                let skip = program.allocate_label();
                let record = program.alloc_register();
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(result.first.0),
                    count: to_u32(result.width),
                    dest_reg: to_u32(record),
                    index_name: Some(seen_index.name.clone()),
                    affinity_str: None,
                });
                program.emit_insn(Insn::Found {
                    cursor_id: seen_cursor,
                    target_pc: skip,
                    record_reg: record,
                    num_regs: 0,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: seen_cursor,
                    record_reg: record,
                    unpacked_start: Some(result.first.0),
                    unpacked_count: Some(to_u32(result.width)),
                    flags: IdxInsertFlags::new().no_op_duplicate().use_seek(true),
                });
                skip
            });
            let sort_width = order
                .iter()
                .map(|term| {
                    let default = match term.order {
                        SortOrder::Asc => NullsOrder::First,
                        SortOrder::Desc => NullsOrder::Last,
                    };
                    1 + usize::from(term.nulls.is_some_and(|nulls| nulls != default))
                })
                .sum::<usize>();
            let queue_row = program.alloc_registers(sort_width + 1 + result.width);
            let mut target = queue_row;
            for term in order {
                let default = match term.order {
                    SortOrder::Asc => NullsOrder::First,
                    SortOrder::Desc => NullsOrder::Last,
                };
                if let Some(nulls) = term.nulls.filter(|nulls| *nulls != default) {
                    let ready = program.allocate_label();
                    let nulls_last = nulls == NullsOrder::Last;
                    program.emit_insn(Insn::Integer {
                        value: i64::from(nulls_last),
                        dest: target,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: result.first.0 + term.output,
                        target_pc: ready,
                    });
                    program.emit_insn(Insn::Integer {
                        value: i64::from(!nulls_last),
                        dest: target,
                    });
                    program.preassign_label_to_next_insn(ready);
                    target += 1;
                }
                program.emit_insn(Insn::Copy {
                    src_reg: result.first.0 + term.output,
                    dst_reg: target,
                    extra_amount: 0,
                });
                target += 1;
            }
            program.emit_insn(Insn::Sequence {
                cursor_id,
                target_reg: queue_row + sort_width,
            });
            program.emit_insn(Insn::Copy {
                src_reg: result.first.0,
                dst_reg: queue_row + sort_width + 1,
                extra_amount: result.width - 1,
            });
            let record = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(queue_row),
                count: to_u32(sort_width + 1 + result.width),
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
            if let Some(skip) = skip_seen {
                program.preassign_label_to_next_insn(skip);
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
