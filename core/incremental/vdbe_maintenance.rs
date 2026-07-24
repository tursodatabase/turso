//! VDBE bytecode maintenance for materialized views.
//!
//! This is the execution half of the IVM engine rewrite: the DBSP circuit
//! remains the logical description of a view, but all row-level evaluation is
//! compiled to a VDBE program so that view maintenance shares the exact
//! expression, comparison, affinity, and NULL semantics of regular query
//! execution instead of re-implementing them.
//!
//! A maintenance program has the shape:
//!
//! ```text
//! OpenRead   c_in    (transaction delta, or the base table for population)
//! OpenWrite  c_view  (the view's btree; records are row values + weight)
//! Rewind c_in -> end
//! loop:
//!   w      := delta weight (Column c_in, n) or the literal +1 for population
//!   if NOT WHERE(row)  -> next     (three-valued logic via translate_condition_expr)
//!   rid    := RowId c_in
//!   out[i] := projection expressions (translate_expr)
//!   merge (rid, out, w) into the view btree:
//!     found:      new_w = cur_w + w
//!     not found:  new_w = w
//!     new_w > 0  -> Insert record(out.., new_w) (upsert at rid)
//!     new_w <= 0 -> Delete, when the row existed
//! next: Next c_in -> loop
//! end:  Halt
//! ```
//!
//! Programs are built with [`ProgramBuilder::new_for_subprogram`], so they
//! emit no transaction instructions and their `Halt` does not commit: they
//! always run inside an already-open write transaction — at statement
//! completion (or before an in-statement view read), or inside the CREATE
//! MATERIALIZED VIEW statement for initial population.
//!
//! Delta rows must be applied in capture order: an UPDATE is captured as
//! delete(old image) followed by insert(new image) under the same rowid, and
//! the rowid-keyed weight merge is only correct when the deletion lands
//! first. The [`DeltaCursor`] therefore iterates the captured `Delta`
//! verbatim, without consolidation.

use rustc_hash::{FxHashMap, FxHashSet};
use turso_parser::ast;

use crate::function::{AggFunc, Func};
use crate::incremental::dag;
use crate::incremental::view::{CapturedDelta, ChangeEvent, SpillSegment, TableChangeId};
use crate::io::{Buffer, Completion, File};
use crate::schema::{BTreeTable, Schema};
use crate::sync::Arc;
use crate::translate::collate::{get_collseq_from_expr_with_symbols, CollationSeq};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, translate_condition_expr,
    translate_expr_no_constant_opt, walk_expr_mut, ConditionMetadata, NoConstantOptReason,
    WalkControl,
};
use crate::translate::plan::{
    Aggregate, ColumnUsedMask, IterationDirection, JoinedTable, Operation, Plan, QueryDestination,
    Scan, SelectPlan, TableReferences,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::select::prepare_select_plan;
use crate::turso_assert;
use crate::types::ImmutableRecord;
use crate::util::walk_expr_with_subqueries;
use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::{BranchOffset, PreparedProgram, Program};
use crate::{
    CompletionError, Connection, IOCompletions, IOResult, LimboError, QueryMode, Result, Value,
};
use turso_parser::ast::TableInternalId;

mod stream;
use stream::*;

mod join;
use join::{emit_join_deltas_to_ephemeral, emit_left_join_deltas_to_ephemeral, JoinContract};

mod aggregate;
use aggregate::emit_group_aggregate;

mod set_op;
use set_op::emit_set_op_to_ephemeral;

/// What the maintenance program reads as its input relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaintenanceInput {
    /// The transaction's captured delta for the view's base table:
    /// rows are (rowid, base column values..., weight).
    TransactionDelta,
    /// A full scan of the base table with an implicit weight of +1 per row.
    /// Used for initial population at CREATE MATERIALIZED VIEW time.
    BaseTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MaintenanceProgramCacheKey {
    view_name: String,
    schema_version: u32,
    root_page: i64,
    num_columns: usize,
    input: MaintenanceInput,
}

/// Per-connection prepared maintenance programs, invalidated by the schema
/// cookie and normal prepared-statement context generation.
pub(crate) struct MaintenanceProgramCache {
    entries: crate::sync::RwLock<FxHashMap<MaintenanceProgramCacheKey, Arc<PreparedProgram>>>,
}

impl MaintenanceProgramCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: crate::sync::RwLock::new(FxHashMap::default()),
        }
    }

    fn get(
        &self,
        key: &MaintenanceProgramCacheKey,
        connection: &Connection,
    ) -> Option<Arc<PreparedProgram>> {
        self.entries
            .read()
            .get(key)
            .filter(|program| program.is_compatible_with(connection))
            .cloned()
    }

    fn insert(&self, key: MaintenanceProgramCacheKey, program: Arc<PreparedProgram>) {
        let mut entries = self.entries.write();
        entries.retain(|existing, _| existing.schema_version == key.schema_version);
        entries.insert(key, program);
    }
}

/// A delta-producing relation consumed by a maintenance operator.
///
/// This is derived exclusively from DAG edges. Emitters must not recover their
/// input by inspecting the original SELECT: doing so makes the DAG descriptive
/// rather than executable and reintroduces per-shape composition.
#[derive(Debug, Clone)]
enum DeltaSource {
    BaseTable {
        table: Arc<BTreeTable>,
        /// Physical multiplicity column when this scan reads another
        /// materialized view. Transaction-delta cursors always expose their
        /// signed weight immediately after the logical columns.
        stored_weight_column: Option<usize>,
    },
    /// A z-set stream emitted by an upstream operator in this program.
    Ephemeral(EphemeralDelta),
}

impl DeltaSource {
    fn identity(&self) -> DeltaIdentity {
        match self {
            Self::BaseTable { .. } => DeltaIdentity::BindingRowids(1),
            Self::Ephemeral(channel) => channel.identity,
        }
    }

    fn binding_rowids(&self) -> Arc<[bool]> {
        match self {
            Self::BaseTable { table, .. } => vec![table.has_rowid].into(),
            Self::Ephemeral(channel) => channel
                .binding_rowid_columns
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>()
                .into(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize(
        &self,
        program: &mut ProgramBuilder,
        view_name: &str,
        source_node: dag::NodeId,
        source_contract: &NodeOutputContract,
        input: MaintenanceInput,
    ) -> Result<EphemeralDelta> {
        let Self::BaseTable {
            table,
            stored_weight_column,
            ..
        } = self
        else {
            let Self::Ephemeral(channel) = self else {
                unreachable!()
            };
            return Ok(channel.clone());
        };
        let channel = open_ephemeral_delta(
            program,
            &format!("{view_name}_scan_delta_{source_node}"),
            source_contract.schema.as_ref().clone(),
            source_contract.emitted_identity,
            source_contract.binding_rowids.clone(),
            false,
        );
        emit_base_scan_delta(
            program,
            view_name,
            table,
            *stored_weight_column,
            input,
            &channel,
        )?;
        Ok(channel)
    }
}

/// The physical result of compiling one DAG node.
///
/// `delta` is the node's transaction change stream. `arrangement` is the
/// maintained integral downstream stateful operators may probe. Linear nodes
/// deliberately have no arrangement: producing one requires an explicit
/// materialization operator rather than silently reopening an ancestor table.
#[derive(Debug, Clone)]
struct NodeOutput {
    delta: DeltaSource,
    arrangement: Option<ArrangementHandle>,
}

/// A [`JoinedTable`] scan entry for synthesized maintenance-program bindings.
/// Expressions are already planner-bound, so no SQL name resolution or
/// USING/NATURAL metadata is reconstructed here.
fn make_joined_table(
    table: &Arc<BTreeTable>,
    identifier: &str,
    id: TableInternalId,
) -> JoinedTable {
    JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        table: crate::schema::Table::BTree(table.clone()),
        identifier: identifier.to_string(),
        internal_id: id,
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: 0,
        indexed: None,
    }
}

type BindingRemap = FxHashMap<TableInternalId, TableInternalId>;

fn stream_table_references(
    program: &mut ProgramBuilder,
    schema: &dag::StreamSchema,
) -> (TableReferences, BindingRemap) {
    let mut remap = FxHashMap::default();
    let tables = schema
        .bindings
        .iter()
        .map(|binding| {
            let phase_id = program.table_reference_counter.next();
            let previous = remap.insert(binding.logical_id, phase_id);
            turso_assert!(
                previous.is_none(),
                "a stream schema must expose each logical binding exactly once"
            );
            make_joined_table(&binding.table, &binding.identifier, phase_id)
        })
        .collect();
    (TableReferences::new(tables, vec![]), remap)
}

/// Retarget a planner-bound expression to one emitter phase's cursor
/// bindings. This is a mechanical id substitution: identifier spelling,
/// DQS, aliases, and rowid-name resolution never run a second time.
fn remap_bound_expr(expr: &ast::Expr, remap: &BindingRemap) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    walk_expr_mut(&mut bound, &mut |node| {
        let logical_id = match node {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => Some(table),
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                return Err(LimboError::InternalError(
                    "maintenance DAG contains an unresolved identifier".to_string(),
                ));
            }
            _ => None,
        };
        if let Some(logical_id) = logical_id {
            *logical_id = *remap.get(logical_id).ok_or_else(|| {
                LimboError::InternalError(
                    "maintenance expression references a binding outside its declared input"
                        .to_string(),
                )
            })?;
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(bound)
}

fn seed_ephemeral_stream_cache<'a>(
    program: &mut ProgramBuilder,
    channel: &EphemeralDelta,
    remap: &BindingRemap,
    resolver: &mut Resolver<'a>,
) -> Result<()> {
    resolver.enable_expr_to_reg_cache();
    for (column, stream_column) in channel.schema.columns.iter().enumerate() {
        let Some(expr) = &stream_column.expr else {
            continue;
        };
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: channel.value_start + column,
            dest: value_reg,
            default: None,
        });
        let bound = remap_bound_expr(expr, remap)?;
        resolver.cache_expr_reg(std::borrow::Cow::Owned(bound), value_reg, false, None);
    }
    turso_assert!(
        channel.binding_rowid_columns.len() == channel.schema.bindings.len(),
        "rowid provenance must have one entry per stream binding",
        {
            "provenance_count": channel.binding_rowid_columns.len(),
            "binding_count": channel.schema.bindings.len()
        }
    );
    for (binding_rowid_column, binding) in channel
        .binding_rowid_columns
        .iter()
        .zip(channel.schema.bindings.iter())
    {
        let Some(binding_rowid_column) = binding_rowid_column else {
            continue;
        };
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: *binding_rowid_column,
            dest: value_reg,
            default: None,
        });
        let phase_id = *remap.get(&binding.logical_id).ok_or_else(|| {
            LimboError::InternalError(
                "rowid stream identity has no phase-local binding".to_string(),
            )
        })?;
        let expr = ast::Expr::RowId {
            database: None,
            table: phase_id,
        };
        resolver.cache_expr_reg(std::borrow::Cow::Owned(expr), value_reg, false, None);
    }
    Ok(())
}

#[derive(Debug)]
struct PendingDeltaRead {
    completion: Completion,
    buffer: Arc<Buffer>,
}

/// Read-only cursor over a transaction's captured changes for one base table,
/// in capture order. Spill segments are loaded asynchronously one bounded
/// batch at a time, followed by the in-memory tail. Exposes base columns at
/// their table positions and the weight as one extra trailing column.
pub struct DeltaCursor {
    file: Option<Arc<dyn File>>,
    spill_segments: Vec<SpillSegment>,
    table_id: Option<TableChangeId>,
    next_segment: usize,
    pending_read: Option<PendingDeltaRead>,
    tail: Arc<Vec<ChangeEvent>>,
    tail_positions: Vec<usize>,
    tail_loaded: bool,
    reading_tail: bool,
    /// Current spill batch, or the in-memory tail.
    rows: Vec<(i64, Vec<Value>, isize)>,
    /// Number of base-table columns; the weight is column `num_columns`.
    num_columns: usize,
    /// Position of the current row; `rows.len()` means exhausted.
    pos: usize,
    /// False until `rewind` has been called.
    positioned: bool,
}

impl DeltaCursor {
    pub(crate) fn new(delta: CapturedDelta, num_columns: usize) -> Self {
        let tail_positions = delta
            .tail
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                (Some(&event.table_id) == delta.table_id.as_ref()).then_some(index)
            })
            .collect();
        Self {
            file: delta.file,
            spill_segments: delta.spilled,
            table_id: delta.table_id,
            next_segment: 0,
            pending_read: None,
            tail: delta.tail,
            tail_positions,
            tail_loaded: false,
            reading_tail: false,
            rows: Vec::new(),
            num_columns,
            pos: 0,
            positioned: false,
        }
    }

    pub fn empty(num_columns: usize) -> Self {
        Self::new(CapturedDelta::empty(), num_columns)
    }

    /// Position at the first row. Returns false when the delta is empty.
    pub fn rewind(&mut self) -> Result<IOResult<bool>> {
        if !self.positioned || self.pending_read.is_none() {
            self.next_segment = 0;
            self.pending_read = None;
            self.tail_loaded = false;
            self.reading_tail = false;
            self.rows.clear();
        }
        self.pos = 0;
        self.positioned = true;
        self.load_next_batch()
    }

    /// Advance to the next row. Returns false when exhausted.
    pub fn next(&mut self) -> Result<IOResult<bool>> {
        turso_assert!(self.positioned, "DeltaCursor::next before rewind");
        let batch_len = if self.reading_tail {
            self.tail_positions.len()
        } else {
            self.rows.len()
        };
        if self.pos < batch_len {
            self.pos += 1;
        }
        if self.pos < batch_len {
            return Ok(IOResult::Done(true));
        }
        self.load_next_batch()
    }

    fn load_next_batch(&mut self) -> Result<IOResult<bool>> {
        loop {
            if let Some(pending) = self.pending_read.take() {
                if !pending.completion.finished() {
                    let completion = pending.completion.clone();
                    self.pending_read = Some(pending);
                    return Ok(IOResult::IO(IOCompletions::Single(completion)));
                }
                if let Some(error) = pending.completion.get_error() {
                    return Err(LimboError::CompletionError(error));
                }
                let segment = &self.spill_segments[self.next_segment];
                self.rows = parse_spilled_delta(
                    pending.buffer.as_slice(),
                    segment,
                    self.table_id.as_ref(),
                )?;
                self.reading_tail = false;
                self.next_segment += 1;
                self.pos = 0;
                if !self.rows.is_empty() {
                    return Ok(IOResult::Done(true));
                }
                continue;
            }

            if self.next_segment < self.spill_segments.len() {
                let segment = &self.spill_segments[self.next_segment];
                if segment.rows == 0
                    || self
                        .table_id
                        .as_ref()
                        .is_none_or(|table_id| !segment.table_ids.contains(table_id))
                {
                    self.next_segment += 1;
                    continue;
                }
                let file = self.file.as_ref().ok_or_else(|| {
                    LimboError::InternalError("spilled view delta has no backing file".to_string())
                })?;
                let buffer = Arc::new(Buffer::new_temporary(segment.len));
                let expected = segment.len;
                let completion = Completion::new_read(buffer.clone(), move |result| {
                    let Ok((_buffer, bytes_read)) = result else {
                        return None;
                    };
                    if bytes_read as usize != expected {
                        return Some(CompletionError::ShortRead {
                            page_idx: 0,
                            expected,
                            actual: bytes_read as usize,
                        });
                    }
                    None
                });
                let completion = file.pread(segment.offset, completion)?;
                self.pending_read = Some(PendingDeltaRead {
                    completion: completion.clone(),
                    buffer,
                });
                if !completion.finished() {
                    return Ok(IOResult::IO(IOCompletions::Single(completion)));
                }
                continue;
            }

            if !self.tail_loaded {
                self.tail_loaded = true;
                self.reading_tail = true;
                self.pos = 0;
                if !self.tail_positions.is_empty() {
                    return Ok(IOResult::Done(true));
                }
            }
            self.reading_tail = false;
            self.rows.clear();
            self.pos = 0;
            return Ok(IOResult::Done(false));
        }
    }

    fn assert_current(&self) {
        turso_assert!(self.positioned, "DeltaCursor read before rewind");
        let len = if self.reading_tail {
            self.tail_positions.len()
        } else {
            self.rows.len()
        };
        turso_assert!(self.pos < len, "DeltaCursor read past end");
    }

    pub fn rowid(&self) -> i64 {
        self.assert_current();
        if self.reading_tail {
            self.tail[self.tail_positions[self.pos]].rowid
        } else {
            self.rows[self.pos].0
        }
    }

    pub fn column(&self, idx: usize) -> Value {
        self.assert_current();
        let (values, weight) = if self.reading_tail {
            let event = &self.tail[self.tail_positions[self.pos]];
            (&event.values, event.weight)
        } else {
            let (_, values, weight) = &self.rows[self.pos];
            (values, *weight)
        };
        if idx == self.num_columns {
            return Value::from_i64(weight as i64);
        }
        let num_columns = self.num_columns;
        turso_assert!(
            idx < num_columns,
            "DeltaCursor column {idx} out of range ({num_columns} columns + weight)"
        );
        turso_assert!(
            values.len() == num_columns,
            "captured row image width mismatch",
            { "actual": values.len(), "expected": num_columns }
        );
        values[idx].clone()
    }
}

fn parse_spilled_delta(
    bytes: &[u8],
    segment: &SpillSegment,
    table_id: Option<&TableChangeId>,
) -> Result<Vec<(i64, Vec<Value>, isize)>> {
    let mut rows = Vec::new();
    let mut offset = 0;
    for _ in 0..segment.rows {
        let size_bytes = bytes.get(offset..offset + 8).ok_or_else(|| {
            LimboError::Corrupt("truncated materialized-view delta frame".to_string())
        })?;
        let payload_len = u64::from_le_bytes(size_bytes.try_into().expect("eight-byte slice"));
        let payload_len = usize::try_from(payload_len).map_err(|_| {
            LimboError::Corrupt("oversized materialized-view delta frame".to_string())
        })?;
        offset += 8;
        let payload = bytes.get(offset..offset + payload_len).ok_or_else(|| {
            LimboError::Corrupt("truncated materialized-view delta record".to_string())
        })?;
        offset += payload_len;
        let mut record = ImmutableRecord::new(payload_len)?;
        record.start_serialization(payload)?;
        let mut values = record.get_values_owned()?;
        if values.len() < 3 {
            return Err(LimboError::Corrupt(
                "materialized-view delta record lacks table identity, rowid, or weight".to_string(),
            ));
        }
        let event_table_id = match values.remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(root_page)) => {
                TableChangeId::RootPage(root_page)
            }
            #[cfg(test)]
            Value::Text(name) => TableChangeId::Name(name.as_str().to_string()),
            _ => {
                return Err(LimboError::Corrupt(
                    "materialized-view delta table identity has an invalid type".to_string(),
                ));
            }
        };
        let rowid = match values.remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(value)) => value,
            _ => {
                return Err(LimboError::Corrupt(
                    "materialized-view delta rowid is not an integer".to_string(),
                ));
            }
        };
        let weight = match values.remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(value)) => value as isize,
            _ => {
                return Err(LimboError::Corrupt(
                    "materialized-view delta weight is not an integer".to_string(),
                ));
            }
        };
        if Some(&event_table_id) == table_id {
            rows.push((rowid, values, weight));
        }
    }
    Ok(rows)
}

/// Whether an aggregate's accumulator must see each distinct argument value
/// exactly once, tracked through the value multiset table. MIN/MAX see each
/// value once by definition, so DISTINCT changes nothing for them — their
/// multiset participation is about extremes, not distinctness.
fn tracks_distinct_values(agg: &Aggregate) -> bool {
    agg.distinctness.is_distinct() && !matches!(agg.func, AggFunc::Min | AggFunc::Max)
}

fn unsupported<T>(what: &str) -> Result<T> {
    Err(LimboError::ParseError(format!(
        "materialized views with {what} are not yet supported",
    )))
}

/// Reject subqueries, bound parameters, and (unless the expression IS a
/// supported aggregate handled by the caller) aggregate function calls.
fn check_scalar_expr(expr: &ast::Expr) -> Result<()> {
    walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                return Err(LimboError::ParseError(
                    "materialized views with subqueries are not yet supported".to_string(),
                ));
            }
            ast::Expr::FunctionCall { name, args, .. } => {
                if matches!(
                    Func::resolve_function(name.as_str(), args.len()),
                    Ok(Some(Func::Agg(_)))
                ) {
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                if matches!(
                    Func::resolve_function(name.as_str(), 0),
                    Ok(Some(Func::Agg(_)))
                ) {
                    return unsupported("expressions over aggregate results");
                }
            }
            ast::Expr::Variable(_) => {
                return Err(LimboError::ParseError(
                    "materialized views with bound parameters are not supported".to_string(),
                ));
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
}

/// Reject collected aggregates the maintenance codegen cannot invert.
///
/// Aggregate recognition and deduplication use the planner's own collector
/// ([`resolve_window_and_aggregate_functions`]); this is only the IVM gate on
/// top of it.
fn validate_supported_aggregate(agg: &Aggregate) -> Result<()> {
    if agg.filter_expr.is_some() {
        return unsupported("FILTER clauses on aggregates");
    }
    match agg.func {
        AggFunc::Count0
        | AggFunc::Count
        | AggFunc::Sum
        | AggFunc::Total
        | AggFunc::Avg
        | AggFunc::Min
        | AggFunc::Max => {}
        // GROUP_CONCAT and friends have no inverse and no multiset-based
        // retraction strategy.
        ref other => return unsupported(&format!("the {} aggregate", other.as_str())),
    }
    if agg.args.len() > 1 {
        return unsupported("multi-argument aggregates");
    }
    for arg in &agg.args {
        check_scalar_expr(arg)?;
    }
    Ok(())
}

/// Validate that a HAVING expression evaluates only group expressions and
/// aggregate results.
///
/// The aggregates themselves were already collected into `aggregates` by the
/// planner's collector; this walk rejects what remains outside them: bare
/// column references (SQLite's arbitrary-row semantics cannot be maintained
/// incrementally), subqueries, and bound parameters.
fn check_group_row_expr(
    expr: &ast::Expr,
    group_exprs: &[ast::Expr],
    aggregates: &[Aggregate],
    bare_column_error: &str,
) -> Result<()> {
    walk_expr_with_subqueries(expr, &mut |e: &ast::Expr| {
        if group_exprs
            .iter()
            .any(|g| crate::util::exprs_are_equivalent(g, e))
            || aggregates
                .iter()
                .any(|a| crate::util::exprs_are_equivalent(&a.original_expr, e))
        {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                Err(LimboError::ParseError(
                    "materialized views with subqueries are not yet supported".to_string(),
                ))
            }
            ast::Expr::Variable(_) => Err(LimboError::ParseError(
                "materialized views with bound parameters are not supported".to_string(),
            )),
            ast::Expr::Id(_)
            | ast::Expr::Qualified(_, _)
            | ast::Expr::DoublyQualified(_, _, _)
            | ast::Expr::Column { .. }
            | ast::Expr::RowId { .. } => unsupported(bare_column_error),
            _ => Ok(WalkControl::Continue),
        }
    })
}

/// Lower the main query planner's bound relational plan into the maintenance
/// DAG. Parser spelling (aliases, stars, USING expansion, ordinal GROUP BY,
/// and compound arity) has already been resolved by this point.
fn build_dag_from_plan(plan: &Plan, resolver: &Resolver) -> Result<dag::MaintenanceDag> {
    let mut builder = dag::DagBuilder::new();
    let root = build_plan_dag_node(&mut builder, plan, resolver)?;
    builder.finish(root)
}

fn build_plan_dag_node(
    builder: &mut dag::DagBuilder,
    plan: &Plan,
    resolver: &Resolver,
) -> Result<dag::NodeId> {
    let root = match plan {
        Plan::Select(select) => build_select_plan_dag(builder, select, resolver)?,
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            let mut inputs = Vec::with_capacity(left.len() + 1);
            for (branch, _) in left {
                inputs.push(build_select_plan_dag(builder, branch, resolver)?);
            }
            inputs.push(build_select_plan_dag(builder, right_most, resolver)?);

            let operators: Vec<_> = left.iter().map(|(_, operator)| *operator).collect();
            let prefix_len = operators
                .iter()
                .rposition(|operator| *operator != ast::CompoundOperator::UnionAll)
                .map_or(0, |last_dedup| last_dedup + 2);
            let arity = right_most.result_columns.len();
            let key_collations = if prefix_len == 0 {
                Vec::new()
            } else {
                let mut folded = vec![None; arity];
                for branch in left
                    .iter()
                    .map(|(branch, _)| branch)
                    .chain(std::iter::once(right_most.as_ref()))
                    .take(prefix_len)
                {
                    for (index, column) in branch.result_columns.iter().enumerate() {
                        if folded[index].is_none() {
                            folded[index] = plan_expr_collation(
                                &column.expr,
                                &branch.table_references,
                                resolver,
                            )?;
                        }
                    }
                }
                folded
                    .into_iter()
                    .map(|collation| collation.unwrap_or(CollationSeq::Binary))
                    .collect()
            };
            builder.push(dag::OpNode::SetOp {
                inputs,
                operators,
                arity,
                prefix_len,
                key_collations,
            })?
        }
        Plan::Delete(_) | Plan::Update(_) => {
            return Err(LimboError::InternalError(
                "materialized view planner produced a DML plan".to_string(),
            ));
        }
    };
    Ok(root)
}

fn synthesized_derived_table(name: &str, columns: Vec<crate::schema::Column>) -> BTreeTable {
    BTreeTable::new(
        0,
        name.to_string(),
        Vec::new(),
        columns,
        crate::schema::BTreeCharacteristics::empty(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    )
}

fn build_select_plan_dag(
    builder: &mut dag::DagBuilder,
    plan: &SelectPlan,
    resolver: &Resolver,
) -> Result<dag::NodeId> {
    if !plan.order_by.is_empty() {
        return unsupported("ORDER BY");
    }
    if plan.limit.is_some() || plan.offset.is_some() {
        return unsupported("LIMIT or OFFSET");
    }
    if !plan.values.is_empty() {
        return unsupported("VALUES");
    }
    if plan.window.is_some() {
        return unsupported("window functions");
    }
    if !plan.non_from_clause_subqueries.is_empty() {
        return unsupported("subqueries");
    }

    let joined_tables = plan.table_references.joined_tables();
    if joined_tables.is_empty() {
        return unsupported("SELECT without a FROM clause");
    }
    for joined in joined_tables {
        if let Some(join) = &joined.join_info {
            match join.join_type {
                crate::translate::plan::JoinType::Inner
                | crate::translate::plan::JoinType::LeftOuter => {}
                crate::translate::plan::JoinType::FullOuter => {
                    // FULL must eventually lower to the arrangement/set-op
                    // composition. Treating it as the existing LEFT flag
                    // loses the right-side padded delta and is incorrect.
                    return unsupported("FULL OUTER joins");
                }
                crate::translate::plan::JoinType::Semi | crate::translate::plan::JoinType::Anti => {
                    return unsupported("semi or anti joins");
                }
            }
        }
    }

    let mut inputs = Vec::with_capacity(joined_tables.len());
    for joined in joined_tables {
        let input = match &joined.table {
            crate::schema::Table::BTree(table) => builder.push(dag::OpNode::Scan {
                table: table.clone(),
                identifier: joined.identifier.clone(),
                logical_id: joined.internal_id,
            })?,
            crate::schema::Table::FromClauseSubquery(subquery) => {
                let input = build_plan_dag_node(builder, &subquery.plan, resolver)?;
                let table = Arc::new(synthesized_derived_table(
                    &subquery.name,
                    subquery.columns.clone(),
                ));
                builder.push(dag::OpNode::Alias {
                    input,
                    table,
                    identifier: joined.identifier.clone(),
                    logical_id: joined.internal_id,
                })?
            }
            crate::schema::Table::Virtual(_) => return unsupported("virtual-table FROM sources"),
        };
        inputs.push(input);
    }

    let binding_positions = joined_tables
        .iter()
        .enumerate()
        .map(|(position, table)| (table.internal_id, position))
        .collect::<FxHashMap<_, _>>();
    let mut on_by_step = vec![Vec::new(); inputs.len()];
    let mut filters_by_step = vec![Vec::new(); inputs.len()];
    for term in &plan.where_clause {
        if let Some(right_binding) = term.from_outer_join {
            let step = *binding_positions.get(&right_binding).ok_or_else(|| {
                LimboError::InternalError(
                    "outer-join predicate references a table outside the FROM clause".to_string(),
                )
            })?;
            if step == 0
                || !joined_tables[step].join_info.as_ref().is_some_and(|join| {
                    join.join_type == crate::translate::plan::JoinType::LeftOuter
                })
            {
                return Err(LimboError::InternalError(
                    "outer-join predicate is not owned by a LEFT JOIN step".to_string(),
                ));
            }
            on_by_step[step].push(term.expr.clone());
        } else {
            let step = max_referenced_binding_position(&term.expr, &binding_positions)?;
            filters_by_step[step].push(term.expr.clone());
        }
    }

    let mut node = inputs[0];
    if let Some(predicate) = combine_predicates(std::mem::take(&mut filters_by_step[0])) {
        node = builder.push(dag::OpNode::Filter {
            input: node,
            predicate,
        })?;
    }
    for step in 1..inputs.len() {
        let kind = match joined_tables[step]
            .join_info
            .as_ref()
            .map(|join| join.join_type)
            .unwrap_or(crate::translate::plan::JoinType::Inner)
        {
            crate::translate::plan::JoinType::Inner => dag::JoinKind::Inner,
            crate::translate::plan::JoinType::LeftOuter => dag::JoinKind::LeftOuter,
            _ => unreachable!("unsupported join kinds were rejected before DAG lowering"),
        };
        node = builder.push(dag::OpNode::Join {
            inputs: [node, inputs[step]],
            on: std::mem::take(&mut on_by_step[step]),
            kind,
        })?;
        if let Some(predicate) = combine_predicates(std::mem::take(&mut filters_by_step[step])) {
            node = builder.push(dag::OpNode::Filter {
                input: node,
                predicate,
            })?;
        }
    }

    let is_distinct = plan.distinctness.is_distinct();
    if is_distinct && (plan.group_by.is_some() || !plan.aggregates.is_empty()) {
        return unsupported("DISTINCT over aggregates");
    }
    let is_aggregate = plan.group_by.is_some() || !plan.aggregates.is_empty() || is_distinct;
    if is_aggregate {
        let (group_exprs, group_collations, scalar) = if is_distinct {
            let mut exprs = Vec::with_capacity(plan.result_columns.len());
            let mut collations = Vec::with_capacity(plan.result_columns.len());
            for column in &plan.result_columns {
                collations.push(
                    plan_expr_collation(&column.expr, &plan.table_references, resolver)?
                        .unwrap_or(CollationSeq::Binary),
                );
                exprs.push(column.expr.clone());
            }
            (exprs, collations, false)
        } else if plan
            .group_by
            .as_ref()
            .is_some_and(|group_by| !group_by.exprs.is_empty())
        {
            let group_by = plan.group_by.as_ref().unwrap();
            let mut exprs = Vec::with_capacity(group_by.exprs.len());
            let mut collations = Vec::with_capacity(group_by.exprs.len());
            for expr in &group_by.exprs {
                collations.push(
                    plan_expr_collation(expr, &plan.table_references, resolver)?
                        .unwrap_or(CollationSeq::Binary),
                );
                exprs.push(expr.clone());
            }
            (exprs, collations, false)
        } else {
            (
                vec![ast::Expr::Literal(ast::Literal::Numeric("0".to_string()))],
                vec![CollationSeq::Binary],
                true,
            )
        };

        let mut multiset_collation = None;
        for aggregate in plan
            .aggregates
            .iter()
            .filter(|aggregate| uses_multiset(aggregate))
        {
            let collation = aggregate
                .args
                .first()
                .map(|argument| {
                    plan_expr_collation(argument, &plan.table_references, resolver)
                        .map(|collation| collation.unwrap_or(CollationSeq::Binary))
                })
                .transpose()?
                .unwrap_or(CollationSeq::Binary);
            if multiset_collation.is_some_and(|existing| existing != collation) {
                return unsupported(
                    "different collations across MIN/MAX or DISTINCT aggregate arguments",
                );
            }
            multiset_collation = Some(collation);
        }
        let multiset_collation = multiset_collation.unwrap_or(CollationSeq::Binary);
        let mut aggregates = hidden_count_aggregate(resolver)?;
        for aggregate in &plan.aggregates {
            let mut normalized = aggregate.clone();
            normalized.fraction_reg = None;
            if !aggregates.iter().any(|existing| {
                crate::util::exprs_are_equivalent(
                    &existing.original_expr,
                    &normalized.original_expr,
                )
            }) {
                aggregates.push(normalized);
            }
        }
        node = builder.push(dag::OpNode::Aggregate {
            input: node,
            group_exprs,
            group_collations,
            aggregates,
            multiset_collation,
            scalar,
        })?;

        if let Some(having) = plan
            .group_by
            .as_ref()
            .and_then(|group_by| group_by.having.as_ref())
        {
            let normalized = having.to_vec();
            if let Some(predicate) = combine_predicates(normalized) {
                node = builder.push(dag::OpNode::Filter {
                    input: node,
                    predicate,
                })?;
            }
        }
    }

    let projections = plan
        .result_columns
        .iter()
        .map(|column| Ok((column.expr.clone(), column.alias.clone())))
        .collect::<Result<Vec<_>>>()?;
    builder.push(dag::OpNode::Project {
        input: node,
        projections,
    })
}

fn combine_predicates(mut predicates: Vec<ast::Expr>) -> Option<ast::Expr> {
    let first = predicates.pop()?;
    Some(predicates.into_iter().fold(first, |right, left| {
        ast::Expr::Binary(Box::new(left), ast::Operator::And, Box::new(right))
    }))
}

fn max_referenced_binding_position(
    expr: &ast::Expr,
    binding_positions: &FxHashMap<TableInternalId, usize>,
) -> Result<usize> {
    let mut max_position = 0;
    walk_expr_with_subqueries(expr, &mut |node| {
        let binding = match node {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => Some(*table),
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                return Err(LimboError::InternalError(
                    "maintenance predicate contains an unresolved identifier".to_string(),
                ));
            }
            _ => None,
        };
        if let Some(binding) = binding {
            let position = binding_positions.get(&binding).ok_or_else(|| {
                LimboError::InternalError(
                    "maintenance predicate references a binding outside its FROM clause"
                        .to_string(),
                )
            })?;
            max_position = max_position.max(*position);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(max_position)
}

fn hidden_count_aggregate(resolver: &Resolver) -> Result<Vec<Aggregate>> {
    let count_star = ast::Expr::FunctionCallStar {
        name: ast::Name::exact("count".to_string()),
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    };
    let mut aggregates = Vec::new();
    resolve_window_and_aggregate_functions(&count_star, resolver, &mut aggregates, None, &mut [])?;
    Ok(aggregates)
}

fn plan_expr_collation(
    expr: &ast::Expr,
    tables: &TableReferences,
    resolver: &Resolver,
) -> Result<Option<CollationSeq>> {
    let collation = get_collseq_from_expr_with_symbols(expr, tables, Some(resolver.symbol_table))?;
    if collation.is_some_and(|collation| collation.is_custom()) {
        return unsupported("custom collations on grouping or dedup keys");
    }
    Ok(collation.map(|collation| match collation {
        CollationSeq::Unset => CollationSeq::Binary,
        other => other,
    }))
}

/// A hidden state/multiset table a view needs, fully named with its CREATE
/// SQL. The single source of truth for the DDL the CREATE program emits.
#[derive(Debug, Clone)]
pub struct HiddenTableDef {
    pub table_name: String,
    pub create_sql: String,
}

/// The complete physical output contract of one DAG node.
///
/// Planning derives this once from the logical node and its already-planned
/// inputs. Bytecode emission consumes it verbatim; emitters must not infer an
/// identity from an operator shape or reconstruct a schema from physical
/// inputs.
#[derive(Debug, Clone)]
struct NodeOutputContract {
    schema: Arc<dag::StreamSchema>,
    /// Whether each logical binding's SQL rowid remains available after this
    /// operator. Transport identity is planned separately below.
    binding_rowids: Arc<[bool]>,
    /// Identity emitted by the relational operator before an optional output
    /// arrangement.
    emitted_identity: DeltaIdentity,
    /// Identity published on the node's DAG edge. An explicit arrangement
    /// replaces the producer identity with its own stable rowid.
    published_identity: DeltaIdentity,
}

/// Persistent storage and output contract assigned to one DAG node.
///
/// `state_table` is the operator's primary integral. `auxiliary_table` is the
/// aggregate value multiset, the LEFT-join unmatched-row bookkeeping, or the
/// trailing-UNION-ALL identity multiset associated with that same node.
/// `arrangement_table` is the node's explicitly materialized output integral
/// when a downstream join or terminal sink needs a one-rowid identity and the
/// operator has no native arrangement.
#[derive(Debug, Clone)]
struct OperatorStateDef {
    node_id: dag::NodeId,
    output: NodeOutputContract,
    state_table: Option<HiddenTableDef>,
    auxiliary_table: Option<HiddenTableDef>,
    arrangement_table: Option<HiddenTableDef>,
}

impl OperatorStateDef {
    fn exposes_arrangement(&self, node: &dag::OpNode) -> bool {
        node_has_native_arrangement(node) || self.arrangement_table.is_some()
    }

    fn state_table_name(&self) -> Result<&str> {
        self.state_table
            .as_ref()
            .map(|table| table.table_name.as_str())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG node {} has no primary state table",
                    self.node_id
                ))
            })
    }

    fn auxiliary_table_name(&self) -> Result<&str> {
        self.auxiliary_table
            .as_ref()
            .map(|table| table.table_name.as_str())
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG node {} has no auxiliary state table",
                    self.node_id
                ))
            })
    }
}

/// Node-indexed inventory shared by CREATE and maintenance code generation.
///
/// The vector has exactly one entry per DAG node, so codegen cannot recover
/// storage by reclassifying a branch or reconstructing a name.
#[derive(Debug, Clone)]
pub struct OperatorStateCatalog {
    nodes: Vec<OperatorStateDef>,
}

impl OperatorStateCatalog {
    fn for_node(&self, node_id: dag::NodeId) -> Result<&OperatorStateDef> {
        let state = self.nodes.get(node_id).ok_or_else(|| {
            LimboError::InternalError(format!(
                "maintenance state catalog has no DAG node {node_id}"
            ))
        })?;
        if state.node_id != node_id {
            return Err(LimboError::InternalError(format!(
                "maintenance state catalog entry {} is stored at node {node_id}",
                state.node_id
            )));
        }
        Ok(state)
    }

    fn output_for_node(&self, node_id: dag::NodeId) -> Result<&NodeOutputContract> {
        Ok(&self.for_node(node_id)?.output)
    }

    pub fn hidden_tables(&self) -> impl Iterator<Item = &HiddenTableDef> {
        self.nodes.iter().flat_map(|state| {
            state
                .state_table
                .iter()
                .chain(state.auxiliary_table.iter())
                .chain(state.arrangement_table.iter())
        })
    }
}

/// The single CREATE/compile-time description of an incremental view.
///
/// Validation builds the DAG once, storage is derived from that DAG, and
/// codegen consumes the same representation. Keeping these together prevents
/// the accepted-query set, hidden schema, and bytecode dispatcher from
/// evolving as three independent shape classifiers.
pub struct MaintenancePlan {
    pub dag: dag::MaintenanceDag,
    pub operator_states: OperatorStateCatalog,
    pub output_arity: usize,
}

/// Validate a view and build its executable maintenance plan.
pub fn plan_view(
    view_name: &str,
    select: &ast::Select,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<MaintenancePlan> {
    let mut planner_program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts::new(8, 32, 8),
    );
    let relational_plan = prepare_select_plan(
        select.clone(),
        resolver,
        &mut planner_program,
        &[],
        QueryDestination::ResultRows,
        connection,
    )?;
    let dag = build_dag_from_plan(&relational_plan, resolver)?;
    let operator_states = plan_operator_states(view_name, &dag, resolver)?;
    let output_arity = dag.root_schema().len();
    Ok(MaintenancePlan {
        dag,
        operator_states,
        output_arity,
    })
}

/// Validate one fully planned node.
///
/// Planning visits nodes in topological order and calls this immediately
/// after assigning the node's identity, state, and arrangement contract.
/// Validation therefore cannot accept a shape that has no physical contract,
/// and it may inspect only contracts already established for declared inputs.
fn validate_planned_node(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    operator_states: &OperatorStateCatalog,
    resolver: &Resolver,
) -> Result<()> {
    let node = &dag.nodes[node_id];
    validate_output_schema(dag, node_id)?;
    if let dag::OpNode::Scan { table, .. } = node {
        if !table.has_rowid {
            return unsupported("WITHOUT ROWID base tables");
        }
        if table.has_virtual_columns {
            return unsupported("base tables with virtual generated columns");
        }
    }
    let check_expr = |expr: &ast::Expr| -> Result<()> {
        if expr_contains_nondeterministic_scalar_function(expr, resolver)? {
            return unsupported("non-deterministic expressions");
        }
        Ok(())
    };
    match node {
        dag::OpNode::Filter { predicate, .. } => check_expr(predicate)?,
        dag::OpNode::Project { projections, .. } => {
            for (expr, _) in projections {
                check_expr(expr)?;
            }
        }
        dag::OpNode::Join { on, .. } => {
            for expr in on {
                check_expr(expr)?;
            }
        }
        dag::OpNode::Aggregate {
            group_exprs,
            aggregates,
            ..
        } => {
            for expr in group_exprs {
                check_expr(expr)?;
            }
            for aggregate in aggregates {
                for expr in &aggregate.args {
                    check_expr(expr)?;
                }
                if let Some(expr) = &aggregate.filter_expr {
                    check_expr(expr)?;
                }
            }
        }
        dag::OpNode::Scan { .. } | dag::OpNode::Alias { .. } | dag::OpNode::SetOp { .. } => {}
    }
    validate_composable_delta_node(dag, node_id, operator_states)
}

fn validate_terminal_delta_identity(
    operator_states: &OperatorStateCatalog,
    node_id: dag::NodeId,
) -> Result<()> {
    match operator_states.for_node(node_id)?.output.published_identity {
        DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid => Ok(()),
        DeltaIdentity::BindingRowids(_) | DeltaIdentity::OperatorKey(_) => unsupported(
            "a root delta with composite identity without an explicit materialized sink",
        ),
    }
}

fn validate_output_schema(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> Result<()> {
    let node = &dag.nodes[node_id];
    let schema = dag.output_schema(node_id);
    let valid = match node {
        dag::OpNode::Scan {
            table,
            identifier,
            logical_id,
        } => {
            schema.len() == table.columns().len()
                && schema.bindings.len() == 1
                && schema.bindings[0].table.root_page == table.root_page
                && schema.bindings[0].identifier == *identifier
                && schema.bindings[0].logical_id == *logical_id
                && schema.columns.iter().enumerate().zip(table.columns()).all(
                    |((column_index, stream), column)| {
                        stream.name == column.name
                            && match (&stream.expr, &column.name) {
                                (
                                    Some(ast::Expr::Column {
                                        table: expr_table,
                                        column: expr_column,
                                        is_rowid_alias,
                                        ..
                                    }),
                                    Some(_),
                                ) => {
                                    *expr_table == *logical_id
                                        && *expr_column == column_index
                                        && *is_rowid_alias == column.is_rowid_alias()
                                }
                                (None, None) => true,
                                _ => false,
                            }
                    },
                )
        }
        dag::OpNode::Filter { input, .. } => {
            schema.columns.len() == dag.output_schema(*input).columns.len()
                && stream_bindings_match(schema, dag.output_schema(*input))
                && schema
                    .columns
                    .iter()
                    .zip(&dag.output_schema(*input).columns)
                    .all(|(left, right)| {
                        left.name == right.name
                            && match (&left.expr, &right.expr) {
                                (Some(left), Some(right)) => {
                                    crate::util::exprs_are_equivalent(left, right)
                                }
                                (None, None) => true,
                                _ => false,
                            }
                    })
        }
        dag::OpNode::Project { input, projections } => {
            schema.len() == projections.len()
                && stream_bindings_match(schema, dag.output_schema(*input))
                && schema
                    .columns
                    .iter()
                    .zip(projections)
                    .all(|(stream, (expr, name))| {
                        stream.name == *name
                            && stream.expr.as_ref().is_some_and(|stream_expr| {
                                crate::util::exprs_are_equivalent(stream_expr, expr)
                            })
                    })
        }
        dag::OpNode::Alias {
            input,
            table,
            identifier,
            logical_id,
        } => {
            schema.len() == dag.output_schema(*input).len()
                && schema.len() == table.columns().len()
                && schema.bindings.len() == 1
                && Arc::ptr_eq(&schema.bindings[0].table, table)
                && schema.bindings[0].identifier == *identifier
                && schema.bindings[0].logical_id == *logical_id
                && schema.columns.iter().enumerate().zip(table.columns()).all(
                    |((column_index, stream), column)| {
                        stream.name == column.name
                            && match (&stream.expr, &column.name) {
                                (
                                    Some(ast::Expr::Column {
                                        table: expr_table,
                                        column: expr_column,
                                        is_rowid_alias,
                                        ..
                                    }),
                                    Some(_),
                                ) => {
                                    *expr_table == *logical_id
                                        && *expr_column == column_index
                                        && *is_rowid_alias == column.is_rowid_alias()
                                }
                                (None, None) => true,
                                _ => false,
                            }
                    },
                )
        }
        dag::OpNode::Join { inputs, .. } => {
            schema.len()
                == inputs
                    .iter()
                    .map(|input| dag.output_schema(*input).len())
                    .sum::<usize>()
                && schema.bindings.len()
                    == inputs
                        .iter()
                        .map(|input| dag.output_schema(*input).bindings.len())
                        .sum::<usize>()
        }
        dag::OpNode::Aggregate {
            input,
            group_exprs,
            aggregates,
            ..
        } => {
            schema.len() == group_exprs.len() + aggregates.len()
                && stream_bindings_match(schema, dag.output_schema(*input))
        }
        dag::OpNode::SetOp { arity, .. } => schema.len() == *arity,
    };
    if !valid {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} has an inconsistent output schema"
        )));
    }
    Ok(())
}

fn stream_bindings_match(left: &dag::StreamSchema, right: &dag::StreamSchema) -> bool {
    left.bindings.len() == right.bindings.len()
        && left
            .bindings
            .iter()
            .zip(&right.bindings)
            .all(|(left, right)| {
                left.table.root_page == right.table.root_page
                    && left.identifier == right.identifier
                    && left.logical_id == right.logical_id
            })
}

fn validate_composable_delta_node(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    operator_states: &OperatorStateCatalog,
) -> Result<()> {
    match &dag.nodes[node_id] {
        dag::OpNode::Scan { .. } => Ok(()),
        dag::OpNode::Filter { input, predicate } => {
            if let dag::OpNode::Aggregate {
                group_exprs,
                aggregates,
                ..
            } = &dag.nodes[*input]
            {
                check_group_row_expr(
                    predicate,
                    group_exprs,
                    aggregates,
                    "HAVING referencing columns outside GROUP BY",
                )?;
            } else {
                check_scalar_expr(predicate)?;
            }
            Ok(())
        }
        dag::OpNode::Project { input, projections } => {
            let (aggregate, output_filter) = match &dag.nodes[*input] {
                dag::OpNode::Aggregate { .. } => (Some(*input), None),
                dag::OpNode::Filter {
                    input: aggregate,
                    predicate,
                } if matches!(dag.nodes[*aggregate], dag::OpNode::Aggregate { .. }) => {
                    (Some(*aggregate), Some(predicate))
                }
                _ => (None, None),
            };
            if let Some(aggregate) = aggregate {
                let dag::OpNode::Aggregate {
                    group_exprs,
                    aggregates,
                    ..
                } = &dag.nodes[aggregate]
                else {
                    unreachable!()
                };
                if let Some(predicate) = output_filter {
                    check_group_row_expr(
                        predicate,
                        group_exprs,
                        aggregates,
                        "HAVING referencing columns outside GROUP BY",
                    )?;
                }
                for (expr, _) in projections {
                    check_group_row_expr(
                        expr,
                        group_exprs,
                        aggregates,
                        "non-aggregate result columns not in GROUP BY",
                    )?;
                }
                return Ok(());
            }
            for (expr, _) in projections {
                check_scalar_expr(expr)?;
            }
            Ok(())
        }
        dag::OpNode::Alias { .. } => Ok(()),
        dag::OpNode::Join { inputs, on, .. } => {
            validate_join_node(dag, inputs, on, operator_states)?;
            Ok(())
        }
        dag::OpNode::SetOp {
            inputs,
            operators,
            arity,
            prefix_len,
            key_collations,
        } => {
            if inputs.len() != operators.len() + 1
                || *prefix_len > inputs.len()
                || *prefix_len == 1
                || inputs
                    .iter()
                    .any(|input| dag.output_schema(*input).len() != *arity)
            {
                return Err(LimboError::InternalError(
                    "composable set-op has inconsistent physical metadata".to_string(),
                ));
            }
            if (*prefix_len == 0 && !key_collations.is_empty())
                || (*prefix_len > 0 && key_collations.len() != *arity)
            {
                return Err(LimboError::InternalError(
                    "composable set-op has an invalid collation layout".to_string(),
                ));
            }
            if key_collations.iter().any(|collation| collation.is_custom()) {
                return unsupported("custom collations on grouping or dedup keys");
            }
            if *prefix_len == 0
                && operators
                    .iter()
                    .any(|operator| *operator != ast::CompoundOperator::UnionAll)
            {
                return Err(LimboError::InternalError(
                    "pure UNION ALL DAG has a non-UNION ALL operator".to_string(),
                ));
            }
            Ok(())
        }
        dag::OpNode::Aggregate {
            group_exprs,
            group_collations,
            aggregates,
            ..
        } => {
            if group_exprs.len() != group_collations.len() {
                return Err(LimboError::InternalError(
                    "aggregate DAG has inconsistent group collation layout".to_string(),
                ));
            }
            if group_collations
                .iter()
                .any(|collation| collation.is_custom())
            {
                return unsupported("custom collations on grouping or dedup keys");
            }
            for expr in group_exprs {
                check_scalar_expr(expr)?;
            }
            for aggregate in aggregates {
                validate_supported_aggregate(aggregate)?;
            }
            Ok(())
        }
    }
}

fn validate_join_node(
    dag: &dag::MaintenanceDag,
    inputs: &[dag::NodeId; 2],
    predicates: &[ast::Expr],
    operator_states: &OperatorStateCatalog,
) -> Result<()> {
    for &input in inputs {
        if !operator_states
            .for_node(input)?
            .exposes_arrangement(&dag.nodes[input])
        {
            return Err(LimboError::InternalError(format!(
                "join input DAG node {input} has no declared arrangement"
            )));
        }
    }
    for predicate in predicates {
        check_scalar_expr(predicate)?;
    }
    Ok(())
}

fn declared_emitted_identity(
    node: &dag::OpNode,
    prior_states: &[OperatorStateDef],
) -> Result<DeltaIdentity> {
    let identity = match node {
        dag::OpNode::Scan { .. } => DeltaIdentity::BindingRowids(1),
        dag::OpNode::Filter { input, .. } | dag::OpNode::Project { input, .. } => {
            prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no identity contract"
                    ))
                })?
                .output
                .published_identity
        }
        dag::OpNode::Alias { input, .. } => {
            let upstream = prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no identity contract"
                    ))
                })?
                .output
                .published_identity;
            alias_identity(upstream)
        }
        dag::OpNode::Join {
            inputs,
            kind: dag::JoinKind::Inner,
            ..
        } => {
            let input_identities = inputs
                .iter()
                .map(|input| {
                    prior_states
                        .get(*input)
                        .map(|state| state.output.published_identity)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "maintenance DAG input node {input} has no identity contract"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            if input_identities
                .iter()
                .all(|identity| matches!(identity, DeltaIdentity::BindingRowids(_)))
            {
                DeltaIdentity::BindingRowids(
                    input_identities
                        .iter()
                        .map(|identity| identity.width())
                        .sum(),
                )
            } else {
                DeltaIdentity::OperatorKey(2)
            }
        }
        // Matched and NULL-padded rows share `(kind, packed source identity)`.
        dag::OpNode::Join {
            kind: dag::JoinKind::LeftOuter,
            ..
        } => DeltaIdentity::OperatorKey(2),
        dag::OpNode::Aggregate { .. } => DeltaIdentity::OperatorRowid,
        dag::OpNode::SetOp { prefix_len, .. } if *prefix_len > 0 => DeltaIdentity::OperatorRowid,
        // Pure UNION ALL namespaces every branch's packed source identity.
        dag::OpNode::SetOp { .. } => DeltaIdentity::OperatorKey(2),
    };
    Ok(identity)
}

fn declared_binding_rowids(
    node: &dag::OpNode,
    prior_states: &[OperatorStateDef],
) -> Result<Arc<[bool]>> {
    let provenance = match node {
        dag::OpNode::Scan { table, .. } => vec![table.has_rowid],
        dag::OpNode::Filter { input, .. } | dag::OpNode::Project { input, .. } => prior_states
            .get(*input)
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "maintenance DAG input node {input} has no rowid provenance contract"
                ))
            })?
            .output
            .binding_rowids
            .to_vec(),
        // A derived-table alias introduces one new SQL namespace. The source
        // transport key still distinguishes rows, but it is not the alias's
        // SQL rowid.
        dag::OpNode::Alias { .. } => vec![false],
        dag::OpNode::Join { inputs, .. } => {
            let mut provenance = Vec::new();
            for input in inputs {
                provenance.extend_from_slice(
                    &prior_states
                        .get(*input)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "maintenance DAG input node {input} has no rowid provenance contract"
                            ))
                        })?
                        .output
                        .binding_rowids,
                );
            }
            provenance
        }
        // These operators may retain source expressions as output values, but
        // they do not preserve a one-to-one source-row namespace.
        dag::OpNode::Aggregate { input, .. } => vec![
            false;
            prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no rowid provenance contract"
                    ))
                })?
                .output
                .binding_rowids
                .len()
        ],
        dag::OpNode::SetOp { inputs, .. } => vec![
            false;
            prior_states
                .get(inputs[0])
                .ok_or_else(|| {
                    LimboError::InternalError(
                        "set-op input has no rowid provenance contract".to_string(),
                    )
                })?
                .output
                .binding_rowids
                .len()
        ],
    };
    Ok(provenance.into())
}

/// A derived-table alias replaces all of its input bindings with one new SQL
/// namespace. The physical source key remains stable, but its slots can no
/// longer be interpreted as one rowid per output binding.
fn alias_identity(upstream: DeltaIdentity) -> DeltaIdentity {
    match upstream {
        DeltaIdentity::BindingRowids(width) => DeltaIdentity::OperatorKey(width),
        identity => identity,
    }
}

fn node_has_native_arrangement(node: &dag::OpNode) -> bool {
    matches!(
        node,
        dag::OpNode::Scan { .. } | dag::OpNode::Aggregate { .. }
    )
}

fn node_is_join_input(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    dag.nodes.iter().any(|node| {
        matches!(
            node,
            dag::OpNode::Join { inputs, .. } if inputs.contains(&node_id)
        )
    })
}

fn node_is_left_join_input(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    dag.nodes.iter().any(|node| {
        matches!(
            node,
            dag::OpNode::Join {
                inputs,
                kind: dag::JoinKind::LeftOuter,
                ..
            } if inputs.contains(&node_id)
        )
    })
}

/// Whether this node must persist its output integral for a downstream join
/// or terminal sink.
///
/// This demand is derived from DAG edges once and shared by validation,
/// hidden-table creation, and codegen. Pure UNION ALL uses a branch id plus
/// packed upstream identity, so even branches with different identity widths
/// expose one fixed arrangement key.
fn node_requires_output_arrangement(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    emitted_identity: DeltaIdentity,
) -> bool {
    if matches!(
        dag.nodes[node_id],
        dag::OpNode::Join {
            kind: dag::JoinKind::LeftOuter,
            ..
        } | dag::OpNode::SetOp { prefix_len: 0, .. }
    ) {
        return true;
    }
    // Preserve binding rowids through consumers that may reference them.
    // Composite identities are normalized only when they reach a terminal
    // sink or must back a downstream join arrangement.
    if node_id == dag.root
        && !matches!(
            emitted_identity,
            DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
        )
    {
        return true;
    }
    if !node_is_join_input(dag, node_id) || node_has_native_arrangement(&dag.nodes[node_id]) {
        return false;
    }
    true
}

/// Build the persistent-state inventory in DAG order.
///
/// Aggregate and deduplicating set operators own their native integral.
/// Joins and pure UNION ALL publish through ordinary output arrangements, so
/// storage selection depends only on node and edge contracts.
fn plan_operator_states(
    view_name: &str,
    dag: &dag::MaintenanceDag,
    resolver: &Resolver,
) -> Result<OperatorStateCatalog> {
    use crate::incremental::view::DBSP_CIRCUIT_VERSION;

    let mut catalog = OperatorStateCatalog {
        nodes: Vec::with_capacity(dag.nodes.len()),
    };
    let mut hidden_names = FxHashSet::default();
    for (node_id, node) in dag.nodes.iter().enumerate() {
        let emitted_identity = declared_emitted_identity(node, &catalog.nodes)?;
        let binding_rowids = declared_binding_rowids(node, &catalog.nodes)?;
        if binding_rowids.len() != dag.output_schema(node_id).bindings.len() {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} rowid provenance does not match its output bindings"
            )));
        }
        let needs_state = node_requires_primary_state(dag, node_id);
        let state_table = if needs_state {
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                crate::schema::DBSP_TABLE_PREFIX,
            );
            Some(HiddenTableDef {
                create_sql: state_table_sql(&table_name, node)?,
                table_name,
            })
        } else {
            None
        };
        let auxiliary_table = if node_needs_auxiliary_state(node) {
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                crate::schema::DBSP_MULTISET_TABLE_PREFIX,
            );
            Some(HiddenTableDef {
                create_sql: multiset_table_sql(
                    &table_name,
                    node,
                    dag.output_schema(node_id).len()
                        + if matches!(
                            node,
                            dag::OpNode::Join {
                                kind: dag::JoinKind::LeftOuter,
                                ..
                            }
                        ) {
                            binding_rowids
                                .iter()
                                .filter(|available| **available)
                                .count()
                        } else {
                            0
                        },
                )?,
                table_name,
            })
        } else {
            None
        };
        let arrangement_table = if node_requires_output_arrangement(dag, node_id, emitted_identity)
        {
            let identity_width = emitted_identity.width();
            let table_name = format!(
                "{}{DBSP_CIRCUIT_VERSION}_{view_name}__n{node_id}",
                crate::schema::DBSP_ARRANGEMENT_TABLE_PREFIX,
            );
            Some(HiddenTableDef {
                create_sql: arrangement_table_sql(
                    &table_name,
                    identity_width,
                    dag.output_schema(node_id).len(),
                    binding_rowid_metadata_width(emitted_identity, &binding_rowids),
                )?,
                table_name,
            })
        } else {
            None
        };
        let published_identity = if arrangement_table.is_some()
            && (node_id == dag.root || node_is_left_join_input(dag, node_id))
        {
            DeltaIdentity::OperatorRowid
        } else {
            emitted_identity
        };
        let state = OperatorStateDef {
            node_id,
            output: NodeOutputContract {
                schema: Arc::new(dag.output_schema(node_id).clone()),
                binding_rowids,
                emitted_identity,
                published_identity,
            },
            state_table,
            auxiliary_table,
            arrangement_table,
        };
        for table in state
            .state_table
            .iter()
            .chain(state.auxiliary_table.iter())
            .chain(state.arrangement_table.iter())
        {
            if !hidden_names.insert(table.table_name.clone()) {
                return Err(LimboError::InternalError(format!(
                    "duplicate maintenance state table {}",
                    table.table_name
                )));
            }
        }
        catalog.nodes.push(state);
        validate_planned_node(dag, node_id, &catalog, resolver)?;
    }
    validate_terminal_delta_identity(&catalog, dag.root)?;
    Ok(catalog)
}

fn arrangement_table_sql(
    table_name: &str,
    identity_width: usize,
    value_width: usize,
    binding_rowid_width: usize,
) -> Result<String> {
    if identity_width == 0 {
        return Err(LimboError::InternalError(
            "an output arrangement requires a non-empty stable identity".to_string(),
        ));
    }
    let table_ident = crate::util::quote_identifier(table_name);
    let mut columns = (0..identity_width)
        .map(|i| format!("i{i}"))
        .collect::<Vec<_>>();
    columns.extend((0..value_width).map(|i| format!("c{i}")));
    let key = columns[..identity_width + value_width].to_vec();
    columns.extend((0..binding_rowid_width).map(|i| format!("r{i}")));
    columns.push("mult".to_string());
    Ok(format!(
        "CREATE TABLE {table_ident} ({}, PRIMARY KEY ({}))",
        columns.join(", "),
        key.join(", ")
    ))
}

fn node_requires_primary_state(dag: &dag::MaintenanceDag, node_id: dag::NodeId) -> bool {
    match &dag.nodes[node_id] {
        dag::OpNode::Aggregate { .. } => true,
        dag::OpNode::SetOp { prefix_len, .. } => *prefix_len > 0,
        _ => false,
    }
}

/// The CREATE TABLE statement for a view's internal state table.
///
/// For a GROUP BY view: one row per live group, holding the group-key values
/// and each payload aggregate's persisted state, with the PRIMARY KEY over
/// the group columns giving the automatic group-lookup index. MIN/MAX
/// aggregates contribute no columns here — their state is the value multiset
/// table.
///
/// For a compound view with a dedup prefix: one row per distinct content
/// key, holding one signed count per prefix branch (the visibility fold
/// reads them). A trailing UNION ALL suffix keeps its
/// (branch, source-identity, multiplicity) rows in the auxiliary table (see
/// [`multiset_table_sql`]); pure UNION ALL is stateless branch composition.
///
/// In each case the state row's rowid is the operator output identity.
///
/// Declared column types are empty so nothing coerces the stored values.
fn state_table_sql(state_table_name: &str, node: &dag::OpNode) -> Result<String> {
    let state_table_ident = crate::util::quote_identifier(state_table_name);
    match node {
        dag::OpNode::Aggregate {
            group_exprs,
            group_collations,
            aggregates,
            ..
        } => {
            let mut columns = Vec::new();
            for (i, collation) in group_collations.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(*collation)));
            }
            for i in 0..aggregates.len() {
                columns.push(format!("v{i}"));
            }
            for (i, agg) in aggregates.iter().enumerate() {
                if let Some(width) = crate::vdbe::execute::agg_payload_width(&agg.func) {
                    for j in 0..width {
                        columns.push(format!("a{i}_{j}"));
                    }
                }
            }
            let key: Vec<String> = (0..group_exprs.len()).map(|i| format!("g{i}")).collect();
            Ok(format!(
                "CREATE TABLE {state_table_ident} ({}, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        dag::OpNode::SetOp {
            arity,
            prefix_len,
            key_collations,
            ..
        } => {
            if *prefix_len == 0 {
                return Err(LimboError::InternalError(
                    "pure UNION ALL is a stateless branch composition".to_string(),
                ));
            }
            let mut columns: Vec<String> = (0..*arity)
                .map(|i| format!("c{i}{}", collate_clause(key_collations[i])))
                .collect();
            for i in 0..*prefix_len {
                columns.push(format!("cnt{i}"));
            }
            let key: Vec<String> = (0..*arity).map(|i| format!("c{i}")).collect();
            Ok(format!(
                "CREATE TABLE {state_table_ident} ({}, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        _ => Err(LimboError::InternalError(
            "this DAG operator has no state table".to_string(),
        )),
    }
}

/// Whether an aggregate keeps per-value state in the multiset table: MIN/MAX
/// (to find the next extreme after a retraction) and DISTINCT aggregates (to
/// step the accumulator only on first occurrence of a value).
fn uses_multiset(agg: &Aggregate) -> bool {
    matches!(agg.func, AggFunc::Min | AggFunc::Max) || tracks_distinct_values(agg)
}

/// Whether this operator needs auxiliary state.
///
/// Aggregate value multisets are part of aggregate semantics. A mixed set-op's
/// trailing UNION ALL identity map is also part of its output contract: the
/// operator must publish one stable identity whether its consumer is another
/// operator or the terminal view.
fn node_needs_auxiliary_state(node: &dag::OpNode) -> bool {
    match node {
        dag::OpNode::Aggregate { aggregates, .. } => aggregates.iter().any(uses_multiset),
        dag::OpNode::SetOp {
            operators,
            prefix_len,
            ..
        } => *prefix_len > 0 && *prefix_len < operators.len() + 1,
        dag::OpNode::Join { kind, .. } => *kind == dag::JoinKind::LeftOuter,
        _ => false,
    }
}

/// The CREATE TABLE statement for a GROUP BY view's value multiset: one row
/// per (aggregate, group, distinct value) with its multiplicity. For MIN/MAX
/// the PRIMARY KEY ordering makes a group's extreme value an index seek, so
/// retracting the current extreme never needs a rescan in Rust; for DISTINCT
/// aggregates the multiplicity decides when a value enters or leaves the
/// accumulator.
fn multiset_table_sql(
    multiset_table_name: &str,
    node: &dag::OpNode,
    payload_width: usize,
) -> Result<String> {
    let multiset_table_ident = crate::util::quote_identifier(multiset_table_name);
    match node {
        dag::OpNode::Aggregate {
            group_collations,
            multiset_collation,
            ..
        } => {
            let mut columns = vec!["agg_id".to_string()];
            let mut key = vec!["agg_id".to_string()];
            for (i, collation) in group_collations.iter().enumerate() {
                columns.push(format!("g{i}{}", collate_clause(*collation)));
                key.push(format!("g{i}"));
            }
            columns.push(format!("val{}", collate_clause(*multiset_collation)));
            key.push("val".to_string());
            Ok(format!(
                "CREATE TABLE {multiset_table_ident} ({}, mult, PRIMARY KEY ({}))",
                columns.join(", "),
                key.join(", ")
            ))
        }
        // A compound view's trailing UNION ALL branches: a multiset of
        // (branch, packed source identity), each row's rowid doubling as its
        // view rowid (disambiguated from content-row rowids by the caller).
        // Packing makes the contract independent of whether a branch carries
        // one scan rowid or a composed join identity tuple.
        dag::OpNode::SetOp { .. } => Ok(format!(
            "CREATE TABLE {multiset_table_ident} (branch, source_identity, mult, PRIMARY KEY (branch, source_identity))"
        )),
        // A LEFT JOIN view's per-left-row bookkeeping: signed presence,
        // count of ON-matches, and whether its NULL-padded row is currently
        // published. The payload preserves that image and its binding-rowid
        // provenance so downstream operators can consume its retraction after
        // the left row departs.
        dag::OpNode::Join { .. } => {
            let payload = (0..payload_width)
                .map(|i| format!(", c{i}"))
                .collect::<String>();
            Ok(format!(
                "CREATE TABLE {multiset_table_ident} (l_rid, present, matches, padded{payload}, PRIMARY KEY (l_rid))"
            ))
        }
        _ => Err(LimboError::InternalError(
            "only GROUP BY, compound, and LEFT JOIN views have multiset tables".to_string(),
        )),
    }
}

/// The `COLLATE` clause for a hidden-table key column, empty for the
/// default binary comparison. The name round-trips through
/// [`CollationSeq::new`] when the generated DDL is parsed back; custom
/// collations (which do not) are rejected at classify time.
fn collate_clause(collation: CollationSeq) -> String {
    match collation {
        CollationSeq::Binary | CollationSeq::Unset => String::new(),
        other => format!(" COLLATE \"{}\"", other.name()),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn compile_maintenance_program(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let key = MaintenanceProgramCacheKey {
        view_name: view_name.to_string(),
        schema_version: schema.schema_version,
        root_page: view_root_page,
        num_columns: num_view_columns,
        input,
    };
    if let Some(prepared) = connection.maintenance_program_cache.get(&key, connection) {
        return Ok(prepared.bind(connection.clone()));
    }
    let program = compile_maintenance_program_uncached(
        view_name,
        select,
        view_root_page,
        num_view_columns,
        input,
        schema,
        connection,
    )?;
    connection
        .maintenance_program_cache
        .insert(key, program.prepared().clone());
    Ok(program)
}

#[allow(clippy::too_many_arguments)]
fn compile_maintenance_program_uncached(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let plan = build_plan_for_connection(view_name, select, schema, connection)?;
    compile_maintenance_plan(
        view_name,
        view_root_page,
        num_view_columns,
        input,
        &plan,
        schema,
        connection,
    )
}

#[allow(clippy::too_many_arguments)]
fn compile_maintenance_plan(
    view_name: &str,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    plan: &MaintenancePlan,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    compile_generic_dag_program(
        view_name,
        &plan.dag,
        plan.dag.root,
        view_root_page,
        num_view_columns,
        input,
        &plan.operator_states,
        schema,
        connection,
    )
}

/// Build the bound maintenance plan with a transient connection resolver.
fn build_plan_for_connection(
    view_name: &str,
    select: &ast::Select,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<MaintenancePlan> {
    let syms = connection.syms.read();
    let resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
    );
    plan_view(view_name, select, &resolver, connection)
}

fn scan_node_output(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    schema: &Schema,
) -> Result<NodeOutput> {
    let dag::OpNode::Scan { table, .. } = &dag.nodes[node_id] else {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} is not a scan"
        )));
    };
    let stored_weight_column = schema
        .is_materialized_view(&table.name)
        .then(|| table.columns().len());
    Ok(NodeOutput {
        delta: DeltaSource::BaseTable {
            table: table.clone(),
            stored_weight_column,
        },
        arrangement: Some(base_arrangement(table.clone(), stored_weight_column)),
    })
}

/// Integrate one node's declared delta into its persistent output arrangement.
///
/// Interior arrangements preserve the producer identity so later binary joins
/// can compose binding-rowid tuples without losing provenance. A terminal
/// arrangement publishes its own rowid to satisfy the materialized-view sink's
/// one-rowid contract.
#[allow(clippy::too_many_arguments)]
fn emit_output_arrangement(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
    arrangement_table_name: &str,
    schema: &Schema,
) -> Result<ArrangementHandle> {
    let identity_width = input.identity_width();
    if identity_width == 0 {
        return Err(LimboError::InternalError(
            "an arranged delta must carry a stable source identity".to_string(),
        ));
    }
    let extra_binding_rowid_width = input.value_start - identity_width;
    turso_assert!(
        (output.identity == input.identity || output.identity == DeltaIdentity::OperatorRowid)
            && output.width == input.width
            && output.binding_rowid_columns.len() == input.binding_rowid_columns.len()
            && output
                .binding_rowid_columns
                .iter()
                .map(Option::is_some)
                .eq(input.binding_rowid_columns.iter().map(Option::is_some))
            && input.weight_column == input.value_start + input.width
            && output.weight_column == output.value_start + output.width,
        "output arrangement must publish its planned identity, values, and weight"
    );

    let table = schema
        .get_btree_table(arrangement_table_name)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "output arrangement {arrangement_table_name} of materialized view {view_name} not found"
            ))
        })?;
    let index = schema
        .get_indices(arrangement_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "output arrangement {arrangement_table_name} of materialized view {view_name} has no index"
            ))
        })?;
    let expected_columns = identity_width + input.width + extra_binding_rowid_width + 1;
    if table.columns().len() != expected_columns {
        return Err(LimboError::InternalError(format!(
            "output arrangement {arrangement_table_name} has {} columns, expected {expected_columns}",
            table.columns().len()
        )));
    }

    let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: 0,
    });
    let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor,
        root_page: RegisterOrLiteral::Literal(index.root_page),
        db: 0,
    });

    // The relational arrangement key is `(source identity, full row value)`.
    // A stable source identity can change values during an update, and the
    // old/new images must integrate independently.
    let key_width = identity_width + input.width;
    let lookup_start = program.alloc_registers(key_width + 1);
    let input_values_start = lookup_start + identity_width;
    let arrangement_rowid_reg = lookup_start + key_width;
    let contribution_reg = program.alloc_register();
    let current_mult_reg = program.alloc_register();
    let new_mult_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let is_new_reg = program.alloc_register();
    let previous_rowid_reg = program.alloc_register();
    // [source identity..., persisted values..., binding rowids..., multiplicity].
    let state_record_start = program.alloc_registers(expected_columns);
    let state_binding_rowids_start = state_record_start + key_width;
    let state_mult_reg = state_binding_rowids_start + extra_binding_rowid_width;
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    // [published identity..., binding rowids..., delta values..., delta weight].
    let output_record_start = program.alloc_registers(output.record_width());
    let output_values_start = output_record_start + output.value_start;
    let output_weight_reg = output_record_start + output.weight_column;
    let output_record_reg = program.alloc_register();
    let output_rowid_reg = program.alloc_register();

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let found_label = program.allocate_label();
    let update_label = program.allocate_label();
    let write_state_label = program.allocate_label();
    let emit_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    program.emit_int(0, zero_reg);
    program.emit_int(1, one_reg);
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..identity_width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: lookup_start + column,
            default: None,
        });
    }
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: input_values_start + column,
            default: None,
        });
    }
    for column in identity_width..input.value_start {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: state_binding_rowids_start + column - identity_width,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: contribution_reg,
        default: None,
    });
    program.emit_insn(Insn::Eq {
        lhs: contribution_reg,
        rhs: zero_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    program.emit_insn(Insn::Found {
        cursor_id: index_cursor,
        target_pc: found_label,
        record_reg: lookup_start,
        num_regs: key_width,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: table_cursor,
        rowid_reg: arrangement_rowid_reg,
        prev_largest_reg: previous_rowid_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: new_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: one_reg,
        dst_reg: is_new_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: lookup_start,
        dst_reg: state_record_start,
        extra_amount: key_width - 1,
    });
    program.emit_insn(Insn::Goto {
        target_pc: write_state_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_insn(Insn::Copy {
        src_reg: zero_reg,
        dst_reg: is_new_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxRowId {
        cursor_id: index_cursor,
        dest: arrangement_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor,
        src_reg: arrangement_rowid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: table_cursor,
        column: key_width + extra_binding_rowid_width,
        dest: current_mult_reg,
        default: None,
    });
    program.emit_insn(Insn::Add {
        lhs: contribution_reg,
        rhs: current_mult_reg,
        dest: new_mult_reg,
    });
    program.emit_insn(Insn::Ne {
        lhs: new_mult_reg,
        rhs: zero_reg,
        target_pc: update_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: lookup_start,
        num_regs: key_width + 1,
        cursor_id: index_cursor,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor,
        table_name: arrangement_table_name.to_string(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: emit_label,
    });

    program.preassign_label_to_next_insn(update_label);
    program.emit_insn(Insn::Copy {
        src_reg: lookup_start,
        dst_reg: state_record_start,
        extra_amount: key_width - 1,
    });

    program.preassign_label_to_next_insn(write_state_label);
    program.emit_insn(Insn::Copy {
        src_reg: new_mult_reg,
        dst_reg: state_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_record_start as u16,
        count: expected_columns as u16,
        dest_reg: state_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: table_cursor,
        key_reg: arrangement_rowid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: arrangement_table_name.to_string(),
    });
    // New rows need a primary-key index entry; replacing an existing table
    // row leaves its identity and index entry unchanged.
    program.emit_insn(Insn::Eq {
        lhs: is_new_reg,
        rhs: zero_reg,
        target_pc: emit_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: lookup_start as u16,
        count: (key_width + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor,
        record_reg: index_record_reg,
        unpacked_start: Some(lookup_start),
        unpacked_count: Some((key_width + 1) as u16),
        flags: IdxInsertFlags::new(),
    });

    program.preassign_label_to_next_insn(emit_label);
    if output.identity == input.identity {
        program.emit_insn(Insn::Copy {
            src_reg: lookup_start,
            dst_reg: output_record_start,
            extra_amount: identity_width - 1,
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: arrangement_rowid_reg,
            dst_reg: output_record_start,
            extra_amount: 0,
        });
    }
    if input.width > 0 {
        program.emit_insn(Insn::Copy {
            src_reg: input_values_start,
            dst_reg: output_values_start,
            extra_amount: input.width - 1,
        });
    }
    for (input_column, output_column) in input
        .binding_rowid_columns
        .iter()
        .zip(output.binding_rowid_columns.iter())
    {
        match (input_column, output_column) {
            (None, None) => {}
            (Some(input_column), Some(output_column))
                if *output_column < output.identity_width() =>
            {
                turso_assert!(
                    output.identity == input.identity && input_column == output_column,
                    "published binding-rowid identity must preserve its source slot"
                );
            }
            (Some(input_column), Some(output_column)) => {
                let source_reg = if *input_column < identity_width {
                    lookup_start + *input_column
                } else {
                    state_binding_rowids_start + *input_column - identity_width
                };
                program.emit_insn(Insn::Copy {
                    src_reg: source_reg,
                    dst_reg: output_record_start + *output_column,
                    extra_amount: 0,
                });
            }
            _ => turso_assert!(
                false,
                "output arrangement must preserve binding-rowid availability"
            ),
        }
    }
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: output_weight_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: output_record_start as u16,
        count: output.record_width() as u16,
        dest_reg: output_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg: output_record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} output arrangement is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);

    let value_columns = (identity_width..identity_width + input.width).collect();
    let binding_rowid_columns = input
        .binding_rowid_columns
        .iter()
        .map(|column| {
            column.map(|column| {
                ArrangementIdentityColumn::Column(if column < identity_width {
                    column
                } else {
                    key_width + column - identity_width
                })
            })
        })
        .collect();
    let (arrangement_identity, identity_columns) = if output.identity == input.identity {
        (
            input.identity,
            (0..identity_width)
                .map(ArrangementIdentityColumn::Column)
                .collect(),
        )
    } else {
        (
            DeltaIdentity::OperatorRowid,
            vec![ArrangementIdentityColumn::RowId],
        )
    };
    Ok(btree_arrangement(
        table,
        arrangement_identity,
        identity_columns,
        binding_rowid_columns,
        value_columns,
        Some(key_width + extra_binding_rowid_width),
    ))
}

#[allow(clippy::too_many_arguments)]
fn materialize_declared_output_arrangement(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    output: NodeOutput,
    maintenance_input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<NodeOutput> {
    let Some(arrangement_def) = &operator_state.arrangement_table else {
        return Ok(output);
    };
    let input = output.delta.materialize(
        program,
        view_name,
        node_id,
        &operator_state.output,
        maintenance_input,
    )?;
    let expected_identity = operator_state.output.emitted_identity;
    if input.identity != expected_identity {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} emits {:?}, expected {:?}",
            input.identity, expected_identity
        )));
    }
    let arranged_output = open_ephemeral_delta(
        program,
        &format!("{view_name}_arranged_delta_{node_id}"),
        operator_state.output.schema.as_ref().clone(),
        operator_state.output.published_identity,
        operator_state.output.binding_rowids.clone(),
        input.requires_positive_first,
    );
    let arrangement = emit_output_arrangement(
        program,
        view_name,
        &input,
        &arranged_output,
        &arrangement_def.table_name,
        schema,
    )?;
    Ok(NodeOutput {
        delta: DeltaSource::Ephemeral(arranged_output),
        arrangement: Some(arrangement),
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_ephemeral_filter(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    output_contract: &NodeOutputContract,
    predicate: &ast::Expr,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<DeltaSource> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_filter_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        input.requires_positive_first,
    );

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
    );
    let (table_references, binding_remap) = stream_table_references(program, &input.schema);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let pass_label = program.allocate_label();
    turso_assert!(
        output.identity == input.identity
            && output.binding_rowid_columns == input.binding_rowid_columns
            && output.value_start == input.value_start
            && output.weight_column == input.weight_column,
        "filter output must preserve its complete input stream contract"
    );
    let record_start = program.alloc_registers(output.record_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    seed_ephemeral_stream_cache(program, input, &binding_remap, &mut resolver)?;
    let bound_predicate = remap_bound_expr(predicate, &binding_remap)?;
    translate_condition_expr(
        program,
        &table_references,
        &bound_predicate,
        ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: pass_label,
            jump_target_when_false: next_label,
            jump_target_when_null: next_label,
        },
        &resolver,
    )?;
    program.preassign_label_to_next_insn(pass_label);

    for column in 0..input.weight_column {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: record_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + output.weight_column,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    Ok(DeltaSource::Ephemeral(output))
}

fn append_alias_values(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
) -> Result<()> {
    if input.width != output.width
        || output.identity_width() != input.identity_width()
        || output.value_start != output.identity_width()
        || output.binding_rowid_columns.iter().any(Option::is_some)
    {
        return Err(LimboError::InternalError(
            "alias stream copy has incompatible input/output layouts".to_string(),
        ));
    }

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let record_start = program.alloc_registers(output.record_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..output.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: record_start + column,
            default: None,
        });
    }
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: record_start + output.value_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + output.weight_column,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}

fn emit_ephemeral_alias(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    output_contract: &NodeOutputContract,
) -> Result<DeltaSource> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_alias_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        input.requires_positive_first,
    );
    append_alias_values(program, input, &output)?;
    Ok(DeltaSource::Ephemeral(output))
}

/// Emit every ancestor needed by `target` once, in DAG topological order.
///
/// The output table is keyed by [`dag::NodeId`], making fan-out explicit:
/// downstream nodes consume the already-compiled [`NodeOutput`] of each
/// declared input rather than recursively re-emitting its subtree.
#[allow(clippy::too_many_arguments)]
fn emit_declared_delta_nodes_to(
    program: &mut ProgramBuilder,
    view_name: &str,
    dag: &dag::MaintenanceDag,
    target: dag::NodeId,
    input: MaintenanceInput,
    operator_states: &OperatorStateCatalog,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<NodeOutput> {
    let mut required = vec![false; dag.nodes.len()];
    let mut pending = vec![target];
    while let Some(node_id) = pending.pop() {
        if std::mem::replace(&mut required[node_id], true) {
            continue;
        }
        pending.extend_from_slice(dag.nodes[node_id].inputs());
    }

    let mut outputs: Vec<Option<NodeOutput>> = vec![None; dag.nodes.len()];
    for node_id in 0..=target {
        if !required[node_id] {
            continue;
        }
        let output_contract = operator_states.output_for_node(node_id)?;
        let output = match &dag.nodes[node_id] {
            dag::OpNode::Scan { .. } => scan_node_output(dag, node_id, schema)?,
            dag::OpNode::Filter {
                input: upstream,
                predicate,
            } => {
                let upstream_id = *upstream;
                let upstream = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "filter DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = upstream.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_filter(
                        program,
                        view_name,
                        node_id,
                        &upstream,
                        output_contract,
                        predicate,
                        schema,
                        connection,
                    )?,
                    arrangement: None,
                }
            }
            dag::OpNode::Project {
                input: upstream,
                projections,
            } => {
                let upstream_id = *upstream;
                let upstream_output = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "project DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = upstream_output.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                let output = open_ephemeral_delta(
                    program,
                    &format!("{view_name}_project_delta_{node_id}"),
                    output_contract.schema.as_ref().clone(),
                    output_contract.emitted_identity,
                    output_contract.binding_rowids.clone(),
                    upstream.requires_positive_first,
                );
                emit_ephemeral_project(
                    program,
                    &upstream,
                    projections,
                    &output,
                    schema,
                    connection,
                )?;
                NodeOutput {
                    delta: DeltaSource::Ephemeral(output),
                    arrangement: None,
                }
            }
            dag::OpNode::Alias {
                input: upstream, ..
            } => {
                let upstream_id = *upstream;
                let upstream_output = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "alias DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = upstream_output.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_alias(
                        program,
                        view_name,
                        node_id,
                        &upstream,
                        output_contract,
                    )?,
                    arrangement: None,
                }
            }
            dag::OpNode::Join { inputs, on, kind } => {
                let declared_input = |input_id: dag::NodeId| {
                    outputs[input_id].as_ref().ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "join DAG node {node_id} input {input_id} was not compiled"
                        ))
                    })
                };
                let declared_inputs = [declared_input(inputs[0])?, declared_input(inputs[1])?];
                let input_schemas = [
                    operator_states.output_for_node(inputs[0])?.schema.clone(),
                    operator_states.output_for_node(inputs[1])?.schema.clone(),
                ];
                let contract = JoinContract::from_outputs(declared_inputs, input_schemas, on)?;
                if *kind == dag::JoinKind::LeftOuter {
                    NodeOutput {
                        delta: emit_left_join_deltas_to_ephemeral(
                            program,
                            view_name,
                            node_id,
                            &contract,
                            output_contract,
                            input,
                            operator_states.for_node(node_id)?,
                            schema,
                            connection,
                        )?,
                        arrangement: None,
                    }
                } else {
                    let syms = connection.syms.read();
                    let mut resolver = Resolver::new(
                        schema,
                        connection.database_schemas(),
                        &connection.temp.database,
                        connection.attached_databases(),
                        &syms,
                        connection.experimental_custom_types_enabled(),
                        connection.get_dqs_dml().into(),
                        Arc::new(crate::dialect::SqliteDialect),
                    );
                    NodeOutput {
                        delta: emit_join_deltas_to_ephemeral(
                            program,
                            &mut resolver,
                            view_name,
                            output_contract,
                            &contract,
                            input,
                        )?,
                        arrangement: None,
                    }
                }
            }
            dag::OpNode::SetOp {
                inputs: set_inputs,
                operators,
                prefix_len,
                key_collations,
                ..
            } => {
                let mut channels = Vec::with_capacity(set_inputs.len());
                for upstream_id in set_inputs {
                    let upstream = outputs[*upstream_id].as_ref().ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "set-op DAG node {node_id} input {upstream_id} was not compiled"
                        ))
                    })?;
                    channels.push(upstream.delta.materialize(
                        program,
                        view_name,
                        *upstream_id,
                        operator_states.output_for_node(*upstream_id)?,
                        input,
                    )?);
                }
                NodeOutput {
                    delta: emit_set_op_to_ephemeral(
                        program,
                        view_name,
                        node_id,
                        &channels,
                        operators,
                        *prefix_len,
                        key_collations,
                        output_contract,
                        operator_states.for_node(node_id)?,
                        schema,
                    )?,
                    arrangement: None,
                }
            }
            dag::OpNode::Aggregate {
                input: upstream,
                group_exprs,
                group_collations,
                aggregates,
                scalar,
                ..
            } => {
                let upstream_id = *upstream;
                let upstream = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "aggregate DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = upstream.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                let output = open_ephemeral_delta(
                    program,
                    &format!("{view_name}_aggregate_delta_{node_id}"),
                    output_contract.schema.as_ref().clone(),
                    output_contract.emitted_identity,
                    output_contract.binding_rowids.clone(),
                    false,
                );
                let operator_state = operator_states.for_node(node_id)?;
                emit_group_aggregate(
                    program,
                    view_name,
                    &upstream,
                    group_exprs,
                    group_collations,
                    aggregates,
                    *scalar,
                    &output,
                    input,
                    operator_state,
                    schema,
                    connection,
                )?;
                let state_table_name = operator_state.state_table_name()?;
                let state_table = schema.get_btree_table(state_table_name).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "aggregate arrangement table {state_table_name} not found"
                    ))
                })?;
                let value_columns = (0..group_exprs.len() + aggregates.len()).collect();
                NodeOutput {
                    delta: DeltaSource::Ephemeral(output),
                    arrangement: Some(btree_arrangement(
                        state_table,
                        DeltaIdentity::OperatorRowid,
                        vec![ArrangementIdentityColumn::RowId],
                        vec![None; output_contract.schema.bindings.len()],
                        value_columns,
                        None,
                    )),
                }
            }
        };
        let operator_state = operator_states.for_node(node_id)?;
        if output.delta.identity() != operator_state.output.emitted_identity {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} emitted {:?}, but its declared identity is {:?}",
                output.delta.identity(),
                operator_state.output.emitted_identity
            )));
        }
        if output.delta.binding_rowids().as_ref() != operator_state.output.binding_rowids.as_ref() {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} emitted the wrong binding-rowid provenance"
            )));
        }
        let output = materialize_declared_output_arrangement(
            program,
            view_name,
            node_id,
            output,
            input,
            operator_state,
            schema,
        )?;
        if output.delta.identity() != operator_state.output.published_identity {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} published {:?}, but its edge contract is {:?}",
                output.delta.identity(),
                operator_state.output.published_identity
            )));
        }
        if output.delta.binding_rowids().as_ref() != operator_state.output.binding_rowids.as_ref() {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} published the wrong binding-rowid provenance"
            )));
        }
        if let Some(arrangement) = &output.arrangement {
            let arrangement_binding_rowids = arrangement
                .binding_rowid_columns()
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>();
            if arrangement_binding_rowids.as_slice()
                != operator_state.output.binding_rowids.as_ref()
            {
                return Err(LimboError::InternalError(format!(
                    "maintenance DAG node {node_id} arrangement exposes the wrong binding-rowid provenance"
                )));
            }
        }
        outputs[node_id] = Some(output);
    }

    outputs[target].take().ok_or_else(|| {
        LimboError::InternalError(format!(
            "maintenance DAG target node {target} was not compiled"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_generic_dag_to_sink(
    program: &mut ProgramBuilder,
    view_name: &str,
    dag: &dag::MaintenanceDag,
    root: dag::NodeId,
    input: MaintenanceInput,
    sink: &ViewSink,
    operator_states: &OperatorStateCatalog,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let node_output = emit_declared_delta_nodes_to(
        program,
        view_name,
        dag,
        root,
        input,
        operator_states,
        schema,
        connection,
    )?;
    let stream = node_output.delta.materialize(
        program,
        view_name,
        root,
        operator_states.output_for_node(root)?,
        input,
    )?;
    emit_terminal_delta(program, view_name, &stream, sink)
}

#[allow(clippy::too_many_arguments)]
fn compile_generic_dag_program(
    view_name: &str,
    dag: &dag::MaintenanceDag,
    root: dag::NodeId,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    operator_states: &OperatorStateCatalog,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 13,
            approx_num_insns: 128 + 64 * num_view_columns,
            approx_num_labels: 48,
        },
    );
    program.prologue();
    emit_generic_dag_to_sink(
        &mut program,
        view_name,
        dag,
        root,
        input,
        &ViewSink {
            root_page: view_root_page,
            num_columns: num_view_columns,
        },
        operator_states,
        schema,
        connection,
    )?;
    program.epilogue(schema);
    program.build(
        connection.clone(),
        false,
        "materialized view generic DAG maintenance",
    )
}

// Emit a Scan node into its declared ephemeral delta channel.
fn emit_base_scan_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    base_table: &Arc<BTreeTable>,
    stored_weight_column: Option<usize>,
    input: MaintenanceInput,
    output: &EphemeralDelta,
) -> Result<()> {
    turso_assert!(
        output.identity == DeltaIdentity::BindingRowids(1)
            && output.binding_rowid_columns.as_ref() == [Some(0)]
            && output.value_start == 1
            && output.width == base_table.columns().len()
            && output.weight_column == output.width + 1,
        "scan delta channel must contain rowid, base columns, and weight"
    );
    let input_cursor_id = match input {
        MaintenanceInput::TransactionDelta => {
            let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                view_name: view_name.to_string(),
                table: base_table.clone(),
            });
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: 0,
                db: 0,
            });
            cursor_id
        }
        MaintenanceInput::BaseTable => {
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(base_table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: base_table.root_page,
                db: 0,
            });
            cursor_id
        }
    };

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    let record_start = program.alloc_registers(output.record_width());
    let weight_reg = record_start + output.weight_column;
    program.emit_insn(Insn::RowId {
        cursor_id: input_cursor_id,
        dest: record_start,
    });
    for column in 0..output.width {
        program.emit_column_or_rowid(
            input_cursor_id,
            column,
            record_start + output.value_start + column,
        );
    }
    match input {
        MaintenanceInput::TransactionDelta => {
            program.emit_insn(Insn::Column {
                cursor_id: input_cursor_id,
                column: base_table.columns().len(),
                dest: weight_reg,
                default: None,
            });
        }
        MaintenanceInput::BaseTable => {
            if let Some(column) = stored_weight_column {
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column,
                    dest: weight_reg,
                    default: None,
                });
            } else {
                program.emit_int(1, weight_reg);
            }
        }
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let output_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}
#[allow(clippy::too_many_arguments)]
fn emit_ephemeral_project(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    projections: &[(ast::Expr, Option<String>)],
    output: &EphemeralDelta,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let num_output_columns = output.width;
    turso_assert!(
        projections.len() == num_output_columns,
        "projection output does not match its sink schema"
    );

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
    );
    let (table_references, binding_remap) = stream_table_references(program, &input.schema);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    seed_ephemeral_stream_cache(program, input, &binding_remap, &mut resolver)?;

    turso_assert!(
        output.identity == input.identity
            && output.binding_rowid_columns == input.binding_rowid_columns
            && output.value_start == input.value_start
            && output.weight_column == output.value_start + num_output_columns,
        "linear projection sink must preserve its input metadata contract"
    );
    let metadata_start = program.alloc_registers(output.value_start);
    for column in 0..input.value_start {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: metadata_start + column,
            default: None,
        });
    }
    let out_start = program.alloc_registers(num_output_columns + 1);
    let new_weight_reg = out_start + num_output_columns;
    for (index, (expr, _)) in projections.iter().enumerate() {
        let bound = remap_bound_expr(expr, &binding_remap)?;
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            &bound,
            out_start + index,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }
    let weight_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });

    debug_assert_eq!(metadata_start + output.value_start, out_start);
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: new_weight_reg,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: metadata_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let output_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    Ok(())
}

/// Consume a fully evaluated one-identity delta stream at the maintenance
/// program boundary. Operator code owns expression evaluation; this adapter
/// only binds the root's standard `(identity, values, weight)` contract to a
/// persistent view.
fn emit_terminal_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &EphemeralDelta,
    sink: &ViewSink,
) -> Result<()> {
    turso_assert!(
        matches!(
            input.identity,
            DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
        ) && input.weight_column == input.value_start + input.width,
        "terminal delta streams require one stable operator identity"
    );
    let num_output_columns = sink.num_columns;
    turso_assert!(
        input.width == num_output_columns,
        "terminal delta width does not match its sink"
    );

    let table = Arc::new(synthesized_view_table(
        view_name,
        sink.root_page,
        num_output_columns,
    ));
    let view_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Literal(sink.root_page),
        db: 0,
    });

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let row_start = program.alloc_registers(1 + num_output_columns + 1);
    let identity_reg = row_start;
    let values_start = row_start + 1;
    let weight_reg = values_start + num_output_columns;

    // Join phase decomposition can retract a cross-generation identity before
    // inserting it. Apply positive contributions first at the persistent
    // boundary so an unknown negative is never discarded before its matching
    // positive arrives.
    let pass_reg = input.requires_positive_first.then(|| {
        let pass_reg = program.alloc_register();
        program.emit_int(0, pass_reg);
        pass_reg
    });
    let pass_start_label = program.allocate_label();
    program.preassign_label_to_next_insn(pass_start_label);
    let pass_done_label = if pass_reg.is_some() {
        program.allocate_label()
    } else {
        end_label
    };
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: 0,
        dest: identity_reg,
        default: None,
    });
    for column in 0..num_output_columns {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: values_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });
    if let Some(pass_reg) = pass_reg {
        let negative_pass_label = program.allocate_label();
        let pass_ok_label = program.allocate_label();
        let zero_reg = program.alloc_register();
        program.emit_int(0, zero_reg);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: negative_pass_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Lt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: pass_ok_label,
        });
        program.preassign_label_to_next_insn(negative_pass_label);
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.preassign_label_to_next_insn(pass_ok_label);
    }

    {
        let not_found_label = program.allocate_label();
        let merge_label = program.allocate_label();
        let write_label = program.allocate_label();
        let delete_label = program.allocate_label();
        let current_weight_reg = program.alloc_register();
        let new_weight_reg = program.alloc_register();
        let found_reg = program.alloc_register();
        let zero_reg = program.alloc_register();
        program.emit_int(0, zero_reg);

        let view_key_reg = identity_reg;
        program.emit_insn(Insn::SeekRowid {
            cursor_id: view_cursor_id,
            src_reg: view_key_reg,
            target_pc: not_found_label,
        });
        program.emit_insn(Insn::Column {
            cursor_id: view_cursor_id,
            column: num_output_columns,
            dest: current_weight_reg,
            default: None,
        });
        program.emit_int(1, found_reg);
        program.emit_insn(Insn::Goto {
            target_pc: merge_label,
        });
        program.preassign_label_to_next_insn(not_found_label);
        program.emit_int(0, current_weight_reg);
        program.emit_int(0, found_reg);
        program.preassign_label_to_next_insn(merge_label);
        program.emit_insn(Insn::Add {
            lhs: weight_reg,
            rhs: current_weight_reg,
            dest: new_weight_reg,
        });
        program.emit_insn(Insn::Gt {
            lhs: new_weight_reg,
            rhs: zero_reg,
            target_pc: write_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: found_reg,
            target_pc: delete_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(delete_label);
        program.emit_insn(Insn::Delete {
            cursor_id: view_cursor_id,
            table_name: view_name.to_string(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });

        program.preassign_label_to_next_insn(write_label);
        // On the negative pass, retain the newer image installed by a
        // positive contribution instead of overwriting it with the
        // retracted old image.
        let values_ready_label = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: values_ready_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        for column in 0..num_output_columns {
            program.emit_insn(Insn::Column {
                cursor_id: view_cursor_id,
                column,
                dest: values_start + column,
                default: None,
            });
        }
        program.preassign_label_to_next_insn(values_ready_label);
        program.emit_insn(Insn::Copy {
            src_reg: new_weight_reg,
            dst_reg: weight_reg,
            extra_amount: 0,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: values_start as u16,
            count: (num_output_columns + 1) as u16,
            dest_reg: record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: view_cursor_id,
            key_reg: view_key_reg,
            record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: view_name.to_string(),
        });
    }

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    if let Some(pass_reg) = pass_reg {
        program.preassign_label_to_next_insn(pass_done_label);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: end_label,
            jump_if_null: false,
        });
        program.emit_int(1, pass_reg);
        program.emit_insn(Insn::Goto {
            target_pc: pass_start_label,
        });
    }
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}

/// A minimal BTreeTable describing the view's on-disk layout (output columns
/// plus the trailing weight column), used only to size the write cursor.
fn synthesized_view_table(view_name: &str, root_page: i64, num_view_columns: usize) -> BTreeTable {
    use crate::schema::BTreeCharacteristics;
    // Declared type is empty (BLOB affinity) so nothing about this synthetic
    // schema can coerce the values the maintenance program writes.
    let mut columns: Vec<crate::schema::Column> = (0..num_view_columns)
        .map(|i| {
            crate::schema::Column::new_default_text(Some(format!("c{i}")), String::new(), None)
        })
        .collect();
    columns.push(crate::schema::Column::new_default_text(
        Some("__ivm_weight".to_string()),
        String::new(),
        None,
    ));
    BTreeTable::new(
        root_page,
        view_name.to_string(),
        Vec::new(),
        columns,
        BTreeCharacteristics::HAS_ROWID,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    )
}
