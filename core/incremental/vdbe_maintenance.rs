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
    Aggregate, ColumnUsedMask, IterationDirection, JoinInfo, JoinedTable, Operation, Plan,
    QueryDestination, Scan, SelectPlan, TableReferences,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::select::prepare_select_plan;
use crate::turso_assert;
use crate::types::ImmutableRecord;
use crate::util::walk_expr_with_subqueries;
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::{BranchOffset, PreparedProgram, Program};
use crate::{
    CompletionError, Connection, IOCompletions, IOResult, LimboError, QueryMode, Result, Value,
};
use turso_parser::ast::TableInternalId;

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
        identifier: String,
        logical_id: TableInternalId,
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

    fn sole_binding(&self) -> Result<(Arc<BTreeTable>, String, TableInternalId)> {
        match self {
            Self::BaseTable {
                table,
                identifier,
                logical_id,
                ..
            } => Ok((table.clone(), identifier.clone(), *logical_id)),
            Self::Ephemeral(channel) => {
                if channel.schema.bindings.len() != 1 {
                    return Err(LimboError::InternalError(
                        "arranged input must expose exactly one logical binding".to_string(),
                    ));
                }
                let binding = &channel.schema.bindings[0];
                Ok((
                    binding.table.clone(),
                    binding.identifier.clone(),
                    binding.logical_id,
                ))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize(
        &self,
        program: &mut ProgramBuilder,
        view_name: &str,
        source_node: dag::NodeId,
        source_schema: &dag::StreamSchema,
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
        let sink = open_ephemeral_delta(
            program,
            &format!("{view_name}_scan_delta_{source_node}"),
            source_schema.clone(),
            DeltaIdentity::BindingRowids(1),
            false,
        );
        let channel = sink.ephemeral()?;
        emit_base_scan_delta(
            program,
            view_name,
            table,
            *stored_weight_column,
            input,
            channel,
        )?;
        Ok(channel.clone())
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

/// Physical inputs declared by one Join DAG node.
struct JoinContract {
    deltas: Vec<DeltaSource>,
    tables: Vec<JoinTable>,
    arrangements: Vec<ArrangementHandle>,
    on: Vec<ast::Expr>,
}

#[derive(Debug, Clone)]
struct EphemeralDelta {
    cursor_id: usize,
    identity: DeltaIdentity,
    value_start: usize,
    width: usize,
    weight_column: usize,
    schema: Arc<dag::StreamSchema>,
    /// Applying negative join contributions before positive ones can
    /// transiently retract an as-yet-unknown group. Consumers that merge into
    /// state must process this stream positive-first.
    requires_positive_first: bool,
}

/// Stable transport identity carried ahead of an ephemeral delta's relational
/// values. Encoding the identity kind as a sum type prevents consumers from
/// treating an operator-owned state rowid as a SQL binding rowid merely
/// because both occupy one physical slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaIdentity {
    /// One SQL rowid per logical binding, in binding order.
    BindingRowids(usize),
    /// One opaque rowid owned by the emitting stateful operator.
    OperatorRowid,
    /// An opaque, fixed-width key owned by an operator. Consumers may carry
    /// it through linear edges or use it to build an arrangement, but must
    /// not interpret its slots as SQL binding rowids.
    OperatorKey(usize),
}

impl EphemeralDelta {
    fn identity_width(&self) -> usize {
        self.identity.width()
    }
}

impl DeltaIdentity {
    fn width(self) -> usize {
        match self {
            Self::BindingRowids(width) => width,
            Self::OperatorRowid => 1,
            Self::OperatorKey(width) => width,
        }
    }
}

/// A maintained integral a stateful operator may probe.
///
/// Base tables, aggregate state, and explicit operator-output arrangements
/// share one logical contract: open one storage relation, map logical values
/// to physical columns, and optionally expose a signed multiplicity.
#[derive(Debug, Clone)]
struct ArrangementHandle {
    table: Arc<BTreeTable>,
    /// Physical table columns corresponding to the operator's logical output
    /// schema. Base arrangements are identity-mapped; aggregate state skips
    /// accumulator payload columns and explicit output arrangements skip
    /// their source-identity prefix.
    value_columns: Arc<[usize]>,
    /// `None` when each physical row has multiplicity one.
    count_column: Option<usize>,
}

impl ArrangementHandle {
    fn table(&self) -> &Arc<BTreeTable> {
        &self.table
    }

    fn value_columns(&self) -> &[usize] {
        &self.value_columns
    }

    fn count_column(&self) -> Option<usize> {
        self.count_column
    }
}

fn btree_arrangement(
    table: Arc<BTreeTable>,
    value_columns: Vec<usize>,
    count_column: Option<usize>,
) -> ArrangementHandle {
    ArrangementHandle {
        table,
        value_columns: value_columns.into(),
        count_column,
    }
}

fn base_arrangement(table: Arc<BTreeTable>, count_column: Option<usize>) -> ArrangementHandle {
    let value_columns = (0..table.columns().len()).collect();
    btree_arrangement(table, value_columns, count_column)
}

/// The terminal consumer of an operator's output delta.
#[derive(Debug, Clone)]
enum DeltaSink {
    View { root_page: i64, num_columns: usize },
    Ephemeral(EphemeralDelta),
}

impl DeltaSink {
    fn view(root_page: i64, num_columns: usize) -> Self {
        Self::View {
            root_page,
            num_columns,
        }
    }

    fn ephemeral(&self) -> Result<&EphemeralDelta> {
        match self {
            Self::Ephemeral(channel) => Ok(channel),
            Self::View { .. } => Err(LimboError::InternalError(
                "operator requires an ephemeral delta sink".to_string(),
            )),
        }
    }

    fn source(&self) -> Result<DeltaSource> {
        Ok(DeltaSource::Ephemeral(self.ephemeral()?.clone()))
    }
}

fn open_ephemeral_delta(
    program: &mut ProgramBuilder,
    name: &str,
    schema: dag::StreamSchema,
    identity: DeltaIdentity,
    requires_positive_first: bool,
) -> DeltaSink {
    let schema = Arc::new(schema);
    let width = schema.len();
    let identity_width = identity.width();
    let table = Arc::new(synthesized_view_table(name, 0, identity_width + width));
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });
    let channel = EphemeralDelta {
        cursor_id,
        identity,
        value_start: identity_width,
        width,
        weight_column: identity_width + width,
        schema,
        requires_positive_first,
    };
    DeltaSink::Ephemeral(channel)
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
            make_joined_table(&binding.table, &binding.identifier, &[], phase_id)
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
    let DeltaIdentity::BindingRowids(identity_width) = channel.identity else {
        return Ok(());
    };
    turso_assert!(
        identity_width == channel.schema.bindings.len(),
        "binding-rowid identity must have one rowid per binding",
        {
            "identity_width": identity_width,
            "binding_count": channel.schema.bindings.len()
        }
    );
    for (identity_column, binding) in channel
        .schema
        .bindings
        .iter()
        .take(identity_width)
        .enumerate()
    {
        if !binding.table.has_rowid {
            continue;
        }
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: identity_column,
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

/// Maximum number of tables in a join view: maintenance runs one delta phase
/// per non-empty subset of the joined tables, so programs grow as 2^N - 1.
pub const MAX_JOIN_TABLES: usize = 4;

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
    let mut using = Vec::with_capacity(joined_tables.len());
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
        using.push(
            joined
                .join_info
                .as_ref()
                .map(|join| {
                    join.using
                        .iter()
                        .map(|name| name.as_str().to_string())
                        .collect()
                })
                .unwrap_or_default(),
        );
    }

    let kind = if joined_tables.iter().any(|table| {
        table
            .join_info
            .as_ref()
            .is_some_and(|join| join.join_type == crate::translate::plan::JoinType::LeftOuter)
    }) {
        if inputs.len() != 2 {
            return unsupported("LEFT JOIN over more than two tables");
        }
        dag::JoinKind::LeftOuter
    } else {
        dag::JoinKind::Inner
    };
    let mut on = Vec::new();
    let mut filters = Vec::new();
    for term in &plan.where_clause {
        let expr = term.expr.clone();
        if inputs.len() > 1 && (kind == dag::JoinKind::Inner || term.from_outer_join.is_some()) {
            on.push(expr);
        } else {
            filters.push(expr);
        }
    }

    let mut node = if inputs.len() == 1 {
        inputs[0]
    } else {
        builder.push(dag::OpNode::Join {
            inputs,
            using,
            on,
            kind,
        })?
    };
    if let Some(predicate) = combine_predicates(filters) {
        node = builder.push(dag::OpNode::Filter {
            input: node,
            predicate,
        })?;
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
            let normalized = having.iter().cloned().collect::<Vec<_>>();
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

/// Persistent storage assigned to one DAG node.
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
    /// Identity emitted by the relational operator before an optional output
    /// arrangement.
    emitted_identity: DeltaIdentity,
    /// Identity published on the node's DAG edge. An explicit arrangement
    /// replaces the producer identity with its own stable rowid.
    output_identity: DeltaIdentity,
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
    match operator_states.for_node(node_id)?.output_identity {
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
        dag::OpNode::Join {
            inputs,
            using,
            on,
            kind,
        } => {
            validate_join_node(dag, inputs, using, on, *kind, operator_states)?;
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
    inputs: &[dag::NodeId],
    using: &[Vec<String>],
    predicates: &[ast::Expr],
    kind: dag::JoinKind,
    operator_states: &OperatorStateCatalog,
) -> Result<()> {
    if inputs.len() > MAX_JOIN_TABLES {
        return unsupported(&format!("joins over more than {MAX_JOIN_TABLES} tables"));
    }
    if kind == dag::JoinKind::LeftOuter && inputs.len() != 2 {
        return Err(LimboError::InternalError(
            "a LEFT JOIN maintenance node must be binary".to_string(),
        ));
    }
    let mut identifiers = Vec::with_capacity(inputs.len());
    for &input in inputs {
        if !operator_states
            .for_node(input)?
            .exposes_arrangement(&dag.nodes[input])
        {
            return Err(LimboError::InternalError(format!(
                "join input DAG node {input} has no declared arrangement"
            )));
        }
        let bindings = &dag.output_schema(input).bindings;
        if bindings.len() != 1 {
            return unsupported("join inputs with multiple logical namespaces");
        }
        identifiers.push(crate::util::normalize_ident(&bindings[0].identifier));
    }
    if using.iter().any(|columns| !columns.is_empty()) {
        identifiers.sort();
        if identifiers.windows(2).any(|pair| pair[0] == pair[1]) {
            return unsupported("NATURAL or USING joins between tables with the same name");
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
                .output_identity
        }
        dag::OpNode::Alias { input, .. } => {
            let upstream = prior_states
                .get(*input)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "maintenance DAG input node {input} has no identity contract"
                    ))
                })?
                .output_identity;
            alias_identity(upstream)
        }
        dag::OpNode::Join {
            inputs,
            kind: dag::JoinKind::Inner,
            ..
        } => DeltaIdentity::BindingRowids(inputs.len()),
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
                    dag.output_schema(node_id).len(),
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
                )?,
                table_name,
            })
        } else {
            None
        };
        let output_identity = if arrangement_table.is_some() {
            DeltaIdentity::OperatorRowid
        } else {
            emitted_identity
        };
        let state = OperatorStateDef {
            node_id,
            emitted_identity,
            output_identity,
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
    columns.push("mult".to_string());
    let key = columns[..identity_width + value_width].to_vec();
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
    output_width: usize,
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
        // one scan rowid or an N-way join identity tuple.
        dag::OpNode::SetOp { .. } => Ok(format!(
            "CREATE TABLE {multiset_table_ident} (branch, source_identity, mult, PRIMARY KEY (branch, source_identity))"
        )),
        // A LEFT JOIN view's per-left-row bookkeeping: signed presence,
        // count of ON-matches, and whether its NULL-padded row is currently
        // published. The payload preserves that image so downstream
        // operators can consume its retraction after the left row departs.
        dag::OpNode::Join { .. } => {
            let payload = (0..output_width)
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
    let dag::OpNode::Scan {
        table,
        identifier,
        logical_id,
    } = &dag.nodes[node_id]
    else {
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
            identifier: identifier.clone(),
            logical_id: *logical_id,
            stored_weight_column,
        },
        arrangement: Some(base_arrangement(table.clone(), stored_weight_column)),
    })
}

fn join_contract_from_inputs(
    inputs: &[NodeOutput],
    using: &[Vec<String>],
    on: &[ast::Expr],
) -> Result<JoinContract> {
    turso_assert!(
        inputs.len() == using.len(),
        "join USING metadata must be parallel to its inputs"
    );
    let mut deltas = Vec::with_capacity(inputs.len());
    let mut tables = Vec::with_capacity(inputs.len());
    let mut arrangements = Vec::with_capacity(inputs.len());
    for (position, input) in inputs.iter().enumerate() {
        let (table, identifier, logical_id) = input.delta.sole_binding()?;
        let arrangement = input.arrangement.clone().ok_or_else(|| {
            LimboError::InternalError("join input has no materialized arrangement".to_string())
        })?;
        if arrangement.value_columns().len() != table.columns().len() {
            return Err(LimboError::InternalError(
                "join arrangement does not match its logical input width".to_string(),
            ));
        }
        deltas.push(input.delta.clone());
        tables.push((table, identifier, using[position].clone(), logical_id));
        arrangements.push(arrangement);
    }
    Ok(JoinContract {
        deltas,
        tables,
        arrangements,
        on: on.to_vec(),
    })
}

/// Integrate one node's declared delta into its persistent output
/// arrangement and republish the same `(values, weight)` changes with the
/// arrangement rowid as their stable identity.
///
/// The source identity is only the key used to find the arrangement row. The
/// downstream identity is always the arrangement's own rowid, so consumers do
/// not need to know whether the producer was a scan, join, aggregate, or
/// another arranged operator.
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
    turso_assert!(
        output.identity == DeltaIdentity::OperatorRowid
            && output.value_start == 1
            && output.width == input.width
            && output.weight_column == input.width + 1,
        "output arrangement must publish rowid, values, and weight"
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
    let expected_columns = identity_width + input.width + 1;
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
    // [source identity..., persisted values..., multiplicity].
    let state_record_start = program.alloc_registers(expected_columns);
    let state_mult_reg = state_record_start + key_width;
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    // [arrangement rowid, delta values..., delta weight].
    let output_record_start = program.alloc_registers(1 + input.width + 1);
    let output_values_start = output_record_start + 1;
    let output_weight_reg = output_values_start + input.width;
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
        column: key_width,
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
    program.emit_insn(Insn::Copy {
        src_reg: arrangement_rowid_reg,
        dst_reg: output_record_start,
        extra_amount: 0,
    });
    if input.width > 0 {
        program.emit_insn(Insn::Copy {
            src_reg: input_values_start,
            dst_reg: output_values_start,
            extra_amount: input.width - 1,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: output_weight_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: output_record_start as u16,
        count: (1 + input.width + 1) as u16,
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
    Ok(btree_arrangement(
        table,
        value_columns,
        Some(identity_width + input.width),
    ))
}

#[allow(clippy::too_many_arguments)]
fn materialize_declared_output_arrangement(
    program: &mut ProgramBuilder,
    view_name: &str,
    dag: &dag::MaintenanceDag,
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
        dag.output_schema(node_id),
        maintenance_input,
    )?;
    let expected_identity = operator_state.emitted_identity;
    if input.identity != expected_identity {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} emits {:?}, expected {:?}",
            input.identity, expected_identity
        )));
    }
    let arranged_sink = open_ephemeral_delta(
        program,
        &format!("{view_name}_arranged_delta_{node_id}"),
        dag.output_schema(node_id).clone(),
        DeltaIdentity::OperatorRowid,
        input.requires_positive_first,
    );
    let arranged_output = arranged_sink.ephemeral()?;
    let arrangement = emit_output_arrangement(
        program,
        view_name,
        &input,
        &arranged_output,
        &arrangement_def.table_name,
        schema,
    )?;
    Ok(NodeOutput {
        delta: arranged_sink.source()?,
        arrangement: Some(arrangement),
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_ephemeral_filter(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    predicate: &ast::Expr,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<DeltaSource> {
    let sink = open_ephemeral_delta(
        program,
        &format!("{view_name}_filter_delta_{node_id}"),
        input.schema.as_ref().clone(),
        input.identity,
        input.requires_positive_first,
    );
    let output = sink.ephemeral()?;

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
    let record_start = program.alloc_registers(input.identity_width() + input.width + 1);
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

    for column in 0..input.identity_width() {
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
            dest: record_start + input.identity_width() + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + input.identity_width() + input.width,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: (input.identity_width() + input.width + 1) as u16,
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
    sink.source()
}

fn append_alias_values(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
) -> Result<()> {
    if input.width != output.width
        || output.identity != alias_identity(input.identity)
        || output.identity_width() != input.identity_width()
        || output.value_start != output.identity_width()
    {
        return Err(LimboError::InternalError(
            "alias stream copy has incompatible input/output layouts".to_string(),
        ));
    }

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let record_start = program.alloc_registers(output.identity_width() + output.width + 1);
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
        count: (output.identity_width() + output.width + 1) as u16,
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
    output_schema: dag::StreamSchema,
) -> Result<DeltaSource> {
    let sink = open_ephemeral_delta(
        program,
        &format!("{view_name}_alias_delta_{node_id}"),
        output_schema,
        alias_identity(input.identity),
        input.requires_positive_first,
    );
    let output = sink.ephemeral()?;
    append_alias_values(program, input, &output)?;
    sink.source()
}

fn emit_union_all_to_ephemeral(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    inputs: &[EphemeralDelta],
    output_schema: dag::StreamSchema,
) -> Result<DeltaSource> {
    let requires_positive_first = inputs.iter().any(|input| input.requires_positive_first);
    let sink = open_ephemeral_delta(
        program,
        &format!("{view_name}_union_all_delta_{node_id}"),
        output_schema,
        DeltaIdentity::OperatorKey(2),
        requires_positive_first,
    );
    let output = sink.ephemeral()?;
    for (branch, input) in inputs.iter().enumerate() {
        append_union_all_branch(program, branch, input, &output)?;
    }
    sink.source()
}

/// Append one UNION ALL branch while namespacing its complete source identity
/// into the fixed `(branch, packed identity)` key declared by the set-op.
fn append_union_all_branch(
    program: &mut ProgramBuilder,
    branch: usize,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
) -> Result<()> {
    if input.width != output.width
        || input.identity_width() == 0
        || output.identity != DeltaIdentity::OperatorKey(2)
        || output.value_start != 2
    {
        return Err(LimboError::InternalError(
            "UNION ALL branch has an incompatible delta identity".to_string(),
        ));
    }

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let record_start = program.alloc_registers(2 + output.width + 1);
    let source_identity_start = program.alloc_registers(input.identity_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    program.emit_int(branch as i64, record_start);
    for column in 0..input.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: source_identity_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::MakeRecord {
        start_reg: source_identity_start as u16,
        count: input.identity_width() as u16,
        dest_reg: (record_start + 1) as u16,
        index_name: None,
        affinity_str: None,
    });
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
        count: (2 + output.width + 1) as u16,
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
                    dag.output_schema(upstream_id),
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_filter(
                        program, view_name, node_id, &upstream, predicate, schema, connection,
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
                    dag.output_schema(upstream_id),
                    input,
                )?;
                let sink = open_ephemeral_delta(
                    program,
                    &format!("{view_name}_project_delta_{node_id}"),
                    dag.output_schema(node_id).clone(),
                    upstream.identity,
                    upstream.requires_positive_first,
                );
                let output = sink.ephemeral()?;
                emit_ephemeral_project(
                    program,
                    &upstream,
                    projections,
                    &output,
                    schema,
                    connection,
                )?;
                NodeOutput {
                    delta: sink.source()?,
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
                    dag.output_schema(upstream_id),
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_alias(
                        program,
                        view_name,
                        node_id,
                        &upstream,
                        dag.output_schema(node_id).clone(),
                    )?,
                    arrangement: None,
                }
            }
            dag::OpNode::Join {
                inputs,
                using,
                on,
                kind,
            } => {
                let declared_inputs = inputs
                    .iter()
                    .map(|input| {
                        outputs[*input].as_ref().cloned().ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "join DAG node {node_id} input {input} was not compiled"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let contract = join_contract_from_inputs(&declared_inputs, using, on)?;
                if *kind == dag::JoinKind::LeftOuter {
                    NodeOutput {
                        delta: emit_left_join_deltas_to_ephemeral(
                            program,
                            view_name,
                            node_id,
                            &contract,
                            dag.output_schema(node_id).clone(),
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
                            &contract.deltas,
                            &contract.tables,
                            &contract.arrangements,
                            &contract.on,
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
                        dag.output_schema(*upstream_id),
                        input,
                    )?);
                }
                if *prefix_len == 0 {
                    if operators
                        .iter()
                        .any(|operator| *operator != ast::CompoundOperator::UnionAll)
                    {
                        return Err(LimboError::InternalError(
                            "stateless set-op has a deduplicating operator".to_string(),
                        ));
                    }
                    NodeOutput {
                        delta: emit_union_all_to_ephemeral(
                            program,
                            view_name,
                            node_id,
                            &channels,
                            dag.output_schema(node_id).clone(),
                        )?,
                        arrangement: None,
                    }
                } else {
                    let requires_positive_first = channels
                        .iter()
                        .any(|channel| channel.requires_positive_first);
                    let sink = open_ephemeral_delta(
                        program,
                        &format!("{view_name}_set_op_delta_{node_id}"),
                        dag.output_schema(node_id).clone(),
                        DeltaIdentity::OperatorRowid,
                        requires_positive_first,
                    );
                    let output = sink.ephemeral()?;
                    emit_deduplicating_set_op(
                        program,
                        view_name,
                        &channels,
                        operators,
                        *prefix_len,
                        key_collations,
                        &output,
                        operator_states.for_node(node_id)?,
                        schema,
                    )?;
                    NodeOutput {
                        delta: sink.source()?,
                        arrangement: None,
                    }
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
                    dag.output_schema(upstream_id),
                    input,
                )?;
                let sink = open_ephemeral_delta(
                    program,
                    &format!("{view_name}_aggregate_delta_{node_id}"),
                    dag.output_schema(node_id).clone(),
                    DeltaIdentity::OperatorRowid,
                    false,
                );
                let output = sink.ephemeral()?;
                let operator_state = operator_states.for_node(node_id)?;
                emit_group_aggregate(
                    program,
                    view_name,
                    &DeltaSource::Ephemeral(upstream),
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
                    delta: sink.source()?,
                    arrangement: Some(btree_arrangement(state_table, value_columns, None)),
                }
            }
        };
        let operator_state = operator_states.for_node(node_id)?;
        if output.delta.identity() != operator_state.emitted_identity {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} emitted {:?}, but its declared identity is {:?}",
                output.delta.identity(),
                operator_state.emitted_identity
            )));
        }
        let output = materialize_declared_output_arrangement(
            program,
            view_name,
            dag,
            node_id,
            output,
            input,
            operator_state,
            schema,
        )?;
        if output.delta.identity() != operator_state.output_identity {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} published {:?}, but its edge contract is {:?}",
                output.delta.identity(),
                operator_state.output_identity
            )));
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
    sink: &DeltaSink,
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
    let stream =
        node_output
            .delta
            .materialize(program, view_name, root, dag.output_schema(root), input)?;
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
        &DeltaSink::view(view_root_page, num_view_columns),
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

    let record_start = program.alloc_registers(output.identity_width() + output.width + 1);
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
        count: (output.identity_width() + output.width + 1) as u16,
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

    let identity_start = program.alloc_registers(input.identity_width());
    for column in 0..input.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: identity_start + column,
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

    turso_assert!(
        output.identity == input.identity
            && output.value_start == input.identity_width()
            && output.weight_column == input.identity_width() + num_output_columns,
        "linear projection sink must preserve its input identity"
    );
    debug_assert_eq!(identity_start + input.identity_width(), out_start);
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: new_weight_reg,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: identity_start as u16,
        count: (input.identity_width() + num_output_columns + 1) as u16,
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
    sink: &DeltaSink,
) -> Result<()> {
    turso_assert!(
        matches!(
            input.identity,
            DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
        ) && input.value_start == 1,
        "terminal delta streams require one stable operator identity"
    );
    let num_output_columns = match sink {
        DeltaSink::View { num_columns, .. } => *num_columns,
        DeltaSink::Ephemeral(_) => {
            return Err(LimboError::InternalError(
                "terminal delta adapter received an interior sink".to_string(),
            ));
        }
    };
    turso_assert!(
        input.width == num_output_columns,
        "terminal delta width does not match its sink"
    );

    let view_cursor_id = if let DeltaSink::View { root_page, .. } = sink {
        let table = Arc::new(synthesized_view_table(
            view_name,
            *root_page,
            num_output_columns,
        ));
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(*root_page),
            db: 0,
        });
        Some(cursor_id)
    } else {
        None
    };

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

    match sink {
        DeltaSink::View { .. } => {
            let view_cursor_id = view_cursor_id.expect("view sink opened its btree cursor");
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
        DeltaSink::Ephemeral(_) => unreachable!("rejected above"),
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

#[allow(clippy::too_many_arguments)]
fn emit_group_aggregate(
    program: &mut ProgramBuilder,
    view_name: &str,
    source: &DeltaSource,
    group_exprs: &[ast::Expr],
    group_collations: &[CollationSeq],
    aggregates: &[Aggregate],
    scalar: bool,
    output: &EphemeralDelta,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    use crate::function::AccumulatorFunc;
    let num_view_columns = output.width;
    // With a collated key, values that compare equal can differ in bytes.
    // The stored key is the group's first-seen representative: reloading it
    // on every lookup keeps the state row, its index entry, and the view row
    // byte-identical, and matches the value batch GROUP BY displays. With
    // binary keys a matched lookup implies byte equality, so the reload is
    // skipped.
    let reload_stored_group = group_collations
        .iter()
        .any(|c| !matches!(c, CollationSeq::Binary | CollationSeq::Unset));

    turso_assert!(
        num_view_columns == group_exprs.len() + aggregates.len(),
        "aggregate output stream must use the node's natural schema"
    );
    let k = group_exprs.len();
    // Payload aggregates persist fixed-width state in the group state table;
    // MIN/MAX keep a value multiset in the multiset table instead
    // (payload_widths[i] is None for them, and they have no accumulator).
    // DISTINCT aggregates have both: the accumulator payload plus multiset
    // rows deciding which values reach the accumulator.
    let payload_widths: Vec<Option<usize>> = aggregates
        .iter()
        .map(|agg| crate::vdbe::execute::agg_payload_width(&agg.func))
        .collect();
    let total_payload: usize = payload_widths.iter().flatten().sum();
    // Offset of each payload aggregate's slots within the payload block.
    let payload_offsets: Vec<Option<usize>> = payload_widths
        .iter()
        .scan(0usize, |off, w| {
            Some(w.map(|w| {
                let this = *off;
                *off += w;
                this
            }))
        })
        .collect();
    let has_multiset = aggregates.iter().any(uses_multiset);

    // The state table and its primary-key index are real schema objects
    // created by CREATE MATERIALIZED VIEW and assigned to this DAG node.
    let state_table_name = operator_state.state_table_name()?.to_string();
    let state_table = schema.get_btree_table(&state_table_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "state table {state_table_name} of materialized view {view_name} not found"
        ))
    })?;
    let state_index = schema
        .get_indices(&state_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "state table {state_table_name} of materialized view {view_name} has no index"
            ))
        })?;

    // The value multiset table, when the view has MIN/MAX or DISTINCT
    // aggregates.
    let multiset_table_name = operator_state
        .auxiliary_table
        .as_ref()
        .map(|table| table.table_name.clone());
    let mm_schema = if has_multiset {
        let multiset_table_name = multiset_table_name.as_ref().ok_or_else(|| {
            LimboError::InternalError(format!(
                "aggregate DAG node {} requires a multiset table",
                operator_state.node_id
            ))
        })?;
        let table = schema.get_btree_table(multiset_table_name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "multiset table {multiset_table_name} of materialized view {view_name} not found"
            ))
        })?;
        let index = schema
            .get_indices(multiset_table_name)
            .next()
            .cloned()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "multiset table {multiset_table_name} of materialized view {view_name} has no index"
                ))
            })?;
        Some((table, index))
    } else {
        None
    };

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

    let channel = match source {
        DeltaSource::Ephemeral(channel) => channel.clone(),
        DeltaSource::BaseTable { .. } => {
            return Err(LimboError::InternalError(
                "aggregate base-table input was not emitted to a typed stream".to_string(),
            ));
        }
    };
    turso_assert!(
        channel.weight_column == channel.value_start + channel.width
            && channel.schema.len() == channel.width,
        "aggregate delta channel layout must match its declared input"
    );
    let input_cursor_id = channel.cursor_id;
    let (table_references, binding_remap) = stream_table_references(program, &channel.schema);

    let state_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor_id,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let state_index_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeIndex(state_index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_index_cursor_id,
        root_page: RegisterOrLiteral::Literal(state_index.root_page),
        db: 0,
    });
    let mut mm_cursors = None;
    let mut mm_index_name = String::new();
    if let Some((mm_table, mm_index)) = &mm_schema {
        let mm_table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(mm_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: mm_table_cursor_id,
            root_page: RegisterOrLiteral::Literal(mm_table.root_page),
            db: 0,
        });
        let mm_index_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(mm_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: mm_index_cursor_id,
            root_page: RegisterOrLiteral::Literal(mm_index.root_page),
            db: 0,
        });
        mm_cursors = Some((mm_table_cursor_id, mm_index_cursor_id));
        mm_index_name.clone_from(&mm_index.name);
    }

    // Register layout.
    let weight_reg = program.alloc_register();
    let null_arg_reg = program.alloc_register(); // stays NULL: COUNT(*) argument
    let zero_reg = program.alloc_register();
    let found_reg = program.alloc_register();
    let cnt_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    let arg_reg = program.alloc_register();
    // Accumulators, one per aggregate (index 0 = hidden liveness COUNT(*)).
    let acc_start = program.alloc_registers(aggregates.len());
    // State record image: [g.., finalized aggregate values, payloads..].
    // Keeping the natural output first makes the state table directly
    // scannable as the aggregate node's arrangement.
    let state_rec_start = program.alloc_registers(k + total_payload + aggregates.len());
    let group_start = state_rec_start;
    let persisted_value_start = state_rec_start + k;
    let payload_start = persisted_value_start + aggregates.len();
    // Index key image: [g.., state_rowid], contiguous for IdxInsert/IdxDelete.
    // The state rowid doubles as the group's view-btree rowid (see
    // aggregate_state_table_sql).
    let index_rec_start = program.alloc_registers(k + 1);
    let state_rowid_reg = index_rec_start + k;
    // Finalized value of each aggregate for the current group.
    let agg_value_start = program.alloc_registers(aggregates.len());
    // Natural aggregate delta: [groups.., aggregate values.., weight].
    let out_start = program.alloc_registers(num_view_columns + 1);
    // Natural aggregate row used when publishing an old arrangement row:
    // [groups.., aggregate values.., weight].
    let old_out_start = program.alloc_registers(k + aggregates.len() + 1);
    let old_out_weight_reg = old_out_start + k + aggregates.len();
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    // Multiset record image: [agg_id, g.., val, mult-or-rowid], contiguous.
    let mm_rec_start = program.alloc_registers(1 + k + 2);
    let mm_val_reg = mm_rec_start + 1 + k;
    let mm_last_reg = mm_rec_start + 1 + k + 1;
    let mm_rowid_reg = program.alloc_register();
    let mm_mult_reg = program.alloc_register();
    let mm_record_reg = program.alloc_register();
    let mm_found_reg = program.alloc_register();

    program.emit_insn(Insn::Null {
        dest: null_arg_reg,
        dest_end: None,
    });
    program.emit_int(0, zero_reg);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    // Joined-delta rows apply in two passes: every positive contribution,
    // then every negative one. The single-table capture order guarantees a
    // group's running weight never dips below zero, but the join phase
    // decomposition does not — a pair that exists only across generations
    // contributes its retraction before its insertion, and the
    // unknown-group-retraction no-op below would drop it, leaving the later
    // positive over-counted. Positives-first restores the invariant: every
    // group's (and multiset value's) running weight stays non-negative, and
    // since aggregate contributions carry their own values, reordering them
    // is sound.
    let pass_reg = channel.requires_positive_first.then(|| {
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
        cursor_id: input_cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    // Accumulators must start fresh for every input row.
    program.emit_insn(Insn::Null {
        dest: acc_start,
        dest_end: Some(acc_start + aggregates.len() - 1),
    });

    // Weight of the current input row.
    program.emit_insn(Insn::Column {
        cursor_id: input_cursor_id,
        column: channel.weight_column,
        dest: weight_reg,
        default: None,
    });
    // Pass filter: pass 0 skips negative contributions, pass 1 skips
    // positive ones.
    if let Some(pass_reg) = pass_reg {
        let negative_pass_label = program.allocate_label();
        let pass_ok_label = program.allocate_label();
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

    seed_ephemeral_stream_cache(program, &channel, &binding_remap, &mut resolver)?;

    for (i, group_expr) in group_exprs.iter().enumerate() {
        let bound = remap_bound_expr(group_expr, &binding_remap)?;
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            &bound,
            group_start + i,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }

    // Group lookup via the state table's primary-key index.
    let found_label = program.allocate_label();
    let apply_label = program.allocate_label();
    program.emit_insn(Insn::Found {
        cursor_id: state_index_cursor_id,
        target_pc: found_label,
        record_reg: group_start,
        num_regs: k,
    });
    // Not found: a retraction for a group the state does not know is a no-op
    // (matching the filter/project merge behavior for unknown rowids).
    program.emit_insn(Insn::Lt {
        lhs: weight_reg,
        rhs: zero_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(0, found_reg);
    program.emit_insn(Insn::Null {
        dest: persisted_value_start,
        dest_end: Some(persisted_value_start + aggregates.len() - 1),
    });
    // A fresh group's DISTINCT accumulators may never be stepped in the apply
    // loop (a NULL argument contributes nothing), but the write path stores
    // every payload accumulator, so materialize empty contexts now. Stepping
    // a NULL argument initializes the context without contributing to it.
    for (i, agg) in aggregates.iter().enumerate() {
        if tracks_distinct_values(agg) {
            program.emit_insn(Insn::AggStep {
                acc_reg: acc_start + i,
                col: null_arg_reg,
                delimiter: 0,
                func: AccumulatorFunc::Agg(agg.func.clone()),
                comparator: None,
            });
        }
    }
    program.emit_insn(Insn::Goto {
        target_pc: apply_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_int(1, found_reg);
    program.emit_insn(Insn::IdxRowId {
        cursor_id: state_index_cursor_id,
        dest: state_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: state_cursor_id,
        src_reg: state_rowid_reg,
        target_pc: corrupt_label,
    });
    if reload_stored_group {
        for i in 0..k {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: i,
                dest: group_start + i,
                default: None,
            });
        }
    }
    for (i, agg) in aggregates.iter().enumerate() {
        let Some(offset) = payload_offsets[i] else {
            continue; // MIN/MAX: state lives in the multiset table
        };
        for j in 0..payload_widths[i].expect("offset implies width") {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + aggregates.len() + offset + j,
                dest: payload_start + offset + j,
                default: None,
            });
        }
        program.emit_insn(Insn::AggContextLoad {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + offset,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }
    for i in 0..aggregates.len() {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor_id,
            column: k + i,
            dest: persisted_value_start + i,
            default: None,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: group_start,
        dst_reg: old_out_start,
        extra_amount: k.saturating_sub(1),
    });
    program.emit_insn(Insn::Copy {
        src_reg: persisted_value_start,
        dst_reg: old_out_start + k,
        extra_amount: aggregates.len().saturating_sub(1),
    });
    program.emit_int(-1, old_out_weight_reg);
    emit_ephemeral_row_with_identity(
        program,
        output,
        state_rowid_reg,
        old_out_start,
        old_out_weight_reg,
    );

    program.preassign_label_to_next_insn(apply_label);
    for (i, agg) in aggregates.iter().enumerate() {
        let is_minmax = matches!(agg.func, AggFunc::Min | AggFunc::Max);
        let is_distinct = tracks_distinct_values(agg);
        let col_reg = match agg.args.first() {
            Some(arg_expr) => {
                let bound = remap_bound_expr(arg_expr, &binding_remap)?;
                translate_expr_no_constant_opt(
                    program,
                    Some(&table_references),
                    &bound,
                    arg_reg,
                    &resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                arg_reg
            }
            None => null_arg_reg,
        };

        if is_minmax || is_distinct {
            // Weight-merge the value into the aggregate's multiset:
            // (agg_id, group.., value) -> multiplicity. NULLs are ignored,
            // matching aggregate NULL semantics. For a DISTINCT aggregate the
            // accumulator steps only when a value's multiplicity transitions
            // 0 -> positive and inverts only on positive -> 0, so it sees
            // each distinct value exactly once. Transition detection relies
            // on delta rows carrying unit weights (each captured DML op is
            // one row of weight +1 or -1, and population scans weigh +1).
            let (mm_table_cursor_id, mm_index_cursor_id) = mm_cursors
                .expect("DAG validation guarantees a multiset table for multiset aggregates");
            let done_label = program.allocate_label();
            let mm_found_label = program.allocate_label();
            let mm_upsert_label = program.allocate_label();

            program.emit_insn(Insn::IsNull {
                reg: col_reg,
                target_pc: done_label,
            });
            // Key image: [agg_id, g.., val].
            program.emit_int(i as i64, mm_rec_start);
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: mm_rec_start + 1,
                extra_amount: k.saturating_sub(1),
            });
            program.emit_insn(Insn::Copy {
                src_reg: col_reg,
                dst_reg: mm_val_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Found {
                cursor_id: mm_index_cursor_id,
                target_pc: mm_found_label,
                record_reg: mm_rec_start,
                num_regs: 1 + k + 1,
            });
            // Not found: retraction of an unknown value is a no-op.
            program.emit_insn(Insn::Lt {
                lhs: weight_reg,
                rhs: zero_reg,
                target_pc: done_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            program.emit_int(0, mm_found_reg);
            program.emit_insn(Insn::NewRowid {
                cursor: mm_table_cursor_id,
                rowid_reg: mm_rowid_reg,
                prev_largest_reg: prev_rowid_scratch,
            });
            program.emit_insn(Insn::Copy {
                src_reg: weight_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            if is_distinct {
                // First occurrence of this value in the group.
                program.emit_insn(Insn::AggStep {
                    acc_reg: acc_start + i,
                    col: mm_val_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::Goto {
                target_pc: mm_upsert_label,
            });

            program.preassign_label_to_next_insn(mm_found_label);
            program.emit_int(1, mm_found_reg);
            program.emit_insn(Insn::IdxRowId {
                cursor_id: mm_index_cursor_id,
                dest: mm_rowid_reg,
            });
            program.emit_insn(Insn::SeekRowid {
                cursor_id: mm_table_cursor_id,
                src_reg: mm_rowid_reg,
                target_pc: corrupt_label,
            });
            program.emit_insn(Insn::Column {
                cursor_id: mm_table_cursor_id,
                column: 1 + k + 1,
                dest: mm_mult_reg,
                default: None,
            });
            program.emit_insn(Insn::Add {
                lhs: weight_reg,
                rhs: mm_mult_reg,
                dest: mm_last_reg,
            });
            program.emit_insn(Insn::Ne {
                lhs: mm_last_reg,
                rhs: zero_reg,
                target_pc: mm_upsert_label,
                flags: CmpInsFlags::default(),
                collation: None,
            });
            // Multiplicity reached zero: remove the value.
            if is_distinct {
                // Last occurrence of this value left the group.
                program.emit_insn(Insn::AggInverse {
                    acc_reg: acc_start + i,
                    col: mm_val_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: mm_rowid_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: mm_rec_start,
                num_regs: 1 + k + 2,
                cursor_id: mm_index_cursor_id,
                raise_error_if_no_matching_entry: true,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: mm_table_cursor_id,
                table_name: multiset_table_name
                    .as_ref()
                    .expect("multiset users require an auxiliary table")
                    .clone(),
                is_part_of_update: true,
            });
            program.emit_insn(Insn::Goto {
                target_pc: done_label,
            });

            // Write (agg_id, g.., val, mult) at the row's rowid; a fresh
            // value also gets an index entry (agg_id, g.., val, rowid).
            program.preassign_label_to_next_insn(mm_upsert_label);
            program.emit_insn(Insn::MakeRecord {
                start_reg: mm_rec_start as u16,
                count: (1 + k + 2) as u16,
                dest_reg: mm_record_reg as u16,
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::Insert {
                cursor: mm_table_cursor_id,
                key_reg: mm_rowid_reg,
                record_reg: mm_record_reg,
                flag: InsertFlags(
                    InsertFlags::REQUIRE_SEEK
                        | InsertFlags::SKIP_LAST_ROWID
                        | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
                ),
                table_name: multiset_table_name
                    .as_ref()
                    .expect("multiset users require an auxiliary table")
                    .clone(),
            });
            // Only a freshly-inserted value needs an index entry; an
            // existing value's entry is already present.
            let mm_skip_idx_label = program.allocate_label();
            program.emit_insn(Insn::If {
                reg: mm_found_reg,
                target_pc: mm_skip_idx_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Copy {
                src_reg: mm_rowid_reg,
                dst_reg: mm_last_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: mm_rec_start as u16,
                count: (1 + k + 2) as u16,
                dest_reg: mm_record_reg as u16,
                index_name: Some(mm_index_name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: mm_index_cursor_id,
                record_reg: mm_record_reg,
                unpacked_start: Some(mm_rec_start),
                unpacked_count: Some((1 + k + 2) as u16),
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(mm_skip_idx_label);
            program.preassign_label_to_next_insn(done_label);
            continue;
        }

        let step_label = program.allocate_label();
        let after_label = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: step_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::AggInverse {
            acc_reg: acc_start + i,
            col: col_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(agg.func.clone()),
            comparator: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: after_label,
        });
        program.preassign_label_to_next_insn(step_label);
        program.emit_insn(Insn::AggStep {
            acc_reg: acc_start + i,
            col: col_reg,
            delimiter: 0,
            func: AccumulatorFunc::Agg(agg.func.clone()),
            comparator: None,
        });
        program.preassign_label_to_next_insn(after_label);
    }

    // Group liveness: the hidden COUNT(*) is zero when every row of the
    // group has been retracted. A scalar aggregate's single group is never
    // deleted — retracting the last row rewrites its row with empty-input
    // aggregate values instead.
    if !scalar {
        program.emit_insn(Insn::AggValue {
            acc_reg: acc_start,
            dest_reg: cnt_reg,
            func: AccumulatorFunc::Agg(AggFunc::Count0),
        });
        let write_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: cnt_reg,
            rhs: zero_reg,
            target_pc: write_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        // Group emptied: remove its state row and index entry. The old
        // arrangement row was already retracted above. A
        // fresh group cannot reach zero (its first change was an insert), so
        // found_reg is always set here; stay total anyway.
        let do_delete_label = program.allocate_label();
        program.emit_insn(Insn::If {
            reg: found_reg,
            target_pc: do_delete_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(do_delete_label);
        if k > 1 {
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: index_rec_start,
                extra_amount: k - 1,
            });
        } else {
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: index_rec_start,
                extra_amount: 0,
            });
        }
        program.emit_insn(Insn::IdxDelete {
            start_reg: index_rec_start,
            num_regs: k + 1,
            cursor_id: state_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: state_cursor_id,
            table_name: state_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(write_label);
    }

    // Group live: persist aggregate state and publish its natural delta.
    let have_rowids_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: have_rowids_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: state_cursor_id,
        rowid_reg: state_rowid_reg,
        prev_largest_reg: prev_rowid_scratch,
    });
    program.preassign_label_to_next_insn(have_rowids_label);
    for (i, agg) in aggregates.iter().enumerate() {
        let Some(offset) = payload_offsets[i] else {
            continue; // MIN/MAX: state lives in the multiset table
        };
        program.emit_insn(Insn::AggContextStore {
            acc_reg: acc_start + i,
            payload_start_reg: payload_start + offset,
            func: AccumulatorFunc::Agg(agg.func.clone()),
        });
    }
    let skip_index_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: found_reg,
        target_pc: skip_index_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Copy {
        src_reg: group_start,
        dst_reg: index_rec_start,
        extra_amount: k.saturating_sub(1),
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: index_rec_start as u16,
        count: (k + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(state_index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: state_index_cursor_id,
        record_reg: index_record_reg,
        unpacked_start: Some(index_rec_start),
        unpacked_count: Some((k + 1) as u16),
        flags: IdxInsertFlags::new(),
    });
    program.preassign_label_to_next_insn(skip_index_label);

    // Finalize every aggregate and publish the node's natural row. HAVING is
    // an ordinary downstream Filter node in the maintenance DAG.
    let emit_row_tail = |program: &mut ProgramBuilder| -> Result<()> {
        for (i, agg) in aggregates.iter().enumerate() {
            let value_reg = agg_value_start + i;
            if matches!(agg.func, AggFunc::Min | AggFunc::Max) {
                // The group's extreme is the first (MIN) or last (MAX)
                // multiset entry with the (agg_id, group..) prefix.
                let (_, mm_index_cursor_id) =
                    mm_cursors.expect("MIN/MAX aggregates imply the multiset table");
                let empty_label = program.allocate_label();
                let have_label = program.allocate_label();
                program.emit_int(i as i64, mm_rec_start);
                program.emit_insn(Insn::Copy {
                    src_reg: group_start,
                    dst_reg: mm_rec_start + 1,
                    extra_amount: k.saturating_sub(1),
                });
                match agg.func {
                    AggFunc::Min => {
                        program.emit_insn(Insn::SeekGE {
                            is_index: true,
                            cursor_id: mm_index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: 1 + k,
                            target_pc: empty_label,
                            eq_only: false,
                        });
                        // Positioned past the group: no values.
                        program.emit_insn(Insn::IdxGT {
                            cursor_id: mm_index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: 1 + k,
                            target_pc: empty_label,
                        });
                    }
                    AggFunc::Max => {
                        program.emit_insn(Insn::SeekLE {
                            is_index: true,
                            cursor_id: mm_index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: 1 + k,
                            target_pc: empty_label,
                            eq_only: false,
                        });
                        // Positioned before the group: no values.
                        program.emit_insn(Insn::IdxLT {
                            cursor_id: mm_index_cursor_id,
                            start_reg: mm_rec_start,
                            num_regs: 1 + k,
                            target_pc: empty_label,
                        });
                    }
                    _ => unreachable!(),
                }
                program.emit_insn(Insn::Column {
                    cursor_id: mm_index_cursor_id,
                    column: 1 + k,
                    dest: value_reg,
                    default: None,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: have_label,
                });
                program.preassign_label_to_next_insn(empty_label);
                program.emit_insn(Insn::Null {
                    dest: value_reg,
                    dest_end: None,
                });
                program.preassign_label_to_next_insn(have_label);
            } else {
                program.emit_insn(Insn::AggValue {
                    acc_reg: acc_start + i,
                    dest_reg: value_reg,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                });
            }
        }

        program.emit_insn(Insn::Copy {
            src_reg: agg_value_start,
            dst_reg: persisted_value_start,
            extra_amount: aggregates.len().saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + total_payload + aggregates.len()) as u16,
            dest_reg: state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: state_rowid_reg,
            record_reg: state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });

        if k > 0 {
            program.emit_insn(Insn::Copy {
                src_reg: group_start,
                dst_reg: out_start,
                extra_amount: k - 1,
            });
        }
        if !aggregates.is_empty() {
            program.emit_insn(Insn::Copy {
                src_reg: agg_value_start,
                dst_reg: out_start + k,
                extra_amount: aggregates.len() - 1,
            });
        }
        program.emit_int(1, out_start + num_view_columns);
        emit_ephemeral_row_with_identity(
            program,
            output,
            state_rowid_reg,
            out_start,
            out_start + num_view_columns,
        );
        Ok(())
    };

    emit_row_tail(program)?;

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
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
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);

    // Batch semantics emit one row for a scalar aggregate even over empty
    // input, so population must leave the single group present when the scan
    // contributed nothing (empty table, or WHERE filtered every row).
    // Maintenance never needs this: the row is created here and, with the
    // group-death path disabled for scalar shapes, never deleted.
    if scalar && input == MaintenanceInput::BaseTable {
        let ensure_done = program.allocate_label();
        // The synthetic group key is the constant 0 by construction.
        program.emit_int(0, group_start);
        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: ensure_done,
            record_reg: group_start,
            num_regs: k,
        });
        // Fresh, empty accumulators: stepping a NULL argument materializes
        // each payload context without contributing to it — except COUNT(*),
        // which counts rows regardless of argument, so its init step is
        // cancelled with the matching inverse.
        program.emit_insn(Insn::Null {
            dest: acc_start,
            dest_end: Some(acc_start + aggregates.len() - 1),
        });
        for (i, agg) in aggregates.iter().enumerate() {
            if payload_offsets[i].is_none() {
                continue; // MIN/MAX: an empty multiset already means NULL
            }
            program.emit_insn(Insn::AggStep {
                acc_reg: acc_start + i,
                col: null_arg_reg,
                delimiter: 0,
                func: AccumulatorFunc::Agg(agg.func.clone()),
                comparator: None,
            });
            if matches!(agg.func, AggFunc::Count0) {
                program.emit_insn(Insn::AggInverse {
                    acc_reg: acc_start + i,
                    col: null_arg_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(agg.func.clone()),
                    comparator: None,
                });
            }
            program.emit_insn(Insn::AggContextStore {
                acc_reg: acc_start + i,
                payload_start_reg: payload_start + payload_offsets[i].expect("checked above"),
                func: AccumulatorFunc::Agg(agg.func.clone()),
            });
        }
        program.emit_insn(Insn::Null {
            dest: persisted_value_start,
            dest_end: Some(persisted_value_start + aggregates.len() - 1),
        });
        program.emit_insn(Insn::NewRowid {
            cursor: state_cursor_id,
            rowid_reg: state_rowid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: group_start,
            dst_reg: index_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: index_rec_start as u16,
            count: (k + 1) as u16,
            dest_reg: index_record_reg as u16,
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: index_record_reg,
            unpacked_start: Some(index_rec_start),
            unpacked_count: Some((k + 1) as u16),
            flags: IdxInsertFlags::new(),
        });
        emit_row_tail(program)?;
        program.preassign_label_to_next_insn(ensure_done);
    }

    drop(syms);
    Ok(())
}

/// Which relation a join phase reads for one side of the join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    /// The transaction's captured delta for the table.
    Delta,
    /// The table's btree, which at maintenance time holds the post-change
    /// state.
    Btree,
}

/// The delta phases of an N-way inner join. With the base btrees holding
/// post-change state when the batch is applied, the view delta is the
/// inclusion–exclusion sum over every non-empty subset S of the joined
/// tables: join the deltas of the tables in S against the post-state btrees
/// of the rest, signed (-1)^(|S|+1). The two-table case is the familiar
/// `d(L ⋈ R) = dL ⋈ R_new + L_new ⋈ dR − dL ⋈ dR`. Population is a single
/// all-btree phase with weight +1.
fn join_subset_phases(n_tables: usize, input: MaintenanceInput) -> Vec<(Vec<JoinSide>, bool)> {
    match input {
        MaintenanceInput::BaseTable => vec![(vec![JoinSide::Btree; n_tables], false)],
        MaintenanceInput::TransactionDelta => (1u32..(1 << n_tables))
            .map(|mask| {
                let sides = (0..n_tables)
                    .map(|i| {
                        if mask & (1 << i) != 0 {
                            JoinSide::Delta
                        } else {
                            JoinSide::Btree
                        }
                    })
                    .collect();
                (sides, mask.count_ones() % 2 == 0)
            })
            .collect(),
    }
}

/// A [`JoinedTable`] scan entry for synthesized maintenance-program table
/// references. `using` carries the table's merged USING/NATURAL column
/// names so unqualified references to them bind without ambiguity, exactly
/// as in the defining query.
fn make_joined_table(
    table: &Arc<BTreeTable>,
    identifier: &str,
    using: &[String],
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
        join_info: (!using.is_empty()).then(|| JoinInfo {
            join_type: crate::translate::plan::JoinType::Inner,
            using: using.iter().map(|n| ast::Name::exact(n.clone())).collect(),
            no_reorder: false,
        }),
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: 0,
        indexed: None,
    }
}

/// A joined table with the identifier its columns bind under and the column
/// names its USING (or NATURAL) join constraint merges.
type JoinTable = (Arc<BTreeTable>, String, Vec<String>, TableInternalId);

/// Per-match emission hook for [`emit_join_phase`]: receives the program,
/// the per-table-position cursor ids, and the phase's table references.
type JoinPhaseSink<'a> =
    dyn FnMut(&mut ProgramBuilder, &[usize], &mut TableReferences, &Resolver) -> Result<()> + 'a;

/// Emit one join delta phase: nested loops over every joined table — reading
/// the transaction delta for tables in the phase's subset and the post-state
/// btree for the rest — with the delta tables outermost so the probe scans
/// run on the btree side. At the innermost level the ON predicates and WHERE
/// are evaluated, the signed weight product lands in `w_reg`, and `sink`
/// emits the per-match work with all cursors positioned (its slice maps
/// table positions to cursor ids).
#[allow(clippy::too_many_arguments)]
fn emit_join_phase(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    deltas: &[DeltaSource],
    tables: &[JoinTable],
    arrangements: &[ArrangementHandle],
    conditions: &[&ast::Expr],
    sides: &[JoinSide],
    negate: bool,
    w_reg: usize,
    sink: &mut JoinPhaseSink<'_>,
) -> Result<()> {
    let n = tables.len();
    turso_assert!(
        deltas.len() == n && arrangements.len() == n,
        "every join input must expose a delta and an arrangement"
    );

    // Fresh table references per phase: the same logical table reads from a
    // different relation (delta vs btree) in each phase, and cursors are
    // keyed by the internal id expressions bind against.
    let ids: Vec<_> = (0..n)
        .map(|_| program.table_reference_counter.next())
        .collect();
    let binding_remap = tables
        .iter()
        .zip(&ids)
        .map(|((_, _, _, logical_id), phase_id)| (*logical_id, *phase_id))
        .collect::<BindingRemap>();
    let mut table_references = TableReferences::new(
        tables
            .iter()
            .zip(&ids)
            .map(|((table, ident, using, _), id)| make_joined_table(table, ident, using, *id))
            .collect(),
        vec![],
    );

    let cursor_ids: Vec<usize> = tables
        .iter()
        .zip(deltas)
        .zip(arrangements)
        .zip(&ids)
        .zip(sides)
        .map(
            |(((((table, _, _, _), delta), arrangement), id), side)| -> Result<usize> {
                let storage = arrangement.table();
                match side {
                    JoinSide::Delta => match delta {
                        DeltaSource::BaseTable {
                            table: delta_table, ..
                        } => {
                            turso_assert!(
                                Arc::ptr_eq(table, delta_table)
                                    || table.root_page == delta_table.root_page,
                                "join delta and arrangement must describe the same relation"
                            );
                            let cursor_id = program.alloc_cursor_id_keyed(
                                CursorKey::table(*id),
                                CursorType::ViewDelta {
                                    view_name: view_name.to_string(),
                                    table: delta_table.clone(),
                                },
                            );
                            program.emit_insn(Insn::OpenRead {
                                cursor_id,
                                root_page: 0,
                                db: 0,
                            });
                            Ok(cursor_id)
                        }
                        DeltaSource::Ephemeral(channel) => Ok(channel.cursor_id),
                    },
                    JoinSide::Btree => {
                        let cursor_id = program.alloc_cursor_id_keyed(
                            CursorKey::table(*id),
                            CursorType::BTreeTable(storage.clone()),
                        );
                        program.emit_insn(Insn::OpenRead {
                            cursor_id,
                            root_page: storage.root_page,
                            db: 0,
                        });
                        Ok(cursor_id)
                    }
                }
            },
        )
        .collect::<Result<Vec<_>>>()?;

    // Nest delta tables outermost.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| sides[i] == JoinSide::Btree);

    let phase_done_label = program.allocate_label();
    let loop_labels: Vec<_> = (0..n).map(|_| program.allocate_label()).collect();
    let next_labels: Vec<_> = (0..n).map(|_| program.allocate_label()).collect();
    let delta_w_regs: Vec<Option<usize>> = sides
        .iter()
        .map(|side| (*side == JoinSide::Delta).then(|| program.alloc_register()))
        .collect();
    let arrangement_count_regs: Vec<Option<usize>> = arrangements
        .iter()
        .zip(sides)
        .map(|(arrangement, side)| {
            (*side == JoinSide::Btree && arrangement.count_column().is_some())
                .then(|| program.alloc_register())
        })
        .collect();

    for (depth, &pos) in order.iter().enumerate() {
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor_ids[pos],
            pc_if_empty: if depth == 0 {
                phase_done_label
            } else {
                next_labels[depth - 1]
            },
        });
        program.preassign_label_to_next_insn(loop_labels[depth]);
        if let Some(w) = delta_w_regs[pos] {
            let weight_column = match &deltas[pos] {
                DeltaSource::BaseTable { .. } => tables[pos].0.columns().len(),
                DeltaSource::Ephemeral(channel) => channel.weight_column,
            };
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: weight_column,
                dest: w,
                default: None,
            });
        }
        if let (Some(count_reg), Some(count_column)) = (
            arrangement_count_regs[pos],
            arrangements[pos].count_column(),
        ) {
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: count_column,
                dest: count_reg,
                default: None,
            });
        }
    }

    // Bind every logical column to the physical arrangement/delta slot used
    // in this phase. This keeps expression translation independent of record
    // layout (aggregate arrangements can project away accumulator payloads,
    // while ephemeral deltas carry identity before their values).
    resolver.enable_expr_to_reg_cache();
    for pos in 0..n {
        let (table, _, _, _) = &tables[pos];
        let phase_id = ids[pos];
        for (logical_column, column) in table.columns().iter().enumerate() {
            if column.name.is_none() {
                continue;
            }
            let physical_column = match (&sides[pos], &deltas[pos]) {
                (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                    channel.value_start + logical_column
                }
                (JoinSide::Delta, DeltaSource::BaseTable { .. }) => logical_column,
                (JoinSide::Btree, _) => arrangements[pos].value_columns()[logical_column],
            };
            let value_reg = program.alloc_register();
            match (&sides[pos], &deltas[pos]) {
                (JoinSide::Delta, DeltaSource::Ephemeral(_)) => {
                    program.emit_insn(Insn::Column {
                        cursor_id: cursor_ids[pos],
                        column: physical_column,
                        dest: value_reg,
                        default: None,
                    });
                }
                _ => program.emit_column_or_rowid(cursor_ids[pos], physical_column, value_reg),
            }
            let expr = ast::Expr::Column {
                database: None,
                table: phase_id,
                column: logical_column,
                is_rowid_alias: column.is_rowid_alias(),
            };
            resolver.cache_expr_reg(std::borrow::Cow::Owned(expr), value_reg, false, None);
        }
        if let (JoinSide::Delta, DeltaSource::Ephemeral(channel)) = (&sides[pos], &deltas[pos]) {
            turso_assert!(
                channel.identity_width() == 1,
                "arranged join inputs must publish one stable rowid"
            );
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: 0,
                dest: rowid_reg,
                default: None,
            });
            resolver.cache_expr_reg(
                std::borrow::Cow::Owned(ast::Expr::RowId {
                    database: None,
                    table: phase_id,
                }),
                rowid_reg,
                false,
                None,
            );
        }
    }

    // ON predicates and WHERE: a FALSE or NULL result means this combination
    // is not a join output.
    let innermost_next = next_labels[n - 1];
    for condition in conditions {
        let bound = remap_bound_expr(condition, &binding_remap)?;
        let pred_true = program.allocate_label();
        translate_condition_expr(
            program,
            &table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: pred_true,
                jump_target_when_false: innermost_next,
                jump_target_when_null: innermost_next,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(pred_true);
    }

    // Signed weight: the product of the delta weights times the phase sign.
    program.emit_int(if negate { -1 } else { 1 }, w_reg);
    for w in delta_w_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *w,
            dest: w_reg,
        });
    }
    for count in arrangement_count_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *count,
            dest: w_reg,
        });
    }

    sink(program, &cursor_ids, &mut table_references, resolver)?;

    for depth in (0..n).rev() {
        program.preassign_label_to_next_insn(next_labels[depth]);
        program.emit_insn(Insn::Next {
            cursor_id: cursor_ids[order[depth]],
            pc_if_next: loop_labels[depth],
        });
    }
    program.preassign_label_to_next_insn(phase_done_label);
    Ok(())
}

/// Emit a join's natural delta row into a typed ephemeral stream. The join
/// knows only its declared inputs and predicates; downstream group keys,
/// aggregate arguments, filters, and projections are evaluated by their own
/// operators from this stream.
///
/// Returns the ephemeral cursor and its row layout. The cursor's
/// [`dag::StreamSchema`] owns the logical binding namespace used by the
/// aggregate consumer.
#[allow(clippy::too_many_arguments)]
fn emit_join_deltas_to_ephemeral(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    deltas: &[DeltaSource],
    tables: &[JoinTable],
    arrangements: &[ArrangementHandle],
    on_conditions: &[ast::Expr],
    input: MaintenanceInput,
) -> Result<DeltaSource> {
    let conditions: Vec<&ast::Expr> = on_conditions.iter().collect();

    // The relational value schema excludes source identity; rowids travel in
    // the channel's identity prefix.
    let mut stream_columns = Vec::new();
    for (table, _, _, logical_id) in tables {
        for (logical_column, column) in table.columns().iter().enumerate() {
            let name = column.name.clone().ok_or_else(|| {
                LimboError::InternalError(
                    "join stream contains an unaddressable column".to_string(),
                )
            })?;
            stream_columns.push(dag::StreamColumn {
                expr: Some(ast::Expr::Column {
                    database: None,
                    table: *logical_id,
                    column: logical_column,
                    is_rowid_alias: column.is_rowid_alias(),
                }),
                name: Some(name),
            });
        }
    }
    let width = stream_columns.len();

    let sink = open_ephemeral_delta(
        program,
        &format!("{view_name}_joined_delta"),
        dag::StreamSchema {
            columns: stream_columns,
            bindings: tables
                .iter()
                .map(|(table, identifier, _, logical_id)| dag::StreamBinding {
                    table: table.clone(),
                    identifier: identifier.clone(),
                    logical_id: *logical_id,
                })
                .collect(),
        },
        DeltaIdentity::BindingRowids(tables.len()),
        true,
    );
    let channel = sink.ephemeral()?;
    let eph_cursor_id = channel.cursor_id;

    // [input rowids.., natural join row.., weight].
    let eph_rec_start = program.alloc_registers(channel.identity_width() + width + 1);
    let eph_value_start = eph_rec_start + channel.identity_width();
    let eph_weight_reg = eph_value_start + width;
    let eph_rowid_reg = program.alloc_register();
    let eph_record_reg = program.alloc_register();

    let emit_eph_insert = |program: &mut ProgramBuilder| {
        program.emit_insn(Insn::MakeRecord {
            start_reg: eph_rec_start as u16,
            count: (channel.identity_width() + width + 1) as u16,
            dest_reg: eph_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: eph_cursor_id,
            rowid_reg: eph_rowid_reg,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor: eph_cursor_id,
            key_reg: eph_rowid_reg,
            record_reg: eph_record_reg,
            flag: InsertFlags::new().is_ephemeral_table_insert(),
            table_name: String::new(),
        });
    };

    for (sides, negate) in join_subset_phases(tables.len(), input) {
        emit_join_phase(
            program,
            resolver,
            view_name,
            deltas,
            tables,
            arrangements,
            &conditions,
            &sides,
            negate,
            eph_weight_reg,
            &mut |program, cursors, _table_references, _resolver| {
                for (position, cursor_id) in cursors.iter().enumerate() {
                    match (&sides[position], &deltas[position]) {
                        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                            program.emit_insn(Insn::Column {
                                cursor_id: *cursor_id,
                                column: 0,
                                dest: eph_rec_start + position,
                                default: None,
                            });
                            turso_assert!(
                                channel.identity == DeltaIdentity::OperatorRowid,
                                "arranged join delta must carry its state rowid"
                            );
                        }
                        _ => program.emit_insn(Insn::RowId {
                            cursor_id: *cursor_id,
                            dest: eph_rec_start + position,
                        }),
                    }
                }
                let mut destination = eph_value_start;
                for (position, ((table, _, _, _), cursor_id)) in
                    tables.iter().zip(cursors).enumerate()
                {
                    for column in 0..table.columns().len() {
                        match (&sides[position], &deltas[position]) {
                            (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                                program.emit_insn(Insn::Column {
                                    cursor_id: *cursor_id,
                                    column: channel.value_start + column,
                                    dest: destination,
                                    default: None,
                                });
                            }
                            (JoinSide::Btree, _) => program.emit_column_or_rowid(
                                *cursor_id,
                                arrangements[position].value_columns()[column],
                                destination,
                            ),
                            (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
                                program.emit_column_or_rowid(*cursor_id, column, destination);
                            }
                        }
                        destination += 1;
                    }
                }
                turso_assert!(
                    destination == eph_value_start + width,
                    "join stream row does not match its declared schema"
                );
                emit_eph_insert(program);
                Ok(())
            },
        )?;
    }

    sink.source()
}

fn emit_operator_keyed_ephemeral_row(
    program: &mut ProgramBuilder,
    output: &EphemeralDelta,
    kind: i64,
    source_identity_start: usize,
    source_identity_width: usize,
    values_start: usize,
    weight_reg: usize,
) {
    turso_assert!(
        output.identity == DeltaIdentity::OperatorKey(2)
            && source_identity_width > 0
            && output.value_start == 2,
        "operator-keyed delta rows carry kind, packed source identity, values, and weight"
    );
    let record_start = program.alloc_registers(2 + output.width + 1);
    program.emit_int(kind, record_start);
    program.emit_insn(Insn::MakeRecord {
        start_reg: source_identity_start as u16,
        count: source_identity_width as u16,
        dest_reg: (record_start + 1) as u16,
        index_name: None,
        affinity_str: None,
    });
    if output.width > 0 {
        program.emit_insn(Insn::Copy {
            src_reg: values_start,
            dst_reg: record_start + output.value_start,
            extra_amount: output.width - 1,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: record_start + output.weight_column,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: (2 + output.width + 1) as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let rowid_reg = program.alloc_register();
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
}

fn emit_natural_join_values(
    program: &mut ProgramBuilder,
    tables: &[JoinTable],
    arrangements: &[ArrangementHandle],
    cursors: &[usize],
    values_start: usize,
) -> usize {
    let mut destination = values_start;
    for (((table, _, _, _), arrangement), cursor) in tables.iter().zip(arrangements).zip(cursors) {
        for column in 0..table.columns().len() {
            program.emit_column_or_rowid(*cursor, arrangement.value_columns()[column], destination);
            destination += 1;
        }
    }
    destination
}

/// Emit a two-input LEFT JOIN as one typed z-set stream.
///
/// Matched rows use `(0, packed(left identity, right identity))`; padded rows
/// use `(1, packed(aux rowid))`. The auxiliary table persists the padded
/// natural row so a departing left row can publish its old image. An explicit
/// output arrangement then normalizes both key forms to one operator rowid
/// before any downstream consumer sees them.
#[allow(clippy::too_many_arguments)]
fn emit_left_join_deltas_to_ephemeral(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    contract: &JoinContract,
    output_schema: dag::StreamSchema,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<DeltaSource> {
    let n = contract.tables.len();
    turso_assert!(n == 2, "DAG validation admits LEFT JOIN for two inputs");

    let sink = open_ephemeral_delta(
        program,
        &format!("{view_name}_left_join_delta_{node_id}"),
        output_schema,
        DeltaIdentity::OperatorKey(2),
        true,
    );
    let output = sink.ephemeral()?;
    let natural_width = contract
        .tables
        .iter()
        .map(|(table, _, _, _)| table.columns().len())
        .sum::<usize>();
    turso_assert!(
        output.width == natural_width,
        "LEFT JOIN output stream must contain its natural row"
    );

    let aux_name = operator_state.auxiliary_table_name()?.to_string();
    let aux_table = schema.get_btree_table(&aux_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "aux table {aux_name} of materialized view {view_name} not found"
        ))
    })?;
    let aux_index = schema
        .get_indices(&aux_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "aux table {aux_name} of materialized view {view_name} has no index"
            ))
        })?;

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

    let zero_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    let corrupt_label = program.allocate_label();
    let main_start_label = program.allocate_label();
    let end_label = program.allocate_label();
    let pair_identity_start = program.alloc_registers(n);
    let matched_values_start = program.alloc_registers(natural_width);
    let matched_weight_reg = program.alloc_register();

    let pad_ids: Vec<_> = (0..n)
        .map(|_| program.table_reference_counter.next())
        .collect();
    let mut pad_references = TableReferences::new(
        contract
            .tables
            .iter()
            .zip(&pad_ids)
            .map(|((table, ident, using, _), id)| make_joined_table(table, ident, using, *id))
            .collect(),
        vec![],
    );
    let pad_cursors: Vec<usize> = contract
        .arrangements
        .iter()
        .zip(&pad_ids)
        .map(|(arrangement, id)| {
            let table = arrangement.table();
            let cursor_id = program.alloc_cursor_id_keyed(
                CursorKey::table(*id),
                CursorType::BTreeTable(table.clone()),
            );
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: table.root_page,
                db: 0,
            });
            cursor_id
        })
        .collect();
    let outer = LeftJoinAux::open(
        program,
        aux_name,
        &aux_table,
        &aux_index,
        natural_width,
        zero_reg,
        prev_rowid_scratch,
        corrupt_label,
    );
    let padded_values_start = outer.content_start;

    program.emit_int(0, zero_reg);
    program.emit_int(1, one_reg);
    program.emit_insn(Insn::Goto {
        target_pc: main_start_label,
    });

    let mut emit_padded_add = |program: &mut ProgramBuilder,
                               pad_cursors: &[usize],
                               _pad_references: &mut TableReferences,
                               srid_reg: usize|
     -> Result<()> {
        let end = emit_natural_join_values(
            program,
            &contract.tables,
            &contract.arrangements,
            pad_cursors,
            padded_values_start,
        );
        turso_assert!(
            end == padded_values_start + natural_width,
            "padded LEFT JOIN row does not match its output schema"
        );
        emit_operator_keyed_ephemeral_row(
            program,
            &output,
            1,
            srid_reg,
            1,
            padded_values_start,
            one_reg,
        );
        Ok(())
    };
    let mut emit_padded_remove = |program: &mut ProgramBuilder, srid_reg: usize| -> Result<()> {
        let negative_one_reg = program.alloc_register();
        program.emit_int(-1, negative_one_reg);
        emit_operator_keyed_ephemeral_row(
            program,
            &output,
            1,
            srid_reg,
            1,
            padded_values_start,
            negative_one_reg,
        );
        Ok(())
    };
    outer.emit_subroutine(
        program,
        &resolver,
        &pad_cursors,
        &mut pad_references,
        None,
        &mut emit_padded_add,
        &mut emit_padded_remove,
    )?;

    program.preassign_label_to_next_insn(main_start_label);
    outer.emit_presence_pass(
        program,
        input,
        &contract.deltas[0],
        &contract.arrangements[0],
        contract.tables[0].0.columns().len(),
        view_name,
    )?;

    let conditions = contract.on.iter().collect::<Vec<_>>();
    for (sides, negate) in join_subset_phases(n, input) {
        emit_join_phase(
            program,
            &mut resolver,
            view_name,
            &contract.deltas,
            &contract.tables,
            &contract.arrangements,
            &conditions,
            &sides,
            negate,
            matched_weight_reg,
            &mut |program, cursors, _table_references, _resolver| {
                for (position, cursor) in cursors.iter().enumerate() {
                    match (&sides[position], &contract.deltas[position]) {
                        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                            turso_assert!(
                                channel.identity == DeltaIdentity::OperatorRowid,
                                "arranged LEFT JOIN delta must carry its arrangement rowid"
                            );
                            program.emit_insn(Insn::Column {
                                cursor_id: *cursor,
                                column: 0,
                                dest: pair_identity_start + position,
                                default: None,
                            });
                        }
                        _ => program.emit_insn(Insn::RowId {
                            cursor_id: *cursor,
                            dest: pair_identity_start + position,
                        }),
                    }
                }
                outer.emit_count_match(program, pair_identity_start, matched_weight_reg);
                let mut destination = matched_values_start;
                for (position, ((table, _, _, _), cursor)) in
                    contract.tables.iter().zip(cursors).enumerate()
                {
                    for column in 0..table.columns().len() {
                        match (&sides[position], &contract.deltas[position]) {
                            (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                                program.emit_insn(Insn::Column {
                                    cursor_id: *cursor,
                                    column: channel.value_start + column,
                                    dest: destination,
                                    default: None,
                                });
                            }
                            (JoinSide::Btree, _) => program.emit_column_or_rowid(
                                *cursor,
                                contract.arrangements[position].value_columns()[column],
                                destination,
                            ),
                            (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
                                program.emit_column_or_rowid(*cursor, column, destination);
                            }
                        }
                        destination += 1;
                    }
                }
                turso_assert!(
                    destination == matched_values_start + natural_width,
                    "matched LEFT JOIN row does not match its output schema"
                );
                emit_operator_keyed_ephemeral_row(
                    program,
                    &output,
                    0,
                    pair_identity_start,
                    n,
                    matched_values_start,
                    matched_weight_reg,
                );
                Ok(())
            },
        )?;
    }

    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    sink.source()
}

/// Consumer hook that materializes one NULL-padded output row: given the
/// padded-image cursors (left seeked, right on its null row), their table
/// references, and the aux row's rowid identifying the padded row.
type PaddedAddFn<'a> =
    dyn FnMut(&mut ProgramBuilder, &[usize], &mut TableReferences, usize) -> Result<()> + 'a;
/// Consumer hook that retracts one NULL-padded output row, given the aux
/// row's rowid.
type PaddedRemoveFn<'a> = dyn FnMut(&mut ProgramBuilder, usize) -> Result<()> + 'a;

/// Per-left-row NULL-padded bookkeeping for a two-table LEFT (or swapped
/// RIGHT) join view.
///
/// A LEFT join's output is the inner matches plus one NULL-padded row for each
/// left row with no ON-match. The inner rows are a plain function of the
/// current inputs (the delta phases reconstruct them, retractions included,
/// from the base-table deltas), but a padded row exists only *because* its
/// left row currently has zero matches — it must appear when the left row
/// arrives unmatched and disappear the moment a match arrives, the left row
/// leaves, or (with a WHERE over the padded image) the predicate flips. That
/// is anti-join maintenance, and it needs state: this auxiliary table keeps,
/// per left row, its signed presence, its count of ON-matches, and whether its
/// padded output row is currently live.
///
/// The subroutine applies a `(l_rid, dPresent, dMatches)` delta to that state
/// and, on a transition of "present with zero matches and the padded image
/// passes WHERE", invokes the consumer's `emit_padded_add` / `padded_remove`
/// to materialize or retract the one padded output row. Presence deltas come
/// from the left-input pass ([`Self::emit_presence_pass`]); match deltas come
/// from each inner join phase ([`Self::emit_count_match`]).
struct LeftJoinAux {
    aux_name: String,
    aux_index_name: String,
    aux_cursor_id: usize,
    aux_index_cursor_id: usize,
    /// Set by the caller before `Gosub(merge_label)`: `[l_rid, aux_rowid]`.
    akey_start: usize,
    /// The aux row's rowid slot (`akey_start + 1`), doubling as the padded
    /// output row's identity for the consumer's add/remove.
    srid_reg: usize,
    /// Set by the caller before the gosub: the signed presence delta.
    dp_reg: usize,
    /// Set by the caller before the gosub: the signed match-count delta.
    dm_reg: usize,
    return_reg: usize,
    merge_label: BranchOffset,
    // Working registers, private to the subroutine.
    rec_start: usize,
    present_reg: usize,
    matches_reg: usize,
    padded_reg: usize,
    fresh_reg: usize,
    padded_new_reg: usize,
    state_record_reg: usize,
    index_record_reg: usize,
    /// Optional padded-output payload stored in the aux row after `padded`:
    /// its first register and width. A consumer that must retract a padded
    /// row it cannot reproject after the left row is gone writes the payload
    /// here on add and reads it back on remove.
    content_start: usize,
    content_width: usize,
    // Shared scratch owned by the caller.
    zero_reg: usize,
    prev_rowid_scratch: usize,
    corrupt_label: BranchOffset,
}

impl LeftJoinAux {
    /// Open the aux table and its index and allocate the subroutine's
    /// registers. `zero_reg`, `prev_rowid_scratch`, and `corrupt_label` are
    /// shared with the caller's other subroutines.
    #[allow(clippy::too_many_arguments)]
    fn open(
        program: &mut ProgramBuilder,
        aux_name: String,
        aux_table: &Arc<BTreeTable>,
        aux_index: &Arc<crate::schema::Index>,
        content_width: usize,
        zero_reg: usize,
        prev_rowid_scratch: usize,
        corrupt_label: BranchOffset,
    ) -> Self {
        let aux_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(aux_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: aux_cursor_id,
            root_page: RegisterOrLiteral::Literal(aux_table.root_page),
            db: 0,
        });
        let aux_index_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeIndex(aux_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: aux_index_cursor_id,
            root_page: RegisterOrLiteral::Literal(aux_index.root_page),
            db: 0,
        });

        // [l_rid, aux_rowid], contiguous for Found/IdxInsert/IdxDelete.
        let akey_start = program.alloc_registers(2);
        let srid_reg = akey_start + 1;
        // [l_rid, present, matches, padded, payload..], the aux row image.
        let rec_start = program.alloc_registers(4 + content_width);
        let present_reg = rec_start + 1;
        let matches_reg = rec_start + 2;
        let padded_reg = rec_start + 3;
        let content_start = rec_start + 4;

        Self {
            aux_name,
            aux_index_name: aux_index.name.clone(),
            aux_cursor_id,
            aux_index_cursor_id,
            akey_start,
            srid_reg,
            dp_reg: program.alloc_register(),
            dm_reg: program.alloc_register(),
            return_reg: program.alloc_register(),
            merge_label: program.allocate_label(),
            rec_start,
            present_reg,
            matches_reg,
            padded_reg,
            fresh_reg: program.alloc_register(),
            padded_new_reg: program.alloc_register(),
            state_record_reg: program.alloc_register(),
            index_record_reg: program.alloc_register(),
            content_start,
            content_width,
            zero_reg,
            prev_rowid_scratch,
            corrupt_label,
        }
    }

    /// Emit the aux-merge subroutine (reached by `Gosub(self.merge_label)`).
    /// `pad_cursors`/`pad_references`/`pad_where` describe the padded image
    /// (left cursor plus a NULL-row right cursor) over which WHERE is tested;
    /// `emit_padded_add`/`emit_padded_remove` materialize and retract the one
    /// padded output row, with `self.srid_reg` holding its identity and (for
    /// add) `pad_cursors` positioned on the padded image.
    #[allow(clippy::too_many_arguments)]
    fn emit_subroutine(
        &self,
        program: &mut ProgramBuilder,
        resolver: &Resolver,
        pad_cursors: &[usize],
        pad_references: &mut TableReferences,
        pad_where: Option<&ast::Expr>,
        emit_padded_add: &mut PaddedAddFn<'_>,
        emit_padded_remove: &mut PaddedRemoveFn<'_>,
    ) -> Result<()> {
        program.preassign_label_to_next_insn(self.merge_label);

        let af_found = program.allocate_label();
        let af_have_row = program.allocate_label();
        let af_transition = program.allocate_label();
        let af_padwrite = program.allocate_label();
        let af_store = program.allocate_label();
        let af_write = program.allocate_label();
        let af_ret = program.allocate_label();

        program.emit_insn(Insn::Found {
            cursor_id: self.aux_index_cursor_id,
            target_pc: af_found,
            record_reg: self.akey_start,
            num_regs: 1,
        });
        // Fresh left row (or a retraction arriving first: recorded with its
        // signs).
        program.emit_int(1, self.fresh_reg);
        program.emit_insn(Insn::NewRowid {
            cursor: self.aux_cursor_id,
            rowid_reg: self.srid_reg,
            prev_largest_reg: self.prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.akey_start,
            dst_reg: self.rec_start,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.dp_reg,
            dst_reg: self.present_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.dm_reg,
            dst_reg: self.matches_reg,
            extra_amount: 0,
        });
        program.emit_int(0, self.padded_reg);
        // The payload is undefined until the row first becomes padded; NULL it
        // so a persisted-but-never-padded aux row carries defined columns.
        if self.content_width > 0 {
            program.emit_insn(Insn::Null {
                dest: self.content_start,
                dest_end: Some(self.content_start + self.content_width - 1),
            });
        }
        program.emit_insn(Insn::Goto {
            target_pc: af_have_row,
        });

        program.preassign_label_to_next_insn(af_found);
        program.emit_int(0, self.fresh_reg);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: self.aux_index_cursor_id,
            dest: self.srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: self.aux_cursor_id,
            src_reg: self.srid_reg,
            target_pc: self.corrupt_label,
        });
        for (column, dest) in [
            (1, self.present_reg),
            (2, self.matches_reg),
            (3, self.padded_reg),
        ] {
            program.emit_insn(Insn::Column {
                cursor_id: self.aux_cursor_id,
                column,
                dest,
                default: None,
            });
        }
        // Load the stored padded payload so a remove can emit the departing
        // row's old image without repositioning cursors.
        for i in 0..self.content_width {
            program.emit_insn(Insn::Column {
                cursor_id: self.aux_cursor_id,
                column: 4 + i,
                dest: self.content_start + i,
                default: None,
            });
        }
        program.emit_insn(Insn::Copy {
            src_reg: self.akey_start,
            dst_reg: self.rec_start,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Add {
            lhs: self.dp_reg,
            rhs: self.present_reg,
            dest: self.present_reg,
        });
        program.emit_insn(Insn::Add {
            lhs: self.dm_reg,
            rhs: self.matches_reg,
            dest: self.matches_reg,
        });

        program.preassign_label_to_next_insn(af_have_row);
        program.emit_int(0, self.padded_new_reg);
        program.emit_insn(Insn::Le {
            lhs: self.present_reg,
            rhs: self.zero_reg,
            target_pc: af_transition,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: self.matches_reg,
            target_pc: af_transition,
            jump_if_null: false,
        });
        // Candidate padded row: position the left cursor on the row (present,
        // so the post-state btree has it), put the right cursor on its null
        // row, and let WHERE decide over that image.
        program.emit_insn(Insn::SeekRowid {
            cursor_id: pad_cursors[0],
            src_reg: self.akey_start,
            target_pc: self.corrupt_label,
        });
        program.emit_insn(Insn::NullRow {
            cursor_id: pad_cursors[1],
        });
        if let Some(pad_where) = pad_where {
            let pad_pass = program.allocate_label();
            translate_condition_expr(
                program,
                pad_references,
                pad_where,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: pad_pass,
                    jump_target_when_false: af_transition,
                    jump_target_when_null: af_transition,
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(pad_pass);
        }
        program.emit_int(1, self.padded_new_reg);
        program.preassign_label_to_next_insn(af_transition);
        program.emit_insn(Insn::Eq {
            lhs: self.padded_reg,
            rhs: self.padded_new_reg,
            target_pc: af_store,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: self.padded_new_reg,
            target_pc: af_padwrite,
            jump_if_null: false,
        });
        // Padded before, not now: retract the padded output row.
        emit_padded_remove(program, self.srid_reg)?;
        program.emit_insn(Insn::Goto {
            target_pc: af_store,
        });

        // Not padded before, padded now: materialize it. The only path that
        // sets `padded_new` positioned the pad cursors, so the consumer reads
        // the left row with NULLs on the right.
        program.preassign_label_to_next_insn(af_padwrite);
        emit_padded_add(program, pad_cursors, pad_references, self.srid_reg)?;

        program.preassign_label_to_next_insn(af_store);
        // Persist the padded bit the transitions just enacted.
        program.emit_insn(Insn::Copy {
            src_reg: self.padded_new_reg,
            dst_reg: self.padded_reg,
            extra_amount: 0,
        });
        // Delete the aux row once everything it tracks is zero; otherwise
        // (re)write it.
        program.emit_insn(Insn::If {
            reg: self.present_reg,
            target_pc: af_write,
            jump_if_null: false,
        });
        program.emit_insn(Insn::If {
            reg: self.matches_reg,
            target_pc: af_write,
            jump_if_null: false,
        });
        program.emit_insn(Insn::If {
            reg: self.fresh_reg,
            target_pc: af_ret,
            jump_if_null: false,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: self.akey_start,
            num_regs: 2,
            cursor_id: self.aux_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: self.aux_cursor_id,
            table_name: self.aux_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto { target_pc: af_ret });

        program.preassign_label_to_next_insn(af_write);
        program.emit_insn(Insn::MakeRecord {
            start_reg: self.rec_start as u16,
            count: (4 + self.content_width) as u16,
            dest_reg: self.state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.aux_cursor_id,
            key_reg: self.srid_reg,
            record_reg: self.state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: self.aux_name.clone(),
        });
        let af_skip_index = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: self.fresh_reg,
            target_pc: af_skip_index,
            jump_if_null: true,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: self.akey_start as u16,
            count: 2,
            dest_reg: self.index_record_reg as u16,
            index_name: Some(self.aux_index_name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: self.aux_index_cursor_id,
            record_reg: self.index_record_reg,
            unpacked_start: Some(self.akey_start),
            unpacked_count: Some(2),
            flags: IdxInsertFlags::new(),
        });
        program.preassign_label_to_next_insn(af_skip_index);
        program.preassign_label_to_next_insn(af_ret);
        program.emit_insn(Insn::Return {
            return_reg: self.return_reg,
            can_fallthrough: false,
        });
        Ok(())
    }

    /// Emit the left-input presence pass: iterate the left input (transaction
    /// delta or, during population, the full left btree) and drive a presence
    /// delta through the subroutine for each left row. A new unmatched left
    /// row reaches no join phase, so this is where it first appears.
    fn emit_presence_pass(
        &self,
        program: &mut ProgramBuilder,
        input: MaintenanceInput,
        delta: &DeltaSource,
        arrangement: &ArrangementHandle,
        logical_width: usize,
        view_name: &str,
    ) -> Result<()> {
        let input_cursor_id = match input {
            MaintenanceInput::TransactionDelta => match delta {
                DeltaSource::BaseTable {
                    table: delta_table, ..
                } => {
                    turso_assert!(
                        arrangement.table().root_page == delta_table.root_page,
                        "outer-join presence delta and arrangement must describe the same relation"
                    );
                    let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                        view_name: view_name.to_string(),
                        table: delta_table.clone(),
                    });
                    program.emit_insn(Insn::OpenRead {
                        cursor_id,
                        root_page: 0,
                        db: 0,
                    });
                    cursor_id
                }
                DeltaSource::Ephemeral(channel) => channel.cursor_id,
            },
            MaintenanceInput::BaseTable => {
                let table = arrangement.table();
                let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: table.root_page,
                    db: 0,
                });
                cursor_id
            }
        };
        let pass_done = program.allocate_label();
        let pass_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: input_cursor_id,
            pc_if_empty: pass_done,
        });
        program.preassign_label_to_next_insn(pass_loop);
        match input {
            MaintenanceInput::TransactionDelta => {
                let weight_column = match delta {
                    DeltaSource::BaseTable { .. } => logical_width,
                    DeltaSource::Ephemeral(channel) => channel.weight_column,
                };
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: weight_column,
                    dest: self.dp_reg,
                    default: None,
                });
            }
            MaintenanceInput::BaseTable => {
                if let Some(column) = arrangement.count_column() {
                    program.emit_insn(Insn::Column {
                        cursor_id: input_cursor_id,
                        column,
                        dest: self.dp_reg,
                        default: None,
                    });
                } else {
                    program.emit_int(1, self.dp_reg);
                }
            }
        }
        match (input, delta) {
            (MaintenanceInput::TransactionDelta, DeltaSource::Ephemeral(channel)) => {
                turso_assert!(
                    channel.identity == DeltaIdentity::OperatorRowid,
                    "arranged LEFT JOIN input must carry its arrangement rowid"
                );
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: 0,
                    dest: self.akey_start,
                    default: None,
                });
            }
            _ => program.emit_insn(Insn::RowId {
                cursor_id: input_cursor_id,
                dest: self.akey_start,
            }),
        }
        program.emit_int(0, self.dm_reg);
        program.emit_insn(Insn::Gosub {
            target_pc: self.merge_label,
            return_reg: self.return_reg,
        });
        program.emit_insn(Insn::Next {
            cursor_id: input_cursor_id,
            pc_if_next: pass_loop,
        });
        program.preassign_label_to_next_insn(pass_done);
        Ok(())
    }

    /// Emit a match-count delta for the left row at `left_rowid_reg`, weighted
    /// by the current phase's signed weight. Called from each inner join phase
    /// so every ON match adjusts its left row's count, whether or not WHERE
    /// lets the inner row through.
    fn emit_count_match(
        &self,
        program: &mut ProgramBuilder,
        left_rowid_reg: usize,
        weight_reg: usize,
    ) {
        program.emit_insn(Insn::Copy {
            src_reg: left_rowid_reg,
            dst_reg: self.akey_start,
            extra_amount: 0,
        });
        program.emit_int(0, self.dp_reg);
        program.emit_insn(Insn::Copy {
            src_reg: weight_reg,
            dst_reg: self.dm_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Gosub {
            target_pc: self.merge_label,
            return_reg: self.return_reg,
        });
    }
}

/// Emit `dst = 1` if the count in `src` is positive, else `0`. Counts are
/// always integers, so three-valued logic never applies.
fn emit_presence(program: &mut ProgramBuilder, src: usize, zero_reg: usize, dst: usize) {
    let done = program.allocate_label();
    program.emit_int(1, dst);
    program.emit_insn(Insn::Gt {
        lhs: src,
        rhs: zero_reg,
        target_pc: done,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(0, dst);
    program.preassign_label_to_next_insn(done);
}

/// Emit the left-associative visibility fold over the per-branch counts at
/// `cnt_start..cnt_start + prefix_len` into `dest`: a content row is in the
/// compound result iff the fold of per-branch presence is true (UNION and
/// UNION ALL fold as OR, INTERSECT as AND, EXCEPT as AND NOT).
fn emit_visibility_fold(
    program: &mut ProgramBuilder,
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    cnt_start: usize,
    zero_reg: usize,
    scratch_reg: usize,
    dest: usize,
) {
    emit_presence(program, cnt_start, zero_reg, dest);
    for i in 1..prefix_len {
        emit_presence(program, cnt_start + i, zero_reg, scratch_reg);
        match operators[i - 1] {
            ast::CompoundOperator::Union | ast::CompoundOperator::UnionAll => {
                program.emit_insn(Insn::Or {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
            ast::CompoundOperator::Intersect => {
                program.emit_insn(Insn::And {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
            ast::CompoundOperator::Except => {
                program.emit_insn(Insn::Not {
                    reg: scratch_reg,
                    dest: scratch_reg,
                });
                program.emit_insn(Insn::And {
                    lhs: dest,
                    rhs: scratch_reg,
                    dest,
                });
            }
        }
    }
}

/// Emit the view rowid for a state rowid in `srid_reg` into `dst`. When the
/// view keeps both content rows and append rows, their state rowids come
/// from two independent tables, so they map to disjoint view rowids:
/// 2*rowid for content rows, 2*rowid + 1 for append rows. With a single
/// state table the state rowid is the view rowid, as everywhere else.
fn emit_split_view_rowid(
    program: &mut ProgramBuilder,
    both_tables: bool,
    srid_reg: usize,
    two_reg: usize,
    one_reg: usize,
    dst: usize,
    append: bool,
) {
    if both_tables {
        program.emit_insn(Insn::Multiply {
            lhs: srid_reg,
            rhs: two_reg,
            dest: dst,
        });
        if append {
            program.emit_insn(Insn::Add {
                lhs: dst,
                rhs: one_reg,
                dest: dst,
            });
        }
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: srid_reg,
            dst_reg: dst,
            extra_amount: 0,
        });
    }
}

fn emit_ephemeral_row_with_identity(
    program: &mut ProgramBuilder,
    output: &EphemeralDelta,
    identity_reg: usize,
    values_start: usize,
    weight_reg: usize,
) {
    turso_assert!(
        output.identity == DeltaIdentity::OperatorRowid && output.value_start == 1,
        "arranged operator output streams carry one stable state rowid"
    );
    let record_start = program.alloc_registers(1 + output.width + 1);
    program.emit_insn(Insn::Copy {
        src_reg: identity_reg,
        dst_reg: record_start,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: values_start,
        dst_reg: record_start + 1,
        extra_amount: output.width.saturating_sub(1),
    });
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: record_start + 1 + output.width,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: (1 + output.width + 1) as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let rowid_reg = program.alloc_register();
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
}

#[allow(clippy::too_many_arguments)]
fn emit_deduplicating_set_op(
    program: &mut ProgramBuilder,
    view_name: &str,
    branch_streams: &[EphemeralDelta],
    operators: &[ast::CompoundOperator],
    prefix_len: usize,
    key_collations: &[CollationSeq],
    output: &EphemeralDelta,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<()> {
    let n_branches = branch_streams.len();
    let num_view_columns = output.width;
    turso_assert!(
        prefix_len > 0,
        "pure UNION ALL uses stateless branch composition"
    );
    let has_append = prefix_len < n_branches;
    let materializes_append = has_append;
    // Width of the content key: every output column.
    let k = num_view_columns;
    // With a collated dedup key, values that compare equal can differ in
    // bytes; the stored key is the row's first-seen representative (see the
    // matching reload in the group-aggregate program). The output columns
    // ARE the key, so the reload refreshes both.
    let reload_stored_key = key_collations
        .iter()
        .any(|c| !matches!(c, CollationSeq::Binary | CollationSeq::Unset));

    let state_table_name = operator_state.state_table_name()?.to_string();
    let state_table = schema.get_btree_table(&state_table_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "state table {state_table_name} of materialized view {view_name} not found"
        ))
    })?;
    let state_index = schema
        .get_indices(&state_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "state table {state_table_name} of materialized view {view_name} has no index"
            ))
        })?;
    // Trailing UNION ALL rows preserve source identity in the auxiliary table.
    let (append_table_name, append_table, append_index) = if materializes_append {
        let name = operator_state.auxiliary_table_name()?.to_string();
        let table = schema.get_btree_table(&name).ok_or_else(|| {
            LimboError::InternalError(format!(
                "append table {name} of materialized view {view_name} not found"
            ))
        })?;
        let index = schema.get_indices(&name).next().cloned().ok_or_else(|| {
            LimboError::InternalError(format!(
                "append table {name} of materialized view {view_name} has no index"
            ))
        })?;
        (name, table, index)
    } else {
        (
            state_table_name.clone(),
            state_table.clone(),
            state_index.clone(),
        )
    };

    let state_cursor = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let state_index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(state_index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_index_cursor,
        root_page: RegisterOrLiteral::Literal(state_index.root_page),
        db: 0,
    });
    let content_cursors = (state_cursor, state_index_cursor);
    let append_cursors = if materializes_append {
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(append_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: table_cursor,
            root_page: RegisterOrLiteral::Literal(append_table.root_page),
            db: 0,
        });
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(append_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor,
            root_page: RegisterOrLiteral::Literal(append_index.root_page),
            db: 0,
        });
        Some((table_cursor, index_cursor))
    } else {
        None
    };
    // Register layout, shared by all branches.
    let zero_reg = program.alloc_register();
    let two_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    // [outputs.., weight], contiguous for MakeRecord.
    let out_start = program.alloc_registers(num_view_columns + 1);
    let out_weight_reg = out_start + num_view_columns;
    let w_reg = program.alloc_register();
    let view_rowid_reg = program.alloc_register();
    // Content-merge registers: [key.., state_rowid] contiguous for
    // Found/IdxInsert/IdxDelete, and [key.., counts..] as the state row
    // image (the counts live in place at the record tail).
    let c_return_reg = program.alloc_register();
    let key_start = program.alloc_registers(k + 1);
    let c_srid_reg = key_start + k;
    let state_rec_start = program.alloc_registers(k + prefix_len.max(1));
    let cnt_start = state_rec_start + k;
    let v_old_reg = program.alloc_register();
    let v_new_reg = program.alloc_register();
    let p_scratch_reg = program.alloc_register();
    let c_state_record_reg = program.alloc_register();
    let c_index_record_reg = program.alloc_register();
    // Append-merge registers: [branch, rid, state_rowid] and the
    // (branch, rid, mult) state row image.
    let a_return_reg = program.alloc_register();
    let akey_start = program.alloc_registers(3);
    let a_srid_reg = akey_start + 2;
    let a_rec_start = program.alloc_registers(3);
    let cur_w_reg = program.alloc_register();
    let new_w_reg = program.alloc_register();
    let a_state_record_reg = program.alloc_register();
    let a_index_record_reg = program.alloc_register();

    program.emit_int(0, zero_reg);
    program.emit_int(2, two_reg);
    program.emit_int(1, one_reg);

    let main_start_label = program.allocate_label();
    let corrupt_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::Goto {
        target_pc: main_start_label,
    });

    // Content-merge subroutines, one per prefix branch (the branch decides
    // which count column the weight lands in): apply (key, outputs, w) to
    // the content state row, then flip the view row on visibility-fold
    // transitions. Negative counts are recorded for uniformity with joins;
    // the per-branch capture-order streams cannot dip below zero on their
    // own, and a negative count reads as "absent" in the fold.
    let mut content_merge_labels = Vec::with_capacity(prefix_len);
    for b in 0..prefix_len {
        let (state_cursor_id, state_index_cursor_id) = content_cursors;
        let cm_label = program.allocate_label();
        program.preassign_label_to_next_insn(cm_label);
        content_merge_labels.push(cm_label);

        let found_label = program.allocate_label();
        let vnew_label = program.allocate_label();
        let not_all_zero_label = program.allocate_label();
        let insert_label = program.allocate_label();
        let ret_label = program.allocate_label();

        program.emit_insn(Insn::Found {
            cursor_id: state_index_cursor_id,
            target_pc: found_label,
            record_reg: key_start,
            num_regs: k,
        });
        // Fresh content row: this branch's count is the weight, the rest 0.
        program.emit_insn(Insn::NewRowid {
            cursor: state_cursor_id,
            rowid_reg: c_srid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        for i in 0..prefix_len {
            program.emit_int(0, cnt_start + i);
        }
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: cnt_start + b,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + prefix_len) as u16,
            dest_reg: c_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: c_srid_reg,
            record_reg: c_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: key_start as u16,
            count: (k + 1) as u16,
            dest_reg: c_index_record_reg as u16,
            index_name: Some(state_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: state_index_cursor_id,
            record_reg: c_index_record_reg,
            unpacked_start: Some(key_start),
            unpacked_count: Some((k + 1) as u16),
            flags: IdxInsertFlags::new(),
        });
        program.emit_int(0, v_old_reg);
        program.emit_insn(Insn::Goto {
            target_pc: vnew_label,
        });

        program.preassign_label_to_next_insn(found_label);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: state_index_cursor_id,
            dest: c_srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: state_cursor_id,
            src_reg: c_srid_reg,
            target_pc: corrupt_label,
        });
        if reload_stored_key {
            for i in 0..k {
                program.emit_insn(Insn::Column {
                    cursor_id: state_cursor_id,
                    column: i,
                    dest: key_start + i,
                    default: None,
                });
            }
            program.emit_insn(Insn::Copy {
                src_reg: key_start,
                dst_reg: out_start,
                extra_amount: k.saturating_sub(1),
            });
        }
        for i in 0..prefix_len {
            program.emit_insn(Insn::Column {
                cursor_id: state_cursor_id,
                column: k + i,
                dest: cnt_start + i,
                default: None,
            });
        }
        emit_visibility_fold(
            program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_old_reg,
        );
        program.emit_insn(Insn::Add {
            lhs: w_reg,
            rhs: cnt_start + b,
            dest: cnt_start + b,
        });
        // Every count zero: the content row is gone from all branches (and
        // the fold below reads all-absent as invisible).
        for i in 0..prefix_len {
            program.emit_insn(Insn::If {
                reg: cnt_start + i,
                target_pc: not_all_zero_label,
                jump_if_null: false,
            });
        }
        program.emit_insn(Insn::IdxDelete {
            start_reg: key_start,
            num_regs: k + 1,
            cursor_id: state_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: state_cursor_id,
            table_name: state_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: vnew_label,
        });

        program.preassign_label_to_next_insn(not_all_zero_label);
        program.emit_insn(Insn::Copy {
            src_reg: key_start,
            dst_reg: state_rec_start,
            extra_amount: k.saturating_sub(1),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: state_rec_start as u16,
            count: (k + prefix_len) as u16,
            dest_reg: c_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: state_cursor_id,
            key_reg: c_srid_reg,
            record_reg: c_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: state_table_name.clone(),
        });

        program.preassign_label_to_next_insn(vnew_label);
        emit_visibility_fold(
            program,
            operators,
            prefix_len,
            cnt_start,
            zero_reg,
            p_scratch_reg,
            v_new_reg,
        );
        // Publish only visibility transitions. A row that stays visible keeps
        // its stored first-seen content, so there is nothing to emit.
        program.emit_insn(Insn::Eq {
            lhs: v_old_reg,
            rhs: v_new_reg,
            target_pc: ret_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: v_new_reg,
            target_pc: insert_label,
            jump_if_null: false,
        });
        // Visible before, not now: publish one retraction.
        program.emit_int(-1, out_weight_reg);
        emit_split_view_rowid(
            program,
            has_append,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
        );
        emit_ephemeral_row_with_identity(
            program,
            output,
            view_rowid_reg,
            out_start,
            out_weight_reg,
        );
        program.emit_insn(Insn::Goto {
            target_pc: ret_label,
        });

        program.preassign_label_to_next_insn(insert_label);
        program.emit_int(1, out_weight_reg);
        emit_split_view_rowid(
            program,
            has_append,
            c_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            false,
        );
        emit_ephemeral_row_with_identity(
            program,
            output,
            view_rowid_reg,
            out_start,
            out_weight_reg,
        );
        program.preassign_label_to_next_insn(ret_label);
        program.emit_insn(Insn::Return {
            return_reg: c_return_reg,
            can_fallthrough: false,
        });
    }

    // Append-merge subroutine: apply ((branch, rid), outputs, w) with a
    // single signed multiplicity, the view row existing while it is
    // positive. Signed multiplicities are kept for uniformity with joins;
    // the per-branch capture-order streams cannot dip below zero on their
    // own.
    let append_merge_label = append_cursors.map(|(append_cursor_id, append_index_cursor_id)| {
        let am_label = program.allocate_label();
        program.preassign_label_to_next_insn(am_label);

        let m_found_label = program.allocate_label();
        let m_update_label = program.allocate_label();
        let m_visible_label = program.allocate_label();
        let m_visible_delta_label = program.allocate_label();
        let m_hidden_label = program.allocate_label();
        let m_retract_label = program.allocate_label();
        let m_write_label = program.allocate_label();
        let m_ret_label = program.allocate_label();
        program.emit_insn(Insn::Found {
            cursor_id: append_index_cursor_id,
            target_pc: m_found_label,
            record_reg: akey_start,
            num_regs: 2,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: append_cursor_id,
            rowid_reg: a_srid_reg,
            prev_largest_reg: prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: akey_start,
            dst_reg: a_rec_start,
            extra_amount: 1,
        });
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: a_rec_start + 2,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: a_rec_start as u16,
            count: 3,
            dest_reg: a_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: append_cursor_id,
            key_reg: a_srid_reg,
            record_reg: a_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: append_table_name.clone(),
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: akey_start as u16,
            count: 3,
            dest_reg: a_index_record_reg as u16,
            index_name: Some(append_index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: append_index_cursor_id,
            record_reg: a_index_record_reg,
            unpacked_start: Some(akey_start),
            unpacked_count: Some(3),
            flags: IdxInsertFlags::new(),
        });
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Gt {
            lhs: w_reg,
            rhs: zero_reg,
            target_pc: m_write_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
        });

        program.preassign_label_to_next_insn(m_found_label);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: append_index_cursor_id,
            dest: a_srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: append_cursor_id,
            src_reg: a_srid_reg,
            target_pc: corrupt_label,
        });
        program.emit_insn(Insn::Column {
            cursor_id: append_cursor_id,
            column: 2,
            dest: cur_w_reg,
            default: None,
        });
        program.emit_insn(Insn::Add {
            lhs: w_reg,
            rhs: cur_w_reg,
            dest: new_w_reg,
        });
        program.emit_insn(Insn::Ne {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_update_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        // Multiplicity reached zero: the row is gone. Its view row exists
        // only if the multiplicity was positive before this contribution.
        program.emit_insn(Insn::IdxDelete {
            start_reg: akey_start,
            num_regs: 3,
            cursor_id: append_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: append_cursor_id,
            table_name: append_table_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_hidden_label,
        });

        program.preassign_label_to_next_insn(m_update_label);
        program.emit_insn(Insn::Copy {
            src_reg: akey_start,
            dst_reg: a_rec_start,
            extra_amount: 1,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_w_reg,
            dst_reg: a_rec_start + 2,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: a_rec_start as u16,
            count: 3,
            dest_reg: a_state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: append_cursor_id,
            key_reg: a_srid_reg,
            record_reg: a_state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: append_table_name.clone(),
        });
        program.emit_insn(Insn::Gt {
            lhs: new_w_reg,
            rhs: zero_reg,
            target_pc: m_visible_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_hidden_label,
        });

        program.preassign_label_to_next_insn(m_visible_label);
        // If the row was already visible, only its multiplicity delta is new
        // downstream state. If it crosses from non-positive to positive,
        // publish the complete newly-visible multiplicity; previously
        // suppressed negative deltas must not escape.
        program.emit_insn(Insn::Gt {
            lhs: cur_w_reg,
            rhs: zero_reg,
            target_pc: m_visible_delta_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_write_label,
        });
        program.preassign_label_to_next_insn(m_visible_delta_label);
        program.emit_insn(Insn::Copy {
            src_reg: w_reg,
            dst_reg: out_weight_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_write_label,
        });

        // A non-positive new multiplicity is invisible. Retract exactly the
        // previously visible multiplicity, if any; retain negative state so a
        // later positive contribution can converge without leaking a
        // retraction for a row the downstream consumer has never seen.
        program.preassign_label_to_next_insn(m_hidden_label);
        program.emit_insn(Insn::Gt {
            lhs: cur_w_reg,
            rhs: zero_reg,
            target_pc: m_retract_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
        });
        program.preassign_label_to_next_insn(m_retract_label);
        emit_split_view_rowid(
            program,
            has_append,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
        );
        program.emit_insn(Insn::Subtract {
            lhs: zero_reg,
            rhs: cur_w_reg,
            dest: out_weight_reg,
        });
        emit_ephemeral_row_with_identity(
            program,
            output,
            view_rowid_reg,
            out_start,
            out_weight_reg,
        );
        program.emit_insn(Insn::Goto {
            target_pc: m_ret_label,
        });

        program.preassign_label_to_next_insn(m_write_label);
        // Fresh positive rows arrive here with `w_reg`; updated rows have
        // already selected either their delta or their newly-visible weight.
        program.emit_insn(Insn::IfNot {
            reg: out_weight_reg,
            target_pc: m_ret_label,
            jump_if_null: true,
        });
        emit_split_view_rowid(
            program,
            has_append,
            a_srid_reg,
            two_reg,
            one_reg,
            view_rowid_reg,
            true,
        );
        emit_ephemeral_row_with_identity(
            program,
            output,
            view_rowid_reg,
            out_start,
            out_weight_reg,
        );
        program.preassign_label_to_next_insn(m_ret_label);
        program.emit_insn(Insn::Return {
            return_reg: a_return_reg,
            can_fallthrough: false,
        });
        am_label
    });

    program.preassign_label_to_next_insn(main_start_label);

    for (branch_idx, stream) in branch_streams.iter().enumerate() {
        let branch_done_label = program.allocate_label();
        let loop_label = program.allocate_label();
        let next_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: stream.cursor_id,
            pc_if_empty: branch_done_label,
        });
        program.preassign_label_to_next_insn(loop_label);

        for column in 0..num_view_columns {
            program.emit_insn(Insn::Column {
                cursor_id: stream.cursor_id,
                column: stream.value_start + column,
                dest: out_start + column,
                default: None,
            });
        }
        program.emit_insn(Insn::Column {
            cursor_id: stream.cursor_id,
            column: stream.weight_column,
            dest: w_reg,
            default: None,
        });

        // Row identity: output content for dedup-prefix branches,
        // (branch, source identity record) for trailing UNION ALL branches.
        if branch_idx < prefix_len {
            program.emit_insn(Insn::Copy {
                src_reg: out_start,
                dst_reg: key_start,
                extra_amount: k.saturating_sub(1),
            });
            program.emit_insn(Insn::Gosub {
                target_pc: content_merge_labels[branch_idx],
                return_reg: c_return_reg,
            });
        } else {
            turso_assert!(
                stream.identity_width() > 0,
                "UNION ALL suffix requires a stable source identity"
            );
            program.emit_int(branch_idx as i64, akey_start);
            let identity_start = program.alloc_registers(stream.identity_width());
            for column in 0..stream.identity_width() {
                program.emit_insn(Insn::Column {
                    cursor_id: stream.cursor_id,
                    column,
                    dest: identity_start + column,
                    default: None,
                });
            }
            program.emit_insn(Insn::MakeRecord {
                start_reg: identity_start as u16,
                count: stream.identity_width() as u16,
                dest_reg: (akey_start + 1) as u16,
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::Gosub {
                target_pc: append_merge_label
                    .expect("UNION ALL suffix implies the append-merge subroutine"),
                return_reg: a_return_reg,
            });
        }

        program.preassign_label_to_next_insn(next_label);
        program.emit_insn(Insn::Next {
            cursor_id: stream.cursor_id,
            pc_if_next: loop_label,
        });
        program.preassign_label_to_next_insn(branch_done_label);
    }

    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} state is corrupted"),
        on_error: None,
        description_reg: None,
    });
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
