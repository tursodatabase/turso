use crate::io::{Buffer, Completion, File, TempFile, IO};
use crate::schema::{BTreeTable, Schema};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult, ImmutableRecord, Value};
use crate::util::{extract_view_columns, ViewColumnSchema};
use crate::{LimboError, Result, TempStore};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

/// Version of the materialized-view state storage format, embedded in the
/// internal state table name (`__turso_internal_dbsp_state_v<N>_<view>`).
/// Views created under a different version are unusable and must be
/// recreated.
///
/// v2: maintenance compiled to VDBE programs. Filter/project views have no
/// state table at all; GROUP BY views use a typed state table (group keys,
/// view rowid, aggregate payloads) instead of v1's generic blob layout.
///
/// v3: pure UNION ALL is stateless branch composition. Its view-row identity
/// is derived directly from `(branch, source rowid)` rather than a hidden
/// state-table rowid.
///
/// v4: hidden state is owned and named by DAG node (`__n<node_id>`), rather
/// than inferred from a root shape or UNION ALL branch. This permits multiple
/// stateful operators in one maintenance circuit without name collisions.
///
/// v5: trailing UNION ALL identity keys store the complete source-identity
/// tuple, not a single rowid.
///
/// v6: aggregate state rows also persist each finalized aggregate value. The
/// state table is therefore the aggregate node's output arrangement and can
/// publish `-old/+new` typed deltas to downstream operators.
///
/// v7: mixed deduplicating/UNION ALL set operators always keep their trailing
/// source-identity map, including when their output feeds another operator.
/// This makes the set-op's stable identity part of its typed delta contract
/// rather than a terminal-view-only implementation detail.
///
/// v8: a non-native operator output consumed by a join owns an explicit
/// `(source identity, values, multiplicity)` arrangement. Its rowid is the
/// stable identity published to the downstream join.
pub const DBSP_CIRCUIT_VERSION: u32 = 9;

/// Whether a version-stripped hidden-state key belongs to `view_name`.
///
/// Current keys are `<view>__n<decimal-node-id>`; legacy branch state used
/// `<view>__b<decimal-branch-id>`, and the oldest layout used exactly the view
/// name. Parsing the entire suffix is important: a prefix check would make
/// dropping `v` steal state from a distinct view named `v__n1`.
pub(crate) fn state_key_belongs_to_view(key: &str, view_name: &str) -> bool {
    if key == view_name {
        return true;
    }
    let Some(suffix) = key.strip_prefix(view_name) else {
        return false;
    };
    let node_id = suffix
        .strip_prefix("__n")
        .or_else(|| suffix.strip_prefix("__b"));
    node_id.is_some_and(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()))
}

/// Whether a complete hidden state/multiset table name belongs to a view,
/// independent of the storage version embedded in that table name.
pub(crate) fn state_table_belongs_to_view(table_name: &str, view_name: &str) -> bool {
    table_name
        .strip_prefix(crate::schema::DBSP_TABLE_PREFIX)
        .or_else(|| table_name.strip_prefix(crate::schema::DBSP_MULTISET_TABLE_PREFIX))
        .or_else(|| table_name.strip_prefix(crate::schema::DBSP_ARRANGEMENT_TABLE_PREFIX))
        .and_then(|suffix| suffix.split_once('_'))
        .is_some_and(|(_, key)| state_key_belongs_to_view(key, view_name))
}

/// The automatic primary-key index of a view's internal state table, derived
/// from the state table's PRIMARY KEY (the group columns).
pub fn create_dbsp_state_index(
    root_page: i64,
    state_table: &BTreeTable,
) -> Result<crate::schema::Index> {
    use crate::schema::{Index, IndexColumn};
    let mut columns = Vec::with_capacity(state_table.primary_key_columns.len());
    for (name, order) in &state_table.primary_key_columns {
        let pos_in_table = state_table
            .columns()
            .iter()
            .position(|c| c.name.as_deref() == Some(name.as_str()))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "state table {} primary key column {name} not found",
                    state_table.name
                ))
            })?;
        columns.push(IndexColumn {
            name: name.clone(),
            order: *order,
            // Key columns carry the defining query's comparison collation
            // (see state_table_sql), and the index must compare the same way.
            collation: state_table.columns()[pos_in_table].collation_opt(),
            pos_in_table,
            default: None,
            expr: None,
        });
    }
    Ok(Index {
        name: "dbsp_state_pk".to_string(),
        table_name: state_table.name.clone(),
        root_page,
        columns,
        unique: true,
        ephemeral: false,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    })
}

/// Stable identity of a changed table within a schema epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TableChangeId {
    RootPage(i64),
    #[cfg(test)]
    Name(String),
}

#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub(crate) table_id: TableChangeId,
    pub(crate) rowid: i64,
    pub(crate) values: Vec<Value>,
    pub(crate) weight: isize,
}

#[derive(Debug)]
struct ChangeBuffer {
    tail: Arc<Vec<ChangeEvent>>,
    in_memory_bytes: usize,
    spilled: Vec<SpillSegment>,
}

#[derive(Debug, Clone)]
pub(crate) struct SpillSegment {
    pub(crate) offset: u64,
    pub(crate) len: usize,
    /// Stable table identities present in this segment. Individual event
    /// identities spill with their row images; only this segment-level routing
    /// index stays resident.
    pub(crate) table_ids: Arc<HashSet<TableChangeId>>,
    /// Logical event prefix visible after savepoint rewinds.
    pub(crate) rows: usize,
}

/// A snapshot consumed by a maintenance cursor. Spilled segments precede the
/// in-memory tail and therefore preserve transaction capture order.
#[derive(Clone)]
pub(crate) struct CapturedDelta {
    pub(crate) file: Option<Arc<dyn File>>,
    pub(crate) spilled: Vec<SpillSegment>,
    pub(crate) tail: Arc<Vec<ChangeEvent>>,
    pub(crate) table_id: Option<TableChangeId>,
}

impl CapturedDelta {
    pub(crate) fn empty() -> Self {
        Self {
            file: None,
            spilled: Vec::new(),
            tail: Arc::new(Vec::new()),
            table_id: None,
        }
    }

    #[cfg(test)]
    fn contains_table(&self, table_id: &TableChangeId) -> bool {
        self.spilled
            .iter()
            .any(|segment| segment.rows > 0 && segment.table_ids.contains(table_id))
            || self.tail.iter().any(|event| &event.table_id == table_id)
    }
}

const CHANGE_LOG_SPILL_THRESHOLD: usize = 256 * 1024;

#[derive(Debug)]
struct PendingSpill {
    table_id: TableChangeId,
    completion: Completion,
    segment: SpillSegment,
    /// Identity of the capture that crossed the threshold. A resumed opcode
    /// presents this change again; a different change (UPDATE delete followed
    /// by insert) must be appended after finishing the pending batch.
    triggering_key: i64,
    triggering_values: Vec<Value>,
    triggering_weight: isize,
}

enum PendingSpillStatus {
    None,
    Waiting(Completion),
    Finished(PendingSpill),
}

/// One globally ordered transaction change log. Row images are owned by this
/// stream exactly once; consumers retain only stable table-identity indexes.
struct SharedChangeLog {
    changes: Mutex<ChangeBuffer>,
    subscriptions: Mutex<HashMap<String, HashSet<TableChangeId>>>,
    /// Monotonic content revision for read-your-own-writes cursors.
    /// A row count is insufficient: rewind followed by different changes can
    /// produce the same length with different contents.
    revision: Mutex<u64>,
    io: Arc<dyn IO>,
    temp_file: Mutex<Option<TempFile>>,
    next_spill_offset: Mutex<u64>,
    pending_spill: Mutex<Option<PendingSpill>>,
}

impl SharedChangeLog {
    fn new(io: Arc<dyn IO>) -> Self {
        Self {
            changes: Mutex::new(ChangeBuffer {
                tail: Arc::new(Vec::new()),
                in_memory_bytes: 0,
                spilled: Vec::new(),
            }),
            subscriptions: Mutex::new(HashMap::default()),
            revision: Mutex::new(0),
            io,
            temp_file: Mutex::new(None),
            next_spill_offset: Mutex::new(0),
            pending_spill: Mutex::new(None),
        }
    }

    fn subscribe_views(&self, view_names: &[String], table_id: &TableChangeId) {
        let mut subscriptions = self.subscriptions.lock();
        for view_name in view_names {
            subscriptions
                .entry(view_name.clone())
                .or_default()
                .insert(table_id.clone());
        }
    }

    fn bump_revision(&self) {
        let mut revision = self.revision.lock();
        *revision = revision.wrapping_add(1);
    }

    fn record_insert(
        &self,
        table_id: TableChangeId,
        key: i64,
        values: Vec<Value>,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        self.record_change(table_id, key, values, 1, temp_store)
    }

    fn record_delete(
        &self,
        table_id: TableChangeId,
        key: i64,
        values: Vec<Value>,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        self.record_change(table_id, key, values, -1, temp_store)
    }

    fn record_change(
        &self,
        table_id: TableChangeId,
        key: i64,
        values: Vec<Value>,
        weight: isize,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        match self.finish_pending_spill()? {
            PendingSpillStatus::None => {}
            PendingSpillStatus::Waiting(completion) => {
                return Ok(IOResult::IO(IOCompletions::Single(completion)));
            }
            PendingSpillStatus::Finished(pending) => {
                let is_resumed_trigger = pending.table_id == table_id
                    && pending.triggering_key == key
                    && pending.triggering_weight == weight
                    && pending.triggering_values == values;
                if is_resumed_trigger {
                    return Ok(IOResult::Done(()));
                }
            }
        }

        let record_bytes = serialized_change_size(&table_id, key, weight, &values)?;
        {
            let mut changes = self.changes.lock();
            Arc::make_mut(&mut changes.tail).push(ChangeEvent {
                table_id: table_id.clone(),
                rowid: key,
                values: values.clone(),
                weight,
            });
            changes.in_memory_bytes = changes.in_memory_bytes.saturating_add(record_bytes);
            self.bump_revision();
            if changes.in_memory_bytes < CHANGE_LOG_SPILL_THRESHOLD {
                return Ok(IOResult::Done(()));
            }
        }

        self.start_spill(table_id, key, values, weight, temp_store)
    }

    /// Complete an outstanding append. Returns its identity so the resumed
    /// opcode can distinguish "same capture, already appended" from the next
    /// capture in an UPDATE pair.
    fn finish_pending_spill(&self) -> Result<PendingSpillStatus> {
        let Some(pending) = self.pending_spill.lock().take() else {
            return Ok(PendingSpillStatus::None);
        };
        if !pending.completion.finished() {
            let completion = pending.completion.clone();
            *self.pending_spill.lock() = Some(pending);
            return Ok(PendingSpillStatus::Waiting(completion));
        }
        if let Some(error) = pending.completion.get_error() {
            return Err(LimboError::CompletionError(error));
        }
        let mut changes = self.changes.lock();
        Arc::make_mut(&mut changes.tail).drain(..pending.segment.rows);
        changes.in_memory_bytes = 0;
        changes.spilled.push(pending.segment.clone());
        Ok(PendingSpillStatus::Finished(pending))
    }

    fn start_spill(
        &self,
        table_id: TableChangeId,
        key: i64,
        values: Vec<Value>,
        weight: isize,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        let (buffer, table_ids, rows) = {
            let changes = self.changes.lock();
            (
                serialize_change_events(&changes.tail)?,
                Arc::new(
                    changes
                        .tail
                        .iter()
                        .map(|event| event.table_id.clone())
                        .collect::<HashSet<_>>(),
                ),
                changes.tail.len(),
            )
        };
        let len = buffer.len();
        let offset = {
            let mut next = self.next_spill_offset.lock();
            let offset = *next;
            *next = next.saturating_add(len as u64);
            offset
        };
        let file = {
            let mut temp_file = self.temp_file.lock();
            if temp_file.is_none() {
                *temp_file = Some(TempFile::with_temp_store(&self.io, temp_store)?);
            }
            temp_file
                .as_ref()
                .expect("temp file initialized above")
                .file
                .clone()
        };
        let completion = Completion::new_write(|_| {});
        let completion = file.pwrite(offset, Arc::new(Buffer::new(buffer)), completion)?;
        *self.pending_spill.lock() = Some(PendingSpill {
            table_id,
            completion: completion.clone(),
            segment: SpillSegment {
                offset,
                len,
                table_ids,
                rows,
            },
            triggering_key: key,
            triggering_values: values,
            triggering_weight: weight,
        });
        Ok(IOResult::IO(IOCompletions::Single(completion)))
    }

    fn snapshot(&self, table_id: &TableChangeId) -> CapturedDelta {
        let changes = self.changes.lock();
        CapturedDelta {
            file: self
                .temp_file
                .lock()
                .as_ref()
                .map(|temp_file| temp_file.file.clone()),
            spilled: changes.spilled.clone(),
            tail: changes.tail.clone(),
            table_id: Some(table_id.clone()),
        }
    }
}

impl std::fmt::Debug for SharedChangeLog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionChangeLog")
            .field("events", &self.changes.lock().tail.len())
            .field("subscriptions", &self.subscriptions.lock().len())
            .field("has_spill_file", &self.temp_file.lock().is_some())
            .finish()
    }
}

fn serialized_change_size(
    table_id: &TableChangeId,
    key: i64,
    weight: isize,
    values: &[Value],
) -> Result<usize> {
    let mut record_values = Vec::with_capacity(values.len() + 3);
    record_values.push(table_id_value(table_id));
    record_values.push(Value::from_i64(key));
    record_values.push(Value::from_i64(weight as i64));
    record_values.extend_from_slice(values);
    Ok(
        8 + ImmutableRecord::from_values(&record_values, record_values.len())?
            .get_payload()
            .len(),
    )
}

fn serialize_change_events(events: &[ChangeEvent]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for event in events {
        let mut values = Vec::with_capacity(event.values.len() + 3);
        values.push(table_id_value(&event.table_id));
        values.push(Value::from_i64(event.rowid));
        values.push(Value::from_i64(event.weight as i64));
        values.extend_from_slice(&event.values);
        let record = ImmutableRecord::from_values(&values, values.len())?;
        let payload = record.get_payload();
        output.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        output.extend_from_slice(payload);
    }
    Ok(output)
}

fn table_id_value(table_id: &TableChangeId) -> Value {
    match table_id {
        TableChangeId::RootPage(root_page) => Value::from_i64(*root_page),
        #[cfg(test)]
        TableChangeId::Name(name) => Value::Text(crate::types::Text::new(name.clone())),
    }
}

/// Per-view subscription into the connection's transaction-wide change log.
#[derive(Debug, Clone)]
pub struct ViewChangeSubscription {
    view_name: String,
    change_log: Arc<SharedChangeLog>,
}

impl ViewChangeSubscription {
    fn new(view_name: String, change_log: Arc<SharedChangeLog>) -> Self {
        Self {
            view_name,
            change_log,
        }
    }

    #[cfg(test)]
    pub fn insert(&self, table_name: &str, key: i64, values: Vec<Value>) {
        let table_id = TableChangeId::Name(table_name.to_string());
        self.change_log
            .subscribe_views(std::slice::from_ref(&self.view_name), &table_id);
        let mut changes = self.change_log.changes.lock();
        Arc::make_mut(&mut changes.tail).push(ChangeEvent {
            table_id,
            rowid: key,
            values,
            weight: 1,
        });
        self.change_log.bump_revision();
    }

    #[cfg(test)]
    pub fn delete(&self, table_name: &str, key: i64, values: Vec<Value>) {
        let table_id = TableChangeId::Name(table_name.to_string());
        self.change_log
            .subscribe_views(std::slice::from_ref(&self.view_name), &table_id);
        let mut changes = self.change_log.changes.lock();
        Arc::make_mut(&mut changes.tail).push(ChangeEvent {
            table_id,
            rowid: key,
            values,
            weight: -1,
        });
        self.change_log.bump_revision();
    }

    /// Clone only the requested table's stream, rather than every delta
    /// captured for the view.
    pub(crate) fn table_delta(&self, table: &BTreeTable) -> CapturedDelta {
        let root_id = TableChangeId::RootPage(table.root_page);
        let captured = self.change_log.snapshot(&root_id);
        #[cfg(test)]
        {
            if !captured.contains_table(&root_id) {
                return self
                    .change_log
                    .snapshot(&TableChangeId::Name(table.name.clone()));
            }
        }
        captured
    }

    pub fn is_empty(&self) -> bool {
        let subscriptions = self.change_log.subscriptions.lock();
        let Some(table_ids) = subscriptions.get(&self.view_name) else {
            return true;
        };
        let changes = self.change_log.changes.lock();
        !changes.spilled.iter().any(|segment| {
            segment.rows > 0 && segment.table_ids.iter().any(|id| table_ids.contains(id))
        }) && !changes
            .tail
            .iter()
            .any(|event| table_ids.contains(&event.table_id))
    }
}

/// Global event position recorded at a statement or named-savepoint boundary.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChangeLogMark(usize);

/// Connection-scoped transaction change stream and its consumer subscriptions.
#[derive(Debug, Clone)]
pub struct TransactionChanges {
    change_log: Arc<SharedChangeLog>,
}
crate::assert::assert_send_sync!(TransactionChanges);

impl TransactionChanges {
    /// Create a transaction-wide change stream.
    pub fn new() -> Self {
        Self::with_io(Arc::new(crate::MemoryIO::new()))
    }

    pub(crate) fn with_io(io: Arc<dyn IO>) -> Self {
        Self {
            change_log: Arc::new(SharedChangeLog::new(io)),
        }
    }

    /// Construct a lightweight handle over this transaction's shared log.
    pub fn view_subscription(&self, view_name: &str) -> Arc<ViewChangeSubscription> {
        Arc::new(ViewChangeSubscription::new(
            view_name.to_string(),
            self.change_log.clone(),
        ))
    }

    /// Get a handle only when the view has subscribed to at least one table.
    pub fn get(&self, view_name: &str) -> Option<Arc<ViewChangeSubscription>> {
        self.change_log
            .subscriptions
            .lock()
            .contains_key(view_name)
            .then(|| self.view_subscription(view_name))
    }

    /// Clear the transaction-wide stream and all subscriptions.
    pub fn clear(&self) {
        let mut changes = self.change_log.changes.lock();
        changes.tail = Arc::new(Vec::new());
        changes.in_memory_bytes = 0;
        changes.spilled.clear();
        drop(changes);
        self.change_log.subscriptions.lock().clear();
        self.change_log.pending_spill.lock().take();
        self.change_log.temp_file.lock().take();
        *self.change_log.next_spill_offset.lock() = 0;
        *self.change_log.revision.lock() = 0;
    }

    /// Check if the shared transaction log has no captured changes.
    pub fn is_empty(&self) -> bool {
        let changes = self.change_log.changes.lock();
        changes.spilled.iter().all(|segment| segment.rows == 0) && changes.tail.is_empty()
    }

    /// Monotonic revision of the transaction-wide logical change log.
    ///
    /// Materialized-view cursors use this global generation rather than only
    /// their direct subscription length: a downstream view has no direct
    /// delta until its upstream view is maintained, but a read must still
    /// drive the dependency-ordered maintenance batch inside the transaction.
    pub fn change_revision(&self) -> u64 {
        *self.change_log.revision.lock()
    }

    /// Get all views subscribed to the transaction stream.
    pub fn get_view_names(&self) -> Vec<String> {
        self.change_log
            .subscriptions
            .lock()
            .keys()
            .cloned()
            .collect()
    }

    /// Record an insertion once and subscribe every dependent view.
    pub fn insert(
        &self,
        table: &BTreeTable,
        view_names: &[String],
        key: i64,
        values: Vec<Value>,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        let table_id = TableChangeId::RootPage(table.root_page);
        self.change_log.subscribe_views(view_names, &table_id);
        self.change_log
            .record_insert(table_id, key, values, temp_store)
    }

    /// Record a deletion once and subscribe every dependent view.
    pub fn delete(
        &self,
        table: &BTreeTable,
        view_names: &[String],
        key: i64,
        values: Vec<Value>,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        let table_id = TableChangeId::RootPage(table.root_page);
        self.change_log.subscribe_views(view_names, &table_id);
        self.change_log
            .record_delete(table_id, key, values, temp_store)
    }

    /// Record an arbitrary signed contribution once and subscribe every
    /// dependent view. Materialized-view records use their trailing physical
    /// multiplicity as this logical weight.
    pub fn change(
        &self,
        table: &BTreeTable,
        view_names: &[String],
        key: i64,
        values: Vec<Value>,
        weight: isize,
        temp_store: TempStore,
    ) -> Result<IOResult<()>> {
        if weight == 0 {
            return Ok(IOResult::Done(()));
        }
        let table_id = TableChangeId::RootPage(table.root_page);
        self.change_log.subscribe_views(view_names, &table_id);
        self.change_log
            .record_change(table_id, key, values, weight, temp_store)
    }

    /// Snapshot the global event position at statement start.
    ///
    /// Delta capture happens inside `op_insert`/`op_delete` as rows are
    /// written, so when a statement subtransaction rolls back, captures made
    /// by the aborted statement must be rewound or they would be applied to
    /// the views as phantom changes.
    pub fn change_log_mark(&self) -> ChangeLogMark {
        let changes = self.change_log.changes.lock();
        ChangeLogMark(
            changes
                .spilled
                .iter()
                .map(|segment| segment.rows)
                .sum::<usize>()
                + changes.tail.len(),
        )
    }

    /// Rewind the ordered event stream to a statement or savepoint boundary.
    pub fn rewind_to_mark(&self, mark: ChangeLogMark) -> Result<()> {
        if self.change_log.pending_spill.lock().is_some() {
            return Err(LimboError::InternalError(
                "cannot rewind a transaction change log with pending spill I/O".to_string(),
            ));
        }
        let mut changes = self.change_log.changes.lock();
        let spilled_rows = changes
            .spilled
            .iter()
            .map(|segment| segment.rows)
            .sum::<usize>();
        let current_len = spilled_rows + changes.tail.len();
        if mark.0 > current_len {
            return Err(LimboError::InternalError(format!(
                "transaction change-log mark {} is ahead of current length {current_len}",
                mark.0
            )));
        }
        if mark.0 >= spilled_rows {
            Arc::make_mut(&mut changes.tail).truncate(mark.0 - spilled_rows);
        } else {
            let mut remaining = mark.0;
            changes.spilled.retain_mut(|segment| {
                if remaining == 0 {
                    return false;
                }
                if segment.rows <= remaining {
                    remaining -= segment.rows;
                    true
                } else {
                    // Spill bytes are immutable. Restrict the segment to its
                    // logical prefix and reclaim the unreachable suffix when
                    // the transaction log is cleared.
                    segment.rows = remaining;
                    remaining = 0;
                    true
                }
            });
            Arc::make_mut(&mut changes.tail).clear();
        }
        changes.in_memory_bytes = serialize_change_events(&changes.tail)?.len();
        self.change_log.bump_revision();
        Ok(())
    }
}

impl Default for TransactionChanges {
    fn default() -> Self {
        Self::new()
    }
}

/// A materialized view: its defining SELECT, output schema, and storage root.
///
/// Maintenance is performed by compiled VDBE programs (see
/// `incremental::vdbe_maintenance`): the transaction's captured deltas are
/// merged into the view's btree at statement completion (or lazily before a
/// view read reached inside the write statement), and initial population runs
/// the same program over the base table.
#[derive(Debug)]
pub struct IncrementalView {
    name: String,
    // The SELECT statement that defines how to transform input data
    pub select_stmt: ast::Select,

    // The view's column schema with table relationships
    pub column_schema: ViewColumnSchema,
    // Root page of the btree storing the materialized state (0 for unmaterialized)
    root_page: i64,
}

crate::assert::assert_send_sync!(IncrementalView);

impl IncrementalView {
    pub fn from_sql(sql: &str, schema: &Schema, main_data_root: i64) -> Result<Self> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        let cmd = cmd.expect("View is an empty statement");
        match cmd {
            Cmd::Stmt(Stmt::CreateMaterializedView {
                if_not_exists: _,
                view_name,
                columns: _,
                select,
            }) => {
                let column_schema = extract_view_columns(&select, schema)?;
                Ok(Self {
                    name: view_name.name.as_str().to_string(),
                    select_stmt: select,
                    column_schema,
                    root_page: main_data_root,
                })
            }
            _ => Err(LimboError::ParseError(format!(
                "View is not a CREATE MATERIALIZED VIEW statement: {sql}"
            ))),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the root page for this materialized view's btree
    pub fn get_root_page(&self) -> i64 {
        self.root_page
    }

    /// Syntactic relation dependencies, independent of schema load order.
    ///
    /// Schema reconstruction uses this to order persisted materialized views
    /// before binding them. The executable DAG is still produced by the main
    /// bound planner; this walker is identity metadata only.
    pub(crate) fn referenced_table_names(select: &ast::Select) -> Vec<String> {
        fn collect_source(
            source: &ast::SelectTable,
            cte_names: &HashSet<String>,
            tables: &mut HashSet<String>,
        ) {
            match source {
                ast::SelectTable::Table(name, _, _) => {
                    if cte_names
                        .iter()
                        .any(|cte| cte.eq_ignore_ascii_case(name.name.as_str()))
                    {
                        return;
                    }
                    tables.insert(crate::util::normalize_ident(name.name.as_str()));
                }
                ast::SelectTable::Select(select, _) => collect_select(select, cte_names, tables),
                ast::SelectTable::Sub(from, _) => {
                    collect_source(&from.select, cte_names, tables);
                    for join in &from.joins {
                        collect_source(&join.table, cte_names, tables);
                    }
                }
                ast::SelectTable::TableCall(_, _, _) => {}
            }
        }

        fn collect_one(
            select: &ast::OneSelect,
            cte_names: &HashSet<String>,
            tables: &mut HashSet<String>,
        ) {
            let ast::OneSelect::Select {
                from: Some(from), ..
            } = select
            else {
                return;
            };
            for source in std::iter::once(from.select.as_ref())
                .chain(from.joins.iter().map(|join| join.table.as_ref()))
            {
                collect_source(source, cte_names, tables);
            }
        }

        fn collect_select(
            select: &ast::Select,
            inherited_ctes: &HashSet<String>,
            tables: &mut HashSet<String>,
        ) {
            let mut cte_names = inherited_ctes.clone();
            if let Some(with) = &select.with {
                for cte in &with.ctes {
                    cte_names.insert(crate::util::normalize_ident(cte.tbl_name.as_str()));
                }
                for cte in &with.ctes {
                    collect_select(&cte.select, &cte_names, tables);
                }
            }
            collect_one(&select.body.select, &cte_names, tables);
            for compound in &select.body.compounds {
                collect_one(&compound.select, &cte_names, tables);
            }
        }

        let mut tables = HashSet::default();
        collect_select(select, &HashSet::default(), &mut tables);
        tables.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::vec;
    use crate::schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column as SchemaColumn, Schema, Type,
    };
    use crate::sync::Arc;
    use turso_parser::ast;

    #[test]
    fn transaction_change_log_captures_once_for_multiple_views() {
        let schema = create_test_schema();
        let table = schema.get_btree_table("customers").unwrap();
        let states = TransactionChanges::new();
        let views = vec!["first".to_string(), "second".to_string()];

        let result = states.insert(
            &table,
            &views,
            7,
            vec![Value::from_i64(7), Value::build_text("Ada")],
            TempStore::Memory,
        );
        assert!(matches!(result, Ok(IOResult::Done(()))));

        let changes = states.change_log.changes.lock();
        assert_eq!(changes.tail.len(), 1, "the base row is captured only once");
        drop(changes);
        let first = states.get("first").unwrap().table_delta(&table);
        let second = states.get("second").unwrap().table_delta(&table);
        assert_eq!(first.tail.len(), 1);
        assert_eq!(second.tail.len(), 1);
        assert!(
            Arc::ptr_eq(&first.tail, &second.tail),
            "subscribers must share the in-memory transaction-log tail"
        );

        let mark = states.change_log_mark();
        let result = states.delete(
            &table,
            &views,
            7,
            vec![Value::from_i64(7), Value::build_text("Ada")],
            TempStore::Memory,
        );
        assert!(matches!(result, Ok(IOResult::Done(()))));
        states.rewind_to_mark(mark).unwrap();
        assert_eq!(
            states.get("first").unwrap().table_delta(&table).tail.len(),
            1
        );
        assert_eq!(
            states.get("second").unwrap().table_delta(&table).tail.len(),
            1
        );
    }

    #[test]
    fn transaction_change_log_preserves_non_unit_weights() {
        let schema = create_test_schema();
        let table = schema.get_btree_table("customers").unwrap();
        let states = TransactionChanges::new();
        let views = vec!["downstream".to_string()];
        let values = vec![Value::from_i64(7), Value::build_text("Ada")];

        assert!(matches!(
            states.change(&table, &views, 7, values.clone(), 3, TempStore::Memory,),
            Ok(IOResult::Done(()))
        ));
        let captured = states.get("downstream").unwrap().table_delta(&table);
        assert_eq!(captured.tail.len(), 1);
        assert_eq!(captured.tail[0].rowid, 7);
        assert_eq!(captured.tail[0].values, values);
        assert_eq!(captured.tail[0].weight, 3);
    }

    #[test]
    fn transaction_change_log_orders_tables_globally_and_projects_by_identity() {
        let schema = create_test_schema();
        let customers = schema.get_btree_table("customers").unwrap();
        let orders = schema.get_btree_table("orders").unwrap();
        let states = TransactionChanges::new();
        let views = vec!["joined_view".to_string()];

        assert!(matches!(
            states.insert(
                &customers,
                &views,
                1,
                vec![Value::from_i64(1), Value::build_text("Ada")],
                TempStore::Memory,
            ),
            Ok(IOResult::Done(()))
        ));
        assert!(matches!(
            states.insert(
                &orders,
                &views,
                10,
                vec![Value::from_i64(10), Value::from_i64(1), Value::from_i64(50)],
                TempStore::Memory,
            ),
            Ok(IOResult::Done(()))
        ));
        assert!(matches!(
            states.delete(
                &customers,
                &views,
                1,
                vec![Value::from_i64(1), Value::build_text("Ada")],
                TempStore::Memory,
            ),
            Ok(IOResult::Done(()))
        ));

        let changes = states.change_log.changes.lock();
        assert_eq!(changes.tail.len(), 3);
        assert_eq!(
            changes.tail[0].table_id,
            TableChangeId::RootPage(customers.root_page)
        );
        assert_eq!(
            changes.tail[1].table_id,
            TableChangeId::RootPage(orders.root_page)
        );
        assert_eq!(
            changes.tail[2].table_id,
            TableChangeId::RootPage(customers.root_page)
        );
        drop(changes);

        let state = states.get("joined_view").unwrap();
        let customers_delta = state.table_delta(&customers);
        let orders_delta = state.table_delta(&orders);
        assert_eq!(
            customers_delta
                .tail
                .iter()
                .filter(|event| event.table_id == TableChangeId::RootPage(customers.root_page))
                .count(),
            2
        );
        assert_eq!(
            orders_delta
                .tail
                .iter()
                .filter(|event| event.table_id == TableChangeId::RootPage(orders.root_page))
                .count(),
            1
        );
        assert!(
            Arc::ptr_eq(&customers_delta.tail, &orders_delta.tail),
            "table projections must share the canonical event stream"
        );

        let mut customers_cursor = crate::incremental::vdbe_maintenance::DeltaCursor::new(
            customers_delta,
            customers.columns().len(),
        );
        assert!(matches!(
            customers_cursor.rewind(),
            Ok(IOResult::Done(true))
        ));
        assert_eq!(customers_cursor.rowid(), 1);
        assert_eq!(
            customers_cursor.column(customers.columns().len()),
            Value::from_i64(1)
        );
        assert!(matches!(customers_cursor.next(), Ok(IOResult::Done(true))));
        assert_eq!(
            customers_cursor.column(customers.columns().len()),
            Value::from_i64(-1)
        );
        assert!(matches!(customers_cursor.next(), Ok(IOResult::Done(false))));
    }

    #[test]
    fn spilled_change_log_rewinds_to_a_logical_segment_prefix() {
        let schema = create_test_schema();
        let table = schema.get_btree_table("customers").unwrap();
        let orders = schema.get_btree_table("orders").unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
        let states = TransactionChanges::with_io(io.clone());
        let views = vec!["customers_view".to_string()];

        let first = vec![Value::from_i64(1), Value::build_text("kept")];
        assert!(matches!(
            states.insert(&table, &views, 1, first.clone(), TempStore::Memory),
            Ok(IOResult::Done(()))
        ));
        let mark = states.change_log_mark();

        let large = vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::Blob(vec![b'x'; CHANGE_LOG_SPILL_THRESHOLD]),
        ];
        let completion = match states
            .insert(&orders, &views, 2, large.clone(), TempStore::Memory)
            .unwrap()
        {
            IOResult::IO(completions) => completions,
            IOResult::Done(()) => panic!("large capture did not spill"),
        };
        completion.wait(io.as_ref()).unwrap();
        assert!(matches!(
            states.insert(&orders, &views, 2, large, TempStore::Memory),
            Ok(IOResult::Done(()))
        ));

        states.rewind_to_mark(mark).unwrap();
        let captured = states.get("customers_view").unwrap().table_delta(&table);
        assert_eq!(captured.spilled.len(), 1);
        assert_eq!(captured.spilled[0].rows, 1);
        let orders_captured = states.get("customers_view").unwrap().table_delta(&orders);
        let mut orders_cursor = crate::incremental::vdbe_maintenance::DeltaCursor::new(
            orders_captured,
            orders.columns().len(),
        );
        loop {
            match orders_cursor.rewind().unwrap() {
                IOResult::Done(has_row) => {
                    assert!(!has_row);
                    break;
                }
                IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
            }
        }

        let mut cursor =
            crate::incremental::vdbe_maintenance::DeltaCursor::new(captured, table.columns().len());
        loop {
            match cursor.rewind().unwrap() {
                IOResult::Done(has_row) => {
                    assert!(has_row);
                    break;
                }
                IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
            }
        }
        assert_eq!(cursor.rowid(), 1);
        assert_eq!(cursor.column(1), first[1]);
        assert_eq!(cursor.column(table.columns().len()), Value::from_i64(1));
        assert!(matches!(cursor.next(), Ok(IOResult::Done(false))));
    }

    // Helper function to create a test schema with multiple tables
    fn create_test_schema() -> Schema {
        let mut schema = Schema::new();

        // Create customers table
        let columns = vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    explicit_notnull: false,
                    unique: false,
                    hidden: false,
                    notnull_conflict_clause: None,
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        ];
        let customers_table = BTreeTable::new(
            2,
            "customers".to_string(),
            vec![("id".to_string(), ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        // Create orders table
        let columns = vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    explicit_notnull: false,
                    unique: false,
                    hidden: false,
                    notnull_conflict_clause: None,
                },
            ),
            SchemaColumn::new(
                Some("customer_id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            ),
            SchemaColumn::new_default_integer(
                Some("total".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let orders_table = BTreeTable::new(
            3,
            "orders".to_string(),
            vec![("id".to_string(), ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        // Create products table
        let columns = vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    explicit_notnull: false,
                    unique: false,
                    hidden: false,
                    notnull_conflict_clause: None,
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
            SchemaColumn::new(
                Some("price".to_string()),
                "REAL".to_string(),
                None,
                None,
                Type::Real,
                None,
                ColDef::default(),
            ),
        ];
        let products_table = BTreeTable::new(
            4,
            "products".to_string(),
            vec![("id".to_string(), ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        // Create logs table - without a rowid alias (no INTEGER PRIMARY KEY)
        let columns = vec![
            SchemaColumn::new(
                Some("message".to_string()),
                "TEXT".to_string(),
                None,
                None,
                Type::Text,
                None,
                ColDef::default(),
            ),
            SchemaColumn::new_default_integer(
                Some("level".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("timestamp".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        // logs has no primary key (no rowid alias) but does have an implicit rowid.
        let logs_table = BTreeTable::new(
            5,
            "logs".to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        schema
            .add_btree_table(Arc::new(customers_table))
            .expect("Test setup: failed to add customers table");

        schema
            .add_btree_table(Arc::new(orders_table))
            .expect("Test setup: failed to add orders table");

        schema
            .add_btree_table(Arc::new(products_table))
            .expect("Test setup: failed to add products table");

        schema
            .add_btree_table(Arc::new(logs_table))
            .expect("Test setup: failed to add logs table");

        schema
    }
}
