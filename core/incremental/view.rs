use crate::alloc::{TursoIteratorExt, TursoTryWithCapacityExt, TursoVecExt};
use crate::io::{Buffer, Completion, File, TempFile, IO};
use crate::schema::{BTreeTable, Schema};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult, ImmutableRecord, Value};
use crate::util::{extract_view_columns, ViewColumnSchema};
use crate::{turso_assert, LimboError, Result, TempStore};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

/// Version of the materialized-view storage format embedded in every hidden
/// table name. Version 1 is the interpreter layout released on `main`;
/// version 2 is the VDBE-maintained operator-DAG layout developed on this
/// branch. Bump this only after a layout has shipped or been shared.
pub const DBSP_CIRCUIT_VERSION: u32 = 2;

pub(crate) fn dbsp_version_marker_table_name(view_name: &str) -> String {
    format!(
        "{}{DBSP_CIRCUIT_VERSION}_{view_name}",
        crate::schema::DBSP_METADATA_TABLE_PREFIX
    )
}

/// Whether a version-stripped hidden-state key belongs to `view_name`.
///
/// Current keys are `<view>__n<decimal-node-id>` with an optional
/// `__a<decimal-aggregate-id>` suffix; legacy branch state used
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
    if let Some(branch_id) = suffix.strip_prefix("__b") {
        return !branch_id.is_empty() && branch_id.bytes().all(|byte| byte.is_ascii_digit());
    }
    let Some(node_suffix) = suffix.strip_prefix("__n") else {
        return false;
    };
    let node_id_len = node_suffix
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if node_id_len == 0 {
        return false;
    }
    let remainder = &node_suffix[node_id_len..];
    if remainder.is_empty() {
        return true;
    }
    remainder == "__g"
        || remainder == "__r"
        || remainder.strip_prefix("__a").is_some_and(|aggregate_id| {
            !aggregate_id.is_empty() && aggregate_id.bytes().all(|byte| byte.is_ascii_digit())
        })
}

/// Whether a complete hidden metadata or operator table belongs to a view,
/// independent of the storage version embedded in that table name.
pub(crate) fn state_table_belongs_to_view(table_name: &str, view_name: &str) -> bool {
    if let Some((_, stored_view_name)) = table_name
        .strip_prefix(crate::schema::DBSP_METADATA_TABLE_PREFIX)
        .and_then(|suffix| suffix.split_once('_'))
    {
        return stored_view_name == view_name;
    }
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
    let mut columns =
        crate::alloc::Vec::try_with_capacity_ext(state_table.primary_key_columns.len())?;
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
        columns.try_push(IndexColumn {
            name: name.clone(),
            order: *order,
            nulls_order: None,
            // Key columns carry the defining query's comparison collation
            // (see state_table_sql), and the index must compare the same way.
            collation: state_table.columns()[pos_in_table].collation_opt(),
            pos_in_table,
            default: None,
            expr: None,
        })?;
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
    pub(crate) values: crate::alloc::Vec<Value>,
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
    completion: Completion,
    segment: SpillSegment,
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
        values: crate::alloc::Vec<Value>,
        temp_store: TempStore,
    ) -> Result<bool> {
        self.record_change(table_id, key, values, 1, temp_store)
    }

    fn record_delete(
        &self,
        table_id: TableChangeId,
        key: i64,
        values: crate::alloc::Vec<Value>,
        temp_store: TempStore,
    ) -> Result<bool> {
        self.record_change(table_id, key, values, -1, temp_store)
    }

    /// Append one change event to the log. Returns `true` when the in-memory
    /// tail crossed the spill threshold and an asynchronous write to the temp
    /// file is now in flight; the caller must wait for it (`finish_spill`)
    /// before recording anything else, since only one spill can be in flight
    /// at a time. The event is appended either way: recording never returns
    /// IO and never needs to be repeated.
    fn record_change(
        &self,
        table_id: TableChangeId,
        key: i64,
        values: crate::alloc::Vec<Value>,
        weight: isize,
        temp_store: TempStore,
    ) -> Result<bool> {
        turso_assert!(
            self.pending_spill.lock().is_none(),
            "a change cannot be recorded while a change-log spill is in flight"
        );
        let record_bytes = serialized_change_size(&table_id, key, weight, &values)?;
        {
            let mut changes = self.changes.lock();
            Arc::make_mut(&mut changes.tail).push(ChangeEvent {
                table_id,
                rowid: key,
                values,
                weight,
            });
            changes.in_memory_bytes = changes.in_memory_bytes.saturating_add(record_bytes);
            self.bump_revision();
            if changes.in_memory_bytes < CHANGE_LOG_SPILL_THRESHOLD {
                return Ok(false);
            }
        }

        self.start_spill(temp_store)?;
        Ok(true)
    }

    /// Wait for a spill started by `record_change` to finish, draining its
    /// rows from the in-memory tail into a recorded segment. A no-op when no
    /// spill is in flight.
    fn finish_pending_spill(&self) -> Result<IOResult<()>> {
        {
            let pending_spill = self.pending_spill.lock();
            let Some(pending) = pending_spill.as_ref() else {
                return Ok(IOResult::Done(()));
            };
            if !pending.completion.finished() {
                return Ok(IOResult::IO(IOCompletions(pending.completion.clone())));
            }
            if let Some(error) = pending.completion.get_error() {
                return Err(LimboError::CompletionError(error));
            }
        }
        let pending = self
            .pending_spill
            .lock()
            .take()
            .expect("checked Some above");
        let mut changes = self.changes.lock();
        Arc::make_mut(&mut changes.tail).drain(..pending.segment.rows);
        changes.in_memory_bytes = 0;
        changes.spilled.push(pending.segment);
        Ok(IOResult::Done(()))
    }

    fn start_spill(&self, temp_store: TempStore) -> Result<()> {
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
            completion,
            segment: SpillSegment {
                offset,
                len,
                table_ids,
                rows,
            },
        });
        Ok(())
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
    let mut record_values = crate::alloc::Vec::try_with_capacity_ext(values.len() + 3)?;
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

fn serialize_change_events(events: &[ChangeEvent]) -> Result<std::vec::Vec<u8>> {
    let mut output = std::vec::Vec::new();
    for event in events {
        let mut values = std::vec::Vec::with_capacity(event.values.len() + 3);
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
    pub fn insert(&self, table_name: &str, key: i64, values: crate::alloc::Vec<Value>) {
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
    pub fn delete(&self, table_name: &str, key: i64, values: crate::alloc::Vec<Value>) {
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
    pub fn get_view_names(&self) -> Result<crate::alloc::Vec<String>> {
        Ok(self
            .change_log
            .subscriptions
            .lock()
            .keys()
            .cloned()
            .try_collect()?)
    }

    /// Record an insertion once and subscribe every dependent view. Returns
    /// `true` when this started a change-log spill the caller must wait for
    /// (`finish_spill`) before recording anything else.
    pub fn insert(
        &self,
        table: &BTreeTable,
        view_names: &[String],
        key: i64,
        values: crate::alloc::Vec<Value>,
        temp_store: TempStore,
    ) -> Result<bool> {
        let table_id = TableChangeId::RootPage(table.root_page);
        self.change_log.subscribe_views(view_names, &table_id);
        self.change_log
            .record_insert(table_id, key, values, temp_store)
    }

    /// Record a deletion once and subscribe every dependent view. See
    /// `insert` for the return value.
    pub fn delete(
        &self,
        table: &BTreeTable,
        view_names: &[String],
        key: i64,
        values: crate::alloc::Vec<Value>,
        temp_store: TempStore,
    ) -> Result<bool> {
        let table_id = TableChangeId::RootPage(table.root_page);
        self.change_log.subscribe_views(view_names, &table_id);
        self.change_log
            .record_delete(table_id, key, values, temp_store)
    }

    /// Record an arbitrary signed contribution once and subscribe every
    /// dependent view. Materialized-view records use their trailing physical
    /// multiplicity as this logical weight. See `insert` for the return
    /// value.
    pub fn change(
        &self,
        table: &BTreeTable,
        view_names: &[String],
        key: i64,
        values: crate::alloc::Vec<Value>,
        weight: isize,
        temp_store: TempStore,
    ) -> Result<bool> {
        if weight == 0 {
            return Ok(false);
        }
        let table_id = TableChangeId::RootPage(table.root_page);
        self.change_log.subscribe_views(view_names, &table_id);
        self.change_log
            .record_change(table_id, key, values, weight, temp_store)
    }

    /// Wait for a spill that `insert`/`delete`/`change` started. The caller
    /// must not record another change until this returns `Done`.
    pub fn finish_spill(&self) -> Result<IOResult<()>> {
        self.change_log.finish_pending_spill()
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
        // A spill's rows leave the in-memory tail only when the spill is
        // collected, so the tail still holds them. Dropping the pending spill
        // leaves its bytes unused in the temp file, and the rewind below works
        // on the tail alone.
        self.change_log.pending_spill.lock().take();
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
    pub(crate) fn referenced_table_names(select: &ast::Select) -> std::vec::Vec<String> {
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
                ast::SelectTable::Select { select, .. } => {
                    collect_select(select, cte_names, tables)
                }
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
                columns,
                from,
                where_clause,
                group_by,
                ..
            } = select
            else {
                return;
            };
            let mut exprs = Vec::new();
            if let Some(from) = from {
                for source in std::iter::once(from.select.as_ref())
                    .chain(from.joins.iter().map(|join| join.table.as_ref()))
                {
                    collect_source(source, cte_names, tables);
                }
                for join in &from.joins {
                    if let Some(ast::JoinConstraint::On(on)) = &join.constraint {
                        exprs.push(on.as_ref());
                    }
                }
            }
            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    exprs.push(expr.as_ref());
                }
            }
            exprs.extend(where_clause.as_deref());
            if let Some(group_by) = group_by {
                exprs.extend(group_by.exprs.iter().map(|expr| expr.as_ref()));
                exprs.extend(group_by.having.as_deref());
            }
            // Subqueries in expressions read tables too: the planner may turn
            // `EXISTS (SELECT ... FROM t)` into a join with `t`.
            for expr in exprs {
                crate::translate::expr::walk_expr(expr, &mut |expr: &ast::Expr| {
                    match expr {
                        ast::Expr::Exists(select)
                        | ast::Expr::Subquery(select)
                        | ast::Expr::InSelect { rhs: select, .. } => {
                            collect_select(select, cte_names, tables);
                        }
                        _ => {}
                    }
                    Ok(crate::translate::expr::WalkControl::Continue)
                })
                .expect("collecting subquery tables cannot fail");
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
    use crate::incremental::yield_test_support::OneShotYieldInjector;
    use crate::mvcc::yield_hooks::YieldPointMarker;
    use crate::schema::{
        BTreeCharacteristics, BTreeTable, ColDef, ColDefFlags, Column as SchemaColumn, Schema, Type,
    };
    use crate::storage::btree::{BTreeWriteYieldPoint, BTREE_WRITE_YIELD_FAMILY};
    use crate::sync::Arc;
    use crate::{ConnectOpts, Database, DatabaseOpts, OpenFlags, SqliteDialect, StepResult};
    use turso_parser::ast;

    #[test]
    fn hidden_state_ownership_parses_aggregate_suffixes() {
        assert!(state_key_belongs_to_view("orders__n12__a3", "orders"));
        assert!(state_key_belongs_to_view("orders__n12__g", "orders"));
        assert!(state_key_belongs_to_view("orders__n12__r", "orders"));
        assert!(!state_key_belongs_to_view("orders__n12__a", "orders"));
        assert!(!state_key_belongs_to_view("orders__n12__g_extra", "orders"));
        assert!(!state_key_belongs_to_view("orders__n12__r3", "orders"));
        assert!(!state_key_belongs_to_view(
            "orders__n12__a3_extra",
            "orders"
        ));
        assert!(!state_key_belongs_to_view("orders__n12__n3__a4", "orders"));
        assert!(state_key_belongs_to_view(
            "orders__n12__n3__a4",
            "orders__n12"
        ));
    }

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
        assert!(matches!(result, Ok(false)));

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
        assert!(matches!(result, Ok(false)));
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
            Ok(false)
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
            Ok(false)
        ));
        assert!(matches!(
            states.insert(
                &orders,
                &views,
                10,
                vec![Value::from_i64(10), Value::from_i64(1), Value::from_i64(50)],
                TempStore::Memory,
            ),
            Ok(false)
        ));
        assert!(matches!(
            states.delete(
                &customers,
                &views,
                1,
                vec![Value::from_i64(1), Value::build_text("Ada")],
                TempStore::Memory,
            ),
            Ok(false)
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
            Ok(false)
        ));
        let mark = states.change_log_mark();

        let large = vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::Blob(vec![b'x'; CHANGE_LOG_SPILL_THRESHOLD]),
        ];
        let spill_started = states
            .insert(&orders, &views, 2, large, TempStore::Memory)
            .unwrap();
        assert!(spill_started, "large capture did not start a spill");
        loop {
            match states.finish_spill().unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
            }
        }

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

    /// An UPDATE records its old row as a delete and its new row as an
    /// insert. Both halves must be able to start their own spill: once the
    /// delete half's spill is fully collected via `finish_spill`, recording
    /// the insert half must find no spill in flight and be free to start a
    /// second one of its own.
    #[test]
    fn update_delete_and_insert_halves_each_start_their_own_spill() {
        let schema = create_test_schema();
        let table = schema.get_btree_table("customers").unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
        let states = TransactionChanges::with_io(io.clone());
        let views = vec!["v".to_string()];

        let old_large = vec![
            Value::from_i64(1),
            Value::Blob(vec![b'x'; CHANGE_LOG_SPILL_THRESHOLD]),
        ];
        let spill_started = states
            .delete(&table, &views, 1, old_large, TempStore::Memory)
            .unwrap();
        assert!(spill_started, "the delete half did not start a spill");
        loop {
            match states.finish_spill().unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
            }
        }

        let new_large = vec![
            Value::from_i64(1),
            Value::Blob(vec![b'y'; CHANGE_LOG_SPILL_THRESHOLD]),
        ];
        let spill_started = states
            .insert(&table, &views, 1, new_large, TempStore::Memory)
            .unwrap();
        assert!(spill_started, "the insert half did not start a spill");
        loop {
            match states.finish_spill().unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
            }
        }

        let changes = states.change_log.changes.lock();
        assert_eq!(
            changes.spilled.len(),
            2,
            "both halves must have spilled as their own segments"
        );
        assert_eq!(changes.spilled[0].rows, 1);
        assert_eq!(changes.spilled[1].rows, 1);
        assert!(
            changes.tail.is_empty(),
            "both rows were collected into spill segments"
        );
    }

    /// A spill that is still in flight when the owning statement fails must
    /// not become a recorded segment: `rewind_to_mark` drops it, leaving the
    /// triggering row in the tail (where the mark-based truncation removes
    /// it) and leaving no spill in flight for the next change to record.
    #[test]
    fn rewind_drops_a_spill_still_in_flight() {
        let schema = create_test_schema();
        let table = schema.get_btree_table("customers").unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
        let states = TransactionChanges::with_io(io.clone());
        let views = vec!["v".to_string()];

        let kept = vec![Value::from_i64(1), Value::build_text("kept")];
        assert!(matches!(
            states.insert(&table, &views, 1, kept, TempStore::Memory),
            Ok(false)
        ));
        let mark = states.change_log_mark();

        let large = vec![
            Value::from_i64(2),
            Value::Blob(vec![b'x'; CHANGE_LOG_SPILL_THRESHOLD]),
        ];
        let spill_started = states
            .insert(&table, &views, 2, large, TempStore::Memory)
            .unwrap();
        assert!(spill_started, "large capture did not start a spill");

        // The statement that captured row 2 fails before the spill is ever
        // collected.
        states.rewind_to_mark(mark).unwrap();

        let changes = states.change_log.changes.lock();
        assert!(
            changes.spilled.is_empty(),
            "an uncollected spill must not become a recorded segment"
        );
        assert_eq!(changes.tail.len(), 1, "only the pre-mark row remains");
        assert_eq!(changes.tail[0].rowid, 1);
        drop(changes);

        // No spill is in flight any more, so a later change may start its own.
        let spill_started = states
            .insert(
                &table,
                &views,
                3,
                vec![
                    Value::from_i64(3),
                    Value::Blob(vec![b'y'; CHANGE_LOG_SPILL_THRESHOLD]),
                ],
                TempStore::Memory,
            )
            .unwrap();
        assert!(spill_started);
    }

    #[test]
    fn aggregate_finalization_error_rolls_back_intermediate_state() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            "ivm-aggregate-finalization-rollback.db",
            OpenFlags::default(),
            DatabaseOpts::new().with_views(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE aggregate_base(x)").unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW aggregate_view AS SELECT SUM(x) AS total FROM aggregate_base",
        )
        .unwrap();
        conn.execute("INSERT INTO aggregate_base VALUES (5)")
            .unwrap();

        let error = conn
            .execute("INSERT INTO aggregate_base VALUES (9223372036854775807), (1)")
            .expect_err("the completed integer sum must overflow");
        assert!(error.to_string().contains("integer overflow"));

        let mut query = conn
            .prepare("SELECT (SELECT COUNT(*) FROM aggregate_base), total FROM aggregate_view")
            .unwrap();
        assert!(matches!(query.step().unwrap(), StepResult::Row));
        let row = query.row().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 1);
        assert_eq!(row.get::<i64>(1).unwrap(), 5);
        assert!(matches!(query.step().unwrap(), StepResult::Done));
        drop(query);

        conn.execute("INSERT INTO aggregate_base VALUES (2)")
            .unwrap();
        let mut query = conn.prepare("SELECT total FROM aggregate_view").unwrap();
        assert!(matches!(query.step().unwrap(), StepResult::Row));
        assert_eq!(query.row().unwrap().get::<i64>(0).unwrap(), 7);
    }

    #[test]
    fn maintenance_insert_resumes_after_overflow_cell_yield() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            "ivm-maintenance-overflow-yield.db",
            OpenFlags::default(),
            DatabaseOpts::new().with_views(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE overflow_base(id INTEGER PRIMARY KEY, payload BLOB)")
            .unwrap();
        for id in 1..260 {
            conn.execute(format!(
                "INSERT INTO overflow_base VALUES ({id}, zeroblob(3600))"
            ))
            .unwrap();
        }
        conn.execute(
            "CREATE MATERIALIZED VIEW overflow_view AS SELECT id, payload FROM overflow_base",
        )
        .unwrap();

        let view_root_page = {
            let view = conn
                .schema
                .read()
                .get_materialized_view("overflow_view")
                .expect("materialized view must be present in the schema");
            let root_page = view.lock().get_root_page();
            root_page
        };
        let injector = OneShotYieldInjector::new(
            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
            BTREE_WRITE_YIELD_FAMILY ^ view_root_page as u64,
        );
        conn.set_yield_injector(Some(injector.clone()));

        let mut stmt = conn
            .prepare("INSERT INTO overflow_base VALUES (926, zeroblob(7200))")
            .unwrap();
        assert!(matches!(stmt.step().unwrap(), StepResult::Yield));
        assert!(injector.fired(), "targeted view B-tree yield did not fire");
        assert!(matches!(stmt.step().unwrap(), StepResult::Done));
        drop(stmt);
        conn.set_yield_injector(None);

        let mut query = conn
            .prepare("SELECT count(*), max(length(payload)) FROM overflow_view WHERE id = 926")
            .unwrap();
        assert!(matches!(query.step().unwrap(), StepResult::Row));
        let row = query.row().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 1);
        assert_eq!(row.get::<i64>(1).unwrap(), 7200);
        drop(query);

        let mut integrity = conn.prepare("PRAGMA integrity_check").unwrap();
        assert!(matches!(integrity.step().unwrap(), StepResult::Row));
        assert_eq!(integrity.row().unwrap().get::<String>(0).unwrap(), "ok");
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
                    flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::NotNull,
                    ..Default::default()
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
                    flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::NotNull,
                    ..Default::default()
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
                    flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::NotNull,
                    ..Default::default()
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
