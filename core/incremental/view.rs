use crate::alloc::{TursoIteratorExt, TursoTryWithCapacityExt, TursoVecExt};
use crate::io::{Buffer, Completion, File, TempFile, IO};
use crate::namespace::NamespaceId;
use crate::schema::{BTreeTable, Schema};
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult, ImmutableRecord, Value};
use crate::util::{extract_view_columns, ViewColumnSchema};
use crate::{turso_assert, LimboError, Result, TempStore};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast;
use turso_parser::ast::{Cmd, Stmt};

/// Version of the materialized-view storage format embedded in every hidden
/// table name. Version 1 is the interpreter layout released on `main`;
/// version 2 is the VDBE-maintained operator-DAG layout developed on this
/// branch. Bump this only after a layout has shipped or been shared.
pub const DBSP_CIRCUIT_VERSION: u32 = 2;

pub fn dbsp_version_marker_table_name(view_name: &str) -> String {
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
pub fn state_key_belongs_to_view(key: &str, view_name: &str) -> bool {
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
            collation: state_table.columns()[pos_in_table].collation(),
            pos_in_table,
            explicit_collation: None,
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
        constraint_name: None,
    })
}

/// Stable identity of a changed table within a schema epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableChangeId {
    RootPage(i64),
    #[cfg(any(test, feature = "test_helper"))]
    Name(String),
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub table_id: TableChangeId,
    pub rowid: i64,
    pub values: crate::alloc::Vec<Value>,
    pub weight: isize,
}

#[derive(Debug)]
pub struct ChangeBuffer {
    pub tail: Arc<Vec<ChangeEvent>>,
    in_memory_bytes: usize,
    pub spilled: Vec<SpillSegment>,
}

#[derive(Debug, Clone)]
pub struct SpillSegment {
    pub(crate) offset: u64,
    pub(crate) len: usize,
    /// Stable table identities present in this segment. Individual event
    /// identities spill with their row images; only this segment-level routing
    /// index stays resident.
    pub(crate) table_ids: Arc<HashSet<TableChangeId>>,
    /// Number of events in this segment.
    pub rows: usize,
}

/// A snapshot consumed by a maintenance cursor. Spilled segments precede the
/// in-memory tail and therefore preserve transaction capture order.
#[derive(Clone)]
pub struct CapturedDelta {
    pub(crate) file: Option<Arc<dyn File>>,
    pub(crate) spilled: Vec<SpillSegment>,
    pub tail: Arc<Vec<ChangeEvent>>,
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

    #[cfg(any(test, feature = "test_helper"))]
    fn contains_table(&self, table_id: &TableChangeId) -> bool {
        self.spilled
            .iter()
            .any(|segment| segment.rows > 0 && segment.table_ids.contains(table_id))
            || self.tail.iter().any(|event| &event.table_id == table_id)
    }
}

pub const CHANGE_LOG_SPILL_THRESHOLD: usize = 256 * 1024;

#[derive(Debug)]
struct PendingSpill {
    completion: Completion,
    segment: SpillSegment,
}

/// One globally ordered transaction change log. Row images are owned by this
/// stream exactly once; consumers retain only stable table-identity indexes.
pub struct SharedChangeLog {
    pub changes: Mutex<ChangeBuffer>,
    subscriptions: Mutex<HashMap<String, HashSet<TableChangeId>>>,
    /// Monotonic content revision for read-your-own-writes cursors.
    /// A row count is insufficient: maintenance empties the log, and later
    /// different changes can produce the same length with different contents.
    revision: Mutex<u64>,
    /// Number of events in the log (tail plus spilled segments). Every
    /// write statement asks whether the log is empty when it ends, and
    /// almost always it is, so this answers without taking `changes`. It
    /// changes only together with `changes`, under that lock.
    event_count: AtomicUsize,
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
            event_count: AtomicUsize::new(0),
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

    /// Account for one event just pushed to the tail. The caller still
    /// holds the `changes` lock.
    fn event_appended(&self) {
        self.event_count.fetch_add(1, Ordering::Release);
        let mut revision = self.revision.lock();
        *revision = revision.wrapping_add(1);
    }

    fn is_empty(&self) -> bool {
        self.event_count.load(Ordering::Acquire) == 0
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
            self.event_appended();
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
        #[cfg(any(test, feature = "test_helper"))]
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

    #[cfg(any(test, feature = "test_helper"))]
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
        self.change_log.event_appended();
    }

    #[cfg(any(test, feature = "test_helper"))]
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
        self.change_log.event_appended();
    }

    /// Clone only the requested table's stream, rather than every delta
    /// captured for the view.
    pub fn table_delta(&self, table: &BTreeTable) -> CapturedDelta {
        let root_id = TableChangeId::RootPage(table.root_page);
        let captured = self.change_log.snapshot(&root_id);
        #[cfg(any(test, feature = "test_helper"))]
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

/// Connection-scoped transaction change stream and its consumer subscriptions.
#[derive(Debug, Clone)]
pub struct TransactionChanges {
    pub change_log: Arc<SharedChangeLog>,
}
crate::assert::assert_send_sync!(TransactionChanges);

impl TransactionChanges {
    /// Create a transaction-wide change stream.
    pub fn new() -> Self {
        Self::with_io(Arc::new(crate::MemoryIO::new()))
    }

    pub fn with_io(io: Arc<dyn IO>) -> Self {
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
        self.change_log.event_count.store(0, Ordering::Release);
        drop(changes);
        self.change_log.subscriptions.lock().clear();
        self.change_log.pending_spill.lock().take();
        self.change_log.temp_file.lock().take();
        *self.change_log.next_spill_offset.lock() = 0;
        *self.change_log.revision.lock() = 0;
    }

    /// Check if the shared transaction log has no captured changes.
    pub fn is_empty(&self) -> bool {
        self.change_log.is_empty()
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
    /// Builds the view from its `CREATE MATERIALIZED VIEW` text. `namespace`
    /// is the namespace the view is in, where its unqualified names resolve.
    pub fn from_sql(
        sql: &str,
        schema: &Schema,
        main_data_root: i64,
        namespace: NamespaceId,
    ) -> Result<Self> {
        let cmd = crate::dialect::engine::parse_statement(sql)?;
        let cmd = cmd.expect("View is an empty statement");
        match cmd {
            Cmd::Stmt(Stmt::CreateMaterializedView {
                if_not_exists: _,
                view_name,
                columns,
                select,
            }) => {
                let mut column_schema = extract_view_columns(namespace, &select, schema)?;
                column_schema.rename_columns(&columns);
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
    /// The tables `select` reads, each with the namespace it is written
    /// with, if any. A name that is a CTE of the select is not a table.
    pub(crate) fn referenced_tables(
        select: &ast::Select,
    ) -> std::vec::Vec<(Option<String>, String)> {
        fn collect_source(
            source: &ast::SelectTable,
            cte_names: &HashSet<String>,
            tables: &mut HashSet<(Option<String>, String)>,
        ) {
            match source {
                ast::SelectTable::Table(name, _, _) => {
                    if cte_names
                        .iter()
                        .any(|cte| cte.eq_ignore_ascii_case(name.name.as_str()))
                    {
                        return;
                    }
                    tables.insert((
                        name.namespace
                            .as_ref()
                            .map(|namespace| crate::util::normalize_ident(namespace.as_str())),
                        crate::util::normalize_ident(name.name.as_str()),
                    ));
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
            tables: &mut HashSet<(Option<String>, String)>,
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
            tables: &mut HashSet<(Option<String>, String)>,
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
