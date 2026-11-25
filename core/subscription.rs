//! Process-local invalidations for committed writes to the main database.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Condvar, Mutex};

use crate::schema::BTreeTable;
use crate::translate::collate::CollationSeq;
use crate::types::ImmutableRecord;
use crate::vdbe::affinity::Affinity;
use crate::{Database, Value, ValueRef};

/// A row key is a tuple of stored SQL values, in PRIMARY KEY declaration order.
/// For an ordinary table with no declared key, the tuple contains the rowid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeFilter {
    All,
    Table {
        table: String,
    },
    Row {
        table: String,
        primary_key: Vec<Value>,
    },
}

/// Rows to reread from one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangedKeys {
    Keys(BTreeSet<Vec<Value>>),
    All,
}

/// One committed transaction. Refresh asks the consumer to reread its entire
/// filter, e.g. after a schema change or when its bounded queue overflows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeBatch {
    Tables(BTreeMap<String, ChangedKeys>),
    Refresh,
}

impl Default for ChangeBatch {
    fn default() -> Self {
        Self::Tables(BTreeMap::new())
    }
}

const MAX_TABLES: usize = 32;
const MAX_KEYS_PER_TABLE: usize = 128;
const MAX_KEY_BYTES: usize = 1024;
const MAX_BATCHES: usize = 8;

impl ChangeBatch {
    fn record(&mut self, name: &str, key: Option<Vec<Value>>) {
        let Self::Tables(tables) = self else { return };
        if tables.len() >= MAX_TABLES && !tables.contains_key(name) {
            *self = Self::Refresh;
            return;
        }
        let entry = if let Some(entry) = tables.get_mut(name) {
            entry
        } else {
            tables
                .entry(name.to_owned())
                .or_insert_with(|| ChangedKeys::Keys(BTreeSet::new()))
        };
        if let ChangedKeys::Keys(keys) = entry {
            if let Some(key) = key {
                keys.insert(key);
                if keys.len() <= MAX_KEYS_PER_TABLE {
                    return;
                }
            }
            *entry = ChangedKeys::All;
        }
    }

    fn for_filter(&self, filter: &ChangeFilter) -> Option<Self> {
        let Self::Tables(tables) = self else {
            return Some(Self::Refresh);
        };
        let filter_name = match filter {
            ChangeFilter::All => return (!tables.is_empty()).then(|| self.clone()),
            ChangeFilter::Table { table } | ChangeFilter::Row { table, .. } => table,
        };
        let (name, keys) = tables
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(filter_name))?;
        let keys = match (filter, keys) {
            (ChangeFilter::Row { primary_key, .. }, ChangedKeys::Keys(keys)) => {
                if !keys.contains(primary_key) {
                    return None;
                }
                ChangedKeys::Keys(BTreeSet::from([primary_key.clone()]))
            }
            (_, keys) => keys.clone(),
        };
        Some(Self::Tables(BTreeMap::from([(name.clone(), keys)])))
    }
}

#[derive(Default)]
struct Inbox {
    batches: VecDeque<ChangeBatch>,
    closed: bool,
}

struct Listener {
    filter: ChangeFilter,
    inbox: Mutex<Inbox>,
    ready: Condvar,
}

/// A bounded receiver. Dropping it unregisters the filter.
pub struct ChangeSubscription {
    listener: Arc<Listener>,
    registry: Weak<ChangeRegistry>,
}

impl ChangeSubscription {
    /// Wait for a committed invalidation. Returns None once the database is
    /// dropped and its pending notifications have been drained.
    pub fn recv(&self) -> Option<ChangeBatch> {
        let mut inbox = self.listener.inbox.lock();
        while inbox.batches.is_empty() && !inbox.closed {
            self.listener.ready.wait(&mut inbox);
        }
        inbox.batches.pop_front()
    }

    /// Take a pending invalidation without blocking.
    pub fn try_recv(&self) -> Option<ChangeBatch> {
        self.listener.inbox.lock().batches.pop_front()
    }
}

impl Drop for ChangeSubscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut listeners = registry.listeners.lock();
            listeners.retain(|listener| !Arc::ptr_eq(listener, &self.listener));
            registry
                .active
                .store(!listeners.is_empty(), Ordering::Release);
        }
    }
}

#[derive(Default)]
pub(crate) struct ChangeRegistry {
    active: AtomicBool,
    listeners: Mutex<Vec<Arc<Listener>>>,
}

impl Database {
    /// Subscribe before reading an initial snapshot. A commit racing the read
    /// may cause an extra refresh. The first subscriber can receive Refresh
    /// for a transaction already in flight. The feed covers SQL and incremental
    /// blob writes to ordinary B-tree tables in this instance's main database.
    /// Notifications are transient and carry no replay cursor.
    ///
    /// ```
    /// use turso_core::{ChangeFilter, ChangeSubscription, Database, Value};
    ///
    /// // For a table declared with PRIMARY KEY (org_id, user_id).
    /// fn watch_member(db: &Database) -> ChangeSubscription {
    ///     db.subscribe_changes(ChangeFilter::Row {
    ///         table: "membership".into(),
    ///         primary_key: vec![Value::from_i64(7), Value::build_text("sam")],
    ///     })
    /// }
    /// ```
    pub fn subscribe_changes(&self, filter: ChangeFilter) -> ChangeSubscription {
        let listener = Arc::new(Listener {
            filter,
            inbox: Mutex::default(),
            ready: Condvar::new(),
        });
        let mut listeners = self.subscriptions.listeners.lock();
        listeners.push(listener.clone());
        self.subscriptions.active.store(true, Ordering::Release);
        ChangeSubscription {
            listener,
            registry: Arc::downgrade(&self.subscriptions),
        }
    }
}

impl ChangeRegistry {
    pub(crate) fn active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn publish(&self, batch: &ChangeBatch) {
        if !self.active() || matches!(batch, ChangeBatch::Tables(tables) if tables.is_empty()) {
            return;
        }
        for listener in self.listeners.lock().iter() {
            if let Some(batch) = batch.for_filter(&listener.filter) {
                let mut inbox = listener.inbox.lock();
                if inbox.batches.len() == MAX_BATCHES {
                    inbox.batches.clear();
                    inbox.batches.push_back(ChangeBatch::Refresh);
                } else {
                    inbox.batches.push_back(batch);
                }
                listener.ready.notify_one();
            }
        }
    }
}

impl Drop for ChangeRegistry {
    fn drop(&mut self) {
        for listener in self.listeners.get_mut() {
            listener.inbox.lock().closed = true;
            listener.ready.notify_all();
        }
    }
}

/// A copy-on-write snapshot lives on each active statement/named savepoint.
/// A cold writer retains a coarse invalidation for listeners that register
/// between the mutation and commit.
#[derive(Clone, Debug, Default)]
pub(crate) struct ChangeSnapshot {
    // MVCC sequence operations can commit an inner transaction on the same
    // connection. Only the owning user transaction can publish these changes.
    owner: Option<u64>,
    batch: PendingBatch,
}

#[derive(Clone, Debug, Default)]
enum PendingBatch {
    #[default]
    Empty,
    Refresh,
    Detailed(Arc<ChangeBatch>),
}

impl PendingBatch {
    fn publish(self, registry: &ChangeRegistry) {
        match self {
            Self::Empty => {}
            Self::Refresh => registry.publish(&ChangeBatch::Refresh),
            Self::Detailed(batch) => registry.publish(&batch),
        }
    }
}

/// The transaction owns delivery once MVCC starts committing. A group leader
/// may finish it after its original connection has moved on.
#[derive(Debug)]
pub(crate) struct CommitNotification {
    registry: Weak<ChangeRegistry>,
    batch: PendingBatch,
}

impl CommitNotification {
    pub(crate) fn publish(self) {
        if let Some(registry) = self.registry.upgrade() {
            self.batch.publish(&registry);
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct PendingChanges(Mutex<ChangeSnapshot>);

impl PendingChanges {
    pub(crate) fn snapshot(&self) -> ChangeSnapshot {
        self.0.lock().clone()
    }

    pub(crate) fn restore(&self, snapshot: ChangeSnapshot) {
        *self.0.lock() = snapshot;
    }

    pub(crate) fn for_mvcc_commit(
        &self,
        tx_id: u64,
        registry: &Arc<ChangeRegistry>,
    ) -> Option<CommitNotification> {
        let pending = self.0.lock();
        if pending.owner != Some(tx_id) || matches!(pending.batch, PendingBatch::Empty) {
            return None;
        }
        Some(CommitNotification {
            registry: Arc::downgrade(registry),
            batch: pending.batch.clone(),
        })
    }

    pub(crate) fn finish(&self, owner: Option<u64>, registry: Option<&ChangeRegistry>) {
        let mut pending = self.0.lock();
        if pending.owner != owner {
            return;
        }
        let batch = std::mem::take(&mut pending.batch);
        drop(pending);
        if let Some(registry) = registry {
            batch.publish(registry);
        }
    }
}

impl crate::Connection {
    pub(crate) fn record_subscription_change(&self, table: Option<&str>, key: Option<Vec<Value>>) {
        let mut pending = self.pending_changes.0.lock();
        if matches!(pending.batch, PendingBatch::Refresh) {
            return;
        }
        if matches!(pending.batch, PendingBatch::Empty) {
            pending.owner = self.get_mv_tx_id();
        }
        let Some(name) = table.filter(|_| self.db.subscriptions.active()) else {
            // No key copies or heap allocations on a database without listeners.
            // A subscriber can still arrive before this transaction commits.
            pending.batch = PendingBatch::Refresh;
            return;
        };
        if matches!(pending.batch, PendingBatch::Empty) {
            pending.batch = PendingBatch::Detailed(Arc::default());
        }
        if let PendingBatch::Detailed(batch) = &mut pending.batch {
            Arc::make_mut(batch).record(name, key);
        }
    }
}

/// Unknown or ambiguous keys invalidate the table. This includes collations
/// whose SQL equality differs from Rust's ordered Value representation.
pub(crate) fn primary_key(
    table: &BTreeTable,
    rowid: Option<i64>,
    record: Option<&ImmutableRecord>,
) -> Option<Vec<Value>> {
    if table.primary_key_columns.is_empty() || table.get_rowid_alias_column().is_some() {
        return Some(vec![Value::from_i64(rowid?)]);
    }
    let record = record?;
    let mut remaining = MAX_KEY_BYTES;
    let mut key = Vec::new();
    for (name, _) in &table.primary_key_columns {
        let (logical, column) = table.get_column(name)?;
        let collation = table
            .primary_key_constraint_collation(name)
            .map_or_else(|| column.collation(), CollationSeq::Text);
        if collation != CollationSeq::BINARY {
            return None;
        }
        let value = record.get_value_opt(table.logical_to_physical_column(logical))?;
        let bytes = match value {
            ValueRef::Null => return None,
            ValueRef::Blob(b) => b.len(),
            ValueRef::Text(t) => t.as_str().len(),
            ValueRef::Numeric(_) => 8,
        };
        remaining = remaining.checked_sub(bytes + std::mem::size_of::<Value>())?;
        // A stored value and the corresponding SQL lookup may have different
        // storage classes, e.g. a numeric string in an integer key column.
        if matches!(
            (column.affinity(), value),
            (
                Affinity::Integer | Affinity::Numeric | Affinity::Real,
                ValueRef::Text(_)
            ) | (Affinity::Text, ValueRef::Numeric(_))
        ) {
            return None;
        }
        key.push(value.to_owned().ok()?);
    }
    Some(key)
}
