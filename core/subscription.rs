//! Process-local invalidations for committed writes to the main database.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Poll, Waker};

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

    fn merge(&mut self, other: Self) {
        let Self::Tables(tables) = other else {
            *self = Self::Refresh;
            return;
        };
        for (name, keys) in tables {
            match keys {
                ChangedKeys::All => self.record(&name, None),
                ChangedKeys::Keys(keys) => {
                    for key in keys {
                        self.record(&name, Some(key));
                    }
                }
            }
        }
    }

    fn table_for_filter(&self, filter: &ChangeFilter) -> Option<(&String, &ChangedKeys)> {
        let Self::Tables(tables) = self else {
            return None;
        };
        let (ChangeFilter::Table { table } | ChangeFilter::Row { table, .. }) = filter else {
            return None;
        };
        let entry = tables
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(table))?;
        if let (ChangeFilter::Row { primary_key, .. }, ChangedKeys::Keys(keys)) = (filter, entry.1)
        {
            if !keys.contains(primary_key) {
                return None;
            }
        }
        Some(entry)
    }
}

// The writer shares one immutable batch across all matching inboxes. The receiver
// copies only its requested keys, after releasing the inbox lock.
enum Delivery {
    Refresh,
    Batch(Arc<ChangeBatch>),
}

impl Delivery {
    fn into_batch(self, filter: &ChangeFilter) -> ChangeBatch {
        let Self::Batch(batch) = self else {
            return ChangeBatch::Refresh;
        };
        if matches!(filter, ChangeFilter::All) {
            return Arc::unwrap_or_clone(batch);
        }
        let (name, keys) = batch
            .table_for_filter(filter)
            .expect("queued matching batch");
        let keys = match (filter, keys) {
            (ChangeFilter::Row { primary_key, .. }, ChangedKeys::Keys(_)) => {
                ChangedKeys::Keys(BTreeSet::from([primary_key.clone()]))
            }
            _ => keys.clone(),
        };
        ChangeBatch::Tables(BTreeMap::from([(name.clone(), keys)]))
    }
}

#[derive(Default)]
struct Inbox {
    batches: VecDeque<Delivery>,
    waker: Option<Waker>,
    closed: bool,
}

struct Listener {
    id: u64,
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
        let delivery = inbox.batches.pop_front();
        drop(inbox);
        delivery.map(|d| d.into_batch(&self.listener.filter))
    }

    /// Await a committed invalidation without blocking the executor thread.
    /// Cancelling a pending receive leaves the next invalidation in the inbox.
    pub async fn recv_async(&mut self) -> Option<ChangeBatch> {
        let waiter = AsyncWaiter(&self.listener);
        let delivery = std::future::poll_fn(|cx| {
            let mut inbox = waiter.0.inbox.lock();
            if let Some(delivery) = inbox.batches.pop_front() {
                Poll::Ready(Some(delivery))
            } else if inbox.closed {
                Poll::Ready(None)
            } else {
                if !inbox
                    .waker
                    .as_ref()
                    .is_some_and(|w| w.will_wake(cx.waker()))
                {
                    inbox.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        })
        .await;
        delivery.map(|d| d.into_batch(&self.listener.filter))
    }

    /// Take a pending invalidation without blocking.
    pub fn try_recv(&self) -> Option<ChangeBatch> {
        let delivery = self.listener.inbox.lock().batches.pop_front();
        delivery.map(|d| d.into_batch(&self.listener.filter))
    }
}

struct AsyncWaiter<'a>(&'a Listener);

impl Drop for AsyncWaiter<'_> {
    fn drop(&mut self) {
        let waker = self.0.inbox.lock().waker.take();
        drop(waker);
    }
}

impl Drop for ChangeSubscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut state = registry.state.lock();
            let name = self.listener.filter.bucket();
            let bucket = state.buckets.get_mut(&name).expect("registered listener");
            bucket.retain(|listener| listener.id != self.listener.id);
            if bucket.is_empty() {
                state.buckets.remove(&name);
            }
            registry
                .active
                .store(!state.buckets.is_empty(), Ordering::Release);
        }
    }
}

#[derive(Default)]
pub(crate) struct ChangeRegistry {
    active: AtomicBool,
    state: Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
    generation: u64,
    // None holds the database-wide filters.
    buckets: BTreeMap<Option<String>, Vec<Arc<Listener>>>,
}

impl ChangeFilter {
    fn bucket(&self) -> Option<String> {
        match self {
            Self::All => None,
            Self::Table { table } | Self::Row { table, .. } => Some(table.to_ascii_lowercase()),
        }
    }
}

impl Database {
    /// Subscribe before reading an initial snapshot. A commit racing the read
    /// may cause an extra refresh. A new subscriber can receive Refresh for
    /// writes that a transaction made before it registered. The feed covers SQL and
    /// incremental blob writes to ordinary B-tree tables in this instance's main
    /// database. A locally imported WAL commit frame causes a full refresh.
    /// Connections sharing this Database see each other's commits. Other processes
    /// are outside the feed. Notifications are transient and carry no replay cursor.
    /// Use the schema-qualified table name for stored namespaces (e.g. `app.t`).
    /// Quote components containing a dot or a double quote as SQL identifiers.
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
        let mut state = self.subscriptions.state.lock();
        state.generation = state
            .generation
            .checked_add(1)
            .expect("subscription ID exhausted");
        let listener = Arc::new(Listener {
            id: state.generation,
            filter,
            inbox: Mutex::default(),
            ready: Condvar::new(),
        });
        state
            .buckets
            .entry(listener.filter.bucket())
            .or_default()
            .push(listener.clone());
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

    pub(crate) fn route(&self, table: impl FnOnce() -> String) -> Capture {
        if !self.active() {
            return Capture::Unwatched(0);
        }
        let name = table();
        let state = self.state.lock();
        if state.buckets.contains_key(&None)
            || state.buckets.contains_key(&Some(name.to_ascii_lowercase()))
        {
            Capture::Keys {
                name,
                through: state.generation,
            }
        } else {
            Capture::Unwatched(state.generation)
        }
    }

    pub(crate) fn refresh(&self) {
        self.publish(0, None);
    }

    fn publish(&self, through: u64, batch: Option<&Arc<ChangeBatch>>) {
        if !self.active() {
            return;
        }
        let state = self.state.lock();
        let mut wakers = Vec::new();
        if state.generation != through
            || matches!(batch.map(Arc::as_ref), Some(ChangeBatch::Refresh))
        {
            for listener in state.buckets.values().flatten() {
                if listener.id > through {
                    listener.enqueue(Delivery::Refresh, &mut wakers);
                } else if let Some(batch) = batch {
                    listener.deliver(batch, &mut wakers);
                }
            }
        } else if let Some(batch) = batch {
            let ChangeBatch::Tables(tables) = batch.as_ref() else {
                unreachable!()
            };
            for listener in state.buckets.get(&None).into_iter().flatten() {
                listener.deliver(batch, &mut wakers);
            }
            for name in tables.keys() {
                for listener in state
                    .buckets
                    .get(&Some(name.to_ascii_lowercase()))
                    .into_iter()
                    .flatten()
                {
                    listener.deliver(batch, &mut wakers);
                }
            }
        }
        drop(state);
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Listener {
    fn deliver(&self, batch: &Arc<ChangeBatch>, wakers: &mut Vec<Waker>) {
        if matches!(batch.as_ref(), ChangeBatch::Refresh) {
            self.enqueue(Delivery::Refresh, wakers);
        } else if matches!(self.filter, ChangeFilter::All)
            || batch.table_for_filter(&self.filter).is_some()
        {
            self.enqueue(Delivery::Batch(batch.clone()), wakers);
        }
    }

    fn enqueue(&self, delivery: Delivery, wakers: &mut Vec<Waker>) {
        let mut inbox = self.inbox.lock();
        if inbox.batches.len() == MAX_BATCHES {
            inbox.batches.clear();
            inbox.batches.push_back(Delivery::Refresh);
        } else {
            inbox.batches.push_back(delivery);
        }
        wakers.extend(inbox.waker.take());
        self.ready.notify_one();
    }
}

impl Drop for ChangeRegistry {
    fn drop(&mut self) {
        for listener in self.state.get_mut().buckets.values().flatten() {
            let mut inbox = listener.inbox.lock();
            inbox.closed = true;
            let waker = inbox.waker.take();
            drop(inbox);
            listener.ready.notify_all();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl crate::Connection {
    pub(crate) fn subscription_route(&self, table: &BTreeTable) -> Capture {
        if table.root_page == 1 || crate::schema::is_system_table(&table.name) {
            return Capture::Ignore;
        }
        self.db.subscriptions.route(|| {
            let schema = self.schema.read();
            let name = |s: &str| {
                if s.contains(['.', '"']) {
                    crate::util::quote_identifier(s)
                } else {
                    s.to_owned()
                }
            };
            for (id, namespace) in schema.namespaces() {
                if schema
                    .get_btree_table(id, &table.name)
                    .is_some_and(|t| t.root_page == table.root_page)
                {
                    return format!("{}.{}", name(namespace), name(&table.name));
                }
            }
            name(&table.name)
        })
    }
}

/// Each write cursor resolves its filter once per execution. A listener added
/// later must reread its filter when this transaction commits.
#[derive(Debug, Default)]
pub(crate) enum Capture {
    #[default]
    Ignore,
    Unwatched(u64),
    Keys {
        name: String,
        through: u64,
    },
}

impl Capture {
    pub(crate) fn wants_keys(&self) -> bool {
        matches!(self, Self::Keys { .. })
    }

    pub(crate) fn record(&mut self, conn: &crate::Connection, key: Option<Vec<Value>>) {
        match self {
            Self::Ignore => {}
            Self::Unwatched(through) => {
                conn.pending_changes.record(conn, *through, None, None);
                // The first successful write covers all later writes on this cursor.
                *self = Self::Ignore;
            }
            Self::Keys { name, through } => {
                conn.pending_changes.record(conn, *through, Some(name), key)
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ChangeSnapshot {
    // MVCC sequences may commit an inner transaction on the same connection.
    owner: Option<u64>,
    // Listeners registered after this ID may have missed a write.
    through: Option<u64>,
    batch: Option<Arc<ChangeBatch>>,
}

impl ChangeSnapshot {
    fn record_generation(&mut self, through: u64) {
        self.through = Some(self.through.map_or(through, |old| old.min(through)));
    }

    fn merge(&mut self, other: Self) {
        let Some(through) = other.through else { return };
        if self.through.is_none() {
            *self = other;
            return;
        }
        debug_assert_eq!(self.owner, other.owner);
        self.record_generation(through);
        if let Some(delta) = other.batch {
            if let Some(batch) = &mut self.batch {
                Arc::make_mut(batch).merge(Arc::unwrap_or_clone(delta));
            } else {
                self.batch = Some(delta);
            }
        }
    }

    fn publish(&self, registry: &ChangeRegistry) {
        if let Some(through) = self.through {
            registry.publish(through, self.batch.as_ref());
        }
    }
}

#[derive(Debug)]
pub(crate) struct CommitNotification {
    registry: Weak<ChangeRegistry>,
    changes: ChangeSnapshot,
}

impl CommitNotification {
    pub(crate) fn publish(self) {
        if let Some(registry) = self.registry.upgrade() {
            self.changes.publish(&registry);
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct PendingChanges(Mutex<PendingState>);

#[derive(Default, Debug)]
struct PendingState {
    transaction: ChangeSnapshot,
    // Present only while a statement may roll back independently.
    statement: Option<ChangeSnapshot>,
}

impl PendingChanges {
    pub(crate) fn begin_statement(&self) {
        let mut pending = self.0.lock();
        debug_assert!(pending.statement.is_none());
        pending.statement = Some(ChangeSnapshot::default());
    }

    pub(crate) fn end_statement(&self, rolled_back: bool) {
        let mut pending = self.0.lock();
        if let Some(delta) = pending.statement.take() {
            if !rolled_back {
                pending.transaction.merge(delta);
            }
        }
    }

    pub(crate) fn snapshot(&self) -> ChangeSnapshot {
        self.0.lock().transaction.clone()
    }

    pub(crate) fn restore(&self, snapshot: ChangeSnapshot) {
        self.0.lock().transaction = snapshot;
    }

    fn record(
        &self,
        conn: &crate::Connection,
        through: u64,
        table: Option<&str>,
        key: Option<Vec<Value>>,
    ) {
        let mut pending = self.0.lock();
        let PendingState {
            transaction,
            statement,
        } = &mut *pending;
        let changes = statement.as_mut().unwrap_or(transaction);
        if changes.through.is_none() {
            changes.owner = conn.get_mv_tx_id();
        }
        changes.record_generation(through);
        if let Some(name) = table {
            Arc::make_mut(changes.batch.get_or_insert_with(Arc::default)).record(name, key);
        }
    }

    pub(crate) fn record_refresh(&self, conn: &crate::Connection) {
        self.record(conn, 0, None, None);
    }

    pub(crate) fn for_mvcc_commit(
        &self,
        tx_id: u64,
        registry: &Arc<ChangeRegistry>,
    ) -> Option<CommitNotification> {
        let state = self.0.lock();
        let pending = &state.transaction;
        if pending.owner != Some(tx_id) || pending.through.is_none() {
            return None;
        }
        Some(CommitNotification {
            registry: Arc::downgrade(registry),
            changes: pending.clone(),
        })
    }

    pub(crate) fn finish(&self, owner: Option<u64>, registry: Option<&ChangeRegistry>) {
        let mut state = self.0.lock();
        if state.statement.as_ref().is_some_and(|s| s.owner == owner) {
            if let Some(delta) = state.statement.take() {
                state.transaction.merge(delta);
            }
        }
        if state.transaction.owner == owner {
            let batch = std::mem::take(&mut state.transaction);
            drop(state);
            if let Some(registry) = registry {
                batch.publish(registry);
            }
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
