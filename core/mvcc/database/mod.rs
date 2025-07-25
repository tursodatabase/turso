use crate::mvcc::clock::LogicalClock;
use crate::mvcc::errors::DatabaseError;
use crate::storage::btree::BTreeKey;
use crate::storage::pager::Pager;
use crate::types::ImmutableRecord;
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowID {
    pub table_id: u64,
    pub row_id: i64,
}

impl RowID {
    pub fn new(table_id: u64, row_id: i64) -> Self {
        Self { table_id, row_id }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]

pub struct Row {
    pub id: RowID,
    pub data: Vec<u8>,
    pub column_count: usize,
}

impl Row {
    pub fn new(id: RowID, data: Vec<u8>, column_count: usize) -> Self {
        Self {
            id,
            data,
            column_count,
        }
    }
}

/// A row version.
#[derive(Clone, Debug, PartialEq)]
pub struct RowVersion {
    begin: TxTimestampOrID,
    end: Option<TxTimestampOrID>,
    row: Row,
}

pub type TxID = u64;

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum TxTimestampOrID {
    /// A committed transaction's timestamp.
    Timestamp(u64),
    /// The ID of a non-committed transaction.
    TxID(TxID),
}

/// Transaction
#[derive(Debug)]
pub struct Transaction {
    /// The state of the transaction.
    state: AtomicTransactionState,
    /// The transaction ID.
    tx_id: u64,
    /// The transaction begin timestamp.
    begin_ts: u64,
    /// The transaction write set.
    write_set: SkipSet<RowID>,
    /// The transaction read set.
    read_set: SkipSet<RowID>,
}

impl Transaction {
    fn new(tx_id: u64, begin_ts: u64) -> Transaction {
        Transaction {
            state: TransactionState::Active.into(),
            tx_id,
            begin_ts,
            write_set: SkipSet::new(),
            read_set: SkipSet::new(),
        }
    }

    fn insert_to_read_set(&self, id: RowID) {
        self.read_set.insert(id);
    }

    fn insert_to_write_set(&mut self, id: RowID) {
        self.write_set.insert(id);
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{{ state: {}, id: {}, begin_ts: {}, write_set: {:?}, read_set: {:?}",
            self.state.load(),
            self.tx_id,
            self.begin_ts,
            // FIXME: I'm sorry, we obviously shouldn't be cloning here.
            self.write_set
                .iter()
                .map(|v| *v.value())
                .collect::<Vec<RowID>>(),
            self.read_set
                .iter()
                .map(|v| *v.value())
                .collect::<Vec<RowID>>()
        )
    }
}

/// Transaction state.
#[derive(Debug, Clone, PartialEq)]
enum TransactionState {
    Active,
    Preparing,
    Aborted,
    Terminated,
    Committed(u64),
}

impl TransactionState {
    pub fn encode(&self) -> u64 {
        match self {
            TransactionState::Active => 0,
            TransactionState::Preparing => 1,
            TransactionState::Aborted => 2,
            TransactionState::Terminated => 3,
            TransactionState::Committed(ts) => {
                // We only support 2*62 - 1 timestamps, because the extra bit
                // is used to encode the type.
                assert!(ts & 0x8000_0000_0000_0000 == 0);
                0x8000_0000_0000_0000 | ts
            }
        }
    }

    pub fn decode(v: u64) -> Self {
        match v {
            0 => TransactionState::Active,
            1 => TransactionState::Preparing,
            2 => TransactionState::Aborted,
            3 => TransactionState::Terminated,
            v if v & 0x8000_0000_0000_0000 != 0 => {
                TransactionState::Committed(v & 0x7fff_ffff_ffff_ffff)
            }
            _ => panic!("Invalid transaction state"),
        }
    }
}

// Transaction state encoded into a single 64-bit atomic.
#[derive(Debug)]
pub(crate) struct AtomicTransactionState {
    pub(crate) state: AtomicU64,
}

impl From<TransactionState> for AtomicTransactionState {
    fn from(state: TransactionState) -> Self {
        Self {
            state: AtomicU64::new(state.encode()),
        }
    }
}

impl From<AtomicTransactionState> for TransactionState {
    fn from(state: AtomicTransactionState) -> Self {
        let encoded = state.state.load(Ordering::Acquire);
        TransactionState::decode(encoded)
    }
}

impl std::cmp::PartialEq<TransactionState> for AtomicTransactionState {
    fn eq(&self, other: &TransactionState) -> bool {
        &self.load() == other
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Preparing => write!(f, "Preparing"),
            TransactionState::Committed(ts) => write!(f, "Committed({ts})"),
            TransactionState::Aborted => write!(f, "Aborted"),
            TransactionState::Terminated => write!(f, "Terminated"),
        }
    }
}

impl AtomicTransactionState {
    fn store(&self, state: TransactionState) {
        self.state.store(state.encode(), Ordering::Release);
    }

    fn load(&self) -> TransactionState {
        TransactionState::decode(self.state.load(Ordering::Acquire))
    }
}

/// A multi-version concurrency control database.
pub struct MvStore<Clock: LogicalClock> {
    rows: SkipMap<RowID, RwLock<Vec<RowVersion>>>,
    txs: SkipMap<TxID, RwLock<Transaction>>,
    tx_ids: AtomicU64,
    next_rowid: AtomicU64,
    clock: Clock,
}

impl<Clock: LogicalClock> std::fmt::Debug for MvStore<Clock> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvStore")
            .field("rows", &self.rows)
            .field("txs", &self.txs)
            .field("tx_ids", &self.tx_ids)
            .field("next_rowid", &self.next_rowid)
            .field("clock", &"<Clock>")
            .finish()
    }
}

impl<Clock: LogicalClock> MvStore<Clock> {
    /// Creates a new database.
    pub fn new(clock: Clock) -> Self {
        Self {
            rows: SkipMap::new(),
            txs: SkipMap::new(),
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            next_rowid: AtomicU64::new(0), // TODO: determine this from B-Tree
            clock,
        }
    }

    pub fn get_next_rowid(&self) -> i64 {
        self.next_rowid.fetch_add(1, Ordering::SeqCst) as i64
    }

    /// Inserts a new row into the database.
    ///
    /// This function inserts a new `row` into the database within the context
    /// of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to insert the new row.
    /// * `row` - the row object containing the values to be inserted.
    ///
    pub fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        tracing::trace!("insert(tx_id={}, row.id={:?})", tx_id, row.id);
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
        let mut tx = tx.value().write().unwrap();
        assert_eq!(tx.state, TransactionState::Active);
        let id = row.id;
        let row_version = RowVersion {
            begin: TxTimestampOrID::TxID(tx.tx_id),
            end: None,
            row,
        };
        tx.insert_to_write_set(id);
        drop(tx);
        self.insert_version(id, row_version);
        Ok(())
    }

    /// Updates a row in the database with new values.
    ///
    /// This function updates an existing row in the database within the
    /// context of the transaction `tx_id`. The `row` argument identifies the
    /// row to be updated as `id` and contains the new values to be inserted.
    ///
    /// If the row identified by the `id` does not exist, this function does
    /// nothing and returns `false`. Otherwise, the function updates the row
    /// with the new values and returns `true`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to update the new row.
    /// * `row` - the row object containing the values to be updated.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully updated, and `false` otherwise.
    pub fn update(&self, tx_id: TxID, row: Row) -> Result<bool> {
        tracing::trace!("update(tx_id={}, row.id={:?})", tx_id, row.id);
        if !self.delete(tx_id, row.id)? {
            return Ok(false);
        }
        self.insert(tx_id, row)?;
        Ok(true)
    }

    /// Inserts a row in the database with new values, previously deleting
    /// any old data if it existed. Bails on a delete error, e.g. write-write conflict.
    pub fn upsert(&self, tx_id: TxID, row: Row) -> Result<()> {
        tracing::trace!("upsert(tx_id={}, row.id={:?})", tx_id, row.id);
        self.delete(tx_id, row.id)?;
        self.insert(tx_id, row)
    }

    /// Deletes a row from the table with the given `id`.
    ///
    /// This function deletes an existing row `id` in the database within the
    /// context of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to delete the new row.
    /// * `id` - the ID of the row to delete.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully deleted, and `false` otherwise.
    ///
    pub fn delete(&self, tx_id: TxID, id: RowID) -> Result<bool> {
        tracing::trace!("delete(tx_id={}, id={:?})", tx_id, id);
        let row_versions_opt = self.rows.get(&id);
        if let Some(ref row_versions) = row_versions_opt {
            let mut row_versions = row_versions.value().write().unwrap();
            for rv in row_versions.iter_mut().rev() {
                let tx = self
                    .txs
                    .get(&tx_id)
                    .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                let tx = tx.value().read().unwrap();
                assert_eq!(tx.state, TransactionState::Active);
                // A transaction cannot delete a version that it cannot see,
                // nor can it conflict with it.
                if !rv.is_visible_to(&tx, &self.txs) {
                    continue;
                }
                if is_write_write_conflict(&self.txs, &tx, rv) {
                    drop(row_versions);
                    drop(row_versions_opt);
                    drop(tx);
                    self.rollback_tx(tx_id);
                    return Err(DatabaseError::WriteWriteConflict);
                }

                rv.end = Some(TxTimestampOrID::TxID(tx.tx_id));
                drop(row_versions);
                drop(row_versions_opt);
                drop(tx);
                let tx = self
                    .txs
                    .get(&tx_id)
                    .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                let mut tx = tx.value().write().unwrap();
                tx.insert_to_write_set(id);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retrieves a row from the table with the given `id`.
    ///
    /// This operation is performed within the scope of the transaction identified
    /// by `tx_id`. If the row is not found in the in-memory MVCC index and a pager
    /// is provided, it will attempt to read the row from disk.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to perform the read operation in.
    /// * `id` - The ID of the row to retrieve.
    /// * `pager` - The pager to use for reading from disk if the row is not in memory.
    ///
    /// # Returns
    ///
    /// Returns `Some(row)` with the row data if the row with the given `id` exists,
    /// and `None` otherwise.
    pub fn read(&self, tx_id: TxID, id: RowID, pager: Rc<Pager>) -> Result<Option<Row>> {
        tracing::trace!("read(tx_id={}, id={:?})", tx_id, id);
        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value().read().unwrap();
        assert_eq!(tx.state, TransactionState::Active);
        if let Some(row_versions) = self.rows.get(&id) {
            let row_versions = row_versions.value().read().unwrap();
            if let Some(rv) = row_versions
                .iter()
                .rev()
                .find(|rv| rv.is_visible_to(&tx, &self.txs))
            {
                tx.insert_to_read_set(id);
                return Ok(Some(rv.row.clone()));
            }
        }

        // If not found in memory, try lazy load from disk
        self.lazy_read_from_pager(tx_id, id, pager)
    }

    /// Gets all row ids in the database.
    pub fn scan_row_ids(&self) -> Result<Vec<RowID>> {
        tracing::trace!("scan_row_ids");
        let keys = self.rows.iter().map(|entry| *entry.key());
        Ok(keys.collect())
    }

    /// Gets all row ids in the database for a given table.
    pub fn scan_row_ids_for_table(&self, table_id: u64, pager: Rc<Pager>) -> Result<Vec<RowID>> {
        use crate::storage::btree::BTreeCursor;
        use crate::types::{IOResult, SeekKey, SeekOp};
        use std::collections::BTreeSet;

        tracing::trace!("scan_row_ids_for_table(table_id={})", table_id);

        let mut row_ids = BTreeSet::new();

        // First, collect row IDs from the MVCC index
        for entry in self.rows.range(
            RowID {
                table_id,
                row_id: 0,
            }..RowID {
                table_id,
                row_id: i64::MAX,
            },
        ) {
            row_ids.insert(*entry.key());
        }

        // Then, scan the disk B-tree to find existing rows
        let root_page = table_id as usize;
        let mut cursor = BTreeCursor::new_table(
            None, // No MVCC cursor for scanning
            pager, root_page, 1, // We'll adjust this as needed
        );

        // Start from the beginning of the table
        match cursor
            .seek(SeekKey::TableRowId(0), SeekOp::GE { eq_only: false })
            .map_err(|e| DatabaseError::Io(e.to_string()))?
        {
            IOResult::Done(_) => {
                // Iterate through all records in the table
                loop {
                    match cursor
                        .rowid()
                        .map_err(|e| DatabaseError::Io(e.to_string()))?
                    {
                        IOResult::Done(Some(row_id)) => {
                            row_ids.insert(RowID { table_id, row_id });
                        }
                        IOResult::Done(None) => break,
                        IOResult::IO => break, // Stop on IO requirement
                    }

                    // Move to next record
                    match cursor
                        .next()
                        .map_err(|e| DatabaseError::Io(e.to_string()))?
                    {
                        IOResult::Done(has_next) => {
                            if !has_next {
                                break;
                            }
                        }
                        IOResult::IO => break, // Stop on IO requirement
                    }
                }
            }
            IOResult::IO => {
                // If we can't scan the disk, just return the MVCC results
                tracing::warn!(
                    "Could not scan disk B-tree for table {}, returning MVCC results only",
                    table_id
                );
            }
        }

        Ok(row_ids.into_iter().collect())
    }

    pub fn get_row_id_range(
        &self,
        table_id: u64,
        start: i64,
        bucket: &mut Vec<RowID>,
        max_items: u64,
    ) -> Result<()> {
        tracing::trace!(
            "get_row_id_in_range(table_id={}, range_start={})",
            table_id,
            start,
        );
        let start_id = RowID {
            table_id,
            row_id: start,
        };

        let end_id = RowID {
            table_id,
            row_id: i64::MAX,
        };

        self.rows
            .range(start_id..end_id)
            .take(max_items as usize)
            .for_each(|entry| bucket.push(*entry.key()));

        Ok(())
    }

    pub fn get_next_row_id_for_table(&self, table_id: u64, start: i64) -> Option<RowID> {
        tracing::trace!(
            "getting_next_id_for_table(table_id={}, range_start={})",
            table_id,
            start,
        );
        let min_bound = RowID {
            table_id,
            row_id: start,
        };

        let max_bound = RowID {
            table_id,
            row_id: i64::MAX,
        };

        self.rows
            .range(min_bound..max_bound)
            .next()
            .map(|entry| *entry.key())
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub fn begin_tx(&self) -> TxID {
        let tx_id = self.get_tx_id();
        let begin_ts = self.get_timestamp();
        let tx = Transaction::new(tx_id, begin_ts);
        tracing::trace!("begin_tx(tx_id={})", tx_id);
        self.txs.insert(tx_id, RwLock::new(tx));
        tx_id
    }

    /// Commits a transaction with the specified transaction ID.
    ///
    /// This function commits the changes made within the specified transaction and finalizes the
    /// transaction. Once a transaction has been committed, all changes made within the transaction
    /// are visible to other transactions that access the same data.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to commit.
    /// * `pager` - The pager to use for persisting the changes.
    /// * `connection` - The connection to use for persisting the changes.
    pub fn commit_tx(
        &self,
        tx_id: TxID,
        pager: Rc<Pager>,
        connection: &crate::Connection,
    ) -> Result<()> {
        let end_ts = self.get_timestamp();
        // NOTICE: the first shadowed tx keeps the entry alive in the map
        // for the duration of this whole function, which is important for correctness!
        let tx = self.txs.get(&tx_id).ok_or(DatabaseError::TxTerminated)?;
        let tx = tx.value().write().unwrap();
        match tx.state.load() {
            TransactionState::Terminated => return Err(DatabaseError::TxTerminated),
            _ => {
                assert_eq!(tx.state, TransactionState::Active);
            }
        }
        tx.state.store(TransactionState::Preparing);
        tracing::trace!("prepare_tx(tx_id={})", tx_id);

        /* TODO: The code we have here is sufficient for snapshot isolation.
        ** In order to implement serializability, we need the following steps:
        **
        ** 1. Validate if all read versions are still visible by inspecting the read_set
        ** 2. Validate if there are no phantoms by walking the scans from scan_set (which we don't even have yet)
        **    - a phantom is a version that became visible in the middle of our transaction,
        **      but wasn't taken into account during one of the scans from the scan_set
        ** 3. Wait for commit dependencies, which we don't even track yet...
        **    Excerpt from what's a commit dependency and how it's tracked in the original paper:
        **    """
                A transaction T1 has a commit dependency on another transaction
                T2, if T1 is allowed to commit only if T2 commits. If T2 aborts,
                T1 must also abort, so cascading aborts are possible. T1 acquires a
                commit dependency either by speculatively reading or speculatively ignoring a version,
                instead of waiting for T2 to commit.
                We implement commit dependencies by a register-and-report
                approach: T1 registers its dependency with T2 and T2 informs T1
                when it has committed or aborted. Each transaction T contains a
                counter, CommitDepCounter, that counts how many unresolved
                commit dependencies it still has. A transaction cannot commit
                until this counter is zero. In addition, T has a Boolean variable
                AbortNow that other transactions can set to tell T to abort. Each
                transaction T also has a set, CommitDepSet, that stores transaction IDs
                of the transactions that depend on T.
                To take a commit dependency on a transaction T2, T1 increments
                its CommitDepCounter and adds its transaction ID to T2’s CommitDepSet.
                When T2 has committed, it locates each transaction in
                its CommitDepSet and decrements their CommitDepCounter. If
                T2 aborted, it tells the dependent transactions to also abort by
                setting their AbortNow flags. If a dependent transaction is not
                found, this means that it has already aborted.
                Note that a transaction with commit dependencies may not have to
                wait at all - the dependencies may have been resolved before it is
                ready to commit. Commit dependencies consolidate all waits into
                a single wait and postpone the wait to just before commit.
                Some transactions may have to wait before commit.
                Waiting raises a concern of deadlocks.
                However, deadlocks cannot occur because an older transaction never
                waits on a younger transaction. In
                a wait-for graph the direction of edges would always be from a
                younger transaction (higher end timestamp) to an older transaction
                (lower end timestamp) so cycles are impossible.
            """
        **  If you're wondering when a speculative read happens, here you go:
        **  Case 1: speculative read of TB:
            """
                If transaction TB is in the Preparing state, it has acquired an end
                timestamp TS which will be V’s begin timestamp if TB commits.
                A safe approach in this situation would be to have transaction T
                wait until transaction TB commits. However, we want to avoid all
                blocking during normal processing so instead we continue with
                the visibility test and, if the test returns true, allow T to
                speculatively read V. Transaction T acquires a commit dependency on
                TB, restricting the serialization order of the two transactions. That
                is, T is allowed to commit only if TB commits.
            """
        **  Case 2: speculative ignore of TE:
            """
                If TE’s state is Preparing, it has an end timestamp TS that will become
                the end timestamp of V if TE does commit. If TS is greater than the read
                time RT, it is obvious that V will be visible if TE commits. If TE
                aborts, V will still be visible, because any transaction that updates
                V after TE has aborted will obtain an end timestamp greater than
                TS. If TS is less than RT, we have a more complicated situation:
                if TE commits, V will not be visible to T but if TE aborts, it will
                be visible. We could handle this by forcing T to wait until TE
                commits or aborts but we want to avoid all blocking during normal processing.
                Instead we allow T to speculatively ignore V and
                proceed with its processing. Transaction T acquires a commit
                dependency (see Section 2.7) on TE, that is, T is allowed to commit
                only if TE commits.
            """
        */
        tx.state.store(TransactionState::Committed(end_ts));
        tracing::trace!("commit_tx(tx_id={})", tx_id);
        let write_set: Vec<RowID> = tx.write_set.iter().map(|v| *v.value()).collect();
        drop(tx);
        // Postprocessing: inserting row versions and updating timestamps
        for ref id in &write_set {
            if let Some(row_versions) = self.rows.get(id) {
                let mut row_versions = row_versions.value().write().unwrap();
                for row_version in row_versions.iter_mut() {
                    if let TxTimestampOrID::TxID(id) = row_version.begin {
                        if id == tx_id {
                            // New version is valid STARTING FROM committing transaction's end timestamp
                            // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                            row_version.begin = TxTimestampOrID::Timestamp(end_ts);
                        }
                    }
                    if let Some(TxTimestampOrID::TxID(id)) = row_version.end {
                        if id == tx_id {
                            // Old version is valid UNTIL committing transaction's end timestamp
                            // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                            row_version.end = Some(TxTimestampOrID::Timestamp(end_ts));
                        }
                    }
                }
            }
        }
        tracing::trace!("updated(tx_id={})", tx_id);
        // We have now updated all the versions with a reference to the
        // transaction ID to a timestamp and can, therefore, remove the
        // transaction. Please note that when we move to lockless, the
        // invariant doesn't necessarily hold anymore because another thread
        // might have speculatively read a version that we want to remove.
        // But that's a problem for another day.
        // FIXME: it actually just become a problem for today!!!
        // TODO: test that reproduces this failure, and then a fix
        self.txs.remove(&tx_id);

        // Start write transaction before writing data to pager
        if let crate::types::IOResult::Done(result) = pager
            .begin_write_tx()
            .map_err(|e| DatabaseError::Io(e.to_string()))
            .unwrap()
        {
            if let crate::result::LimboResult::Busy = result {
                return Err(DatabaseError::Io(
                    "Pager write transaction busy".to_string(),
                ));
            }
        }

        // Write committed data to pager for persistence
        for ref id in &write_set {
            if let Some(row_versions) = self.rows.get(id) {
                let row_versions = row_versions.value().read().unwrap();
                // Find the version that was just committed
                for (i, row_version) in row_versions.iter().enumerate() {
                    if let TxTimestampOrID::Timestamp(ts) = row_version.begin {
                        if ts == end_ts {
                            // This is the version we just committed
                            self.write_row_to_pager(pager.clone(), &row_version.row)
                                .unwrap();
                            break;
                        }
                    }
                }
            }
        }

        tracing::trace!("logged(tx_id={})", tx_id);

        // Flush dirty pages to WAL - this is critical for data persistence
        // Similar to what step_end_write_txn does for legacy transactions
        loop {
            let result = pager
                .end_tx(
                    false, // rollback = false since we're committing
                    false, // schema_did_change = false for now (could be improved)
                    connection,
                    connection.wal_checkpoint_disabled.get(),
                )
                .map_err(|e| DatabaseError::Io(e.to_string()))
                .unwrap();
            if let crate::types::IOResult::Done(result) = result {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        Ok(())
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    pub fn rollback_tx(&self, tx_id: TxID) {
        let tx_unlocked = self.txs.get(&tx_id).unwrap();
        let tx = tx_unlocked.value().write().unwrap();
        assert_eq!(tx.state, TransactionState::Active);
        tx.state.store(TransactionState::Aborted);
        tracing::trace!("abort(tx_id={})", tx_id);
        let write_set: Vec<RowID> = tx.write_set.iter().map(|v| *v.value()).collect();
        drop(tx);

        for ref id in &write_set {
            if let Some(row_versions) = self.rows.get(id) {
                let mut row_versions = row_versions.value().write().unwrap();
                row_versions.retain(|rv| rv.begin != TxTimestampOrID::TxID(tx_id));
                if row_versions.is_empty() {
                    self.rows.remove(id);
                }
            }
        }

        let tx = tx_unlocked.value().read().unwrap();
        tx.state.store(TransactionState::Terminated);
        tracing::trace!("terminate(tx_id={})", tx_id);
        // FIXME: verify that we can already remove the transaction here!
        // Maybe it's fine for snapshot isolation, but too early for serializable?
        self.txs.remove(&tx_id);

        // TODO: rollback the transaction in the pager
    }

    /// Generates next unique transaction id
    pub fn get_tx_id(&self) -> u64 {
        self.tx_ids.fetch_add(1, Ordering::SeqCst)
    }

    /// Gets current timestamp
    pub fn get_timestamp(&self) -> u64 {
        self.clock.get_timestamp()
    }

    /// Removes unused row  versions with very loose heuristics,
    /// which sometimes leaves versions intact for too long.
    /// Returns the number of removed versions.
    pub fn drop_unused_row_versions(&self) -> usize {
        tracing::trace!(
            "drop_unused_row_versions() -> txs: {}; rows: {}",
            self.txs.len(),
            self.rows.len()
        );
        let mut dropped = 0;
        let mut to_remove = Vec::new();
        for entry in self.rows.iter() {
            let mut row_versions = entry.value().write().unwrap();
            row_versions.retain(|rv| {
                // FIXME: should take rv.begin into account as well
                let should_stay = match rv.end {
                    Some(TxTimestampOrID::Timestamp(version_end_ts)) => {
                        // a transaction started before this row version ended, ergo row version is needed
                        // NOTICE: O(row_versions x transactions), but also lock-free, so sounds acceptable
                        self.txs.iter().any(|tx| {
                            let tx = tx.value().read().unwrap();
                            // FIXME: verify!
                            match tx.state.load() {
                                TransactionState::Active | TransactionState::Preparing => {
                                    version_end_ts > tx.begin_ts
                                }
                                _ => false,
                            }
                        })
                    }
                    // Let's skip potentially complex logic if the transafction is still
                    // active/tracked. We will drop the row version when the transaction
                    // gets garbage-collected itself, it will always happen eventually.
                    Some(TxTimestampOrID::TxID(tx_id)) => !self.txs.contains_key(&tx_id),
                    // this row version is current, ergo visible
                    None => true,
                };
                if !should_stay {
                    dropped += 1;
                    tracing::trace!(
                        "Dropping row version {:?} {:?}-{:?}",
                        entry.key(),
                        rv.begin,
                        rv.end
                    );
                }
                should_stay
            });
            if row_versions.is_empty() {
                to_remove.push(*entry.key());
            }
        }
        for id in to_remove {
            self.rows.remove(&id);
        }
        dropped
    }

    /// Lazily reads a row from the pager when it's not in the MVCC index.
    fn lazy_read_from_pager(
        &self,
        tx_id: TxID,
        id: RowID,
        pager: Rc<Pager>,
    ) -> Result<Option<Row>> {
        use crate::storage::btree::BTreeCursor;
        use crate::types::{IOResult, SeekKey, SeekOp};

        tracing::trace!("lazy_read_from_pager(tx_id={}, id={:?})", tx_id, id);

        // Create a BTreeCursor to read from the B-tree
        let root_page = id.table_id as usize;
        let mut cursor = BTreeCursor::new_table(
            None, // No MVCC cursor for direct B-tree read
            pager, root_page, 1, // We'll determine column count from the record
        );

        // Seek to the specific row
        let seek_key = SeekKey::TableRowId(id.row_id);
        match cursor
            .seek(seek_key, SeekOp::GE { eq_only: true })
            .map_err(|e| DatabaseError::Io(e.to_string()))?
        {
            IOResult::Done(seek_result) => {
                if !matches!(seek_result, crate::types::SeekResult::Found) {
                    return Ok(None);
                }
            }
            IOResult::IO => {
                return Err(DatabaseError::Io(
                    "IO operation required during lazy read".to_string(),
                ));
            }
        }

        // Read the record
        let record = match cursor
            .record()
            .map_err(|e| DatabaseError::Io(e.to_string()))?
        {
            IOResult::Done(Some(record)) => record,
            IOResult::Done(None) => return Ok(None),
            IOResult::IO => {
                return Err(DatabaseError::Io(
                    "IO operation required during record read".to_string(),
                ));
            }
        };

        // Get the column count from the record using a record cursor
        let mut record_cursor = crate::types::RecordCursor::with_capacity(10); // Estimate capacity
        let column_count = record_cursor.count(&record);

        // Convert the record to a Row
        let data = record.get_payload().to_vec();
        let row = Row::new(id, data, column_count);

        // Create a row version that appears to be committed at timestamp 0
        // This makes it visible to all transactions
        let row_version = RowVersion {
            begin: TxTimestampOrID::Timestamp(0),
            end: None,
            row: row.clone(),
        };

        // Insert the row version into the MVCC index for future reads
        self.insert_version(id, row_version);

        // Add to the transaction's read set
        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value().read().unwrap();
        tx.insert_to_read_set(id);

        Ok(Some(row))
    }

    /// Writes a row to the pager for persistence.
    fn write_row_to_pager(&self, pager: Rc<Pager>, row: &Row) -> Result<()> {
        use crate::storage::btree::BTreeCursor;
        use crate::types::{IOResult, SeekKey, SeekOp};

        // The row.data is already a properly serialized SQLite record payload
        // Create an ImmutableRecord and copy the data
        let mut record = ImmutableRecord::new(row.data.len());
        record.start_serialization(&row.data);

        // Create a BTreeKey for the row
        let key = BTreeKey::new_table_rowid(row.id.row_id, Some(&record));

        // Get the column count from the row
        let root_page = row.id.table_id as usize;
        let num_columns = row.column_count;

        let mut cursor = BTreeCursor::new_table(
            None, // Write directly to B-tree
            pager,
            root_page,
            num_columns,
        );

        // Position the cursor first by seeking to the row position
        let seek_key = SeekKey::TableRowId(row.id.row_id);
        match cursor
            .seek(seek_key, SeekOp::GE { eq_only: true })
            .map_err(|e| DatabaseError::Io(e.to_string()))?
        {
            IOResult::Done(_) => {}
            IOResult::IO => {
                panic!("IOResult::IO not supported in write_row_to_pager seek");
            }
        }

        // Insert the record into the B-tree
        match cursor
            .insert(&key, true)
            .map_err(|e| DatabaseError::Io(e.to_string()))?
        {
            IOResult::Done(()) => {}
            IOResult::IO => {
                panic!("IOResult::IO not supported in write_row_to_pager insert");
            }
        }

        tracing::trace!(
            "write_row_to_pager(table_id={}, row_id={})",
            row.id.table_id,
            row.id.row_id
        );
        Ok(())
    }

    // Extracts the begin timestamp from a transaction
    fn get_begin_timestamp(&self, ts_or_id: &TxTimestampOrID) -> u64 {
        match ts_or_id {
            TxTimestampOrID::Timestamp(ts) => *ts,
            TxTimestampOrID::TxID(tx_id) => {
                self.txs
                    .get(tx_id)
                    .unwrap()
                    .value()
                    .read()
                    .unwrap()
                    .begin_ts
            }
        }
    }

    /// Inserts a new row version into the database, while making sure that
    /// the row version is inserted in the correct order.
    fn insert_version(&self, id: RowID, row_version: RowVersion) {
        let versions = self.rows.get_or_insert_with(id, || RwLock::new(Vec::new()));
        let mut versions = versions.value().write().unwrap();
        self.insert_version_raw(&mut versions, row_version)
    }

    /// Inserts a new row version into the internal data structure for versions,
    /// while making sure that the row version is inserted in the correct order.
    fn insert_version_raw(&self, versions: &mut Vec<RowVersion>, row_version: RowVersion) {
        // NOTICE: this is an insert a'la insertion sort, with pessimistic linear complexity.
        // However, we expect the number of versions to be nearly sorted, so we deem it worthy
        // to search linearly for the insertion point instead of paying the price of using
        // another data structure, e.g. a BTreeSet. If it proves to be too quadratic empirically,
        // we can either switch to a tree-like structure, or at least use partition_point()
        // which performs a binary search for the insertion point.
        let position = versions
            .iter()
            .rposition(|v| {
                self.get_begin_timestamp(&v.begin) < self.get_begin_timestamp(&row_version.begin)
            })
            .map(|p| p + 1)
            .unwrap_or(0);
        if versions.len() - position > 3 {
            tracing::debug!(
                "Inserting a row version {} positions from the end",
                versions.len() - position
            );
        }
        versions.insert(position, row_version);
    }
}

/// A write-write conflict happens when transaction T_current attempts to update a
/// row version that is:
/// a) currently being updated by an active transaction T_previous, or
/// b) was updated by an ended transaction T_previous that committed AFTER T_current started
/// but BEFORE T_previous commits.
///
/// "Suppose transaction T wants to update a version V. V is updatable
/// only if it is the latest version, that is, it has an end timestamp equal
/// to infinity or its End field contains the ID of a transaction TE and
/// TE’s state is Aborted"
/// Ref: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf , page 301,
/// 2.6. Updating a Version.
pub(crate) fn is_write_write_conflict(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            let te = te.value().read().unwrap();
            if te.tx_id == tx.tx_id {
                return false;
            }
            te.state.load() != TransactionState::Aborted
        }
        // A non-"infinity" end timestamp (here modeled by Some(ts)) functions as a write lock
        // on the row, so it can never be updated by another transaction.
        // Ref: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf , page 301,
        // 2.6. Updating a Version.
        Some(TxTimestampOrID::Timestamp(_)) => true,
        None => false,
    }
}

impl RowVersion {
    pub fn is_visible_to(
        &self,
        tx: &Transaction,
        txs: &SkipMap<TxID, RwLock<Transaction>>,
    ) -> bool {
        is_begin_visible(txs, tx, self) && is_end_visible(txs, tx, self)
    }
}

fn is_begin_visible(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.begin {
        TxTimestampOrID::Timestamp(rv_begin_ts) => tx.begin_ts >= rv_begin_ts,
        TxTimestampOrID::TxID(rv_begin) => {
            let tb = txs.get(&rv_begin).unwrap();
            let tb = tb.value().read().unwrap();
            let visible = match tb.state.load() {
                TransactionState::Active => tx.tx_id == tb.tx_id && rv.end.is_none(),
                TransactionState::Preparing => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => tx.begin_ts >= committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_begin_visible: tx={tx}, tb={tb} rv = {:?}-{:?} visible = {visible}",
                rv.begin,
                rv.end
            );
            visible
        }
    }
}

fn is_end_visible(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            let te = te.value().read().unwrap();
            let visible = match te.state.load() {
                TransactionState::Active => tx.tx_id != te.tx_id,
                TransactionState::Preparing => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => tx.begin_ts < committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_end_visible: tx={tx}, te={te} rv = {:?}-{:?}  visible = {visible}",
                rv.begin,
                rv.end
            );
            visible
        }
        None => true,
    }
}
