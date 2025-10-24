use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    DeleteRowStateMachine, MVTableId, MvStore, RowVersion, TxTimestampOrID, WriteRowStateMachine,
    SQLITE_SCHEMA_MVCC_TABLE_ID,
};
use crate::state_machine::{StateMachine, StateTransition, TransitionResult};
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::storage::pager::CreateBTreeFlags;
use crate::storage::wal::{CheckpointMode, TursoRwLock};
use crate::types::{IOCompletions, IOResult, ImmutableRecord, RecordCursor};
use crate::{
    CheckpointResult, Completion, Connection, IOExt, Pager, Result, TransactionState, Value,
    ValueRef,
};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug)]
pub enum CheckpointState {
    AcquireLock,
    BeginPagerTxn,
    WriteRow {
        write_set_index: usize,
        requires_seek: bool,
    },
    WriteRowStateMachine {
        write_set_index: usize,
    },
    DeleteRowStateMachine {
        write_set_index: usize,
    },
    CommitPagerTxn,
    TruncateLogicalLog,
    FsyncLogicalLog,
    CheckpointWal,
    Finalize,
}

/// The states of the locks held by the state machine - these are tracked for error handling so that they are
/// released if the state machine fails.
pub struct LockStates {
    blocking_checkpoint_lock_held: bool,
    pager_read_tx: bool,
    pager_write_tx: bool,
}

/// A state machine that performs a complete checkpoint operation on the MVCC store.
///
/// The checkpoint process:
/// 1. Takes a blocking lock on the database so that no other transactions can run during the checkpoint.
/// 2. Determines which row versions should be written to the B-tree.
/// 3. Begins a pager transaction
/// 4. Writes all the selected row versions to the B-tree.
/// 5. Commits the pager transaction, effectively flushing to the WAL
/// 6. Truncates the logical log file
/// 7. Immediately does a TRUNCATE checkpoint from the WAL to the DB
/// 8. Releases the blocking_checkpoint_lock
pub struct CheckpointStateMachine<Clock: LogicalClock> {
    /// The current state of the state machine
    state: CheckpointState,
    /// The states of the locks held by the state machine - these are tracked for error handling so that they are
    /// released if the state machine fails.
    lock_states: LockStates,
    /// The highest transaction ID that has been checkpointed in a previous checkpoint.
    checkpointed_txid_max_old: u64,
    /// The highest transaction ID that will be checkpointed in the current checkpoint.
    checkpointed_txid_max_new: u64,
    /// Pager used for writing to the B-tree
    pager: Arc<Pager>,
    /// MVCC store containing the row versions.
    mvstore: Arc<MvStore<Clock>>,
    /// Connection to the database
    connection: Arc<Connection>,
    /// Lock used to block other transactions from running during the checkpoint
    checkpoint_lock: Arc<TursoRwLock>,
    /// All committed versions to write to the B-tree.
    /// In the case of CREATE TABLE / DROP TABLE ops, contains a [SpecialWrite] to create/destroy the B-tree.
    write_set: Vec<(RowVersion, Option<SpecialWrite>)>,
    /// State machine for writing rows to the B-tree
    write_row_state_machine: Option<StateMachine<WriteRowStateMachine>>,
    /// State machine for deleting rows from the B-tree
    delete_row_state_machine: Option<StateMachine<DeleteRowStateMachine>>,
    /// Cursors for the B-trees
    cursors: HashMap<u64, Arc<RwLock<BTreeCursor>>>,
    /// Tables that were destroyed in this checkpoint
    destroyed_tables: HashSet<MVTableId>,
    /// Result of the checkpoint
    checkpoint_result: Option<CheckpointResult>,
    /// Update connection's transaction state on checkpoint. If checkpoint was called as automatic
    /// process in a transaction we don't want to change the state as we assume we are already on a
    /// write transaction and any failure will be cleared on vdbe error handling.
    update_transaction_state: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// Special writes for CREATE TABLE / DROP TABLE ops.
/// These are used to create/destroy B-trees during pager ops.
pub enum SpecialWrite {
    BTreeCreate {
        table_id: MVTableId,
    },
    BTreeDestroy {
        table_id: MVTableId,
        root_page: u64,
        num_columns: usize,
    },
}

impl<Clock: LogicalClock> CheckpointStateMachine<Clock> {
    pub fn new(
        pager: Arc<Pager>,
        mvstore: Arc<MvStore<Clock>>,
        connection: Arc<Connection>,
        update_transaction_state: bool,
    ) -> Self {
        let checkpoint_lock = mvstore.blocking_checkpoint_lock.clone();
        Self {
            state: CheckpointState::AcquireLock,
            lock_states: LockStates {
                blocking_checkpoint_lock_held: false,
                pager_read_tx: false,
                pager_write_tx: false,
            },
            pager,
            checkpointed_txid_max_old: mvstore.checkpointed_txid_max.load(Ordering::SeqCst),
            checkpointed_txid_max_new: mvstore.checkpointed_txid_max.load(Ordering::SeqCst),
            mvstore,
            connection,
            checkpoint_lock,
            write_set: Vec::new(),
            write_row_state_machine: None,
            delete_row_state_machine: None,
            cursors: HashMap::new(),
            destroyed_tables: HashSet::new(),
            checkpoint_result: None,
            update_transaction_state,
        }
    }

    /// Collect all committed versions that need to be written to the B-tree.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    ///    TODO: garbage collect row versions after checkpointing.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    ///      If the row didn't exist in the database file and was deleted, we can simply not write it.
    fn collect_committed_versions(&mut self) {
        // Keep track of the highest timestamp that will be checkpointed in the current checkpoint;
        // This value will be used at the end of the checkpoint to update the corresponding value in
        // the MVCC store, so that we don't checkpoint the same row versions again on the next checkpoint.
        let mut max_timestamp = self.checkpointed_txid_max_old;

        // Since table ids are negative, and we want schema changes (table_id=-1) to be processed first, we iterate in reverse order.
        // Reliance on SkipMap ordering is a bit yolo-swag fragile, but oh well.
        for entry in self.mvstore.rows.iter().rev() {
            let key = entry.key();
            if self.destroyed_tables.contains(&key.table_id) {
                // We won't checkpoint rows for tables that will be destroyed in this checkpoint.
                // There's two forms of destroyed table:
                // 1. A non-checkpointed table that was created in the logical log and then destroyed. We don't need to do anything about this table in the pager/btree layer.
                // 2. A checkpointed table that was destroyed in the logical log. We need to destroy the btree in the pager/btree layer.
                continue;
            }

            let row_versions = entry.value().read();

            let mut version_to_checkpoint = None;
            let mut exists_in_db_file = false;
            for version in row_versions.iter() {
                if let Some(TxTimestampOrID::Timestamp(ts)) = version.begin {
                    //TODO: garbage collect row versions after checkpointing.
                    if ts > self.checkpointed_txid_max_old {
                        version_to_checkpoint = Some(version);
                    } else {
                        exists_in_db_file = true;
                    }
                }
            }

            if let Some(version) = version_to_checkpoint {
                let is_delete = version.end.is_some();
                if let Some(TxTimestampOrID::Timestamp(ts)) = version.begin {
                    max_timestamp = max_timestamp.max(ts);
                }

                // Only write the row to the B-tree if it is not a delete, or if it is a delete and it exists in
                // the database file.
                if !is_delete || exists_in_db_file {
                    let mut special_write = None;

                    if version.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                        let row_data = ImmutableRecord::from_bin_record(version.row.data.clone());
                        let mut record_cursor = RecordCursor::new();
                        record_cursor.parse_full_header(&row_data).unwrap();
                        if let ValueRef::Integer(root_page) =
                            record_cursor.get_value(&row_data, 3).unwrap()
                        {
                            if is_delete {
                                let table_id = self
                                    .mvstore
                                    .table_id_to_rootpage
                                    .iter()
                                    .find(|entry| {
                                        entry.value().is_some_and(|r| r == root_page as u64)
                                    })
                                    .map(|entry| *entry.key())
                                    .unwrap(); // This assumes a valid mapping exists.
                                self.destroyed_tables.insert(table_id);

                                // We might need to create or destroy a B-tree in the pager during checkpoint if a row in root page 1 is deleted or created.
                                special_write = Some(SpecialWrite::BTreeDestroy {
                                    table_id,
                                    root_page: root_page as u64,
                                    num_columns: version.row.column_count,
                                });
                            } else if !exists_in_db_file {
                                let table_id = MVTableId::from(root_page);
                                special_write = Some(SpecialWrite::BTreeCreate { table_id });
                            }
                        }
                    }

                    self.write_set.push((version.clone(), special_write));
                }
            }
        }
        // Writing in ascending order of rowid gives us a better chance of using balance-quick algorithm
        // in case of an insert-heavy checkpoint.
        self.write_set.sort_by_key(|version| {
            (
                // Sort by table_id descending (schema changes first)
                std::cmp::Reverse(version.0.row.id.table_id),
                // Then by row_id ascending
                version.0.row.id.row_id,
            )
        });
        self.checkpointed_txid_max_new = max_timestamp;
    }

    /// Get the current row version to write to the B-tree
    fn get_current_row_version(
        &self,
        write_set_index: usize,
    ) -> Option<&(RowVersion, Option<SpecialWrite>)> {
        self.write_set.get(write_set_index)
    }

    /// Mutably get the current row version to write to the B-tree
    fn get_current_row_version_mut(
        &mut self,
        write_set_index: usize,
    ) -> Option<&mut (RowVersion, Option<SpecialWrite>)> {
        self.write_set.get_mut(write_set_index)
    }

    /// Check if we have more rows to write
    fn has_more_rows(&self, write_set_index: usize) -> bool {
        write_set_index < self.write_set.len()
    }

    /// Fsync the logical log file
    fn fsync_logical_log(&self) -> Result<Completion> {
        self.mvstore.storage.sync()
    }

    /// Truncate the logical log file
    fn truncate_logical_log(&self) -> Result<Completion> {
        self.mvstore.storage.truncate()
    }

    /// Perform a TRUNCATE checkpoint on the WAL
    fn checkpoint_wal(&self) -> Result<IOResult<CheckpointResult>> {
        let Some(wal) = &self.pager.wal else {
            panic!("No WAL to checkpoint");
        };
        let mut wal_ref = wal.borrow_mut();
        match wal_ref.checkpoint(
            &self.pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        )? {
            IOResult::Done(result) => Ok(IOResult::Done(result)),
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        }
    }

    fn step_inner(&mut self, _context: &()) -> Result<TransitionResult<CheckpointResult>> {
        match &self.state {
            CheckpointState::AcquireLock => {
                tracing::debug!("Acquiring blocking checkpoint lock");
                let locked = self.checkpoint_lock.write();
                if !locked {
                    return Err(crate::LimboError::Busy);
                }
                self.lock_states.blocking_checkpoint_lock_held = true;

                self.collect_committed_versions();
                tracing::debug!("Collected {} committed versions", self.write_set.len());

                if self.write_set.is_empty() {
                    // Nothing to checkpoint, skip to truncate logical log
                    self.state = CheckpointState::TruncateLogicalLog;
                } else {
                    self.state = CheckpointState::BeginPagerTxn;
                }
                Ok(TransitionResult::Continue)
            }
            CheckpointState::BeginPagerTxn => {
                tracing::debug!("Beginning pager transaction");
                // Start a pager transaction to write committed versions to B-tree
                let result = self.pager.begin_read_tx();
                if let Err(crate::LimboError::Busy) = result {
                    return Err(crate::LimboError::Busy);
                }
                result?;
                self.lock_states.pager_read_tx = true;

                let result = self.pager.io.block(|| self.pager.begin_write_tx());
                if let Err(crate::LimboError::Busy) = result {
                    return Err(crate::LimboError::Busy);
                }
                result?;
                if self.update_transaction_state {
                    self.connection.set_tx_state(TransactionState::Write {
                        schema_did_change: false,
                    }); // TODO: schema_did_change??
                }
                self.lock_states.pager_write_tx = true;
                self.state = CheckpointState::WriteRow {
                    write_set_index: 0,
                    requires_seek: true,
                };
                Ok(TransitionResult::Continue)
            }

            CheckpointState::WriteRow {
                write_set_index,
                requires_seek,
            } => {
                let write_set_index = *write_set_index;
                let requires_seek = *requires_seek;

                if !self.has_more_rows(write_set_index) {
                    // Done writing all rows
                    self.state = CheckpointState::CommitPagerTxn;
                    return Ok(TransitionResult::Continue);
                }

                let (num_columns, table_id, special_write) = {
                    let (row_version, special_write) =
                        self.get_current_row_version(write_set_index).unwrap();
                    (
                        row_version.row.column_count,
                        row_version.row.id.table_id,
                        *special_write,
                    )
                };

                // Handle CREATE TABLE / DROP TABLE ops
                if let Some(special_write) = special_write {
                    match special_write {
                        SpecialWrite::BTreeCreate { table_id } => {
                            let created_root_page = self.pager.io.block(|| {
                                self.pager.btree_create(&CreateBTreeFlags::new_table())
                            })?;
                            self.mvstore.insert_table_id_to_rootpage(
                                table_id,
                                Some(created_root_page as u64),
                            );
                        }
                        SpecialWrite::BTreeDestroy {
                            table_id,
                            root_page,
                            num_columns,
                        } => {
                            let known_root_page = self
                                .mvstore
                                .table_id_to_rootpage
                                .get(&table_id)
                                .expect("Table ID does not have a root page");
                            let known_root_page = known_root_page
                                .value()
                                .expect("Table ID does not have a root page");
                            assert_eq!(known_root_page, root_page, "MV store root page does not match root page in the sqlite_schema record: {known_root_page} != {root_page}");
                            let cursor = if let Some(cursor) = self.cursors.get(&known_root_page) {
                                cursor.clone()
                            } else {
                                let cursor = BTreeCursor::new_table(
                                    self.pager.clone(),
                                    known_root_page as i64,
                                    num_columns,
                                );
                                let cursor = Arc::new(RwLock::new(cursor));
                                self.cursors.insert(root_page, cursor.clone());
                                cursor
                            };
                            self.pager.io.block(|| cursor.write().btree_destroy())?;
                            self.destroyed_tables.insert(table_id);
                        }
                    }
                }

                if self.destroyed_tables.contains(&table_id) {
                    // Don't write rows for tables that will be destroyed in this checkpoint.
                    self.state = CheckpointState::WriteRow {
                        write_set_index: write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                }

                let root_page = {
                    let root_page = self
                        .mvstore
                        .table_id_to_rootpage
                        .get(&table_id)
                        .unwrap_or_else(|| {
                            panic!(
                                "Table ID does not have a root page: {table_id}, row_version: {:?}",
                                self.get_current_row_version(write_set_index).unwrap()
                            )
                        });
                    root_page.value().unwrap_or_else(|| {
                        panic!(
                            "Table ID does not have a root page: {table_id}, row_version: {:?}",
                            self.get_current_row_version(write_set_index).unwrap()
                        )
                    })
                };

                // If a table was created, it now has a real root page allocated for it, but the 'root_page' field in the sqlite_schema record is still the table id.
                // So we need to rewrite the row version to use the real root page.
                if let Some(SpecialWrite::BTreeCreate { table_id }) = special_write {
                    let root_page = {
                        let root_page = self
                            .mvstore
                            .table_id_to_rootpage
                            .get(&table_id)
                            .expect("Table ID does not have a root page");
                        root_page
                            .value()
                            .expect("Table ID does not have a root page")
                    };
                    let (row_version, _) =
                        self.get_current_row_version_mut(write_set_index).unwrap();
                    let record = ImmutableRecord::from_bin_record(row_version.row.data.clone());
                    let mut record_cursor = RecordCursor::new();
                    record_cursor.parse_full_header(&record).unwrap();
                    let values = record_cursor.get_values(&record)?;
                    let mut values = values
                        .into_iter()
                        .map(|value| value.to_owned())
                        .collect::<Vec<_>>();
                    values[3] = Value::Integer(root_page as i64);
                    let record = ImmutableRecord::from_values(&values, values.len());
                    row_version.row.data = record.get_payload().to_owned();
                }

                // Get or create cursor for this table
                let cursor = if let Some(cursor) = self.cursors.get(&root_page) {
                    cursor.clone()
                } else {
                    let cursor =
                        BTreeCursor::new_table(self.pager.clone(), root_page as i64, num_columns);
                    let cursor = Arc::new(RwLock::new(cursor));
                    self.cursors.insert(root_page, cursor.clone());
                    cursor
                };

                let (row_version, _) = self.get_current_row_version(write_set_index).unwrap();

                // Check if this is an insert or delete
                if row_version.end.is_some() {
                    // This is a delete operation
                    let state_machine = self
                        .mvstore
                        .delete_row_from_pager(row_version.row.id, cursor)?;
                    self.delete_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::DeleteRowStateMachine { write_set_index };
                } else {
                    // This is an insert/update operation
                    let state_machine =
                        self.mvstore
                            .write_row_to_pager(&row_version.row, cursor, requires_seek)?;
                    self.write_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::WriteRowStateMachine { write_set_index };
                }

                Ok(TransitionResult::Continue)
            }

            CheckpointState::WriteRowStateMachine { write_set_index } => {
                let write_set_index = *write_set_index;
                let write_row_state_machine = self.write_row_state_machine.as_mut().unwrap();

                match write_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.state = CheckpointState::WriteRow {
                            write_set_index: write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::DeleteRowStateMachine { write_set_index } => {
                let write_set_index = *write_set_index;
                let delete_row_state_machine = self.delete_row_state_machine.as_mut().unwrap();

                match delete_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.state = CheckpointState::WriteRow {
                            write_set_index: write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::CommitPagerTxn => {
                tracing::debug!("Committing pager transaction");
                let result = self.pager.commit_tx(&self.connection)?;
                match result {
                    IOResult::Done(_) => {
                        self.state = CheckpointState::TruncateLogicalLog;
                        self.lock_states.pager_read_tx = false;
                        self.lock_states.pager_write_tx = false;
                        if self.update_transaction_state {
                            self.connection.set_tx_state(TransactionState::None);
                        }
                        let header = self
                            .pager
                            .io
                            .block(|| {
                                self.pager.with_header_mut(|header| {
                                    header.schema_cookie = self
                                        .connection
                                        .db
                                        .schema
                                        .lock()
                                        .unwrap()
                                        .schema_version
                                        .into();
                                    *header
                                })
                            })
                            .unwrap();
                        self.mvstore.global_header.write().replace(header);
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                }
            }

            CheckpointState::TruncateLogicalLog => {
                tracing::debug!("Truncating logical log file");
                let c = self.truncate_logical_log()?;
                self.state = CheckpointState::FsyncLogicalLog;
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions::Single(c)))
                }
            }

            CheckpointState::FsyncLogicalLog => {
                tracing::debug!("Fsyncing logical log file");
                let c = self.fsync_logical_log()?;
                self.state = CheckpointState::CheckpointWal;
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions::Single(c)))
                }
            }

            CheckpointState::CheckpointWal => {
                tracing::debug!("Performing TRUNCATE checkpoint on WAL");
                match self.checkpoint_wal()? {
                    IOResult::Done(result) => {
                        self.checkpoint_result = Some(result);
                        self.state = CheckpointState::Finalize;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                }
            }

            CheckpointState::Finalize => {
                tracing::debug!("Releasing blocking checkpoint lock");
                self.mvstore
                    .checkpointed_txid_max
                    .store(self.checkpointed_txid_max_new, Ordering::SeqCst);
                self.checkpoint_lock.unlock();
                self.finalize(&())?;
                Ok(TransitionResult::Done(
                    self.checkpoint_result.take().unwrap(),
                ))
            }
        }
    }
}

impl<Clock: LogicalClock> StateTransition for CheckpointStateMachine<Clock> {
    type Context = ();
    type SMResult = CheckpointResult;

    fn step(&mut self, _context: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        let res = self.step_inner(&());
        match res {
            Err(err) => {
                tracing::info!("Error in checkpoint state machine: {err}");
                if self.lock_states.pager_write_tx {
                    self.pager.rollback_tx(self.connection.as_ref());
                    if self.update_transaction_state {
                        self.connection.set_tx_state(TransactionState::None);
                    }
                } else if self.lock_states.pager_read_tx {
                    self.pager.end_read_tx();
                    if self.update_transaction_state {
                        self.connection.set_tx_state(TransactionState::None);
                    }
                }
                if self.lock_states.blocking_checkpoint_lock_held {
                    self.checkpoint_lock.unlock();
                }
                Err(err)
            }
            Ok(result) => Ok(result),
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        matches!(self.state, CheckpointState::Finalize)
    }
}
