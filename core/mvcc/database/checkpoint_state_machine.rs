use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    DeleteRowStateMachine, MvStore, RowVersion, TxTimestampOrID, WriteRowStateMachine,
};
use crate::state_machine::{StateMachine, StateTransition, TransitionResult};
use crate::storage::btree::BTreeCursor;
use crate::storage::pager::CreateBTreeFlags;
use crate::storage::wal::{CheckpointMode, TursoRwLock};
use crate::types::{IOResult, ImmutableRecord, RecordCursor};
use crate::{CheckpointResult, Connection, IOExt, Pager, RefValue, Result, TransactionState};
use parking_lot::RwLock;
use std::collections::HashMap;
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
    cursors: HashMap<i64, Arc<RwLock<BTreeCursor>>>,
    /// Result of the checkpoint
    checkpoint_result: Option<CheckpointResult>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// Special writes for CREATE TABLE / DROP TABLE ops.
/// These are used to create/destroy B-trees during pager ops.
pub enum SpecialWrite {
    BTreeCreate { root_page: i64 },
    BTreeDestroy { root_page: i64, num_columns: usize },
}

impl<Clock: LogicalClock> CheckpointStateMachine<Clock> {
    pub fn new(
        pager: Arc<Pager>,
        mvstore: Arc<MvStore<Clock>>,
        connection: Arc<Connection>,
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
            checkpoint_result: None,
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

        for entry in self.mvstore.rows.iter() {
            let row_versions = entry.value().read();
            let mut exists_in_db_file = false;
            for (i, version) in row_versions.iter().enumerate() {
                let is_last = i == row_versions.len() - 1;
                if let TxTimestampOrID::Timestamp(ts) = &version.begin {
                    if *ts <= self.checkpointed_txid_max_old {
                        exists_in_db_file = true;
                    }

                    let current_version_ts =
                        if let Some(TxTimestampOrID::Timestamp(ts_end)) = version.end {
                            ts_end.max(*ts)
                        } else {
                            *ts
                        };
                    if current_version_ts <= self.checkpointed_txid_max_old {
                        // already checkpointed. TODO: garbage collect row versions after checkpointing.
                        continue;
                    }

                    let get_root_page = |row_data: &Vec<u8>| {
                        let row_data = ImmutableRecord::from_bin_record(row_data.clone());
                        let mut record_cursor = RecordCursor::new();
                        record_cursor.parse_full_header(&row_data).unwrap();
                        let RefValue::Integer(root_page) =
                            record_cursor.get_value(&row_data, 3).unwrap()
                        else {
                            panic!(
                                "Expected integer value for root page, got {:?}",
                                record_cursor.get_value(&row_data, 3)
                            );
                        };
                        root_page
                    };

                    max_timestamp = max_timestamp.max(current_version_ts);
                    if is_last {
                        let is_delete = version.end.is_some();
                        let should_be_deleted_from_db_file = is_delete && exists_in_db_file;

                        // We might need to create or destroy a B-tree in the pager during checkpoint if a row in root page 1 is deleted or created.
                        let special_write =
                            if should_be_deleted_from_db_file && version.row.id.table_id == 1 {
                                let root_page = get_root_page(&version.row.data);
                                Some(SpecialWrite::BTreeDestroy {
                                    root_page,
                                    num_columns: version.row.column_count,
                                })
                            } else if !exists_in_db_file && version.row.id.table_id == 1 {
                                let root_page = get_root_page(&version.row.data);
                                Some(SpecialWrite::BTreeCreate { root_page })
                            } else {
                                None
                            };

                        // Only write the row to the B-tree if it is not a delete, or if it is a delete and it exists in the database file.
                        if !is_delete || should_be_deleted_from_db_file {
                            self.write_set.push((version.clone(), special_write));
                        }
                    }
                }
            }
        }
        self.checkpointed_txid_max_new = max_timestamp;
    }

    /// Get the current row version to write to the B-tree
    fn get_current_row_version(
        &self,
        write_set_index: usize,
    ) -> Option<&(RowVersion, Option<SpecialWrite>)> {
        self.write_set.get(write_set_index)
    }

    /// Check if we have more rows to write
    fn has_more_rows(&self, write_set_index: usize) -> bool {
        write_set_index < self.write_set.len()
    }

    /// Fsync the logical log file
    fn fsync_logical_log(&self) -> Result<IOResult<()>> {
        self.mvstore.storage.sync()
    }

    /// Truncate the logical log file
    fn truncate_logical_log(&self) -> Result<IOResult<()>> {
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
                *self.connection.transaction_state.write() = TransactionState::Write {
                    schema_did_change: false,
                }; // TODO: schema_did_change??
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
                        SpecialWrite::BTreeCreate { root_page } => {
                            let created_root_page =
                                self.pager.io.block(|| {
                                    self.pager.btree_create(&CreateBTreeFlags::new_table())
                                })? as i64;
                            assert_eq!(created_root_page , root_page, "Created root page does not match expected root page: {created_root_page} != {root_page}");
                        }
                        SpecialWrite::BTreeDestroy {
                            root_page,
                            num_columns,
                        } => {
                            let cursor = if let Some(cursor) = self.cursors.get(&root_page) {
                                cursor.clone()
                            } else {
                                let cursor = BTreeCursor::new_table(
                                    None,
                                    self.pager.clone(),
                                    root_page,
                                    num_columns,
                                );
                                let cursor = Arc::new(RwLock::new(cursor));
                                self.cursors.insert(root_page, cursor.clone());
                                cursor
                            };
                            self.pager.io.block(|| cursor.write().btree_destroy())?;
                            self.cursors.remove(&root_page);
                        }
                    }
                }

                // Get or create cursor for this table
                let cursor = if let Some(cursor) = self.cursors.get(&table_id) {
                    cursor.clone()
                } else {
                    let cursor = BTreeCursor::new_table(
                        None, // Write directly to B-tree
                        self.pager.clone(),
                        table_id,
                        num_columns,
                    );
                    let cursor = Arc::new(RwLock::new(cursor));
                    self.cursors.insert(table_id, cursor.clone());
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
                let result = self.pager.end_tx(false, &self.connection)?;
                match result {
                    IOResult::Done(_) => {
                        self.state = CheckpointState::TruncateLogicalLog;
                        self.lock_states.pager_read_tx = false;
                        self.lock_states.pager_write_tx = false;
                        *self.connection.transaction_state.write() = TransactionState::None;
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
                match self.truncate_logical_log()? {
                    IOResult::Done(_) => {
                        self.state = CheckpointState::FsyncLogicalLog;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => {
                        if io.finished() {
                            self.state = CheckpointState::CheckpointWal;
                            Ok(TransitionResult::Continue)
                        } else {
                            Ok(TransitionResult::Io(io))
                        }
                    }
                }
            }

            CheckpointState::FsyncLogicalLog => {
                tracing::debug!("Fsyncing logical log file");
                match self.fsync_logical_log()? {
                    IOResult::Done(_) => {
                        self.state = CheckpointState::CheckpointWal;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
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
                    let rollback = true;
                    self.pager
                        .io
                        .block(|| self.pager.end_tx(rollback, self.connection.as_ref()))
                        .expect("failed to end pager write tx");
                    *self.connection.transaction_state.write() = TransactionState::None;
                } else if self.lock_states.pager_read_tx {
                    self.pager.end_read_tx().unwrap();
                    *self.connection.transaction_state.write() = TransactionState::None;
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
