use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    DeleteRowStateMachine, MVTableId, MvStore, Row, RowID, RowKey, RowVersion, TxTimestampOrID,
    WriteRowStateMachine, MVCC_META_KEY_PERSISTENT_TX_TS_MAX, MVCC_META_TABLE_NAME,
    SQLITE_SCHEMA_MVCC_TABLE_ID,
};
use crate::schema::Index;
use crate::state_machine::{StateMachine, StateTransition, TransitionResult};
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::storage::pager::CreateBTreeFlags;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::{CheckpointMode, TursoRwLock};
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::types::{IOCompletions, IOResult, ImmutableRecord};
use crate::{turso_assert, turso_assert_eq};
use crate::{
    CheckpointResult, Completion, Connection, IOExt, LimboError, Numeric, Pager, Result, SyncMode,
    TransactionState, Value, ValueRef,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    WriteIndexRow {
        index_write_set_index: usize,
        requires_seek: bool,
    },
    WriteIndexRowStateMachine {
        index_write_set_index: usize,
    },
    DeleteIndexRowStateMachine {
        index_write_set_index: usize,
    },
    CommitPagerTxn,
    CheckpointWal,
    /// Fsync the database file after checkpoint, before truncating WAL.
    /// This ensures durability: if we crash after WAL truncation but before DB fsync,
    /// the data would be lost.
    SyncDbFile,
    TruncateLogicalLog,
    FsyncLogicalLog,
    /// Truncate the WAL file after DB file and logical-log cleanup are safely durable.
    TruncateWal,
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
/// 6. Immediately does a TRUNCATE checkpoint from the WAL to the DB
/// 7. Fsync the DB file
/// 8. Truncate logical log to 0 (salt regenerated in memory), fsync, then truncate WAL
/// 9. Releases the blocking_checkpoint_lock
pub struct CheckpointStateMachine<Clock: LogicalClock> {
    /// The current state of the state machine
    state: CheckpointState,
    /// The states of the locks held by the state machine - these are tracked for error handling so that they are
    /// released if the state machine fails.
    lock_states: LockStates,
    /// The highest transaction ID that has been made durable in the WAL in a previous checkpoint.
    durable_txid_max_old: Option<NonZeroU64>,
    /// The highest transaction ID that will be made durable in the WAL in the current checkpoint.
    durable_txid_max_new: u64,
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
    /// Tables or indexes that were created in this checkpoint
    /// key is the rowid in the sqlite_schema table
    created_btrees: HashMap<i64, (MVTableId, RowVersion)>,
    /// Tables that were destroyed in this checkpoint
    destroyed_tables: HashSet<MVTableId>,
    /// Indexes that were destroyed in this checkpoint
    destroyed_indexes: HashSet<MVTableId>,
    /// Index row changes to write: (index_id, row_version, is_delete)
    index_write_set: Vec<(MVTableId, RowVersion, bool)>,
    /// Map from index_id to Index struct (for creating cursors)
    /// This is populated when we process sqlite_schema rows for indexes
    index_id_to_index: HashMap<MVTableId, Arc<Index>>,
    /// Result of the checkpoint
    checkpoint_result: Option<CheckpointResult>,
    /// Update connection's transaction state on checkpoint. If checkpoint was called as automatic
    /// process in a transaction we don't want to change the state as we assume we are already on a
    /// write transaction and any failure will be cleared on vdbe error handling.
    update_transaction_state: bool,
    /// The synchronous mode for fsync operations. When set to Off, fsync is skipped.
    sync_mode: SyncMode,
    /// Internal metadata table info for persisting `persistent_tx_ts_max` atomically with pager commit.
    mvcc_meta_table: Option<(MVTableId, usize)>,
    /// File-backed databases must persist replay boundary durably.
    durable_mvcc_metadata: bool,
    /// Header staged into pager page 1 before commit; published to global_header on success.
    staged_checkpoint_header: Option<DatabaseHeader>,
    /// Guard to avoid restaging page 1 across CommitPagerTxn async retries.
    header_staged_for_commit: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// Special writes for CREATE TABLE / DROP TABLE / CREATE INDEX / DROP INDEX ops.
/// These are used to create/destroy B-trees during pager ops.
pub enum SpecialWrite {
    BTreeCreate {
        table_id: MVTableId,
        sqlite_schema_rowid: i64,
    },
    BTreeDestroy {
        table_id: MVTableId,
        root_page: u64,
        num_columns: usize,
    },
    BTreeCreateIndex {
        index_id: MVTableId,
        sqlite_schema_rowid: i64,
    },
    BTreeDestroyIndex {
        index_id: MVTableId,
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
        sync_mode: SyncMode,
    ) -> Self {
        let checkpoint_lock = mvstore.blocking_checkpoint_lock.clone();
        // Prevent stale per-connection schema during checkpoint by using the shared DB schema.
        // Unlike in WAL mode we actually write stuff from mv store to pager in checkpoint
        // so this is important.
        let schema = connection.db.clone_schema();
        let index_id_to_index = schema
            .indexes
            .values()
            .flatten()
            .map(|index| {
                turso_assert!(index.root_page != 0, "index root_page must be non-zero");
                (
                    mvstore.get_table_id_from_root_page(index.root_page),
                    index.clone(),
                )
            })
            .collect();
        let mvcc_meta_table = schema.get_btree_table(MVCC_META_TABLE_NAME).map(|table| {
            turso_assert!(
                table.root_page != 0,
                "mvcc meta table root_page must be non-zero"
            );
            (
                mvstore.get_table_id_from_root_page(table.root_page),
                table.columns.len(),
            )
        });
        let durable_mvcc_metadata =
            !connection.db.path.starts_with(":memory:") && mvcc_meta_table.is_some();
        let durable_tx_max = mvstore.durable_txid_max.load(Ordering::SeqCst);
        let durable_txid_max_old = NonZeroU64::new(durable_tx_max);
        Self {
            state: CheckpointState::AcquireLock,
            lock_states: LockStates {
                blocking_checkpoint_lock_held: false,
                pager_read_tx: false,
                pager_write_tx: false,
            },
            pager,
            durable_txid_max_old,
            durable_txid_max_new: mvstore.durable_txid_max.load(Ordering::SeqCst),
            mvstore,
            connection,
            checkpoint_lock,
            write_set: Vec::new(),
            write_row_state_machine: None,
            delete_row_state_machine: None,
            cursors: HashMap::default(),
            created_btrees: HashMap::default(),
            destroyed_tables: HashSet::default(),
            destroyed_indexes: HashSet::default(),
            index_write_set: Vec::new(),
            index_id_to_index,
            checkpoint_result: None,
            update_transaction_state,
            sync_mode,
            mvcc_meta_table,
            durable_mvcc_metadata,
            staged_checkpoint_header: None,
            header_staged_for_commit: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn state_for_test(&self) -> CheckpointState {
        self.state
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_bounds_for_test(&self) -> (Option<u64>, u64) {
        (
            self.durable_txid_max_old.map(u64::from),
            self.durable_txid_max_new,
        )
    }

    /// Cleanup path for I/O errors that happen while waiting on completions outside
    /// of `step()`. This mirrors `step()` error handling and also resets pager/WAL
    /// checkpoint bookkeeping.
    pub fn cleanup_after_external_io_error(&mut self) {
        if self.lock_states.pager_write_tx {
            self.pager.rollback_tx(self.connection.as_ref());
            if self.update_transaction_state {
                self.connection.set_tx_state(TransactionState::None);
            }
            self.lock_states.pager_write_tx = false;
            self.lock_states.pager_read_tx = false;
        } else if self.lock_states.pager_read_tx {
            self.pager.end_read_tx();
            if self.update_transaction_state {
                self.connection.set_tx_state(TransactionState::None);
            }
            self.lock_states.pager_read_tx = false;
        }

        // MVCC checkpointing drives WAL checkpoint directly; on errors we must
        // explicitly reset both pager and WAL checkpoint states.
        self.pager.clear_checkpoint_state();
        if let Some(wal) = self.pager.wal.as_ref() {
            wal.abort_checkpoint();
        }

        // Release the checkpoint lock only after checkpoint state has been reset.
        if self.lock_states.blocking_checkpoint_lock_held {
            self.checkpoint_lock.unlock();
            self.lock_states.blocking_checkpoint_lock_held = false;
        }
    }

    /// Determine whether the newest valid version of a row should be checkpointed.
    /// Returns the version to checkpoint if it should be checkpointed, otherwise None.
    fn maybe_get_checkpointable_version(
        &self,
        versions: &[RowVersion],
        table_id: MVTableId,
    ) -> Option<RowVersion> {
        let mut version_to_checkpoint = None;
        let mut exists_in_db_file = false;
        // Iterate versions from oldest-to-newest to determine if the row exists in the database file and whether the newest version should be checkpointed.
        for version in versions.iter() {
            // Rows marked btree_resident existed in the DB file before MVCC tracked them.
            // This also applies to synthetic tombstones that use begin=None.
            if version.btree_resident {
                exists_in_db_file = true;
            }
            // A row is in the database file if:
            // There is a version whose begin timestamp is <= than the last checkpoint timestamp, AND
            // There is NO version whose END timestamp is <= than the last checkpoint timestamp.
            let mut begin_ts = None;
            if let Some(TxTimestampOrID::Timestamp(b)) = version.begin {
                begin_ts = Some(b);
                // A row exists in the DB file if it was checkpointed in a previous checkpoint.
                // For btree_resident rows we set exists_in_db_file above, regardless of begin encoding.
                if self
                    .durable_txid_max_old
                    .is_some_and(|txid_max_old| b <= u64::from(txid_max_old))
                {
                    exists_in_db_file = true;
                }
            }
            let mut end_ts = None;
            if let Some(TxTimestampOrID::Timestamp(e)) = version.end {
                end_ts = Some(e);
                if self
                    .durable_txid_max_old
                    .is_some_and(|txid_max_old| e <= u64::from(txid_max_old))
                {
                    exists_in_db_file = false;
                }
            }
            if begin_ts.is_none() && end_ts.is_none() {
                continue;
            }
            // Should checkpoint the newest version if:
            // - It is not a delete and it hasn't been checkpointed yet OR (begin_ts > max_old)
            // We need the `self.durable_txid_max_old.is_none()` check because before
            // the first checkpoint there is no persisted MVCC watermark.
            let is_uncheckpointed_insert = end_ts.is_none()
                && self.durable_txid_max_old.is_none_or(|txid_max_old| {
                    begin_ts.is_some_and(|b| b > u64::from(txid_max_old))
                });
            // - It is a delete, AND some version of the row exists in the database file.
            let is_delete_and_exists_in_db_file = end_ts.is_some() && exists_in_db_file;
            // - It is a delete of a sqlite_schema row that hasn't been checkpointed yet. We need to
            //   return these even if they don't exist in the DB file so we can track destroyed
            //   tables/indexes and skip their data rows.
            let is_schema_delete = table_id == SQLITE_SCHEMA_MVCC_TABLE_ID
                && !exists_in_db_file
                && self
                    .durable_txid_max_old
                    .is_none_or(|txid_max_old| end_ts.is_some_and(|e| e > u64::from(txid_max_old)));
            let should_checkpoint =
                is_uncheckpointed_insert || is_delete_and_exists_in_db_file || is_schema_delete;
            if should_checkpoint {
                version_to_checkpoint = Some(version.clone());
            }
        }
        version_to_checkpoint
    }

    /// Collect all committed versions that need to be written to the B-tree.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    ///    TODO: garbage collect row versions after checkpointing.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    ///      If the row didn't exist in the database file and was deleted, we can simply not write it.
    fn collect_committed_table_row_versions(&mut self) {
        // Invariant: RowID ordering is (table_id, row_id) with table_id ascending.
        // Since MV table IDs are negative and sqlite_schema is table_id=-1, iterating
        // in reverse visits sqlite_schema first so CREATE/DROP metadata is applied
        // before user-table rows in this checkpoint pass.
        for entry in self.mvstore.rows.iter().rev() {
            let key = entry.key();
            tracing::trace!("collecting {key:?}");
            if self.destroyed_tables.contains(&key.table_id) {
                // We won't checkpoint rows for tables that will be destroyed in this checkpoint.
                // There's two forms of destroyed table:
                // 1. A non-checkpointed table that was created in the logical log and then destroyed. We don't need to do anything about this table in the pager/btree layer.
                // 2. A checkpointed table that was destroyed in the logical log. We need to destroy the btree in the pager/btree layer.
                tracing::trace!("skipping {key:?}");
                continue;
            }

            let row_versions = entry.value().read();

            if let Some(version) =
                self.maybe_get_checkpointable_version(&row_versions, key.table_id)
            {
                let is_delete = version.end.is_some();

                let mut special_write = None;
                // Set to true for schema deletes of never-checkpointed tables/indexes.
                // These don't need to be written to the B-tree, we just need to track them.
                let mut skip_write = false;

                if version.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                    let row_data = ImmutableRecord::from_bin_record(version.row.payload().to_vec());

                    let (col0, col3) = row_data.get_two_values(0, 3).expect(
                        "failed to get columns 0 and 3 (type, rootpage) from sqlite_schema",
                    );

                    let ValueRef::Text(type_str) = col0 else {
                        panic!("sqlite_schema.type column must be TEXT, got {col0:?}");
                    };

                    if let ValueRef::Numeric(Numeric::Integer(root_page)) = col3 {
                        if type_str.as_str() == "index" {
                            // This is an index schema change
                            if is_delete {
                                // DROP INDEX
                                if root_page < 0 {
                                    // Index was never checkpointed - derive index_id directly from root_page.
                                    // No BTreeDestroyIndex needed since there's no physical B-tree.
                                    let index_id = MVTableId(root_page);
                                    self.destroyed_indexes.insert(index_id);
                                    skip_write = true;
                                } else {
                                    // DROP INDEX - index was checkpointed
                                    let index_id = self
                                        .mvstore
                                        .table_id_to_rootpage
                                        .iter()
                                        .find(|entry| {
                                            entry.value().is_some_and(|r| r == root_page as u64)
                                        })
                                        .map(|entry| *entry.key())
                                        .expect(
                                            "index_id to rootpage mapping should exist for dropped index",
                                        );

                                    self.destroyed_indexes.insert(index_id);

                                    // DROP INDEX during checkpoint: schema may no longer contain the index definition.
                                    // Fixes DROP INDEX during checkpoint when the schema cache no longer
                                    // contains the index metadata; we only need a cursor to destroy pages so num_columns is not important.
                                    let num_columns = self
                                        .index_id_to_index
                                        .get(&index_id)
                                        .map(|index| index.columns.len())
                                        .unwrap_or(0);

                                    special_write = Some(SpecialWrite::BTreeDestroyIndex {
                                        index_id,
                                        root_page: root_page as u64,
                                        num_columns,
                                    });
                                }
                            } else if root_page < 0 {
                                // CREATE INDEX (root page is negative so the index has not been checkpointed yet).
                                let index_id = MVTableId::from(root_page);
                                let sqlite_schema_rowid = version.row.id.row_id.to_int_or_panic();

                                special_write = Some(SpecialWrite::BTreeCreateIndex {
                                    index_id,
                                    sqlite_schema_rowid,
                                });
                            } else {
                                // Index schema row update (e.g. ALTER TABLE RENAME COLUMN propagates
                                // to index SQL). No B-tree creation needed; the row itself is written
                                // to sqlite_schema below. See: test_checkpoint_allows_index_schema_update_after_rename_column.
                            }
                        } else if type_str.as_str() == "table" {
                            // This is a table schema change (existing logic)
                            tracing::trace!("table schema change with root page {root_page}, is_delete={is_delete}");
                            if is_delete {
                                if root_page < 0 {
                                    // Table was never checkpointed - derive table_id directly from root_page.
                                    // No BTreeDestroy needed since there's no physical B-tree.
                                    let table_id = MVTableId::from(root_page);
                                    self.destroyed_tables.insert(table_id);
                                    skip_write = true;
                                } else {
                                    // Table was checkpointed - look up by physical root page
                                    let table_id = self
                                        .mvstore
                                        .table_id_to_rootpage
                                        .iter()
                                        .find(|entry| {
                                            entry.value().is_some_and(|r| r == root_page as u64)
                                        })
                                        .map(|entry| *entry.key())
                                        .expect("table_id to rootpage mapping should exist");
                                    self.destroyed_tables.insert(table_id);

                                    // Destroy the B-tree in the pager during checkpoint
                                    special_write = Some(SpecialWrite::BTreeDestroy {
                                        table_id,
                                        root_page: root_page as u64,
                                        num_columns: version.row.column_count,
                                    });
                                }
                            } else if root_page < 0 {
                                // CREATE TABLE (root page is negative so the table has not been checkpointed yet).
                                let table_id = MVTableId::from(root_page);
                                let sqlite_schema_rowid = version.row.id.row_id.to_int_or_panic();
                                special_write = Some(SpecialWrite::BTreeCreate {
                                    table_id,
                                    sqlite_schema_rowid,
                                });
                            } else {
                                // ALTER TABLE. No "special write is needed"; we'll just update the row in sqlite_schema.
                            }
                        }
                    }
                }
                if !skip_write {
                    tracing::trace!("adding to write_set {:?}", (&version, &special_write));
                    self.write_set.push((version, special_write));
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
                version.0.row.id.row_id.clone(),
            )
        });
    }

    /// Collect all committed index row versions that need to be written to the B-tree.
    /// Index rows are stored separately from table rows and must be checkpointed independently.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    fn collect_committed_index_row_versions(&mut self) {
        for entry in self.mvstore.index_rows.iter() {
            let index_id = *entry.key();

            // Skip destroyed indexes - we won't checkpoint rows for indexes that will be destroyed
            if self.destroyed_indexes.contains(&index_id) {
                continue;
            }

            let index_rows_map = entry.value();
            for entry in index_rows_map.iter() {
                let versions = entry.value().read();

                if let Some(version) = self.maybe_get_checkpointable_version(&versions, index_id) {
                    let is_delete = version.end.is_some();

                    // Only write the row to the B-tree if it is not a delete, or if it is a delete and it exists in
                    // the database file.
                    self.index_write_set.push((index_id, version, is_delete));
                }
            }
        }
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
        self.mvstore.storage.sync(self.pager.get_sync_type())
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
        match wal.checkpoint(
            &self.pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        )? {
            IOResult::Done(result) => Ok(IOResult::Done(result)),
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        }
    }

    /// Garbage-collect row versions for rows that were just checkpointed.
    /// Must be called AFTER durable_txid_max is updated and BEFORE the
    /// checkpoint lock is released (no concurrent writers under blocking lock).
    fn gc_checkpointed_versions(&self) {
        // Safety: entry removal after dropping the version-chain write lock has a
        // TOCTOU gap — a concurrent writer could insert between the two. This is
        // only safe because the blocking checkpoint lock prevents concurrent writers.
        // If we ever move to a non-blocking checkpoint, this must switch to lazy
        // removal (like background GC) or hold the write lock across the remove().
        assert!(
            self.lock_states.blocking_checkpoint_lock_held,
            "gc_checkpointed_versions requires the blocking checkpoint lock"
        );
        let lwm = self.mvstore.compute_lwm();
        let ckpt_max = self.durable_txid_max_new;

        for (row_version, _special_write) in &self.write_set {
            let row_id = &row_version.row.id;
            let Some(entry) = self.mvstore.rows.get(row_id) else {
                // The MVCC metadata table row (persistent_tx_ts_max) is staged
                // directly into the write set by maybe_stage_mvcc_metadata_write() and do not
                // have a backing in-memory MVCC version chain. Skip GC for these.
                assert!(
                    self.mvcc_meta_table
                        .is_some_and(|(tid, _)| tid == row_id.table_id),
                    "row {row_id:?} missing from MVCC store but is not an MVCC metadata table row"
                );
                continue;
            };
            let is_now_empty = {
                let mut versions = entry.value().write();
                MvStore::<Clock>::gc_version_chain(&mut versions, lwm, ckpt_max);
                versions.is_empty()
            };
            if is_now_empty {
                self.mvstore.rows.remove(row_id);
            }
        }

        for (index_id, row_version, _is_delete) in &self.index_write_set {
            let RowKey::Record(sortable_key) = &row_version.row.id.row_id else {
                unreachable!("index row versions always have Record keys");
            };
            let outer_entry = self
                .mvstore
                .index_rows
                .get(index_id)
                .expect("index_id from write set must exist in index_rows");
            let inner_map = outer_entry.value();
            let is_now_empty = {
                let inner_entry = inner_map
                    .get(sortable_key)
                    .expect("index row from write set must exist in inner map");
                let mut versions = inner_entry.value().write();
                MvStore::<Clock>::gc_version_chain(&mut versions, lwm, ckpt_max);
                versions.is_empty()
            };
            if is_now_empty {
                inner_map.remove(sortable_key);
            }
        }
    }

    /// Stages synthetic `persistent_tx_ts_max` row into the checkpoint write set
    /// so it is committed atomically with all other data in the same pager transaction.
    /// This is the mechanism that advances the durable replay boundary; on recovery, only
    /// logical-log frames with `commit_ts > persistent_tx_ts_max` are replayed.
    /// No-op when metadata hasn't advanced or when running in-memory (no durable metadata).
    fn maybe_stage_mvcc_metadata_write(&mut self) -> Result<()> {
        if !self.durable_mvcc_metadata {
            return Ok(());
        }
        let old = self.durable_txid_max_old.map(u64::from).unwrap_or_default();
        let new = self.durable_txid_max_new;
        if new <= old {
            return Ok(());
        }

        let (table_id, num_columns) = self.mvcc_meta_table.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "Missing required internal metadata table {MVCC_META_TABLE_NAME}"
            ))
        })?;
        let new_i64 = i64::try_from(new).map_err(|_| {
            LimboError::Corrupt(format!("MVCC checkpoint timestamp does not fit i64: {new}"))
        })?;
        let record = ImmutableRecord::from_values(
            &[
                Value::build_text(MVCC_META_KEY_PERSISTENT_TX_TS_MAX),
                Value::from_i64(new_i64),
            ],
            2,
        );
        let row = Row::new_table_row(
            RowID::new(table_id, RowKey::Int(1)),
            record.get_payload().to_vec(),
            num_columns,
        );
        self.write_set.push((
            RowVersion {
                id: 0,
                begin: Some(TxTimestampOrID::Timestamp(new)),
                end: None,
                row,
                btree_resident: true,
            },
            None,
        ));
        Ok(())
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

                self.collect_committed_table_row_versions();
                tracing::debug!("Collected {} committed versions", self.write_set.len());

                self.collect_committed_index_row_versions();
                tracing::debug!("Collected {} index row changes", self.index_write_set.len());
                // Checkpoint boundary is derived from a stable snapshot under the blocking lock:
                // old durable boundary plus the latest committed tx watermark. This covers both
                // row/index commits and header-only commits.
                let durable_old = self.durable_txid_max_old.map(u64::from).unwrap_or_default();
                let committed_max = self.mvstore.last_committed_tx_ts.load(Ordering::Acquire);
                self.durable_txid_max_new = durable_old.max(committed_max);
                self.maybe_stage_mvcc_metadata_write()?;

                if self.write_set.is_empty() && self.index_write_set.is_empty() {
                    // Nothing to checkpoint, skip pager txn and go straight to WAL checkpoint.
                    self.state = CheckpointState::CheckpointWal;
                } else {
                    self.state = CheckpointState::BeginPagerTxn;
                }
                Ok(TransitionResult::Continue)
            }
            CheckpointState::BeginPagerTxn => {
                tracing::debug!("Beginning pager transaction");
                // Start a pager transaction to write committed versions to B-tree
                let read_tx_active = self
                    .pager
                    .wal
                    .as_ref()
                    .is_some_and(|wal| wal.holds_read_lock());
                if !read_tx_active {
                    self.pager.begin_read_tx()?;
                    self.lock_states.pager_read_tx = true;
                }

                self.pager.io.block(|| self.pager.begin_write_tx())?;
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
                    // Done writing all table rows, now process index rows
                    if self.index_write_set.is_empty() {
                        // No index rows to write, skip to commit
                        self.state = CheckpointState::CommitPagerTxn;
                    } else {
                        // Start writing index rows
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: 0,
                            requires_seek: true,
                        };
                    }
                    return Ok(TransitionResult::Continue);
                }

                let (num_columns, table_id, special_write) = {
                    let (row_version, special_write) = self
                        .get_current_row_version(write_set_index)
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "row version not found in write set".to_string(),
                            )
                        })?;
                    tracing::trace!("checkpointing row {row_version:?} ");
                    (
                        row_version.row.column_count,
                        row_version.row.id.table_id,
                        *special_write,
                    )
                };

                // Handle CREATE TABLE / DROP TABLE / CREATE INDEX / DROP INDEX ops
                if let Some(special_write) = special_write {
                    match special_write {
                        SpecialWrite::BTreeCreate { table_id, .. } => {
                            let created_root_page: u32 = self.pager.io.block(|| {
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
                            turso_assert_eq!(
                                known_root_page,
                                root_page,
                                "checkpoint root page mismatch for BTreeDestroy",
                                { "known_root_page": known_root_page, "schema_root_page": root_page }
                            );
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
                        SpecialWrite::BTreeCreateIndex { index_id, .. } => {
                            let created_root_page: u32 = self.pager.io.block(|| {
                                self.pager.btree_create(&CreateBTreeFlags::new_index())
                            })?;
                            self.mvstore.insert_table_id_to_rootpage(
                                index_id,
                                Some(created_root_page as u64),
                            );
                            // Index struct should already be stored in index_id_to_index from collect_committed_versions
                            turso_assert!(
                                self.index_id_to_index.contains_key(&index_id),
                                "checkpoint index struct missing before BTreeCreateIndex",
                                { "index_id": i64::from(index_id) }
                            );
                        }
                        SpecialWrite::BTreeDestroyIndex {
                            index_id,
                            root_page,
                            num_columns,
                        } => {
                            let known_root_page = self
                                .mvstore
                                .table_id_to_rootpage
                                .get(&index_id)
                                .expect("Index ID does not have a root page");
                            let known_root_page = known_root_page
                                .value()
                                .expect("Index ID does not have a root page");
                            turso_assert_eq!(
                                known_root_page,
                                root_page,
                                "checkpoint root page mismatch for BTreeDestroyIndex",
                                { "known_root_page": known_root_page, "schema_root_page": root_page }
                            );

                            let cursor = if let Some(cursor) = self.cursors.get(&known_root_page) {
                                cursor.clone()
                            } else if let Some(index) = self.index_id_to_index.get(&index_id) {
                                let cursor = BTreeCursor::new_index(
                                    self.pager.clone(),
                                    known_root_page as i64,
                                    index.as_ref(),
                                    num_columns,
                                );
                                let cursor = Arc::new(RwLock::new(cursor));
                                self.cursors.insert(root_page, cursor.clone());
                                cursor
                            } else {
                                // DROP INDEX destroy path: schema may no longer contain the index definition.
                                // We only need a cursor to destroy pages so num_columns is not important.
                                Arc::new(RwLock::new(BTreeCursor::new_table(
                                    self.pager.clone(),
                                    known_root_page as i64,
                                    num_columns,
                                )))
                            };
                            self.pager.io.block(|| cursor.write().btree_destroy())?;
                            self.destroyed_indexes.insert(index_id);
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
                                self.get_current_row_version(write_set_index)
                                    .expect("row version should exist")
                            )
                        });
                    root_page.value().unwrap_or_else(|| {
                        panic!(
                            "Table ID does not have a root page: {table_id}, row_version: {:?}",
                            self.get_current_row_version(write_set_index)
                                .expect("row version should exist")
                        )
                    })
                };

                // If a table was created, it now has a real root page allocated for it, but the 'root_page' field in the sqlite_schema record is still the table id.
                // So we need to rewrite the row version to use the real root page.
                if let Some(SpecialWrite::BTreeCreate {
                    table_id,
                    sqlite_schema_rowid,
                }) = special_write
                {
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
                    let row_version = {
                        let (row_version, _) = self
                            .get_current_row_version_mut(write_set_index)
                            .ok_or_else(|| {
                                LimboError::InternalError(
                                    "row version not found in write set".to_string(),
                                )
                            })?;
                        let record =
                            ImmutableRecord::from_bin_record(row_version.row.payload().to_vec());

                        let mut values = record.get_values_owned()?;
                        values[3] = Value::from_i64(root_page as i64);
                        let record = ImmutableRecord::from_values(&values, values.len());
                        row_version.row.data = Some(record.get_payload().to_owned());
                        row_version.clone()
                    };
                    self.created_btrees
                        .insert(sqlite_schema_rowid, (table_id, row_version));
                } else if let Some(SpecialWrite::BTreeCreateIndex {
                    index_id,
                    sqlite_schema_rowid,
                }) = special_write
                {
                    // Same for index btrees.
                    let root_page = {
                        let root_page = self
                            .mvstore
                            .table_id_to_rootpage
                            .get(&index_id)
                            .expect("Index ID does not have a root page");
                        root_page
                            .value()
                            .expect("Index ID does not have a root page")
                    };
                    let row_version = {
                        let (row_version, _) = self
                            .get_current_row_version_mut(write_set_index)
                            .ok_or_else(|| {
                                LimboError::InternalError(
                                    "row version not found in write set".to_string(),
                                )
                            })?;
                        let record =
                            ImmutableRecord::from_bin_record(row_version.row.payload().to_vec());
                        let mut values = record.get_values_owned()?;
                        values[3] = Value::from_i64(root_page as i64);
                        let record = ImmutableRecord::from_values(&values, values.len());
                        row_version.row.data = Some(record.get_payload().to_owned());
                        row_version.clone()
                    };

                    self.created_btrees
                        .insert(sqlite_schema_rowid, (index_id, row_version));
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

                let (row_version, _) =
                    self.get_current_row_version(write_set_index)
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "row version not found in write set".to_string(),
                            )
                        })?;

                // Check if this is an insert or delete
                if row_version.end.is_some() {
                    // This is a delete operation.
                    // Don't write the deletion record to the b-tree if the b-tree was just created; we can no-op in this case,
                    // since there is no existing row to delete.
                    if self
                        .created_btrees
                        .values()
                        .any(|(table_id, _)| *table_id == row_version.row.id.table_id)
                    {
                        self.state = CheckpointState::WriteRow {
                            write_set_index: write_set_index + 1,
                            requires_seek: true,
                        };
                        return Ok(TransitionResult::Continue);
                    }
                    let state_machine = self
                        .mvstore
                        .delete_row_from_pager(row_version.row.id.clone(), cursor)?;
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
                let write_row_state_machine =
                    self.write_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "write_row_state_machine not initialized".to_string(),
                        )
                    })?;

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
                let delete_row_state_machine =
                    self.delete_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "delete_row_state_machine not initialized".to_string(),
                        )
                    })?;

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

            CheckpointState::WriteIndexRow {
                index_write_set_index,
                requires_seek,
            } => {
                let index_write_set_index = *index_write_set_index;
                let requires_seek = *requires_seek;

                if index_write_set_index >= self.index_write_set.len() {
                    // Done writing all index rows
                    self.state = CheckpointState::CommitPagerTxn;
                    return Ok(TransitionResult::Continue);
                }

                let (index_id, row_version, is_delete) =
                    &self.index_write_set[index_write_set_index];

                // Skip destroyed indexes
                if self.destroyed_indexes.contains(index_id) {
                    self.state = CheckpointState::WriteIndexRow {
                        index_write_set_index: index_write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                }

                // Get Index struct - it should exist for all indexes we're checkpointing
                let index = self.index_id_to_index.get(index_id).unwrap_or_else(|| {
                    panic!(
                    "Index struct for index_id {index_id} must exist when checkpointing index rows",
                )
                });

                // Get root page for this index
                let root_page = self
                    .mvstore
                    .table_id_to_rootpage
                    .get(index_id)
                    .unwrap_or_else(|| panic!("Index ID {index_id} does not have a root page"));
                let root_page = root_page
                    .value()
                    .unwrap_or_else(|| panic!("Index ID {index_id} does not have a root page"));

                // Get or create cursor for this index
                let cursor = if let Some(cursor) = self.cursors.get(&root_page) {
                    cursor.clone()
                } else {
                    let cursor = BTreeCursor::new_index(
                        self.pager.clone(),
                        root_page as i64,
                        index.as_ref(),
                        index.columns.len(),
                    );
                    let cursor = Arc::new(RwLock::new(cursor));
                    self.cursors.insert(root_page, cursor.clone());
                    cursor
                };

                // Check if this is an insert or delete
                if *is_delete {
                    // This is a delete operation. Don't write the deletion record to the b-tree if the b-tree was just created; we can no-op in this case,
                    // since there is no existing row to delete.
                    if self
                        .created_btrees
                        .values()
                        .any(|(table_id, _)| *table_id == row_version.row.id.table_id)
                    {
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: index_write_set_index + 1,
                            requires_seek: true,
                        };
                        return Ok(TransitionResult::Continue);
                    }
                    let state_machine = self
                        .mvstore
                        .delete_row_from_pager(row_version.row.id.clone(), cursor)?;
                    self.delete_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::DeleteIndexRowStateMachine {
                        index_write_set_index,
                    };
                } else {
                    // This is an insert/update operation
                    let state_machine =
                        self.mvstore
                            .write_row_to_pager(&row_version.row, cursor, requires_seek)?;
                    self.write_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::WriteIndexRowStateMachine {
                        index_write_set_index,
                    };
                }

                Ok(TransitionResult::Continue)
            }

            CheckpointState::WriteIndexRowStateMachine {
                index_write_set_index,
            } => {
                let index_write_set_index = *index_write_set_index;
                let write_row_state_machine =
                    self.write_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "write_row_state_machine not initialized".to_string(),
                        )
                    })?;

                match write_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: index_write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::DeleteIndexRowStateMachine {
                index_write_set_index,
            } => {
                let index_write_set_index = *index_write_set_index;
                let delete_row_state_machine =
                    self.delete_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "delete_row_state_machine not initialized".to_string(),
                        )
                    })?;

                match delete_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: index_write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::CommitPagerTxn => {
                if !self.header_staged_for_commit {
                    let mut checkpoint_header =
                        *self.mvstore.global_header.read().as_ref().ok_or_else(|| {
                            LimboError::InternalError(
                                "global_header not initialized during checkpoint".to_string(),
                            )
                        })?;
                    checkpoint_header.schema_cookie =
                        self.connection.db.schema.lock().schema_version.into();
                    let staged_header = self.pager.io.block(|| {
                        self.pager.with_header_mut(|header| {
                            // Keep pager-maintained fields (for example database_size/change_counter)
                            // intact, and apply only MVCC header mutations that are authored via
                            // SetCookie/PRAGMA paths.
                            header.schema_cookie = checkpoint_header.schema_cookie;
                            header.user_version = checkpoint_header.user_version;
                            header.application_id = checkpoint_header.application_id;
                            header.vacuum_mode_largest_root_page =
                                checkpoint_header.vacuum_mode_largest_root_page;
                            header.incremental_vacuum_enabled =
                                checkpoint_header.incremental_vacuum_enabled;
                            *header
                        })
                    })?;
                    self.staged_checkpoint_header = Some(staged_header);
                    self.header_staged_for_commit = true;
                }
                // If commit_tx fails, the `?` propagates to step() which rolls back
                // the pager transaction. durable_txid_max is NOT advanced (only happens
                // on success below), so a retry will re-stage from the previous boundary.
                // The logical log is also unaffected — its offset is not advanced here.
                tracing::debug!("Committing pager transaction");
                let result = self
                    .pager
                    .commit_tx(&self.connection, self.update_transaction_state)?;
                match result {
                    IOResult::Done(_) => {
                        // Pager commit atomically staged data + metadata into WAL.
                        // Advance the in-memory durable boundary immediately so that if
                        // later checkpoint phases fail, a same-process retry starts from
                        // this durable prefix instead of re-staging older versions.
                        self.mvstore
                            .durable_txid_max
                            .store(self.durable_txid_max_new, Ordering::SeqCst);
                        self.state = CheckpointState::CheckpointWal;
                        self.lock_states.pager_read_tx = false;
                        self.lock_states.pager_write_tx = false;
                        // Publish the exact page-1 snapshot that was staged into the pager txn.
                        let header = self.staged_checkpoint_header.take().ok_or_else(|| {
                            LimboError::InternalError(
                                "checkpoint header was not staged before pager commit".to_string(),
                            )
                        })?;
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
                // Skip fsync when synchronous mode is off
                if self.sync_mode == SyncMode::Off {
                    tracing::debug!("Skipping fsync of logical log file (synchronous=off)");
                    self.state = CheckpointState::TruncateWal;
                    return Ok(TransitionResult::Continue);
                }
                tracing::debug!("Fsyncing logical log file");
                let c = self.fsync_logical_log()?;
                self.state = CheckpointState::TruncateWal;
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
                        self.state = CheckpointState::SyncDbFile;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                }
            }

            CheckpointState::SyncDbFile => {
                // Fsync database file before truncating WAL.
                // This ensures durability: if we crash after WAL truncation but before DB fsync,
                // the checkpointed data would be lost.
                if self.sync_mode == SyncMode::Off {
                    tracing::debug!("Skipping fsync of database file (synchronous=off)");
                    self.state = CheckpointState::TruncateLogicalLog;
                    return Ok(TransitionResult::Continue);
                }

                let checkpoint_result = self
                    .checkpoint_result
                    .as_mut()
                    .expect("checkpoint_result should be set");

                // Only sync if we actually backfilled any frames
                if checkpoint_result.wal_checkpoint_backfilled == 0 {
                    self.state = CheckpointState::TruncateLogicalLog;
                    return Ok(TransitionResult::Continue);
                }

                // Check if we already sent the sync
                if checkpoint_result.db_sync_sent {
                    self.state = CheckpointState::TruncateLogicalLog;
                    return Ok(TransitionResult::Continue);
                }

                tracing::debug!("Fsyncing database file before WAL truncation");
                let c = self
                    .pager
                    .db_file
                    .sync(Completion::new_sync(|_| {}), self.pager.get_sync_type())?;
                checkpoint_result.db_sync_sent = true;
                Ok(TransitionResult::Io(IOCompletions::Single(c)))
            }

            CheckpointState::TruncateWal => {
                // Truncate WAL file after DB file is safely synced.
                // This must be done explicitly because MVCC calls wal.checkpoint() directly,
                // bypassing the pager's TruncateWalFile phase.
                let Some(wal) = &self.pager.wal else {
                    panic!("No WAL to truncate");
                };
                let checkpoint_result = self
                    .checkpoint_result
                    .as_mut()
                    .expect("checkpoint_result should be set");
                match wal.truncate_wal(checkpoint_result, self.pager.get_sync_type())? {
                    IOResult::Done(()) => {
                        self.state = CheckpointState::Finalize;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                }
            }

            CheckpointState::Finalize => {
                tracing::debug!("Releasing blocking checkpoint lock");
                // Patch sqlite_schema in MV Store to contain positive rootpages instead of negative ones
                // for tables and indexes that were flushed to the physical database
                for (sqlite_schema_rowid, (_, row_version)) in self.created_btrees.drain() {
                    let key = RowID {
                        table_id: SQLITE_SCHEMA_MVCC_TABLE_ID,
                        row_id: RowKey::Int(sqlite_schema_rowid),
                    };
                    let sqlite_schema_row = self
                        .mvstore
                        .rows
                        .get(&key)
                        .expect("sqlite_schema row not found");
                    let mut row_versions = sqlite_schema_row.value().write();
                    // row_version is a clone of the original with only the root
                    // page column patched, so it shares the same version id. We
                    // must replace the original in-place rather than append,
                    // otherwise the version chain ends up with two entries that
                    // have identical (id, begin, end). A later DELETE only marks
                    // one of them as ended (it returns after the first match),
                    // leaving the other as a phantom current version that causes
                    // spurious write-write conflicts at commit time.
                    let vid = row_version.id;
                    if let Some(existing) = row_versions.iter_mut().find(|rv| rv.id == vid) {
                        *existing = row_version;
                    } else {
                        self.mvstore
                            .insert_version_raw(&mut row_versions, row_version);
                    }
                }

                // Patch in-memory schema to do the same
                self.connection.db.with_schema_mut(|schema| {
                    for table in schema.tables.values_mut() {
                        let table = Arc::get_mut(table).expect("this should be the only reference");
                        let Some(btree_table) = table.btree_mut() else {
                            continue;
                        };
                        let btree_table = Arc::make_mut(btree_table);
                        if btree_table.root_page < 0 {
                            let table_id = MVTableId::from(btree_table.root_page);
                            let entry = self.mvstore.table_id_to_rootpage.get(&table_id).expect(
                                "we should have checkpointed table with table_id {table_id:?}",
                            );
                            let value = entry
                                .value()
                                .expect("table with id {table_id:?} should have a mapping");
                            btree_table.root_page = value as i64;
                        }
                    }
                    for table_index_list in schema.indexes.values_mut() {
                        for index in table_index_list.iter_mut() {
                            if index.root_page < 0 {
                                let table_id = MVTableId::from(index.root_page);
                                let entry = self
                                    .mvstore
                                    .table_id_to_rootpage
                                    .get(&table_id)
                                    .expect(
                                    "we should have checkpointed index with table_id {table_id:?}",
                                );
                                let value = entry
                                    .value()
                                    .expect("index with id {table_id:?} should have a mapping");
                                let index = Arc::make_mut(index);
                                index.root_page = value as i64;
                            }
                        }
                    }

                    schema.schema_version += 1;
                    // Clear dropped root pages now that the checkpoint has completed.
                    // The btree pages for dropped tables have been freed, so integrity_check
                    // no longer needs to track them.
                    schema.dropped_root_pages.clear();
                    let _ = self.pager.io.block(|| {
                        self.pager.with_header_mut(|header| {
                            header.schema_cookie = schema.schema_version.into();
                            self.mvstore.global_header.write().replace(*header);
                            IOResult::Done(())
                        })
                    })?;
                    Ok(())
                })?;

                self.mvstore
                    .durable_txid_max
                    .store(self.durable_txid_max_new, Ordering::SeqCst);
                self.gc_checkpointed_versions();
                self.mvstore.drop_unused_row_versions();
                self.checkpoint_lock.unlock();
                self.finalize(&())?;
                Ok(TransitionResult::Done(
                    self.checkpoint_result.take().ok_or_else(|| {
                        LimboError::InternalError("checkpoint_result not set".to_string())
                    })?,
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
                tracing::debug!("Error in checkpoint state machine: {err}");
                self.cleanup_after_external_io_error();
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
