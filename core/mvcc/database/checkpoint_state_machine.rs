use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    DeleteRowStateMachine, MVTableId, MvStore, Row, RowID, RowKey, RowVersion, SortableIndexKey,
    TxTimestampOrID, WriteRowStateMachine, MVCC_META_KEY_PERSISTENT_TX_TS_MAX,
    MVCC_META_TABLE_NAME, SQLITE_SCHEMA_MVCC_TABLE_ID,
};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::{inject_transition_failure, inject_transition_yield};
use crate::schema::Index;
use crate::state_machine::{StateMachine, StateTransition, TransitionResult};
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::storage::pager::CreateBTreeFlags;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::{CheckpointMode, TursoRwLock, WalAutoActions};
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::types::{IOCompletions, IOResult, ImmutableRecord, ImmutableRecordRef};
use crate::{turso_assert, turso_assert_eq};
use crate::{
    CheckpointResult, Completion, Connection, IOExt, LimboError, Numeric, Pager, Result, SyncMode,
    TransactionState, Value, ValueRef,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::num::NonZeroU64;
use std::ops::Bound;
#[cfg(any(test, injected_yields))]
use strum::EnumCount;

const COLLECT_PREEMPTION_THRESHOLD: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
    /// Unlocked prelude: claim `MvStore::checkpoint_in_progress` (the
    /// single-orchestrator gate, which the off-lock design makes explicit since
    /// `blocking_checkpoint_lock` is no longer the first thing taken), capture a
    /// `snapshot_ts` upper bound, and `refresh_checkpoint_bounds`. Collection then
    /// runs through the preemptible `CollectTableRows`/`CollectIndexRows` states.
    /// No `blocking_checkpoint_lock` is held.
    PrepareCheckpoint,
    /// Preemptible collection of committed table-row versions into `write_set`,
    /// yielding every `COLLECT_PREEMPTION_THRESHOLD` rows (resumed via
    /// `collect_table_cursor`). Runs off-lock; `maybe_get_checkpointable_versions`
    /// filters by `snapshot_ts`, so concurrent commits are deferred to the next pass.
    CollectTableRows,
    /// Preemptible collection of committed index-row versions (resumed via
    /// `collect_index_tableid_cursor` / `collect_index_key_cursor`). Off-lock, same
    /// `snapshot_ts` filtering as `CollectTableRows`.
    CollectIndexRows,
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
    /// Take `blocking_checkpoint_lock.write()` after the unlocked pager work has
    /// committed (WAL frames are durable). Held for the remainder of the state
    /// machine — through CheckpointWal / SyncDbFile / TruncateLog / Truncate WAL /
    /// GC / marker publication / Finalize.
    TakeLock,
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

/// Every resumable boundary in the checkpoint pipeline. The variants are in
/// execution order. Tests enumerate them (via `strum::IntoEnumIterator`) to
/// deterministically pause / fail at *every* point and assert that committed
/// data stays correct and complete — so correctness never depends on lucky
/// timing.
///
/// "(unlocked)" points run before `blocking_checkpoint_lock` is held, so a
/// concurrent reader can run there. "(locked)" points run with the lock held,
/// so a concurrent reader's `BEGIN` returns Busy — for those, the data
/// assertion is made once the checkpoint releases the lock.
#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount, strum_macros::EnumIter)]
#[repr(u8)]
pub(crate) enum CheckpointYieldPoint {
    /// (unlocked) PrepareCheckpoint done: gate claimed, write_set + index
    /// write_set collected, durable_txid_max_new computed, mvcc_meta staged.
    /// Nothing written to the pager yet.
    AfterPrepare,
    /// (unlocked) BeginPagerTxn done: pager read+write tx open, zero rows
    /// written to the B-tree.
    AfterBeginPagerTxn,
    /// (unlocked) Exactly one table row has been written into the pager cache
    /// (partial B-tree write) — exercises reads while the B-tree is half-built.
    AfterFirstTableRow,
    /// (unlocked) All table rows written into the pager cache.
    AfterTableRows,
    /// (unlocked) All index rows written into the pager cache.
    AfterIndexRows,
    /// (unlocked) CommitPagerTxn done: page-1 header staged in the dirty
    /// cache. No WAL commit frame yet.
    AfterStageHeader,
    /// (unlocked) TakeLock entry, before acquiring `blocking_checkpoint_lock`.
    BeforeAcquireLock,
    /// (locked) Lock acquired, before the pager commit. Nothing visible yet.
    AfterAcquireLock,
    /// (locked) pager.commit_tx done: WAL commit frame written, write tx
    /// ended. In-memory markers NOT yet published.
    AfterPagerCommit,
    /// (locked) Markers published: durable_txid_max bumped, global_header
    /// replaced, schema roots published, root_page allocations drained.
    /// GC has not run yet.
    AfterDurableBoundaryAdvanced,
    /// (locked) WAL frames backfilled into the DB file.
    AfterCheckpointWal,
    /// (locked) DB file fsynced.
    AfterSyncDbFile,
    /// (locked) Logical log truncated to 0.
    AfterTruncateLogicalLog,
    /// (locked) Logical log fsynced.
    AfterFsyncLogicalLog,
    /// (locked) WAL truncated. About to GC + unlock in Finalize.
    AfterTruncateWal,
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for CheckpointYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
fn checkpoint_yield_key() -> u64 {
    const CHECKPOINT_SELECTION_TAG: u64 = 0xC4EC_9011_C4EC_9011;
    CHECKPOINT_SELECTION_TAG
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
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
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
    /// The WAL checkpoint mode this run should apply when it reaches
    /// `CheckpointWal`/`TruncateWal`. Auto-checkpoints (post-commit) use
    /// `Passive` so the WAL→DB backfill runs off the lock without fighting
    /// concurrent readers/writers; explicit `PRAGMA wal_checkpoint(TRUNCATE)`,
    /// journal-mode migrations, and `Connection::checkpoint(mode)` pass the
    /// requested mode so a user-requested TRUNCATE actually empties the WAL.
    /// `should_restart_log()` (Truncate/Restart) gates the explicit WAL file
    /// truncation in `TruncateWal`; under `Passive` that step is a pass-through.
    mode: CheckpointMode,
    /// Internal metadata table info for persisting `persistent_tx_ts_max` atomically with pager commit.
    mvcc_meta_table: Option<(MVTableId, usize)>,
    /// File-backed databases must persist replay boundary durably.
    durable_mvcc_metadata: bool,
    /// Header staged into pager page 1 before commit; published to global_header on success.
    staged_checkpoint_header: Option<DatabaseHeader>,
    /// Guard to avoid restaging page 1 across CommitPagerTxn async retries.
    header_staged_for_commit: bool,
    /// Preemptible-collection resume cursors: the last (table, index) positions
    /// processed before a `CollectTableRows`/`CollectIndexRows` yield.
    collect_table_cursor: Option<RowID>,
    collect_index_tableid_cursor: Option<MVTableId>,
    collect_index_key_cursor: Option<Arc<SortableIndexKey>>,
    /// Snapshot of `MvStore::last_committed_tx_ts` captured at the start of the
    /// unlocked PrepareCheckpoint state. Used as the upper-bound filter in
    /// `maybe_get_checkpointable_versions` so concurrent commits with a higher
    /// `begin_ts` arriving during the unlocked collection/write phase aren't
    /// partially picked up — they get picked up by the next checkpoint pass.
    /// Also drives `durable_txid_max_new`, so the marker can never claim a
    /// version durable that we did not actually flush.
    snapshot_ts: u64,
    /// `true` iff this state machine successfully claimed
    /// `MvStore::checkpoint_in_progress`. Required so that the matching
    /// `store(false)` happens exactly when this state machine releases the
    /// gate — in Finalize on success, in cleanup_after_external_io_error on
    /// error, and in Drop as a safety net.
    owns_checkpoint_in_progress: bool,
    /// `true` for the "flush half" of a split checkpoint. When set, after
    /// TakeLock publishes the durable markers + GCs MvStore, the state machine
    /// releases the lock and jumps straight to Finalize without running the
    /// WAL → DB → log-truncate → WAL-truncate sequence. The next regular
    /// checkpoint() (which finds an empty write_set) does the backfill half.
    /// Production paths leave this `false`; the test path uses `flush_to_wal`
    /// to exercise the in-between state where WAL has frames, DB doesn't, and
    /// MvStore has GC'd — so reads MUST come through the pager/WAL path.
    stop_after_flush: bool,
    /// Set in TakeLock when `stop_after_flush` ran the gc + unlock prelude, so
    /// Finalize knows to skip the redundant work and just clean up the gate.
    flush_completed: bool,
    /// Root pages allocated during this checkpoint's WriteRow phase (the
    /// SpecialWrite::BTreeCreate / BTreeCreateIndex branches). These cannot
    /// be published into `MvStore::table_id_to_rootpage` from the unlocked
    /// write phase, because `maybe_transform_root_page_to_positive` reads
    /// that map to translate a connection's still-negative cached root_page
    /// to its real one — a concurrent INSERT would then open a cursor on
    /// the new positive root_page and call `pager.read_page` for a frame
    /// that hasn't yet been published into its own snapshot's max_frame,
    /// fall back to the DB file (where the page does not exist), and hit
    /// ShortRead. The map is drained into MvStore under
    /// `blocking_checkpoint_lock.write()` in TakeLock, alongside the other
    /// marker publications.
    pending_root_page_allocations: HashMap<MVTableId, u64>,
    /// Re-entrancy guard for TakeLock: set once pager.commit_tx has completed
    /// (WAL commit frame written). Lets a yield between commit and marker
    /// publication re-enter TakeLock without re-committing.
    lock_commit_done: bool,
    /// Re-entrancy guard for TakeLock: set once the in-memory markers have
    /// been published. Lets a yield after publication re-enter without
    /// double-publishing (which would, e.g., fail on the already-taken
    /// staged header).
    markers_published: bool,
    /// Re-entrancy guard for TakeLock: set once the targeted GC has run and
    /// the blocking_checkpoint_lock has been released. Everything after this
    /// point (the Passive WAL->DB backfill, DB fsync, logical-log + WAL
    /// truncation) runs unlocked, so readers and writers are no longer
    /// blocked by the checkpoint.
    gc_and_unlock_done: bool,
}

#[cfg(any(test, injected_yields))]
impl<Clock: LogicalClock> ProvidesYieldContext for CheckpointStateMachine<Clock> {
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.connection.yield_injector(),
            self.connection.failure_injector(),
            self.yield_instance_id,
            checkpoint_yield_key(),
        )
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteSchemaBtreeKind {
    Table,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteSchemaBtreeIdentity {
    kind: SqliteSchemaBtreeKind,
    pub root_page: i64,
}

/// Identity of a sqlite_schema row version that refers to a B-tree-backed object.
/// Schema rewrites that preserve this identity are metadata-only and should not be
/// treated as create/drop lifecycle changes.
pub fn sqlite_schema_btree_identity(version: &RowVersion) -> Option<SqliteSchemaBtreeIdentity> {
    if version.row.id.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
        return None;
    }

    // Recovery can synthesize payload-less sqlite_schema tombstones when the
    // pre-delete record is no longer available. Those versions do not carry
    // enough information to recover B-tree identity.
    if version.row.payload().is_empty() {
        return None;
    }

    let row_data = ImmutableRecordRef::from_bin_record(version.row.payload());
    let Ok((col0, col3)) = row_data.get_two_values(0, 3) else {
        return None;
    };

    let kind = match col0 {
        ValueRef::Text(type_str) => match type_str.as_str() {
            "table" => SqliteSchemaBtreeKind::Table,
            "index" => SqliteSchemaBtreeKind::Index,
            _ => return None,
        },
        _ => panic!("sqlite_schema.type column must be TEXT, got {col0:?}"),
    };

    let ValueRef::Numeric(Numeric::Integer(root_page)) = col3 else {
        panic!("sqlite_schema.rootpage column must be INTEGER, got {col3:?}");
    };

    if root_page == 0 {
        return None;
    }

    Some(SqliteSchemaBtreeIdentity { kind, root_page })
}

fn sqlite_schema_versions_refer_to_btree(lhs: &RowVersion, rhs: &RowVersion) -> bool {
    sqlite_schema_btree_identity(lhs)
        .zip(sqlite_schema_btree_identity(rhs))
        .is_some_and(|(lhs_id, rhs_id)| lhs_id == rhs_id)
}

/// A single `sqlite_schema` rowid can be reused across multiple row versions. Some of those
/// transitions are metadata-only rewrites of the same B-tree object, while others represent a
/// real change that mutates the BTREE.
///
/// Checkpoint needs to preserve ended schema versions only for the types of changes so it can
/// register destroyed tables/indexes and skip stale recovered rows. Same-object rewrites, such as
/// `ALTER TABLE ... RENAME COLUMN`, must collapse to the latest version; otherwise checkpoint
/// treats one schema row chain as a DROP+CREATE pair and emits duplicate work for the same rowid.
fn is_schema_metadata_only_rewrite(current: &RowVersion, next: Option<&RowVersion>) -> bool {
    if current.end.is_none() {
        return false;
    }

    let Some(_current_identity) = sqlite_schema_btree_identity(current) else {
        return false;
    };

    match next {
        Some(next) => !sqlite_schema_versions_refer_to_btree(current, next),
        None => true,
    }
}

impl<Clock: LogicalClock> CheckpointStateMachine<Clock> {
    /// Resolve a table_id (or index_id) to its root_page, checking the
    /// state-machine-local pending allocations first and falling back to
    /// the published `MvStore::table_id_to_rootpage`. Used by WriteRow
    /// and WriteIndexRow so the unlocked write phase can use the new
    /// root_page internally without leaking it to concurrent readers
    /// before TakeLock publishes it.
    fn resolve_root_page(&self, table_id: MVTableId) -> Option<u64> {
        if let Some(&rp) = self.pending_root_page_allocations.get(&table_id) {
            return Some(rp);
        }
        self.mvstore
            .table_id_to_rootpage
            .get(&table_id)
            .and_then(|e| *e.value())
    }

    fn refresh_checkpoint_bounds(&mut self) {
        let durable_tx_max = self.mvstore.durable_txid_max.load(Ordering::SeqCst);
        self.durable_txid_max_old = NonZeroU64::new(durable_tx_max);
        self.durable_txid_max_new = durable_tx_max;
    }

    pub fn new(
        pager: Arc<Pager>,
        mvstore: Arc<MvStore<Clock>>,
        connection: Arc<Connection>,
        update_transaction_state: bool,
        sync_mode: SyncMode,
        mode: CheckpointMode,
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
                table.columns().len(),
            )
        });
        let durable_mvcc_metadata = !connection.db.is_in_memory_db() && mvcc_meta_table.is_some();
        let durable_tx_max = mvstore.durable_txid_max.load(Ordering::SeqCst);
        let durable_txid_max_old = NonZeroU64::new(durable_tx_max);
        #[cfg(any(test, injected_yields))]
        let yield_instance_id = connection.next_yield_instance_id();
        Self {
            state: CheckpointState::PrepareCheckpoint,
            lock_states: LockStates {
                blocking_checkpoint_lock_held: false,
                pager_read_tx: false,
                pager_write_tx: false,
            },
            pager,
            durable_txid_max_old,
            durable_txid_max_new: durable_tx_max,
            mvstore,
            connection,
            #[cfg(any(test, injected_yields))]
            yield_instance_id,
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
            mode,
            mvcc_meta_table,
            durable_mvcc_metadata,
            staged_checkpoint_header: None,
            header_staged_for_commit: false,
            collect_table_cursor: None,
            collect_index_tableid_cursor: None,
            collect_index_key_cursor: None,
            snapshot_ts: 0,
            owns_checkpoint_in_progress: false,
            stop_after_flush: false,
            flush_completed: false,
            pending_root_page_allocations: HashMap::default(),
            lock_commit_done: false,
            markers_published: false,
            gc_and_unlock_done: false,
        }
    }

    /// Construct a state machine that performs only the "flush half" of a
    /// checkpoint: PrepareCheckpoint → BeginPagerTxn → ... → CommitPagerTxn →
    /// TakeLock (publish markers + GC) → release lock → Finalize.
    /// The WAL → DB backfill and log/WAL truncation are NOT performed; the
    /// next regular checkpoint covers them naturally because it sees an
    /// empty write_set and falls straight through TakeLock → CheckpointWal.
    pub fn new_flush_only(
        pager: Arc<Pager>,
        mvstore: Arc<MvStore<Clock>>,
        connection: Arc<Connection>,
        update_transaction_state: bool,
        sync_mode: SyncMode,
    ) -> Self {
        // The flush half never reaches CheckpointWal/TruncateWal (it jumps to
        // Finalize after the marker publish + GC), so the WAL mode is moot;
        // Passive is the inert choice.
        let mut sm = Self::new(
            pager,
            mvstore,
            connection,
            update_transaction_state,
            sync_mode,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        sm.stop_after_flush = true;
        sm
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

        // Release the single-orchestrator gate. The error path can be hit at
        // any state past PrepareCheckpoint, including during the unlocked pager
        // write phase where `blocking_checkpoint_lock` was never taken — so
        // this is tied to `owns_checkpoint_in_progress`, not the lock state.
        if self.owns_checkpoint_in_progress {
            self.mvstore
                .checkpoint_in_progress
                .store(false, Ordering::Release);
            self.owns_checkpoint_in_progress = false;
        }
    }

    /// Returns all checkpointable [RowVersion]s for that `table_id`.
    ///
    /// Filters by `self.snapshot_ts`: any version with `begin_ts > snapshot_ts`
    /// (or `end_ts > snapshot_ts`) is considered "arrived after we started this
    /// checkpoint" and is excluded from this pass. The next pass picks it up.
    /// This is the upper-bound counterpart to the existing `durable_txid_max_old`
    /// lower-bound check, and it's what makes the unlocked collection safe under
    /// concurrent commits.
    fn maybe_get_checkpointable_versions(
        &self,
        versions: &[RowVersion],
        table_id: MVTableId,
    ) -> smallvec::SmallVec<[RowVersion; 1]> {
        let mut versions_to_checkpoint: smallvec::SmallVec<[_; 1]> =
            smallvec::SmallVec::with_capacity(1);
        let mut exists_in_db_file = false;
        // Iterate versions from oldest-to-newest to determine if the row exists in the database file and whether the newest version should be checkpointed.
        for version in versions.iter() {
            // A row is in the database file if:
            // There is a version whose begin timestamp is <= than the last checkpoint timestamp, AND
            // There is NO version whose END timestamp is <= than the last checkpoint timestamp.
            let mut begin_ts = None;
            if let Some(TxTimestampOrID::Timestamp(b)) = version.begin {
                begin_ts = Some(b);
            }
            let mut end_ts = None;
            if let Some(TxTimestampOrID::Timestamp(e)) = version.end {
                end_ts = Some(e);
            }
            if begin_ts.is_none() && end_ts.is_none() {
                // Rolled-back garbage and active TxID-only placeholders are not part of
                // the durable row history, so they must not influence DB-file existence.
                continue;
            }
            // Upper-bound filter: skip versions that arrived after this checkpoint
            // pass started. They'll be picked up by the next pass. Note that
            // `exists_in_db_file` is determined by older versions in the same
            // chain; if a newer version is skipped, the older state still
            // correctly reflects what's in the DB file.
            if begin_ts.is_some_and(|b| b > self.snapshot_ts) {
                continue;
            }
            if end_ts.is_some_and(|e| e > self.snapshot_ts) {
                // Tombstone arrived after our snapshot; treat the row as still
                // present from this pass's perspective.
                continue;
            }
            // Rows marked btree_resident existed in the DB file before MVCC tracked them.
            // This also applies to synthetic tombstones that use begin=None.
            if version.btree_resident {
                exists_in_db_file = true;
            }
            // A row exists in the DB file if it was checkpointed in a previous checkpoint.
            // For btree_resident rows we seed exists_in_db_file above, regardless of begin encoding.
            // These timestamp-derived transitions must run after the btree_resident seed so a
            // checkpointed tombstone can clear DB-file existence on a retry checkpoint.
            if self
                .durable_txid_max_old
                .is_some_and(|txid_max_old| begin_ts.is_some_and(|b| b <= u64::from(txid_max_old)))
            {
                exists_in_db_file = true;
            }
            if self
                .durable_txid_max_old
                .is_some_and(|txid_max_old| end_ts.is_some_and(|e| e <= u64::from(txid_max_old)))
            {
                exists_in_db_file = false;
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
            // - It is a delete of an uncheckpointed sqlite_schema row for a
            //   table or index. The schema row itself is not in the DB file, but
            //   checkpoint still needs it so it can remember that the table or
            //   index was destroyed and skip that object's data/index rows.
            //   Views and triggers have rootpage=0, so their uncheckpointed
            //   deletes can be ignored here: there is no B-tree to destroy, and
            //   deleting a missing sqlite_schema row would be wrong.
            let is_schema_delete = table_id == SQLITE_SCHEMA_MVCC_TABLE_ID
                && !exists_in_db_file
                && sqlite_schema_btree_identity(version).is_some()
                && self
                    .durable_txid_max_old
                    .is_none_or(|txid_max_old| end_ts.is_some_and(|e| e > u64::from(txid_max_old)));
            let should_checkpoint =
                is_uncheckpointed_insert || is_delete_and_exists_in_db_file || is_schema_delete;
            if should_checkpoint {
                if table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                    if versions_to_checkpoint.is_empty() {
                        versions_to_checkpoint.push(version.clone())
                    } else {
                        versions_to_checkpoint[0] = version.clone()
                    }
                    continue;
                }

                if let Some(previous_version) = versions_to_checkpoint.last() {
                    let should_drop_previous = previous_version.end.is_some()
                        && !is_schema_metadata_only_rewrite(previous_version, Some(version));
                    if should_drop_previous {
                        versions_to_checkpoint.pop();
                    }
                }

                versions_to_checkpoint.push(version.clone());
            }
        }

        versions_to_checkpoint
    }

    /// Collect all committed versions that need to be written to the B-tree.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    ///    TODO: garbage collect row versions after checkpointing.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    ///      If the row didn't exist in the database file and was deleted, we can simply not write it.
    fn collect_table_rows(&mut self) -> Option<IOCompletions> {
        // Invariant: RowID ordering is (table_id, row_id) with table_id ascending.
        // Since MV table IDs are negative and sqlite_schema is table_id=-1, iterating
        // in reverse visits sqlite_schema first so CREATE/DROP metadata is applied
        // before user-table rows in this checkpoint pass.
        let bounds: (Bound<RowID>, Bound<RowID>) = match self.collect_table_cursor.clone() {
            None => (Bound::Unbounded, Bound::Unbounded),
            Some(last) => (Bound::Unbounded, Bound::Excluded(last)),
        };
        let mut processed = 0;
        for entry in self.mvstore.rows.range(bounds).rev() {
            let key = entry.key();
            tracing::trace!("collecting {key:?}");
            self.collect_table_cursor = Some(key.clone());
            if self.destroyed_tables.contains(&key.table_id) {
                // We won't checkpoint rows for tables that will be destroyed in this checkpoint.
                // There's two forms of destroyed table:
                // 1. A non-checkpointed table that was created in the logical log and then destroyed. We don't need to do anything about this table in the pager/btree layer.
                // 2. A checkpointed table that was destroyed in the logical log. We need to destroy the btree in the pager/btree layer.
                tracing::trace!("skipping {key:?}");
                continue;
            }

            let row_versions = entry.value().read();

            for version in self.maybe_get_checkpointable_versions(&row_versions, key.table_id) {
                // Only a committed delete (`Some(Timestamp(_))`) is a real delete;
                // a speculative tombstone (`Some(TxID(_))`) was admitted by the
                // filter as an `is_uncheckpointed_insert` (end_ts is None for the
                // collector), so treating its raw end as a delete here would route
                // a still-live row to DROP/destroy handling and a missing-btree-row
                // bail downstream.
                let is_delete = matches!(version.end, Some(TxTimestampOrID::Timestamp(_)));

                let mut special_write = None;
                // Set to true for schema deletes of never-checkpointed tables/indexes.
                // These don't need to be written to the B-tree, we just need to track them.
                let mut skip_write = false;

                if let Some(schema_identity) = sqlite_schema_btree_identity(&version) {
                    let root_page = schema_identity.root_page;
                    match schema_identity.kind {
                        SqliteSchemaBtreeKind::Index => {
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
                        }
                        SqliteSchemaBtreeKind::Table => {
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
            processed += 1;
            if processed >= COLLECT_PREEMPTION_THRESHOLD {
                return Some(IOCompletions::Single(Completion::new_yield()));
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
        None
    }

    /// Collect all committed index row versions that need to be written to the B-tree.
    /// Index rows are stored separately from table rows and must be checkpointed independently.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    fn collect_index_rows(&mut self) -> Option<IOCompletions> {
        let outer_bounds: (Bound<MVTableId>, Bound<MVTableId>) =
            match self.collect_index_tableid_cursor {
                None => (Bound::Unbounded, Bound::Unbounded),
                Some(last) if self.collect_index_key_cursor.is_none() => {
                    (Bound::Excluded(last), Bound::Unbounded)
                }
                Some(last) => (Bound::Included(last), Bound::Unbounded),
            };
        let mut processed = 0;
        for entry in self.mvstore.index_rows.range(outer_bounds) {
            let index_id = *entry.key();

            // Skip destroyed indexes - we won't checkpoint rows for indexes that will be destroyed
            if self.destroyed_indexes.contains(&index_id) {
                self.collect_index_tableid_cursor = Some(index_id);
                self.collect_index_key_cursor = None;
                continue;
            }

            let index_rows_map = entry.value();
            let inner_bounds: (Bound<Arc<SortableIndexKey>>, Bound<Arc<SortableIndexKey>>) =
                match self.collect_index_key_cursor.clone() {
                    None => (Bound::Unbounded, Bound::Unbounded),
                    Some(last) => (Bound::Excluded(last), Bound::Unbounded),
                };
            for entry in index_rows_map.range(inner_bounds) {
                let versions = entry.value().read();
                self.collect_index_tableid_cursor = Some(index_id);
                self.collect_index_key_cursor = Some(entry.key().clone());

                for version in self.maybe_get_checkpointable_versions(&versions, index_id) {
                    // Only a committed delete is a real delete -- see the
                    // collector and the table-write dispatcher for the same
                    // discriminator and reasoning.
                    let is_delete =
                        matches!(version.end, Some(TxTimestampOrID::Timestamp(_)));

                    // Only write the row to the B-tree if it is not a delete, or if it is a delete and it exists in
                    // the database file.
                    self.index_write_set.push((index_id, version, is_delete));
                }
                processed += 1;
                if processed >= COLLECT_PREEMPTION_THRESHOLD {
                    return Some(IOCompletions::Single(Completion::new_yield()));
                }
            }
            self.collect_index_tableid_cursor = Some(index_id);
            self.collect_index_key_cursor = None;
        }
        None
    }

    #[cfg(any(test, debug_assertions))]
    fn max_collected_version_timestamp(&self) -> u64 {
        fn max_version_timestamp(version: &RowVersion) -> u64 {
            [version.begin.as_ref(), version.end.as_ref()]
                .into_iter()
                .filter_map(|ts| match ts {
                    Some(TxTimestampOrID::Timestamp(ts)) => Some(*ts),
                    _ => None,
                })
                .max()
                .unwrap_or_default()
        }

        let table_max = self
            .write_set
            .iter()
            .map(|(version, _)| max_version_timestamp(version))
            .max()
            .unwrap_or_default();
        let index_max = self
            .index_write_set
            .iter()
            .map(|(_, version, _)| max_version_timestamp(version))
            .max()
            .unwrap_or_default();
        table_max.max(index_max)
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
        // Pass the published boundary so the log preserves frames for commits
        // that landed (above this boundary) during the off-lock prepare phase and
        // were excluded from this checkpoint's backfill.
        self.mvstore.storage.truncate(self.durable_txid_max_new)
    }

    /// Perform a PASSIVE checkpoint on the WAL (WAL->DB backfill).
    fn checkpoint_wal(&self) -> Result<IOResult<CheckpointResult>> {
        let Some(wal) = &self.pager.wal else {
            panic!("No WAL to checkpoint");
        };
        // The mode is set at construction (see `mode` field):
        //
        // - Passive (auto-checkpoint after commit): backfill up to the safe
        //   frame WITHOUT WAL writer exclusivity, so it does not fight
        //   concurrent workers' WAL read marks the way TRUNCATE does. Runs
        //   UNLOCKED (blocking_checkpoint_lock released in TakeLock after the
        //   marker flip + targeted GC). The WAL is NOT reset here; it resets
        //   via restart-on-write. The WAL is left non-empty as a normal steady
        //   state — recovery treats a committed WAL + truncated logical log as
        //   the Passive steady state (see `maybe_complete_interrupted_checkpoint`,
        //   startup case 2) and recovers it.
        //
        // - Truncate/Restart (explicit `PRAGMA wal_checkpoint(TRUNCATE)`,
        //   journal-mode migration, `Connection::checkpoint`): backfill ALL
        //   frames with writer exclusivity and reset the WAL so `TruncateWal`
        //   can zero the file (SQLite-compatible explicit truncation).
        match wal.checkpoint(&self.pager, self.mode)? {
            IOResult::Done(result) => Ok(IOResult::Done(result)),
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        }
    }

    fn has_unpublished_schema_changes(&self) -> bool {
        let schema = self.connection.db.schema.lock();
        // A negative root_page is only OUR pending publication if THIS checkpoint
        // allocated it (it is present in `table_id_to_rootpage`). A CREATE that
        // committed during the off-lock prepare window (begin_ts above our
        // snapshot_ts) leaves a negative-root object this checkpoint did not cover;
        // a later checkpoint that covers it publishes the root, so it is not
        // pending for us and must not count here (otherwise Finalize's
        // has_pending_root_publication assert trips on a legitimate concurrent
        // CREATE). Mirrors the skip in `publish_checkpointed_schema_roots`.
        let ours = |root_page: i64| {
            root_page < 0
                && self
                    .mvstore
                    .table_id_to_rootpage
                    .get(&MVTableId::from(root_page))
                    .is_some()
        };
        !schema.dropped_root_pages.is_empty()
            || schema
                .tables
                .values()
                .any(|table| table.btree().is_some_and(|btree| ours(btree.root_page)))
            || schema.indexes.values().flatten().any(|index| ours(index.root_page))
    }

    fn has_pending_root_publication(&self) -> bool {
        !self.created_btrees.is_empty() || self.has_unpublished_schema_changes()
    }

    fn publish_checkpointed_schema_roots(&mut self) {
        if !self.has_pending_root_publication() {
            return;
        }

        // Patch sqlite_schema in MV Store to contain positive rootpages instead of negative ones
        // for tables and indexes that were flushed to the physical database.
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

        if !self.has_unpublished_schema_changes() {
            return;
        }

        // Patch in-memory schema to do the same. This must happen as soon as the
        // pager commit succeeds because the committed root pages are then visible
        // through WAL even if the later WAL checkpoint/log truncation is busy.
        let mut schema_ref = self.connection.db.schema.lock();
        let schema = Arc::make_mut(&mut *schema_ref);
        for table in schema.tables.values_mut() {
            let table = Arc::get_mut(table).expect("this should be the only reference");
            let Some(btree_table) = table.btree_mut() else {
                continue;
            };
            let btree_table = Arc::make_mut(btree_table);
            if btree_table.root_page < 0 {
                let table_id = MVTableId::from(btree_table.root_page);
                // A CREATE that committed during this checkpoint's off-lock prepare
                // window (begin_ts above our snapshot_ts) is in the connection's
                // schema but NOT in this pass's write_set, so it has no
                // table_id_to_rootpage mapping yet. Leave its root_page negative —
                // it stays MVCC-tracked and a later checkpoint that covers it
                // publishes the real root. (Under the old blocking checkpoint no
                // concurrent CREATE could slip in, so this was always Some.)
                if let Some(entry) = self.mvstore.table_id_to_rootpage.get(&table_id) {
                    let value = entry
                        .value()
                        .expect("a checkpointed table should have a root-page mapping");
                    btree_table.root_page = value as i64;
                }
            }
        }
        for table_index_list in schema.indexes.values_mut() {
            for index in table_index_list.iter_mut() {
                if index.root_page < 0 {
                    let table_id = MVTableId::from(index.root_page);
                    // Same off-lock case as the table loop above: an index whose
                    // CREATE committed after our snapshot_ts isn't checkpointed by
                    // this pass, so leave it negative for a later checkpoint.
                    if let Some(entry) = self.mvstore.table_id_to_rootpage.get(&table_id) {
                        let value = entry
                            .value()
                            .expect("a checkpointed index should have a root-page mapping");
                        let index = Arc::make_mut(index);
                        index.root_page = value as i64;
                    }
                }
            }
        }

        // Clear dropped root pages now that the pager commit contains the btree frees.
        // integrity_check should now follow the committed freelist state instead.
        schema.dropped_root_pages.clear();
        drop(schema_ref);
        *self.connection.schema.write() = self.connection.db.clone_schema();
        self.connection.bump_prepare_context_generation();
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
        let mut table_rows_to_gc = std::collections::BTreeSet::new();
        let mut index_rows_to_gc = std::collections::BTreeSet::new();

        for (row_version, _special_write) in &self.write_set {
            table_rows_to_gc.insert((
                row_version.row.id.table_id,
                row_version.row.id.row_id.clone(),
            ));
        }

        for (table_id, row_key) in table_rows_to_gc {
            let row_id = RowID::new(table_id, row_key);
            let Some(entry) = self.mvstore.rows.get(&row_id) else {
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
                self.mvstore.rows.remove(&row_id);
            }
        }

        for (index_id, row_version, _is_delete) in &self.index_write_set {
            let RowKey::Record(sortable_key) = &row_version.row.id.row_id else {
                unreachable!("index row versions always have Record keys");
            };
            index_rows_to_gc.insert((*index_id, RowKey::Record(sortable_key.clone())));
        }

        for (index_id, row_key) in index_rows_to_gc {
            let RowKey::Record(sortable_key) = row_key else {
                unreachable!("index row versions always have Record keys");
            };
            let outer_entry = self
                .mvstore
                .index_rows
                .get(&index_id)
                .expect("index_id from write set must exist in index_rows");
            let inner_map = outer_entry.value();
            let is_now_empty = {
                let inner_entry = inner_map
                    .get(&sortable_key)
                    .expect("index row from write set must exist in inner map");
                let mut versions = inner_entry.value().write();
                MvStore::<Clock>::gc_version_chain(&mut versions, lwm, ckpt_max);
                versions.is_empty()
            };
            if is_now_empty {
                inner_map.remove(&sortable_key);
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
        )?;
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
            CheckpointState::PrepareCheckpoint => {
                // Single-orchestrator gate. Multiple commits may have triggered
                // `should_checkpoint()` concurrently; only one runs the checkpoint,
                // the rest skip and let the in-flight one cover their work. This
                // was previously implicit in `blocking_checkpoint_lock` being the
                // first thing taken; now that the lock is moved past the pager
                // write phase, we need an explicit flag.
                if self
                    .mvstore
                    .checkpoint_in_progress
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    // Another checkpoint is already running. Return a no-op result;
                    // the auto-checkpoint caller swallows this and the user-facing
                    // checkpoint caller will see "nothing was backfilled."
                    // Transition to Finalize so the StateMachine wrapper's
                    // is_finalized() assertion is satisfied. We did no work
                    // and own no resources, so the Finalize body's cleanup
                    // is skipped for the skip path.
                    self.state = CheckpointState::Finalize;
                    return Ok(TransitionResult::Done(CheckpointResult::default()));
                }
                self.owns_checkpoint_in_progress = true;

                // Capture an upper bound for what we'll consider "committed enough
                // to flush this pass." Concurrent commits arriving with a higher
                // begin_ts during the unlocked write phase get picked up by the
                // next checkpoint. `durable_txid_max_new` is derived from
                // snapshot_ts, so the marker never claims a version durable that
                // we did not actually flush.
                self.snapshot_ts = self.mvstore.last_committed_tx_ts.load(Ordering::Acquire);

                // Checkpoint state machines can be created before they are run.
                // Resample after serializing (via `checkpoint_in_progress`) so
                // already-durable index deletes are not replayed.
                self.refresh_checkpoint_bounds();
                self.state = CheckpointState::CollectTableRows;
                Ok(TransitionResult::Continue)
            }
            CheckpointState::CollectTableRows => {
                if let Some(io) = self.collect_table_rows() {
                    return Ok(TransitionResult::Io(io));
                }
                tracing::debug!("Collected {} committed versions", self.write_set.len());
                self.state = CheckpointState::CollectIndexRows;
                Ok(TransitionResult::Continue)
            }
            CheckpointState::CollectIndexRows => {
                if let Some(io) = self.collect_index_rows() {
                    return Ok(TransitionResult::Io(io));
                }
                tracing::debug!("Collected {} index row changes", self.index_write_set.len());
                // Checkpoint boundary uses the snapshot_ts captured above. This
                // is the SAME atomic source as the old `committed_max` read, but
                // captured BEFORE collection so versions ≤ snapshot_ts in the
                // write_set are bounded by the same watermark we'll publish.
                let durable_old = self.durable_txid_max_old.map(u64::from).unwrap_or_default();
                #[cfg(any(test, debug_assertions))]
                {
                    let collected_max = self.max_collected_version_timestamp();
                    turso_assert!(
                        self.snapshot_ts >= collected_max,
                        "MVCC checkpoint collected version timestamp above snapshot_ts",
                        { "collected_max": collected_max, "snapshot_ts": self.snapshot_ts }
                    );
                }
                self.durable_txid_max_new = durable_old.max(self.snapshot_ts);
                self.maybe_stage_mvcc_metadata_write()?;

                self.mvstore
                    .storage
                    .on_checkpoint_start(self.durable_txid_max_new)?;

                if self.write_set.is_empty() && self.index_write_set.is_empty() {
                    // Nothing to checkpoint at the pager layer. Skip the pager
                    // write phase and go directly to taking the lock for the
                    // backfill + marker publication.
                    self.state = CheckpointState::TakeLock;
                } else {
                    self.state = CheckpointState::BeginPagerTxn;
                }
                Ok(TransitionResult::Continue)
            }
            CheckpointState::TakeLock => {
                // This state is re-entrant across yields. The three phases
                // (acquire lock / pager commit / marker publish) are each
                // guarded by a flag so a yield between them can resume
                // without redoing the prior phase. The lock is held across
                // all three; it is released only in Finalize (or the
                // stop_after_flush early-exit, or the error path).
                inject_transition_failure!(self, CheckpointYieldPoint::BeforeAcquireLock);
                inject_transition_yield!(self, CheckpointYieldPoint::BeforeAcquireLock);
                if !self.lock_states.blocking_checkpoint_lock_held {
                    tracing::debug!("Acquiring blocking checkpoint lock");
                    let locked = self.checkpoint_lock.write();
                    if !locked {
                        // No other checkpoint is running (the in-progress gate
                        // serialises that). The only other writer that can hold
                        // a conflicting lock here is VACUUM. Returning Busy
                        // propagates through `step()` to
                        // `cleanup_after_external_io_error`, which calls
                        // `pager.rollback_tx` — that drops the dirty pages,
                        // rolls the WAL coordination back to its pre-checkpoint
                        // max_frame (uncommitted frames sit in the WAL file as
                        // junk until overwritten, invisible to recovery), and
                        // hands the freelist back to its pre-checkpoint state.
                        // Auto-checkpoint callers swallow this Busy; user-facing
                        // callers surface it.
                        return Err(crate::LimboError::Busy);
                    }
                    self.lock_states.blocking_checkpoint_lock_held = true;
                }

                inject_transition_failure!(self, CheckpointYieldPoint::AfterAcquireLock);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterAcquireLock);

                // We hold the lock — now publish the checkpoint atomically.
                // The pager tx is still alive: WriteRow's dirty pages, the
                // staged page-1 header from CommitPagerTxn, and any
                // freelist mutations from btree_create are all uncommitted.
                // commit_tx flushes them + writes the WAL commit frame.
                // The commit frame becoming visible is what makes the
                // checkpoint's WAL frames part of the durable BTree state.
                // Pair it with the in-memory marker publication below so
                // recovery and concurrent readers see all-or-nothing.
                //
                // Empty write_set / index_write_set means CommitPagerTxn was
                // skipped (PrepareCheckpoint transitioned directly to TakeLock),
                // so there is no pager tx to commit either.
                let has_pager_work = !self.write_set.is_empty() || !self.index_write_set.is_empty();
                if has_pager_work && !self.lock_commit_done {
                    let commit_result = self
                        .pager
                        .commit_tx(&self.connection, self.update_transaction_state)?;
                    match commit_result {
                        IOResult::Done(_) => {}
                        IOResult::IO(io) => return Ok(TransitionResult::Io(io)),
                    }
                    self.lock_states.pager_read_tx = false;
                    self.lock_states.pager_write_tx = false;
                    self.lock_commit_done = true;
                }

                inject_transition_failure!(self, CheckpointYieldPoint::AfterPagerCommit);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterPagerCommit);

                if has_pager_work && !self.markers_published {
                    // Drain the staged root_page allocations into MvStore
                    // BEFORE publish_checkpointed_schema_roots, which reads
                    // `table_id_to_rootpage` to translate still-negative
                    // schema entries to their real root_page.
                    for (table_id, root_page) in self.pending_root_page_allocations.drain() {
                        self.mvstore
                            .insert_table_id_to_rootpage(table_id, Some(root_page));
                    }
                    self.mvstore
                        .durable_txid_max
                        .store(self.durable_txid_max_new, Ordering::SeqCst);
                    let header = self.staged_checkpoint_header.take().ok_or_else(|| {
                        LimboError::InternalError(
                            "checkpoint header was not staged before pager commit".to_string(),
                        )
                    })?;
                    self.mvstore.global_header.write().replace(header);
                    self.publish_checkpointed_schema_roots();
                    self.markers_published = true;
                }
                inject_transition_failure!(
                    self,
                    CheckpointYieldPoint::AfterDurableBoundaryAdvanced
                );
                inject_transition_yield!(self, CheckpointYieldPoint::AfterDurableBoundaryAdvanced);

                // Targeted GC under the lock, then RELEASE the lock. This is
                // the end of the locked region for ALL paths. Everything after
                // this — the Passive WAL->DB backfill, the DB fsync, and the
                // logical-log + WAL truncation — runs unlocked, so concurrent
                // readers and writers are no longer blocked by the checkpoint.
                // The lock now covers only: commit_tx (the WAL commit frame) +
                // the in-memory marker flip + this targeted GC.
                if !self.gc_and_unlock_done {
                    // Idempotent for the has-pager-work path (already set in
                    // the marker block); required for the empty-write_set
                    // backfill-only path that skipped that block.
                    self.mvstore
                        .durable_txid_max
                        .store(self.durable_txid_max_new, Ordering::SeqCst);
                    self.gc_checkpointed_versions();
                    self.checkpoint_lock.unlock();
                    self.lock_states.blocking_checkpoint_lock_held = false;
                    // Full-store reclamation runs WITHOUT the reader-blocking
                    // lock: drop_unused_row_versions is lwm-driven and uses
                    // per-chain locking + lazy SkipMap removal (it never does
                    // the TOCTOU-prone SkipMap remove that
                    // gc_checkpointed_versions does), so it is safe to run
                    // concurrently with readers and writers.
                    self.mvstore.drop_unused_row_versions();
                    self.gc_and_unlock_done = true;
                }

                if self.stop_after_flush {
                    // Flush-only path: leave the WAL non-empty (no backfill),
                    // so a subsequent reader exercising MvccLazyCursor must
                    // consult pager.read_page → WAL.
                    self.checkpoint_result = Some(CheckpointResult::default());
                    self.flush_completed = true;
                    self.state = CheckpointState::Finalize;
                    return Ok(TransitionResult::Continue);
                }

                self.state = CheckpointState::CheckpointWal;
                Ok(TransitionResult::Continue)
            }
            CheckpointState::BeginPagerTxn => {
                inject_transition_failure!(self, CheckpointYieldPoint::AfterPrepare);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterPrepare);
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

                self.pager
                    .io
                    .block(|| self.pager.begin_write_tx(WalAutoActions::all_enabled()))?;
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

                // write_set_index == 0: pager write tx open, no row written.
                // write_set_index == 1: exactly one table row written (partial
                // B-tree). Both are re-entrant: the yield is before this
                // index's row is processed.
                #[cfg(any(test, injected_yields))]
                if write_set_index == 0 {
                    inject_transition_failure!(self, CheckpointYieldPoint::AfterBeginPagerTxn);
                    inject_transition_yield!(self, CheckpointYieldPoint::AfterBeginPagerTxn);
                } else if write_set_index == 1 {
                    inject_transition_failure!(self, CheckpointYieldPoint::AfterFirstTableRow);
                    inject_transition_yield!(self, CheckpointYieldPoint::AfterFirstTableRow);
                }

                if !self.has_more_rows(write_set_index) {
                    inject_transition_failure!(self, CheckpointYieldPoint::AfterTableRows);
                    inject_transition_yield!(self, CheckpointYieldPoint::AfterTableRows);
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
                            // Stage the allocation locally — publishing it
                            // into `MvStore::table_id_to_rootpage` here would
                            // be visible to concurrent readers before the
                            // matching WAL frames are part of their snapshot.
                            // Drained into MvStore in TakeLock.
                            self.pending_root_page_allocations
                                .insert(table_id, created_root_page as u64);
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
                            // Evict stale cursor.
                            self.cursors.remove(&root_page);
                            self.destroyed_tables.insert(table_id);
                        }
                        SpecialWrite::BTreeCreateIndex { index_id, .. } => {
                            let created_root_page: u32 = self.pager.io.block(|| {
                                self.pager.btree_create(&CreateBTreeFlags::new_index())
                            })?;
                            // See BTreeCreate above — staged locally, drained
                            // into MvStore under the lock in TakeLock.
                            self.pending_root_page_allocations
                                .insert(index_id, created_root_page as u64);
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
                                )?;
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
                            // Evict stale cursor.
                            self.cursors.remove(&root_page);
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

                let root_page = self.resolve_root_page(table_id).unwrap_or_else(|| {
                    panic!(
                        "Table ID does not have a root page: {table_id}, row_version: {:?}",
                        self.get_current_row_version(write_set_index)
                            .expect("row version should exist")
                    )
                });

                // If a table was created, it now has a real root page allocated for it, but the 'root_page' field in the sqlite_schema record is still the table id.
                // So we need to rewrite the row version to use the real root page.
                if let Some(SpecialWrite::BTreeCreate {
                    table_id,
                    sqlite_schema_rowid,
                }) = special_write
                {
                    let root_page = self
                        .resolve_root_page(table_id)
                        .expect("Table ID does not have a root page");
                    let row_version = {
                        let (row_version, _) = self
                            .get_current_row_version_mut(write_set_index)
                            .ok_or_else(|| {
                                LimboError::InternalError(
                                    "row version not found in write set".to_string(),
                                )
                            })?;
                        let record = ImmutableRecordRef::from_bin_record(row_version.row.payload());

                        let mut values = record.get_values_owned()?;
                        values[3] = Value::from_i64(root_page as i64);
                        let record = ImmutableRecord::from_values(&values, values.len())?;
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
                    let root_page = self
                        .resolve_root_page(index_id)
                        .expect("Index ID does not have a root page");
                    let row_version = {
                        let (row_version, _) = self
                            .get_current_row_version_mut(write_set_index)
                            .ok_or_else(|| {
                                LimboError::InternalError(
                                    "row version not found in write set".to_string(),
                                )
                            })?;
                        let record = ImmutableRecordRef::from_bin_record(row_version.row.payload());
                        let mut values = record.get_values_owned()?;
                        values[3] = Value::from_i64(root_page as i64);
                        let record = ImmutableRecord::from_values(&values, values.len())?;
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

                // A committed delete carries `end = Some(Timestamp(_))`; a speculative
                // tombstone from an in-flight writer is `end = Some(TxID(_))`, which
                // `maybe_get_checkpointable_versions` treats as "no end" (the row is
                // still live at our snapshot, since the writer might still abort) and
                // picks up as an `is_uncheckpointed_insert`. The dispatch must use the
                // SAME discriminator -- otherwise a speculative tombstone gets routed
                // to `DeleteRowStateMachine`, whose btree seek finds no row and bails
                // with `"MVCC delete: rowid {N} not found"` (the row was never in the
                // btree to begin with).
                if matches!(row_version.end, Some(TxTimestampOrID::Timestamp(_))) {
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
                    // This is an insert/update (including a row whose speculative
                    // delete by an in-flight writer has not yet committed -- still
                    // live at our snapshot; a later checkpoint applies the delete if
                    // the writer commits).
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
                    inject_transition_failure!(self, CheckpointYieldPoint::AfterIndexRows);
                    inject_transition_yield!(self, CheckpointYieldPoint::AfterIndexRows);
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

                // Get root page for this index — staged allocations first.
                let root_page = self
                    .resolve_root_page(*index_id)
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
                    )?;
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
                // Stage the new page-1 header into the pager's dirty cache.
                // The actual pager `commit_tx` — which writes the WAL commit
                // frame, ends the write tx, and makes everything visible
                // through find_frame — is deferred to TakeLock so it lands
                // atomically with the in-memory marker publication. If
                // TakeLock can't acquire the lock, `pager.rollback_tx` in
                // `cleanup_after_external_io_error` discards the dirty pages,
                // uncommitted WAL frames, and freelist mutations together —
                // there is no half-committed checkpoint state for recovery
                // to choke on.
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
                // No pager.commit_tx here. Pager tx stays alive through
                // TakeLock; lock_states.pager_{read,write}_tx remain set so
                // the rollback path in cleanup_after_external_io_error knows
                // to roll us back if anything between here and TakeLock's
                // commit fails.
                inject_transition_failure!(self, CheckpointYieldPoint::AfterStageHeader);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterStageHeader);
                self.state = CheckpointState::TakeLock;
                Ok(TransitionResult::Continue)
            }

            CheckpointState::TruncateLogicalLog => {
                inject_transition_failure!(self, CheckpointYieldPoint::AfterSyncDbFile);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterSyncDbFile);
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
                inject_transition_failure!(self, CheckpointYieldPoint::AfterTruncateLogicalLog);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterTruncateLogicalLog);
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
                        // State advanced before the yield so a resume lands in
                        // SyncDbFile rather than re-running the backfill.
                        self.state = CheckpointState::SyncDbFile;
                        inject_transition_failure!(self, CheckpointYieldPoint::AfterCheckpointWal);
                        inject_transition_yield!(self, CheckpointYieldPoint::AfterCheckpointWal);
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
                inject_transition_failure!(self, CheckpointYieldPoint::AfterFsyncLogicalLog);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterFsyncLogicalLog);
                if self.mode.should_restart_log() {
                    // Truncate/Restart: the backfill above ran with writer
                    // exclusivity and reset the WAL's max_frame, so zeroing the
                    // WAL file is safe and required for an empty-WAL restart
                    // (SQLite-compatible explicit truncation). truncate_wal is
                    // resumable: it re-enters here on IO until Done. The
                    // one-shot AfterFsyncLogicalLog yield above does not re-fire.
                    let Some(wal) = &self.pager.wal else {
                        panic!("No WAL to truncate");
                    };
                    let checkpoint_result = self
                        .checkpoint_result
                        .as_mut()
                        .expect("checkpoint_result should be set");
                    if let IOResult::IO(io) =
                        wal.truncate_wal(checkpoint_result, self.pager.get_sync_type())?
                    {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                // Passive: no explicit WAL truncation. The backfill above does
                // NOT reset the WAL's max_frame, so zeroing the WAL file here
                // would orphan max_frame past an empty file (ShortReadWalFrame).
                // The WAL resets via restart-on-write.
                self.state = CheckpointState::Finalize;
                inject_transition_failure!(self, CheckpointYieldPoint::AfterTruncateWal);
                inject_transition_yield!(self, CheckpointYieldPoint::AfterTruncateWal);
                Ok(TransitionResult::Continue)
            }

            CheckpointState::Finalize => {
                turso_assert!(
                    !self.has_pending_root_publication(),
                    "checkpoint finalized after pager writes without publishing schema changes"
                );
                // The marker flip, targeted GC, and blocking_checkpoint_lock
                // release all happened in TakeLock; the backfill/sync/truncate
                // that followed ran unlocked. Finalize only releases the
                // single-orchestrator gate and returns the result.
                turso_assert!(
                    !self.lock_states.blocking_checkpoint_lock_held,
                    "blocking_checkpoint_lock should already be released by TakeLock before Finalize"
                );

                // Release the single-orchestrator gate AFTER unlocking
                // blocking_checkpoint_lock so the next checkpoint can take the
                // lock cleanly. Drop is the safety net for error paths.
                if self.owns_checkpoint_in_progress {
                    self.mvstore
                        .checkpoint_in_progress
                        .store(false, Ordering::Release);
                    self.owns_checkpoint_in_progress = false;
                }
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
            Err(ref err) => {
                self.mvstore
                    .storage
                    .on_checkpoint_end(self.durable_txid_max_new, Err(err.clone()))?;
                tracing::debug!("Error in checkpoint state machine: {err}");
                self.cleanup_after_external_io_error();
                res
            }
            Ok(TransitionResult::Done(ref result)) => {
                self.mvstore
                    .storage
                    .on_checkpoint_end(self.durable_txid_max_new, Ok(result))?;
                res
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

impl<Clock: LogicalClock> Drop for CheckpointStateMachine<Clock> {
    /// Safety net for `checkpoint_in_progress`. The flag is normally cleared
    /// by `Finalize` on success or by `cleanup_after_external_io_error` on
    /// error; this guard covers paths where the state machine is dropped
    /// without going through either (e.g. a panic during step).
    fn drop(&mut self) {
        if self.owns_checkpoint_in_progress {
            self.mvstore
                .checkpoint_in_progress
                .store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::database::tests::MvccTestDbNoConn;
    use crate::mvcc::database::SortableIndexKey;
    use crate::translate::collate::CollationSeq;
    use crate::types::{IndexInfo, KeyInfo};
    use turso_parser::ast::SortOrder;

    fn sqlite_schema_row_version(
        rowid: i64,
        entry_type: &'static str,
        name: &'static str,
        table_name: &'static str,
        root_page: i64,
        begin: Option<u64>,
        end: Option<u64>,
    ) -> RowVersion {
        let record = ImmutableRecord::from_values(
            &[
                Value::build_text(entry_type),
                Value::build_text(name),
                Value::build_text(table_name),
                Value::from_i64(root_page),
                Value::build_text(format!("sql:{entry_type}:{name}:{root_page}")),
            ],
            5,
        )
        .unwrap();
        RowVersion {
            id: 1,
            begin: begin.map(TxTimestampOrID::Timestamp),
            end: end.map(TxTimestampOrID::Timestamp),
            row: Row::new_table_row(
                RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(rowid)),
                record.as_blob().to_vec(),
                5,
            ),
            btree_resident: false,
        }
    }

    #[test]
    fn sqlite_schema_identity_treats_index_sql_rewrite_as_same_object() {
        let old = sqlite_schema_row_version(3, "index", "idx_t_a", "t", 7, Some(1), Some(2));
        let new = sqlite_schema_row_version(3, "index", "idx_t_a", "t", 7, Some(2), None);

        assert_eq!(
            sqlite_schema_btree_identity(&old),
            Some(SqliteSchemaBtreeIdentity {
                kind: SqliteSchemaBtreeKind::Index,
                root_page: 7,
            })
        );
        assert!(sqlite_schema_versions_refer_to_btree(&old, &new));
        assert!(!is_schema_metadata_only_rewrite(&old, Some(&new)));
    }

    #[test]
    fn sqlite_schema_identity_treats_table_sql_rewrite_as_same_object() {
        let old = sqlite_schema_row_version(2, "table", "t", "t", 5, Some(1), Some(2));
        let new = sqlite_schema_row_version(2, "table", "t", "t", 5, Some(2), None);

        assert!(sqlite_schema_versions_refer_to_btree(&old, &new));
        assert!(!is_schema_metadata_only_rewrite(&old, Some(&new)));
    }

    #[test]
    fn sqlite_schema_identity_detects_drop_recreate_as_different_objects() {
        let dropped = sqlite_schema_row_version(3, "index", "idx_t_v", "t", -4, Some(1), Some(2));
        let recreated = sqlite_schema_row_version(3, "index", "idx_t_v", "t", -5, Some(2), None);

        assert!(!sqlite_schema_versions_refer_to_btree(&dropped, &recreated));
        assert!(is_schema_metadata_only_rewrite(&dropped, Some(&recreated)));
    }

    #[test]
    fn sqlite_schema_identity_detects_drop_without_successor() {
        let dropped = sqlite_schema_row_version(3, "index", "idx_t_v", "t", 11, Some(1), Some(2));

        assert!(is_schema_metadata_only_rewrite(&dropped, None));
    }

    #[test]
    fn sqlite_schema_identity_ignores_non_btree_schema_entries() {
        let trigger = sqlite_schema_row_version(9, "trigger", "trg_t", "t", 0, Some(1), Some(2));
        let rewritten_trigger =
            sqlite_schema_row_version(9, "trigger", "trg_t", "t", 0, Some(2), None);

        assert_eq!(sqlite_schema_btree_identity(&trigger), None);
        assert!(!sqlite_schema_versions_refer_to_btree(
            &trigger,
            &rewritten_trigger
        ));
        assert!(!is_schema_metadata_only_rewrite(
            &trigger,
            Some(&rewritten_trigger)
        ));
    }

    #[test]
    fn sqlite_schema_identity_ignores_payloadless_tombstones() {
        let tombstone = RowVersion {
            id: 1,
            begin: None,
            end: Some(TxTimestampOrID::Timestamp(2)),
            row: Row::new_table_row(
                RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(9)),
                Vec::new(),
                0,
            ),
            btree_resident: false,
        };

        assert_eq!(sqlite_schema_btree_identity(&tombstone), None);
        assert!(!is_schema_metadata_only_rewrite(&tombstone, None));
    }

    fn index_row_version(
        index_id: MVTableId,
        key_text: &str,
        rowid: i64,
        version_id: u64,
        begin: Option<u64>,
        end: Option<u64>,
        btree_resident: bool,
    ) -> (Arc<SortableIndexKey>, RowVersion) {
        let index_info = Arc::new(IndexInfo {
            key_info: vec![
                KeyInfo {
                    sort_order: SortOrder::Asc,
                    collation: CollationSeq::Binary,
                    nulls_order: None,
                },
                KeyInfo {
                    sort_order: SortOrder::Asc,
                    collation: CollationSeq::Binary,
                    nulls_order: None,
                },
            ],
            has_rowid: true,
            num_cols: 2,
            is_unique: true,
        });
        let key_record = ImmutableRecord::from_values(
            &[
                Value::Text(crate::types::Text::new(key_text.to_string())),
                Value::from_i64(rowid),
            ],
            2,
        )
        .unwrap();
        let sortable_key = SortableIndexKey::new_from_record(key_record, index_info);
        let key_arc = Arc::new(sortable_key.clone());
        let row = Row::new_index_row(RowID::new(index_id, RowKey::Record(sortable_key)), 2);
        let row_version = RowVersion {
            id: version_id,
            begin: begin.map(TxTimestampOrID::Timestamp),
            end: end.map(TxTimestampOrID::Timestamp),
            row,
            btree_resident,
        };
        (key_arc, row_version)
    }

    #[test]
    fn checkpoint_retry_does_not_replay_checkpointed_btree_resident_delete() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        checkpoint.durable_txid_max_old = std::num::NonZeroU64::new(10);
        checkpoint.durable_txid_max_new = 10;

        let index_id = MVTableId::from(-42);
        let (garbage_key, garbage_version) =
            index_row_version(index_id, "blue_river_906", 75, 1, None, None, true);
        let (_, tombstone_version) =
            index_row_version(index_id, "blue_river_906", 75, 2, None, Some(10), true);

        mvstore.insert_index_version(index_id, garbage_key, garbage_version);
        let entry = mvstore
            .index_rows
            .get(&index_id)
            .expect("index entry should exist after first insert");
        let tombstone_key = entry
            .value()
            .front()
            .expect("key bucket should exist after first insert")
            .key()
            .clone();
        mvstore.insert_index_version(index_id, tombstone_key, tombstone_version);

        while checkpoint.collect_index_rows().is_some() {}

        assert!(
            checkpoint.index_write_set.is_empty(),
            "a retry checkpoint must not replay a delete whose btree_resident tombstone was already made durable"
        );
    }

    fn committed_table_row_version(table_id: MVTableId, rowid: i64) -> RowVersion {
        let record = ImmutableRecord::from_values(&[Value::from_i64(rowid)], 1).unwrap();
        RowVersion {
            id: 1,
            begin: Some(TxTimestampOrID::Timestamp(5)),
            end: None,
            row: Row::new_table_row(
                RowID::new(table_id, RowKey::Int(rowid)),
                record.as_blob().to_vec(),
                1,
            ),
            btree_resident: false,
        }
    }

    #[test]
    fn collect_table_rows_preempts_on_large_scan() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        // This drives collect_*_rows() directly, bypassing PrepareCheckpoint (which
        // in production captures snapshot_ts from last_committed_tx_ts). Off-lock
        // collection skips versions with begin_ts > snapshot_ts, so set it to admit
        // all committed rows — this test is about preemption, not the snapshot bound.
        checkpoint.snapshot_ts = u64::MAX;

        // More than one chunk worth of committed rows so collection must preempt.
        let table_id = MVTableId::from(-2);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for i in 0..row_count as i64 {
            let version = committed_table_row_version(table_id, i);
            mvstore.rows.insert(
                RowID::new(table_id, RowKey::Int(i)),
                Arc::new(RwLock::new(vec![version])),
            );
        }

        // The first chunk fills up before the scan finishes, so it must yield.
        let first = checkpoint.collect_table_rows();
        assert!(
            first.is_some_and(|io| io.is_explicit_yield()),
            "scanning more than COLLECT_PREEMPTION_THRESHOLD rows must preempt with an explicit yield"
        );

        // Resume from the cursor until the scan finishes; every row must still
        // be collected exactly once across the chunks.
        while checkpoint.collect_table_rows().is_some() {}
        assert_eq!(checkpoint.write_set.len(), row_count);
    }

    #[test]
    fn collect_index_rows_preempts_on_large_scan() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        // See collect_table_rows_preempts_on_large_scan: this bypasses
        // PrepareCheckpoint, so snapshot_ts must admit the committed rows.
        checkpoint.snapshot_ts = u64::MAX;

        let index_id = MVTableId::from(-7);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for i in 0..row_count as i64 {
            let (key, version) = index_row_version(index_id, "k", i, 1, Some(5), None, false);
            mvstore.insert_index_version(index_id, key, version);
        }

        let first = checkpoint.collect_index_rows();
        assert!(
            first.is_some_and(|io| io.is_explicit_yield()),
            "scanning more than COLLECT_PREEMPTION_THRESHOLD index rows must preempt with an explicit yield"
        );

        while checkpoint.collect_index_rows().is_some() {}
        assert_eq!(checkpoint.index_write_set.len(), row_count);
    }
}
