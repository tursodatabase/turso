use crate::error::io_error;
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_points::YieldInjector;
use crate::statement::StatementOrigin;
use crate::storage::{journal_mode, pager::SavepointResult};
use crate::sync::{
    atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicIsize, AtomicU16, AtomicU64, Ordering},
    Arc, RwLock,
};
use crate::turso_assert;
#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
use crate::types::{WalFrameInfo, WalState};
#[cfg(feature = "fs")]
use crate::util::{OpenMode, OpenOptions};
#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
use crate::Page;
use crate::{
    ast, function,
    io::{MemoryIO, IO},
    parse_schema_rows,
    progress::{ProgressHandler, ProgressHandlerCallback},
    refresh_analyze_stats, translate,
    util::IOExt,
    vdbe, AllViewsTxState, AtomicCipherMode, AtomicSyncMode, AtomicTempStore, BusyHandler,
    BusyHandlerCallback, CaptureDataChangesInfo, CheckpointMode, CheckpointResult, CipherMode, Cmd,
    Completion, ConnectionMetrics, Database, DatabaseCatalog, DatabaseOpts, Duration,
    EncryptionKey, EncryptionOpts, IndexMethod, LimboError, MvStore, OpenFlags, PageSize, Pager,
    Parser, Program, QueryMode, QueryRunner, Result, Schema, Statement, SyncMode, TransactionMode,
    Trigger, Value, VirtualTable,
};
use crate::{MAIN_DB_ID, TEMP_DB_ID};
use arc_swap::ArcSwap;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use smallvec::SmallVec;
use std::fmt::Display;
use std::ops::Deref;
#[cfg(feature = "simulator")]
use std::path::Path;
#[cfg(not(target_family = "wasm"))]
use tempfile::TempDir;
use tracing::{instrument, Level};
use turso_macros::{turso_assert_ne, AtomicEnum};

#[cfg(feature = "simulator")]
fn db_identity_for_testing(db_path: &Path) -> Result<(u32, u32)> {
    let bytes =
        std::fs::read(db_path).map_err(|e| io_error(e, "read db header for simulator testing"))?;
    let db_header_size = crate::storage::sqlite3_ondisk::DatabaseHeader::SIZE;
    if bytes.len() < db_header_size {
        return Err(LimboError::InternalError(format!(
            "database file is smaller than the header: got {}, need at least {}",
            bytes.len(),
            db_header_size
        )));
    }
    let db_size_pages = u32::from_be_bytes(bytes[28..32].try_into().unwrap());
    let crc = crc32c::crc32c(&bytes[..db_header_size]);
    Ok((db_size_pages, crc))
}

#[derive(Clone, AtomicEnum, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TransactionState {
    Write {
        schema_did_change: bool,
    },
    Read,
    /// PendingUpgrade remembers what transaction state was before upgrade to write (has_read_txn is true if before transaction were in Read state)
    /// This is important, because if we failed to initialize write transaction immediatley - we need to end implicitly started read txn (e.g. for simiple INSERT INTO operation)
    /// But for late upgrade of transaction we should keep read transaction active (e.g. BEGIN; SELECT ...; INSERT INTO ...)
    PendingUpgrade {
        has_read_txn: bool,
    },
    None,
}

pub(crate) struct TempDatabase {
    pub(crate) db: Arc<Database>,
    pub(crate) pager: Arc<Pager>,
    #[cfg(not(target_family = "wasm"))]
    _temp_dir: Option<TempDir>,
}

/// All of the connection-local state needed to manage the `TEMP` schema.
///
/// Grouped so that anything that touches the temp database (the pager,
/// the last committed schema snapshot, the dirty-schema flag) lives in
/// one place. The individual fields keep their own locks because their
/// access patterns differ: `database` is read on every temp-qualified
/// lookup while `committed_schema` is only touched at commit/rollback
/// boundaries, and `schema_did_change` is flipped from inside `SetCookie`.
pub(crate) struct TempDbContext {
    /// Per-connection temp database (`TEMP_DB_ID`/schema `temp`).
    /// Lazily initialized on first temp DDL.
    pub(crate) database: RwLock<Option<TempDatabase>>,
    /// Last committed snapshot of `database.read().as_ref().unwrap().db.schema`.
    /// Updated on successful commit and consulted on full-txn rollback
    /// to restore the in-memory temp schema (there is no shared
    /// `Database::schema` for temp the way main has). `None` until the
    /// first temp DDL is committed; a rollback with `None` resets the
    /// temp schema to empty.
    pub(crate) committed_schema: RwLock<Option<Arc<Schema>>>,
    /// Set by `SetCookie` when a temp DDL runs; read by commit/rollback
    /// to decide whether to snapshot/restore. Cleared on transaction
    /// end. Mirrors the `schema_did_change` field inside
    /// `TransactionState::Write` for the main DB.
    pub(crate) schema_did_change: AtomicBool,
}

impl TempDbContext {
    pub(crate) fn new() -> Self {
        Self {
            database: RwLock::new(None),
            committed_schema: RwLock::new(None),
            schema_did_change: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NamedSavepointFrame {
    pub(crate) name: String,
    pub(crate) starts_transaction: bool,
    pub(crate) deferred_fk_violations: isize,
    /// Snapshot of `temp_db.db.schema` taken at SAVEPOINT begin. `None`
    /// when the temp database had not been initialized yet. Used by
    /// ROLLBACK TO to restore the in-memory temp schema after the
    /// on-disk pages have been rolled back via the mirror call.
    pub(crate) temp_schema_snapshot: Option<Arc<Schema>>,
    /// Snapshot of the connection-local `database_schemas` map at
    /// SAVEPOINT begin. Cheap — values are `Arc`. Used by ROLLBACK TO
    /// to restore staged DDL on attached databases.
    pub(crate) staged_schema_snapshot: HashMap<usize, Arc<Schema>>,
}

/// Info returned by `rollback_named_savepoint_frame` so callers can
/// restore in-memory schema state after the pager has rolled back.
pub(crate) struct RollbackFrameInfo {
    pub(crate) temp_schema_snapshot: Option<Arc<Schema>>,
    pub(crate) staged_schema_snapshot: HashMap<usize, Arc<Schema>>,
}

/// Database connection handle.
///
/// If you add a setting that affects SQL compilation or execution, call
/// `bump_prepare_context_generation()` in its setter so cached prepared
/// statements know they need to be reprepared.
pub struct Connection {
    pub(crate) db: Arc<Database>,
    pub(crate) pager: ArcSwap<Pager>,
    pub(crate) schema: RwLock<Arc<Schema>>,
    /// Per-database schema cache (database_index -> schema)
    /// Loaded lazily to avoid copying all schemas on connection open
    pub(super) database_schemas: RwLock<HashMap<usize, Arc<Schema>>>,
    /// Whether to automatically commit transaction
    pub(crate) auto_commit: AtomicBool,
    pub(super) transaction_state: AtomicTransactionState,
    pub(super) last_insert_rowid: AtomicI64,
    pub(crate) changes: AtomicI64,
    pub(crate) total_changes: AtomicI64,
    pub(crate) syms: parking_lot::RwLock<SymbolTable>,
    pub(super) _shared_cache: bool,
    pub(super) cache_size: AtomicI32,
    /// page size used for an uninitialized database or the next vacuum command.
    /// it's not always equal to the current page size of the database
    pub(super) page_size: AtomicU16,
    /// Disable automatic checkpoint behaviour when DB is shutted down or WAL reach certain size
    /// Client still can manually execute PRAGMA wal_checkpoint(...) commands
    pub(super) wal_auto_checkpoint_disabled: AtomicBool,
    pub(super) capture_data_changes: RwLock<Option<CaptureDataChangesInfo>>,
    /// CDC v2: transaction ID for grouping CDC records by transaction.
    /// -1 means unset (will be assigned on first CDC write in the transaction).
    pub(crate) cdc_transaction_id: AtomicI64,
    pub(super) closed: AtomicBool,
    /// Per-connection state for the `TEMP` schema (pager, last-committed
    /// snapshot, dirty-schema flag). See `TempDbContext`.
    pub(crate) temp: TempDbContext,
    /// Attached databases
    pub(super) attached_databases: RwLock<DatabaseCatalog>,
    pub(super) query_only: AtomicBool,
    /// If enabled, the UPDATE/DELETE statements must have a WHERE clause
    pub(super) dml_require_where: AtomicBool,
    /// SQLite DQS misfeature: when ON (default), unresolved double-quoted identifiers
    /// in DML statements fall back to string literals instead of raising an error.
    pub(super) dqs_dml: AtomicBool,
    /// Deprecated pragma: when ON, column names include table prefix (TABLE.COLUMN)
    pub(super) full_column_names: AtomicBool,
    /// Deprecated pragma: when ON (default), column refs use just the column name
    pub(super) short_column_names: AtomicBool,
    pub(crate) mv_tx: RwLock<Option<(crate::mvcc::database::TxID, TransactionMode)>>,
    /// Per-attached-database MVCC transactions.
    /// Main DB uses `mv_tx` above for zero-cost hot path access.
    pub(crate) attached_mv_txs:
        RwLock<HashMap<usize, (crate::mvcc::database::TxID, TransactionMode)>>,
    #[cfg(any(test, injected_yields))]
    pub(super) yield_injector: RwLock<Option<Arc<dyn YieldInjector>>>,
    #[cfg(any(test, injected_yields))]
    pub(super) yield_instance_id_counter: AtomicU64,

    /// Per-connection view transaction states for uncommitted changes. This represents
    /// one entry per view that was touched in the transaction.
    pub(crate) view_transaction_states: AllViewsTxState,
    /// Connection-level metrics aggregation
    pub metrics: RwLock<ConnectionMetrics>,
    /// Greater than zero if connection executes a program within a program
    /// This is necessary in order for connection to not "finalize" transaction (commit/abort) when program ends
    /// (because parent program is still pending and it will handle "finalization" instead)
    ///
    /// The state is integer as we may want to spawn deep nested programs (e.g. Root -[run]-> S1 -[run]-> S2 -[run]-> ...)
    /// and we need to track current nestedness depth in order to properly understand when we will reach the root back again
    pub(super) nestedness: AtomicI32,
    /// Stack of currently compiling triggers to prevent recursive trigger subprogram compilation
    pub(super) compiling_triggers: RwLock<Vec<Arc<Trigger>>>,
    /// Stack of currently executing triggers to prevent recursive trigger execution
    /// Only prevents the same trigger from firing again, allowing different triggers on the same table to fire
    pub(super) executing_triggers: RwLock<Vec<Arc<Trigger>>>,
    pub(crate) encryption_key: RwLock<Option<EncryptionKey>>,
    pub(super) encryption_cipher_mode: AtomicCipherMode,
    pub(super) sync_mode: AtomicSyncMode,
    pub(super) temp_store: AtomicTempStore,
    pub(super) data_sync_retry: AtomicBool,
    /// Busy handler for lock contention
    /// Default is BusyHandler::None (return SQLITE_BUSY immediately)
    pub(super) busy_handler: RwLock<BusyHandler>,
    /// Step-based progress callback for SQLite-compatible cancellation hooks.
    pub(super) progress_handler: ProgressHandler,
    /// Maximum execution time for a single statement on this connection.
    /// `Duration::ZERO` means disabled.
    pub(super) query_timeout_ms: AtomicU64,
    /// True when sqlite3_interrupt()-style cancellation is pending for active root statements.
    pub(super) interrupt_requested: AtomicBool,
    /// Whether this is an internal connection used for MVCC bootstrap
    pub(super) is_mvcc_bootstrap_connection: AtomicBool,
    /// Whether pragma foreign_keys=ON for this connection
    pub(super) fk_pragma: AtomicBool,
    pub(crate) fk_deferred_violations: AtomicIsize,
    /// Number of active write statements on this connection.
    pub(crate) n_active_writes: AtomicI32,
    /// Number of active root statements currently executing on this connection.
    /// This is Turso's equivalent of SQLite's top-level active-VDBE count
    /// (`db->nVdbeActive`) for user statements, excluding internal helpers and
    /// subprogram execution.
    pub(crate) n_active_root_statements: AtomicI32,
    /// Whether pragma ignore_check_constraints=ON for this connection
    pub(super) check_constraints_pragma: AtomicBool,
    /// Track when each virtual table instance is currently in transaction.
    pub(crate) vtab_txn_states: RwLock<HashSet<u64>>,
    /// Connection-level named savepoint stack used to mirror savepoint state
    /// onto temp/attached databases that start participating after SAVEPOINT.
    pub(crate) named_savepoints: RwLock<Vec<NamedSavepointFrame>>,
    /// Generation counter bumped whenever any setting that affects PrepareContext
    /// changes. Allows prepared statements to cheaply detect when they need to be
    /// reprepared (single u64 comparison instead of rebuilding the full context).
    /// IMPORTANT: this is a bit of a regression landmine because the generation
    /// MUST be incremented whenever any setting that affects PrepareContext changes,
    /// and this is not currently centralized; each setter bumps the generation individually.
    pub(crate) prepare_context_generation: AtomicU64,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
crate::assert::assert_send_sync!(Connection);

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.is_closed() {
            // Roll back any active MVCC transactions so that MvStore entries
            // don't leak and block future checkpoints.  The tx may have
            // already been committed/aborted externally (e.g. by tests that
            // manipulate MvStore directly), so only rollback if still active.
            if let Some(mv_store) = self.db.get_mv_store().as_ref() {
                if let Some(tx_id) = self.get_mv_tx_id() {
                    let pager = self.pager.load();
                    if mv_store.is_tx_rollbackable(tx_id) {
                        mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
                    } else {
                        self.set_mv_tx(None);
                    }
                    pager.end_read_tx();
                }
            }
            self.rollback_attached_mvcc_txs(false);

            // Release any WAL locks the connection might be holding.
            // This prevents deadlocks if a connection is dropped (e.g., due to a panic)
            // while holding a read or write lock.
            let pager = self.pager.load();
            if let Some(wal) = &pager.wal {
                if wal.holds_write_lock() {
                    wal.end_write_tx();
                }
                if wal.holds_read_lock() {
                    wal.end_read_tx();
                }
            }

            // Also release WAL locks on all attached database pagers
            self.with_all_attached_pagers_with_index(|attached_pagers| {
                for (_, attached_pager) in attached_pagers {
                    if let Some(wal) = &attached_pager.wal {
                        if wal.holds_write_lock() {
                            wal.end_write_tx();
                        }
                        if wal.holds_read_lock() {
                            wal.end_read_tx();
                        }
                    }
                }
            });

            // if connection wasn't properly closed, decrement the connection counter
            self.db
                .n_connections
                .fetch_sub(1, crate::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Connection {
    pub(crate) fn empty_temp_schema(&self) -> Arc<Schema> {
        let mut schema = Schema::with_options(self.db.experimental_custom_types_enabled());
        schema.generated_columns_enabled = self.db.experimental_generated_columns_enabled();
        Arc::new(schema)
    }

    fn make_temp_database_opts(&self) -> DatabaseOpts {
        DatabaseOpts::new()
            .with_views(self.db.experimental_views_enabled())
            .with_custom_types(self.db.experimental_custom_types_enabled())
            .with_index_method(self.db.experimental_index_method_enabled())
            .with_generated_columns(self.db.experimental_generated_columns_enabled())
    }

    fn effective_temp_store(&self) -> crate::TempStore {
        let temp_store = self.get_temp_store();
        #[cfg(feature = "fs")]
        {
            temp_store
        }
        #[cfg(not(feature = "fs"))]
        {
            let _ = temp_store;
            crate::TempStore::Memory
        }
    }

    #[cfg(feature = "fs")]
    fn create_temp_database(&self) -> Result<TempDatabase> {
        let temp_store = self.effective_temp_store();
        let db_opts = self.make_temp_database_opts();
        let page_size = self.get_page_size();

        if matches!(temp_store, crate::TempStore::Memory) {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io,
                crate::util::MEMORY_PATH,
                OpenFlags::Create,
                db_opts,
                None,
            )?;
            let pager = Arc::new(db._init(None)?);
            pager.set_initial_page_size(page_size)?;
            return Ok(TempDatabase {
                db,
                pager,
                #[cfg(not(target_family = "wasm"))]
                _temp_dir: None,
            });
        }

        #[cfg(not(target_family = "wasm"))]
        {
            let temp_dir = tempfile::tempdir().map_err(|e| io_error(e, "tempdir"))?;
            let temp_path = temp_dir.path().join("tursodb-temp.db");
            let temp_path_str = temp_path.to_str().ok_or_else(|| {
                LimboError::InternalError("temp db path is not valid UTF-8".into())
            })?;
            // Always create a fresh IO for the temp file. Cloning the
            // main db's IO is wrong when the main db uses a mock /
            // simulated backend (e.g. the deterministic simulator
            // with `--io-backend=memory`) that can't access real
            // filesystem paths produced by `tempfile::tempdir()`.
            let io = Database::io_for_path(temp_path_str)?;
            let db = Database::open_file_with_flags(
                io,
                temp_path_str,
                OpenFlags::Create,
                db_opts,
                None,
            )?;
            let pager = Arc::new(db._init(None)?);
            pager.set_initial_page_size(page_size)?;
            Ok(TempDatabase {
                db,
                pager,
                _temp_dir: Some(temp_dir),
            })
        }

        #[cfg(target_family = "wasm")]
        {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io,
                crate::util::MEMORY_PATH,
                OpenFlags::Create,
                db_opts,
                None,
            )?;
            let pager = Arc::new(db._init(None)?);
            pager.set_initial_page_size(page_size)?;
            Ok(TempDatabase { db, pager })
        }
    }

    #[cfg(not(feature = "fs"))]
    fn create_temp_database(&self) -> Result<TempDatabase> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            crate::util::MEMORY_PATH,
            OpenFlags::Create,
            self.make_temp_database_opts(),
            None,
        )?;
        let pager = Arc::new(db._init(None)?);
        pager.set_initial_page_size(self.get_page_size())?;
        Ok(TempDatabase {
            db,
            pager,
            #[cfg(not(target_family = "wasm"))]
            _temp_dir: None,
        })
    }

    pub(crate) fn ensure_temp_database(&self) -> Result<()> {
        if self.temp.database.read().is_some() {
            return Ok(());
        }

        let temp_db = self.create_temp_database()?;
        let mut guard = self.temp.database.write();
        if guard.is_none() {
            *guard = Some(temp_db);
        }
        Ok(())
    }

    /// Tear down the per-connection temp database.
    ///
    /// Drops the temp pager, clears the committed schema snapshot and
    /// the dirty-schema flag. Called by `set_temp_store` when the user
    /// changes `PRAGMA temp_store` outside of an explicit transaction.
    fn reset_temp_database(&self) {
        if let Some(temp_db) = self.temp.database.write().take() {
            temp_db.pager.rollback_attached();
        }
        *self.temp.committed_schema.write() = None;
        self.temp.schema_did_change.store(false, Ordering::Release);
    }

    /// Flag a temp-schema mutation within the current transaction so the
    /// commit/rollback path knows to snapshot or restore the in-memory
    /// temp schema. Called from `SetCookie` for `TEMP_DB_ID`.
    pub(crate) fn mark_temp_schema_did_change(&self) {
        // If we're marking the temp schema dirty, temp DDL must have
        // just run against the temp pager — which means the temp
        // database was initialized. The opposite state is unreachable.
        turso_assert!(
            self.temp.database.read().is_some(),
            "mark_temp_schema_did_change called without an initialized temp database"
        );
        self.temp.schema_did_change.store(true, Ordering::Release);
    }

    /// On successful commit, snapshot the current `temp_db.db.schema`
    /// into `committed_temp_schema` so a future full-txn rollback can
    /// restore it. No-op if no temp DDL ran in this transaction.
    pub(crate) fn commit_temp_schema(&self) {
        if !self.temp.schema_did_change.load(Ordering::Acquire) {
            return;
        }
        // `schema_did_change` is only ever set by
        // `mark_temp_schema_did_change`, which asserts temp is
        // initialized. If it's somehow clear here we have a logic
        // bug — no safe recovery, so fail loud.
        let guard = self.temp.database.read();
        turso_assert!(
            guard.is_some(),
            "commit_temp_schema: schema_did_change set but temp is uninitialized"
        );
        let snap = guard
            .as_ref()
            .expect("asserted above")
            .db
            .schema
            .lock()
            .clone();
        drop(guard);
        // save snapshot for potential future rollback.
        *self.temp.committed_schema.write() = Some(snap);
        self.temp.schema_did_change.store(false, Ordering::Release);
    }

    /// On full-txn rollback, restore `temp_db.db.schema` from the last
    /// committed snapshot. If nothing was ever committed, reset to an
    /// empty schema (matches the disk state the pager rolled back to).
    pub(crate) fn rollback_temp_schema(&self) {
        if !self.temp.schema_did_change.load(Ordering::Acquire) {
            return;
        }
        // Same invariant as `commit_temp_schema` — the flag can only
        // be set while temp is initialized.
        let committed = self.temp.committed_schema.read().clone();
        {
            let guard = self.temp.database.read();
            turso_assert!(
                guard.is_some(),
                "rollback_temp_schema: schema_did_change set but temp is uninitialized"
            );
            let temp_db = guard.as_ref().expect("asserted above");
            match committed {
                Some(snap) => *temp_db.db.schema.lock() = snap,
                None => *temp_db.db.schema.lock() = self.empty_temp_schema(),
            }
        }
        self.temp.schema_did_change.store(false, Ordering::Release);
        self.bump_prepare_context_generation();
    }

    /// Bump the prepare context generation counter. Must be called whenever any
    /// connection setting that is tracked in `PrepareContext` changes, so that
    /// prepared statements know they need to be reprepared.
    #[inline]
    pub(crate) fn bump_prepare_context_generation(&self) {
        self.prepare_context_generation
            .fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub(crate) fn prepare_context_generation(&self) -> u64 {
        self.prepare_context_generation.load(Ordering::Acquire)
    }

    /// check if connection executes nested program (so it must not do any "finalization" work as parent program will handle it)
    pub fn is_nested_stmt(&self) -> bool {
        self.nestedness.load(Ordering::SeqCst) > 0
    }
    /// starts nested program execution
    pub fn start_nested(&self) {
        self.nestedness.fetch_add(1, Ordering::SeqCst);
    }
    /// ends nested program execution
    pub fn end_nested(&self) {
        self.nestedness.fetch_add(-1, Ordering::SeqCst);
    }

    /// Check if a specific trigger is currently compiling (for recursive trigger prevention)
    pub fn trigger_is_compiling(&self, trigger: &Arc<Trigger>) -> bool {
        let compiling = self.compiling_triggers.read();
        if let Some(trigger) = compiling.iter().find(|t| t.name == trigger.name) {
            tracing::debug!("Trigger is already compiling: {}", trigger.name);
            return true;
        }
        false
    }

    pub fn start_trigger_compilation(&self, trigger: Arc<Trigger>) {
        tracing::debug!("Starting trigger compilation: {}", trigger.name);
        self.compiling_triggers.write().push(trigger);
    }

    pub fn end_trigger_compilation(&self) {
        tracing::debug!(
            "Ending trigger compilation: {:?}",
            self.compiling_triggers.read().last().map(|t| &t.name)
        );
        self.compiling_triggers.write().pop();
    }

    /// Check if a specific trigger is currently executing (for recursive trigger prevention)
    pub fn is_trigger_executing(&self, trigger: &Arc<Trigger>) -> bool {
        let executing = self.executing_triggers.read();
        if let Some(trigger) = executing.iter().find(|t| t.name == trigger.name) {
            tracing::debug!("Trigger is already executing: {}", trigger.name);
            return true;
        }
        false
    }

    pub fn start_trigger_execution(&self, trigger: Arc<Trigger>) {
        tracing::debug!("Starting trigger execution: {}", trigger.name);
        self.executing_triggers.write().push(trigger);
    }

    pub fn end_trigger_execution(&self) {
        tracing::debug!(
            "Ending trigger execution: {:?}",
            self.executing_triggers.read().last().map(|t| &t.name)
        );
        self.executing_triggers.write().pop();
    }

    fn should_retry_cross_process_schema_lookup(
        self: &Arc<Connection>,
        err: &LimboError,
    ) -> Result<bool> {
        let LimboError::ParseError(msg) = err else {
            return Ok(false);
        };
        if !msg.contains("no such table") && !msg.contains("table not found") {
            return Ok(false);
        }
        if self.get_tx_state() != TransactionState::None {
            return Ok(false);
        }
        if self.db.shared_wal_coordination()?.is_none() {
            return Ok(false);
        }
        self.maybe_reparse_schema()?;
        Ok(true)
    }

    fn compile_cmd(
        self: &Arc<Connection>,
        cmd: Cmd,
        input: &str,
    ) -> Result<(Program, Arc<Pager>, QueryMode)> {
        self.maybe_update_schema();
        let syms = self.syms.read();
        let pager = self.pager.load().clone();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
        let schema = self.schema.read().clone();
        match translate::translate(
            &schema,
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        ) {
            Ok(program) => Ok((program, pager, mode)),
            Err(err) if self.should_retry_cross_process_schema_lookup(&err)? => {
                // Cold path: re-parse the SQL from scratch after schema refresh rather
                // than cloning the original AST, which can overflow the stack
                // on deeply nested expression trees.
                drop(syms);
                let mut parser = Parser::new(input.as_bytes());
                let Some(cmd) = parser.next_cmd()? else {
                    return Err(err);
                };
                self.maybe_update_schema();
                let syms = self.syms.read();
                let pager = self.pager.load().clone();
                let mode = QueryMode::new(&cmd);
                let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
                let schema = self.schema.read().clone();
                translate::translate(
                    &schema,
                    stmt,
                    pager.clone(),
                    self.clone(),
                    &syms,
                    mode,
                    input,
                )
                .map(|program| (program, pager, mode))
            }
            Err(err) => Err(err),
        }
    }

    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self._prepare(sql)
    }

    pub(crate) fn prepare_internal(
        self: &Arc<Connection>,
        sql: impl AsRef<str>,
    ) -> Result<Statement> {
        self.prepare_with_origin(sql, StatementOrigin::InternalHelper)
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn _prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self.prepare_with_origin(sql, StatementOrigin::Root)
    }

    fn prepare_with_origin(
        self: &Arc<Connection>,
        sql: impl AsRef<str>,
        origin: StatementOrigin,
    ) -> Result<Statement> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }

        let needs_nested_guard = origin.needs_nested_guard();
        if needs_nested_guard {
            self.start_nested();
        }
        let result = (|| {
            let sql = sql.as_ref();
            tracing::debug!("Preparing: {}", sql);
            let mut parser = Parser::new(sql.as_bytes());
            let cmd = match parser.next_cmd()? {
                Some(cmd) => cmd,
                None => {
                    return Err(LimboError::InvalidArgument(
                        "The supplied SQL string contains no statements".to_string(),
                    ));
                }
            };
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let (program, pager, mode) = self.compile_cmd(cmd, input)?;
            Ok(Statement::new_with_origin(
                program,
                pager,
                mode,
                byte_offset_end,
                origin,
                needs_nested_guard,
            ))
        })();
        if result.is_err() && needs_nested_guard {
            self.end_nested();
        }
        result
    }

    /// Prepare a statement from an AST node directly, skipping SQL parsing.
    /// This is more efficient when AST is already available or constructed programmatically.
    pub fn prepare_stmt(self: &Arc<Connection>, stmt: ast::Stmt) -> Result<Statement> {
        self.prepare_stmt_with_origin(stmt, StatementOrigin::Root)
    }

    fn prepare_stmt_with_origin(
        self: &Arc<Connection>,
        stmt: ast::Stmt,
        origin: StatementOrigin,
    ) -> Result<Statement> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let needs_nested_guard = origin.needs_nested_guard();
        if needs_nested_guard {
            self.start_nested();
        }
        let result = (|| {
            self.maybe_update_schema();
            let syms = self.syms.read();
            let pager = self.pager.load().clone();
            let mode = QueryMode::Normal;
            let schema = self.schema.read().clone();
            let program = translate::translate(
                &schema,
                stmt,
                pager.clone(),
                self.clone(),
                &syms,
                mode,
                "<ast>", // No SQL input string available
            )?;
            Ok(Statement::new_with_origin(
                program,
                pager,
                mode,
                0,
                origin,
                needs_nested_guard,
            ))
        })();
        if result.is_err() && needs_nested_guard {
            self.end_nested();
        }
        result
    }

    /// Whether this is an internal connection used for MVCC bootstrap
    pub fn is_mvcc_bootstrap_connection(&self) -> bool {
        self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst)
    }

    /// Promote MVCC bootstrap connection to a regular connection so it reads from the MV store again.
    pub fn promote_to_regular_connection(&self) {
        assert!(self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst));
        self.is_mvcc_bootstrap_connection
            .store(false, Ordering::SeqCst);
    }

    /// Demote regular connection to MVCC bootstrap connection so it does not read from the MV store.
    pub fn demote_to_mvcc_connection(&self) {
        assert!(!self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst));
        self.is_mvcc_bootstrap_connection
            .store(true, Ordering::SeqCst);
    }

    /// Parse schema from scratch if version of schema for the connection differs from the schema cookie in the root page.
    /// This function must be called outside of any transaction because internally it will start transaction session by itself.
    /// In multi-process mode, this is the only way to discover schema changes made by other processes.
    pub fn maybe_reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.load().clone();
        let mv_store = self.mv_store();

        // maybe_reparse_schema must be called outside any explicit transaction
        // because it starts its own read transaction to load a fresh view of
        // sqlite_schema from disk.
        if self.get_tx_state() != TransactionState::None {
            return Ok(());
        }

        if self.db.shared_wal_coordination()?.is_some() {
            // Cross-process schema changes can leave page 1 and sqlite_schema
            // pages cached from an earlier WAL snapshot. Drop the cache before
            // probing the cookie so reparsing observes the current committed view.
            pager.clear_page_cache(false);
            pager.set_schema_cookie(None);
        }

        let on_disk_schema_version = if mv_store.as_ref().is_some() {
            self.read_current_schema_cookie().or_else(|err| match err {
                LimboError::Page1NotAlloc => Ok(0),
                other => Err(other),
            })?
        } else {
            // first, quickly read schema_version from the root page in order to check if schema changed
            pager.begin_read_tx()?;
            let on_disk_schema_version = pager
                .io
                .block(|| pager.with_header(|header| header.schema_cookie));

            let on_disk_schema_version = match on_disk_schema_version {
                Ok(db_schema_version) => db_schema_version.get(),
                Err(LimboError::Page1NotAlloc) => {
                    // this means this is a fresh db, so return a schema version of 0
                    0
                }
                Err(err) => {
                    pager.end_read_tx();
                    return Err(err);
                }
            };
            pager.end_read_tx();
            on_disk_schema_version
        };

        let db_schema_version = self.db.schema.lock().schema_version;
        tracing::debug!(
            "path: {}, db_schema_version={} vs on_disk_schema_version={}",
            self.db.path,
            db_schema_version,
            on_disk_schema_version
        );
        // if schema_versions matches - exit early
        if db_schema_version == on_disk_schema_version {
            return Ok(());
        }
        // start read transaction manually, because we will read schema cookie once again and
        // we must be sure that it will consistent with schema content
        //
        // from now on we must be very careful with errors propagation
        // in order to not accidentally keep read transaction opened
        pager.begin_read_tx()?;
        self.set_tx_state(TransactionState::Read);

        let reparse_result = self.reparse_schema();

        let previous = self.transaction_state.swap(TransactionState::None);
        turso_assert!(
            matches!(previous, TransactionState::None | TransactionState::Read),
            "unexpected end transaction state"
        );
        // close opened transaction if it was kept open
        // (in most cases, it will be automatically closed if stmt was executed properly)
        if previous == TransactionState::Read {
            pager.end_read_tx();
        }

        reparse_result?;

        let schema = self.schema.read().clone();
        self.db.update_schema_if_newer(schema);
        Ok(())
    }

    pub(crate) fn reparse_schema(self: &Arc<Connection>) -> Result<()> {
        // read cookie before consuming statement program - otherwise we can end up reading cookie with closed transaction state
        let cookie = self.read_current_schema_cookie()?;
        self.reparse_schema_with_cookie(cookie)
    }

    pub(crate) fn reparse_schema_with_cookie(self: &Arc<Connection>, cookie: u32) -> Result<()> {
        // create fresh schema as some objects can be deleted
        let mut fresh = Schema::with_options(self.experimental_custom_types_enabled());
        fresh.generated_columns_enabled = self.db.experimental_generated_columns_enabled();
        fresh.schema_version = cookie;

        // Capture built-in table-valued functions (e.g. generate_series, json_each)
        // before dropping the old schema. These are registered programmatically and
        // don't survive re-parsing from sqlite_schema alone.
        let table_valued_functions: Vec<_> = self
            .schema
            .read()
            .tables
            .values()
            .filter_map(|table| match table.as_ref() {
                crate::schema::Table::Virtual(vtab)
                    if matches!(vtab.kind, turso_ext::VTabKind::TableValuedFunction) =>
                {
                    Some(vtab.clone())
                }
                _ => None,
            })
            .collect();

        // TODO: this is hack to avoid a cyclical problem with schema reprepare
        // The problem here is that we prepare a statement here, but when the statement tries
        // to execute it, it first checks the schema cookie to see if it needs to reprepare the statement.
        // But in this occasion it will always reprepare, and we get an error. So we trick the statement by swapping our schema
        // with a new clean schema that has the same header cookie.
        self.with_schema_mut(|schema| {
            *schema = fresh.clone();
        });

        let stmt = self.prepare("SELECT * FROM sqlite_schema")?;

        // MVCC bootstrap connection gets the "baseline" from the DB file and ignores anything in MV store
        let mv_tx = if self.is_mvcc_bootstrap_connection() {
            None
        } else {
            self.get_mv_tx()
        };
        // Resolver so attached-db qualifiers in temp triggers can be
        // mapped to their actual index on this connection (Phase 1.4c).
        let attached_resolver = |name: &str| -> Option<usize> {
            self.attached_databases
                .read()
                .get_database_by_name(&crate::util::normalize_ident(name))
                .map(|(idx, _)| idx)
        };
        // TODO: This function below is synchronous, make it async
        parse_schema_rows(
            stmt,
            &mut fresh,
            &self.syms.read(),
            mv_tx,
            &attached_resolver,
        )?;

        // Rehydrate built-in table-valued functions that were captured above.
        for vtab in &table_valued_functions {
            let normalized = crate::util::normalize_ident(&vtab.name);
            fresh
                .tables
                .entry(normalized)
                .or_insert_with(|| Arc::new(crate::schema::Table::Virtual(vtab.clone())));
        }

        // Load custom types from __turso_internal_types if the table exists
        // and custom types are enabled. Type loading errors are non-fatal: we log
        // warnings and continue with whatever types loaded successfully.
        if self.experimental_custom_types_enabled()
            && fresh
                .tables
                .contains_key(crate::schema::TURSO_TYPES_TABLE_NAME)
        {
            // Temporarily install the schema so we can prepare a query against it
            self.with_schema_mut(|schema| {
                *schema = fresh.clone();
            });
            let load_result: Result<()> = (|| {
                let type_sqls = self.query_stored_type_definitions()?;
                fresh.load_type_definitions(&type_sqls)?;
                Ok(())
            })();
            if let Err(e) = load_result {
                tracing::warn!("Failed to load custom types: {}", e);
            }
        }

        // Best-effort load stats if sqlite_stat1 is present and DB is initialized.
        refresh_analyze_stats(self);

        tracing::debug!(
            "reparse_schema: schema_version={}, tables={:?}",
            fresh.schema_version,
            fresh.tables.keys()
        );
        self.with_schema_mut(|schema| {
            *schema = fresh;
        });
        Result::Ok(())
    }

    pub(crate) fn read_current_schema_cookie(&self) -> Result<u32> {
        if let Some(mv_store) = self.mv_store().as_ref() {
            let tx_id = self.get_mv_tx_id();
            mv_store.with_header(|header| header.schema_cookie.get(), tx_id.as_ref())
        } else {
            let pager = self.pager.load();
            pager
                .io
                .block(|| pager.with_header(|header| header.schema_cookie))
                .map(|cookie| cookie.get())
        }
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn prepare_execute_batch(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }
        let sql = sql.as_ref();
        tracing::trace!("Preparing and executing batch: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next_cmd()? {
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let (program, pager, mode) = self.compile_cmd(cmd, input)?;
            Statement::new(program, pager.clone(), mode, 0).run_ignore_rows()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn query(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        tracing::trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        match cmd {
            Some(cmd) => self.run_cmd(cmd, input),
            None => Ok(None),
        }
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub(crate) fn run_cmd(
        self: &Arc<Connection>,
        cmd: Cmd,
        input: &str,
    ) -> Result<Option<Statement>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let (program, pager, mode) = self.compile_cmd(cmd, input)?;
        let stmt = Statement::new(program, pager, mode, 0);
        Ok(Some(stmt))
    }

    pub fn query_runner<'a>(self: &'a Arc<Connection>, sql: &'a [u8]) -> QueryRunner<'a> {
        QueryRunner::new(self, sql)
    }

    /// Execute will run a query from start to finish taking ownership of I/O because it will run pending I/Os if it didn't finish.
    /// TODO: make this api async
    #[instrument(skip_all, level = Level::INFO)]
    pub fn execute(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next_cmd()? {
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let (program, pager, mode) = self.compile_cmd(cmd, input)?;
            Statement::new(program, pager.clone(), mode, 0).run_ignore_rows()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn consume_stmt(
        self: &Arc<Connection>,
        sql: impl AsRef<str>,
    ) -> Result<Option<(Statement, usize)>> {
        let mut parser = Parser::new(sql.as_ref().as_bytes());
        let Some(cmd) = parser.next_cmd()? else {
            return Ok(None);
        };
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_ref().as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        let (program, pager, mode) = self.compile_cmd(cmd, input)?;
        let stmt = Statement::new(program, pager, mode, 0);
        Ok(Some((stmt, parser.offset())))
    }

    #[cfg(feature = "fs")]
    pub fn from_uri(uri: &str, db_opts: DatabaseOpts) -> Result<(Arc<dyn IO>, Arc<Connection>)> {
        use crate::util::MEMORY_PATH;
        let opts = OpenOptions::parse(uri)?;
        let flags = opts.get_flags()?;
        if opts.path == MEMORY_PATH || matches!(opts.mode, OpenMode::Memory) {
            let io = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(io.clone(), MEMORY_PATH, flags, db_opts, None)?;
            let conn = db.connect()?;
            return Ok((io, conn));
        }
        let encryption_opts = match (opts.cipher.clone(), opts.hexkey.clone()) {
            (Some(cipher), Some(hexkey)) => Some(EncryptionOpts { cipher, hexkey }),
            (Some(_), None) => {
                return Err(LimboError::InvalidArgument(
                    "hexkey is required when cipher is provided".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(LimboError::InvalidArgument(
                    "cipher is required when hexkey is provided".to_string(),
                ));
            }
            (None, None) => None,
        };
        let (io, db) = Database::open_new(
            &opts.path,
            opts.vfs.as_ref(),
            flags,
            db_opts,
            encryption_opts,
        )?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof).map_err(|e| io_error(e, "metadata"))?;
            std::fs::set_permissions(&opts.path, perms.permissions())
                .map_err(|e| io_error(e, "set_permissions"))?;
        }
        let conn = db.connect()?;
        if let Some(cipher) = opts.cipher {
            let _ = conn.pragma_update("cipher", format!("'{cipher}'"));
        }
        if let Some(hexkey) = opts.hexkey {
            let _ = conn.pragma_update("hexkey", format!("'{hexkey}'"));
        }
        Ok((io, conn))
    }

    #[cfg(feature = "fs")]
    fn from_uri_attached(
        uri: &str,
        mut db_opts: DatabaseOpts,
        main_db_flags: OpenFlags,
        io: Arc<dyn IO>,
    ) -> Result<(Arc<Database>, Option<EncryptionOpts>)> {
        let opts = OpenOptions::parse(uri)?;
        let mut flags = opts.get_flags()?;
        if main_db_flags.contains(OpenFlags::ReadOnly) {
            flags |= OpenFlags::ReadOnly;
        }
        let encryption_opts = match (opts.cipher.clone(), opts.hexkey.clone()) {
            (Some(cipher), Some(hexkey)) => Some(EncryptionOpts { cipher, hexkey }),
            (Some(_), None) => {
                return Err(LimboError::InvalidArgument(
                    "hexkey is required when cipher is provided".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(LimboError::InvalidArgument(
                    "cipher is required when hexkey is provided".to_string(),
                ));
            }
            (None, None) => None,
        };
        if encryption_opts.is_some() {
            db_opts = db_opts.with_encryption(true);
        }
        let io = opts.vfs.map(Database::io_for_vfs).unwrap_or(Ok(io))?;
        let db = Database::open_file_with_flags(
            io.clone(),
            &opts.path,
            flags,
            db_opts,
            encryption_opts.clone(),
        )?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof).map_err(|e| io_error(e, "metadata"))?;
            std::fs::set_permissions(&opts.path, perms.permissions())
                .map_err(|e| io_error(e, "set_permissions"))?;
        }
        Ok((db, encryption_opts))
    }

    pub fn set_foreign_keys_enabled(&self, enable: bool) {
        self.fk_pragma.store(enable, Ordering::Release);
        self.bump_prepare_context_generation();
    }

    pub fn foreign_keys_enabled(&self) -> bool {
        self.fk_pragma.load(Ordering::Acquire)
    }

    pub fn set_check_constraints_ignored(&self, ignore: bool) {
        self.check_constraints_pragma
            .store(ignore, Ordering::Release);
    }

    pub fn check_constraints_ignored(&self) -> bool {
        self.check_constraints_pragma.load(Ordering::Acquire)
    }

    pub(crate) fn clear_deferred_foreign_key_violations(&self) -> isize {
        self.fk_deferred_violations.swap(0, Ordering::Release)
    }

    pub(crate) fn get_deferred_foreign_key_violations(&self) -> isize {
        self.fk_deferred_violations.load(Ordering::Acquire)
    }

    pub(crate) fn increment_deferred_foreign_key_violations(&self, v: isize) {
        self.fk_deferred_violations.fetch_add(v, Ordering::AcqRel);
    }

    /// Query the CREATE TYPE SQL definitions stored in __turso_internal_types.
    /// The connection's schema must already contain the table definitions so
    /// that `prepare` can resolve the table name. Returns an empty Vec if the
    /// types table does not exist.
    pub(crate) fn query_stored_type_definitions(self: &Arc<Connection>) -> Result<Vec<String>> {
        let has_types_table = {
            let s = self.schema.read();
            s.tables.contains_key(crate::schema::TURSO_TYPES_TABLE_NAME)
        };
        if !has_types_table {
            return Ok(Vec::new());
        }
        let mut type_stmt = self.prepare(format!(
            "SELECT name, sql FROM {}",
            crate::schema::TURSO_TYPES_TABLE_NAME
        ))?;
        let mut type_rows = Vec::new();
        type_stmt.run_with_row_callback(|row| {
            type_rows.push(row.get::<&str>(1)?.to_string());
            Ok(())
        })?;
        Ok(type_rows)
    }

    pub fn maybe_update_schema(&self) {
        let current_schema_version = self.schema.read().schema_version;
        let schema = self.db.schema.lock();
        if matches!(self.get_tx_state(), TransactionState::None)
            && self.get_mv_tx().is_none()
            && self.next_attached_mv_tx().is_none()
            && current_schema_version != schema.schema_version
        {
            *self.schema.write() = schema.clone();
        }
    }

    /// Read schema version at current transaction
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn read_schema_version(&self) -> Result<u32> {
        let pager = self.pager.load();
        pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie))
            .map(|version| version.get())
    }

    /// Update schema version to the new value within opened write transaction
    ///
    /// New version of the schema must be strictly greater than previous one - otherwise method will panic
    /// Write transaction must be opened in advance - otherwise method will panic
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn write_schema_version(self: &Arc<Connection>, version: u32) -> Result<()> {
        let TransactionState::Write { .. } = self.get_tx_state() else {
            return Err(LimboError::InternalError(
                "write_schema_version must be called from within Write transaction".to_string(),
            ));
        };
        let pager = self.pager.load();
        pager.io.block(|| {
            pager.with_header_mut(|header| {
                turso_assert!(
                    header.schema_cookie.get() < version,
                    "cookie can't go back in time"
                );
                self.set_tx_state(TransactionState::Write {
                    schema_did_change: true,
                });
                self.with_schema_mut(|schema| schema.schema_version = version);
                header.schema_cookie = version.into();
            })
        })?;
        self.reparse_schema()?;
        Ok(())
    }

    /// Try to read page with given ID with fixed WAL watermark position
    /// This method return false if page is not found (so, this is probably new page created after watermark position which wasn't checkpointed to the DB file yet)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page(
        &self,
        page_idx: u32,
        page: &mut [u8],
        frame_watermark: Option<u64>,
    ) -> Result<bool> {
        let Some((page_ref, c)) =
            self.try_wal_watermark_read_page_begin(page_idx, frame_watermark)?
        else {
            return Ok(false);
        };
        match self.get_pager().io.wait_for_completion(c) {
            #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
            Err(LimboError::CompletionError(crate::error::CompletionError::IOError(
                std::io::ErrorKind::UnexpectedEof,
                _,
            ))) => {
                return Ok(false);
            }
            Err(e) => return Err(e),
            _ => {}
        }

        self.try_wal_watermark_read_page_end(page, page_ref)
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page_begin(
        &self,
        page_idx: u32,
        frame_watermark: Option<u64>,
    ) -> Result<Option<(Arc<Page>, Completion)>> {
        let pager = self.pager.load();
        let (page_ref, c) = match pager.read_page_no_cache(page_idx as i64, frame_watermark, true) {
            Ok(result) => result,
            // on windows, zero read will trigger UnexpectedEof
            #[cfg(target_os = "windows")]
            Err(LimboError::CompletionError(crate::error::CompletionError::IOError(
                std::io::ErrorKind::UnexpectedEof,
                _,
            ))) => return Ok(None),
            Err(err) => return Err(err),
        };

        Ok(Some((page_ref, c)))
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page_end(
        &self,
        page: &mut [u8],
        page_ref: Arc<Page>,
    ) -> Result<bool> {
        let content = page_ref.get_contents();
        // empty read - attempt to read absent page
        if content.buffer.as_ref().is_none_or(|b| b.is_empty()) {
            return Ok(false);
        }
        page.copy_from_slice(content.as_ptr());
        Ok(true)
    }

    /// Return unique set of page numbers changes after WAL watermark position in the current WAL session
    /// (so, if concurrent connection wrote something to the WAL - this method will not see this change)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        self.pager.load().wal_changed_pages_after(frame_watermark)
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_state(&self) -> Result<WalState> {
        self.pager.load().wal_state()
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<WalFrameInfo> {
        use crate::storage::sqlite3_ondisk::parse_wal_frame_header;

        let c = self.pager.load().wal_get_frame(frame_no, frame)?;
        self.db.io.wait_for_completion(c)?;
        let (header, _) = parse_wal_frame_header(frame);
        Ok(WalFrameInfo {
            page_no: header.page_number,
            db_size: header.db_size,
        })
    }

    /// Insert `frame` (header included) at the position `frame_no` in the WAL
    /// If WAL already has frame at that position - turso-db will compare content of the page and either report conflict or return OK
    /// If attempt to write frame at the position `frame_no` will create gap in the WAL - method will return error
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        self.pager.load().wal_insert_frame(frame_no, frame)
    }

    /// Start WAL session by initiating read+write transaction for this connection
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_begin(&self) -> Result<()> {
        let pager = self.pager.load();
        pager.begin_read_tx()?;
        pager.io.block(|| pager.begin_write_tx()).inspect_err(|_| {
            pager.end_read_tx();
        })?;

        // start write transaction and disable auto-commit mode as SQL can be executed within WAL session (at caller own risk)
        self.set_tx_state(TransactionState::Write {
            schema_did_change: false,
        });
        self.auto_commit.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Finish WAL session by ending read+write transaction taken in the [Self::wal_insert_begin] method
    /// All frames written after last commit frame (db_size > 0) within the session will be rolled back
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_end(self: &Arc<Connection>, force_commit: bool) -> Result<()> {
        use crate::{return_if_io, types::IOResult};

        {
            let pager = self.pager.load();

            let Some(wal) = pager.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "wal_insert_end called without a wal".to_string(),
                ));
            };

            let commit_err = if force_commit {
                pager
                    .io
                    .block(|| {
                        return_if_io!(pager.commit_dirty_pages(
                            true,
                            self.get_sync_mode(),
                            self.get_data_sync_retry(),
                        ));
                        pager.commit_dirty_pages_end();
                        Ok(IOResult::Done(()))
                    })
                    .err()
            } else {
                None
            };

            self.auto_commit.store(true, Ordering::SeqCst);
            self.set_tx_state(TransactionState::None);
            wal.end_write_tx();
            wal.end_read_tx();

            if !force_commit {
                // remove all non-commited changes in case if WAL session left some suffix without commit frame
                if let Some(mv_store) = self.mv_store().as_ref() {
                    if let Some(tx_id) = self.get_mv_tx_id() {
                        mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
                    }
                }
                pager.rollback(false, self, true);
            }
            if let Some(err) = commit_err {
                return Err(err);
            }
        }

        // let's re-parse schema from scratch if schema cookie changed compared to the our in-memory view of schema
        self.maybe_reparse_schema()?;
        Ok(())
    }

    /// Flush dirty pages to disk.
    pub fn cacheflush(&self) -> Result<Vec<Completion>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pager = self.pager.load();
        pager.io.block(|| pager.cacheflush())
    }

    pub fn checkpoint(self: &Arc<Self>, mode: CheckpointMode) -> Result<CheckpointResult> {
        use crate::mvcc::database::CheckpointStateMachine;
        use crate::state_machine::{StateTransition, TransitionResult};
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if let Some(mv_store) = self.mv_store().as_ref() {
            let pager = self.pager.load().clone();
            let io = pager.io.clone();
            let mut ckpt_sm = CheckpointStateMachine::new(
                pager,
                mv_store.clone(),
                self.clone(),
                true,
                self.get_sync_mode(),
            );
            loop {
                match ckpt_sm.step(&()) {
                    Ok(TransitionResult::Continue) => {}
                    Ok(TransitionResult::Done(result)) => return Ok(result),
                    Ok(TransitionResult::Io(iocompletions)) => {
                        if let Err(err) = iocompletions.wait(io.as_ref()) {
                            ckpt_sm.cleanup_after_external_io_error();
                            return Err(err);
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
        } else {
            self.pager
                .load()
                .blocking_checkpoint(mode, self.get_sync_mode())
        }
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }
        self.closed.store(true, Ordering::SeqCst);
        let pager = self.pager.load();

        match self.get_tx_state() {
            TransactionState::None => {
                // No active transaction
            }
            _ => {
                if self.mvcc_enabled() {
                    if let Some(mv_store) = self.mv_store().as_ref() {
                        if let Some(tx_id) = self.get_mv_tx_id() {
                            mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
                        }
                    }
                    pager.end_read_tx();
                } else {
                    pager.rollback_tx(self);
                }
                // Roll back all attached DB transactions regardless of main
                // DB mode — a :memory: attached DB may use WAL even when the
                // main DB uses MVCC.
                self.rollback_attached_mvcc_txs(false);
                self.rollback_attached_wal_txns();
                self.set_tx_state(TransactionState::None);
            }
        }

        let is_memory_db = self.db.path == ":memory:"
            || self.db.path.starts_with("file::memory:")
            || self.db.path.is_empty();
        let should_checkpoint_on_close = pager
            .wal
            .as_ref()
            .is_none_or(|wal| wal.should_checkpoint_on_close());
        if self.db.n_connections.fetch_sub(1, Ordering::SeqCst).eq(&1)
            && !self.db.is_readonly()
            && !is_memory_db
            && should_checkpoint_on_close
        {
            self.pager.load().checkpoint_shutdown(
                self.is_wal_auto_checkpoint_disabled(),
                self.get_sync_mode(),
            )?;
        };
        Ok(())
    }

    pub fn wal_auto_checkpoint_disable(&self) {
        self.wal_auto_checkpoint_disabled
            .store(true, Ordering::SeqCst);
    }

    pub fn is_wal_auto_checkpoint_disabled(&self) -> bool {
        self.wal_auto_checkpoint_disabled.load(Ordering::SeqCst) || self.db.get_mv_store().is_some()
    }

    #[cfg(feature = "simulator")]
    pub fn checkpoint_for_testing(&self, mode: CheckpointMode) -> Result<CheckpointResult> {
        let pager = self.pager.load();
        pager
            .io
            .block(|| pager.checkpoint(mode, SyncMode::Full, true))
    }

    #[cfg(all(feature = "simulator", target_pointer_width = "64", unix))]
    pub fn install_unpublished_backfill_proof_for_testing(
        &self,
        upper_bound_inclusive: u64,
    ) -> Result<()> {
        let pager = self.pager.load();
        let proof_nbackfills =
            pager.run_checkpoint_until_post_sync_gap_for_testing(CheckpointMode::Passive {
                upper_bound_inclusive: Some(upper_bound_inclusive),
            })?;
        let authority = self.db.shared_wal_coordination()?.ok_or_else(|| {
            LimboError::InternalError("shared WAL authority is unavailable".into())
        })?;
        let snapshot_before_publish = authority.snapshot();
        if snapshot_before_publish.nbackfills != 0 {
            return Err(LimboError::InternalError(
                "unpublished-proof setup requires nbackfills to remain unpublished".into(),
            ));
        }

        let (db_size_pages, db_header_crc32c) = db_identity_for_testing(Path::new(&self.db.path))?;
        authority.install_backfill_proof(
            crate::storage::shared_wal_coordination::SharedWalCoordinationHeader {
                nbackfills: proof_nbackfills,
                ..snapshot_before_publish
            },
            db_size_pages,
            db_header_crc32c,
        );
        Ok(())
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid.load(Ordering::SeqCst)
    }

    pub(crate) fn update_last_rowid(&self, rowid: i64) {
        self.last_insert_rowid.store(rowid, Ordering::SeqCst);
    }

    /// Sets the value of `changes()`, but without altering `total_changes()`.
    pub(crate) fn set_changes_without_total(&self, num_changes: i64) {
        self.changes.store(num_changes, Ordering::SeqCst);
    }

    pub(crate) fn add_total_changes(&self, num_changes: i64) {
        self.total_changes.fetch_add(num_changes, Ordering::SeqCst);
    }

    pub fn set_changes(&self, num_changes: i64) {
        self.set_changes_without_total(num_changes);
        self.add_total_changes(num_changes);
    }

    pub fn changes(&self) -> i64 {
        self.changes.load(Ordering::SeqCst)
    }

    pub fn total_changes(&self) -> i64 {
        self.total_changes.load(Ordering::SeqCst)
    }

    pub fn get_cache_size(&self) -> i32 {
        self.cache_size.load(Ordering::SeqCst)
    }
    pub fn set_cache_size(&self, size: i32) {
        self.cache_size.store(size, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>> {
        self.capture_data_changes.read()
    }
    pub fn set_capture_data_changes_info(&self, opts: Option<CaptureDataChangesInfo>) {
        *self.capture_data_changes.write() = opts;
        self.bump_prepare_context_generation();
    }
    pub fn get_cdc_transaction_id(&self) -> i64 {
        self.cdc_transaction_id.load(Ordering::SeqCst)
    }
    pub fn set_cdc_transaction_id(&self, id: i64) {
        self.cdc_transaction_id.store(id, Ordering::SeqCst);
    }
    pub fn get_page_size(&self) -> PageSize {
        let value = self.page_size.load(Ordering::SeqCst);
        PageSize::new_from_header_u16(value).unwrap_or_default()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn is_query_only(&self) -> bool {
        self.query_only.load(Ordering::SeqCst)
    }

    pub fn get_database_canonical_path(&self) -> String {
        if self.db.path == ":memory:" {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&self.db.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => self.db.path.to_string(),
            }
        }
    }

    /// Check if a specific attached database is read only or not, by its index
    pub fn is_readonly(&self, index: usize) -> bool {
        match index {
            crate::MAIN_DB_ID => self.db.is_readonly(),
            crate::TEMP_DB_ID => self
                .temp
                .database
                .read()
                .as_ref()
                .is_some_and(|temp_db| temp_db.db.is_readonly()),
            _ => {
                let db = self.attached_databases.read().get_database_by_index(index);
                db.expect("Should never have called this without being sure the database exists")
                    .is_readonly()
            }
        }
    }

    /// Reset the page size for the current connection.
    ///
    /// Specifying a new page size does not change the page size immediately.
    /// Instead, the new page size is remembered and is used to set the page size when the database
    /// is first created, if it does not already exist when the page_size pragma is issued,
    /// or at the next VACUUM command that is run on the same database connection while not in WAL mode.
    pub fn reset_page_size(&self, size: u32) -> Result<()> {
        if self.db.initialized() {
            return Ok(());
        }
        let Some(size) = PageSize::new(size) else {
            return Ok(());
        };

        self.page_size.store(size.get_raw(), Ordering::SeqCst);
        self.pager.load().set_initial_page_size(size)?;
        self.bump_prepare_context_generation();

        Ok(())
    }

    #[cfg(feature = "fs")]
    pub fn open_new(&self, path: &str, vfs: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        Database::open_with_vfs(&self.db, path, vfs)
    }

    pub fn list_vfs(&self) -> Vec<String> {
        #[allow(unused_mut)]
        let mut all_vfs = vec![String::from("memory")];
        #[cfg(feature = "fs")]
        {
            #[cfg(target_family = "unix")]
            {
                all_vfs.push("syscall".to_string());
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            {
                all_vfs.push("io_uring".to_string());
            }
            #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
            {
                all_vfs.push("experimental_win_iocp".to_string());
            }
            all_vfs.extend(crate::ext::list_vfs_modules());
        }
        all_vfs
    }

    pub fn get_auto_commit(&self) -> bool {
        self.auto_commit.load(Ordering::SeqCst)
    }

    pub fn reparse_schema_after_extension_load(self: &Arc<Connection>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        // Collect row data from the Statement first, then drop the Statement
        // before taking the schema write lock. This prevents a deadlock in MVCC
        // mode where Statement::drop -> abort -> rollback_tx -> schema.read()
        // would deadlock against the schema write lock.
        let mut rows_data: Vec<(String, String, String, i64, Option<String>)> = Vec::new();
        {
            let mut rows = self
                .query("SELECT * FROM sqlite_schema")?
                .expect("query must be parsed to statement");
            rows.run_with_row_callback(|row| {
                let ty = row.get::<&str>(0)?.to_string();
                let name = row.get::<&str>(1)?.to_string();
                let table_name = row.get::<&str>(2)?.to_string();
                let root_page = row.get::<i64>(3)?;
                let sql = row.get::<&str>(4).ok().map(|s| s.to_string());
                rows_data.push((ty, name, table_name, root_page, sql));
                Ok(())
            })?;
        } // Statement dropped here, before schema write lock

        let syms = self.syms.read();
        self.with_schema_mut(|schema| -> Result<()> {
            // Incremental re-parse after extension loading. The schema already has
            // tables/indices/views from initial parse. We only need to pick up
            // entries that previously failed (e.g. virtual tables whose module
            // wasn't loaded yet). "Already exists" errors are expected and skipped.
            let mut from_sql_indexes = Vec::new();
            let mut automatic_indices = HashMap::default();
            let mut dbsp_state_roots = HashMap::default();
            let mut dbsp_state_index_roots = HashMap::default();
            let mut materialized_view_info = HashMap::default();

            let attached_resolver = |name: &str| -> Option<usize> {
                self.attached_databases
                    .read()
                    .get_database_by_name(&crate::util::normalize_ident(name))
                    .map(|(idx, _)| idx)
            };
            for (ty, name, table_name, root_page, sql) in &rows_data {
                match schema.handle_schema_row(
                    ty,
                    name,
                    table_name,
                    *root_page,
                    sql.as_deref(),
                    &syms,
                    &mut from_sql_indexes,
                    &mut automatic_indices,
                    &mut dbsp_state_roots,
                    &mut dbsp_state_index_roots,
                    &mut materialized_view_info,
                    &attached_resolver,
                ) {
                    Ok(()) => {}
                    Err(LimboError::ParseError(msg)) if msg.contains("already exists") => {}
                    Err(LimboError::ExtensionError(msg)) => {
                        eprintln!("Warning: {msg}");
                    }
                    Err(e) => return Err(e),
                }
            }

            match schema.populate_indices(&syms, from_sql_indexes, automatic_indices, false) {
                Ok(()) => {}
                Err(LimboError::ParseError(msg)) if msg.contains("already exists") => {}
                Err(LimboError::ExtensionError(msg)) => eprintln!("Warning: {msg}"),
                Err(e) => return Err(e),
            }
            match schema.populate_materialized_views(
                materialized_view_info,
                dbsp_state_roots,
                dbsp_state_index_roots,
            ) {
                Ok(()) => {}
                Err(LimboError::ExtensionError(msg)) => eprintln!("Warning: {msg}"),
                Err(e) => return Err(e),
            }
            Ok(())
        })
    }

    // Clearly there is something to improve here, Vec<Vec<Value>> isn't a couple of tea
    /// Query the current rows/values of `pragma_name`.
    pub fn pragma_query(self: &Arc<Connection>, pragma_name: &str) -> Result<Vec<Vec<Value>>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}");
        let mut stmt = self.prepare(pragma)?;
        stmt.run_collect_rows()
    }

    /// Set a new value to `pragma_name`.
    ///
    /// Some pragmas will return the updated value which cannot be retrieved
    /// with this method.
    pub fn pragma_update<V: Display>(
        self: &Arc<Connection>,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Vec<Value>>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name} = {pragma_value}");
        let mut stmt = self.prepare(pragma)?;
        stmt.run_collect_rows()
    }

    pub fn experimental_views_enabled(&self) -> bool {
        self.db.experimental_views_enabled()
    }

    pub fn experimental_index_method_enabled(&self) -> bool {
        self.db.experimental_index_method_enabled()
    }

    pub fn experimental_custom_types_enabled(&self) -> bool {
        self.db.experimental_custom_types_enabled()
    }

    pub fn experimental_attach_enabled(&self) -> bool {
        self.db.experimental_attach_enabled()
    }

    pub fn experimental_generated_columns_enabled(&self) -> bool {
        self.db.experimental_generated_columns_enabled()
    }

    pub fn mvcc_enabled(&self) -> bool {
        self.db.mvcc_enabled()
    }

    pub fn mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        struct TransparentWrapper<T>(T);

        impl<T> Deref for TransparentWrapper<T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        // Never use MV store for bootstrapping - we read state directly from sqlite_schema in the DB file.
        if !self.is_mvcc_bootstrap_connection() {
            either::Left(self.db.get_mv_store())
        } else {
            either::Right(TransparentWrapper(None))
        }
    }

    #[cfg(any(test, injected_yields))]
    pub fn set_yield_injector(&self, injector: Option<Arc<dyn YieldInjector>>) {
        let mut slot = self.yield_injector.write();
        match injector {
            Some(injector) => {
                turso_assert!(
                    slot.is_none(),
                    "yield injector should be empty before installing a new one"
                );
                *slot = Some(injector);
            }
            None => {
                turso_assert!(
                    slot.is_some(),
                    "yield injector should be installed before it is cleared"
                );
                *slot = None;
            }
        }
    }

    #[cfg(any(test, injected_yields))]
    pub(crate) fn yield_injector(&self) -> Option<Arc<dyn YieldInjector>> {
        self.yield_injector.read().clone()
    }

    #[cfg(any(test, injected_yields))]
    #[inline(always)]
    pub(crate) fn next_yield_instance_id(&self) -> u64 {
        self.yield_instance_id_counter
            .fetch_add(1, Ordering::Relaxed)
    }

    /// Query the current value(s) of `pragma_name` associated to
    /// `pragma_value`.
    ///
    /// This method can be used with query-only pragmas which need an argument
    /// (e.g. `table_info('one_tbl')`) or pragmas which returns value(s)
    /// (e.g. `integrity_check`).
    pub fn pragma<V: Display>(
        self: &Arc<Connection>,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Vec<Value>>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}({pragma_value})");
        let mut stmt = self.prepare(pragma)?;
        let mut results = Vec::new();
        loop {
            match stmt.step()? {
                vdbe::StepResult::Row => {
                    let row: Vec<Value> = stmt.row().unwrap().get_values().cloned().collect();
                    results.push(row);
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy);
                }
                _ => break,
            }
        }

        Ok(results)
    }

    #[inline]
    pub fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> T) -> T {
        let mut schema_ref = self.schema.write();
        let schema = Arc::make_mut(&mut *schema_ref);
        f(schema)
    }

    /// Mutate the schema for a specific database (main or attached).
    pub(crate) fn with_database_schema_mut<T>(
        &self,
        database_id: usize,
        f: impl FnOnce(&mut Schema) -> T,
    ) -> T {
        match database_id {
            crate::MAIN_DB_ID => self.with_schema_mut(f),
            crate::TEMP_DB_ID => {
                // The temp database is connection-local, no other connection can
                // reference its schema, so we can mutate it directly without cloning
                // into `database_schemas`.
                let temp_db_guard = self.temp.database.read();
                let temp_db = temp_db_guard
                    .as_ref()
                    .expect("temp database should be initialized before schema mutation");
                let mut schema_guard = temp_db.db.schema.lock();
                let schema = Arc::make_mut(&mut schema_guard);
                let result = f(schema);
                self.bump_prepare_context_generation();
                result
            }
            _ => {
                // For attached databases, update a connection-local copy of the schema.
                // We don't update the shared db.schema until after the WAL commit, so
                // other connections won't see uncommitted schema changes (which would
                // cause SchemaUpdated mismatches).
                let mut schemas = self.database_schemas.write();
                let schema_arc = schemas.entry(database_id).or_insert_with(|| {
                    let attached_dbs = self.attached_databases.read();
                    let (db, _pager) = attached_dbs
                        .index_to_data
                        .get(&database_id)
                        .expect("Database ID should be valid");
                    let schema = db.schema.lock().clone();
                    schema
                });
                let schema = Arc::make_mut(schema_arc);
                let result = f(schema);
                self.bump_prepare_context_generation();
                result
            }
        }
    }

    pub fn is_db_initialized(&self) -> bool {
        self.db.initialized()
    }

    pub(crate) fn get_pager_from_database_index(&self, index: &usize) -> Result<Arc<Pager>> {
        match *index {
            crate::MAIN_DB_ID => Ok(self.pager.load().clone()),
            crate::TEMP_DB_ID => {
                // Lazily initialize the temp database if it hasn't been created yet.
                if self.temp.database.read().is_none() {
                    self.ensure_temp_database()?;
                }
                Ok(self
                    .temp
                    .database
                    .read()
                    .as_ref()
                    .map(|temp_db| temp_db.pager.clone())
                    .expect("temp database should be initialized after ensure_temp_database"))
            }
            _ => Ok(self.attached_databases.read().get_pager_by_index(index)),
        }
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn get_database_name_by_index(&self, index: usize) -> Option<String> {
        match index {
            MAIN_DB_ID => Some("main".to_string()),
            TEMP_DB_ID => Some("temp".to_string()),
            _ => self.attached_databases.read().get_name_by_index(index),
        }
    }

    /// Get the database id for a schema name ("main", "temp", or an attached db alias).
    pub(crate) fn get_database_id_by_name(&self, name: &str) -> Result<usize> {
        let normalized: String = crate::util::normalize_ident(name);
        match normalized.as_str() {
            "main" => Ok(MAIN_DB_ID),
            "temp" => Ok(TEMP_DB_ID),
            _ => self
                .attached_databases
                .read()
                .get_database_by_name(&normalized)
                .map(|(idx, _)| idx)
                .ok_or_else(|| LimboError::InvalidArgument(format!("no such database: {name}"))),
        }
    }

    /// Get the Database object for a given database id.
    pub(crate) fn get_source_database(&self, database_id: usize) -> Arc<Database> {
        match database_id {
            MAIN_DB_ID => self.db.clone(),
            TEMP_DB_ID => self
                .temp
                .database
                .read()
                .as_ref()
                .map(|temp_db| temp_db.db.clone())
                .unwrap_or_else(|| self.db.clone()),
            _ => self
                .attached_databases
                .read()
                .get_database_by_index(database_id)
                .expect("database index should be valid"),
        }
    }

    fn is_attached(&self, alias: &str) -> bool {
        self.attached_databases
            .read()
            .name_to_index
            .contains_key(alias)
    }

    /// Returns the reserved-space value inherited from the main connection's pager.
    /// (This reads the main database pager, not the pager of db to be attached)
    fn inherited_reserved_space_for_fresh_attach(&self) -> u8 {
        let pager = self.pager.load();
        pager
            .get_reserved_space()
            .unwrap_or_else(|| pager.io_ctx.read().get_reserved_space_bytes())
    }

    /// Returns the minimum reserved space required by the attached pager's own IO context.
    /// This is used as a floor so inherited or explicit values cannot undercut the attached DB.
    fn minimum_reserved_space_for_fresh_attach(pager: &Pager) -> u8 {
        pager
            .get_reserved_space()
            .unwrap_or(0)
            .max(pager.io_ctx.read().get_reserved_space_bytes())
    }

    fn database_has_existing_wal_state(db: &Database) -> bool {
        let shared_wal = db.shared_wal.read();
        shared_wal.page_size() != 0 || shared_wal.last_checksum_and_max_frame().1 != 0
    }

    fn install_database_wal_on_pager(db: &Arc<Database>, pager: &mut Arc<Pager>) {
        let shared_wal = db.shared_wal.clone();
        let last_checksum_and_max_frame = shared_wal.read().last_checksum_and_max_frame();
        let wal = Arc::new(crate::storage::wal::WalFile::new(
            db.io.clone(),
            shared_wal,
            last_checksum_and_max_frame,
            db.buffer_pool.clone(),
        ));

        let pager = Arc::get_mut(pager)
            .expect("fresh attached pager must not be shared before bootstrap or publication");
        pager.set_wal(wal);
    }

    fn set_mvcc_journal_mode_fresh_db(pager: &Pager) -> Result<()> {
        turso_assert!(!pager.db_initialized());
        pager.set_initial_journal_version(crate::storage::sqlite3_ondisk::Version::Mvcc)
    }

    fn validate_attach_target(db: &Database, is_fresh: bool, alias: &str) -> Result<()> {
        if is_fresh && Self::database_has_existing_wal_state(db) {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': main database file is uninitialized but WAL state exists"
            )));
        }

        if is_fresh && db.is_readonly() {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': fresh read-only databases cannot be initialized during attach"
            )));
        }
        Ok(())
    }

    fn apply_page_layout_to_fresh_attach_db(
        &self,
        alias: &str,
        attached_db_pager: &Pager,
        reserved_space: Option<u8>,
    ) -> Result<()> {
        let target_page_size = self.get_page_size();
        let attached_min_reserved_space =
            Self::minimum_reserved_space_for_fresh_attach(attached_db_pager);
        let target_reserved_space = match reserved_space {
            Some(space) => {
                // this happens reserved_space is explicitly passed along with encryption or checksum
                if space < attached_min_reserved_space {
                    return Err(LimboError::InvalidArgument(format!(
                        "cannot attach database '{alias}': reserved space {space} is smaller than attached database minimum {attached_min_reserved_space}"
                    )));
                }
                Some(space)
            }
            None => Some(
                self.inherited_reserved_space_for_fresh_attach()
                    .max(attached_min_reserved_space),
            ),
        };

        attached_db_pager.set_initial_page_size(target_page_size)?;
        if let Some(reserved_space) = target_reserved_space {
            attached_db_pager.set_reserved_space_bytes(reserved_space);
        }
        Ok(())
    }

    fn reject_initialized_attach_mismatches(
        &self,
        alias: &str,
        db: &Database,
        pager: &Pager,
    ) -> Result<()> {
        // Reject incompatible journal modes for initialized attached databases:
        // we cannot silently convert the header (the user may have attached read-only).
        if self.mvcc_enabled() != db.mvcc_enabled() {
            let main_mode = if self.mvcc_enabled() { "MVCC" } else { "WAL" };
            let attached_mode = if db.mvcc_enabled() { "MVCC" } else { "WAL" };
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': main database uses {main_mode} journal mode \
                 but attached database uses {attached_mode}. Both must use the same journal mode."
            )));
        }

        // Reject mismatched page sizes: ephemeral tables and cross-database
        // operations assume a uniform page size across all attached databases.
        let main_pager = self.pager.load();
        if let (Some(main_ps), Some(attached_ps)) =
            (main_pager.get_page_size(), pager.get_page_size())
        {
            if main_ps != attached_ps {
                return Err(LimboError::InvalidArgument(format!(
                    "cannot attach database '{alias}': page size mismatch \
                     (main={main_ps:?}, attached={attached_ps:?})"
                )));
            }
        }

        Ok(())
    }

    fn reject_unsupported_fresh_mvcc_attach_durable_storage(
        &self,
        alias: &str,
        db: &Database,
        attached_is_fresh: bool,
    ) -> Result<()> {
        if attached_is_fresh
            && self.mvcc_enabled()
            && self.db.durable_storage.is_some()
            && db.durable_storage.is_none()
        {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': fresh MVCC attach does not support inheriting custom durable storage"
            )));
        }

        Ok(())
    }

    /// Attach a database file with the given alias name
    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database(&self, _path: &str, _alias: &str) -> Result<()> {
        return Err(LimboError::InvalidArgument(format!(
            "attach not available in this build (no-fs)"
        )));
    }

    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database_with_config(
        &self,
        _path: &str,
        _alias: &str,
        _reserved_space: Option<u8>,
    ) -> Result<()> {
        // File-backed ATTACH is unavailable without `fs`, so pre-initialization
        // page-layout overrides are also unsupported in this build.
        self.attach_database(_path, _alias)
    }

    /// Attach a database file with the given alias name
    #[cfg(feature = "fs")]
    pub(crate) fn attach_database(&self, path: &str, alias: &str) -> Result<()> {
        self.attach_database_inner(path, alias, None)
    }

    /// Attach a database file with an optional pre-initialization reserved-space override.
    #[cfg(feature = "fs")]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn attach_database_with_config(
        &self,
        path: &str,
        alias: &str,
        reserved_space: Option<u8>,
    ) -> Result<()> {
        self.attach_database_inner(path, alias, reserved_space)
    }

    #[cfg(feature = "fs")]
    fn attach_database_inner(
        &self,
        path: &str,
        alias: &str,
        reserved_space: Option<u8>,
    ) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if self.is_attached(alias) {
            return Err(LimboError::InvalidArgument(format!(
                "database {alias} is already in use"
            )));
        }

        // Check for reserved database names
        if alias.eq_ignore_ascii_case("main") || alias.eq_ignore_ascii_case("temp") {
            return Err(LimboError::InvalidArgument(format!(
                "reserved name {alias} is already in use"
            )));
        }

        let db_opts = DatabaseOpts::new()
            .with_views(self.db.experimental_views_enabled())
            .with_custom_types(self.db.experimental_custom_types_enabled())
            .with_index_method(self.db.experimental_index_method_enabled())
            .with_generated_columns(self.db.experimental_generated_columns_enabled());
        // Select the IO layer for the attached database:
        // - :memory: databases always get a fresh MemoryIO
        // - File-based databases reuse the parent's IO when the parent is also
        //   file-based (important for simulator fault injection and WAL coordination)
        // - If the parent is :memory: (MemoryIO) but the attached DB is file-based,
        //   we need a file-capable IO layer since MemoryIO can't read real files
        let is_memory_db =
            path == ":memory:" || path.starts_with("file::memory:") || path.is_empty();
        let io: Arc<dyn IO> = if is_memory_db {
            Arc::new(MemoryIO::new())
        } else if self.db.path.starts_with(":memory:") {
            Database::io_for_path(path)?
        } else {
            self.db.io.clone()
        };
        let main_db_flags = self.db.open_flags;
        let (db, encryption_opts) = Self::from_uri_attached(path, db_opts, main_db_flags, io)?;
        let attached_is_fresh = !db.initialized();
        if !is_memory_db {
            Self::validate_attach_target(&db, attached_is_fresh, alias)?;
        }
        self.reject_unsupported_fresh_mvcc_attach_durable_storage(alias, &db, attached_is_fresh)?;

        // Build encryption key from URI opts to pass to _init for decrypting page 1.
        let encryption_key = if let Some(ref enc) = encryption_opts {
            Some(EncryptionKey::from_hex_string(&enc.hexkey)?)
        } else {
            None
        };
        let mut pager = Arc::new(db._init(encryption_key.as_ref())?);

        if !attached_is_fresh {
            self.reject_initialized_attach_mismatches(alias, &db, &pager)?;
            self.attached_databases.write().insert(alias, (db, pager));
            self.bump_prepare_context_generation();
            return Ok(());
        }

        self.apply_page_layout_to_fresh_attach_db(alias, &pager, reserved_space)?;

        // Fresh attached databases inherit the main connection's journal mode.
        // The header must be normalized before page 1 allocation so the first
        // write and MVCC bootstrap agree on the target mode.
        if self.mvcc_enabled() && !db.mvcc_enabled() {
            Self::set_mvcc_journal_mode_fresh_db(&pager)?;
            Self::install_database_wal_on_pager(&db, &mut pager);
            let enc_ctx = pager.io_ctx.read().encryption_context().cloned();
            let mv_store = journal_mode::open_mv_store(
                db.io.clone(),
                &db.path,
                db.open_flags,
                db.durable_storage.clone(),
                enc_ctx,
            )?;
            db.mv_store.store(Some(mv_store.clone()));
            let bootstrap_conn = db._connect(true, Some(pager.clone()), encryption_key)?;
            mv_store.bootstrap(bootstrap_conn)?;
        }
        self.attached_databases.write().insert(alias, (db, pager));
        self.bump_prepare_context_generation();

        Ok(())
    }

    // Detach a database by alias name
    pub(crate) fn detach_database(&self, alias: &str) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if alias == "main" || alias == "temp" {
            return Err(LimboError::InvalidArgument(format!(
                "cannot detach database: {alias}"
            )));
        }

        // Look up the database index first, then rollback any MVCC transaction
        // *before* removing the database from the catalog.  mv_store_for_db
        // and get_pager_from_database_index read `attached_databases`, so we
        // must not hold the write lock during the rollback.
        let database_id = {
            let attached_dbs = self.attached_databases.read();
            match attached_dbs.name_to_index.get(alias).copied() {
                Some(id) => id,
                None => {
                    return Err(LimboError::InvalidArgument(format!(
                        "no such database: {alias}"
                    )));
                }
            }
        };

        // Rollback any active transaction on this database before detaching.
        // After the Database is removed from the catalog, the MvStore / Pager
        // become unreachable and the transaction would leak forever.
        let pager = self
            .get_pager_from_database_index(&database_id)
            .expect("attached database should always have a pager");
        if let Some((tx_id, _mode)) = self.get_mv_tx_for_db(database_id) {
            if let Some(mv_store) = self.mv_store_for_db(database_id) {
                mv_store.rollback_tx(tx_id, pager.clone(), self, database_id);
                pager.end_read_tx();
            }
            self.set_mv_tx_for_db(database_id, None);
        } else {
            // Non-MVCC attached DB (e.g. :memory:) — rollback WAL state.
            pager.rollback_attached();
        }

        // Remove from catalog. The write lock must be released before
        // acquiring database_schemas.write() to maintain consistent lock
        // ordering (attached_databases before database_schemas).
        {
            let mut attached_dbs = self.attached_databases.write();
            attached_dbs.remove(alias);
        }

        // Invalidate the cached schema for this database index so that a future
        // ATTACH reusing the same index won't see stale schema entries.
        self.database_schemas.write().remove(&database_id);
        self.bump_prepare_context_generation();

        Ok(())
    }

    /// List all attached database aliases
    pub fn list_attached_databases(&self) -> Vec<String> {
        self.attached_databases
            .read()
            .name_to_index
            .keys()
            .cloned()
            .collect()
    }

    /// Invoke `f` with a slice of all non-main database (index, pager) pairs
    /// (temp + attached).The internal locks are released before `f` runs, which also
    /// makes it safe for `f` to call back into the connection (e.g. `mv_store_for_db`,
    /// which re-reads the attached-database catalog).
    pub(crate) fn with_all_attached_pagers_with_index<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[(usize, Arc<Pager>)]) -> R,
    {
        let mut pagers: SmallVec<[(usize, Arc<Pager>); 8]> = SmallVec::new();
        if let Some(temp_db) = self.temp.database.read().as_ref() {
            pagers.push((crate::TEMP_DB_ID, temp_db.pager.clone()));
        }
        {
            let catalog = self.attached_databases.read();
            for (&idx, (_db, pager)) in catalog.index_to_data.iter() {
                pagers.push((idx, pager.clone()));
            }
        }
        f(&pagers)
    }

    pub(crate) fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>> {
        &self.database_schemas
    }

    fn cached_non_main_schema(&self, database_id: usize) -> Arc<Schema> {
        turso_assert_ne!(database_id, crate::MAIN_DB_ID);
        // TEMP is the sole source-of-truth path: writes go directly to
        // `temp_db.db.schema` (see `with_database_schema_mut`), so skip
        // `database_schemas` entirely to avoid stale reads.
        if database_id == crate::TEMP_DB_ID {
            return self
                .temp
                .database
                .read()
                .as_ref()
                .map(|temp_db| temp_db.db.schema.lock().clone())
                .unwrap_or_else(|| self.empty_temp_schema());
        }
        if let Some(schema) = self.database_schemas.read().get(&database_id).cloned() {
            return schema;
        }

        let attached_dbs = self.attached_databases.read();
        let (db, _pager) = attached_dbs
            .index_to_data
            .get(&database_id)
            .expect("Database ID should be valid after resolve_database_id");
        let schema = db.schema.lock().clone();
        schema
    }

    /// Publish a connection-local non-main schema after commit.
    ///
    /// TEMP is not staged in `database_schemas` — writes go directly to
    /// `temp_db.db.schema` via `with_database_schema_mut`, so there is
    /// nothing to publish here. Attached databases still stage mutations
    /// in `database_schemas` so other connections don't see uncommitted
    /// DDL; those get published to the shared `db.schema` on commit.
    pub(crate) fn publish_database_schema(&self, database_id: usize) {
        if database_id == crate::TEMP_DB_ID {
            return;
        }
        let mut schemas = self.database_schemas.write();
        if let Some(local_schema) = schemas.remove(&database_id) {
            let attached_dbs = self.attached_databases.read();
            if let Some((db, _pager)) = attached_dbs.index_to_data.get(&database_id) {
                *db.schema.lock() = local_schema;
            }
            self.bump_prepare_context_generation();
        }
    }

    pub(crate) fn attached_databases(&self) -> &RwLock<DatabaseCatalog> {
        &self.attached_databases
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        match database_id {
            crate::MAIN_DB_ID => {
                let schema = self.schema.read();
                f(&schema)
            }
            _ => {
                let schema = self.cached_non_main_schema(database_id);
                f(&schema)
            }
        }
    }

    // Get the canonical path for a database given its Database object
    fn get_canonical_path_for_database(db: &Database) -> String {
        if db.path == ":memory:" {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try to show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&db.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => db.path.to_string(),
            }
        }
    }

    /// List all databases (main + attached) with their sequence numbers, names, and file paths
    /// Returns a vector of tuples: (seq_number, name, file_path)
    pub fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        let mut databases = Vec::new();

        // Add main database (always seq=0, name="main")
        let main_path = Self::get_canonical_path_for_database(&self.db);
        databases.push((MAIN_DB_ID, "main".to_string(), main_path));

        // SQLite only exposes the temp schema in database_list after it has
        // been initialized, and reports an empty path rather than the backing
        // temp filename.
        if self.temp.database.read().is_some() {
            databases.push((crate::TEMP_DB_ID, "temp".to_string(), String::new()));
        }

        // Add attached databases
        let attached_dbs = self.attached_databases.read();
        for (alias, &seq_number) in attached_dbs.name_to_index.iter() {
            let file_path = if let Some((db, _pager)) = attached_dbs.index_to_data.get(&seq_number)
            {
                Self::get_canonical_path_for_database(db)
            } else {
                String::new()
            };
            databases.push((seq_number, alias.clone(), file_path));
        }

        // Sort by sequence number to ensure consistent ordering
        databases.sort_by_key(|&(seq, _, _)| seq);
        databases
    }

    pub fn get_pager(&self) -> Arc<Pager> {
        self.pager.load().clone()
    }

    pub fn get_query_only(&self) -> bool {
        self.is_query_only()
    }

    pub fn set_query_only(&self, value: bool) {
        self.query_only.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_dml_require_where(&self) -> bool {
        self.dml_require_where.load(Ordering::SeqCst)
    }

    pub fn set_dml_require_where(&self, value: bool) {
        self.dml_require_where.store(value, Ordering::SeqCst);
    }

    pub fn get_dqs_dml(&self) -> bool {
        self.dqs_dml.load(Ordering::SeqCst)
    }

    pub fn set_dqs_dml(&self, value: bool) {
        self.dqs_dml.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_full_column_names(&self) -> bool {
        self.full_column_names.load(Ordering::SeqCst)
    }

    pub fn set_full_column_names(&self, value: bool) {
        self.full_column_names.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_short_column_names(&self) -> bool {
        self.short_column_names.load(Ordering::SeqCst)
    }

    pub fn set_short_column_names(&self, value: bool) {
        self.short_column_names.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_sync_mode(&self) -> SyncMode {
        self.sync_mode.get()
    }

    pub fn set_sync_mode(&self, mode: SyncMode) {
        self.sync_mode.set(mode);
        self.bump_prepare_context_generation();
    }

    pub fn get_temp_store(&self) -> crate::TempStore {
        self.temp_store.get()
    }

    pub fn set_temp_store(&self, value: crate::TempStore) {
        if self.temp_store.get() == value {
            return;
        }
        self.reset_temp_database();
        self.temp_store.set(value);
        self.bump_prepare_context_generation();
    }

    pub fn get_data_sync_retry(&self) -> bool {
        self.data_sync_retry
            .load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_data_sync_retry(&self, value: bool) {
        self.data_sync_retry
            .store(value, crate::sync::atomic::Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    /// Get the sync type setting.
    pub fn get_sync_type(&self) -> crate::io::FileSyncType {
        self.pager.load().get_sync_type()
    }

    /// Set the sync type (for PRAGMA fullfsync).
    pub fn set_sync_type(&self, value: crate::io::FileSyncType) {
        self.pager.load().set_sync_type(value);
    }

    /// Creates a HashSet of modules that have been loaded
    pub fn get_syms_vtab_mods(&self) -> HashSet<String> {
        self.syms.read().vtab_modules.keys().cloned().collect()
    }

    /// Returns external (extension) functions: (name, is_aggregate, argc)
    pub fn get_syms_functions(&self) -> Vec<(String, bool, i32)> {
        self.syms
            .read()
            .functions
            .values()
            .map(|f| {
                let is_agg = matches!(f.func, function::ExtFunc::Aggregate { .. });
                let argc = match &f.func {
                    function::ExtFunc::Aggregate { argc, .. } => *argc as i32,
                    function::ExtFunc::Scalar(_) => -1,
                };
                (f.name.clone(), is_agg, argc)
            })
            .collect()
    }

    pub(crate) fn database_ptr(&self) -> usize {
        Arc::as_ptr(&self.db) as usize
    }

    pub fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        tracing::trace!("setting encryption key for connection");
        self.ensure_can_change_encryption_settings()?;
        *self.encryption_key.write() = Some(key);
        self.bump_prepare_context_generation();
        self.set_encryption_context()
    }

    pub fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        tracing::trace!("setting encryption cipher for connection");
        self.ensure_can_change_encryption_settings()?;
        self.encryption_cipher_mode.set(cipher_mode);
        self.bump_prepare_context_generation();
        self.set_encryption_context()
    }

    pub fn set_reserved_bytes(&self, reserved_bytes: u8) -> Result<()> {
        let pager = self.pager.load();
        pager.set_reserved_space_bytes(reserved_bytes);
        Ok(())
    }

    /// Get the reserved bytes value from the pager cache.
    /// Returns None if not yet set (database not initialized).
    pub fn get_reserved_bytes(&self) -> Option<u8> {
        let pager = self.pager.load();
        pager.get_reserved_space()
    }

    pub fn get_encryption_cipher_mode(&self) -> Option<CipherMode> {
        match self.encryption_cipher_mode.get() {
            CipherMode::None => None,
            mode => Some(mode),
        }
    }

    fn ensure_can_change_encryption_settings(&self) -> Result<()> {
        let pager = self.pager.load();
        if pager.is_encryption_ctx_set() {
            return Err(LimboError::InvalidArgument(
                "cannot reset encryption attributes if already set in the session".to_string(),
            ));
        }
        if self.db.get_mv_store().is_some() {
            return Err(LimboError::InvalidArgument(
                "cannot enable encryption after MVCC is active; configure encryption before PRAGMA journal_mode='mvcc'"
                    .to_string(),
            ));
        }
        Ok(())
    }

    // if both key and cipher are set, set encryption context on pager
    fn set_encryption_context(&self) -> Result<()> {
        let key_guard = self.encryption_key.read();
        let Some(key) = key_guard.as_ref() else {
            return Ok(());
        };
        let cipher_mode = self.get_encryption_cipher_mode();
        let Some(cipher_mode) = cipher_mode else {
            return Ok(());
        };
        tracing::trace!("setting encryption ctx for connection");
        let pager = self.pager.load();
        pager.set_encryption_context(cipher_mode, key)
    }

    /// Sets a custom busy handler callback.
    pub fn set_busy_handler(&self, handler: Option<BusyHandlerCallback>) {
        *self.busy_handler.write() = match handler {
            Some(callback) => BusyHandler::Custom { callback },
            None => BusyHandler::None,
        };
        self.bump_prepare_context_generation();
    }

    /// Sets maximum total accumulated timeout. If the duration is Zero, we unset the busy handler.
    pub fn set_busy_timeout(&self, duration: Duration) {
        *self.busy_handler.write() = if duration.is_zero() {
            BusyHandler::None
        } else {
            BusyHandler::Timeout(duration)
        };
        self.bump_prepare_context_generation();
    }

    /// Get the busy timeout duration.
    pub fn get_busy_timeout(&self) -> Duration {
        match &*self.busy_handler.read() {
            BusyHandler::Timeout(d) => *d,
            _ => Duration::ZERO,
        }
    }

    /// Sets the maximum duration a statement is allowed to run.
    /// `Duration::ZERO` disables query timeout.
    pub fn set_query_timeout(&self, duration: Duration) {
        let millis = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.query_timeout_ms.store(millis, Ordering::SeqCst);
    }

    /// Get the query timeout duration.
    pub fn get_query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms.load(Ordering::SeqCst))
    }

    /// Get a reference to the busy handler.
    pub fn get_busy_handler(&self) -> crate::sync::RwLockReadGuard<'_, BusyHandler> {
        self.busy_handler.read()
    }

    /// Sets a progress handler invoked approximately every `ops` VM steps.
    /// Passing `ops == 0` or `None` disables the progress handler.
    pub fn set_progress_handler(&self, ops: u64, handler: Option<ProgressHandlerCallback>) {
        self.progress_handler.set(ops, handler);
    }

    /// Returns true when the step-based progress handler requests interruption.
    pub fn should_interrupt_for_progress(&self, vm_steps: u64) -> bool {
        self.progress_handler.should_interrupt(vm_steps)
    }

    /// Request interruption of currently running root statements on this connection.
    /// If no root statement is active, the request is ignored to match SQLite semantics.
    pub fn interrupt(&self) {
        if self.n_active_root_statements.load(Ordering::SeqCst) > 0 {
            self.interrupt_requested.store(true, Ordering::SeqCst);
        }
    }

    /// Returns true if an interrupt is currently pending for this connection.
    pub fn is_interrupted(&self) -> bool {
        self.interrupt_requested.load(Ordering::SeqCst)
    }

    /// Clear the connection interrupt once no root statements remain active.
    pub(crate) fn clear_interrupt_if_idle(&self) {
        if self.n_active_root_statements.load(Ordering::SeqCst) == 0 {
            self.interrupt_requested.store(false, Ordering::SeqCst);
        }
    }

    pub(crate) fn set_tx_state(&self, state: TransactionState) {
        self.transaction_state.set(state);
    }

    pub(crate) fn get_tx_state(&self) -> TransactionState {
        self.transaction_state.get()
    }

    /// Returns true if the connection is currently in a write transaction.
    /// Used by index methods to determine if it's safe to flush writes.
    pub fn is_in_write_tx(&self) -> bool {
        matches!(self.get_tx_state(), TransactionState::Write { .. })
    }

    pub(crate) fn get_mv_tx_id(&self) -> Option<u64> {
        self.mv_tx.read().map(|(tx_id, _)| tx_id)
    }

    pub(crate) fn get_mv_tx(&self) -> Option<(u64, TransactionMode)> {
        *self.mv_tx.read()
    }

    #[inline(always)]
    pub(crate) fn set_mv_tx(&self, tx_id_and_mode: Option<(u64, TransactionMode)>) {
        tracing::debug!("set_mv_tx: {:?}", tx_id_and_mode);
        *self.mv_tx.write() = tx_id_and_mode;
    }

    /// Get MVCC transaction ID for a specific database.
    /// Uses fast path for main DB, O(1) HashMap lookup for attached DBs.
    pub(crate) fn get_mv_tx_id_for_db(&self, db: usize) -> Option<u64> {
        if db == crate::MAIN_DB_ID {
            self.get_mv_tx_id()
        } else {
            self.attached_mv_txs
                .read()
                .get(&db)
                .map(|(tx_id, _)| *tx_id)
        }
    }

    /// Get MVCC transaction ID and mode for a specific database.
    pub(crate) fn get_mv_tx_for_db(&self, db: usize) -> Option<(u64, TransactionMode)> {
        if db == crate::MAIN_DB_ID {
            self.get_mv_tx()
        } else {
            self.attached_mv_txs.read().get(&db).copied()
        }
    }

    /// Set MVCC transaction for a specific database.
    pub(crate) fn set_mv_tx_for_db(&self, db: usize, val: Option<(u64, TransactionMode)>) {
        if db == crate::MAIN_DB_ID {
            self.set_mv_tx(val);
        } else {
            let mut txs = self.attached_mv_txs.write();
            match val {
                Some(v) => {
                    txs.insert(db, v);
                }
                None => {
                    txs.remove(&db);
                }
            }
        }
    }

    /// Rollback MVCC transactions on all attached databases and clear the
    /// attached transaction list.  When `clear_schemas` is true the
    /// connection-local schema cache for each attached DB is also removed so
    /// that post-rollback queries see the committed schema.
    ///
    /// This is the single source of truth for attached-MVCC rollback logic —
    /// callers in `close()`, `rollback_current_txn()`, and `op_auto_commit`
    /// should all delegate here.
    pub(crate) fn rollback_attached_mvcc_txs(&self, clear_schemas: bool) {
        let txs: HashMap<usize, _> = self.attached_mv_txs.read().clone();
        let mut cleared_any_schema = false;
        for (&db_id, &(tx_id, _mode)) in &txs {
            if let Some(attached_mv_store) = self.mv_store_for_db(db_id) {
                let attached_pager = self
                    .get_pager_from_database_index(&db_id)
                    .expect("attached MVCC transaction should always have a pager");
                if attached_mv_store.is_tx_rollbackable(tx_id) {
                    attached_mv_store.rollback_tx(tx_id, attached_pager.clone(), self, db_id);
                } else {
                    self.set_mv_tx_for_db(db_id, None);
                }
                if clear_schemas {
                    self.database_schemas().write().remove(&db_id);
                    cleared_any_schema = true;
                }
                attached_pager.end_read_tx();
            }
        }
        self.attached_mv_txs.write().clear();
        if cleared_any_schema {
            self.bump_prepare_context_generation();
        }
    }

    /// Rollback WAL-mode transactions on all attached databases and discard
    /// their connection-local schema caches.  MVCC-enabled attached databases
    /// are skipped — those are handled by `rollback_attached_mvcc_txs`.
    pub(crate) fn rollback_attached_wal_txns(&self) {
        self.with_all_attached_pagers_with_index(|pagers| {
            // Record indices of WAL-mode entries so we can batch the schema
            // removal under a single write lock and avoid calling
            // `mv_store_for_db` more than once per entry.
            let mut wal_indices: SmallVec<[usize; 4]> = SmallVec::new();
            for (i, (db_id, _)) in pagers.iter().enumerate() {
                if self.mv_store_for_db(*db_id).is_none() {
                    wal_indices.push(i);
                }
            }
            if wal_indices.is_empty() {
                return;
            }
            {
                let mut schemas = self.database_schemas().write();
                for &i in &wal_indices {
                    schemas.remove(&pagers[i].0);
                }
            }
            self.bump_prepare_context_generation();
            for &i in &wal_indices {
                pagers[i].1.rollback_attached();
            }
        });
    }

    pub(crate) fn with_named_savepoints<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&[NamedSavepointFrame]) -> T,
    {
        let savepoints = self.named_savepoints.read();
        f(&savepoints)
    }

    pub(crate) fn push_named_savepoint(&self, frame: NamedSavepointFrame) {
        self.named_savepoints.write().push(frame);
    }

    /// Snapshot the in-memory non-main schemas for a savepoint frame so
    /// ROLLBACK TO can restore them after the pager rolls back the
    /// underlying pages.
    pub(crate) fn with_snapshot_non_main_schemas<F, T>(&self, f: F) -> T
    where
        F: FnOnce(Option<Arc<Schema>>, HashMap<usize, Arc<Schema>>) -> T,
    {
        let temp_schema_snapshot = self
            .temp
            .database
            .read()
            .as_ref()
            .map(|temp_db| temp_db.db.schema.lock().clone());
        let staged_schema_snapshot = self.database_schemas.read().clone();
        f(temp_schema_snapshot, staged_schema_snapshot)
    }

    pub(crate) fn release_named_savepoint_frame(&self, name: &str) -> SavepointResult {
        let mut savepoints = self.named_savepoints.write();
        let Some(target_idx) = savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)
        else {
            return SavepointResult::NotFound;
        };
        if savepoints[target_idx].starts_transaction && target_idx == 0 {
            return SavepointResult::Commit;
        }
        savepoints.truncate(target_idx);
        SavepointResult::Release
    }

    pub(crate) fn rollback_named_savepoint_frame(&self, name: &str) -> Option<RollbackFrameInfo> {
        let mut savepoints = self.named_savepoints.write();
        let target_idx = savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)?;
        let frame = &savepoints[target_idx];
        let info = RollbackFrameInfo {
            temp_schema_snapshot: frame.temp_schema_snapshot.clone(),
            staged_schema_snapshot: frame.staged_schema_snapshot.clone(),
        };
        // ROLLBACK TO keeps the target savepoint itself on the stack;
        // only nested savepoints above it are discarded.
        savepoints.truncate(target_idx + 1);
        Some(info)
    }

    pub(crate) fn clear_named_savepoints(&self) {
        self.named_savepoints.write().clear();
    }

    /// Roll back the current main-db transaction state and any attached-db
    /// transaction state on this connection.
    pub(crate) fn rollback_current_txn_state(
        &self,
        pager: &Arc<Pager>,
        clear_attached_schemas: bool,
    ) {
        if let Some(mv_store) = self.mv_store().as_ref() {
            if let Some(tx_id) = self.get_mv_tx_id() {
                self.auto_commit.store(true, Ordering::SeqCst);
                if mv_store.is_tx_rollbackable(tx_id) {
                    mv_store.rollback_tx(tx_id, pager.clone(), self, crate::MAIN_DB_ID);
                } else {
                    self.set_mv_tx(None);
                }
            }
            pager.end_read_tx();
            self.rollback_attached_mvcc_txs(clear_attached_schemas);
        } else {
            pager.rollback_tx(self);
            self.auto_commit.store(true, Ordering::SeqCst);
        }
        self.rollback_attached_wal_txns();
        self.set_tx_state(TransactionState::None);
    }

    /// Iterate over all attached MVCC transactions, calling `f(db_id, tx_id)` for each.
    pub(crate) fn for_each_attached_mv_tx(&self, mut f: impl FnMut(usize, u64)) {
        let txs = self.attached_mv_txs.read();
        for (&db_id, &(tx_id, _)) in txs.iter() {
            f(db_id, tx_id);
        }
    }

    /// Get the next attached MVCC transaction.
    /// Returns an arbitrary entry from `attached_mv_txs`, or `None` if empty.
    pub(crate) fn next_attached_mv_tx(&self) -> Option<(usize, u64, TransactionMode)> {
        self.attached_mv_txs
            .read()
            .iter()
            .next()
            .map(|(&db_id, &(tx_id, mode))| (db_id, tx_id, mode))
    }

    /// Get the MvStore for a specific database.
    /// Returns None for databases without MVCC or for bootstrap connections.
    pub(crate) fn mv_store_for_db(&self, db: usize) -> Option<Arc<MvStore>> {
        if self.is_mvcc_bootstrap_connection() {
            return None;
        }
        match db {
            crate::MAIN_DB_ID => self.db.get_mv_store().as_ref().cloned(),
            crate::TEMP_DB_ID => None,
            _ => {
                let catalog = self.attached_databases.read();
                catalog
                    .index_to_data
                    .get(&db)
                    .and_then(|(db, _)| db.get_mv_store().as_ref().cloned())
            }
        }
    }

    pub(crate) fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => {
                mv_store.set_checkpoint_threshold(threshold);
                self.bump_prepare_context_generation();
                Ok(())
            }
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn mvcc_checkpoint_threshold(&self) -> Result<i64> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => Ok(mv_store.checkpoint_threshold()),
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }
}

pub type Row = vdbe::Row;

pub type StepResult = vdbe::StepResult;

#[derive(Default)]
pub struct SymbolTable {
    pub functions: HashMap<String, Arc<function::ExternalFunc>>,
    pub vtabs: HashMap<String, Arc<VirtualTable>>,
    pub vtab_modules: HashMap<String, Arc<crate::ext::VTabImpl>>,
    pub index_methods: HashMap<String, Arc<dyn IndexMethod>>,
}

impl std::fmt::Debug for SymbolTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolTable")
            .field("functions", &self.functions)
            .finish()
    }
}

fn is_shared_library(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext == "so" || ext == "dylib" || ext == "dll")
}

pub fn resolve_ext_path(extpath: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(extpath);
    if !path.exists() {
        if is_shared_library(path) {
            return Err(LimboError::ExtensionError(format!(
                "Extension file not found: {extpath}"
            )));
        };
        let maybe = path.with_extension(std::env::consts::DLL_EXTENSION);
        maybe.exists().then_some(maybe).ok_or_else(|| {
            LimboError::ExtensionError(format!("Extension file not found: {extpath}"))
        })
    } else {
        Ok(path.to_path_buf())
    }
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            functions: HashMap::default(),
            vtabs: HashMap::default(),
            vtab_modules: HashMap::default(),
            index_methods: HashMap::default(),
        }
    }
    pub fn resolve_function(
        &self,
        name: &str,
        _arg_count: usize,
    ) -> Option<Arc<function::ExternalFunc>> {
        self.functions.get(name).cloned()
    }

    pub fn extend(&mut self, other: &SymbolTable) {
        for (name, func) in &other.functions {
            self.functions.insert(name.clone(), func.clone());
        }
        for (name, vtab) in &other.vtabs {
            self.vtabs.insert(name.clone(), vtab.clone());
        }
        for (name, module) in &other.vtab_modules {
            self.vtab_modules.insert(name.clone(), module.clone());
        }
        for (name, module) in &other.index_methods {
            self.index_methods.insert(name.clone(), module.clone());
        }
    }
}

#[cfg(all(test, feature = "fs"))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_connection(path: &std::path::Path) -> Arc<Connection> {
        let io: Arc<dyn IO> = Arc::new(crate::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        db.connect().unwrap()
    }

    fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
        let mut stmt = conn.prepare(sql).unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
            other => panic!("expected a row, got {other:?}"),
        }
    }

    // given a attached 'alias', return the Database and Pager for that attached database
    fn attached_entry(conn: &Connection, alias: &str) -> (Arc<Database>, Arc<Pager>) {
        let catalog = conn.attached_databases.read();
        let index = *catalog.name_to_index.get(alias).unwrap();
        catalog.index_to_data.get(&index).unwrap().clone()
    }

    #[test]
    fn test_attach_database_with_config_overrides_reserved_space_before_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        conn.attach_database_with_config(aux_path.to_str().unwrap(), "aux", Some(48))
            .unwrap();

        let (attached_db, pager) = attached_entry(&conn, "aux");
        assert!(!attached_db.initialized());
        assert!(!pager.db_initialized());
        assert_eq!(pager.get_reserved_space(), Some(48));
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn test_attach_database_with_config_rejects_reserved_space_below_minimum() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        let err = conn
            .attach_database_with_config(aux_path.to_str().unwrap(), "aux", Some(0))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "Invalid argument supplied: cannot attach database 'aux': reserved space 0 is smaller than attached database minimum 8"
        );
    }

    #[test]
    fn test_fresh_mvcc_attach_installs_wal_before_bootstrap() {
        // this is a test to check that mvcc db on attach with a fresh db, makes the
        // attached db also mvcc
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.attach_database(aux_path.to_str().unwrap(), "aux")
            .unwrap();

        let (attached_db, pager) = attached_entry(&conn, "aux");
        assert!(attached_db.get_mv_store().as_ref().is_some());
        assert!(pager.has_wal());

        conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();
        conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)").unwrap();
    }

    #[test]
    fn test_fresh_mvcc_attach_reuses_database_shared_wal() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.attach_database(aux_path.to_str().unwrap(), "aux")
            .unwrap();
        conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();

        let (attached_db, pager) = attached_entry(&conn, "aux");
        let pager_shared_ptr = pager
            .wal_shared_ptr()
            .expect("fresh MVCC attach must expose WAL shared state in tests");
        let db_shared_ptr = Arc::as_ptr(&attached_db.shared_wal) as usize;

        assert_eq!(pager_shared_ptr, db_shared_ptr);
    }

    #[test]
    fn test_temp_tables_are_connection_local_and_shadow_main() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn1 = open_connection(&db_path);

        conn1.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn1.execute("INSERT INTO main.t VALUES(1)").unwrap();
        let conn2 = open_connection(&db_path);
        conn1.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        conn1.execute("INSERT INTO temp.t VALUES(2)").unwrap();

        assert_eq!(query_single_i64(&conn1, "SELECT x FROM t"), 2);
        assert_eq!(query_single_i64(&conn1, "SELECT x FROM main.t"), 1);
        assert_eq!(query_single_i64(&conn2, "SELECT x FROM t"), 1);

        let err = conn2
            .prepare("SELECT x FROM temp.t")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no such table"),
            "expected no such table error, got: {err}"
        );
    }

    #[test]
    fn test_reprepare_after_temp_store_reset_does_not_panic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        let mut stmt = conn.prepare("SELECT x FROM t").unwrap();

        conn.execute("PRAGMA temp_store = MEMORY").unwrap();

        let err = stmt.step().unwrap_err().to_string();
        assert!(
            err.contains("no such table"),
            "expected no such table after temp reset, got: {err}"
        );
    }

    #[test]
    fn test_temp_trigger_abort_rolls_back_temp_writes_without_panicking() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        conn.execute("CREATE TEMP TABLE u(y INTEGER)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE INSERT ON temp.t BEGIN \
             INSERT INTO u VALUES (NEW.x); \
             SELECT RAISE(ABORT, 'boom'); \
             END;",
        )
        .unwrap();

        let err = conn.execute("INSERT INTO temp.t VALUES(1)").unwrap_err();
        assert!(
            err.to_string().contains("boom"),
            "expected trigger abort error, got: {err}"
        );
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.u"), 0);
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.t"), 0);
    }

    #[test]
    fn test_temp_trigger_abort_rolls_back_main_and_temp_writes() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TABLE m(x INTEGER)").unwrap();
        conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        conn.execute("CREATE TEMP TABLE u(y INTEGER)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE INSERT ON temp.t BEGIN \
             INSERT INTO m VALUES (NEW.x); \
             INSERT INTO u VALUES (NEW.x); \
             SELECT RAISE(ABORT, 'boom'); \
             END;",
        )
        .unwrap();

        let err = conn.execute("INSERT INTO temp.t VALUES(1)").unwrap_err();
        assert!(
            err.to_string().contains("boom"),
            "expected trigger abort error, got: {err}"
        );
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM main.m"), 0);
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.u"), 0);
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.t"), 0);
    }
}
