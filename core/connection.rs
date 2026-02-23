use crate::error::io_error;
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
    parse_schema_rows, refresh_analyze_stats, translate,
    util::IOExt,
    vdbe, AllViewsTxState, AtomicCipherMode, AtomicSyncMode, AtomicTempStore, BusyHandler,
    BusyHandlerCallback, CaptureDataChangesInfo, CheckpointMode, CheckpointResult, CipherMode, Cmd,
    Completion, ConnectionMetrics, Database, DatabaseCatalog, DatabaseOpts, Duration,
    EncryptionKey, EncryptionOpts, IndexMethod, LimboError, MvStore, OpenFlags, PageSize, Pager,
    Parser, QueryMode, QueryRunner, Result, Schema, Statement, SyncMode, TransactionMode, Trigger,
    Value, VirtualTable,
};
use arc_swap::ArcSwap;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::fmt::Display;
use std::ops::Deref;
use tracing::{instrument, Level};
use turso_macros::AtomicEnum;

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

/// Database connection handle.
///
/// # Compile-Affecting Fields
///
/// The following fields affect SQL statement compilation and are tracked by `PrepareContext`
/// in `vdbe/mod.rs`. If you add a new field that affects how statements are compiled or
/// executed, you MUST also update `PrepareContext::from_connection()` to include it
/// in core/vdbe/mod.rs.
/// Failure to do so will cause stale cached statements to be used incorrectly.
///
/// Currently tracked fields:
/// - `db` (via database pointer identity)
/// - `fk_pragma` (foreign_keys)
/// - `query_only`
/// - `capture_data_changes`
/// - `syms` / `syms_generation` (registered functions, virtual tables, etc.)
/// - `attached_databases` (via fingerprint)
/// - `busy_handler` (timeout affects retry behavior)
/// - `cache_size`
/// - `page_size`
/// - `sync_mode`
/// - `data_sync_retry`
/// - `encryption_key` (whether set)
/// - `encryption_cipher_mode`
/// - Pager's `spill_enabled` setting
/// - MVCC checkpoint threshold (when MVCC enabled)
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
    pub(crate) last_change: AtomicI64,
    pub(crate) total_changes: AtomicI64,
    pub(crate) syms: parking_lot::RwLock<SymbolTable>,
    pub(crate) syms_generation: AtomicU64,
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
    /// Attached databases
    pub(super) attached_databases: RwLock<DatabaseCatalog>,
    pub(super) query_only: AtomicBool,
    pub(crate) mv_tx: RwLock<Option<(crate::mvcc::database::TxID, TransactionMode)>>,

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
    /// Current depth of nested Program opcode execution (trigger/FK subprogram calls).
    pub(super) executing_subprogram_depth: AtomicI32,
    /// Stack of FK action subprograms currently being compiled.
    /// Used to break recursive FK subprogram compilation cycles.
    pub(super) compiling_fk_actions: RwLock<Vec<String>>,
    /// Current FK action subprogram compilation depth.
    /// Self-referential and cyclic CASCADE actions recursively compile nested subprograms.
    pub(super) compiling_fk_actions_depth: AtomicI32,
    pub(crate) encryption_key: RwLock<Option<EncryptionKey>>,
    pub(super) encryption_cipher_mode: AtomicCipherMode,
    pub(super) sync_mode: AtomicSyncMode,
    pub(super) temp_store: AtomicTempStore,
    pub(super) data_sync_retry: AtomicBool,
    /// Busy handler for lock contention
    /// Default is BusyHandler::None (return SQLITE_BUSY immediately)
    pub(super) busy_handler: RwLock<BusyHandler>,
    /// Whether this is an internal connection used for MVCC bootstrap
    pub(super) is_mvcc_bootstrap_connection: AtomicBool,
    /// Whether pragma foreign_keys=ON for this connection
    pub(super) fk_pragma: AtomicBool,
    pub(crate) fk_deferred_violations: AtomicIsize,
    /// Whether pragma ignore_check_constraints=ON for this connection
    pub(super) check_constraints_pragma: AtomicBool,
    /// Track when each virtual table instance is currently in transaction.
    pub(crate) vtab_txn_states: RwLock<HashSet<u64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AttachedDatabasesFingerprint {
    pub(crate) count: usize,
    pub(crate) hash1: u64,
    pub(crate) hash2: u64,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
crate::assert::assert_send_sync!(Connection);

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.is_closed() {
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
            for attached_pager in self.get_all_attached_pagers() {
                if let Some(wal) = &attached_pager.wal {
                    if wal.holds_write_lock() {
                        wal.end_write_tx();
                    }
                    if wal.holds_read_lock() {
                        wal.end_read_tx();
                    }
                }
            }

            // if connection wasn't properly closed, decrement the connection counter
            self.db
                .n_connections
                .fetch_sub(1, crate::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Connection {
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

    /// SQLite default trigger recursion limit (SQLITE_MAX_TRIGGER_DEPTH).
    /// FK CASCADE actions compile into trigger-like subprograms and follow the same limit.
    const MAX_TRIGGER_RECURSION_DEPTH: i32 = 1000;

    pub(crate) fn start_fk_action_compilation(&self, key: &str) -> Result<()> {
        let depth = self
            .compiling_fk_actions_depth
            .fetch_add(1, Ordering::SeqCst)
            + 1;

        if depth > Self::MAX_TRIGGER_RECURSION_DEPTH {
            self.compiling_fk_actions_depth
                .fetch_add(-1, Ordering::SeqCst);
            return Err(LimboError::ParseError(
                "too many levels of trigger recursion".to_string(),
            ));
        }

        let mut compiling_fk_actions = self.compiling_fk_actions.write();
        if compiling_fk_actions.iter().any(|active| active == key) {
            self.compiling_fk_actions_depth
                .fetch_add(-1, Ordering::SeqCst);
            return Err(LimboError::ParseError(
                "too many levels of trigger recursion".to_string(),
            ));
        }
        compiling_fk_actions.push(key.to_string());

        Ok(())
    }

    pub(crate) fn end_fk_action_compilation(&self, expected_key: &str) {
        let popped = self.compiling_fk_actions.write().pop();
        debug_assert_eq!(
            popped.as_deref(),
            Some(expected_key),
            "fk action compilation stack out of sync"
        );

        let prev = self
            .compiling_fk_actions_depth
            .fetch_add(-1, Ordering::SeqCst);

        debug_assert!(
            prev > 0,
            "end_fk_action_compilation called without matching start"
        );
    }

    pub(crate) fn start_subprogram_execution(&self) -> Result<()> {
        let depth = self
            .executing_subprogram_depth
            .fetch_add(1, Ordering::SeqCst)
            + 1;

        if depth > Self::MAX_TRIGGER_RECURSION_DEPTH {
            self.executing_subprogram_depth
                .fetch_add(-1, Ordering::SeqCst);
            return Err(LimboError::ParseError(
                "too many levels of trigger recursion".to_string(),
            ));
        }

        Ok(())
    }

    pub(crate) fn end_subprogram_execution(&self) {
        let prev = self
            .executing_subprogram_depth
            .fetch_add(-1, Ordering::SeqCst);

        debug_assert!(
            prev > 0,
            "end_subprogram_execution called without matching start"
        );
    }

    /// Check if a specific trigger is currently compiling (for recursive trigger prevention)
    pub fn trigger_is_compiling(&self, trigger: impl AsRef<Trigger>) -> bool {
        let compiling = self.compiling_triggers.read();
        if let Some(trigger) = compiling.iter().find(|t| t.name == trigger.as_ref().name) {
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
    pub fn is_trigger_executing(&self, trigger: impl AsRef<Trigger>) -> bool {
        let executing = self.executing_triggers.read();
        if let Some(trigger) = executing.iter().find(|t| t.name == trigger.as_ref().name) {
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
    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self._prepare(sql)
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn _prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }

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
        let syms = self.syms.read();
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        self.maybe_update_schema();
        let pager = self.pager.load().clone();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;

        // Read lock + Arc::Clone the schema here to avoid a possible recursive read lock in `op_parse_schema`,
        // where we try to read the schema again there
        let schema = self.schema.read().clone();

        let program = translate::translate(
            &schema,
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        )?;
        Ok(Statement::new(program, pager, mode))
    }

    /// Prepare a statement from an AST node directly, skipping SQL parsing.
    /// This is more efficient when AST is already available or constructed programmatically.
    pub fn prepare_stmt(self: &Arc<Connection>, stmt: ast::Stmt) -> Result<Statement> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
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
        Ok(Statement::new(program, pager, mode))
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

    /// Parse schema from scratch if version of schema for the connection differs from the schema cookie in the root page
    /// This function must be called outside of any transaction because internally it will start transaction session by itself
    #[allow(dead_code)]
    fn maybe_reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.load().clone();

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
        // maybe_reparse_schema must be called outside of any transaction
        turso_assert!(
            self.get_tx_state() == TransactionState::None,
            "unexpected start transaction"
        );
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
        let pager = self.pager.load().clone();

        // read cookie before consuming statement program - otherwise we can end up reading cookie with closed transaction state
        let cookie = pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie))?
            .get();

        // create fresh schema as some objects can be deleted
        let mut fresh = Schema::new();
        fresh.schema_version = cookie;

        // Preserve existing views to avoid expensive repopulation.
        // TODO: We may not need to do this if we materialize our views.
        let existing_views = self.schema.read().incremental_views.clone();

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
        // TODO: This function below is synchronous, make it async
        parse_schema_rows(
            stmt,
            &mut fresh,
            &self.syms.read(),
            mv_tx,
            existing_views,
            self.experimental_triggers_enabled(),
        )?;
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
        self.maybe_update_schema();
        let sql = sql.as_ref();
        tracing::trace!("Preparing and executing batch: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next_cmd()? {
            let syms = self.syms.read();
            let pager = self.pager.load().clone();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let mode = QueryMode::new(&cmd);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            let schema = self.schema.read().clone();
            let program = translate::translate(
                &schema,
                stmt,
                pager.clone(),
                self.clone(),
                &syms,
                mode,
                input,
            )?;
            Statement::new(program, pager.clone(), mode).run_ignore_rows()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn query(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        self.maybe_update_schema();
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
        let syms = self.syms.read();
        let pager = self.pager.load().clone();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
        let schema = self.schema.read().clone();
        let program = translate::translate(
            &schema,
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        )?;
        let stmt = Statement::new(program, pager, mode);
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
        self.maybe_update_schema();
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next_cmd()? {
            let syms = self.syms.read();
            let pager = self.pager.load().clone();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let mode = QueryMode::new(&cmd);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            let schema = self.schema.read().clone();
            let program = translate::translate(
                &schema,
                stmt,
                pager.clone(),
                self.clone(),
                &syms,
                mode,
                input,
            )?;
            Statement::new(program, pager.clone(), mode).run_ignore_rows()?;
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
        let syms = self.syms.read();
        let pager = self.pager.load().clone();
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_ref().as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
        let schema = self.schema.read().clone();
        let program = translate::translate(
            &schema,
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        )?;
        let stmt = Statement::new(program, pager, mode);
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
                ))
            }
            (None, Some(_)) => {
                return Err(LimboError::InvalidArgument(
                    "cipher is required when hexkey is provided".to_string(),
                ))
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
        db_opts: DatabaseOpts,
        main_db_flags: OpenFlags,
        io: Arc<dyn IO>,
    ) -> Result<Arc<Database>> {
        let opts = OpenOptions::parse(uri)?;
        let mut flags = opts.get_flags()?;
        if main_db_flags.contains(OpenFlags::ReadOnly) {
            flags |= OpenFlags::ReadOnly;
        }
        let io = opts.vfs.map(Database::io_for_vfs).unwrap_or(Ok(io))?;
        let db = Database::open_file_with_flags(io.clone(), &opts.path, flags, db_opts, None)?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof).map_err(|e| io_error(e, "metadata"))?;
            std::fs::set_permissions(&opts.path, perms.permissions())
                .map_err(|e| io_error(e, "set_permissions"))?;
        }
        Ok(db)
    }

    pub fn set_foreign_keys_enabled(&self, enable: bool) {
        self.fk_pragma.store(enable, Ordering::Release);
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

    pub fn maybe_update_schema(&self) {
        let current_schema_version = self.schema.read().schema_version;
        let schema = self.db.schema.lock();
        if matches!(self.get_tx_state(), TransactionState::None)
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
        self.get_pager().io.wait_for_completion(c)?;
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
                        mv_store.rollback_tx(tx_id, pager.clone(), self);
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
        use crate::state_machine::StateMachine;
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if let Some(mv_store) = self.mv_store().as_ref() {
            let pager = self.pager.load().clone();
            let io = pager.io.clone();
            let mut ckpt_sm = StateMachine::new(CheckpointStateMachine::new(
                pager,
                mv_store.clone(),
                self.clone(),
                true,
                self.get_sync_mode(),
            ));
            io.as_ref().block(|| ckpt_sm.step(&()))
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
                            mv_store.rollback_tx(tx_id, pager.clone(), self);
                        }
                    }
                    pager.end_read_tx();
                } else {
                    pager.rollback_tx(self);
                }
                self.set_tx_state(TransactionState::None);
            }
        }

        if self.db.n_connections.fetch_sub(1, Ordering::SeqCst).eq(&1) && !self.db.is_readonly() {
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

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid.load(Ordering::SeqCst)
    }

    pub(crate) fn update_last_rowid(&self, rowid: i64) {
        self.last_insert_rowid.store(rowid, Ordering::SeqCst);
    }

    pub fn set_changes(&self, nchange: i64) {
        self.last_change.store(nchange, Ordering::SeqCst);
        self.total_changes.fetch_add(nchange, Ordering::SeqCst);
    }

    pub fn changes(&self) -> i64 {
        self.last_change.load(Ordering::SeqCst)
    }

    pub fn total_changes(&self) -> i64 {
        self.total_changes.load(Ordering::SeqCst)
    }

    pub fn get_cache_size(&self) -> i32 {
        self.cache_size.load(Ordering::SeqCst)
    }
    pub fn set_cache_size(&self, size: i32) {
        self.cache_size.store(size, Ordering::SeqCst);
    }

    pub fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>> {
        self.capture_data_changes.read()
    }
    pub fn set_capture_data_changes_info(&self, opts: Option<CaptureDataChangesInfo>) {
        *self.capture_data_changes.write() = opts;
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
        if index <= 1 {
            self.db.is_readonly()
        } else {
            let db = self.attached_databases.read().get_database_by_index(index);
            db.expect("Should never have called this without being sure the database exists")
                .is_readonly()
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
        let enable_triggers = self.experimental_triggers_enabled();
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
                    None,
                    enable_triggers,
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

    pub fn experimental_strict_enabled(&self) -> bool {
        self.db.experimental_strict_enabled()
    }

    pub fn experimental_triggers_enabled(&self) -> bool {
        self.db.experimental_triggers_enabled()
    }

    pub fn experimental_attach_enabled(&self) -> bool {
        self.db.experimental_attach_enabled()
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
        if !crate::is_attached_db(database_id) {
            self.with_schema_mut(f)
        } else {
            // For attached databases, update a connection-local copy of the schema.
            // We don't update the shared db.schema until after the WAL commit, so
            // other connections won't see uncommitted schema changes (which would
            // cause SchemaUpdated mismatches).
            let mut schemas = self.database_schemas.write();
            let schema_arc = schemas.entry(database_id).or_insert_with(|| {
                // Lazily copy from the shared Database schema
                let attached_dbs = self.attached_databases.read();
                let (db, _pager) = attached_dbs
                    .index_to_data
                    .get(&database_id)
                    .expect("Database ID should be valid");
                let schema = db.schema.lock().clone();
                schema
            });
            let schema = Arc::make_mut(schema_arc);
            f(schema)
        }
    }

    pub fn is_db_initialized(&self) -> bool {
        self.db.initialized()
    }

    pub(crate) fn get_pager_from_database_index(&self, index: &usize) -> Arc<Pager> {
        if *index < 2 {
            self.pager.load().clone()
        } else {
            self.attached_databases.read().get_pager_by_index(index)
        }
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn get_database_name_by_index(&self, index: usize) -> Option<String> {
        match index {
            0 => Some("main".to_string()),
            1 => Some("temp".to_string()),
            _ => self.attached_databases.read().get_name_by_index(index),
        }
    }

    #[cfg(feature = "fs")]
    fn is_attached(&self, alias: &str) -> bool {
        self.attached_databases
            .read()
            .name_to_index
            .contains_key(alias)
    }

    /// Attach a database file with the given alias name
    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database(&self, _path: &str, _alias: &str) -> Result<()> {
        return Err(LimboError::InvalidArgument(format!(
            "attach not available in this build (no-fs)"
        )));
    }

    /// Attach a database file with the given alias name
    #[cfg(feature = "fs")]
    pub(crate) fn attach_database(&self, path: &str, alias: &str) -> Result<()> {
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

        let use_views = self.db.experimental_views_enabled();
        let use_strict = self.db.experimental_strict_enabled();

        let db_opts = DatabaseOpts::new()
            .with_views(use_views)
            .with_strict(use_strict);
        // Select the IO layer for the attached database:
        // - :memory: databases always get a fresh MemoryIO
        // - File-based databases reuse the parent's IO when the parent is also
        //   file-based (important for simulator fault injection and WAL coordination)
        // - If the parent is :memory: (MemoryIO) but the attached DB is file-based,
        //   we need a file-capable IO layer since MemoryIO can't read real files
        let io: Arc<dyn IO> = if path.contains(":memory:") {
            Arc::new(MemoryIO::new())
        } else if self.db.path.starts_with(":memory:") {
            Database::io_for_path(path)?
        } else {
            self.db.io.clone()
        };
        let main_db_flags = self.db.open_flags;
        let db = Self::from_uri_attached(path, db_opts, main_db_flags, io)?;
        let pager = Arc::new(db._init(None)?);
        self.attached_databases.write().insert(alias, (db, pager));

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

        // Remove from attached databases
        let mut attached_dbs = self.attached_databases.write();
        let database_id = match attached_dbs.remove(alias) {
            Some(id) => id,
            None => {
                return Err(LimboError::InvalidArgument(format!(
                    "no such database: {alias}"
                )));
            }
        };

        // Invalidate the cached schema for this database index so that a future
        // ATTACH reusing the same index won't see stale schema entries.
        self.database_schemas.write().remove(&database_id);

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

    /// Get all attached database pagers (excludes main/temp databases)
    pub fn get_all_attached_pagers(&self) -> Vec<Arc<Pager>> {
        let catalog = self.attached_databases.read();
        catalog
            .index_to_data
            .values()
            .map(|(_db, pager)| pager.clone())
            .collect()
    }

    /// Get all attached database (index, pager) pairs (excludes main/temp databases)
    pub(crate) fn get_all_attached_pagers_with_index(&self) -> Vec<(usize, Arc<Pager>)> {
        let catalog = self.attached_databases.read();
        catalog
            .index_to_data
            .iter()
            .map(|(&idx, (_db, pager))| (idx, pager.clone()))
            .collect()
    }

    pub(crate) fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>> {
        &self.database_schemas
    }

    /// Publish a connection-local attached DB schema to the shared Database instance.
    /// Called after the attached pager's WAL commit succeeds, so other connections
    /// can now see the schema changes.
    pub(crate) fn publish_attached_schema(&self, database_id: usize) {
        let mut schemas = self.database_schemas.write();
        if let Some(local_schema) = schemas.remove(&database_id) {
            let attached_dbs = self.attached_databases.read();
            if let Some((db, _pager)) = attached_dbs.index_to_data.get(&database_id) {
                *db.schema.lock() = local_schema;
            }
        }
    }

    pub(crate) fn attached_databases(&self) -> &RwLock<DatabaseCatalog> {
        &self.attached_databases
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        match database_id {
            crate::MAIN_DB_ID | crate::TEMP_DB_ID => {
                // Main database - use connection's schema which should be kept in sync
                // NOTE: for Temp databases, for now they can use the connection-local schema
                // but this will change in the future
                let schema = self.schema.read();
                f(&schema)
            }
            _ => {
                // Attached database: prefer the connection-local copy (which may contain
                // uncommitted schema changes from this connection's transaction), falling
                // back to the shared Database schema (last committed state).
                let schemas = self.database_schemas.read();
                if let Some(local_schema) = schemas.get(&database_id) {
                    return f(local_schema);
                }
                drop(schemas);

                let attached_dbs = self.attached_databases.read();
                let (db, _pager) = attached_dbs
                    .index_to_data
                    .get(&database_id)
                    .expect("Database ID should be valid after resolve_database_id");

                let schema = db.schema.lock().clone();
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

    pub(crate) fn attached_databases_fingerprint(&self) -> AttachedDatabasesFingerprint {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

        fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
            for &byte in bytes {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            hash
        }

        fn hash_u64(hash: u64, value: u64) -> u64 {
            hash_bytes(hash, &value.to_le_bytes())
        }

        fn hash_entry(index: usize, alias: &str, db_ptr: usize) -> u64 {
            let mut hash = FNV_OFFSET_BASIS;
            hash = hash_u64(hash, index as u64);
            hash = hash_bytes(hash, alias.as_bytes());
            hash_u64(hash, db_ptr as u64)
        }

        let attached = self.attached_databases.read();
        if attached.name_to_index.is_empty() {
            return AttachedDatabasesFingerprint {
                count: 0,
                hash1: 0,
                hash2: 0,
            };
        }

        let mut hash1 = 0u64;
        let mut hash2 = 0u64;
        for (alias, &index) in attached.name_to_index.iter() {
            let db_ptr = attached
                .index_to_data
                .get(&index)
                .map(|(db, _pager)| Arc::as_ptr(db) as usize)
                .unwrap_or(0);
            let entry_hash = hash_entry(index, alias.as_str(), db_ptr);
            hash1 = hash1.wrapping_add(entry_hash);
            hash2 ^= entry_hash.rotate_left(17);
        }

        AttachedDatabasesFingerprint {
            count: attached.name_to_index.len(),
            hash1,
            hash2,
        }
    }

    /// List all databases (main + attached) with their sequence numbers, names, and file paths
    /// Returns a vector of tuples: (seq_number, name, file_path)
    pub fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        let mut databases = Vec::new();

        // Add main database (always seq=0, name="main")
        let main_path = Self::get_canonical_path_for_database(&self.db);
        databases.push((0, "main".to_string(), main_path));

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
    }

    pub fn get_sync_mode(&self) -> SyncMode {
        self.sync_mode.get()
    }

    pub fn set_sync_mode(&self, mode: SyncMode) {
        self.sync_mode.set(mode);
    }

    pub fn get_temp_store(&self) -> crate::TempStore {
        self.temp_store.get()
    }

    pub fn set_temp_store(&self, value: crate::TempStore) {
        self.temp_store.set(value);
    }

    pub fn get_data_sync_retry(&self) -> bool {
        self.data_sync_retry
            .load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_data_sync_retry(&self, value: bool) {
        self.data_sync_retry
            .store(value, crate::sync::atomic::Ordering::SeqCst);
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

    pub(crate) fn syms_generation(&self) -> u64 {
        self.syms_generation.load(Ordering::SeqCst)
    }

    pub(crate) fn database_ptr(&self) -> usize {
        Arc::as_ptr(&self.db) as usize
    }

    pub fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        tracing::trace!("setting encryption key for connection");
        *self.encryption_key.write() = Some(key);
        self.set_encryption_context()
    }

    pub fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        tracing::trace!("setting encryption cipher for connection");
        self.encryption_cipher_mode.set(cipher_mode);
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
        if pager.is_encryption_ctx_set() {
            return Err(LimboError::InvalidArgument(
                "cannot reset encryption attributes if already set in the session".to_string(),
            ));
        }
        pager.set_encryption_context(cipher_mode, key)
    }

    /// Sets a custom busy handler callback.
    pub fn set_busy_handler(&self, handler: Option<BusyHandlerCallback>) {
        *self.busy_handler.write() = match handler {
            Some(callback) => BusyHandler::Custom { callback },
            None => BusyHandler::None,
        };
    }

    /// Sets maximum total accumulated timeout. If the duration is Zero, we unset the busy handler.
    pub fn set_busy_timeout(&self, duration: Duration) {
        *self.busy_handler.write() = if duration.is_zero() {
            BusyHandler::None
        } else {
            BusyHandler::Timeout(duration)
        };
    }

    /// Get the busy timeout duration.
    pub fn get_busy_timeout(&self) -> Duration {
        match &*self.busy_handler.read() {
            BusyHandler::Timeout(d) => *d,
            _ => Duration::ZERO,
        }
    }

    /// Get a reference to the busy handler.
    pub fn get_busy_handler(&self) -> crate::sync::RwLockReadGuard<'_, BusyHandler> {
        self.busy_handler.read()
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

    pub(crate) fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => {
                mv_store.set_checkpoint_threshold(threshold);
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
