use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use turso_core::{types::WalFrameInfo, LimboError, StepResult};

use crate::{
    database_replay_generator::{DatabaseReplayGenerator, ReplayInfo},
    database_sync_operations::WAL_FRAME_HEADER,
    errors::Error,
    types::{
        Coro, DatabaseChange, DatabaseChangeType, DatabaseSchemaKind, DatabaseSchemaReplay,
        DatabaseTapeOperation, DatabaseTapeRowChangeType, SyncEngineIoResult,
    },
    wal_session::WalSession,
    Result,
};

/// Simple wrapper over [turso::Database] which extends its intereface with few methods
/// to collect changes made to the database and apply/revert arbitrary changes to the database
pub struct DatabaseTape {
    inner: Arc<turso_core::Database>,
    cdc_table: Arc<String>,
    pragma_query: String,
    cdc_version: std::sync::RwLock<Option<turso_core::CdcVersion>>,
    disable_auto_checkpoint: bool,
}

const DEFAULT_CDC_TABLE_NAME: &str = "turso_cdc";
const DEFAULT_CDC_MODE: &str = "full";
const DEFAULT_CHANGES_BATCH_SIZE: usize = 100;
pub const CDC_PRAGMA_NAME: &str = "capture_data_changes_conn";

#[derive(Debug, Clone)]
pub struct DatabaseTapeOpts {
    pub cdc_table: Option<String>,
    pub cdc_mode: Option<String>,
    pub disable_auto_checkpoint: bool,
}

/// Async, coro-threaded counterpart to
/// [`turso_core::Connection::try_wal_watermark_read_page`]. It drives the
/// begin / wait-for-completion / end sequence in one place so the Windows-IOCP
/// `UnexpectedEof -> absent page` handling cannot drift across the watermark
/// read call sites. Returns `Ok(false)` when the page is absent at
/// `frame_watermark` (i.e. allocated only in the WAL portion past it).
pub(crate) async fn try_wal_watermark_read_page<Ctx>(
    coro: &Coro<Ctx>,
    conn: &turso_core::Connection,
    page_idx: u32,
    page: &mut [u8],
    frame_watermark: Option<u64>,
) -> Result<bool> {
    let Some((page_ref, c)) = conn.try_wal_watermark_read_page_begin(page_idx, frame_watermark)?
    else {
        return Ok(false);
    };
    while !c.finished() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    if let Some(err) = c.get_error() {
        if turso_core::Connection::wal_watermark_read_error_is_absent_page(&err) {
            return Ok(false);
        }
        return Err(LimboError::CompletionError(err).into());
    }
    Ok(conn.try_wal_watermark_read_page_end(page, page_ref)?)
}

pub(crate) async fn run_stmt_once<'a, Ctx>(
    coro: &'_ Coro<Ctx>,
    stmt: &'a mut turso_core::Statement,
) -> Result<Option<&'a turso_core::Row>> {
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                coro.yield_(SyncEngineIoResult::IO).await?;
            }
            StepResult::Done => {
                return Ok(None);
            }
            StepResult::Interrupt => {
                return Err(Error::DatabaseTapeError(
                    "statement was interrupted".to_string(),
                ))
            }
            StepResult::Busy => {
                return Err(Error::DatabaseTapeError("database is busy".to_string()))
            }
            StepResult::Row => return Ok(Some(stmt.row().unwrap())),
        }
    }
}

pub(crate) async fn run_stmt_expect_one_row<Ctx>(
    coro: &Coro<Ctx>,
    stmt: &mut turso_core::Statement,
) -> Result<Option<Vec<turso_core::Value>>> {
    let Some(row) = run_stmt_once(coro, stmt).await? else {
        return Ok(None);
    };
    let values = row.get_values().cloned().collect();
    let None = run_stmt_once(coro, stmt).await? else {
        return Err(Error::DatabaseTapeError("single row expected".to_string()));
    };
    Ok(Some(values))
}

pub(crate) async fn run_stmt_ignore_rows<Ctx>(
    coro: &Coro<Ctx>,
    stmt: &mut turso_core::Statement,
) -> Result<()> {
    while run_stmt_once(coro, stmt).await?.is_some() {}
    Ok(())
}

pub(crate) async fn exec_stmt<Ctx>(
    coro: &Coro<Ctx>,
    stmt: &mut turso_core::Statement,
) -> Result<()> {
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                coro.yield_(SyncEngineIoResult::IO).await?;
            }
            StepResult::Done => {
                return Ok(());
            }
            StepResult::Interrupt => {
                return Err(Error::DatabaseTapeError(
                    "statement was interrupted".to_string(),
                ))
            }
            StepResult::Busy => {
                return Err(Error::DatabaseTapeError("database is busy".to_string()))
            }
            StepResult::Row => panic!("statement should not return any rows"),
        }
    }
}

impl DatabaseTape {
    pub fn new(database: Arc<turso_core::Database>) -> Self {
        let opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: None,
            disable_auto_checkpoint: false,
        };
        Self::new_with_opts(database, opts)
    }
    pub fn new_with_opts(database: Arc<turso_core::Database>, opts: DatabaseTapeOpts) -> Self {
        tracing::debug!("create local sync database with options {:?}", opts);
        let cdc_table_name = opts.cdc_table.unwrap_or(DEFAULT_CDC_TABLE_NAME.to_string());
        let cdc_mode = opts.cdc_mode.unwrap_or(DEFAULT_CDC_MODE.to_string());
        let pragma_query = format!("PRAGMA {CDC_PRAGMA_NAME}('{cdc_mode},{cdc_table_name}')");
        Self {
            inner: database,
            cdc_table: Arc::new(cdc_table_name.to_string()),
            pragma_query,
            cdc_version: std::sync::RwLock::new(None),
            disable_auto_checkpoint: opts.disable_auto_checkpoint,
        }
    }
    pub(crate) fn connect_untracked(&self) -> Result<Arc<turso_core::Connection>> {
        let connection = self.inner.connect()?;
        if self.disable_auto_checkpoint {
            connection.wal_auto_actions_disable();
        }
        Ok(connection)
    }
    pub async fn connect<Ctx>(&self, coro: &Coro<Ctx>) -> Result<Arc<turso_core::Connection>> {
        let connection = self.inner.connect()?;
        if self.disable_auto_checkpoint {
            connection.wal_auto_actions_disable();
        }
        tracing::debug!("set '{CDC_PRAGMA_NAME}' for new connection");
        let mut stmt = connection.prepare(&self.pragma_query)?;
        run_stmt_ignore_rows(coro, &mut stmt).await?;
        // Cache CDC version from turso_cdc_version table
        if self.cdc_version.read().unwrap().is_none() {
            let version = Self::read_cdc_version(coro, &connection, &self.cdc_table).await?;
            *self.cdc_version.write().unwrap() = Some(version);
        }
        Ok(connection)
    }

    async fn read_cdc_version<Ctx>(
        coro: &Coro<Ctx>,
        connection: &Arc<turso_core::Connection>,
        cdc_table: &str,
    ) -> Result<turso_core::CdcVersion> {
        let query =
            format!("SELECT version FROM turso_cdc_version WHERE table_name = '{cdc_table}'");
        let mut stmt = match connection.prepare(&query) {
            Ok(stmt) => stmt,
            Err(turso_core::LimboError::ParseError(err)) if err.contains("no such table") => {
                return Ok(turso_core::CdcVersion::V1)
            }
            Err(err) => return Err(err.into()),
        };
        match run_stmt_expect_one_row(coro, &mut stmt).await? {
            Some(row) if !row.is_empty() => {
                if let turso_core::Value::Text(text) = &row[0] {
                    text.to_string()
                        .parse()
                        .map_err(|e: turso_core::LimboError| {
                            Error::DatabaseTapeError(e.to_string())
                        })
                } else {
                    Ok(turso_core::CdcVersion::V1)
                }
            }
            _ => Ok(turso_core::CdcVersion::V1),
        }
    }

    /// Builds an iterator which emits [DatabaseTapeOperation] by extracting data from CDC table
    /// Name of the CDC table this tape reads/writes (default `turso_cdc`).
    pub fn cdc_table(&self) -> &str {
        &self.cdc_table
    }

    pub fn iterate_changes(
        &self,
        opts: DatabaseChangesIteratorOpts,
    ) -> Result<DatabaseChangesIterator> {
        tracing::debug!("opening changes iterator with options {:?}", opts);
        let conn = self.inner.connect()?;
        if self.disable_auto_checkpoint {
            conn.wal_auto_actions_disable();
        }

        let cdc_version = self
            .cdc_version
            .read()
            .unwrap()
            .expect("tape must be connected before iterate changes");

        Ok(DatabaseChangesIterator {
            conn,
            cdc_table: self.cdc_table.clone(),
            cdc_version,
            first_change_id: opts.first_change_id,
            batch: VecDeque::with_capacity(opts.batch_size),
            query_stmt: None,
            txn_boundary_returned: false,
            mode: opts.mode,
            batch_size: opts.batch_size,
            ignore_schema_changes: opts.ignore_schema_changes,
            max_change_id_exclusive: opts.max_change_id_exclusive,
        })
    }
    /// Start raw WAL edit session which can append or rollback pages directly in the current WAL
    pub async fn start_wal_session<Ctx>(&self, coro: &Coro<Ctx>) -> Result<DatabaseWalSession> {
        let conn = self.connect(coro).await?;
        let mut wal_session = WalSession::new(conn);
        wal_session.begin()?;
        DatabaseWalSession::new(coro, wal_session).await
    }

    /// Start replay session which can apply [DatabaseTapeOperation] from [Self::iterate_changes]
    pub async fn start_replay_session<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        opts: DatabaseReplaySessionOpts,
    ) -> Result<DatabaseReplaySession> {
        tracing::debug!("opening replay session");
        let conn = self.connect(coro).await?;
        conn.execute("BEGIN IMMEDIATE")?;
        Ok(DatabaseReplaySession {
            conn: conn.clone(),
            cached_delete_stmt: HashMap::new(),
            cached_insert_stmt: HashMap::new(),
            cached_update_stmt: HashMap::new(),
            in_txn: true,
            generator: DatabaseReplayGenerator { conn, opts },
        })
    }
}

pub struct DatabaseWalSession {
    page_size: usize,
    next_wal_frame_no: u64,
    pub wal_session: WalSession,
    prepared_frame: Option<(u32, Vec<u8>)>,
}

impl DatabaseWalSession {
    pub async fn new<Ctx>(coro: &Coro<Ctx>, wal_session: WalSession) -> Result<Self> {
        let conn = wal_session.conn();
        let frames_count = conn.wal_state()?.max_frame;
        let mut page_size_stmt = conn.prepare("PRAGMA page_size")?;
        let Some(row) = run_stmt_expect_one_row(coro, &mut page_size_stmt).await? else {
            return Err(Error::DatabaseTapeError(
                "unable to get database page size".to_string(),
            ));
        };
        if row.len() != 1 {
            return Err(Error::DatabaseTapeError(
                "unexpected columns count for PRAGMA page_size query".to_string(),
            ));
        }
        let turso_core::Value::Numeric(turso_core::Numeric::Integer(page_size)) = row[0] else {
            return Err(Error::DatabaseTapeError(
                "unexpected column type for PRAGMA page_size query".to_string(),
            ));
        };
        Ok(Self {
            page_size: page_size as usize,
            next_wal_frame_no: frames_count + 1,
            wal_session,
            prepared_frame: None,
        })
    }

    pub fn frames_count(&self) -> Result<u64> {
        Ok(self.wal_session.conn().wal_state()?.max_frame)
    }

    pub fn append_page(&mut self, page_no: u32, page: &[u8]) -> Result<()> {
        if page.len() != self.page_size {
            return Err(Error::DatabaseTapeError(format!(
                "page.len() must be equal to page_size: {} != {}",
                page.len(),
                self.page_size
            )));
        }
        self.flush_prepared_frame(0)?;

        let mut frame = vec![0u8; WAL_FRAME_HEADER + self.page_size];
        frame[WAL_FRAME_HEADER..].copy_from_slice(page);
        self.prepared_frame = Some((page_no, frame));

        Ok(())
    }

    pub async fn rollback_page<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        page_no: u32,
        frame_watermark: u64,
    ) -> Result<()> {
        self.flush_prepared_frame(0)?;

        let conn = self.wal_session.conn();
        let mut frame = vec![0u8; WAL_FRAME_HEADER + self.page_size];
        let end_read_result = try_wal_watermark_read_page(
            coro,
            conn,
            page_no,
            &mut frame[WAL_FRAME_HEADER..],
            Some(frame_watermark),
        )
        .await?;
        if end_read_result {
            tracing::trace!("rollback page {}", page_no);
            self.prepared_frame = Some((page_no, frame));
        } else {
            tracing::trace!(
                "skip rollback page {} as no page existed with given watermark",
                page_no
            );
        }

        Ok(())
    }

    pub async fn rollback_changes_after<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        frame_watermark: u64,
    ) -> Result<usize> {
        let conn = self.wal_session.conn();
        let pages = conn.wal_changed_pages_after(frame_watermark)?;
        tracing::debug!("rolling back {} pages", pages.len());
        let pages_cnt = pages.len();
        for page_no in pages {
            self.rollback_page(coro, page_no, frame_watermark).await?;
        }
        Ok(pages_cnt)
    }

    pub fn commit(&mut self, db_size: u32) -> Result<()> {
        self.flush_prepared_frame(db_size)
    }

    fn flush_prepared_frame(&mut self, db_size: u32) -> Result<()> {
        let Some((page_no, mut frame)) = self.prepared_frame.take() else {
            return Ok(());
        };

        let frame_info = WalFrameInfo { db_size, page_no };
        frame_info.put_to_frame_header(&mut frame);

        let frame_no = self.next_wal_frame_no;
        tracing::debug!(
            "flush prepared frame {:?} as frame_no {}",
            frame_info,
            frame_no
        );
        self.wal_session.conn().wal_insert_frame(frame_no, &frame)?;
        self.next_wal_frame_no += 1;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DatabaseChangesIteratorMode {
    Apply,
    Revert,
}

impl DatabaseChangesIteratorMode {
    pub fn query(&self, table_name: &str, limit: usize, bounded_above: bool) -> String {
        let (operation, order) = match self {
            DatabaseChangesIteratorMode::Apply => (">=", "ASC"),
            DatabaseChangesIteratorMode::Revert => ("<=", "DESC"),
        };
        // `change_id < ?` (bound param 2) restricts the scan to change ids the
        // caller has deemed safe to consume — used by the sync push loop to stop
        // at `sequence_watermark_experimental` so it never reads a change id that
        // a concurrent MVCC transaction may still commit below the current max.
        let upper_bound = if bounded_above {
            " AND change_id < ?"
        } else {
            ""
        };
        format!(
            "SELECT * FROM {table_name} WHERE change_id {operation} ?{upper_bound} ORDER BY change_id {order} LIMIT {limit}",
        )
    }
    pub fn first_id(&self) -> i64 {
        match self {
            DatabaseChangesIteratorMode::Apply => -1,
            DatabaseChangesIteratorMode::Revert => i64::MAX,
        }
    }
    pub fn next_id(&self, id: i64) -> i64 {
        match self {
            DatabaseChangesIteratorMode::Apply => id + 1,
            DatabaseChangesIteratorMode::Revert => id - 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseChangesIteratorOpts {
    pub first_change_id: Option<i64>,
    pub batch_size: usize,
    pub mode: DatabaseChangesIteratorMode,
    pub ignore_schema_changes: bool,
    /// Exclusive upper bound on `change_id`: only rows with `change_id < bound`
    /// are returned. `None` means unbounded. The sync push loop sets this to the
    /// CDC sequence watermark so snapshot-isolation reordering cannot make it skip
    /// a not-yet-committed lower change id.
    pub max_change_id_exclusive: Option<i64>,
}

impl Default for DatabaseChangesIteratorOpts {
    fn default() -> Self {
        Self {
            first_change_id: None,
            batch_size: DEFAULT_CHANGES_BATCH_SIZE,
            mode: DatabaseChangesIteratorMode::Apply,
            ignore_schema_changes: true,
            max_change_id_exclusive: None,
        }
    }
}

pub struct DatabaseChangesIterator {
    conn: Arc<turso_core::Connection>,
    cdc_table: Arc<String>,
    cdc_version: turso_core::CdcVersion,
    query_stmt: Option<turso_core::Statement>,
    first_change_id: Option<i64>,
    batch: VecDeque<DatabaseTapeOperation>,
    txn_boundary_returned: bool,
    mode: DatabaseChangesIteratorMode,
    batch_size: usize,
    ignore_schema_changes: bool,
    max_change_id_exclusive: Option<i64>,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";
impl DatabaseChangesIterator {
    pub async fn next<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<Option<DatabaseTapeOperation>> {
        if self.batch.is_empty() {
            self.refill(coro).await?;
        }
        loop {
            let next = if let Some(op) = self.batch.pop_front() {
                self.txn_boundary_returned = matches!(op, DatabaseTapeOperation::Commit);
                Some(op)
            } else if !self.txn_boundary_returned {
                // For v1 (no explicit COMMIT records), emit a synthetic Commit at end of batch.
                // For v2, COMMIT records are already in the batch, but we also emit a final
                // synthetic one at end-of-table for safety.
                self.txn_boundary_returned = true;
                Some(DatabaseTapeOperation::Commit)
            } else {
                None
            };
            if let Some(DatabaseTapeOperation::RowChange(change)) = &next {
                if self.ignore_schema_changes && change.table_name == SQLITE_SCHEMA_TABLE {
                    continue;
                }
            }
            return Ok(next);
        }
    }
    async fn refill<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<()> {
        if self.query_stmt.is_none() {
            let query = self.mode.query(
                &self.cdc_table,
                self.batch_size,
                self.max_change_id_exclusive.is_some(),
            );
            let stmt = match self.conn.prepare(&query) {
                Ok(stmt) => stmt,
                Err(LimboError::ParseError(err)) if err.contains("no such table") => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            self.query_stmt = Some(stmt);
        }
        let query_stmt = self.query_stmt.as_mut().unwrap();

        let change_id_filter = self.first_change_id.unwrap_or(self.mode.first_id());
        query_stmt.reset()?;
        query_stmt.bind_at(
            1.try_into().unwrap(),
            turso_core::Value::from_i64(change_id_filter),
        )?;
        if let Some(max_change_id_exclusive) = self.max_change_id_exclusive {
            query_stmt.bind_at(
                2.try_into().unwrap(),
                turso_core::Value::from_i64(max_change_id_exclusive),
            )?;
        }

        let mut last_change_id = None;
        while let Some(row) = run_stmt_once(coro, query_stmt).await? {
            let database_change = DatabaseChange::from_row(row, self.cdc_version)?;
            last_change_id = Some(database_change.change_id);
            if database_change.change_type == DatabaseChangeType::Commit {
                self.batch.push_back(DatabaseTapeOperation::Commit);
            } else {
                let tape_change = match self.mode {
                    DatabaseChangesIteratorMode::Apply => database_change.into_apply()?,
                    DatabaseChangesIteratorMode::Revert => database_change.into_revert()?,
                };
                self.batch
                    .push_back(DatabaseTapeOperation::RowChange(tape_change));
            }
        }
        if let Some(change_id) = last_change_id {
            self.first_change_id = Some(self.mode.next_id(change_id));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DatabaseReplaySessionOpts {
    pub use_implicit_rowid: bool,
}

impl std::fmt::Debug for DatabaseReplaySessionOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseReplaySessionOpts")
            .field("use_implicit_rowid", &self.use_implicit_rowid)
            .finish()
    }
}

pub(crate) struct CachedStmt {
    stmt: turso_core::Statement,
    info: ReplayInfo,
}

pub struct DatabaseReplaySession {
    pub(crate) conn: Arc<turso_core::Connection>,
    pub(crate) cached_delete_stmt: HashMap<(String, bool), CachedStmt>,
    pub(crate) cached_insert_stmt: HashMap<(String, usize), CachedStmt>,
    pub(crate) cached_update_stmt: HashMap<(String, Vec<bool>), CachedStmt>,
    pub(crate) in_txn: bool,
    pub(crate) generator: DatabaseReplayGenerator,
}

async fn replay_stmt<Ctx>(
    coro: &Coro<Ctx>,
    stmt: &mut turso_core::Statement,
    values: impl IntoIterator<Item = turso_core::Value>,
) -> Result<()> {
    stmt.reset()?;
    for (i, value) in values.into_iter().enumerate() {
        stmt.bind_at((i + 1).try_into().unwrap(), value)?;
    }
    exec_stmt(coro, stmt).await?;
    Ok(())
}

impl DatabaseReplaySession {
    fn clear_cached_statements(&mut self) {
        self.cached_delete_stmt.clear();
        self.cached_insert_stmt.clear();
        self.cached_update_stmt.clear();
    }

    fn schema_drop_sql(kind: DatabaseSchemaKind, name: &str) -> String {
        let object = match kind {
            DatabaseSchemaKind::Table => "TABLE",
            DatabaseSchemaKind::Index => "INDEX",
            DatabaseSchemaKind::Trigger => "TRIGGER",
            DatabaseSchemaKind::View => "VIEW",
        };
        format!("DROP {object} IF EXISTS {}", quote_ident(name))
    }
}

impl DatabaseReplaySession {
    pub fn conn(&self) -> Arc<turso_core::Connection> {
        self.conn.clone()
    }
    pub async fn replay<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        operation: DatabaseTapeOperation,
    ) -> Result<()> {
        match operation {
            DatabaseTapeOperation::Commit => {
                tracing::debug!("replay: commit replayed changes after transaction boundary");
                if self.in_txn {
                    self.conn.execute("COMMIT")?;
                    self.in_txn = false;
                }
            }
            DatabaseTapeOperation::StmtReplay(replay) => {
                self.clear_cached_statements();
                let mut stmt = self.conn.prepare(&replay.sql)?;
                replay_stmt(coro, &mut stmt, replay.values).await?;
                self.clear_cached_statements();
                return Ok(());
            }
            DatabaseTapeOperation::SchemaReplay(replay) => {
                self.clear_cached_statements();
                match replay {
                    DatabaseSchemaReplay::Create { sql } | DatabaseSchemaReplay::Alter { sql } => {
                        self.generator
                            .execute_ddl_idempotent(coro, &sql)
                            .await
                            .map_err(|err| {
                                Error::DatabaseTapeError(format!(
                                    "failed to replay schema DDL `{sql}`: {err}"
                                ))
                            })?;
                    }
                    DatabaseSchemaReplay::Refresh { kind, name, sql } => {
                        if kind != DatabaseSchemaKind::Table {
                            self.conn.execute(Self::schema_drop_sql(kind, &name))?;
                        }
                        self.generator
                            .execute_ddl_idempotent(coro, &sql)
                            .await
                            .map_err(|err| {
                                Error::DatabaseTapeError(format!(
                                    "failed to replay schema refresh DDL `{sql}`: {err}"
                                ))
                            })?;
                    }
                    DatabaseSchemaReplay::Drop { kind, name } => {
                        self.conn.execute(Self::schema_drop_sql(kind, &name))?;
                    }
                }
                self.clear_cached_statements();
                return Ok(());
            }
            DatabaseTapeOperation::RowChange(change) => {
                if !self.in_txn {
                    tracing::trace!("replay: start txn for replaying changes");
                    self.conn.execute("BEGIN IMMEDIATE")?;
                    self.in_txn = true;
                }
                let table = &change.table_name;
                let change_type = (&change.change).into();

                if table == SQLITE_SCHEMA_TABLE {
                    let replay_info = self.generator.replay_info(coro, &change).await?;
                    if replay_info.is_ddl_replay
                        && matches!(
                            replay_info.change_type,
                            DatabaseChangeType::Insert | DatabaseChangeType::Update
                        )
                    {
                        self.generator
                            .execute_ddl_idempotent(coro, &replay_info.query)
                            .await?;
                    } else {
                        self.conn.execute(replay_info.query.as_str())?;
                    }
                } else {
                    match change.change {
                        DatabaseTapeRowChangeType::Delete {
                            before,
                            key: primary_key,
                        } => {
                            let use_rowid = self
                                .generator
                                .delete_uses_rowid(&before, primary_key.as_deref())?;
                            let cache_key =
                                self.populate_delete_stmt(coro, table, use_rowid).await?;
                            tracing::trace!(
                                "ready to use prepared delete statement for replay: key={cache_key:?}"
                            );
                            let cached = self.cached_delete_stmt.get_mut(&cache_key).unwrap();
                            cached.stmt.reset()?;
                            let values = self.generator.replay_delete_values(
                                &cached.info,
                                change.id,
                                before,
                                primary_key,
                            )?;
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                        DatabaseTapeRowChangeType::Insert { after } => {
                            let key = self.populate_insert_stmt(coro, table, after.len()).await?;
                            tracing::trace!(
                                "ready to use prepared insert statement for replay: key={:?}",
                                key
                            );
                            let needs_predelete = {
                                let cached = self.cached_insert_stmt.get(&key).unwrap();
                                self.generator
                                    .upsert_needs_null_safe_predelete(&cached.info, &after)
                            };
                            if needs_predelete {
                                // ON CONFLICT cannot resolve a key with a NULL
                                // component, so remove the row by its NULL-safe
                                // identity before inserting the new image.
                                let delete_key =
                                    self.populate_delete_stmt(coro, table, false).await?;
                                let cached = self.cached_delete_stmt.get_mut(&delete_key).unwrap();
                                cached.stmt.reset()?;
                                let values = self.generator.replay_delete_values(
                                    &cached.info,
                                    change.id,
                                    after.clone(),
                                    None,
                                )?;
                                replay_stmt(coro, &mut cached.stmt, values).await?;
                            }
                            let cached = self.cached_insert_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset()?;
                            let values = self.generator.replay_values(
                                &cached.info,
                                change_type,
                                change.id,
                                after,
                                None,
                                None,
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                        DatabaseTapeRowChangeType::Update {
                            before,
                            after,
                            updates: Some(updates),
                        } => {
                            assert!(updates.len() % 2 == 0);
                            let columns_cnt = updates.len() / 2;
                            let mut columns = Vec::with_capacity(columns_cnt);
                            for value in updates.iter().take(columns_cnt) {
                                columns.push(match value {
                                    turso_core::Value::Numeric(turso_core::Numeric::Integer(x @ (1 | 0))) => *x > 0,
                                    _ => panic!("unexpected 'changes' binary record first-half component: {value:?}")
                                });
                            }
                            let key = self.populate_update_stmt(coro, table, &columns).await?;
                            tracing::trace!(
                                "ready to use prepared update statement for replay: key={:?}",
                                key
                            );
                            let cached = self.cached_update_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset()?;
                            let values = self.generator.replay_values(
                                &cached.info,
                                change_type,
                                change.id,
                                after,
                                Some(updates),
                                Some(before),
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                        DatabaseTapeRowChangeType::Update {
                            before,
                            after,
                            updates: None,
                        } => {
                            let use_rowid = self.generator.delete_uses_rowid(&before, None)?;
                            let key = self.populate_delete_stmt(coro, table, use_rowid).await?;
                            tracing::trace!(
                                "ready to use prepared delete statement for replay of update: key={:?}",
                                key
                            );
                            let cached = self.cached_delete_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset()?;
                            let values = self.generator.replay_delete_values(
                                &cached.info,
                                change.id,
                                before,
                                None,
                            )?;
                            replay_stmt(coro, &mut cached.stmt, values).await?;

                            let key = self.populate_insert_stmt(coro, table, after.len()).await?;
                            tracing::trace!(
                                "ready to use prepared insert statement for replay of update: key={:?}",
                                key
                            );
                            let cached = self.cached_insert_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset()?;
                            let values = self.generator.replay_values(
                                &cached.info,
                                DatabaseChangeType::Insert,
                                change.id,
                                after,
                                None,
                                None,
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    async fn populate_delete_stmt<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        table: &str,
        use_rowid: bool,
    ) -> Result<(String, bool)> {
        let key = (table.to_string(), use_rowid);
        if self.cached_delete_stmt.contains_key(&key) {
            return Ok(key);
        }
        tracing::trace!("prepare delete statement for replay: table={}", table);
        let info = self.generator.delete_query(coro, table, use_rowid).await?;
        let stmt = self.conn.prepare(&info.query)?;
        self.cached_delete_stmt
            .insert(key.clone(), CachedStmt { stmt, info });
        Ok(key)
    }
    async fn populate_insert_stmt<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        table: &str,
        columns: usize,
    ) -> Result<(String, usize)> {
        let key = (table.to_string(), columns);
        if self.cached_insert_stmt.contains_key(&key) {
            return Ok(key);
        }
        tracing::trace!(
            "prepare insert statement for replay: table={}, columns={}",
            table,
            columns
        );
        let info = self.generator.upsert_query(coro, table, columns).await?;
        let stmt = self.conn.prepare(&info.query)?;
        self.cached_insert_stmt
            .insert(key.clone(), CachedStmt { stmt, info });
        Ok(key)
    }
    async fn populate_update_stmt<Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        table: &str,
        columns: &[bool],
    ) -> Result<(String, Vec<bool>)> {
        let key = (table.to_string(), columns.to_owned());
        if self.cached_update_stmt.contains_key(&key) {
            return Ok(key);
        }
        tracing::trace!("prepare update statement for replay: table={}", table);
        let info = self.generator.update_query(coro, table, columns).await?;
        let stmt = self.conn.prepare(&info.query)?;
        self.cached_update_stmt
            .insert(key.clone(), CachedStmt { stmt, info });
        Ok(key)
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
#[path = "../tests/unit/database_tape/tests.rs"]
mod tests;
