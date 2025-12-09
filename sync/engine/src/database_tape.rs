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
        Coro, DatabaseChange, DatabaseChangeType, DatabaseTapeOperation, DatabaseTapeRowChange,
        DatabaseTapeRowChangeType, SyncEngineIoResult,
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
}

const DEFAULT_CDC_TABLE_NAME: &str = "turso_cdc";
const DEFAULT_CDC_MODE: &str = "full";
const DEFAULT_CHANGES_BATCH_SIZE: usize = 100;
pub const CDC_PRAGMA_NAME: &str = "unstable_capture_data_changes_conn";

#[derive(Debug, Clone)]
pub struct DatabaseTapeOpts {
    pub cdc_table: Option<String>,
    pub cdc_mode: Option<String>,
}

pub(crate) async fn run_stmt_once<'a, Ctx>(
    coro: &'_ Coro<Ctx>,
    stmt: &'a mut turso_core::Statement,
) -> Result<Option<&'a turso_core::Row>> {
    loop {
        match stmt.step()? {
            StepResult::IO => {
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
            StepResult::IO => {
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
        }
    }
    pub(crate) fn connect_untracked(&self) -> Result<Arc<turso_core::Connection>> {
        let connection = self.inner.connect()?;
        Ok(connection)
    }
    pub async fn connect<Ctx>(&self, coro: &Coro<Ctx>) -> Result<Arc<turso_core::Connection>> {
        let connection = self.inner.connect()?;
        tracing::debug!("set '{CDC_PRAGMA_NAME}' for new connection");
        let mut stmt = connection.prepare(&self.pragma_query)?;
        run_stmt_ignore_rows(coro, &mut stmt).await?;
        Ok(connection)
    }
    /// Builds an iterator which emits [DatabaseTapeOperation] by extracting data from CDC table
    pub fn iterate_changes(
        &self,
        opts: DatabaseChangesIteratorOpts,
    ) -> Result<DatabaseChangesIterator> {
        tracing::debug!("opening changes iterator with options {:?}", opts);
        let conn = self.inner.connect()?;
        Ok(DatabaseChangesIterator {
            conn,
            cdc_table: self.cdc_table.clone(),
            first_change_id: opts.first_change_id,
            batch: VecDeque::with_capacity(opts.batch_size),
            query_stmt: None,
            txn_boundary_returned: false,
            mode: opts.mode,
            batch_size: opts.batch_size,
            ignore_schema_changes: opts.ignore_schema_changes,
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
        let turso_core::Value::Integer(page_size) = row[0] else {
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

    pub fn rollback_page(&mut self, page_no: u32, frame_watermark: u64) -> Result<()> {
        self.flush_prepared_frame(0)?;

        let conn = self.wal_session.conn();
        let mut frame = vec![0u8; WAL_FRAME_HEADER + self.page_size];
        if conn.try_wal_watermark_read_page(
            page_no,
            &mut frame[WAL_FRAME_HEADER..],
            Some(frame_watermark),
        )? {
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

    pub fn rollback_changes_after(&mut self, frame_watermark: u64) -> Result<usize> {
        let conn = self.wal_session.conn();
        let pages = conn.wal_changed_pages_after(frame_watermark)?;
        tracing::info!("rolling back {} pages", pages.len());
        let pages_cnt = pages.len();
        for page_no in pages {
            self.rollback_page(page_no, frame_watermark)?;
        }
        Ok(pages_cnt)
    }

    pub fn db_size(&self) -> Result<u32> {
        let frames_count = self.frames_count()?;
        let conn = self.wal_session.conn();
        let mut page = vec![0u8; self.page_size];
        assert!(conn.try_wal_watermark_read_page(1, &mut page, Some(frames_count))?);
        let db_size = u32::from_be_bytes(page[28..32].try_into().unwrap());
        Ok(db_size)
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
    pub fn query(&self, table_name: &str, limit: usize) -> String {
        let (operation, order) = match self {
            DatabaseChangesIteratorMode::Apply => (">=", "ASC"),
            DatabaseChangesIteratorMode::Revert => ("<=", "DESC"),
        };
        format!(
            "SELECT * FROM {table_name} WHERE change_id {operation} ? ORDER BY change_id {order} LIMIT {limit}",
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
}

impl Default for DatabaseChangesIteratorOpts {
    fn default() -> Self {
        Self {
            first_change_id: None,
            batch_size: DEFAULT_CHANGES_BATCH_SIZE,
            mode: DatabaseChangesIteratorMode::Apply,
            ignore_schema_changes: true,
        }
    }
}

pub struct DatabaseChangesIterator {
    conn: Arc<turso_core::Connection>,
    cdc_table: Arc<String>,
    query_stmt: Option<turso_core::Statement>,
    first_change_id: Option<i64>,
    batch: VecDeque<DatabaseTapeRowChange>,
    txn_boundary_returned: bool,
    mode: DatabaseChangesIteratorMode,
    batch_size: usize,
    ignore_schema_changes: bool,
}

const SQLITE_SCHEMA_TABLE: &str = "sqlite_schema";
impl DatabaseChangesIterator {
    pub async fn next<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<Option<DatabaseTapeOperation>> {
        if self.batch.is_empty() {
            self.refill(coro).await?;
        }
        // todo(sivukhin): iterator must be more clever about transaction boundaries - but for that we need to extend CDC table
        // for now, if iterator reach the end of CDC table - we are sure that this is a transaction boundary
        loop {
            let next = if let Some(change) = self.batch.pop_front() {
                self.txn_boundary_returned = false;
                Some(DatabaseTapeOperation::RowChange(change))
            } else if !self.txn_boundary_returned {
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
            let query = self.mode.query(&self.cdc_table, self.batch_size);
            let stmt = match self.conn.prepare(&query) {
                Ok(stmt) => stmt,
                Err(LimboError::ParseError(err)) if err.contains("no such table") => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            self.query_stmt = Some(stmt);
        }
        let query_stmt = self.query_stmt.as_mut().unwrap();

        let change_id_filter = self.first_change_id.unwrap_or(self.mode.first_id());
        query_stmt.reset();
        query_stmt.bind_at(
            1.try_into().unwrap(),
            turso_core::Value::Integer(change_id_filter),
        );

        while let Some(row) = run_stmt_once(coro, query_stmt).await? {
            let database_change: DatabaseChange = row.try_into()?;
            let tape_change = match self.mode {
                DatabaseChangesIteratorMode::Apply => database_change.into_apply()?,
                DatabaseChangesIteratorMode::Revert => database_change.into_revert()?,
            };
            self.batch.push_back(tape_change);
        }
        let batch_len = self.batch.len();
        if batch_len > 0 {
            self.first_change_id = Some(self.mode.next_id(self.batch[batch_len - 1].change_id));
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
    pub(crate) cached_delete_stmt: HashMap<String, CachedStmt>,
    pub(crate) cached_insert_stmt: HashMap<(String, usize), CachedStmt>,
    pub(crate) cached_update_stmt: HashMap<(String, Vec<bool>), CachedStmt>,
    pub(crate) in_txn: bool,
    pub(crate) generator: DatabaseReplayGenerator,
}

async fn replay_stmt<Ctx>(
    coro: &Coro<Ctx>,
    stmt: &mut turso_core::Statement,
    values: Vec<turso_core::Value>,
) -> Result<()> {
    stmt.reset();
    for (i, value) in values.into_iter().enumerate() {
        stmt.bind_at((i + 1).try_into().unwrap(), value);
    }
    exec_stmt(coro, stmt).await?;
    Ok(())
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
                let mut stmt = self.conn.prepare(&replay.sql)?;
                replay_stmt(coro, &mut stmt, replay.values).await?;
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
                    self.conn.execute(replay_info.query.as_str())?;
                } else {
                    match change.change {
                        DatabaseTapeRowChangeType::Delete { before } => {
                            let key = self.populate_delete_stmt(coro, table).await?;
                            tracing::trace!(
                                "ready to use prepared delete statement for replay: key={}",
                                key
                            );
                            let cached = self.cached_delete_stmt.get_mut(key).unwrap();
                            cached.stmt.reset();
                            let values = self.generator.replay_values(
                                &cached.info,
                                change_type,
                                change.id,
                                before,
                                None,
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                        DatabaseTapeRowChangeType::Insert { after } => {
                            let key = self.populate_insert_stmt(coro, table, after.len()).await?;
                            tracing::trace!(
                                "ready to use prepared insert statement for replay: key={:?}",
                                key
                            );
                            let cached = self.cached_insert_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset();
                            let values = self.generator.replay_values(
                                &cached.info,
                                change_type,
                                change.id,
                                after,
                                None,
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                        DatabaseTapeRowChangeType::Update {
                            after,
                            updates: Some(updates),
                            ..
                        } => {
                            assert!(updates.len() % 2 == 0);
                            let columns_cnt = updates.len() / 2;
                            let mut columns = Vec::with_capacity(columns_cnt);
                            for value in updates.iter().take(columns_cnt) {
                                columns.push(match value {
                                    turso_core::Value::Integer(x @ (1 | 0)) => *x > 0,
                                    _ => panic!("unexpected 'changes' binary record first-half component: {value:?}")
                                });
                            }
                            let key = self.populate_update_stmt(coro, table, &columns).await?;
                            tracing::trace!(
                                "ready to use prepared update statement for replay: key={:?}",
                                key
                            );
                            let cached = self.cached_update_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset();
                            let values = self.generator.replay_values(
                                &cached.info,
                                change_type,
                                change.id,
                                after,
                                Some(updates),
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;
                        }
                        DatabaseTapeRowChangeType::Update {
                            before,
                            after,
                            updates: None,
                        } => {
                            let key = self.populate_delete_stmt(coro, table).await?;
                            tracing::trace!(
                                "ready to use prepared delete statement for replay of update: key={:?}",
                                key
                            );
                            let cached = self.cached_delete_stmt.get_mut(key).unwrap();
                            cached.stmt.reset();
                            let values = self.generator.replay_values(
                                &cached.info,
                                DatabaseChangeType::Delete,
                                change.id,
                                before,
                                None,
                            );
                            replay_stmt(coro, &mut cached.stmt, values).await?;

                            let key = self.populate_insert_stmt(coro, table, after.len()).await?;
                            tracing::trace!(
                                "ready to use prepared insert statement for replay of update: key={:?}",
                                key
                            );
                            let cached = self.cached_insert_stmt.get_mut(&key).unwrap();
                            cached.stmt.reset();
                            let values = self.generator.replay_values(
                                &cached.info,
                                DatabaseChangeType::Insert,
                                change.id,
                                after,
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
    async fn populate_delete_stmt<'a, Ctx>(
        &mut self,
        coro: &Coro<Ctx>,
        table: &'a str,
    ) -> Result<&'a str> {
        if self.cached_delete_stmt.contains_key(table) {
            return Ok(table);
        }
        tracing::trace!("prepare delete statement for replay: table={}", table);
        let info = self.generator.delete_query(coro, table).await?;
        let stmt = self.conn.prepare(&info.query)?;
        self.cached_delete_stmt
            .insert(table.to_string(), CachedStmt { stmt, info });
        Ok(table)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::NamedTempFile;

    use crate::{
        database_tape::{
            run_stmt_once, DatabaseChangesIteratorOpts, DatabaseReplaySessionOpts, DatabaseTape,
        },
        types::{Coro, DatabaseTapeOperation, DatabaseTapeRowChange, DatabaseTapeRowChangeType},
    };

    #[test]
    pub fn test_database_tape_connect() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));
        let mut gen = genawaiter::sync::Gen::new({
            let db1 = db1.clone();
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn = db1.connect(&coro).await.unwrap();
                let mut stmt = conn.prepare("SELECT * FROM turso_cdc").unwrap();
                let mut rows = Vec::new();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                rows
            }
        });
        let rows = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        };
        assert_eq!(rows, vec![] as Vec<Vec<turso_core::Value>>);
    }

    #[test]
    pub fn test_database_tape_iterate_changes() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let mut gen = genawaiter::sync::Gen::new({
            let db1 = db1.clone();
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn = db1.connect(&coro).await.unwrap();
                conn.execute("CREATE TABLE t(x)").unwrap();
                conn.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
                let opts = Default::default();
                let mut iterator = db1.iterate_changes(opts).unwrap();
                let mut changes = Vec::new();
                while let Some(change) = iterator.next(&coro).await.unwrap() {
                    changes.push(change);
                }
                changes
            }
        });
        let changes = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        };
        tracing::info!("changes: {:?}", changes);
        assert_eq!(changes.len(), 4);
        assert!(matches!(
            changes[0],
            DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                change_id: 2,
                id: 1,
                ref table_name,
                change: DatabaseTapeRowChangeType::Insert { .. },
                ..
            }) if table_name == "t"
        ));
        assert!(matches!(
            changes[1],
            DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                change_id: 3,
                id: 2,
                ref table_name,
                change: DatabaseTapeRowChangeType::Insert { .. },
                ..
            }) if table_name == "t"
        ));
        assert!(matches!(
            changes[2],
            DatabaseTapeOperation::RowChange(DatabaseTapeRowChange {
                change_id: 4,
                id: 3,
                ref table_name,
                change: DatabaseTapeRowChangeType::Insert { .. },
                ..
            }) if table_name == "t"
        ));
        assert!(matches!(changes[3], DatabaseTapeOperation::Commit));
    }

    #[test]
    pub fn test_database_tape_replay_changes_preserve_rowid() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let mut gen = genawaiter::sync::Gen::new({
            let db1 = db1.clone();
            let db2 = db2.clone();
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1.execute("CREATE TABLE t(x)").unwrap();
                conn1
                    .execute("INSERT INTO t(rowid, x) VALUES (10, 1), (20, 2)")
                    .unwrap();
                let conn2 = db2.connect(&coro).await.unwrap();
                conn2.execute("CREATE TABLE t(x)").unwrap();
                conn2
                    .execute("INSERT INTO t(rowid, x) VALUES (1, -1), (2, -2)")
                    .unwrap();

                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: true,
                    };
                    let mut session = db2.start_replay_session(&coro, opts).await.unwrap();
                    let opts = Default::default();
                    let mut iterator = db1.iterate_changes(opts).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut stmt = conn2.prepare("SELECT rowid, x FROM t").unwrap();
                let mut rows = Vec::new();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                rows
            }
        });
        let rows = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(rows) => break rows,
            }
        };
        tracing::info!("rows: {:?}", rows);
        assert_eq!(
            rows,
            vec![
                vec![
                    turso_core::Value::Integer(1),
                    turso_core::Value::Integer(-1)
                ],
                vec![
                    turso_core::Value::Integer(2),
                    turso_core::Value::Integer(-2)
                ],
                vec![
                    turso_core::Value::Integer(10),
                    turso_core::Value::Integer(1)
                ],
                vec![
                    turso_core::Value::Integer(20),
                    turso_core::Value::Integer(2)
                ]
            ]
        );
    }

    #[test]
    pub fn test_database_tape_replay_changes_do_not_preserve_rowid() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let mut gen = genawaiter::sync::Gen::new({
            let db1 = db1.clone();
            let db2 = db2.clone();
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1.execute("CREATE TABLE t(x)").unwrap();
                conn1
                    .execute("INSERT INTO t(rowid, x) VALUES (10, 1), (20, 2)")
                    .unwrap();
                let conn2 = db2.connect(&coro).await.unwrap();
                conn2.execute("CREATE TABLE t(x)").unwrap();
                conn2
                    .execute("INSERT INTO t(rowid, x) VALUES (1, -1), (2, -2)")
                    .unwrap();

                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db2.start_replay_session(&coro, opts).await.unwrap();
                    let opts = Default::default();
                    let mut iterator = db1.iterate_changes(opts).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut stmt = conn2.prepare("SELECT rowid, x FROM t").unwrap();
                let mut rows = Vec::new();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                rows
            }
        });
        let rows = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(rows) => break rows,
            }
        };
        tracing::info!("rows: {:?}", rows);
        assert_eq!(
            rows,
            vec![
                vec![
                    turso_core::Value::Integer(1),
                    turso_core::Value::Integer(-1)
                ],
                vec![
                    turso_core::Value::Integer(2),
                    turso_core::Value::Integer(-2)
                ],
                vec![turso_core::Value::Integer(3), turso_core::Value::Integer(1)],
                vec![turso_core::Value::Integer(4), turso_core::Value::Integer(2)]
            ]
        );
    }

    #[test]
    pub fn test_database_tape_replay_changes_delete() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let mut gen = genawaiter::sync::Gen::new({
            let db1 = db1.clone();
            let db2 = db2.clone();
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1.execute("CREATE TABLE t(x TEXT PRIMARY KEY)").unwrap();
                conn1.execute("INSERT INTO t(x) VALUES ('a')").unwrap();
                conn1.execute("DELETE FROM t").unwrap();
                let conn2 = db2.connect(&coro).await.unwrap();
                conn2.execute("CREATE TABLE t(x TEXT PRIMARY KEY)").unwrap();
                conn2.execute("INSERT INTO t(x) VALUES ('b')").unwrap();

                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db2.start_replay_session(&coro, opts).await.unwrap();
                    let opts = Default::default();
                    let mut iterator = db1.iterate_changes(opts).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut stmt = conn2.prepare("SELECT rowid, x FROM t").unwrap();
                let mut rows = Vec::new();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                rows
            }
        });
        let rows = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(rows) => break rows,
            }
        };
        tracing::info!("rows: {:?}", rows);
        assert_eq!(
            rows,
            vec![vec![
                turso_core::Value::Integer(1),
                turso_core::Value::Text(turso_core::types::Text::new("b"))
            ]]
        );
    }

    #[test]
    pub fn test_database_tape_replay_schema_changes() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();
        let temp_file3 = NamedTempFile::new().unwrap();
        let db_path3 = temp_file3.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let db3 = turso_core::Database::open_file(io.clone(), db_path3, false).unwrap();
        let db3 = Arc::new(DatabaseTape::new(db3));

        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
                    .unwrap();
                conn1
                    .execute("INSERT INTO t(x, y) VALUES ('a', 10)")
                    .unwrap();
                let conn2 = db2.connect(&coro).await.unwrap();
                conn2
                    .execute("CREATE TABLE q(x TEXT PRIMARY KEY, y)")
                    .unwrap();
                conn2
                    .execute("INSERT INTO q(x, y) VALUES ('b', 20)")
                    .unwrap();

                let conn3 = db3.connect(&coro).await.unwrap();
                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                    let opts = DatabaseChangesIteratorOpts {
                        ignore_schema_changes: false,
                        ..Default::default()
                    };
                    let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                    let mut iterator = db2.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut rows = Vec::new();
                let mut stmt = conn3.prepare("SELECT rowid, x, y FROM t").unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                assert_eq!(
                    rows,
                    vec![vec![
                        turso_core::Value::Integer(1),
                        turso_core::Value::Text(turso_core::types::Text::new("a")),
                        turso_core::Value::Integer(10),
                    ]]
                );

                let mut rows = Vec::new();
                let mut stmt = conn3.prepare("SELECT rowid, x, y FROM q").unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                assert_eq!(
                    rows,
                    vec![vec![
                        turso_core::Value::Integer(1),
                        turso_core::Value::Text(turso_core::types::Text::new("b")),
                        turso_core::Value::Integer(20),
                    ]]
                );
                let mut rows = Vec::new();
                let mut stmt = conn3
                    .prepare(
                        "SELECT * FROM sqlite_schema WHERE name != 'turso_cdc' AND type = 'table'",
                    )
                    .unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            turso_core::Value::Text(turso_core::types::Text::new("table")),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "sqlite_sequence"
                            )),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "sqlite_sequence"
                            )),
                            turso_core::Value::Integer(2),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "CREATE TABLE sqlite_sequence(name,seq)"
                            )),
                        ],
                        vec![
                            turso_core::Value::Text(turso_core::types::Text::new("table")),
                            turso_core::Value::Text(turso_core::types::Text::new("t")),
                            turso_core::Value::Text(turso_core::types::Text::new("t")),
                            turso_core::Value::Integer(4),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "CREATE TABLE t (x TEXT PRIMARY KEY, y)"
                            )),
                        ],
                        vec![
                            turso_core::Value::Text(turso_core::types::Text::new("table")),
                            turso_core::Value::Text(turso_core::types::Text::new("q")),
                            turso_core::Value::Text(turso_core::types::Text::new("q")),
                            turso_core::Value::Integer(6),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "CREATE TABLE q (x TEXT PRIMARY KEY, y)"
                            )),
                        ]
                    ]
                );
                crate::Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    pub fn test_database_tape_replay_create_index() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
                    .unwrap();
                conn1.execute("CREATE INDEX t_idx ON t(y)").unwrap();

                let conn2 = db2.connect(&coro).await.unwrap();
                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                    let opts = DatabaseChangesIteratorOpts {
                        ignore_schema_changes: false,
                        ..Default::default()
                    };
                    let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut rows = Vec::new();
                let mut stmt = conn2
                    .prepare("SELECT * FROM sqlite_schema WHERE name IN ('t', 't_idx')")
                    .unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            turso_core::Value::Text(turso_core::types::Text::new("table")),
                            turso_core::Value::Text(turso_core::types::Text::new("t")),
                            turso_core::Value::Text(turso_core::types::Text::new("t")),
                            turso_core::Value::Integer(4),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "CREATE TABLE t (x TEXT PRIMARY KEY, y)"
                            )),
                        ],
                        vec![
                            turso_core::Value::Text(turso_core::types::Text::new("index")),
                            turso_core::Value::Text(turso_core::types::Text::new("t_idx")),
                            turso_core::Value::Text(turso_core::types::Text::new("t")),
                            turso_core::Value::Integer(6),
                            turso_core::Value::Text(turso_core::types::Text::new(
                                "CREATE INDEX IF NOT EXISTS t_idx ON t (y)"
                            )),
                        ]
                    ]
                );
                crate::Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    pub fn test_database_tape_replay_alter_table() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
                    .unwrap();
                conn1.execute("ALTER TABLE t ADD COLUMN z").unwrap();
                conn1.execute("ALTER TABLE t DROP COLUMN y").unwrap();

                let conn2 = db2.connect(&coro).await.unwrap();
                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db2.start_replay_session(&coro, opts).await.unwrap();

                    let opts = DatabaseChangesIteratorOpts {
                        ignore_schema_changes: false,
                        ..Default::default()
                    };
                    let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut rows = Vec::new();
                let mut stmt = conn2
                    .prepare("SELECT * FROM sqlite_schema WHERE name IN ('t')")
                    .unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                assert_eq!(
                    rows,
                    vec![vec![
                        turso_core::Value::Text(turso_core::types::Text::new("table")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::Text(turso_core::types::Text::new("t")),
                        turso_core::Value::Integer(4),
                        turso_core::Value::Text(turso_core::types::Text::new(
                            "CREATE TABLE t (x TEXT PRIMARY KEY, z)"
                        )),
                    ]]
                );
                crate::Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    pub fn test_database_tape_replay_non_overlapping_updates() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();
        let temp_file3 = NamedTempFile::new().unwrap();
        let db_path3 = temp_file3.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let db3 = turso_core::Database::open_file(io.clone(), db_path3, false).unwrap();
        let db3 = Arc::new(DatabaseTape::new(db3));

        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                    .unwrap();
                conn1
                    .execute("INSERT INTO t VALUES ('turso', 1, 2)")
                    .unwrap();
                conn1
                    .execute("UPDATE t SET y = 10 WHERE x = 'turso'")
                    .unwrap();

                let conn2 = db2.connect_untracked().unwrap();
                conn2
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                    .unwrap();
                conn2
                    .execute("INSERT INTO t VALUES ('turso', 1, 2)")
                    .unwrap();

                let conn2 = db2.connect(&coro).await.unwrap();
                conn2
                    .execute("UPDATE t SET z = 20 WHERE x = 'turso'")
                    .unwrap();

                let conn3 = db3.connect(&coro).await.unwrap();
                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                    let opts = DatabaseChangesIteratorOpts {
                        ignore_schema_changes: false,
                        ..Default::default()
                    };
                    let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }

                    let mut iterator = db2.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut rows = Vec::new();
                let mut stmt = conn3.prepare("SELECT * FROM t").unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                assert_eq!(
                    rows,
                    vec![vec![
                        turso_core::Value::Text(turso_core::types::Text::new("turso")),
                        turso_core::Value::Integer(10),
                        turso_core::Value::Integer(20),
                    ]]
                );
                crate::Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    pub fn test_database_tape_replay_ddl_changes_idempotent() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();
        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();
        let temp_file3 = NamedTempFile::new().unwrap();
        let db_path3 = temp_file3.path().to_str().unwrap();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let db1 = turso_core::Database::open_file(io.clone(), db_path1, false).unwrap();
        let db1 = Arc::new(DatabaseTape::new(db1));

        let db2 = turso_core::Database::open_file(io.clone(), db_path2, false).unwrap();
        let db2 = Arc::new(DatabaseTape::new(db2));

        let db3 = turso_core::Database::open_file(io.clone(), db_path3, false).unwrap();
        let db3 = Arc::new(DatabaseTape::new(db3));

        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn1 = db1.connect(&coro).await.unwrap();
                conn1
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                    .unwrap();
                conn1.execute("CREATE INDEX t_idx ON t(y, z)").unwrap();

                let conn2 = db2.connect(&coro).await.unwrap();
                conn2
                    .execute("CREATE TABLE t(x TEXT PRIMARY KEY, y, z)")
                    .unwrap();
                conn2.execute("CREATE INDEX t_idx ON t(y, z)").unwrap();

                let conn3 = db3.connect(&coro).await.unwrap();
                {
                    let opts = DatabaseReplaySessionOpts {
                        use_implicit_rowid: false,
                    };
                    let mut session = db3.start_replay_session(&coro, opts).await.unwrap();

                    let opts = DatabaseChangesIteratorOpts {
                        ignore_schema_changes: false,
                        ..Default::default()
                    };
                    let mut iterator = db1.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        tracing::info!("1. operation: {:?}", operation);
                        session.replay(&coro, operation).await.unwrap();
                    }

                    let mut iterator = db2.iterate_changes(opts.clone()).unwrap();
                    while let Some(operation) = iterator.next(&coro).await.unwrap() {
                        tracing::info!("2. operation: {:?}", operation);
                        session.replay(&coro, operation).await.unwrap();
                    }
                }
                let mut rows = Vec::new();
                let mut stmt = conn3.prepare("SELECT name FROM sqlite_master").unwrap();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_value(0).to_text().unwrap().to_string());
                }
                assert_eq!(
                    rows,
                    vec![
                        "sqlite_sequence".to_string(),
                        "turso_cdc".to_string(),
                        "t".to_string(),
                        "sqlite_autoindex_t_1".to_string(),
                        "t_idx".to_string()
                    ]
                );
                crate::Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }
}
