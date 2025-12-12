use crate::transaction::DropBehavior;
use crate::transaction::TransactionBehavior;
use crate::Error;
use crate::IntoParams;
use crate::Row;
use crate::Rows;
use crate::Statement;
use std::fmt::Debug;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};
#[cfg(feature = "conn_raw_api")]
use turso_core::types::WalFrameInfo;
pub type Result<T> = std::result::Result<T, Error>;

/// Atomic wrapper for [DropBehavior]
pub(crate) struct AtomicDropBehavior {
    inner: AtomicU8,
}

impl AtomicDropBehavior {
    fn new(behavior: DropBehavior) -> Self {
        Self {
            inner: AtomicU8::new(behavior.into()),
        }
    }

    fn load(&self, ordering: Ordering) -> DropBehavior {
        self.inner.load(ordering).into()
    }

    pub(crate) fn store(&self, behavior: DropBehavior, ordering: Ordering) {
        self.inner.store(behavior.into(), ordering);
    }
}

// A database connection.
pub struct Connection {
    /// Inner is an Option so that when a Connection is dropped we can take the inner
    /// (Actual connection) out of it and put it back into the ConnectionPool
    /// the only time inner will be None is just before the Connection is freed after the
    /// inner connection has been recyled into the connection pool
    inner: Option<Arc<Mutex<Arc<turso_core::Connection>>>>,
    pub(crate) transaction_behavior: TransactionBehavior,
    /// If there is a dangling transaction after it was dropped without being finished,
    /// [Connection::dangling_tx] will be set to the [DropBehavior] of the dangling transaction,
    /// and the corresponding action will be taken when a new transaction is requested
    /// or the connection queries/executes.
    /// We cannot do this eagerly on Drop because drop is not async.
    ///
    /// By default, the value is [DropBehavior::Ignore] which effectively does nothing.
    pub(crate) dangling_tx: AtomicDropBehavior,
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        let i = self.inner.clone();

        Self {
            inner: i,
            //inner: Arc::clone(&self.inner),
            transaction_behavior: self.transaction_behavior,
            dangling_tx: AtomicDropBehavior::new(self.dangling_tx.load(Ordering::SeqCst)),
        }
    }
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    pub fn create(conn: Arc<turso_core::Connection>) -> Self {
        #[allow(clippy::arc_with_non_send_sync)]
        let connection = Connection {
            inner: Some(Arc::new(Mutex::new(conn))),
            transaction_behavior: TransactionBehavior::Deferred,
            dangling_tx: AtomicDropBehavior::new(DropBehavior::Ignore),
        };
        connection
    }

    pub(crate) async fn maybe_handle_dangling_tx(&self) -> Result<()> {
        match self.dangling_tx.load(Ordering::SeqCst) {
            DropBehavior::Rollback => {
                let mut stmt = self.prepare("ROLLBACK").await?;
                stmt.execute(()).await?;
                self.dangling_tx
                    .store(DropBehavior::Ignore, Ordering::SeqCst);
            }
            DropBehavior::Commit => {
                let mut stmt = self.prepare("COMMIT").await?;
                stmt.execute(()).await?;
                self.dangling_tx
                    .store(DropBehavior::Ignore, Ordering::SeqCst);
            }
            DropBehavior::Ignore => {}
            DropBehavior::Panic => {
                panic!("Transaction dropped unexpectedly.");
            }
        }
        Ok(())
    }

    /// Query the database with SQL.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        self.maybe_handle_dangling_tx().await?;
        let mut stmt = self.prepare(sql).await?;
        stmt.query(params).await
    }

    /// Execute SQL statement on the database.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        self.maybe_handle_dangling_tx().await?;
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await
    }

    /// get the inner connection
    fn get_inner_connection(&self) -> Result<MutexGuard<'_, Arc<turso_core::Connection>>> {
        match &self.inner {
            Some(inner) => Ok(inner.lock().map_err(|e| Error::MutexError(e.to_string()))?),
            None => Err(Error::MutexError(
                "Inner connection can't be none".to_string(),
            )),
        }
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn wal_frame_count(&self) -> Result<u64> {
        let conn = self.get_inner_connection()?;
        conn.wal_state()
            .map_err(|e| Error::WalOperationError(format!("wal_insert_begin failed: {e}")))
            .map(|state| state.max_frame)
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn try_wal_watermark_read_page(
        &self,
        page_idx: u32,
        page: &mut [u8],
        frame_watermark: Option<u64>,
    ) -> Result<bool> {
        let conn = self.get_inner_connection()?;
        conn.try_wal_watermark_read_page(page_idx, page, frame_watermark)
            .map_err(|e| {
                Error::WalOperationError(format!("try_wal_watermark_read_page failed: {e}"))
            })
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let conn = self.get_inner_connection()?;
        conn.wal_changed_pages_after(frame_watermark)
            .map_err(|e| Error::WalOperationError(format!("wal_changed_pages_after failed: {e}")))
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn wal_insert_begin(&self) -> Result<()> {
        let conn = self.get_inner_connection()?;
        conn.wal_insert_begin()
            .map_err(|e| Error::WalOperationError(format!("wal_insert_begin failed: {e}")))
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn wal_insert_end(&self, force_commit: bool) -> Result<()> {
        let conn = self.get_inner_connection()?;
        conn.wal_insert_end(force_commit)
            .map_err(|e| Error::WalOperationError(format!("wal_insert_end failed: {e}")))
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        let conn = self.get_inner_connection()?;
        conn.wal_insert_frame(frame_no, frame)
            .map_err(|e| Error::WalOperationError(format!("wal_insert_frame failed: {e}")))
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<WalFrameInfo> {
        let conn = self.get_inner_connection()?;
        conn.wal_get_frame(frame_no, frame)
            .map_err(|e| Error::WalOperationError(format!("wal_insert_frame failed: {e}")))
    }

    /// Execute a batch of SQL statements on the database.
    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.maybe_handle_dangling_tx().await?;
        self.prepare_execute_batch(sql).await?;
        Ok(())
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        let conn = self.get_inner_connection()?;
        let stmt = conn.prepare(sql)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let statement = Statement {
            inner: Arc::new(Mutex::new(stmt)),
        };
        Ok(statement)
    }

    async fn prepare_execute_batch(&self, sql: impl AsRef<str>) -> Result<()> {
        self.maybe_handle_dangling_tx().await?;
        let conn = self.get_inner_connection()?;
        conn.prepare_execute_batch(sql)?;
        Ok(())
    }

    /// Query a pragma.
    pub fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> turso_core::Result<()>,
    {
        let conn = self.get_inner_connection()?;
        let rows: Vec<Row> = conn
            .pragma_query(pragma_name)
            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?
            .iter()
            .map(|row| row.iter().collect::<Row>())
            .collect();

        rows.iter().try_for_each(|row| {
            f(row).map_err(|e| {
                Error::SqlExecutionFailure(format!("Error executing user defined function: {e}"))
            })
        })?;
        Ok(())
    }

    /// Returns the rowid of the last row inserted.
    pub fn last_insert_rowid(&self) -> i64 {
        let conn = self.get_inner_connection().unwrap();
        conn.last_insert_rowid()
    }

    /// Flush dirty pages to disk.
    /// This will write the dirty pages to the WAL.
    pub fn cacheflush(&self) -> Result<()> {
        let conn = self.get_inner_connection()?;
        let completions = conn.cacheflush()?;
        let pager = conn.get_pager();
        for c in completions {
            pager.io.wait_for_completion(c)?;
        }
        Ok(())
    }

    pub fn is_autocommit(&self) -> Result<bool> {
        let conn = self.get_inner_connection()?;
        Ok(conn.get_auto_commit())
    }

    /// Sets maximum total accumuated timeout. If the duration is None or Zero, we unset the busy handler for this Connection
    ///
    /// This api defers slighty from: https://www.sqlite.org/c3ref/busy_timeout.html
    ///
    /// Instead of sleeping for linear amount of time specified by the user,
    /// we will sleep in phases, until the the total amount of time is reached.
    /// This means we first sleep of 1ms, then if we still return busy, we sleep for 2 ms, and repeat until a maximum of 100 ms per phase.
    ///
    /// Example:
    /// 1. Set duration to 5ms
    /// 2. Step through query -> returns Busy -> sleep/yield for 1 ms
    /// 3. Step through query -> returns Busy -> sleep/yield for 2 ms
    /// 4. Step through query -> returns Busy -> sleep/yield for 2 ms (totaling 5 ms of sleep)
    /// 5. Step through query -> returns Busy -> return Busy to user
    pub fn busy_timeout(&self, duration: std::time::Duration) -> Result<()> {
        let conn = self.get_inner_connection()?;
        conn.set_busy_timeout(duration);
        Ok(())
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}
