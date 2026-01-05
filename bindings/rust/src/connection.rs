use crate::assert_send_sync;
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
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Waker;
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
    inner: Option<Arc<turso_sdk_kit::rsapi::TursoConnection>>,
    pub(crate) transaction_behavior: TransactionBehavior,
    /// If there is a dangling transaction after it was dropped without being finished,
    /// [Connection::dangling_tx] will be set to the [DropBehavior] of the dangling transaction,
    /// and the corresponding action will be taken when a new transaction is requested
    /// or the connection queries/executes.
    /// We cannot do this eagerly on Drop because drop is not async.
    ///
    /// By default, the value is [DropBehavior::Ignore] which effectively does nothing.
    pub(crate) dangling_tx: AtomicDropBehavior,
    pub(crate) extra_io: Option<Arc<dyn Fn(Waker) -> Result<()> + Send + Sync>>,
}

assert_send_sync!(Connection);

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            transaction_behavior: self.transaction_behavior,
            dangling_tx: AtomicDropBehavior::new(self.dangling_tx.load(Ordering::SeqCst)),
            extra_io: self.extra_io.clone(),
        }
    }
}

impl Connection {
    pub fn create(
        conn: Arc<turso_sdk_kit::rsapi::TursoConnection>,
        extra_io: Option<Arc<dyn Fn(Waker) -> Result<()> + Send + Sync>>,
    ) -> Self {
        #[allow(clippy::arc_with_non_send_sync)]
        let connection = Connection {
            inner: Some(conn),
            transaction_behavior: TransactionBehavior::Deferred,
            dangling_tx: AtomicDropBehavior::new(DropBehavior::Ignore),
            extra_io,
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
    pub async fn query(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<Rows> {
        self.maybe_handle_dangling_tx().await?;
        let mut stmt = self.prepare(sql).await?;
        stmt.query(params).await
    }

    /// Execute SQL statement on the database.
    pub async fn execute(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<u64> {
        self.maybe_handle_dangling_tx().await?;
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await
    }

    /// get the inner connection
    fn get_inner_connection(&self) -> Result<Arc<turso_sdk_kit::rsapi::TursoConnection>> {
        match &self.inner {
            Some(inner) => Ok(inner.clone()),
            None => Err(Error::Misuse("inner connection must be set".to_string())),
        }
    }

    /// Execute a batch of SQL statements on the database.
    pub async fn execute_batch(&self, sql: impl AsRef<str>) -> Result<()> {
        self.maybe_handle_dangling_tx().await?;
        self.prepare_execute_batch(sql).await?;
        Ok(())
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        let conn = self.get_inner_connection()?;
        let stmt = conn.prepare_single(sql)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let statement = Statement {
            conn: self.clone(),
            inner: Arc::new(Mutex::new(stmt)),
        };
        Ok(statement)
    }

    async fn prepare_execute_batch(&self, sql: impl AsRef<str>) -> Result<()> {
        self.maybe_handle_dangling_tx().await?;
        let conn = self.get_inner_connection()?;
        let mut sql = sql.as_ref();
        while let Some((stmt, offset)) = conn.prepare_first(sql)? {
            let mut stmt = Statement {
                conn: self.clone(),
                inner: Arc::new(Mutex::new(stmt)),
            };
            let _ = stmt.execute(()).await?;
            sql = &sql[offset..];
        }
        Ok(())
    }

    /// Query a pragma.
    pub async fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> std::result::Result<(), turso_sdk_kit::rsapi::TursoError>,
    {
        let sql = format!("PRAGMA {pragma_name}");
        let mut stmt = self.prepare(&sql).await?;
        let mut rows = stmt.query(()).await?;
        while let Some(row) = rows.next().await? {
            f(&row)?;
        }
        Ok(())
    }

    /// Set a pragma value.
    pub async fn pragma_update<V: std::fmt::Display>(
        &self,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Row>> {
        let sql = format!("PRAGMA {pragma_name} = {pragma_value}");
        let mut stmt = self.prepare(&sql).await?;
        let mut rows = stmt.query(()).await?;
        let mut collected = Vec::new();
        while let Some(row) = rows.next().await? {
            collected.push(row);
        }
        Ok(collected)
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
        conn.cacheflush()?;
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

    /// Create a point-in-time snapshot of the database to the specified path.
    /// Note: Does not work with in-memory databases or MVCC mode.
    pub fn snapshot(&self, output_path: &str) -> Result<()> {
        let conn = self.get_inner_connection()?;
        conn.snapshot(output_path)?;
        Ok(())
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}
