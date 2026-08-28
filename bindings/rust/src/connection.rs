use crate::assert_send_sync;
use crate::batch::{BatchResult, BatchStatement, IntoBatchStatement};
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
    pub(crate) fn create(
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

    /// Execute multiple parameterized statements as a batch.
    ///
    /// The statements execute in order. Execution stops at the first
    /// statement that fails: the remaining statements are skipped and the
    /// returned [`Error::BatchStatementFailed`](crate::Error::BatchStatementFailed)
    /// carries the zero-based index of the failing statement together with
    /// the underlying error.
    ///
    /// The batch is not transactional: each statement commits as it
    /// executes, so statements that ran before a failure stay committed.
    /// For all-or-nothing execution use
    /// [`transactional_batch`](Connection::transactional_batch). If a
    /// transaction is open on this connection — including when calling
    /// through a [`Transaction`](crate::transaction::Transaction) — the
    /// statements join it instead of committing individually.
    ///
    /// Accepts plain SQL strings, `(sql, params)` pairs, and
    /// [`crate::BatchStatement`]s (see [`IntoBatchStatement`]). Returns one
    /// [`BatchResult`] per statement, in order.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn run(conn: turso::Connection) -> turso::Result<()> {
    /// // Statements whose parameters have the same type can be passed
    /// // as (sql, params) pairs.
    /// conn.batch([
    ///     ("INSERT INTO users (name) VALUES (?1)", ("Alice",)),
    ///     ("INSERT INTO users (name) VALUES (?1)", ("Bob",)),
    /// ])
    /// .await?;
    ///
    /// // Batches mixing parameter shapes use `BatchStatement`.
    /// use turso::BatchStatement;
    /// let results = conn
    ///     .batch(vec![
    ///         BatchStatement::new("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())?,
    ///         BatchStatement::new("INSERT INTO t (v) VALUES (?1)", ("x",))?,
    ///     ])
    ///     .await?;
    /// assert_eq!(results[1].rows_affected(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch<I>(&self, stmts: I) -> Result<Vec<BatchResult>>
    where
        I: IntoIterator,
        I::Item: IntoBatchStatement,
    {
        self.run_batch(stmts, None).await
    }

    /// Execute multiple parameterized statements atomically.
    ///
    /// Like [`batch`](Connection::batch), but the statements are wrapped in
    /// `BEGIN <behavior>` / `COMMIT`, with a `ROLLBACK` on failure: either
    /// every statement commits or none does. On failure the returned
    /// [`Error::BatchStatementFailed`](crate::Error::BatchStatementFailed)
    /// carries the zero-based index of the failing statement.
    ///
    /// This method owns the surrounding transaction, so the statements must
    /// not contain their own transaction-control SQL (`BEGIN`, `COMMIT`,
    /// `ROLLBACK`, `SAVEPOINT`, `RELEASE`); a user-supplied `COMMIT` would
    /// close the wrapper transaction mid-batch and leave earlier statements
    /// committed, defeating the all-or-nothing contract. If a transaction
    /// is already open on this connection, the wrapping is skipped and the
    /// statements join it, exactly as with [`batch`](Connection::batch).
    pub async fn transactional_batch<I>(
        &self,
        stmts: I,
        behavior: TransactionBehavior,
    ) -> Result<Vec<BatchResult>>
    where
        I: IntoIterator,
        I::Item: IntoBatchStatement,
    {
        self.run_batch(stmts, Some(behavior)).await
    }

    async fn run_batch<I>(
        &self,
        stmts: I,
        wrap: Option<TransactionBehavior>,
    ) -> Result<Vec<BatchResult>>
    where
        I: IntoIterator,
        I::Item: IntoBatchStatement,
    {
        let stmts = stmts
            .into_iter()
            .enumerate()
            .map(|(index, stmt)| {
                stmt.into_batch_statement()
                    .map_err(|error| Error::BatchStatementFailed {
                        index,
                        error: Box::new(error),
                        results: Vec::new(),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if stmts.is_empty() {
            return Ok(Vec::new());
        }
        self.maybe_handle_dangling_tx().await?;
        // With a transaction already open on the connection, another BEGIN
        // would fail; the statements join the open transaction instead
        // (matching the serverless driver).
        let wrap = if self.is_autocommit()? { wrap } else { None };
        if let Some(behavior) = wrap {
            self.execute(behavior.begin_sql(), ()).await?;
        }
        let statement_count = stmts.len();
        let mut results = Vec::with_capacity(statement_count);
        for (index, stmt) in stmts.into_iter().enumerate() {
            match self.execute_batch_statement(stmt).await {
                Ok(result) => results.push(result),
                Err(error) => {
                    // ROLLBACK errors are ignored: the statement failure is
                    // the error being reported, and surfacing a rollback
                    // error would mask it.
                    if wrap.is_some() {
                        let _ = self.execute("ROLLBACK", ()).await;
                    }
                    // One entry per statement: the completed statements'
                    // results, None for the failing and skipped ones.
                    let mut partial: Vec<Option<BatchResult>> =
                        results.into_iter().map(Some).collect();
                    partial.resize_with(statement_count, || None);
                    return Err(Error::BatchStatementFailed {
                        index,
                        error: Box::new(error),
                        results: partial,
                    });
                }
            }
        }
        if wrap.is_some() {
            if let Err(error) = self.execute("COMMIT", ()).await {
                let _ = self.execute("ROLLBACK", ()).await;
                return Err(error);
            }
        }
        Ok(results)
    }

    /// Execute one statement of a batch, buffering its rows.
    async fn execute_batch_statement(&self, stmt: BatchStatement) -> Result<BatchResult> {
        let rowid_before = self.last_insert_rowid();
        let mut prepared = self.prepare(&stmt.sql).await?;
        let mut rows = prepared.query(stmt.params).await?;
        let columns = rows.columns();
        let mut buffered = Vec::new();
        while let Some(row) = rows.next().await? {
            buffered.push(row);
        }
        let rowid_after = self.last_insert_rowid();
        // The engine tracks the inserted rowid per connection, not per
        // statement; a change across this statement means it inserted.
        let last_insert_rowid = (rowid_after != rowid_before).then_some(rowid_after);
        // n_change reports the connection's last change count, which a
        // row-returning statement does not update; report 0 for those
        // rather than the previous statement's count, matching the server.
        let rows_affected = if columns.is_empty() {
            prepared.n_change()
        } else {
            0
        };
        Ok(BatchResult::new(
            columns,
            buffered,
            rows_affected,
            last_insert_rowid,
        ))
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

    /// Prepare a SQL statement for later execution, caching it in the connection.
    pub async fn prepare_cached(&self, sql: impl AsRef<str>) -> Result<Statement> {
        let conn = self.get_inner_connection()?;
        let stmt = conn.prepare_cached(sql)?;

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
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}
