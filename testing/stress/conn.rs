use crate::sql_logging::SqlLogger;
use turso::{Builder, Connection, IntoParams, Row, Rows};
use turso_stress::sync::Arc;
use turso_stress::sync::AsyncMutex as Mutex;
use turso_stress::ThreadId;

/// A wrapper around `Database` that simplifies dropping the db.
pub struct StressDb {
    db: Option<turso::Database>,
    db_file: String,
    vfs: Option<String>,
    multiprocess: bool,
    sql_logger: Arc<SqlLogger>,
}

impl StressDb {
    pub(crate) fn new(
        db_file: String,
        sql_logger: Arc<SqlLogger>,
        vfs: Option<String>,
        multiprocess: bool,
    ) -> Self {
        Self {
            db: None,
            db_file,
            sql_logger,
            vfs,
            multiprocess,
        }
    }

    async fn get_or_init(&mut self) -> turso::Result<&turso::Database> {
        if self.db.is_none() {
            let mut builder = Builder::new_local(&self.db_file);
            if let Some(ref vfs) = self.vfs {
                builder = builder.with_io(vfs.clone());
            }
            if self.multiprocess {
                builder = builder.experimental_multiprocess_wal(true);
            }
            self.db = Some(builder.build().await?);
        }
        Ok(self.db.as_ref().unwrap())
    }

    pub fn reset(&mut self) {
        let old = self.db.take();
        if old.is_none() {
            panic!("cannot reset uninitialized StressDb");
        }
    }

    pub async fn connect(
        this: &Arc<Mutex<Self>>,
        thread: ThreadId,
        busy_timeout: u64,
    ) -> turso::Result<StressConn> {
        let conn = {
            let mut db = this.lock().await;
            let raw_conn = db.get_or_init().await?.connect()?;

            StressConn::new(db.sql_logger.clone(), thread, raw_conn)
        };
        conn.busy_timeout(std::time::Duration::from_millis(busy_timeout))?;
        conn.execute("PRAGMA data_sync_retry = 1", ()).await?;
        Ok(conn)
    }
}

pub struct StressConn {
    thread: ThreadId,
    sql_logger: Arc<SqlLogger>,
    conn: Connection,
}

impl StressConn {
    fn new(sql_logger: Arc<SqlLogger>, thread: ThreadId, conn: Connection) -> Self {
        Self {
            thread,
            sql_logger,
            conn,
        }
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> turso::connection::Result<u64> {
        let result = self.conn.execute(sql, params).await;
        self.sql_logger.log_result(&self.thread, sql, &result);
        result
    }

    pub async fn pragma_update<V: std::fmt::Display>(
        &self,
        pragma_name: &str,
        pragma_value: V,
    ) -> turso::connection::Result<Vec<Row>> {
        self.conn.pragma_update(pragma_name, pragma_value).await
    }

    pub fn busy_timeout(&self, duration: std::time::Duration) -> turso::connection::Result<()> {
        self.conn.busy_timeout(duration)
    }

    pub async fn query(
        &self,
        sql: impl AsRef<str>,
        params: impl IntoParams,
    ) -> turso::connection::Result<Rows> {
        self.conn.query(sql, params).await
    }
}
