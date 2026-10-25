use crate::opts::TxMode;
use crate::sql_logging::SqlLogger;
use crate::ThreadRng;
use turso::{Builder, Connection, IntoParams, Row, Rows};
use turso_stress::sync::Arc;
use turso_stress::sync::AsyncMutex as Mutex;
use turso_stress::ThreadId;

/// A wrapper around `Database` that simplifies dropping the db.
pub struct StressDb {
    db: Option<turso::Database>,
    db_file: String,
    vfs: Option<String>,
    sql_logger: Arc<SqlLogger>,
    fts: bool,
}

impl StressDb {
    pub(crate) fn new(
        db_file: String,
        sql_logger: Arc<SqlLogger>,
        vfs: Option<String>,
        fts: bool,
    ) -> Self {
        Self {
            db: None,
            db_file,
            sql_logger,
            vfs,
            fts,
        }
    }

    async fn get_or_init(&mut self) -> turso::Result<&turso::Database> {
        if self.db.is_none() {
            let mut builder = Builder::new_local(&self.db_file).experimental_index_method(self.fts);
            if let Some(ref vfs) = self.vfs {
                builder = builder.with_io(vfs.clone());
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
        if let Err(error) = &result {
            assert_not_engine_bug(&self.thread, sql, error);
        }
        result
    }

    /// Give this connection a random cache size and checkpoint threshold.
    /// Small values make cache spills and checkpoints happen far more often
    /// than the defaults would.
    pub async fn apply_random_settings(
        &self,
        rng: &mut ThreadRng,
        tx_mode: TxMode,
    ) -> turso::Result<()> {
        let cache_size = rng.choose(&[10, 100, 1000, -2000]);
        self.pragma_update("cache_size", cache_size).await?;
        match tx_mode {
            TxMode::SQLite => {
                let frames = rng.choose(&[0, 10, 100, 1000]);
                self.pragma_update("wal_autocheckpoint", frames).await?;
            }
            TxMode::Concurrent => {
                let bytes = rng.choose(&[0, 4096, 65536, 4_120_000]);
                self.pragma_update("mvcc_checkpoint_threshold", bytes)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn rollback_if_open(&self) -> turso::Result<()> {
        if !self.conn.is_autocommit()? {
            self.execute("ROLLBACK", ()).await?;
        }
        Ok(())
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

/// Fail the run on errors that only a broken engine returns. Busy, constraint,
/// conflict and I/O errors are normal under this workload and pass through.
pub fn assert_not_engine_bug(thread: &ThreadId, sql: &str, error: &turso::Error) {
    if is_engine_bug(error) {
        turso_macros::turso_assert_unreachable!("statement failed with an engine bug error", { "thread": thread, "sql": sql, "error": error });
    }
}

fn is_engine_bug(error: &turso::Error) -> bool {
    match error {
        turso::Error::Corrupt(_) | turso::Error::NotAdb(_) => true,
        turso::Error::Error(message) => message.starts_with("Internal error:"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_engine_bug;

    #[test]
    fn internal_and_corruption_errors_are_engine_bugs() {
        assert!(is_engine_bug(&turso::Error::Error(
            "Internal error: page not loaded".into()
        )));
        assert!(is_engine_bug(&turso::Error::Corrupt(
            "Invalid page type: 0".into()
        )));
        assert!(is_engine_bug(&turso::Error::NotAdb(
            "file is not a database".into()
        )));
    }

    #[test]
    fn errors_the_workload_expects_are_not_engine_bugs() {
        assert!(!is_engine_bug(&turso::Error::Busy(
            "database is locked".into()
        )));
        assert!(!is_engine_bug(&turso::Error::Constraint(
            "UNIQUE constraint failed".into()
        )));
        assert!(!is_engine_bug(&turso::Error::Error(
            "Write-write conflict".into()
        )));
        assert!(!is_engine_bug(&turso::Error::Error(
            "Transaction error: cannot start a transaction within a transaction".into()
        )));
    }
}
