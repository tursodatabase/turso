use crate::sql_logging::SqlLogger;
use std::sync::Arc;
use turso::{Connection, IntoParams, Row, Rows};

pub struct StressConn {
    thread: usize,
    sql_logger: Arc<SqlLogger>,
    conn: Connection,
}

impl StressConn {
    pub fn new(sql_logger: Arc<SqlLogger>, thread: usize, conn: Connection) -> Self {
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
        self.sql_logger.log_result(self.thread, sql, &result);
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
