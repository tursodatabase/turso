use std::sync::Arc;

use crate::Result;

pub struct WalSession {
    conn: Arc<turso_core::Connection>,
    in_txn: bool,
}

impl WalSession {
    pub fn new(conn: Arc<turso_core::Connection>) -> Self {
        Self {
            conn,
            in_txn: false,
        }
    }
    pub fn conn(&self) -> &Arc<turso_core::Connection> {
        &self.conn
    }
    pub fn begin(&mut self) -> Result<()> {
        assert!(!self.in_txn);
        self.conn.wal_insert_begin()?;
        self.in_txn = true;
        Ok(())
    }
    pub fn end(&mut self) -> Result<()> {
        assert!(self.in_txn);
        self.conn.wal_insert_end()?;
        self.in_txn = false;
        Ok(())
    }
    pub fn in_txn(&self) -> bool {
        self.in_txn
    }
}

impl Drop for WalSession {
    fn drop(&mut self) {
        if self.in_txn {
            let _ = self
                .end()
                .inspect_err(|e| tracing::error!("failed to close WAL session: {}", e));
        }
    }
}
