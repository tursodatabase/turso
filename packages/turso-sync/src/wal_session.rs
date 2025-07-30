use turso_sync_protocol::Result;

pub struct WalSession {
    conn: turso::Connection,
    in_txn: bool,
}

impl WalSession {
    pub fn new(conn: turso::Connection) -> Self {
        Self {
            conn,
            in_txn: false,
        }
    }
    pub fn conn(&self) -> &turso::Connection {
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
