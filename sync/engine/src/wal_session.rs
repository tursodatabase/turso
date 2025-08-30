use std::sync::Arc;

use turso_core::types::WalFrameInfo;

use crate::Result;

pub struct WalSession {
    conn: Arc<turso_core::Connection>,
    in_txn: bool,
}

unsafe impl Send for WalSession {}
unsafe impl Sync for WalSession {}

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
    pub fn insert_at(&mut self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        assert!(self.in_txn);
        let info = self.conn.wal_insert_frame(frame_no, frame)?;
        Ok(info)
    }
    pub fn read_at(&mut self, frame_no: u64, frame: &mut [u8]) -> Result<WalFrameInfo> {
        assert!(self.in_txn);
        let info = self.conn.wal_get_frame(frame_no, frame)?;
        Ok(info)
    }
    pub fn end(&mut self, force_commit: bool) -> Result<()> {
        assert!(self.in_txn);
        self.conn.wal_insert_end(force_commit)?;
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
                .end(false)
                .inspect_err(|e| tracing::error!("failed to close WAL session: {}", e));
        }
    }
}
