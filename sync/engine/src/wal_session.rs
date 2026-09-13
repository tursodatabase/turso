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
        let result = self.conn.wal_insert_end(force_commit);
        // Do not use `?` before clearing this flag: an error here can still
        // mean the WAL transaction was ended, so Drop must not retry cleanup.
        self.in_txn = false;
        result?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use turso_core::{
        Clock, Completion, Database, DatabaseOpts, File, MemoryIO, OpenFlags, Result as CoreResult,
        SqliteDialect, IO,
    };

    #[derive(Clone)]
    struct FailWalPwritevIo {
        inner: Arc<MemoryIO>,
        fail_wal_pwritev: Arc<AtomicBool>,
    }

    impl FailWalPwritevIo {
        fn new() -> Self {
            Self {
                inner: Arc::new(MemoryIO::new()),
                fail_wal_pwritev: Arc::new(AtomicBool::new(false)),
            }
        }

        fn fail_wal_pwritev(&self) {
            self.fail_wal_pwritev.store(true, Ordering::SeqCst);
        }
    }

    impl Clock for FailWalPwritevIo {
        fn current_time_monotonic(&self) -> turso_core::io::clock::MonotonicInstant {
            self.inner.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> turso_core::io::clock::WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }

    impl IO for FailWalPwritevIo {
        fn open_file(
            &self,
            path: &str,
            flags: OpenFlags,
            direct: bool,
        ) -> CoreResult<Arc<dyn File>> {
            let inner = self.inner.open_file(path, flags, direct)?;
            Ok(Arc::new(FailWalPwritevFile {
                inner,
                path: path.to_string(),
                fail_wal_pwritev: self.fail_wal_pwritev.clone(),
            }))
        }

        fn remove_file(&self, path: &str) -> CoreResult<()> {
            self.inner.remove_file(path)
        }

        fn file_id(&self, path: &str) -> CoreResult<turso_core::io::FileId> {
            self.inner.file_id(path)
        }
    }

    struct FailWalPwritevFile {
        inner: Arc<dyn File>,
        path: String,
        fail_wal_pwritev: Arc<AtomicBool>,
    }

    impl File for FailWalPwritevFile {
        fn lock_file(&self, exclusive: bool) -> CoreResult<()> {
            self.inner.lock_file(exclusive)
        }

        fn unlock_file(&self) -> CoreResult<()> {
            self.inner.unlock_file()
        }

        fn pread(&self, pos: u64, c: Completion) -> CoreResult<Completion> {
            self.inner.pread(pos, c)
        }

        fn pwrite(
            &self,
            pos: u64,
            buffer: Arc<turso_core::Buffer>,
            c: Completion,
        ) -> CoreResult<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }

        fn sync(
            &self,
            c: Completion,
            sync_type: turso_core::io::FileSyncType,
        ) -> CoreResult<Completion> {
            self.inner.sync(c, sync_type)
        }

        fn pwritev(
            &self,
            pos: u64,
            buffers: Vec<Arc<turso_core::Buffer>>,
            c: Completion,
        ) -> CoreResult<Completion> {
            if self.path.ends_with("-wal") && self.fail_wal_pwritev.load(Ordering::SeqCst) {
                return Err(turso_core::CompletionError::IOError(
                    std::io::ErrorKind::StorageFull,
                    "pwritev",
                )
                .into());
            }
            self.inner.pwritev(pos, buffers, c)
        }

        fn size(&self) -> CoreResult<u64> {
            self.inner.size()
        }

        fn truncate(&self, len: u64, c: Completion) -> CoreResult<Completion> {
            self.inner.truncate(len, c)
        }

        fn has_hole(&self, pos: usize, len: usize) -> CoreResult<bool> {
            self.inner.has_hole(pos, len)
        }

        fn punch_hole(&self, pos: usize, len: usize) -> CoreResult<()> {
            self.inner.punch_hole(pos, len)
        }
    }

    #[test]
    fn failed_commit_error_does_not_make_drop_double_end_wal_session() {
        let io = FailWalPwritevIo::new();
        let db = Database::open_file_with_flags(
            Arc::new(io.clone()),
            "wal-session-commit-error.db",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
            .unwrap();

        let mut session = WalSession::new(conn.clone());
        session.begin().unwrap();
        conn.execute("INSERT INTO t VALUES (1, randomblob(8192))")
            .unwrap();

        io.fail_wal_pwritev();
        let err = session.end(true).unwrap_err();
        assert!(
            err.to_string().contains("pwritev"),
            "expected injected pwritev error, got {err:?}"
        );

        drop(session);
    }
}
