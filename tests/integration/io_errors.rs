//! An I/O error raised by a backend must reach the caller as the error the
//! backend reported, not as one the commit path invented.
//!
//! The commit path used to replace a failed write batch's completion error
//! with `IOError(Other, "write")`, keeping neither the failing operation's
//! error nor which completion failed. That is what made #10909 impossible to
//! diagnose from an application: an aborted operation, a transient Windows
//! resource error, and a genuinely failing disk all looked the same.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use turso_core::{
    io::{FileId, FileSyncType},
    Buffer, Clock, Completion, CompletionError, ConnectOpts, Database, DatabaseOpts, File,
    MemoryIO, MonotonicInstant, OpenFlags, SqliteDialect, WallClockInstant, IO,
};

/// Fails writes on request with an error the test chooses, so a test can
/// tell whether the commit reported that error or one of its own.
struct FailingWritesIo {
    inner: MemoryIO,
    error: CompletionError,
    fail_next_write: Arc<AtomicBool>,
}

impl FailingWritesIo {
    fn new(error: CompletionError) -> Self {
        Self {
            inner: MemoryIO::new(),
            error,
            fail_next_write: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Fail the next write that reaches the file layer.
    fn fail_next_write(&self) {
        self.fail_next_write.store(true, Ordering::SeqCst);
    }
}

impl Clock for FailingWritesIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for FailingWritesIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        Ok(Arc::new(FailingWritesFile {
            inner: self.inner.open_file(path, flags, direct)?,
            error: self.error,
            fail_next_write: self.fail_next_write.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.inner.file_id(path)
    }
}

struct FailingWritesFile {
    inner: Arc<dyn File>,
    error: CompletionError,
    fail_next_write: Arc<AtomicBool>,
}

impl File for FailingWritesFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }

    /// The commit submits its WAL frames through `pwritev`, so failing here
    /// fails the exact completion the commit waits on.
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            c.error(self.error);
            return Ok(c);
        }
        self.inner.pwritev(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

/// A commit whose WAL write fails must report the write's own error. The
/// commit used to answer `IOError(Other, "write")` instead, so an application
/// could not tell a retryable failure from a real disk error.
#[test]
fn a_failed_commit_write_reports_the_backend_error() -> anyhow::Result<()> {
    const BACKEND_ERROR: &str = "injected write failure";

    let io = Arc::new(FailingWritesIo::new(CompletionError::IOError(
        std::io::ErrorKind::Other,
        BACKEND_ERROR,
    )));
    let db = Database::open_file_with_flags(
        io.clone(),
        "io-errors-commit-write.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new())?;

    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t (x)")?;
    conn.execute("INSERT INTO t VALUES (1)")?;
    conn.execute("BEGIN")?;
    conn.execute("UPDATE t SET x = 2")?;

    io.fail_next_write();
    let err = conn
        .execute("COMMIT")
        .expect_err("the commit must fail when its WAL write fails");

    let message = err.to_string();
    assert!(
        message.contains(BACKEND_ERROR),
        "commit reported {message:?} instead of the backend's {BACKEND_ERROR:?}"
    );
    Ok(())
}
