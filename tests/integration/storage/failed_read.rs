use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use turso_core::{
    io::{FileId, FileSyncType},
    Buffer, Clock, Completion, ConnectOpts, Database, File, LimboError, MemoryIO, MonotonicInstant,
    OpenFlags, SqliteDialect, WallClockInstant, IO,
};

/// IO whose database file reads return an error without finishing the
/// completion they were given, once `fail_reads` is set.
struct FailingReadIo {
    inner: Arc<dyn IO>,
    fail_reads: Arc<AtomicBool>,
}

const MAX_DRAIN_STEPS: usize = 10_000;

impl Clock for FailingReadIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for FailingReadIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if !path.ends_with(".db") {
            return Ok(inner);
        }
        Ok(Arc::new(FailingReadFile {
            inner,
            fail_reads: self.fail_reads.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }

    // The default loops until every completion finishes. A completion that
    // nobody finishes would make the test hang, so give up after a bound.
    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        for _ in 0..MAX_DRAIN_STEPS {
            if completions.iter().all(|c| c.finished()) {
                return Ok(());
            }
            self.step()?;
        }
        panic!("drain_completions waited for a completion that never finishes");
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.inner.file_id(path)
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct FailingReadFile {
    inner: Arc<dyn File>,
    fail_reads: Arc<AtomicBool>,
}

impl File for FailingReadFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        if self.fail_reads.load(Ordering::Acquire) {
            return Err(LimboError::InternalError("injected read error".to_string()));
        }
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

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, sync_type)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwritev(pos, buffers, c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

/// A page split reads the sibling pages of the full page. If one of those
/// reads fails before it starts, the split must return the error instead of
/// waiting for the read's completion, which nothing will ever finish.
#[test]
fn failed_sibling_read_during_page_split_returns_error() -> anyhow::Result<()> {
    let fail_reads = Arc::new(AtomicBool::new(false));
    let io: Arc<dyn IO> = Arc::new(FailingReadIo {
        inner: Arc::new(MemoryIO::new()),
        fail_reads: fail_reads.clone(),
    });
    let db = Database::open_file(io, "failed-sibling-read.db", Arc::new(SqliteDialect))?;

    {
        let conn = db.connect(ConnectOpts::new())?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")?;
        conn.execute(
            "INSERT INTO t SELECT value * 10, zeroblob(1000) FROM generate_series(1, 200)",
        )?;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        conn.close()?;
    }

    let conn = db.connect(ConnectOpts::new())?;
    // Load the leaves around id 1005, so that only the split's sibling reads
    // go to the file.
    conn.execute("SELECT v FROM t WHERE id IN (1000, 1010)")?;
    fail_reads.store(true, Ordering::Release);
    let result = conn.execute("INSERT INTO t VALUES (1005, zeroblob(1000))");
    fail_reads.store(false, Ordering::Release);

    let err = result.expect_err("the INSERT must fail with the read error");
    assert!(
        err.to_string().contains("injected read error"),
        "unexpected error: {err}"
    );
    Ok(())
}
