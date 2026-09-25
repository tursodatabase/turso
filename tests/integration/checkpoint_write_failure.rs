use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, CompletionError, Database, DatabaseOpts, File,
    MonotonicInstant, OpenFlags, SqliteDialect, WallClockInstant, IO,
};

const DB_PATH: &str = "checkpoint-write-failure-issue-8163.db";

struct CappedIo {
    inner: Arc<dyn IO>,
    db_write_limit: Arc<AtomicU64>,
}

impl Clock for CappedIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CappedIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        if path != DB_PATH {
            return Ok(inner);
        }
        Ok(Arc::new(CappedFile {
            inner,
            write_limit: self.db_write_limit.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.cancel(completions)
    }
    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.drain_completions(completions)
    }
    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }
    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct CappedFile {
    inner: Arc<dyn File>,
    write_limit: Arc<AtomicU64>,
}

impl File for CappedFile {
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
        if pos + buffer.len() as u64 > self.write_limit.load(Ordering::SeqCst) {
            c.error(CompletionError::IOError(
                std::io::ErrorKind::StorageFull,
                "pwrite",
            ));
            return Ok(c);
        }
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, sync_type)
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
}

fn run_failing_checkpoint() -> Result<(), String> {
    let db_write_limit = Arc::new(AtomicU64::new(u64::MAX));
    let io = Arc::new(CappedIo {
        inner: Arc::new(turso_core::MemoryIO::new()),
        db_write_limit: db_write_limit.clone(),
    });
    let db = Database::open_file_with_flags(
        io,
        DB_PATH,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .map_err(|e| e.to_string())?;
    let conn = db.connect().map_err(|e| e.to_string())?;
    conn.execute("PRAGMA wal_autocheckpoint=0")
        .map_err(|e| e.to_string())?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .map_err(|e| e.to_string())?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(|e| e.to_string())?;
    db_write_limit.store(2 * 4096, Ordering::SeqCst);
    let payload = "x".repeat(1000);
    conn.execute("BEGIN").map_err(|e| e.to_string())?;
    for i in 0..200 {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{payload}')"))
            .map_err(|e| e.to_string())?;
    }
    conn.execute("COMMIT").map_err(|e| e.to_string())?;

    let checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    if checkpoint.is_ok() {
        return Err("checkpoint reported success although database writes failed".into());
    }

    let mut stmt = conn
        .prepare("SELECT COUNT(*) FROM t")
        .map_err(|e| format!("connection unusable after failed checkpoint: {e}"))?;
    let mut count = String::new();
    stmt.run_with_row_callback(|row| {
        count = format!("{}", row.get_value(0));
        Ok(())
    })
    .map_err(|e| format!("connection unusable after failed checkpoint: {e}"))?;
    if count != "200" {
        return Err(format!(
            "expected 200 rows after failed checkpoint, got {count}"
        ));
    }
    Ok(())
}

#[test]
fn checkpoint_with_persistent_write_failure_returns_error_and_keeps_connection_usable() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(run_failing_checkpoint)
            .unwrap_or_else(|_| Err("panicked during failed checkpoint".into()));
        let _ = tx.send(result);
    });
    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{e}"),
        Err(_) => panic!("checkpoint hung after a persistent database write failure"),
    }
}
