//! Checkpoint with database-file writes that keep failing.
//!
//! [`CapacityIo`] stores files in memory and fails every write to the
//! database file that ends past a byte limit, either with an I/O error or by
//! completing with 0 bytes written. Its files implement only `pwrite`, so the
//! checkpoint's vectored writes go through the default `File::pwritev`, which
//! splits them into one `pwrite` per page.
//!
//! A checkpoint whose page writes fail must not finish, must not report
//! success, and must not count the frames of those pages as backfilled.
//! Otherwise a reader skips the WAL for pages that never reached the database
//! file.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, CompletionError, ConnectOpts, Connection,
    Database, DatabaseOpts, File, MonotonicInstant, OpenFlags, SessionRole, WallClockInstant, IO,
};
use turso_sqlite::SqliteDialect;

const PAGE_SIZE: u64 = 4096;

struct CapacityIo {
    inner: Arc<dyn IO>,
    db_path: String,
    /// Writes to the database file that end past this offset fail.
    limit: Arc<AtomicU64>,
    /// Whether a failed write completes with 0 bytes written instead of an error.
    short_write: bool,
}

impl Clock for CapacityIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CapacityIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        let limit = (path == self.db_path).then(|| self.limit.clone());
        Ok(Arc::new(CapacityFile {
            inner,
            limit,
            short_write: self.short_write,
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
        self.inner.fill_bytes(dest)
    }
    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct CapacityFile {
    inner: Arc<dyn File>,
    /// Set for the database file only.
    limit: Option<Arc<AtomicU64>>,
    short_write: bool,
}

// `pwritev` is not implemented so that the default `File::pwritev` runs.
impl File for CapacityFile {
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
        let over_limit = self
            .limit
            .as_ref()
            .is_some_and(|limit| pos + buffer.len() as u64 > limit.load(Ordering::SeqCst));
        if over_limit {
            if self.short_write {
                c.complete(0);
            } else {
                c.error(CompletionError::IOError(
                    std::io::ErrorKind::StorageFull,
                    "capacity",
                ));
            }
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

/// How the test fills the WAL before the checkpoint whose writes fail.
#[derive(Clone, Copy, Debug)]
enum Workload {
    /// One transaction, then `PRAGMA wal_checkpoint`.
    OneTransaction,
    /// Autocommit inserts that pass the automatic checkpoint threshold, so the
    /// automatic checkpoint fails first, then `PRAGMA wal_checkpoint` retries.
    AutoCheckpoint,
}

fn count_rows(conn: &Arc<Connection>) -> anyhow::Result<i64> {
    let mut stmt = conn.prepare("SELECT count(*), sum(length(v)) FROM t")?;
    let mut n = None;
    stmt.run_with_row_callback(|row| {
        n = Some(row.get::<i64>(0)?);
        Ok(())
    })?;
    Ok(n.expect("count(*) returns a row"))
}

/// Runs `PRAGMA wal_checkpoint(<mode>)` and returns its busy column, or the
/// error of the statement.
fn wal_checkpoint(conn: &Arc<Connection>, mode: &str) -> anyhow::Result<i64> {
    let mut stmt = conn.prepare(format!("PRAGMA wal_checkpoint({mode})"))?;
    let mut busy = None;
    stmt.run_with_row_callback(|row| {
        busy = Some(row.get::<i64>(0)?);
        Ok(())
    })?;
    Ok(busy.expect("PRAGMA wal_checkpoint returns a row"))
}

fn run_scenario(short_write: bool, workload: Workload) -> anyhow::Result<()> {
    // Databases opened on the same path share state in the process.
    let db_path = format!("checkpoint-write-failure-{short_write}-{workload:?}.db");
    let limit = Arc::new(AtomicU64::new(u64::MAX));
    let io = Arc::new(CapacityIo {
        inner: Arc::new(turso_core::MemoryIO::new()),
        db_path: db_path.clone(),
        limit: limit.clone(),
        short_write,
    });
    let db = Database::open_file_with_flags(
        io,
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    let value = "x".repeat(200);
    let rows = match workload {
        Workload::OneTransaction => {
            limit.store(2 * PAGE_SIZE, Ordering::SeqCst);
            conn.execute("BEGIN")?;
            for i in 0..200 {
                conn.execute(format!("INSERT INTO t VALUES({i}, '{value}')"))?;
            }
            conn.execute("COMMIT")?;
            200
        }
        Workload::AutoCheckpoint => {
            limit.store(40 * PAGE_SIZE, Ordering::SeqCst);
            let mut insert_errors = 0;
            for i in 0..3000 {
                // The row is committed to the WAL before the automatic
                // checkpoint runs, so an error here comes from the checkpoint.
                if conn
                    .execute(format!("INSERT INTO t VALUES({i}, '{value}')"))
                    .is_err()
                {
                    insert_errors += 1;
                }
            }
            if !short_write {
                assert!(
                    insert_errors > 0,
                    "an automatic checkpoint must have hit the write limit"
                );
            }
            3000
        }
    };

    let result = wal_checkpoint(&conn, "PASSIVE");
    assert!(
        !matches!(result, Ok(0)),
        "checkpoint reported success although page writes failed"
    );
    // A new connection has an empty page cache, so it reads every page from
    // the WAL or from the database file, as the backfill count says.
    assert_eq!(count_rows(&conn)?, rows);
    let reader = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    assert_eq!(count_rows(&reader)?, rows);

    // Once writes work again, the connection commits and checkpoints.
    limit.store(u64::MAX, Ordering::SeqCst);
    conn.execute(format!("INSERT INTO t VALUES(100000, '{value}')"))?;
    assert_eq!(wal_checkpoint(&conn, "TRUNCATE")?, 0);
    let reader = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    assert_eq!(count_rows(&reader)?, rows + 1);
    let mut stmt = reader.prepare("PRAGMA integrity_check")?;
    let mut integrity = Vec::new();
    stmt.run_with_row_callback(|row| {
        integrity.push(row.get::<String>(0)?);
        Ok(())
    })?;
    assert_eq!(integrity, vec!["ok".to_string()]);
    Ok(())
}

/// Runs the scenario on its own thread so that a checkpoint that never
/// finishes fails the test instead of hanging it.
fn run_with_timeout(short_write: bool, workload: Workload) {
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = run_scenario(short_write, workload);
        let _ = tx.send(());
        result
    });
    // The channel also disconnects when the scenario panics; join() then
    // passes the panic on.
    if let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(60)) {
        panic!("checkpoint did not finish within 60 seconds");
    }
    handle.join().unwrap().unwrap();
}

#[test]
fn checkpoint_write_error_fails_checkpoint() {
    run_with_timeout(false, Workload::OneTransaction);
}

#[test]
fn checkpoint_short_write_fails_checkpoint() {
    run_with_timeout(true, Workload::OneTransaction);
}

#[test]
fn auto_checkpoint_write_error_does_not_backfill_unwritten_frames() {
    run_with_timeout(false, Workload::AutoCheckpoint);
}

#[test]
fn auto_checkpoint_short_write_does_not_backfill_unwritten_frames() {
    run_with_timeout(true, Workload::AutoCheckpoint);
}
