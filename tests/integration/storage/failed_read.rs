use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use turso_core::{
    io::{FileId, FileSyncType},
    Buffer, Clock, Completion, ConnectOpts, Connection, Database, File, LimboError, MemoryIO,
    MonotonicInstant, OpenFlags, SqliteDialect, WallClockInstant, IO,
};

/// What the next database file read does.
#[derive(Clone, Copy)]
enum ReadAction {
    /// Return an error without finishing the completion.
    Fail,
    /// Start the read but finish it only when the test calls
    /// `finish_held_reads`, never from `step()`. Like IO that is finished
    /// outside the connection's IO loop.
    Hold,
    /// Start the read on the inner IO, which finishes it as usual.
    #[cfg_attr(not(all(target_os = "linux", feature = "io_uring")), allow(dead_code))]
    Pass,
}

#[derive(Default)]
struct ReadScript {
    next_actions: VecDeque<ReadAction>,
    held: Vec<(Arc<dyn File>, u64, Completion)>,
    passed: Vec<Completion>,
    /// For each `Fail`, how many `Pass` reads were still unfinished.
    passed_in_flight_at_failure: Vec<usize>,
}

/// IO whose database file reads follow a script. Reads not in the script
/// go straight to the inner IO.
struct ScriptedReadIo {
    inner: Arc<dyn IO>,
    script: Arc<Mutex<ReadScript>>,
}

const MAX_DRAIN_STEPS: usize = 10_000;

impl ScriptedReadIo {
    fn new(inner: Arc<dyn IO>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            script: Arc::new(Mutex::new(ReadScript::default())),
        })
    }

    #[cfg_attr(not(all(target_os = "linux", feature = "io_uring")), allow(dead_code))]
    fn passed_in_flight_at_failure(&self) -> Vec<usize> {
        self.script
            .lock()
            .unwrap()
            .passed_in_flight_at_failure
            .clone()
    }

    fn script_next_reads(&self, actions: &[ReadAction]) {
        self.script
            .lock()
            .unwrap()
            .next_actions
            .extend(actions.iter().copied());
    }

    fn held_read_count(&self) -> usize {
        self.script.lock().unwrap().held.len()
    }

    fn finish_held_reads(&self) -> turso_core::Result<()> {
        let held = std::mem::take(&mut self.script.lock().unwrap().held);
        for (file, pos, c) in held {
            drop(file.pread(pos, c)?);
        }
        Ok(())
    }
}

impl Clock for ScriptedReadIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for ScriptedReadIo {
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
        Ok(Arc::new(ScriptedReadFile {
            inner,
            script: self.script.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }

    // The default loops until every completion finishes. A completion that
    // `step()` never finishes would make the test hang, so give up after a
    // bound.
    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        for _ in 0..MAX_DRAIN_STEPS {
            if completions.iter().all(|c| c.finished()) {
                return Ok(());
            }
            self.step()?;
        }
        panic!("drain_completions waited for a completion that step() never finishes");
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

struct ScriptedReadFile {
    inner: Arc<dyn File>,
    script: Arc<Mutex<ReadScript>>,
}

impl File for ScriptedReadFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        let mut script = self.script.lock().unwrap();
        match script.next_actions.pop_front() {
            Some(ReadAction::Fail) => {
                let in_flight = script.passed.iter().filter(|c| !c.finished()).count();
                script.passed_in_flight_at_failure.push(in_flight);
                Err(LimboError::InternalError("injected read error".to_string()))
            }
            Some(ReadAction::Hold) => {
                script.held.push((self.inner.clone(), pos, c.clone()));
                Ok(c)
            }
            Some(ReadAction::Pass) => {
                script.passed.push(c.clone());
                drop(script);
                self.inner.pread(pos, c)
            }
            None => {
                drop(script);
                self.inner.pread(pos, c)
            }
        }
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

/// Opens a database with 200 rows of 1000 bytes, ids 10, 20, ..., 2000,
/// written to the database file, and returns a new connection whose page
/// cache is empty.
fn open_filled_database(io: Arc<ScriptedReadIo>, name: &str) -> anyhow::Result<Arc<Connection>> {
    let db = Database::open_file(io, name, Arc::new(SqliteDialect))?;
    let conn = db.connect(ConnectOpts::new())?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")?;
    conn.execute("INSERT INTO t SELECT value * 10, zeroblob(1000) FROM generate_series(1, 200)")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    conn.close()?;
    Ok(db.connect(ConnectOpts::new())?)
}

/// A page split reads the sibling pages of the full page. If one of those
/// reads fails before it starts, the split must return the error instead of
/// waiting for the read's completion, which nothing will ever finish.
#[test]
fn failed_sibling_read_during_page_split_returns_error() -> anyhow::Result<()> {
    let io = ScriptedReadIo::new(Arc::new(MemoryIO::new()));
    let conn = open_filled_database(io.clone(), "failed-sibling-read.db")?;
    // Load the leaves around id 1005, so that only the split's sibling reads
    // go to the file.
    conn.execute("SELECT v FROM t WHERE id IN (1000, 1010)")?;
    io.script_next_reads(&[ReadAction::Fail]);

    let err = conn
        .execute("INSERT INTO t VALUES (1005, zeroblob(1000))")
        .expect_err("the INSERT must fail with the read error");
    assert!(
        err.to_string().contains("injected read error"),
        "unexpected error: {err}"
    );
    Ok(())
}

/// When one sibling read fails while another is still in flight, the split
/// must return the error without waiting for the in-flight read. That read
/// may be finished by code that cannot run while the split waits, so
/// waiting could never end.
#[test]
fn failed_sibling_read_does_not_wait_for_other_sibling_reads() -> anyhow::Result<()> {
    let io = ScriptedReadIo::new(Arc::new(MemoryIO::new()));
    let conn = open_filled_database(io.clone(), "failed-sibling-read-in-flight.db")?;
    // Load only the leaf that the INSERT of id 1005 lands on, so that the
    // split reads two siblings from the file: the right one first, then the
    // left one.
    conn.execute("SELECT v FROM t WHERE id = 1010")?;
    io.script_next_reads(&[ReadAction::Hold, ReadAction::Fail]);

    let err = conn
        .execute("INSERT INTO t VALUES (1005, zeroblob(1000))")
        .expect_err("the INSERT must fail with the read error");
    assert!(
        err.to_string().contains("injected read error"),
        "unexpected error: {err}"
    );
    assert_eq!(
        io.held_read_count(),
        1,
        "the right sibling read must be in flight"
    );

    io.finish_held_reads()?;
    conn.execute("INSERT INTO t VALUES (1005, zeroblob(1000))")?;
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut result = Vec::new();
    stmt.run_with_row_callback(|row| {
        result.push(row.get::<String>(0)?);
        Ok(())
    })?;
    assert_eq!(result, vec!["ok".to_string()]);
    Ok(())
}

/// The same split with io_uring: the right sibling's read is really in
/// flight in the kernel when the left sibling's read fails. The split must
/// return the error without waiting, and the late read must not break the
/// database.
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[test]
fn failed_sibling_read_with_io_uring_read_in_flight() -> anyhow::Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("failed-sibling-read-io-uring.db");
    let io = ScriptedReadIo::new(Arc::new(turso_core::UringIO::new()?));
    let conn = open_filled_database(io.clone(), path.to_str().unwrap())?;
    conn.execute("SELECT v FROM t WHERE id = 1010")?;
    io.script_next_reads(&[ReadAction::Pass, ReadAction::Fail]);

    let err = conn
        .execute("INSERT INTO t VALUES (1005, zeroblob(1000))")
        .expect_err("the INSERT must fail with the read error");
    assert!(
        err.to_string().contains("injected read error"),
        "unexpected error: {err}"
    );
    assert_eq!(
        io.passed_in_flight_at_failure(),
        vec![1],
        "the right sibling read must be in flight when the left one fails"
    );

    conn.execute("INSERT INTO t VALUES (1005, zeroblob(1000))")?;
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut result = Vec::new();
    stmt.run_with_row_callback(|row| {
        result.push(row.get::<String>(0)?);
        Ok(())
    })?;
    assert_eq!(result, vec!["ok".to_string()]);
    Ok(())
}
