//! Regression test for issue #7952: checkpoint backfill must be crash-atomic
//! under `PRAGMA synchronous=NORMAL`.
//!
//! Under NORMAL, commits do not fsync the WAL. If a checkpoint then backfills
//! those committed-but-not-durable frames into the database file without first
//! forcing the WAL to stable storage, a power loss mid-backfill can persist
//! *some* of the backfilled DB pages while WAL recovery drops the unsynced
//! frames — recovering a torn database that matches no committed prefix, and
//! that `PRAGMA integrity_check` reports as "ok".
//!
//! The crash model here is a test-side IO substrate: every write is volatile
//! until the file it targets is fsynced; at the simulated power-loss point the
//! durable image plus an arbitrary (here: alternating) subset of the unsynced
//! DB-file writes survives, and all unsynced WAL writes are lost. This is a
//! legal power-loss outcome (the kernel may write back dirty DB pages while
//! the WAL tail never reaches the platter).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, Database, DatabaseOpts, File, MonotonicInstant,
    OpenFlags, SqliteDialect, WallClockInstant, IO,
};

/// A single not-yet-durable file mutation.
enum UnsyncedOp {
    Write { pos: usize, data: Vec<u8> },
    Truncate { len: usize },
}

impl UnsyncedOp {
    fn apply(&self, image: &mut Vec<u8>) {
        match self {
            UnsyncedOp::Write { pos, data } => {
                let end = pos + data.len();
                if image.len() < end {
                    image.resize(end, 0);
                }
                image[*pos..end].copy_from_slice(data);
            }
            UnsyncedOp::Truncate { len } => {
                image.resize(*len, 0);
            }
        }
    }
}

#[derive(Default)]
struct FileShadow {
    /// Bytes guaranteed to be on stable storage (covered by an fsync).
    durable: Vec<u8>,
    /// Writes/truncates issued since the last fsync, in submission order.
    unsynced: Vec<UnsyncedOp>,
}

impl FileShadow {
    fn promote_all(&mut self) {
        for op in self.unsynced.drain(..) {
            let durable = &mut self.durable;
            op.apply(durable);
        }
    }
}

/// The captured post-crash disk state.
struct CrashSnapshot {
    files: HashMap<String, Vec<u8>>,
    db_writes_persisted: usize,
    db_writes_dropped: usize,
}

struct CrashSimState {
    files: Mutex<HashMap<String, Arc<Mutex<FileShadow>>>>,
    /// When set, the next fsync of this file (with pending writes) is the
    /// simulated power-loss point.
    armed_db_path: Mutex<Option<String>>,
    snapshot: Mutex<Option<CrashSnapshot>>,
}

impl CrashSimState {
    /// Builds the post-crash disk image: durable bytes everywhere, plus an
    /// alternating subset of the unsynced writes to the crash-target file
    /// (torn writeback), and none of the unsynced writes to any other file
    /// (the WAL tail is lost).
    fn build_crash_snapshot(&self, db_path: &str) -> CrashSnapshot {
        let files = self.files.lock().unwrap();
        let mut images = HashMap::new();
        let mut persisted = 0usize;
        let mut dropped = 0usize;
        for (path, shadow) in files.iter() {
            let shadow = shadow.lock().unwrap();
            let mut image = shadow.durable.clone();
            if path == db_path {
                let mut write_idx = 0usize;
                for op in &shadow.unsynced {
                    if matches!(op, UnsyncedOp::Write { .. }) {
                        if write_idx % 2 == 0 {
                            op.apply(&mut image);
                            persisted += 1;
                        } else {
                            dropped += 1;
                        }
                        write_idx += 1;
                    }
                }
            }
            images.insert(path.clone(), image);
        }
        CrashSnapshot {
            files: images,
            db_writes_persisted: persisted,
            db_writes_dropped: dropped,
        }
    }
}

struct CrashSimIo {
    inner: Arc<dyn IO>,
    state: Arc<CrashSimState>,
}

impl CrashSimIo {
    fn new() -> Self {
        Self {
            inner: Arc::new(turso_core::MemoryIO::new()),
            state: Arc::new(CrashSimState {
                files: Mutex::new(HashMap::new()),
                armed_db_path: Mutex::new(None),
                snapshot: Mutex::new(None),
            }),
        }
    }

    /// Settle: pretend everything written so far reached stable storage
    /// (baseline state before the scenario under test).
    fn mark_all_durable(&self) {
        for shadow in self.state.files.lock().unwrap().values() {
            shadow.lock().unwrap().promote_all();
        }
    }

    /// Arm the power-loss trigger: the next fsync of `db_path` that has
    /// pending (unsynced) writes captures the crash snapshot.
    fn arm_crash_on_db_sync(&self, db_path: &str) {
        *self.state.armed_db_path.lock().unwrap() = Some(db_path.to_string());
    }

    fn take_crash_snapshot(&self) -> Option<CrashSnapshot> {
        self.state.snapshot.lock().unwrap().take()
    }
}

impl Clock for CrashSimIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CrashSimIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        let shadow = self
            .state
            .files
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .clone();
        Ok(Arc::new(CrashSimFile {
            path: path.to_string(),
            inner,
            shadow,
            state: self.state.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.state.files.lock().unwrap().remove(path);
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

struct CrashSimFile {
    path: String,
    inner: Arc<dyn File>,
    shadow: Arc<Mutex<FileShadow>>,
    state: Arc<CrashSimState>,
}

impl File for CrashSimFile {
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
        self.shadow
            .lock()
            .unwrap()
            .unsynced
            .push(UnsyncedOp::Write {
                pos: pos as usize,
                data: buffer.as_slice().to_vec(),
            });
        self.inner.pwrite(pos, buffer, c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        {
            let mut shadow = self.shadow.lock().unwrap();
            let mut off = pos as usize;
            for buffer in &buffers {
                shadow.unsynced.push(UnsyncedOp::Write {
                    pos: off,
                    data: buffer.as_slice().to_vec(),
                });
                off += buffer.len();
            }
        }
        self.inner.pwritev(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        // Simulated power loss: crash while the armed file's fsync is in
        // flight, i.e. after its writes were submitted but before any of them
        // were made durable.
        let crash_here = {
            let armed = self.state.armed_db_path.lock().unwrap();
            armed.as_deref() == Some(self.path.as_str())
                && !self.shadow.lock().unwrap().unsynced.is_empty()
        };
        if crash_here {
            let mut snapshot = self.state.snapshot.lock().unwrap();
            if snapshot.is_none() {
                *snapshot = Some(self.state.build_crash_snapshot(&self.path));
            }
        }
        self.shadow.lock().unwrap().promote_all();
        self.inner.sync(c, sync_type)
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.shadow
            .lock()
            .unwrap()
            .unsynced
            .push(UnsyncedOp::Truncate { len: len as usize });
        self.inner.truncate(len, c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
}

fn query_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values: Vec<String> = row.get_values().map(|value| format!("{value}")).collect();
        rows.push(values.join("|"));
        Ok(())
    })?;
    Ok(rows)
}

/// Runs the crash scenario under the given `PRAGMA synchronous` mode and
/// asserts the committed-prefix recovery contract.
fn run_checkpoint_crash_scenario(sync_pragma: &str) -> anyhow::Result<()> {
    // Unique per scenario: databases opened on the same path share state
    // process-wide, so parallel test runs must not collide.
    let db_path_owned = format!(
        "checkpoint-crash-atomicity-{}.db",
        sync_pragma.to_lowercase()
    );
    let db_path_sim: &str = &db_path_owned;
    const N_ROWS: usize = 256;
    let old = "A".repeat(120);
    let new = "B".repeat(120);

    let io = Arc::new(CrashSimIo::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        db_path_sim,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;

    // S0: a multi-page table, fully checkpointed and durable.
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("BEGIN")?;
    for i in 0..N_ROWS {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{old}')"))?;
    }
    conn.execute("COMMIT")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    io.mark_all_durable();

    // T: a committed transaction touching many pages.
    conn.execute(format!("PRAGMA synchronous={sync_pragma}"))?;
    conn.execute(format!("UPDATE t SET v = '{new}'"))?;

    // Checkpoint T's frames into the DB file; power loss strikes while the
    // backfilled pages are being forced to disk.
    io.arm_crash_on_db_sync(db_path_sim);
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")?;

    let snapshot = io
        .take_crash_snapshot()
        .expect("checkpoint never issued the post-backfill DB fsync; crash point not reached");
    assert!(
        snapshot.db_writes_persisted >= 1 && snapshot.db_writes_dropped >= 1,
        "crash model must persist a strict subset of the backfill writes \
         (persisted={}, dropped={}); grow the workload if the backfill was a single write",
        snapshot.db_writes_persisted,
        snapshot.db_writes_dropped,
    );

    // Materialize the post-crash disk state and recover with a fresh database.
    let dir = tempfile::TempDir::new()?;
    let db_path = dir.path().join("recovered.db");
    let wal_path = dir.path().join("recovered.db-wal");
    std::fs::write(
        &db_path,
        snapshot
            .files
            .get(db_path_sim)
            .expect("db file missing from crash snapshot"),
    )?;
    if let Some(wal) = snapshot.files.get(&format!("{db_path_sim}-wal")) {
        std::fs::write(&wal_path, wal)?;
    }

    let recovered_db = Database::open_file(
        Arc::new(turso_core::PlatformIO::new()?),
        db_path.to_str().unwrap(),
        Arc::new(SqliteDialect),
    )?;
    let recovered = recovered_db.connect()?;

    let integrity = query_rows(&recovered, "PRAGMA integrity_check")?;
    assert_eq!(
        integrity,
        vec!["ok".to_string()],
        "recovered database failed integrity_check"
    );
    let count = query_rows(&recovered, "SELECT COUNT(*) FROM t")?;
    assert_eq!(
        count,
        vec![N_ROWS.to_string()],
        "row count changed across crash recovery"
    );
    let distinct = query_rows(&recovered, "SELECT DISTINCT v FROM t ORDER BY v")?;
    let labels: Vec<String> = distinct
        .iter()
        .map(|v| format!("{}x{}", &v[..1], v.len()))
        .collect();
    assert!(
        distinct == vec![old.clone()] || distinct == vec![new.clone()],
        "torn recovery: database is neither S0 (all 'A') nor the committed \
         transaction T (all 'B'); distinct values: {labels:?}",
    );
    Ok(())
}

/// A crash during checkpoint backfill under `synchronous=NORMAL` (the WAL-mode
/// default durability) must recover to a committed prefix: the database equals
/// either the pre-transaction state S0 (all rows 'A...') or the committed
/// transaction T (all rows 'B...'), never a torn mix — and `integrity_check`
/// must be clean.
#[test]
fn checkpoint_backfill_crash_under_synchronous_normal_recovers_committed_prefix(
) -> anyhow::Result<()> {
    run_checkpoint_crash_scenario("NORMAL")
}

/// Control: the same crash point under `synchronous=FULL`, where the
/// commit-time WAL fsync already made T's frames durable, must heal — this
/// validates that the crash model itself is not the source of the tear.
#[test]
fn checkpoint_backfill_crash_under_synchronous_full_control() -> anyhow::Result<()> {
    run_checkpoint_crash_scenario("FULL")
}
