use super::memory::MemStore;
use super::{Buffer, Clock, Completion, File, OpenFlags, IO};
use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::FileSyncType;
use crate::sync::Mutex;
use crate::Result;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tracing::debug;

/// A memory-backed [`IO`] backend that defers every completion until the next
/// [`IO::step`] call, forcing the engine through its cooperative-yield path on
/// *every* `pread` / `pwrite` / `pwritev` / `sync` / `truncate`.
///
/// This backend performs the identical byte-level data movement as
/// `MemoryIO` (it shares [`MemStore`]) but enqueues the completion instead of
/// signalling it. The completion only becomes `finished()` when `step()` runs,
/// so the engine must return `StepResult::IO`, yield, and re-enter — exercising
/// the resume path behind each yield point.
pub struct MemoryYieldIO {
    files: Arc<Mutex<HashMap<String, Arc<MemoryYieldFile>>>>,
    /// Completions submitted but not yet signalled, drained FIFO by `step()`.
    pending: Arc<Mutex<VecDeque<Deferred>>>,
}

/// A completion awaiting `step()`, together with the byte count it should
/// report. All completion kinds (read/write/sync/truncate) report a single
/// `i32`, so this is all the state we need to defer.
struct Deferred {
    completion: Completion,
    result: i32,
}

impl MemoryYieldIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        debug!("Using IO backend 'memory_yield'");
        Self {
            files: Arc::new(Mutex::new(HashMap::default())),
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Default for MemoryYieldIO {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MemoryYieldIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for MemoryYieldIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock();
        if !files.contains_key(path) && !flags.contains(OpenFlags::Create) {
            return Err(crate::error::CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                "open",
            )
            .into());
        }
        if !files.contains_key(path) {
            files.insert(
                path.to_string(),
                Arc::new(MemoryYieldFile {
                    path: path.to_string(),
                    store: MemStore::new(),
                    pending: self.pending.clone(),
                }),
            );
        }
        Ok(files
            .get(path)
            .ok_or_else(|| {
                crate::LimboError::InternalError("file should exist after insert".to_string())
            })?
            .clone())
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let mut files = self.files.lock();
        files.remove(path);
        Ok(())
    }

    fn file_id(&self, path: &str) -> Result<super::FileId> {
        Ok(super::FileId::from_path_hash(path))
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    /// Signal every completion that was deferred since the last `step()`.
    ///
    /// We snapshot the queue and release the lock before signalling so that a
    /// completion callback is free to submit follow-up I/O (which lands in the
    /// queue for the *next* step) without deadlocking or being drained within
    /// this same call. Each deferred completion required at least one `step()`
    /// to finish, which means at least one yield occurred per submitted op.
    fn step(&self) -> Result<()> {
        let drained: VecDeque<Deferred> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending)
        };
        for Deferred { completion, result } in drained {
            completion.complete(result);
        }
        Ok(())
    }
}

pub struct MemoryYieldFile {
    path: String,
    store: MemStore,
    pending: Arc<Mutex<VecDeque<Deferred>>>,
}

crate::assert::assert_sync!(MemoryYieldFile);

impl MemoryYieldFile {
    /// Defer `c` so it is signalled on the next `IO::step`, leaving it
    /// `finished() == false` for now and forcing the caller to yield.
    fn defer(&self, c: &Completion, result: i32) {
        self.pending.lock().push_back(Deferred {
            completion: c.clone(),
            result,
        });
    }
}

impl File for MemoryYieldFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("pread(path={}): pos={} [deferred]", self.path, pos);
        let n = self.store.read_into(pos, c.as_read().buf());
        self.defer(&c, n);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwrite(path={}): pos={}, size={} [deferred]",
            self.path,
            pos,
            buffer.len()
        );
        let n = self.store.write_at(pos, buffer.as_slice());
        self.defer(&c, n as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        tracing::debug!("sync(path={}) [deferred]", self.path);
        self.defer(&c, 0);
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        tracing::debug!("truncate(path={}): len={} [deferred]", self.path, len);
        self.store.truncate(len);
        self.defer(&c, 0);
        Ok(c)
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        tracing::debug!(
            "pwritev(path={}): pos={}, buffers={:?} [deferred]",
            self.path,
            pos,
            buffers.iter().map(|x| x.len()).collect::<Vec<_>>()
        );
        let n = self.store.writev(pos, &buffers);
        self.defer(&c, n);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        tracing::debug!("size(path={}): {}", self.path, self.store.size());
        Ok(self.store.size())
    }

    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        Ok(self.store.has_hole(pos, len))
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.store.punch_hole(pos, len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::StepResult;
    use crate::{Database, IOResult, OpenFlags};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct StepGuardedIO {
        inner: MemoryYieldIO,
        step_allowed: AtomicBool,
    }

    impl StepGuardedIO {
        fn new() -> Self {
            Self {
                inner: MemoryYieldIO::new(),
                step_allowed: AtomicBool::new(true),
            }
        }

        fn set_step_allowed(&self, allowed: bool) {
            self.step_allowed.store(allowed, Ordering::SeqCst);
        }
    }

    impl Clock for StepGuardedIO {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            self.inner.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }

    impl IO for StepGuardedIO {
        fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
            self.inner.open_file(path, flags, direct)
        }

        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }

        fn file_id(&self, path: &str) -> Result<super::super::FileId> {
            self.inner.file_id(path)
        }

        fn supports_shared_wal_coordination(&self) -> bool {
            self.inner.supports_shared_wal_coordination()
        }

        fn step(&self) -> Result<()> {
            assert!(
                self.step_allowed.load(Ordering::SeqCst),
                "IO::step must only be called by the test driver"
            );
            self.inner.step()
        }
    }

    fn drive_guarded_io<T>(
        io: &StepGuardedIO,
        mut action: impl FnMut() -> Result<IOResult<T>>,
    ) -> (T, usize) {
        let mut io_yields = 0usize;
        loop {
            io.set_step_allowed(false);
            let step = action();
            io.set_step_allowed(true);

            match step.unwrap() {
                IOResult::Done(result) => return (result, io_yields),
                IOResult::IO(io_result) => {
                    io_yields += 1;
                    io_result.wait(io).unwrap();
                }
            }
        }
    }

    /// Every op must come back unfinished and only complete once `step()` runs.
    #[test]
    fn completions_are_deferred_until_step() {
        let io = MemoryYieldIO::new();
        let file = io.open_file("t", OpenFlags::Create, false).unwrap();

        let buf = Arc::new(Buffer::new(vec![0xAB; 64]));
        let wc = file.pwrite(0, buf, Completion::new_write(|_| {})).unwrap();
        assert!(
            !wc.finished(),
            "pwrite completion must not finish before step()"
        );
        io.step().unwrap();
        assert!(wc.finished() && wc.succeeded());

        let sc = file
            .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .unwrap();
        assert!(
            !sc.finished(),
            "sync completion must not finish before step()"
        );
        io.step().unwrap();
        assert!(sc.succeeded());

        let tc = file.truncate(16, Completion::new_trunc(|_| {})).unwrap();
        assert!(
            !tc.finished(),
            "truncate completion must not finish before step()"
        );
        io.step().unwrap();
        assert!(tc.succeeded());
        assert_eq!(file.size().unwrap(), 16);
    }

    /// Data written before a `step()` is visible to a later read, exactly as
    /// with the synchronous `MemoryIO` — only the signalling is deferred.
    #[test]
    fn read_returns_written_bytes_after_step() {
        let io = MemoryYieldIO::new();
        let file = io.open_file("t", OpenFlags::Create, false).unwrap();

        let wbuf = Arc::new(Buffer::new(vec![0x42; 100]));
        let wc = file.pwrite(0, wbuf, Completion::new_write(|_| {})).unwrap();
        io.step().unwrap();
        assert!(wc.succeeded());

        let rbuf = Arc::new(Buffer::new_temporary(100));
        let rc = file
            .pread(0, Completion::new_read(rbuf.clone(), |_| None))
            .unwrap();
        assert!(
            !rc.finished(),
            "pread completion must not finish before step()"
        );
        io.step().unwrap();
        assert!(rc.succeeded());
        assert!(rbuf.as_slice().iter().all(|&b| b == 0x42));
    }

    /// Driving real SQL through the engine on this backend must force at least
    /// one `StepResult::IO` (i.e. a genuine yield + re-entry), which the
    /// synchronous `MemoryIO` fast-paths away, while still producing correct
    /// results.
    #[test]
    fn engine_yields_and_round_trips() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryYieldIO::new());
        let db = Database::open_file(io.clone(), "memory_yield_test.db").unwrap();
        let conn = db.connect().unwrap();

        // Step statements manually so we can observe the cooperative-yield path.
        // Returns (rows, number of StepResult::IO yields).
        let run = |sql: &str| -> (Vec<crate::Value>, usize) {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut io_yields = 0usize;
            let mut rows = Vec::new();
            loop {
                match stmt.step().unwrap() {
                    StepResult::IO => {
                        io_yields += 1;
                        io.step().unwrap();
                    }
                    StepResult::Row => {
                        let v = stmt.row().unwrap().get_values().next().unwrap().clone();
                        rows.push(v);
                    }
                    StepResult::Done => break,
                    other => panic!("unexpected step result: {other:?}"),
                }
            }
            (rows, io_yields)
        };

        // A write transaction must flush pages, so it is guaranteed to defer at
        // least one completion and surface a StepResult::IO. The synchronous
        // MemoryIO would fast-path right past this.
        let (_, create_yields) = run("CREATE TABLE t(x)");
        assert!(
            create_yields > 0,
            "memory_yield backend must force at least one StepResult::IO on a write"
        );
        run("INSERT INTO t VALUES (1), (2), (3)");

        // And the round trip still produces correct results.
        let (rows, _) = run("SELECT x FROM t ORDER BY x");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], crate::Value::from_i64(1));
        assert_eq!(rows[2], crate::Value::from_i64(3));
    }

    #[test]
    fn journal_mode_mvcc_bootstrap_yields_without_internal_step() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(StepGuardedIO::new());
        let db = Database::open_file(io.clone(), "journal_mode_mvcc_yield.db").unwrap();
        let conn = db.connect().unwrap();
        let mut stmt = conn.prepare("PRAGMA journal_mode = 'mvcc'").unwrap();

        let mut rows = Vec::new();
        let mut io_yields = 0usize;
        loop {
            io.set_step_allowed(false);
            let step = stmt.step();
            io.set_step_allowed(true);

            match step.unwrap() {
                StepResult::IO => {
                    io_yields += 1;
                    io.step().unwrap();
                }
                StepResult::Row => {
                    let value = stmt.row().unwrap().get_values().next().unwrap().clone();
                    rows.push(value);
                }
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }

        assert!(
            io_yields > 0,
            "MemoryYieldIO should force PRAGMA journal_mode=mvcc through cooperative yields"
        );
        assert_eq!(rows, vec![crate::Value::build_text("mvcc")]);
        assert!(conn.mvcc_enabled());
    }

    #[test]
    fn fresh_mvcc_attach_yields_without_internal_step() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(StepGuardedIO::new());
        let db = Database::open_file_with_flags(
            io.clone(),
            "attach_main_yield.db",
            OpenFlags::Create,
            crate::DatabaseOpts::new().with_attach(true),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();

        let run = |sql: &str| {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut io_yields = 0usize;
            loop {
                io.set_step_allowed(false);
                let step = stmt.step();
                io.set_step_allowed(true);

                match step.unwrap() {
                    StepResult::IO => {
                        io_yields += 1;
                        io.step().unwrap();
                    }
                    StepResult::Row => {}
                    StepResult::Done => break,
                    other => panic!("unexpected step result: {other:?}"),
                }
            }
            io_yields
        };

        run("PRAGMA journal_mode = 'mvcc'");
        let attach_yields = run("ATTACH 'attach_aux_yield.db' AS aux");

        assert!(
            attach_yields > 0,
            "MemoryYieldIO should force ATTACH through cooperative yields"
        );
        assert!(conn.get_database_id_by_name("aux").is_ok());
        assert!(conn
            .mv_store_for_db(conn.get_database_id_by_name("aux").unwrap())
            .is_some());
    }

    #[test]
    fn pager_allocate_and_free_yield_for_header_reads_without_internal_step() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(StepGuardedIO::new());
        let db = Database::open_file(io.clone(), "pager_header_yield.db").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();

        let pager = conn.get_pager();
        conn.execute("BEGIN IMMEDIATE").unwrap();
        pager.clear_page_cache(false);
        let (_page, allocate_yields) = drive_guarded_io(io.as_ref(), || pager.allocate_page());
        assert!(
            allocate_yields > 0,
            "allocate_page should yield when reading page 1 from storage"
        );
        conn.execute("ROLLBACK").unwrap();

        conn.execute("BEGIN IMMEDIATE").unwrap();
        pager.clear_page_cache(false);
        let ((), free_yields) = drive_guarded_io(io.as_ref(), || pager.free_page(None, 2));
        assert!(
            free_yields > 0,
            "free_page should yield when reading page 1 from storage"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn overflow_delete_yields_for_header_validation_without_internal_step() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(StepGuardedIO::new());
        let db = Database::open_file(io.clone(), "overflow_delete_yield.db").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x BLOB)").unwrap();
        conn.execute("INSERT INTO t VALUES (zeroblob(20000))")
            .unwrap();

        conn.get_pager().clear_page_cache(false);
        let mut stmt = conn.prepare("DELETE FROM t").unwrap();
        let mut io_yields = 0usize;
        loop {
            io.set_step_allowed(false);
            let step = stmt.step();
            io.set_step_allowed(true);

            match step.unwrap() {
                StepResult::IO => {
                    io_yields += 1;
                    io.step().unwrap();
                }
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }

        assert!(
            io_yields > 0,
            "overflow DELETE should yield while clearing overflow pages"
        );
    }
}
