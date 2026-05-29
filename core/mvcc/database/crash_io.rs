//! `CrashIO` — a deterministic, crash-fidelity in-memory IO backend for tests.
//!
//! Unlike `MemoryIO` (where every write is instantly durable) this backend
//! models *fsync-gated* durability: a mutation only becomes durable when the
//! file is `sync`ed. `crash(loss_probability, rng)` then simulates a power-loss
//! event by replaying the un-synced mutations onto the last durable image, each
//! surviving independently with probability `1 - loss_probability`. A **write**
//! resolves per page (and its size-extension survives together with its data);
//! a **truncate** resolves **atomically** — a real `ftruncate` is crash-atomic,
//! so the model never fabricates "old size, but page 0 freed".
//!
//! Why probabilistic (not "always lose un-fsynced")? Real hardware sometimes
//! retains un-fsynced data. Modeling that surfaces a distinct bug class:
//! recovery that double-applies data which happens to exist in *two* durable
//! places (e.g. a checkpoint's WAL frame survived AND the logical log frame it
//! was derived from survived) → duplicated rows. A strict "always lose"
//! model (`loss_probability = 1.0`) only catches data-loss bugs; `< 1.0`
//! catches duplication bugs too.
//!
//! Determinism: pass a seeded RNG to `crash()`; a failing `(seed, loss_p,
//! point)` triple reproduces exactly.

use crate::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::{Buffer, Clock, Completion, File, FileId, FileSyncType, OpenFlags, IO};
use crate::sync::Mutex;
use crate::Result;
use rand::Rng;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Disarmed sentinel for the IO-fault trigger.
const FAULT_DISARMED: i64 = -1;

const PAGE_SIZE: usize = 4096;
type Page = Box<[u8; PAGE_SIZE]>;

fn zero_page() -> Page {
    Box::new([0u8; PAGE_SIZE])
}

/// One un-synced mutation, retained in issue order so a crash can resolve
/// durability faithfully:
///
/// - **Writes** resolve per page (page-cache granularity) — a multi-page write
///   can be torn across pages, and the size-extension a write produced is
///   carried *with* its data so size can never outlive the bytes it exposed
///   (no "size grew but the data didn't persist").
/// - **Truncate** resolves **atomically**: its size change and page removals
///   are one journaled metadata op. A real `ftruncate` is crash-atomic — after
///   a crash the file is either fully truncated or not at all; it can never be
///   left at the old size with page 0 freed. Modeling it per-page (as the old
///   `dirty`-set code did) fabricated that impossible state.
enum CrashOp {
    Write {
        page_no: usize,
        content: Page,
        /// File size after the `pwrite` that produced this page.
        new_size: u64,
    },
    Truncate {
        len: u64,
    },
}

#[derive(Default)]
struct CrashFileInner {
    /// Current visible contents (what reads observe).
    live: BTreeMap<usize, Page>,
    /// Contents as of the last successful `sync` — the floor a crash reverts to.
    durable: BTreeMap<usize, Page>,
    /// Un-synced mutations since the last `sync`, in issue order. A crash
    /// replays these onto `durable`, each surviving independently.
    ops: Vec<CrashOp>,
    live_size: u64,
    durable_size: u64,
}

pub struct CrashFile {
    inner: Mutex<CrashFileInner>,
    /// When `>= 0`, the 1-based index of the mutating IO (since arming) that
    /// should fail; `FAULT_DISARMED` otherwise. Shared with `CrashIO`.
    fault_at: Arc<AtomicI64>,
    /// Counts mutating IOs (pwrite/sync/truncate) **only while a fault is
    /// armed**; reset on each arm. Reads never count. Shared with `CrashIO`.
    armed_io_counter: Arc<AtomicI64>,
}

impl CrashFile {
    /// Called at the start of every mutating IO. When a fault is armed and this
    /// is the armed IO, returns an error so the in-flight operation aborts
    /// exactly here (the test then applies `crash()` and reopens — modeling a
    /// process crash at this IO). When disarmed — the common case, including all
    /// non-fault tests — this is a single relaxed load and early return, so
    /// there is no unconditional shared-atomic increment on the IO path.
    fn maybe_fault(&self) -> Result<()> {
        let target = self.fault_at.load(Ordering::Relaxed);
        if target < 0 {
            return Ok(());
        }
        let n = self.armed_io_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if n == target {
            return Err(crate::error::CompletionError::IOError(
                std::io::ErrorKind::Other,
                "crash_io injected IO fault",
            )
            .into());
        }
        Ok(())
    }

    /// Apply a simulated crash by replaying the un-synced ops onto the last
    /// durable image, in issue order. Each op survives independently with
    /// probability `1 - loss_probability`:
    ///
    /// - A **write** op surviving applies its page *and* the size it produced
    ///   together (size never outlives its data); losing it reverts that page.
    /// - A **truncate** op surviving applies its size change and page removals
    ///   atomically; losing it is a complete no-op (the truncate never
    ///   happened). This is what makes the model faithful to a real crash-atomic
    ///   `ftruncate`: there is no path to "old size, page 0 freed".
    ///
    /// Everything resolves to a new durable floor (the post-crash on-disk
    /// image). Returns how many un-fsynced write-pages were lost vs retained, so
    /// callers can prove a crash actually bit (a test that never drops anything
    /// is vacuous).
    fn apply_crash(&self, loss_probability: f64, rng: &mut impl Rng) -> CrashStats {
        let mut inner = self.inner.lock();
        let ops = std::mem::take(&mut inner.ops);
        let mut pages = inner.durable.clone();
        let mut size = inner.durable_size;
        let mut stats = CrashStats::default();
        for op in &ops {
            let survived = !rng.random_bool(loss_probability);
            match op {
                CrashOp::Write {
                    page_no,
                    content,
                    new_size,
                } => {
                    if survived {
                        stats.pages_retained += 1;
                        pages.insert(*page_no, content.clone());
                        size = size.max(*new_size);
                    } else {
                        stats.pages_lost += 1;
                    }
                }
                CrashOp::Truncate { len } => {
                    if survived {
                        size = *len;
                        let drop: Vec<usize> = pages
                            .keys()
                            .copied()
                            .filter(|&k| k * PAGE_SIZE >= *len as usize)
                            .collect();
                        for k in drop {
                            pages.remove(&k);
                        }
                    }
                }
            }
        }
        inner.live = pages.clone();
        inner.durable = pages;
        inner.live_size = size;
        inner.durable_size = size;
        stats
    }
}

/// How many un-fsynced pages a `crash()` lost vs retained, summed across files.
#[derive(Default, Debug, Clone, Copy)]
pub struct CrashStats {
    pub pages_lost: usize,
    pub pages_retained: usize,
}

impl CrashStats {
    fn add(&mut self, other: CrashStats) {
        self.pages_lost += other.pages_lost;
        self.pages_retained += other.pages_retained;
    }
}

impl File for CrashFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let buf_len = r.buf().len() as u64;
        if buf_len == 0 {
            c.complete(0);
            return Ok(c);
        }
        let inner = self.inner.lock();
        let file_size = inner.live_size;
        if pos >= file_size {
            c.complete(0);
            return Ok(c);
        }
        let read_len = buf_len.min(file_size - pos);
        {
            let read_buf = r.buf();
            let mut offset = pos as usize;
            let mut remaining = read_len as usize;
            let mut buf_offset = 0;
            while remaining > 0 {
                let page_no = offset / PAGE_SIZE;
                let page_offset = offset % PAGE_SIZE;
                let bytes = remaining.min(PAGE_SIZE - page_offset);
                match inner.live.get(&page_no) {
                    Some(page) => read_buf.as_mut_slice()[buf_offset..buf_offset + bytes]
                        .copy_from_slice(&page[page_offset..page_offset + bytes]),
                    None => read_buf.as_mut_slice()[buf_offset..buf_offset + bytes].fill(0),
                }
                offset += bytes;
                buf_offset += bytes;
                remaining -= bytes;
            }
        }
        c.complete(read_len as i32);
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.maybe_fault()?;
        let buf_len = buffer.len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(c);
        }
        let mut inner = self.inner.lock();
        let new_size = core::cmp::max(pos + buf_len as u64, inner.live_size);
        let mut offset = pos as usize;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        let data = buffer.as_slice();
        let mut touched: Vec<usize> = Vec::new();
        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes = remaining.min(PAGE_SIZE - page_offset);
            {
                let page = inner.live.entry(page_no).or_insert_with(zero_page);
                page[page_offset..page_offset + bytes]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes]);
            }
            touched.push(page_no);
            offset += bytes;
            buf_offset += bytes;
            remaining -= bytes;
        }
        inner.live_size = new_size;
        // Record one write op per touched page (per-page tearing on crash),
        // each carrying the resulting file size so a page's bytes and the size
        // they exposed survive or are lost together.
        for page_no in touched {
            let content = inner.live.get(&page_no).expect("page just written").clone();
            inner.ops.push(CrashOp::Write {
                page_no,
                content,
                new_size,
            });
        }
        c.complete(buf_len as i32);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        self.maybe_fault()?;
        let mut inner = self.inner.lock();
        inner.durable = inner.live.clone();
        inner.durable_size = inner.live_size;
        inner.ops.clear();
        c.complete(0);
        Ok(c)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.maybe_fault()?;
        let mut inner = self.inner.lock();
        if len < inner.live_size {
            let removed: Vec<usize> = inner
                .live
                .keys()
                .copied()
                .filter(|&k| k * PAGE_SIZE >= len as usize)
                .collect();
            for k in removed {
                inner.live.remove(&k);
            }
        }
        inner.live_size = len;
        // A truncate resolves atomically on crash (one op), unlike the old
        // dirty-page model that let the size and the page removals disagree.
        inner.ops.push(CrashOp::Truncate { len });
        c.complete(0);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self.inner.lock().live_size)
    }
}

pub struct CrashIO {
    files: Arc<Mutex<HashMap<String, Arc<CrashFile>>>>,
    /// The 1-based mutating-IO index at which to inject a fault, or
    /// `FAULT_DISARMED`. See `CrashFile::maybe_fault`.
    fault_at: Arc<AtomicI64>,
    /// Counts mutating IOs only while a fault is armed (reset on each arm).
    armed_io_counter: Arc<AtomicI64>,
}

impl CrashIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::default())),
            fault_at: Arc::new(AtomicI64::new(FAULT_DISARMED)),
            armed_io_counter: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Simulate a power-loss crash across every open file. Each page written
    /// since its last `sync` is independently lost with `loss_probability`.
    /// Returns the total pages lost vs retained across all files.
    pub fn crash(&self, loss_probability: f64, rng: &mut impl Rng) -> CrashStats {
        let files = self.files.lock();
        // HashMap iteration order is non-deterministic, which would make a seeded
        // crash unreproducible: the shared rng hands its per-op survival draws to
        // the files in whatever order `values()` yields, so the same (seed, loss,
        // fault) could resolve differently from one process to the next — fatal
        // for a fuzzer whose value rests on replaying a failure from its seed.
        // Take a canonical (sorted-by-path) order, then shuffle it with the SEEDED
        // rng (Fisher-Yates): the cross-file resolution order is randomized — so
        // different seeds explore different orderings — but fully reproducible.
        let mut paths: Vec<String> = files.keys().cloned().collect();
        paths.sort();
        for i in (1..paths.len()).rev() {
            let j = rng.random_range(0..=i);
            paths.swap(i, j);
        }
        let mut total = CrashStats::default();
        for p in &paths {
            if let Some(file) = files.get(p) {
                total.add(file.apply_crash(loss_probability, rng));
            }
        }
        total
    }

    /// Arm a fault on the `n`-th (1-based) mutating IO from now: that IO returns
    /// an error, aborting the in-flight operation exactly there. The caller then
    /// applies `crash()` and reopens to model a process crash at that IO.
    /// Resets the armed IO counter. Enumerate `n = 1, 2, …` until the operation
    /// no longer faults (i.e. `n` exceeded its mutating-IO count).
    pub fn arm_fault_at_io(&self, n: i64) {
        self.armed_io_counter.store(0, Ordering::Relaxed);
        self.fault_at.store(n, Ordering::Relaxed);
    }

    /// Disarm the IO fault (back to the zero-cost disarmed path).
    pub fn disarm_fault(&self) {
        self.fault_at.store(FAULT_DISARMED, Ordering::Relaxed);
    }

    /// The number of mutating IOs counted since the last `arm_fault_at_io`. Arm
    /// at a point past the operation (so the fault never fires), run it, then read
    /// this to learn the operation's mutating-IO count — used to pick a random
    /// *valid* crash point in `[1, count]` instead of crashing at every IO.
    pub fn armed_io_count(&self) -> i64 {
        self.armed_io_counter.load(Ordering::Relaxed)
    }
}

impl Default for CrashIO {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for CrashIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for CrashIO {
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
                Arc::new(CrashFile {
                    inner: Mutex::new(CrashFileInner::default()),
                    fault_at: self.fault_at.clone(),
                    armed_io_counter: self.armed_io_counter.clone(),
                }),
            );
        }
        let file = files.get(path).ok_or_else(|| {
            crate::LimboError::InternalError("file should exist after insert".to_string())
        })?;
        Ok(file.clone())
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.files.lock().remove(path);
        Ok(())
    }

    fn file_id(&self, _path: &str) -> Result<FileId> {
        // Returning Err makes `Database::open` skip the global DATABASE_MANAGER
        // entirely (it only consults the registry `if let Ok(file_id)`), so each
        // CrashIO-backed open is fresh and isolated: no cross-test key collision
        // with the shared registry, no `Opening`-sentinel races under parallel
        // tests, and a reopen after `crash()` always re-bootstraps (runs
        // recovery) instead of handing back a cached Database. This is exactly
        // what the crash fuzzer needs.
        Err(crate::LimboError::InternalError(
            "CrashIO is test-only and intentionally bypasses the database registry".to_string(),
        ))
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod crash_io_unit_tests {
    use super::*;
    use crate::io::Completion;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn write(file: &Arc<dyn File>, pos: u64, byte: u8) {
        let buf = Arc::new(Buffer::new_temporary(PAGE_SIZE));
        buf.as_mut_slice().fill(byte);
        let c = Completion::new_write(|_| {});
        let _c = file.pwrite(pos, buf, c).unwrap();
    }

    fn sync(file: &Arc<dyn File>) {
        let c = Completion::new_sync(|_| {});
        let _c = file.sync(c, FileSyncType::Fsync).unwrap();
    }

    fn read_first_byte(file: &Arc<dyn File>, pos: u64) -> u8 {
        let buf = Arc::new(Buffer::new_temporary(PAGE_SIZE));
        let read_buf = buf.clone();
        let c = Completion::new_read(buf, move |_| None);
        let _c = file.pread(pos, c).unwrap();
        read_buf.as_slice()[0]
    }

    #[test]
    fn synced_data_always_survives_crash() {
        let io = CrashIO::new();
        let f = io.open_file("/t", OpenFlags::Create, false).unwrap();
        write(&f, 0, 0xAA);
        sync(&f);
        // p=1.0 would drop *un-fsynced* data, but page 0 was synced.
        let mut rng = StdRng::seed_from_u64(1);
        io.crash(1.0, &mut rng);
        assert_eq!(read_first_byte(&f, 0), 0xAA);
    }

    #[test]
    fn unsynced_data_always_lost_at_p1() {
        let io = CrashIO::new();
        let f = io.open_file("/t", OpenFlags::Create, false).unwrap();
        write(&f, 0, 0xAA);
        sync(&f);
        write(&f, 0, 0xBB); // un-fsynced overwrite
        let mut rng = StdRng::seed_from_u64(2);
        io.crash(1.0, &mut rng);
        assert_eq!(
            read_first_byte(&f, 0),
            0xAA,
            "un-fsynced overwrite must be lost at p=1.0"
        );
    }

    #[test]
    fn unsynced_data_always_kept_at_p0() {
        let io = CrashIO::new();
        let f = io.open_file("/t", OpenFlags::Create, false).unwrap();
        write(&f, 0, 0xAA);
        sync(&f);
        write(&f, 0, 0xBB);
        let mut rng = StdRng::seed_from_u64(3);
        io.crash(0.0, &mut rng);
        assert_eq!(
            read_first_byte(&f, 0),
            0xBB,
            "un-fsynced overwrite must survive at p=0.0"
        );
    }

    #[test]
    fn probabilistic_loss_is_mixed() {
        // Across many independent un-fsynced pages at p=0.5, some survive and
        // some are lost — proving the coin is actually per-page.
        let io = CrashIO::new();
        let f = io.open_file("/t", OpenFlags::Create, false).unwrap();
        let n_pages = 200u64;
        for i in 0..n_pages {
            write(&f, i * PAGE_SIZE as u64, 0x11);
        }
        sync(&f);
        for i in 0..n_pages {
            write(&f, i * PAGE_SIZE as u64, 0x22); // un-fsynced overwrites
        }
        let mut rng = StdRng::seed_from_u64(4);
        io.crash(0.5, &mut rng);
        let mut survived = 0u64;
        let mut lost = 0u64;
        for i in 0..n_pages {
            match read_first_byte(&f, i * PAGE_SIZE as u64) {
                0x22 => survived += 1,
                0x11 => lost += 1,
                other => panic!("unexpected byte {other:#x}"),
            }
        }
        assert!(
            survived > 0 && lost > 0,
            "expected a mix: survived={survived} lost={lost}"
        );
        assert_eq!(survived + lost, n_pages);
    }

    fn try_write(file: &Arc<dyn File>, pos: u64, byte: u8) -> Result<()> {
        let buf = Arc::new(Buffer::new_temporary(PAGE_SIZE));
        buf.as_mut_slice().fill(byte);
        let c = Completion::new_write(|_| {});
        file.pwrite(pos, buf, c).map(|_| ())
    }

    fn try_sync(file: &Arc<dyn File>) -> Result<()> {
        let c = Completion::new_sync(|_| {});
        file.sync(c, FileSyncType::Fsync).map(|_| ())
    }

    #[test]
    fn io_fault_fires_on_armed_nth_mutating_io_and_reads_dont_count() {
        let io = CrashIO::new();
        let f = io.open_file("/t", OpenFlags::Create, false).unwrap();

        // Disarmed: every IO succeeds.
        try_write(&f, 0, 0x01).unwrap();
        try_sync(&f).unwrap();

        // Arm the 3rd mutating IO. Reads in between must NOT advance the count.
        io.arm_fault_at_io(3);
        assert!(try_write(&f, 0, 0x02).is_ok(), "IO 1 should succeed");
        let _ = read_first_byte(&f, 0); // read: must not count
        assert!(try_sync(&f).is_ok(), "IO 2 should succeed");
        let _ = read_first_byte(&f, 0); // read: must not count
        assert!(
            try_write(&f, 0, 0x03).is_err(),
            "IO 3 (the armed one) must fail"
        );

        // After the fault fired, subsequent IOs still fail only if they hit the
        // target again — but the count is past it, so they succeed.
        assert!(
            try_sync(&f).is_ok(),
            "IO 4 should succeed (past the armed point)"
        );

        // Disarm restores the zero-cost path.
        io.disarm_fault();
        try_write(&f, 0, 0x04).unwrap();
        try_sync(&f).unwrap();

        // Re-arming resets the counter: the 1st mutating IO now faults.
        io.arm_fault_at_io(1);
        assert!(try_write(&f, 0, 0x05).is_err(), "re-armed IO 1 must fail");
    }
}
