//! Readahead: does fetching pages before a scan asks for them actually cut the
//! number of trips to storage, and does it leave every answer unchanged?
//!
//! The tests here count reads at the file layer, because that is what
//! readahead is for. A scan that produces the right rows after ten thousand
//! round trips has not been helped by anything.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, Connection, Database, DatabaseOpts, File,
    MonotonicInstant, OpenFlags, PlatformIO, SqliteDialect, WallClockInstant, IO,
};

use crate::common::{limbo_exec_rows, TempDatabase};

/// Wraps a real IO and counts what the database asks the file layer to do.
struct CountingIo {
    inner: Arc<dyn IO>,
    counts: Arc<Counts>,
}

#[derive(Default)]
struct Counts {
    /// Read requests reaching the file. This is the number that matters:
    /// each one is a syscall, and on remote storage a round trip.
    reads: AtomicU64,
    /// Bytes those reads asked for. Readahead trades this up to trade reads
    /// down, so a test that only watched reads could be fooled.
    bytes_read: AtomicU64,
}

impl CountingIo {
    fn new(inner: Arc<dyn IO>) -> Self {
        Self {
            inner,
            counts: Arc::new(Counts::default()),
        }
    }

    fn counts(&self) -> Arc<Counts> {
        self.counts.clone()
    }
}

impl Counts {
    fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
    }
}

impl Clock for CountingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CountingIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(CountingFile {
            inner: file,
            counts: self.counts.clone(),
            // Only the main database file is interesting; WAL traffic would
            // add noise unrelated to readahead.
            count: path.ends_with(".db"),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
}

struct CountingFile {
    inner: Arc<dyn File>,
    counts: Arc<Counts>,
    count: bool,
}

impl File for CountingFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        if self.count {
            self.counts.reads.fetch_add(1, Ordering::Relaxed);
            self.counts
                .bytes_read
                .fetch_add(c.as_read().buf().len() as u64, Ordering::Relaxed);
        }
        self.inner.pread(pos, c)
    }

    /// Readahead reads a run of pages as one request. Counting it as one is
    /// the whole point of these tests, so this must not fall back to the
    /// trait default, which would split it back into one read per page.
    fn preadv(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        if self.count {
            self.counts.reads.fetch_add(1, Ordering::Relaxed);
            self.counts.bytes_read.fetch_add(
                buffers.iter().map(|b| b.len() as u64).sum::<u64>(),
                Ordering::Relaxed,
            );
        }
        self.inner.preadv(pos, buffers, c)
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

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

/// A database with enough rows that a scan crosses many pages, on a counting
/// IO. Returns the connection and the counters.
fn scan_database(name: &str, rows: usize) -> (Arc<Connection>, Arc<Counts>) {
    let tmp = TempDatabase::new_empty();
    let path = tmp.path.parent().unwrap().join(name);
    let io = Arc::new(CountingIo::new(Arc::new(PlatformIO::new().unwrap())));
    let counts = io.counts();
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let filler = "x".repeat(200);
    for i in 0..rows {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{filler}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    // Start from cold memory so the scan really goes to the file.
    conn.execute("PRAGMA cache_size = -80").unwrap();
    (conn, counts)
}

fn scan_with_prefetch(conn: &Arc<Connection>, counts: &Counts, pages: u32) -> u64 {
    conn.execute(format!("PRAGMA prefetch_pages = {pages}"))
        .unwrap();
    // Drop everything cached so both settings start from the same place.
    conn.execute("PRAGMA cache_size = -20").unwrap();
    conn.execute("PRAGMA cache_size = -80").unwrap();
    counts.reset();
    let rows = limbo_exec_rows(conn, "SELECT count(*), sum(id) FROM t");
    assert_eq!(rows.len(), 1);
    counts.reads()
}

/// The headline claim: a full table scan makes far fewer trips to storage
/// with readahead on than off.
#[test]
fn full_scan_makes_far_fewer_reads() {
    let (conn, counts) = scan_database("readahead-scan.db", 20_000);

    let off = scan_with_prefetch(&conn, &counts, 0);
    let on = scan_with_prefetch(&conn, &counts, 32);

    assert!(
        off > 500,
        "the scan should be big enough to measure, but it only did {off} reads"
    );
    assert!(
        on * 4 < off,
        "readahead should cut reads by much more than 4x: {off} without, {on} with"
    );
}

/// Whatever readahead does, the rows must be exactly the same. Checked across
/// several window sizes and several shapes of query.
#[test]
fn results_are_identical_whatever_the_window() {
    let (conn, _counts) = scan_database("readahead-identical.db", 5_000);
    let queries = [
        "SELECT count(*), sum(id) FROM t",
        "SELECT id, v FROM t WHERE id % 997 = 3 ORDER BY id",
        "SELECT count(*) FROM t WHERE v LIKE 'x%'",
        "SELECT id FROM t ORDER BY id DESC LIMIT 50",
        "SELECT a.id FROM t a JOIN t b ON a.id = b.id + 1 WHERE a.id < 100 ORDER BY a.id",
        "SELECT min(id), max(id), count(DISTINCT id) FROM t",
    ];

    conn.execute("PRAGMA prefetch_pages = 0").unwrap();
    let expected: Vec<_> = queries.iter().map(|q| limbo_exec_rows(&conn, q)).collect();

    for pages in [1, 2, 7, 32, 64, 512] {
        conn.execute(format!("PRAGMA prefetch_pages = {pages}"))
            .unwrap();
        for (query, want) in queries.iter().zip(expected.iter()) {
            let got = limbo_exec_rows(&conn, query);
            assert_eq!(
                &got, want,
                "prefetch_pages={pages} changed the answer to {query}"
            );
        }
    }
}

/// Readahead must not make a reader that jumps around pay for pages it will
/// never look at.
#[test]
fn random_access_reads_no_more_than_with_readahead_off() {
    let (conn, counts) = scan_database("readahead-random.db", 20_000);
    // Point lookups scattered across the table, in an order with no run in it.
    let mut probes = String::new();
    let mut key = 7919usize;
    for _ in 0..200 {
        key = key.wrapping_mul(48271) % 19_997;
        probes.push_str(&format!("SELECT v FROM t WHERE id = {key};"));
    }

    let run = |pages: u32| {
        conn.execute(format!("PRAGMA prefetch_pages = {pages}"))
            .unwrap();
        conn.execute("PRAGMA cache_size = -20").unwrap();
        conn.execute("PRAGMA cache_size = -80").unwrap();
        counts.reset();
        for stmt in probes.split(';').filter(|s| !s.trim().is_empty()) {
            limbo_exec_rows(&conn, stmt);
        }
        (counts.reads(), counts.bytes_read())
    };

    let (off_reads, off_bytes) = run(0);
    let (on_reads, on_bytes) = run(32);

    assert!(
        on_reads <= off_reads + off_reads / 10,
        "scattered point lookups should not read more with readahead on: \
         {off_reads} reads off, {on_reads} on"
    );
    assert!(
        on_bytes <= off_bytes + off_bytes / 10,
        "scattered point lookups should not move more bytes with readahead on: \
         {off_bytes} bytes off, {on_bytes} on"
    );
}

/// Turning readahead off must really turn it off: no extra bytes at all.
#[test]
fn window_of_zero_reads_exactly_what_the_query_needs() {
    let (conn, counts) = scan_database("readahead-off.db", 5_000);

    conn.execute("PRAGMA prefetch_pages = 0").unwrap();
    conn.execute("PRAGMA cache_size = -20").unwrap();
    conn.execute("PRAGMA cache_size = -80").unwrap();
    counts.reset();
    limbo_exec_rows(&conn, "SELECT count(*) FROM t");
    let (off_reads, off_bytes) = (counts.reads(), counts.bytes_read());

    // With readahead off every read is exactly one page.
    assert_eq!(
        off_bytes % off_reads,
        0,
        "with readahead off each read should be one page: {off_reads} reads, {off_bytes} bytes"
    );

    let rows = limbo_exec_rows(&conn, "PRAGMA prefetch_stats");
    // hits, hits_in_flight, pages_fetched and reads must all be zero.
    let stats = &rows[0];
    for (i, name) in ["hits", "hits_in_flight"].iter().enumerate() {
        assert_eq!(
            stats[i],
            rusqlite::types::Value::Integer(0),
            "{name} should be zero when readahead is off"
        );
    }
    for (i, name) in ["pages_fetched", "reads"].iter().enumerate() {
        assert_eq!(
            stats[i + 3],
            rusqlite::types::Value::Integer(0),
            "{name} should be zero when readahead is off"
        );
    }
}

/// Readahead moves more bytes than the query strictly needs -- that is the
/// trade. It must stay a small trade, not an open-ended one.
#[test]
fn extra_bytes_stay_a_small_fraction() {
    let (conn, counts) = scan_database("readahead-amplification.db", 20_000);

    conn.execute("PRAGMA prefetch_pages = 0").unwrap();
    conn.execute("PRAGMA cache_size = -20").unwrap();
    conn.execute("PRAGMA cache_size = -80").unwrap();
    counts.reset();
    limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");
    let needed = counts.bytes_read();

    conn.execute("PRAGMA prefetch_pages = 32").unwrap();
    conn.execute("PRAGMA cache_size = -20").unwrap();
    conn.execute("PRAGMA cache_size = -80").unwrap();
    counts.reset();
    limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");
    let fetched = counts.bytes_read();

    assert!(
        fetched < needed * 3 / 2,
        "readahead should not move anywhere near 1.5x the bytes: \
         {needed} needed, {fetched} fetched"
    );
}

/// A scan that runs while rows are being changed must see the writes. This is
/// the case prefetched pages could get wrong: they are pre-write copies taken
/// from the file, and handing one to a reader after the page was modified
/// would serve stale data.
#[test]
fn writes_are_visible_to_a_later_scan() {
    let (conn, _counts) = scan_database("readahead-writes.db", 4_000);
    conn.execute("PRAGMA prefetch_pages = 32").unwrap();

    // Warm readahead up with a scan so there are prefetched pages in hand.
    limbo_exec_rows(&conn, "SELECT count(*) FROM t");

    conn.execute("UPDATE t SET v = 'changed' WHERE id % 3 = 0")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM t WHERE v = 'changed'");
    assert_eq!(
        rows[0][0],
        rusqlite::types::Value::Integer(1334),
        "a scan after an update must see the update"
    );

    // And again after a checkpoint moves those frames into the file.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM t WHERE v = 'changed'");
    assert_eq!(rows[0][0], rusqlite::types::Value::Integer(1334));
}

/// Interleaving a scan with writes to the same pages, repeatedly, is the
/// nastiest shape for a stale prefetched page to survive into. The row count
/// must track the writes exactly every time.
#[test]
fn interleaved_reads_and_writes_never_serve_stale_pages() {
    let (conn, _counts) = scan_database("readahead-interleaved.db", 3_000);
    conn.execute("PRAGMA prefetch_pages = 32").unwrap();

    for round in 1..=8i64 {
        let marker = format!("round{round}");
        conn.execute(format!(
            "UPDATE t SET v = '{marker}' WHERE id % 8 = {}",
            round % 8
        ))
        .unwrap();
        let rows = limbo_exec_rows(
            &conn,
            &format!("SELECT count(*) FROM t WHERE v = '{marker}'"),
        );
        let want = (0..3_000i64).filter(|i| i % 8 == round % 8).count() as i64;
        assert_eq!(
            rows[0][0],
            rusqlite::types::Value::Integer(want),
            "round {round}: scan did not see the rows it just wrote"
        );
        if round % 3 == 0 {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
    }
}

/// Two cursors walking two tables at once should both benefit; neither should
/// get confused by the other.
#[test]
fn two_scans_at_once_both_get_faster() {
    let tmp = TempDatabase::new_empty();
    let path = tmp.path.parent().unwrap().join("readahead-two-tables.db");
    let io = Arc::new(CountingIo::new(Arc::new(PlatformIO::new().unwrap())));
    let counts = io.counts();
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let filler = "y".repeat(200);
    for table in ["a", "b"] {
        conn.execute(format!(
            "CREATE TABLE {table}(id INTEGER PRIMARY KEY, v TEXT)"
        ))
        .unwrap();
        conn.execute("BEGIN").unwrap();
        for i in 0..8_000 {
            conn.execute(format!("INSERT INTO {table} VALUES({i}, '{filler}')"))
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let query = "SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b)";
    let run = |pages: u32| {
        conn.execute(format!("PRAGMA prefetch_pages = {pages}"))
            .unwrap();
        conn.execute("PRAGMA cache_size = -20").unwrap();
        conn.execute("PRAGMA cache_size = -80").unwrap();
        counts.reset();
        let rows = limbo_exec_rows(&conn, query);
        (counts.reads(), rows)
    };

    let (off, off_rows) = run(0);
    let (on, on_rows) = run(32);
    assert_eq!(off_rows, on_rows, "readahead changed the answer");
    assert!(
        on * 3 < off,
        "two interleaved scans should still get much cheaper: {off} reads off, {on} on"
    );
}

/// A tiny page cache must not be flooded with guessed pages. Readahead bounds
/// its window by what the cache can absorb, so a scan under memory pressure
/// still finishes and still returns the right rows.
#[test]
fn a_tiny_cache_still_scans_correctly() {
    let (conn, counts) = scan_database("readahead-tiny-cache.db", 5_000);
    conn.execute("PRAGMA prefetch_pages = 512").unwrap();
    // Ten pages of cache, against a 512-page window request.
    conn.execute("PRAGMA cache_size = -40").unwrap();
    counts.reset();
    let rows = limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");
    assert_eq!(rows[0][0], rusqlite::types::Value::Integer(5_000));
    assert_eq!(
        rows[0][1],
        rusqlite::types::Value::Integer((0..5_000i64).sum::<i64>())
    );
}

/// The setting is per-pager and readable back.
#[test]
fn prefetch_pages_round_trips() {
    let tmp = TempDatabase::new_empty();
    let conn = tmp.connect_limbo();
    let rows = limbo_exec_rows(&conn, "PRAGMA prefetch_pages");
    assert_eq!(
        rows[0][0],
        rusqlite::types::Value::Integer(32),
        "readahead is on out of the box"
    );

    for pages in [0u32, 1, 16, 512] {
        conn.execute(format!("PRAGMA prefetch_pages = {pages}"))
            .unwrap();
        let rows = limbo_exec_rows(&conn, "PRAGMA prefetch_pages");
        assert_eq!(rows[0][0], rusqlite::types::Value::Integer(pages as i64));
    }
}

/// Out-of-range settings are rejected rather than silently clamped, so a
/// deployment that asks for something impossible finds out.
#[test]
fn out_of_range_prefetch_pages_is_rejected() {
    let tmp = TempDatabase::new_empty();
    let conn = tmp.connect_limbo();
    for bad in ["-1", "100000"] {
        assert!(
            conn.execute(format!("PRAGMA prefetch_pages = {bad}"))
                .is_err(),
            "prefetch_pages = {bad} should have been rejected"
        );
    }
}

/// Readahead accounting should show what it claims: on a long scan, most
/// reads are served from pages fetched ahead of time, in far fewer requests
/// than pages.
#[test]
fn stats_show_pages_arriving_in_batches() {
    let (conn, _counts) = scan_database("readahead-stats.db", 20_000);
    conn.execute("PRAGMA prefetch_pages = 32").unwrap();
    conn.execute("PRAGMA cache_size = -20").unwrap();
    conn.execute("PRAGMA cache_size = -80").unwrap();
    limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");

    let rows = limbo_exec_rows(&conn, "PRAGMA prefetch_stats");
    let value = |i: usize| match &rows[0][i] {
        rusqlite::types::Value::Integer(n) => *n as u64,
        other => panic!("unexpected stat value {other:?}"),
    };
    let (hits, in_flight, misses, pages_fetched, reads) =
        (value(0), value(1), value(2), value(3), value(4));

    assert!(
        hits + in_flight > misses * 4,
        "most of a long scan should be served by readahead: \
         {hits} hits, {in_flight} in flight, {misses} misses"
    );
    assert!(reads > 0, "readahead should have made some requests");
    assert!(
        pages_fetched / reads >= 4,
        "readahead should move several pages per request, got {pages_fetched} pages in {reads} reads"
    );
}

/// Running the same scan over and over must not cost more each time.
///
/// Prefetched pages are held outside the page cache, so if they were not
/// bounded and released they would pile up and every run would read more than
/// the last. The exact byte count moves around a little as the page cache
/// settles, so the check is that it never grows past the first run.
#[test]
fn repeated_scans_do_not_cost_more_each_time() {
    let (conn, counts) = scan_database("readahead-repeat.db", 4_000);
    conn.execute("PRAGMA prefetch_pages = 32").unwrap();
    let expected = limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");

    counts.reset();
    limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");
    let first = counts.bytes_read();
    assert!(first > 0, "the scan should have read something");

    for run in 2..=10 {
        counts.reset();
        let rows = limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");
        assert_eq!(rows, expected, "run {run} returned different rows");
        let bytes = counts.bytes_read();
        assert!(
            bytes <= first,
            "run {run} read {bytes} bytes, more than the first run's {first}: \
             prefetched pages are piling up"
        );
    }
}

/// Readahead is an optimization, never a source of failure: if a prefetch read
/// fails, the query must still get its rows from an ordinary read.
#[test]
fn a_failing_prefetch_does_not_fail_the_query() {
    let tmp = TempDatabase::new_empty();
    let path = tmp.path.parent().unwrap().join("readahead-failing.db");
    let failing = Arc::new(FailPrefetchIo {
        inner: Arc::new(PlatformIO::new().unwrap()),
        fail_multi_page: Arc::new(AtomicU64::new(0)),
        page_size: Mutex::new(HashMap::new()),
    });
    let fail_flag = failing.fail_multi_page.clone();
    let db = Database::open_file_with_flags(
        failing,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let filler = "z".repeat(200);
    for i in 0..5_000 {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{filler}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("PRAGMA prefetch_pages = 32").unwrap();
    conn.execute("PRAGMA cache_size = -80").unwrap();

    // Every multi-page read from now on fails.
    fail_flag.store(1, Ordering::SeqCst);
    let rows = limbo_exec_rows(&conn, "SELECT count(*), sum(id) FROM t");
    assert_eq!(rows[0][0], rusqlite::types::Value::Integer(5_000));
    assert_eq!(
        rows[0][1],
        rusqlite::types::Value::Integer((0..5_000i64).sum::<i64>())
    );
}

/// An IO that fails any read larger than one page. Readahead reads a run of
/// pages as one request, so this fails exactly the prefetches and nothing else.
struct FailPrefetchIo {
    inner: Arc<dyn IO>,
    fail_multi_page: Arc<AtomicU64>,
    page_size: Mutex<HashMap<String, usize>>,
}

impl Clock for FailPrefetchIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for FailPrefetchIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        self.page_size.lock().unwrap().insert(path.to_string(), 0);
        let file = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(FailPrefetchFile {
            inner: file,
            fail_multi_page: self.fail_multi_page.clone(),
            applies: path.ends_with(".db"),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
}

struct FailPrefetchFile {
    inner: Arc<dyn File>,
    fail_multi_page: Arc<AtomicU64>,
    applies: bool,
}

impl File for FailPrefetchFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }

    /// Readahead is the only thing that reads a run of pages in one request,
    /// so failing this fails exactly the prefetches and nothing else.
    fn preadv(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        if self.applies && self.fail_multi_page.load(Ordering::SeqCst) == 1 {
            c.error(turso_core::CompletionError::Aborted);
            return Ok(c);
        }
        self.inner.preadv(pos, buffers, c)
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

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}
