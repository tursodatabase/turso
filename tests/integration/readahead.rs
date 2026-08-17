//! End-to-end coverage for scan readahead (`PRAGMA prefetch_pages`).
//!
//! The tests run on `QueuedIo`, which defers every IO operation into a queue
//! the harness drains step by step. That gives two things a plain file DB
//! cannot: an exact history of submitted operations, and reads that stay in
//! flight until the engine actually waits for them — which is precisely the
//! window readahead is supposed to exploit.

use crate::queued_io::{QueuedIo, QueuedIoOpKind};
use std::sync::Arc;
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect};

fn open_db(io: Arc<QueuedIo>, path: &str) -> Arc<Database> {
    Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

/// Executes a query and renders rows as pipe-separated values.
fn query_rows(conn: &Arc<Connection>, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values: Vec<String> = row.get_values().map(|value| format!("{value}")).collect();
        rows.push(values.join("|"));
        Ok(())
    })
    .unwrap();
    rows
}

fn count_preads(io: &QueuedIo, path_suffix: &str) -> usize {
    io.history()
        .iter()
        .filter(|e| e.kind == QueuedIoOpKind::Pread && e.path.ends_with(path_suffix))
        .count()
}

/// Creates a table whose btree spans several hundred leaf pages, then
/// checkpoints so the pages live in the main DB file.
fn build_scan_fixture(io: &Arc<QueuedIo>, path: &str, rows: usize) {
    let db = open_db(io.clone(), path);
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute(format!(
        "INSERT INTO t SELECT value, hex(zeroblob(48)) FROM generate_series(1, {rows})"
    ))
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

const SCAN_FIXTURE_ROWS: usize = 20000;
const SCAN_QUERY: &str = "SELECT count(*), sum(a), max(b) FROM t";

#[test]
fn full_scan_with_readahead_returns_identical_rows_and_submits_reads() {
    let io = Arc::new(QueuedIo::new());
    build_scan_fixture(&io, "readahead-scan.db", SCAN_FIXTURE_ROWS);

    // Baseline: fresh database instance (cold page cache), readahead off.
    // The pragma runs in both variants so the schema loads before the pread
    // counting window starts.
    let db = open_db(io.clone(), "readahead-scan.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 0").unwrap();
    let preads_before = count_preads(&io, "readahead-scan.db");
    let baseline = query_rows(&conn, SCAN_QUERY);
    let baseline_preads = count_preads(&io, "readahead-scan.db") - preads_before;
    let stats = conn.get_pager().readahead_stats();
    assert_eq!(
        stats.pages_submitted, 0,
        "readahead is off by default and must not submit reads"
    );

    // Same scan on another cold instance with readahead on.
    let db = open_db(io.clone(), "readahead-scan.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    let preads_before = count_preads(&io, "readahead-scan.db");
    let with_readahead = query_rows(&conn, SCAN_QUERY);
    let scan_preads = count_preads(&io, "readahead-scan.db") - preads_before;

    assert_eq!(baseline, with_readahead);
    let stats = conn.get_pager().readahead_stats();
    assert!(
        stats.pages_submitted > 100,
        "a scan over hundreds of leaves should submit hundreds of reads early, got {stats:?}"
    );
    // Readahead must not read anything a plain scan would not read: the
    // fixture pages are visited exactly once either way.
    assert_eq!(
        baseline_preads, scan_preads,
        "readahead changed the total number of DB file reads"
    );
}

#[test]
fn point_queries_never_prefetch() {
    let io = Arc::new(QueuedIo::new());
    build_scan_fixture(&io, "readahead-point.db", SCAN_FIXTURE_ROWS);

    let db = open_db(io.clone(), "readahead-point.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    for a in [1, 5000, 9999, 12345, 19999] {
        let rows = query_rows(&conn, &format!("SELECT a FROM t WHERE a = {a}"));
        assert_eq!(rows, vec![a.to_string()]);
    }
    let stats = conn.get_pager().readahead_stats();
    assert_eq!(
        stats.pages_submitted, 0,
        "point lookups are random access and must not trigger readahead: {stats:?}"
    );
}

#[test]
fn short_limit_scan_wastes_at_most_the_initial_window() {
    let io = Arc::new(QueuedIo::new());
    build_scan_fixture(&io, "readahead-limit.db", SCAN_FIXTURE_ROWS);

    let db = open_db(io.clone(), "readahead-limit.db");
    let conn = db.connect().unwrap();
    let budget = 64;
    conn.execute(format!("PRAGMA prefetch_pages = {budget}"))
        .unwrap();
    let rows = query_rows(&conn, "SELECT a FROM t LIMIT 5");
    assert_eq!(rows.len(), 5);
    let stats = conn.get_pager().readahead_stats();
    assert!(
        stats.pages_submitted <= budget as u64,
        "a LIMIT 5 scan must not fetch more than one window ahead: {stats:?}"
    );
}

#[test]
fn scan_reading_from_the_wal_prefetches_frames() {
    let io = Arc::new(QueuedIo::new());
    // No checkpoint: all table data stays in the WAL.
    let db = open_db(io.clone(), "readahead-wal.db");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute(format!(
        "INSERT INTO t SELECT value, hex(zeroblob(48)) FROM generate_series(1, {SCAN_FIXTURE_ROWS})"
    ))
    .unwrap();
    drop(conn);

    let db = open_db(io.clone(), "readahead-wal.db");
    let conn = db.connect().unwrap();
    let baseline = query_rows(&conn, SCAN_QUERY);

    let db = open_db(io.clone(), "readahead-wal.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    let with_readahead = query_rows(&conn, SCAN_QUERY);

    assert_eq!(baseline, with_readahead);
    let stats = conn.get_pager().readahead_stats();
    assert!(
        stats.pages_submitted > 100,
        "WAL-resident pages must be prefetched through the WAL: {stats:?}"
    );
    assert!(
        count_preads(&io, "readahead-wal.db-wal") > 0,
        "expected WAL reads in the IO history"
    );
}

#[test]
fn fragmented_table_scans_match_without_readahead() {
    let io = Arc::new(QueuedIo::new());
    let db = open_db(io.clone(), "readahead-frag.db");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute(format!(
        "INSERT INTO t SELECT value, hex(zeroblob(48)) FROM generate_series(1, {SCAN_FIXTURE_ROWS})"
    ))
    .unwrap();
    // Punch holes everywhere, then add fresh rows so free pages get reused
    // and the leaf order no longer matches the physical page order.
    conn.execute("DELETE FROM t WHERE a % 3 = 0").unwrap();
    conn.execute(format!(
        "INSERT INTO t SELECT value + {SCAN_FIXTURE_ROWS}, hex(zeroblob(48)) FROM generate_series(1, 5000)"
    ))
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    let db = open_db(io.clone(), "readahead-frag.db");
    let conn = db.connect().unwrap();
    let baseline = query_rows(&conn, SCAN_QUERY);

    let db = open_db(io.clone(), "readahead-frag.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    let with_readahead = query_rows(&conn, SCAN_QUERY);

    assert_eq!(baseline, with_readahead);
    let stats = conn.get_pager().readahead_stats();
    assert!(
        stats.pages_submitted > 100,
        "readahead follows btree pointers, so fragmentation must not stop it: {stats:?}"
    );
}

#[test]
fn index_range_scans_prefetch_index_leaves() {
    let io = Arc::new(QueuedIo::new());
    let db = open_db(io.clone(), "readahead-index.db");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER, c TEXT)")
        .unwrap();
    conn.execute(format!(
        "INSERT INTO t SELECT value, value * 7, hex(zeroblob(48)) FROM generate_series(1, {SCAN_FIXTURE_ROWS})"
    ))
    .unwrap();
    conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    const RANGE_QUERY: &str = "SELECT count(*), sum(b) FROM t WHERE b > 0";

    let db = open_db(io.clone(), "readahead-index.db");
    let conn = db.connect().unwrap();
    let baseline = query_rows(&conn, RANGE_QUERY);

    let db = open_db(io.clone(), "readahead-index.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    let with_readahead = query_rows(&conn, RANGE_QUERY);

    assert_eq!(baseline, with_readahead);
    let stats = conn.get_pager().readahead_stats();
    assert!(
        stats.pages_submitted > 20,
        "a covering index range scan should prefetch index leaves: {stats:?}"
    );
}

#[test]
fn backward_scans_do_not_prefetch() {
    let io = Arc::new(QueuedIo::new());
    build_scan_fixture(&io, "readahead-desc.db", SCAN_FIXTURE_ROWS);

    let db = open_db(io.clone(), "readahead-desc.db");
    let conn = db.connect().unwrap();
    let baseline = query_rows(&conn, "SELECT a FROM t ORDER BY a DESC LIMIT 100");

    let db = open_db(io.clone(), "readahead-desc.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    let with_readahead = query_rows(&conn, "SELECT a FROM t ORDER BY a DESC LIMIT 100");

    assert_eq!(baseline, with_readahead);
    let stats = conn.get_pager().readahead_stats();
    assert_eq!(
        stats.pages_submitted, 0,
        "backward iteration is out of scope and must not prefetch: {stats:?}"
    );
}

#[test]
fn joined_scans_over_two_tables_stay_correct() {
    let io = Arc::new(QueuedIo::new());
    let db = open_db(io.clone(), "readahead-join.db");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE t2(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t1 SELECT value, hex(zeroblob(48)) FROM generate_series(1, 5000)")
        .unwrap();
    conn.execute("INSERT INTO t2 SELECT value, hex(zeroblob(48)) FROM generate_series(1, 5000)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    const JOIN_QUERY: &str = "SELECT count(*), sum(t1.a + t2.a) FROM t1 JOIN t2 ON t1.a = t2.a";

    let db = open_db(io.clone(), "readahead-join.db");
    let conn = db.connect().unwrap();
    let baseline = query_rows(&conn, JOIN_QUERY);

    let db = open_db(io.clone(), "readahead-join.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    let with_readahead = query_rows(&conn, JOIN_QUERY);

    assert_eq!(baseline, with_readahead);
}

#[test]
fn scans_under_a_tiny_page_cache_degrade_gracefully() {
    let io = Arc::new(QueuedIo::new());
    build_scan_fixture(&io, "readahead-tiny-cache.db", SCAN_FIXTURE_ROWS);

    let db = open_db(io.clone(), "readahead-tiny-cache.db");
    let conn = db.connect().unwrap();
    // Minimum cache, maximum prefetch budget: prefetches must be dropped,
    // not spilled or errored, and the scan must stay correct.
    conn.execute("PRAGMA cache_size = 200").unwrap();
    conn.execute("PRAGMA prefetch_pages = 4096").unwrap();
    let rows = query_rows(&conn, SCAN_QUERY);

    let db = open_db(io.clone(), "readahead-tiny-cache.db");
    let conn = db.connect().unwrap();
    let baseline = query_rows(&conn, SCAN_QUERY);

    assert_eq!(baseline, rows);
}

#[test]
fn pragma_round_trips_and_clamps() {
    let io = Arc::new(QueuedIo::new());
    let db = open_db(io.clone(), "readahead-pragma.db");
    let conn = db.connect().unwrap();
    assert_eq!(query_rows(&conn, "PRAGMA prefetch_pages"), vec!["0"]);
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    assert_eq!(query_rows(&conn, "PRAGMA prefetch_pages"), vec!["64"]);
    conn.execute("PRAGMA prefetch_pages = -3").unwrap();
    assert_eq!(query_rows(&conn, "PRAGMA prefetch_pages"), vec!["0"]);
    conn.execute("PRAGMA prefetch_pages = 1000000").unwrap();
    assert_eq!(query_rows(&conn, "PRAGMA prefetch_pages"), vec!["4096"]);
}

#[test]
fn bare_count_star_prefetches_like_a_scan() {
    let io = Arc::new(QueuedIo::new());
    build_scan_fixture(&io, "readahead-count.db", SCAN_FIXTURE_ROWS);

    let db = open_db(io.clone(), "readahead-count.db");
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA prefetch_pages = 64").unwrap();
    // A bare COUNT(*) uses the btree count fast path, not next(); it sweeps
    // every leaf and must prefetch the same way.
    let rows = query_rows(&conn, "SELECT count(*) FROM t");
    assert_eq!(rows, vec![SCAN_FIXTURE_ROWS.to_string()]);
    let stats = conn.get_pager().readahead_stats();
    assert!(
        stats.pages_submitted > 100,
        "count(*) sweeps every leaf and should prefetch: {stats:?}"
    );
}
