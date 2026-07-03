//! Minimal reproduction of a turso local-DB corruption (public API only).
//!
//! Distilled from a field report where an app-upgrade migration corrupted users'
//! databases. The original needed a 50k-row wide table, 5 secondary indexes, two
//! sidecar tables, 3 migration statements and 8 concurrent readers. This strips
//! that to the essentials that still corrupt the database **on the 2nd round**:
//!
//!   * one table `t(id, category, payload)` whose `payload` blob overflows onto
//!     overflow pages, with ONE secondary index on `category`;
//!   * one sidecar table `side`;
//!   * a writer that each round (a) rebuilds `side` from `t` via
//!     `INSERT OR REPLACE … SELECT` (frees+reallocates pages, reads overflow) and
//!     (b) `UPDATE t SET category = …` (rewrites every row + its index entry);
//!   * exactly ONE concurrent reader scanning `t`.
//!
//! With zero readers it stays clean; a single concurrent reader is enough to
//! expose the underlying WAL bug. The page churn makes the migration's write
//! transaction large enough to spill dirty pages to the WAL (a cacheflush).
//! The spill advances the WAL frame counter but the following commit re-seeds
//! its frame numbering from the still-unpublished snapshot, so it reuses frame
//! numbers the spill already wrote. The connection-local page->frame cache then
//! holds a stale pre-spill frame number for a page; a concurrent reader follows
//! it and reads a frame that now physically holds a *different* page. The
//! corruption surfaces (release-visible) as `PRAGMA quick_check` failing ("wrong
//! # of entries in index t_category"), readers getting `Invalid page type` /
//! `inconsistent overflow chain`, and — with debug assertions on — an engine
//! assert (`page_type … == page_type_of_siblings`, IndexLeaf vs TableLeaf)
//! during b-tree balancing.
//!
//! No engine internals, tracing hooks, or fault injection are used: the database
//! is driven only through the public `turso` async API and the bug is detected
//! from ordinary statement errors, engine panics, and `PRAGMA quick_check`.
//!
//! This is the same wrong-page-content family as the Antithesis
//! `cell_table_interior_read_rowid` page-type assertion
//! (`matches!(self.page_type(), Ok(PageType::TableInterior))`,
//! `core/storage/pager.rs`) hit by `stress/singleton_driver_stress.sh`: a
//! page resolved through a stale WAL frame mapping comes back with another
//! page's bytes, and whichever traversal touches it first fires its
//! page-type assert. The readers here mix full scans with rowid point
//! lookups so the table-interior descent (`tablebtree_move_to`) — the exact
//! Antithesis assert path — runs continuously against the churned tree.
//!
//! Verified: reproduces on 0311ae90 (pre "Fix WAL spill frame reuse",
//! June 30 2026) within 2 rounds on every attempt; stays clean on main after
//! d380bfbb + 80f8cdd6.
//!
//! Run (reproduces within a couple of rounds on affected builds):
//!   cargo run -p turso --example migration_corruption_minimal
//! Optional overrides: --rows N --rounds N --readers N

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use turso::{Builder, Value, params_from_iter};

// Minimal config that still reproduces reliably (iter 2). Scale floor is ~3000
// rows; 5000 gives a safety margin. A single reader is sufficient.
const ROWS: u64 = 5000;
const ROUNDS: usize = 8;
const READERS: usize = 1;
const BLOB_BYTES: usize = 2048; // large enough to spill onto overflow pages

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1).cloned())
}
fn argn(name: &str, default: u64) -> u64 {
    arg(name).and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// Substrings that mark a turso corruption error/panic (release-visible ones plus
/// the debug-assert text and the quick_check index-mismatch message).
const SIGS: &[&str] = &[
    "invalid page type",
    "inconsistent overflow chain",
    "short read",
    "non-index page",
    "cell_index_read_payload_ptr",
    "malformed",
    "corrupt",
    "wrong # of entries",
    "page_type_of_siblings",
    // tablebtree_move_to descending into a page that is not a table interior:
    // the `cell_table_interior_read_rowid` page-type debug assert seen on
    // Antithesis (`matches! (self.page_type(), Ok(PageType::TableInterior))`).
    "tableinterior",
    "page_type",
    "page buffer not loaded",
];
fn is_corruption(s: &str) -> bool {
    let l = s.to_lowercase();
    SIGS.iter().any(|sig| l.contains(sig))
}

#[derive(Clone, Default)]
struct Hits(Arc<Mutex<Vec<String>>>);
impl Hits {
    fn record(&self, who: &str, msg: &str) {
        self.0.lock().unwrap().push(format!("[{who}] {msg}"));
    }
    fn list(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// Deterministic pseudo-random bytes (xorshift) so each row's overflow blob differs.
fn blob(seed: u64, len: usize) -> Vec<u8> {
    let mut s = seed | 1;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s & 0xff) as u8
        })
        .collect()
}

async fn run(conn: &turso::Connection, sql: &str) -> Result<(), String> {
    let mut rows = conn.query(sql, ()).await.map_err(|e| e.to_string())?;
    while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
    Ok(())
}

/// First line of a (possibly multi-line) message, with a count of the rest —
/// `PRAGMA quick_check` returns one line per corrupt page.
fn first_line(s: &str) -> String {
    let mut lines = s.lines();
    let head = lines.next().unwrap_or("").trim().to_string();
    let rest = lines.filter(|l| !l.trim().is_empty()).count();
    if rest > 0 {
        format!("{head} (+{rest} more lines)")
    } else {
        head
    }
}

fn panic_msg(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<panic>".into())
}

// Oversubscribe worker threads so the single non-yielding writer task is never
// starved by the hot reader task(s) (with worker_threads == cpu count, ≥cpu hot
// readers occupy every worker and the writer never runs).
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    // The engine panics on the corruption; that panic is caught per-task and
    // reported below, so replace the default hook's multi-page backtrace with a
    // single concise line.
    std::panic::set_hook(Box::new(|info| {
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_default();
        eprintln!(
            "  [engine panic] {} :: {}",
            info.location().map(|l| l.to_string()).unwrap_or_default(),
            first_line(&msg)
        );
    }));

    let rows = argn("--rows", ROWS);
    let rounds = argn("--rounds", ROUNDS as u64) as usize;
    let readers = argn("--readers", READERS as u64) as usize;
    // Width of each distinct category key.
    let catlen = argn("--catlen", 1) as usize;
    // Payload size. The bug is driven by page-churn *volume* (the sidecar
    // DELETE+INSERT..SELECT frees/reallocates pages proportional to total bytes),
    // so large overflow payloads let far fewer rows produce the same churn.
    let blob_bytes = argn("--blob", BLOB_BYTES as u64) as usize;

    let mut path = std::env::temp_dir();
    path.push("turso-migration-corruption-minimal.db");
    let path = path.to_string_lossy().to_string();
    for sfx in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{sfx}"));
    }

    let db = Builder::new_local(&path).build().await.expect("open db");

    // Schema: one overflowing-blob table with ONE secondary index, plus one sidecar.
    {
        let c = db.connect().expect("connect");
        for ddl in [
            "CREATE TABLE t (id TEXT PRIMARY KEY, category TEXT NOT NULL, payload BLOB)",
            "CREATE INDEX t_category ON t(category)",
            "CREATE TABLE side (id TEXT PRIMARY KEY, v BLOB NOT NULL)",
        ] {
            c.execute(ddl, ()).await.expect("ddl");
        }
    }

    // Seed rows with overflowing blobs.
    eprintln!("seeding {rows} rows (payload {blob_bytes}B) -> {path}");
    {
        let c = db.connect().expect("connect");
        c.busy_timeout(std::time::Duration::from_secs(60)).ok();
        const BATCH: u64 = 200;
        let mut i = 0;
        while i < rows {
            let n = BATCH.min(rows - i);
            let tuples = vec!["(?,?,?)"; n as usize].join(",");
            let sql = format!("INSERT INTO t (id, category, payload) VALUES {tuples}");
            let mut params: Vec<Value> = Vec::with_capacity(n as usize * 3);
            for k in 0..n {
                let idx = i + k;
                params.push(Value::Text(format!("s{idx:012}")));
                params.push(Value::Text(format!("[\"{idx:0>catlen$}\"]")));
                params.push(Value::Blob(blob(idx + 1, blob_bytes)));
            }
            c.execute(&sql, params_from_iter(params))
                .await
                .expect("seed");
            i += n;
        }
    }

    let hits = Hits::default();
    let mut corrupted = false;

    // Each round mirrors one migration pass: (1) re-arm on a fresh connection
    // (clear `side`, restore the JSON-array category the strip-UPDATE consumes);
    // (2) spawn fresh reader(s) that scan `t`; (3) run the migration on its own
    // fresh connection — rebuild `side` from `t` (a free→reallocate burst that
    // reads every overflow chain) then strip every row's indexed `category`
    // (index delete+insert); (4) stop the readers; (5) quick_check. Using fresh
    // connections (cold page caches), as a real migration + app readers would, is
    // load-bearing: a long-lived reader's warm cache hides the reused page.
    for round in 0..rounds {
        let _ = round;
        {
            let c = db.connect().expect("connect");
            c.busy_timeout(std::time::Duration::from_secs(60)).ok();
            let _ = c.execute("DELETE FROM side", ()).await;
            c.execute(
                "UPDATE t SET category = '[\"' || category || '\"]' WHERE category NOT LIKE '[\"%'",
                (),
            )
            .await
            .expect("re-arm");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let mut reader_handles = Vec::new();
        for r in 0..readers {
            let db = db.clone();
            let stop = stop.clone();
            let hits = hits.clone();
            let nrows = rows;
            reader_handles.push(tokio::spawn(async move {
                let conn = db.connect().expect("connect");
                conn.busy_timeout(std::time::Duration::from_secs(60)).ok();
                let mut rng: u64 = 0x9e3779b97f4a7c15 ^ (r as u64);
                while !stop.load(Ordering::Relaxed) {
                    if let Err(e) = run(
                        &conn,
                        "SELECT count(*) FROM t WHERE typeof(payload) = 'blob'",
                    )
                    .await
                    {
                        if is_corruption(&e) {
                            hits.record(&format!("reader{r}"), &e);
                        }
                    }
                    // Rowid point lookups descend the table b-tree through
                    // tablebtree_move_to — the code path whose TableInterior
                    // page-type assert fired on Antithesis.
                    for _ in 0..64 {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let rowid = 1 + (rng % nrows.max(1));
                        if let Err(e) = run(
                            &conn,
                            &format!("SELECT length(payload) FROM t WHERE rowid = {rowid}"),
                        )
                        .await
                        {
                            if is_corruption(&e) {
                                hits.record(&format!("reader{r}"), &e);
                            }
                        }
                    }
                }
            }));
        }

        let wh = hits.clone();
        let wdb = db.clone();
        let aborted = tokio::spawn(async move {
            let conn = wdb.connect().expect("connect");
            conn.busy_timeout(std::time::Duration::from_secs(60)).ok();
            for sql in [
                "INSERT OR REPLACE INTO side (id, v) SELECT id, payload FROM t WHERE typeof(payload) = 'blob'",
                "UPDATE t SET category = replace(replace(category, '[\"', ''), '\"]', '') WHERE category LIKE '[\"%'",
            ] {
                if let Err(e) = conn.execute(sql, ()).await {
                    if is_corruption(&e.to_string()) {
                        wh.record("writer", &e.to_string());
                    }
                    return true;
                }
            }
            false
        })
        .await;

        let aborted = match aborted {
            Ok(a) => a,
            Err(je) if je.is_panic() => {
                // Engine panic (e.g. page_type mismatch during balance) — always corruption.
                hits.record("writer.panic", &panic_msg(je.into_panic()));
                true
            }
            Err(_) => false,
        };

        stop.store(true, Ordering::Relaxed);
        for h in reader_handles {
            if let Err(je) = h.await {
                if je.is_panic() {
                    hits.record("reader.panic", &panic_msg(je.into_panic()));
                }
            }
        }

        // Integrity check on a fresh connection (catches durable on-disk damage).
        let mut qc = "ok".to_string();
        {
            let c = db.connect().expect("connect");
            match c.query("PRAGMA quick_check", ()).await {
                Ok(mut r) => loop {
                    match r.next().await {
                        Ok(Some(row)) => {
                            if let Ok(s) = row.get::<String>(0) {
                                if s != "ok" {
                                    qc = s;
                                    break;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            qc = e.to_string();
                            break;
                        }
                    }
                },
                Err(e) => qc = e.to_string(),
            }
        }
        let qc_short = first_line(&qc);
        if qc != "ok" {
            hits.record("quick_check", &qc_short);
        }

        eprintln!(
            "  round {:>2}/{rounds}: {}  quick_check={}",
            round + 1,
            if aborted {
                "writer ABORTED"
            } else {
                "writer ok    "
            },
            if qc == "ok" {
                "ok".into()
            } else {
                format!("FAIL({qc_short})")
            },
        );

        if !hits.list().is_empty() {
            corrupted = true;
            break;
        }
    }

    println!();
    if corrupted {
        println!("REPRODUCED — database corrupted by the migration under a concurrent reader:");
        // Collapse the flood of identical reader errors to one line each.
        let mut seen = std::collections::HashSet::new();
        for h in hits.list() {
            if seen.insert(h.clone()) {
                println!("  - {h}");
            }
        }
        println!("\nCorrupt DB left at: {path}");
        std::process::exit(0);
    } else {
        println!("NO corruption observed in {rounds} rounds (try more --rounds or --rows).");
        std::process::exit(2);
    }
}
