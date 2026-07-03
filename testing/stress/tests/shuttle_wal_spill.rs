#![cfg(shuttle)]
//! Deterministic (shuttle) reproducer for the WAL spill frame-reuse corruption
//! family — the bug class behind the Antithesis
//! `cell_table_interior_read_rowid` page-type assertion
//! (`matches!(self.page_type(), Ok(PageType::TableInterior))` in
//! `core/storage/pager.rs`) observed while running
//! `stress/singleton_driver_stress.sh`.
//!
//! Mechanism (fixed by "Fix WAL spill frame reuse" / "fix nonblocking read_page
//! race and wal frame cache slot reuse corruption", June 30 2026):
//!
//!   * A write transaction under page-cache pressure spills dirty pages to the
//!     WAL as uncommitted frames (`append_frames_vectored`) and records
//!     page->frame mappings in the shared frame index.
//!   * The commit path re-seeded its append position from the shared committed
//!     snapshot instead of chaining after the spilled frames, so subsequent
//!     appends rewrote the spilled frame slots with *different* pages while the
//!     stale page->frame mappings survived in the index.
//!   * Any later read that resolved a page through a stale mapping got another
//!     page's bytes: a table-interior lookup then trips the
//!     `cell_table_interior_read_rowid` / `cell_index_read_payload_ptr` /
//!     `page_type_of_siblings` asserts (which one fires depends on which page
//!     the wrong content lands on), and `PRAGMA quick_check` reports
//!     corruption.
//!   * A concurrent reader is load-bearing: its read mark blocks the WAL
//!     restart that would otherwise clear the frame index and hide the stale
//!     mappings.
//!
//! The scenario drives the database exclusively through the public `turso`
//! API: one overflowing-blob table with a secondary index, a sidecar table, a
//! migration-style writer (rebuild sidecar + rewrite the indexed column), and
//! one concurrent reader mixing full scans with rowid point lookups (rowid
//! seeks descend table-interior pages via `tablebtree_move_to`, the exact
//! Antithesis assert path). `PRAGMA cache_size = 200` scales the cache
//! pressure down so spills happen with a few hundred rows instead of
//! thousands.
//!
//! On a build with the bug (< June 30 2026) this fails within the first
//! shuttle iterations — the engine panics with one of the page-type asserts or
//! `quick_check` reports corruption. On current main it must stay green.

use shuttle::scheduler::RandomScheduler;
use turso::{Builder, Value, params_from_iter};
use turso_stress::sync::Arc;
use turso_stress::sync::atomic::{AtomicBool, Ordering};

const ROWS: u64 = 700;
const ROUNDS: usize = 3;
const BLOB_BYTES: usize = 2048;
const POINT_LOOKUPS_PER_SCAN: u64 = 32;

fn shuttle_config() -> shuttle::Config {
    turso_stress::shuttle_config()
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

/// Error substrings that mark wrong-page-content corruption. Anything else
/// (Busy etc.) is tolerated; corruption is not.
const SIGS: &[&str] = &[
    "invalid page type",
    "inconsistent overflow chain",
    "non-index page",
    "cell_index_read_payload_ptr",
    "malformed",
    "corrupt",
    "wrong # of entries",
    "page_type_of_siblings",
    "tableinterior",
    "page_type",
    "page buffer not loaded",
];

fn corruption_sig(s: &str) -> bool {
    let l = s.to_lowercase();
    SIGS.iter().any(|sig| l.contains(sig))
}

async fn exhaust(conn: &turso::Connection, sql: &str) -> Result<(), String> {
    let mut rows = conn.query(sql, ()).await.map_err(|e| e.to_string())?;
    while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
    Ok(())
}

async fn connect(db: &turso::Database) -> turso::Connection {
    let conn = db.connect().expect("connect");
    conn.busy_timeout(std::time::Duration::from_secs(60)).ok();
    // Small page cache => the migration statements overflow it and spill
    // uncommitted frames to the WAL, which is precondition #1 for the bug.
    let _ = conn.execute("PRAGMA cache_size = 200", ()).await;
    conn
}

async fn wal_spill_frame_reuse_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("spill.db");
    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    {
        let conn = connect(&db).await;
        for ddl in [
            "CREATE TABLE t (id TEXT PRIMARY KEY, category TEXT NOT NULL, payload BLOB)",
            "CREATE INDEX t_category ON t(category)",
            "CREATE TABLE side (id TEXT PRIMARY KEY, v BLOB NOT NULL)",
        ] {
            conn.execute(ddl, ()).await.expect("ddl");
        }
        const BATCH: u64 = 100;
        let mut i = 0;
        while i < ROWS {
            let n = BATCH.min(ROWS - i);
            let tuples = vec!["(?,?,?)"; n as usize].join(",");
            let sql = format!("INSERT INTO t (id, category, payload) VALUES {tuples}");
            let mut params: Vec<Value> = Vec::with_capacity(n as usize * 3);
            for k in 0..n {
                let idx = i + k;
                params.push(Value::Text(format!("s{idx:012}")));
                params.push(Value::Text(format!("[\"{idx}\"]")));
                params.push(Value::Blob(blob(idx + 1, BLOB_BYTES)));
            }
            conn.execute(&sql, params_from_iter(params))
                .await
                .expect("seed");
            i += n;
        }
    }

    for round in 0..ROUNDS {
        // Re-arm the migration on a fresh connection: clear the sidecar and
        // restore the JSON-array category the strip-UPDATE consumes.
        {
            let conn = connect(&db).await;
            let _ = conn.execute("DELETE FROM side", ()).await;
            conn.execute(
                "UPDATE t SET category = '[\"' || category || '\"]' WHERE category NOT LIKE '[\"%'",
                (),
            )
            .await
            .expect("re-arm");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // Reader: full scans plus rowid point lookups. The point lookups
        // descend the table b-tree through `tablebtree_move_to`, the code path
        // whose TableInterior page-type assert fired on Antithesis. Its read
        // mark also blocks WAL restarts, keeping stale frame mappings live.
        let reader = {
            let db = db.clone();
            let stop = stop.clone();
            turso_stress::future::spawn(async move {
                let conn = connect(&db).await;
                let mut rng: u64 = 0x9e3779b97f4a7c15 ^ (round as u64);
                let mut hits: Vec<String> = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    if let Err(e) = exhaust(
                        &conn,
                        "SELECT count(*) FROM t WHERE typeof(payload) = 'blob'",
                    )
                    .await
                    {
                        if corruption_sig(&e) {
                            hits.push(e);
                            break;
                        }
                    }
                    for _ in 0..POINT_LOOKUPS_PER_SCAN {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let rowid = 1 + (rng % ROWS);
                        if let Err(e) = exhaust(
                            &conn,
                            &format!("SELECT length(payload) FROM t WHERE rowid = {rowid}"),
                        )
                        .await
                        {
                            if corruption_sig(&e) {
                                hits.push(e);
                                break;
                            }
                        }
                    }
                }
                hits
            })
        };

        // Writer: one migration round on its own fresh connection. Rebuilding
        // `side` from `t` reads every overflow chain and frees/reallocates
        // pages; the strip-UPDATE rewrites every row plus its index entry.
        // Both statements overflow the small page cache and spill.
        let writer = {
            let db = db.clone();
            turso_stress::future::spawn(async move {
                let conn = connect(&db).await;
                let mut hits: Vec<String> = Vec::new();
                for sql in [
                    "INSERT OR REPLACE INTO side (id, v) SELECT id, payload FROM t WHERE typeof(payload) = 'blob'",
                    "UPDATE t SET category = replace(replace(category, '[\"', ''), '\"]', '') WHERE category LIKE '[\"%'",
                ] {
                    if let Err(e) = conn.execute(sql, ()).await {
                        let msg = e.to_string();
                        if corruption_sig(&msg) {
                            hits.push(msg);
                        }
                        break;
                    }
                }
                hits
            })
        };

        let writer_hits = writer.await.expect("writer task");
        stop.store(true, Ordering::Relaxed);
        let reader_hits = reader.await.expect("reader task");

        assert!(
            writer_hits.is_empty() && reader_hits.is_empty(),
            "wrong-page-content corruption on round {round}: writer={writer_hits:?} reader={reader_hits:?}"
        );

        // Durable-damage check on a fresh connection.
        let conn = connect(&db).await;
        let mut rows = conn
            .query("PRAGMA quick_check", ())
            .await
            .expect("quick_check");
        let mut qc = Vec::new();
        while let Some(row) = rows.next().await.expect("quick_check rows") {
            if let Ok(s) = row.get::<String>(0) {
                if s != "ok" {
                    qc.push(s);
                }
            }
        }
        assert!(qc.is_empty(), "quick_check failed on round {round}: {qc:?}");
    }
}

#[test]
fn shuttle_test_wal_spill_frame_reuse() {
    let scheduler = RandomScheduler::new(5);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(wal_spill_frame_reuse_scenario()));
}
