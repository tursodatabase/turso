//! In-process timing of the customer's 8 benchmarked queries against the real cortex DB,
//! comparing turso_core (in-process, NOT the CLI) to rusqlite (real SQLite). This avoids both
//! the tursodb CLI's comfy_table formatting artifact and the Python-binding/allocator confounds.
//!
//! Run:  CORTEX_DB=/path/to/cortex_big.db cargo bench -p turso_core --bench cortex_queries_benchmark
//!
//! Plain harness (no criterion): warms each query once, then reports the median of N drains.

use std::sync::Arc;
use std::time::Instant;
use turso_core::{Database, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const QUERIES: &[(&str, &str)] = &[
    ("correlated_dedup", "SELECT c.match_strategy, COALESCE((SELECT d.review_status FROM match_decisions d WHERE d.equipment_id = c.equipment_id ORDER BY d.id DESC LIMIT 1), 'pending') AS rs, COUNT(*) FROM match_results_candidates c GROUP BY c.match_strategy, rs"),
    ("fulltable_scan_LIKE", "SELECT COUNT(*) FROM equipment_inventory WHERE source_desc LIKE '%description%'"),
    ("groupby_aggregate", "SELECT account_name, COUNT(*) FROM equipment_inventory GROUP BY account_name"),
    ("text_search_LIKE", "SELECT id, account_name FROM equipment_inventory WHERE source_desc LIKE '%999999%'"),
    ("orderby_limit_page", "SELECT id, account_name FROM equipment_inventory ORDER BY source_desc LIMIT 50 OFFSET 100000"),
    ("windowed_rownumber", "WITH winning AS (SELECT equipment_id, review_status, ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY id DESC) rn FROM match_decisions) SELECT c.match_strategy, COALESCE(w.review_status,'pending') rs, COUNT(*) FROM match_results_candidates c LEFT JOIN winning w ON w.equipment_id = c.equipment_id AND w.rn = 1 GROUP BY c.match_strategy, rs"),
    ("indexed_point_lookup", "SELECT COUNT(*) FROM equipment_inventory WHERE account_name = 'Hospital-3'"),
    ("full_fetch_materialize", "SELECT id, account_name, source_desc FROM equipment_inventory"),
];

const TRIALS: usize = 7;

fn drain_turso(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                std::hint::black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let path = std::env::var("CORTEX_DB").expect("set CORTEX_DB to the cortex_big.db path");
    let run_sqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, &path).unwrap();
    let conn = db.connect().unwrap();
    {
        let mut p = conn.prepare("PRAGMA cache_size=-262144").unwrap();
        while !matches!(p.step().unwrap(), StepResult::Done) {
            db.io.step().unwrap();
        }
    }

    let sconn = rusqlite::Connection::open(&path).unwrap();
    sconn.pragma_update(None, "cache_size", -262144i64).unwrap();

    println!(
        "{:<26}{:>12}{:>12}{:>9}",
        "query", "turso", "sqlite", "ratio"
    );
    for (name, sql) in QUERIES {
        // turso_core in-process
        let mut t_ms = Vec::new();
        for i in 0..=TRIALS {
            let mut stmt = conn.prepare(sql).unwrap();
            let t = Instant::now();
            drain_turso(&db, &mut stmt);
            if i > 0 {
                t_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            }
        }
        let tm = median(t_ms);

        let sm = if run_sqlite {
            let mut s_ms = Vec::new();
            for i in 0..=TRIALS {
                let mut stmt = sconn.prepare(sql).unwrap();
                let t = Instant::now();
                let mut rows = stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    std::hint::black_box(row);
                }
                if i > 0 {
                    s_ms.push(t.elapsed().as_secs_f64() * 1000.0);
                }
            }
            median(s_ms)
        } else {
            f64::NAN
        };

        println!("{name:<26}{tm:>10.1}ms{sm:>10.1}ms{:>8.2}x", tm / sm);
    }
}
