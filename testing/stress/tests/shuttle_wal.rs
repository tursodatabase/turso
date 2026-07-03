#![cfg(shuttle)]
//! Reproducer for WAL corruption surfacing as `Corrupt: Invalid page type: 0`
//! (Antithesis, turso-stress).
//!
//! Mechanism (all through public SQL APIs):
//!
//! 1. A PASSIVE checkpoint gets capped by a concurrent reader's read mark, so
//!    it only partially backfills. For a partial backfill,
//!    `CheckpointResult::should_truncate()` is false, so the WAL-side state
//!    machine releases the checkpoint guard immediately
//!    (core/storage/wal.rs `CheckpointState::Finalize`), while the pager side
//!    still has IO-yielding phases (`SyncDbFile` -> `ReadDbIdentity`) to run
//!    before `PublishBackfill` stores `nbackfills`.
//! 2. In that window another connection completes a TRUNCATE checkpoint:
//!    backfills the rest, restarts the WAL generation (max_frame=0,
//!    nbackfills=0) and truncates the WAL. A third connection then commits new
//!    transactions, appending frames 1..k of the new generation.
//! 3. The parked PASSIVE checkpoint finally runs `PublishBackfill` with its
//!    stale frame number X from the previous generation: `publish_backfill`
//!    stores it unconditionally, so now `nbackfills = X > max_frame = k`.
//! 4. Every new reader computes `min_frame = nbackfills + 1 > max_frame`, so
//!    `find_frame` never matches: the WAL content of the new generation is
//!    invisible and reads silently fall back to the DB file. Pages that were
//!    reused from freed zeroblob overflow pages (whose on-disk image is all
//!    zeros) come back as zero pages: `Invalid page type: 0`. The next write
//!    transaction instead trips
//!    `turso_assert!(max_frame >= nbackfills, "backfills can't be more than max_frame")`.
//!
//! Run with:
//!   RUSTFLAGS="--cfg shuttle" cargo test -p turso_stress --test shuttle_wal -- --nocapture

use shuttle::scheduler::PctScheduler;
use turso::{Builder, Value};

fn shuttle_config() -> shuttle::Config {
    turso_stress::shuttle_config()
}

/// Fail loudly (under shuttle => replayable schedule) on any corruption error.
fn check_corruption(e: &turso::Error, ctx: &str) {
    let msg = format!("{e}");
    if matches!(e, turso::Error::Corrupt(_)) || msg.contains("Invalid page type") {
        panic!("CORRUPTION REPRODUCED ({ctx}): {e}");
    }
}

fn is_transient(e: &turso::Error) -> bool {
    let s = format!("{e}");
    s.contains("locked")
        || s.contains("busy")
        || s.contains("Busy")
        || s.contains("snapshot")
        || s.contains("Snapshot")
}

async fn exec(conn: &turso::Connection, sql: &str, ctx: &str) {
    if let Err(e) = conn.execute(sql, ()).await {
        check_corruption(&e, ctx);
        if !is_transient(&e) {
            panic!("unexpected error ({ctx}) running `{sql}`: {e}");
        }
    }
}

/// Run a statement via query() and drain all rows (pragmas execute lazily).
/// Returns the first column of the first row, if any.
async fn drain(conn: &turso::Connection, sql: &str, ctx: &str) -> Option<Value> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(e) => {
            check_corruption(&e, ctx);
            if !is_transient(&e) {
                panic!("unexpected error ({ctx}) preparing `{sql}`: {e}");
            }
            return None;
        }
    };
    let mut first = None;
    loop {
        match rows.next().await {
            Ok(Some(row)) => {
                if first.is_none() {
                    first = Some(row.get_value(0).unwrap());
                }
            }
            Ok(None) => return first,
            Err(e) => {
                check_corruption(&e, ctx);
                if !is_transient(&e) {
                    panic!("unexpected error ({ctx}) stepping `{sql}`: {e}");
                }
                return first;
            }
        }
    }
}

async fn checkpoint_publish_race_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    // ---------------- deterministic prefix ----------------
    let setup = db.connect().unwrap();
    exec(
        &setup,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)",
        "setup",
    )
    .await;
    // Plant all-zero page images in the DB file: a large zeroblob creates
    // ~74 all-zero overflow pages; TRUNCATE-checkpoint them into the DB file,
    // then free them so later inserts reuse those page numbers.
    exec(
        &setup,
        "INSERT INTO t VALUES (1, zeroblob(120000))",
        "setup",
    )
    .await;
    drain(&setup, "PRAGMA wal_checkpoint(TRUNCATE)", "setup").await;
    exec(&setup, "DELETE FROM t WHERE a = 1", "setup").await;
    drain(&setup, "PRAGMA wal_checkpoint(TRUNCATE)", "setup").await;

    // Committed WAL-only content, reusing freed (all-zero-on-disk) pages.
    exec(
        &setup,
        "INSERT INTO t VALUES (2, randomblob(25000))",
        "setup",
    )
    .await;

    // Reader parks its read mark at the current max frame (F1).
    let reader = db.connect().unwrap();
    exec(&reader, "BEGIN", "reader").await;
    drain(&reader, "SELECT count(*) FROM t", "reader").await;

    // More committed frames (F1+1..F2) so the PASSIVE checkpoint below is
    // capped by the reader's mark: a *partial* backfill.
    exec(
        &setup,
        "INSERT INTO t VALUES (3, randomblob(25000))",
        "setup",
    )
    .await;

    // ---------------- concurrent window ----------------
    // Task C: PASSIVE checkpoint. Backfills only up to the reader's mark,
    // releases the checkpoint guard early, and its stale PublishBackfill is
    // still pending across IO yields.
    let conn_c = db.connect().unwrap();
    let c_task = turso_stress::future::spawn(async move {
        drain(
            &conn_c,
            "PRAGMA wal_checkpoint(PASSIVE)",
            "passive-checkpoint",
        )
        .await;
    });

    // Task S: end the reader, TRUNCATE-checkpoint (restarts the WAL
    // generation), then commit new-generation transactions that reuse the
    // freed all-zero pages.
    let conn_w = db.connect().unwrap();
    let s_task = turso_stress::future::spawn(async move {
        exec(&reader, "COMMIT", "reader-commit").await;
        for _ in 0..20 {
            let busy = drain(
                &conn_w,
                "PRAGMA wal_checkpoint(TRUNCATE)",
                "truncate-checkpoint",
            )
            .await;
            match busy {
                Some(Value::Integer(0)) => break, // not busy: truncated
                _ => shuttle::future::yield_now().await,
            }
        }
        // New-generation commits: content only in WAL frames 1..k. Small rows
        // so the reused (all-zero-on-disk) pages become table LEAF pages: a
        // reader that can't see these frames then walks a zero page image and
        // fails with the literal `Invalid page type: 0`. One larger row also
        // exercises an overflow chain (`inconsistent overflow chain`).
        exec(
            &conn_w,
            "INSERT INTO t SELECT 10 + value, randomblob(1500) FROM generate_series(0, 29)",
            "new-gen-insert",
        )
        .await;
        exec(
            &conn_w,
            "INSERT INTO t VALUES (99, randomblob(15000))",
            "new-gen-insert-overflow",
        )
        .await;
    });

    let _ = c_task.await;
    let _ = s_task.await;

    // ---------------- verification ----------------
    // A fresh connection must see a consistent database. If nbackfills was
    // poisoned past max_frame, the new-generation WAL frames are invisible
    // and these reads hit all-zero page images in the DB file
    // (`Corrupt: Invalid page type: 0`) or cross-linked pages.
    let verify = db.connect().unwrap();
    // Pure B-tree walk first (no overflow reads): a zeroed leaf/interior page
    // surfaces as `Corrupt: Invalid page type: 0`, the Antithesis signature.
    drain(&verify, "SELECT count(*) FROM t", "verify-count").await;
    drain(
        &verify,
        "SELECT sum(length(b)), count(*) FROM t",
        "verify-scan",
    )
    .await;
    match drain(&verify, "PRAGMA integrity_check", "verify-integrity").await {
        Some(Value::Text(s)) => {
            assert_eq!(
                s.to_lowercase(),
                "ok",
                "integrity check failed after checkpoint race: {s}"
            );
        }
        other => panic!("integrity_check returned no usable row: {other:?}"),
    }
    // A write transaction would additionally trip
    // `turso_assert!(max_frame >= nbackfills)` if the shared WAL state is
    // poisoned.
    exec(
        &verify,
        "INSERT INTO t VALUES (100, randomblob(1000))",
        "verify-write",
    )
    .await;
}

#[test]
fn shuttle_wal_checkpoint_publish_race_pct() {
    // PCT scheduler: biased toward schedules where one task (the parked
    // PASSIVE checkpoint) is starved while others run to completion — the
    // exact shape of this race. depth = number of priority-change points.
    //
    // A uniform RandomScheduler does NOT find this bug (500 iterations clean):
    // it almost never starves the checkpoint task across its last few steps
    // while the other task runs hundreds of steps. PCT models exactly that
    // "one thread paused for a long time" behavior (as does Antithesis).
    let scheduler = PctScheduler::new(3, 2000);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(checkpoint_publish_race_scenario()));
}
