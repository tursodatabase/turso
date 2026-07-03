use crate::queued_io::QueuedIo;
use std::sync::Arc;
use turso_core::vdbe::StepResult;
use turso_core::{Connection, Database, Result};

/// Step a statement to completion, pumping the queued IO.
fn step_blocking(stmt: &mut turso_core::Statement) -> Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => continue,
            other => return Ok(other),
        }
    }
}

fn exec_ints(conn: &Arc<Connection>, sql: &str) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(sql)?;
    let mut out = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => continue,
            StepResult::Row => {
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    if let turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) = value {
                        out.push(*i);
                    }
                }
            }
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
    Ok(out)
}

fn exec(conn: &Arc<Connection>, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    match step_blocking(&mut stmt)? {
        StepResult::Done | StepResult::Row => Ok(()),
        r => panic!("unexpected step result for `{sql}`: {r:?}"),
    }
}

/// Rows in the first committed batch; enough that the auto-checkpoint
/// threshold (1000 frames) is crossed during the second batch's commit.
const BATCH1: usize = 500;
const BATCH2: usize = 600;

struct Setup {
    _io: Arc<QueuedIo>,
    db: Arc<Database>,
    a: Arc<Connection>,
    b: Arc<Connection>,
    /// A's parked COMMIT statement (auto-checkpoint in progress), plus the
    /// number of IO pumps consumed before parking.
    commit_stmt: turso_core::Statement,
    /// B's parked SELECT statement holding a read mark, if still open.
    reader_stmt: Option<turso_core::Statement>,
    a_done: bool,
}

/// Build the scenario and park A's COMMIT after `park` IO pumps.
///
/// Timeline reproduced (all single-threaded, deterministic):
///   1. A commits BATCH1 rows (WAL grows, no checkpoint yet).
///   2. B parks a `SELECT` mid-scan: it holds a read mark at the current
///      WAL frame count M1.
///   3. A runs one big transaction of BATCH2 rows. Its COMMIT crosses the
///      auto-checkpoint threshold. commit_tx releases A's read+write locks
///      BEFORE running the checkpoint (pager.rs commit_tx), and because B's
///      read mark caps mxSafeFrame at M1 < max, the backfill is PARTIAL, so
///      the WAL layer drops the checkpoint guard at its Finalize step while
///      the pager still has SyncDbFile/ReadDbIdentity/PublishBackfill(M1)
///      pending. Parking the COMMIT statement in that window leaves the
///      checkpoint suspended with a pending stale `publish_backfill(M1)`
///      and NO locks held.
fn setup(park: usize) -> Setup {
    let io = Arc::new(QueuedIo::new());
    let db = Database::open_file(io.clone(), "publish-backfill-race.db").unwrap();
    let a = db.connect().unwrap();
    let b = db.connect().unwrap();

    exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH1 {
        exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
    }
    exec(&a, "COMMIT").unwrap();

    // B parks a scan, pinning a read mark at the current WAL tip.
    let mut reader = b.prepare("SELECT id FROM t").unwrap();
    loop {
        match reader.step().unwrap() {
            StepResult::IO => reader._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => break,
            r => panic!("unexpected step result while parking reader: {r:?}"),
        }
    }

    // A's big transaction; its COMMIT triggers the auto-checkpoint.
    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH2 {
        exec(
            &a,
            &format!("INSERT INTO t VALUES ({}, zeroblob(3000))", BATCH1 + i),
        )
        .unwrap();
    }
    let mut commit_stmt = a.prepare("COMMIT").unwrap();
    let mut pumps = 0;
    let mut a_done = false;
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 10_000_000, "commit statement seems stuck");
        match commit_stmt.step().unwrap() {
            StepResult::IO => {
                if pumps >= park {
                    break;
                }
                pumps += 1;
                commit_stmt._io().step().unwrap();
            }
            StepResult::Row => {}
            StepResult::Yield => {}
            StepResult::Done => {
                a_done = true;
                break;
            }
            r => panic!("unexpected step result: {r:?}"),
        }
    }

    Setup {
        _io: io,
        db,
        a,
        b,
        commit_stmt,
        reader_stmt: Some(reader),
        a_done,
    }
}

fn finish(stmt: &mut turso_core::Statement) {
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 10_000_000, "statement seems stuck");
        match stmt.step().unwrap() {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Row => {}
            StepResult::Yield => {}
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
}

/// Reproducer: a parked auto-checkpoint publishes a stale backfill count
/// into a restarted WAL generation, hiding committed rows (and, once later
/// writers build on the stale view and the WAL is recovered, corrupting the
/// B-tree — the `PageType::TableLeaf` assert in `cell_table_leaf_read_rowid`
/// seen on Antithesis under `turso_stress`).
///
/// See `setup` for the parked state. While A's COMMIT statement is parked
/// inside its auto-checkpoint tail (no locks held, `publish_backfill(M1)`
/// pending), B:
///   1. finishes its parked scan (releases the read mark),
///   2. runs `PRAGMA wal_checkpoint` — full backfill, publishes
///      nbackfills == max_frame,
///   3. INSERTs — the write path restarts the WAL (legal: fully
///      backfilled, no locks held); frames renumber from 1,
///   4. INSERTs more rows — new-generation commits.
/// Then A's COMMIT is resumed: its checkpoint blindly stores the OLD
/// generation's backfill count M1 into the NEW generation's `nbackfills`
/// (`publish_backfill` has no monotonicity/generation guard), leaving
/// nbackfills >> max_frame. Every later reader computes
/// min_frame = nbackfills+1 and finds no WAL frame, so B's committed
/// new-generation rows vanish.
///
/// The parking spot is swept from the end of the COMMIT's IO yields.
#[test]
fn wal_stale_publish_backfill_hides_committed_rows() {
    // Count yields with an explicit dry run.
    let total_yields = {
        let io = Arc::new(QueuedIo::new());
        let db = Database::open_file(io.clone(), "publish-backfill-race-dry.db").unwrap();
        let a = db.connect().unwrap();
        let b = db.connect().unwrap();
        exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
        exec(&a, "BEGIN").unwrap();
        for i in 0..BATCH1 {
            exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
        }
        exec(&a, "COMMIT").unwrap();
        let mut reader = b.prepare("SELECT id FROM t").unwrap();
        loop {
            match reader.step().unwrap() {
                StepResult::IO => reader._io().step().unwrap(),
                StepResult::Yield => continue,
                StepResult::Row => break,
                r => panic!("unexpected: {r:?}"),
            }
        }
        exec(&a, "BEGIN").unwrap();
        for i in 0..BATCH2 {
            exec(
                &a,
                &format!("INSERT INTO t VALUES ({}, zeroblob(3000))", BATCH1 + i),
            )
            .unwrap();
        }
        let mut commit_stmt = a.prepare("COMMIT").unwrap();
        let mut pumps = 0usize;
        loop {
            match commit_stmt.step().unwrap() {
                StepResult::IO => {
                    pumps += 1;
                    commit_stmt._io().step().unwrap();
                }
                StepResult::Row | StepResult::Yield => {}
                StepResult::Done => break,
                r => panic!("unexpected: {r:?}"),
            }
        }
        pumps
    };
    // Sweep the last few parking spots (the checkpoint tail).
    let from = total_yields.saturating_sub(8);
    for park in (from..total_yields).rev() {
        let mut s = setup(park);
        if s.a_done {
            continue;
        }

        // B releases its read mark.
        drop(s.reader_stmt.take());

        // B: full checkpoint, then writes that restart the WAL and commit
        // into the new generation.
        let ck = exec_ints(&s.b, "PRAGMA wal_checkpoint").unwrap();
        if ck.first().copied() != Some(0) {
            // A still holds the checkpoint guard at this parking spot.
            finish(&mut s.commit_stmt);
            continue;
        }
        exec(&s.b, "INSERT INTO t VALUES (1000001, zeroblob(3000))").unwrap();
        exec(&s.b, "INSERT INTO t VALUES (1000002, zeroblob(3000))").unwrap();
        exec(&s.b, "INSERT INTO t VALUES (1000003, zeroblob(3000))").unwrap();

        // Resume A's parked COMMIT: its checkpoint publishes the stale
        // backfill count into the restarted WAL generation.
        finish(&mut s.commit_stmt);

        // Oracle 1: B's committed rows must be visible.
        let got = exec_ints(&s.b, "SELECT id FROM t WHERE id >= 1000000 ORDER BY id").unwrap();
        let visible = got == vec![1000001, 1000002, 1000003];
        if visible {
            // This parking spot did not hit the window; try the next one.
            continue;
        }

        // Bug detected: B's committed rows are gone. Demonstrate the
        // follow-on corruption that surfaces as the
        // `matches!(page_type(), Ok(PageType::TableLeaf))` assert /
        // integrity check failures in the stress runs: keep writing on the
        // stale view (a duplicate of an invisible-but-committed PRIMARY KEY
        // succeeds!), then reopen so WAL recovery replays both divergent
        // histories.
        let dup = exec(&s.b, "INSERT INTO t VALUES (1000001, zeroblob(10))");
        let integrity_after_reopen = {
            let Setup {
                _io: io,
                db,
                a,
                b,
                commit_stmt,
                reader_stmt,
                ..
            } = s;
            drop(commit_stmt);
            drop(reader_stmt);
            drop(a);
            drop(b);
            drop(db);
            turso_core::clear_database_registry();
            let db2 = Database::open_file(io, "publish-backfill-race.db").unwrap();
            let c = db2.connect().unwrap();
            let mut stmt = c.prepare("PRAGMA integrity_check").unwrap();
            let mut msgs = Vec::new();
            loop {
                match stmt.step().unwrap() {
                    StepResult::IO => stmt._io().step().unwrap(),
                    StepResult::Yield => continue,
                    StepResult::Row => {
                        let row = stmt.row().unwrap();
                        msgs.push(format!("{:?}", row.get_values().collect::<Vec<_>>()));
                    }
                    StepResult::Done => break,
                    r => panic!("unexpected: {r:?}"),
                }
            }
            msgs
        };

        panic!(
            "WAL generation poisoned by stale publish_backfill (COMMIT parked after \
             {park}/{total_yields} IO pumps):\n\
             - rows committed by connection B vanished: got {got:?}, expected [1000001, 1000002, 1000003]\n\
             - re-INSERT of invisible committed PRIMARY KEY 1000001: {dup:?} (should be constraint error)\n\
             - integrity_check after reopen+recovery: {integrity_after_reopen:?}"
        );
    }
}
