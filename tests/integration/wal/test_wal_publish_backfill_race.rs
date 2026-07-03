use crate::queued_io::QueuedIo;
use std::sync::Arc;
use turso_core::vdbe::StepResult;
use turso_core::{Connection, Database, Result};

fn exec(conn: &Arc<Connection>, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    finish(&mut stmt)
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

/// Step a statement to completion, pumping the queued IO.
fn finish(stmt: &mut turso_core::Statement) -> Result<()> {
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 1_000_000, "statement seems stuck");
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Row | StepResult::Yield => {}
            StepResult::Done => return Ok(()),
            r => panic!("unexpected step result: {r:?}"),
        }
    }
}

/// Reproducer for the WAL bug behind the Antithesis
/// `matches!(page_type(), Ok(PageType::TableLeaf))` assert in
/// `cell_table_leaf_read_rowid` (`stress/singleton_driver_stress.sh`).
///
/// A partially backfilled checkpoint drops the checkpoint guard at the
/// WAL-level Finalize step (core/storage/wal.rs, `!should_truncate()`
/// branch) but publishes `nbackfills` only later, from the pager phases
/// `SyncDbFile -> ReadDbIdentity -> PublishBackfill`, which yield on IO in
/// between. `publish_backfill` is a blind store with no monotonicity or
/// generation check. So:
///
///   1. B parks a `SELECT` mid-scan: it holds a read mark at WAL frame M1.
///   2. A commits a few more rows (max_frame M2 > M1) and runs
///      `PRAGMA wal_checkpoint`. B's read mark caps mxSafeFrame at M1, the
///      backfill is partial, the guard is dropped early, and we park A's
///      statement at the next IO yield: `publish_backfill(M1)` is now
///      pending and A holds NO locks (the checkpoint pragma runs with
///      `TransactionMode::None`).
///   3. B finishes its scan, runs a full `PRAGMA wal_checkpoint`
///      (nbackfills == max_frame), INSERTs — the write path legally
///      RESTARTS the WAL (frames renumber from 1) — and commits more rows
///      into the new generation.
///   4. A's parked statement resumes and stores the OLD generation's
///      backfill count M1 into the NEW generation: nbackfills > max_frame.
///
/// Every later reader computes min_frame = nbackfills + 1 and finds no WAL
/// frame in range, so B's committed new-generation rows vanish; writes
/// building on the stale view then diverge from the invisible-but-durable
/// frames, and WAL recovery merges both histories into b-tree corruption —
/// which is how the stress workload trips the page-type assert.
///
/// Single-threaded and deterministic: QueuedIo defers every IO completion,
/// so each IO yield of A's checkpoint statement is an external pause point;
/// the test parks at each yield in turn until the vulnerable one is hit.
#[test]
fn wal_stale_publish_backfill_hides_committed_rows() {
    'sweep: for park in 0.. {
        let io = Arc::new(QueuedIo::new());
        let db = Database::open_file(io.clone(), "publish-backfill-race.db").unwrap();
        let a = db.connect().unwrap();
        let b = db.connect().unwrap();

        exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
        // Autocommit inserts: each is its own commit, so the WAL tip (and
        // with it the stale value the parked checkpoint will publish) sits
        // comfortably above anything B commits after the restart below.
        for i in 0..24 {
            exec(&a, &format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        // B parks a scan, pinning a read mark at the current WAL tip (M1).
        let mut reader = b.prepare("SELECT id FROM t").unwrap();
        loop {
            match reader.step().unwrap() {
                StepResult::IO => reader._io().step().unwrap(),
                StepResult::Yield => continue,
                StepResult::Row => break,
                r => panic!("unexpected step result while parking reader: {r:?}"),
            }
        }

        // Grow the WAL past B's mark so A's checkpoint is PARTIAL.
        for i in 24..28 {
            exec(&a, &format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        // Drive A's checkpoint, parking after `park` IO pump steps.
        let mut ckpt = a.prepare("PRAGMA wal_checkpoint").unwrap();
        let mut pumps = 0;
        loop {
            match ckpt.step().unwrap() {
                StepResult::IO => {
                    if pumps >= park {
                        break; // parked mid-checkpoint
                    }
                    pumps += 1;
                    ckpt._io().step().unwrap();
                }
                StepResult::Row | StepResult::Yield => {}
                // Swept past the checkpoint's last IO yield without ever
                // reproducing: the bug is fixed.
                StepResult::Done => return,
                r => panic!("unexpected step result: {r:?}"),
            }
        }

        // B: release the read mark, fully backfill, then write — which
        // restarts the WAL — and commit into the new generation.
        drop(reader);
        let ck = exec_ints(&b, "PRAGMA wal_checkpoint").unwrap();
        if ck.first().copied() != Some(0) {
            // A still holds the checkpoint guard at this parking spot.
            finish(&mut ckpt).unwrap();
            continue 'sweep;
        }
        for id in [9001, 9002, 9003] {
            exec(&b, &format!("INSERT INTO t VALUES ({id})")).unwrap();
        }

        // Resume A's parked checkpoint: it publishes the stale backfill
        // count into the restarted WAL generation.
        finish(&mut ckpt).unwrap();
        drop(ckpt);

        // Oracle: B's committed rows must be visible.
        let got = exec_ints(&b, "SELECT id FROM t WHERE id >= 9000").unwrap();
        if got == [9001, 9002, 9003] {
            continue 'sweep; // this parking spot missed the window
        }

        // Bug hit. Two more user-visible consequences:
        //  - a duplicate of an invisible-but-committed PRIMARY KEY is
        //    accepted, and its page image is built on the stale view (a
        //    leaf without 9001..9003);
        //  - reopen + WAL recovery replays both divergent histories, and
        //    the stale-view frame wins: 9002 and 9003 are permanently
        //    destroyed even though their transactions committed. With
        //    larger trees this same merge yields structural b-tree
        //    corruption — the shape behind the PageType::TableLeaf assert.
        let dup = exec(&b, "INSERT INTO t VALUES (9001)");
        drop(a);
        drop(b);
        drop(db);
        turso_core::clear_database_registry();
        let db = Database::open_file(io, "publish-backfill-race.db").unwrap();
        let c = db.connect().unwrap();
        let after_reopen = exec_ints(&c, "SELECT id FROM t WHERE id >= 9000").unwrap();

        panic!(
            "WAL generation poisoned by stale publish_backfill (checkpoint parked after \
             {park} IO pumps):\n\
             - rows committed by connection B vanished: got {got:?}, expected [9001, 9002, 9003]\n\
             - re-INSERT of invisible committed PRIMARY KEY 9001: {dup:?} (expected constraint error)\n\
             - after reopen + WAL recovery: {after_reopen:?} — committed rows 9002/9003 were \
             permanently destroyed by the stale-view write"
        );
    }
}
