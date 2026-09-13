//! Regression for the SQLite WAL-reset / Tailscale bug class:
//! a checkpointer must not use pre-lock `nbackfills` as `min_frame` after a
//! concurrent full checkpoint + WAL restart rewrites the generation.

use crate::common::TempDatabase;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use turso_core::vdbe::StepResult;
use turso_core::{set_checkpoint_start_before_lock_hook, Connection, Result};

fn step_blocking(stmt: &mut turso_core::Statement) -> Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => continue,
            other => return Ok(other),
        }
    }
}

fn exec(conn: &Arc<Connection>, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    match step_blocking(&mut stmt)? {
        StepResult::Done | StepResult::Row => Ok(()),
        r => panic!("unexpected step result for `{sql}`: {r:?}"),
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

fn assert_integrity_ok(conn: &Arc<Connection>, context: &str) {
    let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => {
                rows.push(stmt.row().unwrap().get::<String>(0).unwrap());
            }
            StepResult::Done => break,
            r => panic!("unexpected integrity_check step: {r:?}"),
        }
    }
    assert_eq!(
        rows,
        vec!["ok".to_string()],
        "integrity_check failed {context}"
    );
}

/// Without post-lock re-sample of `nbackfills`, checkpoint A can publish a
/// watermark that skips low frames of a restarted WAL generation.
///
/// Proof shape (must fail without the Start-path re-sample):
/// - Partial backfill leaves stale floor `P = nbackfills > 0`.
/// - Hook restarts WAL and writes a new generation with frame count `N > P`.
/// - Stale `min_frame = P+1` would skip frames `[1, P]` of the new gen and can
///   still publish `nbackfills = N` (seq check alone does not catch this once
///   the post-lock snapshot is already the new generation).
/// - With the fix, range is rebased to `[1, N']` and no rows are lost.
#[test]
fn wal_reset_stale_prelock_nbackfills_does_not_lose_frames() {
    let tmp = TempDatabase::new("wal-reset-stale-nbackfills.db");
    let a = tmp.connect_limbo();
    let b = tmp.connect_limbo();
    let c = tmp.connect_limbo();

    exec(&a, "PRAGMA journal_mode=WAL").unwrap();
    exec(&a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();

    // Batch 1: enough pages that a pinned reader leaves a partial backfill window.
    const BATCH1: i64 = 80;
    const BATCH2: i64 = 120;

    exec(&a, "BEGIN").unwrap();
    for i in 0..BATCH1 {
        exec(&a, &format!("INSERT INTO t VALUES ({i}, zeroblob(3000))")).unwrap();
    }
    exec(&a, "COMMIT").unwrap();

    // Pin a read mark so a passive checkpoint cannot backfill everything.
    let mut reader = b.prepare("SELECT id FROM t").unwrap();
    loop {
        match reader.step().unwrap() {
            StepResult::IO => reader._io().step().unwrap(),
            StepResult::Yield => continue,
            StepResult::Row => break,
            r => panic!("unexpected while parking reader: {r:?}"),
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
    exec(&a, "COMMIT").unwrap();

    // Partial backfill under the pinned reader. `ckpt_frames` is the stale floor P.
    let partial = exec_ints(&c, "PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_eq!(
        partial.first().copied(),
        Some(0),
        "partial checkpoint busy flag: {partial:?}"
    );
    let log_frames = partial.get(1).copied().unwrap_or(0);
    let ckpt_frames = partial.get(2).copied().unwrap_or(0);
    assert!(
        ckpt_frames > 0 && ckpt_frames < log_frames,
        "need partial backfill for stale-nbackfills race: log={log_frames} ckpt={ckpt_frames} raw={partial:?}"
    );

    // New-gen frame count must exceed stale floor P. With ~1 page/row, rows = P + margin
    // ensures a poisoned min_frame=P+1 still has frames to "checkpoint" and can hide data.
    let new_gen_rows = ckpt_frames + 80;
    assert!(
        new_gen_rows > ckpt_frames,
        "new generation must outgrow stale floor P={ckpt_frames}"
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_flag = hook_ran.clone();
    let writer = c.clone();
    let mut parked_reader = Some(reader);

    set_checkpoint_start_before_lock_hook(Some(Box::new(move || {
        // Release the pin so RESTART can finish a full checkpoint and reset the WAL.
        drop(parked_reader.take());

        let full =
            exec_ints(&writer, "PRAGMA wal_checkpoint(RESTART)").expect("restart checkpoint");
        assert_eq!(
            full.first().copied(),
            Some(0),
            "RESTART checkpoint must succeed inside hook: {full:?}"
        );

        // New generation large enough that stale min_frame=P+1 would skip early frames.
        // Do not checkpoint here: that would backfill the new gen before A takes the lock
        // and collapse the race window.
        exec(&writer, "BEGIN").unwrap();
        for i in 0..new_gen_rows {
            let id = 1_000_000 + i;
            exec(
                &writer,
                &format!("INSERT INTO t VALUES ({id}, zeroblob(3000))"),
            )
            .unwrap();
        }
        exec(&writer, "COMMIT").unwrap();
        hook_ran_flag.store(true, Ordering::Release);
    })));

    // Checkpoint A samples the partial snapshot, then the hook restarts the WAL.
    let after = exec_ints(&a, "PRAGMA wal_checkpoint(PASSIVE)");
    set_checkpoint_start_before_lock_hook(None);

    assert!(
        hook_ran.load(Ordering::Acquire),
        "pre-lock hook must run (checkpoint A should have seen work before the race window)"
    );
    let after = after.expect("checkpoint A after WAL-reset race");
    assert_eq!(
        after.first().copied(),
        Some(0),
        "checkpoint A must not fail after re-basing on the new generation: {after:?}"
    );

    // Checkpoint result is (busy, log, checkpointed). After a correct re-base onto the
    // new generation, checkpointed is a prefix of that generation's log — never larger.
    let after_log = after.get(1).copied().unwrap_or(0);
    let after_ckpt = after.get(2).copied().unwrap_or(0);
    assert!(
        after_ckpt <= after_log,
        "watermark cannot exceed WAL log: log={after_log} ckpt={after_ckpt} P={ckpt_frames} raw={after:?}"
    );
    // With ~1 frame per large blob row, new-gen log should exceed stale floor P. If the
    // log shrank below P the stale min_frame path does no work and the test can false-pass.
    assert!(
        after_log > ckpt_frames,
        "post-race WAL log ({after_log}) must exceed stale floor P={ckpt_frames} so a poisoned \
         min_frame=P+1 would still attempt a partial new-gen backfill: {after:?}"
    );

    let expected_total = BATCH1 + BATCH2 + new_gen_rows;
    let count = exec_ints(&a, "SELECT count(*) FROM t").unwrap();
    assert_eq!(
        count,
        vec![expected_total],
        "all committed rows must survive the WAL-reset race (P={ckpt_frames}, new_gen={new_gen_rows}, \
         after={after:?})"
    );

    let new_gen_count = exec_ints(
        &a,
        &format!(
            "SELECT count(*) FROM t WHERE id >= 1000000 AND id < {}",
            1_000_000 + new_gen_rows
        ),
    )
    .unwrap();
    assert_eq!(
        new_gen_count,
        vec![new_gen_rows],
        "new-generation rows must all be visible"
    );

    // Pre-reset rows were fully checkpointed by RESTART inside the hook.
    let low_id = exec_ints(&a, "SELECT count(*) FROM t WHERE id < 1000000").unwrap();
    assert_eq!(low_id, vec![BATCH1 + BATCH2], "pre-reset rows must remain");

    assert_integrity_ok(&a, "after WAL-reset stale-nbackfills race");

    // Force recovery path: close and reopen.
    drop(a);
    drop(b);
    drop(c);
    let a2 = tmp.connect_limbo();
    let count2 = exec_ints(&a2, "SELECT count(*) FROM t").unwrap();
    assert_eq!(
        count2,
        vec![expected_total],
        "row count must survive reopen/recovery"
    );
    assert_integrity_ok(&a2, "after reopen following WAL-reset race");
}
