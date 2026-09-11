//! WAL checkpoint action for the stress loop: pick a checkpoint mode and
//! run PRAGMA wal_checkpoint, sanity-checking the result.

use rand::Rng;
use turso::Value;
use turso_stress::ThreadId;

use crate::conn::StressConn;
use crate::opts::TxMode;
use crate::sql_logging::SqlLogger;
use crate::ThreadRng;

/// Pick a checkpoint mode: PASSIVE-heavy with occasional
/// RESTART/TRUNCATE/FULL in SQLite mode. PASSIVE checkpoints in MVCC mode
/// need the experimental passive-checkpoint flag, which this binary does
/// not enable, so pick from the other modes there.
pub fn pick_mode(rng: &mut ThreadRng, tx_mode: TxMode) -> &'static str {
    match tx_mode {
        TxMode::SQLite => {
            let pick: u32 = rng.random_range(0..100);
            if pick < 70 {
                "PASSIVE"
            } else if pick < 85 {
                "RESTART"
            } else if pick < 95 {
                "TRUNCATE"
            } else {
                "FULL"
            }
        }
        TxMode::Concurrent => match rng.random_range(0..3) {
            0 => "FULL",
            1 => "RESTART",
            _ => "TRUNCATE",
        },
    }
}

/// Run a WAL checkpoint and sanity-check the (busy, log, checkpointed)
/// result row. Lock contention is logged and skipped; corruption fails the
/// run. A checkpoint that could not run reports busy=1 with NULL counters.
pub async fn run_wal_checkpoint(
    conn: &StressConn,
    sql_logger: &SqlLogger,
    thread: &ThreadId,
    mode: &str,
    tx_mode: TxMode,
) {
    let sql = format!("PRAGMA wal_checkpoint({mode})");
    let mut rows = match conn.query(&sql, ()).await {
        Ok(rows) => rows,
        Err(e) => {
            match e {
                turso::Error::Busy(_) | turso::Error::BusySnapshot(_) => {
                    sql_logger.log(thread, &sql, "SKIPPED: database busy");
                }
                turso::Error::Corrupt(e) => {
                    turso_macros::turso_assert_unreachable!("corrupt error running checkpoint", { "thread": thread, "mode": mode, "error": e });
                }
                e => {
                    sql_logger.log(thread, &sql, &format!("ERROR: {e}"));
                }
            }
            return;
        }
    };
    match rows.next().await {
        Ok(Some(row)) => {
            let int_at = |idx: usize| match row.get_value(idx) {
                Ok(Value::Integer(v)) => Some(v),
                _ => None,
            };
            let (Some(busy), Some(log), Some(checkpointed)) = (int_at(0), int_at(1), int_at(2))
            else {
                // A checkpoint that fails inside the engine (e.g. lock
                // contention) reports busy=1 and leaves the counters NULL.
                sql_logger.log(thread, &sql, "SKIPPED: checkpoint busy");
                return;
            };
            sql_logger.log(
                thread,
                &sql,
                &format!("busy={busy} log={log} checkpointed={checkpointed}"),
            );
            turso_macros::turso_assert!(
                checkpointed <= log,
                "checkpoint never reports more frames copied than the WAL holds",
                { "thread": thread, "mode": mode, "busy": busy, "log": log, "checkpointed": checkpointed }
            );
            // In concurrent mode commits go through the MVCC logical log, so
            // the physical WAL stays empty and these counters are always
            // zero. Only expect checkpoint progress in SQLite mode.
            if tx_mode == TxMode::SQLite {
                turso_macros::turso_assert_sometimes!(
                    checkpointed > 0,
                    "a checkpoint copied frames into the database file",
                    { "mode": mode }
                );
                turso_macros::turso_assert_sometimes!(
                    log > 0 && checkpointed == log,
                    "a checkpoint drained the entire WAL",
                    { "mode": mode }
                );
            }
        }
        Ok(None) => {
            turso_macros::turso_assert_unreachable!("wal_checkpoint returned no rows", { "thread": thread, "mode": mode });
        }
        Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {
            sql_logger.log(thread, &sql, "SKIPPED: database busy");
        }
        Err(turso::Error::Corrupt(e)) => {
            turso_macros::turso_assert_unreachable!("corrupt error reading checkpoint result", { "thread": thread, "mode": mode, "error": e });
        }
        Err(e) => {
            sql_logger.log(thread, &sql, &format!("ERROR: {e}"));
        }
    }
}
