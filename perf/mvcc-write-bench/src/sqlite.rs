use rusqlite::{params, Connection};

use crate::coop::StopClock;
use crate::latency::TxnLatency;
use crate::observe::LogWatch;
use crate::run::unlink_db_tree;
use crate::spec::{BenchError, ResultRow, RunSpec, StopWhen, INSERT_SQL, PAYLOAD, SCHEMA};

pub(crate) fn run_sqlite_once(spec: &RunSpec) -> Result<ResultRow, BenchError> {
    unlink_db_tree(&spec.path)?;
    let mut conn = Connection::open(&spec.path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.busy_timeout(spec.busy_timeout)?;
    conn.execute(SCHEMA, [])?;

    let mut local: i64 = 0;
    if !matches!(spec.warmup, StopWhen::Duration(d) if d.is_zero()) {
        let warmup = StopClock::from_stop(spec.warmup);
        let mut silent = LogWatch::silent();
        drive_sqlite(&mut conn, spec, &warmup, &mut silent, &mut local)?;
    }

    let mut watch = LogWatch::open(&spec.path);
    watch.sample();
    let t0 = std::time::Instant::now();
    let measure = StopClock::from_stop(spec.stop);
    let (inserts, txns, busy, latencies_ns) =
        drive_sqlite(&mut conn, spec, &measure, &mut watch, &mut local)?;
    let elapsed = t0.elapsed();
    let (log_bytes, wal_bytes, trace) = watch.finish();
    let (engine, topology, workers, threads, checkpoint, threshold) = spec.engine.labels();
    Ok(ResultRow {
        engine,
        topology,
        workers,
        threads,
        batch_size: spec.batch_size.get(),
        checkpoint,
        threshold,
        stop: spec.stop,
        repeat: 0,
        inserts,
        committed_txns: txns,
        elapsed,
        busy,
        busy_snapshot: 0,
        schema_updated: 0,
        log_bytes,
        wal_bytes,
        log_peak_bytes: trace.peak_bytes,
        checkpoints_observed: trace.checkpoints_observed,
        sampled: trace.sampled,
        latency: TxnLatency::from_nanos(&latencies_ns),
    })
}

fn drive_sqlite(
    conn: &mut Connection,
    spec: &RunSpec,
    stop: &StopClock,
    watch: &mut LogWatch,
    local: &mut i64,
) -> Result<(u64, u64, u64, Vec<u64>), BenchError> {
    let batch = spec.batch_size.get();
    let mut insert = conn.prepare(INSERT_SQL)?;
    let mut inserts = 0u64;
    let mut txns = 0u64;
    let mut busy = 0u64;
    let mut latencies_ns = Vec::new();
    loop {
        if stop.hit() {
            break;
        }
        let txn_t0 = std::time::Instant::now();
        if let Err(err) = conn.execute("BEGIN", []) {
            if is_busy(&err) {
                busy += 1;
                continue;
            }
            return Err(err.into());
        }
        let mut tx_ok = true;
        let mut tx_inserts = 0u64;
        for _ in 0..batch {
            let pk = *local;
            *local += 1;
            match insert.execute(params![pk, PAYLOAD]) {
                Ok(_) => tx_inserts += 1,
                Err(err) if is_busy(&err) => {
                    busy += 1;
                    tx_ok = false;
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }
        if tx_ok {
            match conn.execute("COMMIT", []) {
                Ok(_) => {
                    inserts += tx_inserts;
                    txns += 1;
                    latencies_ns.push(txn_t0.elapsed().as_nanos() as u64);
                    stop.record_txns(1);
                }
                Err(err) if is_busy(&err) => {
                    busy += 1;
                    let _ = conn.execute("ROLLBACK", []);
                }
                Err(err) => return Err(err.into()),
            }
        } else {
            let _ = conn.execute("ROLLBACK", []);
        }
        watch.sample();
    }
    Ok((inserts, txns, busy, latencies_ns))
}

fn is_busy(err: &rusqlite::Error) -> bool {
    matches!(
        err.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

#[cfg(test)]
mod tests {
    use crate::spec::{Engine, RunSpec, StopWhen};
    use std::num::NonZeroUsize;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn sqlite_warmup_then_measure_does_not_reuse_primary_keys() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mvcc-write-bench-pk-{}-{nanos}.db",
            std::process::id()
        ));
        let spec = RunSpec {
            engine: Engine::Sqlite,
            batch_size: NonZeroUsize::new(10).unwrap(),
            stop: StopWhen::Duration(Duration::from_millis(80)),
            warmup: StopWhen::Duration(Duration::from_millis(80)),
            repeats: NonZeroUsize::new(1).unwrap(),
            path,
            busy_timeout: Duration::from_secs(2),
        };
        let report = crate::run(&spec).expect("UNIQUE constraint means PK reused after warmup");
        assert!(report.rows[0].inserts > 0);
    }
}
