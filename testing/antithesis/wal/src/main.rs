//! Antithesis workload targeting the WAL commit + checkpoint + restart machinery.
//!
//! Two copies of this binary run as separate processes (the two singleton
//! drivers) over one shared database file opened with multiprocess WAL. Each
//! process, purely through SQL, runs a continuous mix of small write
//! transactions (append a per-writer sequence row, bump a per-writer counter,
//! churn existing rows to grow the WAL, and record durable progress — all in
//! one transaction) and WAL checkpoints (mostly PASSIVE, occasionally
//! RESTART/TRUNCATE/FULL), while checking generic WAL correctness invariants
//! with Antithesis assertions. This is the workload shape that caught the
//! upstream SQLite WAL-reset bug
//! (<https://antithesis.com/blog/2026/wal-reset-bug/>).
//!
//! The lost-write oracle: every write transaction inserts row `seq` for this
//! writer and updates `progress.committed_seq = seq` in the same transaction,
//! so at any commit point `COUNT(*)` and `MAX(seq)` for this writer both equal
//! the durable watermark. A checkpoint that skips frames (the WAL-reset bug
//! class) breaks that equality long before integrity_check notices.

use antithesis_sdk::random::AntithesisRng;
use antithesis_sdk::{antithesis_init, assert_always, assert_reachable, assert_sometimes};
use clap::{Parser, Subcommand};
use rand::Rng;
use serde_json::json;
use turso::{Builder, Connection, Database, Error};

const CHURN_ROWS: i64 = 300;

#[derive(Parser)]
#[command(about = "WAL commit/checkpoint/restart stress workload for Antithesis")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the database and schema, then exit (run once before the writers).
    Init,
    /// Run the write + checkpoint + verify loop forever (one process per writer).
    Run,
    /// Final validation: integrity check plus the lost-write oracle for every writer.
    Validate,
}

fn db_path() -> String {
    std::env::var("DB_PATH").unwrap_or_else(|_| "/tmp/wal-workload.db".to_string())
}

fn writer_id() -> i64 {
    std::env::var("WRITER_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

async fn open(path: &str) -> turso::Result<(Database, Connection)> {
    let db = Builder::new_local(path)
        .experimental_multiprocess_wal(true)
        .build()
        .await?;
    let conn = db.connect()?;
    conn.execute("PRAGMA busy_timeout = 2000", ()).await?;
    Ok((db, conn))
}

fn is_busy(err: &Error) -> bool {
    matches!(err, Error::Busy(_) | Error::BusySnapshot(_))
}

fn looks_corrupt(err: &Error) -> bool {
    err.to_string().to_lowercase().contains("corrupt")
}

/// Run a single-value query. Returns None if the query failed with a
/// transient (busy) error or returned no rows.
async fn query_i64(conn: &Connection, sql: &str) -> turso::Result<Option<i64>> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(e) if is_busy(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    match rows.next().await {
        Ok(Some(row)) => Ok(Some(row.get::<i64>(0)?)),
        Ok(None) => Ok(None),
        Err(e) if is_busy(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// PRAGMA integrity_check: Some(true) iff it ran and returned exactly one row "ok".
async fn integrity_ok(conn: &Connection) -> turso::Result<Option<bool>> {
    let mut rows = match conn.query("PRAGMA integrity_check", ()).await {
        Ok(rows) => rows,
        Err(e) if is_busy(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut results = Vec::new();
    loop {
        match rows.next().await {
            Ok(Some(row)) => results.push(row.get::<String>(0)?),
            Ok(None) => break,
            Err(e) if is_busy(&e) => return Ok(None),
            Err(e) => return Err(e),
        }
    }
    Ok(Some(results.len() == 1 && results[0] == "ok"))
}

/// Run a checkpoint and read back the (busy, log, checkpointed) row.
/// Returns None when the checkpoint could not run due to lock contention.
/// A busy checkpoint reports (1, NULL, NULL); treat that as contention too.
async fn checkpoint(conn: &Connection, mode: &str) -> turso::Result<Option<(i64, i64, i64)>> {
    let sql = format!("PRAGMA wal_checkpoint({mode})");
    let mut rows = match conn.query(&sql, ()).await {
        Ok(rows) => rows,
        Err(e) if is_busy(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    match rows.next().await {
        Ok(Some(row)) => {
            let cols: Vec<Option<i64>> = (0..3)
                .map(|i| match row.get_value(i) {
                    Ok(turso::Value::Integer(v)) => Some(v),
                    _ => None,
                })
                .collect();
            match (cols[0], cols[1], cols[2]) {
                (Some(busy), Some(log), Some(checkpointed)) => Ok(Some((busy, log, checkpointed))),
                _ => Ok(None),
            }
        }
        Ok(None) => Ok(None),
        Err(e) if is_busy(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn do_init(path: &str) -> turso::Result<()> {
    let (_db, conn) = open(path).await?;
    conn.execute_batch(
        "BEGIN;
         CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, writer INTEGER, seq INTEGER, blob BLOB);
         CREATE INDEX IF NOT EXISTS t_ws ON t(writer, seq);
         CREATE TABLE IF NOT EXISTS ctr(writer INTEGER PRIMARY KEY, v INTEGER);
         CREATE TABLE IF NOT EXISTS progress(writer INTEGER PRIMARY KEY, committed_seq INTEGER, v INTEGER);
         CREATE TABLE IF NOT EXISTS churn(id INTEGER PRIMARY KEY, blob BLOB);
         INSERT OR IGNORE INTO ctr VALUES (0, 0), (1, 0);
         INSERT OR IGNORE INTO progress VALUES (0, 0, 0), (1, 0, 0);
         COMMIT;",
    )
    .await?;
    // Seed churn rows so writers have existing pages to dirty.
    conn.execute("BEGIN", ()).await?;
    for _ in 0..CHURN_ROWS {
        conn.execute("INSERT INTO churn(blob) VALUES (randomblob(400))", ())
            .await?;
    }
    conn.execute("COMMIT", ()).await?;
    // Fully checkpoint the initial WAL so the writers start from a drained WAL.
    checkpoint(&conn, "TRUNCATE").await?;
    eprintln!("init: created WAL database at {path}");
    Ok(())
}

/// Belt and suspenders on top of the composer's setup ordering: wait until
/// the init process has created the schema.
async fn wait_for_schema(conn: &Connection) {
    for _ in 0..240 {
        let n = query_i64(
            conn,
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'progress'",
        )
        .await;
        if matches!(n, Ok(Some(1))) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    eprintln!("warning: schema not present after wait; proceeding anyway");
}

/// One write transaction. Returns Ok(true) on commit, Ok(false) when it was
/// rolled back due to lock contention.
async fn write_txn(
    conn: &Connection,
    w: i64,
    seq: i64,
    v: i64,
    rng: &mut AntithesisRng,
) -> turso::Result<bool> {
    match conn.execute("BEGIN IMMEDIATE", ()).await {
        Ok(_) => {}
        Err(e) if is_busy(&e) => return Ok(false),
        Err(e) => return Err(e),
    }
    let blob_sz: i64 = rng.random_range(64..1664);
    let churn_sz: i64 = rng.random_range(64..1664);
    let c1: i64 = rng.random_range(1..=CHURN_ROWS);
    let c2: i64 = rng.random_range(1..=CHURN_ROWS);
    let c3: i64 = rng.random_range(1..=CHURN_ROWS);
    let statements = [
        format!("INSERT INTO t(writer, seq, blob) VALUES ({w}, {seq}, randomblob({blob_sz}))"),
        format!("UPDATE ctr SET v = {v} WHERE writer = {w}"),
        format!("UPDATE churn SET blob = randomblob({churn_sz}) WHERE id IN ({c1}, {c2}, {c3})"),
        format!("UPDATE progress SET committed_seq = {seq}, v = {v} WHERE writer = {w}"),
    ];
    for sql in &statements {
        if let Err(e) = conn.execute(sql, ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            if is_busy(&e) {
                return Ok(false);
            }
            return Err(e);
        }
    }
    match conn.execute("COMMIT", ()).await {
        Ok(_) => Ok(true),
        Err(e) => {
            let _ = conn.execute("ROLLBACK", ()).await;
            if is_busy(&e) {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

async fn do_run(path: &str, w: i64) -> turso::Result<()> {
    let (_db, conn) = open(path).await?;
    wait_for_schema(&conn).await;
    assert_reachable!("workload: writer process started", &json!({ "writer": w }));

    // Restart/recovery check: because data and watermark commit atomically,
    // the committed row count and max seq must exactly equal the durable
    // watermark whenever this process starts (first run or post-crash).
    let p_seq = query_i64(
        &conn,
        &format!("SELECT committed_seq FROM progress WHERE writer = {w}"),
    )
    .await?
    .unwrap_or(0);
    let p_v = query_i64(&conn, &format!("SELECT v FROM progress WHERE writer = {w}"))
        .await?
        .unwrap_or(0);
    let cnt = query_i64(&conn, &format!("SELECT count(*) FROM t WHERE writer = {w}")).await?;
    let mx = query_i64(
        &conn,
        &format!("SELECT coalesce(max(seq), 0) FROM t WHERE writer = {w}"),
    )
    .await?;
    let ctrv = query_i64(&conn, &format!("SELECT v FROM ctr WHERE writer = {w}")).await?;
    assert_always!(
        cnt == Some(p_seq) && mx == Some(p_seq) && ctrv == Some(p_v),
        "recovery-preserves-committed: committed state exactly matches the durable watermark at startup",
        &json!({ "writer": w, "count": cnt, "max_seq": mx, "ctr": ctrv, "progress_seq": p_seq, "progress_v": p_v })
    );

    let mut committed_seq = p_seq;
    let mut last_v = p_v;
    let mut rng = AntithesisRng;

    loop {
        let action: u32 = rng.random_range(0..100);
        if action < 70 {
            // Write transaction.
            let seq = committed_seq + 1;
            let v = last_v + 1;
            match write_txn(&conn, w, seq, v, &mut rng).await {
                Ok(true) => {
                    committed_seq = seq;
                    last_v = v;
                    // Our own commit must be immediately visible. A stale
                    // backfill count makes new-generation WAL frames invisible,
                    // so this is the first assertion the WAL-reset bug trips.
                    let got = query_i64(
                        &conn,
                        &format!("SELECT seq FROM t WHERE writer = {w} AND seq = {seq}"),
                    )
                    .await
                    .ok()
                    .flatten();
                    assert_always!(
                        got == Some(seq),
                        "read-your-writes: a committed row is immediately visible to its writer",
                        &json!({ "writer": w, "seq": seq, "got": got })
                    );
                }
                Ok(false) => {
                    assert_reachable!(
                        "contention: a write transaction backed off on busy",
                        &json!({ "writer": w })
                    );
                }
                Err(e) => {
                    assert_always!(
                        !looks_corrupt(&e),
                        "no-corruption: a write transaction never reports corruption",
                        &json!({ "writer": w, "error": e.to_string() })
                    );
                    eprintln!("write error (writer {w}): {e}");
                }
            }
        } else if action < 90 {
            // Checkpoint. PASSIVE most of the time so writers can run
            // concurrently with the backfill; RESTART/TRUNCATE regularly force
            // WAL resets so writer-side restarts collide with checkpoints.
            let pick: u32 = rng.random_range(0..100);
            let mode = if pick < 70 {
                "PASSIVE"
            } else if pick < 85 {
                "RESTART"
            } else if pick < 95 {
                "TRUNCATE"
            } else {
                "FULL"
            };
            match checkpoint(&conn, mode).await {
                Ok(Some((busy, log, checkpointed))) => {
                    let details = json!({
                        "writer": w, "mode": mode,
                        "busy": busy, "log": log, "checkpointed": checkpointed,
                    });
                    assert_always!(
                        checkpointed <= log,
                        "checkpoint-result-sane: a checkpoint never reports more frames copied than the WAL holds",
                        &details
                    );
                    assert_sometimes!(
                        checkpointed > 0,
                        "checkpoint-backfills-frames: a checkpoint copied at least one frame into the db file",
                        &details
                    );
                    assert_sometimes!(
                        log > 0 && checkpointed == log,
                        "wal-fully-drained: a checkpoint backfilled the entire WAL, arming a WAL restart",
                        &details
                    );
                    if mode == "RESTART" || mode == "TRUNCATE" {
                        assert_sometimes!(
                            busy == 0,
                            "wal-restarted: a RESTART/TRUNCATE checkpoint reset the WAL",
                            &details
                        );
                    }
                    if mode == "PASSIVE" {
                        assert_sometimes!(
                            busy != 0 || (log > 0 && checkpointed < log),
                            "passive-checkpoint-backed-off: a PASSIVE checkpoint yielded under contention",
                            &details
                        );
                    }
                }
                Ok(None) => {
                    assert_reachable!(
                        "contention: a checkpoint backed off on busy",
                        &json!({ "writer": w, "mode": mode })
                    );
                }
                Err(e) => {
                    assert_always!(
                        !looks_corrupt(&e),
                        "no-corruption: a checkpoint never reports corruption",
                        &json!({ "writer": w, "mode": mode, "error": e.to_string() })
                    );
                    eprintln!("checkpoint error (writer {w}, {mode}): {e}");
                }
            }
        } else {
            // Correctness sweep: the lost-write oracle.
            let cnt =
                query_i64(&conn, &format!("SELECT count(*) FROM t WHERE writer = {w}")).await?;
            let mx = query_i64(
                &conn,
                &format!("SELECT coalesce(max(seq), 0) FROM t WHERE writer = {w}"),
            )
            .await?;
            if let (Some(cnt), Some(mx)) = (cnt, mx) {
                assert_always!(
                    cnt == committed_seq && mx == committed_seq,
                    "no-lost-committed-writes: every committed row for this writer is present with no gaps",
                    &json!({ "writer": w, "count": cnt, "max_seq": mx, "committed_seq": committed_seq })
                );
            }
            if let Some(ctrv) =
                query_i64(&conn, &format!("SELECT v FROM ctr WHERE writer = {w}")).await?
            {
                assert_always!(
                    ctrv == last_v,
                    "committed-counter-intact: this writer's committed counter matches its last commit",
                    &json!({ "writer": w, "ctr": ctrv, "last_v": last_v })
                );
            }
            // integrity_check is expensive; run it on a fraction of sweeps.
            if rng.random_range(0..4) == 0 {
                if let Some(ok) = integrity_ok(&conn).await? {
                    assert_always!(
                        ok,
                        "integrity-check-clean: PRAGMA integrity_check returns ok",
                        &json!({ "writer": w })
                    );
                }
            }
        }
    }
}

async fn do_validate(path: &str) -> turso::Result<()> {
    let (_db, conn) = open(path).await?;
    if let Some(ok) = integrity_ok(&conn).await? {
        assert_always!(ok, "finally: PRAGMA integrity_check returns ok", &json!({}));
    }
    let mut writers = Vec::new();
    let mut rows = conn
        .query("SELECT writer, committed_seq FROM progress", ())
        .await?;
    while let Some(row) = rows.next().await? {
        writers.push((row.get::<i64>(0)?, row.get::<i64>(1)?));
    }
    for (w, p_seq) in writers {
        let cnt = query_i64(&conn, &format!("SELECT count(*) FROM t WHERE writer = {w}")).await?;
        let mx = query_i64(
            &conn,
            &format!("SELECT coalesce(max(seq), 0) FROM t WHERE writer = {w}"),
        )
        .await?;
        assert_always!(
            cnt == Some(p_seq) && mx == Some(p_seq),
            "finally: no-lost-committed-writes for every writer",
            &json!({ "writer": w, "count": cnt, "max_seq": mx, "progress_seq": p_seq })
        );
        eprintln!("validate: writer {w}: count={cnt:?} max={mx:?} watermark={p_seq}");
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    antithesis_init();
    let cli = Cli::parse();
    let path = db_path();
    let result = match cli.cmd {
        Cmd::Init => do_init(&path).await,
        Cmd::Run => do_run(&path, writer_id()).await,
        Cmd::Validate => do_validate(&path).await,
    };
    if let Err(e) = result {
        // Surface corruption-shaped fatal errors as a property violation, not
        // just a process exit, so Antithesis triage names the failed property.
        assert_always!(
            !looks_corrupt(&e),
            "no-corruption: workload operations never report corruption",
            &json!({ "error": e.to_string() })
        );
        eprintln!("FATAL: {e}");
        std::process::exit(1);
    }
}
