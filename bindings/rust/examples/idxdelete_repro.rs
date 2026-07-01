use std::{env, fs, path::PathBuf, sync::Arc, time::Duration};

use tokio::time::Instant;
use turso::Builder;

const TARGET_SQL: &str = "DELETE FROM dry_wall_615 WHERE new_road_959 = 322";

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn delay_values() -> Vec<u64> {
    env::var("DELAY_US").ok().map(|value| {
        value.split(',').map(|part| part.trim().parse::<u64>().unwrap()).collect()
    }).unwrap_or_else(|| vec![0, 20_000, 40_000, 60_000, 80_000, 100_000])
}

fn clean_path(path: &str) {
    let _ = fs::remove_file(path);
    for suffix in ["-wal", "-shm", "-log", ".db-log"] {
        let _ = fs::remove_file(format!("{path}{suffix}"));
    }
}

async fn exec(conn: &turso::Connection, label: &str, sql: impl AsRef<str>) -> Result<u64, String> {
    let sql = sql.as_ref();
    conn.execute(sql, ()).await.map_err(|err| format!("{label}: {sql}: {err:?}"))
}

async fn exec_allow(conn: &turso::Connection, label: &str, sql: impl AsRef<str>) -> String {
    match exec(conn, label, sql.as_ref()).await {
        Ok(changes) => format!("OK({changes})"),
        Err(err) => err,
    }
}

async fn drain_rows(conn: &turso::Connection, label: &str, sql: impl AsRef<str>) -> Result<(), String> {
    let sql = sql.as_ref();
    let mut rows = conn.query(sql, ()).await.map_err(|err| format!("{label}: {sql}: {err:?}"))?;
    loop {
        match rows.next().await {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(err) => return Err(format!("{label}: {sql}: {err:?}")),
        }
    }
}

fn is_target_error(message: &str) -> bool {
    message.contains("IdxDelete") || message.contains("no matching index entry")
}

async fn open_db(path: &str) -> turso::Database {
    clean_path(path);
    let builder = Builder::new_local(path);
    builder.build().await.unwrap()
}

async fn setup(path: &str, filler_rows: usize, base_payload: i64, checkpoint_threshold: i64) -> turso::Database {
    let db = open_db(path).await;
    let conn = db.connect().unwrap();

    exec(&conn, "setup", "PRAGMA page_size = 512").await.unwrap();
    drain_rows(&conn, "setup", "PRAGMA journal_mode = 'mvcc'").await.unwrap();
    exec(&conn, "setup", "PRAGMA data_sync_retry = 1").await.unwrap();
    exec(&conn, "setup", format!("PRAGMA mvcc_checkpoint_threshold = {checkpoint_threshold}")).await.unwrap();
    exec(&conn, "setup",
        "CREATE TABLE dry_wall_615 (
            sweet_flower_317 NUMERIC NOT NULL,
            fresh_floor_256 REAL UNIQUE,
            wet_rain_327 BLOB NOT NULL,
            new_road_959 INTEGER NOT NULL PRIMARY KEY,
            full_root_761 NUMERIC,
            hot_desk_383 TEXT UNIQUE,
            new_wall_541 REAL
        )").await.unwrap();

    exec(&conn, "setup", "BEGIN CONCURRENT").await.unwrap();
    exec(&conn, "setup", "INSERT INTO dry_wall_615 VALUES(784, 9.99, zeroblob(8192), 322, 627, 'small_leaf_292', 2.24)").await.unwrap();
    exec(&conn, "setup", "INSERT INTO dry_wall_615 VALUES(440, 8.25, zeroblob(8192), 502, 962, 'fast_sun_915', 3.31)").await.unwrap();
    for id in 1..=filler_rows {
        let rowid = 10_000 + id as i64;
        exec(&conn, "setup", format!(
            "INSERT INTO dry_wall_615 VALUES({}, {}, zeroblob({}), {}, {}, 'seed_{}', {})",
            10_000 + id as i64, 100_000.0 + id as f64, base_payload, rowid, 20_000 + id as i64, rowid, (id % 97) as f64 + 0.01
        )).await.unwrap();
    }
    exec(&conn, "setup", "COMMIT").await.unwrap();

    let _ = drain_rows(&conn, "setup", "PRAGMA wal_checkpoint(TRUNCATE)").await;
    db
}

async fn run_attempt(attempt: usize, delay_us: u64, filler_rows: usize, base_payload: i64, stale_payload: i64, checkpoint_threshold: i64) -> bool {
    let mut path = PathBuf::from(env::temp_dir());
    path.push(format!("turso-idxdelete-speculative-abort-{attempt}-{delay_us}.db"));
    let path = path.to_string_lossy().to_string();
    let db = Arc::new(setup(&path, filler_rows, base_payload, checkpoint_threshold).await);

    let old = Arc::new(db.connect().unwrap());
    let deleter = db.connect().unwrap();
    let victim = db.connect().unwrap();

    exec(&old, "old", "PRAGMA data_sync_retry = 1").await.unwrap();
    exec(&victim, "victim", "PRAGMA data_sync_retry = 1").await.unwrap();
    exec(&old, "old", "BEGIN CONCURRENT").await.unwrap();

    let warmup = drain_rows(&old, "old", "SELECT COUNT(*) FROM dry_wall_615 WHERE new_road_959 = 322").await;
    if let Err(err) = warmup {
        eprintln!("attempt={attempt} old snapshot warmup failed: {err}");
    }

    exec(&deleter, "deleter", TARGET_SQL).await.unwrap();

    for id in 1..=filler_rows {
        let rowid = 10_000 + id as i64;
        exec(&old, "old", format!(
            "UPDATE dry_wall_615 SET sweet_flower_317 = {}, fresh_floor_256 = {}, wet_rain_327 = zeroblob({}), full_root_761 = {}, hot_desk_383 = 'old_{}', new_wall_541 = {} WHERE new_road_959 = {}",
            30_000 + id as i64, 300_000.0 + id as f64, base_payload, 40_000 + id as i64, rowid, (id % 101) as f64 + 0.02, rowid
        )).await.unwrap();
    }

    exec(&old, "old", format!(
        "UPDATE dry_wall_615 SET sweet_flower_317 = 179, fresh_floor_256 = 7.75, wet_rain_327 = zeroblob({stale_payload}), full_root_761 = 453, hot_desk_383 = 'hot_hill_935', new_wall_541 = 5.05 WHERE new_road_959 = 322"
    )).await.unwrap();

    let old_for_commit = old.clone();
    let commit_started = Instant::now();
    let commit_handle = tokio::spawn(async move {
        let result = exec_allow(&old_for_commit, "old", "COMMIT").await;
        (result, commit_started.elapsed())
    });

    if delay_us > 0 {
        tokio::time::sleep(Duration::from_micros(delay_us)).await;
    } else {
        tokio::task::yield_now().await;
    }

    exec(&victim, "victim", "BEGIN CONCURRENT").await.unwrap();
    let victim_started = Instant::now();
    let victim_result = exec_allow(&victim, "victim", TARGET_SQL).await;
    let victim_elapsed = victim_started.elapsed();
    let (commit_result, commit_elapsed) = commit_handle.await.unwrap();
    let _ = exec_allow(&victim, "victim", "ROLLBACK").await;

    if is_target_error(&victim_result) {
        eprintln!("TARGET attempt={attempt} delay_us={delay_us} filler_rows={filler_rows} base_payload={base_payload} stale_payload={stale_payload} checkpoint_threshold={checkpoint_threshold}");
        eprintln!("victim_result={victim_result}");
        eprintln!("old_commit={commit_result} commit_elapsed={commit_elapsed:?} victim_elapsed={victim_elapsed:?}");
        return true;
    }

    eprintln!("attempt={attempt} delay_us={delay_us} victim={victim_result} old_commit={commit_result} commit_elapsed={commit_elapsed:?} victim_elapsed={victim_elapsed:?}");
    false
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let attempts = env_usize("ATTEMPTS", 10);
    let filler_rows = env_usize("FILLER_ROWS", 512);
    let base_payload = env_i64("BASE_PAYLOAD", 512);
    let stale_payload = env_i64("STALE_PAYLOAD", 256 * 1024 * 1024);
    let checkpoint_threshold = env_i64("CHECKPOINT_THRESHOLD", -1);
    let delays = delay_values();

    eprintln!("idxdelete speculative-abort repro attempts={attempts} delays={delays:?} filler_rows={filler_rows} base_payload={base_payload} stale_payload={stale_payload} checkpoint_threshold={checkpoint_threshold}");

    for attempt in 0..attempts {
        for &delay_us in &delays {
            if run_attempt(attempt, delay_us, filler_rows, base_payload, stale_payload, checkpoint_threshold).await {
                return;
            }
        }
    }

    eprintln!("no target hit");
    std::process::exit(1);
}
