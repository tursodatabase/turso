use anyhow::{bail, Context, Result};
use clap::Parser;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rusqlite::Connection as SqliteConnection;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use turso::core::diagnostics::vacuum::{
    take_last_vacuum_target_timing, take_last_vacuum_timing, VacuumTimingReport,
};
use turso::{Builder, Connection as TursoConnection, Value};

const DEFAULT_SIZES_MB: &[u64] = &[5, 50, 500, 2_048, 5_120];
const INSERT_PERCENT: u8 = 80;
const UPDATE_PERCENT: u8 = 10;
const DELETE_PERCENT: u8 = 10;

#[derive(Parser, Debug)]
#[command(name = "vacuum-benchmark")]
#[command(about = "Compare VACUUM performance between Turso and SQLite")]
struct Args {
    /// Target logical database sizes, in MB.
    #[arg(long, value_delimiter = ',', default_values_t = DEFAULT_SIZES_MB.to_vec())]
    sizes_mb: Vec<u64>,

    /// Number of operations to generate per transaction batch.
    #[arg(long, default_value_t = 256)]
    batch_ops: usize,

    /// Minimum inserted/updated payload size in KiB.
    #[arg(long, default_value_t = 8)]
    min_payload_kib: usize,

    /// Maximum inserted/updated payload size in KiB.
    #[arg(long, default_value_t = 64)]
    max_payload_kib: usize,

    /// RNG seed for workload generation.
    #[arg(long, default_value_t = 0x5eed_cafe_d00d_beef)]
    seed: u64,

    /// Directory used for generated database files.
    #[arg(long, default_value = "target/vacuum-benchmark")]
    out_dir: PathBuf,
}

#[derive(Debug, Clone)]
enum WorkloadOp {
    Insert {
        id: i64,
        hot: i64,
        note: String,
        payload: Vec<u8>,
    },
    Update {
        id: i64,
        hot: i64,
        note: String,
        payload: Vec<u8>,
    },
    Delete {
        id: i64,
    },
}

struct WorkloadPlanner {
    rng: SmallRng,
    live_ids: Vec<i64>,
    next_id: i64,
    min_payload_bytes: usize,
    max_payload_bytes: usize,
}

impl WorkloadPlanner {
    fn new(seed: u64, min_payload_bytes: usize, max_payload_bytes: usize) -> Result<Self> {
        if min_payload_bytes == 0 || min_payload_bytes > max_payload_bytes {
            bail!(
                "invalid payload range: min={} max={}",
                min_payload_bytes,
                max_payload_bytes
            );
        }
        Ok(Self {
            rng: SmallRng::seed_from_u64(seed),
            live_ids: Vec::new(),
            next_id: 1,
            min_payload_bytes,
            max_payload_bytes,
        })
    }

    fn plan_batch(&mut self, batch_ops: usize) -> Vec<WorkloadOp> {
        let mut ops = Vec::with_capacity(batch_ops);
        for _ in 0..batch_ops {
            let roll = self.rng.random_range(0..100);
            let force_insert = self.live_ids.is_empty();
            let op = if force_insert || roll < INSERT_PERCENT {
                self.plan_insert()
            } else if roll < INSERT_PERCENT + UPDATE_PERCENT {
                self.plan_update()
            } else {
                self.plan_delete()
            };
            ops.push(op);
        }
        ops
    }

    fn plan_insert(&mut self) -> WorkloadOp {
        let id = self.next_id;
        self.next_id += 1;
        self.live_ids.push(id);
        let hot = self.rng.random_range(0..4_096) as i64;
        let payload = self.make_payload(id);
        let note = format!("insert-{id}-{}", payload.len());
        WorkloadOp::Insert {
            id,
            hot,
            note,
            payload,
        }
    }

    fn plan_update(&mut self) -> WorkloadOp {
        let idx = self.rng.random_range(0..self.live_ids.len());
        let id = self.live_ids[idx];
        let hot = self.rng.random_range(0..4_096) as i64;
        let payload = self.make_payload(id);
        let note = format!("update-{id}-{}", payload.len());
        WorkloadOp::Update {
            id,
            hot,
            note,
            payload,
        }
    }

    fn plan_delete(&mut self) -> WorkloadOp {
        let idx = self.rng.random_range(0..self.live_ids.len());
        let id = self.live_ids.swap_remove(idx);
        WorkloadOp::Delete { id }
    }

    fn make_payload(&mut self, row_id: i64) -> Vec<u8> {
        let size = self
            .rng
            .random_range(self.min_payload_bytes..=self.max_payload_bytes);
        let mut payload = vec![0u8; size];
        self.rng.fill(payload.as_mut_slice());
        let prefix = row_id.to_le_bytes();
        let copy_len = prefix.len().min(payload.len());
        payload[..copy_len].copy_from_slice(&prefix[..copy_len]);
        payload
    }
}

#[derive(Debug)]
struct BackendStats {
    pre_page_count: i64,
    post_page_count: i64,
    page_size: i64,
    vacuum_duration: Duration,
    vacuum_timing: Option<VacuumTimingReport>,
    vacuum_target_timing: Option<VacuumTimingReport>,
}

#[derive(Debug)]
struct BenchmarkRow {
    target_size_mb: u64,
    sqlite: BackendStats,
    turso: BackendStats,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let args = Args::parse();

    if args.sizes_mb.is_empty() {
        bail!("at least one target size must be provided");
    }
    if INSERT_PERCENT + UPDATE_PERCENT + DELETE_PERCENT != 100 {
        bail!("workload percentages must sum to 100");
    }

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create output dir {}", args.out_dir.display()))?;

    let mut rows = Vec::new();
    for (idx, size_mb) in args.sizes_mb.iter().copied().enumerate() {
        let result = run_case(
            idx,
            size_mb,
            &args.out_dir,
            args.seed.wrapping_add(idx as u64),
            args.batch_ops,
            args.min_payload_kib * 1024,
            args.max_payload_kib * 1024,
        )
        .await
        .with_context(|| format!("benchmark case {size_mb}MB"))?;
        rows.push(result);
    }

    println!(
        "workload mix: {INSERT_PERCENT}% inserts / {UPDATE_PERCENT}% updates / {DELETE_PERCENT}% deletes"
    );
    print_report(&rows);
    print_turso_vacuum_timing(&rows);
    print_turso_target_timing(&rows);
    Ok(())
}

async fn run_case(
    case_idx: usize,
    target_size_mb: u64,
    out_dir: &Path,
    seed: u64,
    batch_ops: usize,
    min_payload_bytes: usize,
    max_payload_bytes: usize,
) -> Result<BenchmarkRow> {
    let case_dir = out_dir.join(format!("{case_idx:02}-{target_size_mb}mb"));
    fs::create_dir_all(&case_dir)
        .with_context(|| format!("create case dir {}", case_dir.display()))?;

    let sqlite_path = case_dir.join("sqlite.db");
    let turso_path = case_dir.join("turso.db");
    cleanup_db_files(&sqlite_path)?;
    cleanup_db_files(&turso_path)?;

    let sqlite = SqliteBackend::open(&sqlite_path)?;
    let turso = TursoBackend::open(&turso_path).await?;

    let mut planner = WorkloadPlanner::new(seed, min_payload_bytes, max_payload_bytes)?;
    let target_bytes = target_size_mb * 1024 * 1024;
    populate_to_target(target_bytes, batch_ops, &mut planner, &sqlite, &turso).await?;

    sqlite.prepare_for_vacuum()?;
    turso.prepare_for_vacuum().await?;

    let sqlite_pre_pages = sqlite.page_count()?;
    let turso_pre_pages = turso.page_count().await?;

    sync_barrier();
    let sqlite_vacuum = sqlite.vacuum()?;
    sync_barrier();
    let turso_vacuum = turso.vacuum().await?;

    let sqlite_post_pages = sqlite.page_count()?;
    let turso_post_pages = turso.page_count().await?;

    Ok(BenchmarkRow {
        target_size_mb,
        sqlite: BackendStats {
            pre_page_count: sqlite_pre_pages,
            post_page_count: sqlite_post_pages,
            page_size: sqlite.page_size()?,
            vacuum_duration: sqlite_vacuum,
            vacuum_timing: None,
            vacuum_target_timing: None,
        },
        turso: BackendStats {
            pre_page_count: turso_pre_pages,
            post_page_count: turso_post_pages,
            page_size: turso.page_size().await?,
            vacuum_duration: turso_vacuum.duration,
            vacuum_timing: turso_vacuum.timing,
            vacuum_target_timing: turso_vacuum.target_timing,
        },
    })
}

fn sync_barrier() {
    #[cfg(unix)]
    unsafe {
        libc::sync();
    }

    std::thread::sleep(Duration::from_millis(100));
}

async fn populate_to_target(
    target_bytes: u64,
    batch_ops: usize,
    planner: &mut WorkloadPlanner,
    sqlite: &SqliteBackend,
    turso: &TursoBackend,
) -> Result<()> {
    loop {
        let sqlite_bytes = sqlite.logical_size_bytes()?;
        let turso_bytes = turso.logical_size_bytes().await?;
        if sqlite_bytes >= target_bytes && turso_bytes >= target_bytes {
            break;
        }

        let ops = planner.plan_batch(batch_ops);
        sqlite.apply_batch(&ops)?;
        turso.apply_batch(&ops).await?;
    }
    Ok(())
}

fn cleanup_db_files(db_path: &Path) -> Result<()> {
    for candidate in [
        db_path.to_path_buf(),
        PathBuf::from(format!("{}-wal", db_path.display())),
        PathBuf::from(format!("{}-shm", db_path.display())),
        db_path.with_extension("db-log"),
    ] {
        if candidate.exists() {
            fs::remove_file(&candidate)
                .with_context(|| format!("remove old benchmark file {}", candidate.display()))?;
        }
    }
    Ok(())
}

struct SqliteBackend {
    conn: SqliteConnection,
}

impl SqliteBackend {
    fn open(path: &Path) -> Result<Self> {
        let conn = SqliteConnection::open(path)
            .with_context(|| format!("open sqlite db {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "OFF")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        conn.execute_batch(
            "CREATE TABLE workload(
                id INTEGER PRIMARY KEY,
                hot INTEGER NOT NULL,
                payload BLOB NOT NULL,
                note TEXT NOT NULL
            );
             CREATE INDEX workload_hot_idx ON workload(hot);",
        )?;
        Ok(Self { conn })
    }

    fn apply_batch(&self, ops: &[WorkloadOp]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut insert_stmt =
                tx.prepare("INSERT INTO workload(id, hot, payload, note) VALUES(?1, ?2, ?3, ?4)")?;
            let mut update_stmt =
                tx.prepare("UPDATE workload SET hot = ?2, payload = ?3, note = ?4 WHERE id = ?1")?;
            let mut delete_stmt = tx.prepare("DELETE FROM workload WHERE id = ?1")?;

            for op in ops {
                match op {
                    WorkloadOp::Insert {
                        id,
                        hot,
                        note,
                        payload,
                    } => {
                        insert_stmt.execute(rusqlite::params![id, hot, payload, note])?;
                    }
                    WorkloadOp::Update {
                        id,
                        hot,
                        note,
                        payload,
                    } => {
                        update_stmt.execute(rusqlite::params![id, hot, payload, note])?;
                    }
                    WorkloadOp::Delete { id } => {
                        delete_stmt.execute(rusqlite::params![id])?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn prepare_for_vacuum(&self) -> Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA synchronous = FULL;")?;
        Ok(())
    }

    fn vacuum(&self) -> Result<Duration> {
        let start = Instant::now();
        self.conn.execute_batch("VACUUM")?;
        Ok(start.elapsed())
    }

    fn page_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get::<_, i64>(0))?)
    }

    fn page_size(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get::<_, i64>(0))?)
    }

    fn logical_size_bytes(&self) -> Result<u64> {
        Ok((self.page_count()? as u64) * (self.page_size()? as u64))
    }
}

struct TursoBackend {
    conn: TursoConnection,
}

struct TursoVacuumStats {
    duration: Duration,
    timing: Option<VacuumTimingReport>,
    target_timing: Option<VacuumTimingReport>,
}

impl TursoBackend {
    async fn open(path: &Path) -> Result<Self> {
        let db = Builder::new_local(path.to_str().context("non-utf8 turso path")?)
            .experimental_vacuum(true)
            .build()
            .await?;
        let conn = db.connect()?;
        conn.busy_timeout(Duration::from_secs(300))?;
        conn.pragma_update("journal_mode", "'WAL'").await?;
        conn.pragma_update("synchronous", "'OFF'").await?;
        conn.pragma_update("temp_store", "'MEMORY'").await?;
        conn.execute(
            "CREATE TABLE workload(
                id INTEGER PRIMARY KEY,
                hot INTEGER NOT NULL,
                payload BLOB NOT NULL,
                note TEXT NOT NULL
            )",
            (),
        )
        .await?;
        conn.execute("CREATE INDEX workload_hot_idx ON workload(hot)", ())
            .await?;
        Ok(Self { conn })
    }

    async fn apply_batch(&self, ops: &[WorkloadOp]) -> Result<()> {
        self.conn.execute("BEGIN IMMEDIATE", ()).await?;
        let mut insert_stmt = self
            .conn
            .prepare("INSERT INTO workload(id, hot, payload, note) VALUES(?1, ?2, ?3, ?4)")
            .await?;
        let mut update_stmt = self
            .conn
            .prepare("UPDATE workload SET hot = ?2, payload = ?3, note = ?4 WHERE id = ?1")
            .await?;
        let mut delete_stmt = self
            .conn
            .prepare("DELETE FROM workload WHERE id = ?1")
            .await?;

        for op in ops {
            match op {
                WorkloadOp::Insert {
                    id,
                    hot,
                    note,
                    payload,
                } => {
                    insert_stmt
                        .execute((*id, *hot, Value::Blob(payload.clone()), note.clone()))
                        .await?;
                }
                WorkloadOp::Update {
                    id,
                    hot,
                    note,
                    payload,
                } => {
                    update_stmt
                        .execute((*id, *hot, Value::Blob(payload.clone()), note.clone()))
                        .await?;
                }
                WorkloadOp::Delete { id } => {
                    delete_stmt.execute((*id,)).await?;
                }
            }
        }

        self.conn.execute("COMMIT", ()).await?;
        Ok(())
    }

    async fn prepare_for_vacuum(&self) -> Result<()> {
        self.run_pragma_row("PRAGMA wal_checkpoint(TRUNCATE)")
            .await?;
        self.conn.pragma_update("synchronous", "'FULL'").await?;
        Ok(())
    }

    async fn vacuum(&self) -> Result<TursoVacuumStats> {
        let _ = take_last_vacuum_timing();
        let _ = take_last_vacuum_target_timing();
        let start = Instant::now();
        self.conn.execute("VACUUM", ()).await?;
        Ok(TursoVacuumStats {
            duration: start.elapsed(),
            timing: take_last_vacuum_timing(),
            target_timing: take_last_vacuum_target_timing(),
        })
    }

    async fn page_count(&self) -> Result<i64> {
        self.pragma_i64("PRAGMA page_count").await
    }

    async fn page_size(&self) -> Result<i64> {
        self.pragma_i64("PRAGMA page_size").await
    }

    async fn logical_size_bytes(&self) -> Result<u64> {
        Ok((self.page_count().await? as u64) * (self.page_size().await? as u64))
    }

    async fn pragma_i64(&self, sql: &str) -> Result<i64> {
        let mut stmt = self.conn.prepare(sql).await?;
        let row = stmt.query_row(()).await?;
        Ok(row.get::<i64>(0)?)
    }

    async fn run_pragma_row(&self, sql: &str) -> Result<()> {
        let mut stmt = self.conn.prepare(sql).await?;
        let _ = stmt.query_row(()).await?;
        Ok(())
    }
}

fn print_report(rows: &[BenchmarkRow]) {
    println!();
    println!(
        "{:<10} {:<8} {:>14} {:>14} {:>14} {:>14} {:>14}",
        "target", "backend", "pre_pages", "post_pages", "pre_size_mb", "post_size_mb", "vacuum_ms"
    );
    println!("{}", "-".repeat(98));
    for row in rows {
        print_backend_row(row.target_size_mb, "sqlite", &row.sqlite);
        print_backend_row(row.target_size_mb, "turso", &row.turso);
        println!();
    }
}

fn print_backend_row(target_size_mb: u64, backend: &str, stats: &BackendStats) {
    let pre_size_mb =
        (stats.pre_page_count as f64 * stats.page_size as f64) / (1024.0_f64 * 1024.0_f64);
    let post_size_mb =
        (stats.post_page_count as f64 * stats.page_size as f64) / (1024.0_f64 * 1024.0_f64);
    println!(
        "{:<10} {:<8} {:>14} {:>14} {:>14.1} {:>14.1} {:>14.2}",
        format!("{target_size_mb}MB"),
        backend,
        stats.pre_page_count,
        stats.post_page_count,
        pre_size_mb,
        post_size_mb,
        stats.vacuum_duration.as_secs_f64() * 1000.0,
    );
}

fn print_turso_vacuum_timing(rows: &[BenchmarkRow]) {
    if !rows.iter().any(|row| row.turso.vacuum_timing.is_some()) {
        return;
    }

    println!();
    println!("turso vacuum phase timings:");
    println!(
        "{:<10} {:<28} {:>14} {:>10}",
        "target", "phase", "phase_ms", "profile_%"
    );
    println!("{}", "-".repeat(68));
    for row in rows {
        let Some(timing) = &row.turso.vacuum_timing else {
            continue;
        };
        let total_ms = timing.total.as_secs_f64() * 1000.0;
        for entry in &timing.entries {
            let phase_ms = entry.duration.as_secs_f64() * 1000.0;
            let pct = if total_ms > 0.0 {
                (phase_ms / total_ms) * 100.0
            } else {
                0.0
            };
            println!(
                "{:<10} {:<28} {:>14.2} {:>9.1}%",
                format!("{}MB", row.target_size_mb),
                entry.phase,
                phase_ms,
                pct,
            );
        }
        println!(
            "{:<10} {:<28} {:>14.2} {:>9.1}%",
            format!("{}MB", row.target_size_mb),
            "profile_total",
            total_ms,
            100.0,
        );
        println!();
    }
}

fn print_turso_target_timing(rows: &[BenchmarkRow]) {
    if !rows
        .iter()
        .any(|row| row.turso.vacuum_target_timing.is_some())
    {
        return;
    }

    println!();
    println!("turso target-build phase timings:");
    println!(
        "{:<10} {:<32} {:>14} {:>10}",
        "target", "phase", "phase_ms", "target_%"
    );
    println!("{}", "-".repeat(72));
    for row in rows {
        let Some(timing) = &row.turso.vacuum_target_timing else {
            continue;
        };
        let total_ms = timing.total.as_secs_f64() * 1000.0;
        for entry in &timing.entries {
            let phase_ms = entry.duration.as_secs_f64() * 1000.0;
            let pct = if total_ms > 0.0 {
                (phase_ms / total_ms) * 100.0
            } else {
                0.0
            };
            println!(
                "{:<10} {:<32} {:>14.2} {:>9.1}%",
                format!("{}MB", row.target_size_mb),
                entry.phase,
                phase_ms,
                pct,
            );
        }
        println!(
            "{:<10} {:<32} {:>14.2} {:>9.1}%",
            format!("{}MB", row.target_size_mb),
            "target_total",
            total_ms,
            100.0,
        );
        println!();
    }
}
