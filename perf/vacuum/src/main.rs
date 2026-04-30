use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rusqlite::Connection as SqliteConnection;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use turso::core::diagnostics::vacuum::{
    take_last_vacuum_target_timing, take_last_vacuum_timing, VacuumTimingReport,
};
use turso::{Builder, Connection as TursoConnection, Value};

const DEFAULT_SIZES_MB: &[u64] = &[
    5, 50, 100, 250, 500, 750, 1_000, 1_500, 2_048, 3_072, 5_120, 8_192, 10_240,
];
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

    /// Keep generated database files after each benchmark case.
    #[arg(long)]
    keep_files: bool,

    /// Run only the SQLite backend.
    #[arg(long, conflicts_with = "turso")]
    sqlite: bool,

    /// Run only the Turso backend.
    #[arg(long, conflicts_with = "sqlite")]
    turso: bool,

    /// Measure both backends back-to-back in the same generated case.
    #[arg(long)]
    paired: bool,

    /// Independent benchmark runs per target size.
    #[arg(long, default_value_t = 1)]
    runs: usize,

    /// Backend measurement order for each run.
    #[arg(long, value_enum, default_value = "alternate")]
    backend_order: BackendOrderArg,

    /// SQLite locking mode.
    #[arg(long, value_enum, default_value = "normal")]
    sqlite_locking_mode: SqliteLockingMode,

    /// SQLite auto-checkpoint behavior while measuring VACUUM.
    #[arg(long, value_enum, default_value = "default")]
    sqlite_auto_checkpoint: SqliteAutoCheckpoint,

    /// SQLite action measured after VACUUM.
    #[arg(long, value_enum, default_value = "none")]
    sqlite_post_vacuum_checkpoint: SqlitePostVacuumCheckpoint,

    /// Untimed source warmup before each measured backend.
    #[arg(long, value_enum, default_value = "none")]
    warmup: WarmupMode,

    /// Sleep after the file sync barrier before each measured VACUUM.
    #[arg(long, default_value_t = 2_000)]
    barrier_sleep_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum BackendOrderArg {
    Alternate,
    SqliteFirst,
    TursoFirst,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendOrder {
    SqliteFirst,
    TursoFirst,
    SqliteOnly,
    TursoOnly,
}

impl BackendOrderArg {
    fn order_for_run(self, run_idx: usize) -> BackendOrder {
        match self {
            Self::Alternate if run_idx % 2 == 0 => BackendOrder::SqliteFirst,
            Self::Alternate => BackendOrder::TursoFirst,
            Self::SqliteFirst => BackendOrder::SqliteFirst,
            Self::TursoFirst => BackendOrder::TursoFirst,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum SqliteLockingMode {
    Normal,
    Exclusive,
}

impl SqliteLockingMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Exclusive => "exclusive",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum SqliteAutoCheckpoint {
    Default,
    Off,
}

impl SqliteAutoCheckpoint {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Off => "off",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum SqlitePostVacuumCheckpoint {
    None,
    Truncate,
}

impl SqlitePostVacuumCheckpoint {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Truncate => "truncate",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum WarmupMode {
    None,
    SourceScan,
}

impl WarmupMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SourceScan => "source-scan",
        }
    }

    fn should_scan_source(self) -> bool {
        matches!(self, Self::SourceScan)
    }
}

impl BackendOrder {
    fn as_str(self) -> &'static str {
        match self {
            Self::SqliteFirst => "sqlite-first",
            Self::TursoFirst => "turso-first",
            Self::SqliteOnly => "sqlite-only",
            Self::TursoOnly => "turso-only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendSelection {
    Both,
    Sqlite,
    Turso,
}

struct BenchmarkCaseSpec {
    selection: BackendSelection,
    order: BackendOrder,
    label: &'static str,
}

impl BackendSelection {
    fn from_args(args: &Args) -> Self {
        match (args.sqlite, args.turso) {
            (true, false) => Self::Sqlite,
            (false, true) => Self::Turso,
            (false, false) => Self::Both,
            (true, true) => unreachable!("clap enforces --sqlite and --turso conflicts"),
        }
    }

    fn includes_sqlite(self) -> bool {
        matches!(self, Self::Both | Self::Sqlite)
    }

    fn includes_turso(self) -> bool {
        matches!(self, Self::Both | Self::Turso)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::Sqlite => "sqlite",
            Self::Turso => "turso",
        }
    }

    fn case_specs(
        self,
        order_arg: BackendOrderArg,
        run_idx: usize,
        paired: bool,
    ) -> Vec<BenchmarkCaseSpec> {
        match self {
            Self::Sqlite => vec![BenchmarkCaseSpec {
                selection: Self::Sqlite,
                order: BackendOrder::SqliteOnly,
                label: "sqlite",
            }],
            Self::Turso => vec![BenchmarkCaseSpec {
                selection: Self::Turso,
                order: BackendOrder::TursoOnly,
                label: "turso",
            }],
            Self::Both if paired => vec![BenchmarkCaseSpec {
                selection: Self::Both,
                order: order_arg.order_for_run(run_idx),
                label: "paired",
            }],
            Self::Both => match order_arg.order_for_run(run_idx) {
                BackendOrder::SqliteFirst => vec![
                    BenchmarkCaseSpec {
                        selection: Self::Sqlite,
                        order: BackendOrder::SqliteOnly,
                        label: "sqlite",
                    },
                    BenchmarkCaseSpec {
                        selection: Self::Turso,
                        order: BackendOrder::TursoOnly,
                        label: "turso",
                    },
                ],
                BackendOrder::TursoFirst => vec![
                    BenchmarkCaseSpec {
                        selection: Self::Turso,
                        order: BackendOrder::TursoOnly,
                        label: "turso",
                    },
                    BenchmarkCaseSpec {
                        selection: Self::Sqlite,
                        order: BackendOrder::SqliteOnly,
                        label: "sqlite",
                    },
                ],
                BackendOrder::SqliteOnly | BackendOrder::TursoOnly => {
                    unreachable!("backend order arg cannot produce single-backend order")
                }
            },
        }
    }
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
    pre_wal_bytes: u64,
    post_wal_bytes: u64,
    vacuum_duration: Duration,
    vacuum_statement_duration: Duration,
    post_vacuum_checkpoint_duration: Option<Duration>,
    vacuum_timing: Option<VacuumTimingReport>,
    vacuum_target_timing: Option<VacuumTimingReport>,
}

#[derive(Debug)]
struct BenchmarkRow {
    target_size_mb: u64,
    run_idx: usize,
    order: BackendOrder,
    sqlite: Option<BackendStats>,
    turso: Option<BackendStats>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let args = Args::parse();

    if args.sizes_mb.is_empty() {
        bail!("at least one target size must be provided");
    }
    if args.runs == 0 {
        bail!("runs must be greater than zero");
    }
    if INSERT_PERCENT + UPDATE_PERCENT + DELETE_PERCENT != 100 {
        bail!("workload percentages must sum to 100");
    }

    let backend_selection = BackendSelection::from_args(&args);
    if args.paired && backend_selection != BackendSelection::Both {
        bail!("--paired can only be used when both backends are selected");
    }

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create output dir {}", args.out_dir.display()))?;

    let mut rows = Vec::new();
    for (size_idx, size_mb) in args.sizes_mb.iter().copied().enumerate() {
        for run_idx in 0..args.runs {
            for spec in backend_selection.case_specs(args.backend_order, run_idx, args.paired) {
                let result = run_case(
                    size_idx,
                    run_idx,
                    size_mb,
                    spec.label,
                    &args.out_dir,
                    args.seed.wrapping_add(size_idx as u64),
                    args.batch_ops,
                    args.min_payload_kib * 1024,
                    args.max_payload_kib * 1024,
                    spec.selection,
                    spec.order,
                    args.sqlite_locking_mode,
                    args.sqlite_auto_checkpoint,
                    args.sqlite_post_vacuum_checkpoint,
                    args.warmup,
                    Duration::from_millis(args.barrier_sleep_ms),
                    args.keep_files,
                )
                .await
                .with_context(|| {
                    format!(
                        "benchmark case {size_mb}MB run {} {}",
                        run_idx + 1,
                        spec.label
                    )
                })?;
                print_case_progress(&result);
                rows.push(result);
            }
        }
    }

    println!(
        "workload mix: {INSERT_PERCENT}% inserts / {UPDATE_PERCENT}% updates / {DELETE_PERCENT}% deletes"
    );
    println!(
        "runs per target: {}; backend selection: {}; backend execution: {}; backend order: {:?}; barrier: per-file sync + {}ms sleep",
        args.runs,
        backend_selection.as_str(),
        if args.paired { "paired" } else { "isolated" },
        args.backend_order,
        args.barrier_sleep_ms
    );
    println!(
        "sqlite locking mode: {}; auto-checkpoint: {}",
        args.sqlite_locking_mode.as_str(),
        args.sqlite_auto_checkpoint.as_str()
    );
    println!(
        "sqlite post-vacuum checkpoint: {}",
        args.sqlite_post_vacuum_checkpoint.as_str()
    );
    println!("warmup: {}", args.warmup.as_str());
    println!(
        "case files: {}",
        if args.keep_files {
            "kept"
        } else {
            "removed after each case"
        }
    );
    print_report(&rows);
    print_summary(&rows);
    print_turso_vacuum_timing(&rows);
    print_turso_target_timing(&rows);
    Ok(())
}

async fn run_case(
    size_idx: usize,
    run_idx: usize,
    target_size_mb: u64,
    case_label: &str,
    out_dir: &Path,
    seed: u64,
    batch_ops: usize,
    min_payload_bytes: usize,
    max_payload_bytes: usize,
    backend_selection: BackendSelection,
    order: BackendOrder,
    sqlite_locking_mode: SqliteLockingMode,
    sqlite_auto_checkpoint: SqliteAutoCheckpoint,
    sqlite_post_vacuum_checkpoint: SqlitePostVacuumCheckpoint,
    warmup: WarmupMode,
    barrier_sleep: Duration,
    keep_files: bool,
) -> Result<BenchmarkRow> {
    let case_dir = out_dir.join(format!(
        "{size_idx:02}-{target_size_mb}mb-run{run_idx:02}-{case_label}"
    ));
    fs::create_dir_all(&case_dir)
        .with_context(|| format!("create case dir {}", case_dir.display()))?;

    let sqlite_path = case_dir.join("sqlite.db");
    let turso_path = case_dir.join("turso.db");
    if backend_selection.includes_sqlite() {
        cleanup_db_files(&sqlite_path)?;
    }
    if backend_selection.includes_turso() {
        cleanup_db_files(&turso_path)?;
    }

    let sqlite = if backend_selection.includes_sqlite() {
        Some(SqliteBackend::open(&sqlite_path, sqlite_locking_mode)?)
    } else {
        None
    };
    let turso = if backend_selection.includes_turso() {
        Some(TursoBackend::open(&turso_path).await?)
    } else {
        None
    };

    let mut planner = WorkloadPlanner::new(seed, min_payload_bytes, max_payload_bytes)?;
    let target_bytes = target_size_mb * 1024 * 1024;
    populate_to_target(
        target_bytes,
        batch_ops,
        &mut planner,
        sqlite.as_ref(),
        turso.as_ref(),
    )
    .await?;

    if let Some(sqlite) = &sqlite {
        sqlite.prepare_for_vacuum(sqlite_auto_checkpoint)?;
    }
    if let Some(turso) = &turso {
        turso.prepare_for_vacuum().await?;
    }

    let sqlite_pre = if let Some(sqlite) = &sqlite {
        Some((
            sqlite.page_count()?,
            sqlite.page_size()?,
            file_size_or_zero(&wal_path(&sqlite_path))?,
        ))
    } else {
        None
    };
    let turso_pre = if let Some(turso) = &turso {
        Some((
            turso.page_count().await?,
            turso.page_size().await?,
            file_size_or_zero(&wal_path(&turso_path))?,
        ))
    } else {
        None
    };

    let mut db_paths = Vec::new();
    if backend_selection.includes_sqlite() {
        db_paths.push(sqlite_path.as_path());
    }
    if backend_selection.includes_turso() {
        db_paths.push(turso_path.as_path());
    }

    let mut sqlite_vacuum = None;
    let mut turso_vacuum = None;
    match order {
        BackendOrder::SqliteFirst => {
            let sqlite_backend = sqlite
                .as_ref()
                .context("sqlite-first order requires sqlite backend")?;
            let turso_backend = turso
                .as_ref()
                .context("sqlite-first order requires turso backend")?;
            sqlite_vacuum = Some(measure_sqlite_vacuum(
                sqlite_backend,
                sqlite_post_vacuum_checkpoint,
                warmup,
                &case_dir,
                &db_paths,
                barrier_sleep,
            )?);
            turso_vacuum = Some(
                measure_turso_vacuum(turso_backend, warmup, &case_dir, &db_paths, barrier_sleep)
                    .await?,
            );
        }
        BackendOrder::TursoFirst => {
            let sqlite_backend = sqlite
                .as_ref()
                .context("turso-first order requires sqlite backend")?;
            let turso_backend = turso
                .as_ref()
                .context("turso-first order requires turso backend")?;
            turso_vacuum = Some(
                measure_turso_vacuum(turso_backend, warmup, &case_dir, &db_paths, barrier_sleep)
                    .await?,
            );
            sqlite_vacuum = Some(measure_sqlite_vacuum(
                sqlite_backend,
                sqlite_post_vacuum_checkpoint,
                warmup,
                &case_dir,
                &db_paths,
                barrier_sleep,
            )?);
        }
        BackendOrder::SqliteOnly => {
            let sqlite_backend = sqlite
                .as_ref()
                .context("sqlite-only order requires sqlite backend")?;
            sqlite_vacuum = Some(measure_sqlite_vacuum(
                sqlite_backend,
                sqlite_post_vacuum_checkpoint,
                warmup,
                &case_dir,
                &db_paths,
                barrier_sleep,
            )?);
        }
        BackendOrder::TursoOnly => {
            let turso_backend = turso
                .as_ref()
                .context("turso-only order requires turso backend")?;
            turso_vacuum = Some(
                measure_turso_vacuum(turso_backend, warmup, &case_dir, &db_paths, barrier_sleep)
                    .await?,
            );
        }
    }

    let row = BenchmarkRow {
        target_size_mb,
        run_idx,
        order,
        sqlite: if let (
            Some(sqlite),
            Some(vacuum),
            Some((pre_page_count, page_size, pre_wal_bytes)),
        ) = (&sqlite, sqlite_vacuum, sqlite_pre)
        {
            Some(BackendStats {
                pre_page_count,
                post_page_count: sqlite.page_count()?,
                page_size,
                pre_wal_bytes,
                post_wal_bytes: file_size_or_zero(&wal_path(&sqlite_path))?,
                vacuum_duration: vacuum.total_duration,
                vacuum_statement_duration: vacuum.statement_duration,
                post_vacuum_checkpoint_duration: vacuum.post_vacuum_checkpoint_duration,
                vacuum_timing: None,
                vacuum_target_timing: None,
            })
        } else {
            None
        },
        turso: if let (
            Some(turso),
            Some(vacuum),
            Some((pre_page_count, page_size, pre_wal_bytes)),
        ) = (&turso, turso_vacuum, turso_pre)
        {
            Some(BackendStats {
                pre_page_count,
                post_page_count: turso.page_count().await?,
                page_size,
                pre_wal_bytes,
                post_wal_bytes: file_size_or_zero(&wal_path(&turso_path))?,
                vacuum_duration: vacuum.duration,
                vacuum_statement_duration: vacuum.duration,
                post_vacuum_checkpoint_duration: None,
                vacuum_timing: vacuum.timing,
                vacuum_target_timing: vacuum.target_timing,
            })
        } else {
            None
        },
    };

    if !keep_files {
        drop(sqlite);
        drop(turso);
        fs::remove_dir_all(&case_dir)
            .with_context(|| format!("remove benchmark case dir {}", case_dir.display()))?;
    }

    Ok(row)
}

fn measure_sqlite_vacuum(
    sqlite: &SqliteBackend,
    sqlite_post_vacuum_checkpoint: SqlitePostVacuumCheckpoint,
    warmup: WarmupMode,
    case_dir: &Path,
    db_paths: &[&Path],
    barrier_sleep: Duration,
) -> Result<SqliteVacuumStats> {
    sync_barrier(case_dir, db_paths, barrier_sleep)?;
    if warmup.should_scan_source() {
        sqlite.warm_source()?;
    }
    sqlite.vacuum(sqlite_post_vacuum_checkpoint)
}

async fn measure_turso_vacuum(
    turso: &TursoBackend,
    warmup: WarmupMode,
    case_dir: &Path,
    db_paths: &[&Path],
    barrier_sleep: Duration,
) -> Result<TursoVacuumStats> {
    sync_barrier(case_dir, db_paths, barrier_sleep)?;
    if warmup.should_scan_source() {
        turso.warm_source().await?;
    }
    turso.vacuum().await
}

fn sync_barrier(case_dir: &Path, db_paths: &[&Path], sleep: Duration) -> Result<()> {
    for db_path in db_paths {
        sync_db_files(db_path)?;
    }
    sync_directory(case_dir)?;
    std::thread::sleep(sleep);
    Ok(())
}

fn sync_db_files(db_path: &Path) -> Result<()> {
    for candidate in db_related_paths(db_path) {
        if candidate.exists() {
            File::open(&candidate)
                .with_context(|| format!("open benchmark file {}", candidate.display()))?
                .sync_all()
                .with_context(|| format!("sync benchmark file {}", candidate.display()))?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open benchmark directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync benchmark directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

async fn populate_to_target(
    target_bytes: u64,
    batch_ops: usize,
    planner: &mut WorkloadPlanner,
    sqlite: Option<&SqliteBackend>,
    turso: Option<&TursoBackend>,
) -> Result<()> {
    if sqlite.is_none() && turso.is_none() {
        bail!("at least one backend must be selected");
    }

    loop {
        let sqlite_bytes = match sqlite {
            Some(sqlite) => sqlite.logical_size_bytes()?,
            None => target_bytes,
        };
        let turso_bytes = match turso {
            Some(turso) => turso.logical_size_bytes().await?,
            None => target_bytes,
        };
        if sqlite_bytes >= target_bytes && turso_bytes >= target_bytes {
            break;
        }

        let ops = planner.plan_batch(batch_ops);
        if let Some(sqlite) = sqlite {
            sqlite.apply_batch(&ops)?;
        }
        if let Some(turso) = turso {
            turso.apply_batch(&ops).await?;
        }
    }
    Ok(())
}

fn cleanup_db_files(db_path: &Path) -> Result<()> {
    for candidate in db_related_paths(db_path) {
        if candidate.exists() {
            fs::remove_file(&candidate)
                .with_context(|| format!("remove old benchmark file {}", candidate.display()))?;
        }
    }
    Ok(())
}

fn db_related_paths(db_path: &Path) -> [PathBuf; 4] {
    [
        db_path.to_path_buf(),
        wal_path(db_path),
        PathBuf::from(format!("{}-shm", db_path.display())),
        db_path.with_extension("db-log"),
    ]
}

fn wal_path(db_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_path.display()))
}

fn file_size_or_zero(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err).with_context(|| format!("stat {}", path.display())),
    }
}

struct SqliteBackend {
    conn: SqliteConnection,
}

struct SqliteVacuumStats {
    total_duration: Duration,
    statement_duration: Duration,
    post_vacuum_checkpoint_duration: Option<Duration>,
}

impl SqliteBackend {
    fn open(path: &Path, locking_mode: SqliteLockingMode) -> Result<Self> {
        let conn = SqliteConnection::open(path)
            .with_context(|| format!("open sqlite db {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "OFF")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        match locking_mode {
            SqliteLockingMode::Normal => conn.pragma_update(None, "locking_mode", "NORMAL")?,
            SqliteLockingMode::Exclusive => {
                conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?
            }
        }
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

    fn prepare_for_vacuum(&self, auto_checkpoint: SqliteAutoCheckpoint) -> Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA synchronous = FULL;")?;
        if auto_checkpoint == SqliteAutoCheckpoint::Off {
            self.conn.execute_batch("PRAGMA wal_autocheckpoint = 0;")?;
        }
        Ok(())
    }

    fn vacuum(&self, post_checkpoint: SqlitePostVacuumCheckpoint) -> Result<SqliteVacuumStats> {
        let total_start = Instant::now();
        let start = Instant::now();
        self.conn.execute_batch("VACUUM")?;
        let statement_duration = start.elapsed();
        let post_vacuum_checkpoint_duration = match post_checkpoint {
            SqlitePostVacuumCheckpoint::None => None,
            SqlitePostVacuumCheckpoint::Truncate => {
                let start = Instant::now();
                self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
                Some(start.elapsed())
            }
        };
        Ok(SqliteVacuumStats {
            total_duration: total_start.elapsed(),
            statement_duration,
            post_vacuum_checkpoint_duration,
        })
    }

    fn warm_source(&self) -> Result<()> {
        let _: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(length(payload)), 0) FROM workload",
            [],
            |row| row.get(0),
        )?;
        let _: i64 = self.conn.query_row(
            "SELECT count(*) FROM workload INDEXED BY workload_hot_idx WHERE hot >= 0",
            [],
            |row| row.get(0),
        )?;
        Ok(())
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

    async fn warm_source(&self) -> Result<()> {
        let _ = self
            .query_i64("SELECT COALESCE(SUM(length(payload)), 0) FROM workload")
            .await?;
        let _ = self
            .query_i64("SELECT count(*) FROM workload INDEXED BY workload_hot_idx WHERE hot >= 0")
            .await?;
        Ok(())
    }

    async fn pragma_i64(&self, sql: &str) -> Result<i64> {
        self.query_i64(sql).await
    }

    async fn query_i64(&self, sql: &str) -> Result<i64> {
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
        "{:<10} {:>4} {:<12} {:<12} {:>14} {:>14} {:>14} {:>14} {:>12} {:>12} {:>14} {:>14} {:>14}",
        "target",
        "run",
        "order",
        "backend",
        "pre_pages",
        "post_pages",
        "pre_size_mb",
        "post_size_mb",
        "pre_wal_mb",
        "post_wal_mb",
        "stmt_ms",
        "checkpoint_ms",
        "measured_ms"
    );
    println!("{}", "-".repeat(174));
    for row in rows {
        if let Some(sqlite) = &row.sqlite {
            print_backend_row(row, sqlite_backend_label(sqlite), sqlite);
        }
        if let Some(turso) = &row.turso {
            print_backend_row(row, "turso", turso);
        }
        println!();
    }
}

fn print_case_progress(row: &BenchmarkRow) {
    let sqlite_ms = row
        .sqlite
        .as_ref()
        .map(|stats| format!("{:.2}", duration_ms(&stats.vacuum_duration)))
        .unwrap_or_else(|| "-".to_string());
    let turso_ms = row
        .turso
        .as_ref()
        .map(|stats| format!("{:.2}", duration_ms(&stats.vacuum_duration)))
        .unwrap_or_else(|| "-".to_string());
    let sqlite_wal_mb = row
        .sqlite
        .as_ref()
        .map(|stats| {
            format!(
                "{:.1}",
                stats.post_wal_bytes as f64 / (1024.0_f64 * 1024.0_f64)
            )
        })
        .unwrap_or_else(|| "-".to_string());
    let turso_wal_mb = row
        .turso
        .as_ref()
        .map(|stats| {
            format!(
                "{:.1}",
                stats.post_wal_bytes as f64 / (1024.0_f64 * 1024.0_f64)
            )
        })
        .unwrap_or_else(|| "-".to_string());
    eprintln!(
        "completed target={}MB run={} order={} sqlite_ms={} turso_ms={} sqlite_wal_mb={} turso_wal_mb={}",
        row.target_size_mb,
        row.run_idx + 1,
        row.order.as_str(),
        sqlite_ms,
        turso_ms,
        sqlite_wal_mb,
        turso_wal_mb,
    );
}

fn print_backend_row(row: &BenchmarkRow, backend: &str, stats: &BackendStats) {
    let pre_size_mb =
        (stats.pre_page_count as f64 * stats.page_size as f64) / (1024.0_f64 * 1024.0_f64);
    let post_size_mb =
        (stats.post_page_count as f64 * stats.page_size as f64) / (1024.0_f64 * 1024.0_f64);
    let pre_wal_mb = stats.pre_wal_bytes as f64 / (1024.0_f64 * 1024.0_f64);
    let post_wal_mb = stats.post_wal_bytes as f64 / (1024.0_f64 * 1024.0_f64);
    let checkpoint_ms = checkpoint_duration(stats)
        .map(|duration| format!("{:.2}", duration_ms(&duration)))
        .unwrap_or_else(|| "-".to_string());
    println!(
        "{:<10} {:>4} {:<12} {:<12} {:>14} {:>14} {:>14.1} {:>14.1} {:>12.1} {:>12.1} {:>14.2} {:>14} {:>14.2}",
        format!("{}MB", row.target_size_mb),
        row.run_idx + 1,
        row.order.as_str(),
        backend,
        stats.pre_page_count,
        stats.post_page_count,
        pre_size_mb,
        post_size_mb,
        pre_wal_mb,
        post_wal_mb,
        stats.vacuum_statement_duration.as_secs_f64() * 1000.0,
        checkpoint_ms,
        stats.vacuum_duration.as_secs_f64() * 1000.0,
    );
}

fn print_summary(rows: &[BenchmarkRow]) {
    if rows.len() <= unique_target_count(rows) {
        return;
    }

    println!();
    println!(
        "{:<10} {:<12} {:>6} {:>14} {:>14} {:>14} {:>14}",
        "target", "backend", "runs", "min_ms", "median_ms", "mean_ms", "max_ms"
    );
    println!("{}", "-".repeat(94));

    let mut targets = rows
        .iter()
        .map(|row| row.target_size_mb)
        .collect::<Vec<_>>();
    targets.sort_unstable();
    targets.dedup();

    for target in targets {
        let sqlite = rows
            .iter()
            .filter(|row| row.target_size_mb == target)
            .filter_map(|row| row.sqlite.as_ref().map(|stats| stats.vacuum_duration))
            .collect::<Vec<_>>();
        let turso = rows
            .iter()
            .filter(|row| row.target_size_mb == target)
            .filter_map(|row| row.turso.as_ref().map(|stats| stats.vacuum_duration))
            .collect::<Vec<_>>();
        let sqlite_label = rows
            .iter()
            .find(|row| row.target_size_mb == target)
            .and_then(|row| row.sqlite.as_ref())
            .map(sqlite_backend_label)
            .unwrap_or("sqlite");
        if !sqlite.is_empty() {
            print_summary_row(target, sqlite_label, &sqlite);
        }
        if !turso.is_empty() {
            print_summary_row(target, "turso", &turso);
        }
        println!();
    }
}

fn sqlite_backend_label(stats: &BackendStats) -> &'static str {
    if stats.post_vacuum_checkpoint_duration.is_some() {
        "sqlite+ckpt"
    } else {
        "sqlite"
    }
}

fn unique_target_count(rows: &[BenchmarkRow]) -> usize {
    let mut targets = rows
        .iter()
        .map(|row| row.target_size_mb)
        .collect::<Vec<_>>();
    targets.sort_unstable();
    targets.dedup();
    targets.len()
}

fn print_summary_row(target_size_mb: u64, backend: &str, samples: &[Duration]) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let min = sorted.first().copied().unwrap_or_default();
    let max = sorted.last().copied().unwrap_or_default();
    let median = median_duration(&sorted);
    let mean_ms = sorted.iter().map(duration_ms).sum::<f64>() / sorted.len() as f64;
    println!(
        "{:<10} {:<12} {:>6} {:>14.2} {:>14.2} {:>14.2} {:>14.2}",
        format!("{target_size_mb}MB"),
        backend,
        sorted.len(),
        duration_ms(&min),
        duration_ms(&median),
        mean_ms,
        duration_ms(&max),
    );
}

fn median_duration(sorted: &[Duration]) -> Duration {
    match sorted.len() {
        0 => Duration::ZERO,
        len if len % 2 == 1 => sorted[len / 2],
        len => (sorted[(len / 2) - 1] + sorted[len / 2]) / 2,
    }
}

fn duration_ms(duration: &Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn checkpoint_duration(stats: &BackendStats) -> Option<Duration> {
    stats.post_vacuum_checkpoint_duration.or_else(|| {
        stats
            .vacuum_timing
            .as_ref()?
            .entries
            .iter()
            .find(|entry| entry.phase == "checkpoint")
            .map(|entry| entry.duration)
    })
}

fn print_turso_vacuum_timing(rows: &[BenchmarkRow]) {
    if !rows.iter().any(|row| {
        row.turso
            .as_ref()
            .is_some_and(|stats| stats.vacuum_timing.is_some())
    }) {
        return;
    }

    println!();
    println!("turso vacuum phase timings:");
    println!(
        "{:<10} {:>4} {:<28} {:>14} {:>10}",
        "target", "run", "phase", "phase_ms", "profile_%"
    );
    println!("{}", "-".repeat(74));
    for row in rows {
        let Some(timing) = row
            .turso
            .as_ref()
            .and_then(|stats| stats.vacuum_timing.as_ref())
        else {
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
                "{:<10} {:>4} {:<28} {:>14.2} {:>9.1}%",
                format!("{}MB", row.target_size_mb),
                row.run_idx + 1,
                entry.phase,
                phase_ms,
                pct,
            );
        }
        println!(
            "{:<10} {:>4} {:<28} {:>14.2} {:>9.1}%",
            format!("{}MB", row.target_size_mb),
            row.run_idx + 1,
            "profile_total",
            total_ms,
            100.0,
        );
        println!();
    }
}

fn print_turso_target_timing(rows: &[BenchmarkRow]) {
    if !rows.iter().any(|row| {
        row.turso
            .as_ref()
            .is_some_and(|stats| stats.vacuum_target_timing.is_some())
    }) {
        return;
    }

    println!();
    println!("turso target-build phase timings:");
    println!(
        "{:<10} {:>4} {:<32} {:>14} {:>10}",
        "target", "run", "phase", "phase_ms", "target_%"
    );
    println!("{}", "-".repeat(78));
    for row in rows {
        let Some(timing) = row
            .turso
            .as_ref()
            .and_then(|stats| stats.vacuum_target_timing.as_ref())
        else {
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
                "{:<10} {:>4} {:<32} {:>14.2} {:>9.1}%",
                format!("{}MB", row.target_size_mb),
                row.run_idx + 1,
                entry.phase,
                phase_ms,
                pct,
            );
        }
        println!(
            "{:<10} {:>4} {:<32} {:>14.2} {:>9.1}%",
            format!("{}MB", row.target_size_mb),
            row.run_idx + 1,
            "target_total",
            total_ms,
            100.0,
        );
        println!();
    }
}
