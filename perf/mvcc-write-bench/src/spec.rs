use std::io::{self, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

pub use crate::latency::TxnLatency;

pub const SCHEMA_VERSION: u32 = 2;

pub const CSV_HEADER: &str = "schema_version,engine,topology,workers,threads,batch_size,checkpoint,threshold,stop,repeat,inserts,committed_txns,elapsed_secs,busy,busy_snapshot,schema_updated,log_bytes,wal_bytes,log_peak_bytes,checkpoints_observed,sampled,latency_n,latency_mean_us,latency_p50_us,latency_p95_us,latency_p99_us";

pub(crate) const SCHEMA: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT NOT NULL)";
pub(crate) const INSERT_SQL: &str = "INSERT INTO t (id, data) VALUES (?, ?)";
pub(crate) const PAYLOAD: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

/// One measured configuration. Illegal combinations are unrepresentable.
/// SQLite has no topology and no checkpoint policy. Turso always names both.
pub struct RunSpec {
    pub engine: Engine,
    pub batch_size: NonZeroUsize,
    /// Timed window after warmup.
    pub stop: StopWhen,
    /// Discarded window before the first timed sample. `Duration::ZERO` skips it.
    pub warmup: StopWhen,
    pub repeats: NonZeroUsize,
    /// Database file. The lib deletes `{path}`, `{path}-wal`, `{path}-shm`, and
    /// `{path}` with extension `db-log` before each repeat.
    pub path: PathBuf,
    pub busy_timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Turso(Turso),
    Sqlite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Turso {
    pub checkpoint: CheckpointPolicy,
    pub topology: Topology,
}

/// Truncate is `DatabaseOpts` flag off (auto-checkpoint `CheckpointMode::Truncate`).
/// Passive is `with_experimental_mvcc_passive_checkpoint(true)`.
///
/// Matrices must run every Turso (topology, workers, threshold, batch) cell
/// under **both** policies. Never report a dispatch winner from truncate-only
/// or passive-only data — checkpoint mode is a separate axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointPolicy {
    Truncate(LogThreshold),
    Passive(LogThreshold),
}

/// Maps to `PRAGMA mvcc_checkpoint_threshold`. Default is `4120 * 1000` bytes.
/// Negative disables. `0` checkpoints every commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogThreshold {
    Default,
    EveryCommit,
    Disabled,
    Bytes(NonZeroU64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topology {
    /// One OS thread, N in-flight txs. That thread calls `io.step()` when a
    /// worker parks.
    Cooperative { workers: NonZeroUsize },
    /// Same workers as Cooperative, plus a dedicated thread that only
    /// calls `io.step()` so submit and wait overlap.
    IoPump { workers: NonZeroUsize },
    /// M OS threads, each running Cooperative with `workers_per_thread` workers,
    /// all sharing one `Database` and one `UringIO`. Each thread steps the ring.
    Threads {
        threads: NonZeroUsize,
        workers_per_thread: NonZeroUsize,
    },
    /// Same as Threads, plus one dedicated `io.step()` thread. Worker threads
    /// never call `io.step()`.
    ThreadsPump {
        threads: NonZeroUsize,
        workers_per_thread: NonZeroUsize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopWhen {
    Duration(Duration),
    Transactions(NonZeroU64),
}

/// One CSV line. Identity knobs plus measured counters. Plot scripts read these
/// column names and no others. Throughput is committed rows/s:
/// `inserts / elapsed_secs`, never `committed_txns / elapsed_secs`.
/// Latency is BEGIN→COMMIT wall time of successful txns (p50/p95/p99 µs).
pub struct ResultRow {
    pub engine: EngineLabel,
    pub topology: TopologyLabel,
    pub workers: usize,
    pub threads: usize,
    pub batch_size: usize,
    pub checkpoint: Option<CheckpointLabel>,
    /// Bytes passed to the pragma. `None` on SQLite. `4120 * 1000`
    /// when the spec said `LogThreshold::Default`.
    pub threshold: Option<i64>,
    pub stop: StopWhen,
    pub repeat: u32,
    /// Committed rows. In-flight inserts in an uncommitted tx are not counted.
    pub inserts: u64,
    /// Committed transactions (each of size `batch_size` on the happy path).
    pub committed_txns: u64,
    pub elapsed: Duration,
    pub busy: u64,
    pub busy_snapshot: u64,
    pub schema_updated: u64,
    pub log_bytes: u64,
    pub wal_bytes: u64,
    pub log_peak_bytes: u64,
    pub checkpoints_observed: u32,
    pub sampled: bool,
    /// Successful COMMIT-txn wall times. BEGIN start → COMMIT Done.
    pub latency: TxnLatency,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineLabel {
    Turso,
    Sqlite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyLabel {
    Cooperative,
    IoPump,
    Threads,
    ThreadsPump,
    SqliteWriter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointLabel {
    Truncate,
    Passive,
}

pub struct RepeatReport {
    pub rows: Vec<ResultRow>,
}

pub struct Spread {
    pub min: f64,
    pub max: f64,
    pub p25: f64,
    pub p75: f64,
}

/// Opaque. Does not re-export `turso_core::LimboError` or `rusqlite::Error`.
pub struct BenchError {
    kind: BenchErrorKind,
}

pub(crate) enum BenchErrorKind {
    InvalidSpec(&'static str),
    Engine(String),
    Io(io::Error),
    ThreadPanicked,
}

impl BenchError {
    pub fn invalid_spec(msg: &'static str) -> Self {
        Self {
            kind: BenchErrorKind::InvalidSpec(msg),
        }
    }

    pub fn engine(msg: impl Into<String>) -> Self {
        Self {
            kind: BenchErrorKind::Engine(msg.into()),
        }
    }

    pub(crate) fn io(err: io::Error) -> Self {
        Self {
            kind: BenchErrorKind::Io(err),
        }
    }

    pub(crate) fn thread_panicked() -> Self {
        Self {
            kind: BenchErrorKind::ThreadPanicked,
        }
    }
}

impl std::fmt::Debug for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            BenchErrorKind::InvalidSpec(msg) => write!(f, "invalid spec: {msg}"),
            BenchErrorKind::Engine(msg) => write!(f, "{msg}"),
            BenchErrorKind::Io(err) => write!(f, "{err}"),
            BenchErrorKind::ThreadPanicked => write!(f, "worker thread panicked"),
        }
    }
}

impl std::error::Error for BenchError {}

impl From<io::Error> for BenchError {
    fn from(err: io::Error) -> Self {
        Self::io(err)
    }
}

impl From<turso_core::LimboError> for BenchError {
    fn from(err: turso_core::LimboError) -> Self {
        Self::engine(err.to_string())
    }
}

impl From<rusqlite::Error> for BenchError {
    fn from(err: rusqlite::Error) -> Self {
        Self::engine(err.to_string())
    }
}

impl LogThreshold {
    /// Single source of truth for the pragma integer.
    pub fn to_pragma_i64(self) -> i64 {
        match self {
            LogThreshold::Default => 4120 * 1000,
            LogThreshold::EveryCommit => 0,
            LogThreshold::Disabled => -1,
            LogThreshold::Bytes(n) => i64::try_from(n.get()).expect("threshold fits i64"),
        }
    }
}

impl Engine {
    pub(crate) fn labels(
        &self,
    ) -> (
        EngineLabel,
        TopologyLabel,
        usize,
        usize,
        Option<CheckpointLabel>,
        Option<i64>,
    ) {
        match self {
            Engine::Sqlite => (
                EngineLabel::Sqlite,
                TopologyLabel::SqliteWriter,
                1,
                1,
                None,
                None,
            ),
            Engine::Turso(t) => {
                let (topology, workers, threads) = match t.topology {
                    Topology::Cooperative { workers } => {
                        (TopologyLabel::Cooperative, workers.get(), 1)
                    }
                    Topology::IoPump { workers } => (TopologyLabel::IoPump, workers.get(), 2),
                    Topology::Threads {
                        threads,
                        workers_per_thread,
                    } => (
                        TopologyLabel::Threads,
                        threads.get() * workers_per_thread.get(),
                        threads.get(),
                    ),
                    Topology::ThreadsPump {
                        threads,
                        workers_per_thread,
                    } => (
                        TopologyLabel::ThreadsPump,
                        threads.get() * workers_per_thread.get(),
                        threads.get() + 1,
                    ),
                };
                let (checkpoint, threshold) = match t.checkpoint {
                    CheckpointPolicy::Truncate(th) => {
                        (CheckpointLabel::Truncate, th.to_pragma_i64())
                    }
                    CheckpointPolicy::Passive(th) => (CheckpointLabel::Passive, th.to_pragma_i64()),
                };
                (
                    EngineLabel::Turso,
                    topology,
                    workers,
                    threads,
                    Some(checkpoint),
                    Some(threshold),
                )
            }
        }
    }
}

impl EngineLabel {
    pub(crate) fn csv(self) -> &'static str {
        match self {
            EngineLabel::Turso => "turso",
            EngineLabel::Sqlite => "sqlite",
        }
    }
}

impl TopologyLabel {
    pub(crate) fn csv(self) -> &'static str {
        match self {
            TopologyLabel::Cooperative => "coop",
            TopologyLabel::IoPump => "io-pump",
            TopologyLabel::Threads => "threads",
            TopologyLabel::ThreadsPump => "threads-pump",
            TopologyLabel::SqliteWriter => "sqlite-writer",
        }
    }
}

impl CheckpointLabel {
    pub(crate) fn csv(self) -> &'static str {
        match self {
            CheckpointLabel::Truncate => "truncate",
            CheckpointLabel::Passive => "passive",
        }
    }
}

impl StopWhen {
    pub(crate) fn csv(self) -> String {
        match self {
            StopWhen::Duration(d) => format!("duration_ms={}", d.as_millis()),
            StopWhen::Transactions(n) => format!("txns={}", n.get()),
        }
    }
}

impl ResultRow {
    pub fn write_csv_line(&self, w: &mut impl Write) -> io::Result<()> {
        let checkpoint = self.checkpoint.map(CheckpointLabel::csv).unwrap_or("");
        let threshold = self.threshold.map(|t| t.to_string()).unwrap_or_default();
        let sampled = if self.sampled { 1 } else { 0 };
        writeln!(
            w,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            SCHEMA_VERSION,
            self.engine.csv(),
            self.topology.csv(),
            self.workers,
            self.threads,
            self.batch_size,
            checkpoint,
            threshold,
            self.stop.csv(),
            self.repeat,
            self.inserts,
            self.committed_txns,
            self.elapsed.as_secs_f64(),
            self.busy,
            self.busy_snapshot,
            self.schema_updated,
            self.log_bytes,
            self.wal_bytes,
            self.log_peak_bytes,
            self.checkpoints_observed,
            sampled,
            self.latency.n,
            self.latency.mean_us,
            self.latency.p50_us,
            self.latency.p95_us,
            self.latency.p99_us,
        )
    }

    /// Rows per second: committed `inserts / elapsed`. Not transactions/s.
    pub(crate) fn throughput(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.inserts as f64 / secs
        }
    }
}

impl RepeatReport {
    pub fn median_throughput(&self) -> f64 {
        let mut xs: Vec<f64> = self.rows.iter().map(ResultRow::throughput).collect();
        if xs.is_empty() {
            return 0.0;
        }
        xs.sort_by(|a, b| a.total_cmp(b));
        percentile(&xs, 0.5)
    }

    pub fn spread(&self) -> Spread {
        let mut xs: Vec<f64> = self.rows.iter().map(ResultRow::throughput).collect();
        if xs.is_empty() {
            return Spread {
                min: 0.0,
                max: 0.0,
                p25: 0.0,
                p75: 0.0,
            };
        }
        xs.sort_by(|a, b| a.total_cmp(b));
        Spread {
            min: xs[0],
            max: xs[xs.len() - 1],
            p25: percentile(&xs, 0.25),
            p75: percentile(&xs, 0.75),
        }
    }

    pub fn write_csv(&self, mut w: impl Write) -> io::Result<()> {
        writeln!(w, "{CSV_HEADER}")?;
        for row in &self.rows {
            row.write_csv_line(&mut w)?;
        }
        Ok(())
    }
}

impl RunSpec {
    pub(crate) fn check_invariants(&self) -> Result<(), BenchError> {
        if self.busy_timeout.is_zero() {
            return Err(BenchError::invalid_spec("busy_timeout must be > 0"));
        }
        Ok(())
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = p * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let w = idx - lo as f64;
        sorted[lo] * (1.0 - w) + sorted[hi] * w
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_pump_labels_two_threads() {
        let engine = Engine::Turso(Turso {
            checkpoint: CheckpointPolicy::Truncate(LogThreshold::Disabled),
            topology: Topology::IoPump {
                workers: NonZeroUsize::new(8).unwrap(),
            },
        });
        let (_, topology, workers, threads, _, threshold) = engine.labels();
        assert!(matches!(topology, TopologyLabel::IoPump));
        assert_eq!(workers, 8);
        assert_eq!(threads, 2);
        assert_eq!(threshold, Some(-1));
    }

    #[test]
    fn sqlite_engine_has_no_checkpoint_fields() {
        let (engine, topology, workers, threads, checkpoint, threshold) = Engine::Sqlite.labels();
        assert!(matches!(engine, EngineLabel::Sqlite));
        assert!(matches!(topology, TopologyLabel::SqliteWriter));
        assert_eq!(workers, 1);
        assert_eq!(threads, 1);
        assert!(checkpoint.is_none());
        assert!(threshold.is_none());
    }

    #[test]
    fn sqlite_csv_line_leaves_checkpoint_and_threshold_empty() {
        let row = ResultRow {
            engine: EngineLabel::Sqlite,
            topology: TopologyLabel::SqliteWriter,
            workers: 1,
            threads: 1,
            batch_size: 100,
            checkpoint: None,
            threshold: None,
            stop: StopWhen::Duration(Duration::from_secs(10)),
            repeat: 0,
            inserts: 1,
            committed_txns: 1,
            elapsed: Duration::from_secs(1),
            busy: 0,
            busy_snapshot: 0,
            schema_updated: 0,
            log_bytes: 0,
            wal_bytes: 0,
            log_peak_bytes: 0,
            checkpoints_observed: 0,
            sampled: false,
            latency: TxnLatency::default(),
        };
        let mut buf = Vec::new();
        row.write_csv_line(&mut buf).unwrap();
        let line = String::from_utf8(buf).unwrap();
        assert_eq!(
            line,
            "2,sqlite,sqlite-writer,1,1,100,,,duration_ms=10000,0,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0\n"
        );
    }

    #[test]
    fn throughput_is_committed_rows_per_sec_not_transactions() {
        let row = ResultRow {
            engine: EngineLabel::Sqlite,
            topology: TopologyLabel::SqliteWriter,
            workers: 1,
            threads: 1,
            batch_size: 100,
            checkpoint: None,
            threshold: None,
            stop: StopWhen::Duration(Duration::from_secs(1)),
            repeat: 0,
            inserts: 10_000,
            committed_txns: 100,
            elapsed: Duration::from_secs(1),
            busy: 0,
            busy_snapshot: 0,
            schema_updated: 0,
            log_bytes: 0,
            wal_bytes: 0,
            log_peak_bytes: 0,
            checkpoints_observed: 0,
            sampled: false,
            latency: TxnLatency::default(),
        };
        assert_eq!(row.throughput(), 10_000.0);
        assert_ne!(row.throughput(), row.committed_txns as f64);
    }

    #[test]
    fn log_threshold_to_pragma_i64() {
        assert_eq!(LogThreshold::Default.to_pragma_i64(), 4_120_000);
        assert_eq!(LogThreshold::EveryCommit.to_pragma_i64(), 0);
        assert_eq!(LogThreshold::Disabled.to_pragma_i64(), -1);
        assert_eq!(
            LogThreshold::Bytes(NonZeroU64::new(99).unwrap()).to_pragma_i64(),
            99
        );
    }
}
