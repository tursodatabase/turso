/// Whopper is a deterministic simulator for testing the Turso database.
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::{
    generation::{Arbitrary, GenerationContext, Opts},
    model::{
        query::{
            create::Create, create_index::CreateIndex, delete::Delete, drop_index::DropIndex,
            insert::Insert, select::Select, update::Update,
        },
        table::{Column, ColumnType, Index, Table},
    },
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};
use turso_core::{
    CipherMode, Connection, Database, DatabaseOpts, EncryptionOpts, IO, OpenFlags, Statement, Value,
};
use turso_parser::ast::{ColumnConstraint, SortOrder};

mod io;
use crate::io::FILE_SIZE_SOFT_LIMIT;
pub use io::{IOFaultConfig, SimulatorIO};

/// A bounded container for sampling values with reservoir sampling.
#[derive(Debug, Clone)]
pub struct SamplesContainer<T> {
    samples: Vec<T>,
    capacity: usize,
    /// Counter for reservoir sampling
    total_added: usize,
}

impl<T> SamplesContainer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            samples: Vec::with_capacity(capacity),
            capacity,
            total_added: 0,
        }
    }

    /// Add a sample. Uses reservoir sampling to maintain bounded memory.
    pub fn add(&mut self, value: T, rng: &mut ChaCha8Rng) {
        self.total_added += 1;
        if self.samples.len() < self.capacity {
            self.samples.push(value);
        } else {
            // Reservoir sampling: replace with probability capacity/total_added
            let idx = rng.random_range(0..self.total_added);
            if idx < self.capacity {
                self.samples[idx] = value;
            }
        }
    }

    /// Pick a random sample, if any exist.
    pub fn pick(&self, rng: &mut ChaCha8Rng) -> Option<&T> {
        if self.samples.is_empty() {
            None
        } else {
            Some(&self.samples[rng.random_range(0..self.samples.len())])
        }
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

/// State of a simulator fiber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberState {
    Idle,
    InTx,
}

/// Context passed to workloads for initialization.
/// Note: `rng` is passed separately to `Workload::init` to avoid borrow conflicts
/// when calling `Arbitrary::arbitrary(rng, ctx)` which needs both `&mut rng` and `&ctx`.
pub struct WorkloadContext<'a> {
    pub connection: &'a Arc<Connection>,
    pub state: &'a mut FiberState,
    pub statement: &'a RefCell<Option<Statement>>,
    pub tables: &'a Vec<Table>,
    pub indexes: &'a mut Vec<(String, String)>,
    pub simple_tables: &'a mut Vec<String>,
    /// Sample of inserted keys per table for use in selects
    pub simple_tables_keys: &'a mut HashMap<String, SamplesContainer<String>>,
    pub stats: &'a mut Stats,
    pub opts: &'a Opts,
    pub enable_mvcc: bool,
}

impl GenerationContext for WorkloadContext<'_> {
    fn tables(&self) -> &Vec<Table> {
        self.tables
    }

    fn opts(&self) -> &Opts {
        self.opts
    }
}

impl WorkloadContext<'_> {
    /// Prepare a statement on this fiber.
    /// Returns true if successful, false if table doesn't exist.
    /// Panics on other errors.
    pub fn prepare(&self, sql: &str) -> bool {
        match self.connection.prepare(sql) {
            Ok(stmt) => {
                self.statement.replace(Some(stmt));
                true
            }
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                // Allow "no such table" errors - return false to skip this workload
                if err_str.contains("no such table") || err_str.contains("no such index") {
                    debug!("table/index not found, skipping: {}", err_str);
                    false
                } else if err_str.contains("already exists") {
                    debug!("table/index already exists, skipping: {}", err_str);
                    false
                } else {
                    panic!("Failed to prepare statement: {}\nSQL: {}", e, sql);
                }
            }
        }
    }
}

/// A workload is a unit of work that can be executed on a fiber.
/// Returns Ok(true) if the fiber was initialized, Ok(false) if this
/// workload couldn't be applied and another should be tried.
pub trait Workload: Send + Sync {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool>;
}

// ============================================================================
// Default Workload Implementations
// ============================================================================

/// Begin a new transaction.
pub struct BeginWorkload;

impl Workload for BeginWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if *ctx.state != FiberState::Idle {
            return Ok(false);
        }
        let cmd = if ctx.enable_mvcc {
            match rng.random_range(0..3) {
                0 => "BEGIN DEFERRED",
                1 => "BEGIN IMMEDIATE",
                _ => "BEGIN CONCURRENT",
            }
        } else {
            "BEGIN"
        };
        ctx.prepare(cmd);
        *ctx.state = FiberState::InTx;
        debug!("BEGIN: {}", cmd);
        Ok(true)
    }
}

/// Run PRAGMA integrity_check.
pub struct IntegrityCheckWorkload;

impl Workload for IntegrityCheckWorkload {
    fn init(&self, ctx: &mut WorkloadContext, _rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if *ctx.state != FiberState::Idle {
            return Ok(false);
        }
        ctx.prepare("PRAGMA integrity_check");
        ctx.stats.integrity_checks += 1;
        debug!("PRAGMA integrity_check");
        Ok(true)
    }
}

/// Create a new simple key-value table and record its name in state.
pub struct CreateSimpleTableWorkload;

impl Workload for CreateSimpleTableWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        // Only create tables outside of transactions
        if *ctx.state != FiberState::Idle {
            return Ok(false);
        }
        let table_name = format!("simple_kv_{}", rng.random_range(0..100000));
        let sql =
            format!("CREATE TABLE IF NOT EXISTS {table_name} (key TEXT PRIMARY KEY, value BLOB)");
        ctx.prepare(&sql);
        ctx.simple_tables.push(table_name.clone());
        debug!("CREATE SIMPLE TABLE: {}", table_name);
        Ok(true)
    }
}

/// Execute a simple SELECT by key on a random simple table (point lookup).
pub struct SimpleSelectWorkload;

impl Workload for SimpleSelectWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if ctx.simple_tables.is_empty() {
            return Ok(false);
        }
        let table_idx = rng.random_range(0..ctx.simple_tables.len());
        let table_name = ctx.simple_tables[table_idx].clone();

        // 70% chance to use a known inserted key if available
        let key = if rng.random_bool(0.7) {
            ctx.simple_tables_keys
                .get(&table_name)
                .and_then(|keys| keys.pick(rng).cloned())
                .unwrap_or_else(|| format!("key_{}", rng.random_range(0..10000)))
        } else {
            format!("key_{}", rng.random_range(0..10000))
        };

        let sql = format!("SELECT value FROM {table_name} WHERE key = '{key}'");
        if !ctx.prepare(&sql) {
            return Ok(false);
        }
        debug!("SIMPLE SELECT: {}", sql);
        Ok(true)
    }
}

/// Execute a simple INSERT into a random simple table.
pub struct SimpleInsertWorkload;

/// Maximum number of keys to remember per table
const MAX_SAMPLE_KEYS_PER_TABLE: usize = 1000;

impl Workload for SimpleInsertWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if ctx.simple_tables.is_empty() {
            return Ok(false);
        }
        let table_idx = rng.random_range(0..ctx.simple_tables.len());
        let table_name = ctx.simple_tables[table_idx].clone();
        let key = format!("key_{}", rng.random_range(0..10000));
        let value_len = rng.random_range(10..100);
        let value: String = (0..value_len)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect();
        let value_hex = hex::encode(value.as_bytes());
        let sql = format!(
            "INSERT OR REPLACE INTO {table_name} (key, value) VALUES ('{key}', X'{value_hex}')"
        );
        if !ctx.prepare(&sql) {
            return Ok(false);
        }
        ctx.stats.inserts += 1;

        // Remember the key for later selects
        ctx.simple_tables_keys
            .entry(table_name.clone())
            .or_insert_with(|| SamplesContainer::new(MAX_SAMPLE_KEYS_PER_TABLE))
            .add(key.clone(), rng);

        debug!("SIMPLE INSERT: table={}, key={}", table_name, key);
        Ok(true)
    }
}

/// Execute a SELECT query (works in both Idle and InTx states).
pub struct SelectWorkload;

impl Workload for SelectWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        let select = Select::arbitrary(rng, ctx);
        let sql = select.to_string();
        ctx.prepare(&sql);
        debug!("SELECT: {}", sql);
        Ok(true)
    }
}

/// Execute an INSERT statement (works in both Idle and InTx states).
pub struct InsertWorkload;

impl Workload for InsertWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        let insert = Insert::arbitrary(rng, ctx);
        let sql = insert.to_string();
        ctx.prepare(&sql);
        ctx.stats.inserts += 1;
        debug!("INSERT: {}", sql);
        Ok(true)
    }
}

/// Execute an UPDATE statement (works in both Idle and InTx states).
pub struct UpdateWorkload;

impl Workload for UpdateWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        let update = Update::arbitrary(rng, ctx);
        let sql = update.to_string();
        ctx.prepare(&sql);
        ctx.stats.updates += 1;
        debug!("UPDATE: {}", sql);
        Ok(true)
    }
}

/// Execute a DELETE statement (works in both Idle and InTx states).
pub struct DeleteWorkload;

impl Workload for DeleteWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        let delete = Delete::arbitrary(rng, ctx);
        let sql = delete.to_string();
        ctx.prepare(&sql);
        ctx.stats.deletes += 1;
        debug!("DELETE: {}", sql);
        Ok(true)
    }
}

/// Create a new index (works in both Idle and InTx states).
pub struct CreateIndexWorkload;

impl Workload for CreateIndexWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        let create_index = CreateIndex::arbitrary(rng, ctx);
        let sql = create_index.to_string();
        ctx.prepare(&sql);
        ctx.indexes.push((
            create_index.index.table_name.clone(),
            create_index.index_name.clone(),
        ));
        debug!("CREATE INDEX: {}", sql);
        Ok(true)
    }
}

/// Drop an existing index (works in both Idle and InTx states).
pub struct DropIndexWorkload;

impl Workload for DropIndexWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if ctx.indexes.is_empty() {
            return Ok(false);
        }
        let index_idx = rng.random_range(0..ctx.indexes.len());
        let (table_name, index_name) = ctx.indexes.remove(index_idx);
        let drop_index = DropIndex {
            table_name,
            index_name,
        };
        let sql = drop_index.to_string();
        ctx.prepare(&sql);
        debug!("DROP INDEX: {}", sql);
        Ok(true)
    }
}

/// Run WAL checkpoint with a randomly selected mode.
pub struct WalCheckpointWorkload;

impl Workload for WalCheckpointWorkload {
    fn init(&self, ctx: &mut WorkloadContext, rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        // Checkpoint should only run when not in a transaction
        if *ctx.state != FiberState::Idle {
            return Ok(false);
        }
        let mode = match rng.random_range(0..4) {
            0 => "PASSIVE",
            1 => "FULL",
            2 => "RESTART",
            _ => "TRUNCATE",
        };
        let sql = format!("PRAGMA wal_checkpoint({mode})");
        ctx.prepare(&sql);
        debug!("WAL CHECKPOINT: {}", mode);
        Ok(true)
    }
}

/// Commit the current transaction.
pub struct CommitWorkload;

impl Workload for CommitWorkload {
    fn init(&self, ctx: &mut WorkloadContext, _rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if *ctx.state != FiberState::InTx {
            return Ok(false);
        }
        ctx.prepare("COMMIT");
        *ctx.state = FiberState::Idle;
        debug!("COMMIT");
        Ok(true)
    }
}

/// Rollback the current transaction.
pub struct RollbackWorkload;

impl Workload for RollbackWorkload {
    fn init(&self, ctx: &mut WorkloadContext, _rng: &mut ChaCha8Rng) -> anyhow::Result<bool> {
        if *ctx.state != FiberState::InTx {
            return Ok(false);
        }
        ctx.prepare("ROLLBACK");
        *ctx.state = FiberState::Idle;
        debug!("ROLLBACK");
        Ok(true)
    }
}

/// Returns the default workload configuration matching the original probabilities.
pub fn default_workloads() -> Vec<(u32, Box<dyn Workload>)> {
    vec![
        // Idle-only workloads
        (10, Box::new(IntegrityCheckWorkload)),
        (5, Box::new(WalCheckpointWorkload)),
        (10, Box::new(CreateSimpleTableWorkload)),
        (20, Box::new(SimpleSelectWorkload)),
        (20, Box::new(SimpleInsertWorkload)),
        // DML workloads (work in both Idle and InTx)
        (1, Box::new(SelectWorkload)),
        (30, Box::new(InsertWorkload)),
        (20, Box::new(UpdateWorkload)),
        (10, Box::new(DeleteWorkload)),
        (2, Box::new(CreateIndexWorkload)),
        (2, Box::new(DropIndexWorkload)),
        // InTx-only workloads
        (30, Box::new(BeginWorkload)),
        (10, Box::new(CommitWorkload)),
        (10, Box::new(RollbackWorkload)),
    ]
}

/// Configuration options for the Whopper simulator.
pub struct WhopperOpts {
    /// Random seed for deterministic simulation. If None, a random seed is generated.
    pub seed: Option<u64>,
    /// Maximum number of concurrent connections (1-8 typical).
    pub max_connections: usize,
    /// Maximum number of simulation steps to run.
    pub max_steps: usize,
    /// Probability of cosmic ray bit flip on each step (0.0-1.0).
    pub cosmic_ray_probability: f64,
    /// Keep mmap I/O files on disk after run.
    pub keep_files: bool,
    /// Enable MVCC (Multi-Version Concurrency Control).
    pub enable_mvcc: bool,
    /// Enable database encryption with random cipher.
    pub enable_encryption: bool,
    /// Workloads with weights: (weight, workload). Higher weight = more likely.
    pub workloads: Vec<(u32, Box<dyn Workload>)>,
}

impl Default for WhopperOpts {
    fn default() -> Self {
        Self {
            seed: None,
            max_connections: 4,
            max_steps: 100_000,
            cosmic_ray_probability: 0.0,
            keep_files: false,
            enable_mvcc: false,
            enable_encryption: false,
            workloads: default_workloads(),
        }
    }
}

impl WhopperOpts {
    /// Create options for "fast" mode: 100k steps, no cosmic rays.
    pub fn fast() -> Self {
        Self {
            max_steps: 100_000,
            ..Default::default()
        }
    }

    /// Create options for "chaos" mode: 10M steps, no cosmic rays.
    pub fn chaos() -> Self {
        Self {
            max_steps: 10_000_000,
            ..Default::default()
        }
    }

    /// Create options for "ragnarök" mode: 1M steps, 0.01% cosmic ray probability.
    pub fn ragnarok() -> Self {
        Self {
            max_steps: 1_000_000,
            cosmic_ray_probability: 0.0001,
            ..Default::default()
        }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps;
        self
    }

    pub fn with_cosmic_ray_probability(mut self, probability: f64) -> Self {
        self.cosmic_ray_probability = probability;
        self
    }

    pub fn with_keep_files(mut self, keep: bool) -> Self {
        self.keep_files = keep;
        self
    }

    pub fn with_enable_mvcc(mut self, enable: bool) -> Self {
        self.enable_mvcc = enable;
        self
    }

    pub fn with_enable_encryption(mut self, enable: bool) -> Self {
        self.enable_encryption = enable;
        self
    }

    pub fn with_workloads(mut self, workloads: Vec<(u32, Box<dyn Workload>)>) -> Self {
        self.workloads = workloads;
        self
    }
}

/// Statistics collected during simulation.
#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub integrity_checks: usize,
}

/// Result of a single simulation step.
#[derive(Debug)]
pub enum StepResult {
    /// Step completed normally.
    Ok,
    /// WAL file size exceeded soft limit, simulation should stop.
    WalSizeLimitExceeded,
}

struct SimulatorFiber {
    connection: Arc<Connection>,
    state: FiberState,
    statement: RefCell<Option<Statement>>,
    rows_fetched: Vec<Vec<Value>>,
    /// Current execution ID for tracing statement lifecycle
    execution_id: Option<u64>,
}

struct SimulatorContext {
    fibers: Vec<SimulatorFiber>,
    tables: Vec<Table>,
    indexes: Vec<(String, String)>,
    simple_tables: Vec<String>,
    /// Sample of inserted keys per table for use in selects
    simple_tables_keys: HashMap<String, SamplesContainer<String>>,
    enable_mvcc: bool,
}

/// The Whopper deterministic simulator.
pub struct Whopper {
    context: SimulatorContext,
    rng: ChaCha8Rng,
    io: Arc<dyn IO>,
    file_sizes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    db_path: String,
    wal_path: String,
    encryption_opts: Option<EncryptionOpts>,
    max_connections: usize,
    workloads: Vec<(u32, Box<dyn Workload>)>,
    total_weight: u32,
    opts: Opts,
    pub current_step: usize,
    pub max_steps: usize,
    pub seed: u64,
    pub stats: Stats,
    /// Counter for generating unique execution IDs
    next_execution_id: u64,
}

impl Whopper {
    /// Create a new Whopper simulator with the given options.
    pub fn new(opts: WhopperOpts) -> anyhow::Result<Self> {
        let seed = opts.seed.unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.next_u64()
        });

        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Create a separate RNG for IO operations with a derived seed
        let io_rng = ChaCha8Rng::seed_from_u64(seed.wrapping_add(1));

        let fault_config = IOFaultConfig {
            cosmic_ray_probability: opts.cosmic_ray_probability,
        };

        let simulator_io = Arc::new(SimulatorIO::new(opts.keep_files, io_rng, fault_config));
        let file_sizes = simulator_io.file_sizes();
        let io = simulator_io as Arc<dyn IO>;

        let db_path = format!("whopper-{}-{}.db", seed, std::process::id());
        let wal_path = format!("{db_path}-wal");

        let encryption_opts = if opts.enable_encryption {
            Some(random_encryption_config(&mut rng))
        } else {
            None
        };

        let db = {
            let db_opts = DatabaseOpts::new().with_encryption(encryption_opts.is_some());

            match Database::open_file_with_flags(
                io.clone(),
                &db_path,
                OpenFlags::default(),
                db_opts,
                encryption_opts.clone(),
            ) {
                Ok(db) => db,
                Err(e) => {
                    return Err(anyhow::anyhow!("Database open failed: {}", e));
                }
            }
        };

        let bootstrap_conn = match db.connect() {
            Ok(conn) => may_be_set_encryption(conn, &encryption_opts)?,
            Err(e) => {
                return Err(anyhow::anyhow!("Connection failed: {}", e));
            }
        };

        // Enable MVCC if requested
        if opts.enable_mvcc {
            bootstrap_conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")?;
        }

        let schema = create_initial_schema(&mut rng);
        let tables = schema.iter().map(|t| t.table.clone()).collect::<Vec<_>>();
        for create_table in &schema {
            let sql = create_table.to_string();
            debug!("{}", sql);
            bootstrap_conn.execute(&sql)?;
        }

        let indexes = create_initial_indexes(&mut rng, &tables);
        for create_index in &indexes {
            let sql = create_index.to_string();
            debug!("{}", sql);
            bootstrap_conn.execute(&sql)?;
        }

        bootstrap_conn.close()?;

        let context = SimulatorContext {
            fibers: vec![],
            tables,
            indexes: indexes
                .iter()
                .map(|idx| (idx.table_name.clone(), idx.index_name.clone()))
                .collect(),
            simple_tables: vec![],
            simple_tables_keys: HashMap::new(),
            enable_mvcc: opts.enable_mvcc,
        };

        let total_weight: u32 = opts.workloads.iter().map(|(w, _)| w).sum();

        let mut whopper = Self {
            context,
            rng,
            io,
            file_sizes,
            db_path,
            wal_path,
            encryption_opts,
            max_connections: opts.max_connections,
            workloads: opts.workloads,
            total_weight,
            opts: Opts::default(),
            current_step: 0,
            max_steps: opts.max_steps,
            seed,
            stats: Stats::default(),
            next_execution_id: 0,
        };

        whopper.open_connections()?;

        Ok(whopper)
    }

    /// Check if the simulation is complete (reached max steps or WAL limit).
    pub fn is_done(&self) -> bool {
        self.current_step >= self.max_steps
    }

    /// Perform a single simulation step.
    /// Returns `StepResult::Ok` if the step completed normally,
    /// or `StepResult::WalSizeLimitExceeded` if the WAL file exceeded the soft limit.
    pub fn step(&mut self) -> anyhow::Result<StepResult> {
        if self.current_step >= self.max_steps {
            return Ok(StepResult::Ok);
        }

        let fiber_idx = self.current_step % self.context.fibers.len();
        self.perform_work(fiber_idx)?;
        self.io.step()?;
        self.current_step += 1;

        if file_size_soft_limit_exceeded(&self.wal_path, self.file_sizes.clone()) {
            return Ok(StepResult::WalSizeLimitExceeded);
        }

        Ok(StepResult::Ok)
    }

    fn perform_work(&mut self, fiber_idx: usize) -> anyhow::Result<()> {
        // If we have a statement, step it.
        let execution_id = self.context.fibers[fiber_idx].execution_id;
        let done = {
            let mut stmt_borrow = self.context.fibers[fiber_idx].statement.borrow_mut();
            if let Some(stmt) = stmt_borrow.as_mut() {
                let span = tracing::debug_span!("step", fiber = fiber_idx, exec = execution_id);
                let _enter = span.enter();

                let step_result = stmt.step();
                match step_result {
                    Ok(result) => {
                        trace!("{:?}", result);
                        match result {
                            turso_core::StepResult::Row => {
                                if let Some(row) = stmt.row() {
                                    let values: Vec<Value> = row.get_values().cloned().collect();
                                    drop(stmt_borrow);
                                    self.context.fibers[fiber_idx].rows_fetched.push(values);
                                } else {
                                    drop(stmt_borrow);
                                }
                                false
                            }
                            turso_core::StepResult::Done => true,
                            _ => false,
                        }
                    }
                    Err(e) => match e {
                        turso_core::LimboError::SchemaUpdated => {
                            drop(stmt_borrow);
                            let rows_discarded = self.context.fibers[fiber_idx].rows_fetched.len();
                            debug!(rows_discarded, "error SchemaUpdated");
                            self.context.fibers[fiber_idx].statement.replace(None);
                            self.context.fibers[fiber_idx].rows_fetched.clear();
                            self.context.fibers[fiber_idx].execution_id = None;
                            if matches!(self.context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) = self.context.fibers[fiber_idx]
                                    .connection
                                    .prepare("ROLLBACK")
                            {
                                self.context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                self.context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        turso_core::LimboError::Busy => {
                            drop(stmt_borrow);
                            let rows_discarded = self.context.fibers[fiber_idx].rows_fetched.len();
                            debug!(rows_discarded, "error Busy");
                            self.context.fibers[fiber_idx].statement.replace(None);
                            self.context.fibers[fiber_idx].rows_fetched.clear();
                            self.context.fibers[fiber_idx].execution_id = None;
                            if matches!(self.context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) = self.context.fibers[fiber_idx]
                                    .connection
                                    .prepare("ROLLBACK")
                            {
                                self.context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                self.context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        turso_core::LimboError::WriteWriteConflict => {
                            drop(stmt_borrow);
                            let rows_discarded = self.context.fibers[fiber_idx].rows_fetched.len();
                            debug!(rows_discarded, "error WriteWriteConflict");
                            self.context.fibers[fiber_idx].statement.replace(None);
                            self.context.fibers[fiber_idx].rows_fetched.clear();
                            self.context.fibers[fiber_idx].execution_id = None;
                            self.context.fibers[fiber_idx].state = FiberState::Idle;
                            return Ok(());
                        }
                        turso_core::LimboError::BusySnapshot => {
                            drop(stmt_borrow);
                            let rows_discarded = self.context.fibers[fiber_idx].rows_fetched.len();
                            debug!(rows_discarded, "error BusySnapshot");
                            self.context.fibers[fiber_idx].statement.replace(None);
                            self.context.fibers[fiber_idx].rows_fetched.clear();
                            self.context.fibers[fiber_idx].execution_id = None;
                            if matches!(self.context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) = self.context.fibers[fiber_idx]
                                    .connection
                                    .prepare("ROLLBACK")
                            {
                                self.context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                self.context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        _ => {
                            return Err(e.into());
                        }
                    },
                }
            } else {
                true
            }
        };

        // If the statement has more work, we're done for this simulation step
        if !done {
            return Ok(());
        }

        // Statement completed - log with span
        {
            let span = tracing::debug_span!("done", fiber = fiber_idx, exec = execution_id);
            let _enter = span.enter();
            let rows = self.context.fibers[fiber_idx].rows_fetched.len();
            debug!(
                rows,
                "completed: {:?}", self.context.fibers[fiber_idx].rows_fetched
            );
        }
        self.context.fibers[fiber_idx].statement.replace(None);
        self.context.fibers[fiber_idx].rows_fetched.clear();
        self.context.fibers[fiber_idx].execution_id = None;

        // Select a workload using weighted random selection
        if self.total_weight == 0 {
            return Ok(());
        }

        let mut roll = self.rng.random_range(0..self.total_weight);
        for (weight, workload) in &self.workloads {
            if roll < *weight {
                // Assign new execution_id for this statement
                let exec_id = self.next_execution_id;
                self.next_execution_id += 1;

                let fiber = &mut self.context.fibers[fiber_idx];
                let state = format!("{:?}", &fiber.state);
                let span =
                    tracing::debug_span!("init", fiber = fiber_idx, exec = exec_id, state = state);
                let _enter = span.enter();

                let mut ctx = WorkloadContext {
                    connection: &fiber.connection,
                    state: &mut fiber.state,
                    statement: &fiber.statement,
                    tables: &self.context.tables,
                    indexes: &mut self.context.indexes,
                    simple_tables: &mut self.context.simple_tables,
                    simple_tables_keys: &mut self.context.simple_tables_keys,
                    stats: &mut self.stats,
                    opts: &self.opts,
                    enable_mvcc: self.context.enable_mvcc,
                };
                if workload.init(&mut ctx, &mut self.rng)? {
                    // Statement was successfully prepared, store execution_id
                    self.context.fibers[fiber_idx].execution_id = Some(exec_id);
                    return Ok(());
                }
                // If workload returned false, continue to try next
            }
            roll = roll.saturating_sub(*weight);
        }

        Ok(())
    }

    /// Run the simulation to completion (up to max_steps or WAL limit).
    pub fn run(&mut self) -> anyhow::Result<()> {
        while !self.is_done() {
            match self.step()? {
                StepResult::Ok => {}
                StepResult::WalSizeLimitExceeded => break,
            }
        }
        Ok(())
    }

    /// Restart the database by closing all connections and reopening them.
    /// This simulates a database restart/reopen scenario.
    /// Active statements are run to completion before closing.
    pub fn restart(&mut self) -> anyhow::Result<()> {
        debug!(
            "Restarting database, completing active statements for {} fibers",
            self.context.fibers.len()
        );

        // Run all active statements to completion
        for (fiber_idx, fiber) in self.context.fibers.iter_mut().enumerate() {
            while fiber.statement.borrow().is_some() {
                let done = {
                    let mut stmt_borrow = fiber.statement.borrow_mut();
                    if let Some(stmt) = stmt_borrow.as_mut() {
                        match stmt.step() {
                            Ok(result) => match result {
                                turso_core::StepResult::Row => {
                                    if let Some(row) = stmt.row() {
                                        let values: Vec<Value> =
                                            row.get_values().cloned().collect();
                                        fiber.rows_fetched.push(values);
                                    }
                                    false
                                }
                                turso_core::StepResult::Done => true,
                                _ => false,
                            },
                            Err(_) => true, // On error, consider statement done
                        }
                    } else {
                        true
                    }
                };
                if done {
                    debug!(
                        "fiber {}: completed with {} rows before restart",
                        fiber_idx,
                        fiber.rows_fetched.len()
                    );
                    fiber.statement.replace(None);
                    fiber.rows_fetched.clear();
                    break;
                }
            }
        }

        // Close and drop all fiber connections to release database Arc references
        {
            let fibers = self.context.fibers.drain(..).collect::<Vec<_>>();
            for fiber in fibers {
                // Drop statement first
                drop(fiber.statement.into_inner());
                // Close and drop connection
                if let Err(e) = fiber.connection.close() {
                    debug!("Error closing connection during restart: {}", e);
                }
                drop(fiber.connection);
            }
            // All fibers are now dropped, database Arc should be released
        }

        // Reopen connections (creates new Database instance)
        self.open_connections()?;

        debug!(
            "Database restarted with {} fibers",
            self.context.fibers.len()
        );
        Ok(())
    }

    /// Open database connections for all fibers.
    fn open_connections(&mut self) -> anyhow::Result<()> {
        let db_opts = DatabaseOpts::new().with_encryption(self.encryption_opts.is_some());
        let db = Database::open_file_with_flags(
            self.io.clone(),
            &self.db_path,
            OpenFlags::default(),
            db_opts,
            self.encryption_opts.clone(),
        )
        .map_err(|e| anyhow::anyhow!("Database open failed: {}", e))?;

        for i in 0..self.max_connections {
            let conn = db
                .connect()
                .map_err(|e| anyhow::anyhow!("Failed to create fiber connection {}: {}", i, e))?;
            let conn = may_be_set_encryption(conn, &self.encryption_opts)?;
            self.context.fibers.push(SimulatorFiber {
                connection: conn,
                state: FiberState::Idle,
                statement: RefCell::new(None),
                rows_fetched: vec![],
                execution_id: None,
            });
        }

        Ok(())
    }
}

fn may_be_set_encryption(
    conn: Arc<Connection>,
    opts: &Option<EncryptionOpts>,
) -> anyhow::Result<Arc<Connection>> {
    if let Some(opts) = opts {
        conn.pragma_update("cipher", format!("'{}'", opts.cipher.clone()))?;
        conn.pragma_update("hexkey", format!("'{}'", opts.hexkey.clone()))?;
    }
    Ok(conn)
}

fn create_initial_indexes(rng: &mut ChaCha8Rng, tables: &[Table]) -> Vec<CreateIndex> {
    let mut indexes = Vec::new();

    // Create 0-3 indexes per table
    for table in tables {
        let num_indexes = rng.random_range(0..=3);
        for i in 0..num_indexes {
            if !table.columns.is_empty() {
                // Pick 1-3 columns for the index
                let num_columns = rng.random_range(1..=std::cmp::min(3, table.columns.len()));
                let mut selected_columns = Vec::new();
                let mut available_columns = table.columns.clone();

                for _ in 0..num_columns {
                    if available_columns.is_empty() {
                        break;
                    }
                    let col_idx = rng.random_range(0..available_columns.len());
                    let column = available_columns.remove(col_idx);
                    let sort_order = if rng.random_bool(0.5) {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    };
                    selected_columns.push((column.name, sort_order));
                }

                if !selected_columns.is_empty() {
                    let index_name = format!("idx_{}_{}", table.name, i);
                    let create_index = CreateIndex {
                        index: Index {
                            index_name,
                            table_name: table.name.clone(),
                            columns: selected_columns,
                        },
                    };
                    indexes.push(create_index);
                }
            }
        }
    }

    indexes
}

fn create_initial_schema(rng: &mut ChaCha8Rng) -> Vec<Create> {
    let mut schema = Vec::new();

    // Generate random number of tables (1-5)
    let num_tables = rng.random_range(1..=5);

    for i in 0..num_tables {
        let table_name = format!("table_{i}");

        // Generate random number of columns (2-8)
        let num_columns = rng.random_range(2..=8);
        let mut columns = Vec::new();

        // TODO: there is no proper unique generation yet in whopper, so disable primary keys for now
        columns.push(Column {
            name: "id".to_string(),
            column_type: ColumnType::Integer,
            constraints: vec![],
        });

        // Add random columns
        for j in 1..num_columns {
            let col_type = match rng.random_range(0..3) {
                0 => ColumnType::Integer,
                1 => ColumnType::Text,
                _ => ColumnType::Float,
            };

            // FIXME: before sql_generation did not incorporate ColumnConstraint into the sql string
            // now it does and it the simulation here fails `whopper` with UNIQUE CONSTRAINT ERROR
            // 20% chance of unique
            let constraints = if rng.random_bool(0.0) {
                vec![ColumnConstraint::Unique(None)]
            } else {
                Vec::new()
            };

            columns.push(Column {
                name: format!("col_{j}"),
                column_type: col_type,
                constraints,
            });
        }

        let table = Table {
            name: table_name,
            columns,
            rows: vec![],
            indexes: vec![],
        };

        schema.push(Create { table });
    }
    schema
}

fn random_encryption_config(rng: &mut ChaCha8Rng) -> EncryptionOpts {
    let cipher_modes = [
        CipherMode::Aes128Gcm,
        CipherMode::Aes256Gcm,
        CipherMode::Aegis256,
        CipherMode::Aegis128L,
        CipherMode::Aegis128X2,
        CipherMode::Aegis128X4,
        CipherMode::Aegis256X2,
        CipherMode::Aegis256X4,
    ];

    let cipher_mode = cipher_modes[rng.random_range(0..cipher_modes.len())];

    let key_size = cipher_mode.required_key_size();
    let mut key = vec![0u8; key_size];
    rng.fill_bytes(&mut key);

    EncryptionOpts {
        cipher: cipher_mode.to_string(),
        hexkey: hex::encode(&key),
    }
}

fn file_size_soft_limit_exceeded(
    wal_path: &str,
    file_sizes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
) -> bool {
    let wal_size = {
        let sizes = file_sizes.lock().unwrap();
        sizes.get(wal_path).cloned().unwrap_or(0)
    };
    wal_size > FILE_SIZE_SOFT_LIMIT
}
