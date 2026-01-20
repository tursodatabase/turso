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
use tracing::trace;
use turso_core::{
    CipherMode, Connection, Database, DatabaseOpts, EncryptionOpts, IO, OpenFlags, Statement,
};
use turso_parser::ast::{ColumnConstraint, SortOrder};

mod io;
use crate::io::FILE_SIZE_SOFT_LIMIT;
pub use io::{IOFaultConfig, SimulatorIO};

/// Configuration options for the Whopper simulator.
#[derive(Debug, Clone)]
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
    /// Disable creation and manipulation of indexes.
    pub disable_indexes: bool,
    /// Enable MVCC (Multi-Version Concurrency Control).
    pub enable_mvcc: bool,
    /// Enable database encryption with random cipher.
    pub enable_encryption: bool,
}

impl Default for WhopperOpts {
    fn default() -> Self {
        Self {
            seed: None,
            max_connections: 4,
            max_steps: 100_000,
            cosmic_ray_probability: 0.0,
            keep_files: false,
            disable_indexes: false,
            enable_mvcc: false,
            enable_encryption: false,
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

    pub fn with_disable_indexes(mut self, disable: bool) -> Self {
        self.disable_indexes = disable;
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

#[derive(Debug)]
enum FiberState {
    Idle,
    InTx,
}

struct SimulatorFiber {
    connection: Arc<Connection>,
    state: FiberState,
    statement: RefCell<Option<Statement>>,
}

struct SimulatorContext {
    fibers: Vec<SimulatorFiber>,
    tables: Vec<Table>,
    indexes: Vec<(String, String)>,
    opts: Opts,
    stats: Stats,
    disable_indexes: bool,
    enable_mvcc: bool,
}

impl GenerationContext for SimulatorContext {
    fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    fn opts(&self) -> &Opts {
        &self.opts
    }
}

/// The Whopper deterministic simulator.
pub struct Whopper {
    context: SimulatorContext,
    rng: ChaCha8Rng,
    io: Arc<dyn IO>,
    file_sizes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    wal_path: String,
    current_step: usize,
    max_steps: usize,
    seed: u64,
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
            trace!("{}", sql);
            bootstrap_conn.execute(&sql)?;
        }

        let indexes = if opts.disable_indexes {
            Vec::new()
        } else {
            create_initial_indexes(&mut rng, &tables)
        };
        for create_index in &indexes {
            let sql = create_index.to_string();
            trace!("{}", sql);
            bootstrap_conn.execute(&sql)?;
        }

        bootstrap_conn.close()?;

        let mut fibers = Vec::new();
        for i in 0..opts.max_connections {
            let conn = match db.connect() {
                Ok(conn) => may_be_set_encryption(conn, &encryption_opts)?,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to create fiber connection {}: {}",
                        i,
                        e
                    ));
                }
            };
            fibers.push(SimulatorFiber {
                connection: conn,
                state: FiberState::Idle,
                statement: RefCell::new(None),
            });
        }

        let context = SimulatorContext {
            fibers,
            tables,
            indexes: indexes
                .iter()
                .map(|idx| (idx.table_name.clone(), idx.index_name.clone()))
                .collect(),
            opts: Opts::default(),
            stats: Stats::default(),
            disable_indexes: opts.disable_indexes,
            enable_mvcc: opts.enable_mvcc,
        };

        Ok(Self {
            context,
            rng,
            io,
            file_sizes,
            wal_path,
            current_step: 0,
            max_steps: opts.max_steps,
            seed,
        })
    }

    /// Get the seed used for this simulation.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Get the current step number.
    pub fn current_step(&self) -> usize {
        self.current_step
    }

    /// Get the maximum number of steps.
    pub fn max_steps(&self) -> usize {
        self.max_steps
    }

    /// Get a reference to the current statistics.
    pub fn stats(&self) -> &Stats {
        &self.context.stats
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
        perform_work(fiber_idx, &mut self.rng, &mut self.context)?;
        self.io.step()?;
        self.current_step += 1;

        if file_size_soft_limit_exceeded(&self.wal_path, self.file_sizes.clone()) {
            return Ok(StepResult::WalSizeLimitExceeded);
        }

        Ok(StepResult::Ok)
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

fn perform_work(
    fiber_idx: usize,
    rng: &mut ChaCha8Rng,
    context: &mut SimulatorContext,
) -> anyhow::Result<()> {
    // If we have a statement, step it.
    let done = {
        let mut stmt_borrow = context.fibers[fiber_idx].statement.borrow_mut();
        if let Some(stmt) = stmt_borrow.as_mut() {
            let step_result = {
                let span = tracing::debug_span!(
                    "fiber",
                    fiber_idx = fiber_idx,
                    state = format!("{:?}", context.fibers[fiber_idx].state)
                );
                let _enter = span.enter();
                stmt.step()
            };
            match step_result {
                Ok(result) => {
                    tracing::debug!("fiber: {} {:?}", fiber_idx, result);
                    matches!(result, turso_core::StepResult::Done)
                }
                Err(e) => {
                    match e {
                        turso_core::LimboError::SchemaUpdated => {
                            trace!("{} Schema changed, rolling back transaction", fiber_idx);
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            // Rollback the transaction if we're in one
                            if matches!(context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) =
                                    context.fibers[fiber_idx].connection.prepare("ROLLBACK")
                            {
                                context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        turso_core::LimboError::Busy => {
                            trace!("{} Database busy, rolling back transaction", fiber_idx);
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            // Rollback the transaction if we're in one
                            if matches!(context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) =
                                    context.fibers[fiber_idx].connection.prepare("ROLLBACK")
                            {
                                context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        turso_core::LimboError::WriteWriteConflict => {
                            trace!(
                                "{} Write-write conflict, transaction automatically rolled back",
                                fiber_idx
                            );
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            context.fibers[fiber_idx].state = FiberState::Idle;
                            return Ok(());
                        }
                        turso_core::LimboError::BusySnapshot => {
                            trace!("{} Stale snapshot, rolling back transaction", fiber_idx);
                            drop(stmt_borrow);
                            context.fibers[fiber_idx].statement.replace(None);
                            if matches!(context.fibers[fiber_idx].state, FiberState::InTx)
                                && let Ok(rollback_stmt) =
                                    context.fibers[fiber_idx].connection.prepare("ROLLBACK")
                            {
                                context.fibers[fiber_idx]
                                    .statement
                                    .replace(Some(rollback_stmt));
                                context.fibers[fiber_idx].state = FiberState::Idle;
                            }
                            return Ok(());
                        }
                        _ => {
                            return Err(e.into());
                        }
                    }
                }
            }
        } else {
            true
        }
    };
    // If the statement has more work, we're done for this simulation step
    if !done {
        return Ok(());
    }
    context.fibers[fiber_idx].statement.replace(None);
    match context.fibers[fiber_idx].state {
        FiberState::Idle => {
            let action = rng.random_range(0..100);
            if action <= 29 {
                let begin_cmd = if context.enable_mvcc {
                    match rng.random_range(0..3) {
                        0 => "BEGIN DEFERRED",
                        1 => "BEGIN IMMEDIATE",
                        _ => "BEGIN CONCURRENT",
                    }
                } else {
                    "BEGIN"
                };
                if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare(begin_cmd) {
                    context.fibers[fiber_idx].statement.replace(Some(stmt));
                    context.fibers[fiber_idx].state = FiberState::InTx;
                    trace!("fiber: {} {}", fiber_idx, begin_cmd);
                }
            } else if action == 30 {
                // Integrity check
                if let Ok(stmt) = context.fibers[fiber_idx]
                    .connection
                    .prepare("PRAGMA integrity_check")
                {
                    context.fibers[fiber_idx].statement.replace(Some(stmt));
                    context.stats.integrity_checks += 1;
                    trace!("fiber: {} PRAGMA integrity_check", fiber_idx);
                }
            }
        }
        FiberState::InTx => {
            let action = rng.random_range(0..100);
            match action {
                0..=9 => {
                    // SELECT (10%)
                    let select = Select::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(select.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                    }
                    trace!("fiber: {} SELECT: {}", fiber_idx, select.to_string());
                }
                10..=39 => {
                    // INSERT (30%)
                    let insert = Insert::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(insert.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.stats.inserts += 1;
                    }
                    trace!("fiber: {} INSERT: {}", fiber_idx, insert.to_string());
                }
                40..=59 => {
                    // UPDATE (20%)
                    let update = Update::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(update.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.stats.updates += 1;
                    }
                    trace!("fiber: {} UPDATE: {}", fiber_idx, update.to_string());
                }
                60..=69 => {
                    // DELETE (10%)
                    let delete = Delete::arbitrary(rng, context);
                    if let Ok(stmt) = context.fibers[fiber_idx]
                        .connection
                        .prepare(delete.to_string())
                    {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.stats.deletes += 1;
                    }
                    trace!("fiber: {} DELETE: {}", fiber_idx, delete.to_string());
                }
                70..=71 => {
                    // CREATE INDEX (2%)
                    if !context.disable_indexes {
                        let create_index = CreateIndex::arbitrary(rng, context);
                        let sql = create_index.to_string();
                        if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare(&sql) {
                            context.fibers[fiber_idx].statement.replace(Some(stmt));
                            context.indexes.push((
                                create_index.index.table_name.clone(),
                                create_index.index_name.clone(),
                            ));
                        }
                        trace!("fiber: {} CREATE INDEX: {}", fiber_idx, sql);
                    }
                }
                72..=73 => {
                    // DROP INDEX (2%)
                    if !context.disable_indexes && !context.indexes.is_empty() {
                        let index_idx = rng.random_range(0..context.indexes.len());
                        let (table_name, index_name) = context.indexes.remove(index_idx);
                        let drop_index = DropIndex {
                            table_name,
                            index_name,
                        };
                        let sql = drop_index.to_string();
                        if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare(&sql) {
                            context.fibers[fiber_idx].statement.replace(Some(stmt));
                        }
                        trace!("fiber: {} DROP INDEX: {}", fiber_idx, sql);
                    }
                }
                74..=86 => {
                    // COMMIT (13%)
                    if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare("COMMIT") {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.fibers[fiber_idx].state = FiberState::Idle;
                    }
                    trace!("fiber: {} COMMIT", fiber_idx);
                }
                87..=99 => {
                    // ROLLBACK (13%)
                    if let Ok(stmt) = context.fibers[fiber_idx].connection.prepare("ROLLBACK") {
                        context.fibers[fiber_idx].statement.replace(Some(stmt));
                        context.fibers[fiber_idx].state = FiberState::Idle;
                    }
                    trace!("fiber: {} ROLLBACK", fiber_idx);
                }
                _ => {}
            }
        }
    }
    Ok(())
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
