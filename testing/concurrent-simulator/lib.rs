/// Whopper is a deterministic simulator for testing the Turso database.
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::{
    generation::Opts,
    model::{
        query::{create::Create, create_index::CreateIndex},
        table::{Column, ColumnType, Index, Table},
    },
};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;
use tracing::{debug, error, trace};
use turso_core::{
    CipherMode, Connection, Database, DatabaseOpts, EncryptionOpts, IO, OpenFlags, Statement, Value,
};
use turso_parser::ast::{ColumnConstraint, SortOrder};

mod io;
mod operations;
pub mod properties;
pub mod workloads;

use crate::{
    io::FILE_SIZE_SOFT_LIMIT,
    properties::Property,
    workloads::{Workload, WorkloadContext},
};
pub use io::{IOFaultConfig, SimulatorIO};
pub use operations::{FiberState, OpContext, Operation};

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

/// A map backed by BTree that supports merge operations with tombstones.
/// Tombstones mark deleted keys and are propagated during merge.
#[derive(Debug, Clone)]
pub struct MergableMap<K: Ord + Clone, V: Clone> {
    /// None value represents a tombstone (deleted key)
    data: BTreeMap<K, Option<V>>,
}

impl<K: Ord + Clone, V: Clone> Default for MergableMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V: Clone> MergableMap<K, V> {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    /// Get a value by key. Returns None if key doesn't exist or is tombstoned.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key).and_then(|v| v.as_ref())
    }

    /// Insert a value. Removes any existing tombstone.
    pub fn insert(&mut self, key: K, value: V) {
        self.data.insert(key, Some(value));
    }

    /// Remove a key by inserting a tombstone.
    pub fn remove(&mut self, key: &K) {
        self.data.insert(key.clone(), None);
    }

    /// Check if a key exists (not tombstoned).
    pub fn contains_key(&self, key: &K) -> bool {
        self.data.get(key).is_some_and(|v| v.is_some())
    }

    /// Pick a random key-value pair within the given bounds.
    /// Returns None if no live entries exist in the range.
    pub fn pick_range(
        &self,
        lower: Bound<&K>,
        upper: Bound<&K>,
        rng: &mut ChaCha8Rng,
    ) -> Option<(&K, &V)> {
        let live_entries: Vec<_> = self
            .data
            .range((lower, upper))
            .filter_map(|(k, v)| v.as_ref().map(|val| (k, val)))
            .collect();

        if live_entries.is_empty() {
            None
        } else {
            let idx = rng.random_range(0..live_entries.len());
            Some(live_entries[idx])
        }
    }

    /// Pick a random key-value pair from the entire map.
    pub fn pick(&self, rng: &mut ChaCha8Rng) -> Option<(&K, &V)> {
        self.pick_range(Bound::Unbounded, Bound::Unbounded, rng)
    }

    /// Merge another map into this one.
    /// Values from `other` overwrite values in `self`.
    /// Tombstones from `other` are copied to `self`.
    pub fn merge(&mut self, other: &Self) {
        for (key, value) in &other.data {
            self.data.insert(key.clone(), value.clone());
        }
    }

    /// Get the number of live (non-tombstoned) entries.
    pub fn len(&self) -> usize {
        self.data.values().filter(|v| v.is_some()).count()
    }

    /// Check if the map has no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over live entries.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.data
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k, val)))
    }

    /// Get all live keys.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }

    /// Compact the map by removing tombstones.
    pub fn compact(&mut self) {
        self.data.retain(|_, v| v.is_some());
    }
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
    /// Properties to check
    pub properties: Vec<Box<dyn Property>>,
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
            workloads: vec![],
            properties: vec![],
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

    pub fn with_properties(mut self, properties: Vec<Box<dyn Property>>) -> Self {
        self.properties = properties;
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

pub struct SimulatorFiber {
    connection: Arc<Connection>,
    state: FiberState,
    statement: RefCell<Option<Statement>>,
    rows: Vec<Vec<Value>>,
    /// Current execution ID for tracing statement lifecycle
    execution_id: Option<u64>,
    /// Current transaction ID for tracing statement lifecycle
    txn_id: Option<u64>,
    /// Current operation being executed
    current_op: Option<Operation>,
}

/// Shared state for simulator that can be accessed by workloads.
#[derive(Debug, Clone)]
pub struct SimulatorState {
    /// Schema tables
    pub tables: MergableMap<String, Table>,
    /// Active indexes: index_name -> table_name
    pub indexes: MergableMap<String, String>,
    /// Simple key-value tables for SimpleSelectWorkload/SimpleInsertWorkload
    pub simple_tables: MergableMap<String, ()>,
    /// Sample of inserted keys per table for use in selects
    pub simple_tables_keys: HashMap<String, SamplesContainer<String>>,
    /// Counter for generating unique execution IDs
    pub execution_id: u64,
    /// Counter for generating unique transaction IDs
    pub txn_id: u64,
}

impl SimulatorState {
    pub fn new(tables: Vec<Table>, indexes: Vec<(String, String)>) -> Self {
        let mut table_map = MergableMap::new();
        for table in &tables {
            table_map.insert(table.name.clone(), table.clone());
        }
        let mut index_map = MergableMap::new();
        for (table_name, index_name) in indexes {
            index_map.insert(index_name, table_name);
        }
        Self {
            tables: table_map,
            indexes: index_map,
            simple_tables: MergableMap::new(),
            simple_tables_keys: HashMap::new(),
            execution_id: 0,
            txn_id: 0,
        }
    }
    pub fn tables_vec(&self) -> Vec<Table> {
        self.tables.iter().map(|(_, t)| t.clone()).collect()
    }
    pub fn gen_execution_id(&mut self) -> u64 {
        self.execution_id += 1;
        self.execution_id
    }
    pub fn gen_txn_id(&mut self) -> u64 {
        self.txn_id += 1;
        self.txn_id
    }
}

struct SimulatorContext {
    fibers: Vec<SimulatorFiber>,
    state: SimulatorState,
    enable_mvcc: bool,
}

/// The Whopper deterministic simulator.
pub struct Whopper {
    context: SimulatorContext,
    io: Arc<dyn IO>,
    file_sizes: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    db_path: String,
    wal_path: String,
    encryption_opts: Option<EncryptionOpts>,
    max_connections: usize,
    workloads: Vec<(u32, Box<dyn Workload>)>,
    properties: Vec<std::sync::Mutex<Box<dyn Property>>>,
    total_weight: u32,
    opts: Opts,
    pub rng: ChaCha8Rng,
    pub current_step: usize,
    pub max_steps: usize,
    pub seed: u64,
    pub stats: Stats,
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

        let indexes_vec: Vec<(String, String)> = indexes
            .iter()
            .map(|idx| (idx.table_name.clone(), idx.index_name.clone()))
            .collect();

        let context = SimulatorContext {
            fibers: vec![],
            state: SimulatorState::new(tables, indexes_vec),
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
            properties: opts
                .properties
                .into_iter()
                .map(std::sync::Mutex::new)
                .collect(),
            total_weight,
            opts: Opts::default(),
            current_step: 0,
            max_steps: opts.max_steps,
            seed,
            stats: Stats::default(),
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
        trace!("step={}", self.current_step);
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
        let exec_id = self.context.fibers[fiber_idx].execution_id;
        let txn_id = self.context.fibers[fiber_idx].txn_id;
        trace!(
            "perform_work: step={}, fiber_idx={}/{} exec_id={:?} txn_id={:?}",
            self.current_step,
            fiber_idx,
            self.context.fibers.len(),
            exec_id,
            txn_id
        );

        // If we have a statement, step it.
        let step_result = {
            let mut stmt_borrow = self.context.fibers[fiber_idx].statement.borrow_mut();
            if let Some(stmt) = stmt_borrow.as_mut() {
                let span = tracing::debug_span!(
                    "step",
                    step = self.current_step,
                    fiber = fiber_idx,
                    exec_id = exec_id,
                    txn_id = txn_id
                );
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
                                    self.context.fibers[fiber_idx].rows.push(values);
                                }
                                Ok(None)
                            }
                            turso_core::StepResult::Done => Ok(Some(())),
                            turso_core::StepResult::Busy => Err(turso_core::LimboError::Busy),
                            _ => Ok(None),
                        }
                    }
                    Err(e) => Err(e),
                }
            } else {
                Ok(Some(()))
            }
        };

        // If the statement has more work, we're done for this simulation step
        if let Ok(None) = step_result {
            return Ok(());
        }

        // drop statement - we finished its execution
        self.context.fibers[fiber_idx].statement.replace(None);
        self.context.fibers[fiber_idx].execution_id = None;

        // get current completed operation
        let completed_op = self.context.fibers[fiber_idx].current_op.take();
        if let Some(completed_op) = completed_op {
            let rows = std::mem::take(&mut self.context.fibers[fiber_idx].rows);
            let current_exec_id = self.context.state.execution_id;
            let mut ctx = OpContext {
                fiber: &mut self.context.fibers[fiber_idx],
                sim_state: &mut self.context.state,
                stats: &mut self.stats,
                rng: &mut self.rng,
            };
            // handle operation result
            let span = tracing::debug_span!(
                "complete",
                step = self.current_step,
                fiber = fiber_idx,
                exec_id = exec_id,
                current_exec_id = current_exec_id,
                txn_id = txn_id
            );
            let _enter = span.enter();
            debug!("result={step_result:?}, rows.len()={}", rows.len());

            if matches!(completed_op, Operation::Begin { .. }) && step_result.is_ok() {
                ctx.fiber.state = FiberState::InTx;
            }

            let txn_id = ctx.fiber.txn_id;

            let op_result = step_result.map(|_| rows);
            completed_op.finish_op(&mut ctx, &op_result);

            for property in &self.properties {
                let mut property = property.lock().unwrap();
                property
                    .finish_op(
                        self.current_step,
                        fiber_idx,
                        txn_id,
                        exec_id.unwrap(),
                        current_exec_id,
                        &completed_op,
                        &op_result,
                    )
                    .inspect_err(|e| error!("property failed: {e}"))?;
            }

            if matches!(&completed_op, Operation::Rollback | Operation::Commit) && op_result.is_ok() {
                ctx.fiber.state = FiberState::Idle;
                ctx.fiber.txn_id = None;
            }

            if let Err(error) = op_result {
                match error {
                    // initiate rollback in case of some errors for fiber within transaction
                    turso_core::LimboError::SchemaUpdated
                    | turso_core::LimboError::TableLocked
                    | turso_core::LimboError::Busy
                    | turso_core::LimboError::BusySnapshot
                    | turso_core::LimboError::WriteWriteConflict
                    | turso_core::LimboError::InvalidArgument(..) => {
                        if matches!(self.context.fibers[fiber_idx].state, FiberState::InTx) {
                            self.context.fibers[fiber_idx].current_op = Some(Operation::Rollback);
                        } else {
                            self.context.fibers[fiber_idx].txn_id = None;
                        }
                    }
                    _ => return Err(error.into()),
                }
            }
        }

        if self.context.fibers[fiber_idx].current_op.is_none() {
            // Generate new operation from workloads list using weighted random selection
            if self.total_weight == 0 {
                return Ok(());
            }

            let mut roll = self.rng.random_range(0..self.total_weight);
            for (weight, workload) in &self.workloads {
                if roll >= *weight {
                    roll = roll.saturating_sub(*weight);
                    continue;
                }
                let fiber = &self.context.fibers[fiber_idx];
                let state_str = format!("{:?}", &fiber.state);
                let span = tracing::debug_span!("generate", fiber = fiber_idx, state = state_str);
                let _enter = span.enter();

                let ctx = WorkloadContext {
                    fiber_state: &fiber.state,
                    sim_state: &self.context.state,
                    opts: &self.opts,
                    enable_mvcc: self.context.enable_mvcc,
                    tables_vec: self.context.state.tables_vec(),
                };

                // Generate operation from workload; skip current workload if it returned None
                let Some(op) = workload.generate(&ctx, &mut self.rng) else {
                    continue;
                };

                debug!("set fiber operation: {:?}", op);
                self.context.fibers[fiber_idx].current_op = Some(op);
                break;
            }
        }

        // initialize new operation
        if let Some(op) = self.context.fibers[fiber_idx].current_op.take() {
            // Assign new execution_id for this statement
            let exec_id = self.context.state.gen_execution_id();

            let fiber = &mut self.context.fibers[fiber_idx];
            let state_str = format!("{:?}", &fiber.state);
            let span = tracing::debug_span!(
                "init",
                step = self.current_step,
                fiber = fiber_idx,
                exec_id = exec_id,
                txn_id = fiber.txn_id,
                state = state_str
            );
            let _enter = span.enter();

            // Prepare the operation
            let mut ctx = OpContext {
                fiber,
                sim_state: &mut self.context.state,
                stats: &mut self.stats,
                rng: &mut self.rng,
            };
            debug!("prepare operation: op={:?}", op);
            if let Err(e) = op.init_op(&mut ctx) {
                let err = e.to_string().to_lowercase();
                // Allow "no such table/index" and "already exists" errors
                if err.contains("no such")
                    || err.contains("already exists")
                    || err.contains("not exist")
                {
                    debug!("init operation skipped: {}", err);
                    return Ok(());
                } else {
                    panic!("Failed to init operation: {}\nSQL: {}", e, op.sql());
                }
            }
            // Store operation and execution_id
            if let Operation::Begin { .. } = &op {
                self.context.fibers[fiber_idx].txn_id = Some(ctx.sim_state.gen_txn_id());
            }
            self.context.fibers[fiber_idx].execution_id = Some(exec_id);
            self.context.fibers[fiber_idx].current_op = Some(op);
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

    /// Reopen the database by closing all connections and recreating them.
    /// This simulates a database restart/reopen scenario.
    /// Active statements are run to completion before closing.
    pub fn reopen(&mut self) -> anyhow::Result<()> {
        debug!(
            "Restarting database, completing active statements for {} fibers",
            self.context.fibers.len()
        );

        let fibers = &mut self.context.fibers;
        // Run all active statements to completion
        while fibers.iter().any(|f| f.statement.borrow().is_some()) {
            for (fiber_idx, fiber) in fibers.iter_mut().enumerate() {
                if fiber.statement.borrow().is_some() {
                    let done = {
                        let span = tracing::debug_span!(
                            "step",
                            step = self.current_step,
                            fiber = fiber_idx
                        );
                        let _enter = span.enter();

                        let mut stmt_borrow = fiber.statement.borrow_mut();
                        if let Some(stmt) = stmt_borrow.as_mut() {
                            match stmt.step() {
                                Ok(result) => match result {
                                    turso_core::StepResult::Row => {
                                        if let Some(row) = stmt.row() {
                                            let values: Vec<Value> =
                                                row.get_values().cloned().collect();
                                            fiber.rows.push(values);
                                        }
                                        false
                                    }
                                    turso_core::StepResult::Done => true,
                                    turso_core::StepResult::Busy => true,
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
                            fiber.rows.len()
                        );
                        fiber.statement.replace(None).unwrap().reset();
                        fiber.rows.clear();
                    }
                }
            }
            self.io.step().unwrap();
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
                rows: vec![],
                execution_id: None,
                txn_id: None,
                current_op: None,
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
