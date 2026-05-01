//! Multiprocess coordinator for Whopper.
//!
//! Spawns N worker processes, each opening the same on-disk database with real
//! filesystem I/O. Reuses the existing workload generation and property
//! validation infrastructure while exercising the full multiprocess coordination
//! stack (shared byte-range locks, .tshm shared memory, WAL reader slots, MVCC
//! tx slots).

use serde::Serialize;
use std::fs::{File, create_dir_all};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::generation::Opts;
use tracing::{debug, error, info};
use turso_core::{Database, DatabaseOpts, LimboError, OpenFlags};

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::operations::{FiberState, OpResult, Operation, TxMode};
use crate::properties::Property;
use crate::protocol::{
    WorkerCommand, WorkerResponse, WorkerSharedWalSnapshot, WorkerStartupTelemetry,
};
use crate::workloads::{Workload, WorkloadContext};
use crate::{
    SimulatorState, Stats, StepResult, create_initial_indexes, create_initial_schema,
    multiprocess_platform_io,
};

/// Configuration for the multiprocess simulator.
pub struct MultiprocessOpts {
    pub seed: Option<u64>,
    pub enable_mvcc: bool,
    pub process_count: usize,
    pub connections_per_process: usize,
    pub max_steps: usize,
    pub elle_tables: Vec<(String, String)>,
    pub workloads: Vec<(u32, Box<dyn Workload>)>,
    pub properties: Vec<Box<dyn Property>>,
    pub chaotic_profiles: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)>,
    pub kill_probability: f64,
    pub restart_probability: f64,
    pub history_output: Option<PathBuf>,
    pub keep_files: bool,
}

struct OperationHistoryWriter {
    output: Option<BufWriter<File>>,
}

impl OperationHistoryWriter {
    fn new(output_path: Option<&Path>) -> anyhow::Result<Self> {
        let output = if let Some(path) = output_path {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    create_dir_all(parent)?;
                }
            }
            Some(BufWriter::new(File::create(path)?))
        } else {
            None
        };
        Ok(Self { output })
    }

    fn record(&mut self, event: &HistoryEvent) -> anyhow::Result<()> {
        let Some(output) = self.output.as_mut() else {
            return Ok(());
        };

        serde_json::to_writer(&mut *output, event)?;
        output.write_all(b"\n")?;
        output.flush()?;
        Ok(())
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HistoryEvent {
    WorkerSpawned {
        step: Option<usize>,
        process: usize,
        pid: u32,
        reason: &'static str,
        telemetry: WorkerStartupTelemetry,
    },
    WorkerKilled {
        step: usize,
        process: usize,
        pid: u32,
        reason: &'static str,
    },
    WorkerStateAborted {
        step: usize,
        process: usize,
        connection: usize,
        reason: &'static str,
        txn_id: Option<u64>,
        exec_id: Option<u64>,
        fiber_state: String,
        current_op: Option<String>,
    },
    CohortRestartStarted {
        step: usize,
        process_count: usize,
        connection_count: usize,
    },
    OperationStarted {
        step: usize,
        process: usize,
        connection: usize,
        exec_id: u64,
        txn_id: Option<u64>,
        op: String,
        sql: String,
    },
    OperationFinished {
        step: usize,
        process: usize,
        connection: usize,
        exec_id: u64,
        txn_id: Option<u64>,
        op: String,
        sql: String,
        result: HistoryResult,
    },
    OperationTransportFailure {
        step: usize,
        process: usize,
        connection: usize,
        exec_id: Option<u64>,
        txn_id: Option<u64>,
        op: Option<String>,
        sql: Option<String>,
        error: String,
    },
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "status", rename_all = "snake_case")]
enum HistoryResult {
    Ok { rows: Vec<Vec<turso_core::Value>> },
    Err { error_kind: String, message: String },
}

impl From<&OpResult> for HistoryResult {
    fn from(result: &OpResult) -> Self {
        match result {
            Ok(rows) => Self::Ok { rows: rows.clone() },
            Err(error) => Self::Err {
                error_kind: crate::protocol::limbo_error_to_kind(error).to_string(),
                message: error.to_string(),
            },
        }
    }
}

/// Handle for a worker child process.
struct WorkerProcessHandle {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    responses: Receiver<anyhow::Result<WorkerResponse>>,
    process_idx: usize,
}

#[derive(Debug, Clone, Copy)]
struct LogicalConnectionLocation {
    process_idx: usize,
    connection_idx: usize,
}

/// Mirror of each logical connection's state, maintained by the coordinator.
struct ConnectionState {
    fiber_state: FiberState,
    txn_id: Option<u64>,
    execution_id: Option<u64>,
    current_op: Option<Operation>,
    chaotic_workload: Option<Box<dyn ChaoticWorkload>>,
    last_chaotic_result: Option<OpResult>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            fiber_state: FiberState::Idle,
            txn_id: None,
            execution_id: None,
            current_op: None,
            chaotic_workload: None,
            last_chaotic_result: None,
        }
    }
}

/// The multiprocess Whopper coordinator.
pub struct MultiprocessWhopper {
    processes: Vec<WorkerProcessHandle>,
    connection_states: Vec<ConnectionState>,
    sim_state: SimulatorState,
    worker_startup_telemetries: Vec<WorkerStartupTelemetry>,
    history: OperationHistoryWriter,
    workloads: Vec<(u32, Box<dyn Workload>)>,
    properties: Vec<std::sync::Mutex<Box<dyn Property>>>,
    chaotic_profiles: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)>,
    total_weight: u32,
    opts: Opts,
    pub rng: ChaCha8Rng,
    pub current_step: usize,
    pub max_steps: usize,
    pub seed: u64,
    pub stats: Stats,
    db_path: PathBuf,
    enable_mvcc: bool,
    connections_per_process: usize,
    kill_probability: f64,
    restart_probability: f64,
    keep_files: bool,
}

impl MultiprocessWhopper {
    /// Create a new multiprocess coordinator.
    /// Bootstraps the database schema, then spawns worker processes.
    pub fn new(opts: MultiprocessOpts) -> anyhow::Result<Self> {
        if opts.process_count == 0 {
            return Err(anyhow::anyhow!("process_count must be greater than zero"));
        }
        if opts.connections_per_process == 0 {
            return Err(anyhow::anyhow!(
                "connections_per_process must be greater than zero"
            ));
        }
        let seed = opts.seed.unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.next_u64()
        });
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Create database file on disk.
        // Use an atomic counter alongside the timestamp to guarantee uniqueness
        // even when parallel test threads call SystemTime::now() within the same
        // clock tick.
        static PATH_COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_nanos();
        let counter = PATH_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let db_path = std::env::temp_dir().join(format!(
            "whopper-mp-{}-{}-{}-{}.db",
            seed,
            std::process::id(),
            unique_suffix,
            counter
        ));
        info!("multiprocess db: {}", db_path.display());

        // Bootstrap schema using real I/O
        {
            let io = multiprocess_platform_io()?;
            let db = Database::open_file_with_flags(
                io,
                db_path.to_str().unwrap(),
                OpenFlags::default(),
                DatabaseOpts::new().with_multiprocess_wal(true),
                None,
            )?;
            let conn = db.connect()?;

            if opts.enable_mvcc {
                conn.execute("PRAGMA journal_mode = 'mvcc'")?;
            }

            let schema = create_initial_schema(&mut rng);
            let tables = schema.iter().map(|t| t.table.clone()).collect::<Vec<_>>();
            for create_table in &schema {
                conn.execute(create_table.to_string())?;
            }

            let indexes = create_initial_indexes(&mut rng, &tables);
            for create_index in &indexes {
                conn.execute(create_index.to_string())?;
            }

            for (_, create_sql) in &opts.elle_tables {
                conn.execute(create_sql)?;
            }

            // Checkpoint to ensure all schema changes are in the main DB file
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

            conn.close()?;
            // db + io dropped here, releasing all file locks
        }

        // Remove the coordination file - it gets invalidated (truncated to 0) on close.
        // Workers will recreate it when they open the database.
        // The WAL and log files contain actual data and must be kept.
        let db_str = db_path.to_str().unwrap_or_default();
        let _ = std::fs::remove_file(format!("{db_str}-tshm"));

        // Build initial simulator state from the schema we just created
        let mut schema_rng = ChaCha8Rng::seed_from_u64(seed);
        let schema = create_initial_schema(&mut schema_rng);
        let tables = schema.iter().map(|t| t.table.clone()).collect::<Vec<_>>();
        let indexes = create_initial_indexes(&mut schema_rng, &tables);
        let indexes_vec: Vec<(String, String)> = indexes
            .iter()
            .map(|idx| (idx.table_name.clone(), idx.index_name.clone()))
            .collect();
        let mut sim_state = SimulatorState::new(tables, indexes_vec);
        for (table_name, _) in &opts.elle_tables {
            sim_state.elle_tables.insert(table_name.clone(), ());
        }

        let total_weight: u32 = opts.workloads.iter().map(|(w, _)| w).sum();
        let total_connections = opts
            .process_count
            .checked_mul(opts.connections_per_process)
            .ok_or_else(|| anyhow::anyhow!("multiprocess connection topology overflow"))?;

        // Spawn worker processes one at a time.
        // Each worker must fully initialize (open DB, create .tshm) before the next
        // starts, to avoid races on coordination file creation.
        let mut processes = Vec::new();
        let mut connection_states = Vec::with_capacity(total_connections);
        let mut worker_startup_telemetries = Vec::new();
        let mut history = OperationHistoryWriter::new(opts.history_output.as_deref())?;
        for process_idx in 0..opts.process_count {
            let (handle, telemetry) = spawn_ready_worker(
                process_idx,
                &db_path,
                opts.enable_mvcc,
                opts.connections_per_process,
            )?;
            debug!("process {} ready: {:?}", handle.process_idx, telemetry);
            history.record(&HistoryEvent::WorkerSpawned {
                step: None,
                process: process_idx,
                pid: handle.child.id(),
                reason: "initial_spawn",
                telemetry,
            })?;
            worker_startup_telemetries.push(telemetry);
            processes.push(handle);
            for _ in 0..opts.connections_per_process {
                connection_states.push(ConnectionState::new());
            }
        }

        info!(
            "all {} processes ready ({} total connections)",
            processes.len(),
            connection_states.len()
        );

        Ok(Self {
            processes,
            connection_states,
            sim_state,
            worker_startup_telemetries,
            history,
            workloads: opts.workloads,
            properties: opts
                .properties
                .into_iter()
                .map(std::sync::Mutex::new)
                .collect(),
            chaotic_profiles: opts.chaotic_profiles,
            total_weight,
            opts: Opts::default(),
            rng,
            current_step: 0,
            max_steps: opts.max_steps,
            seed,
            stats: Stats::default(),
            db_path,
            enable_mvcc: opts.enable_mvcc,
            connections_per_process: opts.connections_per_process,
            kill_probability: opts.kill_probability,
            restart_probability: opts.restart_probability,
            keep_files: opts.keep_files,
        })
    }

    /// Check if the simulation is complete.
    pub fn is_done(&self) -> bool {
        self.current_step >= self.max_steps
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn worker_startup_telemetries(&self) -> &[WorkerStartupTelemetry] {
        &self.worker_startup_telemetries
    }

    fn connection_location(&self, logical_connection_idx: usize) -> LogicalConnectionLocation {
        LogicalConnectionLocation {
            process_idx: logical_connection_idx / self.connections_per_process,
            connection_idx: logical_connection_idx % self.connections_per_process,
        }
    }

    fn process_connection_indices(&self, process_idx: usize) -> std::ops::Range<usize> {
        let start = process_idx * self.connections_per_process;
        start..start + self.connections_per_process
    }

    /// Execute SQL directly on one logical connection without involving workload generation.
    /// This is used by deterministic restart regressions.
    pub fn execute_sql_direct(
        &mut self,
        connection_idx: usize,
        sql: impl Into<String>,
    ) -> anyhow::Result<OpResult> {
        let sql = sql.into();
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::Execute {
                connection_idx: location.connection_idx,
                sql,
            },
        )?;
        let response = recv_response(&mut self.processes[location.process_idx])?;
        Ok(response.into_op_result())
    }

    /// Execute SQL on an idle logical connection that is not currently inside a simulated transaction.
    pub fn execute_sql_via_idle_worker(
        &mut self,
        sql: impl Into<String>,
    ) -> anyhow::Result<(usize, OpResult)> {
        let connection_idx = self
            .connection_states
            .iter()
            .enumerate()
            .find_map(|(idx, state)| {
                (state.fiber_state == FiberState::Idle
                    && state.txn_id.is_none()
                    && state.execution_id.is_none()
                    && state.current_op.is_none()
                    && state.chaotic_workload.is_none()
                    && state.last_chaotic_result.is_none())
                .then_some(idx)
            })
            .ok_or_else(|| anyhow::anyhow!("no idle logical connection available for probe"))?;
        let result = self.execute_sql_direct(connection_idx, sql)?;
        Ok((connection_idx, result))
    }

    pub fn disable_auto_checkpoint_direct(&mut self, connection_idx: usize) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::DisableAutoCheckpoint {
                connection_idx: location.connection_idx,
            },
        )?;
        match recv_response(&mut self.processes[location.process_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "logical connection {} returned unexpected response to DisableAutoCheckpoint: {:?}",
                connection_idx,
                other
            )),
        }
    }

    pub fn passive_checkpoint_direct(
        &mut self,
        connection_idx: usize,
        upper_bound_inclusive: Option<u64>,
    ) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::PassiveCheckpoint {
                connection_idx: location.connection_idx,
                upper_bound_inclusive,
            },
        )?;
        match recv_response(&mut self.processes[location.process_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "logical connection {} returned unexpected response to PassiveCheckpoint: {:?}",
                connection_idx,
                other
            )),
        }
    }

    pub fn clear_backfill_proof_direct(&mut self, connection_idx: usize) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::ClearBackfillProof,
        )?;
        match recv_response(&mut self.processes[location.process_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "logical connection {} returned unexpected response to ClearBackfillProof: {:?}",
                connection_idx,
                other
            )),
        }
    }

    pub fn install_unpublished_backfill_proof_direct(
        &mut self,
        connection_idx: usize,
        upper_bound_inclusive: u64,
    ) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::InstallUnpublishedBackfillProof {
                connection_idx: location.connection_idx,
                upper_bound_inclusive,
            },
        )?;
        match recv_response(&mut self.processes[location.process_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "logical connection {} returned unexpected response to InstallUnpublishedBackfillProof: {:?}",
                connection_idx,
                other
            )),
        }
    }

    pub fn shared_wal_snapshot_direct(
        &mut self,
        connection_idx: usize,
    ) -> anyhow::Result<Option<WorkerSharedWalSnapshot>> {
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::ReadSharedWalSnapshot,
        )?;
        match recv_response(&mut self.processes[location.process_idx])? {
            WorkerResponse::SharedWalSnapshot { snapshot } => Ok(snapshot),
            other => Err(anyhow::anyhow!(
                "logical connection {} returned unexpected response to ReadSharedWalSnapshot: {:?}",
                connection_idx,
                other
            )),
        }
    }

    pub fn find_frame_for_page_direct(
        &mut self,
        connection_idx: usize,
        page_id: u64,
    ) -> anyhow::Result<Option<u64>> {
        let location = self.connection_location(connection_idx);
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::FindFrameForPage { page_id },
        )?;
        match recv_response(&mut self.processes[location.process_idx])? {
            WorkerResponse::FrameLookup { frame_id } => Ok(frame_id),
            other => Err(anyhow::anyhow!(
                "logical connection {} returned unexpected response to FindFrameForPage: {:?}",
                connection_idx,
                other
            )),
        }
    }

    /// Execute SQL through a brand-new worker process, then shut it down.
    /// This is useful for probing reopen behavior without perturbing the live cohort.
    pub fn execute_sql_via_fresh_worker(
        &self,
        sql: impl Into<String>,
    ) -> anyhow::Result<(WorkerStartupTelemetry, OpResult)> {
        let sql = sql.into();
        let probe_worker_id = self.processes.len();
        let (mut worker, telemetry) =
            spawn_ready_worker(probe_worker_id, &self.db_path, self.enable_mvcc, 1)?;

        let result = (|| -> anyhow::Result<OpResult> {
            for _ in 0..8 {
                send_command(
                    &mut worker,
                    &WorkerCommand::Execute {
                        connection_idx: 0,
                        sql: sql.clone(),
                    },
                )?;
                let response = recv_response(&mut worker)?;
                let op_result = response.into_op_result();
                match &op_result {
                    Err(
                        LimboError::SchemaUpdated
                        | LimboError::SchemaConflict
                        | LimboError::Busy
                        | LimboError::BusySnapshot
                        | LimboError::TableLocked,
                    ) => continue,
                    _ => return Ok(op_result),
                }
            }
            Ok(Err(LimboError::Busy))
        })();

        let _ = send_command(&mut worker, &WorkerCommand::Shutdown);
        let _ = worker.child.wait();

        result.map(|op_result| (telemetry, op_result))
    }

    /// Restart the entire worker cohort while preserving the on-disk database,
    /// WAL, and tshm artifacts.
    pub fn restart_all_workers_preserve_files(&mut self) -> anyhow::Result<()> {
        info!(
            "restarting all workers while preserving {}",
            self.db_path.display()
        );
        self.history.record(&HistoryEvent::CohortRestartStarted {
            step: self.current_step,
            process_count: self.processes.len(),
            connection_count: self.connection_states.len(),
        })?;

        self.stop_all_workers_preserve_files("cohort_restart")?;
        self.respawn_all_workers_preserve_files("cohort_restart")
    }

    pub fn mutate_on_disk_and_restart(
        &mut self,
        mutate: impl FnOnce(&Path) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        info!(
            "stopping workers for on-disk mutation of {}",
            self.db_path.display()
        );
        self.stop_all_workers_preserve_files("on_disk_mutation")?;
        mutate(&self.db_path)?;
        self.respawn_all_workers_preserve_files("on_disk_mutation")
    }

    fn stop_all_workers_preserve_files(&mut self, reason: &'static str) -> anyhow::Result<()> {
        for process_idx in 0..self.processes.len() {
            let pid = self.processes[process_idx].child.id();
            self.history.record(&HistoryEvent::WorkerKilled {
                step: self.current_step,
                process: process_idx,
                pid,
                reason,
            })?;
            let _ = self.processes[process_idx].child.kill();
        }
        for process in &mut self.processes {
            let _ = process.child.wait();
        }

        for process_idx in 0..self.processes.len() {
            self.abort_process_state(process_idx, reason)?;
        }
        Ok(())
    }

    fn respawn_all_workers_preserve_files(&mut self, reason: &'static str) -> anyhow::Result<()> {
        let mut new_processes = Vec::with_capacity(self.processes.len());
        let mut new_telemetries = Vec::with_capacity(self.processes.len());
        for process_idx in 0..self.processes.len() {
            let (handle, telemetry) = spawn_ready_worker(
                process_idx,
                &self.db_path,
                self.enable_mvcc,
                self.connections_per_process,
            )?;
            self.history.record(&HistoryEvent::WorkerSpawned {
                step: Some(self.current_step),
                process: process_idx,
                pid: handle.child.id(),
                reason,
                telemetry,
            })?;
            info!("process {} restarted: {:?}", process_idx, telemetry);
            new_processes.push(handle);
            new_telemetries.push(telemetry);
        }

        self.processes = new_processes;
        self.worker_startup_telemetries = new_telemetries;
        Ok(())
    }

    /// Perform a single simulation step.
    pub fn step(&mut self) -> anyhow::Result<StepResult> {
        if self.current_step >= self.max_steps {
            return Ok(StepResult::Ok);
        }

        if self.restart_probability > 0.0 && self.rng.random_bool(self.restart_probability) {
            self.restart_all_workers_preserve_files()?;
            self.current_step += 1;
            return Ok(StepResult::Ok);
        }

        let connection_idx = self.current_step % self.connection_states.len();

        // Optionally kill a worker process for crash recovery testing
        if self.kill_probability > 0.0 && self.rng.random_bool(self.kill_probability) {
            self.kill_and_respawn_process(connection_idx, "crash_recovery_test")?;
        }

        self.perform_work(connection_idx)?;
        self.current_step += 1;

        Ok(StepResult::Ok)
    }

    fn perform_work(&mut self, connection_idx: usize) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        let ws = &self.connection_states[connection_idx];
        let exec_id = ws.execution_id;
        let txn_id = ws.txn_id;

        debug!(
            "perform_work: step={}, process={}, connection={}, exec_id={:?}, txn_id={:?}, state={:?}",
            self.current_step,
            location.process_idx,
            connection_idx,
            exec_id,
            txn_id,
            ws.fiber_state
        );

        // If the worker has a pending operation (e.g., auto-rollback), use that.
        // Otherwise, try chaotic workload, then regular workloads.
        if self.connection_states[connection_idx].current_op.is_none() {
            // Try chaotic workload first
            if !self.chaotic_profiles.is_empty() {
                self.try_resume_chaotic(connection_idx);
            }

            // Fall through to regular workloads if chaotic didn't produce an op
            if self.connection_states[connection_idx].current_op.is_none() && self.total_weight > 0
            {
                let mut roll = self.rng.random_range(0..self.total_weight);
                for (weight, workload) in &self.workloads {
                    if roll >= *weight {
                        roll = roll.saturating_sub(*weight);
                        continue;
                    }
                    let ctx = WorkloadContext {
                        fiber_state: &self.connection_states[connection_idx].fiber_state,
                        sim_state: &self.sim_state,
                        opts: &self.opts,
                        enable_mvcc: self.enable_mvcc,
                        tables_vec: self.sim_state.tables_vec(),
                    };
                    let Some(op) = workload.generate(&ctx, &mut self.rng) else {
                        continue;
                    };
                    debug!("generated op for connection {}: {:?}", connection_idx, op);
                    self.connection_states[connection_idx].current_op = Some(op);
                    break;
                }
            }
        }

        // Execute the operation
        let Some(op) = self.connection_states[connection_idx].current_op.take() else {
            return Ok(()); // No operation generated this step
        };

        // Assign execution ID
        let exec_id = self.sim_state.gen_execution_id();
        self.connection_states[connection_idx].execution_id = Some(exec_id);

        // Assign txn_id for BEGIN
        if let Operation::Begin { .. } = &op {
            self.connection_states[connection_idx].txn_id = Some(self.sim_state.gen_txn_id());
        }
        let txn_id = self.connection_states[connection_idx].txn_id;

        // Notify properties: operation starting
        for property in &self.properties {
            let mut property = property.lock().unwrap();
            property.init_op(self.current_step, connection_idx, txn_id, exec_id, &op)?;
        }

        // Send to worker and get result
        let sql = op.sql();
        let op_description = format!("{op:?}");
        self.history.record(&HistoryEvent::OperationStarted {
            step: self.current_step,
            process: location.process_idx,
            connection: connection_idx,
            exec_id,
            txn_id,
            op: op_description.clone(),
            sql: sql.clone(),
        })?;
        debug!(
            "sending to process {} connection {}: {}",
            location.process_idx, location.connection_idx, sql
        );
        send_command(
            &mut self.processes[location.process_idx],
            &WorkerCommand::Execute {
                connection_idx: location.connection_idx,
                sql: sql.clone(),
            },
        )?;
        let response = match recv_response(&mut self.processes[location.process_idx]) {
            Ok(r) => r,
            Err(e) => {
                // The worker process crashed. Kill the old process before respawning to prevent
                // duplicate live access to the same database from one logical slot.
                error!("process {} crashed: {}", location.process_idx, e);
                self.history
                    .record(&HistoryEvent::OperationTransportFailure {
                        step: self.current_step,
                        process: location.process_idx,
                        connection: connection_idx,
                        exec_id: Some(exec_id),
                        txn_id,
                        op: Some(op_description),
                        sql: Some(sql),
                        error: e.to_string(),
                    })?;
                self.kill_and_respawn_process(connection_idx, "transport_failure")?;
                return Ok(());
            }
        };
        let op_result = response.into_op_result();
        self.history.record(&HistoryEvent::OperationFinished {
            step: self.current_step,
            process: location.process_idx,
            connection: connection_idx,
            exec_id,
            txn_id,
            op: op_description,
            sql,
            result: HistoryResult::from(&op_result),
        })?;
        match &op_result {
            Ok(_) => debug!("connection {} result: ok", connection_idx),
            Err(err) => debug!("connection {} result: err={err:?}", connection_idx),
        }

        // Skip benign errors that occur in multiprocess mode
        if let Err(ref e) = op_result {
            let err = e.to_string().to_lowercase();
            // Schema visibility lag across processes
            if err.contains("no such")
                || err.contains("already exists")
                || err.contains("not exist")
            {
                debug!("connection {}: skipped op ({})", connection_idx, err);
                self.connection_states[connection_idx].execution_id = None;
                return Ok(());
            }
            // Schema/index desync after respawn or cross-process schema lag
            if err.contains("not found in schema") {
                debug!("connection {}: schema desync ({})", connection_idx, err);
                self.connection_states[connection_idx].execution_id = None;
                return Ok(());
            }
            // Worker's connection already auto-rolled back (state desync)
            if err.contains("no transaction is active") || err.contains("cannot rollback") {
                debug!(
                    "connection {}: transaction already ended ({})",
                    connection_idx, err
                );
                self.connection_states[connection_idx].fiber_state = FiberState::Idle;
                self.connection_states[connection_idx].txn_id = None;
                self.connection_states[connection_idx].execution_id = None;
                self.connection_states[connection_idx].chaotic_workload = None;
                self.connection_states[connection_idx].last_chaotic_result = None;
                return Ok(());
            }
        }

        // Update fiber state for BEGIN
        if let Operation::Begin { mode } = &op {
            if op_result.is_ok() {
                self.connection_states[connection_idx].fiber_state = if *mode == TxMode::Concurrent
                {
                    FiberState::InConcurrentTx
                } else {
                    FiberState::InTx
                };
            }
        }

        // Apply state changes (sim_state + stats)
        let end_exec_id = self.sim_state.execution_id;
        op.apply_state_changes(
            &mut self.sim_state,
            &mut self.stats,
            &mut self.rng,
            &op_result,
        );

        // Notify properties: operation finished
        for property in &self.properties {
            let mut property = property.lock().unwrap();
            property
                .finish_op(
                    self.current_step,
                    connection_idx,
                    txn_id,
                    exec_id,
                    end_exec_id,
                    &op,
                    &op_result,
                )
                .inspect_err(|e| error!("property failed: {e}"))?;
        }

        // Save result for chaotic workload
        if self.connection_states[connection_idx]
            .chaotic_workload
            .is_some()
            && self.connection_states[connection_idx]
                .last_chaotic_result
                .is_none()
        {
            self.connection_states[connection_idx].last_chaotic_result = Some(op_result.clone());
        }

        // Update fiber state for COMMIT/ROLLBACK and auto-commit
        if matches!(op, Operation::Commit | Operation::Rollback) && op_result.is_ok() {
            self.connection_states[connection_idx].fiber_state = FiberState::Idle;
            self.connection_states[connection_idx].txn_id = None;
        }

        // Handle errors: initiate auto-rollback for retryable errors
        if let Err(ref error) = op_result {
            match error {
                LimboError::SchemaUpdated
                | LimboError::SchemaConflict
                | LimboError::TableLocked
                | LimboError::Busy
                | LimboError::BusySnapshot
                | LimboError::WriteWriteConflict
                | LimboError::CommitDependencyAborted
                | LimboError::InvalidArgument(..) => {
                    if self.connection_states[connection_idx]
                        .fiber_state
                        .is_in_tx()
                    {
                        debug!(
                            "connection {}: auto-rollback after {:?}",
                            connection_idx, error
                        );
                        self.connection_states[connection_idx].current_op =
                            Some(Operation::Rollback);
                    } else {
                        self.connection_states[connection_idx].txn_id = None;
                    }
                }
                // Corruption and checkpoint errors: log and respawn the worker.
                // These are real multiprocess bugs we want to surface but not crash on.
                LimboError::Corrupt(_) | LimboError::CheckpointFailed(_) => {
                    error!(
                        "process {} hit corruption on step {} via connection {}: {} -- respawning",
                        location.process_idx, self.current_step, connection_idx, error
                    );
                    self.stats.corruption_events += 1;
                    self.kill_and_respawn_process(connection_idx, "corruption_recovery")?;
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "connection {} fatal error on step {}: {}",
                        connection_idx,
                        self.current_step,
                        error
                    ));
                }
            }
        }

        self.connection_states[connection_idx].execution_id = None;
        Ok(())
    }

    /// Try to resume or start a chaotic workload for the given logical connection.
    fn try_resume_chaotic(&mut self, connection_idx: usize) {
        // Resume active workload with saved result
        if let Some(result) = self.connection_states[connection_idx]
            .last_chaotic_result
            .take()
        {
            let mut workload = self.connection_states[connection_idx]
                .chaotic_workload
                .take();
            if let Some(ref mut wl) = workload {
                if let Some(op) = wl.next(Some(result)) {
                    debug!(
                        "chaotic: resumed workload for connection {}, next op: {:?}",
                        connection_idx, op
                    );
                    self.connection_states[connection_idx].current_op = Some(op);
                    self.connection_states[connection_idx].chaotic_workload = workload;
                    return;
                }
                debug!(
                    "chaotic: workload completed for connection {}",
                    connection_idx
                );
            }
        }

        // Pick a new chaotic workload (only when idle)
        if self.connection_states[connection_idx]
            .chaotic_workload
            .is_none()
            && self.connection_states[connection_idx].fiber_state == FiberState::Idle
        {
            if let Some(op) = self.pick_chaotic_workload(connection_idx) {
                self.connection_states[connection_idx].current_op = Some(op);
            }
        }
    }

    fn pick_chaotic_workload(&mut self, connection_idx: usize) -> Option<Operation> {
        for (probability, name, profile) in &self.chaotic_profiles {
            if !self.rng.random_bool(*probability) {
                continue;
            }
            let fiber_rng = ChaCha8Rng::seed_from_u64(self.rng.next_u64());
            let mut workload = profile.generate(fiber_rng, connection_idx);
            if let Some(op) = workload.next(None) {
                debug!(
                    "chaotic: picked workload '{}' for connection {}",
                    name, connection_idx
                );
                self.connection_states[connection_idx].chaotic_workload = Some(workload);
                return Some(op);
            }
        }
        None
    }

    fn abort_connection_state(
        &mut self,
        connection_idx: usize,
        reason: &'static str,
    ) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        let txn_id = self.connection_states[connection_idx].txn_id;
        let exec_id = self.connection_states[connection_idx].execution_id;
        let fiber_state = format!("{:?}", self.connection_states[connection_idx].fiber_state);
        let current_op = self.connection_states[connection_idx]
            .current_op
            .as_ref()
            .map(|op| format!("{op:?}"));
        self.history.record(&HistoryEvent::WorkerStateAborted {
            step: self.current_step,
            process: location.process_idx,
            connection: connection_idx,
            reason,
            txn_id,
            exec_id,
            fiber_state,
            current_op,
        })?;
        for property in &self.properties {
            let mut property = property.lock().unwrap();
            property.abort_fiber(connection_idx, txn_id)?;
        }
        self.connection_states[connection_idx] = ConnectionState::new();
        Ok(())
    }

    fn abort_process_state(
        &mut self,
        process_idx: usize,
        reason: &'static str,
    ) -> anyhow::Result<()> {
        for connection_idx in self.process_connection_indices(process_idx) {
            self.abort_connection_state(connection_idx, reason)?;
        }
        Ok(())
    }

    /// Kill one worker process and respawn it (tests crash recovery).
    fn kill_and_respawn_process(
        &mut self,
        connection_idx: usize,
        reason: &'static str,
    ) -> anyhow::Result<()> {
        let location = self.connection_location(connection_idx);
        let pid = self.processes[location.process_idx].child.id();
        info!(
            "killing process {} (pid {}) for {}",
            location.process_idx, pid, reason
        );
        self.history.record(&HistoryEvent::WorkerKilled {
            step: self.current_step,
            process: location.process_idx,
            pid,
            reason,
        })?;
        let _ = self.processes[location.process_idx].child.kill();
        let _ = self.processes[location.process_idx].child.wait();

        // Reset every logical connection hosted by the dead process.
        self.abort_process_state(location.process_idx, reason)?;

        // Respawn
        let (handle, telemetry) = spawn_ready_worker(
            location.process_idx,
            &self.db_path,
            self.enable_mvcc,
            self.connections_per_process,
        )?;
        self.history.record(&HistoryEvent::WorkerSpawned {
            step: Some(self.current_step),
            process: location.process_idx,
            pid: handle.child.id(),
            reason,
            telemetry,
        })?;
        self.processes[location.process_idx] = handle;
        self.worker_startup_telemetries[location.process_idx] = telemetry;
        info!(
            "process {} respawned and ready: {:?}",
            location.process_idx, telemetry
        );
        Ok(())
    }

    /// Run the simulation to completion.
    pub fn run(&mut self) -> anyhow::Result<()> {
        while !self.is_done() {
            self.step()?;
        }
        self.finalize()?;
        Ok(())
    }

    /// Finalize: check properties, shut down workers, clean up.
    pub fn finalize(&mut self) -> anyhow::Result<()> {
        // Finalize properties
        for property in &self.properties {
            let mut property = property.lock().unwrap();
            property.finalize()?;
        }

        // Shut down workers
        for process in &mut self.processes {
            let _ = send_command(process, &WorkerCommand::Shutdown);
            let _ = process.child.wait();
        }

        // Clean up database files
        if !self.keep_files {
            let db_str = self.db_path.to_str().unwrap_or_default();
            let _ = std::fs::remove_file(&self.db_path);
            let _ = std::fs::remove_file(format!("{db_str}-wal"));
            let _ = std::fs::remove_file(format!("{db_str}-tshm"));
            let _ = std::fs::remove_file(format!("{db_str}-log"));
        }

        Ok(())
    }
}

impl Drop for MultiprocessWhopper {
    fn drop(&mut self) {
        // Ensure worker processes are killed on drop
        for process in &mut self.processes {
            let _ = process.child.kill();
            let _ = process.child.wait();
        }
    }
}

fn spawn_ready_worker(
    process_idx: usize,
    db_path: &Path,
    enable_mvcc: bool,
    connections_per_process: usize,
) -> anyhow::Result<(WorkerProcessHandle, WorkerStartupTelemetry)> {
    let mut handle = spawn_worker(process_idx, db_path, enable_mvcc, connections_per_process)?;
    let response = recv_response(&mut handle)?;
    match response {
        WorkerResponse::Ready { telemetry } => Ok((handle, telemetry)),
        other => Err(anyhow::anyhow!(
            "process {} sent unexpected response during init: {:?}",
            process_idx,
            other
        )),
    }
}

/// Spawn a worker child process.
fn spawn_worker(
    process_idx: usize,
    db_path: &Path,
    enable_mvcc: bool,
    connections_per_process: usize,
) -> anyhow::Result<WorkerProcessHandle> {
    let exe = worker_executable()?;
    let mut cmd = Command::new(&exe);
    cmd.arg("worker")
        .arg("--db-path")
        .arg(db_path)
        .arg("--connections-per-process")
        .arg(connections_per_process.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    if enable_mvcc {
        cmd.arg("--enable-mvcc");
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn process {process_idx}: {e}"))?;

    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let responses = spawn_stdout_reader(process_idx, stdout);

    Ok(WorkerProcessHandle {
        child,
        stdin: BufWriter::new(stdin),
        responses,
        process_idx,
    })
}

fn worker_executable() -> anyhow::Result<PathBuf> {
    if let Some(path) = std::env::var_os("TURSO_WHOPPER_WORKER_EXE") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_turso_whopper") {
        return Ok(PathBuf::from(path));
    }
    Ok(std::env::current_exe()?)
}

/// Send a command to a worker.
fn send_command(process: &mut WorkerProcessHandle, cmd: &WorkerCommand) -> anyhow::Result<()> {
    serde_json::to_writer(&mut process.stdin, cmd)?;
    process.stdin.write_all(b"\n")?;
    process.stdin.flush()?;
    Ok(())
}

/// Receive a response from a worker.
fn recv_response(process: &mut WorkerProcessHandle) -> anyhow::Result<WorkerResponse> {
    recv_response_timeout(process, worker_response_timeout())
}

fn worker_response_timeout() -> Duration {
    #[cfg(target_os = "windows")]
    {
        Duration::from_secs(30)
    }

    #[cfg(not(target_os = "windows"))]
    {
        Duration::from_secs(10)
    }
}

fn recv_response_timeout(
    process: &mut WorkerProcessHandle,
    timeout: Duration,
) -> anyhow::Result<WorkerResponse> {
    match process.responses.recv_timeout(timeout) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => Err(anyhow::anyhow!(
            "process {} timed out after {:?} waiting for response",
            process.process_idx,
            timeout
        )),
        Err(RecvTimeoutError::Disconnected) => Err(anyhow::anyhow!(
            "process {} response channel disconnected",
            process.process_idx
        )),
    }
}

fn spawn_stdout_reader(
    worker_id: usize,
    stdout: ChildStdout,
) -> Receiver<anyhow::Result<WorkerResponse>> {
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name(format!("whopper-worker-{worker_id}-stdout"))
        .spawn(move || {
            let mut stdout = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match stdout.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(Err(anyhow::anyhow!(
                            "worker {} closed stdout (crashed?)",
                            worker_id
                        )));
                        break;
                    }
                    Ok(_) => {
                        let parsed = parse_worker_response_line(worker_id, &line);
                        let should_stop = parsed.is_err();
                        if tx.send(parsed).is_err() || should_stop {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.into()));
                        break;
                    }
                }
            }
        })
        .expect("failed to spawn worker stdout reader thread");
    rx
}

fn parse_worker_response_line(worker_id: usize, line: &str) -> anyhow::Result<WorkerResponse> {
    match serde_json::from_str(line) {
        Ok(response) => Ok(response),
        Err(e) => {
            let preview = if line.len() > 200 {
                format!("{}...", &line[..200])
            } else {
                line.trim().to_string()
            };
            Err(anyhow::anyhow!(
                "worker {} sent invalid JSON: {e} (got: {preview})",
                worker_id
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_worker_response_line, recv_response_timeout};
    use crate::protocol::{WorkerCoordinationOpenMode, WorkerResponse};
    use serde_json::Value as JsonValue;
    use std::io::BufWriter;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    use super::WorkerProcessHandle;

    fn placeholder_child_command() -> Command {
        #[cfg(windows)]
        {
            let mut cmd = Command::new("cmd");
            cmd.args(["/C", "timeout", "/T", "60", "/NOBREAK"]);
            cmd
        }

        #[cfg(not(windows))]
        {
            let mut cmd = Command::new("sleep");
            cmd.arg("60");
            cmd
        }
    }

    fn history_output_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "turso-whopper-history-{label}-{}-{}.jsonl",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ))
    }

    #[test]
    fn parse_worker_response_line_rejects_invalid_json() {
        let err =
            parse_worker_response_line(7, "not-json\n").expect_err("invalid JSON should fail");
        assert!(err.to_string().contains("worker 7 sent invalid JSON"));
    }

    #[test]
    fn recv_response_times_out_when_worker_stops_responding() {
        let (_tx, rx) = mpsc::channel();
        let mut child = placeholder_child_command()
            .stdin(Stdio::piped())
            .spawn()
            .expect("failed to spawn placeholder child");
        let stdin = child.stdin.take();
        let mut handle = WorkerProcessHandle {
            child,
            stdin: BufWriter::new(stdin.expect("placeholder child should have stdin")),
            responses: rx,
            process_idx: 3,
        };

        let err = recv_response_timeout(&mut handle, Duration::from_millis(10))
            .expect_err("recv_response should time out");
        assert!(err.to_string().contains("process 3 timed out"));

        let _ = handle.child.kill();
        let _ = handle.child.wait();
    }

    #[test]
    fn parse_worker_response_line_accepts_valid_json() {
        let response = parse_worker_response_line(
            2,
            "{\"Ready\":{\"telemetry\":{\"loaded_from_disk_scan\":false,\"reopened_max_frame\":0,\"reopened_nbackfills\":0,\"reopened_checkpoint_seq\":0,\"coordination_open_mode\":\"Exclusive\",\"sanitized_backfill_proof_on_open\":false}}}\n",
        )
        .expect("valid JSON line should parse");
        assert!(matches!(
            response,
            WorkerResponse::Ready {
                telemetry: crate::protocol::WorkerStartupTelemetry {
                    coordination_open_mode: Some(WorkerCoordinationOpenMode::Exclusive),
                    ..
                }
            }
        ));
    }

    #[test]
    fn operation_history_writer_streams_jsonl_events() {
        let path = history_output_path("writer");
        let mut writer =
            super::OperationHistoryWriter::new(Some(&path)).expect("create history writer");
        let telemetry = crate::protocol::WorkerStartupTelemetry {
            loaded_from_disk_scan: false,
            reopened_max_frame: 7,
            reopened_nbackfills: 3,
            reopened_checkpoint_seq: 9,
            coordination_open_mode: Some(WorkerCoordinationOpenMode::MultiProcess),
            sanitized_backfill_proof_on_open: false,
        };

        writer
            .record(&super::HistoryEvent::WorkerSpawned {
                step: None,
                process: 1,
                pid: 4242,
                reason: "initial_spawn",
                telemetry,
            })
            .expect("record worker spawned");
        writer
            .record(&super::HistoryEvent::OperationFinished {
                step: 7,
                process: 1,
                connection: 3,
                exec_id: 12,
                txn_id: Some(5),
                op: "SimpleSelect { table_name: \"t\", key: \"k\" }".to_string(),
                sql: "SELECT key, length(value) FROM t WHERE key = 'k'".to_string(),
                result: super::HistoryResult::Err {
                    error_kind: "Busy".to_string(),
                    message: "Database is busy".to_string(),
                },
            })
            .expect("record operation finished");

        let contents = std::fs::read_to_string(&path).expect("read history file");
        let lines: Vec<_> = contents.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "writer should emit one JSON object per line"
        );

        let worker_spawned: JsonValue =
            serde_json::from_str(lines[0]).expect("parse worker spawned event");
        assert_eq!(worker_spawned["kind"], "worker_spawned");
        assert_eq!(worker_spawned["process"], 1);
        assert_eq!(worker_spawned["reason"], "initial_spawn");
        assert_eq!(worker_spawned["telemetry"]["reopened_nbackfills"], 3);

        let operation_finished: JsonValue =
            serde_json::from_str(lines[1]).expect("parse operation finished event");
        assert_eq!(operation_finished["kind"], "operation_finished");
        assert_eq!(operation_finished["process"], 1);
        assert_eq!(operation_finished["connection"], 3);
        assert_eq!(operation_finished["exec_id"], 12);
        assert_eq!(operation_finished["result"]["status"], "err");
        assert_eq!(operation_finished["result"]["error_kind"], "Busy");

        let _ = std::fs::remove_file(path);
    }
}
