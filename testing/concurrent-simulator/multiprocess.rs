//! Multiprocess coordinator for Whopper.
//!
//! Spawns N worker processes, each opening the same on-disk database with real
//! filesystem I/O. Reuses the existing workload generation and property
//! validation infrastructure while exercising the full multiprocess coordination
//! stack (OFD locks, .tshm shared memory, WAL reader slots, MVCC tx slots).

use serde::Serialize;
use std::fs::{File, create_dir_all};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::generation::Opts;
use tracing::{debug, error, info};
use turso_core::{Database, DatabaseOpts, LimboError, OpenFlags, UnixIO};

use crate::chaotic_elle::{ChaoticWorkload, ChaoticWorkloadProfile};
use crate::operations::{FiberState, OpResult, Operation, TxMode};
use crate::properties::Property;
use crate::protocol::{
    WorkerCommand, WorkerResponse, WorkerSharedWalSnapshot, WorkerStartupTelemetry,
};
use crate::workloads::{Workload, WorkloadContext};
use crate::{SimulatorState, Stats, StepResult, create_initial_indexes, create_initial_schema};

/// Configuration for the multiprocess simulator.
pub struct MultiprocessOpts {
    pub seed: Option<u64>,
    pub enable_mvcc: bool,
    pub max_connections: usize,
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
        worker: usize,
        pid: u32,
        reason: &'static str,
        telemetry: WorkerStartupTelemetry,
    },
    WorkerKilled {
        step: usize,
        worker: usize,
        pid: u32,
        reason: &'static str,
    },
    WorkerStateAborted {
        step: usize,
        worker: usize,
        reason: &'static str,
        txn_id: Option<u64>,
        exec_id: Option<u64>,
        fiber_state: String,
        current_op: Option<String>,
    },
    CohortRestartStarted {
        step: usize,
        worker_count: usize,
    },
    OperationStarted {
        step: usize,
        worker: usize,
        exec_id: u64,
        txn_id: Option<u64>,
        op: String,
        sql: String,
    },
    OperationFinished {
        step: usize,
        worker: usize,
        exec_id: u64,
        txn_id: Option<u64>,
        op: String,
        sql: String,
        result: HistoryResult,
    },
    OperationTransportFailure {
        step: usize,
        worker: usize,
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
struct WorkerHandle {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    responses: Receiver<anyhow::Result<WorkerResponse>>,
    worker_id: usize,
}

/// Mirror of each worker's state, maintained by the coordinator.
struct WorkerState {
    fiber_state: FiberState,
    txn_id: Option<u64>,
    execution_id: Option<u64>,
    current_op: Option<Operation>,
    chaotic_workload: Option<Box<dyn ChaoticWorkload>>,
    last_chaotic_result: Option<OpResult>,
}

impl WorkerState {
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
    workers: Vec<WorkerHandle>,
    worker_states: Vec<WorkerState>,
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
    kill_probability: f64,
    restart_probability: f64,
    keep_files: bool,
}

impl MultiprocessWhopper {
    /// Create a new multiprocess coordinator.
    /// Bootstraps the database schema, then spawns worker processes.
    pub fn new(opts: MultiprocessOpts) -> anyhow::Result<Self> {
        let seed = opts.seed.unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.next_u64()
        });
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Create database file on disk
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before UNIX_EPOCH")
            .as_nanos();
        let db_path = PathBuf::from(format!(
            "/tmp/whopper-mp-{}-{}-{}.db",
            seed,
            std::process::id(),
            unique_suffix
        ));
        info!("multiprocess db: {}", db_path.display());

        // Bootstrap schema using real I/O
        {
            let io = Arc::new(UnixIO::new()?);
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

        // Spawn worker processes one at a time.
        // Each worker must fully initialize (open DB, create .tshm) before the next
        // starts, to avoid races on coordination file creation.
        let mut workers = Vec::new();
        let mut worker_states = Vec::new();
        let mut worker_startup_telemetries = Vec::new();
        let mut history = OperationHistoryWriter::new(opts.history_output.as_deref())?;
        for i in 0..opts.max_connections {
            let (handle, telemetry) = spawn_ready_worker(i, &db_path, opts.enable_mvcc)?;
            debug!("worker {} ready: {:?}", handle.worker_id, telemetry);
            history.record(&HistoryEvent::WorkerSpawned {
                step: None,
                worker: i,
                pid: handle.child.id(),
                reason: "initial_spawn",
                telemetry,
            })?;
            worker_startup_telemetries.push(telemetry);
            workers.push(handle);
            worker_states.push(WorkerState::new());
        }

        info!("all {} workers ready", workers.len());

        Ok(Self {
            workers,
            worker_states,
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

    /// Execute SQL directly on one worker without involving workload generation.
    /// This is used by deterministic restart regressions.
    pub fn execute_sql_direct(
        &mut self,
        worker_idx: usize,
        sql: impl Into<String>,
    ) -> anyhow::Result<OpResult> {
        let sql = sql.into();
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::Execute { sql },
        )?;
        let response = recv_response(&mut self.workers[worker_idx])?;
        Ok(response.into_op_result())
    }

    /// Execute SQL on an idle worker that is not currently inside a simulated transaction.
    pub fn execute_sql_via_idle_worker(
        &mut self,
        sql: impl Into<String>,
    ) -> anyhow::Result<(usize, OpResult)> {
        let worker_idx = self
            .worker_states
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
            .ok_or_else(|| anyhow::anyhow!("no idle worker available for probe"))?;
        let result = self.execute_sql_direct(worker_idx, sql)?;
        Ok((worker_idx, result))
    }

    pub fn disable_auto_checkpoint_direct(&mut self, worker_idx: usize) -> anyhow::Result<()> {
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::DisableAutoCheckpoint,
        )?;
        match recv_response(&mut self.workers[worker_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "worker {} returned unexpected response to DisableAutoCheckpoint: {:?}",
                worker_idx,
                other
            )),
        }
    }

    pub fn passive_checkpoint_direct(
        &mut self,
        worker_idx: usize,
        upper_bound_inclusive: Option<u64>,
    ) -> anyhow::Result<()> {
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::PassiveCheckpoint {
                upper_bound_inclusive,
            },
        )?;
        match recv_response(&mut self.workers[worker_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "worker {} returned unexpected response to PassiveCheckpoint: {:?}",
                worker_idx,
                other
            )),
        }
    }

    pub fn clear_backfill_proof_direct(&mut self, worker_idx: usize) -> anyhow::Result<()> {
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::ClearBackfillProof,
        )?;
        match recv_response(&mut self.workers[worker_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "worker {} returned unexpected response to ClearBackfillProof: {:?}",
                worker_idx,
                other
            )),
        }
    }

    pub fn install_unpublished_backfill_proof_direct(
        &mut self,
        worker_idx: usize,
        upper_bound_inclusive: u64,
    ) -> anyhow::Result<()> {
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::InstallUnpublishedBackfillProof {
                upper_bound_inclusive,
            },
        )?;
        match recv_response(&mut self.workers[worker_idx])? {
            WorkerResponse::Ack => Ok(()),
            other => Err(anyhow::anyhow!(
                "worker {} returned unexpected response to InstallUnpublishedBackfillProof: {:?}",
                worker_idx,
                other
            )),
        }
    }

    pub fn shared_wal_snapshot_direct(
        &mut self,
        worker_idx: usize,
    ) -> anyhow::Result<Option<WorkerSharedWalSnapshot>> {
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::ReadSharedWalSnapshot,
        )?;
        match recv_response(&mut self.workers[worker_idx])? {
            WorkerResponse::SharedWalSnapshot { snapshot } => Ok(snapshot),
            other => Err(anyhow::anyhow!(
                "worker {} returned unexpected response to ReadSharedWalSnapshot: {:?}",
                worker_idx,
                other
            )),
        }
    }

    pub fn find_frame_for_page_direct(
        &mut self,
        worker_idx: usize,
        page_id: u64,
    ) -> anyhow::Result<Option<u64>> {
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::FindFrameForPage { page_id },
        )?;
        match recv_response(&mut self.workers[worker_idx])? {
            WorkerResponse::FrameLookup { frame_id } => Ok(frame_id),
            other => Err(anyhow::anyhow!(
                "worker {} returned unexpected response to FindFrameForPage: {:?}",
                worker_idx,
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
        let probe_worker_id = self.workers.len();
        let (mut worker, telemetry) =
            spawn_ready_worker(probe_worker_id, &self.db_path, self.enable_mvcc)?;

        let result = (|| -> anyhow::Result<OpResult> {
            for _ in 0..8 {
                send_command(&mut worker, &WorkerCommand::Execute { sql: sql.clone() })?;
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
            worker_count: self.workers.len(),
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
        for worker_idx in 0..self.workers.len() {
            let worker_id = self.workers[worker_idx].worker_id;
            let pid = self.workers[worker_idx].child.id();
            self.history.record(&HistoryEvent::WorkerKilled {
                step: self.current_step,
                worker: worker_id,
                pid,
                reason,
            })?;
            let _ = self.workers[worker_idx].child.kill();
        }
        for worker in &mut self.workers {
            let _ = worker.child.wait();
        }

        for worker_idx in 0..self.worker_states.len() {
            self.abort_worker_state(worker_idx, reason)?;
        }
        Ok(())
    }

    fn respawn_all_workers_preserve_files(&mut self, reason: &'static str) -> anyhow::Result<()> {
        let mut new_workers = Vec::with_capacity(self.workers.len());
        let mut new_telemetries = Vec::with_capacity(self.workers.len());
        for worker_idx in 0..self.workers.len() {
            let (handle, telemetry) =
                spawn_ready_worker(worker_idx, &self.db_path, self.enable_mvcc)?;
            self.history.record(&HistoryEvent::WorkerSpawned {
                step: Some(self.current_step),
                worker: worker_idx,
                pid: handle.child.id(),
                reason,
                telemetry,
            })?;
            info!("worker {} restarted: {:?}", worker_idx, telemetry);
            new_workers.push(handle);
            new_telemetries.push(telemetry);
        }

        self.workers = new_workers;
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

        let worker_idx = self.current_step % self.workers.len();

        // Optionally kill a worker for crash recovery testing
        if self.kill_probability > 0.0 && self.rng.random_bool(self.kill_probability) {
            self.kill_and_respawn(worker_idx, "crash_recovery_test")?;
        }

        self.perform_work(worker_idx)?;
        self.current_step += 1;

        Ok(StepResult::Ok)
    }

    fn perform_work(&mut self, worker_idx: usize) -> anyhow::Result<()> {
        let ws = &self.worker_states[worker_idx];
        let exec_id = ws.execution_id;
        let txn_id = ws.txn_id;

        debug!(
            "perform_work: step={}, worker={}, exec_id={:?}, txn_id={:?}, state={:?}",
            self.current_step, worker_idx, exec_id, txn_id, ws.fiber_state
        );

        // If the worker has a pending operation (e.g., auto-rollback), use that.
        // Otherwise, try chaotic workload, then regular workloads.
        if self.worker_states[worker_idx].current_op.is_none() {
            // Try chaotic workload first
            if !self.chaotic_profiles.is_empty() {
                self.try_resume_chaotic(worker_idx);
            }

            // Fall through to regular workloads if chaotic didn't produce an op
            if self.worker_states[worker_idx].current_op.is_none() && self.total_weight > 0 {
                let mut roll = self.rng.random_range(0..self.total_weight);
                for (weight, workload) in &self.workloads {
                    if roll >= *weight {
                        roll = roll.saturating_sub(*weight);
                        continue;
                    }
                    let ctx = WorkloadContext {
                        fiber_state: &self.worker_states[worker_idx].fiber_state,
                        sim_state: &self.sim_state,
                        opts: &self.opts,
                        enable_mvcc: self.enable_mvcc,
                        tables_vec: self.sim_state.tables_vec(),
                    };
                    let Some(op) = workload.generate(&ctx, &mut self.rng) else {
                        continue;
                    };
                    debug!("generated op for worker {}: {:?}", worker_idx, op);
                    self.worker_states[worker_idx].current_op = Some(op);
                    break;
                }
            }
        }

        // Execute the operation
        let Some(op) = self.worker_states[worker_idx].current_op.take() else {
            return Ok(()); // No operation generated this step
        };

        // Assign execution ID
        let exec_id = self.sim_state.gen_execution_id();
        self.worker_states[worker_idx].execution_id = Some(exec_id);

        // Assign txn_id for BEGIN
        if let Operation::Begin { .. } = &op {
            self.worker_states[worker_idx].txn_id = Some(self.sim_state.gen_txn_id());
        }
        let txn_id = self.worker_states[worker_idx].txn_id;

        // Notify properties: operation starting
        for property in &self.properties {
            let mut property = property.lock().unwrap();
            property.init_op(self.current_step, worker_idx, txn_id, exec_id, &op)?;
        }

        // Send to worker and get result
        let sql = op.sql();
        let op_description = format!("{op:?}");
        self.history.record(&HistoryEvent::OperationStarted {
            step: self.current_step,
            worker: worker_idx,
            exec_id,
            txn_id,
            op: op_description.clone(),
            sql: sql.clone(),
        })?;
        debug!("sending to worker {}: {}", worker_idx, sql);
        send_command(
            &mut self.workers[worker_idx],
            &WorkerCommand::Execute { sql: sql.clone() },
        )?;
        let response = match recv_response(&mut self.workers[worker_idx]) {
            Ok(r) => r,
            Err(e) => {
                // Worker crashed - kill old process before respawning to prevent
                // two workers for the same slot from accessing the database simultaneously.
                error!("worker {} crashed: {}", worker_idx, e);
                self.history
                    .record(&HistoryEvent::OperationTransportFailure {
                        step: self.current_step,
                        worker: worker_idx,
                        exec_id: Some(exec_id),
                        txn_id,
                        op: Some(op_description),
                        sql: Some(sql),
                        error: e.to_string(),
                    })?;
                self.kill_and_respawn(worker_idx, "transport_failure")?;
                return Ok(());
            }
        };
        let op_result = response.into_op_result();
        self.history.record(&HistoryEvent::OperationFinished {
            step: self.current_step,
            worker: worker_idx,
            exec_id,
            txn_id,
            op: op_description,
            sql,
            result: HistoryResult::from(&op_result),
        })?;
        match &op_result {
            Ok(_) => debug!("worker {} result: ok", worker_idx),
            Err(err) => debug!("worker {} result: err={err:?}", worker_idx),
        }

        // Skip benign errors that occur in multiprocess mode
        if let Err(ref e) = op_result {
            let err = e.to_string().to_lowercase();
            // Schema visibility lag across processes
            if err.contains("no such")
                || err.contains("already exists")
                || err.contains("not exist")
            {
                debug!("worker {}: skipped op ({})", worker_idx, err);
                self.worker_states[worker_idx].execution_id = None;
                return Ok(());
            }
            // Schema/index desync after respawn or cross-process schema lag
            if err.contains("not found in schema") {
                debug!("worker {}: schema desync ({})", worker_idx, err);
                self.worker_states[worker_idx].execution_id = None;
                return Ok(());
            }
            // Worker's connection already auto-rolled back (state desync)
            if err.contains("no transaction is active") || err.contains("cannot rollback") {
                debug!("worker {}: transaction already ended ({})", worker_idx, err);
                self.worker_states[worker_idx].fiber_state = FiberState::Idle;
                self.worker_states[worker_idx].txn_id = None;
                self.worker_states[worker_idx].execution_id = None;
                self.worker_states[worker_idx].chaotic_workload = None;
                self.worker_states[worker_idx].last_chaotic_result = None;
                return Ok(());
            }
        }

        // Update fiber state for BEGIN
        if let Operation::Begin { mode } = &op {
            if op_result.is_ok() {
                self.worker_states[worker_idx].fiber_state = if *mode == TxMode::Concurrent {
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
                    worker_idx,
                    txn_id,
                    exec_id,
                    end_exec_id,
                    &op,
                    &op_result,
                )
                .inspect_err(|e| error!("property failed: {e}"))?;
        }

        // Save result for chaotic workload
        if self.worker_states[worker_idx].chaotic_workload.is_some()
            && self.worker_states[worker_idx].last_chaotic_result.is_none()
        {
            self.worker_states[worker_idx].last_chaotic_result = Some(op_result.clone());
        }

        // Update fiber state for COMMIT/ROLLBACK and auto-commit
        if matches!(op, Operation::Commit | Operation::Rollback) && op_result.is_ok() {
            self.worker_states[worker_idx].fiber_state = FiberState::Idle;
            self.worker_states[worker_idx].txn_id = None;
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
                    if self.worker_states[worker_idx].fiber_state.is_in_tx() {
                        debug!("worker {}: auto-rollback after {:?}", worker_idx, error);
                        self.worker_states[worker_idx].current_op = Some(Operation::Rollback);
                    } else {
                        self.worker_states[worker_idx].txn_id = None;
                    }
                }
                // Corruption and checkpoint errors: log and respawn the worker.
                // These are real multiprocess bugs we want to surface but not crash on.
                LimboError::Corrupt(_) | LimboError::CheckpointFailed(_) => {
                    error!(
                        "worker {} hit corruption on step {}: {} -- respawning",
                        worker_idx, self.current_step, error
                    );
                    self.stats.corruption_events += 1;
                    self.kill_and_respawn(worker_idx, "corruption_recovery")?;
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "worker {} fatal error on step {}: {}",
                        worker_idx,
                        self.current_step,
                        error
                    ));
                }
            }
        }

        self.worker_states[worker_idx].execution_id = None;
        Ok(())
    }

    /// Try to resume or start a chaotic workload for the given worker.
    fn try_resume_chaotic(&mut self, worker_idx: usize) {
        // Resume active workload with saved result
        if let Some(result) = self.worker_states[worker_idx].last_chaotic_result.take() {
            let mut workload = self.worker_states[worker_idx].chaotic_workload.take();
            if let Some(ref mut wl) = workload {
                if let Some(op) = wl.next(Some(result)) {
                    debug!(
                        "chaotic: resumed workload for worker {}, next op: {:?}",
                        worker_idx, op
                    );
                    self.worker_states[worker_idx].current_op = Some(op);
                    self.worker_states[worker_idx].chaotic_workload = workload;
                    return;
                }
                debug!("chaotic: workload completed for worker {}", worker_idx);
            }
        }

        // Pick a new chaotic workload (only when idle)
        if self.worker_states[worker_idx].chaotic_workload.is_none()
            && self.worker_states[worker_idx].fiber_state == FiberState::Idle
        {
            if let Some(op) = self.pick_chaotic_workload(worker_idx) {
                self.worker_states[worker_idx].current_op = Some(op);
            }
        }
    }

    fn pick_chaotic_workload(&mut self, worker_idx: usize) -> Option<Operation> {
        for (probability, name, profile) in &self.chaotic_profiles {
            if !self.rng.random_bool(*probability) {
                continue;
            }
            let fiber_rng = ChaCha8Rng::seed_from_u64(self.rng.next_u64());
            let mut workload = profile.generate(fiber_rng, worker_idx);
            if let Some(op) = workload.next(None) {
                debug!(
                    "chaotic: picked workload '{}' for worker {}",
                    name, worker_idx
                );
                self.worker_states[worker_idx].chaotic_workload = Some(workload);
                return Some(op);
            }
        }
        None
    }

    fn abort_worker_state(
        &mut self,
        worker_idx: usize,
        reason: &'static str,
    ) -> anyhow::Result<()> {
        let txn_id = self.worker_states[worker_idx].txn_id;
        let exec_id = self.worker_states[worker_idx].execution_id;
        let fiber_state = format!("{:?}", self.worker_states[worker_idx].fiber_state);
        let current_op = self.worker_states[worker_idx]
            .current_op
            .as_ref()
            .map(|op| format!("{op:?}"));
        self.history.record(&HistoryEvent::WorkerStateAborted {
            step: self.current_step,
            worker: worker_idx,
            reason,
            txn_id,
            exec_id,
            fiber_state,
            current_op,
        })?;
        for property in &self.properties {
            let mut property = property.lock().unwrap();
            property.abort_fiber(worker_idx, txn_id)?;
        }
        self.worker_states[worker_idx] = WorkerState::new();
        Ok(())
    }

    /// Kill a worker and respawn it (tests crash recovery).
    fn kill_and_respawn(&mut self, worker_idx: usize, reason: &'static str) -> anyhow::Result<()> {
        let pid = self.workers[worker_idx].child.id();
        info!("killing worker {} (pid {}) for {}", worker_idx, pid, reason);
        self.history.record(&HistoryEvent::WorkerKilled {
            step: self.current_step,
            worker: worker_idx,
            pid,
            reason,
        })?;
        let _ = self.workers[worker_idx].child.kill();
        let _ = self.workers[worker_idx].child.wait();

        // Reset worker state and let properties discard or finalize any
        // pending per-fiber state that died with the worker.
        self.abort_worker_state(worker_idx, reason)?;

        // Respawn
        let (handle, telemetry) = spawn_ready_worker(worker_idx, &self.db_path, self.enable_mvcc)?;
        self.history.record(&HistoryEvent::WorkerSpawned {
            step: Some(self.current_step),
            worker: worker_idx,
            pid: handle.child.id(),
            reason,
            telemetry,
        })?;
        self.workers[worker_idx] = handle;
        self.worker_startup_telemetries[worker_idx] = telemetry;
        info!("worker {} respawned and ready: {:?}", worker_idx, telemetry);
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
        for worker in &mut self.workers {
            let _ = send_command(worker, &WorkerCommand::Shutdown);
            let _ = worker.child.wait();
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
        // Ensure workers are killed on drop
        for worker in &mut self.workers {
            let _ = worker.child.kill();
            let _ = worker.child.wait();
        }
    }
}

fn spawn_ready_worker(
    worker_id: usize,
    db_path: &Path,
    enable_mvcc: bool,
) -> anyhow::Result<(WorkerHandle, WorkerStartupTelemetry)> {
    let mut handle = spawn_worker(worker_id, db_path, enable_mvcc)?;
    let response = recv_response(&mut handle)?;
    match response {
        WorkerResponse::Ready { telemetry } => Ok((handle, telemetry)),
        other => Err(anyhow::anyhow!(
            "worker {} sent unexpected response during init: {:?}",
            worker_id,
            other
        )),
    }
}

/// Spawn a worker child process.
fn spawn_worker(
    worker_id: usize,
    db_path: &Path,
    enable_mvcc: bool,
) -> anyhow::Result<WorkerHandle> {
    let exe = worker_executable()?;
    let mut cmd = Command::new(&exe);
    cmd.arg("worker")
        .arg("--db-path")
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    if enable_mvcc {
        cmd.arg("--enable-mvcc");
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn worker {worker_id}: {e}"))?;

    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let responses = spawn_stdout_reader(worker_id, stdout);

    Ok(WorkerHandle {
        child,
        stdin: BufWriter::new(stdin),
        responses,
        worker_id,
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
fn send_command(worker: &mut WorkerHandle, cmd: &WorkerCommand) -> anyhow::Result<()> {
    serde_json::to_writer(&mut worker.stdin, cmd)?;
    worker.stdin.write_all(b"\n")?;
    worker.stdin.flush()?;
    Ok(())
}

/// Receive a response from a worker.
fn recv_response(worker: &mut WorkerHandle) -> anyhow::Result<WorkerResponse> {
    recv_response_timeout(worker, Duration::from_secs(10))
}

fn recv_response_timeout(
    worker: &mut WorkerHandle,
    timeout: Duration,
) -> anyhow::Result<WorkerResponse> {
    match worker.responses.recv_timeout(timeout) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => Err(anyhow::anyhow!(
            "worker {} timed out after {:?} waiting for response",
            worker.worker_id,
            timeout
        )),
        Err(RecvTimeoutError::Disconnected) => Err(anyhow::anyhow!(
            "worker {} response channel disconnected",
            worker.worker_id
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

    use super::WorkerHandle;

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
        let mut child = Command::new("sleep")
            .arg("60")
            .stdin(Stdio::piped())
            .spawn()
            .expect("failed to spawn placeholder child");
        let stdin = child.stdin.take();
        let mut handle = WorkerHandle {
            child,
            stdin: BufWriter::new(stdin.expect("placeholder child should have stdin")),
            responses: rx,
            worker_id: 3,
        };

        let err = recv_response_timeout(&mut handle, Duration::from_millis(10))
            .expect_err("recv_response should time out");
        assert!(err.to_string().contains("worker 3 timed out"));

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
                worker: 1,
                pid: 4242,
                reason: "initial_spawn",
                telemetry,
            })
            .expect("record worker spawned");
        writer
            .record(&super::HistoryEvent::OperationFinished {
                step: 7,
                worker: 1,
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
        assert_eq!(worker_spawned["reason"], "initial_spawn");
        assert_eq!(worker_spawned["telemetry"]["reopened_nbackfills"], 3);

        let operation_finished: JsonValue =
            serde_json::from_str(lines[1]).expect("parse operation finished event");
        assert_eq!(operation_finished["kind"], "operation_finished");
        assert_eq!(operation_finished["exec_id"], 12);
        assert_eq!(operation_finished["result"]["status"], "err");
        assert_eq!(operation_finished["result"]["error_kind"], "Busy");

        let _ = std::fs::remove_file(path);
    }
}
