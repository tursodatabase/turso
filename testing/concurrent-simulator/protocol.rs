//! JSON-line protocol for coordinator-worker communication in multiprocess mode.

use serde::{Deserialize, Serialize};
use turso_core::{LimboError, Value};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCoordinationOpenMode {
    Exclusive,
    MultiProcess,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerStartupTelemetry {
    pub loaded_from_disk_scan: bool,
    pub reopened_max_frame: u64,
    pub reopened_nbackfills: u64,
    pub reopened_checkpoint_seq: u32,
    pub coordination_open_mode: Option<WorkerCoordinationOpenMode>,
    pub sanitized_backfill_proof_on_open: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSharedWalSnapshot {
    pub max_frame: u64,
    pub nbackfills: u64,
    pub checkpoint_seq: u32,
    pub frame_index_overflowed: bool,
}

/// Command sent from coordinator to worker over stdin.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WorkerCommand {
    /// Execute a SQL statement and return the result.
    Execute { connection_idx: usize, sql: String },
    /// Disable connection-level automatic checkpointing.
    DisableAutoCheckpoint { connection_idx: usize },
    /// Run a bounded passive checkpoint directly through the pager.
    PassiveCheckpoint {
        connection_idx: usize,
        upper_bound_inclusive: Option<u64>,
    },
    /// Clear the durable backfill proof without disturbing the authority snapshot.
    ClearBackfillProof,
    /// Install a valid durable proof while keeping published nbackfills at zero.
    InstallUnpublishedBackfillProof {
        connection_idx: usize,
        upper_bound_inclusive: u64,
    },
    /// Read the authoritative shared WAL snapshot for deterministic assertions.
    ReadSharedWalSnapshot,
    /// Read the authoritative shared wal-index mapping for one page.
    FindFrameForPage { page_id: u64 },
    /// Gracefully shut down the worker.
    Shutdown,
}

/// Response sent from worker to coordinator over stdout.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WorkerResponse {
    /// Worker has initialized and is ready to accept commands.
    Ready {
        telemetry: WorkerStartupTelemetry,
    },
    /// A non-query control command succeeded.
    Ack,
    /// Shared WAL snapshot read for deterministic restart assertions.
    SharedWalSnapshot {
        snapshot: Option<WorkerSharedWalSnapshot>,
    },
    FrameLookup {
        frame_id: Option<u64>,
    },
    /// SQL execution succeeded, returning rows.
    Ok {
        rows: Vec<Vec<Value>>,
    },
    /// SQL execution failed with an error.
    Error {
        error_kind: String,
        message: String,
    },
}

impl WorkerResponse {
    /// Convert this response into an `OpResult` for the property system.
    pub fn into_op_result(self) -> Result<Vec<Vec<Value>>, LimboError> {
        match self {
            WorkerResponse::Ok { rows } => Ok(rows),
            WorkerResponse::Error {
                error_kind,
                message,
            } => Err(error_kind_to_limbo_error(&error_kind, &message)),
            WorkerResponse::Ready { .. }
            | WorkerResponse::Ack
            | WorkerResponse::SharedWalSnapshot { .. }
            | WorkerResponse::FrameLookup { .. } => Err(LimboError::InternalError(
                "worker returned a non-SQL response to SQL execution".into(),
            )),
        }
    }
}

/// Classify a `LimboError` into an error_kind string for the protocol.
pub fn limbo_error_to_kind(err: &LimboError) -> &'static str {
    match err {
        LimboError::Busy => "Busy",
        LimboError::BusySnapshot => "BusySnapshot",
        LimboError::WriteWriteConflict => "WriteWriteConflict",
        LimboError::CommitDependencyAborted => "CommitDependencyAborted",
        LimboError::SchemaUpdated => "SchemaUpdated",
        LimboError::SchemaConflict => "SchemaConflict",
        LimboError::TableLocked => "TableLocked",
        LimboError::InvalidArgument(_) => "InvalidArgument",
        LimboError::Constraint(_) => "Constraint",
        LimboError::Corrupt(_) => "Corrupt",
        LimboError::ReadOnly => "ReadOnly",
        LimboError::Interrupt => "Interrupt",
        LimboError::InternalError(_) => "InternalError",
        LimboError::Conflict(_) => "Conflict",
        LimboError::CheckpointFailed(_) => "CheckpointFailed",
        _ => "Other",
    }
}

/// Map an error_kind string back to a `LimboError`.
fn error_kind_to_limbo_error(kind: &str, message: &str) -> LimboError {
    match kind {
        "Busy" => LimboError::Busy,
        "BusySnapshot" => LimboError::BusySnapshot,
        "WriteWriteConflict" => LimboError::WriteWriteConflict,
        "CommitDependencyAborted" => LimboError::CommitDependencyAborted,
        "SchemaUpdated" => LimboError::SchemaUpdated,
        "SchemaConflict" => LimboError::SchemaConflict,
        "TableLocked" => LimboError::TableLocked,
        "InvalidArgument" => LimboError::InvalidArgument(message.to_string()),
        "Constraint" => LimboError::Constraint(message.to_string()),
        "Corrupt" => LimboError::Corrupt(message.to_string()),
        "ReadOnly" => LimboError::ReadOnly,
        "Interrupt" => LimboError::Interrupt,
        "InternalError" => LimboError::InternalError(message.to_string()),
        "Conflict" => LimboError::Conflict(message.to_string()),
        "CheckpointFailed" => LimboError::CheckpointFailed(message.to_string()),
        _ => LimboError::InternalError(format!("{kind}: {message}")),
    }
}
