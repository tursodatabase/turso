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

/// Extract the wire payload for a `LimboError`'s message field. For
/// variants that carry an inner `String`, return that string directly
/// (so it round-trips through `error_kind_to_limbo_error` unchanged).
/// For unit variants, return the Display form. Using `Display` for
/// inner-string variants would prepend the Display prefix
/// (e.g. `"Database is full: {0}"`); the receiver would then
/// reconstruct `LimboError::DatabaseFull("Database is full: ...")`,
/// breaking any callsite that pattern-matches on the inner message
/// (notably the multiprocess driver's `is_seq_exhaustion` check on
/// `"nextval: reached "`).
pub fn limbo_error_to_message(err: &LimboError) -> String {
    match err {
        LimboError::DatabaseFull(s)
        | LimboError::Corrupt(s)
        | LimboError::InternalError(s)
        | LimboError::ParseError(s)
        | LimboError::TxError(s)
        | LimboError::InvalidArgument(s)
        | LimboError::Constraint(s)
        | LimboError::Conflict(s)
        | LimboError::CheckpointFailed(s) => s.clone(),
        other => other.to_string(),
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
        LimboError::ParseError(_) => "ParseError",
        LimboError::TxError(_) => "TxError",
        LimboError::DatabaseFull(_) => "DatabaseFull",
        LimboError::OutOfMemory => "OutOfMemory",
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
        "ParseError" => LimboError::ParseError(message.to_string()),
        "TxError" => LimboError::TxError(message.to_string()),
        "DatabaseFull" => LimboError::DatabaseFull(message.to_string()),
        "OutOfMemory" => LimboError::OutOfMemory,
        _ => LimboError::InternalError(format!("{kind}: {message}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `LimboError` variant the multiprocess handler relies on for
    /// classification must round-trip through `limbo_error_to_kind` +
    /// `error_kind_to_limbo_error` without collapsing into a different
    /// variant. The historical failure mode: ParseError (and TxError, and
    /// DatabaseFull) fell through to the `_ => "Other"` arm and the
    /// receiving side reconstructed them as `InternalError`, masking the
    /// classification from the multiprocess error handler — which then
    /// fatal'd on any benign ParseError such as the workload-generated
    /// `currval` on an undefined sequence.
    #[test]
    fn protocol_round_trips_classification_relevant_variants() {
        type RoundTripCase = (LimboError, fn(&LimboError) -> bool);
        let cases: &[RoundTripCase] = &[
            (
                LimboError::ParseError("x".into()),
                |e| matches!(e, LimboError::ParseError(m) if m == "x"),
            ),
            (
                LimboError::TxError("y".into()),
                |e| matches!(e, LimboError::TxError(m) if m == "y"),
            ),
            (
                LimboError::DatabaseFull("nextval: reached minimum value of sequence \"s\"".into()),
                |e| matches!(e, LimboError::DatabaseFull(m) if m.starts_with("nextval: reached ")),
            ),
            (LimboError::Busy, |e| matches!(e, LimboError::Busy)),
            (LimboError::OutOfMemory, |e| {
                matches!(e, LimboError::OutOfMemory)
            }),
            (LimboError::WriteWriteConflict, |e| {
                matches!(e, LimboError::WriteWriteConflict)
            }),
            (
                LimboError::Corrupt("c".into()),
                |e| matches!(e, LimboError::Corrupt(m) if m == "c"),
            ),
        ];
        for (orig, predicate) in cases {
            let kind = limbo_error_to_kind(orig);
            let msg = match orig {
                LimboError::ParseError(s)
                | LimboError::TxError(s)
                | LimboError::DatabaseFull(s)
                | LimboError::Corrupt(s) => s.clone(),
                _ => String::new(),
            };
            let reconstructed = error_kind_to_limbo_error(kind, &msg);
            assert!(
                predicate(&reconstructed),
                "{orig:?} did not round-trip; got kind={kind:?}, reconstructed={reconstructed:?}",
            );
        }
    }
}
