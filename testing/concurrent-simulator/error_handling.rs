//! Shared error classification for whopper's in-process and multi-
//! process drivers. Both drivers respond identically to the engine's
//! retryable/non-retryable error contract; centralising the rules
//! here keeps the two paths from drifting when a new error class is
//! added.

use turso_core::LimboError;

/// What the simulator driver should do when an operation returns an
/// error. Computed by `classify_op_error`; the actual state mutation
/// (queuing an operation, resetting txn id, respawning a worker
/// process, propagating the error) lives in each driver because the
/// state shape differs (per-fiber struct in-process vs.
/// per-connection slot multiprocess).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorAction {
    /// Queue `Operation::Rollback` so the outer transaction is
    /// closed cleanly before the next workload pick.
    Rollback,
    /// Drop the per-fiber transaction id; the engine has already
    /// returned to autocommit on its side.
    ClearTxn,
    /// The worker is in an unrecoverable state — respawn its
    /// process. Multiprocess only; in-process drivers map this to
    /// `Fatal`.
    Respawn,
    /// Propagate the error and abort the simulation step.
    Fatal,
}

pub fn recoverable_error_action(in_tx: bool) -> ErrorAction {
    if in_tx {
        ErrorAction::Rollback
    } else {
        ErrorAction::ClearTxn
    }
}

/// Classify a `LimboError` returned by a simulator op.
///
/// `in_tx` should be true iff the fiber is mid an explicit
/// transaction that needs rolling back (i.e. not autocommit). The
/// drivers compute this from their own fiber-state tracking before
/// calling in.
pub fn classify_op_error(err: &LimboError, in_tx: bool) -> ErrorAction {
    // A non-cycling sequence reaching its max/min value is expected: the
    // workload generator easily triggers it with tight bounds (e.g.
    // start=1, increment=5, max=36 → only 8 values exist). DatabaseFull
    // (pager out of pages, autoincrement rowid overflow) stays fatal.
    match err {
        LimboError::SchemaUpdated
        | LimboError::SchemaConflict
        | LimboError::TableLocked
        | LimboError::Busy
        | LimboError::BusySnapshot
        | LimboError::WriteWriteConflict
        | LimboError::CommitDependencyAborted
        | LimboError::InvalidArgument(..)
        | LimboError::ParseError(..)
        | LimboError::TxError(..)
        | LimboError::OutOfMemory => recoverable_error_action(in_tx),
        LimboError::SequenceExhausted { .. } => recoverable_error_action(in_tx),
        LimboError::Corrupt(_) | LimboError::CheckpointFailed(_) => ErrorAction::Respawn,
        _ => ErrorAction::Fatal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_in_tx_queues_rollback() {
        let err = LimboError::ParseError("nope".into());
        assert_eq!(classify_op_error(&err, true), ErrorAction::Rollback);
    }

    #[test]
    fn parse_error_autocommit_clears_txn() {
        let err = LimboError::ParseError("nope".into());
        assert_eq!(classify_op_error(&err, false), ErrorAction::ClearTxn);
    }

    #[test]
    fn sequence_exhaustion_is_swallowed() {
        let err = LimboError::SequenceExhausted {
            name: "s".into(),
            ascending: false,
        };
        assert_eq!(classify_op_error(&err, true), ErrorAction::Rollback);
        assert_eq!(classify_op_error(&err, false), ErrorAction::ClearTxn);
    }

    #[test]
    fn generic_database_full_is_fatal() {
        let err = LimboError::DatabaseFull;
        assert_eq!(classify_op_error(&err, true), ErrorAction::Fatal);
        assert_eq!(classify_op_error(&err, false), ErrorAction::Fatal);
    }

    #[test]
    fn out_of_memory_is_recoverable() {
        assert_eq!(
            classify_op_error(&LimboError::OutOfMemory, true),
            ErrorAction::Rollback
        );
        assert_eq!(
            classify_op_error(&LimboError::OutOfMemory, false),
            ErrorAction::ClearTxn
        );
    }

    #[test]
    fn corrupt_respawns() {
        let err = LimboError::Corrupt("bad page".into());
        assert_eq!(classify_op_error(&err, true), ErrorAction::Respawn);
    }

    #[test]
    fn unknown_error_is_fatal() {
        let err = LimboError::InternalError("unexpected".into());
        assert_eq!(classify_op_error(&err, true), ErrorAction::Fatal);
    }
}
