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
    // DatabaseFull is overloaded — it is raised for both real storage
    // exhaustion (pager.rs) and autoincrement max-rowid overflow
    // (translate/insert.rs). Only swallow the well-defined "non-
    // cycling sequence reached its max/min value" case — the
    // workload generator easily triggers this with tight bounds
    // (e.g. start=1, increment=5, max=36 → only 8 values exist).
    // sequence_rmw_nextval emits messages that start with the
    // literal "nextval: reached "; other DatabaseFull paths use
    // unrelated messages, so a substring match keeps the tolerance
    // scoped to true sequence exhaustion. Generalising this match
    // risks hiding pager out-of-pages, which is a real bug.
    let is_seq_exhaustion = matches!(
        err,
        LimboError::DatabaseFull(msg) if msg.starts_with("nextval: reached ")
    );

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
        LimboError::DatabaseFull(_) if is_seq_exhaustion => recoverable_error_action(in_tx),
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
        let err = LimboError::DatabaseFull("nextval: reached minimum value of \"s\"".into());
        assert_eq!(classify_op_error(&err, true), ErrorAction::Rollback);
        assert_eq!(classify_op_error(&err, false), ErrorAction::ClearTxn);
    }

    #[test]
    fn generic_database_full_is_fatal() {
        let err = LimboError::DatabaseFull("disk image is full".into());
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
