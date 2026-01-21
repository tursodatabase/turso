//! Property-based validation for simulation.

use anyhow::bail;
use turso_core::{LimboError, Value};

use crate::operations::{OpResult, Operation};

/// A property that can be validated during simulation.
/// Properties observe operations and can validate invariants.
pub trait Property: Send + Sync {
    /// Called when an operation starts execution
    fn start_op(&mut self, tick: usize, fiber_id: usize, op: &Operation);

    /// Called when an operation finishes execution.
    /// Can perform validation and return an error if invariant is violated.
    fn finish_op(
        &mut self,
        step: usize,
        fiber_id: usize,
        txn_id: Option<u64>,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()>;
}

/// Property that validates integrity check results.
/// Integrity check must either return a busy error or a single row with "ok".
pub struct IntegrityCheckProperty;

impl Property for IntegrityCheckProperty {
    fn start_op(&mut self, _tick: usize, _fiber_id: usize, _op: &Operation) {}

    fn finish_op(
        &mut self,
        step: usize,
        fiber_id: usize,
        _txn_id: Option<u64>,
        op: &Operation,
        result: &OpResult,
    ) -> anyhow::Result<()> {
        if !matches!(op, Operation::IntegrityCheck) {
            return Ok(());
        }

        match result {
            OpResult::Error { error } => {
                // Busy errors are acceptable
                if matches!(error, LimboError::Busy | LimboError::BusySnapshot) {
                    return Ok(());
                }
                bail!(
                    "step {step}, fiber {fiber_id}: integrity_check failed with error: {error}"
                );
            }
            OpResult::Success { rows } => {
                if rows.len() != 1 {
                    bail!(
                        "step {step}, fiber {fiber_id}: integrity_check returned {} rows, expected 1",
                        rows.len()
                    );
                }
                let row = &rows[0];
                if row.len() != 1 {
                    bail!(
                        "step {step}, fiber {fiber_id}: integrity_check row has {} columns, expected 1",
                        row.len()
                    );
                }
                match &row[0] {
                    Value::Text(text) if text.as_str() == "ok" => Ok(()),
                    other => {
                        bail!(
                            "step {step}, fiber {fiber_id}: integrity_check returned {:?}, expected \"ok\"",
                            other
                        );
                    }
                }
            }
        }
    }
}
