//! Property-based validation for simulation.

use crate::operations::{OpResult, Operation};

/// A property that can be validated during simulation.
/// Properties observe operations and can validate invariants.
pub trait Property {
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
