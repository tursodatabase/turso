pub mod insert;
pub mod mixed;
pub mod read;
pub mod scan;

/// A phase of the workload. Memory snapshots are taken at phase boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Schema creation, seeding — not measured
    Setup,
    /// The measured workload
    Run,
    /// No more work
    Done,
}

/// A single unit of work the benchmark should execute.
pub struct WorkItem {
    pub sql: String,
    pub params: Vec<turso::Value>,
}

/// Trait that workload profiles implement to generate SQL workloads.
///
/// The benchmark engine calls `next_batch()` repeatedly. Each call returns
/// the current phase and a batch of SQL statements to execute within a
/// single transaction. When the phase changes, the engine takes a memory
/// snapshot. Returning `Phase::Done` signals completion.
pub trait Profile {
    /// Human-readable name for reports.
    fn name(&self) -> &str;

    /// Returns the current phase and a batch of work items to execute.
    fn next_batch(&mut self) -> (Phase, Vec<WorkItem>);
}
