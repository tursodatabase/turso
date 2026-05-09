pub mod checkpoint;
pub mod create_index;
pub mod insert;
pub mod mixed;
pub mod read;
pub mod scan;
pub mod series_blob;

/// A phase of the workload. Memory snapshots are taken at phase boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Schema creation, seeding — not measured
    Setup,
    /// The measured workload
    Run,
    /// A final checkpoint pass after the measured workload completes
    Checkpoint,
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
/// the current phase and a vec of batches — one per connection. During Setup,
/// typically only one batch is returned (executed on a single connection).
/// During Run, `connections` batches are returned for concurrent execution.
/// Returning `Phase::Done` signals completion.
pub trait Profile {
    /// Human-readable name for reports.
    fn name(&self) -> &str;

    /// Returns the current phase and batches of work items (one per connection).
    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>);

    /// Whether each Run batch should be wrapped in `BEGIN[ CONCURRENT]; …;
    /// COMMIT`. Default `true`. Set to `false` for workloads that include
    /// DDL — MVCC rejects DDL inside `BEGIN CONCURRENT` (it needs an
    /// exclusive tx) and autocommit is the canonical mode for those
    /// statements anyway. With this off, each `WorkItem` autocommits as
    /// its own tx.
    fn wraps_run_in_tx(&self) -> bool {
        true
    }
}
