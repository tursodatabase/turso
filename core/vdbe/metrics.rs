use crate::sync::atomic::{AtomicU64, Ordering};
use std::fmt;

/// The work that the MVCC cursors of a statement did inside their operations.
///
/// One cursor operation (a `Next` or a seek) can look at many entries that the
/// transaction cannot see. The opcode counters of [`StatementMetrics`] count the
/// operation once. These counters count the work inside it, so a test can see
/// when the cost of an operation grows with the hidden entries.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CursorWork {
    /// Raw MvStore entries that cursors took from a skiplist iterator, visible or not.
    pub mvcc_entries_examined: u64,
    /// Raw B-tree rows whose key cursors read, shadowed by MVCC or not.
    pub btree_rows_examined: u64,
    /// Positionings of an MvStore skiplist iterator (a seek, a rewind or a re-seek).
    pub mvcc_seeks: u64,
    /// Root-to-leaf positionings of the B-tree cursor (a seek, a rewind, a last or an exists).
    pub btree_seeks: u64,
    /// Visibility decisions: whether an MvStore entry is visible, or whether MVCC shadows
    /// a B-tree row. A check can include one MvStore point lookup.
    pub visibility_checks: u64,
}

impl CursorWork {
    pub fn merge(&mut self, other: &CursorWork) {
        self.mvcc_entries_examined = self
            .mvcc_entries_examined
            .wrapping_add(other.mvcc_entries_examined);
        self.btree_rows_examined = self
            .btree_rows_examined
            .wrapping_add(other.btree_rows_examined);
        self.mvcc_seeks = self.mvcc_seeks.wrapping_add(other.mvcc_seeks);
        self.btree_seeks = self.btree_seeks.wrapping_add(other.btree_seeks);
        self.visibility_checks = self.visibility_checks.wrapping_add(other.visibility_checks);
    }

    /// The raw entries of both sources that cursors looked at.
    pub fn entries_examined(&self) -> u64 {
        self.mvcc_entries_examined
            .wrapping_add(self.btree_rows_examined)
    }

    /// The positionings of both sources.
    pub fn seeks(&self) -> u64 {
        self.mvcc_seeks.wrapping_add(self.btree_seeks)
    }
}

/// The live [`CursorWork`] counters of one statement. The statement shares them with
/// each MVCC cursor that it opens.
///
/// Only the thread that steps the statement changes the counters, so an increment is a
/// load and a store, not an atomic read-modify-write. The atomics only make the type
/// `Sync`.
#[derive(Debug, Default)]
pub struct CursorWorkCounters {
    mvcc_entries_examined: AtomicU64,
    btree_rows_examined: AtomicU64,
    mvcc_seeks: AtomicU64,
    btree_seeks: AtomicU64,
    visibility_checks: AtomicU64,
}

impl CursorWorkCounters {
    #[inline]
    fn bump(counter: &AtomicU64) {
        counter.store(
            counter.load(Ordering::Relaxed).wrapping_add(1),
            Ordering::Relaxed,
        );
    }

    #[inline]
    pub(crate) fn mvcc_entry_examined(&self) {
        Self::bump(&self.mvcc_entries_examined);
    }

    #[inline]
    pub(crate) fn btree_row_examined(&self) {
        Self::bump(&self.btree_rows_examined);
    }

    #[inline]
    pub(crate) fn mvcc_seek(&self) {
        Self::bump(&self.mvcc_seeks);
    }

    #[inline]
    pub(crate) fn btree_seek(&self) {
        Self::bump(&self.btree_seeks);
    }

    #[inline]
    pub(crate) fn visibility_check(&self) {
        Self::bump(&self.visibility_checks);
    }

    pub fn get(&self) -> CursorWork {
        CursorWork {
            mvcc_entries_examined: self.mvcc_entries_examined.load(Ordering::Relaxed),
            btree_rows_examined: self.btree_rows_examined.load(Ordering::Relaxed),
            mvcc_seeks: self.mvcc_seeks.load(Ordering::Relaxed),
            btree_seeks: self.btree_seeks.load(Ordering::Relaxed),
            visibility_checks: self.visibility_checks.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.get() == CursorWork::default()
    }

    pub(crate) fn reset(&self) {
        self.mvcc_entries_examined.store(0, Ordering::Relaxed);
        self.btree_rows_examined.store(0, Ordering::Relaxed);
        self.mvcc_seeks.store(0, Ordering::Relaxed);
        self.btree_seeks.store(0, Ordering::Relaxed);
        self.visibility_checks.store(0, Ordering::Relaxed);
    }

    /// Sets one counter to zero. Ignores a counter that is not cursor work.
    pub(crate) fn reset_counter(&self, counter: crate::statement::StatementStatusCounter) {
        use crate::statement::StatementStatusCounter as C;
        let counter = match counter {
            C::MvccEntriesExamined => &self.mvcc_entries_examined,
            C::BtreeRowsExamined => &self.btree_rows_examined,
            C::MvccSeeks => &self.mvcc_seeks,
            C::BtreeSeeks => &self.btree_seeks,
            C::VisibilityChecks => &self.visibility_checks,
            C::FullscanStep
            | C::Sort
            | C::VmStep
            | C::Reprepare
            | C::Run
            | C::RowsRead
            | C::RowsWritten => return,
        };
        counter.store(0, Ordering::Relaxed);
    }
}

/// Hash join spill/probe metrics.
#[derive(Debug, Default, Clone)]
pub struct HashJoinMetrics {
    // Spill metrics
    pub spill_bytes_written: u64,
    pub spill_chunks: u64,
    pub spill_max_chunks_per_partition: u64,
    pub spill_max_partition_bytes: u64,

    // Load metrics
    pub load_bytes_read: u64,

    // Probe metrics
    pub probe_calls: u64,

    // Grace hash join metrics
    pub probe_spill_bytes_written: u64,
    pub probe_spill_chunks: u64,
    pub grace_partitions_processed: u64,
    pub grace_probe_rows_streamed: u64,
    pub grace_probe_rows_buffered: u64,
    pub grace_matches: u64,
}

impl HashJoinMetrics {
    pub fn merge(&mut self, other: &HashJoinMetrics) {
        self.spill_bytes_written = self
            .spill_bytes_written
            .wrapping_add(other.spill_bytes_written);
        self.spill_chunks = self.spill_chunks.wrapping_add(other.spill_chunks);
        self.spill_max_chunks_per_partition = self
            .spill_max_chunks_per_partition
            .max(other.spill_max_chunks_per_partition);
        self.spill_max_partition_bytes = self
            .spill_max_partition_bytes
            .max(other.spill_max_partition_bytes);
        self.load_bytes_read = self.load_bytes_read.wrapping_add(other.load_bytes_read);
        self.probe_calls = self.probe_calls.wrapping_add(other.probe_calls);
        self.probe_spill_bytes_written = self
            .probe_spill_bytes_written
            .wrapping_add(other.probe_spill_bytes_written);
        self.probe_spill_chunks = self
            .probe_spill_chunks
            .wrapping_add(other.probe_spill_chunks);
        self.grace_partitions_processed = self
            .grace_partitions_processed
            .wrapping_add(other.grace_partitions_processed);
        self.grace_probe_rows_streamed = self
            .grace_probe_rows_streamed
            .wrapping_add(other.grace_probe_rows_streamed);
        self.grace_probe_rows_buffered = self
            .grace_probe_rows_buffered
            .wrapping_add(other.grace_probe_rows_buffered);
        self.grace_matches = self.grace_matches.wrapping_add(other.grace_matches);
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Statement-level execution metrics
///
/// These metrics are collected unconditionally during statement execution
/// with minimal overhead (simple counter increments). The cost of incrementing
/// these counters is negligible compared to the actual work being measured.
#[derive(Debug, Default, Clone)]
pub struct StatementMetrics {
    // Row operations
    pub rows_read: u64,
    pub rows_written: u64,

    // Execution statistics
    pub vm_steps: u64,
    pub insn_executed: u64,
    pub reprepares: u64,
    /// Number of `Init` instructions executed, as SQLite counts
    /// SQLITE_STMTSTATUS_RUN: one per run plus one per trigger or foreign
    /// key action program started.
    pub runs: u64,

    // Table scan metrics
    pub fullscan_steps: u64,
    pub index_steps: u64,

    // Sort and filter operations
    pub sort_operations: u64,
    pub filter_operations: u64,

    // B-tree operations
    pub btree_seeks: u64,
    pub btree_table_seeks: u64,
    pub btree_index_seeks: u64,
    pub btree_deferred_seeks: u64,
    pub btree_next: u64,
    pub btree_prev: u64,

    /// Signed counter tracking seeks and cursor advances, decremented on sort
    /// rewind to avoid double-counting. Exposed as `sqlite3_search_count`.
    pub search_count: i64,

    // Hash join spill/probe metrics
    pub hash_join: HashJoinMetrics,

    /// Work inside the operations of MVCC cursors.
    pub cursor_work: CursorWork,
}

impl StatementMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total row operations
    pub fn total_row_ops(&self) -> u64 {
        self.rows_read + self.rows_written
    }

    /// Merge another metrics instance into this one (for aggregation)
    pub fn merge(&mut self, other: &StatementMetrics) {
        self.rows_read = self.rows_read.wrapping_add(other.rows_read);
        self.rows_written = self.rows_written.wrapping_add(other.rows_written);
        self.vm_steps = self.vm_steps.wrapping_add(other.vm_steps);
        self.insn_executed = self.insn_executed.wrapping_add(other.insn_executed);
        self.reprepares = self.reprepares.wrapping_add(other.reprepares);
        self.runs = self.runs.wrapping_add(other.runs);
        self.fullscan_steps = self.fullscan_steps.wrapping_add(other.fullscan_steps);
        self.index_steps = self.index_steps.wrapping_add(other.index_steps);
        self.sort_operations = self.sort_operations.wrapping_add(other.sort_operations);
        self.filter_operations = self.filter_operations.wrapping_add(other.filter_operations);
        self.btree_seeks = self.btree_seeks.wrapping_add(other.btree_seeks);
        self.btree_table_seeks = self.btree_table_seeks.wrapping_add(other.btree_table_seeks);
        self.btree_index_seeks = self.btree_index_seeks.wrapping_add(other.btree_index_seeks);
        self.btree_deferred_seeks = self
            .btree_deferred_seeks
            .wrapping_add(other.btree_deferred_seeks);
        self.btree_next = self.btree_next.wrapping_add(other.btree_next);
        self.btree_prev = self.btree_prev.wrapping_add(other.btree_prev);
        self.search_count = self.search_count.wrapping_add(other.search_count);
        self.hash_join.merge(&other.hash_join);
        self.cursor_work.merge(&other.cursor_work);
    }

    /// Reset all counters to zero
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for StatementMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Statement Metrics:")?;
        writeln!(f, "  Row Operations:")?;
        writeln!(f, "    Rows read:        {}", self.rows_read)?;
        writeln!(f, "    Rows written:     {}", self.rows_written)?;
        writeln!(f, "  Execution:")?;
        writeln!(f, "    VM steps:         {}", self.vm_steps)?;
        writeln!(f, "    Instructions:     {}", self.insn_executed)?;
        writeln!(f, "    Reprepares:       {}", self.reprepares)?;
        writeln!(f, "    Runs:             {}", self.runs)?;
        writeln!(f, "  Table Access:")?;
        writeln!(f, "    Full scan steps:  {}", self.fullscan_steps)?;
        writeln!(f, "    Index steps:      {}", self.index_steps)?;
        writeln!(f, "  Operations:")?;
        writeln!(f, "    Sort operations:  {}", self.sort_operations)?;
        writeln!(f, "    Filter operations:{}", self.filter_operations)?;
        writeln!(f, "  B-tree Operations:")?;
        writeln!(f, "    Seeks:            {}", self.btree_seeks)?;
        writeln!(f, "    Table seeks:      {}", self.btree_table_seeks)?;
        writeln!(f, "    Index seeks:      {}", self.btree_index_seeks)?;
        writeln!(f, "    Deferred seeks:   {}", self.btree_deferred_seeks)?;
        writeln!(f, "    Next:             {}", self.btree_next)?;
        writeln!(f, "    Prev:             {}", self.btree_prev)?;
        writeln!(f, "  Hash Join:")?;
        writeln!(
            f,
            "    Spill bytes:      {}",
            self.hash_join.spill_bytes_written
        )?;
        writeln!(f, "    Spill chunks:     {}", self.hash_join.spill_chunks)?;
        writeln!(
            f,
            "    Max chunks/part:  {}",
            self.hash_join.spill_max_chunks_per_partition
        )?;
        writeln!(
            f,
            "    Max part bytes:   {}",
            self.hash_join.spill_max_partition_bytes
        )?;
        writeln!(
            f,
            "    Load bytes:       {}",
            self.hash_join.load_bytes_read
        )?;
        writeln!(f, "    Probes:           {}", self.hash_join.probe_calls)?;
        writeln!(
            f,
            "    Probe spill bytes: {}",
            self.hash_join.probe_spill_bytes_written
        )?;
        writeln!(
            f,
            "    Probe spill chunks: {}",
            self.hash_join.probe_spill_chunks
        )?;
        writeln!(
            f,
            "    Grace partitions:  {}",
            self.hash_join.grace_partitions_processed
        )?;
        writeln!(
            f,
            "    Grace streamed:    {}",
            self.hash_join.grace_probe_rows_streamed
        )?;
        writeln!(
            f,
            "    Grace buffered:    {}",
            self.hash_join.grace_probe_rows_buffered
        )?;
        writeln!(f, "    Grace matches:     {}", self.hash_join.grace_matches)?;
        writeln!(f, "  MVCC Cursor Work:")?;
        writeln!(
            f,
            "    MvStore entries:  {}",
            self.cursor_work.mvcc_entries_examined
        )?;
        writeln!(
            f,
            "    B-tree rows:      {}",
            self.cursor_work.btree_rows_examined
        )?;
        writeln!(f, "    MvStore seeks:    {}", self.cursor_work.mvcc_seeks)?;
        writeln!(f, "    B-tree seeks:     {}", self.cursor_work.btree_seeks)?;
        writeln!(
            f,
            "    Visibility checks:{}",
            self.cursor_work.visibility_checks
        )?;
        Ok(())
    }
}

/// Connection-level metrics aggregation
#[derive(Debug, Default, Clone)]
pub struct ConnectionMetrics {
    /// Total number of statements executed
    pub total_statements: u64,

    /// Aggregate metrics from all statements
    pub aggregate: StatementMetrics,

    /// High-water marks for monitoring
    pub max_vm_steps_per_statement: u64,
    pub max_rows_read_per_statement: u64,
}

impl ConnectionMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a completed statement's metrics (borrows, no clone).
    pub fn record_statement(&mut self, metrics: &StatementMetrics) {
        self.total_statements = self.total_statements.wrapping_add(1);

        // Update high-water marks
        self.max_vm_steps_per_statement = self.max_vm_steps_per_statement.max(metrics.vm_steps);
        self.max_rows_read_per_statement = self.max_rows_read_per_statement.max(metrics.rows_read);

        // Aggregate into total
        self.aggregate.merge(metrics);
    }

    /// Reset connection metrics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for ConnectionMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Connection Metrics:")?;
        writeln!(f, "  Total statements:     {}", self.total_statements)?;
        writeln!(f, "  High-water marks:")?;
        writeln!(
            f,
            "    Max VM steps:       {}",
            self.max_vm_steps_per_statement
        )?;
        writeln!(
            f,
            "    Max rows read:      {}",
            self.max_rows_read_per_statement
        )?;
        writeln!(f)?;
        writeln!(f, "Aggregate Statistics:")?;
        write!(f, "{}", self.aggregate)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_merge() {
        let mut m1 = StatementMetrics::new();
        m1.rows_read = 100;
        m1.vm_steps = 50;
        m1.btree_table_seeks = 3;
        m1.btree_index_seeks = 4;
        m1.btree_deferred_seeks = 2;
        m1.hash_join.spill_bytes_written = 42;

        let mut m2 = StatementMetrics::new();
        m2.rows_read = 200;
        m2.vm_steps = 75;
        m2.btree_table_seeks = 5;
        m2.btree_index_seeks = 6;
        m2.btree_deferred_seeks = 1;
        m2.hash_join.spill_bytes_written = 8;
        m2.hash_join.spill_max_partition_bytes = 1024;

        m1.merge(&m2);
        assert_eq!(m1.rows_read, 300);
        assert_eq!(m1.vm_steps, 125);
        assert_eq!(m1.btree_table_seeks, 8);
        assert_eq!(m1.btree_index_seeks, 10);
        assert_eq!(m1.btree_deferred_seeks, 3);
        assert_eq!(m1.hash_join.spill_bytes_written, 50);
        assert_eq!(m1.hash_join.spill_max_partition_bytes, 1024);
    }

    #[test]
    fn test_connection_metrics_high_water() {
        let mut conn_metrics = ConnectionMetrics::new();

        let mut stmt1 = StatementMetrics::new();
        stmt1.vm_steps = 100;
        stmt1.rows_read = 50;
        conn_metrics.record_statement(&stmt1);

        let mut stmt2 = StatementMetrics::new();
        stmt2.vm_steps = 75;
        stmt2.rows_read = 100;
        conn_metrics.record_statement(&stmt2);

        assert_eq!(conn_metrics.max_vm_steps_per_statement, 100);
        assert_eq!(conn_metrics.max_rows_read_per_statement, 100);
        assert_eq!(conn_metrics.total_statements, 2);
        assert_eq!(conn_metrics.aggregate.vm_steps, 175);
        assert_eq!(conn_metrics.aggregate.rows_read, 150);
    }
}
