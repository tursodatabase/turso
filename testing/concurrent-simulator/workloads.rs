//! Workload definitions for the simulator.

use rand::{Rng, seq::IndexedRandom};
use rand_chacha::ChaCha8Rng;
use sql_generation::{
    generation::{Arbitrary, GenerationContext, Opts},
    model::{
        query::{
            create_index::CreateIndex, delete::Delete, drop_index::DropIndex, insert::Insert,
            select::Select, update::Update,
        },
        table::Table,
    },
};

use crate::elle::{ELLE_LIST_APPEND_KEY_COUNT, ELLE_RW_REGISTER_KEY_COUNT, elle_key_name};
use crate::operations::{Operation, TxMode};
use crate::{FiberState, SimulatorState};

/// Context passed to workloads for generating operations.
/// Note: `rng` is passed separately to `Workload::generate` to avoid borrow conflicts
/// when calling `Arbitrary::arbitrary(rng, ctx)` which needs both `&mut rng` and `&ctx`.
pub struct WorkloadContext<'a> {
    pub fiber_state: &'a FiberState,
    pub sim_state: &'a SimulatorState,
    pub opts: &'a Opts,
    pub enable_mvcc: bool,
    /// Tables vec built from sim_state.tables for GenerationContext
    pub(crate) tables_vec: Vec<Table>,
}

impl GenerationContext for WorkloadContext<'_> {
    fn tables(&self) -> &Vec<Table> {
        &self.tables_vec
    }

    fn opts(&self) -> &Opts {
        self.opts
    }
}

/// A workload generates operations to be executed on a fiber.
/// Returns Some(Operation) if an operation was generated, None if this
/// workload couldn't be applied and another should be tried.
pub trait Workload: Send + Sync {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation>;
}

// ============================================================================
// Default Workload Implementations
// ============================================================================

/// Begin a new transaction.
pub struct BeginWorkload;

impl Workload for BeginWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        let mode = if ctx.enable_mvcc {
            *[TxMode::Deferred, TxMode::Immediate, TxMode::Concurrent]
                .choose(rng)
                .expect("array is not empty")
        } else {
            TxMode::Default
        };
        Some(Operation::Begin { mode })
    }
}

/// Run PRAGMA integrity_check.
pub struct IntegrityCheckWorkload;

impl Workload for IntegrityCheckWorkload {
    fn generate(&self, ctx: &WorkloadContext, _rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        Some(Operation::IntegrityCheck)
    }
}

/// Create a new simple key-value table and record its name in state.
pub struct CreateSimpleTableWorkload;

impl Workload for CreateSimpleTableWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // Only create tables outside of transactions
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        let table_name = format!("simple_kv_{}", rng.random_range(0..100000));
        Some(Operation::CreateSimpleTable { table_name })
    }
}

/// Execute a simple SELECT by key on a random simple table (point lookup).
pub struct SimpleSelectWorkload;

impl Workload for SimpleSelectWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.simple_tables.is_empty() {
            return None;
        }
        let table_name = match ctx.sim_state.simple_tables.pick(rng) {
            Some((name, _)) => name.clone(),
            None => return None,
        };

        // 70% chance to use a known inserted key if available
        let key = if rng.random_bool(0.7) {
            ctx.sim_state
                .simple_tables_keys
                .get(&table_name)
                .and_then(|keys| keys.pick(rng).cloned())
                .unwrap_or_else(|| format!("key_{}", rng.random_range(0..10000)))
        } else {
            format!("key_{}", rng.random_range(0..10000))
        };

        Some(Operation::SimpleSelect { table_name, key })
    }
}

/// Execute a simple INSERT into a random simple table.
pub struct SimpleInsertWorkload;

impl Workload for SimpleInsertWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.simple_tables.is_empty() {
            return None;
        }
        let table_name = match ctx.sim_state.simple_tables.pick(rng) {
            Some((name, _)) => name.clone(),
            None => return None,
        };
        let key = format!("key_{}", rng.random_range(0..10000));
        let value_length = rng.random_range(10..16 * 1024);

        Some(Operation::SimpleInsert {
            table_name,
            key,
            value_length,
        })
    }
}

/// Execute a SELECT query (works in both Idle and InTx states).
pub struct SelectWorkload;

impl Workload for SelectWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        let select = Select::arbitrary(rng, ctx);
        let sql = select.to_string();
        Some(Operation::Select { sql })
    }
}

/// Execute an INSERT statement (works in both Idle and InTx states).
pub struct InsertWorkload;

impl Workload for InsertWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        let insert = Insert::arbitrary(rng, ctx);
        let sql = insert.to_string();
        Some(Operation::Insert { sql })
    }
}

/// Execute an UPDATE statement (works in both Idle and InTx states).
pub struct UpdateWorkload;

impl Workload for UpdateWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        let update = Update::arbitrary(rng, ctx);
        let sql = update.to_string();
        Some(Operation::Update { sql })
    }
}

/// Execute a DELETE statement (works in both Idle and InTx states).
pub struct DeleteWorkload;

impl Workload for DeleteWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        let delete = Delete::arbitrary(rng, ctx);
        let sql = delete.to_string();
        Some(Operation::Delete { sql })
    }
}

/// Create a new index (works in both Idle and InTx states).
pub struct CreateIndexWorkload;

impl Workload for CreateIndexWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // DDL is not allowed inside concurrent transactions
        if *ctx.fiber_state == FiberState::InConcurrentTx {
            return None;
        }
        let create_index = CreateIndex::arbitrary(rng, ctx);
        let sql = create_index.to_string();
        Some(Operation::CreateIndex {
            sql,
            index_name: create_index.index_name.clone(),
            table_name: create_index.index.table_name.clone(),
        })
    }
}

/// Drop an existing index (works in both Idle and InTx states).
pub struct DropIndexWorkload;

impl Workload for DropIndexWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // DDL is not allowed inside concurrent transactions
        if *ctx.fiber_state == FiberState::InConcurrentTx {
            return None;
        }
        if ctx.sim_state.indexes.is_empty() {
            return None;
        }
        let (index_name, table_name) = match ctx.sim_state.indexes.pick(rng) {
            Some((idx_name, tbl_name)) => (idx_name.clone(), tbl_name.clone()),
            None => return None,
        };
        let drop_index = DropIndex {
            table_name,
            index_name: index_name.clone(),
        };
        let sql = drop_index.to_string();
        Some(Operation::DropIndex { sql, index_name })
    }
}

/// Run WAL checkpoint with a randomly selected mode.
pub struct WalCheckpointWorkload {
    pub allow_passive: bool,
}

impl Workload for WalCheckpointWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // Checkpoint should only run when not in a transaction
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        let modes: &[&str] = if self.allow_passive {
            &["PASSIVE"]
        } else {
            &["FULL", "RESTART", "TRUNCATE"]
        };
        let mode = modes.choose(rng).expect("array is not empty");
        Some(Operation::WalCheckpoint {
            mode: mode.to_string(),
        })
    }
}

/// Run the checkpoint mode that is valid for both WAL and MVCC.
pub struct TruncateCheckpointWorkload;

impl Workload for TruncateCheckpointWorkload {
    fn generate(&self, ctx: &WorkloadContext, _rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        Some(Operation::WalCheckpoint {
            mode: "TRUNCATE".to_string(),
        })
    }
}

/// Churn schema objects to force schema copy-on-write under allocation faults.
pub struct SchemaChurnWorkload;

impl Workload for SchemaChurnWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state == FiberState::InConcurrentTx {
            return None;
        }

        let table_id = rng.random_range(0..64);
        let index_id = rng.random_range(0..256);
        let sql = match rng.random_range(0..12) {
            0..=2 => schema_churn_create_table_sql(table_id),
            3..=4 => schema_churn_create_index_sql(table_id, index_id),
            5 => schema_churn_create_view_sql(table_id),
            6 => schema_churn_create_trigger_sql(table_id),
            7 => schema_churn_drop_index_sql(table_id, index_id),
            8 => schema_churn_drop_view_sql(table_id),
            9 => schema_churn_schema_read_sql(table_id),
            10 => existing_table_index_sql(&ctx.tables_vec, index_id, rng)
                .unwrap_or_else(|| schema_churn_create_table_sql(table_id)),
            _ => "SELECT name, sql FROM sqlite_schema WHERE name LIKE 'schema_clone_%' ORDER BY name LIMIT 8".to_string(),
        };
        Some(Operation::Execute { sql })
    }
}

fn schema_churn_table_name(table_id: u32) -> String {
    format!("schema_clone_t_{table_id}")
}

fn schema_churn_create_table_sql(table_id: u32) -> String {
    let table_name = schema_churn_table_name(table_id);
    format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (\
         id INTEGER PRIMARY KEY, \
         k TEXT NOT NULL DEFAULT 'k', \
         v INTEGER NOT NULL DEFAULT 0, \
         payload BLOB, \
         CHECK (v >= 0), \
         UNIQUE (k, v))"
    )
}

fn schema_churn_create_index_sql(table_id: u32, index_id: u32) -> String {
    let table_name = schema_churn_table_name(table_id);
    format!(
        "CREATE INDEX IF NOT EXISTS schema_clone_idx_{table_id}_{index_id} \
         ON {table_name}(k, v) WHERE v >= 0"
    )
}

fn schema_churn_create_view_sql(table_id: u32) -> String {
    let table_name = schema_churn_table_name(table_id);
    format!(
        "CREATE VIEW IF NOT EXISTS schema_clone_v_{table_id} AS \
         SELECT id, k, v FROM {table_name} WHERE v >= 0"
    )
}

fn schema_churn_create_trigger_sql(table_id: u32) -> String {
    let table_name = schema_churn_table_name(table_id);
    format!(
        "CREATE TRIGGER IF NOT EXISTS schema_clone_tr_{table_id} \
         AFTER INSERT ON {table_name} \
         WHEN new.v > 100 \
         BEGIN \
             UPDATE {table_name} SET v = new.v WHERE id = new.id; \
         END"
    )
}

fn schema_churn_drop_index_sql(table_id: u32, index_id: u32) -> String {
    format!("DROP INDEX IF EXISTS schema_clone_idx_{table_id}_{index_id}")
}

fn schema_churn_drop_view_sql(table_id: u32) -> String {
    format!("DROP VIEW IF EXISTS schema_clone_v_{table_id}")
}

fn schema_churn_schema_read_sql(table_id: u32) -> String {
    let table_name = schema_churn_table_name(table_id);
    format!("PRAGMA table_info('{table_name}')")
}

fn existing_table_index_sql(
    tables: &[Table],
    index_id: u32,
    rng: &mut ChaCha8Rng,
) -> Option<String> {
    let table = tables.choose(rng)?;
    let column = table.columns.choose(rng)?;
    Some(format!(
        "CREATE INDEX IF NOT EXISTS schema_clone_existing_idx_{}_{} ON {}({})",
        table.name, index_id, table.name, column.name
    ))
}

/// Commit the current transaction.
pub struct CommitWorkload;

impl Workload for CommitWorkload {
    fn generate(&self, ctx: &WorkloadContext, _rng: &mut ChaCha8Rng) -> Option<Operation> {
        if !ctx.fiber_state.is_in_tx() {
            return None;
        }
        Some(Operation::Commit)
    }
}

/// Rollback the current transaction.
pub struct RollbackWorkload;

impl Workload for RollbackWorkload {
    fn generate(&self, ctx: &WorkloadContext, _rng: &mut ChaCha8Rng) -> Option<Operation> {
        if !ctx.fiber_state.is_in_tx() {
            return None;
        }
        Some(Operation::Rollback)
    }
}

// ============================================================================
// Elle Workloads for Consistency Checking
// ============================================================================

/// Create Elle list table for consistency checking.
pub struct CreateElleTableWorkload;

impl Workload for CreateElleTableWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // Only create tables outside of transactions
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        let table_name = format!("elle_lists_{}", rng.random_range(0..100));
        Some(Operation::CreateElleTable { table_name })
    }
}

/// Append to a random key in an Elle table.
pub struct ElleAppendWorkload {
    /// Counter for generating unique append values
    pub value_counter: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

impl ElleAppendWorkload {
    pub fn new() -> Self {
        Self {
            value_counter: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(1)),
        }
    }

    /// Create with a shared counter (for coordinating with chaotic Elle workloads).
    pub fn with_counter(counter: std::sync::Arc<std::sync::atomic::AtomicI64>) -> Self {
        Self {
            value_counter: counter,
        }
    }
}

impl Default for ElleAppendWorkload {
    fn default() -> Self {
        Self::new()
    }
}

impl Workload for ElleAppendWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.elle_tables.is_empty() {
            return None;
        }
        let table_name = ctx.sim_state.elle_tables.pick(rng)?.0.clone();
        let key = elle_key_name(rng.random_range(0..ELLE_LIST_APPEND_KEY_COUNT));
        let value = self
            .value_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Some(Operation::ElleAppend {
            table_name,
            key,
            value,
        })
    }
}

/// Read a random key from an Elle table.
pub struct ElleReadWorkload;

impl Workload for ElleReadWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.elle_tables.is_empty() {
            return None;
        }
        let table_name = ctx.sim_state.elle_tables.pick(rng)?.0.clone();
        let key = elle_key_name(rng.random_range(0..ELLE_LIST_APPEND_KEY_COUNT));

        Some(Operation::ElleRead { table_name, key })
    }
}

// ============================================================================
// Elle Rw-Register Workloads for Consistency Checking
// ============================================================================

/// Write a single value to a random key in an Elle rw-register table.
pub struct ElleRwWriteWorkload {
    /// Counter for generating unique write values
    pub value_counter: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

impl ElleRwWriteWorkload {
    /// Create with a shared counter (for coordinating with chaotic Elle workloads).
    pub fn with_counter(counter: std::sync::Arc<std::sync::atomic::AtomicI64>) -> Self {
        Self {
            value_counter: counter,
        }
    }
}

impl Workload for ElleRwWriteWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.elle_tables.is_empty() {
            return None;
        }
        let table_name = ctx.sim_state.elle_tables.pick(rng)?.0.clone();
        let key = elle_key_name(rng.random_range(0..ELLE_RW_REGISTER_KEY_COUNT));
        let value = self
            .value_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Some(Operation::ElleRwWrite {
            table_name,
            key,
            value,
        })
    }
}

/// Read a random key from an Elle rw-register table.
pub struct ElleRwReadWorkload;

impl Workload for ElleRwReadWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.elle_tables.is_empty() {
            return None;
        }
        let table_name = ctx.sim_state.elle_tables.pick(rng)?.0.clone();
        let key = elle_key_name(rng.random_range(0..ELLE_RW_REGISTER_KEY_COUNT));

        Some(Operation::ElleRwRead { table_name, key })
    }
}

// ============================================================================
// Sequence Workloads
// ============================================================================

/// Create a new sequence with random parameters including min/max/cycle.
pub struct CreateSequenceWorkload;

impl Workload for CreateSequenceWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // DDL only outside transactions
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        let seq_name = format!("seq_{}", rng.random_range(0..100000u32));
        let increments: [i64; 6] = [1, 2, 3, 5, -1, -2];
        let increment = increments[rng.random_range(0..increments.len())];

        // Randomly choose between unbounded, bounded+cycle, and bounded+no-cycle
        let arm = rng.random_range(0..4u32);
        let (start, min_value, max_value, cycle) = if arm == 0 {
            // Bounded sequence with cycle — small range to stress wrap-around
            let range_size = rng.random_range(5..20i64);
            if increment > 0 {
                let min = 1;
                let max = min + range_size * increment.abs();
                (min, min, max, true)
            } else {
                let max = -1;
                let min = max - range_size * increment.abs();
                (max, min, max, true)
            }
        } else if arm == 1 {
            // Bounded sequence without cycle — small range to stress overflow errors
            let range_size = rng.random_range(5..15i64);
            if increment > 0 {
                let min = 1;
                let max = min + range_size * increment.abs();
                (min, min, max, false)
            } else {
                let max = -1;
                let min = max - range_size * increment.abs();
                (max, min, max, false)
            }
        } else {
            // Unbounded (large range, no cycle)
            if increment > 0 {
                (increment, 1, i64::MAX, false)
            } else {
                (increment, i64::MIN + 1, -1, false)
            }
        };

        Some(Operation::CreateSequence {
            seq_name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
        })
    }
}

/// Call currval() on a random existing sequence.
pub struct CurrValWorkload;

impl Workload for CurrValWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.sequences.is_empty() {
            return None;
        }
        let seq_name = ctx.sim_state.sequences.pick(rng)?.0.clone();
        Some(Operation::CurrVal { seq_name })
    }
}

/// Call nextval() on a random existing sequence.
pub struct NextValWorkload;

impl Workload for NextValWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.sequences.is_empty() {
            return None;
        }
        let seq_name = ctx.sim_state.sequences.pick(rng)?.0.clone();
        Some(Operation::NextVal { seq_name })
    }
}

/// Call setval() on a random existing sequence with a random in-bounds value.
pub struct SetValWorkload;

impl Workload for SetValWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.sequences.is_empty() {
            return None;
        }
        let (seq_name, params) = ctx.sim_state.sequences.pick(rng)?;
        let seq_name = seq_name.clone();
        // Pick a value within the sequence's bounds
        let value = rng.random_range(params.min_value..=params.max_value);
        // Align to the increment grid
        let aligned = params.start + ((value - params.start) / params.increment) * params.increment;
        let aligned = aligned.clamp(params.min_value, params.max_value);
        let is_called = rng.random_range(0..2u32) == 0;
        Some(Operation::SetVal {
            seq_name,
            value: aligned,
            is_called,
        })
    }
}

/// Drop a random existing sequence.
pub struct DropSequenceWorkload;

impl Workload for DropSequenceWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // DDL only outside transactions
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        if ctx.sim_state.sequences.is_empty() {
            return None;
        }
        let seq_name = ctx.sim_state.sequences.pick(rng)?.0.clone();
        Some(Operation::DropSequence { seq_name })
    }
}

/// Create a table with a column that defaults to nextval() of an existing sequence.
pub struct CreateTableWithSeqDefaultWorkload;

impl Workload for CreateTableWithSeqDefaultWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        if ctx.sim_state.sequences.is_empty() {
            return None;
        }
        let seq_name = ctx.sim_state.sequences.pick(rng)?.0.clone();
        let table_name = format!("seq_tbl_{}", rng.random_range(0..100000u32));
        Some(Operation::CreateTableWithSeqDefault {
            table_name,
            seq_name,
        })
    }
}

/// Insert a row into a table that has a sequence-backed DEFAULT column.
pub struct InsertSeqDefaultWorkload;

impl Workload for InsertSeqDefaultWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        if ctx.sim_state.seq_default_tables.is_empty() {
            return None;
        }
        let table_name = ctx.sim_state.seq_default_tables.pick(rng)?.0.clone();
        Some(Operation::InsertSeqDefault { table_name })
    }
}

/// Insert a row into the AUTOINCREMENT table with a NULL rowid and
/// RETURNING id, so the `AutoincWatermarkMonotonicity` property can
/// observe the engine-assigned rowid.
pub struct AutoincInsertWorkload;

impl Workload for AutoincInsertWorkload {
    fn generate(&self, _ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        let payload = format!("p{}", rng.random_range(0..1_000_000u32));
        Some(Operation::AutoincInsert { payload })
    }
}

/// Move an existing rowid in the AUTOINCREMENT table to a strictly
/// higher value. This is the historical bug class — the engine must
/// bump `sqlite_sequence.seq` so the next NULL-rowid insert in any
/// fiber returns something above the relocated id.
///
/// The chosen `new_id` is drawn from a wide range so most attempts move
/// the row past whatever the current watermark is; `old_id` is drawn
/// from a small range so it has a reasonable chance of matching some
/// recently-inserted row (early inserts use small ids). If the WHERE
/// matches zero rows the engine treats the UPDATE as a no-op and the
/// property check is satisfied trivially.
pub struct AutoincUpdateRowidWorkload;

impl Workload for AutoincUpdateRowidWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // Only generate inside an explicit tx OR outside any tx: any
        // active tx mode is fine for UPDATE, and the workload should not
        // be skipped on either.
        let _ = ctx;
        let old_id = rng.random_range(1..1_000i64);
        // Bias high so new_id is almost always above the current
        // watermark (early-run watermark is small).
        let new_id = rng.random_range(10_000..1_000_000_000i64);
        Some(Operation::AutoincUpdateRowid { old_id, new_id })
    }
}

/// Delete a row from the AUTOINCREMENT table by id. Mostly there to
/// keep the table from being a strict-monotone-growth-only workload
/// (delete-then-insert exercises the watermark-vs-btree-max interplay).
pub struct AutoincDeleteWorkload;

impl Workload for AutoincDeleteWorkload {
    fn generate(&self, _ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        let id = rng.random_range(1..10_000i64);
        Some(Operation::AutoincDelete { id })
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use sql_generation::{
        generation::Opts,
        model::table::{Column, ColumnType, Table},
    };

    use super::*;

    #[test]
    fn schema_churn_create_table_uses_schema_features() {
        let sql = schema_churn_create_table_sql(7);

        assert!(sql.contains("schema_clone_t_7"));
        assert!(sql.contains("UNIQUE"));
        assert!(sql.contains("CHECK"));
    }

    #[test]
    fn schema_churn_skips_concurrent_transactions() {
        let table = Table {
            name: "table_0".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                column_type: ColumnType::Integer,
                constraints: vec![],
            }],
            rows: vec![],
            indexes: vec![],
        };
        let state = SimulatorState::new(vec![table.clone()], vec![]);
        let opts = Opts::default();
        let tables_vec = vec![table];
        let ctx = WorkloadContext {
            fiber_state: &FiberState::InConcurrentTx,
            sim_state: &state,
            opts: &opts,
            enable_mvcc: true,
            tables_vec,
        };
        let mut rng = ChaCha8Rng::seed_from_u64(1);

        assert!(SchemaChurnWorkload.generate(&ctx, &mut rng).is_none());
    }

    #[test]
    fn existing_table_index_targets_generated_schema() {
        let table = Table {
            name: "table_0".to_string(),
            columns: vec![Column {
                name: "col_0".to_string(),
                column_type: ColumnType::Integer,
                constraints: vec![],
            }],
            rows: vec![],
            indexes: vec![],
        };
        let mut rng = ChaCha8Rng::seed_from_u64(1);

        let sql = existing_table_index_sql(&[table], 9, &mut rng).unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX IF NOT EXISTS schema_clone_existing_idx_table_0_9 ON table_0(col_0)"
        );
    }
}
