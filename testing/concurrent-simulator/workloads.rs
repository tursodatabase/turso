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
use crate::operations::Operation;
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
    /// Views vec built from sim_state.views for GenerationContext
    pub(crate) views_vec: Vec<sql_generation::model::view::View>,
}

impl GenerationContext for WorkloadContext<'_> {
    fn tables(&self) -> &Vec<Table> {
        &self.tables_vec
    }

    fn views(&self) -> &Vec<sql_generation::model::view::View> {
        &self.views_vec
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
            ["BEGIN DEFERRED", "BEGIN IMMEDIATE", "BEGIN CONCURRENT"]
                .choose(rng)
                .expect("array is not empty")
        } else {
            "BEGIN"
        };
        Some(Operation::Begin {
            mode: mode.to_string(),
        })
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
pub struct WalCheckpointWorkload;

impl Workload for WalCheckpointWorkload {
    fn generate(&self, ctx: &WorkloadContext, rng: &mut ChaCha8Rng) -> Option<Operation> {
        // Checkpoint should only run when not in a transaction
        if *ctx.fiber_state != FiberState::Idle {
            return None;
        }
        let mode = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"]
            .choose(rng)
            .expect("array is not empty");
        Some(Operation::WalCheckpoint {
            mode: mode.to_string(),
        })
    }
}

/// Commit the current transaction.
pub struct CommitWorkload;

impl Workload for CommitWorkload {
    fn generate(&self, ctx: &WorkloadContext, _rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state != FiberState::InTx {
            return None;
        }
        Some(Operation::Commit)
    }
}

/// Rollback the current transaction.
pub struct RollbackWorkload;

impl Workload for RollbackWorkload {
    fn generate(&self, ctx: &WorkloadContext, _rng: &mut ChaCha8Rng) -> Option<Operation> {
        if *ctx.fiber_state != FiberState::InTx {
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
