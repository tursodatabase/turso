//! Operations that can be executed on the database.

use rand_chacha::ChaCha8Rng;
use turso_core::{LimboError, Value};

use crate::{SamplesContainer, SequenceParams, SimulatorFiber, SimulatorState, Stats};

/// Maximum number of keys to remember per table
const MAX_SAMPLE_KEYS_PER_TABLE: usize = 1000;

/// State of a simulator fiber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberState {
    Idle,
    InTx,
    InConcurrentTx,
}

impl FiberState {
    pub fn is_in_tx(self) -> bool {
        matches!(self, FiberState::InTx | FiberState::InConcurrentTx)
    }
}

/// Transaction begin mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxMode {
    Default,
    Deferred,
    Immediate,
    Concurrent,
}

impl TxMode {
    pub fn as_sql(self) -> &'static str {
        match self {
            TxMode::Default => "BEGIN",
            TxMode::Deferred => "BEGIN DEFERRED",
            TxMode::Immediate => "BEGIN IMMEDIATE",
            TxMode::Concurrent => "BEGIN CONCURRENT",
        }
    }

    pub fn is_deferred(self) -> bool {
        matches!(self, TxMode::Default | TxMode::Deferred)
    }
}

/// An operation that can be executed on the database.
/// Operations are produced by workloads and contain all data needed for execution.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Begin a transaction
    Begin { mode: TxMode },
    /// Commit current transaction
    Commit,
    /// Rollback current transaction
    Rollback,
    /// Run PRAGMA integrity_check
    IntegrityCheck,
    /// Run WAL checkpoint with specified mode
    WalCheckpoint { mode: String },
    /// Create a simple key-value table
    CreateSimpleTable { table_name: String },
    /// Select from a simple table by key
    SimpleSelect { table_name: String, key: String },
    /// Insert into a simple table
    SimpleInsert {
        table_name: String,
        key: String,
        value_length: usize,
    },
    /// Generic SELECT query
    Select { sql: String },
    /// Generic INSERT query
    Insert { sql: String },
    /// Generic UPDATE query
    Update { sql: String },
    /// Generic DELETE query
    Delete { sql: String },
    /// Create an index
    CreateIndex {
        sql: String,
        index_name: String,
        table_name: String,
    },
    /// Drop an index
    DropIndex { sql: String, index_name: String },
    /// Create Elle list table for consistency checking
    CreateElleTable { table_name: String },
    /// Append value to an Elle list key
    ElleAppend {
        table_name: String,
        key: String,
        value: i64,
    },
    /// Read an Elle list by key
    ElleRead { table_name: String, key: String },
    /// Write a single value to an Elle rw-register key
    ElleRwWrite {
        table_name: String,
        key: String,
        value: i64,
    },
    /// Read a single value from an Elle rw-register key
    ElleRwRead { table_name: String, key: String },
    /// Create a sequence with specified parameters
    CreateSequence {
        seq_name: String,
        start: i64,
        increment: i64,
        min_value: i64,
        max_value: i64,
        cycle: bool,
    },
    /// Call nextval('seq_name') — returns an integer
    NextVal { seq_name: String },
    /// Call setval('seq_name', value, is_called)
    SetVal {
        seq_name: String,
        value: i64,
        is_called: bool,
    },
    /// Call currval('seq_name') — returns the last value from nextval in this session
    CurrVal { seq_name: String },
    /// Drop a sequence
    DropSequence { seq_name: String },
    /// Create a table with a column that defaults to nextval() of an existing sequence
    CreateTableWithSeqDefault {
        table_name: String,
        seq_name: String,
    },
    /// Insert a row into a table that has a sequence-backed DEFAULT column
    InsertSeqDefault { table_name: String },
    /// Insert a row into the AUTOINCREMENT table with NULL rowid.
    /// RETURNING id gives the engine-assigned rowid back so the
    /// `AutoincWatermarkMonotonicity` property can verify it strictly
    /// advanced past the previous committed max.
    AutoincInsert { payload: String },
    /// Reassign an existing rowid in the AUTOINCREMENT table to a higher
    /// value. The historical bug class: this should bump
    /// `sqlite_sequence.seq` so subsequent NULL-rowid inserts skip the
    /// reassigned region instead of colliding with it.
    AutoincUpdateRowid { old_id: i64, new_id: i64 },
    /// Delete a row from the AUTOINCREMENT table.  Mostly there to give
    /// the table some non-trivial churn so the watermark/btree interplay
    /// has fewer trivially-flat scenarios.
    AutoincDelete { id: i64 },
}
pub type OpResult = Result<Vec<Vec<Value>>, LimboError>;
/// Context passed to Operation::start_op and Operation::finish_op.
pub struct OpContext<'a> {
    pub fiber: &'a mut SimulatorFiber,
    pub sim_state: &'a mut SimulatorState,
    pub stats: &'a mut Stats,
    pub rng: &'a mut ChaCha8Rng,
}

impl Operation {
    /// Get the SQL string for this operation
    pub fn sql(&self) -> String {
        match self {
            Operation::Begin { mode } => mode.as_sql().to_string(),
            Operation::Commit => "COMMIT".to_string(),
            Operation::Rollback => "ROLLBACK".to_string(),
            Operation::IntegrityCheck => "PRAGMA integrity_check".to_string(),
            Operation::WalCheckpoint { mode } => format!("PRAGMA wal_checkpoint({mode})"),
            Operation::CreateSimpleTable { table_name } => {
                format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (key TEXT PRIMARY KEY, value BLOB)"
                )
            }
            Operation::SimpleSelect { table_name, key } => {
                format!("SELECT key, length(value) FROM {table_name} WHERE key = '{key}'")
            }
            Operation::SimpleInsert {
                table_name,
                key,
                value_length,
            } => {
                format!(
                    "INSERT OR REPLACE INTO {table_name} (key, value) VALUES ('{key}', zeroblob({value_length}))"
                )
            }
            Operation::Select { sql } => sql.clone(),
            Operation::Insert { sql } => sql.clone(),
            Operation::Update { sql } => sql.clone(),
            Operation::Delete { sql } => sql.clone(),
            Operation::CreateIndex { sql, .. } => sql.clone(),
            Operation::DropIndex { sql, .. } => sql.clone(),
            Operation::CreateElleTable { table_name } => {
                // Store values as comma-separated integers (e.g., "1,2,3")
                // This avoids JSON function complexity while still being parseable
                format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (key TEXT PRIMARY KEY, vals TEXT DEFAULT '')"
                )
            }
            Operation::ElleAppend {
                table_name,
                key,
                value,
            } => {
                // Append value to vals column. If empty, set to value. Otherwise append with comma.
                // Uses CASE to handle empty string vs non-empty string
                format!(
                    "INSERT INTO {table_name} (key, vals) VALUES ('{key}', '{value}') \
                     ON CONFLICT(key) DO UPDATE SET vals = CASE \
                       WHEN vals = '' THEN '{value}' \
                       ELSE vals || ',' || '{value}' \
                     END"
                )
            }
            Operation::ElleRead { table_name, key } => {
                format!("SELECT vals FROM {table_name} WHERE key = '{key}'")
            }
            Operation::ElleRwWrite {
                table_name,
                key,
                value,
            } => {
                format!(
                    "INSERT INTO {table_name} (key, val) VALUES ('{key}', {value}) \
                     ON CONFLICT(key) DO UPDATE SET val = {value}"
                )
            }
            Operation::ElleRwRead { table_name, key } => {
                format!("SELECT val FROM {table_name} WHERE key = '{key}'")
            }
            Operation::CreateSequence {
                seq_name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
            } => {
                let cycle_str = if *cycle { " CYCLE" } else { "" };
                format!(
                    "CREATE SEQUENCE IF NOT EXISTS {seq_name} START WITH {start} INCREMENT BY {increment} MINVALUE {min_value} MAXVALUE {max_value}{cycle_str}"
                )
            }
            Operation::NextVal { seq_name } => {
                format!("SELECT nextval('{seq_name}')")
            }
            Operation::SetVal {
                seq_name,
                value,
                is_called,
            } => {
                format!(
                    "SELECT setval('{seq_name}', {value}, {})",
                    if *is_called { "true" } else { "false" }
                )
            }
            Operation::CurrVal { seq_name } => {
                format!("SELECT currval('{seq_name}')")
            }
            Operation::DropSequence { seq_name } => {
                format!("DROP SEQUENCE IF EXISTS {seq_name}")
            }
            Operation::CreateTableWithSeqDefault {
                table_name,
                seq_name,
            } => {
                format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, seq_col INTEGER DEFAULT (nextval('{seq_name}')), data TEXT)"
                )
            }
            Operation::InsertSeqDefault { table_name } => {
                format!("INSERT INTO {table_name}(data) VALUES ('row')")
            }
            Operation::AutoincInsert { payload } => {
                format!(
                    "INSERT INTO {table}(payload) VALUES ('{payload}') RETURNING id",
                    table = crate::AUTOINC_TABLE_NAME
                )
            }
            Operation::AutoincUpdateRowid { old_id, new_id } => {
                format!(
                    "UPDATE {table} SET id = {new_id} WHERE id = {old_id} RETURNING id",
                    table = crate::AUTOINC_TABLE_NAME
                )
            }
            Operation::AutoincDelete { id } => {
                format!(
                    "DELETE FROM {table} WHERE id = {id} RETURNING id",
                    table = crate::AUTOINC_TABLE_NAME
                )
            }
        }
    }

    /// Prepare this operation on a connection.
    /// Returns Ok(Statement) on success, or an error.
    pub fn init_op(&self, ctx: &mut OpContext) -> Result<(), turso_core::LimboError> {
        let stmt = ctx.fiber.connection.prepare(self.sql())?;
        ctx.fiber.statement.replace(Some(stmt));
        Ok(())
    }

    /// Called when an operation finishes execution.
    /// Applies state changes based on operation type and result.
    pub fn finish_op(&self, ctx: &mut OpContext, result: &OpResult) {
        self.apply_state_changes(ctx.sim_state, ctx.stats, ctx.rng, result);
    }

    /// Apply state changes without requiring a SimulatorFiber/Connection.
    /// Used by both in-process fibers and multiprocess workers.
    pub fn apply_state_changes(
        &self,
        sim_state: &mut SimulatorState,
        stats: &mut Stats,
        rng: &mut ChaCha8Rng,
        result: &OpResult,
    ) {
        // Only apply state changes on success
        if result.is_err() {
            return;
        }

        match self {
            Operation::CreateSimpleTable { table_name } => {
                sim_state.simple_tables.insert(table_name.clone(), ());
            }
            Operation::SimpleInsert {
                table_name, key, ..
            } => {
                let table_name = table_name.clone();
                let keys = &mut sim_state.simple_tables_keys;
                let container = keys
                    .entry(table_name)
                    .or_insert_with(|| SamplesContainer::new(MAX_SAMPLE_KEYS_PER_TABLE));
                container.add(key.clone(), rng);
                stats.inserts += 1;
            }
            Operation::Insert { .. } => {
                stats.inserts += 1;
            }
            Operation::Delete { .. } => {
                stats.deletes += 1;
            }
            Operation::Update { .. } => {
                stats.updates += 1;
            }
            Operation::IntegrityCheck => {
                stats.integrity_checks += 1;
            }
            Operation::CreateIndex {
                index_name,
                table_name,
                ..
            } => {
                let index_name = index_name.clone();
                let table_name = table_name.clone();
                sim_state.indexes.insert(index_name, table_name);
            }
            Operation::DropIndex { index_name, .. } => {
                sim_state.indexes.remove(index_name);
            }
            Operation::CreateElleTable { table_name } => {
                sim_state.elle_tables.insert(table_name.clone(), ());
            }
            Operation::ElleAppend { .. } | Operation::ElleRwWrite { .. } => {
                stats.elle_writes += 1;
            }
            Operation::ElleRead { .. } | Operation::ElleRwRead { .. } => {
                stats.elle_reads += 1;
            }
            Operation::CreateSequence {
                seq_name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
            } => {
                // CREATE SEQUENCE IF NOT EXISTS is a no-op when the sequence
                // already exists, so DO NOT overwrite params we've already
                // tracked — the engine still has the original definition,
                // and the workload's new params never took effect. Without
                // this guard, subsequent NextVal results are validated
                // against the wrong bounds/start/increment and false-positive.
                if !sim_state.sequences.contains_key(seq_name) {
                    sim_state.sequences.insert(
                        seq_name.clone(),
                        SequenceParams {
                            start: *start,
                            increment: *increment,
                            min_value: *min_value,
                            max_value: *max_value,
                            cycle: *cycle,
                        },
                    );
                }
            }
            Operation::DropSequence { seq_name } => {
                sim_state.sequences.remove(seq_name);
            }
            Operation::NextVal { .. } | Operation::SetVal { .. } | Operation::CurrVal { .. } => {
                stats.sequence_nextvals += 1;
            }
            Operation::CreateTableWithSeqDefault {
                table_name,
                seq_name,
            } => {
                sim_state
                    .seq_default_tables
                    .insert(table_name.clone(), seq_name.clone());
            }
            Operation::InsertSeqDefault { .. } => {
                stats.inserts += 1;
            }
            Operation::AutoincInsert { .. } => {
                stats.inserts += 1;
            }
            Operation::AutoincUpdateRowid { .. } => {
                stats.updates += 1;
            }
            Operation::AutoincDelete { .. } => {
                stats.deletes += 1;
            }
            _ => {}
        }
    }
}
