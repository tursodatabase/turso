use rand::seq::IndexedRandom;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use turso::{Builder, Value};

use crate::common::rng_from_time_or_env;

// In-memory representation of the database state
#[derive(Debug, Clone, PartialEq)]
struct DbRow {
    id: i64,
    other_columns: BTreeMap<String, Value>,
}

impl std::fmt::Display for DbRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.other_columns.is_empty() {
            write!(f, "(id={})", self.id)
        } else {
            write!(
                f,
                "(id={},{})",
                self.id,
                self.other_columns
                    .iter()
                    .map(|(k, v)| format!("{k}={v:?}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        }
    }
}

#[derive(Debug, Clone)]
struct TransactionState {
    // The schema this transaction can see (snapshot)
    schema: BTreeMap<String, TableSchema>,
    // The rows this transaction can see (snapshot)
    visible_rows: BTreeMap<i64, DbRow>,
    // Pending changes in this transaction
    pending_changes: Vec<Operation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Column {
    name: String,
    ty: String,
    primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableSchema {
    columns: Vec<Column>,
}

#[derive(Debug)]
struct ShadowDb {
    // Schema
    schema: BTreeMap<String, TableSchema>,
    // Committed state (what's actually in the database)
    committed_rows: BTreeMap<i64, DbRow>,
    // Transaction states
    transactions: BTreeMap<usize, Option<TransactionState>>,
    query_gen_options: QueryGenOptions,
}

impl ShadowDb {
    fn new(
        initial_schema: BTreeMap<String, TableSchema>,
        query_gen_options: QueryGenOptions,
    ) -> Self {
        Self {
            schema: initial_schema,
            committed_rows: BTreeMap::new(),
            transactions: BTreeMap::new(),
            query_gen_options,
        }
    }

    fn begin_transaction(&mut self, tx_id: usize, immediate: bool) {
        self.transactions.insert(
            tx_id,
            if immediate {
                Some(TransactionState {
                    schema: self.schema.clone(),
                    visible_rows: self.committed_rows.clone(),
                    pending_changes: Vec::new(),
                })
            } else {
                None
            },
        );
    }

    fn take_snapshot_if_not_exists(&mut self, tx_id: usize) {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            if tx_state.is_some() {
                // tx already has snapshot
                return;
            }
            tx_state.replace(TransactionState {
                schema: self.schema.clone(),
                visible_rows: self.committed_rows.clone(),
                pending_changes: Vec::new(),
            });
        }
    }

    fn commit_transaction(&mut self, tx_id: usize) {
        if let Some(tx_state) = self.transactions.remove(&tx_id) {
            let Some(tx_state) = tx_state else {
                // Transaction hasn't accessed the DB yet -> do nothing
                return;
            };
            // Apply pending changes to committed state
            for op in tx_state.pending_changes {
                match op {
                    Operation::Insert { id, other_columns } => {
                        assert!(
                            other_columns.len()
                                == self.schema.get("test_table").unwrap().columns.len() - 1,
                            "Inserted row has {} columns, expected {}",
                            other_columns.len() + 1,
                            self.schema.get("test_table").unwrap().columns.len()
                        );
                        self.committed_rows.insert(id, DbRow { id, other_columns });
                    }
                    Operation::Update { id, other_columns } => {
                        let mut row_to_update = self.committed_rows.get(&id).unwrap().clone();
                        for (k, v) in other_columns {
                            row_to_update.other_columns.insert(k, v);
                        }
                        self.committed_rows.insert(id, row_to_update);
                    }
                    Operation::Delete { id } => {
                        self.committed_rows.remove(&id);
                    }
                    Operation::AlterTable { op } => match op {
                        AlterTableOp::AddColumn { name, ty } => {
                            let table_columns =
                                &mut self.schema.get_mut("test_table").unwrap().columns;
                            table_columns.push(Column {
                                name: name.clone(),
                                ty: ty.clone(),
                                primary_key: false,
                            });
                            for row in self.committed_rows.values_mut() {
                                row.other_columns.insert(name.clone(), Value::Null);
                            }
                        }
                        AlterTableOp::DropColumn { name } => {
                            let table_columns =
                                &mut self.schema.get_mut("test_table").unwrap().columns;
                            table_columns.retain(|c| c.name != name);
                            for row in self.committed_rows.values_mut() {
                                row.other_columns.remove(&name);
                            }
                        }
                        AlterTableOp::RenameColumn { old_name, new_name } => {
                            let table_columns =
                                &mut self.schema.get_mut("test_table").unwrap().columns;
                            let col_type = table_columns
                                .iter()
                                .find(|c| c.name == old_name)
                                .unwrap()
                                .ty
                                .clone();
                            table_columns.retain(|c| c.name != old_name);
                            table_columns.push(Column {
                                name: new_name.clone(),
                                ty: col_type,
                                primary_key: false,
                            });
                            for row in self.committed_rows.values_mut() {
                                let value = row.other_columns.remove(&old_name).unwrap();
                                row.other_columns.insert(new_name.clone(), value);
                            }
                        }
                    },
                    _ => unreachable!("Unexpected operation: {op}"),
                }
            }
        }
    }

    fn rollback_transaction(&mut self, tx_id: usize) {
        self.transactions.remove(&tx_id);
    }

    fn insert(
        &mut self,
        tx_id: usize,
        id: i64,
        other_columns: BTreeMap<String, Value>,
    ) -> Result<(), String> {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            // Check if row exists in visible state
            if tx_state.as_ref().unwrap().visible_rows.contains_key(&id) {
                return Err("UNIQUE constraint failed".to_string());
            }
            let row = DbRow {
                id,
                other_columns: other_columns.clone(),
            };
            tx_state
                .as_mut()
                .unwrap()
                .pending_changes
                .push(Operation::Insert { id, other_columns });
            tx_state.as_mut().unwrap().visible_rows.insert(id, row);
            Ok(())
        } else {
            Err("No active transaction".to_string())
        }
    }

    fn update(
        &mut self,
        tx_id: usize,
        id: i64,
        other_columns: BTreeMap<String, Value>,
    ) -> Result<(), String> {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            // Check if row exists in visible state
            let visible_rows = &tx_state.as_ref().unwrap().visible_rows;
            if !visible_rows.contains_key(&id) {
                return Err("Row not found".to_string());
            }
            let mut new_row = visible_rows.get(&id).unwrap().clone();
            for (k, v) in other_columns {
                new_row.other_columns.insert(k, v);
            }
            tx_state
                .as_mut()
                .unwrap()
                .pending_changes
                .push(Operation::Update {
                    id,
                    other_columns: new_row.other_columns.clone(),
                });
            tx_state.as_mut().unwrap().visible_rows.insert(id, new_row);
            Ok(())
        } else {
            Err("No active transaction".to_string())
        }
    }

    fn delete(&mut self, tx_id: usize, id: i64) -> Result<(), String> {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            // Check if row exists in visible state
            if !tx_state.as_ref().unwrap().visible_rows.contains_key(&id) {
                return Err("Row not found".to_string());
            }
            tx_state
                .as_mut()
                .unwrap()
                .pending_changes
                .push(Operation::Delete { id });
            tx_state.as_mut().unwrap().visible_rows.remove(&id);
            Ok(())
        } else {
            Err("No active transaction".to_string())
        }
    }

    fn alter_table(&mut self, tx_id: usize, op: AlterTableOp) -> Result<(), String> {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            let table_columns = &mut tx_state
                .as_mut()
                .unwrap()
                .schema
                .get_mut("test_table")
                .unwrap()
                .columns;
            match op {
                AlterTableOp::AddColumn { name, ty } => {
                    table_columns.push(Column {
                        name: name.clone(),
                        ty: ty.clone(),
                        primary_key: false,
                    });
                    let pending_changes = &mut tx_state.as_mut().unwrap().pending_changes;
                    pending_changes.push(Operation::AlterTable {
                        op: AlterTableOp::AddColumn {
                            name: name.clone(),
                            ty: ty.clone(),
                        },
                    });
                    let visible_rows = &mut tx_state.as_mut().unwrap().visible_rows;
                    for visible_row in visible_rows.values_mut() {
                        visible_row.other_columns.insert(name.clone(), Value::Null);
                    }
                }
                AlterTableOp::DropColumn { name } => {
                    table_columns.retain(|c| c.name != name);
                    let pending_changes = &mut tx_state.as_mut().unwrap().pending_changes;
                    pending_changes.push(Operation::AlterTable {
                        op: AlterTableOp::DropColumn { name: name.clone() },
                    });
                    let visible_rows = &mut tx_state.as_mut().unwrap().visible_rows;
                    for visible_row in visible_rows.values_mut() {
                        visible_row.other_columns.remove(&name);
                    }
                }
                AlterTableOp::RenameColumn { old_name, new_name } => {
                    let col_type = table_columns
                        .iter()
                        .find(|c| c.name == old_name)
                        .unwrap()
                        .ty
                        .clone();
                    table_columns.retain(|c| c.name != old_name);
                    table_columns.push(Column {
                        name: new_name.clone(),
                        ty: col_type,
                        primary_key: false,
                    });
                    let pending_changes = &mut tx_state.as_mut().unwrap().pending_changes;
                    pending_changes.push(Operation::AlterTable {
                        op: AlterTableOp::RenameColumn {
                            old_name: old_name.clone(),
                            new_name: new_name.clone(),
                        },
                    });
                    let visible_rows = &mut tx_state.as_mut().unwrap().visible_rows;
                    for visible_row in visible_rows.values_mut() {
                        let value = visible_row.other_columns.remove(&old_name).unwrap();
                        visible_row.other_columns.insert(new_name.clone(), value);
                    }
                }
            }
            Ok(())
        } else {
            Err("No active transaction".to_string())
        }
    }

    fn get_visible_rows(&self, tx_id: Option<usize>) -> Vec<DbRow> {
        let Some(tx_id) = tx_id else {
            // No transaction - see committed state
            return self.committed_rows.values().cloned().collect();
        };
        if let Some(tx_state) = self.transactions.get(&tx_id) {
            let Some(tx_state) = tx_state.as_ref() else {
                // Transaction hasn't accessed the DB yet -> see committed state
                return self.committed_rows.values().cloned().collect();
            };
            tx_state.visible_rows.values().cloned().collect()
        } else {
            // No transaction - see committed state
            self.committed_rows.values().cloned().collect()
        }
    }
}

#[derive(Debug, Clone)]
enum CheckpointMode {
    Passive,
    Restart,
    Truncate,
    Full,
}

impl std::fmt::Display for CheckpointMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointMode::Passive => write!(f, "PASSIVE"),
            CheckpointMode::Restart => write!(f, "RESTART"),
            CheckpointMode::Truncate => write!(f, "TRUNCATE"),
            CheckpointMode::Full => write!(f, "FULL"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
enum AlterTableOp {
    AddColumn { name: String, ty: String },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
}

impl std::fmt::Display for AlterTableOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOp::AddColumn { name, ty } => write!(f, "ADD COLUMN {name} {ty}"),
            AlterTableOp::DropColumn { name } => write!(f, "DROP COLUMN {name}"),
            AlterTableOp::RenameColumn { old_name, new_name } => {
                write!(f, "RENAME COLUMN {old_name} TO {new_name}")
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Begin {
        concurrent: bool,
    },
    Commit,
    Rollback,
    Insert {
        id: i64,
        other_columns: BTreeMap<String, Value>,
    },
    Update {
        id: i64,
        other_columns: BTreeMap<String, Value>,
    },
    Delete {
        id: i64,
    },
    Checkpoint {
        mode: CheckpointMode,
    },
    AlterTable {
        op: AlterTableOp,
    },
    Select,
}

fn value_to_sql(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::Text(s) => format!("'{s}'"),
        Value::Null => "NULL".to_string(),
        _ => unreachable!(),
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Begin { concurrent } => {
                write!(f, "BEGIN{}", if *concurrent { " CONCURRENT" } else { "" })
            }
            Operation::Commit => write!(f, "COMMIT"),
            Operation::Rollback => write!(f, "ROLLBACK"),
            Operation::Insert { id, other_columns } => {
                let col_names = other_columns.keys().cloned().collect::<Vec<_>>().join(", ");
                let col_values = other_columns
                    .values()
                    .map(value_to_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                if col_names.is_empty() {
                    write!(f, "INSERT INTO test_table (id) VALUES ({id})")
                } else {
                    write!(
                        f,
                        "INSERT INTO test_table (id, {col_names}) VALUES ({id}, {col_values})"
                    )
                }
            }
            Operation::Update { id, other_columns } => {
                let update_set = other_columns
                    .iter()
                    .map(|(k, v)| format!("{k}={}", value_to_sql(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "UPDATE test_table SET {update_set} WHERE id = {id}")
            }
            Operation::Delete { id } => write!(f, "DELETE FROM test_table WHERE id = {id}"),
            Operation::Select => write!(f, "SELECT * FROM test_table"),
            Operation::Checkpoint { mode } => write!(f, "PRAGMA wal_checkpoint({mode})"),
            Operation::AlterTable { op } => write!(f, "ALTER TABLE test_table {op}"),
        }
    }
}

#[tokio::test]
/// Verify translation isolation semantics with multiple concurrent connections.
async fn test_multiple_connections_fuzz_non_mvcc() {
    multiple_connections_fuzz(FuzzOptions::default()).await
}

#[tokio::test]
// Same as test_multiple_connections_fuzz, but with MVCC enabled.
async fn test_multiple_connections_fuzz_mvcc() {
    let mvcc_fuzz_options = FuzzOptions {
        mvcc_enabled: true,
        max_num_connections: 2,
        query_gen_options: QueryGenOptions {
            weight_begin_deferred: 4,
            weight_begin_concurrent: 12,
            weight_commit: 8,
            weight_rollback: 8,
            weight_checkpoint: 2,
            checkpoint_modes: vec![CheckpointMode::Truncate],
            weight_ddl: 10,
            weight_dml: 66,
            dml_gen_options: DmlGenOptions {
                weight_insert: 25,
                weight_delete: 25,
                weight_select: 25,
                weight_update: 25,
            },
        },
        // FIXME: temporary disable reopen logic for MVCC because it will spam CI otherwise (due to some unfixed bug)
        reopen_probability: 0.0,
        ..FuzzOptions::default()
    };
    multiple_connections_fuzz(mvcc_fuzz_options).await
}

#[derive(Debug, Clone)]
struct FuzzOptions {
    num_iterations: usize,
    operations_per_connection: usize,
    reopen_probability: f64,
    max_num_connections: usize,
    query_gen_options: QueryGenOptions,
    mvcc_enabled: bool,
}

#[derive(Debug, Clone)]
struct QueryGenOptions {
    weight_begin_deferred: usize,
    weight_begin_concurrent: usize,
    weight_commit: usize,
    weight_rollback: usize,
    weight_checkpoint: usize,
    checkpoint_modes: Vec<CheckpointMode>,
    weight_ddl: usize,
    weight_dml: usize,
    dml_gen_options: DmlGenOptions,
}

#[derive(Debug, Clone)]
struct DmlGenOptions {
    weight_insert: usize,
    weight_update: usize,
    weight_delete: usize,
    weight_select: usize,
}

impl Default for FuzzOptions {
    fn default() -> Self {
        Self {
            num_iterations: 50,
            operations_per_connection: 30,
            reopen_probability: 0.1,
            max_num_connections: 8,
            query_gen_options: QueryGenOptions::default(),
            mvcc_enabled: false,
        }
    }
}

impl Default for QueryGenOptions {
    fn default() -> Self {
        Self {
            weight_begin_deferred: 10,
            weight_begin_concurrent: 0,
            weight_commit: 10,
            weight_rollback: 10,
            weight_checkpoint: 5,
            checkpoint_modes: vec![
                CheckpointMode::Passive,
                CheckpointMode::Restart,
                CheckpointMode::Truncate,
                CheckpointMode::Full,
            ],
            weight_ddl: 5,
            weight_dml: 55,
            dml_gen_options: DmlGenOptions::default(),
        }
    }
}

impl Default for DmlGenOptions {
    fn default() -> Self {
        Self {
            weight_insert: 25,
            weight_update: 25,
            weight_delete: 25,
            weight_select: 25,
        }
    }
}

async fn multiple_connections_fuzz(opts: FuzzOptions) {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("Multiple connections fuzz test seed: {seed}");

    for iteration in 0..opts.num_iterations {
        let num_connections =
            rng.random_range(2.min(opts.max_num_connections)..=opts.max_num_connections);
        println!("--- Seed {seed} Iteration {iteration} ---");
        println!("Options: {opts:?}");
        // Create a fresh database for each iteration. Use TempDir instead of
        // NamedTempFile because MVCC creates a separate .db-log file that wouldn't
        // be cleaned up otherwise, causing "MVCC log exists but DB is WAL mode" errors.
        let tempdir = tempfile::TempDir::new().unwrap();
        let db_path = tempdir.path().join("test.db");
        let db = Builder::new_local(db_path.to_str().unwrap())
            .build()
            .await
            .unwrap();

        // Enable MVCC if requested
        if opts.mvcc_enabled {
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'experimental_mvcc'")
                .await
                .unwrap();
        }

        // SHARED shadow database for all connections
        let mut schema = BTreeMap::new();
        schema.insert(
            "test_table".to_string(),
            TableSchema {
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        ty: "INTEGER".to_string(),
                        primary_key: true,
                    },
                    Column {
                        name: "text".to_string(),
                        ty: "TEXT".to_string(),
                        primary_key: false,
                    },
                ],
            },
        );
        let mut shared_shadow_db = ShadowDb::new(schema, opts.query_gen_options.clone());
        let mut next_tx_id = 0;

        // Create connections
        let mut connections = Vec::new();
        for conn_id in 0..num_connections {
            let conn = db.connect().unwrap();

            // Create table if it doesn't exist
            tracing::info!("Creating table test_table for connection {conn_id}");
            conn.execute(
                "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)",
                (),
            )
            .await
            .unwrap();

            connections.push((conn, conn_id, None::<usize>)); // (connection, conn_id, current_tx_id)
        }

        let is_acceptable_error = |e: &turso::Error| -> bool {
            let e_string = e.to_string();
            e_string.contains("is locked")
                || e_string.contains("busy")
                || e_string.contains("snapshot is stale")
                || e_string.contains("Write-write conflict")
                || e_string.contains("schema changed")
                || e_string.contains("schema conflict")
                || e_string.contains("has no column named")
                || e_string.contains("no such column:")
                || e_string.contains("cannot drop column")
        };
        let requires_rollback = |e: &turso::Error| -> bool {
            let e_string = e.to_string();
            e_string.contains("Write-write conflict")
        };

        let handle_error = |e: &turso::Error,
                            tx_id: &mut Option<usize>,
                            conn_id: usize,
                            op_num: usize,
                            shadow_db: &mut ShadowDb| {
            println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
            if requires_rollback(e) {
                if let Some(tx_id) = tx_id {
                    println!("Connection {conn_id}(op={op_num}) rolling back transaction {tx_id}");
                    shadow_db.rollback_transaction(*tx_id);
                }
                *tx_id = None;
            }
            if is_acceptable_error(e) {
                return;
            }
            panic!("Unexpected error: {e}");
        };

        let mut db = Some(db);

        // Interleave operations between all connections
        for op_num in 0..opts.operations_per_connection {
            if rng.random_bool(opts.reopen_probability) {
                connections.clear();
                let _ = db.take();
                let reopened = Builder::new_local(db_path.to_str().unwrap())
                    .build()
                    .await
                    .unwrap();
                for conn_id in 0..num_connections {
                    let conn = reopened.connect().unwrap();
                    connections.push((conn, conn_id, None::<usize>)); // (connection, conn_id, current_tx_id)
                }
                db = Some(reopened);
            }
            for (conn, conn_id, current_tx_id) in &mut connections {
                // Generate operation based on current transaction state
                let (operation, visible_rows) =
                    generate_operation(&mut rng, *current_tx_id, &mut shared_shadow_db);

                let is_in_tx = current_tx_id.is_some();
                let is_in_tx_str = if is_in_tx {
                    format!("true(tx_id={:?})", current_tx_id.unwrap())
                } else {
                    "false".to_string()
                };
                let has_snapshot = current_tx_id.is_some_and(|tx_id| {
                    shared_shadow_db.transactions.get(&tx_id).unwrap().is_some()
                });
                println!("Connection {conn_id}(op={op_num}): {operation}, is_in_tx={is_in_tx_str}, has_snapshot={has_snapshot}");

                match operation {
                    Operation::Begin { concurrent } => {
                        let query = operation.to_string();

                        let result = conn.execute(query.as_str(), ()).await;
                        match result {
                            Ok(_) => {
                                shared_shadow_db.begin_transaction(next_tx_id, false);
                                if concurrent {
                                    // in tursodb, BEGIN CONCURRENT immediately starts a transaction.
                                    shared_shadow_db.take_snapshot_if_not_exists(next_tx_id);
                                }
                                *current_tx_id = Some(next_tx_id);
                                next_tx_id += 1;
                            }
                            Err(e) => handle_error(
                                &e,
                                current_tx_id,
                                *conn_id,
                                op_num,
                                &mut shared_shadow_db,
                            ),
                        }
                    }
                    Operation::Commit => {
                        let Some(tx_id) = *current_tx_id else {
                            panic!("Connection {conn_id}(op={op_num}) FAILED: No transaction");
                        };
                        // Try real DB commit first
                        let result = conn.execute("COMMIT", ()).await;

                        match result {
                            Ok(_) => {
                                // Success - update shadow DB
                                shared_shadow_db.commit_transaction(tx_id);
                                *current_tx_id = None;
                            }
                            Err(e) => handle_error(
                                &e,
                                current_tx_id,
                                *conn_id,
                                op_num,
                                &mut shared_shadow_db,
                            ),
                        }
                    }
                    Operation::Rollback => {
                        if let Some(tx_id) = *current_tx_id {
                            // Try real DB rollback first
                            let result = conn.execute("ROLLBACK", ()).await;

                            match result {
                                Ok(_) => {
                                    // Success - update shadow DB
                                    shared_shadow_db.rollback_transaction(tx_id);
                                    *current_tx_id = None;
                                }
                                Err(e) => handle_error(
                                    &e,
                                    current_tx_id,
                                    *conn_id,
                                    op_num,
                                    &mut shared_shadow_db,
                                ),
                            }
                        }
                    }
                    Operation::Insert { id, other_columns } => {
                        let col_names =
                            other_columns.keys().cloned().collect::<Vec<_>>().join(", ");
                        let col_values = other_columns
                            .values()
                            .map(value_to_sql)
                            .collect::<Vec<_>>()
                            .join(", ");
                        let query = if col_names.is_empty() {
                            format!("INSERT INTO test_table (id) VALUES ({id})")
                        } else {
                            format!("INSERT INTO test_table (id, {col_names}) VALUES ({id}, {col_values})")
                        };
                        let result = conn.execute(query.as_str(), ()).await;

                        // Check if real DB operation succeeded
                        match result {
                            Ok(_) => {
                                // Success - update shadow DB
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.take_snapshot_if_not_exists(tx_id);
                                    // In transaction - update transaction's view
                                    shared_shadow_db
                                        .insert(tx_id, id, other_columns.clone())
                                        .unwrap();
                                } else {
                                    // Auto-commit - update shadow DB committed state
                                    shared_shadow_db.begin_transaction(next_tx_id, true);
                                    shared_shadow_db
                                        .insert(next_tx_id, id, other_columns.clone())
                                        .unwrap();
                                    shared_shadow_db.commit_transaction(next_tx_id);
                                    next_tx_id += 1;
                                }
                            }
                            Err(e) => handle_error(
                                &e,
                                current_tx_id,
                                *conn_id,
                                op_num,
                                &mut shared_shadow_db,
                            ),
                        }
                    }
                    Operation::Update { id, other_columns } => {
                        let col_set = other_columns
                            .iter()
                            .map(|(k, v)| format!("{k}={}", value_to_sql(v)))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let query = format!("UPDATE test_table SET {col_set} WHERE id = {id}");
                        let result = conn.execute(query.as_str(), ()).await;

                        // Check if real DB operation succeeded
                        match result {
                            Ok(_) => {
                                // Success - update shadow DB
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.take_snapshot_if_not_exists(tx_id);
                                    // In transaction - update transaction's view
                                    shared_shadow_db
                                        .update(tx_id, id, other_columns.clone())
                                        .unwrap();
                                } else {
                                    // Auto-commit - update shadow DB committed state
                                    shared_shadow_db.begin_transaction(next_tx_id, true);
                                    shared_shadow_db
                                        .update(next_tx_id, id, other_columns.clone())
                                        .unwrap();
                                    shared_shadow_db.commit_transaction(next_tx_id);
                                    next_tx_id += 1;
                                }
                            }
                            Err(e) => handle_error(
                                &e,
                                current_tx_id,
                                *conn_id,
                                op_num,
                                &mut shared_shadow_db,
                            ),
                        }
                    }
                    Operation::Delete { id } => {
                        let result = conn
                            .execute(
                                format!("DELETE FROM test_table WHERE id = {id}").as_str(),
                                (),
                            )
                            .await;

                        // Check if real DB operation succeeded
                        match result {
                            Ok(_) => {
                                // Success - update shadow DB
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.take_snapshot_if_not_exists(tx_id);
                                    // In transaction - update transaction's view
                                    shared_shadow_db.delete(tx_id, id).unwrap();
                                } else {
                                    // Auto-commit - update shadow DB committed state
                                    shared_shadow_db.begin_transaction(next_tx_id, true);
                                    shared_shadow_db.delete(next_tx_id, id).unwrap();
                                    shared_shadow_db.commit_transaction(next_tx_id);
                                    next_tx_id += 1;
                                }
                            }
                            Err(e) => handle_error(
                                &e,
                                current_tx_id,
                                *conn_id,
                                op_num,
                                &mut shared_shadow_db,
                            ),
                        }
                    }
                    Operation::Select => {
                        let query_str = "SELECT * FROM test_table ORDER BY id";
                        let mut stmt = conn.prepare(query_str).await.unwrap();
                        let columns = stmt.columns();
                        let mut rows = stmt.query(()).await.unwrap();

                        let mut real_rows = Vec::new();
                        let ok = loop {
                            match rows.next().await {
                                Err(e) => {
                                    handle_error(
                                        &e,
                                        current_tx_id,
                                        *conn_id,
                                        op_num,
                                        &mut shared_shadow_db,
                                    );
                                    break false;
                                }
                                Ok(None) => {
                                    break true;
                                }
                                Ok(Some(row)) => {
                                    let Value::Integer(id) = row.get_value(0).unwrap() else {
                                        panic!("Unexpected value for id: {:?}", row.get_value(0));
                                    };
                                    let mut other_columns = BTreeMap::new();
                                    for i in 1..columns.len() {
                                        let column = columns.get(i).unwrap();
                                        let value = row.get_value(i).unwrap();
                                        other_columns.insert(column.name().to_string(), value);
                                    }
                                    real_rows.push(DbRow { id, other_columns });
                                }
                            }
                        };

                        if !ok {
                            continue;
                        }

                        if let Some(tx_id) = *current_tx_id {
                            shared_shadow_db.take_snapshot_if_not_exists(tx_id);
                        }

                        real_rows.sort_by_key(|r| r.id);

                        let mut expected_rows = visible_rows.clone();
                        expected_rows.sort_by_key(|r| r.id);

                        if real_rows != expected_rows {
                            let diff = {
                                let mut diff = Vec::new();
                                for row in expected_rows.iter() {
                                    if !real_rows.contains(row) {
                                        diff.push(row);
                                    }
                                }
                                for row in real_rows.iter() {
                                    if !expected_rows.contains(row) {
                                        diff.push(row);
                                    }
                                }
                                diff
                            };
                            panic!(
                                "Row mismatch in iteration {iteration} Connection {conn_id}(op={op_num}). Query: {query_str}.\n\nExpected: {}\n\nGot: {}\n\nDiff: {}\n\nSeed: {seed}",
                                expected_rows.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "),
                                real_rows.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "),
                                diff.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "),
                            );
                        }
                    }
                    Operation::AlterTable { op } => {
                        let query = format!("ALTER TABLE test_table {op}");
                        let result = conn.execute(&query, ()).await;

                        match result {
                            Ok(_) => {
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.take_snapshot_if_not_exists(tx_id);
                                    // In transaction - update transaction's view
                                    shared_shadow_db.alter_table(tx_id, op).unwrap();
                                } else {
                                    // Auto-commit - update shadow DB committed state
                                    shared_shadow_db.begin_transaction(next_tx_id, true);
                                    shared_shadow_db
                                        .alter_table(next_tx_id, op.clone())
                                        .unwrap();
                                    shared_shadow_db.commit_transaction(next_tx_id);
                                    next_tx_id += 1;
                                }
                            }
                            Err(e) => handle_error(
                                &e,
                                current_tx_id,
                                *conn_id,
                                op_num,
                                &mut shared_shadow_db,
                            ),
                        }
                    }
                    Operation::Checkpoint { mode } => {
                        let query = format!("PRAGMA wal_checkpoint({mode})");
                        let mut rows = conn.query(&query, ()).await.unwrap();

                        match rows.next().await {
                            Ok(Some(row)) => {
                                let checkpoint_ok = matches!(row.get_value(0).unwrap(), Value::Integer(0));
                                let wal_page_count = match row.get_value(1).unwrap() {
                                    Value::Integer(count) => count.to_string(),
                                    Value::Null => "NULL".to_string(),
                                    _ => panic!("Unexpected value for wal_page_count: {:?}", row.get_value(1)),
                                };
                                let checkpoint_count = match row.get_value(2).unwrap() {
                                    Value::Integer(count) => count.to_string(),
                                    Value::Null => "NULL".to_string(),
                                    _ => panic!("Unexpected value for checkpoint_count: {:?}", row.get_value(2)),
                                };
                                println!("Connection {conn_id}(op={op_num}) Checkpoint {mode}: OK: {checkpoint_ok}, wal_page_count: {wal_page_count}, checkpointed_count: {checkpoint_count}");
                            }
                            Ok(None) => panic!("Connection {conn_id}(op={op_num}) Checkpoint {mode}: No rows returned"),
                            Err(e) => {
                                println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                if !e.to_string().contains("database is locked") && !e.to_string().contains("database table is locked") {
                                    panic!("Unexpected error during checkpoint: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn generate_operation(
    rng: &mut ChaCha8Rng,
    current_tx_id: Option<usize>,
    shadow_db: &mut ShadowDb,
) -> (Operation, Vec<DbRow>) {
    let in_transaction = current_tx_id.is_some();
    let schema_clone = if let Some(tx_id) = current_tx_id {
        if let Some(Some(tx_state)) = shadow_db.transactions.get(&tx_id) {
            tx_state.schema.clone()
        } else {
            shadow_db.schema.clone()
        }
    } else {
        shadow_db.schema.clone()
    };
    let get_visible_rows = || {
        if let Some(tx_id) = current_tx_id {
            shadow_db.get_visible_rows(Some(tx_id))
        } else {
            shadow_db.get_visible_rows(None) // No transaction
        }
    };

    let mut start = 0;
    let range_begin_deferred = start..start + shadow_db.query_gen_options.weight_begin_deferred;
    start += shadow_db.query_gen_options.weight_begin_deferred;
    let range_begin_concurrent = start..start + shadow_db.query_gen_options.weight_begin_concurrent;
    start += shadow_db.query_gen_options.weight_begin_concurrent;
    let range_commit = start..start + shadow_db.query_gen_options.weight_commit;
    start += shadow_db.query_gen_options.weight_commit;
    let range_rollback = start..start + shadow_db.query_gen_options.weight_rollback;
    start += shadow_db.query_gen_options.weight_rollback;
    let range_checkpoint = start..start + shadow_db.query_gen_options.weight_checkpoint;
    start += shadow_db.query_gen_options.weight_checkpoint;
    let range_ddl = start..start + shadow_db.query_gen_options.weight_ddl;
    start += shadow_db.query_gen_options.weight_ddl;
    let range_dml = start..start + shadow_db.query_gen_options.weight_dml;
    start += shadow_db.query_gen_options.weight_dml;

    let random_val = rng.random_range(0..start);

    if range_begin_deferred.contains(&random_val) {
        if !in_transaction {
            (Operation::Begin { concurrent: false }, get_visible_rows())
        } else {
            let visible_rows = get_visible_rows();
            (
                generate_data_operation(
                    rng,
                    &visible_rows,
                    &schema_clone,
                    &shadow_db.query_gen_options.dml_gen_options,
                ),
                visible_rows,
            )
        }
    } else if range_begin_concurrent.contains(&random_val) {
        if !in_transaction {
            (Operation::Begin { concurrent: true }, get_visible_rows())
        } else {
            let visible_rows = get_visible_rows();
            (
                generate_data_operation(
                    rng,
                    &visible_rows,
                    &schema_clone,
                    &shadow_db.query_gen_options.dml_gen_options,
                ),
                visible_rows,
            )
        }
    } else if range_commit.contains(&random_val) {
        if in_transaction {
            (Operation::Commit, get_visible_rows())
        } else {
            let visible_rows = get_visible_rows();
            (
                generate_data_operation(
                    rng,
                    &visible_rows,
                    &schema_clone,
                    &shadow_db.query_gen_options.dml_gen_options,
                ),
                visible_rows,
            )
        }
    } else if range_rollback.contains(&random_val) {
        if in_transaction {
            (Operation::Rollback, get_visible_rows())
        } else {
            let visible_rows = get_visible_rows();
            (
                generate_data_operation(
                    rng,
                    &visible_rows,
                    &schema_clone,
                    &shadow_db.query_gen_options.dml_gen_options,
                ),
                visible_rows,
            )
        }
    } else if range_checkpoint.contains(&random_val) {
        let mode = shadow_db
            .query_gen_options
            .checkpoint_modes
            .choose(rng)
            .unwrap();
        (
            Operation::Checkpoint { mode: mode.clone() },
            get_visible_rows(),
        )
    } else if range_ddl.contains(&random_val) {
        let op = match rng.random_range(0..6) {
            0..=2 => AlterTableOp::AddColumn {
                name: format!("col_{}", rng.random_range(1..i64::MAX)),
                ty: "TEXT".to_string(),
            },
            3..=4 => {
                let table_schema = schema_clone.get("test_table").unwrap();
                let columns_no_id = table_schema
                    .columns
                    .iter()
                    .filter(|c| c.name != "id")
                    .collect::<Vec<_>>();
                if columns_no_id.is_empty() {
                    AlterTableOp::AddColumn {
                        name: format!("col_{}", rng.random_range(1..i64::MAX)),
                        ty: "TEXT".to_string(),
                    }
                } else {
                    let column = columns_no_id.choose(rng).unwrap();
                    AlterTableOp::DropColumn {
                        name: column.name.clone(),
                    }
                }
            }
            5 => {
                let columns_no_id = schema_clone
                    .get("test_table")
                    .unwrap()
                    .columns
                    .iter()
                    .filter(|c| c.name != "id")
                    .collect::<Vec<_>>();
                if columns_no_id.is_empty() {
                    AlterTableOp::AddColumn {
                        name: format!("col_{}", rng.random_range(1..i64::MAX)),
                        ty: "TEXT".to_string(),
                    }
                } else {
                    let column = columns_no_id.choose(rng).unwrap();
                    AlterTableOp::RenameColumn {
                        old_name: column.name.clone(),
                        new_name: format!("col_{}", rng.random_range(1..i64::MAX)),
                    }
                }
            }
            _ => unreachable!(),
        };
        (Operation::AlterTable { op }, get_visible_rows())
    } else if range_dml.contains(&random_val) {
        let visible_rows = get_visible_rows();
        (
            generate_data_operation(
                rng,
                &visible_rows,
                &schema_clone,
                &shadow_db.query_gen_options.dml_gen_options,
            ),
            visible_rows,
        )
    } else {
        unreachable!()
    }
}

fn generate_data_operation(
    rng: &mut ChaCha8Rng,
    visible_rows: &[DbRow],
    schema: &BTreeMap<String, TableSchema>,
    dml_gen_options: &DmlGenOptions,
) -> Operation {
    let table_schema = schema.get("test_table").unwrap();
    let generate_insert_operation = |rng: &mut ChaCha8Rng| {
        let id = rng.random_range(1..i64::MAX);
        let mut other_columns = BTreeMap::new();
        for column in table_schema.columns.iter() {
            if column.name == "id" {
                continue;
            }
            other_columns.insert(
                column.name.clone(),
                match column.ty.as_str() {
                    "TEXT" => Value::Text(format!("text_{}", rng.random_range(1..i64::MAX))),
                    "INTEGER" => Value::Integer(rng.random_range(1..i64::MAX)),
                    "REAL" => Value::Real(rng.random_range(1..i64::MAX) as f64),
                    _ => Value::Null,
                },
            );
        }
        Operation::Insert { id, other_columns }
    };
    let mut start = 0;
    let range_insert = start..start + dml_gen_options.weight_insert;
    start += dml_gen_options.weight_insert;
    let range_update = start..start + dml_gen_options.weight_update;
    start += dml_gen_options.weight_update;
    let range_delete = start..start + dml_gen_options.weight_delete;
    start += dml_gen_options.weight_delete;
    let range_select = start..start + dml_gen_options.weight_select;
    start += dml_gen_options.weight_select;

    let random_val = rng.random_range(0..start);

    if range_insert.contains(&random_val) {
        generate_insert_operation(rng)
    } else if range_update.contains(&random_val) {
        if visible_rows.is_empty() {
            // No rows to update, try insert instead
            generate_insert_operation(rng)
        } else {
            let columns_no_id = table_schema
                .columns
                .iter()
                .filter(|c| c.name != "id")
                .collect::<Vec<_>>();
            if columns_no_id.is_empty() {
                // No columns to update, try insert instead
                return generate_insert_operation(rng);
            }
            let id = visible_rows.choose(rng).unwrap().id;
            let col_name_to_update = columns_no_id.choose(rng).unwrap().name.clone();
            let mut other_columns = BTreeMap::new();
            other_columns.insert(
                col_name_to_update.clone(),
                match columns_no_id
                    .iter()
                    .find(|c| c.name == col_name_to_update)
                    .unwrap()
                    .ty
                    .as_str()
                {
                    "TEXT" => Value::Text(format!("updated_{}", rng.random_range(1..i64::MAX))),
                    "INTEGER" => Value::Integer(rng.random_range(1..i64::MAX)),
                    "REAL" => Value::Real(rng.random_range(1..i64::MAX) as f64),
                    _ => Value::Null,
                },
            );
            Operation::Update { id, other_columns }
        }
    } else if range_delete.contains(&random_val) {
        if visible_rows.is_empty() {
            // No rows to delete, try insert instead
            generate_insert_operation(rng)
        } else {
            let id = visible_rows.choose(rng).unwrap().id;
            Operation::Delete { id }
        }
    } else if range_select.contains(&random_val) {
        Operation::Select
    } else {
        unreachable!()
    }
}
