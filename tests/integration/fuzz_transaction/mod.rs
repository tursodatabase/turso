use rand::seq::IndexedRandom;
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use std::collections::HashMap;
use turso::{Builder, Value};

// In-memory representation of the database state
#[derive(Debug, Clone, PartialEq)]
struct DbRow {
    id: i64,
    other_columns: HashMap<String, Value>,
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
    schema: HashMap<String, TableSchema>,
    // The rows this transaction can see (snapshot)
    visible_rows: HashMap<i64, DbRow>,
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
    schema: HashMap<String, TableSchema>,
    // Committed state (what's actually in the database)
    committed_rows: HashMap<i64, DbRow>,
    // Transaction states
    transactions: HashMap<usize, Option<TransactionState>>,
}

impl ShadowDb {
    fn new(initial_schema: HashMap<String, TableSchema>) -> Self {
        Self {
            schema: initial_schema,
            committed_rows: HashMap::new(),
            transactions: HashMap::new(),
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

    fn take_snapshot(&mut self, tx_id: usize) {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            assert!(tx_state.is_none());
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
        other_columns: HashMap<String, Value>,
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
        other_columns: HashMap<String, Value>,
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
    Begin,
    Commit,
    Rollback,
    Insert {
        id: i64,
        other_columns: HashMap<String, Value>,
    },
    Update {
        id: i64,
        other_columns: HashMap<String, Value>,
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
            Operation::Begin => write!(f, "BEGIN"),
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

fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

#[tokio::test]
/// Verify translation isolation semantics with multiple concurrent connections.
/// This test is ignored because it still fails sometimes; unsure if it fails due to a bug in the test or a bug in the implementation.
async fn test_multiple_connections_fuzz() {
    multiple_connections_fuzz(false).await
}

#[tokio::test]
#[ignore = "MVCC is currently under development, it is expected to fail"]
// Same as test_multiple_connections_fuzz, but with MVCC enabled.
async fn test_multiple_connections_fuzz_mvcc() {
    multiple_connections_fuzz(true).await
}

async fn multiple_connections_fuzz(mvcc_enabled: bool) {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("Multiple connections fuzz test seed: {seed}");

    const NUM_ITERATIONS: usize = 50;
    const OPERATIONS_PER_CONNECTION: usize = 30;
    const NUM_CONNECTIONS: usize = 2;

    for iteration in 0..NUM_ITERATIONS {
        println!("--- Seed {seed} Iteration {iteration} ---");
        // Create a fresh database for each iteration
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let db = Builder::new_local(tempfile.path().to_str().unwrap())
            .with_mvcc(mvcc_enabled)
            .build()
            .await
            .unwrap();

        // SHARED shadow database for all connections
        let mut schema = HashMap::new();
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
        let mut shared_shadow_db = ShadowDb::new(schema);
        let mut next_tx_id = 0;

        // Create connections
        let mut connections = Vec::new();
        for conn_id in 0..NUM_CONNECTIONS {
            let conn = db.connect().unwrap();

            // Create table if it doesn't exist
            conn.execute(
                "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)",
                (),
            )
            .await
            .unwrap();

            connections.push((conn, conn_id, None::<usize>)); // (connection, conn_id, current_tx_id)
        }

        // Interleave operations between all connections
        for op_num in 0..OPERATIONS_PER_CONNECTION {
            for (conn, conn_id, current_tx_id) in &mut connections {
                // Generate operation based on current transaction state
                let (operation, visible_rows) =
                    generate_operation(&mut rng, *current_tx_id, &mut shared_shadow_db);

                println!("Connection {conn_id}(op={op_num}): {operation}");

                match operation {
                    Operation::Begin => {
                        shared_shadow_db.begin_transaction(next_tx_id, false);
                        *current_tx_id = Some(next_tx_id);
                        next_tx_id += 1;

                        conn.execute("BEGIN", ()).await.unwrap();
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
                            Err(e) => {
                                println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.rollback_transaction(tx_id);
                                    *current_tx_id = None;
                                }

                                // Check if it's an acceptable error
                                if !e.to_string().contains("database is locked") {
                                    panic!("Unexpected error during commit: {e}");
                                }
                            }
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
                                Err(e) => {
                                    println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                    shared_shadow_db.rollback_transaction(tx_id);
                                    *current_tx_id = None;

                                    // Check if it's an acceptable error
                                    if !e.to_string().contains("Busy")
                                        && !e.to_string().contains("database is locked")
                                    {
                                        panic!("Unexpected error during rollback: {e}");
                                    }
                                }
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
                            Err(e) => {
                                println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.rollback_transaction(tx_id);
                                    *current_tx_id = None;
                                }
                                // Check if it's an acceptable error
                                if !e.to_string().contains("database is locked") {
                                    panic!("Unexpected error during insert: {e}");
                                }
                            }
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
                            Err(e) => {
                                println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.rollback_transaction(tx_id);
                                    *current_tx_id = None;
                                }
                                // Check if it's an acceptable error
                                if !e.to_string().contains("database is locked") {
                                    panic!("Unexpected error during update: {e}");
                                }
                            }
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
                            Err(e) => {
                                println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                if let Some(tx_id) = *current_tx_id {
                                    shared_shadow_db.rollback_transaction(tx_id);
                                    *current_tx_id = None;
                                }
                                // Check if it's an acceptable error
                                if !e.to_string().contains("database is locked") {
                                    panic!("Unexpected error during delete: {e}");
                                }
                            }
                        }
                    }
                    Operation::Select => {
                        let query_str = "SELECT * FROM test_table ORDER BY id";
                        let mut stmt = conn.prepare(query_str).await.unwrap();
                        let columns = stmt.columns();
                        let mut rows = stmt.query(()).await.unwrap();

                        let mut real_rows = Vec::new();
                        while let Some(row) = rows.next().await.unwrap() {
                            let Value::Integer(id) = row.get_value(0).unwrap() else {
                                panic!("Unexpected value for id: {:?}", row.get_value(0));
                            };
                            let mut other_columns = HashMap::new();
                            for i in 1..columns.len() {
                                let column = columns.get(i).unwrap();
                                let value = row.get_value(i).unwrap();
                                other_columns.insert(column.name().to_string(), value);
                            }
                            real_rows.push(DbRow { id, other_columns });
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
                            Err(e) => {
                                println!("Connection {conn_id}(op={op_num}) FAILED: {e}");
                                // Check if it's an acceptable error
                                if e.to_string().contains("database is locked") {
                                    if let Some(tx_id) = *current_tx_id {
                                        shared_shadow_db.rollback_transaction(tx_id);
                                        *current_tx_id = None;
                                    }
                                } else {
                                    panic!("Unexpected error during alter table: {e}");
                                }
                            }
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
    let mut get_visible_rows = |accesses_db: bool| {
        if let Some(tx_id) = current_tx_id {
            let tx_state = shadow_db.transactions.get(&tx_id).unwrap();
            // Take snapshot during first operation that accesses the DB after a BEGIN, not immediately at BEGIN (the semantics is BEGIN DEFERRED)
            if accesses_db && tx_state.is_none() {
                shadow_db.take_snapshot(tx_id);
            }
            shadow_db.get_visible_rows(Some(tx_id))
        } else {
            shadow_db.get_visible_rows(None) // No transaction
        }
    };
    match rng.random_range(0..100) {
        0..=9 => {
            if !in_transaction {
                (Operation::Begin, get_visible_rows(false))
            } else {
                let visible_rows = get_visible_rows(true);
                (
                    generate_data_operation(rng, &visible_rows, &schema_clone),
                    visible_rows,
                )
            }
        }
        10..=14 => {
            if in_transaction {
                (Operation::Commit, get_visible_rows(false))
            } else {
                let visible_rows = get_visible_rows(true);
                (
                    generate_data_operation(rng, &visible_rows, &schema_clone),
                    visible_rows,
                )
            }
        }
        15..=19 => {
            if in_transaction {
                (Operation::Rollback, get_visible_rows(false))
            } else {
                let visible_rows = get_visible_rows(true);
                (
                    generate_data_operation(rng, &visible_rows, &schema_clone),
                    visible_rows,
                )
            }
        }
        20..=22 => {
            let mode = match rng.random_range(0..=3) {
                0 => CheckpointMode::Passive,
                1 => CheckpointMode::Restart,
                2 => CheckpointMode::Truncate,
                3 => CheckpointMode::Full,
                _ => unreachable!(),
            };
            (Operation::Checkpoint { mode }, get_visible_rows(false))
        }
        23..=26 => {
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
            (Operation::AlterTable { op }, get_visible_rows(true))
        }
        _ => {
            let visible_rows = get_visible_rows(true);
            (
                generate_data_operation(rng, &visible_rows, &schema_clone),
                visible_rows,
            )
        }
    }
}

fn generate_data_operation(
    rng: &mut ChaCha8Rng,
    visible_rows: &[DbRow],
    schema: &HashMap<String, TableSchema>,
) -> Operation {
    let table_schema = schema.get("test_table").unwrap();
    let op_num = rng.random_range(0..4);
    let mut generate_insert_operation = || {
        let id = rng.random_range(1..i64::MAX);
        let mut other_columns = HashMap::new();
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
    match op_num {
        0 => {
            // Insert
            generate_insert_operation()
        }
        1 => {
            // Update
            if visible_rows.is_empty() {
                // No rows to update, try insert instead
                generate_insert_operation()
            } else {
                let columns_no_id = table_schema
                    .columns
                    .iter()
                    .filter(|c| c.name != "id")
                    .collect::<Vec<_>>();
                if columns_no_id.is_empty() {
                    // No columns to update, try insert instead
                    return generate_insert_operation();
                }
                let id = visible_rows.choose(rng).unwrap().id;
                let col_name_to_update = columns_no_id.choose(rng).unwrap().name.clone();
                let mut other_columns = HashMap::new();
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
        }
        2 => {
            // Delete
            if visible_rows.is_empty() {
                // No rows to delete, try insert instead
                generate_insert_operation()
            } else {
                let id = visible_rows.choose(rng).unwrap().id;
                Operation::Delete { id }
            }
        }
        3 => {
            // Select
            Operation::Select
        }
        _ => unreachable!(),
    }
}
