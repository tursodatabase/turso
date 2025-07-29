use rand::seq::SliceRandom;
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use std::collections::{HashMap, HashSet};
use turso::{Builder, Value};

// In-memory representation of the database state
#[derive(Debug, Clone, PartialEq, Eq)]
struct DbRow {
    id: i64,
    text: String,
}

impl std::fmt::Display for DbRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(id={}, text={})", self.id, self.text)
    }
}

#[derive(Debug, Clone)]
struct TransactionState {
    // What this transaction can see (snapshot)
    visible_rows: HashMap<i64, DbRow>,
    // Pending changes in this transaction
    pending_inserts: HashMap<i64, DbRow>,
    pending_updates: HashMap<i64, DbRow>,
    pending_deletes: HashSet<i64>,
}

#[derive(Debug)]
struct ShadowDb {
    // Committed state (what's actually in the database)
    committed_rows: HashMap<i64, DbRow>,
    // Transaction states
    transactions: HashMap<usize, Option<TransactionState>>,
}

impl ShadowDb {
    fn new() -> Self {
        Self {
            committed_rows: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    fn begin_transaction(&mut self, tx_id: usize, immediate: bool) {
        self.transactions.insert(
            tx_id,
            if immediate {
                Some(TransactionState {
                    visible_rows: self.committed_rows.clone(),
                    pending_inserts: HashMap::new(),
                    pending_updates: HashMap::new(),
                    pending_deletes: HashSet::new(),
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
                visible_rows: self.committed_rows.clone(),
                pending_inserts: HashMap::new(),
                pending_updates: HashMap::new(),
                pending_deletes: HashSet::new(),
            });
        }
    }

    fn commit_transaction(&mut self, tx_id: usize) {
        if let Some(tx_state) = self.transactions.remove(&tx_id) {
            let tx_state = tx_state.unwrap();
            // Apply pending changes to committed state
            for (id, row) in tx_state.pending_inserts {
                println!("Committing insert of row {}", row);
                self.committed_rows.insert(id, row);
            }
            for (id, row) in tx_state.pending_updates {
                println!("Committing update of row {}", row);
                self.committed_rows.insert(id, row);
            }
            for id in tx_state.pending_deletes {
                println!("Committing delete of row {id}");
                self.committed_rows.remove(&id);
            }
        }
    }

    fn rollback_transaction(&mut self, tx_id: usize) {
        self.transactions.remove(&tx_id);
    }

    fn insert(&mut self, tx_id: usize, id: i64, text: String) -> Result<(), String> {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            // Check if row exists in visible state
            if tx_state.as_ref().unwrap().visible_rows.contains_key(&id)
                || tx_state.as_ref().unwrap().pending_inserts.contains_key(&id)
            {
                return Err("UNIQUE constraint failed".to_string());
            }
            let row = DbRow { id, text };
            tx_state
                .as_mut()
                .unwrap()
                .pending_inserts
                .insert(id, row.clone());
            tx_state.as_mut().unwrap().visible_rows.insert(id, row);
            Ok(())
        } else {
            Err("No active transaction".to_string())
        }
    }

    fn update(&mut self, tx_id: usize, id: i64, text: String) -> Result<(), String> {
        if let Some(tx_state) = self.transactions.get_mut(&tx_id) {
            // Check if row exists in visible state
            if !tx_state.as_ref().unwrap().visible_rows.contains_key(&id) {
                return Err("Row not found".to_string());
            }
            let row = DbRow { id, text };
            tx_state
                .as_mut()
                .unwrap()
                .pending_updates
                .insert(id, row.clone());
            tx_state.as_mut().unwrap().visible_rows.insert(id, row);
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
            tx_state.as_mut().unwrap().pending_deletes.insert(id);
            tx_state.as_mut().unwrap().visible_rows.remove(&id);
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
            let tx_state = tx_state.as_ref().unwrap();
            tx_state.visible_rows.values().cloned().collect()
        } else {
            // No transaction - see committed state
            self.committed_rows.values().cloned().collect()
        }
    }
}

#[derive(Debug)]
enum Operation {
    Begin,
    Commit,
    Rollback,
    Insert { id: i64, text: String },
    Update { id: i64, text: String },
    Delete { id: i64 },
    Select,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Begin => write!(f, "BEGIN"),
            Operation::Commit => write!(f, "COMMIT"),
            Operation::Rollback => write!(f, "ROLLBACK"),
            Operation::Insert { id, text } => {
                write!(f, "INSERT INTO test_table (id, text) VALUES ({id}, {text})")
            }
            Operation::Update { id, text } => {
                write!(f, "UPDATE test_table SET text = {text} WHERE id = {id}")
            }
            Operation::Delete { id } => write!(f, "DELETE FROM test_table WHERE id = {id}"),
            Operation::Select => write!(f, "SELECT * FROM test_table"),
        }
    }
}

#[derive(Debug)]
struct ConnectionState {
    tx_id: Option<usize>,
    shadow_db: ShadowDb,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            tx_id: None,
            shadow_db: ShadowDb::new(),
        }
    }

    fn generate_operation(&self, rng: &mut ChaCha8Rng) -> Operation {
        let visible_rows = if let Some(tx_id) = self.tx_id {
            self.shadow_db.get_visible_rows(Some(tx_id))
        } else {
            self.shadow_db.get_visible_rows(None) // No transaction
        };

        match rng.gen_range(0..100) {
            // 10% chance to begin transaction
            0..=9 => {
                if self.tx_id.is_none() {
                    Operation::Begin
                } else {
                    // If already in transaction, do something else
                    self.generate_data_operation(rng, &visible_rows)
                }
            }
            // 5% chance to commit
            10..=14 => {
                if self.tx_id.is_some() {
                    Operation::Commit
                } else {
                    self.generate_data_operation(rng, &visible_rows)
                }
            }
            // 5% chance to rollback
            15..=19 => {
                if self.tx_id.is_some() {
                    Operation::Rollback
                } else {
                    self.generate_data_operation(rng, &visible_rows)
                }
            }
            // 80% chance for data operations
            _ => self.generate_data_operation(rng, &visible_rows),
        }
    }

    fn generate_data_operation(&self, rng: &mut ChaCha8Rng, visible_ids: &[DbRow]) -> Operation {
        match rng.gen_range(0..4) {
            0 => {
                // Insert - generate a new ID that doesn't exist
                let id = if visible_ids.is_empty() {
                    rng.gen_range(1..1000)
                } else {
                    let max_id = visible_ids.iter().map(|r| r.id).max().unwrap();
                    rng.gen_range(max_id + 1..max_id + 100)
                };
                let text = format!("text_{}", rng.gen_range(1..1000));
                Operation::Insert { id, text }
            }
            1 => {
                // Update - only if there are visible rows
                if visible_ids.is_empty() {
                    // No rows to update, try insert instead
                    let id = rng.gen_range(1..1000);
                    let text = format!("text_{}", rng.gen_range(1..1000));
                    Operation::Insert { id, text }
                } else {
                    let id = visible_ids.choose(rng).unwrap().id;
                    let text = format!("updated_{}", rng.gen_range(1..1000));
                    Operation::Update { id, text }
                }
            }
            2 => {
                // Delete - only if there are visible rows
                if visible_ids.is_empty() {
                    // No rows to delete, try insert instead
                    let id = rng.gen_range(1..1000);
                    let text = format!("text_{}", rng.gen_range(1..1000));
                    Operation::Insert { id, text }
                } else {
                    let id = visible_ids.choose(rng).unwrap().id;
                    Operation::Delete { id }
                }
            }
            3 => Operation::Select,
            _ => unreachable!(),
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
#[ignore]
/// Verify translation isolation semantics with multiple concurrent connections.
/// This test is ignored because it still fails sometimes; unsure if it fails due to a bug in the test or a bug in the implementation.
async fn test_multiple_connections_fuzz() {
    let (mut rng, seed) = rng_from_time_or_env();
    println!("Multiple connections fuzz test seed: {}", seed);

    const NUM_ITERATIONS: usize = 50;
    const OPERATIONS_PER_CONNECTION: usize = 30;
    const NUM_CONNECTIONS: usize = 2;

    for iteration in 0..NUM_ITERATIONS {
        // Create a fresh database for each iteration
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let db = Builder::new_local(tempfile.path().to_str().unwrap())
            .build()
            .await
            .unwrap();

        // SHARED shadow database for all connections
        let mut shared_shadow_db = ShadowDb::new();
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
                let visible_rows = if let Some(tx_id) = *current_tx_id {
                    // Take snapshot during first operation after a BEGIN, not immediately at BEGIN (the semantics is BEGIN DEFERRED)
                    let tx_state = shared_shadow_db.transactions.get(&tx_id).unwrap();
                    if tx_state.is_none() {
                        shared_shadow_db.take_snapshot(tx_id);
                    }
                    shared_shadow_db.get_visible_rows(Some(tx_id))
                } else {
                    shared_shadow_db.get_visible_rows(None) // No transaction
                };

                let operation =
                    generate_operation(&mut rng, current_tx_id.is_some(), &visible_rows);

                println!("Connection {conn_id}(op={op_num}): {}", operation);

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
                                if !e.to_string().contains("Busy")
                                    && !e.to_string().contains("database is locked")
                                {
                                    panic!("Unexpected error during commit: {}", e);
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
                    Operation::Insert { id, text } => {
                        let result = conn
                            .execute(
                                "INSERT INTO test_table (id, text) VALUES (?, ?)",
                                vec![Value::Integer(id), Value::Text(text.clone())],
                            )
                            .await;

                        // Check if real DB operation succeeded
                        match result {
                            Ok(_) => {
                                // Success - update shadow DB
                                if let Some(tx_id) = *current_tx_id {
                                    // In transaction - update transaction's view
                                    shared_shadow_db.insert(tx_id, id, text.clone()).unwrap();
                                } else {
                                    // Auto-commit - update shadow DB committed state
                                    shared_shadow_db.begin_transaction(next_tx_id, true);
                                    shared_shadow_db
                                        .insert(next_tx_id, id, text.clone())
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
                    Operation::Update { id, text } => {
                        let result = conn
                            .execute(
                                "UPDATE test_table SET text = ? WHERE id = ?",
                                vec![Value::Text(text.clone()), Value::Integer(id)],
                            )
                            .await;

                        // Check if real DB operation succeeded
                        match result {
                            Ok(_) => {
                                // Success - update shadow DB
                                if let Some(tx_id) = *current_tx_id {
                                    // In transaction - update transaction's view
                                    shared_shadow_db.update(tx_id, id, text.clone()).unwrap();
                                } else {
                                    // Auto-commit - update shadow DB committed state
                                    shared_shadow_db.begin_transaction(next_tx_id, true);
                                    shared_shadow_db
                                        .update(next_tx_id, id, text.clone())
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
                                "DELETE FROM test_table WHERE id = ?",
                                vec![Value::Integer(id)],
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
                        let query_str = "SELECT id, text FROM test_table ORDER BY id";
                        let mut rows = conn.query(query_str, ()).await.unwrap();

                        let mut real_rows = Vec::new();
                        while let Some(row) = rows.next().await.unwrap() {
                            let id = row.get_value(0).unwrap();
                            let text = row.get_value(1).unwrap();

                            if let (Value::Integer(id), Value::Text(text)) = (id, text) {
                                real_rows.push(DbRow { id, text });
                            }
                        }
                        real_rows.sort_by_key(|r| r.id);

                        let mut expected_rows = visible_rows.clone();
                        expected_rows.sort_by_key(|r| r.id);

                        if real_rows != expected_rows {
                            panic!(
                                "Row mismatch in iteration {iteration} Connection {conn_id}(op={op_num}). Query: {query_str}.\n\nExpected: {}\n\nGot: {}\n\nSeed: {seed}",
                                expected_rows.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "),
                                real_rows.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "),
                            );
                        }
                    }
                }
            }
        }
    }
}

fn generate_operation(
    rng: &mut ChaCha8Rng,
    in_transaction: bool,
    visible_rows: &[DbRow],
) -> Operation {
    match rng.gen_range(0..100) {
        // 10% chance to begin transaction
        0..=9 => {
            if !in_transaction {
                Operation::Begin
            } else {
                generate_data_operation(rng, visible_rows)
            }
        }
        // 5% chance to commit
        10..=14 => {
            if in_transaction {
                Operation::Commit
            } else {
                generate_data_operation(rng, visible_rows)
            }
        }
        // 5% chance to rollback
        15..=19 => {
            if in_transaction {
                Operation::Rollback
            } else {
                generate_data_operation(rng, visible_rows)
            }
        }
        // 80% chance for data operations
        _ => generate_data_operation(rng, visible_rows),
    }
}

fn generate_data_operation(rng: &mut ChaCha8Rng, visible_rows: &[DbRow]) -> Operation {
    match rng.gen_range(0..4) {
        0 => {
            // Insert - generate a new ID that doesn't exist
            let id = if visible_rows.is_empty() {
                rng.gen_range(1..1000)
            } else {
                let max_id = visible_rows.iter().map(|r| r.id).max().unwrap();
                rng.gen_range(max_id + 1..max_id + 100)
            };
            let text = format!("text_{}", rng.gen_range(1..1000));
            Operation::Insert { id, text }
        }
        1 => {
            // Update - only if there are visible rows
            if visible_rows.is_empty() {
                // No rows to update, try insert instead
                let id = rng.gen_range(1..1000);
                let text = format!("text_{}", rng.gen_range(1..1000));
                Operation::Insert { id, text }
            } else {
                let id = visible_rows.choose(rng).unwrap().id;
                let text = format!("updated_{}", rng.gen_range(1..1000));
                Operation::Update { id, text }
            }
        }
        2 => {
            // Delete - only if there are visible rows
            if visible_rows.is_empty() {
                // No rows to delete, try insert instead
                let id = rng.gen_range(1..1000);
                let text = format!("text_{}", rng.gen_range(1..1000));
                Operation::Insert { id, text }
            } else {
                let id = visible_rows.choose(rng).unwrap().id;
                Operation::Delete { id }
            }
        }
        3 => Operation::Select,
        _ => unreachable!(),
    }
}
