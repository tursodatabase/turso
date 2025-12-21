use core_tester::common::{
    limbo_exec_rows, maybe_setup_tracing, rng_from_time_or_env, sqlite_exec_rows, ExecRows,
    TempDatabase, TempDatabaseBuilder,
};
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rusqlite::params;
use std::sync::Arc;
use tempfile::TempDir;

/// Minimal reproduction of the journal mode switching bug.
/// Deletes performed in experimental_mvcc mode are lost when switching to wal mode.
#[test]
fn journal_mode_delete_lost_on_switch() {
    maybe_setup_tracing();

    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Create schema
    let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);";

    // Open and enable MVCC via PRAGMA
    let limbo_db = TempDatabaseBuilder::new().with_db_path(&db_path).build();
    let conn = limbo_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("enable mvcc");

    // Create table
    conn.prepare_execute_batch(schema).unwrap();

    // Verify we start in experimental_mvcc mode
    let mode = get_limbo_journal_mode(&conn);
    println!("Initial mode: {mode}");
    assert_eq!(mode, "experimental_mvcc");

    // Insert a row in experimental_mvcc mode
    conn.execute("INSERT INTO t(id, val) VALUES (1, 'test')")
        .unwrap();

    // Verify row exists
    let rows = limbo_exec_rows(&conn, "SELECT * FROM t ORDER BY id");
    println!("After insert: {rows:?}");
    assert_eq!(rows.len(), 1);

    // Switch to WAL mode
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("switch to wal");
    let mode = result[0][0].to_string();
    println!("Switched to: {mode}");
    assert_eq!(mode, "wal");

    // Verify row still exists after switch
    let rows = limbo_exec_rows(&conn, "SELECT * FROM t ORDER BY id");
    println!("After switch to WAL: {rows:?}");
    assert_eq!(rows.len(), 1);

    // Switch back to experimental_mvcc
    let result = conn
        .pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("switch to mvcc");
    let mode = result[0][0].to_string();
    println!("Switched to: {mode}");
    assert_eq!(mode, "experimental_mvcc");

    // Delete the row in experimental_mvcc mode
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Verify row is deleted
    let rows = limbo_exec_rows(&conn, "SELECT * FROM t ORDER BY id");
    println!("After delete in MVCC: {rows:?}");
    assert_eq!(rows.len(), 0, "Row should be deleted");

    // Switch back to WAL mode - THIS IS WHERE THE BUG OCCURS
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("switch to wal");
    let mode = result[0][0].to_string();
    println!("Switched to: {mode}");

    // BUG: Row reappears after switching to WAL!
    let rows = limbo_exec_rows(&conn, "SELECT * FROM t ORDER BY id");
    println!("After switch to WAL (BUG CHECK): {rows:?}");
    assert_eq!(
        rows.len(),
        0,
        "BUG: Row was deleted but reappeared after switching from experimental_mvcc to wal!"
    );
}

/// Regression test for UPDATE-then-DELETE of B-tree resident rows.
///
/// This tests a specific bug where:
/// 1. A row is inserted in MVCC and checkpointed to B-tree
/// 2. After switching WAL->MVCC, a new MvStore is created (row only in B-tree)
/// 3. The row is UPDATED (creating an MVCC version with begin_ts > 0)
/// 4. The row is DELETED
/// 5. When switching back to WAL, the delete was not being checkpointed because
///    the checkpoint logic didn't know the row existed in B-tree
///
/// The bug occurs because:
/// - After WAL->MVCC switch, `checkpointed_txid_max_old` is None (fresh MvStore)
/// - The UPDATE creates an MVCC version with `begin_ts > 0`
/// - The checkpoint logic with `is_some_and` requires `begin_ts <= checkpointed_txid_max_old`
///   to recognize the row as existing in B-tree, but that check fails when
///   `checkpointed_txid_max_old` is None
/// - Result: the delete is not checkpointed, and the row "reappears" with its old value
///
/// The fix adds a `btree_resident` flag to RowVersion that's set when updating
/// a row that exists in B-tree but not in MvStore. The checkpoint logic then
/// checks this flag to properly recognize B-tree resident rows.
#[test]
fn journal_mode_update_then_delete_btree_resident() {
    maybe_setup_tracing();

    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);";

    // Open and enable MVCC via PRAGMA
    let limbo_db = TempDatabaseBuilder::new().with_db_path(&db_path).build();
    let conn = limbo_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("enable mvcc");

    // Create table
    conn.prepare_execute_batch(schema).unwrap();

    // Step 1: Insert a row in MVCC mode
    conn.execute("INSERT INTO t(id, val) VALUES (1, 'original')")
        .unwrap();
    println!("Step 1: Inserted row in MVCC");

    // Step 2: Switch to WAL (checkpoints the row to B-tree)
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("switch to wal");
    println!("Step 2: Switched to WAL (checkpointed to B-tree)");
    assert_eq!(result[0][0].to_string(), "wal");

    // Verify row exists in B-tree
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "original");

    // Step 3: Switch back to MVCC (creates new MvStore, row only in B-tree)
    // This is the key step - after this, the row ONLY exists in B-tree,
    // and checkpointed_txid_max will be 0 (None when converted to NonZeroU64)
    let result = conn
        .pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("switch to mvcc");
    println!("Step 3: Switched to MVCC (new MvStore, row only in B-tree)");
    assert_eq!(result[0][0].to_string(), "experimental_mvcc");

    // Verify row still visible (reading from B-tree)
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);

    // Step 4: UPDATE the row
    // This creates an MVCC version for a B-tree-resident row.
    // Without the btree_resident fix, this version has btree_resident=false
    // and begin_ts > 0, which means checkpoint won't recognize it as a B-tree row.
    conn.execute("UPDATE t SET val = 'updated' WHERE id = 1")
        .unwrap();
    println!("Step 4: Updated row (creates MVCC version for B-tree resident row)");

    // Verify update worked
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "updated");

    // Step 5: DELETE the row
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    println!("Step 5: Deleted row");

    // Verify delete worked in MVCC
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 0, "Row should be deleted in MVCC");

    // Step 6: Switch to WAL - this triggers checkpoint
    // BUG: Without btree_resident fix, the checkpoint doesn't recognize
    // that the deleted row existed in B-tree, so it doesn't checkpoint the delete.
    // Result: the old value from B-tree "reappears"
    let result = conn
        .pragma_update("journal_mode", "'wal'")
        .expect("switch to wal");
    println!("Step 6: Switched to WAL (checkpoint)");
    assert_eq!(result[0][0].to_string(), "wal");

    // BUG CHECK: Row should remain deleted after checkpoint
    // Without the fix, the row reappears with value "original" (from B-tree)
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM t ORDER BY id");
    println!("Final state: {rows:?}");
    assert_eq!(
        rows.len(),
        0,
        "BUG: Row was updated then deleted, but reappeared after checkpoint! \
         The btree_resident flag should ensure the delete is checkpointed."
    );
}

/// Fuzz test that attempts to constantly change the journal mode of the database
/// between `wal` and `experimental_mvcc`
///
/// It tries to insert, delete, update some data and do the same thing in SQLite
/// and constantly checks if the data is exactly the same as in SQLite
#[turso_macros::test(mvcc)]
pub fn journal_mode_fuzz(db: TempDatabase) {
    maybe_setup_tracing();
    let (mut rng, seed) = rng_from_time_or_env();
    println!("journal_mode_fuzz seed: {seed}");

    let iterations = std::env::var("FUZZ_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);

    // Create a temp directory for the Limbo database
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("test.db");

    // Schema for both databases
    let schema = r#"
        CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT, num INT);
        CREATE TABLE t2(id INTEGER PRIMARY KEY, data TEXT, count INT);
    "#;

    let start_with_mvcc = db.enable_mvcc;

    // If the test does not start with MVCC, initially open the DB in sqlite with WAL mode
    if !start_with_mvcc {
        // Step 1: Create a proper WAL database using SQLite first
        // This ensures the database is properly initialized before Limbo opens it
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "wal").unwrap();
            conn.execute_batch(schema).unwrap();
            // Checkpoint to ensure header is properly written
            conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
                .unwrap();
            drop(conn);
        }
    }

    // Step 2: Open the database with Limbo
    let builder = TempDatabaseBuilder::new()
        .with_db_path(&db_path)
        .with_flags(db.db_flags)
        .with_opts(db.db_opts);

    let limbo_db = builder.build();
    let limbo_conn = limbo_db.connect_limbo();

    // Enable MVCC via PRAGMA
    limbo_conn
        .pragma_update("journal_mode", "'experimental_mvcc'")
        .expect("enable mvcc");

    // If starting with MVCC, create the schema in Limbo
    if start_with_mvcc {
        limbo_conn.prepare_execute_batch(schema).unwrap();
    }

    // Step 3: Create SQLite in-memory database for comparison
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    sqlite_conn.execute_batch(schema).unwrap();

    // Track current journal mode
    let mut current_mode = get_limbo_journal_mode(&limbo_conn);
    println!("Initial journal mode: {current_mode}");

    // Track next id for inserts
    let mut next_id_t1: i64 = 1;
    let mut next_id_t2: i64 = 1;

    // Define actions with weights
    #[derive(Clone, Copy)]
    enum Action {
        Insert,
        Update,
        Delete,
        SwitchMode,
    }

    let actions = [
        (Action::Insert, 40),     // 40% chance
        (Action::Update, 20),     // 20% chance
        (Action::Delete, 20),     // 20% chance
        (Action::SwitchMode, 20), // 20% chance
    ];
    let weights: Vec<_> = actions.iter().map(|(_, w)| *w).collect();
    let action_dist = WeightedIndex::new(&weights).unwrap();

    for iter in 0..iterations {
        if iter % 50 == 0 {
            println!("journal_mode_fuzz iter {iter}/{iterations} (mode: {current_mode})");
        }

        let action = actions[action_dist.sample(&mut rng)].0;

        // Enable verbose logging for debugging
        let verbose = std::env::var("VERBOSE").is_ok();

        match action {
            Action::Insert => {
                let table = if rng.random_bool(0.5) { "t1" } else { "t2" };
                let (id, val_col, num_col) = if table == "t1" {
                    let id = next_id_t1;
                    next_id_t1 += 1;
                    (id, "val", "num")
                } else {
                    let id = next_id_t2;
                    next_id_t2 += 1;
                    (id, "data", "count")
                };

                let text_val = generate_random_text(&mut rng);
                let num_val = rng.random_range(-1000..1000);

                let stmt = format!(
                    "INSERT INTO {table}(id, {val_col}, {num_col}) VALUES ({id}, '{text_val}', {num_val})"
                );

                if verbose {
                    println!("[{iter}] INSERT {table} id={id} (mode: {current_mode})");
                }
                execute_on_both(&limbo_conn, &sqlite_conn, &stmt, seed, iter);
            }
            Action::Update => {
                let table = if rng.random_bool(0.5) { "t1" } else { "t2" };
                let (val_col, num_col, max_id) = if table == "t1" {
                    ("val", "num", next_id_t1)
                } else {
                    ("data", "count", next_id_t2)
                };

                if max_id > 1 {
                    let id = rng.random_range(1..max_id);
                    let new_text = generate_random_text(&mut rng);
                    let new_num = rng.random_range(-1000..1000);

                    let stmt = format!(
                        "UPDATE {table} SET {val_col} = '{new_text}', {num_col} = {new_num} WHERE id = {id}"
                    );

                    if verbose {
                        println!("[{iter}] UPDATE {table} id={id} (mode: {current_mode})");
                    }
                    execute_on_both(&limbo_conn, &sqlite_conn, &stmt, seed, iter);
                }
            }
            Action::Delete => {
                let table = if rng.random_bool(0.5) { "t1" } else { "t2" };
                let max_id = if table == "t1" {
                    next_id_t1
                } else {
                    next_id_t2
                };

                if max_id > 1 {
                    let id = rng.random_range(1..max_id);
                    let stmt = format!("DELETE FROM {table} WHERE id = {id}");

                    if verbose {
                        println!("[{iter}] DELETE {table} id={id} (mode: {current_mode})");
                    }
                    execute_on_both(&limbo_conn, &sqlite_conn, &stmt, seed, iter);
                }
            }
            Action::SwitchMode => {
                let new_mode = if current_mode == "wal" {
                    "experimental_mvcc"
                } else {
                    "wal"
                };

                let result = limbo_conn
                    .pragma_update("journal_mode", format!("'{new_mode}'"))
                    .expect("PRAGMA journal_mode update should not fail");

                assert!(
                    !result.is_empty(),
                    "journal mode should always return something"
                );
                current_mode = result[0][0].to_string();
                println!("Switched journal mode to: {current_mode} at iter {iter}");
            }
        }

        // Verify data consistency after each operation
        verify_tables_match(&limbo_conn, &sqlite_conn, seed, iter);
    }

    println!("journal_mode_fuzz completed successfully after {iterations} iterations");
}

fn get_limbo_journal_mode(conn: &Arc<turso_core::Connection>) -> String {
    let result = conn
        .pragma_query("journal_mode")
        .expect("PRAGMA journal_mode query should not fail");
    assert!(!result.is_empty(), "jounral mode result cannot be empty");
    result[0][0].to_string()
}

fn generate_random_text(rng: &mut ChaCha8Rng) -> String {
    let len = rng.random_range(1..20);
    let chars: Vec<char> = (0..len)
        .map(|_| {
            let c = rng.random_range(b'a'..=b'z');
            c as char
        })
        .collect();
    chars.into_iter().collect()
}

fn execute_on_both(
    limbo_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    stmt: &str,
    seed: u64,
    iter: usize,
) {
    // Execute on SQLite
    if let Err(e) = sqlite_conn.execute(stmt, params![]) {
        panic!(
            "SQLite execution failed!\nSeed: {seed}\nIteration: {iter}\nStatement: {stmt}\nError: {e}"
        );
    }

    // Execute on Limbo
    if let Err(e) = limbo_conn.execute(stmt) {
        panic!(
            "Limbo execution failed!\nSeed: {seed}\nIteration: {iter}\nStatement: {stmt}\nError: {e}"
        );
    }
}

fn verify_tables_match(
    limbo_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    seed: u64,
    iter: usize,
) {
    // Check t1
    let query_t1 = "SELECT id, val, num FROM t1 ORDER BY id";
    let limbo_rows_t1 = limbo_exec_rows(limbo_conn, query_t1);
    let sqlite_rows_t1 = sqlite_exec_rows(sqlite_conn, query_t1);

    similar_asserts::assert_eq!(
        Turso: limbo_rows_t1,
        Sqlite: sqlite_rows_t1,
        "TABLE t1 MISMATCH!\nSeed: {seed}\nIteration: {iter}\n\
            Turso ({} rows)\nSQLite ({} rows)",
        limbo_rows_t1.len(),
        sqlite_rows_t1.len(),
    );

    // Check t2
    let query_t2 = "SELECT id, data, count FROM t2 ORDER BY id";
    let limbo_rows_t2 = limbo_exec_rows(limbo_conn, query_t2);
    let sqlite_rows_t2 = sqlite_exec_rows(sqlite_conn, query_t2);

    similar_asserts::assert_eq!(
        Turso: limbo_rows_t2,
        Sqlite: sqlite_rows_t2,
        "TABLE t2 MISMATCH!\nSeed: {seed}\nIteration: {iter}\n\
            Turso ({} rows)\nSQLite ({} rows)",
        limbo_rows_t2.len(),
        sqlite_rows_t2.len(),
    );
}
