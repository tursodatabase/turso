use core_tester::common::{
    limbo_exec_rows, maybe_setup_tracing, rng_from_time_or_env, sqlite_exec_rows, TempDatabase,
    TempDatabaseBuilder,
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

    // Open with MVCC enabled
    let opts = turso_core::DatabaseOpts::new().with_mvcc(true);
    let limbo_db = TempDatabaseBuilder::new()
        .with_db_path(&db_path)
        .with_opts(opts)
        .build();
    let conn = limbo_db.connect_limbo();

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

    // If the test does not start with MVCC, initially open the DB in sqlite with WAL mode
    if !db.db_opts.enable_mvcc {
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

    // Step 2: Open the database with Limbo with MVCC enabled
    let opts = db.db_opts.with_mvcc(true);
    let builder = TempDatabaseBuilder::new()
        .with_db_path(&db_path)
        .with_flags(db.db_flags)
        .with_opts(opts);

    let limbo_db = builder.build();
    let limbo_conn = limbo_db.connect_limbo();

    // If starting with MVCC, create the schema in Limbo
    if db.db_opts.enable_mvcc {
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
