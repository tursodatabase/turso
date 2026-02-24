use core_tester::common::{
    limbo_exec_rows, maybe_setup_tracing, rng_from_time_or_env, sqlite_exec_rows, TempDatabase,
    TempDatabaseBuilder,
};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rusqlite::params;
use std::sync::Arc;

/// Read the `FUZZ_MULTIPLIER` env var (default 1.0) and scale `base` by it.
/// Returns at least 1 so loops always execute at least once.
pub fn fuzz_iterations(base: usize) -> usize {
    static MULTIPLIER: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    let m = *MULTIPLIER.get_or_init(|| {
        std::env::var("FUZZ_MULTIPLIER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1.0)
    });
    ((base as f64 * m) as usize).max(1)
}

/// Initialize a fuzz test: set up env_logger, create an RNG, and print the seed.
/// Returns `(rng, seed)`.
pub fn init_fuzz_test(name: &str) -> (ChaCha8Rng, u64) {
    let _ = env_logger::try_init();
    let (rng, seed) = rng_from_time_or_env();
    println!("{name} seed: {seed}");
    (rng, seed)
}

/// Initialize a fuzz test with tracing instead of env_logger.
/// Returns `(rng, seed)`.
pub fn init_fuzz_test_tracing(name: &str) -> (ChaCha8Rng, u64) {
    maybe_setup_tracing();
    let (rng, seed) = rng_from_time_or_env();
    println!("{name} seed: {seed}");
    (rng, seed)
}

/// Run a query on both Limbo and SQLite, assert results are equal.
pub fn assert_differential(
    limbo_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    query: &str,
    context: &str,
) {
    let limbo_result = limbo_exec_rows(limbo_conn, query);
    let sqlite_result = sqlite_exec_rows(sqlite_conn, query);
    assert_eq!(
        limbo_result, sqlite_result,
        "{context}\nquery: {query}\nlimbo: {limbo_result:?}\nsqlite: {sqlite_result:?}"
    );
}

pub fn assert_differential_no_ordering(
    limbo_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    query: &str,
    context: &str,
) {
    let limbo_results = limbo_exec_rows(limbo_conn, query);
    let sqlite_results = sqlite_exec_rows(sqlite_conn, query);

    // Check if results match
    if limbo_results.len() != sqlite_results.len() {
        panic!(
                "Row count mismatch for query: {}\nLimbo: {} rows, SQLite: {} rows\nLimbo: {:?}\nSQLite: {:?}\n",
                query, limbo_results.len(), sqlite_results.len(), limbo_results, sqlite_results,
            );
    }

    // Check if all rows match (order might be different)
    // Since Value doesn't implement Ord, we'll check containment both ways
    let all_limbo_in_sqlite = limbo_results.iter().all(|limbo_row| {
        sqlite_results
            .iter()
            .any(|sqlite_row| limbo_row == sqlite_row)
    });
    let all_sqlite_in_limbo = sqlite_results.iter().all(|sqlite_row| {
        limbo_results
            .iter()
            .any(|limbo_row| sqlite_row == limbo_row)
    });

    if !all_limbo_in_sqlite || !all_sqlite_in_limbo {
        panic!(
                "Results mismatch for query: {query}\nLimbo: {limbo_results:?}\nSQLite: {sqlite_results:?}\n{context}",
            );
    }
}

/// Assert that both engines either succeeded or both failed. Panics on mismatch.
pub fn assert_outcome_parity<T: std::fmt::Debug, U: std::fmt::Debug>(
    sqlite_res: &Result<T, impl std::fmt::Debug>,
    limbo_res: &Result<U, impl std::fmt::Debug>,
    stmt: &str,
    context: &str,
) {
    match (sqlite_res.is_ok(), limbo_res.is_ok()) {
        (true, true) | (false, false) => {}
        _ => {
            panic!(
                "outcome mismatch\n{context}\nstmt: {stmt}\nsqlite: {sqlite_res:?}\nlimbo: {limbo_res:?}"
            );
        }
    }
}

/// Execute a statement on both Limbo and SQLite, panicking if either fails.
pub fn execute_on_both(
    limbo_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    stmt: &str,
    context: &str,
) {
    if let Err(e) = sqlite_conn.execute(stmt, params![]) {
        panic!("SQLite execution failed!\n{context}\nStatement: {stmt}\nError: {e}");
    }
    if let Err(e) = limbo_conn.execute(stmt) {
        panic!("Limbo execution failed!\n{context}\nStatement: {stmt}\nError: {e}");
    }
}

/// Print progress at evenly spaced intervals through a loop.
pub fn log_progress(name: &str, iter: usize, total: usize, num_prints: usize) {
    let interval = (total / num_prints).max(1);
    if iter % interval == 0 {
        println!("{name} iteration {}/{}", iter + 1, total);
    }
}

/// Format the last `max_lines` entries from a statement history for debug output.
pub fn history_tail(history: &[String], max_lines: usize) -> String {
    let start = history.len().saturating_sub(max_lines);
    history[start..]
        .iter()
        .enumerate()
        .map(|(i, stmt)| format!("{:04}: {}", start + i + 1, stmt))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generate a nullable integer: returns `"NULL"` with probability `null_prob`,
/// otherwise a random integer from `range`.
pub fn random_nullable_int(
    rng: &mut ChaCha8Rng,
    range: std::ops::RangeInclusive<i64>,
    null_prob: f64,
) -> String {
    if rng.random_bool(null_prob) {
        "NULL".to_string()
    } else {
        rng.random_range(range).to_string()
    }
}

/// Generate a random lowercase ASCII string of length 1..=`max_len`.
pub fn generate_random_text(rng: &mut ChaCha8Rng, max_len: usize) -> String {
    let len = rng.random_range(1..=max_len);
    (0..len)
        .map(|_| rng.random_range(b'a'..=b'z') as char)
        .collect()
}

/// Create a `TempDatabaseBuilder` pre-configured with the flags and opts from an
/// existing `TempDatabase`.
pub fn builder_from_db(db: &TempDatabase) -> TempDatabaseBuilder {
    TempDatabase::builder()
        .with_flags(db.db_flags)
        .with_opts(db.db_opts)
}

/// Verify that a set of queries returns identical results on both engines.
pub fn verify_tables_match(
    limbo_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    queries: &[(&str, &str)],
    context: &str,
) {
    for (label, query) in queries {
        let limbo_rows = limbo_exec_rows(limbo_conn, query);
        let sqlite_rows = sqlite_exec_rows(sqlite_conn, query);
        similar_asserts::assert_eq!(
            Turso: limbo_rows,
            Sqlite: sqlite_rows,
            "{label} mismatch!\n{context}\nTurso ({} rows)\nSQLite ({} rows)",
            limbo_rows.len(),
            sqlite_rows.len(),
        );
    }
}
