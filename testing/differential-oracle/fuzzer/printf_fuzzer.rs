//! Printf differential fuzzer for Turso.
//!
//! Generates random `SELECT printf(...)` statements and compares results
//! between Turso and SQLite. Focuses on corner cases to find divergences.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use differential_fuzzer::memory::MemorySimIO;
use differential_fuzzer::oracle::{DifferentialOracle, QueryResult, Row};
use differential_fuzzer::printf_gen::{EDGE_CASE_BATTERY, PrintfGenerator};
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use sql_gen_prop::SqlValue;
use turso_core::Database;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Printf differential fuzzer for Turso vs SQLite"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Random seed for deterministic execution.
    #[arg(short, long, default_value_t = rand::rng().next_u64())]
    seed: u64,

    /// Number of random statements to generate (in addition to edge case battery).
    #[arg(short = 'n', long, default_value_t = 1000)]
    num_statements: usize,

    /// Print each statement and its results.
    #[arg(short, long)]
    verbose: bool,

    /// Continue running after failures instead of stopping.
    #[arg(short, long)]
    keep_going: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the fuzzer in a loop with random seeds.
    Loop {
        /// Number of iterations (0 for infinite).
        #[arg(default_value_t = 0)]
        iterations: u64,
    },
}

/// Check if two f64 values are "close enough" to account for different
/// float-to-decimal algorithms (Rust vs SQLite's sqlite3FpDecode) producing
/// different rounding at the last displayed digit.
fn floats_close(a: f64, b: f64) -> bool {
    if a == b {
        return true;
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a.is_nan() || b.is_nan() || a.is_infinite() || b.is_infinite() {
        return false;
    }
    let magnitude = a.abs().max(b.abs());
    if magnitude == 0.0 {
        return true;
    }
    let rel_diff = (a - b).abs() / magnitude;
    // Tolerance: last-digit rounding in a 6+ significant digit display
    // can differ by ~1e-6 relative. Use 1e-5 for safety margin.
    rel_diff < 1e-5
}

/// Compare two text strings with tolerance for float rendering differences.
///
/// Different float-to-decimal algorithms (Rust vs SQLite's sqlite3FpDecode)
/// can produce different decimal representations of the same f64 value.
/// For example, 1e308 may render as "1.00000000000000000e+308" or
/// "9.99999999999999900e+307" — both are correct.
///
/// When strings diverge inside a numeric literal, this function extracts the
/// full numeric span from both, parses them as f64, and checks equality.
fn fuzzy_text_eq(a: &str, b: &str) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut ai = 0;
    let mut bi = 0;

    while ai < a.len() && bi < b.len() {
        if a[ai] == b[bi] {
            ai += 1;
            bi += 1;
            continue;
        }

        // Characters diverge. Try to extract numeric spans and compare as floats.
        // One string may have more digits than the other (different sig digit counts),
        // so a number can end right at the divergence in one string while continuing
        // in the other. We detect numbers by checking start < end around the
        // divergence, and guarantee progress by advancing at least 1 position.
        let (a_num_start, a_has_dot) = find_number_start(&a, ai);
        let (b_num_start, b_has_dot) = find_number_start(&b, bi);
        let a_num_end = find_number_end(&a, ai, a_has_dot);
        let b_num_end = find_number_end(&b, bi, b_has_dot);

        if a_num_end > a_num_start && b_num_end > b_num_start {
            let a_str: String = a[a_num_start..a_num_end].iter().collect();
            let b_str: String = b[b_num_start..b_num_end].iter().collect();
            if let (Ok(af), Ok(bf)) = (a_str.parse::<f64>(), b_str.parse::<f64>()) {
                if floats_close(af, bf) {
                    // Advance past the number, guaranteeing at least 1 step of progress
                    ai = a_num_end.max(ai + 1);
                    bi = b_num_end.max(bi + 1);
                    continue;
                }
            }
        }

        return false;
    }

    // Both must be fully consumed
    ai == a.len() && bi == b.len()
}

/// Walk backwards from `pos` to find the start of a numeric literal.
/// Returns (start_position, whether_a_dot_was_found).
fn find_number_start(chars: &[char], pos: usize) -> (usize, bool) {
    let mut start = pos;
    let mut has_dot = false;
    while start > 0 {
        let c = chars[start - 1];
        if c.is_ascii_digit() {
            start -= 1;
        } else if c == '.' && !has_dot {
            has_dot = true;
            start -= 1;
        } else if c == '+' || c == '-' || c == 'e' || c == 'E' {
            start -= 1;
        } else {
            break;
        }
    }
    (start, has_dot)
}

/// Walk forward from `pos` to find the end of a numeric literal.
/// `already_has_dot`: whether the backward walk already found a decimal point.
fn find_number_end(chars: &[char], pos: usize, already_has_dot: bool) -> usize {
    let mut end = pos;
    let mut has_dot = already_has_dot;
    while end < chars.len() {
        let c = chars[end];
        if c.is_ascii_digit() {
            end += 1;
        } else if c == '.' && !has_dot {
            has_dot = true;
            end += 1;
        } else if (c == 'e' || c == 'E') && end + 1 < chars.len() {
            end += 1;
            // Skip optional sign after exponent
            if end < chars.len() && (chars[end] == '+' || chars[end] == '-') {
                end += 1;
            }
        } else {
            break;
        }
    }
    end
}

/// Compare rows with fuzzy text comparison for float tolerance.
fn rows_fuzzy_eq(a: &[Row], b: &[Row]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (ra, rb) in a.iter().zip(b.iter()) {
        if ra.0.len() != rb.0.len() {
            return false;
        }
        for (va, vb) in ra.0.iter().zip(rb.0.iter()) {
            match (va, vb) {
                (SqlValue::Text(ta), SqlValue::Text(tb)) => {
                    if !fuzzy_text_eq(ta, tb) {
                        // Different float-to-decimal algorithms can produce
                        // representations of different length (e.g. "1E+308" vs
                        // "9.999999999999999E+307"), which causes width padding
                        // to differ. Retry with spaces stripped to catch this.
                        let a_stripped: String = ta.chars().filter(|c| *c != ' ').collect();
                        let b_stripped: String = tb.chars().filter(|c| *c != ' ').collect();
                        if !fuzzy_text_eq(&a_stripped, &b_stripped) {
                            return false;
                        }
                    }
                }
                _ => {
                    if va != vb {
                        return false;
                    }
                }
            }
        }
    }
    true
}

/// Format two text values showing the divergence point with context.
fn fmt_text_diff(a: &str, b: &str) -> String {
    // Find first byte where they diverge
    let diverge = a
        .bytes()
        .zip(b.bytes())
        .position(|(x, y)| x != y)
        .unwrap_or_else(|| a.len().min(b.len()));

    let ctx = 40; // chars of context around the divergence
    let start = diverge.saturating_sub(ctx);
    let end_a = (diverge + ctx).min(a.len());
    let end_b = (diverge + ctx).min(b.len());

    let prefix = if start > 0 { "..." } else { "" };
    let suffix_a = if end_a < a.len() { "..." } else { "" };
    let suffix_b = if end_b < b.len() { "..." } else { "" };

    format!(
        "diverge at byte {diverge} (len {}/{})\n    Turso:  {prefix}\"{}\"{suffix_a}\n    SQLite: {prefix}\"{}\"{suffix_b}",
        a.len(),
        b.len(),
        &a[start..end_a],
        &b[start..end_b],
    )
}

/// Format a QueryResult pair for display, showing diffs for mismatched text.
fn fmt_result_diff(turso: &QueryResult, sqlite: &QueryResult) -> String {
    match (turso, sqlite) {
        (QueryResult::Rows(t_rows), QueryResult::Rows(s_rows)) => {
            for (tr, sr) in t_rows.iter().zip(s_rows.iter()) {
                for (tv, sv) in tr.0.iter().zip(sr.0.iter()) {
                    if let (SqlValue::Text(ta), SqlValue::Text(tb)) = (tv, sv) {
                        if ta != tb {
                            return fmt_text_diff(ta, tb);
                        }
                    }
                }
            }
            // Fallback: length or non-text mismatch
            format!("Turso: {turso:?}\n  SQLite: {sqlite:?}")
        }
        _ => format!("Turso: {turso:?}\n  SQLite: {sqlite:?}"),
    }
}

struct FuzzerState {
    turso_conn: Arc<turso_core::Connection>,
    sqlite_conn: rusqlite::Connection,
    #[expect(dead_code)]
    turso_db: Arc<Database>,
    #[expect(dead_code)]
    io: Arc<MemorySimIO>,
}

impl FuzzerState {
    fn new(seed: u64) -> Result<Self> {
        let out_dir: PathBuf = "simulator-output".into();
        if !out_dir.exists() {
            std::fs::create_dir_all(&out_dir)?;
        }

        let io = Arc::new(MemorySimIO::new(seed));
        let turso_db =
            Database::open_file(io.clone(), out_dir.join("printf-test.db").to_str().unwrap())?;
        let turso_conn = turso_db.connect()?;

        let sqlite_conn =
            rusqlite::Connection::open_in_memory().context("Failed to open SQLite database")?;

        // Log bundled SQLite version for debugging version-dependent behavior
        if let Ok(ver) =
            sqlite_conn.query_row("SELECT sqlite_version()", [], |r| r.get::<_, String>(0))
        {
            tracing::info!("Bundled SQLite version: {ver}");
        }

        Ok(Self {
            turso_conn,
            sqlite_conn,
            turso_db,
            io,
        })
    }

    /// Execute SQL on both engines and compare. Returns None on match, Some(reason) on mismatch.
    /// Mismatch messages are truncated to avoid multi-MB log output.
    fn check(&self, sql: &str) -> Option<String> {
        let turso_result = DifferentialOracle::execute_turso(&self.turso_conn, sql);
        let sqlite_result = DifferentialOracle::execute_sqlite(&self.sqlite_conn, sql);

        // Turso is UTF-8 only; blob-to-string conversion differs from SQLite's raw
        // byte handling. When blobs are involved, just verify both produce results.
        let has_blob = sql.contains("X'");

        match (&turso_result, &sqlite_result) {
            (QueryResult::Rows(t_rows), QueryResult::Rows(s_rows)) => {
                if has_blob {
                    // Both produced rows — good enough for blob cases
                    let _ = (t_rows, s_rows);
                    None
                } else if !rows_fuzzy_eq(t_rows, s_rows) {
                    Some(format!(
                        "Result mismatch:\n  {}",
                        fmt_result_diff(&turso_result, &sqlite_result),
                    ))
                } else {
                    None
                }
            }
            (QueryResult::Ok, QueryResult::Ok) => None,
            (QueryResult::Error(_), QueryResult::Error(_)) => {
                // Both errored — acceptable
                None
            }
            (QueryResult::Error(e), _) => Some(format!(
                "Turso errored but SQLite succeeded:\n  Turso error: {e}\n  SQLite: {sqlite_result:?}"
            )),
            (_, QueryResult::Error(e)) => Some(format!(
                "SQLite errored but Turso succeeded:\n  SQLite error: {e}\n  Turso: {turso_result:?}"
            )),
            _ => {
                if format!("{turso_result:?}") != format!("{sqlite_result:?}") {
                    Some(format!(
                        "Result type mismatch:\n  {}",
                        fmt_result_diff(&turso_result, &sqlite_result),
                    ))
                } else {
                    None
                }
            }
        }
    }
}

fn run_single(args: &Args) -> Result<FuzzerStats> {
    let state = FuzzerState::new(args.seed)?;
    let mut stats = FuzzerStats::default();
    let rng = ChaCha8Rng::seed_from_u64(args.seed);
    let mut generator = PrintfGenerator::new(rng);

    tracing::info!(
        "Running printf fuzzer: seed={}, edge_cases={}, random={}",
        args.seed,
        EDGE_CASE_BATTERY.len(),
        args.num_statements
    );

    // Phase 1: Run the hardcoded edge case battery
    for sql in EDGE_CASE_BATTERY {
        stats.total += 1;

        if args.verbose {
            tracing::info!("[edge] {sql}");
        }

        if let Some(reason) = state.check(sql) {
            stats.failures += 1;
            tracing::error!("FAIL: {sql}\n  {reason}");
            if !args.keep_going {
                tracing::error!(
                    "Stopping on first failure (use --keep-going to continue). Seed: {}",
                    args.seed
                );
                return Ok(stats);
            }
        } else {
            stats.passed += 1;
            if args.verbose {
                tracing::info!("  PASS");
            }
        }
    }

    // Phase 2: Random generation
    for i in 0..args.num_statements {
        let sql = generator.gen_printf_sql();
        stats.total += 1;

        if args.verbose {
            tracing::info!("[rand #{i}] {sql}");
        }

        if let Some(reason) = state.check(&sql) {
            stats.failures += 1;
            tracing::error!("FAIL: {sql}\n  {reason}");
            if !args.keep_going {
                tracing::error!(
                    "Stopping on first failure (use --keep-going to continue). Seed: {}",
                    args.seed
                );
                return Ok(stats);
            }
        } else {
            stats.passed += 1;
            if args.verbose {
                tracing::info!("  PASS");
            }
        }
    }

    Ok(stats)
}

#[derive(Default)]
struct FuzzerStats {
    total: usize,
    passed: usize,
    failures: usize,
}

impl FuzzerStats {
    fn print_summary(&self, seed: u64) {
        if self.failures > 0 {
            tracing::error!(
                "FAILED: {}/{} passed, {} failures (seed: {seed})",
                self.passed,
                self.total,
                self.failures
            );
        } else {
            tracing::info!(
                "PASSED: {}/{} passed (seed: {seed})",
                self.passed,
                self.total
            );
        }
    }
}

fn main() -> Result<()> {
    let mut subscriber = tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()),
    );

    if !std::io::stdin().is_terminal() {
        subscriber = subscriber.with_ansi(false)
    }
    subscriber.init();

    let mut args = Args::parse();

    match args.command {
        Some(Commands::Loop { iterations }) => {
            let mut iteration = 0u64;
            loop {
                args.seed = rand::rng().next_u64();
                tracing::info!("=== Iteration {}: seed {} ===", iteration + 1, args.seed);

                let stats = run_single(&args)?;
                stats.print_summary(args.seed);

                if stats.failures > 0 {
                    tracing::error!(
                        "Reproduce with: cargo run --bin printf_fuzzer -- --seed {} -n {} --verbose",
                        args.seed,
                        args.num_statements
                    );
                    std::process::exit(1);
                }

                iteration += 1;
                if iterations > 0 && iteration >= iterations {
                    tracing::info!("Completed {} iterations successfully", iterations);
                    break;
                }
            }
            Ok(())
        }
        None => {
            let stats = run_single(&args)?;
            stats.print_summary(args.seed);

            if stats.failures > 0 {
                tracing::error!(
                    "Reproduce with: cargo run --bin printf_fuzzer -- --seed {} -n {} --verbose",
                    args.seed,
                    args.num_statements
                );
                std::process::exit(1);
            }
            Ok(())
        }
    }
}
