// Copyright 2025 the Limbo authors. All rights reserved. MIT license.

//! Crash recovery test harness for Turso.
//!
//! Repeatedly runs `turso_stress` against a database, kills it with SIGKILL
//! after a random duration, then verifies the database is recoverable and
//! structurally sound. This validates WAL crash safety.

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::{Rng, SeedableRng};
use turso_core::{Database, PlatformIO, StepResult, IO};
use turso_dbhash::{hash_database, DbHashOptions};

#[derive(Parser)]
#[command(name = "turso_crash_test")]
#[command(about = "Crash recovery test harness — stress + SIGKILL + verify")]
struct Opts {
    /// Number of crash cycles to run.
    #[clap(long, default_value_t = 10)]
    iterations: u32,

    /// Maximum seconds before SIGKILL.
    #[clap(long, default_value_t = 5.0)]
    max_stress_duration: f64,

    /// Minimum seconds before SIGKILL.
    #[clap(long, default_value_t = 1.0)]
    min_stress_duration: f64,

    /// Number of threads for turso_stress.
    #[clap(long, default_value_t = 2)]
    threads: usize,

    /// Iterations per turso_stress run.
    #[clap(long, default_value_t = 100_000)]
    stress_iterations: usize,

    /// Random seed for reproducibility.
    #[clap(long)]
    seed: Option<u64>,

    /// Database file path (default: tempfile).
    #[clap(long)]
    db_file: Option<PathBuf>,

    /// Don't delete database on success.
    #[clap(long, default_value_t = false)]
    keep_db: bool,
}

fn main() {
    let opts = Opts::parse();

    assert!(
        opts.min_stress_duration > 0.0,
        "min-stress-duration must be positive"
    );
    assert!(
        opts.max_stress_duration >= opts.min_stress_duration,
        "max-stress-duration must be >= min-stress-duration"
    );

    // Resolve the stress binary path — same target directory as ourselves.
    let stress_binary = find_stress_binary();

    // Set up the database path.
    let tmp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = match &opts.db_file {
        Some(p) => p.clone(),
        None => tmp_dir.path().join("crash_test.db"),
    };

    let seed = opts.seed.unwrap_or_else(rand::random);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    println!("turso_crash_test");
    println!("  seed:       {seed}");
    println!("  db:         {}", db_path.display());
    println!("  iterations: {}", opts.iterations);
    println!(
        "  kill range: {:.1}s – {:.1}s",
        opts.min_stress_duration, opts.max_stress_duration
    );
    println!("  threads:    {}", opts.threads);
    println!("  stress_its: {}", opts.stress_iterations);
    println!();

    let mut crashed_cycles = 0u32;
    let mut skipped_cycles = 0u32;

    for cycle in 1..=opts.iterations {
        let wait_secs = rng.random_range(opts.min_stress_duration..opts.max_stress_duration);

        print!("[{cycle}/{}] ", opts.iterations);
        println!("spawning turso_stress (kill after {wait_secs:.2}s) ...",);

        // --- spawn turso_stress ---
        let mut child = Command::new(&stress_binary)
            .args([
                "--db-file",
                db_path.to_str().expect("non-UTF-8 path"),
                "--nr-threads",
                &opts.threads.to_string(),
                "--nr-iterations",
                &opts.stress_iterations.to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "failed to spawn turso_stress at {}: {e}",
                    stress_binary.display()
                )
            });

        // --- wait, then SIGKILL ---
        let wait_dur = Duration::from_secs_f64(wait_secs);
        std::thread::sleep(wait_dur);

        let pid = Pid::from_raw(child.id() as i32);
        let kill_result = signal::kill(pid, Signal::SIGKILL);
        let status = child.wait().expect("failed to wait on turso_stress");

        // Verify that we actually killed the process mid-flight.
        // If it exited on its own before SIGKILL, we didn't test crash recovery.
        let was_killed = match kill_result {
            Ok(()) => {
                // SIGKILL was delivered. Confirm the exit status shows a signal death.
                status.signal() == Some(nix::libc::SIGKILL)
            }
            Err(nix::errno::Errno::ESRCH) => {
                // Process already exited before we could kill it.
                false
            }
            Err(e) => panic!("unexpected error sending SIGKILL: {e}"),
        };

        if !was_killed {
            let exit_desc = if let Some(code) = status.code() {
                format!("exited with code {code}")
            } else if let Some(sig) = status.signal() {
                format!("killed by signal {sig}")
            } else {
                "unknown exit".to_string()
            };
            println!("  skipped — turso_stress {exit_desc} before SIGKILL (not a crash)");
            skipped_cycles += 1;
            continue;
        }

        println!("  killed (SIGKILL delivered), verifying recovery ...");

        // --- verify recovery ---
        if let Err(e) = verify_recovery(&db_path) {
            eprintln!("FAIL on cycle {cycle}: {e}");
            eprintln!("  db preserved at: {}", db_path.display());
            std::process::exit(1);
        }
        crashed_cycles += 1;
        println!("  ok");
    }

    println!();
    if crashed_cycles == 0 {
        eprintln!(
            "FAIL: all {skipped_cycles} cycles were skipped — turso_stress always exited \
             before SIGKILL. Increase --max-stress-duration or --stress-iterations."
        );
        std::process::exit(1);
    }
    println!(
        "Passed: {crashed_cycles} crash cycles verified, {skipped_cycles} skipped (seed={seed})"
    );

    if !opts.keep_db {
        // tmp_dir drop cleans up if we used a tempfile; otherwise leave it.
        if opts.db_file.is_none() {
            // tmp_dir's Drop will handle cleanup
        }
    } else {
        // Persist the temp dir so the user can inspect it.
        let persisted = tmp_dir.keep();
        println!("database kept at: {}", db_path.display());
        println!("temp dir kept at: {}", persisted.display());
    }
}

/// Locate the `turso_stress` binary next to our own binary.
fn find_stress_binary() -> PathBuf {
    let self_path = std::env::current_exe().expect("cannot determine own exe path");
    let dir = self_path.parent().expect("exe has no parent dir");
    let candidate = dir.join("turso_stress");
    if candidate.exists() {
        return candidate;
    }
    // Fallback: maybe it's on PATH
    which_in_path("turso_stress").unwrap_or_else(|| {
        panic!(
            "turso_stress binary not found in {} or PATH. Build it first: cargo build -p turso_stress",
            dir.display()
        )
    })
}

fn which_in_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(name))
            .find(|p| p.exists())
    })
}

// ---------------------------------------------------------------------------
// Recovery verification — all 6 invariant checks
// ---------------------------------------------------------------------------

fn verify_recovery(db_path: &Path) -> Result<(), String> {
    let path_str = db_path.to_str().ok_or("non-UTF-8 db path")?;

    // 1. Database opens (WAL recovery completes)
    let io: Arc<dyn IO> =
        Arc::new(PlatformIO::new().map_err(|e| format!("PlatformIO::new failed: {e}"))?);
    let db = Database::open_file(io.clone(), path_str)
        .map_err(|e| format!("check 1 — Database::open_file failed: {e}"))?;
    let conn = db
        .connect()
        .map_err(|e| format!("check 1 — connect failed: {e}"))?;

    // 2. PRAGMA integrity_check = "ok"
    run_integrity_check(&conn, &io, "check 2 — turso integrity_check")?;

    // 3. SELECT count(*) FROM <table> for every table; assert data exists
    let total_rows = check_table_counts(&conn, &io)?;
    println!("    {total_rows} rows across all tables");

    // 4. PRAGMA wal_checkpoint(TRUNCATE) succeeds
    run_pragma(
        &conn,
        &io,
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "check 4 — checkpoint",
    )?;

    drop(conn);
    drop(db);

    // 5. dbhash stability — checkpoint again from scratch and compare hashes
    //    The first checkpoint (check 4) flushed WAL to the db file.
    //    Hash the result, open + checkpoint again, hash again.
    //    If checkpoint is idempotent, the hashes must match.
    check_dbhash_stability(path_str)?;

    // 6. SQLite cross-check via rusqlite
    check_rusqlite(db_path)?;

    Ok(())
}

/// Run `PRAGMA integrity_check` and assert the result is "ok".
fn run_integrity_check(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    label: &str,
) -> Result<(), String> {
    let mut stmt = conn
        .prepare("PRAGMA integrity_check")
        .map_err(|e| format!("{label}: prepare failed: {e}"))?;

    let mut result_text = String::new();
    loop {
        match stmt
            .step()
            .map_err(|e| format!("{label}: step failed: {e}"))?
        {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let val = row.get_value(0);
                result_text = val.to_string();
            }
            StepResult::IO => {
                io.step()
                    .map_err(|e| format!("{label}: io.step failed: {e}"))?;
            }
            StepResult::Done => break,
            StepResult::Busy => return Err(format!("{label}: database busy")),
            StepResult::Interrupt => return Err(format!("{label}: interrupted")),
        }
    }

    if result_text != "ok" {
        return Err(format!("{label}: expected 'ok', got '{result_text}'"));
    }
    Ok(())
}

/// Execute a pragma and consume all rows (we don't care about the result values,
/// only that it doesn't error).
fn run_pragma(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    sql: &str,
    label: &str,
) -> Result<(), String> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("{label}: prepare failed: {e}"))?;

    loop {
        match stmt
            .step()
            .map_err(|e| format!("{label}: step failed: {e}"))?
        {
            StepResult::Row => {}
            StepResult::IO => {
                io.step()
                    .map_err(|e| format!("{label}: io.step failed: {e}"))?;
            }
            StepResult::Done => break,
            StepResult::Busy => return Err(format!("{label}: database busy")),
            StepResult::Interrupt => return Err(format!("{label}: interrupted")),
        }
    }
    Ok(())
}

/// Check 3: `SELECT count(*) FROM <table>` for every user table.
/// Returns the total row count across all tables.
fn check_table_counts(conn: &Arc<turso_core::Connection>, io: &Arc<dyn IO>) -> Result<u64, String> {
    let tables = get_table_names(conn, io)?;
    if tables.is_empty() {
        return Err("check 3 — no user tables found (turso_stress wrote no data)".to_string());
    }

    let mut total_rows: u64 = 0;
    for table in &tables {
        let sql = format!("SELECT count(*) FROM \"{}\"", table.replace('"', "\"\""));
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("check 3 — prepare count(*) for '{table}': {e}"))?;
        let mut count: u64 = 0;
        loop {
            match stmt
                .step()
                .map_err(|e| format!("check 3 — count(*) step for '{table}': {e}"))?
            {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    let val = row.get_value(0);
                    count = val.to_string().parse::<u64>().unwrap_or(0);
                }
                StepResult::IO => {
                    io.step()
                        .map_err(|e| format!("check 3 — io.step for '{table}': {e}"))?;
                }
                StepResult::Done => break,
                StepResult::Busy => {
                    return Err(format!("check 3 — count(*) busy for '{table}'"));
                }
                StepResult::Interrupt => {
                    return Err(format!("check 3 — count(*) interrupted for '{table}'"));
                }
            }
        }
        total_rows += count;
    }

    if total_rows == 0 {
        return Err(format!(
            "check 3 — all {} tables are empty (turso_stress wrote no data)",
            tables.len()
        ));
    }
    Ok(total_rows)
}

/// Get user table names from sqlite_schema.
fn get_table_names(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
) -> Result<Vec<String>, String> {
    let sql = r#"SELECT name FROM sqlite_schema
                 WHERE type = 'table'
                   AND name NOT LIKE 'sqlite_%'
                 ORDER BY name"#;
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("get_table_names: prepare failed: {e}"))?;

    let mut names = Vec::new();
    loop {
        match stmt
            .step()
            .map_err(|e| format!("get_table_names: step failed: {e}"))?
        {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_string();
                names.push(name);
            }
            StepResult::IO => {
                io.step()
                    .map_err(|e| format!("get_table_names: io.step failed: {e}"))?;
            }
            StepResult::Done => break,
            StepResult::Busy => return Err("get_table_names: database busy".to_string()),
            StepResult::Interrupt => {
                return Err("get_table_names: interrupted".to_string());
            }
        }
    }
    Ok(names)
}

/// Check 5: dbhash stability — verify that re-opening and re-checkpointing
/// produces the same database file content (checkpoint idempotency).
///
/// After check 4 already checkpointed once, we:
///   hash1 = dbhash of the checkpointed file
///   open → checkpoint(TRUNCATE) → close
///   hash2 = dbhash again
///
/// hash1 == hash2 proves checkpoint is idempotent.
fn check_dbhash_stability(path: &str) -> Result<(), String> {
    let hash_opts = DbHashOptions::default();

    let hash1 = hash_database(path, &hash_opts)
        .map_err(|e| format!("check 5 — first dbhash failed: {e}"))?;

    // Re-open the database and checkpoint again (should be a no-op).
    {
        let io: Arc<dyn IO> =
            Arc::new(PlatformIO::new().map_err(|e| format!("check 5 — PlatformIO: {e}"))?);
        let db = Database::open_file(io.clone(), path)
            .map_err(|e| format!("check 5 — open for re-checkpoint: {e}"))?;
        let conn = db
            .connect()
            .map_err(|e| format!("check 5 — connect for re-checkpoint: {e}"))?;
        run_pragma(
            &conn,
            &io,
            "PRAGMA wal_checkpoint(TRUNCATE)",
            "check 5 — re-checkpoint",
        )?;
    }

    let hash2 = hash_database(path, &hash_opts)
        .map_err(|e| format!("check 5 — second dbhash failed: {e}"))?;

    if hash1.hash != hash2.hash {
        return Err(format!(
            "check 5 — dbhash mismatch after re-checkpoint: {} != {} \
             (checkpoint is not idempotent)",
            hash1.hash, hash2.hash
        ));
    }
    Ok(())
}

/// Check 6: rusqlite cross-check — open with SQLite and run integrity_check.
fn check_rusqlite(db_path: &Path) -> Result<(), String> {
    let conn = rusqlite::Connection::open(db_path)
        .map_err(|e| format!("check 6 — rusqlite open failed: {e}"))?;

    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| format!("check 6 — rusqlite integrity_check failed: {e}"))?;

    if result != "ok" {
        return Err(format!(
            "check 6 — rusqlite integrity_check: expected 'ok', got '{result}'"
        ));
    }
    Ok(())
}
