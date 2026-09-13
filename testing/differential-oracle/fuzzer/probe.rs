//! Run a SQL script on Turso and SQLite side by side and show where they
//! disagree.
//!
//! This is the follow-up tool to a fuzzer failure: `minimized.sql` (or any
//! statement-per-line script) goes in, and every statement's outcome on both
//! engines comes out, with divergences marked. Unlike the tursodb shell, the
//! Turso connection here has ATTACH enabled and an `aux` in-memory database
//! attached when the script asks for one, so fuzzer reproductions run
//! unmodified.
//!
//! Usage:
//!
//!     cargo run -q -p differential-fuzzer --bin differential_probe -- \
//!         simulator-output/minimized.sql
//!
//! With no argument the script is read from stdin. Statements are one per
//! line; lines starting with `--` are skipped. The exit code is 1 when any
//! statement diverged or the final table contents differ, 0 otherwise.

use std::io::Read;

use anyhow::Result;
use differential_fuzzer::oracle::QueryResult;
use differential_fuzzer::shrink::{EnginePair, query_results_differ};

fn brief(result: &QueryResult) -> String {
    match result {
        QueryResult::Ok => "ok (no rows)".to_string(),
        QueryResult::Error(e) => format!("error: {}", e.lines().next().unwrap_or("")),
        QueryResult::Rows(rows) => {
            let mut out = format!("{} row(s): ", rows.len());
            for (i, row) in rows.iter().take(4).enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&format!("{:?}", row.0));
            }
            if rows.len() > 4 {
                out.push_str(", ...");
            }
            if out.len() > 300 {
                out.truncate(300);
                out.push_str("...");
            }
            out
        }
    }
}

fn main() -> Result<()> {
    let script = match std::env::args().nth(1) {
        Some(path) => std::fs::read_to_string(&path)?,
        None => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };

    // An empty state: the script itself builds everything.
    let pair = EnginePair::build("")?;
    let mut divergences = 0usize;

    for (lineno, line) in script.lines().enumerate() {
        let stmt = line.trim();
        if stmt.is_empty() || stmt.starts_with("--") {
            continue;
        }
        let (turso, sqlite) = pair.run_both(stmt);
        let statement_diverges = match (&turso, &sqlite) {
            (QueryResult::Error(_), QueryResult::Error(_)) => false,
            (QueryResult::Error(_), _) | (_, QueryResult::Error(_)) => true,
            _ => query_results_differ(&turso, &sqlite),
        };
        let show = if stmt.len() > 160 {
            format!("{}...", &stmt[..160])
        } else {
            stmt.to_string()
        };
        println!("line {:>3}: {show}", lineno + 1);
        println!("  turso : {}", brief(&turso));
        println!("  sqlite: {}", brief(&sqlite));
        if statement_diverges {
            divergences += 1;
            println!("  ^^^ DIVERGES");
        }
    }

    if pair.states_differ() {
        divergences += 1;
        println!("final table contents DIVERGE between the engines");
    } else {
        println!("final table contents match");
    }

    if divergences > 0 {
        println!("{divergences} divergence(s)");
        std::process::exit(1);
    }
    Ok(())
}
