//! MVCC Concurrent Transaction REPL
//!
//! Utility for interactive testing of concurrent MVCC transactions.
//!
//! # Overview
//!
//! This provides an interactive REPL where you can drive multiple in-process database
//! connections and test concurrent transaction behavior, conflict detection, and isolation
//! semantics. Connections are created lazily, when they are first used.
//!
//! # Invocation
//!
//! ```bash
//! cargo run --bin tursodb --features mvcc_repl -- --mvcc [path]
//! ```
//!
//! # Usage Examples
//!
//! Once started, you'll see a prompt where you can run SQL on specific connections:
//!
//! ```text
//! mvcc> conn1 CREATE TABLE t(x INT)
//! [conn1] OK
//!
//! mvcc> conn1 BEGIN CONCURRENT
//! [conn1] OK
//!
//! mvcc> conn2 BEGIN CONCURRENT
//! [conn2] OK
//!
//! mvcc> conn1 INSERT INTO t VALUES (42)
//! [conn1] OK
//!
//! mvcc> conn2 INSERT INTO t VALUES (42)
//! [conn2] ERROR: write-write conflict (transaction rolled back)
//! ```
use anyhow::Context as _;
use reedline::{DefaultPrompt, DefaultPromptSegment, Reedline, Signal};
use std::{collections::HashMap, sync::Arc};
use turso_core::{Connection, Database, DatabaseOpts, LimboError, OpenFlags, Value};

pub fn run(path: &str) -> anyhow::Result<()> {
    let interactive_stdin = std::io::IsTerminal::is_terminal(&std::io::stdin());
    if interactive_stdin {
        println!("MVCC REPL");
        println!("Type `connN SQL` to run SQL on connection N (e.g. `conn1 BEGIN CONCURRENT;`)");
        println!("Connections are auto-created on first use");
        println!();
    }

    let db = open_mvcc_db(path)?;
    let mut connections: HashMap<String, Arc<Connection>> = HashMap::new();
    let mut rl = Reedline::create();
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("mvcc> ".to_string()),
        DefaultPromptSegment::Empty,
    );

    loop {
        match rl.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line == ".quit" {
                    break;
                }

                if let Some((conn_name, sql)) = parse_conn_input(line) {
                    let conn = connections
                        .entry(conn_name.clone())
                        .or_insert_with(|| db.connect().expect("failed to create connection"));

                    match execute_and_display(conn, sql, &conn_name) {
                        Ok(()) => {}
                        Err(e) => {
                            eprintln!("[{conn_name}] ERROR: {e}");
                        }
                    }
                } else {
                    eprintln!("Error: format is `connN SQL` (e.g. `conn1 SELECT * FROM t`)");
                }
            }
            Ok(Signal::CtrlD) => break,
            Ok(Signal::CtrlC) => {
                println!();
                continue;
            }
            Err(e) => anyhow::bail!(e),
        }
    }

    Ok(())
}

fn open_mvcc_db(path: &str) -> anyhow::Result<Arc<Database>> {
    let (_, db) = Database::open_new::<&str>(
        path,
        None,
        OpenFlags::default(),
        DatabaseOpts::default(),
        None,
    )
    .context("failed to open database")?;

    let boot = db.connect()?;
    boot.execute("PRAGMA journal_mode = 'mvcc'")?;
    boot.close()?;

    Ok(db)
}

fn parse_conn_input(line: &str) -> Option<(String, &str)> {
    let (first, rest) = line.split_once(char::is_whitespace)?;

    if !first.starts_with("conn") {
        return None;
    }

    let num_part = &first[4..];
    num_part.parse::<u32>().ok()?;

    Some((first.to_string(), rest.trim()))
}

fn execute_and_display(conn: &Arc<Connection>, sql: &str, conn_name: &str) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

    match stmt.run_collect_rows() {
        Ok(rows) => {
            if rows.is_empty() {
                println!("[{conn_name}] OK");
            } else {
                for row in rows {
                    let formatted: Vec<String> = row.iter().map(fmt_value).collect();
                    println!("[{conn_name}] {}", formatted.join(" | "));
                }
            }
            Ok(())
        }
        Err(LimboError::WriteWriteConflict) => Err(anyhow::anyhow!(
            "write-write conflict (transaction rolled back)"
        )),
        Err(e) => Err(anyhow::anyhow!("{e}")),
    }
}

fn fmt_value(v: &Value) -> String {
    use turso_core::Numeric;
    match v {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(i)) => i.to_string(),
        Value::Numeric(Numeric::Float(f)) => {
            let fval: f64 = (*f).into();
            // Format floats without trailing zeros for cleaner display
            if fval.fract() == 0.0 && fval.abs() < 1e10 {
                format!("{fval:.1}")
            } else {
                format!("{fval}")
            }
        }
        Value::Text(s) => s.to_string(),
        Value::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}
