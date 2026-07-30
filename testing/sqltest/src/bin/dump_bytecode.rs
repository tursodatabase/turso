//! Scratch analysis tool: dump VDBE bytecode for every statement in .sqltest corpora.
//!
//! For each test case we open a fresh database (same wiring as the rust sqltest
//! backend), run its setups, then for every statement in the test body we first
//! capture `EXPLAIN <stmt>` output and then execute the statement so later
//! statements see the right schema/data. Output is JSONL, one record per
//! explained statement:
//!
//! {"file": "...", "test": "...", "db": ":memory:", "sql": "...",
//!  "insns": [[addr, "Opcode", p1, p2, p3, "p4", p5, "comment"], ...]}

use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use clap::Parser as ClapParser;
use serde_json::json;
use sqltest::backends::rust::RustBackend;
use sqltest::{
    DatabaseConfig, DatabaseLocation, DefaultDatabaseResolver, DefaultDatabases, SqlBackend,
    load_test_files,
};
use tokio::sync::Semaphore;

#[derive(ClapParser)]
struct Args {
    /// .sqltest files or directories to scan
    paths: Vec<PathBuf>,
    /// Output JSONL file
    #[arg(long, default_value = "bytecode_corpus.jsonl")]
    out: PathBuf,
    /// Concurrent test cases
    #[arg(long, default_value_t = 8)]
    jobs: usize,
    /// Per-test-case timeout in seconds
    #[arg(long, default_value_t = 60)]
    case_timeout_secs: u64,
}

struct DbResolver(Option<PathBuf>, Option<PathBuf>);

impl DefaultDatabaseResolver for DbResolver {
    fn resolve(&self, location: &DatabaseLocation) -> Option<PathBuf> {
        match location {
            DatabaseLocation::Default => self.0.clone(),
            DatabaseLocation::DefaultNoRowidAlias => self.1.clone(),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Stats {
    cases: AtomicUsize,
    cases_timeout: AtomicUsize,
    cases_panic: AtomicUsize,
    cases_db_error: AtomicUsize,
    stmts: AtomicUsize,
    programs: AtomicUsize,
    explain_errors: AtomicUsize,
    exec_errors: AtomicUsize,
    split_errors: AtomicUsize,
}

/// Split multi-statement SQL the same way the rust sqltest backend does.
fn split_statements(sql: &str) -> (Vec<String>, bool) {
    let mut stmts = Vec::new();
    let mut remaining = sql;
    let mut split_error = false;
    while !remaining.trim().is_empty() {
        let mut parser = turso_parser::parser::Parser::new(remaining.as_bytes());
        match parser.next() {
            Some(Ok(_cmd)) => {
                let offset = parser.offset();
                let stmt = remaining[..offset].trim();
                if !stmt.is_empty() {
                    stmts.push(stmt.to_string());
                }
                remaining = &remaining[offset..];
            }
            Some(Err(_)) => {
                split_error = true;
                break;
            }
            None => break,
        }
    }
    (stmts, split_error)
}

struct CaseInput {
    file: String,
    test: String,
    db_config: DatabaseConfig,
    setups: Vec<String>,
    sql: String,
}

async fn run_case(backend: Arc<RustBackend>, case: CaseInput, stats: Arc<Stats>) -> Vec<String> {
    let mut lines = Vec::new();
    let mut db = match backend.create_database(&case.db_config).await {
        Ok(db) => db,
        Err(_) => {
            stats.cases_db_error.fetch_add(1, Ordering::Relaxed);
            return lines;
        }
    };
    for setup in &case.setups {
        let _ = db.execute_setup(setup).await;
    }
    let (stmts, split_error) = split_statements(&case.sql);
    if split_error {
        stats.split_errors.fetch_add(1, Ordering::Relaxed);
    }
    let db_name = case.db_config.location.to_string();
    for stmt in stmts {
        if stmt.starts_with('.') {
            continue;
        }
        stats.stmts.fetch_add(1, Ordering::Relaxed);
        let lower = stmt.to_ascii_lowercase();
        let already_explain = lower.starts_with("explain");
        let explain_sql = if already_explain {
            stmt.clone()
        } else {
            format!("EXPLAIN {stmt}")
        };
        match db.execute(&explain_sql).await {
            Ok(result) if result.error.is_none() => {
                // Keep only bytecode listings (8 columns); EXPLAIN QUERY PLAN has 4.
                let insns: Vec<serde_json::Value> = result
                    .rows
                    .iter()
                    .filter(|row| row.len() == 8)
                    .filter_map(|row| {
                        let addr: i64 = row[0].parse().ok()?;
                        let p1: i64 = row[2].parse().unwrap_or(0);
                        let p2: i64 = row[3].parse().unwrap_or(0);
                        let p3: i64 = row[4].parse().unwrap_or(0);
                        let p5: i64 = row[6].parse().unwrap_or(0);
                        Some(json!([addr, row[1], p1, p2, p3, row[5], p5, row[7]]))
                    })
                    .collect();
                if !insns.is_empty() {
                    stats.programs.fetch_add(1, Ordering::Relaxed);
                    let record = json!({
                        "file": case.file,
                        "test": case.test,
                        "db": db_name,
                        "sql": stmt,
                        "insns": insns,
                    });
                    lines.push(record.to_string());
                }
            }
            _ => {
                stats.explain_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        if !already_explain {
            match db.execute(&stmt).await {
                Ok(result) if result.error.is_none() => {}
                _ => {
                    stats.exec_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    lines
}

fn main() -> anyhow::Result<()> {
    // Deep expression trees recurse heavily in debug builds; the default 2MB
    // worker stack overflows on some corpus statements.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(512 * 1024 * 1024)
        .build()?;
    runtime.block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let loaded = load_test_files(&args.paths)
        .await
        .map_err(|e| anyhow::anyhow!("failed to load test files: {e}"))?;
    eprintln!(
        "loaded {} files ({} parse errors)",
        loaded.files.len(),
        loaded.errors.len()
    );

    let needs = DefaultDatabases::scan_needs(loaded.test_files());
    let default_dbs = if needs.any() {
        eprintln!("generating default databases...");
        DefaultDatabases::generate(needs, 42, 10000, false).await?
    } else {
        None
    };
    let mut backend = RustBackend::new();
    if let Some(dbs) = &default_dbs {
        backend = backend.with_default_db_resolver(Arc::new(DbResolver(
            dbs.default_path.clone(),
            dbs.no_rowid_alias_path.clone(),
        )));
    }
    let backend = Arc::new(backend);

    let mut cases = Vec::new();
    for (path, tf) in &loaded.files {
        let file = path.to_string_lossy().to_string();
        for db_config in &tf.databases {
            let lookup_setups = |refs: &[sqltest::SetupRef]| -> Vec<String> {
                refs.iter()
                    .filter_map(|r| tf.setups.get(&r.name).cloned())
                    .collect()
            };
            for test in &tf.tests {
                cases.push(CaseInput {
                    file: file.clone(),
                    test: test.name.clone(),
                    db_config: db_config.clone(),
                    setups: lookup_setups(&test.modifiers.setups),
                    sql: test.sql.clone(),
                });
            }
            for snap in &tf.snapshots {
                if snap.eqp_only {
                    continue;
                }
                cases.push(CaseInput {
                    file: file.clone(),
                    test: format!("snapshot:{}", snap.name),
                    db_config: db_config.clone(),
                    setups: lookup_setups(&snap.modifiers.setups),
                    sql: snap.sql.clone(),
                });
            }
        }
    }
    eprintln!("collected {} cases", cases.len());

    let stats = Arc::new(Stats::default());
    let sem = Arc::new(Semaphore::new(args.jobs));
    let out = Arc::new(std::sync::Mutex::new(BufWriter::new(
        std::fs::File::create(&args.out)?,
    )));
    let timeout = Duration::from_secs(args.case_timeout_secs);
    let total = cases.len();

    let mut handles = Vec::new();
    for case in cases {
        let sem = sem.clone();
        let backend = backend.clone();
        let stats = stats.clone();
        let out = out.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let label = format!("{}::{}", case.file, case.test);
            match tokio::time::timeout(timeout, run_case(backend, case, stats.clone())).await {
                Ok(lines) => {
                    let mut w = out.lock().unwrap();
                    for line in lines {
                        writeln!(w, "{line}").unwrap();
                    }
                    w.flush().unwrap();
                }
                Err(_) => {
                    stats.cases_timeout.fetch_add(1, Ordering::Relaxed);
                    eprintln!("TIMEOUT: {label}");
                }
            }
            let done = stats.cases.fetch_add(1, Ordering::Relaxed) + 1;
            if done % 200 == 0 {
                eprintln!("progress: {done}/{total}");
            }
        }));
    }
    for h in handles {
        if h.await.is_err() {
            stats.cases_panic.fetch_add(1, Ordering::Relaxed);
        }
    }
    out.lock().unwrap().flush()?;

    eprintln!(
        "done: cases={} timeouts={} panics={} db_errors={} stmts={} programs={} explain_errors={} exec_errors={} split_errors={}",
        stats.cases.load(Ordering::Relaxed),
        stats.cases_timeout.load(Ordering::Relaxed),
        stats.cases_panic.load(Ordering::Relaxed),
        stats.cases_db_error.load(Ordering::Relaxed),
        stats.stmts.load(Ordering::Relaxed),
        stats.programs.load(Ordering::Relaxed),
        stats.explain_errors.load(Ordering::Relaxed),
        stats.exec_errors.load(Ordering::Relaxed),
        stats.split_errors.load(Ordering::Relaxed),
    );
    Ok(())
}
