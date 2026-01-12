mod opts;

use clap::Parser;
use core::panic;
use opts::{Opts, TxMode};
use sqlsmith_rs_common::rand_by_seed::LcgRng;
#[cfg(not(feature = "antithesis"))]
use rand::rngs::StdRng;
#[cfg(not(feature = "antithesis"))]
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
#[cfg(not(feature = "antithesis"))]
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use turso::Builder;

#[cfg(not(feature = "antithesis"))]
static RNG: std::sync::OnceLock<StdMutex<StdRng>> = std::sync::OnceLock::new();

#[cfg(not(feature = "antithesis"))]
fn init_rng(seed: u64) {
    RNG.get_or_init(|| StdMutex::new(StdRng::seed_from_u64(seed)));
}

#[cfg(not(feature = "antithesis"))]
fn get_random() -> u64 {
    RNG.get()
        .expect("RNG not initialized")
        .lock()
        .unwrap()
        .random()
}

#[cfg(feature = "antithesis")]
fn get_random() -> u64 {
    antithesis_sdk::random::get_random()
}

#[derive(Debug)]
pub struct Plan {
    pub ddl_statements: Vec<String>,
    // Note: queries_per_thread is no longer used with sqlsmith-rs as queries are generated dynamically
    pub queries_per_thread: Vec<Vec<String>>,
    pub nr_iterations: usize,
    pub nr_threads: usize,
}


pub fn gen_bool(probability_true: f64) -> bool {
    (get_random() as f64 / u64::MAX as f64) < probability_true
}

/// Generate SQL statement using sqlsmith-rs standalone generators
fn generate_random_statement(rng: &mut LcgRng) -> String {
    // Use only standalone generators that don't require schema introspection
    let stmt_kind = (rng.rand().unsigned_abs() % 4) as u64;
    
    match stmt_kind {
        0 => {
            // CREATE TABLE
            sqlsmith_rs_executor::generators::common::create_table_stmt_common::gen_create_table_stmt(rng)
                .unwrap_or_else(|| "SELECT 1;".to_string())
        }
        1 => {
            // VACUUM
            sqlsmith_rs_executor::generators::common::vacuum_stmt_common::gen_vacuum_stmt()
                .unwrap_or_else(|| "SELECT 2;".to_string())
        }
        2 => {
            // TRANSACTION
            sqlsmith_rs_executor::generators::common::transaction_stmt_common::gen_transaction_stmt(rng)
                .unwrap_or_else(|| "SELECT 3;".to_string())
        }
        _ => {
            // DATE FUNC
            sqlsmith_rs_executor::generators::common::datefunc_stmt_common::gen_datefunc_stmt(rng)
                .unwrap_or_else(|| "SELECT 4;".to_string())
        }
    }
}

fn generate_plan(opts: &Opts) -> Result<Plan, Box<dyn std::error::Error + Send + Sync>> {
    let mut log_file = File::create(&opts.log_file)?;
    if !opts.skip_log {
        writeln!(log_file, "{}", opts.nr_threads)?;
        writeln!(log_file, "{}", opts.nr_iterations)?;
    }
    let mut plan = Plan {
        ddl_statements: vec![],
        queries_per_thread: vec![],
        nr_iterations: opts.nr_iterations,
        nr_threads: opts.nr_threads,
    };

    // For now, if db_ref is provided, return an error since we're using simplified SQL generation
    if opts.db_ref.is_some() {
        writeln!(log_file, "{}", 0)?;
        return Err("db_ref feature not yet implemented with sqlsmith-rs".into());
    } else {
        // Generate simple DDL statements using sqlsmith-rs
        let mut rng = LcgRng::new(get_random());
        let table_count = opts.tables.unwrap_or(5);
        let mut ddl_statements = vec![];
        
        for _ in 0..table_count {
            if let Some(create_table_stmt) = sqlsmith_rs_executor::generators::common::create_table_stmt_common::gen_create_table_stmt(&mut rng) {
                ddl_statements.push(create_table_stmt);
            }
        }
        
        if !opts.skip_log {
            writeln!(log_file, "{}", ddl_statements.len())?;
            for stmt in &ddl_statements {
                writeln!(log_file, "{stmt}")?;
            }
        }
        plan.ddl_statements = ddl_statements;
    }

    // Initialize empty query vectors (queries are now generated dynamically during execution)
    for id in 0..opts.nr_threads {
        writeln!(log_file, "{id}")?;
        plan.queries_per_thread.push(vec![]);
    }

    Ok(plan)
}

fn read_plan_from_log_file(opts: &Opts) -> Result<Plan, Box<dyn std::error::Error + Send + Sync>> {
    let mut file = File::open(&opts.log_file)?;
    let mut buf = String::new();
    let mut plan = Plan {
        ddl_statements: vec![],
        queries_per_thread: vec![],
        nr_iterations: 0,
        nr_threads: 0,
    };
    file.read_to_string(&mut buf).unwrap();
    let mut lines = buf.lines();
    plan.nr_threads = lines.next().expect("missing threads").parse().unwrap();
    plan.nr_iterations = lines
        .next()
        .expect("missing nr_iterations")
        .parse()
        .unwrap();
    let nr_ddl = lines
        .next()
        .expect("number of ddl statements")
        .parse()
        .unwrap();
    for _ in 0..nr_ddl {
        plan.ddl_statements
            .push(lines.next().expect("expected ddl statement").to_string());
    }
    for line in lines {
        if line.parse::<i64>().is_ok() {
            plan.queries_per_thread.push(Vec::new());
        } else {
            plan.queries_per_thread
                .last_mut()
                .unwrap()
                .push(line.to_string());
        }
    }
    Ok(plan)
}

pub type LogLevelReloadHandle = reload::Handle<EnvFilter, tracing_subscriber::Registry>;

pub fn init_tracing() -> Result<(WorkerGuard, LogLevelReloadHandle), std::io::Error> {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
    let filter = EnvFilter::from_default_env();
    let (filter_layer, reload_handle) = reload::Layer::new(filter);

    if let Err(e) = tracing_subscriber::registry()
        .with(filter_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_line_number(true)
                .with_thread_ids(true)
                .json(),
        )
        .try_init()
    {
        println!("Unable to setup tracing appender: {e:?}");
    }
    Ok((guard, reload_handle))
}

const LOG_LEVEL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

const LOG_LEVEL_FILE: &str = "RUST_LOG";

/// Spawns a background thread that watches for a RUST_LOG file and dynamically
/// updates the log level when the file contents change.
///
/// The file should contain a valid tracing filter string (e.g., "debug", "info",
/// "limbo_core=trace,warn"). If the file is removed or contains an invalid filter,
/// the current log level is preserved.
pub fn spawn_log_level_watcher(reload_handle: LogLevelReloadHandle) {
    std::thread::spawn(move || {
        let mut last_content: Option<String> = None;

        loop {
            std::thread::sleep(LOG_LEVEL_POLL_INTERVAL);

            let content = match std::fs::read_to_string(LOG_LEVEL_FILE) {
                Ok(content) => content.trim().to_string(),
                Err(_) => {
                    continue;
                }
            };

            if last_content.as_ref() == Some(&content) {
                continue;
            }

            match content.parse::<EnvFilter>() {
                Ok(new_filter) => {
                    if let Err(e) = reload_handle.reload(new_filter) {
                        eprintln!("Failed to reload log filter: {e}");
                    } else {
                        last_content = Some(content);
                    }
                }
                Err(e) => {
                    eprintln!("Invalid log filter in {LOG_LEVEL_FILE}: {e}");
                    last_content = Some(content);
                }
            }
        }
    });
}

fn sqlite_integrity_check(
    db_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    assert!(db_path.exists());
    let conn = rusqlite::Connection::open(db_path)?;
    let mut stmt = conn.prepare("SELECT * FROM pragma_integrity_check;")?;
    let mut rows = stmt.query(())?;
    let mut result: Vec<String> = Vec::new();

    while let Some(row) = rows.next()? {
        result.push(row.get(0)?);
    }
    assert!(!result.is_empty());
    if !result[0].eq_ignore_ascii_case("ok") {
        // Build a list of problems
        result.iter_mut().for_each(|row| *row = format!("- {row}"));
        return Err(format!("SQLite integrity check failed: {}", result.join("\n")).into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (_guard, reload_handle) = init_tracing()?;

    spawn_log_level_watcher(reload_handle);

    let opts = Opts::parse();

    // Initialize RNG
    #[cfg(feature = "antithesis")]
    {
        if opts.seed.is_some() {
            eprintln!("Error: --seed is not supported under Antithesis");
            std::process::exit(1);
        }
        println!("Using randomness from Antithesis");
    }
    #[cfg(not(feature = "antithesis"))]
    {
        let seed = opts.seed.unwrap_or_else(rand::random);
        init_rng(seed);
        println!("Using seed: {seed}");
    }
    if opts.nr_threads > 1 {
        println!("WARNING: Multi-threaded data access is not yet supported: https://github.com/tursodatabase/turso/issues/1552");
    }

    let plan = if opts.load_log {
        println!("Loading plan from log file...");
        read_plan_from_log_file(&opts)?
    } else {
        println!("Generating plan...");
        generate_plan(&opts)?
    };

    let mut handles = Vec::with_capacity(opts.nr_threads);
    let plan = Arc::new(plan);
    let mut stop = false;

    let tempfile = tempfile::NamedTempFile::new()?;
    let (_, path) = tempfile.keep().unwrap();
    let db_file = if let Some(db_file) = opts.db_file {
        db_file
    } else {
        if let Some(db_ref) = opts.db_ref {
            std::fs::copy(db_ref, &path)?;
        }
        path.to_string_lossy().to_string()
    };

    println!("db_file={db_file}");

    let vfs_option = opts.vfs.clone();

    for thread in 0..plan.nr_threads {
        if stop {
            break;
        }
        let db_file = db_file.clone();
        let mut builder = Builder::new_local(&db_file);
        if let Some(ref vfs) = vfs_option {
            builder = builder.with_io(vfs.clone());
        }
        let db = Arc::new(Mutex::new(builder.build().await?));
        let plan = plan.clone();
        let conn = db.lock().await.connect()?;

        match opts.tx_mode {
            TxMode::SQLite => {
                conn.pragma_update("journal_mode", "WAL").await?;
                conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;
            }
            TxMode::Concurrent => {
                conn.pragma_update("journal_mode", "experimental_mvcc")
                    .await?;
            }
        };

        conn.execute("PRAGMA data_sync_retry = 1", ()).await?;

        // Apply each DDL statement individually
        for stmt in &plan.ddl_statements {
            if opts.verbose {
                println!("executing ddl {stmt}");
            }
            let mut retry_counter = 0;
            while retry_counter < 10 {
                match conn.execute(stmt, ()).await {
                    Ok(_) => break,
                    Err(turso::Error::Busy(e)) => {
                        println!("Error (busy) creating table: {e}");
                        retry_counter += 1;
                    }
                    Err(turso::Error::DatabaseFull(e)) => {
                        eprintln!("Database full, stopping: {e}");
                        stop = true;
                        break;
                    }
                    Err(turso::Error::IoError(std::io::ErrorKind::StorageFull)) => {
                        eprintln!("No storage space, stopping");
                        stop = true;
                        break;
                    }
                    Err(turso::Error::BusySnapshot(e)) => {
                        println!("Error (busy snapshot): {e}");
                        retry_counter += 1;
                    }
                    Err(turso::Error::IoError(kind)) => {
                        eprintln!("I/O error ({kind:?}), stopping");
                        stop = true;
                        break;
                    }
                    Err(e) => {
                        panic!("Error creating table: {e}");
                    }
                }
            }
            if stop {
                break;
            }
            if retry_counter == 10 {
                panic!("Could not execute statement [{stmt}] after {retry_counter} attempts.");
            }
        }

        let nr_queries = plan.nr_iterations;
        let db = db.clone();
        let vfs_for_task = vfs_option.clone();
        let busy_timeout = opts.busy_timeout;
        let verbose = opts.verbose;
        let silent = opts.silent;
        let seed = get_random();

        let handle = tokio::spawn(async move {
            let mut conn = db.lock().await.connect()?;

            conn.busy_timeout(std::time::Duration::from_millis(busy_timeout))?;

            conn.execute("PRAGMA data_sync_retry = 1", ()).await?;

            println!("\rExecuting queries...");
            
            // Initialize RNG for SQL generation
            let mut rng = LcgRng::new(seed);
            
            for query_index in 0..nr_queries {
                if gen_bool(0.0) {
                    // disabled
                    if verbose {
                        println!("Reopening database");
                    }
                    // Reopen the database
                    let mut db_guard = db.lock().await;
                    let mut builder = Builder::new_local(&db_file);
                    if let Some(ref vfs) = vfs_for_task {
                        builder = builder.with_io(vfs.clone());
                    }
                    *db_guard = builder.build().await?;
                    conn = db_guard.connect()?;
                    conn.busy_timeout(std::time::Duration::from_millis(busy_timeout))?;
                } else if gen_bool(0.0) {
                    // disabled
                    // Reconnect to the database
                    if verbose {
                        println!("Reconnecting to database");
                    }
                    let db_guard = db.lock().await;
                    conn = db_guard.connect()?;
                    conn.busy_timeout(std::time::Duration::from_millis(busy_timeout))?;
                }
                
                // Generate SQL dynamically using sqlsmith-rs
                let sql = generate_random_statement(&mut rng);
                
                if !silent {
                    if verbose {
                        println!("thread#{thread} executing query {sql}");
                    } else if query_index % 100 == 0 {
                        print!(
                            "\r{:.2} %",
                            (query_index as f64 / nr_queries as f64 * 100.0)
                        );
                        std::io::stdout().flush().unwrap();
                    }
                }
                if verbose {
                    eprintln!("thread#{thread}(start): {sql}");
                }
                if let Err(e) = conn.execute(&sql, ()).await {
                    match e {
                        turso::Error::Corrupt(e) => {
                            panic!("thread#{thread} Error[FATAL] executing query: {}", e);
                        }
                        turso::Error::Constraint(e) => {
                            if verbose {
                                println!("thread#{thread} Error[WARNING] executing query: {e}");
                            }
                        }
                        turso::Error::Busy(e) => {
                            println!("thread#{thread} Error[WARNING] executing query: {e}");
                        }
                        turso::Error::BusySnapshot(e) => {
                            println!("thread#{thread} Error[WARNING] busy snapshot: {e}");
                        }
                        turso::Error::Error(e) => {
                            if verbose {
                                println!("thread#{thread} Error[INFO] executing query: {e}");
                            }
                        }
                        turso::Error::DatabaseFull(e) => {
                            eprintln!("thread#{thread} Database full: {e}");
                        }
                        turso::Error::IoError(kind) => {
                            eprintln!("thread#{thread} I/O error ({kind:?}), continuing...");
                        }
                        _ => {
                            if verbose {
                                println!("thread#{thread} Error[INFO] executing query: {}", e);
                            }
                        }
                    }
                }
                if verbose {
                    eprintln!("thread#{thread}(end): {sql}");
                }
                const INTEGRITY_CHECK_INTERVAL: usize = 100;
                if query_index % INTEGRITY_CHECK_INTERVAL == 0 {
                    if verbose {
                        eprintln!("thread#{thread}(start): PRAGMA integrity_check");
                    }
                    let mut res = conn.query("PRAGMA integrity_check", ()).await.unwrap();
                    match res.next().await {
                        Ok(Some(row)) => {
                            let value = row.get_value(0).unwrap();
                            if value != "ok".into() {
                                panic!("thread#{thread} integrity check failed: {:?}", value);
                            }
                        }
                        Ok(None) => {
                            panic!("thread#{thread} integrity check failed: no rows");
                        }
                        Err(e) => {
                            println!("thread#{thread} Error performing integrity check: {e}");
                        }
                    }
                    match res.next().await {
                        Ok(Some(_)) => {
                            panic!("thread#{thread} integrity check failed: more than 1 row")
                        }
                        Err(e) => println!("thread#{thread} Error performing integrity check: {e}"),
                        _ => {}
                    }
                    if verbose {
                        eprintln!("thread#{thread}(end): PRAGMA integrity_check");
                    }
                }
            }
            // In case this thread is running an exclusive transaction, commit it so that it doesn't block other threads.
            let _ = conn.execute("COMMIT", ()).await;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }
    println!("Done. SQL statements written to {}", opts.log_file);
    println!("Database file: {db_file}");

    // Switch back to WAL mode before SQLite integrity check if we were in MVCC mode.
    // SQLite/rusqlite doesn't understand MVCC journal mode.
    if opts.tx_mode == TxMode::Concurrent {
        let mut builder = Builder::new_local(&db_file);
        if let Some(ref vfs) = vfs_option {
            builder = builder.with_io(vfs.clone());
        }
        let db = builder.build().await?;
        let conn = db.connect()?;
        conn.pragma_update("journal_mode", "WAL").await?;
        println!("Switched journal mode back to WAL for SQLite integrity check");
    }

    #[cfg(not(miri))]
    {
        println!("Running SQLite Integrity check");
        sqlite_integrity_check(std::path::Path::new(&db_file))?;
    }

    Ok(())
}
