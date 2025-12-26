mod opts;

use clap::Parser;
use core::panic;
use opts::Opts;
#[cfg(not(feature = "antithesis"))]
use rand::rngs::StdRng;
#[cfg(not(feature = "antithesis"))]
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
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

pub struct Plan {
    pub ddl_statements: Vec<String>,
    pub queries_per_thread: Vec<Vec<String>>,
    pub nr_iterations: usize,
    pub nr_threads: usize,
}

/// Represents a column in a SQLite table
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

/// Represents SQLite data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

/// Represents column constraints
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
}

/// Represents a table in a SQLite schema
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Represents a complete SQLite schema
#[derive(Debug, Clone)]
pub struct ArbitrarySchema {
    pub tables: Vec<Table>,
}

// Word lists for generating readable identifiers
const ADJECTIVES: &[&str] = &[
    "red", "blue", "green", "fast", "slow", "big", "small", "old", "new", "hot", "cold", "dark",
    "light", "soft", "hard", "loud", "quiet", "sweet", "sour", "fresh", "dry", "wet", "clean",
    "dirty", "empty", "full", "happy", "sad", "angry", "calm", "brave", "shy", "smart", "wild",
];

const NOUNS: &[&str] = &[
    "cat", "dog", "bird", "fish", "tree", "rock", "lake", "river", "cloud", "star", "moon", "sun",
    "book", "desk", "chair", "door", "wall", "roof", "floor", "road", "path", "hill", "cave",
    "leaf", "root", "seed", "fruit", "flower", "grass", "stone", "sand", "wave", "wind", "rain",
];

// Helper functions for generating random data
fn generate_random_identifier() -> String {
    let adj = ADJECTIVES[get_random() as usize % ADJECTIVES.len()];
    let noun = NOUNS[get_random() as usize % NOUNS.len()];
    let num = get_random() % 1000;
    format!("{adj}_{noun}_{num}")
}

fn generate_random_data_type() -> DataType {
    match get_random() % 5 {
        0 => DataType::Integer,
        1 => DataType::Real,
        2 => DataType::Text,
        3 => DataType::Blob,
        _ => DataType::Numeric,
    }
}

fn generate_random_constraint() -> Constraint {
    match get_random() % 2 {
        0 => Constraint::NotNull,
        _ => Constraint::Unique,
    }
}

fn generate_random_column() -> Column {
    let name = generate_random_identifier();
    let data_type = generate_random_data_type();

    let constraint_count = (get_random() % 2) as usize;
    let mut constraints = Vec::with_capacity(constraint_count);

    for _ in 0..constraint_count {
        constraints.push(generate_random_constraint());
    }

    Column {
        name,
        data_type,
        constraints,
    }
}

fn generate_random_table() -> Table {
    let name = generate_random_identifier();
    let column_count = (get_random() % 10 + 1) as usize;
    let mut columns = Vec::with_capacity(column_count);
    let mut column_names = HashSet::new();

    // First, generate all columns without primary keys
    for _ in 0..column_count {
        let mut column = generate_random_column();

        // Ensure column names are unique within the table
        while column_names.contains(&column.name) {
            column.name = generate_random_identifier();
        }

        column_names.insert(column.name.clone());
        columns.push(column);
    }

    // Then, randomly select one column to be the primary key
    let pk_index = (get_random() % column_count as u64) as usize;
    columns[pk_index].constraints.push(Constraint::PrimaryKey);
    Table { name, columns }
}

pub fn gen_bool(probability_true: f64) -> bool {
    (get_random() as f64 / u64::MAX as f64) < probability_true
}

pub fn gen_schema(table_count: Option<usize>) -> ArbitrarySchema {
    let table_count = table_count.unwrap_or((get_random() % 10 + 1) as usize);
    let mut tables = Vec::with_capacity(table_count);
    let mut table_names = HashSet::new();

    for _ in 0..table_count {
        let mut table = generate_random_table();

        // Ensure table names are unique
        while table_names.contains(&table.name) {
            table.name = generate_random_identifier();
        }

        table_names.insert(table.name.clone());
        tables.push(table);
    }

    ArbitrarySchema { tables }
}

impl ArbitrarySchema {
    /// Convert the schema to a vector of SQL DDL statements
    pub fn to_sql(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| {
                let columns = table
                    .columns
                    .iter()
                    .map(|col| {
                        let mut col_def =
                            format!("  {} {}", col.name, data_type_to_sql(&col.data_type));
                        for constraint in &col.constraints {
                            col_def.push(' ');
                            col_def.push_str(&constraint_to_sql(constraint));
                        }
                        col_def
                    })
                    .collect::<Vec<_>>()
                    .join(",");

                format!("CREATE TABLE IF NOT EXISTS {} ({});", table.name, columns)
            })
            .collect()
    }
}

fn data_type_to_sql(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Integer => "INTEGER",
        DataType::Real => "REAL",
        DataType::Text => "TEXT",
        DataType::Blob => "BLOB",
        DataType::Numeric => "NUMERIC",
    }
}

fn constraint_to_sql(constraint: &Constraint) -> String {
    match constraint {
        Constraint::PrimaryKey => "PRIMARY KEY".to_string(),
        Constraint::NotNull => "NOT NULL".to_string(),
        Constraint::Unique => "UNIQUE".to_string(),
    }
}

/// Generate a random value for a given data type
fn generate_random_value(data_type: &DataType) -> String {
    match data_type {
        DataType::Integer => (get_random() % 1000).to_string(),
        DataType::Real => format!("{:.2}", (get_random() % 1000) as f64 / 100.0),
        DataType::Text => format!("'{}'", generate_random_identifier()),
        DataType::Blob => format!("x'{}'", hex::encode(generate_random_identifier())),
        DataType::Numeric => (get_random() % 1000).to_string(),
    }
}

/// Generate a random INSERT statement for a table
fn generate_insert(table: &Table) -> String {
    let columns = table
        .columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<Vec<_>>()
        .join(", ");

    let values = table
        .columns
        .iter()
        .map(|col| generate_random_value(&col.data_type))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO {} ({}) VALUES ({});",
        table.name, columns, values
    )
}

/// Generate a random UPDATE statement for a table
fn generate_update(table: &Table) -> String {
    // Find the primary key column
    let pk_column = table
        .columns
        .iter()
        .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
        .expect("Table should have a primary key");

    // Get all non-primary key columns
    let non_pk_columns: Vec<_> = table
        .columns
        .iter()
        .filter(|col| col.name != pk_column.name)
        .collect();

    // If we have no non-PK columns, just update the primary key itself
    let set_clause = if non_pk_columns.is_empty() {
        format!(
            "{} = {}",
            pk_column.name,
            generate_random_value(&pk_column.data_type)
        )
    } else {
        non_pk_columns
            .iter()
            .map(|col| format!("{} = {}", col.name, generate_random_value(&col.data_type)))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let where_clause = format!(
        "{} = {}",
        pk_column.name,
        generate_random_value(&pk_column.data_type)
    );

    format!(
        "UPDATE {} SET {} WHERE {};",
        table.name, set_clause, where_clause
    )
}

/// Generate a random DELETE statement for a table
fn generate_delete(table: &Table) -> String {
    // Find the primary key column
    let pk_column = table
        .columns
        .iter()
        .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
        .expect("Table should have a primary key");

    let where_clause = format!(
        "{} = {}",
        pk_column.name,
        generate_random_value(&pk_column.data_type)
    );

    format!("DELETE FROM {} WHERE {};", table.name, where_clause)
}

/// Generate a random SQL statement for a schema
fn generate_random_statement(schema: &ArbitrarySchema) -> String {
    let table = &schema.tables[get_random() as usize % schema.tables.len()];
    match get_random() % 3 {
        0 => generate_insert(table),
        1 => generate_update(table),
        _ => generate_delete(table),
    }
}

fn generate_plan(opts: &Opts) -> Result<Plan, Box<dyn std::error::Error + Send + Sync>> {
    let schema = gen_schema(opts.tables);
    // Write DDL statements to log file
    let mut log_file = File::create(&opts.log_file)?;
    let ddl_statements = schema.to_sql();
    let mut plan = Plan {
        ddl_statements: vec![],
        queries_per_thread: vec![],
        nr_iterations: opts.nr_iterations,
        nr_threads: opts.nr_threads,
    };
    if !opts.skip_log {
        writeln!(log_file, "{}", opts.nr_threads)?;
        writeln!(log_file, "{}", opts.nr_iterations)?;
        writeln!(log_file, "{}", ddl_statements.len())?;
        for stmt in &ddl_statements {
            writeln!(log_file, "{stmt}")?;
        }
    }
    plan.ddl_statements = ddl_statements;
    for id in 0..opts.nr_threads {
        writeln!(log_file, "{id}",)?;
        let mut queries = vec![];
        let mut push = |sql: &str| {
            queries.push(sql.to_string());
            if !opts.skip_log {
                writeln!(log_file, "{sql}").unwrap();
            }
        };
        for i in 0..opts.nr_iterations {
            if !opts.silent && !opts.verbose && i % 100 == 0 {
                print!(
                    "\r{} %",
                    (i as f64 / opts.nr_iterations as f64 * 100.0) as usize
                );
                std::io::stdout().flush().unwrap();
            }
            let tx = if get_random() % 2 == 0 {
                Some("BEGIN")
            } else {
                None
            };
            if let Some(tx) = tx {
                push(tx);
            }
            let sql = generate_random_statement(&schema);
            push(&sql);
            if tx.is_some() {
                if get_random() % 2 == 0 {
                    push("COMMIT");
                } else {
                    push("ROLLBACK");
                }
            }
        }
        plan.queries_per_thread.push(queries);
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
                .with_thread_ids(true),
        )
        .try_init()
    {
        println!("Unable to setup tracing appender: {e:?}");
    }
    Ok((guard, reload_handle))
}

const LOG_LEVEL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .unhandled_panic(tokio::runtime::UnhandledPanic::ShutdownRuntime)
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

    let tempfile = tempfile::NamedTempFile::new()?;
    let (_, path) = tempfile.keep().unwrap();
    let db_file = if let Some(db_file) = opts.db_file {
        db_file
    } else {
        path.to_string_lossy().to_string()
    };

    println!("db_file={db_file}");

    let vfs_option = opts.vfs.clone();

    for thread in 0..opts.nr_threads {
        let db_file = db_file.clone();
        let mut builder = Builder::new_local(&db_file);
        if let Some(ref vfs) = vfs_option {
            builder = builder.with_io(vfs.clone());
        }
        let db = Arc::new(Mutex::new(builder.build().await?));
        let plan = plan.clone();
        let conn = db.lock().await.connect()?;

        conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;

        conn.execute("PRAGMA data_sync_retry = 1", ()).await?;

        // Apply each DDL statement individually
        for stmt in &plan.ddl_statements {
            if opts.verbose {
                println!("executing ddl {stmt}");
            }
            if let Err(e) = conn.execute(stmt, ()).await {
                match e {
                    turso::Error::Corrupt(e) => {
                        panic!("Error creating table: {e}");
                    }
                    turso::Error::Error(e) => {
                        println!("Error creating table: {e}");
                    }
                    _ => {
                        println!("Error creating table: {e}");
                        // Exit on any other error during table creation
                        std::process::exit(1);
                    }
                }
            }
        }

        let nr_iterations = plan.nr_iterations;
        let db = db.clone();
        let vfs_for_task = vfs_option.clone();

        let handle = tokio::spawn(async move {
            let mut conn = db.lock().await.connect()?;

            conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;

            conn.execute("PRAGMA data_sync_retry = 1", ()).await?;

            println!("\rExecuting queries...");
            for query_index in 0..nr_iterations {
                if gen_bool(0.0) {
                    // disabled
                    if opts.verbose {
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
                    conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;
                } else if gen_bool(0.0) {
                    // disabled
                    // Reconnect to the database
                    if opts.verbose {
                        println!("Reconnecting to database");
                    }
                    let db_guard = db.lock().await;
                    conn = db_guard.connect()?;
                    conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;
                }
                let sql = &plan.queries_per_thread[thread][query_index];
                if !opts.silent {
                    if opts.verbose {
                        println!("executing query {sql}");
                    } else if query_index % 100 == 0 {
                        print!(
                            "\r{:.2} %",
                            (query_index as f64 / nr_iterations as f64 * 100.0)
                        );
                        std::io::stdout().flush().unwrap();
                    }
                }
                if let Err(e) = conn.execute(sql, ()).await {
                    match e {
                        turso::Error::Corrupt(e) => {
                            panic!("Error executing query: {}", e);
                        }
                        turso::Error::Constraint(e) => {
                            if opts.verbose {
                                println!("Skipping UNIQUE constraint violation: {e}");
                            }
                        }
                        turso::Error::Error(e) => {
                            if opts.verbose {
                                println!("Error executing query: {e}");
                            }
                        }
                        _ => panic!("Error executing query: {}", e),
                    }
                }
                const INTEGRITY_CHECK_INTERVAL: usize = 100;
                if query_index % INTEGRITY_CHECK_INTERVAL == 0 {
                    let mut res = conn.query("PRAGMA integrity_check", ()).await.unwrap();
                    match res.next().await {
                        Ok(Some(row)) => {
                            let value = row.get_value(0).unwrap();
                            if value != "ok".into() {
                                panic!("integrity check failed: {:?}", value);
                            }
                        }
                        Ok(None) => {
                            panic!("integrity check failed: no rows");
                        }
                        Err(e) => {
                            println!("Error performing integrity check: {e}");
                        }
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

    #[cfg(not(miri))]
    {
        println!("Running SQLite Integrity check");
        sqlite_integrity_check(std::path::Path::new(&db_file))?;
    }

    Ok(())
}
