mod opts;

use clap::Parser;
use core::panic;
use opts::{Opts, TxMode};
#[cfg(not(feature = "antithesis"))]
use rand::rngs::StdRng;
#[cfg(not(feature = "antithesis"))]
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
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
    pub pk_values: Vec<String>,
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
    Table {
        name,
        columns,
        pk_values: vec![],
    }
}

pub fn gen_bool(probability_true: f64) -> bool {
    (get_random() as f64 / u64::MAX as f64) < probability_true
}

pub fn gen_schema(table_count: Option<usize>) -> ArbitrarySchema {
    let table_count = table_count.unwrap_or_else(|| (get_random() % 10 + 1) as usize);
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
        .map(|col| {
            if !table.pk_values.is_empty()
                && col.constraints.contains(&Constraint::PrimaryKey)
                && get_random() % 100 < 50
            {
                table.pk_values[get_random() as usize % table.pk_values.len()].clone()
            } else {
                generate_random_value(&col.data_type)
            }
        })
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

    let where_clause = if !table.pk_values.is_empty() && get_random() % 100 < 50 {
        format!(
            "{} = {}",
            pk_column.name,
            table.pk_values[get_random() as usize % table.pk_values.len()]
        )
    } else {
        format!(
            "{} = {}",
            pk_column.name,
            generate_random_value(&pk_column.data_type)
        )
    };

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

    let where_clause = if !table.pk_values.is_empty() && get_random() % 100 < 50 {
        format!(
            "{} = {}",
            pk_column.name,
            table.pk_values[get_random() as usize % table.pk_values.len()]
        )
    } else {
        format!(
            "{} = {}",
            pk_column.name,
            generate_random_value(&pk_column.data_type)
        )
    };

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

/// Convert SQLite type string to DataType
fn map_sqlite_type(type_str: &str) -> DataType {
    let t = type_str.to_uppercase();

    if t.contains("INT") {
        DataType::Integer
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        DataType::Text
    } else if t.contains("BLOB") {
        DataType::Blob
    } else if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        DataType::Real
    } else {
        DataType::Numeric
    }
}

/// Load full schema from SQLite database
pub fn load_schema(
    db_path: &Path,
) -> Result<ArbitrarySchema, Box<dyn std::error::Error + Send + Sync>> {
    let conn = rusqlite::Connection::open(db_path)?;

    // Fetch user tables (ignore sqlite internal tables)
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;

    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;

    let mut tables = Vec::new();

    for table_name in table_names {
        let pragma = format!("PRAGMA table_info({table_name})");
        let mut pragma_stmt = conn.prepare(&pragma)?;

        let columns = pragma_stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let type_str: String = row.get(2)?;
                let not_null: bool = row.get::<_, i32>(3)? != 0;
                let is_pk: bool = row.get::<_, i32>(5)? != 0;

                let mut constraints = Vec::new();

                if is_pk {
                    constraints.push(Constraint::PrimaryKey);
                }
                if not_null {
                    constraints.push(Constraint::NotNull);
                }

                Ok(Column {
                    name,
                    data_type: map_sqlite_type(&type_str),
                    constraints,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let pk_column = columns
            .iter()
            .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
            .expect("Table should have a primary key");
        let mut select_stmt =
            conn.prepare(&format!("SELECT {} FROM {table_name}", pk_column.name))?;
        let mut rows = select_stmt.query(())?;
        let mut pk_values = Vec::new();
        while let Some(row) = rows.next()? {
            let value = match row.get_ref(0)? {
                rusqlite::types::ValueRef::Null => "NULL".to_string(),
                rusqlite::types::ValueRef::Integer(x) => x.to_string(),
                rusqlite::types::ValueRef::Real(x) => x.to_string(),
                rusqlite::types::ValueRef::Text(text) => {
                    format!("'{}'", std::str::from_utf8(text)?)
                }
                rusqlite::types::ValueRef::Blob(blob) => format!("x'{}'", hex::encode(blob)),
            };
            pk_values.push(value);
        }
        tables.push(Table {
            name: table_name,
            columns,
            pk_values,
        });
    }

    Ok(ArbitrarySchema { tables })
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
    let schema = if let Some(db_ref) = &opts.db_ref {
        writeln!(log_file, "{}", 0)?;
        load_schema(db_ref)?
    } else {
        let schema = gen_schema(opts.tables);
        let ddl_statements = schema.to_sql();
        if !opts.skip_log {
            writeln!(log_file, "{}", ddl_statements.len())?;
            for stmt in &ddl_statements {
                writeln!(log_file, "{stmt}")?;
            }
        }
        plan.ddl_statements = ddl_statements;
        schema
    };
    // Write DDL statements to log file
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
                std::io::stdout().flush()?;
            }
            let tx = if get_random() % 2 == 0 {
                match opts.tx_mode {
                    TxMode::SQLite => Some("BEGIN;"),
                    TxMode::Concurrent => Some("BEGIN CONCURRENT;"),
                }
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
                    push("COMMIT;");
                } else {
                    push("ROLLBACK;");
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
    file.read_to_string(&mut buf)?;
    let mut lines = buf.lines();
    plan.nr_threads = lines.next().expect("missing threads").parse()?;
    plan.nr_iterations = lines
        .next()
        .expect("missing nr_iterations")
        .parse()?;
    let nr_ddl = lines
        .next()
        .expect("number of ddl statements")
        .parse()?;
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
    db_path: &Path,
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

#[cfg(not(shuttle))]
compile_error!("The stress test binary requires the `shuttle` cfg to be enabled. Run with: cargo run -p turso-stress --config 'build.rustflags = [\"--cfg\", \"shuttle\"]'");

#[cfg(shuttle)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use shuttle::scheduler::RandomScheduler;
    let opts = Opts::parse();

    let (_guard, reload_handle) = init_tracing()?;
    spawn_log_level_watcher(reload_handle);

    init_rng(&opts);

    let mut config = shuttle::Config::default();
    config.stack_size *= 10;
    config.max_steps = shuttle::MaxSteps::FailAfter(10_000_000);

    let scheduler = RandomScheduler::new_from_seed(opts.seed.unwrap_or_else(|| get_random()), 5);
    let runner = shuttle::Runner::new(scheduler, config);
    runner.run(move || shuttle::future::block_on(Box::pin(async_main(opts.clone()))).unwrap());

    Ok(())
}

fn init_rng(opts: &Opts) {
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
        RNG.get_or_init(|| StdMutex::new(StdRng::seed_from_u64(seed)));
        println!("Using seed: {seed}");
    }
    if opts.nr_threads > 1 {
        println!("WARNING: Multi-threaded data access is not yet supported: https://github.com/tursodatabase/turso/issues/1552");
    }
}

async fn async_main(opts: Opts) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    let (_, path) = tempfile.keep()?;
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

        let nr_queries = plan.queries_per_thread[thread].len();
        let db = db.clone();
        let vfs_for_task = vfs_option.clone();

        let handle = turso_stress::future::spawn(async move {
            let mut conn = db.lock().await.connect()?;

            conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;

            conn.execute("PRAGMA data_sync_retry = 1", ()).await?;

            println!("\rExecuting queries...");
            for query_index in 0..nr_queries {
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
                        println!("thread#{thread} executing query {sql}");
                    } else if query_index % 100 == 0 {
                        print!(
                            "\r{:.2} %",
                            query_index as f64 / nr_queries as f64 * 100.0
                        );
                        std::io::stdout().flush().unwrap();
                    }
                }
                if opts.verbose {
                    eprintln!("thread#{thread}(start): {sql}");
                }
                if let Err(e) = conn.execute(sql, ()).await {
                    match e {
                        turso::Error::Corrupt(e) => {
                            panic!("thread#{thread} Error[FATAL] executing query: {}", e);
                        }
                        turso::Error::Constraint(e) => {
                            if opts.verbose {
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
                            if opts.verbose {
                                println!("thread#{thread} Error executing query: {e}");
                            }
                        }
                        turso::Error::DatabaseFull(e) => {
                            eprintln!("thread#{thread} Database full: {e}");
                        }
                        turso::Error::IoError(kind) => {
                            eprintln!("thread#{thread} I/O error ({kind:?}), continuing...");
                        }
                        _ => panic!("thread#{thread} Error[FATAL] executing query: {}", e),
                    }
                }
                if opts.verbose {
                    eprintln!("thread#{thread}(end): {sql}");
                }
                const INTEGRITY_CHECK_INTERVAL: usize = 100;
                if query_index % INTEGRITY_CHECK_INTERVAL == 0 {
                    if opts.verbose {
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
                    if opts.verbose {
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
        sqlite_integrity_check(Path::new(&db_file))?;
    }

    Ok(())
}
