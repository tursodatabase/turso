use rand::Rng;
mod conn;
mod counter;
mod logging;
mod opts;
mod progress;
mod sql_logging;

use clap::Parser;
use opts::{Opts, TxMode};
#[cfg(not(antithesis))]
use rand::rngs::StdRng;
#[cfg(not(antithesis))]
use rand::SeedableRng;
#[cfg(shuttle)]
use shuttle::scheduler::Scheduler;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use turso_stress::sync::atomic::{AtomicBool, Ordering};
use turso_stress::sync::Arc;
use turso_stress::sync::AsyncBarrier as Barrier;
use turso_stress::sync::AsyncMutex as Mutex;

use crate::conn::{StressConn, StressDb};
use crate::counter::StressCounter;
use crate::logging::Tracer;
use crate::progress::ProgressBars;
use crate::sql_logging::SqlLogger;
use turso::core::clear_database_registry;
use turso::{Builder, Value};
use turso_stress::ThreadId;

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

#[cfg(not(antithesis))]
type StressRng = StdRng;
#[cfg(antithesis)]
type StressRng = antithesis_sdk::random::AntithesisRng;

struct ThreadRng {
    rng: StressRng,
}

impl Deref for ThreadRng {
    type Target = StressRng;

    fn deref(&self) -> &Self::Target {
        &self.rng
    }
}

impl DerefMut for ThreadRng {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rng
    }
}

impl ThreadRng {
    fn choose<'a, T>(&mut self, ts: &'a [T]) -> &'a T {
        #[cfg(not(antithesis))]
        return &ts[self.rng.random_range(0..ts.len())];
        #[cfg(antithesis)]
        antithesis_sdk::random::random_choice(ts).expect("choose called on empty slice")
    }
}

#[cfg(not(antithesis))]
impl ThreadRng {
    fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

#[cfg(antithesis)]
impl ThreadRng {
    fn new(_seed: u64) -> Self {
        Self {
            rng: antithesis_sdk::random::AntithesisRng,
        }
    }
}

// Helper functions for generating random data
fn generate_random_identifier(rng: &mut ThreadRng) -> String {
    let adj = rng.choose(ADJECTIVES);
    let noun = rng.choose(NOUNS);
    let num = rng.random_range(0..1000);
    format!("{adj}_{noun}_{num}")
}

fn generate_random_data_type(rng: &mut ThreadRng) -> DataType {
    rng.choose(&[
        DataType::Integer,
        DataType::Real,
        DataType::Text,
        DataType::Blob,
        DataType::Numeric,
    ])
    .clone()
}

fn generate_random_constraint(rng: &mut ThreadRng) -> Constraint {
    rng.choose(&[Constraint::NotNull, Constraint::Unique])
        .clone()
}

fn generate_random_column(rng: &mut ThreadRng) -> Column {
    let name = generate_random_identifier(rng);
    let data_type = generate_random_data_type(rng);

    let constraint_count = rng.random_range(0..2) as usize;
    let mut constraints = Vec::with_capacity(constraint_count);

    for _ in 0..constraint_count {
        constraints.push(generate_random_constraint(rng));
    }

    Column {
        name,
        data_type,
        constraints,
    }
}

fn generate_random_table(rng: &mut ThreadRng) -> Table {
    let name = generate_random_identifier(rng);
    let column_count = rng.random_range(1..11) as usize;
    let mut columns = Vec::with_capacity(column_count);
    let mut column_names = HashSet::new();

    // First, generate all columns without primary keys
    for _ in 0..column_count {
        let mut column = generate_random_column(rng);

        // Ensure column names are unique within the table
        while column_names.contains(&column.name) {
            column.name = generate_random_identifier(rng);
        }

        column_names.insert(column.name.clone());
        columns.push(column);
    }

    // Then, randomly select one column to be the primary key
    let pk_index = rng.random_range(0..column_count);
    columns[pk_index].constraints.push(Constraint::PrimaryKey);
    Table {
        name,
        columns,
        pk_values: vec![],
    }
}

fn gen_schema(rng: &mut ThreadRng, table_count: Option<usize>) -> ArbitrarySchema {
    let table_count = table_count.unwrap_or_else(|| rng.random_range(1..11) as usize);
    let mut tables = Vec::with_capacity(table_count);
    let mut table_names = HashSet::new();

    for _ in 0..table_count {
        let mut table = generate_random_table(rng);

        // Ensure table names are unique
        while table_names.contains(&table.name) {
            table.name = generate_random_identifier(rng);
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
fn generate_random_value(rng: &mut ThreadRng, data_type: &DataType) -> String {
    match data_type {
        DataType::Integer => rng.random_range(0..1000).to_string(),
        DataType::Real => format!("{:.2}", rng.random_range(0f32..100f32)),
        DataType::Text => format!("'{}'", generate_random_identifier(rng)),
        DataType::Blob => {
            // 20% chance of generating a large blob via zeroblob() to trigger
            // page allocation (the pattern that exposed the savepoint rollback
            // bug in tursodatabase/turso#6176).
            if rng.random_ratio(1, 5) {
                let size = rng.random_range(1000..9000);
                format!("zeroblob({size})")
            } else {
                format!("x'{}'", hex::encode(generate_random_identifier(rng)))
            }
        }
        DataType::Numeric => rng.random_range(0..1000).to_string(),
    }
}

/// Generate a random INSERT statement for a table
fn generate_insert(rng: &mut ThreadRng, table: &Table) -> String {
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
                && rng.random_ratio(1, 2)
            {
                rng.choose(&table.pk_values).clone()
            } else {
                generate_random_value(rng, &col.data_type)
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
fn generate_update(rng: &mut ThreadRng, table: &Table) -> String {
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
            generate_random_value(rng, &pk_column.data_type)
        )
    } else {
        non_pk_columns
            .iter()
            .map(|col| {
                format!(
                    "{} = {}",
                    col.name,
                    generate_random_value(rng, &col.data_type)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let where_clause = if !table.pk_values.is_empty() && rng.random_ratio(1, 2) {
        format!("{} = {}", pk_column.name, rng.choose(&table.pk_values))
    } else {
        format!(
            "{} = {}",
            pk_column.name,
            generate_random_value(rng, &pk_column.data_type)
        )
    };

    format!(
        "UPDATE {} SET {} WHERE {};",
        table.name, set_clause, where_clause
    )
}

/// Generate a random DELETE statement for a table
fn generate_delete(rng: &mut ThreadRng, table: &Table) -> String {
    // Find the primary key column
    let pk_column = table
        .columns
        .iter()
        .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
        .expect("Table should have a primary key");

    let where_clause = if !table.pk_values.is_empty() && rng.random_ratio(1, 2) {
        format!("{} = {}", pk_column.name, rng.choose(&table.pk_values))
    } else {
        format!(
            "{} = {}",
            pk_column.name,
            generate_random_value(rng, &pk_column.data_type)
        )
    };

    format!("DELETE FROM {} WHERE {};", table.name, where_clause)
}

/// Generate a random SQL statement for a schema
fn generate_random_statement(rng: &mut ThreadRng, schema: &ArbitrarySchema) -> String {
    let table = rng.choose(&schema.tables);
    match rng.random_range(0..=2) {
        0 => generate_insert(rng, table),
        1 => generate_update(rng, table),
        _ => generate_delete(rng, table),
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
    let mut stmt = conn.prepare_cached(
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
            conn.prepare_cached(&format!("SELECT {} FROM {table_name}", pk_column.name))?;
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

/// Load the schema through an existing turso connection. Used by multiprocess
/// workers: sibling worker processes may already be writing to the shared
/// database, so it must not be opened with rusqlite here (SQLite does not
/// participate in turso's multiprocess coordination).
async fn load_schema_via_conn(
    conn: &StressConn,
) -> Result<ArbitrarySchema, Box<dyn std::error::Error + Send + Sync>> {
    // Exclude turso-internal tables too: MVCC mode exposes
    // `__turso_internal_mvcc_meta` in sqlite_master, and internal tables are
    // not part of the workload schema (and may lack a primary key).
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type='table' \
             AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%'",
            (),
        )
        .await?;
    let mut table_names = Vec::new();
    while let Some(row) = rows.next().await? {
        let Value::Text(name) = row.get_value(0)? else {
            return Err("sqlite_master.name must be text".into());
        };
        table_names.push(name);
    }
    turso_macros::turso_assert!(
        !table_names.is_empty(),
        "multiprocess worker must observe the schema created by the coordinator"
    );

    let mut tables = Vec::new();
    for table_name in table_names {
        let mut rows = conn
            .query(format!("PRAGMA table_info({table_name})"), ())
            .await?;
        let mut columns = Vec::new();
        while let Some(row) = rows.next().await? {
            let Value::Text(name) = row.get_value(1)? else {
                return Err(format!("table_info({table_name}).name must be text").into());
            };
            let Value::Text(type_str) = row.get_value(2)? else {
                return Err(format!("table_info({table_name}).type must be text").into());
            };
            let not_null = !matches!(row.get_value(3)?, Value::Integer(0));
            let is_pk = !matches!(row.get_value(5)?, Value::Integer(0));

            let mut constraints = Vec::new();
            if is_pk {
                constraints.push(Constraint::PrimaryKey);
            }
            if not_null {
                constraints.push(Constraint::NotNull);
            }
            columns.push(Column {
                name,
                data_type: map_sqlite_type(&type_str),
                constraints,
            });
        }

        let pk_column = columns
            .iter()
            .find(|col| col.constraints.contains(&Constraint::PrimaryKey))
            .expect("Table should have a primary key");
        let mut rows = conn
            .query(format!("SELECT {} FROM {table_name}", pk_column.name), ())
            .await?;
        let mut pk_values = Vec::new();
        while let Some(row) = rows.next().await? {
            pk_values.push(value_to_sql_literal(&row.get_value(0)?));
        }

        tables.push(Table {
            name: table_name,
            columns,
            pk_values,
        });
    }

    Ok(ArbitrarySchema { tables })
}

fn value_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(x) => x.to_string(),
        Value::Real(x) => x.to_string(),
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Blob(blob) => format!("x'{}'", hex::encode(blob)),
    }
}

/// Generate the initial schema, or load it from `--db-ref`.
fn initial_schema(
    opts: &Opts,
    rng: &mut ThreadRng,
) -> Result<ArbitrarySchema, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(ref db_ref) = opts.db_ref {
        load_schema(db_ref)
    } else {
        Ok(gen_schema(rng, opts.tables))
    }
}

/// Resolve the target database file, creating a temp file when none is given
/// (seeded from `--db-ref` if provided).
fn resolve_db_file(opts: &Opts) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(db_file) = opts.db_file.clone() {
        return Ok(db_file);
    }
    let tempfile = tempfile::NamedTempFile::new()?;
    let (_, path) = tempfile.keep().unwrap();
    if let Some(ref db_ref) = opts.db_ref {
        std::fs::copy(db_ref, &path)?;
    }
    Ok(path.to_string_lossy().to_string())
}

/// Run `PRAGMA wal_checkpoint` with a random mode. Busy/locked errors are
/// expected under contention and ignored; corruption is fatal.
async fn run_random_wal_checkpoint(
    conn: &StressConn,
    rng: &mut ThreadRng,
    sql_logger: &SqlLogger,
    thread: &ThreadId,
) {
    let mode = *rng.choose(&["PASSIVE", "FULL", "RESTART", "TRUNCATE"]);
    let sql = format!("PRAGMA wal_checkpoint({mode})");
    let result: Result<(), turso::Error> = async {
        let mut rows = conn.query(&sql, ()).await?;
        while rows.next().await?.is_some() {}
        Ok(())
    }
    .await;
    match result {
        Ok(()) => sql_logger.log(thread, &sql, "OK"),
        Err(turso::Error::Corrupt(e)) => {
            turso_macros::turso_assert_unreachable!("corrupt error during checkpoint", { "thread": thread, "error": e, "sql": sql });
        }
        Err(e) => sql_logger.log(thread, &sql, &format!("ERROR: {e}")),
    }
}

/// Set the journal mode, retrying on contention. Multiprocess workers run this
/// while sibling processes are already writing to the shared database.
async fn set_journal_mode_with_retry(
    conn: &StressConn,
    mode: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut retry_counter = 0;
    loop {
        match conn.pragma_update("journal_mode", mode).await {
            Ok(_) => return Ok(()),
            Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) if retry_counter < 10 => {
                retry_counter += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn sqlite_integrity_check(
    db_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    turso_macros::turso_assert!(db_path.exists(), "database path must exist", { "path": db_path });
    let conn = rusqlite::Connection::open(db_path)?;
    let mut stmt = conn.prepare_cached("SELECT * FROM pragma_integrity_check;")?;
    let mut rows = stmt.query(())?;
    let mut result: Vec<String> = Vec::new();

    while let Some(row) = rows.next()? {
        result.push(row.get(0)?);
    }
    turso_macros::turso_assert!(
        !result.is_empty(),
        "integrity check result must not be empty"
    );
    if !result[0].eq_ignore_ascii_case("ok") {
        // Build a list of problems
        result.iter_mut().for_each(|row| *row = format!("- {row}"));
        return Err(format!("SQLite integrity check failed: {}", result.join("\n")).into());
    }
    Ok(())
}

#[cfg(not(shuttle))]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let opts = Opts::parse();
    if opts.kill_workers && opts.nr_processes < 2 {
        return Err("--kill-workers requires --nr-processes > 1".into());
    }
    // Mirrors the engine-level guard: MVCC does not support multiprocess
    // access, so fail fast in the coordinator instead of erroring in every
    // worker at the journal_mode pragma.
    if opts.nr_processes > 1 && opts.tx_mode == TxMode::Concurrent {
        return Err(
            "--tx-mode concurrent cannot be combined with --nr-processes > 1: MVCC does not support multiprocess access"
                .into(),
        );
    }
    if opts.nr_processes > 1 && !opts.multiprocess_worker {
        return rt.block_on(Box::pin(multiprocess_main(opts)));
    }
    rt.block_on(Box::pin(async_main(opts)))
}

#[cfg(shuttle)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use shuttle::scheduler::{RandomScheduler, UncontrolledNondeterminismCheckScheduler};

    let config = turso_stress::shuttle_config();

    let mut opts = Opts::parse();
    if opts.nr_processes > 1 || opts.multiprocess_worker || opts.kill_workers {
        return Err("multiprocess mode is not supported under shuttle".into());
    }
    let seed = opts.seed.unwrap_or_else(rand::random);
    opts.seed = Some(seed);
    eprintln!("Using seed: {seed}");

    let scheduler: Box<dyn Scheduler + Send> = if opts.check_uncontrolled_nondeterminism {
        opts.nr_threads = 5;
        opts.nr_iterations = 10;
        Box::new(UncontrolledNondeterminismCheckScheduler::new(
            RandomScheduler::new(1),
        ))
    } else {
        Box::new(RandomScheduler::new_from_seed(seed, 1))
    };

    let runner = shuttle::Runner::new(scheduler, config);
    runner.run(move || shuttle::future::block_on(Box::pin(async_main(opts.clone()))).unwrap());
    Ok(())
}

fn resolve_global_seed(opts: &Opts) -> u64 {
    #[cfg(antithesis)]
    {
        if opts.seed.is_some() {
            eprintln!("Error: --seed is not supported under Antithesis");
            std::process::exit(1);
        }
        println!("Using randomness from Antithesis");
        0 // Antithesis doesn't use seed-based RNG
    }

    #[cfg(not(antithesis))]
    {
        // Under shuttle, opts.seed is already resolved in main()
        let seed = opts.seed.unwrap_or_else(rand::random);
        #[cfg(not(shuttle))]
        println!("Using seed: {seed}");
        seed
    }
}

async fn async_main(opts: Opts) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let global_seed = resolve_global_seed(&opts);

    // Generate schema upfront on main thread with seed. Multiprocess workers
    // instead load the schema the coordinator already created, once they have
    // a connection to the shared database.
    let mut main_rng = ThreadRng::new(global_seed);
    let generated_schema = if opts.multiprocess_worker {
        turso_macros::turso_assert!(
            opts.db_file.is_some(),
            "--multiprocess-worker requires --db-file"
        );
        None
    } else {
        Some(initial_schema(&opts, &mut main_rng)?)
    };

    let mut stop = false;

    let progress_bars = ProgressBars::new(opts.nr_iterations, opts.multiprocess_worker);

    let db_file = resolve_db_file(&opts)?;

    println!("db_file={db_file}");

    // Multiprocess workers share the database file, so give each worker its
    // own log files to avoid processes truncating each other's output.
    let log_prefix = if opts.multiprocess_worker {
        format!("{db_file}.{}", std::process::id())
    } else {
        db_file.clone()
    };

    let sql_log_path = format!("{log_prefix}.sql");
    println!("sql_log={sql_log_path}");
    let sql_logger = Arc::new(SqlLogger::new(&sql_log_path)?);

    let tracing_log_path = format!("{log_prefix}.jsonl");
    println!("tracing_log={tracing_log_path}");
    let _tracer = Tracer::new(&tracing_log_path)?;

    let vfs_option = opts.vfs.clone();

    let db = Arc::new(Mutex::new(StressDb::new(
        db_file.clone(),
        sql_logger.clone(),
        vfs_option.clone(),
        opts.multiprocess_worker,
    )));

    let mut stress_counter = StressCounter::new(opts.nr_threads, opts.nr_iterations);

    let threads: Vec<_> = (0..opts.nr_threads)
        .map(ThreadId::new)
        .enumerate()
        .collect();

    let progress_bars: Vec<_> = threads
        .iter()
        .map(|(_, thread)| progress_bars.add(thread.to_string()))
        .collect();

    let conn = StressDb::connect(&db, ThreadId::new(usize::MAX), opts.busy_timeout).await?;
    let ddl_statements = generated_schema
        .as_ref()
        .map(ArbitrarySchema::to_sql)
        .unwrap_or_default();
    'stmts: for stmt in &ddl_statements {
        let mut retry_counter = 0;
        while retry_counter < 10 {
            match conn.execute(stmt, ()).await {
                Ok(_) => {
                    break;
                }
                Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {
                    retry_counter += 1;
                }
                Err(
                    turso::Error::DatabaseFull(_)
                    | turso::Error::IoError(std::io::ErrorKind::StorageFull, _)
                    | turso::Error::IoError(_, _),
                ) => {
                    stop = true;
                    break 'stmts;
                }
                Err(e) => {
                    turso_macros::turso_assert_unreachable!("fatal error creating table", { "stmt": stmt, "error": e });
                }
            }
        }
        if retry_counter == 10 {
            eprintln!(
                "WARNING: Could not execute statement [{stmt}] after {retry_counter} attempts."
            );
        }
    }

    match opts.tx_mode {
        TxMode::SQLite => {
            set_journal_mode_with_retry(&conn, "WAL").await?;
            conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;
        }
        TxMode::Concurrent => {
            set_journal_mode_with_retry(&conn, "mvcc").await?;
        }
    };

    let schema = match generated_schema {
        Some(schema) => schema,
        // Multiprocess worker: the coordinator already created the schema.
        // Load it through our own turso connection, after the journal mode is
        // set so that MVCC bootstrap has run and the schema is visible. Other
        // worker processes may already be writing, so the file must not be
        // touched with rusqlite here (SQLite does not participate in turso's
        // multiprocess coordination).
        None => load_schema_via_conn(&conn).await?,
    };
    let schema = Arc::new(schema);

    if let (TxMode::Concurrent, Some(threshold)) = (opts.tx_mode, opts.mvcc_checkpoint_threshold) {
        conn.execute(
            &format!("PRAGMA mvcc_checkpoint_threshold = {threshold}"),
            (),
        )
        .await?;
    }

    // Random wal_checkpoint injection: on by default for multiprocess workers
    // so checkpoints contend across processes, off otherwise.
    let checkpoint_ratio = opts
        .checkpoint_ratio
        .unwrap_or(if opts.multiprocess_worker { 100 } else { 0 });

    // Drop the setup connection so that `StressDb::reset` below fully closes
    // the database: a lingering connection keeps the old `Database` alive
    // across the reopen, which defeats the recovery the reset is meant to
    // trigger and makes reopening with multiprocess WAL fail (the coordination
    // registry rejects a second same-process open of the same file).
    drop(conn);

    while !stop && !stress_counter.all_done() {
        let mut handles = Vec::with_capacity(opts.nr_threads);
        let reopen_requested = Arc::new(AtomicBool::new(false));
        let all_threads_ready = Arc::new(Barrier::new(stress_counter.incomplete_threads()));

        for (iteration_idx, ((thread_idx, thread), mut progress_bar)) in threads
            .iter()
            .cloned()
            .zip(progress_bars.iter().cloned())
            .enumerate()
        {
            if stress_counter.done(thread_idx) {
                continue;
            } else if stop {
                break;
            }

            let reopen_requested = reopen_requested.clone();
            let db = db.clone();
            let schema_for_task = schema.clone();
            let sql_logger = sql_logger.clone();
            let all_threads_ready = all_threads_ready.clone();
            let stress_counter = stress_counter.clone();

            let handle = turso_stress::future::spawn(async move {
                let mut conn = StressDb::connect(&db, thread.clone(), opts.busy_timeout).await?;
                let mut rng = ThreadRng::new(
                    global_seed
                        .wrapping_add(thread_idx as u64)
                        .wrapping_add(iteration_idx as u64 * 1000),
                );
                let mut iteration_count_this_batch = 0;

                all_threads_ready.wait().await;
                for interaction_idx in stress_counter.iteration_idx(thread_idx)..opts.nr_iterations
                {
                    if rng.random_ratio(1, 50) {
                        // Reconnect to the database
                        conn = StressDb::connect(&db, thread.clone(), opts.busy_timeout).await?;
                    }

                    let tx = match opts.tx_mode {
                        TxMode::SQLite => (rng.random_ratio(1, 2)).then_some("BEGIN;"),
                        TxMode::Concurrent => {
                            rng.random_ratio(9, 10).then_some("BEGIN CONCURRENT;")
                        }
                    };

                    if let Some(tx_stmt) = tx {
                        let _ = conn.execute(tx_stmt, ()).await;
                    }

                    let sql = generate_random_statement(&mut rng, &schema_for_task);

                    if let Err(turso::Error::Corrupt(e)) = conn.execute(&sql, ()).await {
                        turso_macros::turso_assert_unreachable!("corrupt error executing query", { "thread": thread, "error": e, "sql": sql });
                    }

                    // When inside a transaction, 30% chance to exercise savepoints.
                    // This generates the pattern that exposed the pager rollback bug
                    // in tursodatabase/turso#6176: SAVEPOINT → DML (possibly with
                    // large blobs that allocate new pages) → ROLLBACK TO / RELEASE.
                    if tx.is_some() && rng.random_ratio(3, 10) {
                        let sp_name = format!("sp_{}", rng.random_range(0..100));
                        let savepoint_sql = format!("SAVEPOINT {sp_name};");
                        let _ = conn.execute(&savepoint_sql, ()).await;

                        // Execute 1-3 DML statements inside the savepoint.
                        let sp_stmts = rng.random_range(1..4) as usize;
                        for _ in 0..sp_stmts {
                            let sp_sql = generate_random_statement(&mut rng, &schema_for_task);
                            if let Err(turso::Error::Corrupt(e)) = conn.execute(&sp_sql, ()).await {
                                turso_macros::turso_assert_unreachable!("corrupt error in savepoint", { "thread": thread, "error": e, "sql": sp_sql });
                            }
                        }

                        // 50% ROLLBACK TO (partial undo), 50% RELEASE (keep changes).
                        if rng.random_ratio(1, 2) {
                            let rollback_sql = format!("ROLLBACK TO {sp_name};");
                            let _ = conn.execute(&rollback_sql, ()).await;
                        }
                        let release_sql = format!("RELEASE {sp_name};");
                        let _ = conn.execute(&release_sql, ()).await;
                    }

                    if tx.is_some() {
                        let end_tx = *rng.choose(&["COMMIT;", "ROLLBACK;"]);
                        let _ = conn.execute(end_tx, ()).await;
                    }

                    // Checkpoints from the workload make --kill-workers
                    // SIGKILLs sometimes land mid-checkpoint and, in
                    // multiprocess mode, make checkpoints contend across
                    // processes.
                    if checkpoint_ratio > 0 && rng.random_ratio(1, checkpoint_ratio) {
                        run_random_wal_checkpoint(&conn, &mut rng, &sql_logger, &thread).await;
                    }

                    const INTEGRITY_CHECK_INTERVAL: usize = 100;
                    if interaction_idx % INTEGRITY_CHECK_INTERVAL == 0 {
                        let mut res = conn.query("PRAGMA integrity_check", ()).await.unwrap();
                        match res.next().await {
                            Ok(Some(row))
                                if Value::Text("ok".into()) == row.get_value(0).unwrap() =>
                            {
                                sql_logger.log(&thread, "PRAGMA integrity_check", "OK");
                            }
                            Ok(Some(row)) => {
                                let mut rows = vec![row.get_value(0).unwrap()];
                                while let Some(r) = res.next().await? {
                                    rows.push(r.get_value(0).unwrap());
                                }
                                sql_logger.log(
                                    &thread,
                                    "PRAGMA integrity_check",
                                    &format!("ERROR: {rows:?}"),
                                );
                                turso_macros::turso_assert_unreachable!("integrity check failed", { "thread": thread, "rows": rows });
                            }
                            Ok(None) => {
                                sql_logger.log(&thread, "PRAGMA integrity_check", "ERROR: no rows");
                                turso_macros::turso_assert_unreachable!("integrity check returned no rows", { "thread": thread });
                            }
                            Err(e) => {
                                sql_logger.log(
                                    &thread,
                                    "PRAGMA integrity_check",
                                    &format!("ERROR: {e}"),
                                );
                                turso_macros::turso_assert_unreachable!("Error performing integrity check", { "thread": thread, "error": e });
                            }
                        }
                    }

                    if rng.random_ratio(1, 100 * stress_counter.incomplete_threads() as u32) {
                        reopen_requested.store(true, Ordering::Release);
                    }
                    if reopen_requested.load(Ordering::Acquire) {
                        break;
                    }

                    progress_bar.tick();
                    iteration_count_this_batch += 1;
                    turso_stress::note_progress();
                }

                // In case this thread is running an exclusive transaction, commit it so that it doesn't block other threads.
                let _ = conn.execute("COMMIT", ()).await;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    thread_idx,
                    iteration_count_this_batch,
                ))
            });
            handles.push(handle);
        }

        for handle in handles {
            let (thread_idx, completed_iteration_count) = handle.await??;
            stress_counter.register_iterations(thread_idx, completed_iteration_count);
        }

        // This is what triggers MVCC recovery
        db.lock().await.reset();
        clear_database_registry();
    }

    progress_bars
        .into_iter()
        .for_each(|mut progress_bar| progress_bar.finish());

    println!("Database file: {db_file}");

    // Multiprocess workers must not run the rusqlite-based integrity check:
    // sibling workers may still have the database open, and SQLite does not
    // participate in turso's multiprocess coordination. The coordinator runs
    // the check after every worker has exited.
    if !opts.multiprocess_worker {
        finalize_and_integrity_check(&db_file, opts.tx_mode, vfs_option.as_ref()).await?;
    }

    Ok(())
}

/// Switch an MVCC-mode database back to WAL (SQLite/rusqlite doesn't understand
/// the MVCC journal mode) and run the final SQLite integrity check.
async fn finalize_and_integrity_check(
    db_file: &str,
    tx_mode: TxMode,
    vfs: Option<&String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if tx_mode == TxMode::Concurrent {
        let mut builder = Builder::new_local(db_file);
        if let Some(vfs) = vfs {
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
        sqlite_integrity_check(std::path::Path::new(db_file))?;
    }

    Ok(())
}

/// How often the coordinator reaps finished workers and rolls the kill dice.
#[cfg(not(shuttle))]
const WORKER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
/// 1-in-N chance per poll tick to SIGKILL a random worker with --kill-workers
/// (a kill roughly every KILL_RATIO_PER_TICK * WORKER_POLL_INTERVAL).
#[cfg(not(shuttle))]
const KILL_RATIO_PER_TICK: u32 = 10;
/// Kills per run are capped at this multiple of --nr-processes, bounding total
/// runtime: a killed worker's replacement restarts from iteration zero.
#[cfg(not(shuttle))]
const MAX_KILLS_PER_PROCESS: usize = 3;

/// Spawns multiprocess workers: copies of this binary pointed at the shared
/// database file. Every spawn (including respawns after kills) gets a distinct
/// seed via an internal nonce.
///
/// Any option that affects worker behavior must be forwarded here; workers
/// otherwise run with defaults.
#[cfg(not(shuttle))]
struct WorkerSpawner<'a> {
    opts: &'a Opts,
    exe: std::path::PathBuf,
    db_file: &'a str,
    global_seed: u64,
    spawn_nonce: u64,
}

#[cfg(not(shuttle))]
impl WorkerSpawner<'_> {
    fn spawn(
        &mut self,
        process_idx: usize,
    ) -> Result<std::process::Child, Box<dyn std::error::Error + Send + Sync>> {
        let mut cmd = std::process::Command::new(&self.exe);
        cmd.arg("--multiprocess-worker")
            .arg("--db-file")
            .arg(self.db_file)
            .arg("--tx-mode")
            .arg(self.opts.tx_mode.to_string())
            .arg("--nr-threads")
            .arg(self.opts.nr_threads.to_string())
            .arg("--nr-iterations")
            .arg(self.opts.nr_iterations.to_string())
            .arg("--busy-timeout")
            .arg(self.opts.busy_timeout.to_string());
        if let Some(ref vfs) = self.opts.vfs {
            cmd.arg("--vfs").arg(vfs);
        }
        if let Some(ratio) = self.opts.checkpoint_ratio {
            cmd.arg("--checkpoint-ratio").arg(ratio.to_string());
        }
        if let Some(threshold) = self.opts.mvcc_checkpoint_threshold {
            cmd.arg("--mvcc-checkpoint-threshold")
                .arg(threshold.to_string());
        }
        // Under Antithesis, workers draw randomness from the Antithesis SDK
        // and reject --seed.
        if cfg!(not(antithesis)) {
            let worker_seed = self
                .global_seed
                .wrapping_add((self.spawn_nonce + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            cmd.arg("--seed").arg(worker_seed.to_string());
        }
        self.spawn_nonce += 1;
        let child = cmd.spawn()?;
        println!("spawned worker {process_idx} (pid {})", child.id());
        Ok(child)
    }
}

/// After SIGKILLing a worker mid-workload, assert from a surviving process
/// that the database is still usable: the dead process may have died holding
/// the WAL writer lock, the checkpoint lock, or reader/tx slots in the shared
/// coordination file, and none of those may wedge the survivors or corrupt
/// the on-disk state.
#[cfg(not(shuttle))]
async fn post_kill_probe(
    conn: &turso::Connection,
    rng: &mut ThreadRng,
    schema: &ArbitrarySchema,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let table = rng.choose(&schema.tables).clone();
    let sql = generate_insert(rng, &table);
    let mut wrote = false;
    for _ in 0..20 {
        match conn.execute(&sql, ()).await {
            // A constraint violation still proves the write path is live: the
            // insert acquired the write locks and reached constraint checking.
            Ok(_) | Err(turso::Error::Constraint(_)) => {
                wrote = true;
                break;
            }
            Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    turso_macros::turso_assert!(
        wrote,
        "a surviving process must be able to write after a worker is SIGKILLed",
        { "sql": sql }
    );

    let mut rows = conn.query("PRAGMA integrity_check", ()).await?;
    let mut results = Vec::new();
    while let Some(row) = rows.next().await? {
        results.push(row.get_value(0)?);
    }
    turso_macros::turso_assert!(
        results == vec![Value::Text("ok".into())],
        "integrity check must pass from a surviving process after a worker is SIGKILLed",
        { "results": results }
    );
    Ok(())
}

/// Coordinator for multiprocess stress: bootstraps the schema, spawns
/// `--nr-processes` copies of this binary as workers that all open the same
/// database file with multiprocess WAL enabled, waits for them, and runs the
/// final integrity check once no process has the database open anymore.
#[cfg(not(shuttle))]
async fn multiprocess_main(opts: Opts) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if cfg!(miri) {
        return Err("multiprocess mode is not supported under miri".into());
    }
    if opts.check_uncontrolled_nondeterminism {
        return Err(
            "--check-uncontrolled-nondeterminism is not supported in multiprocess mode".into(),
        );
    }
    if let Some(ref vfs) = opts.vfs {
        if vfs.starts_with("memory") {
            return Err(
                format!("multiprocess mode requires a file-backed VFS, got '{vfs}'").into(),
            );
        }
    }

    let global_seed = resolve_global_seed(&opts);

    let mut main_rng = ThreadRng::new(global_seed);
    let schema = initial_schema(&opts, &mut main_rng)?;
    let db_file = resolve_db_file(&opts)?;
    println!("db_file={db_file}");

    // Bootstrap the schema before any worker starts, and keep this database
    // open until every worker has exited: the coordinator creates the shared
    // WAL coordination file, so workers only ever attach to an existing valid
    // one and never race on creating it themselves.
    let mut builder = Builder::new_local(&db_file).experimental_multiprocess_wal(true);
    if let Some(ref vfs) = opts.vfs {
        builder = builder.with_io(vfs.clone());
    }
    let bootstrap_db = builder.build().await?;
    let bootstrap_conn = bootstrap_db.connect()?;
    for stmt in schema.to_sql() {
        bootstrap_conn.execute(&stmt, ()).await?;
    }
    match opts.tx_mode {
        TxMode::SQLite => {
            bootstrap_conn.pragma_update("journal_mode", "WAL").await?;
        }
        TxMode::Concurrent => {
            bootstrap_conn.pragma_update("journal_mode", "mvcc").await?;
        }
    }

    let mut spawner = WorkerSpawner {
        opts: &opts,
        exe: std::env::current_exe()?,
        db_file: &db_file,
        global_seed,
        spawn_nonce: 0,
    };
    let mut workers: Vec<Option<std::process::Child>> = Vec::with_capacity(opts.nr_processes);
    for process_idx in 0..opts.nr_processes {
        workers.push(Some(spawner.spawn(process_idx)?));
    }

    // With --kill-workers, SIGKILL a random worker every so often and respawn
    // it. The kill is deliberately not graceful: the dead process may hold the
    // WAL writer lock, the checkpoint lock, or reader/tx slots in the shared
    // coordination file, all of which the survivors must recover from. Kill
    // timing draws from the harness RNG, so under Antithesis the fuzzer can
    // steer when in the workload the kill lands.
    let max_kills = if opts.kill_workers {
        opts.nr_processes * MAX_KILLS_PER_PROCESS
    } else {
        0
    };
    let mut kills_done = 0usize;
    let mut failures: Vec<usize> = Vec::new();

    loop {
        let mut all_done = true;
        for (process_idx, slot) in workers.iter_mut().enumerate() {
            let Some(child) = slot else { continue };
            match child.try_wait()? {
                Some(status) if status.success() => {
                    println!("worker {process_idx} (pid {}) finished", child.id());
                    *slot = None;
                }
                Some(status) => {
                    eprintln!("worker {process_idx} (pid {}) failed: {status}", child.id());
                    failures.push(process_idx);
                    *slot = None;
                }
                None => all_done = false,
            }
        }
        if all_done || !failures.is_empty() {
            break;
        }

        if kills_done < max_kills && main_rng.random_ratio(1, KILL_RATIO_PER_TICK) {
            let live: Vec<usize> = workers
                .iter()
                .enumerate()
                .filter_map(|(idx, child)| child.is_some().then_some(idx))
                .collect();
            if !live.is_empty() {
                let victim = *main_rng.choose(&live);
                let child = workers[victim].as_mut().expect("victim must be live");
                let pid = child.id();
                child.kill()?;
                let _ = child.wait()?;
                kills_done += 1;
                println!("killed worker {victim} (pid {pid}), kill {kills_done}/{max_kills}");

                post_kill_probe(&bootstrap_conn, &mut main_rng, &schema).await?;

                workers[victim] = Some(spawner.spawn(victim)?);
            }
        }

        tokio::time::sleep(WORKER_POLL_INTERVAL).await;
    }

    if !failures.is_empty() {
        for child in workers.iter_mut().flatten() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    turso_macros::turso_assert!(
        failures.is_empty(),
        "all multiprocess workers must exit cleanly",
        { "failed_workers": failures }
    );
    if opts.kill_workers {
        println!("killed and respawned {kills_done} workers during the run");
    }

    // Release the coordinator's handle before the integrity check: rusqlite
    // must only touch the file once no turso process has it open.
    drop(bootstrap_conn);
    drop(bootstrap_db);
    clear_database_registry();

    println!("Database file: {db_file}");
    finalize_and_integrity_check(&db_file, opts.tx_mode, opts.vfs.as_ref()).await
}
