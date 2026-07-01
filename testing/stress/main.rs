use rand::Rng;
mod conn;
mod counter;
mod logging;
mod opts;
mod progress;
mod sql_logging;

use clap::Parser;
use opts::{Opts, TxMode};
use rand::distr::uniform::{SampleRange, SampleUniform};
#[cfg(not(antithesis))]
use rand::rngs::StdRng;
#[cfg(not(antithesis))]
use rand::SeedableRng;
#[cfg(shuttle)]
use shuttle::scheduler::Scheduler;
use std::collections::HashSet;
use std::path::Path;
use turso_stress::sync::atomic::{AtomicBool, Ordering};
use turso_stress::sync::Arc;
use turso_stress::sync::AsyncBarrier as Barrier;
use turso_stress::sync::AsyncMutex as Mutex;

use crate::conn::StressDb;
use crate::counter::StressCounter;
use crate::logging::Tracer;
use crate::progress::ProgressBars;
use crate::sql_logging::SqlLogger;
use turso::core::clear_database_registry;
use turso::Builder;
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

/// RNG wrapper that works with both Shuttle and Antithesis
#[cfg(not(antithesis))]
struct ThreadRng {
    rng: StdRng,
}

#[cfg(not(antithesis))]
impl ThreadRng {
    fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }

    fn get_random(&mut self) -> u64 {
        self.rng.random()
    }

    fn random_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.rng.random_range(range)
    }

    fn true_with_probability(&mut self, numerator: u32, denominator: u32) -> bool {
        self.rng.random_ratio(numerator, denominator)
    }

    fn choose<'a, T>(&mut self, ts: &'a [T]) -> &'a T {
        &ts[self.rng.random_range(0..ts.len())]
    }
}

#[cfg(antithesis)]
struct ThreadRng;

#[cfg(antithesis)]
impl ThreadRng {
    fn new(_seed: u64) -> Self {
        // Antithesis uses its own RNG, seed is ignored
        Self
    }

    fn get_random(&mut self) -> u64 {
        antithesis_sdk::random::get_random()
    }

    fn random_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        antithesis_sdk::random::AntithesisRng.random_range::<T, R>(range)
    }

    fn true_with_probability(&mut self, numerator: u32, denominator: u32) -> bool {
        antithesis_sdk::random::AntithesisRng.random_ratio(numerator, denominator)
    }

    fn choose<'a, T>(&mut self, ts: &'a [T]) -> &'a T {
        antithesis_sdk::random::random_choice(ts).expect("choose called on empty slice")
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

fn gen_bool(rng: &mut ThreadRng, probability_true: f64) -> bool {
    (rng.get_random() as f64 / u64::MAX as f64) < probability_true
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
            if rng.true_with_probability(1, 5) {
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
                && rng.true_with_probability(1, 2)
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

    let where_clause = if !table.pk_values.is_empty() && rng.true_with_probability(1, 2) {
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

    let where_clause = if !table.pk_values.is_empty() && rng.true_with_probability(1, 2) {
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
    rt.block_on(Box::pin(async_main(opts)))
}

#[cfg(shuttle)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use shuttle::scheduler::{RandomScheduler, UncontrolledNondeterminismCheckScheduler};

    let config = turso_stress::shuttle_config();

    let mut opts = Opts::parse();
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

async fn async_main(opts: Opts) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(antithesis)]
    let global_seed: u64 = {
        if opts.seed.is_some() {
            eprintln!("Error: --seed is not supported under Antithesis");
            std::process::exit(1);
        }
        println!("Using randomness from Antithesis");
        0 // Antithesis doesn't use seed-based RNG
    };

    #[cfg(not(antithesis))]
    let global_seed: u64 = {
        // Under shuttle, opts.seed is already resolved in main()
        let seed = opts.seed.unwrap_or_else(rand::random);
        #[cfg(not(shuttle))]
        println!("Using seed: {seed}");
        seed
    };

    // Generate schema upfront on main thread with seed
    let mut main_rng = ThreadRng::new(global_seed);
    let schema = if let Some(ref db_ref) = opts.db_ref {
        load_schema(db_ref)?
    } else {
        gen_schema(&mut main_rng, opts.tables)
    };

    let ddl_statements = schema.to_sql();
    let schema = Arc::new(schema);

    let mut stop = false;

    let progress_bars = ProgressBars::new(opts.nr_iterations);

    let tempfile = tempfile::NamedTempFile::new()?;
    let (_, path) = tempfile.keep().unwrap();
    let db_file = if let Some(db_file) = opts.db_file.clone() {
        db_file
    } else {
        if let Some(ref db_ref) = opts.db_ref {
            std::fs::copy(db_ref, &path)?;
        }
        path.to_string_lossy().to_string()
    };

    println!("db_file={db_file}");

    let sql_log_path = format!("{db_file}.sql");
    println!("sql_log={sql_log_path}");
    let sql_logger = Arc::new(SqlLogger::new(&sql_log_path)?);

    let tracing_log_path = format!("{db_file}.jsonl");
    println!("tracing_log={tracing_log_path}");
    let _tracer = Tracer::new(&tracing_log_path)?;

    let vfs_option = opts.vfs.clone();

    let db = Arc::new(Mutex::new(StressDb::new(
        db_file.clone(),
        sql_logger.clone(),
        vfs_option.clone(),
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
            conn.pragma_update("journal_mode", "WAL").await?;
            conn.busy_timeout(std::time::Duration::from_millis(opts.busy_timeout))?;
        }
        TxMode::Concurrent => {
            conn.pragma_update("journal_mode", "mvcc").await?;
        }
    };

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
                    if gen_bool(&mut rng, 0.02) {
                        // Reconnect to the database
                        conn = StressDb::connect(&db, thread.clone(), opts.busy_timeout).await?;
                    }

                    let tx = match opts.tx_mode {
                        TxMode::SQLite => (rng.true_with_probability(1, 2)).then_some("BEGIN;"),
                        TxMode::Concurrent => rng
                            .true_with_probability(9, 10)
                            .then_some("BEGIN CONCURRENT;"),
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
                    if tx.is_some() && rng.true_with_probability(3, 10) {
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
                        if rng.true_with_probability(1, 2) {
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

                    const INTEGRITY_CHECK_INTERVAL: usize = 100;
                    if interaction_idx % INTEGRITY_CHECK_INTERVAL == 0 {
                        let mut res = conn.query("PRAGMA integrity_check", ()).await.unwrap();
                        match res.next().await {
                            Ok(Some(row)) => {
                                let value = row.get_value(0).unwrap();
                                if value != "ok".into() {
                                    sql_logger.log(
                                        &thread,
                                        "PRAGMA integrity_check",
                                        &format!("ERROR: {value:?}"),
                                    );
                                    turso_macros::turso_assert_unreachable!("integrity check failed", { "thread": thread, "value": value });
                                }
                                sql_logger.log(&thread, "PRAGMA integrity_check", "OK");
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
                                println!("thread#{thread} Error performing integrity check: {e}");
                            }
                        }
                        match res.next().await {
                            Ok(Some(_)) => {
                                turso_macros::turso_assert_unreachable!("integrity check returned more than 1 row", { "thread": thread });
                            }
                            Err(e) => {
                                println!("thread#{thread} Error performing integrity check: {e}")
                            }
                            _ => {}
                        }
                    }

                    if gen_bool(&mut rng, 0.01 / stress_counter.incomplete_threads() as f64) {
                        reopen_requested.store(true, Ordering::Release);
                    }
                    if reopen_requested.load(Ordering::Acquire) {
                        break;
                    }

                    progress_bar.tick();
                    iteration_count_this_batch += 1;
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
