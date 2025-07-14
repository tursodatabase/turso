mod opts;

use anarchist_readable_name_generator_lib::readable_name_custom;
use antithesis_sdk::random::{get_random, AntithesisRng};
use antithesis_sdk::*;
use clap::Parser;
use core::panic;
use opts::Opts;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use turso::Builder;

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
    #[cfg(feature = "experimental_indexes")]
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

// Helper functions for generating random data
fn generate_random_identifier() -> String {
    readable_name_custom("_", AntithesisRng).replace('-', "_")
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
    #[cfg(feature = "experimental_indexes")]
    match get_random() % 2 {
        0 => Constraint::NotNull,
        _ => Constraint::Unique,
    }
    #[cfg(not(feature = "experimental_indexes"))]
    Constraint::NotNull
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

    #[cfg(feature = "experimental_indexes")]
    {
        // Then, randomly select one column to be the primary key
        let pk_index = (get_random() % column_count as u64) as usize;
        columns[pk_index].constraints.push(Constraint::PrimaryKey);
        Table { name, columns }
    }
    #[cfg(not(feature = "experimental_indexes"))]
    {
        // Pick a random column that is exactly INTEGER type to be the primary key (INTEGER PRIMARY KEY does not require indexes,
        // as it becomes an alias for the ROWID).
        let pk_candidates = columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.data_type == DataType::Integer)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if pk_candidates.is_empty() {
            // if there are no INTEGER columns, make a random column INTEGER and set it as PRIMARY KEY
            let col_id = (get_random() % column_count as u64) as usize;
            columns[col_id].data_type = DataType::Integer;
            columns[col_id].constraints.push(Constraint::PrimaryKey);
            return Table { name, columns };
        }
        let pk_index = pk_candidates
            .get((get_random() % pk_candidates.len() as u64) as usize)
            .unwrap();
        columns[*pk_index].constraints.push(Constraint::PrimaryKey);

        Table { name, columns }
    }
}

pub fn gen_bool(probability_true: f64) -> bool {
    (get_random() as f64 / u64::MAX as f64) < probability_true
}

pub fn gen_schema() -> ArbitrarySchema {
    let table_count = (get_random() % 10 + 1) as usize;
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

                format!("CREATE TABLE {} ({});", table.name, columns)
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
        #[cfg(feature = "experimental_indexes")]
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
    let schema = gen_schema();
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
    for _ in 0..opts.nr_threads {
        let mut queries = vec![];
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
                queries.push(tx.to_string());
            }
            let sql = generate_random_statement(&schema);
            if !opts.skip_log {
                writeln!(log_file, "{sql}")?;
            }
            queries.push(sql);
            if tx.is_some() {
                if get_random() % 2 == 0 {
                    queries.push("COMMIT".to_string());
                } else {
                    queries.push("ROLLBACK".to_string());
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
    for _ in 0..plan.nr_threads {
        let mut queries = vec![];
        for _ in 0..plan.nr_iterations {
            queries.push(
                lines
                    .next()
                    .expect("missing query for thread {}")
                    .to_string(),
            );
        }
        plan.queries_per_thread.push(queries);
    }
    Ok(plan)
}

pub fn init_tracing() -> Result<WorkerGuard, std::io::Error> {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
    if let Err(e) = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_line_number(true)
                .with_thread_ids(true),
        )
        .with(EnvFilter::from_default_env())
        .try_init()
    {
        println!("Unable to setup tracing appender: {e:?}");
    }
    Ok(guard)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _g = init_tracing()?;
    antithesis_init();

    let opts = Opts::parse();
    if opts.nr_threads > 1 {
        println!("ERROR: Multi-threaded data access is not yet supported: https://github.com/tursodatabase/turso/issues/1552");
        return Ok(());
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
    let db_file = if let Some(db_file) = opts.db_file {
        db_file
    } else {
        tempfile.path().to_string_lossy().to_string()
    };

    for thread in 0..opts.nr_threads {
        let db_file = db_file.clone();
        let db = Arc::new(Mutex::new(Builder::new_local(&db_file).build().await?));
        let plan = plan.clone();
        let conn = db.lock().await.connect()?;

        // Apply each DDL statement individually
        for stmt in &plan.ddl_statements {
            if opts.verbose {
                println!("executing ddl {stmt}");
            }
            if let Err(e) = conn.execute(stmt, ()).await {
                match e {
                    turso::Error::SqlExecutionFailure(e) => {
                        if e.contains("Corrupt database") {
                            panic!("Error creating table: {}", e);
                        } else {
                            println!("Error creating table: {e}");
                        }
                    }
                    _ => panic!("Error creating table: {}", e),
                }
            }
        }

        let nr_iterations = opts.nr_iterations;
        let db = db.clone();

        let handle = tokio::spawn(async move {
            let mut conn = db.lock().await.connect()?;
            println!("\rExecuting queries...");
            for query_index in 0..nr_iterations {
                if gen_bool(0.001) {
                    if opts.verbose {
                        println!("Reopening database");
                    }
                    // Reopen the database
                    let mut db_guard = db.lock().await;
                    *db_guard = Builder::new_local(&db_file).build().await?;
                    conn = db_guard.connect()?;
                } else if gen_bool(0.01) {
                    // Reconnect to the database
                    if opts.verbose {
                        println!("Reconnecting to database");
                    }
                    let db_guard = db.lock().await;
                    conn = db_guard.connect()?;
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
                        turso::Error::SqlExecutionFailure(e) => {
                            if e.contains("Corrupt database") {
                                panic!("Error executing query: {}", e);
                            } else if e.contains("UNIQUE constraint failed") {
                                if opts.verbose {
                                    println!("Skipping UNIQUE constraint violation: {e}");
                                }
                            } else if opts.verbose {
                                println!("Error executing query: {e}");
                            }
                        }
                        _ => panic!("Error executing query: {}", e),
                    }
                }
                const INTEGRITY_CHECK_INTERVAL: usize = 100;
                if query_index % INTEGRITY_CHECK_INTERVAL == 0 {
                    let mut res = conn.query("PRAGMA integrity_check", ()).await.unwrap();
                    if let Some(row) = res.next().await? {
                        let value = row.get_value(0).unwrap();
                        if value != "ok".into() {
                            panic!("integrity check failed: {:?}", value);
                        }
                    } else {
                        panic!("integrity check failed: no rows");
                    }
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }
    println!("Done. SQL statements written to {}", opts.log_file);
    println!("Database file: {db_file}");
    Ok(())
}
