use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde_json::{json, Value as JsonValue};
use turso_core::{
    Connection, Database, Numeric, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, StepResult,
    Value,
};

#[derive(Parser)]
#[command(name = "turso-join-benchmark")]
#[command(about = "Measure join query execution without printing result rows")]
struct Args {
    /// Database file that contains the benchmark data.
    #[arg(long)]
    database: PathBuf,

    /// Directory that contains one SQL query per file.
    #[arg(long)]
    query_dir: PathBuf,

    /// Run only queries whose file stem contains this text.
    #[arg(long)]
    filter: Option<String>,

    /// Run a query with this exact file stem. This option can occur more than once.
    #[arg(long = "query")]
    query_names: Vec<String>,

    /// Number of unmeasured executions before each measured query.
    #[arg(long, default_value_t = 1)]
    warmups: usize,

    /// Number of measured executions for each query.
    #[arg(long, default_value_t = 5)]
    repetitions: usize,

    /// Stop an execution after this many seconds. Zero disables the timeout.
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,

    /// Print each query plan as one JSON record. Do not execute the query.
    #[arg(long)]
    plans: bool,

    /// With --plans, also print the bytecode of each query.
    #[arg(long)]
    bytecode: bool,

    /// Print every result row as one JSON record. Do not report timings.
    #[arg(long)]
    print_rows: bool,

    /// Open the database for writing. A query file that creates a view needs this.
    #[arg(long)]
    writable: bool,

    /// Print the cost parameters this process loaded, then stop.
    #[arg(long)]
    print_params: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.print_params {
        return print_params();
    }

    let queries = load_queries(&args.query_dir, args.filter.as_deref(), &args.query_names)?;
    if queries.is_empty() {
        bail!("no SQL files matched the query selection");
    }

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new()?);
    let flags = if args.writable {
        OpenFlags::default()
    } else {
        OpenFlags::ReadOnly
    };
    let database = Database::open(
        io,
        &args.database.to_string_lossy(),
        OpenOptions::new(Arc::new(SqliteDialect)).flags(flags),
    )?;
    let connection = database.connect()?;

    for query in queries {
        if args.plans {
            print_plan(&connection, &query, args.bytecode, args.timeout_seconds)?;
            continue;
        }
        if args.print_rows {
            print_rows(&database, &connection, &query, args.timeout_seconds)?;
            continue;
        }
        measure(&database, &connection, &query, &args)?;
    }

    Ok(())
}

#[cfg(feature = "optimizer_params")]
fn print_params() -> Result<()> {
    let params: &turso_core::CostModelParams = &turso_core::LOADED_PARAMS;
    println!("{}", serde_json::to_string(params)?);
    Ok(())
}

#[cfg(not(feature = "optimizer_params"))]
fn print_params() -> Result<()> {
    bail!("--print-params needs the optimizer_params feature");
}

/// A query file, split into the statements the parser found in it.
///
/// A file with more than one statement cannot prepare its later statements
/// before its earlier ones have run, so such a file is measured as a whole
/// sequence and the measurement includes preparation.
struct Query {
    name: String,
    statements: Vec<QueryStatement>,
}

/// One statement of a query file.
struct QueryStatement {
    sql: String,
    /// A `SELECT` returns rows and has a plan. Every other statement is setup
    /// or cleanup: the plan mode runs it so the statements after it can plan.
    is_select: bool,
}

impl Query {
    fn is_single_statement(&self) -> bool {
        self.statements.len() == 1
    }
}

fn load_queries(
    query_dir: &Path,
    filter: Option<&str>,
    query_names: &[String],
) -> Result<Vec<Query>> {
    let mut paths = fs::read_dir(query_dir)
        .with_context(|| format!("read query directory {}", query_dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();

    let mut queries = Vec::new();
    for path in paths {
        if path.extension().and_then(|value| value.to_str()) != Some("sql") {
            continue;
        }
        let name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("query file name is not UTF-8")?
            .to_string();
        if filter.is_some_and(|filter| !name.contains(filter)) {
            continue;
        }
        if !query_names.is_empty() && !query_names.contains(&name) {
            continue;
        }
        let sql = fs::read_to_string(&path)
            .with_context(|| format!("read query file {}", path.display()))?;
        let statements = split_statements(&sql)
            .with_context(|| format!("split query file {}", path.display()))?;
        if statements.is_empty() {
            bail!("query file {} holds no statement", path.display());
        }
        queries.push(Query { name, statements });
    }
    Ok(queries)
}

/// Split SQL into its statements with the engine's own parser.
///
/// The parser knows about comments, string literals and quoted names, so this
/// keeps a statement that contains a semicolon in one piece.
fn split_statements(sql: &str) -> Result<Vec<QueryStatement>> {
    let bytes = sql.as_bytes();
    let mut parser = turso_parser::parser::Parser::new(bytes);
    let mut statements = Vec::new();
    let mut start = 0;
    loop {
        match parser.next_cmd() {
            Ok(Some(cmd)) => {
                let end = parser.offset();
                let text = sql[start..end].trim();
                if !text.is_empty() {
                    statements.push(QueryStatement {
                        sql: text.to_string(),
                        is_select: matches!(
                            cmd,
                            turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::Select(_))
                        ),
                    });
                }
                start = end;
            }
            Ok(None) => break,
            Err(err) => bail!("parse error: {err}"),
        }
    }
    Ok(statements)
}

fn set_timeout(statement: &mut turso_core::Statement, timeout_seconds: u64) {
    let timeout = (timeout_seconds != 0).then(|| Duration::from_secs(timeout_seconds));
    statement.set_query_timeout_override(Some(timeout));
}

fn measure(
    database: &Database,
    connection: &Arc<Connection>,
    query: &Query,
    args: &Args,
) -> Result<()> {
    if query.is_single_statement() {
        return measure_one_statement(database, connection, query, args);
    }
    measure_statement_sequence(database, connection, query, args)
}

/// Measure a query file that holds one statement.
///
/// The statement is prepared once, outside the measured interval, so the
/// reported time is execution time alone.
fn measure_one_statement(
    database: &Database,
    connection: &Arc<Connection>,
    query: &Query,
    args: &Args,
) -> Result<()> {
    let sql = &query.statements[0].sql;
    let mut statement = connection
        .prepare(sql)
        .with_context(|| format!("prepare query {}", query.name))?;
    set_timeout(&mut statement, args.timeout_seconds);

    for _ in 0..args.warmups {
        execute(database, &mut statement)?;
        statement.reset()?;
    }

    for repetition in 0..args.repetitions {
        statement.reset_metrics();
        let start = Instant::now();
        let result_rows = execute(database, &mut statement)?;
        let elapsed = start.elapsed();
        let metrics = statement.metrics();
        println!(
            "{}",
            json!({
                "query": query.name,
                "repetition": repetition,
                "elapsed_ns": elapsed.as_nanos().min(u64::MAX as u128) as u64,
                "includes_preparation": false,
                "result_rows": result_rows,
                "rows_read": metrics.rows_read,
                "vm_steps": metrics.vm_steps,
                "instructions": metrics.insn_executed,
                "fullscan_steps": metrics.fullscan_steps,
                "index_steps": metrics.index_steps,
                "btree_seeks": metrics.btree_seeks,
                "btree_table_seeks": metrics.btree_table_seeks,
                "btree_index_seeks": metrics.btree_index_seeks,
                "btree_deferred_seeks": metrics.btree_deferred_seeks,
                "btree_next": metrics.btree_next,
                "btree_prev": metrics.btree_prev,
                "sort_operations": metrics.sort_operations,
                "hash_spill_bytes": metrics.hash_join.spill_bytes_written,
                "hash_load_bytes": metrics.hash_join.load_bytes_read,
                "hash_probe_calls": metrics.hash_join.probe_calls,
            })
        );
        statement.reset()?;
    }
    Ok(())
}

/// Measure a query file that holds more than one statement.
///
/// Every statement runs in file order and the measured interval covers the
/// whole sequence, which is what the shell harness times for such a file.
fn measure_statement_sequence(
    database: &Database,
    connection: &Arc<Connection>,
    query: &Query,
    args: &Args,
) -> Result<()> {
    for _ in 0..args.warmups {
        run_sequence(database, connection, query, args.timeout_seconds)?;
    }

    for repetition in 0..args.repetitions {
        let start = Instant::now();
        let (result_rows, metrics) =
            run_sequence(database, connection, query, args.timeout_seconds)?;
        let elapsed = start.elapsed();
        println!(
            "{}",
            json!({
                "query": query.name,
                "repetition": repetition,
                "elapsed_ns": elapsed.as_nanos().min(u64::MAX as u128) as u64,
                "includes_preparation": true,
                "statement_count": query.statements.len(),
                "result_rows": result_rows,
                "rows_read": metrics.rows_read,
                "vm_steps": metrics.vm_steps,
                "instructions": metrics.insn_executed,
                "fullscan_steps": metrics.fullscan_steps,
                "index_steps": metrics.index_steps,
                "btree_seeks": metrics.btree_seeks,
                "btree_table_seeks": metrics.btree_table_seeks,
                "btree_index_seeks": metrics.btree_index_seeks,
                "btree_deferred_seeks": metrics.btree_deferred_seeks,
                "btree_next": metrics.btree_next,
                "btree_prev": metrics.btree_prev,
                "sort_operations": metrics.sort_operations,
                "hash_spill_bytes": metrics.hash_join.spill_bytes_written,
                "hash_load_bytes": metrics.hash_join.load_bytes_read,
                "hash_probe_calls": metrics.hash_join.probe_calls,
            })
        );
    }
    Ok(())
}

#[derive(Default)]
struct SequenceMetrics {
    rows_read: u64,
    vm_steps: u64,
    insn_executed: u64,
    fullscan_steps: u64,
    index_steps: u64,
    btree_seeks: u64,
    btree_table_seeks: u64,
    btree_index_seeks: u64,
    btree_deferred_seeks: u64,
    btree_next: u64,
    btree_prev: u64,
    sort_operations: u64,
    hash_join: HashJoinMetrics,
}

#[derive(Default)]
struct HashJoinMetrics {
    spill_bytes_written: u64,
    load_bytes_read: u64,
    probe_calls: u64,
}

fn run_sequence(
    database: &Database,
    connection: &Arc<Connection>,
    query: &Query,
    timeout_seconds: u64,
) -> Result<(u64, SequenceMetrics)> {
    let mut result_rows = 0_u64;
    let mut totals = SequenceMetrics::default();
    for statement_text in &query.statements {
        let mut statement = connection
            .prepare(&statement_text.sql)
            .with_context(|| format!("prepare statement of query {}", query.name))?;
        set_timeout(&mut statement, timeout_seconds);
        result_rows = result_rows.saturating_add(execute(database, &mut statement)?);
        let metrics = statement.metrics();
        totals.rows_read += metrics.rows_read;
        totals.vm_steps += metrics.vm_steps;
        totals.insn_executed += metrics.insn_executed;
        totals.fullscan_steps += metrics.fullscan_steps;
        totals.index_steps += metrics.index_steps;
        totals.btree_seeks += metrics.btree_seeks;
        totals.btree_table_seeks += metrics.btree_table_seeks;
        totals.btree_index_seeks += metrics.btree_index_seeks;
        totals.btree_deferred_seeks += metrics.btree_deferred_seeks;
        totals.btree_next += metrics.btree_next;
        totals.btree_prev += metrics.btree_prev;
        totals.sort_operations += metrics.sort_operations;
        totals.hash_join.spill_bytes_written += metrics.hash_join.spill_bytes_written;
        totals.hash_join.load_bytes_read += metrics.hash_join.load_bytes_read;
        totals.hash_join.probe_calls += metrics.hash_join.probe_calls;
    }
    Ok((result_rows, totals))
}

fn execute(database: &Database, statement: &mut turso_core::Statement) -> Result<u64> {
    let mut result_rows = 0_u64;
    loop {
        match statement.step()? {
            StepResult::Row => result_rows = result_rows.saturating_add(1),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => database.io.step()?,
            StepResult::Done => return Ok(result_rows),
            StepResult::Interrupt => bail!("query was interrupted"),
            StepResult::Busy => bail!("database was busy"),
        }
    }
}

/// Print the plan of every statement in a query file that has one.
///
/// A statement without a plan, such as `CREATE VIEW`, runs instead, so that the
/// statements after it can be planned against the schema it makes.
fn print_plan(
    connection: &Arc<Connection>,
    query: &Query,
    with_bytecode: bool,
    timeout_seconds: u64,
) -> Result<()> {
    for (position, statement_text) in query.statements.iter().enumerate() {
        let sql = &statement_text.sql;
        if !statement_text.is_select {
            let mut statement = connection.prepare(sql)?;
            set_timeout(&mut statement, timeout_seconds);
            statement.run_collect_rows()?;
            continue;
        }
        let explained = format!("EXPLAIN QUERY PLAN FORMAT=JSON {sql}");
        let rows = connection
            .prepare(&explained)
            .and_then(|mut statement| statement.run_collect_rows())
            .with_context(|| format!("plan statement {position} of query {}", query.name))?;
        let Some(Value::Text(plan)) = rows.first().and_then(|row| row.first()) else {
            bail!("query {} did not return a JSON plan", query.name);
        };
        let plan: JsonValue = serde_json::from_str(plan.as_str())?;
        let bytecode = if with_bytecode {
            json!(collect_bytecode(connection, sql)?)
        } else {
            JsonValue::Null
        };
        println!(
            "{}",
            json!({
                "query": query.name,
                "statement": position,
                "statement_count": query.statements.len(),
                "plan": plan,
                "bytecode": bytecode,
            })
        );
    }
    Ok(())
}

/// Collect the bytecode of one statement, without the comment column.
///
/// The comment column is display text, so it is left out to keep the record
/// usable as an identity for the program itself.
fn collect_bytecode(connection: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<String>>> {
    let mut statement = connection.prepare(format!("EXPLAIN {sql}"))?;
    let rows = statement.run_collect_rows()?;
    Ok(rows
        .iter()
        .map(|row| row.iter().take(7).map(value_to_text).collect())
        .collect())
}

/// Print every result row of a query file as one JSON record, for a
/// correctness comparison that is separate from the timed runs.
fn print_rows(
    database: &Database,
    connection: &Arc<Connection>,
    query: &Query,
    timeout_seconds: u64,
) -> Result<()> {
    for statement_text in &query.statements {
        let mut statement = connection.prepare(&statement_text.sql)?;
        set_timeout(&mut statement, timeout_seconds);
        loop {
            match statement.step()? {
                StepResult::Row => {
                    let row = statement.row().context("stepped row is missing")?;
                    let values: Vec<JsonValue> = row.get_values().map(json_value_of).collect();
                    println!("{}", JsonValue::Array(values));
                }
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    database.io.step()?
                }
                StepResult::Done => break,
                StepResult::Interrupt => bail!("query was interrupted"),
                StepResult::Busy => bail!("database was busy"),
            }
        }
    }
    Ok(())
}

fn json_value_of(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Numeric(Numeric::Integer(number)) => json!({"i": number}),
        Value::Numeric(Numeric::Float(number)) => json!({"f": f64::from(*number)}),
        Value::Text(text) => json!({"t": text.as_str()}),
        Value::Blob(blob) => json!({"b": hex_of(blob.as_slice())}),
    }
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(number)) => number.to_string(),
        Value::Numeric(Numeric::Float(number)) => format!("{:?}", f64::from(*number)),
        Value::Text(text) => text.as_str().to_string(),
        Value::Blob(blob) => hex_of(blob.as_slice()),
    }
}

fn hex_of(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}
