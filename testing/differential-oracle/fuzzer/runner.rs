//! Main simulation runner.
//!
//! This module orchestrates the simulation by:
//! 1. Creating both Turso and SQLite databases
//! 2. Generating and executing CREATE TABLE statements
//! 3. Generating statements (DML and DDL) using sql_gen
//! 4. Executing them on both databases
//! 5. Checking the differential oracle
//! 6. Re-introspecting schemas after DDL statements

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::panic::{AssertUnwindSafe, RefUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;
use turso_core::SqliteDialect;

use anyhow::{Context, Result, bail};
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::Database;

use crate::cdc;
use crate::generate::{
    Generated, GeneratedStatement, GeneratorKind, Matviews, PropTestBackend, SqlGenBackend,
    SqlGenerator, WeightProfile,
};
use crate::memory::{MemorySimIO, SimIO};
use crate::oracle::{DifferentialOracle, OracleResult, QueryResult, check_differential};
use crate::schema::SchemaIntrospector;
pub use sql_gen::TreeMode;
use sql_gen_prop::SqlValue;

const MAX_GENERATED_SQL_BYTES: usize = 64 * 1024;

fn generated_sql_is_too_large(sql: &str) -> bool {
    sql.len() > MAX_GENERATED_SQL_BYTES
}

/// Run `sql` on Turso and return each row as a vector of `ncols` text columns.
/// Used by the failure-state dumper, whose queries only ever select text.
fn turso_text_rows(
    conn: &Arc<turso_core::Connection>,
    sql: &str,
    ncols: usize,
) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    let Ok(mut stmt) = conn.prepare(sql) else {
        return out;
    };
    let _ = stmt.run_with_row_callback(|row| {
        let mut cols = Vec::with_capacity(ncols);
        for c in 0..ncols {
            cols.push(match row.get_value(c).clone() {
                turso_core::Value::Text(s) => s.as_str().to_string(),
                turso_core::Value::Null => String::new(),
                other => format!("{other:?}"),
            });
        }
        out.push(cols);
        Ok(())
    });
    out
}

/// Same as [`turso_text_rows`] but for the SQLite reference connection.
fn sqlite_text_rows(conn: &rusqlite::Connection, sql: &str, ncols: usize) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    let Ok(mut stmt) = conn.prepare(sql) else {
        return out;
    };
    let Ok(mut rows) = stmt.query([]) else {
        return out;
    };
    while let Ok(Some(r)) = rows.next() {
        let mut cols = Vec::with_capacity(ncols);
        for c in 0..ncols {
            cols.push(
                r.get::<_, Option<String>>(c)
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            );
        }
        out.push(cols);
    }
    out
}

/// The sqlite_master query for one object kind in one database.
fn state_ddl_query(db: &str, kind: &str) -> String {
    let master = if db == "main" {
        "sqlite_master".to_string()
    } else {
        format!("{db}.sqlite_master")
    };
    format!("SELECT name, sql FROM {master} WHERE type='{kind}' AND sql IS NOT NULL")
}

/// Rewrite a stored CREATE statement so it recreates the object in the same
/// database it came from: temp objects get the TEMP keyword, aux objects get
/// an `aux.` prefix on the object name. Generated names are simple unquoted
/// identifiers, so a single textual replacement is enough.
fn rewrite_state_ddl(db: &str, name: &str, sql: &str, keyword: &str) -> String {
    match db {
        "temp" => sql.replacen("CREATE ", "CREATE TEMP ", 1),
        "aux" => sql.replacen(
            &format!("{keyword} {name}"),
            &format!("{keyword} aux.{name}"),
            1,
        ),
        _ => sql.to_string(),
    }
}

/// Build a self-contained SQL script that reconstructs one engine's full state
/// (main, temp, and aux tables with all rows), followed by the failing
/// statement as a comment. `query` runs a text-only query against that engine.
fn build_state_dump(
    schema: &sql_gen::Schema,
    failing_sql: &str,
    query: &dyn Fn(&str, usize) -> Vec<Vec<String>>,
) -> String {
    let mut out = String::new();
    out.push_str("-- Reconstructed engine state captured at an oracle failure.\n");
    out.push_str("ATTACH ':memory:' AS aux;\n\n-- tables\n");

    // Tables first, so the data inserts below have somewhere to go.
    for db in ["main", "temp", "aux"] {
        for row in query(&state_ddl_query(db, "table"), 2) {
            out.push_str(&rewrite_state_ddl(db, &row[0], &row[1], "TABLE"));
            out.push_str(";\n");
        }
    }

    // Data. qualified_name() already carries the temp./aux. prefix.
    out.push_str("\n-- data\n");
    for table in &schema.tables {
        if table.columns.is_empty() {
            continue;
        }
        let quoted: Vec<String> = table
            .columns
            .iter()
            .map(|c| format!("quote(\"{}\")", c.name))
            .collect();
        let qname = table.qualified_name();
        let insert_query = format!(
            "SELECT 'INSERT INTO {qname} VALUES(' || {} || ');' FROM {qname}",
            quoted.join(" || ',' || ")
        );
        for row in query(&insert_query, 1) {
            out.push_str(&row[0]);
            out.push('\n');
        }
    }

    // Indexes and triggers last, so inserting the data above did not fire them.
    out.push_str("\n-- indexes and triggers\n");
    for db in ["main", "temp", "aux"] {
        for (kind, keyword) in [("index", "INDEX"), ("trigger", "TRIGGER")] {
            for row in query(&state_ddl_query(db, kind), 2) {
                out.push_str(&rewrite_state_ddl(db, &row[0], &row[1], keyword));
                out.push_str(";\n");
            }
        }
    }

    out.push_str("\n-- FAILING STATEMENT:\n-- ");
    out.push_str(failing_sql);
    out.push('\n');
    out
}

/// Configuration for the simulator.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Random seed for deterministic execution.
    pub seed: u64,
    /// Number of tables to create.
    pub num_tables: usize,
    /// Number of columns per table.
    pub columns_per_table: usize,
    /// Number of statements to generate and execute.
    pub num_statements: usize,
    /// Whether to print verbose output.
    pub verbose: bool,
    /// Keep simulation databases
    pub keep_files: bool,
    /// Which SQL generator backend to use.
    pub generator: GeneratorKind,
    /// Whether to write a coverage report.
    pub coverage: bool,
    /// Coverage report tree mode.
    pub tree_mode: TreeMode,
    /// Whether to enable MVCC mode.
    pub mvcc: bool,
    /// Probability that each expression-list SELECT column is generated
    /// as a window function. 0.0 disables window-function generation
    /// entirely; values up to 1.0 weight the SELECT list heavily toward
    /// `func(...) OVER (...)` projections.
    pub window_function_probability: f64,
    /// Generate SELECT-only workloads whose CTE definitions are all recursive.
    pub recursive_cte_focus: bool,
    /// Named statement-weight mix to generate with.
    pub weight_profile: WeightProfile,
    /// Generate materialized views: Turso maintains them, SQLite runs them as plain views.
    pub matview: bool,
    /// Probability that a non-DDL statement starts a BEGIN ... COMMIT batch.
    pub batch_probability: f64,
    /// Probability that a batch holds 50-300 statements instead of 2..=`max_batch_size`.
    pub large_batch_probability: f64,
    pub max_batch_size: usize,
    /// Probability that a step closes and reopens the Turso database.
    pub reopen_probability: f64,
    /// Probability that a write that passed the check runs again unchanged.
    pub redundant_dml_probability: f64,
    /// Turn on CDC on the Turso connection and check its records after every
    /// statement.
    pub cdc: bool,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: rand::rng().next_u64(),
            num_tables: 2,
            columns_per_table: 5,
            num_statements: 100,
            verbose: false,
            keep_files: false,
            generator: GeneratorKind::default(),
            coverage: false,
            tree_mode: TreeMode::default(),
            mvcc: false,
            window_function_probability: 0.0,
            recursive_cte_focus: false,
            weight_profile: WeightProfile::default(),
            matview: false,
            batch_probability: 0.0,
            large_batch_probability: 0.0,
            max_batch_size: 10,
            reopen_probability: 0.0,
            redundant_dml_probability: 0.0,
            cdc: false,
        }
    }
}

/// Statistics from a simulation run.
#[derive(Debug, Default)]
pub struct SimStats {
    /// Number of statements executed.
    pub statements_executed: usize,
    /// Number of statements skipped because EXPLAIN failed in at least one engine.
    pub statements_skipped: usize,
    /// Number of oracle warnings (e.g., LIMIT without ORDER BY mismatches).
    pub warnings: usize,
    /// Number of oracle failures.
    pub oracle_failures: usize,
    /// Number of errors encountered.
    pub errors: usize,
    /// Correlated SELECTs checked with unnesting forced and disabled.
    pub unnesting_invariants_checked: usize,
    /// BEGIN ... COMMIT batches run.
    pub batches: usize,
    /// Times the Turso database was closed and reopened.
    pub reopens: usize,
    /// Writes that ran a second time.
    pub repeats: usize,
}

impl SimStats {
    /// Returns true if the simulation completed successfully (no failures).
    pub fn is_success(&self) -> bool {
        self.oracle_failures == 0
    }

    /// Create a colorful table displaying simulation results.
    pub fn to_table(&self, config: &SimConfig) -> Table {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);

        // Header
        let status = if self.is_success() {
            Cell::new("PASSED")
                .fg(Color::Green)
                .add_attribute(Attribute::Bold)
        } else {
            Cell::new("FAILED")
                .fg(Color::Red)
                .add_attribute(Attribute::Bold)
        };

        table.set_header(vec![
            Cell::new("Simulation Results").add_attribute(Attribute::Bold),
            status,
        ]);

        // Config section
        table.add_row(vec![
            Cell::new("Seed").fg(Color::Cyan),
            Cell::new(config.seed),
        ]);
        table.add_row(vec![
            Cell::new("Target Statements").fg(Color::Cyan),
            Cell::new(config.num_statements),
        ]);

        // Results section
        table.add_row(vec![
            Cell::new("Statements Executed").fg(Color::Blue),
            Cell::new(self.statements_executed).fg(Color::Blue),
        ]);
        table.add_row(vec![
            Cell::new("Statements Skipped").fg(Color::Yellow),
            Cell::new(self.statements_skipped).fg(Color::Yellow),
        ]);

        // Warnings - yellow if any
        let warnings_cell = if self.warnings > 0 {
            Cell::new(self.warnings).fg(Color::Yellow)
        } else {
            Cell::new(self.warnings).fg(Color::Green)
        };
        table.add_row(vec![Cell::new("Warnings").fg(Color::Yellow), warnings_cell]);

        // Failures - red if any
        let failures_cell = if self.oracle_failures > 0 {
            Cell::new(self.oracle_failures)
                .fg(Color::Red)
                .add_attribute(Attribute::Bold)
        } else {
            Cell::new(self.oracle_failures).fg(Color::Green)
        };
        table.add_row(vec![
            Cell::new("Oracle Failures").fg(Color::Red),
            failures_cell,
        ]);

        // Errors - red if any
        let errors_cell = if self.errors > 0 {
            Cell::new(self.errors).fg(Color::Red)
        } else {
            Cell::new(self.errors).fg(Color::Green)
        };
        table.add_row(vec![Cell::new("Errors").fg(Color::Red), errors_cell]);
        table.add_row(vec![
            Cell::new("Unnesting invariants").fg(Color::Blue),
            Cell::new(self.unnesting_invariants_checked).fg(Color::Blue),
        ]);
        table.add_row(vec![
            Cell::new("Batches").fg(Color::Blue),
            Cell::new(self.batches).fg(Color::Blue),
        ]);
        table.add_row(vec![
            Cell::new("Reopens").fg(Color::Blue),
            Cell::new(self.reopens).fg(Color::Blue),
        ]);
        table.add_row(vec![
            Cell::new("Repeated writes").fg(Color::Blue),
            Cell::new(self.repeats).fg(Color::Blue),
        ]);

        table
    }

    /// Print the stats as a colorful table to stdout.
    pub fn print_table(&self, config: &SimConfig) {
        println!("\n{}", self.to_table(config));
    }
}

/// The main simulator.
pub struct Fuzzer {
    config: SimConfig,
    rng: RefCell<ChaCha8Rng>,
    turso_conn: RefCell<Arc<turso_core::Connection>>,
    sqlite_conn: rusqlite::Connection,
    turso_db: RefCell<Arc<Database>>,
    /// In-memory IO for the Turso database.
    io: Arc<MemorySimIO>,
    /// Directory to save run artifacts
    pub out_dir: PathBuf,
    /// Captures panic hook info (location + backtrace) for the last panic.
    panic_context: Arc<Mutex<Option<String>>>,
    /// The SQL that the current step runs, reported when the step panics.
    current_sql: RefCell<String>,
    /// The last CDC change id before the current Turso transaction began.
    cdc_transaction_start: std::cell::Cell<i64>,
}

/// What one iteration of the run loop does.
enum Step {
    Single(Generated),
    /// Non-DDL statements run inside BEGIN ... COMMIT.
    Batch(Vec<GeneratedStatement>),
    Reopen,
}

impl RefUnwindSafe for Fuzzer {}

impl Fuzzer {
    /// Create a new simulator with in-memory databases.
    ///
    /// Uses `MemorySimIO` for deterministic in-memory storage.
    pub fn new(config: SimConfig) -> Result<Self> {
        Self::with_out_dir(config, "simulator-output".into())
    }

    /// `out_dir` also names the Turso database, and all opens of one path in a
    /// process share one Turso database.
    fn with_out_dir(config: SimConfig, out_dir: PathBuf) -> Result<Self> {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);

        if !out_dir.exists() {
            std::fs::create_dir_all(&out_dir)?;
        }

        let io = Arc::new(MemorySimIO::new(config.seed));
        let (turso_db, turso_conn) = open_turso(&io, &out_dir, &config)?;

        // Create SQLite in-memory database
        let sqlite_conn = if config.keep_files {
            let path = out_dir.join("test-sqlite.db");
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            rusqlite::Connection::open(path.to_str().unwrap())
        } else {
            rusqlite::Connection::open_in_memory()
        }
        .context("Failed to open SQLite database")?;

        sqlite_conn
            .execute("ATTACH ':memory:' AS aux", [])
            .context("Failed to ATTACH on SQLite")?;
        tracing::info!("Attached ':memory:' AS aux on both connections");

        // Enable MVCC after ATTACH (ATTACH is not supported in MVCC mode)
        if config.mvcc {
            turso_conn
                .execute("PRAGMA journal_mode = 'mvcc'")
                .context("Failed to enable MVCC mode")?;
        }

        Ok(Self {
            config,
            rng: RefCell::new(rng),
            turso_conn: RefCell::new(turso_conn),
            sqlite_conn,
            turso_db: RefCell::new(turso_db),
            io,
            out_dir,
            panic_context: Arc::new(Mutex::new(None)),
            current_sql: RefCell::new(String::new()),
            cdc_transaction_start: std::cell::Cell::new(0),
        })
    }

    fn turso_conn(&self) -> Arc<turso_core::Connection> {
        self.turso_conn.borrow().clone()
    }

    /// Persist the in-memory database files to disk.
    ///
    /// Writes `.db`, `.wal`, and `.log` files to the filesystem.
    pub fn persist_files(&self) -> Result<()> {
        self.io.persist_files()?;
        Ok(())
    }

    /// Introspect and return the current schema from the Turso
    /// database, including attached databases.
    ///
    /// Uses `from_turso_with_attached` so callers see the same view
    /// the internal schema verification path (`introspect_and_verify_schemas`)
    /// uses; otherwise the diff fuzzer's "my view of the schema" and
    /// "the schema I run integrity checks against" could silently
    /// diverge when attached databases are present.
    pub fn get_schema(&self) -> Result<sql_gen::Schema> {
        SchemaIntrospector::from_turso_with_attached(&self.turso_conn())
            .context("Failed to introspect Turso schema (with attached)")
    }

    /// Run the simulation.
    pub fn run(&self) -> Result<SimStats> {
        let mut stats = SimStats::default();
        let mut executed_sql = Vec::new();
        let mut coverage = None;

        let result = self.run_inner(&mut stats, &mut executed_sql, &mut coverage);

        // Always write SQL file and print stats, even on error
        if let Err(e) = self.write_sql_file(&executed_sql) {
            tracing::warn!("Failed to write test.sql: {e}");
        }
        if self.config.coverage {
            if let Some(cov) = coverage {
                if let Err(e) = self.write_coverage_report(&cov) {
                    tracing::warn!("Failed to write coverage report: {e}");
                }
            }
        }
        stats.print_table(&self.config);

        result.map(|()| stats)
    }

    /// Write the coverage report to simulator-output/coverage.txt
    fn write_coverage_report(&self, coverage: &sql_gen::Coverage) -> Result<()> {
        let report = coverage.report_with_mode(self.config.tree_mode);
        let full_path = self.out_dir.join("coverage.txt");
        std::fs::write(&full_path, report.to_string())?;
        tracing::info!("Wrote coverage report to {}", full_path.display());
        Ok(())
    }

    /// Write all executed SQL statements to test.sql
    fn write_sql_file(&self, statements: &[String]) -> Result<()> {
        let full_path = self.out_dir.join("test.sql");
        let mut file = std::fs::File::create(full_path.clone())?;
        for sql in statements {
            writeln!(file, "{sql};")?;
        }
        tracing::info!(
            "Wrote {} statements to {}",
            statements.len(),
            full_path.display()
        );
        Ok(())
    }

    /// On an oracle failure, dump each engine's full state as reconstructable
    /// SQL (turso-state.sql, sqlite-state.sql). This captures main, temp, and
    /// aux tables with all rows, so a divergence that depends on accumulated
    /// state can be replayed as a small self-contained script. Returns the
    /// SQLite-side dump, which the shrinker uses as its replay baseline.
    fn dump_failure_state(&self, schema: &sql_gen::Schema, failing_sql: &str) -> String {
        let turso_conn = self.turso_conn();
        let turso_query = move |sql: &str, ncols: usize| turso_text_rows(&turso_conn, sql, ncols);
        let turso_dump = build_state_dump(schema, failing_sql, &turso_query);
        let sqlite_query =
            |sql: &str, ncols: usize| sqlite_text_rows(&self.sqlite_conn, sql, ncols);
        let sqlite_dump = build_state_dump(schema, failing_sql, &sqlite_query);
        for (name, body) in [
            ("turso-state.sql", turso_dump.as_str()),
            ("sqlite-state.sql", sqlite_dump.as_str()),
        ] {
            let path = self.out_dir.join(name);
            if let Err(e) = std::fs::write(&path, body) {
                tracing::warn!("Failed to write {}: {e}", path.display());
            } else {
                tracing::info!("Wrote state dump to {}", path.display());
            }
        }
        sqlite_dump
    }

    /// Minimize the failing statement against the dumped state — or against
    /// the run's executed statements when the divergence needs the history —
    /// and write the result (state script + minimized statement) to
    /// minimized.sql.
    fn shrink_and_write(&self, state_dump: &str, executed_sql: &[String], failing_sql: &str) {
        // The executed statements double as a replay script. Skip comment
        // lines (skipped/warning markers) the runner interleaves.
        let history: String = executed_sql
            .iter()
            .filter(|s| !s.trim_start().starts_with("--"))
            .map(|s| format!("{s};\n"))
            .collect();
        match crate::shrink::shrink_statement(state_dump, &history, failing_sql) {
            Ok(Some(minimized)) => {
                let path = self.out_dir.join("minimized.sql");
                let body = format!(
                    "{}\n-- MINIMIZED STATEMENT:\n{};\n",
                    minimized.state_sql, minimized.statement
                );
                if let Err(e) = std::fs::write(&path, body) {
                    tracing::warn!("Failed to write {}: {e}", path.display());
                } else {
                    tracing::info!(
                        "Wrote minimized reproduction ({} state lines, {} byte statement) to {}",
                        minimized.state_sql.lines().count(),
                        minimized.statement.len(),
                        path.display()
                    );
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("Shrinking failed: {e}"),
        }
    }

    fn run_inner(
        &self,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
        coverage_out: &mut Option<sql_gen::Coverage>,
    ) -> Result<()> {
        tracing::info!(
            "Starting simulation with seed={}, tables={}, statements={}, generator={:?}",
            self.config.seed,
            self.config.num_tables,
            self.config.num_statements,
            self.config.generator,
        );

        let mut generator: Box<dyn SqlGenerator> = match self.config.generator {
            GeneratorKind::SqlGen => {
                let seed: u64 = self.rng.borrow_mut().next_u64();
                Box::new(SqlGenBackend::new_with_window_weight(
                    seed,
                    self.config.window_function_probability,
                    self.config.weight_profile,
                ))
            }
            GeneratorKind::SqlGenProp => {
                let seed_bytes: [u8; 32] = {
                    let mut bytes = [0u8; 32];
                    self.rng.borrow_mut().fill_bytes(&mut bytes);
                    bytes
                };
                Box::new(PropTestBackend::new(
                    seed_bytes,
                    self.config.recursive_cte_focus,
                    self.config.weight_profile,
                    self.config.matview,
                    self.config.cdc,
                ))
            }
        };

        let mut schema = self.introspect_and_verify_schemas()?;
        let mut matviews = Matviews::new();
        let mut pending = None;

        for i in 0..self.config.num_statements {
            let step = self.next_step(generator.as_mut(), &schema, &matviews, &mut pending)?;
            let result = self.catch_panic(AssertUnwindSafe(|| {
                self.run_step(i, &step, &mut schema, &mut matviews, stats, executed_sql)
            }));
            match result {
                Ok(result) => result?,
                Err(panic) => {
                    let sql = self.current_sql.borrow().clone();
                    executed_sql.push(format!("-- PANIC: {sql}"));
                    stats.oracle_failures += 1;
                    tracing::error!("Panic at statement {i}: {panic}");
                    tracing::error!("Panicking SQL: {sql}");
                    return Err(anyhow::anyhow!(
                        "Panic during statement {i}: {panic}\n  SQL: {sql}"
                    ));
                }
            }
        }

        self.check_final_state(&matviews, stats, executed_sql)?;

        *coverage_out = generator.take_coverage();

        Ok(())
    }

    /// Run `f`, turning a panic into its message and backtrace, so that the
    /// caller can still record the statement and write `test.sql`.
    fn catch_panic<T>(&self, f: impl FnOnce() -> T + std::panic::UnwindSafe) -> Result<T, String> {
        let ctx = Arc::clone(&self.panic_context);
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let bt = std::backtrace::Backtrace::force_capture();
            *ctx.lock() = Some(format!("{info}\n{bt}"));
        }));
        let result = std::panic::catch_unwind(f);
        std::panic::set_hook(prev_hook);
        result.map_err(|panic| {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "Unknown panic".to_string());
            let context = self.panic_context.lock().take().unwrap_or_default();
            format!("{msg}\n{context}")
        })
    }

    /// Generate the next step. A statement that ends a batch early is kept in
    /// `pending` and becomes the next step.
    fn next_step(
        &self,
        generator: &mut dyn SqlGenerator,
        schema: &sql_gen::Schema,
        matviews: &Matviews,
        pending: &mut Option<Generated>,
    ) -> Result<Step> {
        if self.roll(self.config.reopen_probability) {
            return Ok(Step::Reopen);
        }
        let first = match pending.take() {
            Some(generated) => generated,
            None => generator.generate(schema, matviews)?,
        };
        let Generated::Statement(first) = first else {
            return Ok(Step::Single(first));
        };
        if first.is_ddl || !self.roll(self.config.batch_probability) {
            return Ok(Step::Single(Generated::Statement(first)));
        }
        let size = if self.roll(self.config.large_batch_probability) {
            50 + self.below(251)
        } else {
            2 + self.below(self.config.max_batch_size - 1)
        };
        let mut batch = vec![first];
        while batch.len() < size {
            match generator.generate(schema, matviews)? {
                Generated::Statement(stmt) if !stmt.is_ddl => batch.push(stmt),
                other => {
                    *pending = Some(other);
                    break;
                }
            }
        }
        Ok(Step::Batch(batch))
    }

    /// True with the given probability. Draws nothing when it is 0, so that
    /// runs without batches or reopens use the same random numbers as before.
    fn roll(&self, probability: f64) -> bool {
        probability > 0.0
            && (self.rng.borrow_mut().next_u64() as f64 / u64::MAX as f64) < probability
    }

    fn below(&self, n: usize) -> usize {
        (self.rng.borrow_mut().next_u64() % n as u64) as usize
    }

    fn run_step(
        &self,
        i: usize,
        step: &Step,
        schema: &mut sql_gen::Schema,
        matviews: &mut Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        match step {
            Step::Single(generated) => {
                *self.current_sql.borrow_mut() = generated.sql().to_string();
                self.run_generated(i, generated, schema, matviews, stats, executed_sql)
            }
            Step::Batch(stmts) => self.run_batch(i, stmts, schema, matviews, stats, executed_sql),
            Step::Reopen => self.reopen(schema, matviews, stats, executed_sql),
        }
    }

    fn run_batch(
        &self,
        i: usize,
        stmts: &[GeneratedStatement],
        schema: &mut sql_gen::Schema,
        matviews: &mut Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        assert!(
            self.sqlite_conn.is_autocommit(),
            "a batch must start outside a transaction"
        );
        stats.batches += 1;
        if self.config.verbose {
            tracing::info!("Statement {i} [BATCH]: {} statements", stmts.len());
        }
        let (turso, sqlite) = self.execute_transaction_control("BEGIN", stats, executed_sql)?;
        if matches!(turso, QueryResult::Error(_)) || matches!(sqlite, QueryResult::Error(_)) {
            stats.oracle_failures += 1;
            bail!("BEGIN failed:\n  Turso: {turso:?}\n  SQLite: {sqlite:?}");
        }
        for stmt in stmts {
            self.current_sql.borrow_mut().clone_from(&stmt.sql);
            self.run_statement(i, stmt, schema, matviews, stats, executed_sql)?;
        }
        let (turso, sqlite) = self.execute_transaction_control("COMMIT", stats, executed_sql)?;
        match (&turso, &sqlite) {
            (QueryResult::Error(turso_err), QueryResult::Error(_)) => {
                executed_sql.push(format!("-- COMMIT failed on both: {turso_err}"));
                let (turso, sqlite) =
                    self.execute_transaction_control("ROLLBACK", stats, executed_sql)?;
                if matches!(turso, QueryResult::Error(_)) || matches!(sqlite, QueryResult::Error(_))
                {
                    stats.oracle_failures += 1;
                    bail!(
                        "ROLLBACK after a failed COMMIT failed:\n  Turso: {turso:?}\n  SQLite: {sqlite:?}"
                    );
                }
            }
            (QueryResult::Error(turso_err), _) => {
                stats.oracle_failures += 1;
                bail!("Turso COMMIT failed, SQLite succeeded: {turso_err}");
            }
            (_, QueryResult::Error(sqlite_err)) => {
                stats.oracle_failures += 1;
                bail!("SQLite COMMIT failed, Turso succeeded: {sqlite_err}");
            }
            _ => {}
        }
        self.verify_matviews(matviews, stats, executed_sql)
    }

    fn execute_transaction_control(
        &self,
        sql: &str,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<(QueryResult, QueryResult)> {
        let mark = self.cdc_mark(None)?;
        let results = self.execute_on_both(sql, executed_sql);
        self.check_cdc(mark, sql, stats, executed_sql)?;
        Ok(results)
    }

    fn execute_on_both(
        &self,
        sql: &str,
        executed_sql: &mut Vec<String>,
    ) -> (QueryResult, QueryResult) {
        *self.current_sql.borrow_mut() = sql.to_string();
        executed_sql.push(sql.to_string());
        (
            DifferentialOracle::execute_turso(&self.turso_conn(), sql),
            DifferentialOracle::execute_sqlite(&self.sqlite_conn, sql),
        )
    }

    /// Close and reopen the Turso database on the same in-memory files, then
    /// compare every table and materialized view with SQLite.
    fn reopen(
        &self,
        schema: &mut sql_gen::Schema,
        matviews: &Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        assert!(
            self.sqlite_conn.is_autocommit(),
            "a reopen must happen outside a transaction"
        );
        assert!(
            schema.tables.iter().all(|t| t.database.is_none()),
            "a reopen loses tables outside the main database"
        );
        stats.reopens += 1;
        *self.current_sql.borrow_mut() = "-- REOPEN".to_string();
        executed_sql.push("-- REOPEN".to_string());
        self.turso_conn().close()?;
        let (turso_db, turso_conn) = match open_turso(&self.io, &self.out_dir, &self.config) {
            Ok(opened) => opened,
            Err(e) => {
                stats.oracle_failures += 1;
                return Err(e.context("Failed to reopen the Turso database"));
            }
        };
        *self.turso_db.borrow_mut() = turso_db;
        *self.turso_conn.borrow_mut() = turso_conn;
        *schema = self
            .introspect_and_verify_schemas()
            .map_err(|e| anyhow::anyhow!("Schema mismatch after reopen: {e}"))?;
        self.verify_tables(schema, stats, executed_sql)?;
        self.verify_matviews(matviews, stats, executed_sql)
    }

    fn verify_tables(
        &self,
        schema: &sql_gen::Schema,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        for table in &schema.tables {
            let sql = format!("SELECT rowid, * FROM {} ORDER BY rowid", table.name);
            let turso = DifferentialOracle::execute_turso(&self.turso_conn(), &sql);
            let sqlite = DifferentialOracle::execute_sqlite(&self.sqlite_conn, &sql);
            if turso != sqlite {
                stats.oracle_failures += 1;
                executed_sql.push(format!("-- TABLE VERIFY FAILED: {sql}"));
                bail!(
                    "Table mismatch after reopen in '{}':\n  Turso:  {turso:?}\n  SQLite: {sqlite:?}",
                    table.name
                );
            }
        }
        Ok(())
    }

    fn run_generated(
        &self,
        i: usize,
        generated: &Generated,
        schema: &mut sql_gen::Schema,
        matviews: &mut Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        match generated {
            Generated::Statement(stmt) => {
                self.run_statement(i, stmt, schema, matviews, stats, executed_sql)
            }
            Generated::CreateMatview {
                turso_sql,
                sqlite_sql,
                name,
                columns,
            } => {
                if self.config.verbose {
                    tracing::info!("Statement {i} [MATVIEW]: {turso_sql}");
                }
                executed_sql.push(turso_sql.clone());
                executed_sql.push(format!("-- SQLITE: {sqlite_sql}"));
                let turso = DifferentialOracle::execute_turso(&self.turso_conn(), turso_sql);
                let sqlite = DifferentialOracle::execute_sqlite(&self.sqlite_conn, sqlite_sql);
                // The generator only emits views that both engines accept.
                if matches!(turso, QueryResult::Error(_)) || matches!(sqlite, QueryResult::Error(_))
                {
                    stats.oracle_failures += 1;
                    executed_sql.push(format!("-- FAILED: {turso_sql}"));
                    bail!(
                        "Oracle failure at statement {i}: materialized view DDL failed.\n  Turso SQL: {turso_sql}\n  Turso: {turso:?}\n  SQLite SQL: {sqlite_sql}\n  SQLite: {sqlite:?}"
                    );
                }
                stats.statements_executed += 1;
                matviews.insert(name.clone(), columns.clone());
                self.verify_matviews(matviews, stats, executed_sql)
            }
            Generated::DropMatview { sql, name } => {
                if self.config.verbose {
                    tracing::info!("Statement {i} [MATVIEW]: {sql}");
                }
                self.drop_view_on_both(sql, stats, executed_sql)?;
                stats.statements_executed += 1;
                matviews.remove(name);
                self.drop_matviews_sqlite_cannot_read(matviews, stats, executed_sql)?;
                self.verify_matviews(matviews, stats, executed_sql)
            }
        }
    }

    fn run_statement(
        &self,
        i: usize,
        stmt: &GeneratedStatement,
        schema: &mut sql_gen::Schema,
        matviews: &mut Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        let ran = self.check_statement(i, stmt, schema, matviews, stats, executed_sql)?;
        if ran && stmt.mutates_data && self.roll(self.config.redundant_dml_probability) {
            stats.repeats += 1;
            executed_sql.push("-- REPEAT".to_string());
            self.check_statement(i, stmt, schema, matviews, stats, executed_sql)?;
        }
        Ok(())
    }

    /// Run one statement on both engines and compare the outcome. Returns
    /// false when the statement was skipped.
    fn check_statement(
        &self,
        i: usize,
        stmt: &GeneratedStatement,
        schema: &mut sql_gen::Schema,
        matviews: &mut Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<bool> {
        // A generated expression can branch into several subqueries at
        // each level. This occasionally produces hundreds of kilobytes of
        // SQL. Preparing such a statement uses enough recursive calls to
        // exhaust the process stack before either engine can return an
        // error. These statements add little useful coverage, so skip
        // them before passing them to either engine.
        if generated_sql_is_too_large(&stmt.sql) {
            stats.statements_skipped += 1;
            let reason = format!(
                "Statement skipped because it is {} bytes; the limit is {MAX_GENERATED_SQL_BYTES} bytes",
                stmt.sql.len()
            );
            push_warning_comments(executed_sql, i, &reason);
            tracing::debug!("Skipped generated statement {i}: {reason}");
            return Ok(false);
        }

        if self.config.verbose {
            let stmt_type = if stmt.is_ddl { "DDL" } else { "DML" };
            tracing::info!("Statement {} [{}]: {}", i, stmt_type, stmt.sql);
        }

        let writes_rows = stmt.mutates_data && !stmt.is_ddl;
        let cdc_mark = self.cdc_mark(writes_rows.then_some(&*schema))?;
        match check_differential(&self.turso_conn(), &self.sqlite_conn, schema, stmt) {
            OracleResult::Pass => {
                stats.statements_executed += 1;
                executed_sql.push(stmt.sql.clone());
            }
            OracleResult::PassWithUnnestingInvariant => {
                stats.statements_executed += 1;
                stats.unnesting_invariants_checked += 1;
                executed_sql.push(stmt.sql.clone());
            }
            OracleResult::Skipped(reason) => {
                stats.statements_skipped += 1;
                push_warning_comments(executed_sql, i, &reason);
                executed_sql.push(format!("-- SKIPPED: {}", stmt.sql));
                tracing::debug!("Skipped generated statement {i}: {reason}");
                return Ok(false);
            }
            OracleResult::Warning(reason) => {
                stats.statements_executed += 1;
                stats.warnings += 1;
                push_warning_comments(executed_sql, i, &reason);
                executed_sql.push(stmt.sql.clone());
                tracing::warn!("Oracle warning at statement {i}: {reason}");
            }
            OracleResult::Fail(reason) => {
                stats.oracle_failures += 1;
                executed_sql.push(format!("-- FAILED: {}", stmt.sql));
                tracing::error!("Oracle failure at statement {i}: {reason}");
                if !self.config.verbose {
                    tracing::error!("Failing SQL: {}", stmt.sql);
                }
                if let Err(e) = self.write_sql_file(executed_sql) {
                    tracing::warn!("Failed to write test.sql: {e}");
                }
                let state_dump = self.dump_failure_state(schema, &stmt.sql);
                self.shrink_and_write(&state_dump, executed_sql, &stmt.sql);
                return Err(anyhow::anyhow!("Oracle failure: {reason}"));
            }
        }
        self.check_cdc(cdc_mark, &stmt.sql, stats, executed_sql)?;

        if stmt.is_ddl {
            self.drop_matviews_sqlite_cannot_read(matviews, stats, executed_sql)?;
            *schema = self.introspect_and_verify_schemas().map_err(|e| {
                anyhow::anyhow!(
                    "Schema mismatch after DDL statement {i} ({}): {e}",
                    stmt.sql
                )
            })?;
            tracing::debug!(
                "Schema updated after DDL: {} tables, {} indexes",
                schema.tables.len(),
                schema.indexes.len()
            );
        }
        if stmt.mutates_data {
            self.verify_matviews(matviews, stats, executed_sql)?;
        }
        Ok(true)
    }

    /// Remember where the CDC records of the next statement start. With a
    /// schema, also count the rows of its tables.
    fn cdc_mark(&self, count_rows_of: Option<&sql_gen::Schema>) -> Result<Option<CdcMark>> {
        if !self.config.cdc {
            return Ok(None);
        }
        let conn = self.turso_conn();
        let statement_start = cdc::last_change_id(&conn)?;
        if conn.get_auto_commit() {
            self.cdc_transaction_start.set(statement_start);
        }
        let row_counts = count_rows_of
            .map(|schema| self.turso_row_counts(schema))
            .transpose()?;
        Ok(Some(CdcMark {
            statement_start,
            row_counts,
        }))
    }

    fn check_cdc(
        &self,
        mark: Option<CdcMark>,
        sql: &str,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        let Some(mark) = mark else {
            return Ok(());
        };
        let conn = self.turso_conn();
        let transaction = cdc::records_after(&conn, self.cdc_transaction_start.get())?;
        let statement: Vec<cdc::Record> = transaction
            .iter()
            .filter(|r| r.change_id > mark.statement_start)
            .cloned()
            .collect();
        let mut failure = cdc::check_transaction(&transaction, conn.get_auto_commit()).err();
        if let (None, Some(before)) = (&failure, &mark.row_counts) {
            let schema = self.get_schema()?;
            let after = self.turso_row_counts(&schema)?;
            failure = cdc::check_row_counts(&statement, before, &after).err();
        }
        let Some(reason) = failure else {
            return Ok(());
        };
        stats.oracle_failures += 1;
        executed_sql.push(format!("-- CDC CHECK FAILED: {sql}"));
        let records: Vec<String> = transaction.iter().map(|r| r.to_string()).collect();
        bail!(
            "CDC check failed: {reason}\n  SQL: {sql}\n  Records of the transaction: {}",
            records.join(", ")
        );
    }

    /// Row counts by table name. CDC records name a table without its
    /// database, so tables of the same name in main, temp and aux add up.
    fn turso_row_counts(&self, schema: &sql_gen::Schema) -> Result<BTreeMap<String, i64>> {
        let mut counts = BTreeMap::new();
        for table in &schema.tables {
            let sql = format!("SELECT count(*) FROM {}", table.qualified_name());
            let QueryResult::Rows(rows) =
                DifferentialOracle::execute_turso(&self.turso_conn(), &sql)
            else {
                bail!("{sql} returned no row");
            };
            let SqlValue::Integer(count) = rows[0].0[0] else {
                bail!("{sql} returned {:?}", rows[0]);
            };
            *counts.entry(table.name.clone()).or_insert(0) += count;
        }
        Ok(counts)
    }

    /// Compare every materialized view on Turso with the plain view on SQLite.
    fn verify_matviews(
        &self,
        matviews: &Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        for (name, columns) in matviews {
            assert!(!columns.is_empty(), "matview {name} has no columns");
            let order_by = (1..=columns.len())
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT * FROM {name} ORDER BY {order_by}");
            let turso = DifferentialOracle::execute_turso(&self.turso_conn(), &sql);
            let sqlite = DifferentialOracle::execute_sqlite(&self.sqlite_conn, &sql);
            let failure = match (&turso, &sqlite) {
                (QueryResult::Error(_), _) | (_, QueryResult::Error(_)) => Some("read error"),
                _ if turso != sqlite => Some("data mismatch"),
                _ => None,
            };
            if let Some(failure) = failure {
                stats.oracle_failures += 1;
                executed_sql.push(format!("-- MATVIEW VERIFY FAILED: {sql}"));
                bail!("Matview {failure} in '{name}':\n  Turso:  {turso:?}\n  SQLite: {sqlite:?}");
            }
        }
        Ok(())
    }

    /// After a DDL statement, a plain view on SQLite stops working when a table
    /// or view it reads is gone, while Turso keeps the materialized rows. Such
    /// views can no longer be compared, so drop them on both engines.
    fn drop_matviews_sqlite_cannot_read(
        &self,
        matviews: &mut Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        let unreadable: Vec<String> = matviews
            .keys()
            .filter(|name| {
                matches!(
                    DifferentialOracle::execute_sqlite(
                        &self.sqlite_conn,
                        &format!("SELECT * FROM {name} LIMIT 0")
                    ),
                    QueryResult::Error(_)
                )
            })
            .cloned()
            .collect();
        for name in unreadable {
            let sql = format!("DROP VIEW {name}");
            executed_sql.push(format!("-- SOURCE GONE: {name}"));
            self.drop_view_on_both(&sql, stats, executed_sql)?;
            matviews.remove(&name);
        }
        Ok(())
    }

    fn drop_view_on_both(
        &self,
        sql: &str,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        let turso = DifferentialOracle::execute_turso(&self.turso_conn(), sql);
        let sqlite = DifferentialOracle::execute_sqlite(&self.sqlite_conn, sql);
        executed_sql.push(sql.to_string());
        if matches!(turso, QueryResult::Error(_)) || matches!(sqlite, QueryResult::Error(_)) {
            stats.oracle_failures += 1;
            executed_sql.push(format!("-- FAILED: {sql}"));
            bail!("DROP VIEW failed: {sql}\n  Turso:  {turso:?}\n  SQLite: {sqlite:?}");
        }
        Ok(())
    }

    /// Compare the materialized views and check both databases for corruption.
    /// A view mismatch does not skip the corruption check.
    fn check_final_state(
        &self,
        matviews: &Matviews,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        let views = self.verify_matviews(matviews, stats, executed_sql);
        let integrity = self.run_integrity_check(stats, executed_sql);
        match (views, integrity) {
            (Err(views), Err(integrity)) => bail!("{views:#}\n{integrity:#}"),
            (views, integrity) => views.and(integrity),
        }
    }

    /// Run `PRAGMA integrity_check` on both databases and fail if either reports corruption.
    fn run_integrity_check(
        &self,
        stats: &mut SimStats,
        executed_sql: &mut Vec<String>,
    ) -> Result<()> {
        if self.config.mvcc {
            tracing::info!("Skipping integrity check (not supported with MVCC)");
            return Ok(());
        }
        tracing::info!("Running integrity check on both databases...");

        let sql = "PRAGMA integrity_check";
        executed_sql.push(sql.to_string());

        let turso_result = DifferentialOracle::execute_turso(&self.turso_conn(), sql);
        let sqlite_result = DifferentialOracle::execute_sqlite(&self.sqlite_conn, sql);

        let check_ok = |result: &QueryResult, db_name: &str| -> Result<()> {
            match result {
                QueryResult::Rows(rows) if rows.len() == 1 && rows[0].0.len() == 1 => {
                    if let SqlValue::Text(ref text) = rows[0].0[0] {
                        if text == "ok" {
                            return Ok(());
                        }
                    }
                    bail!("{db_name} integrity check failed: {:?}", rows);
                }
                QueryResult::Rows(rows) => {
                    // Multiple rows means multiple integrity errors
                    bail!("{db_name} integrity check failed: {:?}", rows);
                }
                QueryResult::Error(e) => {
                    bail!("{db_name} integrity check errored: {e}");
                }
                QueryResult::Ok => {
                    bail!("{db_name} integrity check returned no results");
                }
            }
        };

        if let Err(e) = check_ok(&turso_result, "Turso") {
            stats.oracle_failures += 1;
            executed_sql.push(format!("-- FAILED: {sql} ({e})"));
            tracing::error!("{e}");
            return Err(e);
        }

        if let Err(e) = check_ok(&sqlite_result, "SQLite") {
            stats.oracle_failures += 1;
            executed_sql.push(format!("-- FAILED: {sql} ({e})"));
            tracing::error!("{e}");
            return Err(e);
        }

        tracing::info!("Integrity check passed on both databases");
        Ok(())
    }

    /// Introspect schemas from both databases and verify they match.
    fn introspect_and_verify_schemas(&self) -> Result<sql_gen::Schema> {
        let (turso_schema, sqlite_schema) = (
            SchemaIntrospector::from_turso_with_attached(&self.turso_conn())
                .context("Failed to introspect Turso schema (with attached)")?,
            SchemaIntrospector::from_sqlite_with_attached(&self.sqlite_conn)
                .context("Failed to introspect SQLite schema (with attached)")?,
        );

        // Verify table names match (using qualified names to distinguish databases)
        let turso_tables: std::collections::HashSet<_> = turso_schema
            .tables
            .iter()
            .map(|t| t.qualified_name())
            .collect();
        let sqlite_tables: std::collections::HashSet<_> = sqlite_schema
            .tables
            .iter()
            .map(|t| t.qualified_name())
            .collect();

        if turso_tables != sqlite_tables {
            bail!(
                "Table mismatch: Turso has {:?}, SQLite has {:?}",
                turso_tables,
                sqlite_tables
            );
        }

        let turso_indexes: std::collections::HashSet<_> = turso_schema
            .indexes
            .iter()
            .map(|i| i.qualified_name())
            .collect();
        let sqlite_indexes: std::collections::HashSet<_> = sqlite_schema
            .indexes
            .iter()
            .map(|i| i.qualified_name())
            .collect();

        if turso_indexes != sqlite_indexes {
            bail!(
                "Index mismatch: Turso has {:?}, SQLite has {:?}",
                turso_indexes,
                sqlite_indexes
            );
        }

        let turso_triggers: std::collections::HashSet<_> = turso_schema
            .triggers
            .iter()
            .map(|trigger| {
                (
                    trigger.qualified_name(),
                    trigger.table_name.as_str().to_string(),
                )
            })
            .collect();
        let sqlite_triggers: std::collections::HashSet<_> = sqlite_schema
            .triggers
            .iter()
            .map(|trigger| {
                (
                    trigger.qualified_name(),
                    trigger.table_name.as_str().to_string(),
                )
            })
            .collect();

        if turso_triggers != sqlite_triggers {
            bail!(
                "Trigger mismatch: Turso has {:?}, SQLite has {:?}",
                turso_triggers,
                sqlite_triggers
            );
        }

        // Verify each table's columns and strict flags match
        for turso_table in turso_schema.tables.iter() {
            let sqlite_table = sqlite_schema
                .tables
                .iter()
                .find(|t| t.name == turso_table.name && t.database == turso_table.database)
                .expect("Table should exist in SQLite schema");

            if turso_table.strict != sqlite_table.strict {
                bail!(
                    "STRICT mismatch in table '{}': Turso strict={}, SQLite strict={}",
                    turso_table.name,
                    turso_table.strict,
                    sqlite_table.strict
                );
            }

            let turso_cols: Vec<_> = turso_table.columns.iter().map(|c| &c.name).collect();
            let sqlite_cols: Vec<_> = sqlite_table.columns.iter().map(|c| &c.name).collect();

            if turso_cols != sqlite_cols {
                bail!(
                    "Column mismatch in table '{}': Turso has {:?}, SQLite has {:?}",
                    turso_table.qualified_name(),
                    turso_cols,
                    sqlite_cols
                );
            }
        }

        for turso_index in turso_schema.indexes.iter() {
            let sqlite_index = sqlite_schema
                .indexes
                .iter()
                .find(|i| i.name == turso_index.name && i.database == turso_index.database)
                .expect("Index should exist in SQLite schema");

            if turso_index.table_name != sqlite_index.table_name {
                bail!(
                    "Index target mismatch for '{}': Turso targets '{}', SQLite targets '{}'",
                    turso_index.qualified_name(),
                    turso_index.table_name,
                    sqlite_index.table_name
                );
            }

            if turso_index.unique != sqlite_index.unique {
                bail!(
                    "UNIQUE mismatch for index '{}': Turso unique={}, SQLite unique={}",
                    turso_index.qualified_name(),
                    turso_index.unique,
                    sqlite_index.unique
                );
            }

            if turso_index.columns != sqlite_index.columns {
                bail!(
                    "Index column mismatch for '{}': Turso has {:?}, SQLite has {:?}",
                    turso_index.qualified_name(),
                    turso_index.columns,
                    sqlite_index.columns
                );
            }
        }

        Ok(turso_schema)
    }
}

/// Where the CDC records of one statement start.
struct CdcMark {
    statement_start: i64,
    row_counts: Option<BTreeMap<String, i64>>,
}

/// Open the Turso database file in `io` and attach an in-memory `aux` database,
/// as SQLite has one.
fn open_turso(
    io: &Arc<MemorySimIO>,
    out_dir: &std::path::Path,
    config: &SimConfig,
) -> Result<(Arc<Database>, Arc<turso_core::Connection>)> {
    let opts = turso_core::DatabaseOpts::new()
        .with_attach(true)
        .with_views(config.matview);
    let turso_db = Database::open_file_with_flags(
        io.clone(),
        out_dir.join("test.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )?;
    let turso_conn = turso_db.connect()?;
    turso_conn
        .execute("ATTACH ':memory:' AS aux")
        .context("Failed to ATTACH on Turso")?;
    if config.cdc {
        turso_conn
            .execute(cdc::ENABLE_CDC)
            .context("Failed to turn on CDC on Turso")?;
    }
    Ok((turso_db, turso_conn))
}

fn push_warning_comments(executed_sql: &mut Vec<String>, stmt_idx: usize, reason: &str) {
    for (line_idx, line) in reason.lines().enumerate() {
        executed_sql.push(format!(
            "-- WARNING stmt={stmt_idx} line={line_idx}: {line}"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn test_sim_config_default() {
        let config = SimConfig::default();
        // seed is now randomly generated by default
        assert!(config.seed > 0);
        assert_eq!(config.num_tables, 2);
        assert_eq!(config.num_statements, 100);
    }

    #[test]
    fn test_simulator_creation() {
        let config = SimConfig {
            seed: 12345,
            num_tables: 1,
            columns_per_table: 3,
            num_statements: 10,
            verbose: false,
            keep_files: false,
            generator: GeneratorKind::default(),
            coverage: false,
            tree_mode: TreeMode::default(),
            mvcc: false,
            window_function_probability: 0.0,
            recursive_cte_focus: false,
            weight_profile: WeightProfile::default(),
            matview: false,
            batch_probability: 0.0,
            large_batch_probability: 0.0,
            max_batch_size: 10,
            reopen_probability: 0.0,
            redundant_dml_probability: 0.0,
            cdc: false,
        };
        let sim = Fuzzer::new(config);
        assert!(sim.is_ok());
    }

    fn matview_fuzzer() -> TestFuzzer {
        test_fuzzer(SimConfig {
            seed: 7,
            generator: GeneratorKind::SqlGenProp,
            matview: true,
            ..SimConfig::default()
        })
    }

    fn test_fuzzer(config: SimConfig) -> TestFuzzer {
        static NEXT_OUT_DIR: AtomicUsize = AtomicUsize::new(0);
        let out_dir = std::env::temp_dir().join(format!(
            "differential-fuzzer-test-{}-{}",
            std::process::id(),
            NEXT_OUT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let fuzzer = Fuzzer::with_out_dir(config, out_dir.clone()).unwrap();
        TestFuzzer { fuzzer, out_dir }
    }

    struct TestFuzzer {
        fuzzer: Fuzzer,
        out_dir: PathBuf,
    }

    impl std::ops::Deref for TestFuzzer {
        type Target = Fuzzer;

        fn deref(&self) -> &Fuzzer {
            &self.fuzzer
        }
    }

    impl std::ops::DerefMut for TestFuzzer {
        fn deref_mut(&mut self) -> &mut Fuzzer {
            &mut self.fuzzer
        }
    }

    impl Drop for TestFuzzer {
        fn drop(&mut self) {
            let removed = std::fs::remove_dir_all(&self.out_dir);
            if !std::thread::panicking() {
                removed.unwrap();
            }
        }
    }

    fn write(sql: &str) -> GeneratedStatement {
        GeneratedStatement {
            sql: sql.to_string(),
            is_ddl: false,
            mutates_data: true,
            has_unordered_limit: false,
            unordered_limit_reason: None,
            check_unnesting_invariant: false,
        }
    }

    fn matview_over_t(fuzzer: &Fuzzer, executed_sql: &mut Vec<String>) -> Matviews {
        for sql in [
            "CREATE TABLE t(a INTEGER, b TEXT)",
            "INSERT INTO t VALUES (1, 'x'), (2, NULL)",
        ] {
            let (turso, sqlite) = fuzzer.execute_on_both(sql, executed_sql);
            assert!(!matches!(turso, QueryResult::Error(_)), "{turso:?}");
            assert!(!matches!(sqlite, QueryResult::Error(_)), "{sqlite:?}");
        }
        let select = "SELECT a, b FROM t WHERE a > 1";
        fuzzer
            .turso_conn()
            .execute(format!("CREATE MATERIALIZED VIEW v AS {select}"))
            .unwrap();
        fuzzer
            .sqlite_conn
            .execute(&format!("CREATE VIEW v AS {select}"), [])
            .unwrap();
        Matviews::from([(
            "v".to_string(),
            vec![
                sql_gen_prop::ColumnDef::new("a", sql_gen_prop::DataType::Integer),
                sql_gen_prop::ColumnDef::new("b", sql_gen_prop::DataType::Text),
            ],
        )])
    }

    #[test]
    fn reopen_keeps_tables_and_materialized_views_and_later_writes_use_the_new_connection() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let matviews = matview_over_t(&fuzzer, &mut executed_sql);
        let mut schema = fuzzer.introspect_and_verify_schemas().unwrap();

        fuzzer
            .reopen(&mut schema, &matviews, &mut stats, &mut executed_sql)
            .unwrap();
        fuzzer.execute_on_both("INSERT INTO t VALUES (3, 'y')", &mut executed_sql);
        fuzzer
            .verify_tables(&schema, &mut stats, &mut executed_sql)
            .unwrap();
        fuzzer
            .verify_matviews(&matviews, &mut stats, &mut executed_sql)
            .unwrap();
        assert_eq!(stats.reopens, 1);
        assert_eq!(stats.oracle_failures, 0);

        fuzzer
            .sqlite_conn
            .execute("INSERT INTO t VALUES (0, 'q')", [])
            .unwrap();
        let err = fuzzer
            .verify_tables(&schema, &mut stats, &mut executed_sql)
            .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("Table mismatch after reopen in 't'"),
            "{err}"
        );
    }

    #[test]
    fn a_materialized_view_that_fails_on_both_engines_is_a_failure() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mut matviews = matview_over_t(&fuzzer, &mut executed_sql);
        let mut schema = fuzzer.introspect_and_verify_schemas().unwrap();
        let select = "SELECT t.NULL FROM t";
        let create = Generated::CreateMatview {
            turso_sql: format!("CREATE MATERIALIZED VIEW w AS {select}"),
            sqlite_sql: format!("CREATE VIEW w AS {select}"),
            name: "w".to_string(),
            columns: vec![sql_gen_prop::ColumnDef::new(
                "a",
                sql_gen_prop::DataType::Integer,
            )],
        };

        let err = fuzzer
            .run_generated(
                0,
                &create,
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap_err();

        assert!(
            err.to_string().contains("materialized view DDL failed"),
            "{err}"
        );
        assert_eq!(stats.oracle_failures, 1);
        assert!(!matviews.contains_key("w"));
    }

    #[test]
    fn batch_runs_between_begin_and_commit_on_both_engines() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mut matviews = matview_over_t(&fuzzer, &mut executed_sql);
        let mut schema = fuzzer.introspect_and_verify_schemas().unwrap();
        let batch = [
            write("INSERT INTO t VALUES (5, 'z')"),
            write("DELETE FROM t WHERE a = 2"),
        ];

        fuzzer
            .run_batch(
                0,
                &batch,
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap();

        assert!(fuzzer.sqlite_conn.is_autocommit());
        assert!(fuzzer.turso_conn().get_auto_commit());
        let batch_sql = &executed_sql[executed_sql.len() - 4..];
        assert_eq!(
            batch_sql,
            [
                "BEGIN",
                "INSERT INTO t VALUES (5, 'z')",
                "DELETE FROM t WHERE a = 2",
                "COMMIT"
            ]
        );
        assert_eq!(stats.batches, 1);
        assert_eq!(stats.oracle_failures, 0);
    }

    #[test]
    fn a_write_that_passes_runs_once_more_and_views_are_compared_after_it() {
        let mut fuzzer = matview_fuzzer();
        fuzzer.config.redundant_dml_probability = 1.0;
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mut matviews = matview_over_t(&fuzzer, &mut executed_sql);
        let mut schema = fuzzer.introspect_and_verify_schemas().unwrap();

        fuzzer
            .run_statement(
                0,
                &write("INSERT OR REPLACE INTO t VALUES (7, 'r')"),
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap();

        assert_eq!(
            &executed_sql[executed_sql.len() - 3..],
            [
                "INSERT OR REPLACE INTO t VALUES (7, 'r')",
                "-- REPEAT",
                "INSERT OR REPLACE INTO t VALUES (7, 'r')"
            ]
        );
        assert_eq!(stats.repeats, 1);
        assert_eq!(stats.oracle_failures, 0);
    }

    fn make_view_v_stale_on_turso(fuzzer: &Fuzzer) {
        fuzzer
            .sqlite_conn
            .execute("INSERT INTO t VALUES (9, 's')", [])
            .unwrap();
    }

    #[test]
    fn views_are_compared_after_a_commit_that_fails_on_both_engines_is_rolled_back() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mut matviews = matview_over_t(&fuzzer, &mut executed_sql);
        for sql in [
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(x INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            fuzzer.execute_on_both(sql, &mut executed_sql);
        }
        let mut schema = fuzzer.introspect_and_verify_schemas().unwrap();
        make_view_v_stale_on_turso(&fuzzer);
        let batch = [GeneratedStatement {
            mutates_data: false,
            ..write("INSERT INTO c VALUES (5)")
        }];

        let err = fuzzer
            .run_batch(
                0,
                &batch,
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap_err();

        assert!(
            err.to_string().starts_with("Matview data mismatch in 'v'"),
            "{err}"
        );
        assert!(
            executed_sql
                .iter()
                .any(|s| s.starts_with("-- COMMIT failed on both"))
        );
        assert!(executed_sql.iter().any(|s| s == "ROLLBACK"));
        assert!(fuzzer.sqlite_conn.is_autocommit());
        assert!(fuzzer.turso_conn().get_auto_commit());
    }

    #[test]
    fn views_are_compared_at_the_end_of_the_run() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let matviews = matview_over_t(&fuzzer, &mut executed_sql);
        make_view_v_stale_on_turso(&fuzzer);

        let err = fuzzer
            .check_final_state(&matviews, &mut stats, &mut executed_sql)
            .unwrap_err();

        assert!(
            err.to_string().starts_with("Matview data mismatch in 'v'"),
            "{err}"
        );
        assert_eq!(stats.oracle_failures, 1);
    }

    #[test]
    fn the_integrity_check_runs_after_a_view_mismatch_at_the_end_of_the_run() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let matviews = matview_over_t(&fuzzer, &mut executed_sql);
        make_view_v_stale_on_turso(&fuzzer);
        fuzzer
            .sqlite_conn
            .execute_batch(
                "CREATE TABLE c(x INTEGER CHECK (x > 0));
                 PRAGMA ignore_check_constraints = ON;
                 INSERT INTO c VALUES (0);
                 PRAGMA ignore_check_constraints = OFF;",
            )
            .unwrap();

        let err = fuzzer
            .check_final_state(&matviews, &mut stats, &mut executed_sql)
            .unwrap_err()
            .to_string();

        assert!(err.starts_with("Matview data mismatch in 'v'"), "{err}");
        assert!(err.contains("SQLite integrity check failed"), "{err}");
        assert_eq!(stats.oracle_failures, 2);
    }

    fn cdc_fuzzer_with_table_t() -> (TestFuzzer, sql_gen::Schema) {
        let fuzzer = test_fuzzer(SimConfig {
            seed: 7,
            generator: GeneratorKind::SqlGenProp,
            cdc: true,
            ..SimConfig::default()
        });
        fuzzer.execute_on_both("CREATE TABLE t(a INTEGER, b TEXT)", &mut Vec::new());
        let schema = fuzzer.introspect_and_verify_schemas().unwrap();
        (fuzzer, schema)
    }

    fn turso_without_cdc(fuzzer: &Fuzzer, sql: &str) {
        let conn = fuzzer.turso_conn();
        conn.execute("PRAGMA capture_data_changes_conn('off')")
            .unwrap();
        conn.execute(sql).unwrap();
        conn.execute(cdc::ENABLE_CDC).unwrap();
    }

    #[test]
    fn writes_in_autocommit_mode_and_in_a_batch_pass_the_cdc_check() {
        let (fuzzer, mut schema) = cdc_fuzzer_with_table_t();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mut matviews = Matviews::new();

        fuzzer
            .run_statement(
                0,
                &write("INSERT INTO t VALUES (1, 'x'), (2, 'y')"),
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap();
        fuzzer
            .run_batch(
                1,
                &[
                    write("UPDATE t SET b = 'z' WHERE a = 1"),
                    write("DELETE FROM t WHERE a = 2"),
                ],
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap();

        let records: Vec<String> = cdc::records_after(&fuzzer.turso_conn(), 0)
            .unwrap()
            .iter()
            .map(|r| r.to_string())
            .collect();
        assert_eq!(
            records,
            [
                "#1 INSERT sqlite_schema txn=1",
                "#2 COMMIT txn=1",
                "#3 INSERT t txn=3",
                "#4 INSERT t txn=3",
                "#5 COMMIT txn=3",
                "#6 UPDATE t txn=6",
                "#7 DELETE t txn=6",
                "#8 COMMIT txn=6"
            ]
        );
        assert_eq!(stats.oracle_failures, 0);
    }

    #[test]
    fn a_commit_record_without_a_change_before_it_fails_the_cdc_check() {
        let (fuzzer, _) = cdc_fuzzer_with_table_t();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mark = fuzzer.cdc_mark(None).unwrap();

        turso_without_cdc(
            &fuzzer,
            "INSERT INTO turso_cdc(change_type, change_txn_id) VALUES (2, 7)",
        );

        let err = fuzzer
            .check_cdc(mark, "SELECT 1", &mut stats, &mut executed_sql)
            .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CDC check failed: a COMMIT record without a change before it"),
            "{err}"
        );
        assert_eq!(stats.oracle_failures, 1);
    }

    #[test]
    fn a_row_change_without_a_record_fails_the_cdc_check() {
        let (fuzzer, schema) = cdc_fuzzer_with_table_t();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mark = fuzzer.cdc_mark(Some(&schema)).unwrap();

        turso_without_cdc(&fuzzer, "INSERT INTO t VALUES (1, 'x')");

        let err = fuzzer
            .check_cdc(
                mark,
                "INSERT INTO t VALUES (1, 'x')",
                &mut stats,
                &mut executed_sql,
            )
            .unwrap_err();
        assert!(
            err.to_string().starts_with(
                "CDC check failed: table t: the row count changed by 1, but the records add up to 0"
            ),
            "{err}"
        );
    }

    #[test]
    fn a_probability_of_zero_draws_no_random_number() {
        let fuzzer = matview_fuzzer();
        let before = fuzzer.rng.borrow().get_word_pos();
        assert!(!fuzzer.roll(0.0));
        assert_eq!(fuzzer.rng.borrow().get_word_pos(), before);
        assert!(fuzzer.roll(1.0));
        assert_ne!(fuzzer.rng.borrow().get_word_pos(), before);
    }

    #[test]
    fn test_sql_drops_a_materialized_view_whose_table_is_gone() {
        let fuzzer = matview_fuzzer();
        let (mut stats, mut executed_sql) = (SimStats::default(), Vec::new());
        let mut matviews = matview_over_t(&fuzzer, &mut executed_sql);
        let mut schema = fuzzer.introspect_and_verify_schemas().unwrap();
        let drop_table = GeneratedStatement {
            is_ddl: true,
            mutates_data: false,
            ..write("DROP TABLE t")
        };

        fuzzer
            .run_statement(
                0,
                &drop_table,
                &mut schema,
                &mut matviews,
                &mut stats,
                &mut executed_sql,
            )
            .unwrap();

        let replayed: Vec<&String> = executed_sql
            .iter()
            .filter(|s| !s.starts_with("--"))
            .collect();
        assert_eq!(
            &replayed[replayed.len() - 2..],
            ["DROP TABLE t", "DROP VIEW v"]
        );
        assert!(matviews.is_empty());
        assert_eq!(stats.oracle_failures, 0);
    }

    #[test]
    fn test_push_warning_comments_multiline() {
        let mut out = Vec::new();
        push_warning_comments(&mut out, 465, "first\nsecond");
        assert_eq!(out[0], "-- WARNING stmt=465 line=0: first");
        assert_eq!(out[1], "-- WARNING stmt=465 line=1: second");
    }

    #[test]
    fn statements_over_64_kib_are_too_large() {
        assert!(!generated_sql_is_too_large(
            &"x".repeat(MAX_GENERATED_SQL_BYTES)
        ));
        assert!(generated_sql_is_too_large(
            &"x".repeat(MAX_GENERATED_SQL_BYTES + 1)
        ));
    }
}
