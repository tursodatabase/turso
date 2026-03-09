//! Turso-only custom types fuzzer.
//!
//! Exercises Turso-specific type extensions (arrays, and in the future other
//! custom types like vectors, enums, etc.) that have no SQLite equivalent.
//!
//! Uses sql_gen with `Policy::with_array_support()` and runs against Turso
//! only. Detects crashes, panics, and self-consistency violations.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_gen::{Full, Policy, SqlGen, StmtKind, StmtWeights};
use turso_core::Database;

use differential_fuzzer::memory::MemorySimIO;
use differential_fuzzer::schema::SchemaIntrospector;

/// Custom types fuzzer: Turso-only fuzzer for custom type operations.
#[derive(Parser, Debug)]
#[command(name = "custom_types_fuzzer")]
struct Args {
    /// Random seed for deterministic execution.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Number of SQL statements to generate and execute.
    #[arg(short = 'n', long, default_value_t = 200)]
    statements: usize,

    /// Number of tables to create.
    #[arg(short = 't', long, default_value_t = 2)]
    tables: usize,

    /// Print verbose output (every statement).
    #[arg(long)]
    verbose: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        "Custom types fuzzer: seed={}, statements={}, tables={}",
        args.seed,
        args.statements,
        args.tables,
    );

    let out_dir: PathBuf = "custom-types-fuzzer-output".into();
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir)?;
    }

    let mut rng = ChaCha8Rng::seed_from_u64(args.seed);

    // Create Turso in-memory database
    let io = Arc::new(MemorySimIO::new(args.seed));
    let turso_db = Database::open_file_with_flags(
        io,
        out_dir.join("array_test.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::default().with_custom_types(true),
        None,
    )?;
    let conn = turso_db.connect()?;

    // Set up sql_gen with array support
    let gen_seed: u64 = rng.next_u64();
    let mut ctx = sql_gen::Context::new_with_seed(gen_seed);
    let policy = Policy::default()
        .with_array_support()
        .with_max_expr_depth(3)
        .with_max_subquery_depth(1)
        .with_function_config(
            sql_gen::FunctionConfig::deterministic().disable(&["LIKELY", "UNLIKELY"]),
        );

    // Panic capture context
    let panic_context: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let mut executed_sql: Vec<String> = Vec::new();
    let mut stats = FuzzerStats::default();

    // Phase 1: Create tables with array columns
    tracing::info!(
        "Phase 1: Creating {} tables with array columns",
        args.tables
    );
    let create_table_policy = policy.clone().with_stmt_weights(StmtWeights {
        create_table: 100,
        select: 0,
        insert: 0,
        update: 0,
        delete: 0,
        drop_table: 0,
        alter_table: 0,
        create_index: 0,
        drop_index: 0,
        begin: 0,
        commit: 0,
        rollback: 0,
        create_trigger: 0,
        drop_trigger: 0,
        ..Default::default()
    });
    let mut schema = sql_gen::Schema::default();
    for i in 0..args.tables {
        let generator: SqlGen<Full> = SqlGen::new(schema.clone(), create_table_policy.clone());
        let create_stmt = generator
            .statement(&mut ctx)
            .map_err(|e| anyhow::anyhow!("Failed to generate CREATE TABLE: {e}"))?;
        let sql = create_stmt.to_string();

        if args.verbose {
            tracing::info!("CREATE TABLE {}: {}", i, sql);
        }

        match execute_turso(&conn, &sql) {
            Ok(_) => {
                executed_sql.push(sql);
                stats.statements_executed += 1;
            }
            Err(e) => {
                tracing::warn!("CREATE TABLE {} failed: {}", i, e);
                executed_sql.push(format!("-- ERROR: {sql} ({e})"));
                stats.errors += 1;
            }
        }

        schema = SchemaIntrospector::from_turso(&conn)
            .context("Failed to introspect schema after CREATE TABLE")?;
    }

    tracing::info!("Schema: {} tables created", schema.tables.len(),);

    // Phase 2: Generate and execute statements
    tracing::info!("Phase 2: Executing {} statements", args.statements);
    for i in 0..args.statements {
        let generator: SqlGen<Full> = SqlGen::new(schema.clone(), policy.clone());
        let stmt = match generator.statement(&mut ctx) {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!("Failed to generate statement {}: {}", i, e);
                stats.gen_failures += 1;
                continue;
            }
        };
        let sql = stmt.to_string();
        let is_ddl = StmtKind::from(&stmt).is_ddl();

        if args.verbose {
            tracing::info!("Statement {}: {}", i, sql);
        }

        // Execute with catch_unwind to detect panics
        let ctx_clone = Arc::clone(&panic_context);
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let bt = std::backtrace::Backtrace::force_capture();
            *ctx_clone.lock() = Some(format!("{info}\n{bt}"));
        }));

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute_turso(&conn, &sql)));

        std::panic::set_hook(prev_hook);

        match result {
            Ok(Ok(_)) => {
                stats.statements_executed += 1;
                executed_sql.push(sql.clone());
            }
            Ok(Err(e)) => {
                stats.errors += 1;
                let err_msg = e.to_string();
                if is_internal_error(&err_msg) {
                    stats.internal_errors += 1;
                    tracing::error!("INTERNAL ERROR at statement {}: {}", i, err_msg);
                    tracing::error!("SQL: {}", sql);
                } else if args.verbose {
                    tracing::debug!("Statement {} error: {}", i, e);
                }
                executed_sql.push(format!("-- ERROR: {sql} ({e})"));
            }
            Err(panic) => {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "Unknown panic".to_string());
                let bt = panic_context.lock().take().unwrap_or_default();
                stats.panics += 1;
                tracing::error!("PANIC at statement {}: {}", i, msg);
                tracing::error!("Panicking SQL: {}", sql);
                tracing::error!("Backtrace:\n{}", bt);
                executed_sql.push(format!("-- PANIC: {sql}"));
                // A panic is a bug — fail immediately
                write_sql_file(&out_dir, &executed_sql)?;
                print_stats(&stats, &args);
                anyhow::bail!("Panic during statement {i}: {msg}\n  SQL: {sql}\n{bt}");
            }
        }

        // Re-introspect after DDL
        if is_ddl {
            schema = SchemaIntrospector::from_turso(&conn)
                .context("Failed to introspect schema after DDL")?;
        }

        // Periodic self-consistency checks every 50 statements
        if (i + 1) % 50 == 0 {
            run_consistency_checks(&conn, &schema, &mut stats, &mut executed_sql, args.verbose)?;
        }
    }

    // Phase 3: Final checks
    tracing::info!("Phase 3: Final consistency checks");
    run_consistency_checks(&conn, &schema, &mut stats, &mut executed_sql, args.verbose)?;

    // Integrity check
    let integrity_sql = "PRAGMA integrity_check";
    executed_sql.push(integrity_sql.to_string());
    match execute_turso_rows(&conn, integrity_sql) {
        Ok(rows) => {
            if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] == "ok" {
                tracing::info!("PRAGMA integrity_check: ok");
            } else {
                stats.consistency_failures += 1;
                tracing::error!("PRAGMA integrity_check failed: {:?}", rows);
            }
        }
        Err(e) => {
            stats.consistency_failures += 1;
            tracing::error!("PRAGMA integrity_check error: {}", e);
        }
    }

    write_sql_file(&out_dir, &executed_sql)?;
    print_stats(&stats, &args);

    if stats.panics > 0 || stats.consistency_failures > 0 || stats.internal_errors > 0 {
        anyhow::bail!(
            "Custom types fuzzer found {} panics, {} consistency failures, {} internal errors",
            stats.panics,
            stats.consistency_failures,
            stats.internal_errors,
        );
    }

    tracing::info!("Custom types fuzzer completed successfully");
    Ok(())
}

/// Self-consistency checks on array data.
fn run_consistency_checks(
    conn: &Arc<turso_core::Connection>,
    schema: &sql_gen::Schema,
    stats: &mut FuzzerStats,
    executed_sql: &mut Vec<String>,
    verbose: bool,
) -> Result<()> {
    for table in &schema.tables {
        let array_cols: Vec<_> = table
            .columns
            .iter()
            .filter(|c| c.data_type.is_array())
            .collect();

        if array_cols.is_empty() {
            continue;
        }

        // Check 1: SELECT array columns round-trips without error
        for col in &array_cols {
            let sql = format!("SELECT \"{}\" FROM \"{}\"", col.name, table.name);
            if verbose {
                tracing::debug!("Consistency check: {}", sql);
            }
            match execute_turso(conn, &sql) {
                Ok(_) => {}
                Err(e) => {
                    stats.consistency_failures += 1;
                    tracing::error!("Array round-trip failed: {} ({})", sql, e);
                    executed_sql.push(format!("-- CONSISTENCY FAIL: {sql} ({e})"));
                }
            }
        }

        // Check 2: INSERT-then-SELECT round-trip
        {
            let test_table = format!("__array_check_{}", table.name);
            let arr_col = array_cols.first().unwrap();
            let create_sql = format!(
                "CREATE TABLE IF NOT EXISTS \"{}\" (id INTEGER PRIMARY KEY, arr {}) STRICT",
                test_table, arr_col.data_type
            );
            let insert_sql =
                format!("INSERT INTO \"{test_table}\" (id, arr) VALUES (1, ARRAY[1,2,3])");
            let select_sql = format!("SELECT array_length(arr) FROM \"{test_table}\" WHERE id = 1");
            let drop_sql = format!("DROP TABLE IF EXISTS \"{test_table}\"");

            // Best-effort: ignore errors from CREATE/INSERT (table may already exist, etc.)
            let _ = execute_turso(conn, &create_sql);
            let _ = execute_turso(conn, &insert_sql);
            if let Ok(rows) = execute_turso_rows(conn, &select_sql) {
                if rows.len() == 1 && rows[0].len() == 1 && rows[0][0] == "3" {
                    if verbose {
                        tracing::debug!("INSERT-then-SELECT round-trip: ok");
                    }
                } else if !rows.is_empty() {
                    // Only flag if we got data back but it was wrong
                    let got = &rows[0];
                    if got.len() == 1 && got[0] != "3" && got[0] != "null" {
                        stats.consistency_failures += 1;
                        tracing::error!(
                            "INSERT-then-SELECT round-trip: expected array_length=3, got {:?}",
                            got
                        );
                        executed_sql.push(format!(
                            "-- CONSISTENCY FAIL: round-trip for {}",
                            table.name
                        ));
                    }
                }
            }
            let _ = execute_turso(conn, &drop_sql);
        }

        // Check 3: algebraic identity — array_length(array_append(arr, 99)) = array_length(arr) + 1
        for col in &array_cols {
            let sql = format!(
                "SELECT \"{}\" FROM \"{}\" WHERE \"{}\" IS NOT NULL LIMIT 5",
                col.name, table.name, col.name
            );
            if let Ok(rows) = execute_turso_rows(conn, &sql) {
                if !rows.is_empty() {
                    // Verify the identity on the table directly
                    let check_sql = format!(
                        "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" IS NOT NULL AND \
                         array_length(array_append(\"{}\", 99)) != array_length(\"{}\") + 1",
                        table.name, col.name, col.name, col.name
                    );
                    if let Ok(check_rows) = execute_turso_rows(conn, &check_sql) {
                        if check_rows.len() == 1
                            && check_rows[0].len() == 1
                            && check_rows[0][0] != "0"
                        {
                            stats.consistency_failures += 1;
                            tracing::error!(
                                "Algebraic identity violated for {}.{}: {} rows differ",
                                table.name,
                                col.name,
                                check_rows[0][0]
                            );
                            executed_sql.push(format!(
                                "-- CONSISTENCY FAIL: algebraic identity {}.{}",
                                table.name, col.name
                            ));
                        }
                    }
                }
            }
        }

        // Check 4: equivalent query — WHERE arr @> ARRAY[1] should match WHERE array_contains(arr, 1) = 1
        for col in &array_cols {
            let sql_contains_op = format!(
                "SELECT COUNT(*) FROM \"{}\" WHERE \"{}\" @> ARRAY[1]",
                table.name, col.name
            );
            let sql_contains_fn = format!(
                "SELECT COUNT(*) FROM \"{}\" WHERE array_contains(\"{}\", 1) = 1",
                table.name, col.name
            );
            if let (Ok(rows_op), Ok(rows_fn)) = (
                execute_turso_rows(conn, &sql_contains_op),
                execute_turso_rows(conn, &sql_contains_fn),
            ) {
                if rows_op.len() == 1
                    && rows_fn.len() == 1
                    && rows_op[0].len() == 1
                    && rows_fn[0].len() == 1
                    && rows_op[0][0] != rows_fn[0][0]
                {
                    stats.consistency_failures += 1;
                    tracing::error!(
                        "Equivalent query mismatch for {}.{}: @> gave {}, array_contains gave {}",
                        table.name,
                        col.name,
                        rows_op[0][0],
                        rows_fn[0][0]
                    );
                    executed_sql.push(format!(
                        "-- CONSISTENCY FAIL: equivalent query {}.{}",
                        table.name, col.name
                    ));
                }
            }
        }

        // Check 5: array_length returns non-negative or NULL
        for col in &array_cols {
            let sql = format!(
                "SELECT array_length(\"{}\") FROM \"{}\"",
                col.name, table.name
            );
            if verbose {
                tracing::debug!("Consistency check: {}", sql);
            }
            match execute_turso_rows(conn, &sql) {
                Ok(rows) => {
                    for row in &rows {
                        if row.len() == 1 {
                            let val = &row[0];
                            // Should be NULL or a non-negative integer
                            if val != "null" && val != "NULL" {
                                if let Ok(n) = val.parse::<i64>() {
                                    if n < 0 {
                                        stats.consistency_failures += 1;
                                        tracing::error!(
                                            "array_length returned negative: {} for {}.{}",
                                            n,
                                            table.name,
                                            col.name
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    stats.consistency_failures += 1;
                    tracing::error!("array_length check failed: {} ({})", sql, e);
                    executed_sql.push(format!("-- CONSISTENCY FAIL: {sql} ({e})"));
                }
            }
        }
    }
    Ok(())
}

/// Execute a SQL statement on Turso (no rows expected).
fn execute_turso(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<()> {
    conn.execute(sql)
        .map_err(|e| anyhow::anyhow!("Turso error: {e}"))
}

/// Execute a SQL query on Turso and return rows as strings.
fn execute_turso_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<Vec<Vec<String>>> {
    let mut rows_result = conn
        .query(sql)
        .map_err(|e| anyhow::anyhow!("Turso query error: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("No result from query"))?;

    let mut rows = Vec::new();
    rows_result
        .run_with_row_callback(|row| {
            let mut values = Vec::with_capacity(row.len());
            for col in 0..row.len() {
                let val = row.get_value(col);
                match val {
                    turso_core::Value::Null => {
                        values.push("null".to_string());
                    }
                    turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => {
                        values.push(i.to_string());
                    }
                    turso_core::Value::Numeric(turso_core::Numeric::Float(f)) => {
                        values.push(f.to_string());
                    }
                    turso_core::Value::Text(s) => {
                        values.push(s.as_str().to_string());
                    }
                    turso_core::Value::Blob(b) => {
                        let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                        values.push(format!("x'{hex}'"));
                    }
                }
            }
            rows.push(values);
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("Row callback error: {e}"))?;

    Ok(rows)
}

fn write_sql_file(out_dir: &std::path::Path, statements: &[String]) -> Result<()> {
    let path = out_dir.join("array_test.sql");
    let mut file = std::fs::File::create(&path)?;
    for sql in statements {
        writeln!(file, "{sql};")?;
    }
    tracing::info!(
        "Wrote {} statements to {}",
        statements.len(),
        path.display()
    );
    Ok(())
}

#[derive(Debug, Default)]
struct FuzzerStats {
    statements_executed: usize,
    errors: usize,
    panics: usize,
    gen_failures: usize,
    consistency_failures: usize,
    internal_errors: usize,
}

fn print_stats(stats: &FuzzerStats, args: &Args) {
    println!("\n--- Array Fuzzer Results ---");
    println!("Seed:                  {}", args.seed);
    println!("Target statements:     {}", args.statements);
    println!("Statements executed:   {}", stats.statements_executed);
    println!("Generation failures:   {}", stats.gen_failures);
    println!("Execution errors:      {}", stats.errors);
    println!("Panics:                {}", stats.panics);
    println!("Consistency failures:  {}", stats.consistency_failures);
    println!("Internal errors:       {}", stats.internal_errors);
    if stats.panics == 0 && stats.consistency_failures == 0 && stats.internal_errors == 0 {
        println!("Status:                PASSED");
    } else {
        println!("Status:                FAILED");
    }
    println!("----------------------------");
}

/// Returns true if the error message indicates an internal/unexpected error.
fn is_internal_error(msg: &str) -> bool {
    let msg_lower = msg.to_lowercase();
    msg_lower.contains("internal error")
        || msg_lower.contains("assertion failed")
        || msg_lower.contains("unreachable")
        || msg_lower.contains("unwrap() on none")
        || msg_lower.contains("called `option::unwrap()` on a `none`")
        || msg_lower.contains("index out of bounds")
}
