//! Runs random statements with INSERT, UPDATE and DELETE inside WITH on
//! tursopg and on a real PostgreSQL server, and fails on the first difference.
//!
//! After every statement it compares the outcome (rows or error), the command
//! tag, and the full contents of every table. Rows are compared as multisets,
//! because the statements do not fix the order of their rows.
//!
//! ```text
//! cargo build -p tursopg -p turso_pg_differential
//! target/debug/pg-differential --postgres-dsn postgres://postgres@127.0.0.1:5432/postgres
//! ```

mod generator;

use anyhow::{bail, Context, Result};
use clap::Parser;
use generator::{Case, Generator};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use turso_pg_client::{error_message, BackendEvent, ConnParams, PgConn};

#[derive(Parser)]
struct Args {
    /// PostgreSQL server to compare against. The user must be allowed to
    /// create and drop databases.
    #[arg(long)]
    postgres_dsn: String,
    /// The tursopg binary to test.
    #[arg(long, default_value = "target/debug/tursopg")]
    tursopg: PathBuf,
    /// Seed of the first case; case N uses seed + N.
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, default_value_t = 200)]
    cases: u64,
    /// Statements per case, after the tables are created and filled.
    #[arg(long, default_value_t = 8)]
    statements: usize,
    /// Run each case in one transaction, with a savepoint around every
    /// statement. Catches a failed statement that leaves some of its writes.
    #[arg(long)]
    transaction: bool,
    /// Run tursopg with the MVCC journal mode.
    #[arg(long)]
    mvcc: bool,
    /// Give the tables foreign keys with ON DELETE actions, and turn on
    /// foreign key enforcement in tursopg (PostgreSQL always enforces them).
    #[arg(long)]
    foreign_keys: bool,
    /// Where to write the SQL of a failing case.
    #[arg(long, default_value = "pg-differential-failure.sql")]
    failure_file: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let seed = args.seed.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    });
    let postgres = PostgresDatabase::create(&args.postgres_dsn)?;
    let mut tursopg = Tursopg::spawn(&args.tursopg)?;
    let mut postgres_conn = postgres.connect()?;
    if args.mvcc {
        expect_ok(&mut tursopg.conn, "SET journal_mode = 'mvcc'")?;
    }
    if args.foreign_keys {
        expect_ok(&mut tursopg.conn, "SET foreign_keys = on")?;
    }
    println!(
        "seed {seed}, {} cases of {} statements{}{}{}",
        args.cases,
        args.statements,
        if args.transaction {
            ", in transactions"
        } else {
            ""
        },
        if args.mvcc { ", MVCC" } else { "" },
        if args.foreign_keys {
            ", foreign keys"
        } else {
            ""
        },
    );
    let mut stats = Stats::default();
    for case_number in 0..args.cases {
        let case_seed = seed + case_number;
        let case = Generator::new(ChaCha8Rng::seed_from_u64(case_seed), args.foreign_keys)
            .case(args.statements);
        let result = run_case(
            &case,
            args.transaction,
            &mut tursopg.conn,
            &mut postgres_conn,
            &mut stats,
        );
        if let Err(difference) = result {
            let sql = case_sql(&case);
            std::fs::write(&args.failure_file, &sql)?;
            println!("case seed {case_seed} differs: {difference:#}");
            println!("SQL of the case written to {}", args.failure_file.display());
            bail!("tursopg and PostgreSQL differ");
        }
        // A case may leave the connection inside a failed transaction; reset both sides.
        drop_tables(&case, &mut tursopg.conn, &mut postgres_conn)?;
        if (case_number + 1) % 50 == 0 {
            println!("{} cases passed", case_number + 1);
        }
    }
    println!("all {} cases passed", args.cases);
    println!(
        "{} statements: {} failed on both sides, {} returned rows ({} rows), {} changed a table; {} cases stopped at a known limitation",
        stats.statements,
        stats.failed,
        stats.returned_rows,
        stats.rows,
        stats.changed_tables,
        stats.stopped_at_limitation
    );
    Ok(())
}

/// What the compared statements did, to show that a run tested something.
#[derive(Default)]
struct Stats {
    statements: u64,
    failed: u64,
    returned_rows: u64,
    rows: u64,
    changed_tables: u64,
    stopped_at_limitation: u64,
}

/// Errors of statements that the engine rejects although PostgreSQL runs them.
/// PostgreSQL runs foreign key actions after all parts of a statement. The
/// engine runs them inside the part that causes them, so it rejects statements
/// where an action changes a table another part writes.
const KNOWN_LIMITATIONS: &[&str] = &["a foreign key action of"];

fn run_case(
    case: &Case,
    transaction: bool,
    tursopg: &mut PgConn,
    postgres: &mut PgConn,
    stats: &mut Stats,
) -> Result<()> {
    for sql in &case.setup {
        let turso = run(tursopg, sql)?;
        let pg = run(postgres, sql)?;
        if let Some(error) = turso.error.or(pg.error) {
            bail!("setup statement failed: {sql}: {error}");
        }
    }
    if transaction {
        both_ok(tursopg, postgres, "BEGIN")?;
    }
    let mut tables = table_contents(case, postgres)?;
    for (index, sql) in case.statements.iter().enumerate() {
        if transaction {
            both_ok(tursopg, postgres, "SAVEPOINT s")?;
        }
        let turso = run(tursopg, sql)?;
        let pg = run(postgres, sql)?;
        if pg.error.is_none()
            && turso.error.as_deref().is_some_and(|error| {
                KNOWN_LIMITATIONS
                    .iter()
                    .any(|limitation| error.contains(limitation))
            })
        {
            // The two databases now differ, so the rest of the case is not comparable.
            stats.stopped_at_limitation += 1;
            return Ok(());
        }
        compare_outcomes(&turso, &pg).with_context(|| format!("statement {index}: {sql}"))?;
        if transaction {
            let end = if pg.error.is_some() {
                "ROLLBACK TO SAVEPOINT s"
            } else {
                "RELEASE SAVEPOINT s"
            };
            both_ok(tursopg, postgres, end)?;
        }
        compare_tables(case, tursopg, postgres)
            .with_context(|| format!("tables after statement {index}: {sql}"))?;

        stats.statements += 1;
        if pg.error.is_some() {
            stats.failed += 1;
        } else if !pg.rows.is_empty() {
            stats.returned_rows += 1;
            stats.rows += pg.rows.len() as u64;
        }
        let tables_after = table_contents(case, postgres)?;
        if tables_after != tables {
            stats.changed_tables += 1;
        }
        tables = tables_after;
    }
    if transaction {
        both_ok(tursopg, postgres, "COMMIT")?;
        compare_tables(case, tursopg, postgres).context("tables after COMMIT")?;
    }
    Ok(())
}

fn table_contents(case: &Case, conn: &mut PgConn) -> Result<Vec<Vec<Vec<Option<String>>>>> {
    case.tables
        .iter()
        .map(|table| Ok(run(conn, &table_query(table))?.rows))
        .collect()
}

fn table_query(table: &str) -> String {
    format!("SELECT id, a, b, u FROM {table} ORDER BY id")
}

/// Words in an error that point to a bug in the engine rather than in the
/// statement, so the error is a failure even when PostgreSQL also fails.
const ENGINE_BUG_ERRORS: &[&str] = &["internal", "panic", "assert", "unreachable", "corrupt"];

fn compare_outcomes(turso: &Outcome, pg: &Outcome) -> Result<()> {
    match (&turso.error, &pg.error) {
        (None, None) => {}
        (Some(error), Some(_)) => {
            let lowercase = error.to_lowercase();
            if ENGINE_BUG_ERRORS
                .iter()
                .any(|word| lowercase.contains(word))
            {
                bail!("both failed, but tursopg with an engine error: {error}");
            }
            return Ok(());
        }
        (Some(error), None) => bail!("tursopg failed but PostgreSQL succeeded: {error}"),
        (None, Some(error)) => bail!("PostgreSQL failed but tursopg succeeded: {error}"),
    }
    let mut turso_rows = turso.rows.clone();
    let mut pg_rows = pg.rows.clone();
    turso_rows.sort();
    pg_rows.sort();
    if turso_rows != pg_rows {
        bail!(
            "rows differ\n  tursopg:    {}\n  PostgreSQL: {}",
            render_rows(&turso_rows),
            render_rows(&pg_rows)
        );
    }
    if turso.tag != pg.tag {
        bail!(
            "command tags differ: tursopg {:?}, PostgreSQL {:?}",
            turso.tag,
            pg.tag
        );
    }
    Ok(())
}

fn compare_tables(case: &Case, tursopg: &mut PgConn, postgres: &mut PgConn) -> Result<()> {
    for table in &case.tables {
        let sql = table_query(table);
        let turso = run(tursopg, &sql)?;
        let pg = run(postgres, &sql)?;
        if let Some(error) = turso.error.or(pg.error) {
            bail!("reading {table} failed: {error}");
        }
        if turso.rows != pg.rows {
            bail!(
                "table {table} differs\n  tursopg:    {}\n  PostgreSQL: {}",
                render_rows(&turso.rows),
                render_rows(&pg.rows)
            );
        }
    }
    Ok(())
}

fn drop_tables(case: &Case, tursopg: &mut PgConn, postgres: &mut PgConn) -> Result<()> {
    for conn in [tursopg, postgres] {
        // ROLLBACK outside a transaction only warns.
        run(conn, "ROLLBACK")?;
        // Children first: their foreign keys reference the first table.
        for table in case.tables.iter().rev() {
            let outcome = run(conn, &format!("DROP TABLE IF EXISTS {table}"))?;
            if let Some(error) = outcome.error {
                bail!("dropping {table} failed: {error}");
            }
        }
    }
    Ok(())
}

fn expect_ok(conn: &mut PgConn, sql: &str) -> Result<()> {
    if let Some(error) = run(conn, sql)?.error {
        bail!("{sql} failed: {error}");
    }
    Ok(())
}

fn both_ok(tursopg: &mut PgConn, postgres: &mut PgConn, sql: &str) -> Result<()> {
    for (name, conn) in [("tursopg", tursopg), ("PostgreSQL", postgres)] {
        if let Some(error) = run(conn, sql)?.error {
            bail!("{name} failed on {sql}: {error}");
        }
    }
    Ok(())
}

struct Outcome {
    rows: Vec<Vec<Option<String>>>,
    tag: Option<String>,
    error: Option<String>,
}

fn run(conn: &mut PgConn, sql: &str) -> Result<Outcome> {
    let events = conn
        .simple_query(sql)
        .with_context(|| format!("connection failed while running: {sql}"))?;
    let mut outcome = Outcome {
        rows: Vec::new(),
        tag: None,
        error: None,
    };
    for event in events {
        match event {
            BackendEvent::DataRow(row) => outcome.rows.push(row),
            BackendEvent::CommandComplete(tag) => outcome.tag = Some(tag),
            BackendEvent::ErrorResponse(fields) => {
                outcome.error = Some(error_message(&fields).to_string());
            }
            _ => {}
        }
    }
    Ok(outcome)
}

fn render_rows(rows: &[Vec<Option<String>>]) -> String {
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| {
            let values: Vec<&str> = row
                .iter()
                .map(|value| value.as_deref().unwrap_or("NULL"))
                .collect();
            format!("({})", values.join(", "))
        })
        .collect();
    format!("[{}]", rendered.join(", "))
}

fn case_sql(case: &Case) -> String {
    let mut sql = String::new();
    for statement in case.setup.iter().chain(&case.statements) {
        sql.push_str(statement);
        sql.push_str(";\n");
    }
    sql
}

/// A scratch database on the PostgreSQL server, dropped when the run ends.
struct PostgresDatabase {
    admin: ConnParams,
    name: String,
}

impl PostgresDatabase {
    fn create(dsn: &str) -> Result<Self> {
        let admin = ConnParams::parse(dsn)?;
        let name = format!("pg_differential_{}", std::process::id());
        let mut conn = PgConn::connect(&admin, &[])?;
        for sql in [
            format!("DROP DATABASE IF EXISTS {name}"),
            format!("CREATE DATABASE {name}"),
        ] {
            if let Some(error) = run(&mut conn, &sql)?.error {
                bail!("{sql} failed: {error}");
            }
        }
        Ok(Self { admin, name })
    }

    fn connect(&self) -> Result<PgConn> {
        let mut params = self.admin.clone();
        params.database.clone_from(&self.name);
        Ok(PgConn::connect(&params, &[])?)
    }
}

impl Drop for PostgresDatabase {
    fn drop(&mut self) {
        if let Ok(mut conn) = PgConn::connect(&self.admin, &[]) {
            let _ = run(
                &mut conn,
                &format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name),
            );
        }
    }
}

/// A tursopg server on an in-memory database, killed when the run ends.
struct Tursopg {
    child: Child,
    conn: PgConn,
}

impl Tursopg {
    fn spawn(binary: &PathBuf) -> Result<Self> {
        let mut child = Command::new(binary)
            .args([":memory:", "--server", "127.0.0.1:0", "-q"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawning {}", binary.display()))?;
        let stdout = child.stdout.take().context("tursopg has no stdout")?;
        let mut lines = BufReader::new(stdout).lines();
        let port = loop {
            let line = lines
                .next()
                .context("tursopg exited before it reported its address")??;
            let Some(rest) = line.strip_prefix("PostgreSQL server listening on ") else {
                continue;
            };
            let address = rest.split_whitespace().next().context("no address")?;
            break address
                .rsplit_once(':')
                .context("no port")?
                .1
                .parse::<u16>()?;
        };
        std::thread::spawn(move || for _ in lines {});
        let params = ConnParams {
            host: "127.0.0.1".to_string(),
            port,
            user: "pg_differential".to_string(),
            password: None,
            database: "main".to_string(),
        };
        let mut attempts = 0;
        let conn = loop {
            match PgConn::connect(&params, &[]) {
                Ok(conn) => break conn,
                Err(_) if attempts < 100 => {
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => return Err(e.into()),
            }
        };
        Ok(Self { child, conn })
    }
}

impl Drop for Tursopg {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}
