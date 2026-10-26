use anyhow::Result;
use clap::{Parser, ValueEnum};
use memory_benchmark::workload::JournalMode;
use serde::Serialize;
use std::path::PathBuf;
use turso::{Builder, Connection, Database};

#[cfg(not(clippy))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Parser)]
#[command(about = "Measure the heap that one transaction keeps for each row it writes")]
struct Args {
    #[arg(long, default_value = "insert")]
    statement: Statement,
    #[arg(long, default_value = "100000")]
    rows: usize,
    #[arg(long, default_value = "100")]
    payload_bytes: usize,
    #[arg(long, default_value = "mvcc")]
    mode: JournalMode,
    #[arg(long, default_value = "dhat-heap.json")]
    dhat_file: PathBuf,
}

/// The statement that the measured transaction runs. `update`,
/// `update-counter` and `delete` change rows that a checkpoint already wrote
/// to the B-tree.
#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Statement {
    Insert,
    /// Writes a new payload into each row.
    Update,
    /// Adds one to an integer column of each row, and keeps the payload. Only
    /// this statement gives the table the integer column.
    UpdateCounter,
    Delete,
}

#[derive(Serialize)]
struct Report {
    statement: Statement,
    mode: JournalMode,
    rows: usize,
    payload_bytes: usize,
    /// Live heap after the statement, before COMMIT.
    uncommitted_bytes_per_row: f64,
    /// Highest live heap during the statement.
    statement_peak_bytes_per_row: f64,
    /// Highest live heap from the start of the statement to the end of COMMIT.
    commit_peak_bytes_per_row: f64,
    /// Live heap after COMMIT, with automatic checkpoints and GC turned off.
    committed_bytes_per_row: f64,
    /// Heap allocations that the statement made.
    statement_allocations_per_row: f64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let dir = std::env::temp_dir().join(format!("turso-mvcc-row-memory-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let db_path = dir.join("rows.db").to_string_lossy().into_owned();
    prepare_table(&db_path, &args).await?;
    let report = measure(&db_path, &args).await?;
    std::fs::remove_dir_all(&dir)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn prepare_table(db_path: &str, args: &Args) -> Result<()> {
    let (_db, conn) = open(db_path, args.mode).await?;
    let columns = match args.statement {
        Statement::UpdateCounter => "id INTEGER PRIMARY KEY, n INTEGER, v BLOB",
        _ => "id INTEGER PRIMARY KEY, v BLOB",
    };
    conn.execute(&format!("CREATE TABLE t({columns})"), ())
        .await?;
    if !matches!(args.statement, Statement::Insert) {
        conn.execute(&insert_sql(args), ()).await?;
    }
    drain(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").await?;
    Ok(())
}

async fn measure(db_path: &str, args: &Args) -> Result<Report> {
    let (_db, conn) = open(db_path, args.mode).await?;
    drain(&conn, "SELECT count(*) FROM t").await?;
    if matches!(args.mode, JournalMode::Mvcc) {
        drain(&conn, "PRAGMA mvcc_checkpoint_threshold = -1").await?;
        drain(&conn, "PRAGMA mvcc_gc_threshold = -1").await?;
    }
    let sql = match args.statement {
        Statement::Insert => insert_sql(args),
        Statement::Update => format!("UPDATE t SET v = randomblob({})", args.payload_bytes),
        Statement::UpdateCounter => "UPDATE t SET n = n + 1".to_string(),
        Statement::Delete => "DELETE FROM t".to_string(),
    };

    #[cfg(not(clippy))]
    let _profiler = dhat::Profiler::builder().file_name(&args.dhat_file).build();
    conn.execute("BEGIN", ()).await?;
    let start = dhat::HeapStats::get();
    conn.execute(&sql, ()).await?;
    let after_statement = dhat::HeapStats::get();
    conn.execute("COMMIT", ()).await?;
    let after_commit = dhat::HeapStats::get();
    release_deferred_frees(&conn).await?;
    let committed = dhat::HeapStats::get();

    let per_row = |bytes: usize| bytes as f64 / args.rows as f64;
    Ok(Report {
        statement: args.statement,
        mode: args.mode,
        rows: args.rows,
        payload_bytes: args.payload_bytes,
        uncommitted_bytes_per_row: per_row(after_statement.curr_bytes - start.curr_bytes),
        statement_peak_bytes_per_row: per_row(after_statement.max_bytes - start.curr_bytes),
        commit_peak_bytes_per_row: per_row(after_commit.max_bytes - start.curr_bytes),
        committed_bytes_per_row: per_row(committed.curr_bytes.saturating_sub(start.curr_bytes)),
        statement_allocations_per_row: (after_statement.total_blocks - start.total_blocks) as f64
            / args.rows as f64,
    })
}

/// The MVCC skip maps free removed entries only after later transactions move
/// their epoch forward, so the committed measurement runs small transactions
/// first. Without them, it also counts the write set of the finished transaction.
async fn release_deferred_frees(conn: &Connection) -> Result<()> {
    for _ in 0..1000 {
        drain(conn, "SELECT count(*) FROM sqlite_schema").await?;
    }
    Ok(())
}

fn insert_sql(args: &Args) -> String {
    let counter = match args.statement {
        Statement::UpdateCounter => "0, ",
        _ => "",
    };
    format!(
        "INSERT INTO t SELECT value, {counter}randomblob({}) FROM generate_series(1, {})",
        args.payload_bytes, args.rows
    )
}

async fn open(db_path: &str, mode: JournalMode) -> Result<(Database, Connection)> {
    let db = Builder::new_local(db_path).build().await?;
    let conn = db.connect()?;
    drain(&conn, &format!("PRAGMA journal_mode = '{mode}'")).await?;
    Ok((db, conn))
}

async fn drain(conn: &Connection, sql: &str) -> Result<()> {
    let mut rows = conn.query(sql, ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}
