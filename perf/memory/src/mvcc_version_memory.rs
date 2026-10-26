use anyhow::{Result, ensure};
use clap::Parser;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::Serialize;
use std::path::PathBuf;
use turso::{Builder, Connection, Value};

#[cfg(not(clippy))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Parser)]
#[command(about = "Measure the heap that MVCC keeps for the old versions of updated rows")]
struct Args {
    /// Rows in `t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)`.
    #[arg(long, default_value = "10000")]
    rows: usize,
    /// Length of the text in column `b`. Each update changes only column `a`.
    #[arg(long, default_value = "300")]
    text_bytes: usize,
    /// Update transactions. Each one adds 1 to `a` in one random row.
    #[arg(long, default_value = "100000")]
    updates: usize,
    /// Run the updates without a reader that keeps the snapshot from before them.
    #[arg(long)]
    no_reader: bool,
    #[arg(long, default_value = "dhat-heap.json")]
    dhat_file: PathBuf,
}

#[derive(Serialize)]
struct Report {
    rows: usize,
    text_bytes: usize,
    updates: usize,
    reader: bool,
    /// Live heap that the updates added, for each update. With a reader, GC
    /// cannot remove the old versions, so this is the cost of one old version.
    live_bytes_per_update: f64,
    /// Highest live heap during the updates, above the live heap before them.
    peak_bytes: usize,
    /// Live heap after the updates, above the live heap before them.
    live_bytes: usize,
    /// Heap allocations that the updates made, for each update.
    allocations_per_update: f64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let dir =
        std::env::temp_dir().join(format!("turso-mvcc-version-memory-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let db_path = dir.join("versions.db").to_string_lossy().into_owned();
    let report = measure(&db_path, &args).await?;
    std::fs::remove_dir_all(&dir)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn measure(db_path: &str, args: &Args) -> Result<Report> {
    let db = Builder::new_local(db_path).build().await?;
    let writer = db.connect()?;
    drain(&writer, "PRAGMA journal_mode = 'mvcc'").await?;
    writer
        .execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
            (),
        )
        .await?;
    writer
        .execute(
            &format!(
                "INSERT INTO t SELECT value, 0, '{}' FROM generate_series(1, {})",
                "x".repeat(args.text_bytes),
                args.rows
            ),
            (),
        )
        .await?;
    drain(&writer, "PRAGMA wal_checkpoint(TRUNCATE)").await?;
    drain(&writer, "PRAGMA mvcc_checkpoint_threshold = -1").await?;

    let reader = db.connect()?;
    if !args.no_reader {
        reader.execute("BEGIN CONCURRENT", ()).await?;
        drain(&reader, "SELECT count(*) FROM t").await?;
    }
    let mut update = writer
        .prepare("UPDATE t SET a = a + 1 WHERE id = ?")
        .await?;
    let mut rng = StdRng::seed_from_u64(memory_benchmark::profile::WORKLOAD_RNG_SEED);

    #[cfg(not(clippy))]
    let _profiler = dhat::Profiler::builder().file_name(&args.dhat_file).build();
    let start = dhat::HeapStats::get();
    for _ in 0..args.updates {
        let id = rng.random_range(1..=args.rows as i64);
        update.execute([Value::Integer(id)]).await?;
    }
    let end = dhat::HeapStats::get();

    if !args.no_reader {
        let mut rows = reader.query("SELECT count(*), sum(a) FROM t", ()).await?;
        let row = rows.next().await?.expect("count(*) returns one row");
        let (count, sum) = (row.get::<i64>(0)?, row.get::<i64>(1)?);
        ensure!(
            count == args.rows as i64 && sum == 0,
            "the reader must see its old snapshot, but it saw {count} rows with sum(a) = {sum}"
        );
        drop(rows);
        reader.execute("COMMIT", ()).await?;
    }

    let live_bytes = end.curr_bytes.saturating_sub(start.curr_bytes);
    Ok(Report {
        rows: args.rows,
        text_bytes: args.text_bytes,
        updates: args.updates,
        reader: !args.no_reader,
        live_bytes_per_update: live_bytes as f64 / args.updates as f64,
        peak_bytes: end.max_bytes.saturating_sub(start.curr_bytes),
        live_bytes,
        allocations_per_update: (end.total_blocks - start.total_blocks) as f64
            / args.updates as f64,
    })
}

async fn drain(conn: &Connection, sql: &str) -> Result<()> {
    let mut rows = conn.query(sql, ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}
