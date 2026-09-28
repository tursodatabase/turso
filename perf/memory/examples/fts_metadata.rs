use anyhow::{Result, ensure};
use clap::{Parser, ValueEnum};
use memory_benchmark::workload::JournalMode;
use std::{path::PathBuf, sync::Arc, time::Instant};
use turso_core::IO;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Clone, Copy, Parser)]
#[command(about = "Measure warm FTS metadata scans and row-delete batches")]
struct Workload {
    #[arg(long, default_value_t = 10000)]
    documents: usize,
    #[arg(long, default_value_t = 0)]
    tombstones: usize,
    #[arg(long, default_value_t = 100)]
    operations: usize,
    #[arg(long, value_enum, default_value = "open")]
    operation: Operation,
    #[arg(long, default_value = "mvcc")]
    mode: JournalMode,
}

#[derive(Clone, Copy, ValueEnum)]
enum Operation {
    Open,
    Delete,
}

#[derive(Parser)]
struct Args {
    #[command(flatten)]
    workload: Workload,
    #[arg(long)]
    dhat_file: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = args.workload;
    ensure!(
        config.documents > 0 && config.operations > 0,
        "counts must be positive"
    );
    ensure!(
        config.tombstones < config.documents,
        "need surviving documents"
    );
    if matches!(config.operation, Operation::Delete) {
        ensure!(
            config.operations <= config.documents - config.tombstones,
            "not enough documents to delete"
        );
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fts.db");
    let db = turso::Builder::new_local(path.to_str().unwrap())
        .experimental_index_method(true)
        .build()
        .await?;
    let conn = db.connect()?;
    conn.pragma_update(
        "journal_mode",
        match config.mode {
            JournalMode::Wal => "'wal'",
            JournalMode::Mvcc => "'mvcc'",
        },
    )
    .await?;
    conn.pragma_update("fts_merge_threshold", "0").await?;
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", ())
        .await?;
    conn.execute("BEGIN", ()).await?;
    let mut insert = conn
        .prepare("INSERT INTO docs VALUES (?1, 'common word')")
        .await?;
    for id in 0..config.documents {
        insert.execute([id as i64]).await?;
    }
    drop(insert);
    conn.execute("COMMIT", ()).await?;
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)", ())
        .await?;
    if config.tombstones != 0 {
        let deleted = conn
            .execute("DELETE FROM docs WHERE id < ?1", [config.tombstones as i64])
            .await?;
        ensure!(
            deleted == config.tombstones as u64,
            "wrong setup delete count"
        );
    }
    let (segments, tombstones) = backing_counts(&path)?;
    ensure!(
        segments == config.documents.div_ceil(1000),
        "unexpected segment count: {segments}"
    );
    ensure!(
        tombstones == config.tombstones,
        "unexpected tombstone count"
    );
    let sql = "SELECT id FROM docs WHERE fts_match(body, 'absent')";
    let mut rows = conn.query(sql, ()).await?;
    ensure!(rows.next().await?.is_none(), "unexpected search hit");
    drop(rows);
    let profiler = args
        .dhat_file
        .as_ref()
        .map(|path| dhat::Profiler::builder().file_name(path).build());
    let start = Instant::now();
    match config.operation {
        Operation::Open => {
            for _ in 0..config.operations {
                let mut rows = conn.query(sql, ()).await?;
                ensure!(rows.next().await?.is_none(), "unexpected search hit");
            }
        }
        Operation::Delete => {
            conn.execute("BEGIN", ()).await?;
            let deleted = conn
                .execute(
                    "DELETE FROM docs WHERE id >= ?1 AND id < ?2",
                    turso::params![
                        config.tombstones as i64,
                        (config.tombstones + config.operations) as i64
                    ],
                )
                .await?;
            ensure!(
                deleted == config.operations as u64,
                "wrong measured delete count"
            );
            conn.execute("ROLLBACK", ()).await?;
        }
    }
    let elapsed = start.elapsed();
    let heap = profiler.as_ref().map(|_| {
        let stats = dhat::HeapStats::get();
        serde_json::json!({"bytes": stats.total_bytes, "allocations": stats.total_blocks, "peak_bytes": stats.max_bytes})
    });
    drop(profiler);
    println!(
        "{}",
        serde_json::json!({
            "documents": config.documents, "segments": segments, "tombstones": tombstones,
            "mode": config.mode, "operation": config.operation.to_possible_value().unwrap().get_name(),
            "operations": config.operations, "elapsed_us": elapsed.as_micros(), "heap": heap,
        })
    );
    Ok(())
}

fn backing_counts(path: &std::path::Path) -> Result<(usize, usize)> {
    let io = Arc::new(turso_core::PlatformIO::new()?);
    let db = turso_core::Database::open(
        io.clone(),
        path.to_str().unwrap(),
        turso_core::OpenOptions::new(Arc::new(turso_core::SqliteDialect))
            .db_opts(turso_core::DatabaseOpts::new().with_index_method(true)),
    )?;
    let conn = db.connect()?;
    conn.execute("BEGIN")?;
    conn.execute("SELECT count(*) FROM docs")?;
    let mut dumper = turso_core::index_method::fts::FtsBackingRowDumper::new(
        &conn,
        turso_core::MAIN_DB_ID,
        "docs_fts",
    )?;
    loop {
        match dumper.step()? {
            turso_core::IOResult::Done(()) => break,
            turso_core::IOResult::IO(completions) => {
                while !completions.finished() {
                    io.step()?;
                }
            }
        }
    }
    let segments = dumper
        .rows
        .iter()
        .filter(|(path, _, _, _)| path.starts_with("fts2/seg/"))
        .count();
    let tombstones = dumper
        .rows
        .iter()
        .filter(|(path, _, _, _)| path.starts_with("fts2/tomb/"))
        .count();
    drop(dumper);
    conn.execute("ROLLBACK")?;
    conn.close()?;
    Ok((segments, tombstones))
}
