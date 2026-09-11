//! Multithreaded MVCC checkpoint benchmark: TRUNCATE (blocking) vs PASSIVE (non-blocking).
//!
//! Concurrent writer and reader tasks hammer a small table while the auto-checkpoint fires
//! aggressively (low `mvcc_checkpoint_threshold`). The blocking Truncate checkpoint takes the
//! global lock for the whole checkpoint, so transactions stall whenever one runs — visible as
//! tail-latency spikes and throughput dips. The passive checkpoint only contends for a brief
//! publish window. We record per-operation latency with timestamps, so the output includes a
//! time series (CSV + self-contained SVG plots) that makes contention visible.
//!
//! File sizes (db / -wal / logical log) are reported per mode as a "did checkpointing keep up"
//! signal: if checkpoints fall behind, the logical log and WAL grow without bound during the run.
//!
//! Example:
//!   cargo run --profile bench-profile -p mvcc-checkpoint-bench -- \
//!       --writers 8 --readers 8 --duration 15 --rows 2000 --checkpoint-threshold 4096

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::sync::Mutex;
use turso::Builder;

#[derive(Parser, Clone)]
#[command(about = "Compare TRUNCATE vs PASSIVE MVCC checkpoint latency/throughput")]
struct Args {
    /// Concurrent writer tasks (each runs BEGIN CONCURRENT / UPDATE / COMMIT in a loop).
    #[arg(long, default_value_t = 4)]
    writers: usize,
    /// Concurrent reader tasks (each runs point SELECTs in a loop).
    #[arg(long, default_value_t = 4)]
    readers: usize,
    /// Measurement duration per mode, in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,
    /// Rows seeded into the table (also the key space writers/readers touch).
    #[arg(long, default_value_t = 1000)]
    rows: i64,
    /// `PRAGMA mvcc_checkpoint_threshold` in bytes: 0 = checkpoint on every commit,
    /// N > 0 = when the logical log exceeds N bytes, -1 = never.
    #[arg(long, default_value_t = 0)]
    checkpoint_threshold: i64,
    /// `PRAGMA synchronous`: full = fsync per commit (durable, default), normal/off = relax fsync.
    #[arg(long, default_value = "full")]
    synchronous: String,
    /// Which mode(s) to run.
    #[arg(long, value_enum, default_value_t = ModeArg::Both)]
    mode: ModeArg,
    /// Time-series bucket width in milliseconds.
    #[arg(long, default_value_t = 200)]
    bucket_ms: u32,
    /// Output path prefix for the CSV and HTML report.
    #[arg(long, default_value = "checkpoint-bench-out")]
    out: String,
    /// Tokio worker threads (0 = tokio default = number of cores).
    #[arg(long, default_value_t = 0)]
    worker_threads: usize,
}

#[derive(clap::ValueEnum, Clone, Copy, PartialEq)]
enum ModeArg {
    Truncate,
    Passive,
    Both,
}

/// One operation: milliseconds since the measurement window started, and its latency (us).
type Sample = (u32, u32);

struct ModeData {
    writes: Vec<Sample>,
    reads: Vec<Sample>,
    write_conflicts: u64,
    elapsed_secs: f64,
    files: Vec<(String, u64)>,
}

struct Stats {
    count: u64,
    per_sec: f64,
    mean: f64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

impl Stats {
    fn compute(samples: &[Sample], elapsed_secs: f64) -> Self {
        let mut lat: Vec<u64> = samples.iter().map(|&(_, l)| l as u64).collect();
        let count = lat.len() as u64;
        if lat.is_empty() {
            return Stats {
                count: 0,
                per_sec: 0.0,
                mean: 0.0,
                p50: 0,
                p90: 0,
                p99: 0,
                p999: 0,
                max: 0,
            };
        }
        lat.sort_unstable();
        let sum: u128 = lat.iter().map(|&x| x as u128).sum();
        let pct =
            |p: f64| lat[((p * (lat.len() as f64 - 1.0)).round() as usize).min(lat.len() - 1)];
        Stats {
            count,
            per_sec: count as f64 / elapsed_secs,
            mean: sum as f64 / lat.len() as f64,
            p50: pct(0.50),
            p90: pct(0.90),
            p99: pct(0.99),
            p999: pct(0.999),
            max: *lat.last().unwrap(),
        }
    }
}

/// Per-time-bucket aggregate for the time series.
struct Bucket {
    t_ms: u32,
    throughput: f64,
    p50: u64,
    p99: u64,
    max: u64,
}

fn bucketize(samples: &[Sample], bucket_ms: u32, duration_ms: u32) -> Vec<Bucket> {
    let n = (duration_ms / bucket_ms) as usize + 1;
    let mut buckets: Vec<Vec<u64>> = vec![Vec::new(); n];
    for &(t, l) in samples {
        let b = (t / bucket_ms) as usize;
        if b < n {
            buckets[b].push(l as u64);
        }
    }
    let secs = bucket_ms as f64 / 1000.0;
    buckets
        .into_iter()
        .enumerate()
        .map(|(i, mut v)| {
            let t_ms = i as u32 * bucket_ms;
            if v.is_empty() {
                return Bucket {
                    t_ms,
                    throughput: 0.0,
                    p50: 0,
                    p99: 0,
                    max: 0,
                };
            }
            v.sort_unstable();
            let pct = |p: f64| v[((p * (v.len() as f64 - 1.0)).round() as usize).min(v.len() - 1)];
            Bucket {
                t_ms,
                throughput: v.len() as f64 / secs,
                p50: pct(0.50),
                p99: pct(0.99),
                max: *v.last().unwrap(),
            }
        })
        .collect()
}

#[inline]
fn next_id(rng: &mut u64, rows: i64) -> i64 {
    *rng = rng
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*rng >> 33) as i64).rem_euclid(rows)
}

async fn configure(
    conn: &turso::Connection,
    threshold: i64,
    synchronous: &str,
) -> turso::Result<()> {
    conn.pragma_update("journal_mode", "mvcc").await?;
    conn.pragma_update("synchronous", synchronous).await?;
    conn.pragma_update("mvcc_checkpoint_threshold", threshold)
        .await?;
    Ok(())
}

fn sibling_files(db_path: &Path) -> Vec<(String, u64)> {
    let Some(dir) = db_path.parent() else {
        return Vec::new();
    };
    let Some(name) = db_path.file_name().and_then(|n| n.to_str()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Some(fname) = e.file_name().to_str() {
                if fname.starts_with(name) {
                    let suffix = fname.strip_prefix(name).unwrap_or(fname);
                    let label = if suffix.is_empty() {
                        "db".to_string()
                    } else {
                        suffix.to_string()
                    };
                    let size = e.metadata().map(|m| m.len()).unwrap_or(0);
                    out.push((label, size));
                }
            }
        }
    }
    out.sort();
    out
}

async fn run_mode(args: &Args, passive: bool) -> turso::Result<ModeData> {
    let mode_name = if passive { "passive" } else { "truncate" };
    let db_path: PathBuf = std::env::temp_dir().join(format!(
        "mvcc-ckpt-bench-{}-{}.db",
        mode_name,
        std::process::id()
    ));
    for (suffix, _) in sibling_files(&db_path) {
        let _ = std::fs::remove_file(format!(
            "{}{}",
            db_path.display(),
            if suffix == "db" { "" } else { &suffix }
        ));
    }

    let db = Builder::new_local(db_path.to_str().unwrap())
        .experimental_mvcc_passive_checkpoint(passive)
        .build()
        .await?;
    let db = Arc::new(Mutex::new(db));

    // Setup: enable MVCC, create + seed the table, materialize it, arm aggressive checkpoints.
    {
        let setup = db.lock().await.connect()?;
        setup.pragma_update("journal_mode", "mvcc").await?;
        setup
            .pragma_update("synchronous", &args.synchronous)
            .await?;
        setup
            .execute(
                "CREATE TABLE bench(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
                (),
            )
            .await?;
        for i in 0..args.rows {
            setup
                .execute(format!("INSERT INTO bench VALUES ({i}, 0)"), ())
                .await?;
        }
        let mut ck = setup.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
        while ck.next().await?.is_some() {}
        setup
            .pragma_update("mvcc_checkpoint_threshold", args.checkpoint_threshold)
            .await?;
    }

    // Pre-connect and configure every worker connection BEFORE the measurement window, so
    // connection setup (which contends with active checkpoints) never eats into the timed run.
    let mut wconns = Vec::with_capacity(args.writers);
    for _ in 0..args.writers {
        let c = db.lock().await.connect()?;
        configure(&c, args.checkpoint_threshold, &args.synchronous).await?;
        wconns.push(c);
    }
    let mut rconns = Vec::with_capacity(args.readers);
    for _ in 0..args.readers {
        let c = db.lock().await.connect()?;
        configure(&c, args.checkpoint_threshold, &args.synchronous).await?;
        rconns.push(c);
    }

    let run_start = Instant::now();
    let deadline = run_start + Duration::from_secs(args.duration);
    let rows = args.rows;

    let mut writer_handles = Vec::new();
    for (w, conn) in wconns.into_iter().enumerate() {
        writer_handles.push(tokio::spawn(async move {
            let mut samples: Vec<Sample> = Vec::with_capacity(1 << 16);
            let mut conflicts = 0u64;
            let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (w as u64).wrapping_mul(0x100_0000_01b3);
            while Instant::now() < deadline {
                let id = next_id(&mut rng, rows);
                loop {
                    let attempt = Instant::now();
                    if conn.execute("BEGIN CONCURRENT", ()).await.is_err() {
                        conflicts += 1;
                        if Instant::now() >= deadline {
                            break;
                        }
                        continue;
                    }
                    let updated = conn
                        .execute(format!("UPDATE bench SET v = v + 1 WHERE id = {id}"), ())
                        .await
                        .is_ok();
                    if !updated {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        conflicts += 1;
                        if Instant::now() >= deadline {
                            break;
                        }
                        continue;
                    }
                    match conn.execute("COMMIT", ()).await {
                        Ok(_) => {
                            let t_ms = run_start.elapsed().as_millis() as u32;
                            samples.push((t_ms, attempt.elapsed().as_micros() as u32));
                            break;
                        }
                        Err(_) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            conflicts += 1;
                            if Instant::now() >= deadline {
                                break;
                            }
                        }
                    }
                }
            }
            (samples, conflicts)
        }));
    }

    let mut reader_handles = Vec::new();
    for (r, conn) in rconns.into_iter().enumerate() {
        reader_handles.push(tokio::spawn(async move {
            let mut samples: Vec<Sample> = Vec::with_capacity(1 << 16);
            let mut rng = 0xd1b5_4a32_d192_ed03u64 ^ (r as u64).wrapping_mul(0x100_0000_01b3);
            while Instant::now() < deadline {
                let id = next_id(&mut rng, rows);
                let start = Instant::now();
                let mut q = match conn
                    .query(format!("SELECT v FROM bench WHERE id = {id}"), ())
                    .await
                {
                    Ok(q) => q,
                    Err(_) => continue,
                };
                let mut ok = true;
                loop {
                    match q.next().await {
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(_) => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    let t_ms = run_start.elapsed().as_millis() as u32;
                    samples.push((t_ms, start.elapsed().as_micros() as u32));
                }
            }
            samples
        }));
    }

    let mut writes = Vec::new();
    let mut write_conflicts = 0u64;
    for h in writer_handles {
        let (s, c) = h.await.expect("writer task");
        writes.extend(s);
        write_conflicts += c;
    }
    let mut reads = Vec::new();
    for h in reader_handles {
        reads.extend(h.await.expect("reader task"));
    }
    let elapsed_secs = run_start.elapsed().as_secs_f64();
    let files = sibling_files(&db_path);

    // Drop the database (close connections) before deleting the files.
    drop(db);
    for (suffix, _) in &files {
        let path = if suffix == "db" {
            db_path.display().to_string()
        } else {
            format!("{}{}", db_path.display(), suffix)
        };
        let _ = std::fs::remove_file(path);
    }

    Ok(ModeData {
        writes,
        reads,
        write_conflicts,
        elapsed_secs,
        files,
    })
}

fn print_table(args: &Args, truncate: Option<&ModeData>, passive: Option<&ModeData>) {
    let col = |m: Option<&ModeData>, f: &dyn Fn(&ModeData) -> String| -> String {
        m.map(f).unwrap_or_else(|| "-".to_string())
    };
    println!();
    println!(
        "config: writers={} readers={} duration={}s rows={} checkpoint_threshold={} synchronous={}",
        args.writers,
        args.readers,
        args.duration,
        args.rows,
        args.checkpoint_threshold,
        args.synchronous
    );
    println!("{:<26} {:>16} {:>16}", "metric", "TRUNCATE", "PASSIVE");
    println!("{}", "-".repeat(60));

    let w = |m: &ModeData| Stats::compute(&m.writes, m.elapsed_secs);
    let r = |m: &ModeData| Stats::compute(&m.reads, m.elapsed_secs);

    let row_u = |label: &str, g: &dyn Fn(&ModeData) -> u64| {
        println!(
            "{:<26} {:>16} {:>16}",
            label,
            col(truncate, &|m| g(m).to_string()),
            col(passive, &|m| g(m).to_string()),
        );
    };
    let row_f = |label: &str, g: &dyn Fn(&ModeData) -> f64| {
        println!(
            "{:<26} {:>16} {:>16}",
            label,
            col(truncate, &|m| format!("{:.1}", g(m))),
            col(passive, &|m| format!("{:.1}", g(m))),
        );
    };

    println!("-- writes (BEGIN CONCURRENT..COMMIT) --");
    row_u("  committed", &|m| w(m).count);
    row_f("  commits/sec", &|m| w(m).per_sec);
    row_f("  mean (us)", &|m| w(m).mean);
    row_u("  p50 (us)", &|m| w(m).p50);
    row_u("  p90 (us)", &|m| w(m).p90);
    row_u("  p99 (us)", &|m| w(m).p99);
    row_u("  p99.9 (us)", &|m| w(m).p999);
    row_u("  max (us)", &|m| w(m).max);
    row_u("  conflicts/retries", &|m| m.write_conflicts);
    println!("-- reads (point SELECT) --");
    row_u("  reads", &|m| r(m).count);
    row_f("  reads/sec", &|m| r(m).per_sec);
    row_f("  mean (us)", &|m| r(m).mean);
    row_u("  p50 (us)", &|m| r(m).p50);
    row_u("  p99 (us)", &|m| r(m).p99);
    row_u("  p99.9 (us)", &|m| r(m).p999);
    row_u("  max (us)", &|m| r(m).max);

    println!("-- final on-disk sizes (checkpoint keep-up signal) --");
    for label in ["db", "-wal", "-log", "-wal.log"] {
        let g = |m: &ModeData| {
            m.files
                .iter()
                .find(|(s, _)| s == label)
                .map(|(_, b)| format!("{} KiB", b / 1024))
                .unwrap_or_else(|| "-".to_string())
        };
        let any = truncate.map(g).filter(|s| s != "-").is_some()
            || passive.map(g).filter(|s| s != "-").is_some();
        if any {
            println!(
                "{:<26} {:>16} {:>16}",
                format!("  {label}"),
                col(truncate, &g),
                col(passive, &g)
            );
        }
    }
}

/// One plotted line: (name, color, (x_ms, y) points).
type ChartSeries<'a> = (&'a str, &'a str, Vec<(f64, f64)>);

fn svg_chart(title: &str, y_label: &str, x_max_ms: f64, series: &[ChartSeries]) -> String {
    let (w, h) = (760.0, 240.0);
    let (pl, pr, pt, pb) = (60.0, 16.0, 30.0, 28.0);
    let y_max = series
        .iter()
        .flat_map(|(_, _, pts)| pts.iter().map(|&(_, y)| y))
        .fold(1.0_f64, f64::max)
        * 1.1;
    let x_max = x_max_ms.max(1.0);
    let sx = |x: f64| pl + (x / x_max) * (w - pl - pr);
    let sy = |y: f64| h - pb - (y / y_max) * (h - pt - pb);

    let mut s = String::new();
    s.push_str(&format!(
        "<svg viewBox=\"0 0 {w} {h}\" width=\"{w}\" height=\"{h}\" style=\"background:#161b22;border:1px solid #30363d;border-radius:8px\">"
    ));
    s.push_str(&format!(
        "<text x=\"{pl}\" y=\"18\" fill=\"#e6edf3\" font-family=\"sans-serif\" font-size=\"13\">{title}</text>"
    ));
    // axes
    s.push_str(&format!(
        "<line x1=\"{pl}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" stroke=\"#30363d\"/>",
        h - pb,
        w - pr,
        h - pb
    ));
    s.push_str(&format!(
        "<line x1=\"{pl}\" y1=\"{pt}\" x2=\"{pl}\" y2=\"{}\" stroke=\"#30363d\"/>",
        h - pb
    ));
    // y gridlines + labels
    for i in 0..=4 {
        let yv = y_max * i as f64 / 4.0;
        let yp = sy(yv);
        s.push_str(&format!(
            "<line x1=\"{pl}\" y1=\"{yp}\" x2=\"{}\" y2=\"{yp}\" stroke=\"#21262d\"/>",
            w - pr
        ));
        s.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" fill=\"#8b949e\" font-family=\"monospace\" font-size=\"10\" text-anchor=\"end\">{:.0}</text>",
            pl - 4.0,
            yp + 3.0,
            yv
        ));
    }
    // x labels (seconds)
    let secs = (x_max / 1000.0).ceil() as u32;
    for i in 0..=secs {
        let xp = sx(i as f64 * 1000.0);
        s.push_str(&format!(
            "<text x=\"{xp}\" y=\"{}\" fill=\"#8b949e\" font-family=\"monospace\" font-size=\"10\" text-anchor=\"middle\">{i}s</text>",
            h - pb + 14.0
        ));
    }
    s.push_str(&format!(
        "<text x=\"12\" y=\"{}\" fill=\"#8b949e\" font-family=\"sans-serif\" font-size=\"10\" transform=\"rotate(-90 12 {})\">{y_label}</text>",
        h / 2.0,
        h / 2.0
    ));
    // series
    let mut lx = w - 220.0;
    for (name, color, pts) in series {
        if pts.is_empty() {
            continue;
        }
        let path: String = pts
            .iter()
            .map(|&(x, y)| format!("{:.1},{:.1}", sx(x), sy(y)))
            .collect::<Vec<_>>()
            .join(" ");
        s.push_str(&format!(
            "<polyline fill=\"none\" stroke=\"{color}\" stroke-width=\"1.5\" points=\"{path}\"/>"
        ));
        s.push_str(&format!(
            "<rect x=\"{lx}\" y=\"8\" width=\"10\" height=\"10\" fill=\"{color}\"/><text x=\"{}\" y=\"17\" fill=\"#e6edf3\" font-family=\"sans-serif\" font-size=\"11\">{name}</text>",
            lx + 14.0
        ));
        lx += 100.0;
    }
    s.push_str("</svg>");
    s
}

fn write_report(
    args: &Args,
    truncate: Option<&ModeData>,
    passive: Option<&ModeData>,
) -> std::io::Result<()> {
    let duration_ms = (args.duration as u32) * 1000;
    let bm = args.bucket_ms;

    // CSV
    let mut csv = String::from("mode,op,t_ms,throughput_per_s,p50_us,p99_us,max_us\n");
    let mut push_csv = |mode: &str, op: &str, buckets: &[Bucket]| {
        for b in buckets {
            csv.push_str(&format!(
                "{mode},{op},{},{:.1},{},{},{}\n",
                b.t_ms, b.throughput, b.p50, b.p99, b.max
            ));
        }
    };
    let mode_buckets = |m: &ModeData| {
        (
            bucketize(&m.writes, bm, duration_ms),
            bucketize(&m.reads, bm, duration_ms),
        )
    };
    let tb = truncate.map(mode_buckets);
    let pb = passive.map(mode_buckets);
    if let Some((wt, rt)) = &tb {
        push_csv("truncate", "write", wt);
        push_csv("truncate", "read", rt);
    }
    if let Some((wp, rp)) = &pb {
        push_csv("passive", "write", wp);
        push_csv("passive", "read", rp);
    }
    let csv_path = format!("{}.csv", args.out);
    std::fs::write(&csv_path, csv)?;

    // SVG charts embedded in a self-contained HTML.
    let line = |buckets: &Option<(Vec<Bucket>, Vec<Bucket>)>,
                write: bool,
                metric: u8|
     -> Vec<(f64, f64)> {
        match buckets {
            None => Vec::new(),
            Some((wb, rb)) => {
                let b = if write { wb } else { rb };
                b.iter()
                    .map(|x| {
                        let y = match metric {
                            0 => x.throughput,
                            1 => x.p99 as f64,
                            _ => x.max as f64,
                        };
                        (x.t_ms as f64, y)
                    })
                    .collect()
            }
        }
    };
    let red = "#f85149";
    let green = "#3fb950";
    let charts = [
        ("Write throughput (commits/sec)", "commits/s", true, 0u8),
        ("Write p99 latency (µs)", "µs", true, 1u8),
        ("Read throughput (selects/sec)", "selects/s", false, 0u8),
        ("Read p99 latency (µs)", "µs", false, 1u8),
    ];
    let mut svgs = String::new();
    for (title, ylab, write, metric) in charts {
        let series = vec![
            ("truncate", red, line(&tb, write, metric)),
            ("passive", green, line(&pb, write, metric)),
        ];
        svgs.push_str(&svg_chart(title, ylab, duration_ms as f64, &series));
        svgs.push_str("<br>");
    }

    let html = format!(
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>MVCC checkpoint bench</title>\
         <style>body{{background:#0d1117;color:#e6edf3;font-family:sans-serif;margin:24px;}}\
         h1{{font-size:20px}} .cfg{{color:#8b949e;font-family:monospace;font-size:13px;margin-bottom:16px}}</style></head><body>\
         <h1>MVCC checkpoint benchmark — TRUNCATE (red) vs PASSIVE (green)</h1>\
         <div class=\"cfg\">writers={} readers={} duration={}s rows={} checkpoint_threshold={} bucket={}ms</div>\
         {}</body></html>",
        args.writers, args.readers, args.duration, args.rows, args.checkpoint_threshold, args.bucket_ms, svgs
    );
    let html_path = format!("{}.html", args.out);
    std::fs::write(&html_path, html)?;

    println!();
    println!("wrote {csv_path}");
    println!("wrote {html_path}");
    Ok(())
}

fn main() {
    let args = Args::parse();
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if args.worker_threads > 0 {
        rt.worker_threads(args.worker_threads);
    }
    let rt = rt.build().expect("tokio runtime");

    rt.block_on(async {
        let truncate = if args.mode != ModeArg::Passive {
            println!("running TRUNCATE (blocking) mode...");
            Some(run_mode(&args, false).await.expect("truncate run"))
        } else {
            None
        };
        let passive = if args.mode != ModeArg::Truncate {
            println!("running PASSIVE (non-blocking) mode...");
            Some(run_mode(&args, true).await.expect("passive run"))
        } else {
            None
        };
        print_table(&args, truncate.as_ref(), passive.as_ref());
        write_report(&args, truncate.as_ref(), passive.as_ref()).expect("write report");
    });
}
