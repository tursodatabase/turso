use rand::{Rng, SeedableRng};
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier};
use std::time::Instant;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, Statement, StepResult, Value};

const USAGE: &str = "usage: mvcc-e2e <rows> <workload> [threads] [ops]
workloads: insert_seq insert_rand insert_conc point_select index_point full_scan index_range update";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let rows: i64 = args[1].parse().unwrap();
    let workload = args[2].as_str();
    let threads: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1);
    let ops: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(200_000);

    let db = open_db();
    let conn = db.connect().unwrap();
    setup_schema(&conn);

    let is_insert_workload = matches!(workload, "insert_seq" | "insert_rand" | "insert_conc");
    if !is_insert_workload {
        load_rows(&db, &conn, rows, false);
    }

    perf_control("enable");
    let start = Instant::now();
    let done_ops = match workload {
        "insert_seq" => load_rows(&db, &conn, rows, false),
        "insert_rand" => load_rows(&db, &conn, rows, true),
        _ => run_threads(&db, workload, rows, threads, ops),
    };
    let elapsed = start.elapsed();
    perf_control("disable");

    println!(
        "workload={workload} rows={rows} threads={threads} ops={done_ops} secs={:.3} ops_per_sec={:.0} ns_per_op={:.0}",
        elapsed.as_secs_f64(),
        done_ops as f64 / elapsed.as_secs_f64(),
        elapsed.as_nanos() as f64 / done_ops as f64
    );
}

fn perf_control(cmd: &str) {
    let Ok(path) = std::env::var("PERF_CTL_FIFO") else {
        return;
    };
    let mut ctl = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    writeln!(ctl, "{cmd}").unwrap();
    if let Ok(ack_path) = std::env::var("PERF_ACK_FIFO") {
        let mut buf = [0u8; 5];
        let mut ack = std::fs::File::open(ack_path).unwrap();
        let _ = std::io::Read::read(&mut ack, &mut buf);
    }
}

fn open_db() -> Arc<Database> {
    let io = Arc::new(MemoryIO::new());
    Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap()
}

fn setup_schema(conn: &Arc<Connection>) {
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.wal_auto_actions_disable();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
}

fn load_rows(db: &Arc<Database>, conn: &Arc<Connection>, rows: i64, random_ids: bool) -> u64 {
    let mut ids: Vec<i64> = (1..=rows).collect();
    if random_ids {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for i in (1..ids.len()).rev() {
            let j = rng.random_range(0..=i);
            ids.swap(i, j);
        }
    }
    let mut stmt = conn.prepare("INSERT INTO t VALUES (?, ?, ?)").unwrap();
    for chunk in ids.chunks(1000) {
        conn.execute("BEGIN").unwrap();
        for &id in chunk {
            stmt.bind_at(NonZeroUsize::new(1).unwrap(), Value::from_i64(id))
                .unwrap();
            stmt.bind_at(NonZeroUsize::new(2).unwrap(), Value::from_i64(a_of(id)))
                .unwrap();
            stmt.bind_at(
                NonZeroUsize::new(3).unwrap(),
                Value::build_text(format!("payload-{id:012}-xxxxxxxxxxxxxxxxxxxxxxxx")),
            )
            .unwrap();
            run(db, &mut stmt);
            stmt.reset().unwrap();
        }
        conn.execute("COMMIT").unwrap();
    }
    rows as u64
}

fn a_of(id: i64) -> i64 {
    (id.wrapping_mul(2_654_435_761)) % 1_000_003
}

fn run(db: &Arc<Database>, stmt: &mut Statement) -> u64 {
    let mut n = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Row => n += 1,
            StepResult::Done => return n,
            StepResult::Busy => panic!("busy"),
            StepResult::Interrupt => panic!("interrupt"),
        }
    }
}

fn try_run(db: &Arc<Database>, stmt: &mut Statement) -> turso_core::Result<u64> {
    let mut n = 0;
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
            StepResult::Row => n += 1,
            StepResult::Done => return Ok(n),
            StepResult::Busy => return Err(turso_core::LimboError::Busy),
            StepResult::Interrupt => panic!("interrupt"),
        }
    }
}

fn run_threads(db: &Arc<Database>, workload: &str, rows: i64, threads: usize, ops: u64) -> u64 {
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            let workload = workload.to_string();
            std::thread::spawn(move || {
                let conn = db.connect().unwrap();
                conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
                    .unwrap();
                conn.wal_auto_actions_disable();
                barrier.wait();
                worker(&db, &conn, &workload, rows, ops, t as u64)
            })
        })
        .collect();
    handles.into_iter().map(|h| h.join().unwrap()).sum()
}

fn worker(
    db: &Arc<Database>,
    conn: &Arc<Connection>,
    workload: &str,
    rows: i64,
    ops: u64,
    seed: u64,
) -> u64 {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1000 + seed);
    let p1 = NonZeroUsize::new(1).unwrap();
    let p2 = NonZeroUsize::new(2).unwrap();
    match workload {
        "point_select" => {
            let mut stmt = conn.prepare("SELECT b FROM t WHERE id = ?").unwrap();
            for _ in 0..ops {
                let id = rng.random_range(1..=rows);
                stmt.bind_at(p1, Value::from_i64(id)).unwrap();
                let n = run(db, &mut stmt);
                assert_eq!(n, 1);
                stmt.reset().unwrap();
            }
            ops
        }
        "index_point" => {
            let mut stmt = conn.prepare("SELECT id FROM t WHERE a = ?").unwrap();
            for _ in 0..ops {
                let id = rng.random_range(1..=rows);
                stmt.bind_at(p1, Value::from_i64(a_of(id))).unwrap();
                let n = run(db, &mut stmt);
                assert!(n >= 1);
                stmt.reset().unwrap();
            }
            ops
        }
        "full_scan" => {
            let mut stmt = conn.prepare("SELECT sum(length(b)) FROM t").unwrap();
            let scans = ops.max(1);
            for _ in 0..scans {
                run(db, &mut stmt);
                stmt.reset().unwrap();
            }
            scans * rows as u64
        }
        "index_range" => {
            let mut stmt = conn
                .prepare("SELECT count(*) FROM t WHERE a BETWEEN ? AND ?")
                .unwrap();
            for _ in 0..ops {
                let lo = rng.random_range(0..1_000_003i64);
                stmt.bind_at(p1, Value::from_i64(lo)).unwrap();
                stmt.bind_at(p2, Value::from_i64(lo + 10_000)).unwrap();
                run(db, &mut stmt);
                stmt.reset().unwrap();
            }
            ops
        }
        "update" => {
            let mut stmt = conn.prepare("UPDATE t SET b = ? WHERE id = ?").unwrap();
            let mut done = 0;
            while done < ops {
                conn.execute("BEGIN CONCURRENT").unwrap();
                for _ in 0..100 {
                    let id = rng.random_range(1..=rows);
                    stmt.bind_at(p1, Value::build_text(format!("upd-{id}-{done}")))
                        .unwrap();
                    stmt.bind_at(p2, Value::from_i64(id)).unwrap();
                    run(db, &mut stmt);
                    stmt.reset().unwrap();
                    done += 1;
                }
                if conn.execute("COMMIT").is_err() {
                    let _ = conn.execute("ROLLBACK");
                }
            }
            done
        }
        "insert_conc" => {
            let mut stmt = conn.prepare("INSERT INTO t(a, b) VALUES (?, ?)").unwrap();
            let mut done = 0;
            let mut restarts = 0u64;
            while done < ops {
                conn.execute("BEGIN CONCURRENT").unwrap();
                let mut ok = true;
                for i in 0..100 {
                    let a = rng.random_range(0..1_000_003i64);
                    stmt.bind_at(p1, Value::from_i64(a)).unwrap();
                    stmt.bind_at(
                        p2,
                        Value::build_text(format!("conc-{seed}-{done}-{i}-xxxxxxxxxxxxxxxxxxxx")),
                    )
                    .unwrap();
                    if try_run(db, &mut stmt).is_err() {
                        ok = false;
                        let _ = stmt.reset();
                        break;
                    }
                    stmt.reset().unwrap();
                }
                if ok && conn.execute("COMMIT").is_ok() {
                    done += 100;
                } else {
                    restarts += 1;
                    let _ = conn.execute("ROLLBACK");
                }
            }
            if restarts > 0 {
                eprintln!("thread {seed}: {restarts} restarted transactions");
            }
            done
        }
        other => panic!("unknown workload {other}\n{USAGE}"),
    }
}
