#![feature(coroutines)]
#![feature(coroutine_trait)]
#![feature(stmt_expr_attributes)]

use clap::Parser;
use hdrhistogram::Histogram;
use std::{
    ops::{Coroutine, CoroutineState},
    pin::Pin,
    sync::Arc,
    time::Instant,
};
use turso_core::{Database, PlatformIO, IO};

#[derive(Parser)]
struct Opts {
    count: usize,
}

fn main() {
    env_logger::init();
    let opts = Opts::parse();
    let mut hist = Histogram::<u64>::new(2).unwrap().into_sync();
    let io = Arc::new(PlatformIO::new().unwrap());
    let mut tenants = vec![];
    for i in 0..opts.count {
        let mut recorder = hist.recorder();
        let io = io.clone();
        let tenant = #[coroutine]
        move || {
            let database = format!("database{}.db", i);
            let db = Database::open_file(io.clone(), &database).unwrap();
            let conn = db.connect();
            let mut stmt = conn.prepare("SELECT * FROM user LIMIT 100").unwrap();
            for _ in 0..100 {
                for _ in 0..10 {
                    let now = Instant::now();
                    let mut rows = stmt.query().unwrap();
                    let mut count = 0;
                    loop {
                        let row = rows.step().unwrap();
                        match row {
                            turso_core::StepResult::Row(_) => {
                                let row = statement.row();
                                count += 1;
                            }
                            turso_core::StepResult::IO => yield,
                            turso_core::StepResult::Done => break,
                        }
                    }
                    assert!(count == 100);
                    recorder.record(now.elapsed().as_nanos() as u64).unwrap();
                }
                yield;
            }
        };
        tenants.push(tenant);
    }
    let mut completed = 0usize;
    while completed < opts.count {
        for i in 0..tenants.len() {
            match tenants.get_mut(i) {
                Some(tenant) => {
                    let tenant = Pin::new(tenant);
                    match tenant.resume(()) {
                        CoroutineState::Complete(_) => {
                            let _ = tenants.remove(i);
                            completed += 1;
                        }
                        CoroutineState::Yielded(_) => {}
                    }
                }
                None => {
                    continue;
                }
            }
        }
        io.step().unwrap();
    }
    hist.refresh();
    println!("count,p50,p90,p95,p99,p999,p9999,p99999");
    println!(
        "{},{},{},{},{},{},{},{}",
        opts.count,
        hist.value_at_quantile(0.5),
        hist.value_at_quantile(0.90),
        hist.value_at_quantile(0.95),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.value_at_quantile(0.9999),
        hist.value_at_quantile(0.99999)
    );
}
