use clap::Parser;
use hdrhistogram::Histogram;
use std::{
    sync::Arc,
    time::Instant,
};
use turso_core::{Database, PlatformIO};

#[derive(Parser)]
struct Opts {
    database: String,
    #[arg(short, long, default_value = "100")]
    iterations: usize,
}

fn main() {
    env_logger::init();
    let opts = Opts::parse();
    let mut hist = Histogram::<u64>::new(2).unwrap();
    let io = Arc::new(PlatformIO::new().unwrap());
    
    println!("Testing connection performance with database: {}", opts.database);
    
    // Open the database object.
    let db = Database::open_file(io.clone(), &opts.database, false).unwrap();
    for i in 0..opts.iterations {
        let start = Instant::now();
        
        // now open a new connection and prepare a statement.
        let conn = db.connect().unwrap();
        let _stmt = conn.prepare("SELECT name FROM table_0 WHERE id = ?").unwrap();
        
        let elapsed = start.elapsed();
        hist.record(elapsed.as_nanos() as u64).unwrap();
        
        if (i + 1) % 10 == 0 {
            println!("Completed {} iterations", i + 1);
        }
    }
    
    // Extract database name and table count for CSV output
    let db_name = opts.database.replace(".db", "").replace("database_", "");
    
    println!("database,iterations,p50,p90,p95,p99,p999,p9999,p99999");
    println!(
        "{},{},{},{},{},{},{},{},{}",
        db_name,
        opts.iterations,
        hist.value_at_quantile(0.5),
        hist.value_at_quantile(0.90),
        hist.value_at_quantile(0.95),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.value_at_quantile(0.9999),
        hist.value_at_quantile(0.99999)
    );
}
