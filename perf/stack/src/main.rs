// Stack-usage measurement harness for the SQL prepare path.
//
// Spawns a fresh thread per query, primes the stack with a sentinel byte,
// runs `conn.prepare(sql)`, and scans for the deepest byte touched.
//
// Run: `cd perf/stack && cargo run --release`

use std::sync::Arc;
use turso_core::{Database, MemoryIO};

const SENTINEL: u8 = 0xAA;
const SCAN_REGION: usize = 512 * 1024; // 512 KB sentinel region per measurement
const THREAD_STACK: usize = 8 * 1024 * 1024;

#[inline(never)]
fn sp_addr() -> usize {
    let probe: u32 = 0;
    let addr = &probe as *const u32 as usize;
    std::hint::black_box(addr)
}

// Recursively grow the stack by 4 KB per frame, writing the sentinel to each
// frame's local buffer. When the recursion unwinds, the bytes remain in the
// "freed" stack memory and form the primed region for measurement.
// Large buf (~32 KB) per recursive frame so the tiny per-frame saved-register
// gap (~64 B) is negligible compared to the SENTINEL run inside each buf.
// Any WINDOW < 32 KB inside scan_deepest will land cleanly in one buf.
const PRIME_FRAME_BYTES: usize = 4 * 1024;

#[inline(never)]
fn prime_to(target_low: usize) -> u8 {
    let mut buf = [0u8; PRIME_FRAME_BYTES];
    let buf_start = buf.as_ptr() as usize;
    for i in 0..buf.len() {
        unsafe {
            std::ptr::write_volatile(buf.as_mut_ptr().add(i), SENTINEL);
        }
    }
    let mut acc: u8 = 0;
    if buf_start > target_low {
        acc = prime_to(target_low);
    }
    // Use buf AFTER the recursion to defeat tail-call optimization.
    acc.wrapping_add(unsafe { std::ptr::read_volatile(buf.as_ptr()) })
}

#[inline(never)]
fn scan_deepest(target_low: usize, sp_top: usize) -> usize {
    // Walk down from sp_top, tracking runs of SENTINEL bytes. The recursive
    // priming leaves small gaps (~32–64 B) between frames for saved registers
    // and alignment, so we can't just look for the first SENTINEL byte.
    // Instead, find the first stretch of WINDOW consecutive SENTINEL bytes —
    // that marks the transition from f's touched region into primed territory.
    const WINDOW: usize = 1024;
    let mut run = 0usize;
    let mut addr = sp_top;
    while addr > target_low {
        addr -= 1;
        let byte = unsafe { std::ptr::read_volatile(addr as *const u8) };
        if byte == SENTINEL {
            run += 1;
            if run >= WINDOW {
                // Bytes [addr .. addr+WINDOW] are all SENTINEL; deepest touched
                // byte is just above this run.
                return sp_top.saturating_sub(addr + WINDOW);
            }
        } else {
            run = 0;
        }
    }
    sp_top.saturating_sub(target_low)
}

#[inline(never)]
fn measure<F: FnOnce()>(f: F) -> usize {
    let sp_top = sp_addr();
    let target_low = sp_top.saturating_sub(SCAN_REGION);
    std::hint::black_box(prime_to(target_low));
    f();
    scan_deepest(target_low, sp_top)
}

fn setup_schema(conn: &Arc<turso_core::Connection>) {
    let stmts = [
        "CREATE TABLE column_metadata(id INTEGER PRIMARY KEY, name TEXT, value TEXT)",
        "CREATE TABLE core(id INTEGER PRIMARY KEY, deletion_timestamp TEXT)",
        "CREATE TABLE select_list_options(id INTEGER PRIMARY KEY, select_list_grid_id INTEGER, name TEXT, row_rank INTEGER, row_deletion_timestamp TEXT)",
        "CREATE TABLE subscriptions(id INTEGER PRIMARY KEY, container_doc_id INTEGER)",
    ];
    for sql in stmts {
        conn.prepare(sql)
            .expect("schema prepare")
            .run_ignore_rows()
            .expect("schema run");
    }
}

fn run_query(name: &'static str, sql: &'static str) -> (usize, usize) {
    std::thread::Builder::new()
        .stack_size(THREAD_STACK)
        .name(format!("measure-{name}"))
        .spawn(move || {
            let io = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, "stack-measure.db").expect("open db");
            let conn = db.connect().expect("connect");
            setup_schema(&conn);

            // Baseline: measurement harness overhead on an empty closure.
            let baseline = measure(|| {});

            // Warm any one-shot caches by running once, discarding.
            let _ = conn.prepare(sql);

            // Take the max of three consecutive measurements to smooth noise.
            let mut peak = 0usize;
            for _ in 0..3 {
                let m = measure(|| {
                    let _stmt = conn.prepare(sql);
                });
                if m > peak {
                    peak = m;
                }
            }
            (baseline, peak)
        })
        .unwrap()
        .join()
        .unwrap()
}

fn main() {
    let queries: &[(&'static str, &'static str)] = &[
        ("BEGIN CONCURRENT", "BEGIN CONCURRENT"),
        ("SELECT * FROM column_metadata", "SELECT * FROM column_metadata"),
        (
            "SELECT * FROM metadata (missing table)",
            "SELECT * FROM metadata",
        ),
        (
            "SELECT ... FROM select_list_options ORDER BY ...",
            "SELECT select_list_grid_id, name, id, row_rank, row_deletion_timestamp FROM select_list_options ORDER BY select_list_grid_id, row_rank",
        ),
        (
            "SELECT COUNT(*) FROM core WHERE ... IS NULL",
            "SELECT COUNT(*) AS row_count FROM core WHERE deletion_timestamp IS NULL",
        ),
        (
            "SELECT * FROM subscriptions WHERE ... = ?1",
            "SELECT subscriptions.* FROM subscriptions WHERE container_doc_id = ?1",
        ),
    ];

    println!(
        "{:>10}  {:>10}  {:>10}  {}",
        "peak_B", "net_B", "base_B", "query"
    );
    println!("{}", "-".repeat(80));
    for (name, sql) in queries {
        let (baseline, peak) = run_query(name, sql);
        let net = peak.saturating_sub(baseline);
        println!("{:>10}  {:>10}  {:>10}  {}", peak, net, baseline, name);
    }
}
