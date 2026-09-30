use serde::Serialize;
use std::path::Path;
use std::time::Instant;

const KB: f64 = 1024.0;
const MB: f64 = 1024.0 * KB;
const GB: f64 = 1024.0 * MB;

#[derive(Debug, Clone, Serialize)]
pub struct MemorySnapshot {
    pub rss_bytes: usize,
    pub phase: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct MemoryReport {
    /// Journal mode used (wal or mvcc)
    pub mode: String,
    /// Name of the workload profile that was executed
    pub workload: String,
    /// Number of batch iterations executed per connection
    pub iterations: usize,
    /// Number of SQL statements per transaction batch
    pub batch_size: usize,
    /// Number of concurrent connections used during the run phase
    pub connections: usize,
    /// Process RSS before any database work (includes runtime overhead)
    pub baseline_bytes: usize,
    /// Highest RSS observed across all periodic snapshots
    pub peak_bytes: usize,
    /// Process RSS at the end of the benchmark
    pub final_bytes: usize,
    /// final_bytes - baseline_bytes; net RSS growth attributable to the workload
    pub net_growth_bytes: usize,
    /// Heap bytes still allocated at measurement time (via dhat)
    pub heap_current_bytes: usize,
    /// Highest simultaneous heap allocation observed during the entire run (via dhat)
    pub heap_peak_bytes: usize,
    /// Total number of individual allocations made during the entire run (via dhat)
    pub total_allocs: u64,
    /// Cumulative bytes allocated (including already-freed); measures allocation pressure
    pub total_bytes_allocated: u64,
    /// Time-series of RSS snapshots taken at phase transitions and periodically
    pub snapshots: Vec<MemorySnapshot>,
    /// Size of the .db file on disk after the benchmark
    pub db_file_bytes: u64,
    /// Size of the .db-wal file (WAL mode); None if absent or empty
    pub wal_file_bytes: Option<u64>,
    /// Size of the .db-log file (MVCC logical log); None if absent or empty
    pub log_file_bytes: Option<u64>,
}

pub fn take_snapshot(start: Instant, phase: &str) -> MemorySnapshot {
    let stats = memory_stats::memory_stats().expect("failed to get memory stats");
    MemorySnapshot {
        rss_bytes: stats.physical_mem,
        phase: phase.to_string(),
        elapsed_ms: start.elapsed().as_millis() as u64,
    }
}

pub fn file_size(path: &str) -> u64 {
    Path::new(path).metadata().map(|m| m.len()).unwrap_or(0)
}

impl MemoryReport {
    pub fn print_human(&self) {
        println!(
            "=== MEMORY BENCHMARK ({}, {}) ===",
            self.mode, self.workload
        );
        println!(
            "Iterations:  {} x {} rows",
            self.iterations, self.batch_size
        );
        println!("Connections: {}", self.connections);

        println!();
        println!("--- RSS (process-level) ---");
        println!("Baseline:    {}", format_bytes(self.baseline_bytes));
        if self.snapshots.len() > 2 {
            for snap in &self.snapshots[1..self.snapshots.len() - 1] {
                println!(
                    "{:<12} {}  (at {}ms)",
                    format!("{}:", snap.phase),
                    format_bytes(snap.rss_bytes),
                    snap.elapsed_ms
                );
            }
        }
        println!("Peak:        {}", format_bytes(self.peak_bytes));
        println!("Final:       {}", format_bytes(self.final_bytes));
        println!("Net growth:  {}", format_bytes(self.net_growth_bytes));

        println!();
        println!("--- Heap (dhat) ---");
        println!("Current:     {}", format_bytes(self.heap_current_bytes));
        println!("Peak:        {}", format_bytes(self.heap_peak_bytes));
        println!("Total allocs:  {}", self.total_allocs);
        println!(
            "Total bytes:   {}",
            format_bytes(self.total_bytes_allocated as usize)
        );

        println!();
        println!("--- Disk ---");
        println!("DB file:     {}", format_bytes(self.db_file_bytes as usize));
        if let Some(wal) = self.wal_file_bytes {
            println!("WAL file:    {}", format_bytes(wal as usize));
        }
        if let Some(log) = self.log_file_bytes {
            println!("Log file:    {}", format_bytes(log as usize));
        }
    }

    pub fn print_json(&self) {
        println!(
            "{}",
            serde_json::to_string_pretty(self).expect("failed to serialize report")
        );
    }

    pub fn print_csv_header() {
        println!(
            "mode,workload,iterations,batch_size,connections,baseline_mb,rss_peak_mb,rss_final_mb,rss_growth_mb,heap_current_mb,heap_peak_mb,total_allocs,total_bytes_mb,db_mb,wal_mb,log_mb"
        );
    }

    pub fn print_csv(&self) {
        println!(
            "{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{:.2},{:.2},{:.2},{:.2}",
            self.mode,
            self.workload,
            self.iterations,
            self.batch_size,
            self.connections,
            self.baseline_bytes as f64 / MB,
            self.peak_bytes as f64 / MB,
            self.final_bytes as f64 / MB,
            self.net_growth_bytes as f64 / MB,
            self.heap_current_bytes as f64 / MB,
            self.heap_peak_bytes as f64 / MB,
            self.total_allocs,
            self.total_bytes_allocated as f64 / MB,
            self.db_file_bytes as f64 / MB,
            self.wal_file_bytes.unwrap_or(0) as f64 / MB,
            self.log_file_bytes.unwrap_or(0) as f64 / MB,
        );
    }
}

fn format_bytes(bytes: usize) -> String {
    let bytes_f = bytes as f64;
    if bytes_f >= GB {
        format!("{:.2} GB", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{:.2} MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.2} KB", bytes_f / KB)
    } else {
        format!("{bytes} B")
    }
}
