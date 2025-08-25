use std::time::{Duration, Instant};
use std::fs::File;
use std::io::Write;

/// Metrics collection for benchmark operations
#[derive(Debug, Clone)]
pub struct OperationMetrics {
    operation_times: Vec<Duration>,
    start_time: Option<Instant>,
    total_operations: u64,
    latency_file: Option<File>,
    iops_file: Option<File>,
}

impl OperationMetrics {
    /// Create a new metrics collector
    pub fn new(latency_file_path: Option<&str>, iops_file_path: Option<&str>) -> crate::Result<Self> {
        let latency_file = if let Some(path) = latency_file_path {
            Some(File::create(path)?)
        } else {
            None
        };

        let iops_file = if let Some(path) = iops_file_path {
            Some(File::create(path)?)
        } else {
            None
        };

        Ok(Self {
            operation_times: Vec::new(),
            start_time: None,
            total_operations: 0,
            latency_file,
            iops_file,
        })
    }

    /// Start timing the benchmark
    pub fn start_timing(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Record an operation completion
    pub fn record_operation(&mut self, duration: Duration) {
        self.operation_times.push(duration);
        self.total_operations += 1;

        // Write latency data if file is specified
        if let Some(ref mut file) = self.latency_file {
            let _ = writeln!(file, "{:.0}\tusec", duration.as_micros());
        }
    }

    /// Calculate and return benchmark statistics
    pub fn calculate_stats(&self) -> BenchmarkStats {
        let total_time = self.start_time
            .map(|start| start.elapsed())
            .unwrap_or_default();

        let total_ops = self.total_operations;
        let ops_per_sec = if total_time.as_secs_f64() > 0.0 {
            total_ops as f64 / total_time.as_secs_f64()
        } else {
            0.0
        };

        let (min_latency, max_latency, avg_latency) = if !self.operation_times.is_empty() {
            let min = self.operation_times.iter().min().copied().unwrap_or_default();
            let max = self.operation_times.iter().max().copied().unwrap_or_default();
            let sum: Duration = self.operation_times.iter().sum();
            let avg = sum / self.operation_times.len() as u32;
            (min, max, avg)
        } else {
            (Duration::default(), Duration::default(), Duration::default())
        };

        BenchmarkStats {
            total_time,
            total_operations: total_ops,
            operations_per_second: ops_per_sec,
            min_latency,
            max_latency,
            avg_latency,
        }
    }

    /// Write IOPS data if file is specified
    pub fn write_iops(&mut self, iops: f64) -> crate::Result<()> {
        if let Some(ref mut file) = self.iops_file {
            writeln!(file, "{:.2} IOPS", iops)?;
        }
        Ok(())
    }
}

/// Benchmark statistics
#[derive(Debug, Clone)]
pub struct BenchmarkStats {
    pub total_time: Duration,
    pub total_operations: u64,
    pub operations_per_second: f64,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub avg_latency: Duration,
}

impl BenchmarkStats {
    /// Print formatted benchmark results
    pub fn print_results(&self, operation_name: &str, num_threads: u32) {
        println!("-----------------------------------------");
        println!("[Measurement Result]");
        println!("-----------------------------------------");
        
        println!(
            "[TIME] : {:.3} sec. {:.2} {}/sec",
            self.total_time.as_secs_f64(),
            self.operations_per_second,
            operation_name
        );

        if num_threads > 1 {
            println!(
                "Total operations: {} across {} threads ({:.2} ops/thread/sec)",
                self.total_operations,
                num_threads,
                self.operations_per_second / num_threads as f64
            );
        }

        if self.total_operations > 0 {
            println!(
                "[LATENCY] Min: {:.2}ms, Max: {:.2}ms, Avg: {:.2}ms",
                self.min_latency.as_millis(),
                self.max_latency.as_millis(),
                self.avg_latency.as_millis()
            );
        }
    }
}

/// Combined metrics for all threads
#[derive(Debug)]
pub struct BenchmarkMetrics {
    thread_metrics: Vec<OperationMetrics>,
    start_time: Option<Instant>,
}

impl BenchmarkMetrics {
    /// Create a new benchmark metrics collector
    pub fn new() -> Self {
        Self {
            thread_metrics: Vec::new(),
            start_time: None,
        }
    }

    /// Add metrics from a thread
    pub fn add_thread_metrics(&mut self, metrics: OperationMetrics) {
        self.thread_metrics.push(metrics);
    }

    /// Start timing the entire benchmark
    pub fn start_timing(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Calculate combined statistics across all threads
    pub fn calculate_combined_stats(&self) -> BenchmarkStats {
        let total_time = self.start_time
            .map(|start| start.elapsed())
            .unwrap_or_default();

        let total_operations: u64 = self.thread_metrics
            .iter()
            .map(|m| m.total_operations)
            .sum();

        let ops_per_sec = if total_time.as_secs_f64() > 0.0 {
            total_operations as f64 / total_time.as_secs_f64()
        } else {
            0.0
        };

        // Combine all operation times for latency calculations
        let all_times: Vec<Duration> = self.thread_metrics
            .iter()
            .flat_map(|m| m.operation_times.iter())
            .copied()
            .collect();

        let (min_latency, max_latency, avg_latency) = if !all_times.is_empty() {
            let min = all_times.iter().min().copied().unwrap_or_default();
            let max = all_times.iter().max().copied().unwrap_or_default();
            let sum: Duration = all_times.iter().sum();
            let avg = sum / all_times.len() as u32;
            (min, max, avg)
        } else {
            (Duration::default(), Duration::default(), Duration::default())
        };

        BenchmarkStats {
            total_time,
            total_operations,
            operations_per_second: ops_per_sec,
            min_latency,
            max_latency,
            avg_latency,
        }
    }
}