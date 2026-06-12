use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
}

/// Lightweight latency recorder. Stores raw samples (in milliseconds) and
/// computes summary percentiles at the end. For the request volumes a single
/// load run produces this is cheap enough; swap for an HdrHistogram if runs
/// ever need to sustain tens of millions of samples.
#[derive(Default)]
struct Series {
    latencies: Vec<f64>,
    /// Seconds since the measured window began, parallel to `latencies`.
    times: Vec<f64>,
    ok: u64,
    errors: u64,
    skips: u64,
}

#[derive(Default)]
pub struct Stats {
    series: BTreeMap<String, Series>,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&mut self, op: &str) -> &mut Series {
        self.series.entry(op.to_string()).or_default()
    }

    pub fn record_ok(&mut self, op: &str, latency_ms: f64, at_sec: f64) {
        let s = self.get(op);
        s.ok += 1;
        s.latencies.push(latency_ms);
        s.times.push(at_sec);
    }

    pub fn record_error(&mut self, op: &str) {
        self.get(op).errors += 1;
    }

    pub fn record_skip(&mut self, op: &str) {
        self.get(op).skips += 1;
    }

    /// Merge another recorder into this one (used to aggregate per-worker stats).
    pub fn merge(&mut self, other: Stats) {
        for (op, theirs) in other.series {
            let mine = self.get(&op);
            mine.ok += theirs.ok;
            mine.errors += theirs.errors;
            mine.skips += theirs.skips;
            mine.latencies.extend(theirs.latencies);
            mine.times.extend(theirs.times);
        }
    }

    /// Raw (time, latency) samples per operation, for plotting. Ordered by
    /// sample count, busiest operation first.
    pub fn samples(&self) -> Vec<OpSamples> {
        let mut out: Vec<OpSamples> = self
            .series
            .iter()
            .map(|(name, s)| {
                let mut samples: Vec<(f64, f64)> = s
                    .times
                    .iter()
                    .copied()
                    .zip(s.latencies.iter().copied())
                    .collect();
                samples.sort_by(|a, b| a.0.total_cmp(&b.0));
                OpSamples {
                    name: name.clone(),
                    samples,
                }
            })
            .collect();
        out.sort_by(|a, b| b.samples.len().cmp(&a.samples.len()));
        out
    }

    pub fn summary(&self, elapsed_secs: f64) -> Summary {
        let mut operations = Vec::new();
        let mut totals = Series::default();
        for (name, s) in &self.series {
            operations.push(summarize(name, s, elapsed_secs));
            totals.ok += s.ok;
            totals.errors += s.errors;
            totals.skips += s.skips;
            totals.latencies.extend_from_slice(&s.latencies);
        }
        operations.sort_by(|a, b| b.ok.cmp(&a.ok));
        Summary {
            elapsed_secs,
            total: summarize("total", &totals, elapsed_secs),
            operations,
        }
    }
}

pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * sorted.len() as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn summarize(name: &str, s: &Series, elapsed_secs: f64) -> OperationSummary {
    let mut sorted = s.latencies.clone();
    sorted.sort_by(f64::total_cmp);
    let sum: f64 = sorted.iter().sum();
    OperationSummary {
        name: name.to_string(),
        ok: s.ok,
        errors: s.errors,
        skips: s.skips,
        throughput: (s.ok + s.errors) as f64 / elapsed_secs,
        latency: Latency {
            min: sorted.first().copied().unwrap_or(0.0),
            mean: if sorted.is_empty() {
                0.0
            } else {
                sum / sorted.len() as f64
            },
            p50: percentile(&sorted, 50.0),
            p95: percentile(&sorted, 95.0),
            p99: percentile(&sorted, 99.0),
            max: sorted.last().copied().unwrap_or(0.0),
        },
    }
}

#[derive(Debug, Serialize)]
pub struct Latency {
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct OperationSummary {
    pub name: String,
    pub ok: u64,
    pub errors: u64,
    pub skips: u64,
    pub throughput: f64,
    pub latency: Latency,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub elapsed_secs: f64,
    pub operations: Vec<OperationSummary>,
    pub total: OperationSummary,
}

pub struct OpSamples {
    pub name: String,
    /// (seconds since measure start, latency ms)
    pub samples: Vec<(f64, f64)>,
}

pub fn print_summary(s: &Summary, format: OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(s).unwrap());
        }
        OutputFormat::Csv => {
            println!(
                "operation,ok,errors,skips,ops_per_sec,min_ms,mean_ms,p50_ms,p95_ms,p99_ms,max_ms"
            );
            for op in s.operations.iter().chain([&s.total]) {
                let l = &op.latency;
                println!(
                    "{},{},{},{},{:.1},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                    op.name,
                    op.ok,
                    op.errors,
                    op.skips,
                    op.throughput,
                    l.min,
                    l.mean,
                    l.p50,
                    l.p95,
                    l.p99,
                    l.max
                );
            }
        }
        OutputFormat::Text => {
            let name_width = s
                .operations
                .iter()
                .map(|r| r.name.len())
                .chain([9])
                .max()
                .unwrap();
            let header = format!(
                "{:<name_width$}  {:>9}  {:>6}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
                "operation", "ok", "err", "skip", "ops/s", "min", "p50", "p95", "p99", "max"
            );
            println!();
            println!("=== turso-load results ===");
            println!("duration: {:.2}s (measured)", s.elapsed_secs);
            println!();
            println!("{header}");
            println!("{}", "-".repeat(header.len()));
            for op in s.operations.iter().chain([&s.total]) {
                if op.name == "total" {
                    println!("{}", "-".repeat(header.len()));
                }
                let l = &op.latency;
                let ms = |n: f64| format!("{n:.1}ms");
                println!(
                    "{:<name_width$}  {:>9}  {:>6}  {:>6}  {:>9.1}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
                    op.name,
                    op.ok,
                    op.errors,
                    op.skips,
                    op.throughput,
                    ms(l.min),
                    ms(l.p50),
                    ms(l.p95),
                    ms(l.p99),
                    ms(l.max)
                );
            }
        }
    }
}
