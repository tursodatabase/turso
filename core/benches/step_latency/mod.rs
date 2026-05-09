//! Per-`Statement::step()` latency histogram for tursodb benches.
//!
//! Each `stmt.step()` call corresponds to one yield boundary at the
//! user-visible API: the state-machine driver
//! (`core/state_machine.rs`) only returns to the caller on
//! `TransitionResult::Io(...)`, so any spike here is wall-clock the executor
//! was held inside one `step()` invocation. This module makes that
//! distribution observable from any bench without invasive instrumentation.
//!
//! Cargo will not pick this up as a bench because it lives under a
//! subdirectory (`benches/step_latency/`); top-level `.rs` files in
//! `benches/` are auto-discovered, subdirectories are not.
//!
//! ## Usage
//!
//! In a bench file:
//!
//! ```ignore
//! #[path = "step_latency/mod.rs"]
//! mod step_latency;
//! use step_latency::{StepLatency, run_to_completion_measured};
//!
//! let mut latency = StepLatency::new();
//! // ... wherever you currently call `run_to_completion(&mut stmt, &db)`,
//! // call the measured variant instead:
//! run_to_completion_measured(&mut stmt, &db, &mut latency)?;
//! // ... at the end, dump a one-line summary to stderr:
//! latency.print_summary("my_bench/100k_rows");
//! ```
//!
//! `StepLatency` is also usable directly: capture an `Instant` before the
//! `step()` call and pass it to `record(...)`.

// Each cargo bench is its own binary, so methods unused by the *current*
// consumer look dead even though they're part of the shared API surface.
#![allow(dead_code)]

use hdrhistogram::Histogram;
use std::sync::Arc;
use std::time::Instant;
use turso_core::{Database, StepResult};

/// Histogram of single-`Statement::step()` durations, in nanoseconds.
///
/// Bounds: 1 ns – 60 s, 3 significant figures. Wide enough that pre-fix
/// commits (multi-hundred-ms `step()` spikes on large transactions) and
/// post-fix commits (sub-millisecond) both land inside the trackable range.
pub struct StepLatency {
    histo: Histogram<u64>,
}

impl StepLatency {
    pub fn new() -> Self {
        Self {
            histo: Histogram::new_with_bounds(1, 60_000_000_000, 3)
                .expect("hdrhistogram bounds"),
        }
    }

    /// Record one yield-boundary sample. `start` should be the `Instant`
    /// captured immediately before the `Statement::step()` call.
    pub fn record(&mut self, start: Instant) {
        let ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let _ = self.histo.record(ns);
    }

    /// Merge another histogram into `self`. Useful when collecting samples
    /// from multiple inner loops into one summary.
    pub fn merge(&mut self, other: &Self) {
        self.histo.add(&other.histo).expect("histogram merge");
    }

    pub fn sample_count(&self) -> u64 {
        self.histo.len()
    }

    pub fn max_ns(&self) -> u64 {
        self.histo.max()
    }

    pub fn quantile_ns(&self, q: f64) -> u64 {
        self.histo.value_at_quantile(q)
    }

    /// Dump a one-line CSV-friendly summary to stderr (so it doesn't
    /// collide with criterion's stdout reporter):
    /// `label count=N max_us=… p99_us=… p95_us=… p50_us=…`.
    pub fn print_summary(&self, label: &str) {
        if self.histo.len() == 0 {
            eprintln!("{label} (no samples)");
            return;
        }
        eprintln!(
            "{label} count={count} max_us={max} p99_us={p99} p95_us={p95} p50_us={p50}",
            count = self.histo.len(),
            max = self.histo.max() / 1_000,
            p99 = self.histo.value_at_quantile(0.99) / 1_000,
            p95 = self.histo.value_at_quantile(0.95) / 1_000,
            p50 = self.histo.value_at_quantile(0.50) / 1_000,
        );
    }
}

impl Default for StepLatency {
    fn default() -> Self {
        Self::new()
    }
}

/// Drive a statement to completion, recording the wall-clock duration of
/// each `Statement::step()` call into `latency` (in nanoseconds).
///
/// Drop-in replacement for the typical bench-local `run_to_completion`
/// helper — same signature plus a `&mut StepLatency`.
pub fn run_to_completion_measured(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
    latency: &mut StepLatency,
) -> turso_core::Result<()> {
    loop {
        let start = Instant::now();
        let result = stmt.step()?;
        latency.record(start);
        match result {
            StepResult::IO => db.io.step()?,
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::Interrupt | StepResult::Busy => {
                panic!("unexpected step result: {result:?}");
            }
        }
    }
    Ok(())
}
