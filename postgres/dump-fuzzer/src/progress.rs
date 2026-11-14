use indicatif::{ProgressBar, ProgressStyle};
use std::{
    io::{self, IsTerminal},
    time::{Duration, Instant},
};

pub struct Progress {
    bar: ProgressBar,
    enabled: bool,
    terminal: bool,
    started: Instant,
    attempts: u64,
    passed: u64,
    failed: u64,
}

impl Progress {
    pub fn new(enabled: bool) -> Self {
        let terminal = io::stderr().is_terminal();
        let bar = if enabled && terminal {
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::with_template("{spinner:.cyan} [{elapsed_precise}] {prefix} {msg}")
                    .unwrap(),
            );
            bar.enable_steady_tick(Duration::from_millis(100));
            bar
        } else {
            ProgressBar::hidden()
        };
        Self {
            bar,
            enabled,
            terminal,
            started: Instant::now(),
            attempts: 0,
            passed: 0,
            failed: 0,
        }
    }

    pub fn begin(&mut self, operation: &str) {
        self.attempts += 1;
        self.bar.set_prefix(format!(
            "attempt {} | {} passed, {} failed{} |",
            self.attempts,
            self.passed,
            self.failed,
            if self.failed > 0 {
                " | rechecks/shrinking"
            } else {
                ""
            }
        ));
        self.stage(if self.failed == 0 {
            operation
        } else {
            "rechecking after failure (includes shrinking/replay)"
        });
    }

    pub fn stage(&self, message: &str) {
        if !self.enabled {
            return;
        }
        if self.terminal {
            self.bar.set_message(message.to_owned());
        } else {
            eprintln!(
                "[{:>6.1}s] attempt {} | {} passed, {} failed | {message}",
                self.started.elapsed().as_secs_f64(),
                self.attempts,
                self.passed,
                self.failed
            );
        }
    }

    pub fn complete(&mut self, error: Option<&anyhow::Error>) {
        if let Some(error) = error {
            self.failed += 1;
            if self.enabled {
                self.bar.suspend(|| {
                    eprintln!("Attempt {} failed: {error:#}", self.attempts);
                });
            }
        } else {
            self.passed += 1;
        }
    }

    pub fn finish(&self) {
        self.bar.finish_and_clear();
        if self.enabled {
            eprintln!(
                "Finished in {:.1}s: {} attempts, {} passed, {} failed",
                self.started.elapsed().as_secs_f64(),
                self.attempts,
                self.passed,
                self.failed
            );
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.bar.finish_and_clear();
    }
}
