use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::{Duration, Instant};
use turso_stress::sync::{Arc, StdMutex};

const TEXT_REPORT_INTERVAL: Duration = Duration::from_secs(10);

pub struct ProgressBars {
    num_ticks: usize,
    multi_progress: MultiProgress,
    text_report: Option<Arc<StdMutex<TextReport>>>,
}

impl ProgressBars {
    pub(crate) fn new(num_ticks: usize) -> Self {
        let multi_progress = MultiProgress::new();
        let text_report = multi_progress.is_hidden().then(|| {
            Arc::new(StdMutex::new(TextReport::new(
                Instant::now(),
                ShuttleStats::current(),
            )))
        });
        Self {
            multi_progress,
            num_ticks,
            text_report,
        }
    }

    pub(crate) fn add(&self, id: String) -> Progress {
        let progress_bar = self
            .multi_progress
            .add(ProgressBar::new(self.num_ticks as u64));

        progress_bar.set_style(Self::style());
        progress_bar.set_prefix(id);

        if let Some(text_report) = &self.text_report {
            text_report.lock().unwrap().planned_iterations += self.num_ticks as u64;
        }

        Progress::new(progress_bar, self.text_report.clone())
    }

    fn style() -> ProgressStyle {
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] {prefix} {bar:40.cyan/blue} {pos:>7}/{len:7} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("##-")
    }
}

#[derive(Clone)]
pub struct Progress {
    progress_bar: ProgressBar,
    text_report: Option<Arc<StdMutex<TextReport>>>,
}

impl Progress {
    fn new(progress_bar: ProgressBar, text_report: Option<Arc<StdMutex<TextReport>>>) -> Self {
        progress_bar.set_message("executing queries...");

        Self {
            progress_bar,
            text_report,
        }
    }

    pub fn tick(&mut self) {
        self.progress_bar.inc(1);
        if let Some(text_report) = &self.text_report {
            let line = text_report
                .lock()
                .unwrap()
                .record_iteration(Instant::now(), ShuttleStats::current());
            if let Some(line) = line {
                println!("{line}");
            }
        }
    }

    pub fn finish(&mut self) {
        self.progress_bar.finish_with_message("done");
        if let Some(text_report) = &self.text_report {
            if let Some(line) = text_report.lock().unwrap().finish(Instant::now()) {
                println!("{line}");
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShuttleStats {
    steps: u64,
    tasks_created: u64,
}

impl ShuttleStats {
    #[cfg(shuttle)]
    fn current() -> Option<Self> {
        let current_task: usize = shuttle::current::me().into();
        Some(Self {
            steps: shuttle::current::context_switches() as u64,
            tasks_created: current_task as u64 + 1,
        })
    }

    #[cfg(not(shuttle))]
    fn current() -> Option<Self> {
        None
    }
}

struct TextReport {
    started: Instant,
    planned_iterations: u64,
    iterations: u64,
    interval_started: Instant,
    interval_iterations: u64,
    interval_started_shuttle_steps: Option<u64>,
    shuttle_tasks_created: u64,
    finished: bool,
}

impl TextReport {
    fn new(now: Instant, shuttle_stats: Option<ShuttleStats>) -> Self {
        Self {
            started: now,
            planned_iterations: 0,
            iterations: 0,
            interval_started: now,
            interval_iterations: 0,
            interval_started_shuttle_steps: shuttle_stats.map(|stats| stats.steps),
            shuttle_tasks_created: shuttle_stats.map_or(0, |stats| stats.tasks_created),
            finished: false,
        }
    }

    fn record_iteration(
        &mut self,
        now: Instant,
        shuttle_stats: Option<ShuttleStats>,
    ) -> Option<String> {
        self.iterations += 1;
        self.interval_iterations += 1;
        if let Some(stats) = shuttle_stats {
            self.shuttle_tasks_created = self.shuttle_tasks_created.max(stats.tasks_created);
        }
        let interval = now.duration_since(self.interval_started);
        if interval < TEXT_REPORT_INTERVAL {
            return None;
        }
        let mut line = format!(
            "[{:>8.1}s] {}/{} iterations, {:.1} iterations/s in the last {:.1}s",
            now.duration_since(self.started).as_secs_f64(),
            self.iterations,
            self.planned_iterations,
            self.interval_iterations as f64 / interval.as_secs_f64(),
            interval.as_secs_f64(),
        );
        if let (Some(stats), Some(started_steps)) =
            (shuttle_stats, self.interval_started_shuttle_steps)
        {
            line.push_str(&format!(
                ", {} shuttle steps/iteration, {} shuttle tasks created",
                (stats.steps - started_steps) / self.interval_iterations,
                self.shuttle_tasks_created,
            ));
        }
        self.interval_started = now;
        self.interval_iterations = 0;
        self.interval_started_shuttle_steps = shuttle_stats.map(|stats| stats.steps);
        Some(line)
    }

    fn finish(&mut self, now: Instant) -> Option<String> {
        if std::mem::replace(&mut self.finished, true) {
            return None;
        }
        let elapsed = now.duration_since(self.started).as_secs_f64();
        Some(format!(
            "[{elapsed:>8.1}s] {}/{} iterations done, {:.1} iterations/s on average",
            self.iterations,
            self.planned_iterations,
            self.iterations as f64 / elapsed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_report_prints_the_rate_once_per_interval() {
        let start = Instant::now();
        let mut report = TextReport::new(start, None);
        report.planned_iterations = 1000;

        for i in 1..100 {
            let now = start + TEXT_REPORT_INTERVAL * i / 100;
            assert_eq!(report.record_iteration(now, None), None);
        }
        let first_interval_end = start + TEXT_REPORT_INTERVAL;
        assert_eq!(
            report.record_iteration(first_interval_end, None).as_deref(),
            Some("[    10.0s] 100/1000 iterations, 10.0 iterations/s in the last 10.0s")
        );

        for _ in 1..50 {
            assert_eq!(report.record_iteration(first_interval_end, None), None);
        }
        let second_interval_end = start + TEXT_REPORT_INTERVAL * 2;
        assert_eq!(
            report
                .record_iteration(second_interval_end, None)
                .as_deref(),
            Some("[    20.0s] 150/1000 iterations, 5.0 iterations/s in the last 10.0s")
        );
    }

    #[test]
    fn text_report_prints_shuttle_steps_per_iteration() {
        let start = Instant::now();
        let stats = |steps, tasks_created| {
            Some(ShuttleStats {
                steps,
                tasks_created,
            })
        };
        let mut report = TextReport::new(start, stats(1_000, 1));
        report.planned_iterations = 100;

        assert_eq!(report.record_iteration(start, stats(4_000, 3)), None);
        assert_eq!(report.record_iteration(start, stats(7_000, 3)), None);
        assert_eq!(
            report
                .record_iteration(start + TEXT_REPORT_INTERVAL, stats(10_000, 5))
                .as_deref(),
            Some(
                "[    10.0s] 3/100 iterations, 0.3 iterations/s in the last 10.0s, \
                 3000 shuttle steps/iteration, 5 shuttle tasks created"
            )
        );
    }

    #[test]
    fn text_report_prints_the_summary_once() {
        let start = Instant::now();
        let mut report = TextReport::new(start, None);
        report.planned_iterations = 20;
        for _ in 0..20 {
            report.record_iteration(start, None);
        }
        let end = start + Duration::from_secs(4);
        assert_eq!(
            report.finish(end).as_deref(),
            Some("[     4.0s] 20/20 iterations done, 5.0 iterations/s on average")
        );
        assert_eq!(report.finish(end), None);
    }
}
