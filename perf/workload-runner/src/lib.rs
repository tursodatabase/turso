pub mod cli;
pub mod db;
pub mod process;
pub mod retry;
pub mod workloads;

#[cfg(test)]
mod tests;

use db::{Connection, Database, Error, Result};
use hdrhistogram::Histogram;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

pub type LocalFuture<'a, T = ()> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;
pub type Task = Arc<dyn for<'a> Fn(&'a Connection, Worker) -> LocalFuture<'a> + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Limit {
    Once,
    Count(u64),
    Duration(Duration),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Load {
    Continuous,
    Fixed { per_second: f64 },
    Poisson { per_second: f64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Window {
    pub start: Duration,
    pub end: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Measurement {
    Off,
    All,
    Windows(Vec<Window>),
}

impl Measurement {
    fn contains(&self, offset: Duration) -> bool {
        match self {
            Self::Off => false,
            Self::All => true,
            Self::Windows(w) => w.iter().any(|w| w.start <= offset && offset < w.end),
        }
    }

    fn end(&self, offset: Duration, stage_end: Duration) -> Duration {
        match self {
            Self::Windows(w) => w
                .iter()
                .find(|w| w.start <= offset && offset < w.end)
                .map_or(stage_end, |w| w.end),
            _ => stage_end,
        }
    }

    fn seconds(&self, elapsed: Duration) -> f64 {
        match self {
            Self::Off => 0.0,
            Self::All => elapsed.as_secs_f64(),
            Self::Windows(w) => w
                .iter()
                .map(|w| w.end.min(elapsed).saturating_sub(w.start).as_secs_f64())
                .sum(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stage {
    pub name: String,
    pub groups: Vec<String>,
    pub limit: Limit,
    pub load: Load,
    pub measurement: Measurement,
    pub timeout: Duration,
    pub drain_timeout: Duration,
    pub raw_samples: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    pub stages: Vec<Stage>,
    pub seed: u64,
    pub startup_timeout: Duration,
}

impl Plan {
    pub fn validate(&self, groups: &[Group]) -> Result<()> {
        let names: BTreeSet<_> = groups.iter().map(|g| &g.name).collect();
        if groups.is_empty() || names.len() != groups.len() || groups.iter().any(|g| g.workers == 0)
        {
            return Err(Error::Workload(
                "groups must be nonempty and uniquely named".into(),
            ));
        }
        let mut stages = BTreeSet::new();
        for s in &self.stages {
            if !stages.insert(&s.name)
                || s.groups.is_empty()
                || s.groups.iter().collect::<BTreeSet<_>>().len() != s.groups.len()
                || s.groups.iter().any(|g| !names.contains(g))
            {
                return Err(Error::Workload(
                    "invalid stage name or worker groups".into(),
                ));
            }
            if s.timeout.is_zero() || s.drain_timeout.is_zero() || self.startup_timeout.is_zero() {
                return Err(Error::Workload("timeouts must be positive".into()));
            }
            match s.limit {
                Limit::Count(0) => return Err(Error::Workload("count must be positive".into())),
                Limit::Duration(d) if d.is_zero() || d > s.timeout => {
                    return Err(Error::Workload(
                        "duration must be positive and within timeout".into(),
                    ))
                }
                _ => {}
            }
            if let Load::Fixed { per_second } | Load::Poisson { per_second } = s.load {
                if !per_second.is_finite() || per_second <= 0.0 || per_second > 1e9 {
                    return Err(Error::Workload(
                        "rate must be finite, positive and at most 1e9/s".into(),
                    ));
                }
            }
            if let Measurement::Windows(w) = &s.measurement {
                let mut end = Duration::ZERO;
                for window in w {
                    let limit = match s.limit {
                        Limit::Duration(d) => d,
                        _ => s.timeout,
                    };
                    if window.start < end || window.end <= window.start || window.end > limit {
                        return Err(Error::Workload(
                            "measurement windows must be ordered, disjoint and within the stage"
                                .into(),
                        ));
                    }
                    end = window.end;
                }
            }
        }
        Ok(())
    }
}

pub struct Group {
    pub name: String,
    pub workers: usize,
    pub task: Task,
}

#[derive(Clone)]
pub struct Signal(Arc<tokio::sync::watch::Sender<bool>>);

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}

impl Signal {
    pub fn new() -> Self {
        Self(Arc::new(tokio::sync::watch::channel(false).0))
    }

    pub fn set(&self) {
        self.0.send_replace(true);
    }

    pub fn is_set(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn wait(&self, cancellation: &Signal) -> Result<()> {
        let mut signal = self.0.subscribe();
        let mut cancel = cancellation.0.subscribe();
        loop {
            if *cancel.borrow_and_update() {
                return Err(Error::Cancelled);
            }
            if *signal.borrow_and_update() {
                return Ok(());
            }
            tokio::select! {
                _ = signal.changed() => {},
                _ = cancel.changed() => {},
            }
        }
    }

    pub async fn sleep(&self, duration: Duration) -> Result<()> {
        let mut cancel = self.0.subscribe();
        if *cancel.borrow_and_update() {
            return Err(Error::Cancelled);
        }
        tokio::select! {
            _ = tokio::time::sleep(duration) => Ok(()),
            _ = cancel.changed() => Err(Error::Cancelled),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub scheduled_ns: u64,
    pub started_ns: u64,
    pub completed_ns: u64,
    pub attempts: u64,
    pub success: bool,
    pub drain: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Distribution {
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    pub successes: u64,
    pub failures: u64,
    pub unfinished: u64,
    pub attempts: u64,
    pub retries: u64,
    pub window_completions: u64,
    pub drain_completions: u64,
    pub throughput_per_second: f64,
    pub execution: Distribution,
    pub scheduling_delay: Distribution,
    pub response: Distribution,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone)]
struct Recorder {
    metrics: Metrics,
    execution: Histogram<u64>,
    scheduling: Histogram<u64>,
    response: Histogram<u64>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            metrics: Metrics {
                successes: 0,
                failures: 0,
                unfinished: 0,
                attempts: 0,
                retries: 0,
                window_completions: 0,
                drain_completions: 0,
                throughput_per_second: 0.0,
                execution: Distribution::default(),
                scheduling_delay: Distribution::default(),
                response: Distribution::default(),
                samples: Vec::new(),
            },
            execution: Histogram::new(3).unwrap(),
            scheduling: Histogram::new(3).unwrap(),
            response: Histogram::new(3).unwrap(),
        }
    }

    fn record(&mut self, sample: Sample, raw: bool) {
        self.metrics.attempts += sample.attempts;
        self.metrics.retries += sample.attempts.saturating_sub(1);
        if sample.success {
            self.metrics.successes += 1;
        } else {
            self.metrics.failures += 1;
        }
        if sample.drain {
            self.metrics.drain_completions += 1;
        } else {
            self.metrics.window_completions += 1;
        }
        self.execution
            .record(sample.completed_ns - sample.started_ns)
            .unwrap();
        self.scheduling
            .record(sample.started_ns - sample.scheduled_ns)
            .unwrap();
        self.response
            .record(sample.completed_ns - sample.scheduled_ns)
            .unwrap();
        if raw {
            self.metrics.samples.push(sample);
        }
    }

    fn merge(&mut self, other: &Self) {
        self.metrics.successes += other.metrics.successes;
        self.metrics.failures += other.metrics.failures;
        self.metrics.unfinished += other.metrics.unfinished;
        self.metrics.attempts += other.metrics.attempts;
        self.metrics.retries += other.metrics.retries;
        self.metrics.window_completions += other.metrics.window_completions;
        self.metrics.drain_completions += other.metrics.drain_completions;
        self.execution.add(&other.execution).unwrap();
        self.scheduling.add(&other.scheduling).unwrap();
        self.response.add(&other.response).unwrap();
    }

    fn finish(mut self, seconds: f64) -> Metrics {
        self.metrics.execution = distribution(&self.execution);
        self.metrics.scheduling_delay = distribution(&self.scheduling);
        self.metrics.response = distribution(&self.response);
        self.metrics.throughput_per_second = if seconds > 0.0 {
            self.metrics.successes as f64 / seconds
        } else {
            0.0
        };
        self.metrics
    }
}

fn distribution(h: &Histogram<u64>) -> Distribution {
    Distribution {
        p50_ns: h.value_at_quantile(0.5),
        p95_ns: h.value_at_quantile(0.95),
        p99_ns: h.value_at_quantile(0.99),
        max_ns: h.max(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerReport {
    pub worker: usize,
    pub group: String,
    pub stage: String,
    pub operations: BTreeMap<String, Metrics>,
    pub all_successes: u64,
    pub all_failures: u64,
    pub error: Option<String>,
    #[serde(skip)]
    records: BTreeMap<String, Recorder>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StageReport {
    pub name: String,
    pub elapsed_ns: u64,
    pub drain_ns: u64,
    pub skipped: u64,
    pub backlog_at_deadline: u64,
    pub unfinished_operations: u64,
    pub unfinished_workers: usize,
    pub operations: BTreeMap<String, Metrics>,
    pub workers: Vec<WorkerReport>,
    pub complete: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub schema_version: u32,
    pub plan: Plan,
    pub groups: BTreeMap<String, usize>,
    pub settings: db::Settings,
    pub transaction_sql: String,
    pub stages: Vec<StageReport>,
    pub complete: bool,
    pub errors: Vec<String>,
}

struct Schedule {
    next: u64,
    due: Duration,
    rng: ChaCha8Rng,
    admitted: u64,
    completed: u64,
    completed_before_deadline: u64,
}

struct SharedStage {
    spec: Stage,
    start: Instant,
    schedule: Mutex<Schedule>,
    cancel: Signal,
    abort: Signal,
}

pub struct Worker {
    pub id: usize,
    pub group: String,
    pub seed: u64,
    commands: tokio::sync::mpsc::UnboundedReceiver<Arc<SharedStage>>,
    events: mpsc::Sender<Event>,
    ready: bool,
    pending_report: Arc<Mutex<Option<WorkerReport>>>,
}

impl Worker {
    pub async fn next_stage(&mut self) -> Option<StageRun> {
        if let Some(report) = self.pending_report.lock().unwrap().take() {
            self.events.send(Event::Stage(report)).ok()?;
        }
        if !self.ready {
            self.events.send(Event::Ready).ok()?;
            self.ready = true;
        }
        let shared = self.commands.recv().await?;
        Some(StageRun {
            shared,
            pending_report: self.pending_report.clone(),
            worker: self.id,
            group: self.group.clone(),
            records: BTreeMap::new(),
            all_successes: 0,
            all_failures: 0,
            pending: None,
        })
    }
}

pub struct Admission {
    scheduled: Duration,
    started: Instant,
    counted: bool,
}

pub struct StageRun {
    shared: Arc<SharedStage>,
    pending_report: Arc<Mutex<Option<WorkerReport>>>,
    worker: usize,
    group: String,
    records: BTreeMap<String, Recorder>,
    all_successes: u64,
    all_failures: u64,
    pending: Option<String>,
}

impl StageRun {
    pub fn name(&self) -> &str {
        &self.shared.spec.name
    }

    pub fn cancellation(&self) -> &Signal {
        &self.shared.cancel
    }

    pub fn abort(&self) -> &Signal {
        &self.shared.abort
    }

    pub fn elapsed(&self) -> Duration {
        self.shared.start.elapsed()
    }

    pub async fn next(&mut self) -> Option<Admission> {
        let shared = &self.shared;
        if shared.cancel.is_set() {
            return None;
        }
        let scheduled = {
            let mut schedule = shared.schedule.lock().unwrap();
            let now = shared.start.elapsed();
            let deadline = match shared.spec.limit {
                Limit::Duration(d) => d,
                _ => shared.spec.timeout,
            };
            if now >= deadline {
                return None;
            }
            let count = match shared.spec.limit {
                Limit::Once => 1,
                Limit::Count(n) => n,
                Limit::Duration(_) => u64::MAX,
            };
            if schedule.next >= count {
                return None;
            }
            let scheduled = match shared.spec.load {
                Load::Continuous => now,
                _ => schedule.due,
            };
            if scheduled >= deadline {
                return None;
            }
            schedule.next += 1;
            advance(&mut schedule, &shared.spec.load);
            scheduled
        };
        let delay = (shared.start + scheduled).saturating_duration_since(Instant::now());
        if !delay.is_zero() && shared.cancel.sleep(delay).await.is_err() {
            return None;
        }
        let deadline = match shared.spec.limit {
            Limit::Duration(d) => d,
            _ => shared.spec.timeout,
        };
        let mut schedule = shared.schedule.lock().unwrap();
        let started = Instant::now();
        if shared.cancel.is_set() || started - shared.start >= deadline {
            return None;
        }
        schedule.admitted += 1;
        Some(Admission {
            scheduled,
            started,
            counted: true,
        })
    }

    pub async fn measure<T>(
        &mut self,
        name: &str,
        admission: Admission,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let attempts = std::cell::Cell::new(1);
        self.measure_attempts(name, admission, &attempts, future)
            .await
    }

    pub async fn measure_custom<T>(
        &mut self,
        name: &str,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let admission = Admission {
            scheduled: self.elapsed(),
            started: Instant::now(),
            counted: false,
        };
        self.measure(name, admission, future).await
    }

    pub async fn measure_with<T>(
        &mut self,
        name: &str,
        admission: Admission,
        operation: impl std::ops::AsyncFnOnce(&mut Submeasurements) -> Result<T>,
    ) -> Result<T> {
        let scheduled = admission.scheduled;
        let mut measurements = Submeasurements {
            start: self.shared.start,
            samples: Vec::new(),
        };
        let result = self
            .measure(name, admission, operation(&mut measurements))
            .await;
        if self.shared.spec.measurement.contains(scheduled) {
            let stage_end = match self.shared.spec.limit {
                Limit::Duration(d) => d,
                _ => self.shared.spec.timeout,
            };
            let end = nanos(self.shared.spec.measurement.end(scheduled, stage_end));
            for (subname, mut sample) in measurements.samples {
                sample.drain = sample.completed_ns >= end;
                self.records
                    .entry(format!("{name}/{subname}"))
                    .or_insert_with(Recorder::new)
                    .record(sample, self.shared.spec.raw_samples);
            }
        }
        result
    }

    pub async fn measure_attempts<T>(
        &mut self,
        name: &str,
        admission: Admission,
        attempts: &std::cell::Cell<u64>,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        self.pending = self
            .shared
            .spec
            .measurement
            .contains(admission.scheduled)
            .then(|| name.into());
        let result = operation.await;
        let completed = self.elapsed();
        if admission.counted {
            let mut schedule = self.shared.schedule.lock().unwrap();
            schedule.completed += 1;
            let deadline = match self.shared.spec.limit {
                Limit::Duration(d) => d,
                _ => self.shared.spec.timeout,
            };
            if completed < deadline {
                schedule.completed_before_deadline += 1;
            }
        }
        if result.is_ok() {
            self.all_successes += 1;
        } else {
            self.all_failures += 1;
        }
        if self.pending.take().is_some() {
            let stage_end = match self.shared.spec.limit {
                Limit::Duration(d) => d,
                _ => self.shared.spec.timeout,
            };
            let end = self
                .shared
                .spec
                .measurement
                .end(admission.scheduled, stage_end);
            self.records
                .entry(name.into())
                .or_insert_with(Recorder::new)
                .record(
                    Sample {
                        scheduled_ns: nanos(admission.scheduled),
                        started_ns: nanos(admission.started - self.shared.start),
                        completed_ns: nanos(completed),
                        attempts: attempts.get(),
                        success: result.is_ok(),
                        drain: completed >= end,
                    },
                    self.shared.spec.raw_samples,
                );
        }
        result
    }

    pub async fn repeat<S>(
        &mut self,
        name: &str,
        state: &mut S,
        mut operation: impl for<'a> FnMut(&'a mut S) -> LocalFuture<'a>,
    ) -> Result<()> {
        while let Some(admission) = self.next().await {
            self.measure(name, admission, operation(state)).await?;
        }
        Ok(())
    }
}

pub struct Submeasurements {
    start: Instant,
    samples: Vec<(String, Sample)>,
}

impl Submeasurements {
    pub async fn measure<T>(
        &mut self,
        name: &str,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let started_ns = nanos(self.start.elapsed());
        let result = operation.await;
        self.samples.push((
            name.into(),
            Sample {
                scheduled_ns: started_ns,
                started_ns,
                completed_ns: nanos(self.start.elapsed()),
                attempts: 1,
                success: result.is_ok(),
                drain: false,
            },
        ));
        result
    }
}

impl Drop for StageRun {
    fn drop(&mut self) {
        if let Some(name) = self.pending.take() {
            self.records
                .entry(name)
                .or_insert_with(Recorder::new)
                .metrics
                .unfinished += 1;
        }
        let report = WorkerReport {
            worker: self.worker,
            group: self.group.clone(),
            stage: self.name().into(),
            operations: BTreeMap::new(),
            all_successes: self.all_successes,
            all_failures: self.all_failures,
            error: None,
            records: std::mem::take(&mut self.records),
        };
        let previous = self.pending_report.lock().unwrap().replace(report);
        assert!(
            previous.is_none(),
            "worker must request the next stage before running it"
        );
    }
}

enum Event {
    Ready,
    Stage(WorkerReport),
    Finished(Result<()>),
}

pub fn run(db: Database, plan: Plan, groups: Vec<Group>) -> Result<Report> {
    plan.validate(&groups)?;
    let (events, receive) = mpsc::channel();
    let mut commands = Vec::new();
    let mut handles = Vec::new();
    let group_sizes = groups.iter().map(|g| (g.name.clone(), g.workers)).collect();
    for group in groups {
        for _ in 0..group.workers {
            let id = handles.len();
            let (send, recv) = tokio::sync::mpsc::unbounded_channel();
            commands.push((group.name.clone(), send));
            let worker = Worker {
                id,
                group: group.name.clone(),
                seed: plan.seed.wrapping_add(id as u64),
                commands: recv,
                events: events.clone(),
                ready: false,
                pending_report: Arc::new(Mutex::new(None)),
            };
            let db = db.clone();
            let task = group.task.clone();
            let events = events.clone();
            let pending_report = worker.pending_report.clone();
            handles.push(thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime().block_on(async {
                        let conn = db.connect().await?;
                        task(&conn, worker).await
                    })
                }))
                .unwrap_or_else(|_| Err(Error::Workload("worker panicked".into())));
                if let Some(mut report) = pending_report.lock().unwrap().take() {
                    report.error = result.as_ref().err().map(ToString::to_string);
                    let _ = events.send(Event::Stage(report));
                }
                let _ = events.send(Event::Finished(result));
            }));
        }
    }
    drop(events);
    let mut report = Report {
        schema_version: 1,
        settings: db.settings().clone(),
        transaction_sql: db.settings().transaction.sql().into(),
        plan: plan.clone(),
        groups: group_sizes,
        stages: Vec::new(),
        complete: true,
        errors: Vec::new(),
    };
    let startup = Instant::now() + plan.startup_timeout;
    for _ in &handles {
        match receive.recv_timeout(startup.saturating_duration_since(Instant::now())) {
            Ok(Event::Ready) => {}
            other => {
                report.errors.push(event_error(other));
                break;
            }
        }
    }
    if report.errors.is_empty() {
        for spec in &plan.stages {
            let shared = Arc::new(SharedStage {
                spec: spec.clone(),
                start: Instant::now(),
                cancel: Signal::new(),
                abort: Signal::new(),
                schedule: Mutex::new(Schedule {
                    next: 0,
                    due: Duration::ZERO,
                    rng: ChaCha8Rng::seed_from_u64(plan.seed),
                    admitted: 0,
                    completed: 0,
                    completed_before_deadline: 0,
                }),
            });
            let mut expected = 0;
            for (group, send) in &commands {
                if spec.groups.contains(group) {
                    if send.send(shared.clone()).is_ok() {
                        expected += 1;
                    } else {
                        report.errors.push(format!("worker in {group} exited"));
                    }
                }
            }
            let admission_end = match spec.limit {
                Limit::Duration(d) => d,
                _ => spec.timeout,
            };
            let mut workers = Vec::new();
            let deadline = shared.start + admission_end;
            let mut cancelled = false;
            while workers.len() < expected {
                let until = if cancelled {
                    deadline + spec.drain_timeout
                } else {
                    deadline
                };
                match receive.recv_timeout(until.saturating_duration_since(Instant::now())) {
                    Ok(Event::Stage(w)) => {
                        if let Some(error) = &w.error {
                            report.errors.push(error.clone());
                        }
                        workers.push(w);
                        if !report.errors.is_empty() {
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) if !cancelled => {
                        shared.cancel.set();
                        cancelled = true;
                    }
                    other => {
                        report.errors.push(event_error(other));
                        break;
                    }
                }
            }
            let mut schedule = shared.schedule.lock().unwrap();
            shared.cancel.set();
            if workers.len() != expected || !report.errors.is_empty() {
                shared.abort.set();
            }
            if matches!(spec.limit, Limit::Duration(_)) && report.errors.is_empty() {
                thread::sleep(deadline.saturating_duration_since(Instant::now()));
            }
            let elapsed = shared.start.elapsed();
            let skipped = skipped(&mut schedule, spec);
            let measured_elapsed = match spec.limit {
                Limit::Duration(d) => d,
                _ => elapsed,
            };
            let seconds = spec.measurement.seconds(measured_elapsed);
            let mut totals = BTreeMap::new();
            for worker in &mut workers {
                for (name, recorder) in std::mem::take(&mut worker.records) {
                    totals
                        .entry(name.clone())
                        .or_insert_with(Recorder::new)
                        .merge(&recorder);
                    worker.operations.insert(name, recorder.finish(seconds));
                }
            }
            let complete = workers.len() == expected
                && report.errors.is_empty()
                && schedule.admitted == schedule.completed
                && (!matches!(spec.limit, Limit::Count(_)) || skipped == 0)
                && workers.iter().all(|w| {
                    w.all_failures == 0 && w.operations.values().all(|m| m.unfinished == 0)
                });
            report.stages.push(StageReport {
                name: spec.name.clone(),
                elapsed_ns: nanos(elapsed),
                drain_ns: nanos(elapsed.saturating_sub(admission_end)),
                skipped,
                backlog_at_deadline: schedule.admitted - schedule.completed_before_deadline
                    + skipped,
                unfinished_operations: schedule.admitted - schedule.completed,
                unfinished_workers: expected - workers.len(),
                operations: totals
                    .into_iter()
                    .map(|(name, recorder)| (name, recorder.finish(seconds)))
                    .collect(),
                workers,
                complete,
            });
            if !complete {
                report.complete = false;
                break;
            }
        }
    }
    drop(commands);
    let shutdown = Instant::now() + plan.startup_timeout;
    for handle in handles {
        while !handle.is_finished() && Instant::now() < shutdown {
            thread::sleep(Duration::from_millis(1));
        }
        if handle.is_finished() {
            let _ = handle.join();
        } else {
            report
                .errors
                .push("worker did not stop; child process must be terminated".into());
        }
    }
    while let Ok(event) = receive.try_recv() {
        if let Event::Finished(Err(e)) = event {
            report.errors.push(e.to_string());
        }
    }
    report.complete &= report.errors.is_empty();
    Ok(report)
}

fn event_error(event: std::result::Result<Event, mpsc::RecvTimeoutError>) -> String {
    match event {
        Ok(Event::Finished(Err(e))) => e.to_string(),
        Ok(Event::Finished(Ok(()))) => "worker exited before completing the plan".into(),
        Ok(_) => "unexpected worker event".into(),
        Err(e) => format!("worker wait failed: {e}"),
    }
}

fn advance(schedule: &mut Schedule, load: &Load) {
    match load {
        Load::Continuous => {}
        Load::Fixed { per_second } => {
            schedule.due = Duration::from_secs_f64(schedule.next as f64 / per_second);
        }
        Load::Poisson { per_second } => {
            let gap = -(1.0 - schedule.rng.random::<f64>()).ln() / per_second;
            schedule.due += Duration::from_secs_f64(gap).max(Duration::from_nanos(1));
        }
    }
}

fn skipped(schedule: &mut Schedule, spec: &Stage) -> u64 {
    match spec.limit {
        Limit::Once => schedule.next.saturating_sub(schedule.admitted),
        Limit::Count(n) => n.saturating_sub(schedule.admitted),
        Limit::Duration(d) => {
            if matches!(spec.load, Load::Continuous) {
                return 0;
            }
            while schedule.due < d {
                schedule.next += 1;
                advance(schedule, &spec.load);
            }
            schedule.next - schedule.admitted
        }
    }
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}
