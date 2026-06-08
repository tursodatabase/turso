use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;

use crate::profile::{Phase, WorkItem};

#[derive(Debug, Clone)]
pub struct WorkloadTrace {
    inner: Arc<TraceInner>,
}

#[derive(Debug)]
struct TraceInner {
    start: Instant,
    events: Mutex<Vec<TraceEvent>>,
}

#[derive(Debug, Clone, Copy)]
pub struct TraceContext {
    pub phase: Phase,
    pub connection: usize,
    pub batch: u64,
}

#[derive(Debug)]
pub struct BatchTrace {
    trace: WorkloadTrace,
    ctx: TraceContext,
    begin_us: u128,
    statement_count: usize,
    sql_summary: Option<Vec<SqlSummary>>,
}

#[derive(Debug)]
pub struct StatementTrace {
    trace: WorkloadTrace,
    ctx: TraceContext,
    statement: usize,
    begin_us: u128,
    sql: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TraceReport<'a> {
    version: u32,
    time_unit: &'static str,
    time_origin: &'static str,
    mode: &'a str,
    workload: &'a str,
    iterations: usize,
    batch_size: usize,
    connections: usize,
    events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct SqlSummary {
    sql: String,
    count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TraceEvent {
    PhaseMarker {
        phase: &'static str,
        at_us: u128,
    },
    Batch {
        phase: &'static str,
        connection: usize,
        batch: u64,
        begin_us: u128,
        end_us: u128,
        statement_count: usize,
        sql_summary: Vec<SqlSummary>,
    },
    Statement {
        phase: &'static str,
        connection: usize,
        batch: u64,
        statement: usize,
        begin_us: u128,
        end_us: u128,
        sql: String,
    },
}

impl WorkloadTrace {
    pub fn new(start: Instant) -> Self {
        Self {
            inner: Arc::new(TraceInner {
                start,
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn record_phase(&self, phase: Phase) {
        self.push(TraceEvent::PhaseMarker {
            phase: phase_name(phase),
            at_us: self.now_us(),
        });
    }

    pub fn begin_batch(&self, ctx: TraceContext, items: &[WorkItem]) -> BatchTrace {
        BatchTrace {
            trace: self.clone(),
            ctx,
            begin_us: self.now_us(),
            statement_count: items.len(),
            sql_summary: Some(summarize_sql(items)),
        }
    }

    pub fn begin_statement(
        &self,
        ctx: TraceContext,
        statement: usize,
        sql: &str,
    ) -> StatementTrace {
        StatementTrace {
            trace: self.clone(),
            ctx,
            statement,
            begin_us: self.now_us(),
            sql: Some(sql.to_string()),
        }
    }

    pub fn write_report(
        &self,
        path: impl AsRef<Path>,
        mode: &str,
        workload: &str,
        iterations: usize,
        batch_size: usize,
        connections: usize,
    ) -> Result<()> {
        let mut events = self
            .inner
            .events
            .lock()
            .expect("workload trace mutex poisoned")
            .clone();
        events.sort_by_key(trace_event_start_us);

        let report = TraceReport {
            version: 1,
            time_unit: "microseconds",
            time_origin: "same monotonic clock as dhat heap profile, captured immediately after profiler initialization",
            mode,
            workload,
            iterations,
            batch_size,
            connections,
            events,
        };

        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, &report)?;
        Ok(())
    }

    fn now_us(&self) -> u128 {
        self.inner.start.elapsed().as_micros()
    }

    fn push(&self, event: TraceEvent) {
        self.inner
            .events
            .lock()
            .expect("workload trace mutex poisoned")
            .push(event);
    }
}

impl BatchTrace {
    fn finish_now(&mut self) {
        let Some(sql_summary) = self.sql_summary.take() else {
            return;
        };
        self.trace.push(TraceEvent::Batch {
            phase: phase_name(self.ctx.phase),
            connection: self.ctx.connection,
            batch: self.ctx.batch,
            begin_us: self.begin_us,
            end_us: self.trace.now_us(),
            statement_count: self.statement_count,
            sql_summary,
        });
    }
}

impl StatementTrace {
    fn finish_now(&mut self) {
        let Some(sql) = self.sql.take() else {
            return;
        };
        self.trace.push(TraceEvent::Statement {
            phase: phase_name(self.ctx.phase),
            connection: self.ctx.connection,
            batch: self.ctx.batch,
            statement: self.statement,
            begin_us: self.begin_us,
            end_us: self.trace.now_us(),
            sql,
        });
    }
}

impl Drop for BatchTrace {
    fn drop(&mut self) {
        self.finish_now();
    }
}

impl Drop for StatementTrace {
    fn drop(&mut self) {
        self.finish_now();
    }
}

fn trace_event_start_us(event: &TraceEvent) -> u128 {
    match event {
        TraceEvent::PhaseMarker { at_us, .. } => *at_us,
        TraceEvent::Batch { begin_us, .. } | TraceEvent::Statement { begin_us, .. } => *begin_us,
    }
}

fn phase_name(phase: Phase) -> &'static str {
    match phase {
        Phase::Setup => "setup",
        Phase::Run => "run",
        Phase::Checkpoint => "checkpoint",
        Phase::Done => "done",
    }
}

fn summarize_sql(items: &[WorkItem]) -> Vec<SqlSummary> {
    let mut counts = BTreeMap::new();
    for item in items {
        *counts.entry(item.sql.clone()).or_insert(0) += 1;
    }
    counts
        .into_iter()
        .map(|(sql, count)| SqlSummary { sql, count })
        .collect()
}
