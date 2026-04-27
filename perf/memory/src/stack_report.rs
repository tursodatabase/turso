use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use tracing::{Event, Level, Subscriber, field::Visit};
use tracing_subscriber::{
    Layer,
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
};
use turso_parser::parser::Parser as SqlParser;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
    Csv,
}

#[derive(Parser, Debug)]
#[command(name = "stack-report")]
#[command(about = "Run a SQL payload and summarize turso stacker tracing samples")]
struct Args {
    /// SQL file to execute. Use '-' to read from stdin.
    #[arg(long = "sql", short = 's')]
    sql: PathBuf,

    /// Maximum number of lowest-stack labels to print in human output.
    #[arg(long = "top", default_value = "40")]
    top: usize,

    /// Output format.
    #[arg(long = "format", default_value = "human")]
    format: OutputFormat,
}

#[derive(Debug, Default)]
struct StackCollector {
    samples: Mutex<Vec<StackSample>>,
}

#[derive(Debug, Clone, Serialize)]
struct StackSample {
    label: String,
    detail: Option<String>,
    remaining_stack: usize,
}

#[derive(Debug, Clone, Serialize)]
struct StackSummary {
    label: String,
    detail: Option<String>,
    count: usize,
    min_remaining_stack: usize,
    max_remaining_stack: usize,
    avg_remaining_stack: usize,
}

#[derive(Debug, Serialize)]
struct StackReport {
    sql: String,
    db: String,
    samples: usize,
    summaries: Vec<StackSummary>,
}

#[derive(Clone)]
struct StackLayer {
    collector: Arc<StackCollector>,
}

impl<S> Layer<S> for StackLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: LayerContext<'_, S>) -> bool {
        metadata.target() == "turso_stack" && *metadata.level() <= Level::DEBUG
    }

    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if event.metadata().target() != "turso_stack" {
            return;
        }

        let mut visitor = StackEventVisitor::default();
        event.record(&mut visitor);

        let Some(label) = visitor.label else {
            return;
        };
        let Some(remaining_stack) = visitor.remaining_stack else {
            return;
        };

        self.collector
            .samples
            .lock()
            .expect("stack collector mutex poisoned")
            .push(StackSample {
                label,
                detail: visitor.detail,
                remaining_stack,
            });
    }
}

#[derive(Default)]
struct StackEventVisitor {
    label: Option<String>,
    detail: Option<String>,
    remaining_stack: Option<usize>,
}

impl Visit for StackEventVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        match field.name() {
            "label" => self.label = Some(unquote_debug_string(&rendered)),
            "detail" => {
                let detail = parse_debug_option_string(&rendered);
                if detail.as_deref().is_some_and(|value| !value.is_empty()) {
                    self.detail = detail;
                }
            }
            "remaining_stack" => {
                self.remaining_stack = rendered.parse().ok();
            }
            _ => {}
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "label" => self.label = Some(value.to_string()),
            "detail" if !value.is_empty() => self.detail = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "remaining_stack" {
            self.remaining_stack = Some(value as usize);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if field.name() == "remaining_stack" {
            self.remaining_stack = usize::try_from(value).ok();
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    // This single-threaded tool sets the flag before building the database
    // objects, so stack guards consistently emit tracing events.
    unsafe {
        std::env::set_var("TURSO_TRACE_STACK", "1");
    }

    let collector = Arc::new(StackCollector::default());
    let subscriber = tracing_subscriber::registry().with(StackLayer {
        collector: Arc::clone(&collector),
    });
    tracing::subscriber::set_global_default(subscriber)
        .context("failed to install stack tracing subscriber")?;

    let sql = read_sql(&args.sql)?;

    let db = turso::Builder::new_local(":memory")
        .experimental_generated_columns(true)
        .experimental_custom_types(true)
        .experimental_materialized_views(true)
        .build()
        .await?;
    let conn = db.connect()?;
    execute_sql_payload(&conn, &sql).await?;

    let samples = collector
        .samples
        .lock()
        .expect("stack collector mutex poisoned")
        .clone();
    let report = StackReport {
        sql: args.sql.display().to_string(),
        db: "memory".to_string(),
        samples: samples.len(),
        summaries: summarize(samples),
    };

    match args.format {
        OutputFormat::Human => print_human(&report, args.top),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Csv => print_csv(&report),
    }

    Ok(())
}

fn read_sql(path: &PathBuf) -> Result<String> {
    if path.as_os_str() == "-" {
        std::io::read_to_string(std::io::stdin()).context("failed to read SQL from stdin")
    } else {
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))
    }
}

async fn execute_sql_payload(conn: &turso::Connection, sql: &str) -> Result<()> {
    for statement in split_sql_statements(sql)? {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }

        let mut stmt = conn
            .prepare(statement)
            .await
            .with_context(|| format!("failed to prepare statement: {statement}"))?;
        if stmt.column_count() == 0 {
            stmt.execute(())
                .await
                .with_context(|| format!("failed to execute statement: {statement}"))?;
        } else {
            let mut rows = stmt
                .query(())
                .await
                .with_context(|| format!("failed to query statement: {statement}"))?;
            while rows
                .next()
                .await
                .with_context(|| format!("failed to drain statement rows: {statement}"))?
                .is_some()
            {}
        }
    }
    Ok(())
}

fn split_sql_statements(sql: &str) -> Result<Vec<&str>> {
    let mut statements = Vec::new();
    let mut parser = SqlParser::new(sql.as_bytes());
    let mut start = 0;
    while parser.next_cmd()?.is_some() {
        let end = parser.offset();
        let statement = &sql[start..end];
        if !statement.trim().is_empty() {
            statements.push(statement);
        }
        start = end;
    }
    Ok(statements)
}

fn summarize(samples: Vec<StackSample>) -> Vec<StackSummary> {
    let mut grouped: BTreeMap<(String, Option<String>), (usize, usize, usize, usize)> =
        BTreeMap::new();

    for sample in samples {
        let key = (sample.label, sample.detail);
        let entry = grouped.entry(key).or_insert((0, usize::MAX, 0, 0));
        entry.0 += 1;
        entry.1 = entry.1.min(sample.remaining_stack);
        entry.2 = entry.2.max(sample.remaining_stack);
        entry.3 += sample.remaining_stack;
    }

    let mut summaries = grouped
        .into_iter()
        .map(|((label, detail), (count, min, max, sum))| StackSummary {
            label,
            detail,
            count,
            min_remaining_stack: min,
            max_remaining_stack: max,
            avg_remaining_stack: sum / count,
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|a, b| {
        a.min_remaining_stack
            .cmp(&b.min_remaining_stack)
            .then_with(|| a.label.cmp(&b.label))
            .then_with(|| a.detail.cmp(&b.detail))
    });
    summaries
}

fn print_human(report: &StackReport, top: usize) {
    println!("=== STACK USAGE REPORT ===");
    println!("SQL:     {}", report.sql);
    println!("DB:      {}", report.db);
    println!("Samples: {}", report.samples);
    println!();
    println!(
        "{:>8} {:>12} {:>12} {:>12}  label",
        "count", "min", "max", "avg"
    );
    for summary in report.summaries.iter().take(top) {
        let label = match &summary.detail {
            Some(detail) => format!("{} detail={detail}", summary.label),
            None => summary.label.clone(),
        };
        println!(
            "{:>8} {:>12} {:>12} {:>12}  {}",
            summary.count,
            summary.min_remaining_stack,
            summary.max_remaining_stack,
            summary.avg_remaining_stack,
            label
        );
    }
}

fn print_csv(report: &StackReport) {
    println!("label,detail,count,min_remaining_stack,max_remaining_stack,avg_remaining_stack");
    for summary in &report.summaries {
        println!(
            "{},{},{},{},{},{}",
            csv_escape(&summary.label),
            csv_escape(summary.detail.as_deref().unwrap_or("")),
            summary.count,
            summary.min_remaining_stack,
            summary.max_remaining_stack,
            summary.avg_remaining_stack
        );
    }
}

fn unquote_debug_string(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
        .to_string()
}

fn parse_debug_option_string(value: &str) -> Option<String> {
    if value == "None" {
        return None;
    }
    value
        .strip_prefix("Some(\"")
        .and_then(|value| value.strip_suffix("\")"))
        .map(str::to_string)
        .or_else(|| Some(unquote_debug_string(value)))
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
