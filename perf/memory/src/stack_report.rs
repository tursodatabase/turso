use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use tracing::{Event, Id, Level, Subscriber, field::Visit, span::Attributes};
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

    /// Maximum number of heaviest span samples to print per statement in human output.
    #[arg(long = "top", default_value = "40")]
    top: usize,

    /// Output format.
    #[arg(long = "format", default_value = "human")]
    format: OutputFormat,

    /// Only include reports for these 1-based statement indexes. Can be repeated or comma-separated.
    #[arg(long = "statement", value_delimiter = ',')]
    statements: Vec<usize>,

    /// Only include reports for statements whose SQL contains this substring. Can be repeated.
    #[arg(long = "sql-contains")]
    sql_contains: Vec<String>,
}

#[derive(Debug, Default)]
struct StackCollector {
    samples: Mutex<Vec<StackSample>>,
}

impl StackCollector {
    fn sample_count(&self) -> usize {
        self.samples
            .lock()
            .expect("stack collector mutex poisoned")
            .len()
    }

    fn samples_from(&self, start: usize) -> Vec<StackSample> {
        self.samples.lock().expect("stack collector mutex poisoned")[start..].to_vec()
    }
}

#[derive(Debug, Clone, Serialize)]
struct StackSample {
    label: String,
    detail: Option<String>,
    phase: StackPhase,
    remaining_stack: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StackPhase {
    Enter,
    Exit,
    Sample,
}

#[derive(Debug, Clone, Serialize)]
struct StatementReport {
    index: usize,
    sql: String,
    baseline_remaining_stack: usize,
    min_remaining_stack: usize,
    stack_used: usize,
    samples: usize,
    span_aggregates: Vec<SpanAggregateReport>,
    spans: Vec<SpanReport>,
}

#[derive(Debug, Clone, Serialize)]
struct SpanReport {
    trace_sequence: usize,
    label: String,
    detail: Option<String>,
    remaining_stack: usize,
    cumulative_stack_used: usize,
    stack_used: usize,
    inclusive_stack_used: usize,
    peak_path_hits: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SpanAggregateReport {
    label: String,
    detail: Option<String>,
    calls: usize,
    total_self_stack_used: usize,
    max_self_stack_used: usize,
    total_inclusive_stack_used: usize,
    max_inclusive_stack_used: usize,
    max_cumulative_stack_used: usize,
    peak_path_hits: usize,
}

#[derive(Debug, Serialize)]
struct StackReport {
    sql: String,
    db: String,
    samples: usize,
    span_aggregates: Vec<SpanAggregateReport>,
    statements: Vec<StatementReport>,
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

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let mut visitor = StackSpanVisitor::default();
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(StackSpanFields {
                module_path: visitor.module_path,
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
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
        let label = scoped_stack_label(&ctx, event, &label);

        self.collector
            .samples
            .lock()
            .expect("stack collector mutex poisoned")
            .push(StackSample {
                label,
                detail: visitor.detail,
                phase: visitor.phase.unwrap_or(StackPhase::Sample),
                remaining_stack,
            });
    }
}

#[derive(Debug)]
struct StackSpanFields {
    module_path: Option<String>,
}

#[derive(Default)]
struct StackSpanVisitor {
    module_path: Option<String>,
}

impl Visit for StackSpanVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "module_path" {
            self.module_path = Some(unquote_debug_string(&format!("{value:?}")));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "module_path" {
            self.module_path = Some(value.to_string());
        }
    }
}

#[derive(Default)]
struct StackEventVisitor {
    label: Option<String>,
    detail: Option<String>,
    phase: Option<StackPhase>,
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
            "phase" => self.phase = parse_stack_phase(&unquote_debug_string(&rendered)),
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
            "phase" => self.phase = parse_stack_phase(value),
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct StackPathSegment {
    module_path: Option<String>,
    name: String,
}

fn scoped_stack_label<S>(ctx: &LayerContext<'_, S>, event: &Event<'_>, label: &str) -> String
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let event_segment = StackPathSegment {
        module_path: None,
        name: label.to_string(),
    };
    let Some(scope) = ctx.event_scope(event) else {
        return format_stack_path(&[], &event_segment);
    };

    let scope_segments = scope
        .from_root()
        .filter(|span| span.metadata().target() == "turso_stack")
        .map(|span| {
            let module_path = span
                .extensions()
                .get::<StackSpanFields>()
                .and_then(|fields| fields.module_path.clone());
            StackPathSegment {
                module_path,
                name: span.metadata().name().to_string(),
            }
        })
        .collect::<Vec<_>>();

    format_stack_path(&scope_segments, &event_segment)
}

fn format_stack_path(scope_segments: &[StackPathSegment], event: &StackPathSegment) -> String {
    let mut path = String::new();

    for segment in scope_segments {
        append_stack_path_segment(&mut path, segment);
    }

    if scope_segments
        .last()
        .is_some_and(|segment| stack_segments_match(segment, event))
    {
        return path;
    }

    append_stack_path_segment(&mut path, event);
    path
}

fn stack_segments_match(
    scope_segment: &StackPathSegment,
    event_segment: &StackPathSegment,
) -> bool {
    scope_segment.name == event_segment.name
        || event_segment
            .name
            .strip_prefix(scope_segment.module_path.as_deref().unwrap_or_default())
            .is_some_and(|suffix| suffix == format!("::{}", scope_segment.name))
}

fn append_stack_path_segment(path: &mut String, segment: &StackPathSegment) {
    let rendered = render_stack_path_segment(
        segment.module_path.as_deref(),
        &segment.name,
        path.is_empty(),
    );
    if !path.is_empty() && !rendered.is_empty() {
        path.push_str("::");
    }
    path.push_str(&rendered);
}

fn render_stack_path_segment(module_path: Option<&str>, name: &str, is_first: bool) -> String {
    if let Some(module_path) = module_path {
        if let Some(local_name) = name.strip_prefix(&format!("{module_path}::")) {
            return if is_first {
                format!("{module_path}::{local_name}")
            } else {
                local_name.to_string()
            };
        }

        if name.contains("::") {
            return name.to_string();
        }

        if is_first {
            format!("{module_path}::{name}")
        } else {
            name.to_string()
        }
    } else {
        name.to_string()
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

    let db = turso::Builder::new_local(":memory:")
        .experimental_generated_columns(true)
        .experimental_custom_types(true)
        .experimental_materialized_views(true)
        .build()
        .await?;
    let conn = db.connect()?;
    let mut statements = execute_sql_payload(&conn, &sql, Arc::clone(&collector), &args).await?;
    statements.sort_by(|a, b| {
        b.stack_used
            .cmp(&a.stack_used)
            .then_with(|| a.index.cmp(&b.index))
    });
    let samples = statements.iter().map(|statement| statement.samples).sum();
    let span_aggregates = build_global_span_aggregates(&statements);
    let report = StackReport {
        sql: args.sql.display().to_string(),
        db: ":memory:".to_string(),
        samples,
        span_aggregates,
        statements,
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

async fn execute_sql_payload(
    conn: &turso::Connection,
    sql: &str,
    collector: Arc<StackCollector>,
    args: &Args,
) -> Result<Vec<StatementReport>> {
    let mut reports = Vec::new();

    for (index, statement) in split_sql_statements(sql)?.into_iter().enumerate() {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }

        let start_sample = collector.sample_count();
        let baseline_remaining_stack =
            stacker::remaining_stack().context("failed to read baseline remaining stack")?;
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

        let statement_index = index + 1;
        if statement_matches_report_filters(statement_index, statement, args) {
            let samples = collector.samples_from(start_sample);
            reports.push(build_statement_report(
                statement_index,
                statement,
                baseline_remaining_stack,
                samples,
            ));
        }
    }
    Ok(reports)
}

fn statement_matches_report_filters(index: usize, sql: &str, args: &Args) -> bool {
    if !args.statements.is_empty() && !args.statements.contains(&index) {
        return false;
    }
    if !args.sql_contains.is_empty()
        && !args
            .sql_contains
            .iter()
            .any(|needle| contains_ignore_ascii_case(sql, needle))
    {
        return false;
    }
    true
}

fn contains_ignore_ascii_case(value: &str, needle: &str) -> bool {
    value
        .as_bytes()
        .windows(needle.len())
        .any(|candidate| candidate.eq_ignore_ascii_case(needle.as_bytes()))
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

fn build_statement_report(
    index: usize,
    sql: &str,
    baseline_remaining_stack: usize,
    samples: Vec<StackSample>,
) -> StatementReport {
    let min_remaining_stack = samples
        .iter()
        .map(|sample| sample.remaining_stack)
        .min()
        .unwrap_or(baseline_remaining_stack);
    let mut spans = build_span_reports(baseline_remaining_stack, min_remaining_stack, &samples);
    let span_aggregates = aggregate_span_reports(&spans);
    spans.sort_by(|a, b| {
        b.stack_used
            .cmp(&a.stack_used)
            .then_with(|| a.trace_sequence.cmp(&b.trace_sequence))
    });

    StatementReport {
        index,
        sql: single_line_sql(sql),
        baseline_remaining_stack,
        min_remaining_stack,
        stack_used: baseline_remaining_stack.saturating_sub(min_remaining_stack),
        samples: spans.len(),
        span_aggregates,
        spans,
    }
}

#[derive(Debug)]
struct ActiveSpan {
    span_index: usize,
    parent_remaining_stack: usize,
    entry_remaining_stack: usize,
    min_remaining_stack: usize,
}

fn build_span_reports(
    baseline_remaining_stack: usize,
    statement_min_remaining_stack: usize,
    samples: &[StackSample],
) -> Vec<SpanReport> {
    let mut active_spans = Vec::<ActiveSpan>::new();
    let mut spans = Vec::new();

    for (trace_sequence, sample) in samples.iter().enumerate() {
        match sample.phase {
            StackPhase::Enter => {
                update_active_span_min(&mut active_spans, sample.remaining_stack);
                let parent_remaining = active_spans
                    .last()
                    .map(|span| span.entry_remaining_stack)
                    .unwrap_or(baseline_remaining_stack);
                let span_index = spans.len();
                spans.push(SpanReport {
                    trace_sequence: trace_sequence + 1,
                    label: sample.label.clone(),
                    detail: sample.detail.clone(),
                    remaining_stack: sample.remaining_stack,
                    cumulative_stack_used: baseline_remaining_stack
                        .saturating_sub(sample.remaining_stack),
                    stack_used: parent_remaining.saturating_sub(sample.remaining_stack),
                    inclusive_stack_used: 0,
                    peak_path_hits: 0,
                });
                active_spans.push(ActiveSpan {
                    span_index,
                    parent_remaining_stack: parent_remaining,
                    entry_remaining_stack: sample.remaining_stack,
                    min_remaining_stack: sample.remaining_stack,
                });
                if sample.remaining_stack == statement_min_remaining_stack {
                    mark_peak_path(&active_spans, &mut spans);
                }
            }
            StackPhase::Exit => {
                update_active_span_min(&mut active_spans, sample.remaining_stack);
                if sample.remaining_stack == statement_min_remaining_stack {
                    mark_peak_path(&active_spans, &mut spans);
                }
                if let Some(active_span) = active_spans.pop() {
                    finalize_active_span(active_span, &mut spans);
                }
            }
            StackPhase::Sample => {
                update_active_span_min(&mut active_spans, sample.remaining_stack);
                if sample.remaining_stack == statement_min_remaining_stack {
                    mark_peak_path(&active_spans, &mut spans);
                }
                let parent_remaining = active_spans
                    .last()
                    .map(|span| span.entry_remaining_stack)
                    .unwrap_or(baseline_remaining_stack);
                let stack_used = parent_remaining.saturating_sub(sample.remaining_stack);
                spans.push(SpanReport {
                    trace_sequence: trace_sequence + 1,
                    label: sample.label.clone(),
                    detail: sample.detail.clone(),
                    remaining_stack: sample.remaining_stack,
                    cumulative_stack_used: baseline_remaining_stack
                        .saturating_sub(sample.remaining_stack),
                    stack_used,
                    inclusive_stack_used: stack_used,
                    peak_path_hits: usize::from(
                        sample.remaining_stack == statement_min_remaining_stack,
                    ),
                });
            }
        }
    }
    while let Some(active_span) = active_spans.pop() {
        finalize_active_span(active_span, &mut spans);
    }
    spans
}

fn update_active_span_min(active_spans: &mut [ActiveSpan], remaining_stack: usize) {
    for active_span in active_spans {
        active_span.min_remaining_stack = active_span.min_remaining_stack.min(remaining_stack);
    }
}

fn mark_peak_path(active_spans: &[ActiveSpan], spans: &mut [SpanReport]) {
    for active_span in active_spans {
        spans[active_span.span_index].peak_path_hits += 1;
    }
}

fn finalize_active_span(active_span: ActiveSpan, spans: &mut [SpanReport]) {
    spans[active_span.span_index].inclusive_stack_used = active_span
        .parent_remaining_stack
        .saturating_sub(active_span.min_remaining_stack);
}

fn build_global_span_aggregates(statements: &[StatementReport]) -> Vec<SpanAggregateReport> {
    aggregate_span_reports(
        statements
            .iter()
            .flat_map(|statement| statement.spans.iter()),
    )
}

fn aggregate_span_reports<'a, I>(spans: I) -> Vec<SpanAggregateReport>
where
    I: IntoIterator<Item = &'a SpanReport>,
{
    let mut aggregates = BTreeMap::<(String, Option<String>), SpanAggregateReport>::new();
    for span in spans {
        let entry = aggregates
            .entry((span.label.clone(), span.detail.clone()))
            .or_insert_with(|| SpanAggregateReport {
                label: span.label.clone(),
                detail: span.detail.clone(),
                calls: 0,
                total_self_stack_used: 0,
                max_self_stack_used: 0,
                total_inclusive_stack_used: 0,
                max_inclusive_stack_used: 0,
                max_cumulative_stack_used: 0,
                peak_path_hits: 0,
            });
        entry.calls += 1;
        entry.total_self_stack_used += span.stack_used;
        entry.max_self_stack_used = entry.max_self_stack_used.max(span.stack_used);
        entry.total_inclusive_stack_used += span.inclusive_stack_used;
        entry.max_inclusive_stack_used = entry
            .max_inclusive_stack_used
            .max(span.inclusive_stack_used);
        entry.max_cumulative_stack_used = entry
            .max_cumulative_stack_used
            .max(span.cumulative_stack_used);
        entry.peak_path_hits += span.peak_path_hits;
    }

    let mut aggregates = aggregates.into_values().collect::<Vec<_>>();
    sort_span_aggregates(&mut aggregates);
    aggregates
}

fn sort_span_aggregates(aggregates: &mut [SpanAggregateReport]) {
    aggregates.sort_by(|a, b| {
        b.total_inclusive_stack_used
            .cmp(&a.total_inclusive_stack_used)
            .then_with(|| b.max_inclusive_stack_used.cmp(&a.max_inclusive_stack_used))
            .then_with(|| b.peak_path_hits.cmp(&a.peak_path_hits))
            .then_with(|| b.calls.cmp(&a.calls))
            .then_with(|| b.total_self_stack_used.cmp(&a.total_self_stack_used))
            .then_with(|| a.label.cmp(&b.label))
            .then_with(|| a.detail.cmp(&b.detail))
    });
}

fn print_human(report: &StackReport, top: usize) {
    println!("=== STACK USAGE REPORT ===");
    println!("SQL:     {}", report.sql);
    println!("DB:      {}", report.db);
    println!("Samples: {}", report.samples);
    println!();

    if !report.span_aggregates.is_empty() {
        println!("global span aggregates:");
        print_span_aggregate_rows(&report.span_aggregates, top, "  ");
        println!();
    }

    for statement in &report.statements {
        println!(
            "statement #{:<4} stack_used={:<8} baseline={:<10} min_remaining={:<10} samples={:<6} {}",
            statement.index,
            statement.stack_used,
            statement.baseline_remaining_stack,
            statement.min_remaining_stack,
            statement.samples,
            statement.sql
        );
        if statement.spans.is_empty() {
            println!("  no stack samples");
            continue;
        }
        println!("  aggregate spans:");
        print_span_aggregate_rows(&statement.span_aggregates, top, "    ");
        if statement.span_aggregates.len() > top {
            println!(
                "    ... {} more aggregate spans",
                statement.span_aggregates.len() - top
            );
        }
        println!("  span samples:");
        println!(
            "    {:>5} {:>12} {:>12} {:>12} {:>10} {:>12}  span",
            "seq", "self_used", "incl_used", "cum_used", "peak_hits", "remaining"
        );
        for span in statement.spans.iter().take(top) {
            let label = span_label(&span.label, span.detail.as_deref());
            println!(
                "    {:>5} {:>12} {:>12} {:>12} {:>10} {:>12}  {}",
                span.trace_sequence,
                span.stack_used,
                span.inclusive_stack_used,
                span.cumulative_stack_used,
                span.peak_path_hits,
                span.remaining_stack,
                label
            );
        }
        if statement.spans.len() > top {
            println!("    ... {} more spans", statement.spans.len() - top);
        }
        println!();
    }
}

fn print_span_aggregate_rows(aggregates: &[SpanAggregateReport], top: usize, indent: &str) {
    println!(
        "{indent}{:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>10}  span",
        "calls", "total_self", "max_self", "total_incl", "max_incl", "max_cum", "peak_hits"
    );
    for aggregate in aggregates.iter().take(top) {
        let label = span_label(&aggregate.label, aggregate.detail.as_deref());
        println!(
            "{indent}{:>7} {:>12} {:>12} {:>12} {:>12} {:>12} {:>10}  {}",
            aggregate.calls,
            aggregate.total_self_stack_used,
            aggregate.max_self_stack_used,
            aggregate.total_inclusive_stack_used,
            aggregate.max_inclusive_stack_used,
            aggregate.max_cumulative_stack_used,
            aggregate.peak_path_hits,
            label
        );
    }
}

fn print_csv(report: &StackReport) {
    println!(
        "row_type,statement_index,statement_sql,statement_stack_used,statement_baseline_remaining_stack,statement_min_remaining_stack,statement_samples,span_trace_sequence,span_label,span_detail,span_self_stack_used,span_inclusive_stack_used,span_cumulative_stack_used,span_peak_path_hits,span_remaining_stack,aggregate_label,aggregate_detail,aggregate_calls,aggregate_total_self_stack_used,aggregate_max_self_stack_used,aggregate_total_inclusive_stack_used,aggregate_max_inclusive_stack_used,aggregate_max_cumulative_stack_used,aggregate_peak_path_hits"
    );
    for aggregate in &report.span_aggregates {
        print_csv_aggregate_row("global_aggregate", None, aggregate);
    }
    for statement in &report.statements {
        if statement.spans.is_empty() {
            print_csv_statement_row(statement);
            continue;
        }
        for aggregate in &statement.span_aggregates {
            print_csv_aggregate_row("statement_aggregate", Some(statement), aggregate);
        }
        for span in &statement.spans {
            print_csv_span_row(statement, span);
        }
    }
}

fn print_csv_statement_row(statement: &StatementReport) {
    print_csv_row(vec![
        "statement".to_string(),
        statement.index.to_string(),
        csv_escape(&statement.sql),
        statement.stack_used.to_string(),
        statement.baseline_remaining_stack.to_string(),
        statement.min_remaining_stack.to_string(),
        statement.samples.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    ]);
}

fn print_csv_span_row(statement: &StatementReport, span: &SpanReport) {
    print_csv_row(vec![
        "span".to_string(),
        statement.index.to_string(),
        csv_escape(&statement.sql),
        statement.stack_used.to_string(),
        statement.baseline_remaining_stack.to_string(),
        statement.min_remaining_stack.to_string(),
        statement.samples.to_string(),
        span.trace_sequence.to_string(),
        csv_escape(&span.label),
        csv_escape(span.detail.as_deref().unwrap_or("")),
        span.stack_used.to_string(),
        span.inclusive_stack_used.to_string(),
        span.cumulative_stack_used.to_string(),
        span.peak_path_hits.to_string(),
        span.remaining_stack.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    ]);
}

fn print_csv_aggregate_row(
    row_type: &str,
    statement: Option<&StatementReport>,
    aggregate: &SpanAggregateReport,
) {
    let (
        statement_index,
        statement_sql,
        statement_stack_used,
        statement_baseline_remaining_stack,
        statement_min_remaining_stack,
        statement_samples,
    ) = match statement {
        Some(statement) => (
            statement.index.to_string(),
            csv_escape(&statement.sql),
            statement.stack_used.to_string(),
            statement.baseline_remaining_stack.to_string(),
            statement.min_remaining_stack.to_string(),
            statement.samples.to_string(),
        ),
        None => (
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ),
    };

    print_csv_row(vec![
        row_type.to_string(),
        statement_index,
        statement_sql,
        statement_stack_used,
        statement_baseline_remaining_stack,
        statement_min_remaining_stack,
        statement_samples,
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        csv_escape(&aggregate.label),
        csv_escape(aggregate.detail.as_deref().unwrap_or("")),
        aggregate.calls.to_string(),
        aggregate.total_self_stack_used.to_string(),
        aggregate.max_self_stack_used.to_string(),
        aggregate.total_inclusive_stack_used.to_string(),
        aggregate.max_inclusive_stack_used.to_string(),
        aggregate.max_cumulative_stack_used.to_string(),
        aggregate.peak_path_hits.to_string(),
    ]);
}

fn print_csv_row(fields: Vec<String>) {
    println!("{}", fields.join(","));
}

fn span_label(label: &str, detail: Option<&str>) -> String {
    match detail {
        Some(detail) => format!("{label} detail={detail}"),
        None => label.to_string(),
    }
}

fn single_line_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
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

fn parse_stack_phase(value: &str) -> Option<StackPhase> {
    match value {
        "enter" => Some(StackPhase::Enter),
        "exit" => Some(StackPhase::Exit),
        "sample" => Some(StackPhase::Sample),
        _ => None,
    }
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_filter_matches_one_based_indexes() {
        let args = args_with_filters(vec![2, 4], Vec::new());

        assert!(!statement_matches_report_filters(1, "SELECT 1", &args));
        assert!(statement_matches_report_filters(2, "SELECT 2", &args));
        assert!(!statement_matches_report_filters(3, "SELECT 3", &args));
        assert!(statement_matches_report_filters(4, "SELECT 4", &args));
    }

    #[test]
    fn sql_contains_filter_matches_any_substring() {
        let args = args_with_filters(Vec::new(), vec!["trigger", "GENERATED"]);

        assert!(statement_matches_report_filters(
            1,
            "CREATE TRIGGER cleanup AFTER INSERT ON t BEGIN SELECT 1; END",
            &args
        ));
        assert!(statement_matches_report_filters(
            2,
            "CREATE TABLE t(x INT GENERATED ALWAYS AS (1))",
            &args
        ));
        assert!(!statement_matches_report_filters(
            3,
            "CREATE TABLE t(x)",
            &args
        ));
    }

    #[test]
    fn combined_filters_require_index_and_sql_match() {
        let args = args_with_filters(vec![3], vec!["trigger"]);

        assert!(!statement_matches_report_filters(
            2,
            "CREATE TRIGGER cleanup AFTER INSERT ON t BEGIN SELECT 1; END",
            &args
        ));
        assert!(!statement_matches_report_filters(3, "SELECT 1", &args));
        assert!(statement_matches_report_filters(
            3,
            "CREATE TRIGGER cleanup AFTER INSERT ON t BEGIN SELECT 1; END",
            &args
        ));
    }

    #[test]
    fn stack_path_does_not_repeat_same_module_for_nested_spans() {
        let scope = vec![
            StackPathSegment {
                module_path: Some("turso_core::connection".to_string()),
                name: "prepare_with_origin".to_string(),
            },
            StackPathSegment {
                module_path: Some("turso_core::connection".to_string()),
                name: "compile_cmd".to_string(),
            },
        ];
        let event = StackPathSegment {
            module_path: Some("turso_core::connection".to_string()),
            name: "compile".to_string(),
        };

        assert_eq!(
            format_stack_path(&scope, &event),
            "turso_core::connection::prepare_with_origin::compile_cmd::compile"
        );
    }

    #[test]
    fn stack_path_does_not_append_event_when_it_matches_current_span() {
        let scope = vec![StackPathSegment {
            module_path: Some("turso_core::connection".to_string()),
            name: "prepare_with_origin".to_string(),
        }];
        let event = StackPathSegment {
            module_path: Some("turso_core::connection".to_string()),
            name: "prepare_with_origin".to_string(),
        };

        assert_eq!(
            format_stack_path(&scope, &event),
            "turso_core::connection::prepare_with_origin"
        );
    }

    #[test]
    fn stack_path_uses_tracing_style_names_when_scope_moves_to_another_module() {
        let scope = vec![
            StackPathSegment {
                module_path: Some("turso_core::connection".to_string()),
                name: "prepare_with_origin".to_string(),
            },
            StackPathSegment {
                module_path: Some("turso_core::translate".to_string()),
                name: "translate".to_string(),
            },
        ];
        let event = StackPathSegment {
            module_path: Some("turso_core::translate".to_string()),
            name: "dispatch".to_string(),
        };

        assert_eq!(
            format_stack_path(&scope, &event),
            "turso_core::connection::prepare_with_origin::translate::dispatch"
        );
    }

    #[test]
    fn statement_stack_used_is_peak_delta_not_sum() {
        let report = build_statement_report(
            1,
            "SELECT 1",
            10_000,
            vec![
                StackSample {
                    label: "outer".to_string(),
                    detail: None,
                    phase: StackPhase::Enter,
                    remaining_stack: 9_000,
                },
                StackSample {
                    label: "inner".to_string(),
                    detail: None,
                    phase: StackPhase::Enter,
                    remaining_stack: 7_500,
                },
                StackSample {
                    label: "inner:end".to_string(),
                    detail: None,
                    phase: StackPhase::Exit,
                    remaining_stack: 9_000,
                },
                StackSample {
                    label: "outer:end".to_string(),
                    detail: None,
                    phase: StackPhase::Exit,
                    remaining_stack: 9_100,
                },
            ],
        );

        assert_eq!(report.stack_used, 2_500);
        assert_eq!(report.min_remaining_stack, 7_500);
        assert_eq!(report.spans[0].label, "inner");
        assert_eq!(report.spans[0].stack_used, 1_500);
        assert_eq!(report.spans[0].inclusive_stack_used, 1_500);
        assert_eq!(report.spans[0].cumulative_stack_used, 2_500);
        let outer = report
            .span_aggregates
            .iter()
            .find(|aggregate| aggregate.label == "outer")
            .expect("outer aggregate should exist");
        assert_eq!(outer.total_self_stack_used, 1_000);
        assert_eq!(outer.total_inclusive_stack_used, 2_500);
        assert_eq!(outer.peak_path_hits, 1);
        let inner = report
            .span_aggregates
            .iter()
            .find(|aggregate| aggregate.label == "inner")
            .expect("inner aggregate should exist");
        assert_eq!(inner.total_self_stack_used, 1_500);
        assert_eq!(inner.total_inclusive_stack_used, 1_500);
        assert_eq!(inner.peak_path_hits, 1);
        assert_eq!(
            report
                .spans
                .iter()
                .map(|span| span.stack_used)
                .sum::<usize>(),
            2_500
        );
    }

    #[test]
    fn span_sort_keeps_trace_sequence_for_ties() {
        let report = build_statement_report(
            1,
            "SELECT 1",
            10_000,
            vec![
                StackSample {
                    label: "first".to_string(),
                    detail: None,
                    phase: StackPhase::Sample,
                    remaining_stack: 8_000,
                },
                StackSample {
                    label: "second".to_string(),
                    detail: None,
                    phase: StackPhase::Sample,
                    remaining_stack: 8_000,
                },
            ],
        );

        assert_eq!(report.spans[0].trace_sequence, 1);
        assert_eq!(report.spans[1].trace_sequence, 2);
    }

    #[test]
    fn aggregate_spans_count_repeated_calls() {
        let report = build_statement_report(
            1,
            "SELECT 1",
            10_000,
            vec![
                StackSample {
                    label: "leaf".to_string(),
                    detail: Some("expr".to_string()),
                    phase: StackPhase::Sample,
                    remaining_stack: 9_000,
                },
                StackSample {
                    label: "leaf".to_string(),
                    detail: Some("expr".to_string()),
                    phase: StackPhase::Sample,
                    remaining_stack: 8_500,
                },
            ],
        );

        assert_eq!(report.span_aggregates.len(), 1);
        let aggregate = &report.span_aggregates[0];
        assert_eq!(aggregate.label, "leaf");
        assert_eq!(aggregate.detail.as_deref(), Some("expr"));
        assert_eq!(aggregate.calls, 2);
        assert_eq!(aggregate.total_self_stack_used, 2_500);
        assert_eq!(aggregate.max_self_stack_used, 1_500);
        assert_eq!(aggregate.total_inclusive_stack_used, 2_500);
        assert_eq!(aggregate.max_inclusive_stack_used, 1_500);
        assert_eq!(aggregate.peak_path_hits, 1);
    }

    #[test]
    fn exit_samples_contribute_to_active_span_inclusive_usage() {
        let report = build_statement_report(
            1,
            "SELECT 1",
            10_000,
            vec![
                StackSample {
                    label: "scope".to_string(),
                    detail: None,
                    phase: StackPhase::Enter,
                    remaining_stack: 9_500,
                },
                StackSample {
                    label: "scope:end".to_string(),
                    detail: None,
                    phase: StackPhase::Exit,
                    remaining_stack: 9_000,
                },
            ],
        );

        assert_eq!(report.stack_used, 1_000);
        assert_eq!(report.spans.len(), 1);
        assert_eq!(report.spans[0].stack_used, 500);
        assert_eq!(report.spans[0].inclusive_stack_used, 1_000);
        assert_eq!(report.spans[0].peak_path_hits, 1);
    }

    #[test]
    fn stacker_remaining_stack_tracks_real_stack_growth() {
        let before = stacker::remaining_stack().expect("remaining stack should be available");
        let inside = consume_stack_frame();
        assert!(
            before > inside,
            "remaining stack should shrink inside a stack-consuming frame: before={before}, inside={inside}"
        );
        assert!(
            before - inside >= 8 * 1024,
            "expected at least an 8KiB observed stack delta, got {} bytes",
            before - inside
        );
    }

    #[inline(never)]
    fn consume_stack_frame() -> usize {
        let buffer = [0_u8; 16 * 1024];
        std::hint::black_box(&buffer);
        stacker::remaining_stack().expect("remaining stack should be available")
    }

    fn args_with_filters(statements: Vec<usize>, sql_contains: Vec<&str>) -> Args {
        Args {
            sql: PathBuf::from("-"),
            top: 40,
            format: OutputFormat::Human,
            statements,
            sql_contains: sql_contains.into_iter().map(str::to_string).collect(),
        }
    }
}
