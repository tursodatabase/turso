//! Renders the latency report as a self-contained HTML page (static inline
//! SVG, no scripts or external assets): one latency-over-time chart with a
//! p50 line per operation, then a per-operation latency histogram grid.

use std::collections::BTreeMap;
use std::fmt::Write;

use crate::stats::{percentile, OpSamples, Summary};

const PALETTE: [&str; 10] = [
    "#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2", "#ca8a04", "#db2777",
    "#4b5563", "#65a30d",
];

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

struct LogScale {
    lo: f64,
    hi: f64,
    y0: f64,
    y1: f64,
}

impl LogScale {
    fn new(min: f64, max: f64, y0: f64, y1: f64) -> Self {
        let lo = min.max(0.05).log10();
        let hi = max.max(min * 10.0).log10();
        Self { lo, hi, y0, y1 }
    }

    fn map(&self, ms: f64) -> f64 {
        let span = if self.hi - self.lo == 0.0 {
            1.0
        } else {
            self.hi - self.lo
        };
        self.y1 - ((ms.max(0.05).log10() - self.lo) / span) * (self.y1 - self.y0)
    }

    fn ticks(&self) -> Vec<f64> {
        (self.lo.ceil() as i32..=self.hi.floor() as i32)
            .map(|e| 10f64.powi(e))
            .collect()
    }
}

fn fmt_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{}s", ms / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

/// p50-per-time-bucket lines for every operation, on a shared log y axis.
fn time_chart(ops: &[OpSamples], elapsed_secs: f64) -> String {
    let width = 920.0;
    let height = 340.0;
    let (top, right, bottom, left) = (16.0, 200.0, 36.0, 64.0);
    let x0 = left;
    let x1 = width - right;
    let all: Vec<f64> = ops
        .iter()
        .flat_map(|o| o.samples.iter().map(|s| s.1))
        .collect();
    if all.is_empty() {
        return String::new();
    }
    let min = all.iter().copied().fold(f64::INFINITY, f64::min);
    let max = all.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let y = LogScale::new(min, max, top, height - bottom);
    let x = |t: f64| x0 + (t / elapsed_secs) * (x1 - x0);
    let bucket_secs = (elapsed_secs / 60.0).max(0.5);

    let mut parts = String::new();
    for tick in y.ticks() {
        let ty = y.map(tick);
        write!(
            parts,
            r##"<line x1="{x0}" y1="{ty}" x2="{x1}" y2="{ty}" stroke="#e5e7eb"/>"##
        )
        .unwrap();
        write!(
            parts,
            r#"<text x="{}" y="{}" text-anchor="end" class="tick">{}</text>"#,
            x0 - 8.0,
            ty + 4.0,
            fmt_ms(tick)
        )
        .unwrap();
    }
    let x_step = if elapsed_secs > 120.0 {
        30
    } else if elapsed_secs > 30.0 {
        10
    } else {
        5
    };
    let mut t = 0;
    while (t as f64) <= elapsed_secs {
        write!(
            parts,
            r#"<text x="{}" y="{}" text-anchor="middle" class="tick">{t}s</text>"#,
            x(t as f64),
            height - bottom + 18.0
        )
        .unwrap();
        t += x_step;
    }
    write!(
        parts,
        r##"<line x1="{x0}" y1="{}" x2="{x1}" y2="{}" stroke="#9ca3af"/>"##,
        height - bottom,
        height - bottom
    )
    .unwrap();

    for (i, op) in ops.iter().enumerate() {
        let color = PALETTE[i % PALETTE.len()];
        let mut buckets: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
        for (t, ms) in &op.samples {
            buckets
                .entry((t / bucket_secs).floor() as i64)
                .or_default()
                .push(*ms);
        }
        let mut points = Vec::new();
        for (b, mut values) in buckets {
            values.sort_by(f64::total_cmp);
            points.push(format!(
                "{:.1},{:.1}",
                x((b as f64 + 0.5) * bucket_secs),
                y.map(percentile(&values, 50.0))
            ));
        }
        if !points.is_empty() {
            write!(
                parts,
                r#"<polyline points="{}" fill="none" stroke="{color}" stroke-width="1.8"/>"#,
                points.join(" ")
            )
            .unwrap();
        }
        let ly = top + 8.0 + i as f64 * 18.0;
        write!(
            parts,
            r#"<line x1="{}" y1="{ly}" x2="{}" y2="{ly}" stroke="{color}" stroke-width="3"/>"#,
            x1 + 16.0,
            x1 + 40.0
        )
        .unwrap();
        write!(
            parts,
            r#"<text x="{}" y="{}" class="label">{}</text>"#,
            x1 + 46.0,
            ly + 4.0,
            esc(&op.name)
        )
        .unwrap();
    }

    let label_y = (top + height - bottom) / 2.0;
    write!(
        parts,
        r#"<text x="{0}" y="{label_y}" class="label" transform="rotate(-90 {0} {label_y})" text-anchor="middle">latency (p50 per {bucket_secs:.1}s bucket)</text>"#,
        x0 - 48.0
    )
    .unwrap();
    format!(r#"<svg viewBox="0 0 {width} {height}" width="{width}">{parts}</svg>"#)
}

/// One latency histogram per operation on a shared log x axis, with p50/p99 markers.
fn histograms(ops: &[OpSamples]) -> String {
    let all: Vec<f64> = ops
        .iter()
        .flat_map(|o| o.samples.iter().map(|s| s.1))
        .collect();
    if all.is_empty() {
        return String::new();
    }
    let width = 290.0;
    let height = 150.0;
    let (top, right, bottom, left) = (24.0, 8.0, 26.0, 8.0);
    let min = all.iter().copied().fold(f64::INFINITY, f64::min);
    let max = all.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let x = LogScale::new(min, max, left, width - right);
    // LogScale maps "low value -> y1" for y axes; for an x axis we flip it.
    let px = |ms: f64| left + (width - right) - x.map(ms);
    let bucket_of = |ms: f64| (ms.max(0.05).log10() * 8.0).floor() as i64;

    let mut charts = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let color = PALETTE[i % PALETTE.len()];
        let mut counts: BTreeMap<i64, u64> = BTreeMap::new();
        for (_, ms) in &op.samples {
            *counts.entry(bucket_of(*ms)).or_default() += 1;
        }
        let max_count = counts.values().copied().max().unwrap_or(1).max(1);
        let mut parts = String::new();
        for (bucket, count) in &counts {
            let lo = 10f64.powf(*bucket as f64 / 8.0);
            let hi = 10f64.powf((*bucket as f64 + 1.0) / 8.0);
            let bar_x = px(lo);
            let bar_w = (px(hi) - px(lo) - 1.0).max(1.5);
            let bar_h = (*count as f64 / max_count as f64) * (height - top - bottom);
            write!(
                parts,
                r#"<rect x="{bar_x:.1}" y="{:.1}" width="{bar_w:.1}" height="{bar_h:.1}" fill="{color}" opacity="0.75"/>"#,
                height - bottom - bar_h
            )
            .unwrap();
        }
        for tick in x.ticks() {
            write!(
                parts,
                r#"<text x="{}" y="{}" text-anchor="middle" class="tick">{}</text>"#,
                px(tick),
                height - 8.0,
                fmt_ms(tick)
            )
            .unwrap();
        }
        let mut sorted: Vec<f64> = op.samples.iter().map(|s| s.1).collect();
        sorted.sort_by(f64::total_cmp);
        for (p, dash) in [(50.0, ""), (99.0, r#" stroke-dasharray="4 3""#)] {
            let vx = px(percentile(&sorted, p));
            write!(
                parts,
                r##"<line x1="{vx}" y1="{}" x2="{vx}" y2="{}" stroke="#111827" stroke-width="1"{dash}/>"##,
                top - 4.0,
                height - bottom
            )
            .unwrap();
            write!(
                parts,
                r#"<text x="{}" y="{}" class="tick">p{p:.0}</text>"#,
                vx + 3.0,
                top + 6.0
            )
            .unwrap();
        }
        write!(
            parts,
            r##"<line x1="{left}" y1="{0}" x2="{1}" y2="{0}" stroke="#9ca3af"/>"##,
            height - bottom,
            width - right
        )
        .unwrap();
        write!(
            parts,
            r#"<text x="{left}" y="12" class="label">{} <tspan class="tick">({} ops)</tspan></text>"#,
            esc(&op.name),
            op.samples.len()
        )
        .unwrap();
        charts.push(format!(
            r#"<svg viewBox="0 0 {width} {height}" width="{width}">{parts}</svg>"#
        ));
    }
    format!("<div class=\"grid\">{}</div>", charts.join("\n"))
}

pub struct PlotMeta {
    pub workload: String,
    pub description: Option<String>,
    pub db: String,
    pub connections: usize,
}

pub fn render_plot_html(summary: &Summary, mut ops: Vec<OpSamples>, meta: &PlotMeta) -> String {
    ops.retain(|o| !o.samples.is_empty());
    let mut rows = String::new();
    for op in &summary.operations {
        write!(
            rows,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\n<td>{:.1}</td><td>{:.1}</td><td>{:.1}</td><td>{:.1}</td><td>{:.1}</td></tr>\n",
            esc(&op.name),
            op.ok,
            op.errors,
            op.skips,
            op.throughput,
            op.latency.p50,
            op.latency.p95,
            op.latency.p99,
            op.latency.max
        )
        .unwrap();
    }

    let description = meta
        .description
        .as_ref()
        .map(|description| format!(r#"<p>{}</p>"#, esc(description)))
        .unwrap_or_default();

    format!(
        r#"<!doctype html>
<html lang="en">
<meta charset="utf-8">
<title>turso-load - {name}</title>
<style>
  body {{ font: 14px/1.5 system-ui, sans-serif; color: #111827; max-width: 960px; margin: 32px auto; padding: 0 16px; }}
  h1 {{ font-size: 20px; }} h2 {{ font-size: 16px; margin-top: 32px; }}
  .meta {{ color: #6b7280; }}
  .tick {{ font: 10px system-ui, sans-serif; fill: #6b7280; }}
  .label {{ font: 12px system-ui, sans-serif; fill: #111827; }}
  .grid {{ display: flex; flex-wrap: wrap; gap: 12px; }}
  table {{ border-collapse: collapse; margin-top: 8px; }}
  th, td {{ text-align: right; padding: 4px 10px; border-bottom: 1px solid #e5e7eb; }}
  th:first-child, td:first-child {{ text-align: left; }}
</style>
<h1>turso-load: {name}</h1>
{description}
<p class="meta">{date} &middot; {db} &middot; {connections} connections
&middot; {elapsed:.1}s measured &middot; {tput:.1} ops/s
&middot; {ok} ok / {errors} errors / {skips} skips</p>
<h2>Latency over time</h2>
{time_chart}
<h2>Latency distribution per operation</h2>
{histograms}
<h2>Summary</h2>
<table>
<tr><th>operation</th><th>ok</th><th>err</th><th>skip</th><th>ops/s</th><th>p50 (ms)</th><th>p95 (ms)</th><th>p99 (ms)</th><th>max (ms)</th></tr>
{rows}
</table>
</html>
"#,
        name = esc(&meta.workload),
        description = description,
        date = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
        db = esc(&meta.db),
        connections = meta.connections,
        elapsed = summary.elapsed_secs,
        tput = summary.total.throughput,
        ok = summary.total.ok,
        errors = summary.total.errors,
        skips = summary.total.skips,
        time_chart = time_chart(&ops, summary.elapsed_secs),
        histograms = histograms(&ops),
    )
}
