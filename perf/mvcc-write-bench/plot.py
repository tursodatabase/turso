#!/usr/bin/env python3
"""Plot mvcc-write-bench CSV.

Throughput is committed rows / elapsed_secs (not transactions/s).
One Y-axis per batch_size. Mixing batch 1 with batch 100 flattens the chart.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict


def load_rows(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def throughput(row: dict[str, str]) -> float:
    elapsed = float(row["elapsed_secs"])
    if elapsed <= 0:
        return 0.0
    return float(row["inserts"]) / elapsed


def col_f(row: dict[str, str], name: str) -> float | None:
    raw = row.get(name)
    if raw is None or raw == "":
        return None
    return float(raw)


def latency_p50(row: dict[str, str]) -> float | None:
    return col_f(row, "latency_p50_us")


def latency_p99(row: dict[str, str]) -> float | None:
    return col_f(row, "latency_p99_us")


def group_key(row: dict[str, str]) -> tuple[str, ...]:
    engine = row["engine"]
    checkpoint = row.get("checkpoint") or ""
    topology = row.get("topology") or ""
    threshold = row.get("threshold") or ""
    batch = row.get("batch_size") or ""
    if engine == "sqlite":
        return ("sqlite", "", "sqlite-writer", "", batch)
    return (engine, checkpoint, topology, threshold, batch)


def series_label(key: tuple[str, ...]) -> str:
    engine, checkpoint, topology, threshold, batch = key
    if engine == "sqlite":
        return f"sqlite b={batch}"
    parts = [engine, checkpoint, topology]
    if threshold:
        parts.append(f"th={threshold}")
    parts.append(f"b={batch}")
    return " ".join(p for p in parts if p)


def median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    if n % 2:
        return ys[n // 2]
    return 0.5 * (ys[n // 2 - 1] + ys[n // 2])


def workers_series(
    rows: list[dict[str, str]],
    value_fn=throughput,
) -> dict[tuple[str, ...], list[tuple[int, float]]]:
    buckets: dict[tuple[str, ...], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        val = value_fn(row)
        if val is None:
            continue
        key = group_key(row)
        w = int(row["workers"])
        buckets[key][w].append(val)
    out: dict[tuple[str, ...], list[tuple[int, float]]] = {}
    for key, by_w in buckets.items():
        out[key] = sorted((w, median(vs)) for w, vs in by_w.items())
    return out


def checkpoint_series(rows: list[dict[str, str]]) -> list[tuple[str, float]]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        engine, checkpoint, topology, threshold, batch = group_key(row)
        bits = [engine]
        if checkpoint:
            bits.append(checkpoint)
        if topology and engine != "sqlite":
            bits.append(topology)
        if threshold:
            bits.append(f"th={threshold}")
        bits.append(f"b={batch}")
        label = " ".join(bits)
        buckets[label].append(throughput(row))
    return [(k, median(vs)) for k, vs in sorted(buckets.items())]


def config_key(row: dict[str, str]) -> tuple[str, ...]:
    engine, checkpoint, topology, threshold, _batch = group_key(row)
    return (engine, checkpoint, topology, threshold)


def config_label(key: tuple[str, ...]) -> str:
    if len(key) == 5:
        return series_label(key)
    engine, checkpoint, topology, threshold = key
    if engine == "sqlite":
        return "sqlite"
    parts = [engine, checkpoint, topology]
    if threshold:
        parts.append(f"th={threshold}")
    return " ".join(p for p in parts if p)


def rows_vs_batch(
    rows: list[dict[str, str]],
    workers: int = 8,
    value_fn=throughput,
) -> dict[tuple[str, ...], list[tuple[int, float]]]:
    """Metric vs batch_size at a fixed worker count (sqlite is the one-writer cell)."""
    buckets: dict[tuple[str, ...], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if row["engine"] != "sqlite" and int(row["workers"]) != workers:
            continue
        val = value_fn(row)
        if val is None:
            continue
        key = config_key(row)
        b = int(row["batch_size"])
        buckets[key][b].append(val)
    out: dict[tuple[str, ...], list[tuple[int, float]]] = {}
    for key, by_b in buckets.items():
        out[key] = sorted((b, median(vs)) for b, vs in by_b.items())
    return out


def batch_sizes(rows: list[dict[str, str]]) -> list[str]:
    seen = sorted({row.get("batch_size") or "" for row in rows}, key=lambda s: int(s) if s.isdigit() else 0)
    return [b for b in seen if b]


def filter_batch(rows: list[dict[str, str]], batch: str) -> list[dict[str, str]]:
    return [r for r in rows if (r.get("batch_size") or "") == batch]


def try_matplotlib(workers, checkpoints, out_workers: str, out_ckpt: str, title_suffix: str) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False

    fig, ax = plt.subplots(figsize=(8, 5))
    for key, pts in sorted(workers.items(), key=lambda kv: series_label(kv[0])):
        if not pts:
            continue
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        (line,) = ax.plot(xs, ys, marker="o", label=series_label(key))
        if key[0] == "sqlite" and len(xs) == 1:
            ax.axhline(ys[0], color=line.get_color(), linestyle="--")
    ax.set_xlabel("workers")
    ax.set_ylabel("rows / s")
    ax.set_title(f"rows/s vs workers{title_suffix}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_workers)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5))
    labels = [p[0] for p in checkpoints]
    vals = [p[1] for p in checkpoints]
    ax.bar(labels, vals)
    ax.set_ylabel("rows / s")
    ax.set_title(f"rows/s vs checkpoint policy{title_suffix}")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(out_ckpt)
    plt.close(fig)
    return True


def write_svg_line(
    path: str,
    title: str,
    series: dict[tuple[str, ...], list[tuple[int, float]]],
    xlabel: str = "workers",
    ylabel: str = "rows / s",
    label_fn=series_label,
) -> None:
    width, height = 960, 580
    pad_l, pad_r, pad_t, pad_b = 70, 280, 40, 50
    all_x = [x for pts in series.values() for x, _ in pts]
    all_y = [y for pts in series.values() for _, y in pts]
    xmin, xmax = (0, 1) if not all_x else (min(all_x), max(all_x))
    ymin, ymax = (0.0, 1.0) if not all_y else (0.0, max(all_y) * 1.05)
    if xmax <= xmin:
        xmax = xmin + 1
    if ymax <= ymin:
        ymax = ymin + 1.0

    def sx(x: float) -> float:
        return pad_l + (x - xmin) / (xmax - xmin) * (width - pad_l - pad_r)

    def sy(y: float) -> float:
        return height - pad_b - (y - ymin) / (ymax - ymin) * (height - pad_t - pad_b)

    colors = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
        "#393b79",
    ]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        f'<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="24" text-anchor="middle" font-size="16">{title}</text>',
        f'<text x="{(width - pad_r + pad_l) / 2}" y="{height - 12}" text-anchor="middle" font-size="12">{xlabel}</text>',
        f'<text x="16" y="{height / 2}" text-anchor="middle" font-size="12" transform="rotate(-90 16 {height / 2})">{ylabel}</text>',
        f'<text x="{pad_l}" y="{pad_t + 4}" font-size="11" fill="#333">{ymax:.0f}</text>',
        f'<text x="{pad_l}" y="{height - pad_b}" font-size="11" fill="#333">0</text>',
    ]
    for i, (key, pts) in enumerate(sorted(series.items(), key=lambda kv: label_fn(kv[0]))):
        if len(pts) < 1:
            continue
        color = colors[i % len(colors)]
        engine = key[0]
        if engine == "sqlite" and len(pts) == 1:
            y = pts[0][1]
            parts.append(
                f'<path d="M{sx(xmin):.1f},{sy(y):.1f} L{sx(xmax):.1f},{sy(y):.1f}" fill="none" stroke="{color}" stroke-width="2" stroke-dasharray="6 4"/>'
            )
            parts.append(f'<circle cx="{sx(pts[0][0]):.1f}" cy="{sy(y):.1f}" r="3" fill="{color}"/>')
        else:
            d = " ".join(
                f"{'M' if j == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}" for j, (x, y) in enumerate(pts)
            )
            parts.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"/>')
            for x, y in pts:
                parts.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="3" fill="{color}"/>')
        last_y = pts[-1][1] if pts else 0.0
        parts.append(
            f'<text x="{width - pad_r + 8}" y="{pad_t + 14 + i * 14}" font-size="11" fill="{color}">{label_fn(key)} ({last_y:.0f}/s)</text>'
        )
    parts.append("</svg>")
    with open(path, "w") as f:
        f.write("\n".join(parts))
        f.write("\n")


def write_svg_bars(path: str, title: str, bars: list[tuple[str, float]]) -> None:
    width, height = 960, 520
    pad_l, pad_r, pad_t, pad_b = 60, 20, 40, 120
    ymax = max((v for _, v in bars), default=1.0)
    if ymax <= 0:
        ymax = 1.0
    n = max(len(bars), 1)
    bar_w = (width - pad_l - pad_r) / n * 0.7
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        f'<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="24" text-anchor="middle" font-size="16">{title}</text>',
        f'<text x="{pad_l}" y="{pad_t + 4}" font-size="11" fill="#333">{ymax:.0f}</text>',
    ]
    plot_h = height - pad_t - pad_b
    for i, (label, val) in enumerate(bars):
        x = pad_l + (i + 0.15) * ((width - pad_l - pad_r) / n)
        h = (val / ymax) * plot_h
        y = height - pad_b - h
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="#1f77b4"/>'
        )
        parts.append(
            f'<text x="{x + bar_w / 2:.1f}" y="{height - pad_b + 14}" text-anchor="end" font-size="10" transform="rotate(-40 {x + bar_w / 2:.1f} {height - pad_b + 14})">{label}</text>'
        )
        parts.append(
            f'<text x="{x + bar_w / 2:.1f}" y="{y - 4:.1f}" text-anchor="middle" font-size="10">{val:.0f}</text>'
        )
    parts.append("</svg>")
    with open(path, "w") as f:
        f.write("\n".join(parts))
        f.write("\n")


def write_summary_csv(path: str, workers, checkpoints) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["chart", "series", "x", "throughput"])
        for key, pts in workers.items():
            for x, y in pts:
                w.writerow(["workers", series_label(key), x, y])
        for label, y in checkpoints:
            w.writerow(["checkpoint", label, label, y])


def emit_charts(rows: list[dict[str, str]], out_dir: str, stem: str, batch: str | None) -> list[str]:
    suffix = f" (batch {batch})" if batch else ""
    tag = f"-b{batch}" if batch else ""
    workers = workers_series(rows)
    checkpoints = checkpoint_series(rows)
    out_workers_png = os.path.join(out_dir, f"{stem}{tag}-throughput-vs-workers.png")
    out_ckpt_png = os.path.join(out_dir, f"{stem}{tag}-throughput-vs-checkpoint.png")
    out_workers_svg = os.path.join(out_dir, f"{stem}{tag}-throughput-vs-workers.svg")
    out_ckpt_svg = os.path.join(out_dir, f"{stem}{tag}-throughput-vs-checkpoint.svg")
    written = []
    if try_matplotlib(workers, checkpoints, out_workers_png, out_ckpt_png, suffix):
        written.extend([out_workers_png, out_ckpt_png])
    else:
        write_svg_line(out_workers_svg, f"rows/s vs workers{suffix}", workers)
        write_svg_bars(out_ckpt_svg, f"rows/s vs checkpoint policy{suffix}", checkpoints)
        written.extend([out_workers_svg, out_ckpt_svg])
        p50 = workers_series(rows, latency_p50)
        if any(pts for pts in p50.values()):
            out_lat = os.path.join(out_dir, f"{stem}{tag}-latency-p50-vs-workers.svg")
            write_svg_line(
                out_lat,
                f"commit latency p50 vs workers{suffix}",
                p50,
                ylabel="txn latency (µs)",
            )
            written.append(out_lat)
            p99 = workers_series(rows, latency_p99)
            out_lat99 = os.path.join(out_dir, f"{stem}{tag}-latency-p99-vs-workers.svg")
            write_svg_line(
                out_lat99,
                f"commit latency p99 vs workers{suffix}",
                p99,
                ylabel="txn latency (µs)",
            )
            written.append(out_lat99)
    return written


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    parser.add_argument("--out", default=None, help="output directory (default: next to csv)")
    parser.add_argument(
        "--batch",
        default=None,
        help="only this batch_size (default: one chart set per batch, plus batch 100 as the untagged story chart)",
    )
    args = parser.parse_args()
    csv_path = args.csv
    out_dir = args.out or os.path.dirname(os.path.abspath(csv_path)) or "."
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(csv_path))[0]
    rows = load_rows(csv_path)
    if not rows:
        print("no rows", file=sys.stderr)
        return 1

    if args.batch:
        batches = [args.batch]
    else:
        batches = batch_sizes(rows)

    all_workers = workers_series(rows)
    all_ckpt = checkpoint_series(rows)
    summary = os.path.join(out_dir, f"{stem}-summary.csv")
    write_summary_csv(summary, all_workers, all_ckpt)
    print(summary)

    vs_batch = rows_vs_batch(rows, workers=8)
    if vs_batch:
        out_batch_svg = os.path.join(out_dir, f"{stem}-rows-vs-batch.svg")
        write_svg_line(
            out_batch_svg,
            "rows/s vs batch size (sqlite; turso @ 8 workers)",
            vs_batch,
            xlabel="batch size (rows per txn)",
            label_fn=config_label,
        )
        print(out_batch_svg)
        lat_batch = rows_vs_batch(rows, workers=8, value_fn=latency_p50)
        if any(pts for pts in lat_batch.values()):
            out_lat_batch = os.path.join(out_dir, f"{stem}-latency-p50-vs-batch.svg")
            write_svg_line(
                out_lat_batch,
                "commit latency p50 vs batch size (sqlite; turso @ 8 workers)",
                lat_batch,
                xlabel="batch size (rows per txn)",
                ylabel="txn latency (µs)",
                label_fn=config_label,
            )
            print(out_lat_batch)
        with open(summary, "a", newline="") as f:
            w = csv.writer(f)
            for key, pts in vs_batch.items():
                for x, y in pts:
                    w.writerow(["batch", config_label(key), x, y])

    for batch in batches:
        subset = filter_batch(rows, batch)
        for path in emit_charts(subset, out_dir, stem, batch):
            print(path)

    # Untagged names are the batch-100 story chart so existing paths stay valid.
    story = filter_batch(rows, "100") or rows
    for path in emit_charts(story, out_dir, stem, None):
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
