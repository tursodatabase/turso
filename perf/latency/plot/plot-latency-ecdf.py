#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy"]
# ///
"""Plot transaction latency as an eCDF from txn-latency CSV output.

Usage: uv run plot-latency-ecdf.py sqlite.csv turso.csv [--column total_ns]
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e6e5df"
AXIS = "#c3c2b7"
TURSO = "#2a78d6"  # categorical slot 1
SQLITE = "#eb6834"  # categorical slot 2
FALLBACK = ["#4a9c6d", "#8b5cd6", "#c9a227"]

COLUMN_LABELS = {
    "total_ns": "Transaction latency",
    "queue_ns": "Time waiting for a free connection",
    "begin_ns": "Time to start the transaction",
    "work_ns": "Time inserting rows",
    "commit_ns": "Time to commit",
}


def read_series(path, column):
    """Group one CSV file's samples by engine and mode."""
    series = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            key = (row["engine"], row["mode"], row["connections"])
            series.setdefault(key, []).append(float(row[column]) / 1e6)
    return {k: np.array(v) for k, v in series.items()}


def label_for(engine, mode, modes_per_engine):
    name = {"sqlite": "SQLite", "turso": "Turso"}.get(engine, engine)
    if len(modes_per_engine[engine]) > 1:
        return f"{name} ({mode})"
    return name


def color_for(engine, index):
    if engine == "turso":
        return TURSO
    if engine == "sqlite":
        return SQLITE
    return FALLBACK[index % len(FALLBACK)]


def ecdf_points(samples, max_points=3000):
    """Sorted samples and their cumulative percentage, thinned for plotting.

    The thinning keeps the far tail intact: points are picked both on an even
    grid and on a log grid running back from the largest sample, so the last
    handful of transactions still show up individually.
    """
    x = np.sort(samples)
    n = x.size
    even = np.linspace(0, n - 1, min(max_points, n))
    tail = n - 1 - np.logspace(0, np.log10(n), min(max_points, n))
    idx = np.unique(np.clip(np.concatenate([even, tail]), 0, n - 1).astype(int))
    return x[idx], (idx + 1) / n * 100.0


def fmt_ms(value):
    if value >= 10:
        return f"{value:,.0f} ms"
    if value >= 1:
        return f"{value:.1f} ms"
    return f"{value:.2f} ms"


def fmt_tick(value, _pos):
    """Plain numbers on the latency axis: 0.1, 1, 10, 1,000 - never 10^3."""
    if value == 0:
        return "0"
    if value >= 1:
        return f"{value:,.0f}"
    return f"{value:g}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("--column", default="total_ns", choices=sorted(COLUMN_LABELS))
    parser.add_argument("-o", "--output", default="latency-ecdf.png", type=Path)
    parser.add_argument("--title", default=None,
                        help="chart title (default: built from the column name)")
    args = parser.parse_args()

    series = {}
    for path in args.csv_files:
        series.update(read_series(path, args.column))
    if not series:
        raise SystemExit("no samples found")

    modes_per_engine = {}
    for engine, mode, _ in series:
        modes_per_engine.setdefault(engine, set()).add(mode)

    plt.rcParams.update(
        {
            "font.family": ["Helvetica Neue", "Helvetica", "Arial", "Noto Sans",
                            "Liberation Sans", "DejaVu Sans"],
            "font.size": 11,
        }
    )

    fig, ax = plt.subplots(figsize=(9, 5.4), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    ax.set_xscale("log")
    ax.grid(True, axis="y", which="major", color=GRID, linewidth=0.8, zorder=0)
    ax.grid(True, axis="x", which="major", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)

    lo, hi = np.inf, 0.0
    counts = set()
    connections_seen = set()
    legend_handles = []
    for index, ((engine, mode, connections), samples) in enumerate(sorted(series.items())):
        color = color_for(engine, index)
        label = label_for(engine, mode, modes_per_engine)
        x, y = ecdf_points(samples)
        ax.plot(x, y, color=color, linewidth=2.2, zorder=3, solid_capstyle="round",
                solid_joinstyle="round")

        # The p99 value goes in the legend rather than as text next to the
        # marker: with several series the markers sit at the same height and
        # their labels overlap.
        p99 = float(np.percentile(samples, 99))
        ax.plot([p99], [99], marker="o", markersize=8, color=color,
                markeredgecolor=SURFACE, markeredgewidth=2, zorder=4)
        legend_handles.append(Line2D([], [], color=color, linewidth=2.2, marker="o",
                                     markersize=7, markeredgecolor=SURFACE,
                                     markeredgewidth=1.5,
                                     label=f"{label}   p99 {fmt_ms(p99)}"))

        lo = min(lo, float(np.min(samples)))
        hi = max(hi, float(np.max(samples)))
        counts.add(samples.size)
        connections_seen.add(connections)

    ax.set_ylabel("Transactions finished within this latency", color=INK,
                  fontsize=11.5, labelpad=12)

    ax.set_xlim(10 ** np.floor(np.log10(max(lo, 1e-3))), 10 ** np.ceil(np.log10(hi)))
    ax.set_xlabel(f"{COLUMN_LABELS[args.column]} in milliseconds (log scale)",
                  color=INK, fontsize=12, labelpad=12)
    ax.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(fmt_tick))

    ax.set_ylim(0, 104)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}%"))
    ax.tick_params(colors=MUTED, labelcolor=MUTED, length=0, pad=8, labelsize=10.5)
    ax.tick_params(which="minor", length=0)

    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.spines["bottom"].set_linewidth(1)

    # Title and subtitle live in the figure, above the axes, flush with
    # the plot's left edge.
    LEFT = 0.105
    title = args.title or f"{COLUMN_LABELS[args.column]}: " + " vs ".join(
        h.get_label().split("   ")[0] for h in legend_handles)
    n = ", ".join(f"{c:,}" for c in sorted(counts))
    conns = ", ".join(sorted(connections_seen, key=int))
    subtitle = (f"{n} transactions per engine, {conns} concurrent connections. "
                "Higher and further left is better.")
    fig.text(LEFT, 0.955, title, ha="left", va="top", color=INK, fontsize=15,
             fontweight="semibold")
    fig.text(LEFT, 0.905, subtitle, ha="left", va="top", color=MUTED, fontsize=10.5)

    legend = ax.legend(handles=legend_handles, loc="lower right", frameon=False,
                       fontsize=11.5, handlelength=2.4, handletextpad=0.9,
                       labelspacing=0.7, borderaxespad=0.6)
    for text in legend.get_texts():
        text.set_color(INK)

    fig.subplots_adjust(left=LEFT, right=0.97, top=0.82, bottom=0.14)
    fig.savefig(args.output, facecolor=SURFACE)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
