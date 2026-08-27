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
            "font.family": ["Helvetica Neue", "Helvetica", "Arial",
                            "Liberation Sans", "DejaVu Sans"],
            "font.size": 11,
        }
    )

    fig, ax = plt.subplots(figsize=(9, 5), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    ax.set_xscale("log")
    ax.grid(True, axis="y", which="major", color=GRID, linewidth=0.8, zorder=0)
    ax.grid(True, axis="x", which="major", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)

    lo, hi = np.inf, 0.0
    p99_points = []
    for index, ((engine, mode, connections), samples) in enumerate(sorted(series.items())):
        color = color_for(engine, index)
        label = label_for(engine, mode, modes_per_engine)
        x, y = ecdf_points(samples)
        ax.plot(x, y, color=color, linewidth=2.2, zorder=3, solid_capstyle="round",
                solid_joinstyle="round")

        p99 = float(np.percentile(samples, 99))
        ax.plot([p99], [99], marker="o", markersize=8, color=color,
                markeredgecolor=SURFACE, markeredgewidth=2, zorder=4)
        p99_points.append((p99, label))

        lo = min(lo, float(np.min(samples)))
        hi = max(hi, float(np.max(samples)))

    ax.set_ylabel("Transactions (%)", color=INK, fontsize=12, labelpad=12)

    ax.set_xlim(10 ** np.floor(np.log10(max(lo, 1e-3))), 10 ** np.ceil(np.log10(hi)))
    ax.set_xlabel(f"{COLUMN_LABELS[args.column]} (ms)", color=INK, fontsize=12, labelpad=12)
    ax.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(fmt_tick))

    ax.set_ylim(0, 112)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.tick_params(colors=MUTED, labelcolor=MUTED, length=0, pad=8, labelsize=10.5)
    ax.tick_params(which="minor", length=0)

    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.spines["bottom"].set_linewidth(1)

    LEFT = 0.105

    # Label each p99 dot in place, centred above it.
    for p99, label in p99_points:
        ax.annotate(f"{label}\n{fmt_ms(p99)}", xy=(p99, 99), xytext=(0, 11),
                    textcoords="offset points", ha="center", va="bottom",
                    linespacing=1.25, color=INK, fontsize=11.5, fontweight="bold",
                    zorder=5)

    fig.subplots_adjust(left=LEFT, right=0.97, top=0.95, bottom=0.14)
    fig.savefig(args.output, facecolor=SURFACE)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
