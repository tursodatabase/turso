#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Plot the transaction latency distribution from txn-latency CSV output.

Usage: uv run plot-latency-ecdf.py sqlite-c1.csv turso-c1.csv [--column total_ns]

This is the eCDF with the axes swapped and the tail stretched: percentile
along the x axis on a log scale of 1/(1-p), so 50, 90, 99, 99.9 and 99.99
are evenly spaced, and latency up the y axis. That is the HdrHistogram
percentile plot, and the tail, which is where engines differ, fills the
chart instead of the top few percent of it. A line's right-hand end is its
slowest transaction.

Pass files from more than one connection count to compare concurrency
levels: colour and marker shape say which engine, line style and marker
fill say how many connections. Two levels read well; the full sweep belongs
in plot-latency-percentiles.py instead.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import scienceplots  # noqa: F401  (registers the styles)
from matplotlib.lines import Line2D
from matplotlib.ticker import FixedLocator, FuncFormatter, LogLocator, NullLocator

plt.style.use(["science", "no-latex", "vibrant"])

# Paul Tol's "vibrant" cycle, which the style above installs: colour-blind
# safe and legible in greyscale.
ENGINES = {
    "sqlite": {"name": "SQLite", "color": "#CC3311", "marker": "s"},
    "turso": {"name": "Turso", "color": "#0077BB", "marker": "o"},
}
FALLBACK_COLORS = ["#009988", "#EE7733", "#33BBEE"]
FALLBACK_MARKERS = ["^", "D", "v"]
# One entry per connection count, in ascending order: line style and
# whether the markers are filled.
LEVEL_STYLES = [("solid", True), ((0, (4, 2)), False), ((0, (1, 1.5)), True)]

# Percentiles that get a tick and a marker.
PERCENTILES = [0, 50, 90, 99, 99.9, 99.99]

COLUMN_LABELS = {
    "total_ns": "Transaction latency",
    "queue_ns": "Time waiting for a free connection",
    "begin_ns": "Time to start the transaction",
    "work_ns": "Time inserting rows",
    "commit_ns": "Time to commit",
}


def read_series(path, column):
    """Group one CSV file's samples by engine, mode and connection count."""
    series = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            key = (row["engine"], row["mode"], int(row["connections"]))
            series.setdefault(key, []).append(float(row[column]) / 1e6)
    return {k: np.array(v) for k, v in series.items()}


def engine_look(engine, index):
    if engine in ENGINES:
        return ENGINES[engine]
    return {
        "name": engine,
        "color": FALLBACK_COLORS[index % len(FALLBACK_COLORS)],
        "marker": FALLBACK_MARKERS[index % len(FALLBACK_MARKERS)],
    }


def label_for(look, mode, connections, modes_per_engine, connection_levels):
    name = look["name"]
    if len(modes_per_engine) > 1:
        name = f"{name} ({mode})"
    if len(connection_levels) > 1:
        name = f"{name}, {connections} conn."
    return name


def stretch(percentile):
    """Map a percentile to the log-stretched x axis: 1 / (1 - p)."""
    return 1.0 / (1.0 - np.asarray(percentile) / 100.0)


def percentile_curve(samples, max_points=2000):
    """Latency against percentile, thinned for plotting.

    Sample i of n sits at percentile (i + 0.5) / n, so the slowest sample lands
    at a finite spot just short of 100. Points are picked on an even grid and
    on a log grid running back from the slowest sample, so the last handful
    of transactions still show up individually.
    """
    x = np.sort(samples)
    n = x.size
    even = np.linspace(0, n - 1, min(max_points, n))
    tail = n - 1 - np.logspace(0, np.log10(n), min(max_points, n))
    idx = np.unique(np.clip(np.concatenate([even, tail]), 0, n - 1).astype(int))
    p = (idx + 0.5) / n * 100.0
    return stretch(p), x[idx]


def fmt_ms(value, _pos=None):
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
    connection_levels = sorted({connections for _, _, connections in series})
    if len(connection_levels) > len(LEVEL_STYLES):
        raise SystemExit(
            f"{len(connection_levels)} connection counts is too many for one chart; "
            "plot the sweep with plot-latency-percentiles.py"
        )

    fig, ax = plt.subplots(figsize=(4.6, 3.2), dpi=300)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, which="major", linewidth=0.4, alpha=0.6)
    ax.set_axisbelow(True)

    handles = []
    most = 0.0
    for index, ((engine, mode, connections), samples) in enumerate(sorted(series.items())):
        look = engine_look(engine, index)
        linestyle, filled = LEVEL_STYLES[connection_levels.index(connections)]
        face = look["color"] if filled else "white"
        x, y = percentile_curve(samples)
        ax.plot(x, y, color=look["color"], linewidth=1.3, linestyle=linestyle, zorder=3)
        # A marker at each labelled percentile, so the values can be read
        # against the grid and the series told apart even where lines cross.
        marks = [p for p in PERCENTILES if p > 0]
        ax.plot(stretch(marks), np.percentile(samples, marks), linestyle="none",
                marker=look["marker"], markersize=4.5, color=look["color"],
                markerfacecolor=face, markeredgewidth=1.0, zorder=4)
        handles.append(Line2D(
            [], [], color=look["color"], linewidth=1.3, linestyle=linestyle,
            marker=look["marker"], markersize=4.5, markerfacecolor=face,
            markeredgewidth=1.0,
            label=label_for(look, mode, connections, modes_per_engine[engine],
                            connection_levels),
        ))
        most = max(most, x[-1])

    ax.set_xlim(1, most * 1.5)
    ax.xaxis.set_major_locator(FixedLocator(stretch(PERCENTILES)))
    ax.xaxis.set_major_formatter(FuncFormatter(
        lambda v, _: f"{100 - 100 / v:g}" if v > 1 else "0"))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xlabel("Percentile")

    ax.yaxis.set_major_locator(LogLocator(base=10, numticks=10))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_ms))
    ax.set_ylabel(f"{COLUMN_LABELS[args.column]} (ms)")

    ax.legend(handles=handles, loc="lower right", frameon=False, handlelength=2.8,
              labelspacing=0.5)

    fig.savefig(args.output, bbox_inches="tight")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
