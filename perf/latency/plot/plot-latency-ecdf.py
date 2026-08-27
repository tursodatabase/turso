#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Plot transaction latency as an eCDF from txn-latency CSV output.

Usage: uv run plot-latency-ecdf.py sqlite-c1.csv turso-c1.csv [--column total_ns]

Latency along the x axis on a log scale, the share of transactions at or
below it up the y axis. A marker sits on each curve at p50, p90 and p99,
and a line's right-hand end is its slowest transaction.

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
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator

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

# Percentiles that get a marker on the curve.
PERCENTILES = [50, 90, 99]

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


def ecdf_points(samples, max_points=2000):
    """Sorted samples and their cumulative percentage, thinned for plotting.

    Points are picked on an even grid and on a log grid running back from the
    slowest sample, so the last handful of transactions still show up
    individually.
    """
    x = np.sort(samples)
    n = x.size
    even = np.linspace(0, n - 1, min(max_points, n))
    tail = n - 1 - np.logspace(0, np.log10(n), min(max_points, n))
    idx = np.unique(np.clip(np.concatenate([even, tail]), 0, n - 1).astype(int))
    return x[idx], (idx + 1) / n * 100.0


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
    ax.grid(True, which="major", linewidth=0.4, alpha=0.6)
    ax.set_axisbelow(True)

    handles = []
    lo, hi = np.inf, 0.0
    for index, ((engine, mode, connections), samples) in enumerate(sorted(series.items())):
        look = engine_look(engine, index)
        linestyle, filled = LEVEL_STYLES[connection_levels.index(connections)]
        face = look["color"] if filled else "white"
        x, y = ecdf_points(samples)
        ax.plot(x, y, color=look["color"], linewidth=1.3, linestyle=linestyle, zorder=3)
        # A marker at p50, p90 and p99, so the values can be read against
        # the grid and the series told apart even where lines overlap.
        ax.plot(np.percentile(samples, PERCENTILES), PERCENTILES, linestyle="none",
                marker=look["marker"], markersize=4.5, color=look["color"],
                markerfacecolor=face, markeredgewidth=1.0, zorder=4)
        handles.append(Line2D(
            [], [], color=look["color"], linewidth=1.3, linestyle=linestyle,
            marker=look["marker"], markersize=4.5, markerfacecolor=face,
            markeredgewidth=1.0,
            label=label_for(look, mode, connections, modes_per_engine[engine],
                            connection_levels),
        ))
        lo = min(lo, float(x[0]))
        hi = max(hi, float(x[-1]))

    ax.set_xlim(10 ** np.floor(np.log10(max(lo, 1e-3))), 10 ** np.ceil(np.log10(hi)))
    ax.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(fmt_ms))
    ax.set_xlabel(f"{COLUMN_LABELS[args.column]} (ms)")

    ax.set_ylim(0, 103)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.set_ylabel("Transactions (%)")

    ax.legend(handles=handles, loc="lower right", frameon=False, handlelength=2.8,
              labelspacing=0.5)

    fig.savefig(args.output, bbox_inches="tight")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
