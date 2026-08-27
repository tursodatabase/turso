#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Plot transaction latency as an eCDF from txn-latency CSV output.

Usage: uv run plot-latency-ecdf.py sqlite-c{1,8,16,32}.csv turso-c{1,8,16,32}.csv

Latency along the x axis on a log scale, the share of transactions at or
below it up the y axis. A marker sits on each curve at p50 and p90, and a
dashed vertical line labelled with the value marks its p99.9. A curve's
right-hand end is its slowest transaction.

Each connection count gets its own panel, two to a row with shared axes,
so the engines are compared within a panel and the effect of concurrency
is read across panels. One file per engine and count, as `bench.sh`
writes them.
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

# Percentiles that get a marker on the curve.
PERCENTILES = [50, 90]
# Vertical line at the tail percentile.
TAIL_STYLE = (0, (4, 2))

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


def label_for(look, mode, modes_per_engine):
    name = look["name"]
    if len(modes_per_engine) > 1:
        name = f"{name} ({mode})"
    return name


def panel_title(connections):
    return "1 connection" if connections == 1 else f"{connections} connections"


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

    # One panel per connection count, two to a row.
    ncols = min(2, len(connection_levels))
    nrows = -(-len(connection_levels) // ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.3 * ncols, 2.6 * nrows), dpi=300,
                             sharex=True, sharey=True, squeeze=False)
    lo = min(float(np.min(v)) for v in series.values())
    hi = max(float(np.max(v)) for v in series.values())

    handles = {}
    for ax, connections in zip(axes.flat, connection_levels):
        ax.set_xscale("log")
        ax.grid(True, which="major", linewidth=0.4, alpha=0.6)
        ax.set_axisbelow(True)
        panel = {k: v for k, v in series.items() if k[2] == connections}
        for index, ((engine, mode, _), samples) in enumerate(sorted(panel.items())):
            look = engine_look(engine, index)
            x, y = ecdf_points(samples)
            ax.plot(x, y, color=look["color"], linewidth=1.3, zorder=3)
            # A marker at p50 and p90, so the values can be read against the
            # grid and the series told apart even where lines overlap.
            ax.plot(np.percentile(samples, PERCENTILES), PERCENTILES, linestyle="none",
                    marker=look["marker"], markersize=4, color=look["color"],
                    markeredgewidth=1.0, zorder=4)
            # The tail, as a vertical line at p99.9 with the value written
            # along it in the engine's colour. Each engine gets its own half
            # of the height, so the labels stay apart when the lines fall on
            # the same spot.
            tail = float(np.percentile(samples, 99.9))
            ax.axvline(tail, color=look["color"], linewidth=0.9, linestyle=TAIL_STYLE,
                       zorder=2)
            y, va = (45, "top") if index == 0 else (55, "bottom")
            ax.annotate(f"{fmt_ms(tail)} ms", xy=(tail, y), xytext=(-3, 0),
                        textcoords="offset points", rotation=90, ha="right", va=va,
                        color=look["color"], fontsize=6.5, zorder=5)
            label = label_for(look, mode, modes_per_engine[engine])
            handles.setdefault(label, Line2D(
                [], [], color=look["color"], linewidth=1.3, marker=look["marker"],
                markersize=4, label=label))
        ax.set_title(panel_title(connections), loc="left", fontsize=plt.rcParams["font.size"])

    for ax in axes.flat[len(connection_levels):]:
        ax.set_visible(False)

    ax0 = axes[0, 0]
    # Start at a decade, end just past the slowest transaction: rounding the
    # top up to a decade can leave most of a panel empty.
    ax0.set_xlim(10 ** np.floor(np.log10(max(lo, 1e-3))), hi * 1.5)
    ax0.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax0.xaxis.set_minor_locator(NullLocator())
    ax0.xaxis.set_major_formatter(FuncFormatter(fmt_ms))
    ax0.set_ylim(0, 103)
    ax0.set_yticks([0, 25, 50, 75, 100])
    for ax in axes[-1, :]:
        ax.set_xlabel(f"{COLUMN_LABELS[args.column]} (ms)")
    for ax in axes[:, 0]:
        ax.set_ylabel("Transactions (%)")

    # One legend for the figure, centred below the panels.
    fig.legend(handles=list(handles.values()), loc="lower center", ncol=len(handles),
               frameon=False, handlelength=2.4, columnspacing=2.0,
               bbox_to_anchor=(0.5, -0.2 / nrows))

    fig.savefig(args.output, bbox_inches="tight")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
