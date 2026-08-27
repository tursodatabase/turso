#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Plot transaction latency as an eCDF from txn-latency CSV output.

Usage: uv run plot-latency-ecdf.py sqlite-c1.csv turso-c1.csv [--column total_ns]

Pass files from more than one connection count to compare concurrency levels:
each engine keeps its colour and each connection count gets its own line
style. Two levels read well; the full sweep belongs in
plot-latency-percentiles.py instead.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import scienceplots  # noqa: F401  (registers the styles)
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator

plt.style.use(["science", "no-latex", "vibrant"])

# Paul Tol's "vibrant" cycle, which the style above installs: colour-blind
# safe and legible in greyscale.
COLORS = {"turso": "#0077BB", "sqlite": "#CC3311"}
FALLBACK = ["#009988", "#EE7733", "#33BBEE"]
LINE_STYLES = ["solid", (0, (4, 2)), (0, (1, 1.5))]

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


def label_for(engine, mode, connections, modes_per_engine, connection_levels):
    name = {"sqlite": "SQLite", "turso": "Turso"}.get(engine, engine)
    if len(modes_per_engine[engine]) > 1:
        name = f"{name} ({mode})"
    if len(connection_levels) > 1:
        unit = "connection" if connections == 1 else "connections"
        name = f"{name}, {connections} {unit}"
    return name


def color_for(engine, index):
    return COLORS.get(engine, FALLBACK[index % len(FALLBACK)])


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
    connection_levels = sorted({connections for _, _, connections in series})
    if len(connection_levels) > len(LINE_STYLES):
        raise SystemExit(
            f"{len(connection_levels)} connection counts is too many for one eCDF; "
            "plot the sweep with plot-latency-percentiles.py"
        )

    fig, ax = plt.subplots(figsize=(6, 3.6), dpi=300)
    ax.set_xscale("log")
    ax.grid(True, which="major", linewidth=0.4, alpha=0.5)
    ax.set_axisbelow(True)

    Y_MAX = 104
    lo, hi = np.inf, 0.0
    for index, ((engine, mode, connections), samples) in enumerate(sorted(series.items())):
        color = color_for(engine, index)
        style = LINE_STYLES[connection_levels.index(connections)]
        # The p99 goes in the legend next to the line's name, and a dot on
        # the curve shows where it sits.
        p99 = float(np.percentile(samples, 99))
        label = label_for(engine, mode, connections, modes_per_engine, connection_levels)
        label = f"{label}: p99 {fmt_ms(p99)}"
        x, y = ecdf_points(samples)
        ax.plot(x, y, color=color, linewidth=1.4, linestyle=style, zorder=3, label=label)
        ax.plot([p99], [99], marker="o", markersize=4.5, color=color,
                markeredgecolor="white", markeredgewidth=0.8, zorder=4)

        # A faint vertical line marks the slowest transaction: where the tail
        # ends. Its label hangs from the top, leaving the lower right corner
        # to the legend.
        worst = float(np.max(samples))
        ax.axvline(worst, ymax=100 / Y_MAX, color=color, linewidth=0.7, alpha=0.5,
                   linestyle=(0, (3, 2)), zorder=2)
        ax.annotate(fmt_ms(worst), xy=(worst, 100), xytext=(-2, -4),
                    textcoords="offset points", ha="right", va="top", rotation=90,
                    color=color, fontsize=6.5, zorder=5)

        lo = min(lo, float(np.min(samples)))
        hi = max(hi, worst)

    ax.set_xlim(10 ** np.floor(np.log10(max(lo, 1e-3))), 10 ** np.ceil(np.log10(hi)))
    ax.set_xlabel(f"{COLUMN_LABELS[args.column]} (ms)")
    ax.xaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(fmt_tick))
    ax.set_ylim(0, Y_MAX)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.set_ylabel("Transactions (\\%)" if plt.rcParams["text.usetex"] else "Transactions (%)")

    ax.legend(loc="lower right", frameon=True, framealpha=1, edgecolor="0.8",
              fancybox=False, handlelength=2.6)

    fig.savefig(args.output, bbox_inches="tight")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
