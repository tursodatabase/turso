#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy"]
# ///
"""Plot tail latency against the number of connections from txn-latency CSVs.

Usage: uv run plot-latency-percentiles.py plot/*-c*.csv

Every file is one engine at one connection count. The chart has one line per
engine and connections along the x axis, so a sweep such as
`CONNECTIONS="1 2 4 8 16" scripts/bench.sh` fits on one figure. It shows p99.9;
pass `--percentiles 99,99.9` for one panel per percentile.
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
COLORS = {"turso": "#2a78d6", "sqlite": "#eb6834"}
NAMES = {"sqlite": "SQLite", "turso": "Turso"}


def read_samples(paths, column):
    """Latency samples in ms, grouped by engine and connection count."""
    series = {}
    for path in paths:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                key = (row["engine"], int(row["connections"]))
                series.setdefault(key, []).append(float(row[column]) / 1e6)
    return {k: np.array(v) for k, v in series.items()}


def fmt_tick(value, _pos):
    if value >= 1:
        return f"{value:,.0f}"
    return f"{value:g}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("--column", default="total_ns")
    parser.add_argument("--percentiles", default="99.9",
                        help="comma-separated, one panel each")
    parser.add_argument("-o", "--output", default="latency-percentiles.png", type=Path)
    args = parser.parse_args()

    percentiles = [float(p) for p in args.percentiles.split(",")]
    series = read_samples(args.csv_files, args.column)
    if not series:
        raise SystemExit("no samples found")
    engines = sorted({engine for engine, _ in series})
    levels = sorted({connections for _, connections in series})

    plt.rcParams.update(
        {
            "font.family": ["Helvetica Neue", "Helvetica", "Arial",
                            "Liberation Sans", "DejaVu Sans"],
            "font.size": 11,
        }
    )
    fig, axes = plt.subplots(1, len(percentiles), figsize=(max(6.0, 4.5 * len(percentiles)), 4.2),
                             dpi=200, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    axes = np.atleast_1d(axes)

    for ax, percentile in zip(axes, percentiles):
        ax.set_facecolor(SURFACE)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.grid(True, axis="y", which="major", color=GRID, linewidth=0.8, zorder=0)
        ax.set_axisbelow(True)
        for engine in engines:
            xs = [c for c in levels if (engine, c) in series]
            ys = [float(np.percentile(series[(engine, c)], percentile)) for c in xs]
            color = COLORS.get(engine, INK)
            ax.plot(xs, ys, color=color, linewidth=2.2, marker="o", markersize=6.5,
                    markeredgecolor=SURFACE, markeredgewidth=1.5, zorder=3,
                    label=NAMES.get(engine, engine))
            # Name the line at its right end rather than in a legend box.
            ax.annotate(NAMES.get(engine, engine), xy=(xs[-1], ys[-1]), xytext=(8, 0),
                        textcoords="offset points", ha="left", va="center",
                        color=color, fontsize=11, fontweight="bold")
        if len(percentiles) > 1:
            ax.set_title(f"p{percentile:g}", loc="left", color=INK, fontsize=12, pad=10)
        ax.set_xticks(levels)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.set_xlim(levels[0] / 1.3, levels[-1] * 2.2)
        ax.set_xlabel("Connections", color=INK, fontsize=12, labelpad=10)
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=10))
        ax.yaxis.set_minor_locator(NullLocator())
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_tick))
        ax.tick_params(colors=MUTED, labelcolor=MUTED, length=0, pad=8, labelsize=10.5)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(AXIS)
        ax.spines["bottom"].set_linewidth(1)

    if len(percentiles) == 1:
        ylabel = f"p{percentiles[0]:g} transaction latency (ms)"
    else:
        ylabel = "Transaction latency (ms)"
    axes[0].set_ylabel(ylabel, color=INK, fontsize=12, labelpad=12)
    fig.subplots_adjust(left=0.14, right=0.95, top=0.92, bottom=0.16, wspace=0.12)
    fig.savefig(args.output, facecolor=SURFACE)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
