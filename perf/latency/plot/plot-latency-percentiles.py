#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
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
import scienceplots  # noqa: F401  (registers the styles)
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator

plt.style.use(["science", "no-latex", "vibrant"])

COLORS = {"turso": "#0077BB", "sqlite": "#CC3311"}
MARKERS = {"turso": "o", "sqlite": "s"}
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

    fig, axes = plt.subplots(1, len(percentiles),
                             figsize=(3.6 * len(percentiles) + 0.4, 3.0), dpi=300,
                             sharey=True)
    axes = np.atleast_1d(axes)

    for ax, percentile in zip(axes, percentiles):
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.grid(True, axis="y", which="major", linewidth=0.4, alpha=0.5)
        ax.set_axisbelow(True)
        for engine in engines:
            xs = [c for c in levels if (engine, c) in series]
            ys = [float(np.percentile(series[(engine, c)], percentile)) for c in xs]
            ax.plot(xs, ys, color=COLORS.get(engine, "black"), linewidth=1.4,
                    marker=MARKERS.get(engine, "o"), markersize=4, zorder=3,
                    label=NAMES.get(engine, engine))
        if len(percentiles) > 1:
            ax.set_title(f"p{percentile:g}", loc="left")
        ax.set_xticks(levels)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.set_xlim(levels[0] / 1.4, levels[-1] * 1.4)
        ax.set_xlabel("Connections")
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=10))
        ax.yaxis.set_minor_locator(NullLocator())
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_tick))
        ax.legend(loc="center right", frameon=True, framealpha=1, edgecolor="0.8",
                  fancybox=False)

    if len(percentiles) == 1:
        ylabel = f"p{percentiles[0]:g} transaction latency (ms)"
    else:
        ylabel = "Transaction latency (ms)"
    axes[0].set_ylabel(ylabel)
    fig.savefig(args.output, bbox_inches="tight")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
