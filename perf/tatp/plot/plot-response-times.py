#!/usr/bin/env python3
# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///
"""Draw the response time of every TATP transaction against connections,
for one database size.

Usage: uv run plot-response-times.py --subscribers 100000 \
           plot/*-transactions.csv -o response-times.png -o response-times.pdf

One panel per transaction, all on the same log-scaled response time axis
so the panels can be compared at a glance. In each panel every engine has
a solid line for the 99th percentile and a dashed line for the median,
every point the mean over the runs of the same engine and connection
count. A transaction the mix never ran gets no panel.
"""

import argparse
import csv
from pathlib import Path

import numpy as np

# The same look as plot-mqth.py: Okabe-Ito colours, an engine told apart
# by its marker shape as well as its colour.
ENGINES = {
    "sqlite": {"name": "SQLite", "color": "#E69F00", "marker": "s"},
    "turso": {"name": "Turso", "color": "#0072B2", "marker": "o"},
}
FALLBACK_COLORS = ["#009E73", "#D55E00", "#CC79A7"]
FALLBACK_MARKERS = ["^", "D", "v"]

TRANSACTIONS = [
    "GET_SUBSCRIBER_DATA",
    "GET_NEW_DESTINATION",
    "GET_ACCESS_DATA",
    "UPDATE_SUBSCRIBER_DATA",
    "UPDATE_LOCATION",
    "INSERT_CALL_FORWARDING",
    "DELETE_CALL_FORWARDING",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("-o", "--output", action="append", type=Path, metavar="FILE",
                        help="output file; repeat for several formats "
                             "(default response-times.png)")
    parser.add_argument("--name", action="append", default=[], metavar="ENGINE=NAME",
                        help="legend name for an engine, e.g. turso=Limbo")
    parser.add_argument("--subscribers", type=int, required=True,
                        help="database size to draw; runs of other sizes are left out")
    args = parser.parse_args()
    names = dict(name.split("=", 1) for name in args.name)

    # (engine, transaction, connections) -> [(p50, p99)] over runs
    runs = {}
    for path in args.csv_files:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                if int(row["completed"]) == 0 or int(row["subscribers"]) != args.subscribers:
                    continue
                key = (row["engine"], row["transaction"], int(row["connections"]))
                runs.setdefault(key, []).append((float(row["p50_ms"]), float(row["p99_ms"])))
    if not runs:
        raise SystemExit("no results found")

    for output in args.output or [Path("response-times.png")]:
        draw(runs, names, output)
        print(f"wrote {output}")


def draw(runs, names, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import scienceplots  # noqa: F401  (registers the styles)
    from matplotlib.lines import Line2D
    from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator

    plt.style.use(["science", "no-latex"])

    engines = sorted({e for e, _, _ in runs})
    transactions = [t for t in TRANSACTIONS if any(k[1] == t for k in runs)]
    connections = sorted({c for _, _, c in runs})
    columns = 4 if len(transactions) > 4 else len(transactions)
    rows = -(-len(transactions) // columns)
    fig, axes = plt.subplots(rows, columns, figsize=(1.9 * columns, 1.75 * rows), dpi=300,
                             sharex=True, sharey=True, squeeze=False)

    looks = {}
    for i, engine in enumerate(engines):
        look = ENGINES.get(engine)
        looks[engine] = (
            names.get(engine) or (look["name"] if look else engine),
            look["color"] if look else FALLBACK_COLORS[i % len(FALLBACK_COLORS)],
            look["marker"] if look else FALLBACK_MARKERS[i % len(FALLBACK_MARKERS)],
        )

    for ax, transaction in zip(axes.flat, transactions):
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.grid(True, which="major", linewidth=0.5, linestyle=(0, (2, 2)), color="0.7")
        ax.set_axisbelow(True)
        for engine in engines:
            _, color, marker = looks[engine]
            points = sorted((c, v) for (e, t, c), v in runs.items()
                            if e == engine and t == transaction)
            if not points:
                continue
            xs = [c for c, _ in points]
            p50 = [float(np.mean([p for p, _ in v])) for _, v in points]
            p99 = [float(np.mean([p for _, p in v])) for _, v in points]
            ax.plot(xs, p99, color=color, linewidth=1.2, marker=marker, markersize=3.5, zorder=3)
            ax.plot(xs, p50, color=color, linewidth=1.0, linestyle=(0, (4, 2)), marker=marker,
                    markersize=3.5, markerfacecolor="white", zorder=3)
        ax.set_title(transaction.replace("_", " ").lower(), fontsize=7)
        ax.set_xticks(connections)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=12))
        ax.yaxis.set_minor_locator(NullLocator())
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.set_xlim(connections[0] / 1.5, connections[-1] * 1.5)
        ax.tick_params(labelsize=6)
    for ax in axes.flat[len(transactions):]:
        ax.set_visible(False)
    for ax in axes[:, 0]:
        ax.set_ylabel("Response time (ms)", fontsize=7)
    for ax in axes[-1, :]:
        ax.set_xlabel("Connections", fontsize=7)
    # A hidden panel leaves the one above it as the lowest in its column.
    for column in range(columns):
        visible = [axes[r, column] for r in range(rows) if axes[r, column].get_visible()]
        if visible:
            visible[-1].xaxis.set_tick_params(labelbottom=True)
            visible[-1].set_xlabel("Connections", fontsize=7)

    handles = [Line2D([], [], color=color, linestyle="none", marker=marker, markersize=4,
                      label=name) for name, color, marker in looks.values()]
    handles += [Line2D([], [], color="0.4", linewidth=1.2, label="p99"),
                Line2D([], [], color="0.4", linewidth=1.0, linestyle=(0, (4, 2)), label="p50")]
    fig.legend(handles=handles, loc="lower center", ncol=len(handles), frameon=False,
               fontsize=7, handlelength=1.6, columnspacing=1.6, bbox_to_anchor=(0.5, -0.04))
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    main()
