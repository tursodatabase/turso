# /// script
# dependencies = ["matplotlib", "numpy", "scienceplots"]
# ///

import argparse
import csv
import importlib
from pathlib import Path

import numpy as np

QUERIES = ("rare", "common", "and", "or", "phrase", "ranked")
SERIES = {
    ("sqlite", "wal"): ("SQLite FTS5 (WAL)", "#E69F00", ""),
    ("turso", "wal"): ("Turso (WAL)", "#0072B2", ""),
    ("turso", "mvcc"): ("Turso (MVCC)", "#0072B2", "///"),
}
CONFIGURATION = (
    "benchmark",
    "state",
    "documents",
    "connections",
    "requested_queries",
    "min_seconds",
    "debug_assertions",
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_files", nargs="+", type=Path)
    parser.add_argument("-o", "--output", action="append", type=Path)
    parser.add_argument("--sweep", action="store_true")
    parser.add_argument("--relative", action="store_true", help="plot speedup relative to SQLite (sweeps only)")
    parser.add_argument("--percentile", choices=["p50", "p95", "p99"], default="p50")
    args = parser.parse_args()
    if args.relative and not args.sweep:
        parser.error("--relative requires --sweep")
    configuration, samples = (
        read_sweep(args.csv_files, args.percentile) if args.sweep else read_runs(args.csv_files, args.percentile)
    )
    for output in args.output or [Path("fts.png"), Path("fts.pdf")]:
        if args.sweep:
            plot_sweep(configuration, samples, output, args.relative)
        else:
            plot(configuration, samples, output)
        print(f"wrote {output}")


def read_sweep(paths, percentile="p50"):
    configuration = None
    points = {}
    for path in paths:
        current, samples = read_runs([path], percentile)
        connections = int(current.pop("connections"))
        if connections <= 0 or connections in points:
            raise ValueError("each sweep file must have a distinct positive connection count")
        if configuration is None:
            configuration = current
        if current != configuration or (points and set(samples) != set(next(iter(points.values())))):
            raise ValueError("sweep files must match benchmark, sample budget, fixture and engine/mode series")
        points[connections] = samples
    if not points:
        raise ValueError("no sweep results found")
    return configuration, dict(sorted(points.items()))


def read_runs(paths, percentile="p50"):
    configuration = None
    samples = {}
    expected_series = None
    for path in paths:
        current, runs = read_file(path, percentile, samples)
        if configuration is None:
            configuration = current
        if configuration != current:
            raise ValueError("plot only one configuration at a time")
        for run in runs.values():
            if expected_series is None:
                expected_series = set(run)
            if set(run) != expected_series or any(seen != set(QUERIES) for seen in run.values()):
                raise ValueError(f"{path}: each run must contain the same series and all six query cases")
    if configuration is None:
        raise ValueError("no results found")
    configuration = dict(zip(CONFIGURATION, configuration))
    configuration["percentile"] = percentile
    return configuration, samples


def read_file(path, percentile, samples):
    configuration = None
    runs = {}
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        required = set(CONFIGURATION) | {"engine", "mode", "run", "query", "queries"}
        if not required <= set(reader.fieldnames or []):
            raise ValueError(f"{path}: missing benchmark columns")
        for row in reader:
            if row.get("profiled", "false") != "false":
                raise ValueError("profiled timings must not be used for benchmark comparisons")
            current = tuple(row[key] for key in CONFIGURATION)
            if configuration is None:
                configuration = current
            if configuration != current:
                raise ValueError("plot only one configuration at a time")
            key = (row["engine"], row["mode"])
            if key not in SERIES:
                raise ValueError(f"unsupported engine/mode: {key}")
            series = samples.setdefault(key, {query: [] for query in QUERIES})
            query = row["query"]
            seen = runs.setdefault(row["run"], {}).setdefault(key, set())
            if query not in QUERIES or query in seen:
                raise ValueError(f"{path}: unknown or duplicate query {query}")
            seen.add(query)
            series[query].append(read_measurement(row, percentile))
    if not runs:
        raise ValueError(f"{path}: no results found")
    return configuration, runs


def read_measurement(row, percentile):
    queries = int(row["queries"])
    positive_number(queries)
    if row["benchmark"] != "memory":
        seconds = positive_number(row["seconds"])
    if row["benchmark"] == "memory":
        if row["engine"] != "turso" or queries != int(row["requested_queries"]) * int(row["connections"]):
            raise ValueError("heap results require Turso and the requested queries per connection")
        value = float(row["peak_heap_bytes"]) / (1024 * 1024)
    elif row["benchmark"] == "search":
        if not 0 < int(row["connections"]) <= queries or queries != int(row["requested_queries"]):
            raise ValueError("search results require at least one sample per connection and the requested query count")
        value = float(row[f"{percentile}_ms"])
    elif row["benchmark"] == "throughput":
        minimum = positive_number(row["min_seconds"])
        if seconds < minimum:
            raise ValueError("throughput results must meet a positive minimum duration")
        value = queries / seconds
    else:
        raise ValueError("unknown benchmark")
    return positive_number(value)


def positive_number(value):
    value = float(value)
    if not np.isfinite(value) or value <= 0:
        raise ValueError("measurements must be finite and positive")
    return value


def plot_sweep(configuration, points, output, relative=False):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, MaxNLocator, NullLocator

    importlib.import_module("scienceplots")
    plt.style.use(["science", "no-latex"])
    fig, axes = plt.subplots(2, 3, figsize=(11, 8), dpi=300)
    connections = list(points)
    if relative and (configuration["benchmark"] == "memory" or ("sqlite", "wal") not in points[connections[0]]):
        raise ValueError("speedup plots require SQLite timing results")
    for index, (query, ax) in enumerate(zip(QUERIES, axes.flat)):
        table_rows = []
        for key in SERIES:
            if key not in points[connections[0]]:
                continue
            label, color, hatch = SERIES[key]
            medians, lows, highs = sweep_values(points, key, query, configuration["benchmark"], relative)
            short_label = "SQLite" if key[0] == "sqlite" else key[1].upper()
            table_rows.append([short_label, *[format_value(value) for value in medians]])
            ax.errorbar(
                connections,
                medians,
                yerr=[medians - lows, highs - medians],
                label=label,
                color=color,
                marker="s" if key[0] == "sqlite" else "o",
                markerfacecolor="white" if hatch else color,
                linestyle="--" if hatch else "-",
                linewidth=1.3,
                markersize=4,
                capsize=2,
                elinewidth=0.9,
            )
        ax.set_xscale("log", base=2)
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
        ax.set_xticks(connections)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: format_value(value)))
        ax.yaxis.set_minor_locator(NullLocator())
        ax.set_title(query)
        ax.grid(True, which="major", linewidth=0.5, linestyle=(0, (2, 2)), color="0.7")
        ax.set_axisbelow(True)
        ax.set_xlabel("Connections")
        ax.set_ylabel("Speedup vs SQLite (×)" if relative else measurement_label(configuration))
        if relative:
            ax.axhline(1, color="0.4", linewidth=0.8, linestyle=":")
        table = ax.table(
            cellText=table_rows, colLabels=["conn.", *connections], cellLoc="center", bbox=(0, -0.59, 1, 0.34)
        )
        table.auto_set_font_size(False)
        table.set_fontsize(7)
        for cell in table.get_celld().values():
            cell.set_linewidth(0.4)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(labels), frameon=False)
    build = "debug assertions" if configuration["debug_assertions"] == "true" else "no debug assertions"
    budget = (
        f'{configuration["requested_queries"]} queries/connection'
        if configuration["benchmark"] == "memory"
        else f'{configuration["requested_queries"]} samples per case/run'
        if configuration["benchmark"] == "search"
        else f'{configuration["min_seconds"]} s minimum per case/run'
    )
    direction = " · above 1× = faster than SQLite" if relative else ""
    fig.suptitle(
        f'{int(configuration["documents"]):,} synthetic documents · {configuration["state"]} · '
        f'{budget} · {build}{direction}',
        fontsize=10,
    )
    fig.subplots_adjust(left=0.075, right=0.98, top=0.91, bottom=0.22, hspace=1.15, wspace=0.35)
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)


def sweep_values(points, key, query, benchmark, relative):
    values = [point[key][query] for point in points.values()]
    medians = np.array([np.median(value) for value in values])
    lows = np.array([min(value) for value in values])
    highs = np.array([max(value) for value in values])
    if relative:
        baseline = np.array([np.median(point[("sqlite", "wal")][query]) for point in points.values()])
        if key == ("sqlite", "wal"):
            return np.ones_like(medians), np.ones_like(medians), np.ones_like(medians)
        if benchmark == "search":
            return baseline / medians, baseline / highs, baseline / lows
        return medians / baseline, lows / baseline, highs / baseline
    return medians, lows, highs


def plot(configuration, samples, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, NullLocator

    importlib.import_module("scienceplots")
    plt.style.use(["science", "no-latex"])
    fig, ax = plt.subplots(figsize=(7.2, 2.8), dpi=300)
    ax.set_yscale("log")
    ax.grid(True, axis="y", which="major", linewidth=0.5, linestyle=(0, (2, 2)), color="0.7")
    ax.set_axisbelow(True)
    table_rows, labels, all_values = [], [], []
    width = 0.8 / len(samples)
    for index, key in enumerate(key for key in SERIES if key in samples):
        label, color, hatch = SERIES[key]
        values = samples[key]
        medians = np.array([np.median(values[q]) for q in QUERIES])
        lows = np.array([min(values[q]) for q in QUERIES])
        highs = np.array([max(values[q]) for q in QUERIES])
        all_values.extend([*lows, *highs])
        table_rows.append([format_value(value) for value in medians])
        labels.append(label)
        offset = (index - (len(samples) - 1) / 2) * width
        ax.bar(
            np.arange(len(QUERIES)) + offset,
            medians,
            width,
            color=color,
            hatch=hatch,
            edgecolor="white",
            linewidth=0,
            label=label,
            zorder=3,
            yerr=[medians - lows, highs - medians],
            capsize=1.2,
            error_kw={"linewidth": 0.5, "capthick": 0.5, "zorder": 4},
        )
    ax.set_ylim(10 ** np.floor(np.log10(min(all_values))), 10 ** (np.ceil(np.log10(max(all_values))) + 0.5))
    ax.set_xlim(-0.5, len(QUERIES) - 0.5)
    ax.set_xticks([])
    ax.xaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
    ax.set_ylabel(measurement_label(configuration))
    table = ax.table(
        cellText=table_rows,
        rowLabels=labels,
        colLabels=QUERIES,
        cellLoc="center",
        loc="bottom",
        colWidths=[1 / len(QUERIES)] * len(QUERIES),
    )
    table.auto_set_font_size(False)
    table.set_fontsize(5.5)
    table.scale(1, 1.1)
    for cell in table.get_celld().values():
        cell.set_linewidth(0.4)
        cell.set_edgecolor("black")
    ax.legend(loc="upper left", ncol=len(samples), frameon=False, handlelength=1, handletextpad=0.4, columnspacing=1.8)
    build = "debug assertions" if configuration["debug_assertions"] == "true" else "no debug assertions"
    ax.set_title(
        f'{configuration["benchmark"]} · {int(configuration["documents"]):,} synthetic documents · '
        f'{configuration["state"]} · '
        f'{configuration["connections"]} connection(s) · {build}',
        fontsize=8,
    )
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)


def measurement_label(configuration):
    if configuration["benchmark"] == "memory":
        return "Peak live query heap (MiB)"
    if configuration["benchmark"] == "search":
        return f'{configuration["percentile"]} query latency (ms)'
    return "Searches per second"


def format_value(value):
    return f"{value:,.0f}" if round(value) >= 1000 else f"{value:.3g}"


if __name__ == "__main__":
    main()
