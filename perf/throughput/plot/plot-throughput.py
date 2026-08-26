# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib", "pandas"]
# ///
"""Plot write throughput against thread count from benchmark CSVs.

Usage: uv run plot-throughput.py turso.csv sqlite.csv
"""

import argparse
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

# Fixed slot order, so a system keeps its colour no matter which CSVs are passed.
SYSTEM_COLORS = {"Turso": "#2a78d6", "SQLite": "#eb6834"}
FALLBACK_COLORS = ["#1baf7a", "#eda100", "#e87ba4", "#4a3aa7"]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv", nargs="+", help="benchmark CSV files")
    p.add_argument("-o", "--output", default="throughput.png")
    p.add_argument("--compute", type=int, default=0,
                   help="per-transaction compute time (us) to plot")
    p.add_argument("--max-threads", type=int, default=None)
    p.add_argument("--x-label", default="Threads")
    p.add_argument("--no-caption", action="store_true",
                   help="omit the run-configuration caption")
    return p.parse_args()


def format_count(value, _pos=None):
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value / 1_000:g}k"
    return f"{value:g}"


def load(paths, compute, max_threads):
    df = pd.concat([pd.read_csv(path) for path in paths], ignore_index=True)

    missing = {"system", "threads", "compute", "throughput"} - set(df.columns)
    if missing:
        sys.exit(f"CSV is missing columns: {', '.join(sorted(missing))}")

    df = df[df["compute"] == compute]
    if max_threads is not None:
        df = df[df["threads"] <= max_threads]
    if df.empty:
        sys.exit(f"no rows with compute={compute}")

    # Repeated runs of the same configuration collapse to their median.
    runs = df.groupby(["system", "threads"]).size().max()
    df = (df.groupby(["system", "threads"], as_index=False)["throughput"]
            .median()
            .sort_values("threads"))
    return df, runs


def caption_for(paths, compute, runs):
    df = pd.concat([pd.read_csv(path) for path in paths], ignore_index=True)
    parts = []
    if "mode" in df.columns:
        modes = sorted(str(m) for m in df["mode"].dropna().unique())
        parts.append(", ".join(modes))
    if "batch_size" in df.columns and df["batch_size"].nunique() == 1:
        parts.append(f"batch size {int(df['batch_size'].iloc[0])}")
    parts.append(f"{compute}us compute per transaction")
    parts.append(f"median of {runs} run{'s' if runs != 1 else ''}")
    return " · ".join(parts)


def main():
    args = parse_args()
    df, runs = load(args.csv, args.compute, args.max_threads)

    plt.rcParams.update({
        "font.family": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
        "font.size": 11,
    })

    fig, ax = plt.subplots(figsize=(8, 5), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)

    systems = sorted(df["system"].unique(), key=lambda s: s != "SQLite")
    fallback = iter(FALLBACK_COLORS)
    for system in systems:
        data = df[df["system"] == system]
        color = SYSTEM_COLORS.get(system) or next(fallback, "#888888")
        ax.plot(data["threads"], data["throughput"], color=color, linewidth=2,
                marker="o", markersize=5, markeredgecolor=SURFACE,
                markeredgewidth=1.5, zorder=3, solid_capstyle="round")
        ax.annotate(system, xy=(data["threads"].iloc[-1], data["throughput"].iloc[-1]),
                    xytext=(8, 0), textcoords="offset points", va="center",
                    ha="left", color=INK, fontsize=12)

    threads = sorted(df["threads"].unique())
    span = threads[-1] - threads[0]
    ax.set_xlim(threads[0] - 0.25 * span / max(len(threads) - 1, 1),
                threads[-1] + 0.25 * span / max(len(threads) - 1, 1))
    ax.set_ylim(0, df["throughput"].max() * 1.1)
    ax.set_xticks(threads)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(format_count))
    ax.tick_params(colors=MUTED, labelcolor=MUTED, length=0, pad=6)

    ax.set_xlabel(args.x_label, color=INK, fontsize=12, labelpad=10)
    ax.set_ylabel("Throughput (rows/sec)", color=INK, fontsize=12, labelpad=10)

    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(1)

    fig.tight_layout(pad=1.6)
    fig.subplots_adjust(right=0.88)

    if not args.no_caption:
        fig.text(0.5, 0.015, caption_for(args.csv, args.compute, runs),
                 ha="center", color=MUTED, fontsize=9)
        fig.subplots_adjust(bottom=0.16)

    fig.savefig(args.output, facecolor=SURFACE)
    print(f"Saved plot to {args.output}")


if __name__ == "__main__":
    main()
