#!/usr/bin/env python3
"""Plot EC2 compare summary as throughput and latency PNGs."""
from __future__ import annotations

import re
from pathlib import Path

import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
SUMMARY = HERE / "summary.txt"
OUT_THR = HERE / "throughput.png"
OUT_LAT = HERE / "latency.png"

# Okabe-Ito
COLORS = {
    "sqlite": "#E69F00",
    "turso-main": "#0072B2",
    "turso-reclaim-new": "#009E73",
    "turso-gc-new": "#D55E00",
}
LABELS = {
    "sqlite": "SQLite",
    "turso-main": "Turso main",
    "turso-reclaim-new": "reclaim",
    "turso-gc-new": "reclaim + group-commit",
}
ORDER = ["sqlite", "turso-main", "turso-reclaim-new", "turso-gc-new"]


def parse_summary(text: str):
    lat = {}
    thr = {}
    section = None
    for line in text.splitlines():
        if line.startswith("=== LATENCY"):
            section = "lat"
            continue
        if line.startswith("=== THROUGHPUT"):
            section = "thr"
            continue
        if section == "lat":
            m = re.match(
                r"(\S+) c=(\d+) n=\d+ p50=([0-9.]+) p99\.9=([0-9.]+)", line
            )
            if m:
                eng, c, p50, p999 = m.group(1), int(m.group(2)), float(m.group(3)), float(m.group(4))
                lat.setdefault(eng, {})[c] = (p50, p999)
        elif section == "thr":
            m = re.match(r"(\S+) c=(\d+) mean=([0-9.]+) sd=([0-9.]+)", line)
            if m:
                eng, c, mean, sd = m.group(1), int(m.group(2)), float(m.group(3)), float(m.group(4))
                thr.setdefault(eng, {})[c] = (mean, sd)
    return lat, thr


def plot_throughput(thr: dict) -> None:
    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=160)
    for eng in ORDER:
        if eng not in thr:
            continue
        xs = sorted(thr[eng])
        ys = [thr[eng][c][0] for c in xs]
        yerr = [thr[eng][c][1] for c in xs]
        ax.errorbar(
            xs,
            ys,
            yerr=yerr,
            label=LABELS[eng],
            color=COLORS[eng],
            marker="o",
            linewidth=2,
            capsize=3,
        )
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8, 16, 32, 64])
    ax.set_xticklabels(["1", "2", "4", "8", "16", "32", "64"])
    ax.set_xlabel("Connections")
    ax.set_ylabel("Transactions per second")
    ax.set_title("EC2 NVMe throughput (mean ± sd, 3 runs)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(OUT_THR)
    plt.close(fig)


def plot_latency(lat: dict) -> None:
    conns = [1, 8, 16, 32]
    engines = [e for e in ORDER if e in lat]
    x = range(len(conns))
    width = 0.18
    fig, axes = plt.subplots(1, 2, figsize=(11, 5), dpi=160, sharex=True)

    for ax, idx, title, ylabel in (
        (axes[0], 0, "Latency p50", "ms"),
        (axes[1], 1, "Latency p99.9", "ms"),
    ):
        for i, eng in enumerate(engines):
            vals = [lat[eng][c][idx] for c in conns]
            offset = (i - (len(engines) - 1) / 2) * width
            ax.bar(
                [xi + offset for xi in x],
                vals,
                width=width,
                label=LABELS[eng],
                color=COLORS[eng],
            )
        ax.set_yscale("log")
        ax.set_xticks(list(x))
        ax.set_xticklabels([str(c) for c in conns])
        ax.set_xlabel("Connections")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, which="both", axis="y", alpha=0.3)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.suptitle("EC2 NVMe latency (log scale)", y=1.08)
    fig.tight_layout()
    fig.savefig(OUT_LAT, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    text = SUMMARY.read_text()
    lat, thr = parse_summary(text)
    if not lat or not thr:
        raise SystemExit(f"failed to parse {SUMMARY}")
    plot_throughput(thr)
    plot_latency(lat)
    print(f"wrote {OUT_THR}")
    print(f"wrote {OUT_LAT}")


if __name__ == "__main__":
    main()
