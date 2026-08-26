#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def load_rows(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def rate(row: dict) -> float:
    elapsed = float(row["elapsed_secs"])
    if elapsed <= 0:
        return 0.0
    return float(row["inserts"]) / elapsed


def median(xs: list[float]) -> float:
    ys = sorted(xs)
    n = len(ys)
    if n == 0:
        return 0.0
    if n % 2:
        return ys[n // 2]
    return (ys[n // 2 - 1] + ys[n // 2]) / 2


def detect_x(rows: list[dict]) -> str:
    topos = {r["topology"] for r in rows if r["engine"] == "turso"}
    if topos <= {"threads", "threads-pump"}:
        return "threads"
    return "workers"


def sql_threads(row: dict) -> int:
    topology = row["topology"]
    threads = int(row["threads"])
    # threads-pump CSV `threads` counts the io.step helper. X is SQL threads.
    if topology == "threads-pump":
        return threads - 1
    return threads


def x_value(row: dict, x_axis: str) -> int:
    if x_axis == "workers":
        return int(row["workers"])
    if x_axis == "threads":
        return sql_threads(row)
    raise ValueError(f"unknown x axis: {x_axis}")


def xlabel(x_axis: str) -> str:
    if x_axis == "workers":
        return "Workers on 1 SQL thread"
    if x_axis == "threads":
        return "SQL threads"
    raise ValueError(f"unknown x axis: {x_axis}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--x", choices=("workers", "threads"), default=None)
    args = parser.parse_args()
    rows = load_rows(args.csv_path)
    x_axis = args.x or detect_x(rows)

    sqlite_rates = [rate(r) for r in rows if r["engine"] == "sqlite"]
    sqlite = median(sqlite_rates)

    by_ckpt: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if r["engine"] != "turso":
            continue
        by_ckpt[r["checkpoint"]][x_value(r, x_axis)].append(rate(r))

    fig, ax = plt.subplots(figsize=(8, 5))
    xs_all = sorted({x for ckpt in by_ckpt.values() for x in ckpt})
    for ckpt, color in (("truncate", "#4C78A8"), ("passive", "#54A24B")):
        if ckpt not in by_ckpt:
            continue
        xs = sorted(by_ckpt[ckpt])
        ys = [median(by_ckpt[ckpt][x]) for x in xs]
        ax.plot(xs, ys, marker="o", color=color, linewidth=2, label=f"Turso {ckpt}")

    if sqlite_rates:
        ax.axhline(
            sqlite,
            color="#B279A2",
            linestyle="--",
            linewidth=1.5,
            label="SQLite occupancy-1",
        )

    ax.set_xlabel(xlabel(x_axis))
    ax.set_ylabel("Committed rows/s")
    ax.set_xticks(xs_all or [1, 2, 3, 4])
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _p: f"{int(x / 1000)}k"))
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.legend(frameon=True)
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=160)
    fig.savefig(args.out.with_suffix(".pdf"))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
