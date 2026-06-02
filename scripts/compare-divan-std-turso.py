#!/usr/bin/env python3
"""Compare Divan benchmark pairs named *_std and *_turso."""

import argparse
import csv
import os
import re
import subprocess
import sys

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
BENCH_RE = re.compile(
    r"^.*[\u251c\u2570]\u2500\s*"
    r"([A-Za-z0-9_]+_(?:std|turso))\s*(?:[|\u2502]|$)"
)
SIZE_RE = re.compile(r"[\u251c\u2570]\u2500\s*([0-9][0-9_,]*)\s+(.+?)\s*$")


def strip_ansi(line):
    return ANSI_RE.sub("", line)


def parse_duration_ns(value):
    value = value.strip()
    match = re.search(r"([0-9.]+)\s*(ns|us|\u00b5s|ms|s)", value)
    if not match:
        return None

    amount = float(match.group(1))
    unit = match.group(2)
    if unit == "ns":
        return amount
    if unit in ("us", "\u00b5s"):
        return amount * 1_000
    if unit == "ms":
        return amount * 1_000_000
    if unit == "s":
        return amount * 1_000_000_000
    return None


def parse_size_and_median(parts):
    for index, part in enumerate(parts):
        size_match = SIZE_RE.search(part)
        if not size_match:
            continue

        median_index = index + 2
        if len(parts) <= median_index:
            return None

        median = parts[median_index].strip()
        median_ns = parse_duration_ns(median)
        if median_ns is None:
            return None

        size = int(size_match.group(1).replace("_", "").replace(",", ""))
        return size, median, median_ns

    return None


def parse_bench_lines(lines):
    benches = {}
    current = None

    for raw_line in lines:
        line = strip_ansi(raw_line.rstrip("\n"))

        bench_match = BENCH_RE.search(line)
        if bench_match:
            current = bench_match.group(1)
            benches.setdefault(current, {})
            continue

        if current is None:
            continue

        parts = line.split("\u2502")
        if len(parts) < 3:
            parts = line.split("|")
        if len(parts) < 3:
            continue

        size_and_median = parse_size_and_median(parts)
        if size_and_median is None:
            continue
        size, median, median_ns = size_and_median

        benches[current][size] = {
            "median": median,
            "median_ns": median_ns,
        }

    return benches


def parse_benches(path):
    with open(path, encoding="utf-8") as bench_file:
        return parse_bench_lines(bench_file)


def run_bench(args):
    command = [
        "cargo",
        "bench",
        "-p",
        args.package,
        "--bench",
        args.bench,
    ]
    if args.nightly:
        command[0:1] = ["cargo", "+nightly"]
    if args.bench_filter:
        command.append(args.bench_filter)

    print(f"Running: {' '.join(command)}", file=sys.stderr)
    env = os.environ.copy()
    if args.nightly:
        rustflags = env.get("RUSTFLAGS")
        env["RUSTFLAGS"] = (
            f"{rustflags} --cfg nightly" if rustflags else "--cfg nightly"
        )
    result = subprocess.run(
        command,
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )

    if args.save_output:
        with open(args.save_output, "w", encoding="utf-8") as output_file:
            output_file.write(result.stdout)

    return parse_bench_lines(result.stdout.splitlines())


def paired_rows(benches, name_filter):
    bases = sorted(
        {
            name.rsplit("_", 1)[0]
            for name in benches
            if name.endswith("_std") or name.endswith("_turso")
        }
    )

    rows = []
    pattern = re.compile(name_filter) if name_filter else None
    for base in bases:
        if pattern and not pattern.search(base):
            continue

        std = benches.get(f"{base}_std")
        turso = benches.get(f"{base}_turso")
        if not std or not turso:
            continue

        for size in sorted(set(std) & set(turso)):
            std_ns = std[size]["median_ns"]
            turso_ns = turso[size]["median_ns"]
            ratio = turso_ns / std_ns
            delta = ((turso_ns - std_ns) / std_ns) * 100
            if ratio < 0.97:
                winner = "turso"
            elif ratio > 1.03:
                winner = "std"
            else:
                winner = "parity"

            rows.append(
                {
                    "benchmark": base,
                    "size": size,
                    "std_median": std[size]["median"],
                    "turso_median": turso[size]["median"],
                    "ratio": ratio,
                    "delta": delta,
                    "winner": winner,
                }
            )

    winner_order = {"std": 0, "parity": 1, "turso": 2}
    return sorted(
        rows,
        key=lambda row: (
            winner_order[row["winner"]],
            -row["delta"],
            row["benchmark"],
            row["size"],
        ),
    )


def print_markdown(rows):
    print("| benchmark | size | std median | turso median | turso/std | delta | winner |")
    print("|---|---:|---:|---:|---:|---:|---|")
    for row in rows:
        print(
            "| {benchmark} | {size} | {std_median} | {turso_median} | "
            "{ratio:.2f}x | {delta:+.1f}% | {winner} |".format(**row)
        )
    counts = winner_counts(rows)
    print()
    print(
        f"Summary: std={counts['std']}, parity={counts['parity']}, "
        f"turso={counts['turso']}"
    )


def winner_counts(rows):
    counts = {"std": 0, "parity": 0, "turso": 0}
    for row in rows:
        counts[row["winner"]] += 1
    return counts


def print_csv(rows):
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=[
            "benchmark",
            "size",
            "std_median",
            "turso_median",
            "ratio",
            "delta",
            "winner",
        ],
    )
    writer.writeheader()
    for row in rows:
        csv_row = dict(row)
        csv_row["ratio"] = f"{row['ratio']:.4f}"
        csv_row["delta"] = f"{row['delta']:.2f}"
        writer.writerow(csv_row)


def main():
    parser = argparse.ArgumentParser(
        description="Compare Divan benchmark output for *_std and *_turso pairs."
    )
    parser.add_argument(
        "bench_output",
        nargs="?",
        help="Path to captured cargo bench output. Omit when using --run.",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run cargo bench before printing the comparison table.",
    )
    parser.add_argument(
        "--package",
        default="turso_core",
        help="Cargo package to bench when using --run.",
    )
    parser.add_argument(
        "--bench",
        default="alloc_collections",
        help="Cargo bench target to run when using --run.",
    )
    parser.add_argument(
        "--bench-filter",
        help="Optional benchmark name filter passed to cargo bench when using --run.",
    )
    parser.add_argument(
        "--nightly",
        action="store_true",
        help="Run cargo bench with +nightly and RUSTFLAGS='--cfg nightly'.",
    )
    parser.add_argument(
        "--save-output",
        help="Save raw Divan stdout to this path when using --run.",
    )
    parser.add_argument(
        "--filter",
        help="Only include benchmark base names matching this regular expression",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "csv"),
        default="markdown",
        help="Output format",
    )
    args = parser.parse_args()

    if args.run:
        benches = run_bench(args)
    elif args.bench_output:
        benches = parse_benches(args.bench_output)
    else:
        parser.error("provide bench_output or use --run")

    rows = paired_rows(benches, args.filter)
    if args.format == "csv":
        print_csv(rows)
    else:
        print_markdown(rows)


if __name__ == "__main__":
    main()
