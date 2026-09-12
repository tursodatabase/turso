#!/usr/bin/env python3
"""Summarize retained Divan/Callgrind runs using the fixed acceptance protocol."""

import argparse
import csv
import json
from pathlib import Path
import re
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    args = parser.parse_args()
    baseline = summarize(args.baseline)
    if args.candidate:
        candidate = summarize(args.candidate)
        if baseline.keys() != candidate.keys():
            raise ValueError("Baseline and candidate workload sets differ")
        comparisons = []
        for name, before in baseline.items():
            after = candidate[name]
            native_delta = after["native_median_ns"] - before["native_median_ns"]
            instructions_delta = after["instructions_median"] - before["instructions_median"]
            comparisons.append({
                "workload": name,
                "baseline_ns": before["native_median_ns"],
                "candidate_ns": after["native_median_ns"],
                "native_delta_percent": 100 * native_delta / before["native_median_ns"],
                "native_uncertainty_ns": before["native_uncertainty_ns"],
                "native_failed": native_delta > before["native_uncertainty_ns"],
                "baseline_instructions": before["instructions_median"],
                "candidate_instructions": after["instructions_median"],
                "instructions_delta_percent": 100 * instructions_delta / before["instructions_median"],
                "instructions_failed": max(after["instructions"]) > max(before["instructions"]),
            })
        with (args.candidate / "comparison.csv").open("w") as output:
            writer = csv.DictWriter(output, fieldnames=comparisons[0].keys())
            writer.writeheader()
            writer.writerows(comparisons)
        failures = [row for row in comparisons if row["native_failed"] or row["instructions_failed"]]
        print(json.dumps({"workloads": len(comparisons), "failures": failures}, indent=2))
        raise SystemExit(bool(failures))
    else:
        print(f"Summarized {len(baseline)} workloads in {args.baseline / 'summary.json'}")


def summarize(directory):
    native = [native_run(directory / f"native-{repeat}.txt") for repeat in range(1, 8)]
    instructions = [instruction_run(directory, repeat) for repeat in range(1, 4)]
    names = native[0].keys()
    if any(run.keys() != names for run in native + instructions):
        raise ValueError("Recorded runs have different workload sets")
    summary = {}
    for name in names:
        times = [run[name] for run in native]
        counts = [run[name] for run in instructions]
        median = statistics.median(times)
        mad = statistics.median(abs(value - median) for value in times)
        summary[name] = {
            "native_ns": times,
            "native_median_ns": median,
            "native_uncertainty_ns": max(max(times) - min(times), 3 * mad),
            "instructions": counts,
            "instructions_median": statistics.median(counts),
        }
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def native_run(path):
    entries = tree_entries(path)
    values = {}
    for name, parts in entries:
        if len(parts) < 4 or not parts[2].strip():
            continue
        if name in values:
            raise ValueError(f"Duplicate workload {name} in {path}")
        values[name] = nanoseconds(parts[2])
    if not values:
        raise ValueError(f"No timed workloads in {path}; native runs require --bench")
    return values


def instruction_run(directory, repeat):
    entries = tree_entries(directory / f"callgrind-{repeat}.txt")
    leaves = [name for index, (name, _) in enumerate(entries)
              if index + 1 == len(entries) or not entries[index + 1][0].startswith(name + "/")]
    dumps = json.loads((directory / f"callgrind-{repeat}.json").read_text())
    counts = []
    for dump in dumps:
        if not any("Trigger: --dump-after=prepare_benchmark::measure_prepare" in field for field in dump["raw"]):
            continue
        totals = [int(field.split()[1]) for field in dump["raw"] if field.startswith("totals:")]
        if len(totals) != 1 or totals[0] <= 0:
            raise ValueError(f"Missing instruction count in {dump['file']}")
        counts.append(totals[0])
    if len(leaves) != len(counts) or len(set(leaves)) != len(leaves):
        raise ValueError(f"Cannot pair {len(leaves)} workloads with {len(counts)} instruction dumps")
    return dict(zip(leaves, counts))


def tree_entries(path):
    stack = []
    entries = []
    for line in path.read_text().splitlines():
        match = re.match(r"^([│ ]*)[├╰]─ (.*)$", line)
        if not match:
            continue
        depth = len(match[1]) // 3
        parts = match[2].split("│")
        name = re.split(r"\s{2,}", parts[0].strip())[0]
        if depth > len(stack):
            raise ValueError(f"Invalid benchmark tree in {path}: {line}")
        stack = stack[:depth] + [name]
        entries.append(("/".join(stack), parts))
    return entries


def nanoseconds(value):
    match = re.fullmatch(r"\s*([\d.]+)\s*(ns|µs|us|ms|s)\s*", value)
    if not match:
        raise ValueError(f"Unrecognized timing: {value!r}")
    return float(match[1]) * {"ns": 1, "µs": 1000, "us": 1000, "ms": 1_000_000, "s": 1_000_000_000}[match[2]]


if __name__ == "__main__":
    main()
