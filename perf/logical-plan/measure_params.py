#!/usr/bin/env python3
"""Retain seven native runs of the existing Criterion parameter prepare corpus."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    args = parser.parse_args()
    binary = args.binary.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise SystemExit(f"Refusing to overwrite {output}")
    metadata = {
        "binary": str(binary),
        "binary_sha256": hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
        "source_revision": args.source_revision,
        "checkout_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True).strip(),
        "profile": "dev", "features": "default,bench", "cpu": args.cpu,
        "commands": [], "exit_codes": [],
    }
    for repeat in range(8):
        run = output / ("test" if repeat == 0 else f"native-{repeat}")
        env = dict(os.environ, CRITERION_HOME=str(run))
        command = ["taskset", "-c", str(args.cpu), str(binary), "--color", "never", "--noplot"]
        command += (["--test"] if repeat == 0 else [
            "--bench", "--sample-size", "10", "--warm-up-time", "1", "--measurement-time", "1",
        ])
        metadata["commands"].append(command)
        with run.with_suffix(".txt").open("w") as log:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        metadata["exit_codes"].append(result.returncode)
        (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(f"Benchmark failed: {run}.txt")
        print(f"Completed {run}", flush=True)
    summarize(output)


def summarize(output):
    runs = []
    for repeat in range(1, 8):
        run = output / f"native-{repeat}"
        samples = {}
        for path in run.glob("**/new/sample.json"):
            sample = json.loads(path.read_text())
            if not sample["iters"] or len(sample["iters"]) != len(sample["times"]):
                raise ValueError(f"Incomplete samples in {path}")
            samples[str(path.parent.parent.relative_to(run))] = [
                time / iterations for time, iterations in zip(sample["times"], sample["iters"])
            ]
        if not samples:
            raise ValueError(f"No Criterion samples in {run}")
        runs.append(samples)
    if any(run.keys() != runs[0].keys() for run in runs):
        raise ValueError("Criterion workload sets differ")
    summary = {}
    for name in runs[0]:
        medians = [statistics.median(run[name]) for run in runs]
        median = statistics.median(medians)
        mad = statistics.median(abs(value - median) for value in medians)
        summary[name] = {
            "samples_ns": [run[name] for run in runs], "native_ns": medians,
            "native_median_ns": median,
            "native_uncertainty_ns": max(max(medians) - min(medians), 3 * mad),
        }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


if __name__ == "__main__":
    main()
