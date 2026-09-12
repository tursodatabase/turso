#!/usr/bin/env python3
"""Record the fixed logical-plan prepare protocol; retain per-workload output."""

import argparse
import gzip
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--phase", choices=("native", "callgrind"), required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--filter", default="")
    args = parser.parse_args()
    binary = args.binary.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    metadata = {
        "binary": str(binary),
        "cpu": args.cpu,
        "filter": args.filter,
        "phase": args.phase,
        "profile": "dev",
        "features": "default,bench",
    }
    for name, command in {
        "commit": ["git", "rev-parse", "HEAD"],
        "rustc": ["rustc", "-Vv"],
        "cargo": ["cargo", "-V"],
        "kernel": ["uname", "-a"],
        "cpu_info": ["lscpu"],
        "valgrind": ["valgrind", "--version"],
    }.items():
        metadata[name] = subprocess.check_output(command, text=True).strip()
    rounds = 7 if args.phase == "native" else 3
    for repeat in range(1, rounds + 1):
        prefix = output / f"{args.phase}-{repeat}"
        command = ["taskset", "-c", str(args.cpu)]
        if args.phase == "callgrind":
            function = "prepare_benchmark::measure_prepare"
            command += [
                "valgrind", "--tool=callgrind", "--collect-atstart=no",
                f"--toggle-collect={function}",
                f"--zero-before={function}", f"--dump-after={function}",
                f"--callgrind-out-file={prefix}.out",
            ]
        command += [str(binary), "--color", "never"]
        command += (["--test"] if args.phase == "callgrind" else [
            "--bench", "--sample-count", "10", "--sample-size", "1", "--timer", "os",
        ])
        if args.filter:
            command.append(args.filter)
        metadata.setdefault("commands", []).append(command)
        print(f"{args.phase} {repeat}/{rounds}: {prefix}", flush=True)
        with prefix.with_suffix(".txt").open("w") as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        metadata.setdefault("exit_codes", []).append(result.returncode)
        if args.phase == "callgrind":
            measurements = []
            files = sorted(output.glob(f"{prefix.name}.out*"), key=part_number)
            for path in files:
                fields = [line for line in path.read_text().splitlines() if
                          line.startswith(("desc:", "events:", "summary:", "totals:"))]
                measurements.append({"file": path.name + ".gz", "raw": fields})
                with path.open("rb") as source, gzip.open(str(path) + ".gz", "wb") as dest:
                    shutil.copyfileobj(source, dest)
                path.unlink()
            prefix.with_suffix(".json").write_text(json.dumps(measurements, indent=2) + "\n")
        (output / f"{args.phase}-environment.json").write_text(
            json.dumps(metadata, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(f"Benchmark failed; see {prefix}.txt")


def part_number(path):
    suffix = path.name.rsplit(".", 1)[-1]
    return int(suffix) if suffix.isdigit() else 0


if __name__ == "__main__":
    main()
