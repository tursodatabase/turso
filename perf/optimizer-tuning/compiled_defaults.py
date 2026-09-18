#!/usr/bin/env python3
"""Compare two ordinary release builds that carry their defaults compiled in.

The search and the validation both run a build with the `optimizer_params`
feature and a parameter file. This script removes both: it runs two builds
without that feature, with `TURSO_OPTIMIZER_PARAMS` unset, so the result cannot
depend on the override path or on a stale executable.

The two builds run in turn inside every round and their order turns around on
every other round.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import runner

REGIMES = ("nostats", "analyzed")


def refuses_the_override_feature(binary):
    """A build without the feature must not be able to print its parameters."""
    result = subprocess.run(
        [binary, "--print-params", "--database", "x", "--query-dir", "x"],
        capture_output=True,
    )
    return result.returncode != 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", nargs="+", required=True,
                        help="name=path, for example stock=/tmp/bin/jb-stock")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--query-dir", default="perf/tpc-h/queries")
    parser.add_argument("--out", required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args()

    os.environ.pop("TURSO_OPTIMIZER_PARAMS", None)

    workload = runner.Workload(args.query_dir)
    names = workload.names
    databases = {
        "nostats": os.path.join(args.data_dir, "tpch-nostats.db"),
        "nostats_rw": os.path.join(args.data_dir, "tpch-nostats-rw.db"),
        "analyzed": os.path.join(args.data_dir, "tpch-analyzed.db"),
        "analyzed_rw": os.path.join(args.data_dir, "tpch-analyzed-rw.db"),
    }

    builds = []
    out_dir = os.path.dirname(os.path.abspath(args.out)) or "."
    os.makedirs(out_dir, exist_ok=True)
    for entry in args.binaries:
        name, path = entry.split("=", 1)
        engine = runner.Engine(path, databases,
                               os.path.join(out_dir, "vectors"),
                               os.path.join(out_dir, f"logs-{name}"))
        if not refuses_the_override_feature(path):
            raise SystemExit(
                f"{path} still carries the optimizer_params feature; "
                "this comparison needs builds without it"
            )
        builds.append((name, engine))

    print("plans each build selects:")
    programs = {}
    for name, engine in builds:
        for regime in REGIMES:
            records = engine.plans(regime, names, None, workload)
            for query in names:
                programs[(name, regime, query)] = runner.plan_fingerprint(
                    records[query]
                )[0]
    first = builds[0][0]
    for name, _ in builds[1:]:
        changed = [
            f"{regime}/Q{query}" for regime in REGIMES for query in names
            if programs[(name, regime, query)] != programs[(first, regime, query)]
        ]
        print(f"  {name} differs from {first} on {len(changed)} of "
              f"{2 * len(names)}: {changed}")

    done = set()
    if os.path.exists(args.out):
        with open(args.out, encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    row = json.loads(line)
                    done.add((row["build"], row["round"], row["regime"],
                              row["query"]))

    for round_index in range(args.rounds):
        order = builds if round_index % 2 == 0 else list(reversed(builds))
        for name, engine in order:
            for regime in REGIMES:
                for query in names:
                    if (name, round_index, regime, query) in done:
                        continue
                    outcome = engine.measure(
                        regime, query, None, workload, args.warmups,
                        args.repetitions, args.timeout_seconds,
                    )
                    median = (
                        statistics.median(
                            s["elapsed_ns"] / 1e9 for s in outcome["samples"]
                        )
                        if outcome["status"] == "ok" else float(args.timeout_seconds)
                    )
                    record = {
                        "build": name, "round": round_index, "regime": regime,
                        "query": query, "status": outcome["status"],
                        "median_seconds": median,
                        "samples": [s["elapsed_ns"] / 1e9
                                    for s in outcome["samples"]],
                        "program": programs[(name, regime, query)],
                        "measured_at": time.time(),
                    }
                    with open(args.out, "a", encoding="utf-8") as handle:
                        handle.write(json.dumps(record, sort_keys=True) + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())
            print(f"round {round_index} {name} done", flush=True)

    rows = [json.loads(line) for line in open(args.out, encoding="utf-8")
            if line.strip()]
    totals = {}
    for row in rows:
        totals.setdefault((row["build"], row["round"], row["regime"]), 0.0)
        totals[(row["build"], row["round"], row["regime"])] += row["median_seconds"]
    print(f"\n{'build':22} {'regime':>9} {'mean total':>12} {'rounds':>8}")
    means = {}
    for (name, _), in [((b, 0),) for b, _ in builds]:
        pass
    for name, _ in builds:
        for regime in REGIMES:
            values = [v for (b, _, r), v in totals.items()
                      if b == name and r == regime]
            means[(name, regime)] = statistics.mean(values)
            print(f"{name:22} {regime:>9} {statistics.mean(values):11.2f}s "
                  f"{len(values):8}")
    for name, _ in builds[1:]:
        ratios = []
        for regime in REGIMES:
            ratios.append(means[(name, regime)] / means[(first, regime)])
        print(f"\n{name} against {first}: "
              + ", ".join(f"{r}={ratio:.4f}" for r, ratio in zip(REGIMES, ratios))
              + f", J={0.5 * sum(ratios):.4f}")


if __name__ == "__main__":
    main()
