#!/usr/bin/env python3
"""Compare two cost-parameter sets over the benchmark queries.

The two sets run one after the other on each query, so a machine that gets
slower during the run slows both sides by the same amount. Every query keeps
its fastest run.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bench
import space


def load_params(path):
    if path in (None, "", "default"):
        return dict(space.DEFAULTS)
    return space.repair({**space.DEFAULTS, **json.load(open(path))})


def compare(binary, queries, before_path, after_path, repeats, timeout):
    rows = {}
    for query in queries:
        best = {"before": None, "after": None}
        bench.run_once(binary, query, before_path, timeout)
        bench.run_once(binary, query, after_path, timeout)
        for _ in range(repeats):
            for side, params_path in (("before", before_path), ("after", after_path)):
                sample = bench.run_once(binary, query, params_path, timeout)
                if sample is None:
                    continue
                if best[side] is None or sample < best[side]:
                    best[side] = sample
        rows[query.key] = best
        print(
            f"{query.key:18s} before {_show(best['before'])} after {_show(best['after'])}",
            flush=True,
        )
    return rows


def _show(value):
    return "     FAIL" if value is None else f"{value:9.1f}"


def summarize(rows):
    suites = {}
    for key, best in rows.items():
        suite = key.split("/")[0]
        totals = suites.setdefault(suite, {"before": 0.0, "after": 0.0, "queries": 0})
        if best["before"] is None or best["after"] is None:
            continue
        totals["before"] += best["before"]
        totals["after"] += best["after"]
        totals["queries"] += 1
    return suites


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default=bench.DEFAULT_BINARY)
    parser.add_argument("--before", default="default")
    parser.add_argument("--after", required=True)
    parser.add_argument("--suites", default="tpch,clickbench")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--work-dir", default="/tmp")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    before_path = bench.write_params(
        load_params(args.before), os.path.join(args.work_dir, "compare-before.json")
    )
    after_path = bench.write_params(
        load_params(args.after), os.path.join(args.work_dir, "compare-after.json")
    )
    queries = bench.load_workload(tuple(args.suites.split(",")))
    rows = compare(args.binary, queries, before_path, after_path, args.repeats, args.timeout)
    suites = summarize(rows)

    print()
    for suite, totals in suites.items():
        before, after = totals["before"] / 1000, totals["after"] / 1000
        change = (after / before - 1.0) * 100 if before else 0.0
        print(f"{suite:12s} {before:8.2f} s -> {after:8.2f} s  ({change:+.1f}%)")
    json.dump({"rows": rows, "suites": suites}, open(args.out, "w"), indent=2)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
