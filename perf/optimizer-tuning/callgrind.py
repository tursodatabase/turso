#!/usr/bin/env python3
"""Count the instructions each benchmark query runs, with callgrind.

Wall time moves with whatever else the machine is doing. An instruction count
does not, so it shows a small change that timing noise would hide. Callgrind
runs the binary about 70 times slower, so give this script time.
"""

import argparse
import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bench

INSTRUCTIONS = re.compile(r"I\s+refs:\s+([0-9,]+)")


def count_instructions(binary, query, params_path, out_dir):
    out_file = os.path.join(out_dir, f"callgrind.{query.suite}.{query.name}.out")
    env = dict(os.environ)
    env["RUST_LOG"] = "off"
    if params_path is None:
        env.pop("TURSO_OPTIMIZER_PARAMS", None)
    else:
        env["TURSO_OPTIMIZER_PARAMS"] = params_path
    script = ";\n".join(bench.split_statements(query.sql)) + ";\n.quit\n"
    done = subprocess.run(
        [
            "valgrind",
            "--tool=callgrind",
            "--cache-sim=no",
            "--branch-sim=no",
            f"--callgrind-out-file={out_file}",
            binary,
            query.db_path,
            "--quiet",
            "--output-mode",
            "list",
        ],
        input=script,
        env=env,
        capture_output=True,
        text=True,
    )
    found = INSTRUCTIONS.search(done.stderr)
    if os.path.exists(out_file):
        os.remove(out_file)
    if not found:
        return None
    return int(found.group(1).replace(",", ""))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default=bench.DEFAULT_BINARY)
    parser.add_argument("--params", default=None, help="cost parameter JSON, or none for the compiled defaults")
    parser.add_argument("--suites", default="tpch,clickbench")
    parser.add_argument("--only", default=None, help="comma separated query keys")
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--work-dir", default="/tmp")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    queries = bench.load_workload(tuple(args.suites.split(",")))
    if args.only:
        wanted = set(args.only.split(","))
        queries = [q for q in queries if q.key in wanted]
    queries = [q for index, q in enumerate(queries) if index % args.shards == args.shard]

    results = {}
    for query in queries:
        count = count_instructions(args.binary, query, args.params, args.work_dir)
        results[query.key] = count
        print(f"{query.key:18s} {count if count is not None else 'FAIL'}", flush=True)
        json.dump(results, open(args.out, "w"), indent=2)


if __name__ == "__main__":
    main()
