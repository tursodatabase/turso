#!/usr/bin/env python3
"""Score cost vectors by the conformance tests they break.

Changing a cost default changes two kinds of test at once:

* Tests that quote the estimated rows and costs of a plan. Any change to any
  parameter changes those numbers, and the plan they describe can still be the
  same one. Updating them is mechanical.
* Tests that assert a plan shape, such as "this join uses a hash join" or
  "this query intersects two indexes". A vector that breaks one of those has
  changed what the optimizer does on a small table, which a TPC-H result does
  not justify on its own.

This script separates the two, so a search can keep the second kind at zero.
"""

import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cost_vector

# Every test in these files quotes estimated rows or costs, so every one of
# them changes when any parameter changes.
ESTIMATE_TEXT_FILES = (
    "turso-sqltests/explain-query-plan-json.sqltest",
    "sqlite-sqltests/join/memory.sqltest",
)

# Files whose tests state a capability the optimizer is meant to keep, such as
# "this join uses a hash join" or "this query intersects two indexes". A vector
# that breaks one of these has changed what the optimizer does on a small
# table, which a TPC-H result does not justify on its own.
CAPABILITY_FILES = (
    "sqlite-sqltests/join/hash.sqltest",
    "sqlite-sqltests/join/hash_join_order_by.sqltest",
    "sqlite-sqltests/multi_index_intersection.sqltest",
)

FAILURE_LINE = re.compile(r"^── (?P<name>.+?) \((?P<file>[^)]+)\)")
SUMMARY_LINE = re.compile(r"^\s*(?P<passed>\d+) passed(?:, (?P<failed>\d+) failed)?")


def run_conformance(runner, conformance_dir, vector_path, corpora):
    environment = dict(os.environ)
    if vector_path is None:
        environment.pop("TURSO_OPTIMIZER_PARAMS", None)
    else:
        environment["TURSO_OPTIMIZER_PARAMS"] = vector_path
    result = subprocess.run(
        [runner, "run", *corpora, "--backend", "rust"],
        cwd=conformance_dir, capture_output=True, env=environment,
    )
    text = re.sub(
        r"\x1b\[[0-9;]*m", "",
        result.stdout.decode("utf-8", "replace")
        + result.stderr.decode("utf-8", "replace"),
    )
    failures = []
    lines = text.splitlines()
    for position, line in enumerate(lines):
        match = FAILURE_LINE.match(line)
        if match:
            # The two lines after the header say how the test failed: a
            # recorded snapshot that no longer matches, or an assertion about
            # the output that no longer holds.
            following = " ".join(lines[position + 1: position + 3])
            kind = ("snapshot" if "Snapshot mismatch" in following
                    else "assertion")
            failures.append((match.group("name"), match.group("file"), kind))
    passed = failed = None
    for line in text.splitlines():
        match = SUMMARY_LINE.match(line)
        if match:
            passed = int(match.group("passed"))
            failed = int(match.group("failed") or 0)
    if passed is None:
        raise RuntimeError(
            "the conformance runner printed no summary; "
            f"exit={result.returncode}, last output: {text[-400:]}"
        )
    plan_shape = [f for f in failures if f[1] not in ESTIMATE_TEXT_FILES]
    estimate_text = [f for f in failures if f[1] in ESTIMATE_TEXT_FILES]
    capability = [f for f in failures if f[1] in CAPABILITY_FILES]
    return {
        "passed": passed, "failed": failed,
        "plan_shape_failures": plan_shape,
        "estimate_text_failures": estimate_text,
        "capability_failures": capability,
    }


def honours_the_override(runner, conformance_dir, corpora):
    """Make sure the runner was built with the override feature.

    A build without `turso_core/optimizer_params` ignores
    `TURSO_OPTIMIZER_PARAMS` and answers with its compiled defaults, so every
    vector would look the same and the run would say nothing. Two vectors that
    must select different plans are tried, and the check fails when they give
    the same result.
    """
    import tempfile
    probes = []
    for selectivity in (0.5, 0.001):
        vector = dict(cost_vector.DEFAULTS)
        vector["sel_eq_unindexed"] = selectivity
        vector["sel_eq_indexed"] = min(vector["sel_eq_indexed"], selectivity)
        vector["rows_per_table_fallback"] = 1e6 if selectivity == 0.5 else 5e7
        vector["cpu_cost_per_row"] = 0.003 if selectivity == 0.5 else 0.3
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, encoding="utf-8"
        )
        handle.close()
        cost_vector.write(handle.name, vector)
        probes.append(handle.name)
    results = [run_conformance(runner, conformance_dir, p, corpora)
               for p in probes]
    for path in probes:
        os.unlink(path)
    return results[0]["failed"] != results[1]["failed"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--conformance-dir", required=True)
    parser.add_argument("--vectors", nargs="+", required=True)
    parser.add_argument("--corpora", nargs="+",
                        default=["sqlite-sqltests", "turso-sqltests"])
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--out")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not honours_the_override(args.runner, args.conformance_dir, args.corpora):
        raise SystemExit(
            f"{args.runner} ignores TURSO_OPTIMIZER_PARAMS: two very different "
            "vectors gave the same result. Rebuild it with\n"
            "  cargo build --release -p sqltest -p turso_core "
            "--features turso_core/optimizer_params"
        )

    def check(path):
        cost_vector.read(path)
        return path, run_conformance(args.runner, args.conformance_dir,
                                     os.path.abspath(path), args.corpora)

    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for path, outcome in pool.map(check, args.vectors):
            name = os.path.splitext(os.path.basename(path))[0]
            rows.append({"vector": path, "name": name, **outcome})
            print(f"{name:34} passed={outcome['passed']:6} "
                  f"failed={outcome['failed']:3} "
                  f"capability={len(outcome['capability_failures']):3} "
                  f"plan-shape={len(outcome['plan_shape_failures']):3} "
                  f"estimate-text={len(outcome['estimate_text_failures']):3}",
                  flush=True)
            if args.verbose:
                for test, source, kind in outcome["plan_shape_failures"][:12]:
                    print(f"      [{kind}] {test}  ({source})")
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
