"""Prints a CSV comparison of the pull times of the base and pr variants.

Usage: python3 summarize_pull_bench.py <directory with <case>.<variant>.out files>

Each .out file holds the CSV lines that large_update_pull_scaling prints:
case,rows,repeat,checkpoint_policy,remote_update_seconds,pull_seconds,...
When a directory holds several runs of one (case, variant), for example
<case>.<variant>.r1.out, the script prints the median of the runs.
"""

import glob
import os
import statistics
import sys

CASES = ["indexed", "generated_indexed", "plain", "generated"]
POLICIES = ["automatic", "after_pull"]


def main() -> None:
    directory = sys.argv[1]
    pulls: dict[tuple[str, int, str, str], list[float]] = {}
    for path in glob.glob(os.path.join(directory, "*.out")):
        parts = os.path.basename(path).split(".")
        case, variant = parts[0], parts[1]
        with open(path) as f:
            for line in f:
                fields = line.strip().split(",")
                if len(fields) == 11 and fields[0] == case:
                    key = (case, int(fields[1]), fields[3], variant)
                    pulls.setdefault(key, []).append(float(fields[5]))
    print("case,rows,policy,runs,base_pull_s,pr_pull_s,speedup")
    sizes = sorted({key[1] for key in pulls})
    for case in CASES:
        for size in sizes:
            for policy in POLICIES:
                base = pulls.get((case, size, policy, "base"))
                pr = pulls.get((case, size, policy, "pr"))
                if not base or not pr:
                    continue
                b = statistics.median(base)
                p = statistics.median(pr)
                print(f"{case},{size},{policy},{len(base)},{b:.3f},{p:.3f},{b / p:.1f}")


if __name__ == "__main__":
    main()
