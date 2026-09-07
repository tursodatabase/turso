#!/usr/bin/env python3
"""Subtract two Callgrind self-cost tables and report cost per operation."""

import re
import sys
from collections import defaultdict
from pathlib import Path


ROW = re.compile(r"^\s*([0-9,]+)\s+\([^)]*\)\s+(.+)$")


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: callgrind-annotation-diff.py SMALL LARGE OPERATION_DIFFERENCE"
        )

    operations = int(sys.argv[3])
    if operations <= 0:
        raise SystemExit("OPERATION_DIFFERENCE must be positive")

    small = read_self_costs(Path(sys.argv[1]))
    large = read_self_costs(Path(sys.argv[2]))
    differences = []
    for function in small.keys() | large.keys():
        difference = large[function] - small[function]
        if difference > 0:
            differences.append((difference / operations, difference, function))

    differences.sort(reverse=True)
    print("instructions/op  instruction difference  function")
    for per_operation, difference, function in differences[:80]:
        print(f"{per_operation:15.2f}  {difference:22,d}  {function}")


def read_self_costs(path: Path) -> dict[str, int]:
    costs: dict[str, int] = defaultdict(int)
    in_function_table = False
    saw_row = False

    for line in path.read_text().splitlines():
        if line.rstrip().endswith("file:function"):
            in_function_table = True
            continue
        if not in_function_table:
            continue

        match = ROW.match(line)
        if match:
            saw_row = True
            costs[match.group(2)] += int(match.group(1).replace(",", ""))
        elif saw_row and not line.strip():
            break

    if not costs:
        raise SystemExit(f"no function self-cost table found in {path}")
    return costs


if __name__ == "__main__":
    main()
