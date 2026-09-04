#!/usr/bin/env python3
"""Print p50/p99.9 latency and mean throughput from the compare CSVs."""
import csv
import statistics
from pathlib import Path

LAT = Path("/mnt/nvme/run/compare/latency")
THR = Path("/mnt/nvme/run/compare/throughput")


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[i]


print("=== latency p50/p99.9 ms by engine, connections ===")
rows = {}
for path in sorted(LAT.glob("*-c*-r*.csv")):
    if "checkpoint" in path.name:
        continue
    with path.open() as f:
        for row in csv.DictReader(f):
            if row.get("warmup") in ("1", "true", "True"):
                continue
            key = (row["engine"], int(row["connections"]))
            rows.setdefault(key, []).append(float(row["total_ns"]) / 1e6)
for key in sorted(rows):
    xs = rows[key]
    print(
        f"{key[0]:16} c={key[1]:3} n={len(xs):7}  "
        f"p50={pct(xs, 50):8.3f}  p99.9={pct(xs, 99.9):8.3f}"
    )

print("=== throughput tx/s by engine, connections ===")
thr = {}
for path in sorted(THR.glob("*-result.csv")):
    with path.open() as f:
        for row in csv.DictReader(f):
            key = (row["engine"], int(row["connections"]))
            thr.setdefault(key, []).append(float(row["transactions_per_s"]))
for key in sorted(thr):
    xs = thr[key]
    mean = statistics.mean(xs)
    sd = statistics.pstdev(xs) if len(xs) > 1 else 0.0
    print(f"{key[0]:16} c={key[1]:3}  {mean:8.1f} ± {sd:.1f}  runs={xs}")
