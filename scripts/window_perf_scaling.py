#!/usr/bin/env python3
"""Scaling-shape check for window-frame execution.

Debug builds make absolute times meaningless, so this measures how wall
time GROWS with row count per frame shape and compares the growth
exponent against the system sqlite3's shape on identical queries. A
shape that is super-linear in Turso but linear in SQLite is a blowup;
matching exponents (even at different constants) are fine.

Usage: scripts/window_perf_scaling.py
"""

import subprocess
import time

SIZES = [2_000, 8_000, 32_000]

SHAPES = {
    "default-range": "sum(v) OVER (PARTITION BY p ORDER BY k)",
    "rows-100-preceding-sum": "sum(v) OVER (PARTITION BY p ORDER BY k ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)",
    "rows-100-preceding-min": "min(v) OVER (PARTITION BY p ORDER BY k ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)",
    "groups-2-preceding": "count(*) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW)",
    "range-100-offset": "sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING)",
    "exclude-ties-fullscan": "sum(v) OVER (PARTITION BY p ORDER BY k ROWS BETWEEN 100 PRECEDING AND CURRENT ROW EXCLUDE TIES)",
    "first-value-cached": "first_value(v) OVER (PARTITION BY p ORDER BY k ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
}


def script_for(n, expr):
    return (
        "CREATE TABLE d(x);\n"
        "INSERT INTO d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);\n"
        "CREATE TABLE t(p INT, k INT, v INT);\n"
        "INSERT INTO t SELECT i % 4, i, (i * 7919) % 100000 FROM (\n"
        "  SELECT a.x*10000 + b.x*1000 + c.x*100 + e.x*10 + f.x AS i\n"
        "  FROM d a, d b, d c, d e, d f LIMIT {n}\n"
        ");\n"
        "SELECT count(*), sum(w) FROM (SELECT {expr} AS w FROM t);\n"
    ).format(n=n, expr=expr)


def run_timed(cmd, sql):
    start = time.monotonic()
    out = subprocess.run(cmd, input=sql.encode(), capture_output=True, timeout=900)
    elapsed = time.monotonic() - start
    if b"error" in out.stderr.lower() or b"\xc3\x97" in out.stdout:
        return None
    return elapsed


def main():
    turso_cmd = ["target/debug/tursodb", "-q", "-m", "list", ":memory:"]
    sqlite_cmd = ["sqlite3", ":memory:"]
    print(f"{'shape':34} {'engine':7} " + " ".join(f"{n:>9}" for n in SIZES) + "   growth")
    for shape, expr in SHAPES.items():
        for engine, cmd in [("turso", turso_cmd), ("sqlite", sqlite_cmd)]:
            times = []
            for n in SIZES:
                t = run_timed(cmd, script_for(n, expr))
                times.append(t)
            cells = " ".join(f"{t:9.3f}" if t else f"{'ERR':>9}" for t in times)
            if all(times) and times[0] > 0:
                # growth per 4x rows, geometric mean of the two steps;
                # ~4 = linear, ~16 = quadratic.
                g = ((times[1] / times[0]) * (times[2] / times[1])) ** 0.5
                growth = f"{g:5.1f}x per 4x rows"
            else:
                growth = "-"
            print(f"{shape:34} {engine:7} {cells}   {growth}")


if __name__ == "__main__":
    main()
