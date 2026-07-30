#!/usr/bin/env python3
"""Exhaustive window-frame differential matrix: tursodb vs sqlite3.

Enumerates frame mode x bound pair x offsets x EXCLUDE x ORDER BY direction
over a set of fixed data shapes, runs the identical SQL through both engines,
and reports per-query divergences. Frames that SQLite rejects are part of the
matrix on purpose: both engines must agree on error-vs-success (message text
is not compared).

Usage:
  scripts/window_matrix_diff.py [--tursodb PATH] [--sqlite3 PATH] [--table NAME]

Divergences are printed with their SQL and written to
window-matrix-divergences.sql for minimization.
"""

import argparse
import re
import subprocess
import sys

MARKER = "===WQ"

# Data shapes. Window aggregates read the ORDER BY key itself so peer-tied
# rows are indistinguishable and tie order cannot cause false diffs.
TABLES = {
    # Distinct integer keys, single partition.
    "t_distinct": (
        "CREATE TABLE t_distinct(p INT, k INT);"
        "INSERT INTO t_distinct VALUES (0,1),(0,2),(0,3),(0,4),(0,5),(0,6),(0,7);"
    ),
    # Heavy duplicates: peer groups of sizes 3/1/2/3.
    "t_dups": (
        "CREATE TABLE t_dups(p INT, k INT);"
        "INSERT INTO t_dups VALUES (0,1),(0,1),(0,1),(0,3),(0,4),(0,4),(0,7),(0,7),(0,7);"
    ),
    # NULLs in the ORDER BY key.
    "t_nulls": (
        "CREATE TABLE t_nulls(p INT, k INT);"
        "INSERT INTO t_nulls VALUES (0,NULL),(0,1),(0,NULL),(0,2),(0,2),(0,NULL),(0,5);"
    ),
    # Text keys (RANGE offset arithmetic must pass them through unchanged).
    "t_text": (
        "CREATE TABLE t_text(p INT, k TEXT);"
        "INSERT INTO t_text VALUES (0,'a'),(0,'b'),(0,'b'),(0,'c'),(0,'x');"
    ),
    # REAL keys including fractional gaps.
    "t_real": (
        "CREATE TABLE t_real(p INT, k REAL);"
        "INSERT INTO t_real VALUES (0,0.5),(0,1.0),(0,1.5),(0,3.25),(0,100.0);"
    ),
    # Single row.
    "t_single": "CREATE TABLE t_single(p INT, k INT); INSERT INTO t_single VALUES (0,42);",
    # Two partitions, one of size 1.
    "t_parts": (
        "CREATE TABLE t_parts(p INT, k INT);"
        "INSERT INTO t_parts VALUES (1,1),(1,2),(1,2),(1,4),(2,9);"
    ),
    # Extreme integer keys (overflow of key +/- offset under RANGE).
    "t_extreme": (
        "CREATE TABLE t_extreme(p INT, k INT);"
        "INSERT INTO t_extreme VALUES (0,-9223372036854775808),(0,-5),(0,0),"
        "(0,5),(0,9223372036854775807);"
    ),
}

STARTS = [
    "UNBOUNDED PRECEDING",
    "0 PRECEDING",
    "1 PRECEDING",
    "2 PRECEDING",
    "100 PRECEDING",
    "CURRENT ROW",
    "0 FOLLOWING",
    "1 FOLLOWING",
    "2 FOLLOWING",
]

ENDS = [
    "2 PRECEDING",
    "1 PRECEDING",
    "0 PRECEDING",
    "CURRENT ROW",
    "0 FOLLOWING",
    "1 FOLLOWING",
    "2 FOLLOWING",
    "100 FOLLOWING",
    "UNBOUNDED FOLLOWING",
]

EXCLUDES = [
    "",
    " EXCLUDE NO OTHERS",
    " EXCLUDE CURRENT ROW",
    " EXCLUDE GROUP",
    " EXCLUDE TIES",
]

# The aggregated column is the ORDER BY key so peer ties are harmless.
# avg is printf-wrapped for stable float formatting.
FUNCS = (
    "sum(k) OVER w, count(*) OVER w, printf('%.4f', avg(k) OVER w), "
    "min(k) OVER w, max(k) OVER w, group_concat(k,',') OVER w, "
    "first_value(k) OVER w, last_value(k) OVER w, nth_value(k,2) OVER w"
)


def gen_queries(table):
    qs = []
    dirs = ["ASC", "DESC"]
    nulls_variants = [""]
    if table == "t_nulls":
        nulls_variants = ["", " NULLS FIRST", " NULLS LAST"]
    for mode in ("ROWS", "GROUPS", "RANGE"):
        for start in STARTS:
            for end in ENDS:
                for exclude in EXCLUDES:
                    for d in dirs:
                        for nv in nulls_variants:
                            frame = f"{mode} BETWEEN {start} AND {end}{exclude}"
                            qs.append(
                                f"SELECT p, k, {FUNCS} FROM {table} "
                                f"WINDOW w AS (PARTITION BY p ORDER BY k {d}{nv} {frame}) "
                                f"ORDER BY p, k {d}{nv}"
                            )
    # No-ORDER-BY variants: modes degrade to one whole-partition peer group.
    for mode in ("ROWS", "GROUPS", "RANGE"):
        for exclude in EXCLUDES:
            frame = f"{mode} BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING{exclude}"
            qs.append(
                f"SELECT p, k, {FUNCS} FROM {table} "
                f"WINDOW w AS (PARTITION BY p {frame}) ORDER BY p, k"
            )
    return qs


def run_engine(cmd, setup, queries):
    """Feed setup + marker-separated queries; return list of per-query blocks."""
    script = [setup]
    for i, q in enumerate(queries):
        script.append(f"SELECT '{MARKER}{i}';")
        script.append(q + ";")
    script.append(f"SELECT '{MARKER}END';")
    out = subprocess.run(
        cmd,
        input="\n".join(script),
        capture_output=True,
        text=True,
        timeout=1800,
    )
    combined = out.stdout
    # Both engines print errors to stderr as they occur; stderr lines lose
    # their position relative to stdout markers, so instead of interleaving
    # we detect missing output blocks below and re-run those queries
    # individually to classify them as errors.
    blocks = {}
    current = None
    for line in combined.splitlines():
        m = re.fullmatch(rf"{MARKER}(\d+|END)", line.strip())
        if m:
            current = m.group(1)
            if current != "END":
                blocks[int(current)] = []
            continue
        if current is not None and current != "END":
            blocks[int(current)].append(line.rstrip())
    return blocks


def is_error_line(line):
    return "error" in line.lower() and ("×" in line or "Error" in line or "error:" in line)


def normalize_block(lines):
    """None for empty/missing blocks (needs solo classification — the error,
    if any, went to stderr); otherwise (status, rows-before-error). tursodb
    prints errors in-stream so its blocks classify directly; a query can
    legitimately emit rows and then fail mid-stream (e.g. sum() integer
    overflow), so the row prefix is part of the comparison."""
    if not lines:
        return None
    rows = []
    for l in (x.strip() for x in lines):
        if not l:
            continue
        if is_error_line(l):
            return ("error", tuple(rows))
        rows.append(l)
    if not rows:
        return None
    return ("ok", tuple(rows))


def classify(cmd, setup, query):
    """Run one query alone; return (status, rows-before-error)."""
    out = subprocess.run(
        cmd,
        input=f"{setup}\n{query};",
        capture_output=True,
        text=True,
        timeout=120,
    )
    rows = tuple(
        l.strip() for l in out.stdout.splitlines() if l.strip() and not is_error_line(l)
    )
    errored = (
        "error" in out.stderr.lower()
        or "×" in out.stderr
        or any(is_error_line(l) for l in out.stdout.splitlines())
    )
    return ("error" if errored else "ok", rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tursodb", default="target/debug/tursodb")
    ap.add_argument("--sqlite3", default="sqlite3")
    ap.add_argument("--table", default=None, help="run only this data shape")
    args = ap.parse_args()

    turso_cmd = [args.tursodb, "-q", "-m", "list", ":memory:"]
    sqlite_cmd = [args.sqlite3, ":memory:"]

    divergences = []
    total = 0
    tables = {args.table: TABLES[args.table]} if args.table else TABLES
    for table, setup in tables.items():
        queries = gen_queries(table)
        total += len(queries)
        tblocks = run_engine(turso_cmd, setup, queries)
        sblocks = run_engine(sqlite_cmd, setup, queries)
        for i, q in enumerate(queries):
            tres = normalize_block(tblocks.get(i))
            sres = normalize_block(sblocks.get(i))
            # An empty block is either an error whose text went to stderr
            # (sqlite3) or — impossible here, every query scans a non-empty
            # table unconditionally — zero rows. Disambiguate solo.
            if tres is None:
                tres = classify(turso_cmd, setup, q)
            if sres is None:
                sres = classify(sqlite_cmd, setup, q)
            if tres == sres:
                continue
            # A batch block can't see stderr, so a query that emitted rows
            # and then errored looks like a clean "ok" there. Reclassify
            # both sides solo before declaring a divergence.
            tres = classify(turso_cmd, setup, q)
            sres = classify(sqlite_cmd, setup, q)
            if tres == sres:
                continue
            if tres[0] == sres[0] == "error" and tres[1] == sres[1]:
                continue  # same rows, both rejected; message text not compared
            divergences.append((table, q, tres, sres))
        print(f"{table}: {len(queries)} queries checked", file=sys.stderr)

    print(f"\n{total} queries total, {len(divergences)} divergences")
    with open("window-matrix-divergences.sql", "w") as f:
        for table, q, tres, sres in divergences:
            f.write(f"-- table={table}\n-- turso={tres[0]} sqlite={sres[0]}\n{q};\n\n")
    for table, q, tres, sres in divergences[:20]:
        print(f"\n--- {table}: {q}")
        print(f"  turso : {tres[0]} {str(tres[1:])[:300]!r}")
        print(f"  sqlite: {sres[0]} {str(sres[1:])[:300]!r}")
    if len(divergences) > 20:
        print(f"... {len(divergences) - 20} more in window-matrix-divergences.sql")
    sys.exit(1 if divergences else 0)


if __name__ == "__main__":
    main()
