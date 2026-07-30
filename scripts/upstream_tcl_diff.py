#!/usr/bin/env python3
"""Differentially replay SQL mined from SQLite's Tcl test files.

Extracts the SQL blocks of `do_execsql_test` / `do_catchsql_test` cases
(in file order, so schema/data statements carry state forward), runs them
through tursodb and the system sqlite3, and reports blocks where the
engines disagree (rows before any error + errored-or-not; error text is
not compared). Tcl-generated cases (foreach/proc/string-map) are not
expanded — this mines the literal cases only.

Usage: scripts/upstream_tcl_diff.py ~/projects/sqlite/test/window1.test [...]
"""

import re
import subprocess
import sys

MARKER = "===UTQ"


def extract_blocks(path):
    """Yield (name, sql, is_catch) for each literal test case."""
    src = open(path, encoding="utf-8", errors="replace").read()
    pat = re.compile(r"do_(execsql|catchsql)_test\s+(\S+)\s*\{")
    blocks = []
    for m in pat.finditer(src):
        depth = 1
        i = m.end()
        while i < len(src) and depth > 0:
            if src[i] == "{":
                depth += 1
            elif src[i] == "}":
                depth -= 1
            i += 1
        sql = src[m.end() : i - 1]
        # Skip cases whose SQL embeds Tcl substitution — not literal SQL.
        if "$" in sql or "[" in sql.split("--")[0]:
            continue
        blocks.append((m.group(2), sql.strip(), m.group(1) == "catchsql"))
    return blocks


def run_engine(cmd, blocks):
    script = []
    for i, (_, sql, _) in enumerate(blocks):
        script.append(f"SELECT '{MARKER}{i}';")
        script.append(sql if sql.rstrip().endswith(";") else sql + ";")
    out = subprocess.run(
        cmd,
        input="\n".join(script).encode(),
        capture_output=True,
        timeout=1800,
    )
    text = out.stdout.decode("utf-8", errors="replace")
    parsed = {}
    cur = None
    for line in text.splitlines():
        m = re.fullmatch(rf"{MARKER}(\d+)", line.strip())
        if m:
            cur = int(m.group(1))
            parsed[cur] = []
        elif cur is not None:
            parsed[cur].append(line)
    return parsed


def error_count(stderr, stdout_lines):
    n = stderr.lower().count("error")
    n += sum(1 for l in stdout_lines if "error" in l.lower() and ("×" in l or "Error" in l))
    return n


def classify(cmd, prior_sql, sql):
    """Classify only the statement under test: prior statements may
    themselves error (in either engine), so the error signal is the
    *delta* between running the priors alone and priors + statement,
    and rows are read from after a marker emitted between them."""
    prior = "\n".join(prior_sql)
    base = subprocess.run(cmd, input=prior.encode(), capture_output=True, timeout=300)
    full = subprocess.run(
        cmd,
        input=f"{prior}\nSELECT '{MARKER}X';\n{sql};".encode(),
        capture_output=True,
        timeout=300,
    )
    base_stdout = base.stdout.decode("utf-8", errors="replace").splitlines()
    full_stdout = full.stdout.decode("utf-8", errors="replace").splitlines()
    tail = []
    seen_marker = False
    for l in full_stdout:
        if l.strip() == f"{MARKER}X":
            seen_marker = True
            continue
        if seen_marker:
            tail.append(l)
    base_errs = error_count(base.stderr.decode("utf-8", errors="replace"), base_stdout)
    full_errs = error_count(full.stderr.decode("utf-8", errors="replace"), full_stdout)
    errored = full_errs > base_errs
    rows = tuple(
        l.strip()
        for l in tail
        if l.strip() and not ("error" in l.lower() and ("×" in l or "Error" in l))
    )
    return ("error" if errored else "ok", rows)


def main():
    turso_cmd = ["target/debug/tursodb", "-q", "-m", "list", ":memory:"]
    sqlite_cmd = ["sqlite3", ":memory:"]
    total_blocks = 0
    total_diverge = 0
    for path in sys.argv[1:]:
        blocks = extract_blocks(path)
        total_blocks += len(blocks)
        tout = run_engine(turso_cmd, blocks)
        sout = run_engine(sqlite_cmd, blocks)
        diverged = []
        for i, (name, sql, _is_catch) in enumerate(blocks):
            tl = [l.strip() for l in tout.get(i, []) if l.strip()]
            sl = [l.strip() for l in sout.get(i, []) if l.strip()]
            terr = any("error" in l.lower() and ("×" in l or "Error" in l) for l in tl)
            trows = tuple(l for l in tl if not ("error" in l.lower() and ("×" in l or "Error" in l)))
            # sqlite3 errors go to stderr: an empty block could be either an
            # error or a legitimately empty result. Only investigate blocks
            # that already look different.
            if (terr and not sl) or (not terr and trows == tuple(sl)):
                continue
            # Re-run this block on both engines with all prior SQL as
            # state, classifying only the statement under test.
            prior = []
            for _, prior_sql, _ in blocks[:i]:
                prior.append(prior_sql if prior_sql.rstrip().endswith(";") else prior_sql + ";")
            tres = classify(turso_cmd, prior, sql)
            sres = classify(sqlite_cmd, prior, sql)
            if tres == sres or (tres[0] == "error" and sres[0] == "error"):
                continue
            diverged.append((name, sql, tres, sres))
        print(f"{path}: {len(blocks)} literal cases, {len(diverged)} divergences")
        for name, sql, tres, sres in diverged[:10]:
            print(f"  DIVERGE {name}: {sql[:160]!r}")
            print(f"    turso : {tres[0]} {str(tres[1])[:200]}")
            print(f"    sqlite: {sres[0]} {str(sres[1])[:200]}")
        total_diverge += len(diverged)
    print(f"\nTOTAL: {total_blocks} cases, {total_diverge} divergences")
    sys.exit(1 if total_diverge else 0)


if __name__ == "__main__":
    main()
