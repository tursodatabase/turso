#!/usr/bin/env python3
"""Replay a differential-fuzzer test.sql against tursodb and the system
sqlite3, diffing their outputs. The fuzzer's bundled SQLite can lag the
current release (e.g. JSON real formatting), so replaying against a modern
sqlite3 separates oracle-version artifacts from real divergences.

Usage: scripts/replay_fuzz_sql.py [simulator-output/test.sql]
"""

import subprocess
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "simulator-output/test.sql"
lines = ["ATTACH ':memory:' AS aux;"]
for raw in open(path):
    line = raw.rstrip("\n")
    # `-- FAILED:` / `-- ERROR:` prefix comments out the statement that
    # tripped the oracle; strip the prefix so the replay executes it.
    for prefix in ("-- FAILED: ", "-- ERROR: "):
        if line.startswith(prefix):
            line = line[len(prefix):]
    lines.append(line)
script = "\n".join(lines)

def run(cmd):
    out = subprocess.run(
        cmd, input=script.encode(), capture_output=True, timeout=600
    )
    return out.stdout.decode("utf-8", errors="replace")

turso = run(["target/debug/tursodb", "-q", "-m", "list", ":memory:"])
sqlite = run(["sqlite3", ":memory:"])
if turso == sqlite:
    print("IDENTICAL output against system sqlite3")
    sys.exit(0)
tl, sl = turso.splitlines(), sqlite.splitlines()
print(f"outputs differ: turso {len(tl)} lines, sqlite3 {len(sl)} lines")
import difflib
for d in list(difflib.unified_diff(sl, tl, "sqlite3", "turso", lineterm=""))[:60]:
    print(d)
sys.exit(1)
