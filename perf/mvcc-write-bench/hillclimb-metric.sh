#!/usr/bin/env bash
# Frozen hillclimb cell: batch 100, 8 writers, synchronous=FULL, 5s x 3, isolated.
# Every Turso topology is run with truncate AND passive (same threshold).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-perf/mvcc-write-bench/results/hillclimb}"
mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/metric.csv"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="${BIN:-$TARGET_DIR/bench-profile/mvcc-write-bench}"
ISOLATE="${ISOLATE:-perf/mvcc-write-bench/isolate.sh}"
DURATION="${DURATION:-5s}"
WARMUP="${WARMUP:-1s}"
REPEATS="${REPEATS:-3}"
BATCH=100
WORKERS=8

if [[ ! -x "$BIN" ]]; then
    cargo build --profile bench-profile -p mvcc-write-bench
    BIN="$TARGET_DIR/bench-profile/mvcc-write-bench"
fi

header_written=0
run_one() {
    local tmp
    tmp="$(mktemp)"
    "$ISOLATE" --cpus 0-3 --drop-caches -- "$BIN" "$@" --out "$tmp"
    if [[ "$header_written" -eq 0 ]]; then
        cat "$tmp" >"$CSV"
        header_written=1
    else
        tail -n +2 "$tmp" >>"$CSV"
    fi
    rm -f "$tmp"
}

turso_pair() {
    local ckpt
    for ckpt in truncate passive; do
        run_one --engine turso --checkpoint "$ckpt" "$@" --path "$OUT_DIR/turso-${ckpt}.db"
    done
}

common=(--batch "$BATCH" --duration "$DURATION" --warmup "$WARMUP" --repeats "$REPEATS")

run_one --engine sqlite "${common[@]}" --path "$OUT_DIR/sqlite.db"

turso_pair --topology coop --workers "$WORKERS" --threshold disabled "${common[@]}"
turso_pair --topology io-pump --workers "$WORKERS" --threshold disabled "${common[@]}"
turso_pair --topology threads --threads "$WORKERS" --workers-per-thread 1 \
    --threshold disabled "${common[@]}"
turso_pair --topology threads-pump --threads "$WORKERS" --workers-per-thread 1 \
    --threshold disabled "${common[@]}"

python3 - <<'PY' "$CSV"
import csv, sys
from collections import defaultdict
from statistics import median

path = sys.argv[1]
by = defaultdict(list)
with open(path, newline="") as f:
    for row in csv.DictReader(f):
        elapsed = float(row["elapsed_secs"])
        tput = float(row["inserts"]) / elapsed if elapsed > 0 else 0.0
        key = (
            row["engine"],
            row["topology"],
            row.get("checkpoint") or "",
            row["workers"],
            row.get("threshold") or "",
        )
        by[key].append(tput)

print("engine topology checkpoint workers threshold median_rows_per_s")
best_turso = 0.0
sqlite = 0.0
for key, xs in sorted(by.items()):
    m = median(xs)
    print(f"{key[0]} {key[1]} {key[2]} {key[3]} {key[4]} {m:.1f}")
    if key[0] == "sqlite":
        sqlite = m
    else:
        best_turso = max(best_turso, m)
print(f"sqlite={sqlite:.1f} best_turso={best_turso:.1f} beat={best_turso > sqlite}")
PY

echo "wrote $CSV"
