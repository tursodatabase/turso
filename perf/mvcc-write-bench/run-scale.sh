#!/usr/bin/env bash
# Worker × batch matrix. Throughput is committed rows/s (inserts/elapsed).
# Batches 1, 10, 100. Every Turso cell: truncate AND passive.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-perf/mvcc-write-bench/results}"
mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/write-throughput.csv"
BIN="${BIN:-target/bench-profile/mvcc-write-bench}"
ISOLATE="${ISOLATE:-perf/mvcc-write-bench/isolate.sh}"
DURATION="${DURATION:-5s}"
WARMUP="${WARMUP:-1s}"
REPEATS="${REPEATS:-3}"

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

for batch in 1 10 100; do
    common=(--batch "$batch" --duration "$DURATION" --warmup "$WARMUP" --repeats "$REPEATS")
    run_one --engine sqlite "${common[@]}" --path "$OUT_DIR/sqlite-b${batch}.db"
    for workers in 1 4 8; do
        turso_pair --topology coop --workers "$workers" --threshold disabled "${common[@]}"
        turso_pair --topology io-pump --workers "$workers" --threshold disabled "${common[@]}"
    done
done

python3 perf/mvcc-write-bench/plot.py "$CSV" --out "$OUT_DIR"
echo "wrote $CSV"
