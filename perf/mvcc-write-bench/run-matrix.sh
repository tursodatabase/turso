#!/usr/bin/env bash
# Full matrix: batches 1, 10, 100. Throughput is committed rows/s.
# Every Turso cell is paired: truncate AND passive, same other knobs.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-perf/mvcc-write-bench/results}"
mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/write-throughput.csv"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="${BIN:-$TARGET_DIR/bench-profile/mvcc-write-bench}"
ISOLATE="${ISOLATE:-perf/mvcc-write-bench/isolate.sh}"
DURATION="${DURATION:-5s}"
WARMUP="${WARMUP:-1s}"
REPEATS="${REPEATS:-3}"

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

for batch in 1 10 100; do
    common=(--batch "$batch" --duration "$DURATION" --warmup "$WARMUP" --repeats "$REPEATS")
    run_one --engine sqlite "${common[@]}" --path "$OUT_DIR/sqlite-b${batch}.db"
    for workers in 1 4 8; do
        turso_pair --topology coop --workers "$workers" --threshold disabled "${common[@]}"
        turso_pair --topology io-pump --workers "$workers" --threshold disabled "${common[@]}"
    done
    turso_pair --topology threads-pump --threads 4 --workers-per-thread 1 \
        --threshold disabled "${common[@]}"
done

python3 perf/mvcc-write-bench/plot.py "$CSV" --out "$OUT_DIR"
echo "wrote $CSV"
