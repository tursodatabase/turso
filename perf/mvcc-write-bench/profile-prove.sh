#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
OUT_DIR="${OUT_DIR:-perf/mvcc-write-bench/profile/prove}"
mkdir -p "$OUT_DIR"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="${BIN:-$TARGET_DIR/bench-profile/mvcc-write-bench}"
ISOLATE="${ISOLATE:-perf/mvcc-write-bench/isolate.sh}"

run_cell() {
    local name="$1"
    shift
    local args=(--batch 100 --duration 5s --warmup 0s --repeats 1 --out "$OUT_DIR/${name}.csv" --path "$OUT_DIR/${name}.db")
    echo "=== $name syscalls ==="
    "$ISOLATE" --cpus 0-3 --drop-caches -- \
        perf trace -s -o "$OUT_DIR/${name}.syscalls" -- \
        "$BIN" "$@" "${args[@]}" >/dev/null
    echo "=== $name perfstat ==="
    "$ISOLATE" --cpus 0-3 --drop-caches -- \
        perf stat -o "$OUT_DIR/${name}.perfstat" -- \
        "$BIN" "$@" "${args[@]}" >/dev/null
}

run_cell sqlite --engine sqlite
run_cell turso-nockpt --engine turso --checkpoint truncate --topology coop --workers 8 --threshold disabled
echo "wrote $OUT_DIR"
