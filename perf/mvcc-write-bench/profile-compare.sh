#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-perf/mvcc-write-bench/profile}"
mkdir -p "$OUT_DIR"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="${BIN:-$TARGET_DIR/bench-profile/mvcc-write-bench}"
ISOLATE="${ISOLATE:-perf/mvcc-write-bench/isolate.sh}"
BT="${BT:-perf/mvcc-write-bench/profile-io.bt}"
DURATION="${DURATION:-5s}"
BATCH="${BATCH:-100}"

if [[ ! -x "$BIN" ]]; then
    cargo build --profile bench-profile -p mvcc-write-bench
    BIN="$TARGET_DIR/bench-profile/mvcc-write-bench"
fi

run_cell() {
    local name="$1"
    shift
    local db="$OUT_DIR/${name}.db"
    local csv="$OUT_DIR/${name}.csv"
    local args=(--batch "$BATCH" --duration "$DURATION" --warmup 0s --repeats 1 --out "$csv" --path "$db")

    echo "=== $name syscall-summary ==="
    "$ISOLATE" --cpus 0-3 --drop-caches -- \
        perf trace -s -o "$OUT_DIR/${name}.syscalls" -- \
        "$BIN" "$@" "${args[@]}" >/dev/null

    echo "=== $name perf-stat ==="
    "$ISOLATE" --cpus 0-3 --drop-caches -- \
        perf stat -o "$OUT_DIR/${name}.perfstat" -- \
        "$BIN" "$@" "${args[@]}" >/dev/null

    echo "=== $name bpftrace ==="
    bpftrace -o "$OUT_DIR/${name}.bpf" "$BT" &
    local bpid=$!
    sleep 1
    "$ISOLATE" --cpus 0-3 --drop-caches -- \
        "$BIN" "$@" "${args[@]}" >/dev/null
    kill -INT "$bpid" 2>/dev/null || true
    wait "$bpid" 2>/dev/null || true

    echo "=== $name perf-record ==="
    "$ISOLATE" --cpus 0-3 --drop-caches -- \
        perf record --call-graph dwarf -F 999 -o "$OUT_DIR/${name}.perf.data" -- \
        "$BIN" "$@" "${args[@]}" >/dev/null
    perf report --stdio --no-children -n --sort=overhead -i "$OUT_DIR/${name}.perf.data" \
        >"$OUT_DIR/${name}.top.txt" || true
    perf report --stdio -g folded,0.5,callee -i "$OUT_DIR/${name}.perf.data" \
        | head -n 80 >"$OUT_DIR/${name}.folded-head.txt" || true
}

run_cell sqlite --engine sqlite
run_cell turso-coop8-truncate --engine turso --checkpoint truncate --topology coop --workers 8
run_cell turso-coop8-nockpt --engine turso --checkpoint truncate --topology coop --workers 8 --threshold disabled

echo "wrote $OUT_DIR"
