#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
export TMPDIR="${TMPDIR:-$ROOT/.tmp}"
mkdir -p "$TMPDIR"

CELL="${CELL:-workers}"
case "$CELL" in
    workers|threads) ;;
    *)
        echo "CELL must be workers or threads, got: $CELL" >&2
        exit 1
        ;;
esac

OUT_DIR="${OUT_DIR:-$ROOT/perf/throughput/plot/out}"
TIMELINE_DIR="${TIMELINE_DIR:-$OUT_DIR/ecdf}"
mkdir -p "$OUT_DIR" "$TIMELINE_DIR"
CSV="$OUT_DIR/write-scaling.csv"
SIDECAR="$OUT_DIR/write-throughput-sqlite.csv"
BIN="${BIN:-$CARGO_TARGET_DIR/bench-profile/mvcc-write-bench}"
SQLITE_BIN="${SQLITE_BIN:-$CARGO_TARGET_DIR/bench-profile/write-throughput-sqlite}"
ISOLATE="${ISOLATE:-perf/mvcc-write-bench/isolate.sh}"
DURATION="${DURATION:-5s}"
WARMUP="${WARMUP:-1s}"
REPEATS="${REPEATS:-3}"
BATCH="${BATCH:-100}"
THRESHOLD="${THRESHOLD:-disabled}"
SQLITE_ITERS="${SQLITE_ITERS:-3000}"

if [[ ! -x "$BIN" ]]; then
    cargo build --profile bench-profile -p mvcc-write-bench
    BIN="$CARGO_TARGET_DIR/bench-profile/mvcc-write-bench"
fi

run_isolated() {
    if [[ -x "$ISOLATE" ]]; then
        "$ISOLATE" --cpus 0-3 --drop-caches -- "$@"
    elif command -v taskset >/dev/null; then
        taskset -c 0-3 "$@"
    else
        "$@"
    fi
}

timeline_args=()
if "$BIN" --help 2>/dev/null | grep -q -- '--timeline-dir'; then
    timeline_args+=(--timeline-dir "$TIMELINE_DIR")
fi

header_written=0
run_one() {
    local tmp
    tmp="$(mktemp -p "$OUT_DIR")"
    echo "=== $* $(date -Is) ===" >&2
    run_isolated "$BIN" "$@" --out "$tmp" "${timeline_args[@]}"
    if [[ "$header_written" -eq 0 ]]; then
        cat "$tmp" >"$CSV"
        header_written=1
    else
        tail -n +2 "$tmp" >>"$CSV"
    fi
    rm -f "$tmp"
}

turso_pair() {
    local stem="$1"
    shift
    local ckpt
    for ckpt in truncate passive; do
        run_one --engine turso --checkpoint "$ckpt" "$@" \
            --path "$OUT_DIR/turso-${ckpt}-${stem}.db"
        rm -f "$OUT_DIR/turso-${ckpt}-${stem}.db" \
            "$OUT_DIR/turso-${ckpt}-${stem}.db-log" \
            "$OUT_DIR/turso-${ckpt}-${stem}.db-wal" \
            "$OUT_DIR/turso-${ckpt}-${stem}.db-shm"
    done
}

common=(--batch "$BATCH" --threshold "$THRESHOLD" --duration "$DURATION" \
    --warmup "$WARMUP" --repeats "$REPEATS")

run_one --engine sqlite "${common[@]}" --path "$OUT_DIR/sqlite.db"
rm -f "$OUT_DIR/sqlite.db" "$OUT_DIR/sqlite.db-wal" "$OUT_DIR/sqlite.db-shm"

x_flag=workers
if [[ "$CELL" == "workers" ]]; then
    for n in 1 2 3 4; do
        turso_pair "w${n}" --topology io-pump --workers "$n" "${common[@]}"
    done
else
    x_flag=threads
    for n in 1 2 3 4; do
        turso_pair "t${n}" --topology threads-pump --threads "$n" \
            --workers-per-thread 1 "${common[@]}"
    done
    if [[ -f "$ROOT/perf/throughput/rusqlite/Cargo.toml" ]]; then
        if [[ ! -x "$SQLITE_BIN" ]]; then
            cargo build --profile bench-profile -p write-throughput-sqlite
            SQLITE_BIN="$CARGO_TARGET_DIR/bench-profile/write-throughput-sqlite"
        fi
        echo "source,threads,batch_size,compute,rows_per_sec,repeat" >"$SIDECAR"
        for n in 1 2 3 4; do
            for repeat in $(seq 0 $((REPEATS - 1))); do
                echo "=== write-throughput-sqlite threads=$n repeat=$repeat $(date -Is) ===" >&2
                (
                    cd "$OUT_DIR"
                    rm -f write_throughput_test.db write_throughput_test.db-wal \
                        write_throughput_test.db-shm
                    line="$(run_isolated "$SQLITE_BIN" --threads "$n" --batch-size "$BATCH" \
                        --compute 0 -i "$SQLITE_ITERS")"
                    rm -f write_throughput_test.db write_throughput_test.db-wal \
                        write_throughput_test.db-shm
                    thr="$(echo "$line" | awk -F, '{print $5}')"
                    echo "write-throughput-sqlite,$n,$BATCH,0,$thr,$repeat"
                ) >>"$SIDECAR"
            done
        done
    fi
fi

plot_py() {
    local script="$1"
    shift
    if command -v uv >/dev/null; then
        uv run --python 3.13 --with matplotlib -- "$script" "$@"
    else
        python3 "$script" "$@"
    fi
}

PLOT="$(dirname "$0")/plot"
plot_py "$PLOT/plot-throughput-scaling.py" --x "$x_flag" "$CSV" \
    "$OUT_DIR/throughput-scaling.png"
plot_py "$PLOT/plot-txn-latency-ecdf.py" "$TIMELINE_DIR" \
    "$OUT_DIR/txn-latency-ecdf.png"
echo "wrote $CSV $OUT_DIR/throughput-scaling.png"
