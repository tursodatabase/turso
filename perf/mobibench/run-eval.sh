#!/bin/bash
#
# Mobibench evaluation: SQLite vs Turso
#
# Measures Insert/Update/Delete throughput (TPS) for both systems
# under WAL journal mode + FULL synchronous — the recommended
# production configuration.
#
# Output: plot/results.csv (one row per run)

set -euo pipefail

MOBIBENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
SQLITE_BIN="$MOBIBENCH_DIR/Mobibench/shell/mobibench-sqlite3"
TURSO_BIN="$MOBIBENCH_DIR/Mobibench/shell/mobibench-turso"
DATA_DIR="$MOBIBENCH_DIR/mobibench-data"
RESULTS="$MOBIBENCH_DIR/plot/results.csv"
RUNS=${RUNS:-5}

for bin in "$SQLITE_BIN" "$TURSO_BIN"; do
    if [ ! -x "$bin" ]; then
        echo "ERROR: $bin not found. Build it first (see README.md)."
        exit 1
    fi
done

extract_tps() {
    grep "Transactions/sec" | sed 's/.*\t//' | awk '{print $1}'
}

run_one() {
    local bin="$1"
    local db_mode="$2"  # -d: 0=insert, 1=update, 2=delete

    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    "$bin" -p "$DATA_DIR" -f 1024 -r 4 -a 0 -y 2 -t 1 \
           -d "$db_mode" -n 1000 -j 3 -s 2 \
           -T 1 -D 1 -q 2>&1 | extract_tps
}

echo "system,operation,run,tps" > "$RESULTS"

for dmode in 0 1 2; do
    case $dmode in
        0) label="Insert" ;;
        1) label="Update" ;;
        2) label="Delete" ;;
    esac

    for system in sqlite turso; do
        if [ "$system" = "sqlite" ]; then
            bin="$SQLITE_BIN"
            sys_label="SQLite"
        else
            bin="$TURSO_BIN"
            sys_label="Turso"
        fi

        for run in $(seq 1 "$RUNS"); do
            echo -n "  $sys_label $label run $run/$RUNS ... "
            tps=$(run_one "$bin" "$dmode")
            echo "$sys_label,$label,$run,$tps" >> "$RESULTS"
            echo "${tps} TPS"
        done
    done
done

echo ""
echo "Results written to $RESULTS"
rm -rf "$DATA_DIR"
