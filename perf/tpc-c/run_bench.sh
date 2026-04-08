#!/bin/bash
set -e

usage() {
    echo "Usage: $0 [warehouses] [connections] [warmup] [measure] [interval]"
    echo
    echo "  Runs TPC-C benchmark against both turso and system sqlite3,"
    echo "  then prints a comparison report."
    echo
    echo "  Defaults: 1 warehouse, 1 connection, 5s warmup, 30s measure, 1s interval"
    echo
    echo "  Can run standalone (clones turso automatically) or inside the turso repo"
    echo "  at e.g. perf/tpc-c/ (detects and uses the parent turso tree)."
    exit 1
}

[ "$1" = "--help" ] || [ "$1" = "-h" ] && usage

WAREHOUSES=${1:-1}
CONNECTIONS=${2:-1}
WARMUP=${3:-5}
MEASURE=${4:-30}
INTERVAL=${5:-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RESULTS_DIR="results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TURSO_LOG="$RESULTS_DIR/turso_${TIMESTAMP}.log"
SQLITE_LOG="$RESULTS_DIR/sqlite_${TIMESTAMP}.log"
REPORT="$RESULTS_DIR/report_${TIMESTAMP}.txt"

# ── Locate turso root ────────────────────────────────────────────────────────
# Priority: 1) TURSO_ROOT env var  2) walk up to find turso repo  3) clone

if [ -n "$TURSO_ROOT" ] && [ -f "$TURSO_ROOT/sqlite3/include/sqlite3.h" ]; then
    echo "==> Using turso at $TURSO_ROOT (from environment)"
else
    TURSO_ROOT=""
    dir="$SCRIPT_DIR"
    while [ "$dir" != "/" ]; do
        dir="$(dirname "$dir")"
        if [ -f "$dir/sqlite3/include/sqlite3.h" ] && [ -f "$dir/Cargo.toml" ]; then
            if grep -q '"sqlite3"' "$dir/Cargo.toml" 2>/dev/null; then
                TURSO_ROOT="$dir"
                break
            fi
        fi
    done

    if [ -n "$TURSO_ROOT" ]; then
        echo "==> Detected turso repo at $TURSO_ROOT"
    else
        TURSO_ROOT="$SCRIPT_DIR/tmp-turso"
        if [ ! -d "$TURSO_ROOT" ]; then
            echo "==> Cloning turso into tmp-turso..."
            git clone git@github.com:tursodatabase/turso.git "$TURSO_ROOT"
        fi
        echo "==> Using turso at $TURSO_ROOT"
    fi
fi

MAKE="make -C src TURSO_ROOT=$TURSO_ROOT"

create_db() {
    local dbfile="$1"
    rm -f "$dbfile" "${dbfile}-wal" "${dbfile}-shm" "${dbfile}-journal"
    sqlite3 "$dbfile" < create_table_sqlite.sql
    sqlite3 "$dbfile" < add_fkey_idx_sqlite.sql
}

# ── Build both sets of binaries ──────────────────────────────────────────────

echo "==> Building turso sqlite3 library (release)..."
(cd "$TURSO_ROOT" && cargo build -p turso_sqlite3 --release --quiet)

echo "==> Building tpcc (turso)..."
$MAKE clean -s
$MAKE -j all BACKEND=turso TURSO_PROFILE=release -s

echo "==> Building tpcc (sqlite)..."
$MAKE clean -s
$MAKE -j all BACKEND=sqlite -s

# ── Run SQLite benchmark ─────────────────────────────────────────────────────

echo ""
echo "==> Loading data (sqlite, ${WAREHOUSES} warehouse(s))..."
create_db tpcc-sqlite.db
./tpcc_load-sqlite -w "$WAREHOUSES"

echo "==> Running benchmark (sqlite): ${WAREHOUSES}W / ${CONNECTIONS}C / ${WARMUP}s warmup / ${MEASURE}s measure"
./tpcc_start-sqlite \
    -w "$WAREHOUSES" -c "$CONNECTIONS" -r "$WARMUP" -l "$MEASURE" -i "$INTERVAL" \
    2>&1 | tee "$SQLITE_LOG"

# ── Run Turso benchmark ──────────────────────────────────────────────────────

echo ""
echo "==> Loading data (turso, ${WAREHOUSES} warehouse(s))..."
create_db tpcc.db
./tpcc_load -w "$WAREHOUSES"

echo "==> Running benchmark (turso): ${WAREHOUSES}W / ${CONNECTIONS}C / ${WARMUP}s warmup / ${MEASURE}s measure"
./tpcc_start \
    -w "$WAREHOUSES" -c "$CONNECTIONS" -r "$WARMUP" -l "$MEASURE" -i "$INTERVAL" \
    2>&1 | tee "$TURSO_LOG"

# ── Generate report ──────────────────────────────────────────────────────────

parse_results() {
    local log="$1"
    local tpmc avg_new avg_pay avg_ord avg_del avg_slev
    tpmc=$(grep -oP '[\d.]+(?= TpmC)' "$log" | tail -1)
    avg_new=$(grep '  \[0\] sc:' "$log" | head -1 | grep -oP 'avg_rt: \K[\d.]+')
    avg_pay=$(grep '  \[1\] sc:' "$log" | head -1 | grep -oP 'avg_rt: \K[\d.]+')
    avg_ord=$(grep '  \[2\] sc:' "$log" | head -1 | grep -oP 'avg_rt: \K[\d.]+')
    avg_del=$(grep '  \[3\] sc:' "$log" | head -1 | grep -oP 'avg_rt: \K[\d.]+')
    avg_slev=$(grep '  \[4\] sc:' "$log" | head -1 | grep -oP 'avg_rt: \K[\d.]+')
    local sc_new lt_new sc_pay lt_pay sc_ord lt_ord sc_del lt_del sc_slev lt_slev
    sc_new=$(grep '  \[0\] sc:' "$log" | head -1 | grep -oP 'sc:\K\d+')
    lt_new=$(grep '  \[0\] sc:' "$log" | head -1 | grep -oP 'lt:\K\d+')
    sc_pay=$(grep '  \[1\] sc:' "$log" | head -1 | grep -oP 'sc:\K\d+')
    lt_pay=$(grep '  \[1\] sc:' "$log" | head -1 | grep -oP 'lt:\K\d+')
    sc_ord=$(grep '  \[2\] sc:' "$log" | head -1 | grep -oP 'sc:\K\d+')
    lt_ord=$(grep '  \[2\] sc:' "$log" | head -1 | grep -oP 'lt:\K\d+')
    sc_del=$(grep '  \[3\] sc:' "$log" | head -1 | grep -oP 'sc:\K\d+')
    lt_del=$(grep '  \[3\] sc:' "$log" | head -1 | grep -oP 'lt:\K\d+')
    sc_slev=$(grep '  \[4\] sc:' "$log" | head -1 | grep -oP 'sc:\K\d+')
    lt_slev=$(grep '  \[4\] sc:' "$log" | head -1 | grep -oP 'lt:\K\d+')
    local time_taken
    time_taken=$(grep -oP '[\d.]+(?= seconds)' "$log" | tail -1)
    echo "$tpmc|$avg_new|$avg_pay|$avg_ord|$avg_del|$avg_slev|$sc_new|$lt_new|$sc_pay|$lt_pay|$sc_ord|$lt_ord|$sc_del|$lt_del|$sc_slev|$lt_slev|$time_taken"
}

IFS='|' read -r S_TPMC S_RT0 S_RT1 S_RT2 S_RT3 S_RT4 S_SC0 S_LT0 S_SC1 S_LT1 S_SC2 S_LT2 S_SC3 S_LT3 S_SC4 S_LT4 S_TIME <<< "$(parse_results "$SQLITE_LOG")"
IFS='|' read -r T_TPMC T_RT0 T_RT1 T_RT2 T_RT3 T_RT4 T_SC0 T_LT0 T_SC1 T_LT1 T_SC2 T_LT2 T_SC3 T_LT3 T_SC4 T_LT4 T_TIME <<< "$(parse_results "$TURSO_LOG")"

S_TOTAL=$(( ${S_SC0:-0} + ${S_LT0:-0} + ${S_SC1:-0} + ${S_LT1:-0} + ${S_SC2:-0} + ${S_LT2:-0} + ${S_SC3:-0} + ${S_LT3:-0} + ${S_SC4:-0} + ${S_LT4:-0} ))
T_TOTAL=$(( ${T_SC0:-0} + ${T_LT0:-0} + ${T_SC1:-0} + ${T_LT1:-0} + ${T_SC2:-0} + ${T_LT2:-0} + ${T_SC3:-0} + ${T_LT3:-0} + ${T_SC4:-0} + ${T_LT4:-0} ))

if [ -n "$S_TPMC" ] && [ -n "$T_TPMC" ] && [ "$S_TPMC" != "0" ] && [ "$S_TPMC" != "0.000" ]; then
    RATIO=$(awk "BEGIN { printf \"%.2f\", $T_TPMC / $S_TPMC }")
    if awk "BEGIN { exit !($T_TPMC > $S_TPMC) }"; then
        WINNER="turso"
        PCT=$(awk "BEGIN { printf \"%.1f\", (($T_TPMC / $S_TPMC) - 1) * 100 }")
    else
        WINNER="sqlite"
        PCT=$(awk "BEGIN { printf \"%.1f\", (($S_TPMC / $T_TPMC) - 1) * 100 }")
    fi
else
    RATIO="N/A"
    WINNER="N/A"
    PCT="N/A"
fi

{
cat <<EOF
================================================================================
                        TPC-C Benchmark Comparison Report
================================================================================
Date:         $(date)
Turso:        $TURSO_ROOT
Warehouses:   $WAREHOUSES
Connections:  $CONNECTIONS
Warmup:       ${WARMUP}s
Measurement:  ${MEASURE}s

--------------------------------------------------------------------------------
  THROUGHPUT (TpmC = New-Order transactions per minute)
--------------------------------------------------------------------------------
                        SQLite              Turso
  TpmC                  $(printf "%-20s" "${S_TPMC:-N/A}") ${T_TPMC:-N/A}
  Total transactions    $(printf "%-20s" "$S_TOTAL") $T_TOTAL
  Wall time (s)         $(printf "%-20s" "${S_TIME:-N/A}") ${T_TIME:-N/A}

--------------------------------------------------------------------------------
  AVERAGE RESPONSE TIME (ms)
--------------------------------------------------------------------------------
  Transaction           SQLite              Turso
  New-Order             $(printf "%-20s" "${S_RT0:-N/A}") ${T_RT0:-N/A}
  Payment               $(printf "%-20s" "${S_RT1:-N/A}") ${T_RT1:-N/A}
  Order-Status          $(printf "%-20s" "${S_RT2:-N/A}") ${T_RT2:-N/A}
  Delivery              $(printf "%-20s" "${S_RT3:-N/A}") ${T_RT3:-N/A}
  Stock-Level           $(printf "%-20s" "${S_RT4:-N/A}") ${T_RT4:-N/A}

--------------------------------------------------------------------------------
  TRANSACTION COUNTS (success / late)
--------------------------------------------------------------------------------
  Transaction           SQLite              Turso
  New-Order             $(printf "%-20s" "${S_SC0:-0} / ${S_LT0:-0}") ${T_SC0:-0} / ${T_LT0:-0}
  Payment               $(printf "%-20s" "${S_SC1:-0} / ${S_LT1:-0}") ${T_SC1:-0} / ${T_LT1:-0}
  Order-Status          $(printf "%-20s" "${S_SC2:-0} / ${S_LT2:-0}") ${T_SC2:-0} / ${T_LT2:-0}
  Delivery              $(printf "%-20s" "${S_SC3:-0} / ${S_LT3:-0}") ${T_SC3:-0} / ${T_LT3:-0}
  Stock-Level           $(printf "%-20s" "${S_SC4:-0} / ${S_LT4:-0}") ${T_SC4:-0} / ${T_LT4:-0}

--------------------------------------------------------------------------------
  VERDICT
--------------------------------------------------------------------------------
  Turso / SQLite ratio: ${RATIO}x
  Winner: ${WINNER} (+${PCT}%)
================================================================================

  Logs: $SQLITE_LOG
        $TURSO_LOG
EOF
} | tee "$REPORT"

echo ""
echo "Report saved to: $REPORT"
