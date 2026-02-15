#!/bin/bash
# SQLRight fuzzer for Turso - Production run script.
# Usage: ./run_production.sh [OPTIONS]
#   --oracle NOREC|TLP   Oracle type (default: NOREC)
#   --cores N            Number of parallel instances (default: 1)
#   --timeout SECONDS    Duration, 0 = forever (default: 0)
#   --resume             Continue from the most recent run with the same oracle
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIMBO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ORACLE=NOREC
CORES=1
TIMEOUT=0
RESUME=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --oracle) ORACLE="$2"; shift 2 ;;
        --cores) CORES="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --resume) RESUME=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

BUILD_DIR="$SCRIPT_DIR/build"
AFL="$BUILD_DIR/afl-fuzz"
TURSODB="$LIMBO_ROOT/target/fuzzing/tursodb"
SEEDS_DIR="$SCRIPT_DIR/seeds"
RESULTS_DIR="$SCRIPT_DIR/results"

# Verify setup
if [ ! -x "$AFL" ]; then
    echo "Error: afl-fuzz not found at $AFL"
    echo "Run setup first: $SCRIPT_DIR/setup.sh"
    exit 1
fi

if [ ! -x "$TURSODB" ]; then
    echo "Error: tursodb not found at $TURSODB"
    echo "Build it first: cargo afl build --profile fuzzing --bin tursodb"
    exit 1
fi

export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
export AFL_SKIP_CPUFREQ=1
export AFL_MAP_SIZE=2097152
export AFL_OLD_FORKSERVER=1
export AFL_HANG_TMOUT=30000  # 30 seconds - only mark truly stuck queries as hangs
export AFL_SKIP_CRASHES=1    # Skip inputs that crash, don't fuzz them indefinitely
export AFL_NO_AFFINITY=1     # Disable automatic CPU binding to prevent race condition

mkdir -p "$RESULTS_DIR"

AFL_CMD="$AFL -t 5000"  # 5 second timeout for slow SQL queries
# Enable all experimental features to maximize attack surface
AFL_TARGET="-- $TURSODB -q -m list --experimental-views --experimental-strict --experimental-triggers --experimental-index-method --experimental-autovacuum --experimental-attach --experimental-encryption"

# Find the most recent run directory for this oracle (for --resume)
if [ "$RESUME" = true ]; then
    OUTPUT=$(ls -dt "$RESULTS_DIR"/run_${ORACLE}_* 2>/dev/null | head -1)
    if [ -n "$OUTPUT" ] && [ -d "$OUTPUT/primary/queue" ]; then
        echo "Resuming from $OUTPUT..."
        INPUT_FLAG="-"
    else
        echo "No previous run found for oracle $ORACLE, starting fresh."
        RESUME=false
    fi
fi

if [ "$RESUME" = false ]; then
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    OUTPUT="$RESULTS_DIR/run_${ORACLE}_${TIMESTAMP}"

    # Extract seeds from sqltest corpus
    echo "Extracting seeds from sqltest corpus..."
    cargo run --manifest-path "$LIMBO_ROOT/Cargo.toml" --profile fuzzing --bin test-runner -- \
        extract-sql "$LIMBO_ROOT/testing/runner/tests/" \
        --output-dir "$SEEDS_DIR"
    echo "Seeds: $(ls "$SEEDS_DIR" | wc -l) files"

    INPUT_FLAG="$SEEDS_DIR"
fi

echo "=== SQLRight Turso Fuzzer ==="
echo "Oracle:   $ORACLE"
echo "Cores:    $CORES"
echo "Timeout:  ${TIMEOUT}s (0=forever)"
echo "Output:   $OUTPUT"
echo "Binary:   $TURSODB"
echo "Resume:   $RESUME"
echo ""

# Limit cores to avoid using CPU 0 (reserved for system)
RESERVED_CORES=1  # Reserve CPU 0 for system interrupts
MAX_CORES=63
AVAILABLE_CORES=$(($(nproc) - RESERVED_CORES))
if [ "$AVAILABLE_CORES" -gt "$MAX_CORES" ]; then
    AVAILABLE_CORES=$MAX_CORES
fi
if [ "$CORES" -gt "$AVAILABLE_CORES" ]; then
    echo "WARNING: Requested $CORES cores, limiting to $AVAILABLE_CORES (CPU 0 reserved, max $MAX_CORES for safety)"
    CORES=$AVAILABLE_CORES
fi
echo "Using $CORES cores (CPUs 1-$CORES), CPU 0 reserved for system"
echo ""

# Set up base working directory
BASE_WORK_DIR=$(mktemp -d /tmp/sqlright_work_XXXXXX)

PIDS=()
cleanup() {
    echo ""
    echo "Stopping all fuzzer instances..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null

    # Print summary from all instances
    echo ""
    echo "=== Final Results ==="
    total_crashes=0
    total_hangs=0
    total_execs=0
    for stats in "$OUTPUT"/*/fuzzer_stats; do
        [ -f "$stats" ] || continue
        instance=$(basename "$(dirname "$stats")")
        crashes=$(grep "unique_crashes" "$stats" | awk '{print $3}')
        hangs=$(grep "unique_hangs" "$stats" | awk '{print $3}')
        execs=$(grep "execs_done" "$stats" | awk '{print $3}')
        bitmap=$(grep "bitmap_cvg" "$stats" | awk '{print $3}')
        echo "  $instance: execs=$execs bitmap=$bitmap crashes=$crashes hangs=$hangs"
        total_crashes=$((total_crashes + crashes))
        total_hangs=$((total_hangs + hangs))
        total_execs=$((total_execs + execs))
    done
    echo "  TOTAL: execs=$total_execs crashes=$total_crashes hangs=$total_hangs"

    rm -rf "$BASE_WORK_DIR"
    echo "Done."
}
trap cleanup INT TERM EXIT

echo "Fix: Each instance has its own working directory to avoid file contention"
echo ""

# Helper function to create per-instance working directory
setup_work_dir() {
    local instance_name=$1
    local work_dir="$BASE_WORK_DIR/$instance_name"
    mkdir -p "$work_dir"
    cp -r "$BUILD_DIR/init_lib" "$work_dir/"
    cp "$BUILD_DIR/pragma" "$work_dir/"
    touch "$work_dir/map_id_triggered.txt"
    echo "$work_dir"
}

# Launch primary instance with its own working directory
PRIMARY_WORK_DIR=$(setup_work_dir "primary")
PRIMARY_CPU=1
(cd "$PRIMARY_WORK_DIR" && taskset -c $PRIMARY_CPU $AFL_CMD -M primary -i "$INPUT_FLAG" -o "$OUTPUT" -c 0 -O "$ORACLE" -m none $AFL_TARGET) &
PIDS+=($!)
echo "  primary (PID $!, CPU $PRIMARY_CPU) started in $PRIMARY_WORK_DIR"

if [ "$CORES" -gt 1 ]; then
    sleep 2
    for i in $(seq 2 "$CORES"); do
        SEC_WORK_DIR=$(setup_work_dir "secondary_$i")
        SEC_CPU=$i
        (cd "$SEC_WORK_DIR" && taskset -c $SEC_CPU $AFL_CMD -S "secondary_$i" -i "$INPUT_FLAG" -o "$OUTPUT" -c 0 -O "$ORACLE" -m none $AFL_TARGET) &
        PIDS+=($!)
        echo "  secondary_$i (PID $!, CPU $SEC_CPU) started in $SEC_WORK_DIR"
    done
fi

echo ""
echo "Stats: cat $OUTPUT/primary/fuzzer_stats"
echo "Stop:  Ctrl+C"
echo ""

# Verify CPU affinity
verify_affinity() {
    sleep 3
    echo "=== Verifying CPU Affinity ==="
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            affinity=$(taskset -cp "$pid" 2>/dev/null | awk '{print $NF}')
            instance=$(ps -o args= -p "$pid" 2>/dev/null | grep -oP '(-M|-S) \K\S+' || echo "unknown")
            echo "  $instance (PID $pid): CPU affinity = $affinity"
        fi
    done
    echo ""
}

verify_affinity

# If timeout, wait then cleanup; otherwise wait for primary
if [ "$TIMEOUT" -gt 0 ] 2>/dev/null; then
    sleep "$TIMEOUT" || true
else
    wait "${PIDS[0]}" 2>/dev/null || true
fi
